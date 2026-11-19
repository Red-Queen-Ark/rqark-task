import asyncio
import logging
from collections.abc import Awaitable
from typing import TypeVar
from uuid import uuid4

from redis.asyncio import Redis

from rqark_task.config import TASK_LEADER_TTL
from rqark_task.redis import get_redis

logger = logging.getLogger(__name__)

_RENEW = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"
_RELEASE = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"

_CALL_TIMEOUT = 10.0

_T = TypeVar('_T')


class LeaderLease:
    """Lets one worker own the task queue at a time.

    Every uvicorn worker runs its own TaskLoop, so without a lease they all
    claim and TASK_CONCURRENCY ends up multiplied by the worker count. The
    lease is a short-lived Redis key the leader keeps renewing: a leader that
    dies hands the queue over within one TTL, and the worker that takes over
    does not need to know who held it before.

    Ownership cannot be confirmed without Redis, so a worker that cannot reach
    it stops claiming rather than risk a second leader. Tasks simply wait for
    Redis to come back, which is at most one renewal interval away.
    """

    def __init__(self, key: str = 'tasks:leader', ttl: float = TASK_LEADER_TTL, timeout: float = _CALL_TIMEOUT):
        self._key = key
        self._ttl = max(int(ttl), 3)
        self._timeout = max(timeout, 0.1)
        self._owner = uuid4().hex
        self._task: asyncio.Task[None] | None = None
        self._leader = False

    @property
    def is_leader(self) -> bool:
        return self._leader

    @property
    def key(self) -> str:
        return self._key

    def _interval(self) -> float:
        return max(self._ttl / 3, 1.0)

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._attempt()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval())
            try:
                await self._attempt()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('task lease worker failed, staying idle key=%s', self._key)
                self._leader = False

    async def _call(self, awaitable: Awaitable[_T]) -> _T:
        return await asyncio.wait_for(awaitable, self._timeout)

    async def _attempt(self) -> None:
        try:
            redis: Redis | None = await self._call(get_redis())
        except Exception as error:
            logger.warning('task lease cannot reach redis key=%s error=%s', self._key, error)
            self._leader = False
            return
        if redis is None:
            logger.warning('task lease idle: redis is not configured key=%s', self._key)
            self._leader = False
            return

        if self._leader:
            try:
                renewed = await self._call(redis.eval(_RENEW, 1, self._key, self._owner, self._ttl))
            except Exception as error:
                logger.warning('task lease renew failed, giving up leadership key=%s error=%s', self._key, error)
                self._leader = False
                return
            if not renewed:
                self._leader = False
                logger.error('task leadership lost key=%s owner=%s', self._key, self._owner)
            return

        try:
            if await self._call(redis.set(self._key, self._owner, nx=True, ex=self._ttl)):
                self._leader = True
                logger.info('task leadership acquired key=%s owner=%s ttl=%ds', self._key, self._owner, self._ttl)
        except Exception as error:
            logger.warning('task lease acquire failed key=%s error=%s', self._key, error)

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if not self._leader:
            return
        self._leader = False
        try:
            redis: Redis | None = await self._call(get_redis())
            if redis is None:
                return
            await self._call(redis.eval(_RELEASE, 1, self._key, self._owner))
        except Exception as error:
            logger.warning('task lease release failed key=%s error=%s', self._key, error)
