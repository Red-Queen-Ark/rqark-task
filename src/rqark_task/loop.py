import asyncio
import logging
from collections.abc import Awaitable, Callable

import httpx

from rqark_task.config import DATA_CLOUD_URL, INTERNAL_TOKEN, TASK_CONCURRENCY, TASK_TICK_SECONDS
from rqark_task.leader import LeaderLease
from rqark_task.runner import ClaimedRun, InternalClient, execute

logger = logging.getLogger(__name__)


class TaskLoop:
    def __init__(
        self,
        client: InternalClient | None = None,
        concurrency: int = TASK_CONCURRENCY,
        tick_seconds: float = TASK_TICK_SECONDS,
        runner: Callable[..., Awaitable[None]] = execute,
        lease: LeaderLease | None = None,
    ):
        self.client = client or InternalClient()
        self.concurrency = max(concurrency, 1)
        self.tick_seconds = tick_seconds if tick_seconds > 0 else 1.0
        self._runner = runner
        self._lease = lease or LeaderLease()
        self._inflight: set[asyncio.Task] = set()
        self._task: asyncio.Task | None = None
        self._blocked_logged = False
        self._not_configured_logged = False
        self._not_leader_logged = False

    @property
    def configured(self) -> bool:
        return bool(DATA_CLOUD_URL and INTERNAL_TOKEN)

    def busy(self) -> int:
        return len(self._inflight)

    def free_slots(self) -> int:
        return max(self.concurrency - len(self._inflight), 0)

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._lease.start()
        self._task = asyncio.create_task(self._run())
        logger.info('task loop started tick=%.1fs concurrency=%d leader=%s', self.tick_seconds, self.concurrency, self._lease.is_leader)

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        pending = list(self._inflight)
        for run in pending:
            run.cancel()
        for run in pending:
            try:
                await run
            except (asyncio.CancelledError, Exception):
                pass
        await self._lease.stop()
        logger.info('task loop stopped')

    async def tick(self) -> int:
        if not self._lease.is_leader:
            if not self._not_leader_logged:
                self._not_leader_logged = True
                logger.info('task loop idle: another worker holds the lease')
            return 0
        self._not_leader_logged = False
        slots = self.free_slots()
        if slots == 0:
            return 0

        try:
            runs = await self.client.claim(slots)
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status == 401 and not self._blocked_logged:
                self._blocked_logged = True
                logger.error('task claim refused, check INTERNAL_TOKEN: %s', status)
            elif status != 401:
                logger.warning('task claim failed status=%s', status)
            return 0
        except Exception:
            logger.exception('task claim failed')
            return 0

        self._blocked_logged = False
        for run in runs:
            self._spawn(run)
        return len(runs)

    def _spawn(self, run: ClaimedRun) -> None:
        task = asyncio.create_task(self._run_one(run))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _run_one(self, run: ClaimedRun) -> None:
        try:
            await self._runner(self.client, run)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('task run crashed run=%s', run.run_id)

    async def _run(self) -> None:
        while True:
            if not self.configured:
                if not self._not_configured_logged:
                    self._not_configured_logged = True
                    logger.warning('task loop idle: DATA_CLOUD_URL or INTERNAL_TOKEN is not set')
            else:
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception('task tick crashed')
            await asyncio.sleep(self.tick_seconds)


_loop: TaskLoop | None = None


async def start() -> TaskLoop | None:
    global _loop
    if _loop is not None:
        return _loop

    _loop = TaskLoop()
    await _loop.start()
    return _loop


async def stop() -> None:
    global _loop
    loop = _loop
    _loop = None
    if loop is not None:
        await loop.stop()


def current() -> TaskLoop | None:
    return _loop
