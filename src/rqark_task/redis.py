import logging

from redis.asyncio import ConnectionPool, Redis

from rqark_task.config import REDIS_URL

logger = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


def _init_pool() -> ConnectionPool | None:
    global _pool
    if _pool is not None:
        return _pool
    try:
        _pool = ConnectionPool.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
    except Exception as exc:
        logger.debug('Redis unavailable, caching disabled: %s', exc)
        _pool = None  # type: ignore[assignment]
    return _pool


async def get_redis() -> Redis | None:
    pool = _init_pool()
    if pool is None:
        return None
    return Redis(connection_pool=pool)
