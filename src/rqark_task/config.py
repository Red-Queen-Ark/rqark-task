import os


def _bool_env(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() == 'true'


def _int_env(name: str, default: str) -> int:
    return int(os.getenv(name, default))


def _float_env(name: str, default: str) -> float:
    return float(os.getenv(name, default))


REDIS_SERVER = os.getenv('REDIS_SERVER', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_URL = os.getenv('REDIS_URL') or f'redis://{REDIS_SERVER}:{REDIS_PORT}/0'

DATA_CLOUD_URL = os.getenv('DATA_CLOUD_URL', 'http://localhost:7302')
DATA_CLOUD_TIMEOUT = _float_env('DATA_CLOUD_TIMEOUT', '30')
INTERNAL_TOKEN = os.getenv('INTERNAL_TOKEN', '')

TASK_RUN_TIMEOUT = _float_env('TASK_RUN_TIMEOUT', '600')
TASK_TICK_SECONDS = _float_env('TASK_TICK_SECONDS', '30')
TASK_CONCURRENCY = _int_env('TASK_CONCURRENCY', '1')
TASK_STREAM_MAX_CHARS = _int_env('TASK_STREAM_MAX_CHARS', str(200_000))
TASK_LEADER_TTL = _float_env('TASK_LEADER_TTL', '60')

SANDBOX_ENABLED = _bool_env('SANDBOX_ENABLED', 'true')
SANDBOX_IMAGE = os.getenv('SANDBOX_IMAGE', 'ubuntu:24.04')
WORKSPACE_START_TIMEOUT = _float_env('WORKSPACE_START_TIMEOUT', '30')
