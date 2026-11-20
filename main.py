import argparse
import asyncio
import signal

from rqark_task import loop as task_loop
from rqark_task.logging import setup_logging
from rqark_task.memory import freeze, release_memory
from rqark_task.runner import run_once


async def serve() -> None:
    await task_loop.start()
    stop = asyncio.Event()
    running = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        running.add_signal_handler(name, stop.set)
    try:
        await stop.wait()
    finally:
        await task_loop.stop()


async def once(limit: int) -> int:
    try:
        return await run_once(limit)
    finally:
        release_memory()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog='rqark-task', description='Run the scheduled agent runs the data cloud hands out.')
    parser.add_argument('--once', action='store_true', help='claim one batch, run it, then exit')
    parser.add_argument('--limit', type=int, default=1, help='runs to claim in --once mode')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging()
    freeze()
    if args.once:
        asyncio.run(once(max(args.limit, 1)))
        return
    asyncio.run(serve())


if __name__ == '__main__':
    main()
