import asyncio
import signal

from rqark_task import loop as task_loop
from rqark_task.logging import setup_logging


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


def main() -> None:
    setup_logging()
    asyncio.run(serve())


if __name__ == '__main__':
    main()
