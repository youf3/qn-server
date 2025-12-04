"""
Main module.
"""
import asyncio
import os
import signal
import logging
import uvloop

from types import FrameType
from typing import Optional
from quantnet_controller.common.config import Config

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger = logging.getLogger(__name__)


def handle_register(online, tokens, queue, modules):
    print(online)
    print(tokens)
    print(queue)
    print(modules)


class QuantnetClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.started = False
        self.should_exit = False
        self.force_exit = False
        self.registry = None

    def run(self) -> None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return asyncio.run(self.serve())

    async def serve(self) -> None:
        process_id = os.getpid()

        """ install signal handlers """
        loop = asyncio.get_event_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, self.handle_exit, signal.SIGINT, None)
            loop.add_signal_handler(signal.SIGTERM, self.handle_exit, signal.SIGTERM, None)
        except NotImplementedError:
            return

        message = "Started server process [%d]"
        logger.info(message, process_id)

        await self.startup()
        if self.should_exit:
            return
        await self.main_loop()
        await self.shutdown()

        message = "Finished server process [%d]"
        logger.info(message, process_id)

    async def startup(self) -> None:
        self.started = True

    async def main_loop(self) -> None:
        counter = 0
        should_exit = self.should_exit
        while not should_exit:
            counter += 1
            counter = counter % 864000
            await asyncio.sleep(0.1)
            should_exit = self.should_exit

    async def shutdown(self) -> None:
        logger.info("Shutting down")

    def handle_exit(self, sig: int, frame: Optional[FrameType]) -> None:
        if self.should_exit and sig == signal.SIGINT:
            self.force_exit = True
        else:
            self.should_exit = True
