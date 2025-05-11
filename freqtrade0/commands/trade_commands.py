import logging
import signal
from typing import Any

from ..interface import IStrategy
logger = logging.getLogger(__name__)


def start_trading(args: dict[str, Any],strategy:IStrategy|None=None) -> int:
    """
    Main entry point for trading mode
    """
    # Import here to avoid loading worker module when it's not used
    from ..worker import Worker

    def term_handler(signum, frame):
        # Raise KeyboardInterrupt - so we can handle it in the same way as Ctrl-C
        raise KeyboardInterrupt()

    # Create and run worker
    worker = None
    try:
        signal.signal(signal.SIGTERM, term_handler)
        worker = Worker(args,strategy)
        worker.run()
    finally:
        if worker:
            logger.info("worker found ... calling exit")
            worker.exit()
    return 0
