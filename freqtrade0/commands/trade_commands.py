import logging
import signal
from typing import Any
from freqtrade0.worker import Worker

logger = logging.getLogger(__name__)


def start_trading(args: dict[str, Any]) -> int:
    """
    Main entry point for trading mode
    """
    # Import here to avoid loading worker module when it's not used
    

    def term_handler(signum, frame):
        # Raise KeyboardInterrupt - so we can handle it in the same way as Ctrl-C
        raise KeyboardInterrupt()

    # Create and run worker
    worker = None
    try:
        signal.signal(signal.SIGTERM, term_handler)
        worker = Worker(args)
        worker.run()
    finally:
        if worker:
            logger.info("worker found ... calling exit")
            worker.exit()
    return 0
