"""
Main Freqtrade worker class.
"""

import logging
import time
import traceback
from collections.abc import Callable
from os import getpid
from typing import Any

import sdnotify

from freqtrade import __version__
from freqtrade.configuration import Configuration
from freqtrade.constants import PROCESS_THROTTLE_SECS, RETRY_TIMEOUT, Config
from freqtrade.enums import RPCMessageType, State
from freqtrade.exceptions import OperationalException, TemporaryError
from .freqtradebot import FreqtradeBot
from .interface import IStrategy

logger = logging.getLogger(__name__)


class Worker:
    """
    Freqtradebot worker class
    """

    def __init__(self, args: dict[str, Any], config: Config | None = None,strategy:IStrategy|None=None) -> None:
        """
        Init all variables and objects the bot needs to work
        """
        logger.info(f"Starting worker {__version__}")

        self._args = args
        self._config = config
        self._init(False,strategy)

        self._heartbeat_msg: float = 0

        # Tell systemd that we completed initialization phase
        self._notify("READY=1")

    def _init(self, reconfig: bool,strategy:IStrategy|None=None) -> None:
        """
        Also called from the _reconfigure() method (with reconfig=True).
        """
        if reconfig or self._config is None:
            # Load configuration
            self._config = Configuration(self._args, None).get_config()

        # Init the instance of the bot
        self.freqtrade = FreqtradeBot(self._config,strategy=strategy)

        internals_config = self._config.get("internals", {})
        self._throttle_secs = internals_config.get("process_throttle_secs", PROCESS_THROTTLE_SECS)
        self._heartbeat_interval = internals_config.get("heartbeat_interval", 60)

        self._sd_notify = (
            sdnotify.SystemdNotifier()
            if self._config.get("internals", {}).get("sd_notify", False)
            else None
        )

    def _notify(self, message: str) -> None:
        """
        Removes the need to verify in all occurrences if sd_notify is enabled
        :param message: Message to send to systemd if it's enabled.
        """
        if self._sd_notify:
            logger.debug(f"sd_notify: {message}")
            self._sd_notify.notify(message)

    def run(self) -> None:
        state = None
        while True:
            state = self._worker(old_state=state)
            if state == State.RELOAD_CONFIG:
                self._reconfigure()

    def _worker(self, old_state: State | None) -> State:
        """
        The main routine that runs each throttling iteration and handles the states.
        :param old_state: the previous service state from the previous call
        :return: current service state
        """
        state = self.freqtrade.state

        # Log state transition
        if state != old_state:
            if old_state != State.RELOAD_CONFIG:
                self.freqtrade.notify_status(f"{state.name.lower()}")

            logger.info(
                f"Changing state{f' from {old_state.name}' if old_state else ''} to: {state.name}"
            )
            if state in (State.RUNNING, State.PAUSED) and old_state not in (
                State.RUNNING,
                State.PAUSED,
            ):
                self.freqtrade.startup()

            if state == State.STOPPED:
                self.freqtrade.check_for_open_trades()

            # Reset heartbeat timestamp to log the heartbeat message at
            # first throttling iteration when the state changes
            self._heartbeat_msg = 0

        if state == State.STOPPED:
            # Ping systemd watchdog before sleeping in the stopped state
            self._notify("WATCHDOG=1\nSTATUS=State: STOPPED.")

            self._throttle(func=self._process_stopped, throttle_secs=self._throttle_secs)

        elif state in (State.RUNNING, State.PAUSED):
            state_str = "RUNNING" if state == State.RUNNING else "PAUSED"
            # Ping systemd watchdog before throttling
            self._notify(f"WATCHDOG=1\nSTATUS=State: {state_str}.")

            # Use an offset of 1s to ensure a new candle has been issued
            self._throttle(
                func=self._process_running,
                throttle_secs=self._throttle_secs,
                timeframe=self._config["timeframe"] if self._config else None,
                timeframe_offset=1,
            )

        if self._heartbeat_interval:
            now = time.time()
            if (now - self._heartbeat_msg) > self._heartbeat_interval:
                version = __version__
                strategy_version = self.freqtrade.strategy.version()
                if strategy_version is not None:
                    version += ", strategy_version: " + strategy_version
                logger.info(
                    f"Bot heartbeat. PID={getpid()}, version='{version}', state='{state.name}'"
                )
                self._heartbeat_msg = now

        return state
    def _gettime(self) -> float:
        return time.time()
    def _throttle(
        self,
        func: Callable[..., Any],
        throttle_secs: float,
        *args,
        **kwargs,
    ) -> Any:
        """
        Throttles the given callable that it
        takes at least `min_secs` to finish execution.
        :param func: Any callable
        :param throttle_secs: throttling iteration execution time limit in seconds
        :param timeframe: ensure iteration is executed at the beginning of the next candle.
        :param timeframe_offset: offset in seconds to apply to the next candle time.
        :return: Any (result of execution of func)
        """
        now = self._gettime
        last_throttle_start_time = now()
        logger.debug("========================================")
        result = func(*args, **kwargs)
        time_passed = now() - last_throttle_start_time
        sleep_duration = throttle_secs - time_passed
        sleep_duration = max(sleep_duration, 0.0)
        if sleep_duration > 0:
            time.sleep(sleep_duration)
        return result

    @staticmethod
    def _sleep(sleep_duration: float) -> None:
        """Local sleep method - to improve testability"""
        time.sleep(sleep_duration)

    def _process_stopped(self) -> None:
        self.freqtrade.process_stopped()

    def _process_running(self) -> None:
        try:
            self.freqtrade.process()
        except TemporaryError as error:
            logger.warning(f"Error: {error}, retrying in {RETRY_TIMEOUT} seconds...")
            time.sleep(RETRY_TIMEOUT)
        except OperationalException:
            tb = traceback.format_exc()
            hint = "Issue `/start` if you think it is safe to restart."

            self.freqtrade.notify_status(
                f"*OperationalException:*\n```\n{tb}```\n {hint}", msg_type=RPCMessageType.EXCEPTION
            )

            logger.exception("OperationalException. Stopping trader ...")
            self.freqtrade.state = State.STOPPED

    def _reconfigure(self) -> None:
        """
        Cleans up current freqtradebot instance, reloads the configuration and
        replaces it with the new instance
        """
        # Tell systemd that we initiated reconfiguration
        self._notify("RELOADING=1")

        # Clean up current freqtrade modules
        self.freqtrade.cleanup()

        # Load and validate config and create new instance of the bot
        self._init(True)

        self.freqtrade.notify_status(f"{State(self.freqtrade.state)} after config reloaded")

        # Tell systemd that we completed reconfiguration
        self._notify("READY=1")

    def exit(self) -> None:
        # Tell systemd that we are exiting now
        self._notify("STOPPING=1")

        if self.freqtrade:
            self.freqtrade.notify_status("process died")
            self.freqtrade.cleanup()
