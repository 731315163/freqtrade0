

"""
Main Freqtrade bot script.
Read the documentation to know what cli arguments you need.
"""
import copy
import logging
from re import I
import sys
from collections.abc import MutableSequence
from datetime import datetime, timedelta, timezone, tzinfo
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Tuple, overload

#!/usr/bin/env python3
"""
Main Freqtrade bot script.
Read the documentation to know what cli arguments you need.
"""

import logging
import sys
from typing import Any
# check min. python version
if sys.version_info < (3, 10):  # pragma: no cover  # noqa: UP036
    sys.exit("Freqtrade requires Python version >= 3.10")


import copy
from freqtrade import __version__
from freqtrade.commands import Arguments
from freqtrade.constants import DOCS_LINK
from freqtrade.exceptions import ConfigurationError, FreqtradeException, OperationalException
from freqtrade.loggers import setup_logging_pre
from freqtrade.system import asyncio_setup, gc_set_threshold, print_version_info

from freqtrade.configuration import TimeRange
from freqtrade.data.history import history_utils
from freqtrade.enums import CandleType
from . import start_trading
from . import hp,cmd
from ..interface import IStrategy
from .timeframestr import TimeFrameStr
logger = logging.getLogger("freqtrade")



class Runer:
    """
    "1m", "5m", "15m", "30m", "1h", "2h", "4h","8h","12h", "1d", "1w", "1M"
    """

    def __init__(
        self,
        user_data_path: Path,
        timeframe: timedelta | None = None,
        configpath: Path | None = None,
        logfile: Path | None = None,
        loglevel: str = "INFO",
        name: str = "",
        strategy_name: str = "",
        strategy: IStrategy | None = None,
    ):
        setup_logging_pre()
        asyncio_setup()
        self.name = name
        self.state = None
        self.user_data_path = user_data_path
        self.logfile = logfile if logfile else user_data_path / "logs" / "log.txt"
        if configpath is not None and configpath.exists():
            self.configpath = configpath
        else:
            cp = user_data_path / "config.json"
            if cp.exists():
                self.configpath = cp

        self.strategy_name = strategy_name
        self.strategy = strategy
        self.timeframe = timeframe

    def add_basecommands(
        self,
        commands: list,
        confpath: Path | None = None,
        timeframe: timedelta | None = None,
        timeframedetail: timedelta | None = None,
        starttime: datetime | None = None,
        endtime: datetime | None = None,
        strategyorname: IStrategy | str | None = None,
        verb: Literal["-v", "-vv", "-vvv"] | None = None,
    ):

        confpath = self.configpath if confpath is None else confpath
        commands += [cmd.config, confpath, cmd.logfile, self.logfile]
        if timeframedetail :
            commands += [cmd.timeframedetail, str(TimeFrameStr(timeframedetail))]
        if timeframe :
            commands += [cmd.timeframe, str(TimeFrameStr(timeframe))]
        if starttime :
            commands += [cmd.timerange, Runer.timestr(start=starttime, end=endtime)]
        if strategyorname :
            commands += [cmd.strategy, strategyorname]
        if verb :
            commands.append(verb)
        return commands

    @staticmethod
    def _elements_to_str(ary: MutableSequence):
        ret = copy.copy(ary)
        for index in range(len(ret)):
            v = ret[index]
            if isinstance(v, str):
                continue
            elif isinstance(v, Enum):
                ret[index] = v.value
            elif isinstance(v, IStrategy):
                ret[index] = v
            else:
                ret[index] = str(v)
        return ret

    def trade(self, confp=None):
        if confp is None:
            confp = self.configpath
        commandlist = ["trade", cmd.config, confp, cmd.strategy, self.strategy_name]
        args = self.get_arguments(commandlist)
        start_trading(args,self.strategy)

    def read_data(
        self,
        pair: str,
        timeframe: timedelta,
        exchange="binance",
        timerange: tuple[datetime, datetime] | None = None,
        candletype=CandleType.FUTURES,
    ):
        if timerange is None:
            tr = timeframe
        else:
            b, e = timerange
            tr = TimeRange(startts=int(b.timestamp()), stopts=int(e.timestamp()))
        return history_utils.load_pair_history(
            datadir=self.user_data_path / "data" / exchange,
            pair=pair,
            timeframe=str(TimeFrameStr(freq=timeframe)),
            timerange=tr,
            candle_type=candletype,
        )

    # download-data --dl-trades --timeframes 1m -c user_data/config.json --timerange 20230101-
    def backtesting(
        self,
        start: datetime,
        end: datetime | None = None,
        confp: Path | None = None,
        timeframedetail: timedelta | None =None,
        timeframe: timedelta | None = None,
    ):

        commandslist = [
            "backtesting",
            cmd.strategy,
            self.strategy_name,
            "--cache",
            "none",
        ]
        # if eps:
        #     commandslist.append("--eps")
        timeframe = timeframe if timeframe else self.timeframe
        commandslist = self.add_basecommands(
            commandslist,
            confpath=confp,
            timeframe=timeframe,
            timeframedetail=timeframedetail,
            starttime=start,
            endtime=end,
        )

        args = self.get_arguments(commandslist)
        self._run(args)

    def lookahead_analysis(
        self,
        start: datetime,
        end: datetime | None = None,
        timeframe: timedelta | None = None,
        timeframedetail: timedelta | None = timedelta(minutes=1),
    ):
        nowdatastr = datetime.now().strftime("%Y%m%d_%H%M")
        commandslist = [
            "lookahead-analysis",
            cmd.strategy,
            self.strategy_name,
            "--lookahead-analysis-exportfilename",
            self.user_data_path
            / "analysis"
            / f"lookaheadanalysis_{self.strategy_name}_{nowdatastr}.csv",
        ]
        timeframe = timeframe if timeframe else self.timeframe
        commandslist = self.add_basecommands(
            commands=commandslist,
            timeframe=timeframe,
            timeframedetail=timeframedetail,
            starttime=start,
            endtime=end,
            # verb="-v"
        )
        args = self.get_arguments(commandslist)
        self._run(args)

    def recursive_analysis(
        self,
        start: datetime,
        end: datetime | None = None,
        timeframe: timedelta | None = None,
    ):
        nowdatastr = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
        timeframe = timeframe if timeframe else self.timeframe
        commandslist = [
            "recursive-analysis",
            cmd.strategy,
            self.strategy_name,
            "--logfile",
            self.user_data_path
            / "analysis"
            / f"recursiveanalysis_{self.strategy_name}_{nowdatastr}.csv",
        ]
        commandslist = self.add_basecommands(
            commands=commandslist, timeframe=timeframe, starttime=start, endtime=end, verb="-vv"
        )
        args = self.get_arguments(commandslist)
        self._run(args)

    def edge(
        self,
        start: datetime,
        end: datetime | None = None,
        timeframe: timedelta | None = None,
    ):
        nowdatastr = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")

        commandslist = [
            "edge",
            cmd.strategy,
            self.strategy_name,
            "--logfile",
            self.user_data_path
            / "analysis"
            / f"edge_{self.strategy_name}_{nowdatastr}.csv",
        ]
        commandslist = self.add_basecommands(
            commandslist, timeframe=timeframe, starttime=start, endtime=end, verb="-v"
        )
        args = self.get_arguments(commandslist)
        self._run(args)

    def hyperparameter_optimize(
        self,
        space: MutableSequence[str],
        start: datetime,
        end: datetime | None = None,
        confp=None,
        epochs: int = 100,
        lossmethod: str = hp.SharpeHyperOptLoss,
        timeframedetail: timedelta | None = None,
        timeframe: timedelta | None = None,
        randomstate=3405,
    ):
        timerange = Runer.timestr(start, end)

        commandslist = [
            "hyperopt",
            cmd.timerange,
            timerange,
            cmd.strategy,
            self.strategy_name,
            "--space",
            *space,
            "--epochs",
            epochs,
            "--hyperopt-loss",
            lossmethod,
            "--random-state",
            randomstate,
        ]
        # if eps:
        #     commandslist.append("--eps")
        timeframe = timeframe if timeframe else self.timeframe
        commandslist = self.add_basecommands(
            commandslist, confp, timeframe, timeframedetail
        )

        args = self.get_arguments(commandslist)
        self._run(args)

    def hyperopt_show(self, n: int, disabexport=True, filename: str | None = None):
        commandslist = ["hyperopt-show", "--index", n]
        if filename is not None:
            commandslist += ["--hyperopt-filename", filename]
        if disabexport is True:
            commandslist.append("--disable-param-export")

        args = self.get_arguments(commandslist)
        self._run(args)

    def plot_dataframe(
        self,
        start: datetime,
        end: datetime | None = None,
        confp: Path | None = None,
        timeframe: timedelta | None = None,
        indicators1=None,
        indicators2=None,
    ):
        commandslist = ["plot-dataframe", cmd.strategy, self.strategy_name]
        timeframe = timeframe if timeframe else self.timeframe
        commandslist = self.add_basecommands(
            commands=commandslist,
            confpath=confp,
            timeframe=timeframe,
            starttime=start,
            endtime=end,
        )
        if indicators1 is not None:
            commandslist.extend(["--indicators1", *indicators1])
        if indicators2 is not None:
            commandslist.extend(["--indicators2", *indicators2])
        args = self.get_arguments(commandslist)
        self._run(args)

    def plot_profit(
        self,
        start: datetime,
        end: datetime | None = None,
        auto_open=True,
        confp: Path | None = None,
        timeframedetail: timedelta | None = timedelta(minutes=1),
        timeframe: timedelta | None = None,
    ):
        commandslist = [
            "plot-profit",
            cmd.strategy,
            self.strategy_name,
            "--auto-open",
            auto_open,
        ]

        commandslist = self.add_basecommands(
            commandslist,
            confpath=confp,
            timeframe=timeframe,
            timeframedetail=timeframedetail,
            starttime=start,
            endtime=end,
        )

        args = self.get_arguments(commandslist)
        self._run(args)

    @staticmethod
    def timestr(start: datetime | None = None, end: datetime | None = None):
        
        if start is not None:
            timestr = start.strftime("%Y%m%d") + "-"
        else:
            timestr = "-"
        if end is not None:
            timestr += end.strftime("%Y%m%d")
        return timestr

    @overload
    def download(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        configpath: str | None = None,
        pairs: list[str] | None = None,
        timeframe: timedelta | None = None,
        erase=False,
    ): ...

    @overload
    def download(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        configpath: Path | None = None,
        pairs: Path | None = None,
        timeframe: timedelta | None = None,
        erase=False,
    ): ...

    def download(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        configpath=None,
        pairs=None,
        timeframe: timedelta | None = None,
        erase=False,
    ):

        commandslist = ["download-data"]
        timeframe = timeframe if timeframe else self.timeframe
        commandslist = self.add_basecommands(
            commands=commandslist,
            confpath=configpath,
            timeframe=timeframe,
            starttime=start_date,
            endtime=end_date,
        )

        if pairs is not None:
            if isinstance(pairs, Path):
                commandslist += ["--pairs-file", pairs]
            elif isinstance(pairs, list):
                commandslist.append("--pairs")
                commandslist += pairs
        args = self.get_arguments(commandslist)
        # args["erase"] = erase
        self._run(args)
    def webserver(self, 
        configpath: Path | None = None,
       ):
        commandslist = ["webserver"]
        commandslist = self.add_basecommands(
            commands=commandslist,
            confpath=configpath,
        )
        args = self.get_arguments(commandslist)
        self._run(args)
    def get_arguments(self, sysargv: MutableSequence):
        sysargv = Runer._elements_to_str(sysargv)

        return sysargv

    def _run(self, sysargv: MutableSequence[str],fun=None):
        return_code: Any = 1
        try:
            
            arguments = Arguments(sysargv)
            args = arguments.get_parsed_arg()

            # Call subcommand.
            if args.get("version") or args.get("version_main"):
                print_version_info()
                return_code = 0
            elif "func" in args:
                logger.info(f"freqtrade {__version__}")
                gc_set_threshold()
                fun = fun if fun else args["func"]
                return_code = fun(args)
            else:
                # No subcommand was issued.
                raise OperationalException(
                    "Usage of Freqtrade requires a subcommand to be specified.\n"
                    "To have the bot executing trades in live/dry-run modes, "
                    "depending on the value of the `dry_run` setting in the config, run Freqtrade "
                    "as `freqtrade trade [options...]`.\n"
                    "To see the full list of options available, please use "
                    "`freqtrade --help` or `freqtrade <command> --help`."
                )

        except SystemExit as e:  # pragma: no cover
            return_code = e
        except KeyboardInterrupt:
            logger.info("SIGINT received, aborting ...")
            return_code = 0
        except ConfigurationError as e:
            logger.error(
                f"Configuration error: {e}\n"
                f"Please make sure to review the documentation at {DOCS_LINK}."
            )
        except FreqtradeException as e:
            logger.error(str(e))
            return_code = 2
        except Exception:
            logger.exception("Fatal exception!")
        finally:
            return return_code
