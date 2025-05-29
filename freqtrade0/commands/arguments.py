"""
This module contains the argument manager class
"""

from argparse import ArgumentParser, Namespace, _ArgumentGroup
from functools import partial
from pathlib import Path
from typing import Any

from freqtrade.commands.cli_options import AVAILABLE_CLI_OPTIONS
from freqtrade.constants import DEFAULT_CONFIG
from freqtrade.commands.arguments import  ARGS_COMMON,ARGS_MAIN ,ARGS_STRATEGY ,ARGS_TRADE ,ARGS_WEBSERVER,ARGS_COMMON_OPTIMIZE ,ARGS_BACKTEST ,ARGS_HYPEROPT ,ARGS_EDGE ,ARGS_LIST_STRATEGIES ,ARGS_LIST_FREQAIMODELS ,ARGS_LIST_HYPEROPTS ,ARGS_BACKTEST_SHOW ,ARGS_LIST_EXCHANGES ,ARGS_LIST_TIMEFRAMES ,ARGS_LIST_PAIRS ,ARGS_TEST_PAIRLIST,ARGS_CREATE_USERDIR ,ARGS_BUILD_CONFIG ,ARGS_SHOW_CONFIG,ARGS_BUILD_STRATEGY ,ARGS_CONVERT_DATA_TRADES ,ARGS_CONVERT_DATA,ARGS_CONVERT_DATA_OHLCV ,ARGS_CONVERT_TRADES,ARGS_LIST_DATA,ARGS_DOWNLOAD_DATA,ARGS_PLOT_DATAFRAME,ARGS_PLOT_PROFIT,ARGS_CONVERT_DB,ARGS_INSTALL_UI,ARGS_SHOW_TRADES,ARGS_HYPEROPT_LIST,ARGS_HYPEROPT_SHOW,ARGS_ANALYZE_ENTRIES_EXITS ,ARGS_STRATEGY_UPDATER,ARGS_LOOKAHEAD_ANALYSIS,ARGS_RECURSIVE_ANALYSIS,NO_CONF_REQURIED,NO_CONF_ALLOWED

        
class Arguments:
    """
    Arguments Class. Manage the arguments received by the cli
    """

    def __init__(self, args: list[str] | None) -> None:
        self.args = args
        self._parsed_arg: Namespace | None = None

    def get_parsed_arg(self) -> dict[str, Any]:
        """
        Return the list of arguments
        :return: List[str] List of arguments
        """
        if self._parsed_arg is None:
            self._build_subcommands()
            self._parsed_arg = self._parse_args()

        return vars(self._parsed_arg)

    def _parse_args(self) -> Namespace:
        """
        Parses given arguments and returns an argparse Namespace instance.
        """
        parsed_arg = self.parser.parse_args(self.args)

        # Workaround issue in argparse with action='append' and default value
        # (see https://bugs.python.org/issue16399)
        # Allow no-config for certain commands (like downloading / plotting)
        if "config" in parsed_arg and parsed_arg.config is None:
            conf_required = "command" in parsed_arg and parsed_arg.command in NO_CONF_REQURIED

            if "user_data_dir" in parsed_arg and parsed_arg.user_data_dir is not None:
                user_dir = parsed_arg.user_data_dir
            else:
                # Default case
                user_dir = "user_data"
                # Try loading from "user_data/config.json"
            cfgfile = Path(user_dir) / DEFAULT_CONFIG
            if cfgfile.is_file():
                parsed_arg.config = [str(cfgfile)]
            else:
                # Else use "config.json".
                cfgfile = Path.cwd() / DEFAULT_CONFIG
                if cfgfile.is_file() or not conf_required:
                    parsed_arg.config = [DEFAULT_CONFIG]

        return parsed_arg

    def _build_args(self, optionlist: list[str], parser: ArgumentParser | _ArgumentGroup) -> None:
        for val in optionlist:
            opt = AVAILABLE_CLI_OPTIONS[val]
            parser.add_argument(*opt.cli, dest=val, **opt.kwargs)

    def _build_subcommands(self) -> None:
        """
        Builds and attaches all subcommands.
        :return: None
        """
        from freqtrade.commands import (
            start_analysis_entries_exits,
            start_backtesting,
            start_backtesting_show,
            start_convert_data,
            start_convert_db,
            start_convert_trades,
            start_create_userdir,
            start_download_data,
            start_edge,
            start_hyperopt,
            start_hyperopt_list,
            start_hyperopt_show,
            start_install_ui,
            start_list_data,
            start_list_exchanges,
            start_list_freqAI_models,
            start_list_hyperopt_loss_functions,
            start_list_markets,
            start_list_strategies,
            start_list_timeframes,
            start_lookahead_analysis,
            start_new_config,
            start_new_strategy,
            start_plot_dataframe,
            start_plot_profit,
            start_recursive_analysis,
            start_show_config,
            start_show_trades,
            start_strategy_update,
            start_test_pairlist,
            start_webserver,
        )
        from freqtrade0.commands import start_trading
        # Build shared arguments (as group Common Options)
        _common_parser = ArgumentParser(add_help=False)
        group = _common_parser.add_argument_group("Common arguments")
        self._build_args(optionlist=ARGS_COMMON, parser=group)

        _strategy_parser = ArgumentParser(add_help=False)
        strategy_group = _strategy_parser.add_argument_group("Strategy arguments")
        self._build_args(optionlist=ARGS_STRATEGY, parser=strategy_group)

        # Build main command
        self.parser = ArgumentParser(
            prog="freqtrade", description="Free, open source crypto trading bot"
        )
        self._build_args(optionlist=ARGS_MAIN, parser=self.parser)

      
        subparsers = self.parser.add_subparsers(
            dest="command",
            # Use custom message when no subhandler is added
            # shown from `main.py`
            # required=True
        )

        # Add trade subcommand
        trade_cmd = subparsers.add_parser(
            "trade", help="Trade module.", parents=[_common_parser, _strategy_parser]
        )
        trade_cmd.set_defaults(func=start_trading)
        self._build_args(optionlist=ARGS_TRADE, parser=trade_cmd)

        # add create-userdir subcommand
        create_userdir_cmd = subparsers.add_parser(
            "create-userdir",
            help="Create user-data directory.",
        )
        create_userdir_cmd.set_defaults(func=start_create_userdir)
        self._build_args(optionlist=ARGS_CREATE_USERDIR, parser=create_userdir_cmd)

        # add new-config subcommand
        build_config_cmd = subparsers.add_parser(
            "new-config",
            help="Create new config",
        )
        build_config_cmd.set_defaults(func=start_new_config)
        self._build_args(optionlist=ARGS_BUILD_CONFIG, parser=build_config_cmd)

        # add show-config subcommand
        show_config_cmd = subparsers.add_parser(
            "show-config",
            help="Show resolved config",
        )
        show_config_cmd.set_defaults(func=start_show_config)
        self._build_args(optionlist=ARGS_SHOW_CONFIG, parser=show_config_cmd)

        # add new-strategy subcommand
        build_strategy_cmd = subparsers.add_parser(
            "new-strategy",
            help="Create new strategy",
        )
        build_strategy_cmd.set_defaults(func=start_new_strategy)
        self._build_args(optionlist=ARGS_BUILD_STRATEGY, parser=build_strategy_cmd)

        # Add download-data subcommand
        download_data_cmd = subparsers.add_parser(
            "download-data",
            help="Download backtesting data.",
            parents=[_common_parser],
        )
        download_data_cmd.set_defaults(func=start_download_data)
        self._build_args(optionlist=ARGS_DOWNLOAD_DATA, parser=download_data_cmd)

        # Add convert-data subcommand
        convert_data_cmd = subparsers.add_parser(
            "convert-data",
            help="Convert candle (OHLCV) data from one format to another.",
            parents=[_common_parser],
        )
        convert_data_cmd.set_defaults(func=partial(start_convert_data, ohlcv=True))
        self._build_args(optionlist=ARGS_CONVERT_DATA_OHLCV, parser=convert_data_cmd)

        # Add convert-trade-data subcommand
        convert_trade_data_cmd = subparsers.add_parser(
            "convert-trade-data",
            help="Convert trade data from one format to another.",
            parents=[_common_parser],
        )
        convert_trade_data_cmd.set_defaults(func=partial(start_convert_data, ohlcv=False))
        self._build_args(optionlist=ARGS_CONVERT_DATA_TRADES, parser=convert_trade_data_cmd)

        # Add trades-to-ohlcv subcommand
        convert_trade_data_cmd = subparsers.add_parser(
            "trades-to-ohlcv",
            help="Convert trade data to OHLCV data.",
            parents=[_common_parser],
        )
        convert_trade_data_cmd.set_defaults(func=start_convert_trades)
        self._build_args(optionlist=ARGS_CONVERT_TRADES, parser=convert_trade_data_cmd)

        # Add list-data subcommand
        list_data_cmd = subparsers.add_parser(
            "list-data",
            help="List downloaded data.",
            parents=[_common_parser],
        )
        list_data_cmd.set_defaults(func=start_list_data)
        self._build_args(optionlist=ARGS_LIST_DATA, parser=list_data_cmd)

        # Add backtesting subcommand
        backtesting_cmd = subparsers.add_parser(
            "backtesting", help="Backtesting module.", parents=[_common_parser, _strategy_parser]
        )
        backtesting_cmd.set_defaults(func=start_backtesting)
        self._build_args(optionlist=ARGS_BACKTEST, parser=backtesting_cmd)

        # Add backtesting-show subcommand
        backtesting_show_cmd = subparsers.add_parser(
            "backtesting-show",
            help="Show past Backtest results",
            parents=[_common_parser],
        )
        backtesting_show_cmd.set_defaults(func=start_backtesting_show)
        self._build_args(optionlist=ARGS_BACKTEST_SHOW, parser=backtesting_show_cmd)

        # Add backtesting analysis subcommand
        analysis_cmd = subparsers.add_parser(
            "backtesting-analysis", help="Backtest Analysis module.", parents=[_common_parser]
        )
        analysis_cmd.set_defaults(func=start_analysis_entries_exits)
        self._build_args(optionlist=ARGS_ANALYZE_ENTRIES_EXITS, parser=analysis_cmd)

        # Add edge subcommand
        edge_cmd = subparsers.add_parser(
            "edge", help="Edge module.", parents=[_common_parser, _strategy_parser]
        )
        edge_cmd.set_defaults(func=start_edge)
        self._build_args(optionlist=ARGS_EDGE, parser=edge_cmd)

        # Add hyperopt subcommand
        hyperopt_cmd = subparsers.add_parser(
            "hyperopt",
            help="Hyperopt module.",
            parents=[_common_parser, _strategy_parser],
        )
        hyperopt_cmd.set_defaults(func=start_hyperopt)
        self._build_args(optionlist=ARGS_HYPEROPT, parser=hyperopt_cmd)

        # Add hyperopt-list subcommand
        hyperopt_list_cmd = subparsers.add_parser(
            "hyperopt-list",
            help="List Hyperopt results",
            parents=[_common_parser],
        )
        hyperopt_list_cmd.set_defaults(func=start_hyperopt_list)
        self._build_args(optionlist=ARGS_HYPEROPT_LIST, parser=hyperopt_list_cmd)

        # Add hyperopt-show subcommand
        hyperopt_show_cmd = subparsers.add_parser(
            "hyperopt-show",
            help="Show details of Hyperopt results",
            parents=[_common_parser],
        )
        hyperopt_show_cmd.set_defaults(func=start_hyperopt_show)
        self._build_args(optionlist=ARGS_HYPEROPT_SHOW, parser=hyperopt_show_cmd)

        # Add list-exchanges subcommand
        list_exchanges_cmd = subparsers.add_parser(
            "list-exchanges",
            help="Print available exchanges.",
            parents=[_common_parser],
        )
        list_exchanges_cmd.set_defaults(func=start_list_exchanges)
        self._build_args(optionlist=ARGS_LIST_EXCHANGES, parser=list_exchanges_cmd)

        # Add list-markets subcommand
        list_markets_cmd = subparsers.add_parser(
            "list-markets",
            help="Print markets on exchange.",
            parents=[_common_parser],
        )
        list_markets_cmd.set_defaults(func=partial(start_list_markets, pairs_only=False))
        self._build_args(optionlist=ARGS_LIST_PAIRS, parser=list_markets_cmd)

        # Add list-pairs subcommand
        list_pairs_cmd = subparsers.add_parser(
            "list-pairs",
            help="Print pairs on exchange.",
            parents=[_common_parser],
        )
        list_pairs_cmd.set_defaults(func=partial(start_list_markets, pairs_only=True))
        self._build_args(optionlist=ARGS_LIST_PAIRS, parser=list_pairs_cmd)

        # Add list-strategies subcommand
        list_strategies_cmd = subparsers.add_parser(
            "list-strategies",
            help="Print available strategies.",
            parents=[_common_parser],
        )
        list_strategies_cmd.set_defaults(func=start_list_strategies)
        self._build_args(optionlist=ARGS_LIST_STRATEGIES, parser=list_strategies_cmd)

        # Add list-Hyperopt loss subcommand
        list_hyperopt_loss_cmd = subparsers.add_parser(
            "list-hyperoptloss",
            help="Print available hyperopt loss functions.",
            parents=[_common_parser],
        )
        list_hyperopt_loss_cmd.set_defaults(func=start_list_hyperopt_loss_functions)
        self._build_args(optionlist=ARGS_LIST_HYPEROPTS, parser=list_hyperopt_loss_cmd)

        # Add list-freqAI Models subcommand
        list_freqaimodels_cmd = subparsers.add_parser(
            "list-freqaimodels",
            help="Print available freqAI models.",
            parents=[_common_parser],
        )
        list_freqaimodels_cmd.set_defaults(func=start_list_freqAI_models)
        self._build_args(optionlist=ARGS_LIST_FREQAIMODELS, parser=list_freqaimodels_cmd)

        # Add list-timeframes subcommand
        list_timeframes_cmd = subparsers.add_parser(
            "list-timeframes",
            help="Print available timeframes for the exchange.",
            parents=[_common_parser],
        )
        list_timeframes_cmd.set_defaults(func=start_list_timeframes)
        self._build_args(optionlist=ARGS_LIST_TIMEFRAMES, parser=list_timeframes_cmd)

        # Add show-trades subcommand
        show_trades = subparsers.add_parser(
            "show-trades",
            help="Show trades.",
            parents=[_common_parser],
        )
        show_trades.set_defaults(func=start_show_trades)
        self._build_args(optionlist=ARGS_SHOW_TRADES, parser=show_trades)

        # Add test-pairlist subcommand
        test_pairlist_cmd = subparsers.add_parser(
            "test-pairlist",
            help="Test your pairlist configuration.",
        )
        test_pairlist_cmd.set_defaults(func=start_test_pairlist)
        self._build_args(optionlist=ARGS_TEST_PAIRLIST, parser=test_pairlist_cmd)

        # Add db-convert subcommand
        convert_db = subparsers.add_parser(
            "convert-db",
            help="Migrate database to different system",
        )
        convert_db.set_defaults(func=start_convert_db)
        self._build_args(optionlist=ARGS_CONVERT_DB, parser=convert_db)

        # Add install-ui subcommand
        install_ui_cmd = subparsers.add_parser(
            "install-ui",
            help="Install FreqUI",
        )
        install_ui_cmd.set_defaults(func=start_install_ui)
        self._build_args(optionlist=ARGS_INSTALL_UI, parser=install_ui_cmd)

        # Add Plotting subcommand
        plot_dataframe_cmd = subparsers.add_parser(
            "plot-dataframe",
            help="Plot candles with indicators.",
            parents=[_common_parser, _strategy_parser],
        )
        plot_dataframe_cmd.set_defaults(func=start_plot_dataframe)
        self._build_args(optionlist=ARGS_PLOT_DATAFRAME, parser=plot_dataframe_cmd)

        # Plot profit
        plot_profit_cmd = subparsers.add_parser(
            "plot-profit",
            help="Generate plot showing profits.",
            parents=[_common_parser, _strategy_parser],
        )
        plot_profit_cmd.set_defaults(func=start_plot_profit)
        self._build_args(optionlist=ARGS_PLOT_PROFIT, parser=plot_profit_cmd)

        # Add webserver subcommand
        webserver_cmd = subparsers.add_parser(
            "webserver", help="Webserver module.", parents=[_common_parser]
        )
        webserver_cmd.set_defaults(func=start_webserver)
        self._build_args(optionlist=ARGS_WEBSERVER, parser=webserver_cmd)

        # Add strategy_updater subcommand
        strategy_updater_cmd = subparsers.add_parser(
            "strategy-updater",
            help="updates outdated strategy files to the current version",
            parents=[_common_parser],
        )
        strategy_updater_cmd.set_defaults(func=start_strategy_update)
        self._build_args(optionlist=ARGS_STRATEGY_UPDATER, parser=strategy_updater_cmd)

        # Add lookahead_analysis subcommand
        lookahead_analayis_cmd = subparsers.add_parser(
            "lookahead-analysis",
            help="Check for potential look ahead bias.",
            parents=[_common_parser, _strategy_parser],
        )
        lookahead_analayis_cmd.set_defaults(func=start_lookahead_analysis)

        self._build_args(optionlist=ARGS_LOOKAHEAD_ANALYSIS, parser=lookahead_analayis_cmd)

        # Add recursive_analysis subcommand
        recursive_analayis_cmd = subparsers.add_parser(
            "recursive-analysis",
            help="Check for potential recursive formula issue.",
            parents=[_common_parser, _strategy_parser],
        )
        recursive_analayis_cmd.set_defaults(func=start_recursive_analysis)

        self._build_args(optionlist=ARGS_RECURSIVE_ANALYSIS, parser=recursive_analayis_cmd)
