"""
Freqtrade is the main module of this bot. It contains the class Freqtrade()
"""

import logging
import traceback
from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
from math import isclose
from threading import Lock
from time import sleep
from typing import Any, cast

from pandas import DataFrame

from freqtrade import constants
from freqtrade.configuration import validate_config_consistency
from freqtrade.constants import (BuySell, Config, EntryExecuteMode,
                                 ExchangeConfig, LongShort)
from freqtrade.data.converter import order_book_to_dataframe
from freqtrade.edge import Edge
from freqtrade.enums import (ExitCheckTuple, ExitType, MarginMode,
                             RPCMessageType, SignalDirection, State,
                             TradingMode)
from freqtrade.enums.runmode import RunMode
from freqtrade.exceptions import (DependencyException, ExchangeError,
                                  InsufficientFundsError,
                                  InvalidOrderException, PricingError)
from freqtrade0.exchange import (ROUND_DOWN, ROUND_UP,
                                remove_exchange_credentials,
                                timeframe_to_minutes, timeframe_to_next_date,
                                timeframe_to_seconds)
from freqtrade0.exchange.exchange_types import CcxtOrder, OrderBook
from freqtrade.leverage.liquidation_price import update_liquidation_prices
from freqtrade.misc import safe_value_fallback, safe_value_fallback2
from freqtrade.mixins import LoggingMixin
from freqtrade.persistence import Order, PairLocks, Trade, init_db
from freqtrade.persistence.key_value_store import set_startup_time
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.plugins.protectionmanager import ProtectionManager
from freqtrade.rpc import RPCManager
from freqtrade.rpc.external_message_consumer import ExternalMessageConsumer
from freqtrade.rpc.rpc_types import (ProfitLossStr, RPCCancelMsg, RPCEntryMsg,
                                     RPCExitCancelMsg, RPCExitMsg,
                                     RPCProtectionMsg)
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import (FtPrecise, MeasureTime, PeriodicCache, dt_from_ts,
                            dt_now)
from freqtrade.util.migrations.binance_mig import migrate_binance_futures_names
from freqtrade.wallets import Wallets
from schedule import Scheduler

from freqtrade0.data.dataprovider import DataProvider

from freqtrade0.resolvers import StrategyResolver,ExchangeResolver
from freqtrade0.strategy import IStrategy

logger = logging.getLogger(__name__)

import freqtrade.freqtradebot


class FreqtradeBot(freqtrade.freqtradebot.FreqtradeBot):
    """
    Freqtrade is the main class of the bot.
    This is from here the bot start its logic.
    """
    def __init__(self, config: Config,strategy_type:type|None=None) -> None:
        """
        Init all variables and objects the bot needs to work
        :param config: configuration dict, you can use Configuration.get_config()
        to get the config dict.
        """
        self.active_pair_whitelist: list[str] = []

        # Init bot state
        self.state = State.STOPPED

        # Init objects
        self.config = config
        exchange_config: ExchangeConfig = deepcopy(config["exchange"])
        # Remove credentials from original exchange config to avoid accidental credential exposure
        remove_exchange_credentials(config["exchange"], True)
        if strategy_type:
            self.strategy :IStrategy= StrategyResolver.create_strategy(strategy_type=strategy_type,config=self.config)
        else:
            self.strategy :IStrategy=cast(IStrategy,  StrategyResolver.load_strategy(self.config))

        # Check config consistency here since strategies can set certain options
        validate_config_consistency(config)

        self.exchange = ExchangeResolver.load_exchange(
            self.config, exchange_config=exchange_config, load_leverage_tiers=True
        )

        init_db(self.config["db_url"])

        self.wallets = Wallets(self.config, self.exchange)

        PairLocks.timeframe = self.config["timeframe"]

        self.trading_mode: TradingMode = self.config.get("trading_mode", TradingMode.SPOT)
        self.margin_mode: MarginMode = self.config.get("margin_mode", MarginMode.NONE)
        self.last_process: datetime | None = None

        # RPC runs in separate threads, can start handling external commands just after
        # initialization, even before Freqtradebot has a chance to start its throttling,
        # so anything in the Freqtradebot instance should be ready (initialized), including
        # the initial state of the bot.
        # Keep this at the end of this initialization method.
        self.rpc: RPCManager = RPCManager(self)

        self.dataprovider = DataProvider(self.config, self.exchange, rpc=self.rpc)
        self.pairlists = PairListManager(self.exchange, self.config, self.dataprovider)

        self.dataprovider.add_pairlisthandler(self.pairlists)

        # Attach Dataprovider to strategy instance
        self.strategy.dp = self.dataprovider
        # Attach Wallets to strategy instance
        self.strategy.wallets = self.wallets

        # Initializing Edge only if enabled
        self.edge = (
            Edge(self.config, self.exchange, self.strategy)
            if self.config.get("edge", {}).get("enabled", False)
            else None
        )

        # Init ExternalMessageConsumer if enabled
        self.emc = (
            ExternalMessageConsumer(self.config, self.dataprovider)
            if self.config.get("external_message_consumer", {}).get("enabled", False)
            else None
        )

        logger.info("Starting initial pairlist refresh")
        with MeasureTime(
            lambda duration, _: logger.info(f"Initial Pairlist refresh took {duration:.2f}s"), 0
        ):
            self.active_pair_whitelist = self._refresh_active_whitelist()

        # Set initial bot state from config
        initial_state:str =cast(str, self.config.get("initial_state"))
        self.state = State[initial_state.upper()] if initial_state else State.STOPPED

        # Protect exit-logic from forcesell and vice versa
        self._exit_lock = Lock()
        timeframe_secs = timeframe_to_seconds(self.strategy.timeframe)
        self._exit_reason_cache = PeriodicCache(100, ttl=timeframe_secs)
        LoggingMixin.__init__(self, logger, timeframe_secs)

        self._schedule = Scheduler()

        if self.trading_mode == TradingMode.FUTURES:

            def update():
                self.update_funding_fees()
                self.update_all_liquidation_prices()
                self.wallets.update()

            # This would be more efficient if scheduled in utc time, and performed at each
            # funding interval, specified by funding_fee_times on the exchange classes
            # However, this reduces the precision - and might therefore lead to problems.
            for time_slot in range(0, 24):
                for minutes in [1, 31]:
                    t = str(time(time_slot, minutes, 2))
                    self._schedule.every().day.at(t).do(update)

        self._schedule.every().day.at("00:02").do(self.exchange.ws_connection_reset)

        self.strategy.ft_bot_start()
        # Initialize protections AFTER bot start - otherwise parameters are not loaded.
        self.protections = ProtectionManager(self.config, self.strategy.protections)

        def log_took_too_long(duration: float, time_limit: float):
            logger.warning(
                f"Strategy analysis took {duration:.2f}s, more than 25% of the timeframe "
                f"({time_limit:.2f}s). This can lead to delayed orders and missed signals."
                "Consider either reducing the amount of work your strategy performs "
                "or reduce the amount of pairs in the Pairlist."
            )

        self._measure_execution = MeasureTime(log_took_too_long, timeframe_secs * 0.25)
        
        
        
    def process(self) -> None:
        """
        Queries the persistence layer for open trades and handles them,
        otherwise a new trade is created.
        :return: True if one or more trades has been created or closed, False otherwise
        """

        # Check whether markets have to be reloaded and reload them when it's needed
        self.exchange.reload_markets()

        self.update_trades_without_assigned_fees()

        # Query trades from persistence layer
        trades: list[Trade] = Trade.get_open_trades()

        self.active_pair_whitelist = self._refresh_active_whitelist(trades)
        pairlist = self.pairlists.create_pair_list(self.active_pair_whitelist)
        ohlcv_pair_list =self.dataprovider.merge_pairs_helperpairs(pairlist,
            self.strategy.gather_informative_pairs())
        self.dataprovider.refresh_latest_ohlcv(ohlcv_pair_list)
        trade_pair_list = self.dataprovider.merge_pairs_helperpairs(pairlist,self.strategy.gather_informative_trade_pairs())
        self.dataprovider.refresh_latest_trades(trade_pair_list)
        strategy_safe_wrapper(self.strategy.bot_loop_start, supress_error=True)(
            current_time=datetime.now(timezone.utc)
        )

        with self._measure_execution:
                self.strategy.analyze(self.active_pair_whitelist)

        with self._exit_lock:
            # Check for exchange cancellations, timeouts and user requested replace
            self.manage_open_orders()

        # Protect from collisions with force_exit.
        # Without this, freqtrade may try to recreate stoploss_on_exchange orders
        # while exiting is in process, since telegram messages arrive in an different thread.
        with self._exit_lock:
            trades = Trade.get_open_trades()
            # First process current opened trades (positions)
            self.exit_positions(trades)
            Trade.commit()

        # Check if we need to adjust our current positions before attempting to enter new trades.
        if self.strategy.position_adjustment_enable:
            with self._exit_lock:
                    self.process_open_trade_positions()

        # Then looking for entry opportunities
        if self.state == State.RUNNING and self.get_free_open_trades():
            self.enter_positions()
               
        self._schedule.run_pending()
        Trade.commit()
        self.rpc.process_msg_queue(self.dataprovider._msg_queue)
        self.last_process = datetime.now(timezone.utc)

    # async def process_trades(self):
    #     if not self.strategy.loop_enable :
    #          return 
    #     trades: list[Trade] = Trade.get_open_trades()

    #     self.active_pair_whitelist = self._refresh_active_whitelist(trades)

    #     # Refreshing candles
    #     pair_list =self.dataprovider.merge_pairs_helperpairs(self.pairlists.create_pair_list(self.active_pair_whitelist),
    #         self.strategy.gather_informative_pairs())
    #     self.dataprovider.refresh_latest_trades(
    #         pair_list
    #     )
     
    #     with self._exit_lock:
    #         # Check for exchange cancellations, timeouts and user requested replace
    #         self.manage_open_orders()

    #     # Protect from collisions with force_exit.
    #     # Without this, freqtrade may try to recreate stoploss_on_exchange orders
    #     # while exiting is in process, since telegram messages arrive in an different thread.
    #     with self._exit_lock:
    #         trades = Trade.get_open_trades()
    #         # First process current opened trades (positions)
    #         self.exit_positions(trades)
    #         Trade.commit()

    #     # Check if we need to adjust our current positions before attempting to enter new trades.
    #     if self.strategy.position_adjustment_enable:
    #         with self._exit_lock:
    #                 self.process_open_trade_positions()

    #     # Then looking for entry opportunities
    #     if self.state == State.RUNNING and self.get_free_open_trades():
    #         whitelist = self._get_nolock_whitelist()
    #         if not whitelist:
    #             self.log_once("Active pair whitelist is empty.", logger.info)
    #         else:
    #             trades_created = 0
    #     # Create entity and execute trade for each pair from whitelist
    #             for pair in whitelist:
    #                 try:
    #                     with self._exit_lock:
    #                         if self.strategy.loop_enable :
    #                             trades_created += self._create_trade_loop(pair)
                           
    #                 except DependencyException as exception:
    #                     logger.warning("Unable to create trade for %s: %s", pair, exception)

    #                 if not trades_created:
    #                     logger.debug("Found no enter signals for whitelisted currencies. Trying again...")
               
            
    #     self._schedule.run_pending()
    #     Trade.commit()
    #     self.rpc.process_msg_queue(self.dataprovider._msg_queue)
    #     self.last_process = datetime.now(timezone.utc)

    def _get_bidirectional_pairs(self):

        trade_pairs:dict[str,str] = {}
        
        for trade in Trade.get_open_trades():
            trade = cast(Trade, trade)
            pair = trade.pair
            direcation: str = trade_pairs.get(pair, "")
            if trade.trade_direction not in direcation:
                trade_pairs[pair] = direcation + trade.trade_direction
        return trade_pairs

    # def _check_pair_direction_match(self, pair: str, signal: str | None, can_hedge_mode: bool):
    #     """Check if trading pair direction matches the given signal under current hedging mode.

    #     Args:
    #         pair: Trading pair identifier (e.g. 'BTC/USD')
    #         signal: Trading direction signal from strategy, None indicates no signal
    #         can_hedge_mode: Flag indicating if hedge trading mode is enabled

    #     Returns:
    #         bool: True if direction matches requirements, False otherwise

    #     Note:
    #         Decision logic depends on bidirectional pairs configuration and hedging mode status
    #     """
    #     pairs_directions = self._get_bidirectional_pairs()
    #     direction = pairs_directions.get(pair, "")
    #     if pair not in pairs_directions:
    #         # open
    #         return False
    #     elif (not can_hedge_mode) or (signal is None) or (len(signal) == len(direction)) or (signal in direction):
    #         # not open
    #         return True
    #     else:
    #         return False 
    def _get_nolock_whitelist(self,can_hedge_mode: bool=False) -> dict[str,str]|None:
        """
        获取非锁定状态下的白名单，若存在全局锁定则返回空列表或 None。
        """
        # 创建白名单的深拷贝
        whitelist = deepcopy(self.active_pair_whitelist)
        
        # 如果白名单为空，记录日志并返回
        if not whitelist:
            self.log_once("Active pair whitelist is empty.", logger.info)
            return None
        
        # 检查全局锁定状态
        if PairLocks.is_global_lock(side="*"):
            # 全局锁定存在时，记录日志并返回空列表
            lock = PairLocks.get_pair_longest_lock("*")
            if lock:
                self.log_once(
                    f"Global pairlock active until "
                    f"{lock.lock_end_time.strftime(constants.DATETIME_PRINT_FORMAT)}. "
                    f"Not creating new trades, reason: {lock.reason}.",
                    logger.info,
                )
            else:
                self.log_once("Global pairlock active. Not creating new trades.", logger.info)
            
            # 显式返回空列表，表示因锁定无法创建新交易
            return []
    
        # 所有检查通过，返回白名单
        tradespairs = self._get_bidirectional_pairs()
        for pair in whitelist:
            if pair in tradespairs:
                match len( tradespairs[pair]) :
                    case 4 | 5  if not can_hedge_mode:
                        del tradespairs[pair]
                    case 9:
                        del tradespairs[pair]
                    case _:
                        del tradespairs[pair]
            else:
                tradespairs[pair]=""
        return tradespairs
   
    #
    # enter positions / open trades logic and methods
    #
    def _create_trade_loop(self, pair:str,df:DataFrame,direction=""):
        # current_time = self.dataprovider.orderbook(pair=pair,maximum=1)["timestamp"]
        result = self.strategy._loop_entry(pair=pair,timestamp=dt_now(),df=df)
        if result is None:
            return False
        signal,stake_amount,price,entry_tag= result
        # not_opening = not self._check_pair_direction_match(
        #     pair=pair, signal=signal, can_hedge_mode=self.strategy.can_hedge_mode
        # )
        if not signal or signal == direction or signal in direction:
            self.logger.info(f" trade_loop,not opening {pair} because of direction mismatch")
            return False
        stake_amount = stake_amount if stake_amount else self.wallets.get_trade_stake_amount(
                pair, self.config["max_open_trades"], self.edge
            )
        return self.execute_entry(pair=pair, stake_amount=stake_amount, price=price,is_short=(signal==SignalDirection.SHORT),enter_tag=entry_tag)
    def enter_positions(self) -> int:
        whitelist = self._get_nolock_whitelist(can_hedge_mode=self.strategy.can_hedge_mode)
       
       
        if not whitelist:
            self.log_once("Active pair whitelist is empty.", logger.info)
        else:
            trades_created = 0
            for pair,direction in whitelist.items():
                try:
                    analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair=pair, timeframe=self.strategy.timeframe)
                    with self._exit_lock:
                            if self.strategy.loop_enable :
                                trades_created += self._create_trade_loop(pair,analyzed_df,direction)
                            else:
                                trades_created += self.create_trade(pair,analyzed_df,direction)
                except DependencyException as exception:
                    logger.warning("Unable to create trade for %s: %s", pair, exception)
                if not trades_created:
                    logger.debug("Found no enter signals for whitelisted currencies. Trying again...")
        return trades_created

    def create_trade(self, pair: str,df:DataFrame|None =None,direction="") -> bool:
        """
        Check the implemented trading strategy for entry signals.

        If the pair triggers the enter signal a new trade record gets created
        and the entry-order opening the trade gets issued towards the exchange.

        :return: True if a trade has been created.
        """
       
        if df:
            analyzed_df = df
        else:
            analyzed_df, _ = self.dataprovider.get_analyzed_dataframe(pair, self.strategy.timeframe)
        nowtime = analyzed_df.iloc[-1]["date"] if len(analyzed_df) > 0 else None

        

        # running get_signal on historical data fetched
        #价格或交易都可以直接返回none
        signal, enter_tag = self.strategy.get_entry_signal(
            pair, self.strategy.timeframe, analyzed_df
        )
       
        if not signal or signal == direction or signal in direction:
            self.logger.info(f" trade_loop,not opening {pair} because of direction mismatch")
            return False
        if self.strategy.is_pair_locked(pair, candle_date=nowtime, side=signal):
            lock = PairLocks.get_pair_longest_lock(pair, nowtime, signal)
            if lock:
                self.log_once(
                    f"Pair {pair} {lock.side} is locked until "
                    f"{lock.lock_end_time.strftime(constants.DATETIME_PRINT_FORMAT)} "
                    f"due to {lock.reason}.",
                    logger.info,
                )
            else:
                self.log_once(f"Pair {pair} is currently locked.", logger.info)
            return False
        stake_amount = self.wallets.get_trade_stake_amount(
            pair, self.config["max_open_trades"], self.edge
        )
        bid_check_dom = self.config.get("entry_pricing", {}).get("check_depth_of_market", {})
        if (bid_check_dom.get("enabled", False)) and (
            bid_check_dom.get("bids_to_ask_delta", 0) > 0
        ):
            if self._check_depth_of_market(pair, bid_check_dom, side=signal):
                return self.execute_entry(
                    pair,
                    stake_amount,
                    enter_tag=enter_tag,
                    is_short=(signal == SignalDirection.SHORT),
                )
            else:
                return False
        
        
        return self.execute_entry(
            pair=pair, stake_amount=stake_amount ,enter_tag=enter_tag, is_short=(signal == SignalDirection.SHORT)
        )
      

