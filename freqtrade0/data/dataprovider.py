"""
Dataprovider
Responsible to provide data to the bot
including ticker and orderbook data, live and historical candle (OHLCV) data
Common Interface for bot and strategy to access data.
"""

import asyncio
from copy import deepcopy
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

from pandas import DataFrame, Timedelta, Timestamp, to_timedelta

from freqtrade.configuration import TimeRange
from freqtrade.constants import (
    FULL_DATAFRAME_THRESHOLD,
    Config,
    ListPairsWithTimeframes,
    PairWithTimeframe,
)
from freqtrade.data.history import get_datahandler, load_pair_history
from freqtrade.enums import CandleType, RPCMessageType, RunMode, TradingMode
from freqtrade.exceptions import ExchangeError, OperationalException
from freqtrade.exchange import  timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.exchange.exchange_types import OrderBook
from freqtrade.misc import append_candles_to_dataframe
from freqtrade.rpc import RPCManager
from freqtrade.rpc.rpc_types import RPCAnalyzedDFMsg
from freqtrade.util import PeriodicCache
from freqtrade.data import dataprovider
from freqtrade0.exchange import Exchange
logger = logging.getLogger(__name__)

NO_EXCHANGE_EXCEPTION = "Exchange is not available to DataProvider."
MAX_DATAFRAME_CANDLES = 1000


class DataProvider(dataprovider.DataProvider):
    def __init__(
        self,
        config: Config,
        exchange: Exchange | None,
        pairlists=None,
        rpc: RPCManager | None = None,
    ) -> None:
        self._config = config
        self._exchange = exchange
        self._pairlists = pairlists
        self.__rpc = rpc
        self.__cached_pairs: dict[PairWithTimeframe, tuple[DataFrame, datetime]] = {}
        self.__slice_index: dict[str, int] = {}
        self.__slice_date: datetime | None = None

        self.__cached_pairs_backtesting: dict[PairWithTimeframe, DataFrame] = {}
        self.__producer_pairs_df: dict[
            str, dict[PairWithTimeframe, tuple[DataFrame, datetime]]
        ] = {}
        self.__producer_pairs: dict[str, list[str]] = {}
        self._msg_queue: deque = deque()

        self._default_candle_type = self._config.get("candle_type_def", CandleType.SPOT)
        self._default_timeframe = self._config.get("timeframe", "1h")

        self.__msg_cache = PeriodicCache(
            maxsize=1000, ttl=timeframe_to_seconds(self._default_timeframe)
        )

        self.producers = self._config.get("external_message_consumer", {}).get("producers", [])
        self.external_data_enabled = len(self.producers) > 0





   



 


  


 





    def merge_pairs_helperpairs(self,pairlist: ListPairsWithTimeframes,
        helping_pairs: ListPairsWithTimeframes | None = None):
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        final_pairs = (pairlist + helping_pairs) if helping_pairs else pairlist
        return final_pairs
    # Exchange functions

    def refresh(
        self,
        pairlist: ListPairsWithTimeframes,
        helping_pairs: ListPairsWithTimeframes | None = None,
    ) -> None:
        """
        Refresh data, called with each cycle
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        final_pairs = (pairlist + helping_pairs) if helping_pairs else pairlist
        # refresh latest ohlcv data
        self._exchange.refresh_latest_ohlcv(final_pairs)
        # refresh latest trades data
        self.refresh_latest_trades(pairlist)

    def refresh_latest_trades(self, pairlist: ListPairsWithTimeframes) -> None:
        """
        Refresh latest trades data (if enabled in config)
        """

        use_public_trades = self._config.get("exchange", {}).get("use_public_trades", False)
        if use_public_trades:
            if self._exchange:
                self._exchange.refresh_latest_trades(pairlist)
    async def refresh_latest_ohlcv(self, pairlist: ListPairsWithTimeframes) -> dict:
        """
        Refresh latest ohlcv data (if enabled in config)
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        ohlcv_refresh_time={}
       
        last_refresh_time=deepcopy(self._exchange._pairs_last_refresh_time)
        await asyncio.to_thread( self._exchange.refresh_latest_ohlcv,pairlist)
        for pair_timeframe,time in self._exchange._pairs_last_refresh_time.items():
            if pair_timeframe in last_refresh_time and last_refresh_time[pair_timeframe] < time:
                ohlcv_refresh_time[pair_timeframe]=time
        return ohlcv_refresh_time
    async def async_refresh_latest_trades(self, pairlist: ListPairsWithTimeframes) -> dict:
        """
        Refresh latest trades data (if enabled in config)
        """
        use_public_trades = self._config.get("exchange", {}).get("use_public_trades", False)
        if use_public_trades and self._exchange:
            return await asyncio.to_thread(self._exchange.refresh_latest_trades,pairlist)
        else:
            return {}
   

    def ohlcv(
        self, pair: str, timeframe: str | None = None, copy: bool = True, candle_type: str = ""
    ) -> DataFrame:
        """
        Get candle (OHLCV) data for the given pair as DataFrame
        Please use the `available_pairs` method to verify which pairs are currently cached.
        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        :param copy: copy dataframe before returning if True.
                     Use False only for read-only operations (where the dataframe is not modified)
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
            _candle_type = (
                CandleType.from_string(candle_type)
                if candle_type != ""
                else self._config["candle_type_def"]
            )
            return self._exchange.klines(
                (pair, timeframe or self._config["timeframe"], _candle_type), copy=copy
            )
        else:
            return DataFrame()

    def trades(
        self, pair: str, timeframe: str | None = None, copy: bool = True, candle_type: str = ""
    ) -> DataFrame:
        """
        Get candle (TRADES) data for the given pair as DataFrame
        Please use the `available_pairs` method to verify which pairs are currently cached.
        This is not meant to be used in callbacks because of lookahead bias.
        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        :param copy: copy dataframe before returning if True.
                     Use False only for read-only operations (where the dataframe is not modified)
        """
        if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
            if self._exchange is None:
                raise OperationalException(NO_EXCHANGE_EXCEPTION)
            _candle_type = (
                CandleType.from_string(candle_type)
                if candle_type != ""
                else self._config["candle_type_def"]
            )
            return self._exchange.trades(
                (pair, timeframe or self._config["timeframe"], _candle_type), copy=copy
            )
        else:
            data_handler = get_datahandler(
                self._config["datadir"], data_format=self._config["dataformat_trades"]
            )
            trades_df = data_handler.trades_load(
                pair, self._config.get("trading_mode", TradingMode.SPOT)
            )
            return trades_df

    def market(self, pair: str) -> dict[str, Any] | None:
        """
        Return market data for the pair
        :param pair: Pair to get the data for
        :return: Market data dict from ccxt or None if market info is not available for the pair
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        return self._exchange.markets.get(pair)

    def ticker(self, pair: str):
        """
        Return last ticker data from exchange
        :param pair: Pair to get the data for
        :return: Ticker dict from exchange or empty dict if ticker is not available for the pair
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        try:
            return self._exchange.fetch_ticker(pair)
        except ExchangeError:
            return {}

    def orderbook(self, pair: str, maximum: int) -> OrderBook:
        """
        Fetch latest l2 orderbook data
        Warning: Does a network request - so use with common sense.
        :param pair: pair to get the data for
        :param maximum: Maximum number of orderbook entries to query
        :return: dict including bids/asks with a total of `maximum` entries.
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        return self._exchange.fetch_l2_order_book(pair, maximum)

    def send_msg(self, message: str, *, always_send: bool = False) -> None:
        """
        Send custom RPC Notifications from your bot.
        Will not send any bot in modes other than Dry-run or Live.
        :param message: Message to be sent. Must be below 4096.
        :param always_send: If False, will send the message only once per candle, and suppress
                            identical messages.
                            Careful as this can end up spaming your chat.
                            Defaults to False
        """
        if self.runmode not in (RunMode.DRY_RUN, RunMode.LIVE):
            return

        if always_send or message not in self.__msg_cache:
            self._msg_queue.append(message)
        self.__msg_cache[message] = True
