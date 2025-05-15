"""
Dataprovider
Responsible to provide data to the bot
including ticker and orderbook data, live and historical candle (OHLCV) data
Common Interface for bot and strategy to access data.
"""

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
from freqtrade.exchange import Exchange, timeframe_to_prev_date, timeframe_to_seconds
from freqtrade.exchange.exchange_types import OrderBook
from freqtrade.misc import append_candles_to_dataframe
from freqtrade.rpc import RPCManager
from freqtrade.rpc.rpc_types import RPCAnalyzedDFMsg
from freqtrade.util import PeriodicCache
from freqtrade.data import dataprovider

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





   



 
    def get_producer_df(
        self,
        pair: str,
        timeframe: str | None = None,
        candle_type: CandleType | None = None,
        producer_name: str = "default",
    ) -> tuple[DataFrame, datetime]:
        """
        Get the pair data from producers.

        :param pair: pair to get the data for
        :param timeframe: Timeframe to get data for
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        :returns: Tuple of the DataFrame and last analyzed timestamp
        """
        _timeframe = self._default_timeframe if not timeframe else timeframe
        _candle_type = self._default_candle_type if not candle_type else candle_type

        pair_key = (pair, _timeframe, _candle_type)

        # If we have no data from this Producer yet
        if producer_name not in self.__producer_pairs_df:
            # We don't have this data yet, return empty DataFrame and datetime (01-01-1970)
            return (DataFrame(), datetime.fromtimestamp(0, tz=timezone.utc))

        # If we do have data from that Producer, but no data on this pair_key
        if pair_key not in self.__producer_pairs_df[producer_name]:
            # We don't have this data yet, return empty DataFrame and datetime (01-01-1970)
            return (DataFrame(), datetime.fromtimestamp(0, tz=timezone.utc))

        # We have it, return this data
        df, la = self.__producer_pairs_df[producer_name][pair_key]
        return (df.copy(), la)


    def historic_ohlcv(self, pair: str, timeframe: str, candle_type: str = "") -> DataFrame:
        """
        Get stored historical candle (OHLCV) data
        :param pair: pair to get the data for
        :param timeframe: timeframe to get data for
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        """
        _candle_type = (
            CandleType.from_string(candle_type)
            if candle_type != ""
            else self._config["candle_type_def"]
        )
        saved_pair: PairWithTimeframe = (pair, str(timeframe), _candle_type)
        if saved_pair not in self.__cached_pairs_backtesting:
            timerange = TimeRange.parse_timerange(
                None
                if self._config.get("timerange") is None
                else str(self._config.get("timerange"))
            )

            startup_candles = self.get_required_startup(str(timeframe))
            tf_seconds = timeframe_to_seconds(str(timeframe))
            timerange.subtract_start(tf_seconds * startup_candles)

            logger.info(
                f"Loading data for {pair} {timeframe} "
                f"from {timerange.start_fmt} to {timerange.stop_fmt}"
            )

            self.__cached_pairs_backtesting[saved_pair] = load_pair_history(
                pair=pair,
                timeframe=timeframe,
                datadir=self._config["datadir"],
                timerange=timerange,
                data_format=self._config["dataformat_ohlcv"],
                candle_type=_candle_type,
            )
        return self.__cached_pairs_backtesting[saved_pair].copy()

    def get_required_startup(self, timeframe: str) -> int:
        freqai_config = self._config.get("freqai", {})
        if not freqai_config.get("enabled", False):
            return self._config.get("startup_candle_count", 0)
        else:
            startup_candles = self._config.get("startup_candle_count", 0)
            indicator_periods = freqai_config["feature_parameters"]["indicator_periods_candles"]
            # make sure the startupcandles is at least the set maximum indicator periods
            self._config["startup_candle_count"] = max(startup_candles, max(indicator_periods))
            tf_seconds = timeframe_to_seconds(timeframe)
            train_candles = freqai_config["train_period_days"] * 86400 / tf_seconds
            total_candles = int(self._config["startup_candle_count"] + train_candles)
            logger.info(
                f"Increasing startup_candle_count for freqai on {timeframe} to {total_candles}"
            )
        return total_candles

 

    def get_analyzed_dataframe(self, pair: str, timeframe: str) -> tuple[DataFrame, datetime]:
        """
        Retrieve the analyzed dataframe. Returns the full dataframe in trade mode (live / dry),
        and the last 1000 candles (up to the time evaluated at this moment) in all other modes.
        :param pair: pair to get the data for
        :param timeframe: timeframe to get data for
        :return: Tuple of (Analyzed Dataframe, lastrefreshed) for the requested pair / timeframe
            combination.
            Returns empty dataframe and Epoch 0 (1970-01-01) if no dataframe was cached.
        """
        pair_key = (pair, timeframe, self._config.get("candle_type_def", CandleType.SPOT))
        if pair_key in self.__cached_pairs:
            if self.runmode in (RunMode.DRY_RUN, RunMode.LIVE):
                df, date = self.__cached_pairs[pair_key]
            else:
                df, date = self.__cached_pairs[pair_key]
                if (max_index := self.__slice_index.get(pair)) is not None:
                    df = df.iloc[max(0, max_index - MAX_DATAFRAME_CANDLES) : max_index]
                else:
                    return (DataFrame(), datetime.fromtimestamp(0, tz=timezone.utc))
            return df, date
        else:
            return (DataFrame(), datetime.fromtimestamp(0, tz=timezone.utc))




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
    ) -> tuple[dict,dict]:
        """
        Refresh data, called with each cycle
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        final_pairs = (pairlist + helping_pairs) if helping_pairs else pairlist
        # refresh latest ohlcv data
        ohlcv_refresh_time={}
       
        last_refresh_time=deepcopy(self._exchange._pairs_last_refresh_time)
        self._exchange.refresh_latest_ohlcv(final_pairs)
        for pair_timeframe,time in self._exchange._pairs_last_refresh_time.items():
            if pair_timeframe in last_refresh_time and last_refresh_time[pair_timeframe] < time:
                ohlcv_refresh_time[pair_timeframe]=time
        # refresh latest trades data
        trades_refresh_time= self.refresh_latest_trades(pairlist)
        return ohlcv_refresh_time,trades_refresh_time
    def refresh_latest_ohlcv(self, pairlist: ListPairsWithTimeframes) -> dict:
        """
        Refresh latest ohlcv data (if enabled in config)
        """
        if self._exchange is None:
            raise OperationalException(NO_EXCHANGE_EXCEPTION)
        ohlcv_refresh_time={}
       
        last_refresh_time=deepcopy(self._exchange._pairs_last_refresh_time)
        self._exchange.refresh_latest_ohlcv(pairlist)
        for pair_timeframe,time in self._exchange._pairs_last_refresh_time.items():
            if pair_timeframe in last_refresh_time and last_refresh_time[pair_timeframe] < time:
                ohlcv_refresh_time[pair_timeframe]=time
        return ohlcv_refresh_time
    def refresh_latest_trades(self, pairlist: ListPairsWithTimeframes) -> dict:
        """
        Refresh latest trades data (if enabled in config)
        """

        use_public_trades = self._config.get("exchange", {}).get("use_public_trades", False)
        if use_public_trades:
            if self._exchange:
                return self._exchange.refresh_latest_trades(pairlist)
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
