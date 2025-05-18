# pragma pylint: disable=W0603
"""
Cryptocurrency Exchanges support
"""

import asyncio
import inspect
import logging
import signal
from collections.abc import Coroutine, Generator
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from math import floor, isnan
from threading import Lock
from typing import Any, Literal, TypeGuard, TypeVar

import ccxt
import ccxt.pro as ccxt_pro
from cachetools import TTLCache
from ccxt import TICK_SIZE
from dateutil import parser
from freqtrade.constants import (DEFAULT_AMOUNT_RESERVE_PERCENT,
                                 DEFAULT_TRADES_COLUMNS,
                                 NON_OPEN_EXCHANGE_STATES, BidAsk, BuySell,
                                 Config, EntryExit, ExchangeConfig,
                                 ListPairsWithTimeframes, MakerTaker,
                                 OBLiteral, PairWithTimeframe)
from freqtrade.data.converter import (clean_ohlcv_dataframe,
                                      ohlcv_to_dataframe,
                                      trades_df_remove_duplicates,
                                      trades_dict_to_list, trades_list_to_df)
from freqtrade.enums import (OPTIMIZE_MODES, TRADE_MODES, CandleType,
                             MarginMode, PriceType, RunMode, TradingMode)
from freqtrade.exceptions import (ConfigurationError, DDosProtection,
                                  ExchangeError, InsufficientFundsError,
                                  InvalidOrderException, OperationalException,
                                  PricingError, RetryableOrderError,
                                  TemporaryError)
from freqtrade.exchange.common import (API_FETCH_ORDER_RETRY_COUNT,
                                       remove_exchange_credentials, retrier,
                                       retrier_async)
from freqtrade.exchange.exchange_types import (CcxtBalances, CcxtOrder,
                                               CcxtPosition, FtHas,
                                               OHLCVResponse, OrderBook,
                                               Ticker, Tickers)
from freqtrade.exchange.exchange_utils import (ROUND, ROUND_DOWN, ROUND_UP,
                                               amount_to_contract_precision,
                                               amount_to_contracts,
                                               amount_to_precision,
                                               contracts_to_amount,
                                               date_minus_candles,
                                               is_exchange_known_ccxt,
                                               market_is_active,
                                               price_to_precision)
from freqtrade.exchange.exchange_utils_timeframe import (
    timeframe_to_minutes, timeframe_to_msecs, timeframe_to_next_date,
    timeframe_to_prev_date, timeframe_to_seconds)

from freqtrade.misc import (chunks, deep_merge_dicts, file_dump_json,
                            file_load_json, safe_value_fallback2)
from freqtrade.util import dt_from_ts, dt_now
from freqtrade.util.datetime_helpers import (dt_humanize_delta, dt_ts,
                                             format_ms_time)
from freqtrade.util.periodic_cache import PeriodicCache
from pandas import DataFrame, concat
from freqtrade0.exchange.exchange_ws import ExchangeWS
from freqtrade import exchange
logger = logging.getLogger(__name__)

T = TypeVar("T")


class Exchange(exchange.Exchange):
    # Parameters to add directly to buy/sell calls (like agreeing to trading agreement)
    _params: dict = {}

    # Additional parameters - added to the ccxt object
    _ccxt_params: dict = {}

    # Dict to specify which options each exchange implements
    # This defines defaults, which can be selectively overridden by subclasses using _ft_has
    # or by specifying them in the configuration.
    _ft_has_default: FtHas = {
        "stoploss_on_exchange": False,
        "stop_price_param": "stopLossPrice",  # Used for stoploss_on_exchange request
        "stop_price_prop": "stopLossPrice",  # Used for stoploss_on_exchange response parsing
        "stoploss_order_types": {},
        "order_time_in_force": ["GTC"],
        "ohlcv_params": {},
        "ohlcv_has_history": True,  # Some exchanges (Kraken) don't provide history via ohlcv
        "ohlcv_partial_candle": True,
        "ohlcv_require_since": False,
        # Check https://github.com/ccxt/ccxt/issues/10767 for removal of ohlcv_volume_currency
        "ohlcv_volume_currency": "base",  # "base" or "quote"
        "tickers_have_quoteVolume": True,
        "tickers_have_percentage": True,
        "tickers_have_bid_ask": True,  # bid / ask empty for fetch_tickers
        "tickers_have_price": True,
        "trades_limit": 1000,  # Limit for 1 call to fetch_trades
        "trades_pagination": "time",  # Possible are "time" or "id"
        "trades_pagination_arg": "since",
        "trades_has_history": False,
        "l2_limit_range": None,
        "l2_limit_range_required": True,  # Allow Empty L2 limit (kucoin)
        "l2_limit_upper": None,  # Upper limit for L2 limit
        "mark_ohlcv_price": "mark",
        "mark_ohlcv_timeframe": "8h",
        "funding_fee_timeframe": "8h",
        "ccxt_futures_name": "swap",
        "needs_trading_fees": False,  # use fetch_trading_fees to cache fees
        "order_props_in_contracts": ["amount", "filled", "remaining"],
        # Override createMarketBuyOrderRequiresPrice where ccxt has it wrong
        "marketOrderRequiresPrice": False,
        "exchange_has_overrides": {},  # Dictionary overriding ccxt's "has".
        "proxy_coin_mapping": {},  # Mapping for proxy coins
        # Expected to be in the format {"fetchOHLCV": True} or {"fetchOHLCV": False}
        "ws_enabled": False,  # Set to true for exchanges with tested websocket support
    }
    _ft_has: FtHas = {}
    _ft_has_futures: FtHas = {}

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        # TradingMode.SPOT always supported and not required in this list
    ]

    def __init__(
        self,
        config: Config,
        *,
        exchange_config: ExchangeConfig | None = None,
        validate: bool = True,
        load_leverage_tiers: bool = False,
    ) -> None:
        """
        Initializes this module with the given config,
        it does basic validation whether the specified exchange and pairs are valid.
        :return: None
        """
        # self._trades_lock = Lock()
        self._api: ccxt.Exchange
        self._api_async: ccxt_pro.Exchange
        self._ws_async: ccxt_pro.Exchange = None
        self._exchange_ws: ExchangeWS | None = None
        self._markets: dict = {}
        self._trading_fees: dict[str, Any] = {}
        self._leverage_tiers: dict[str, list[dict]] = {}
        # Lock event loop. This is necessary to avoid race-conditions when using force* commands
        # Due to funding fee fetching.
        self._loop_lock = Lock()
        self.loop = self._init_async_loop()
        self._config: Config = {}

        self._config.update(config)

        # Holds last candle refreshed time of each pair
        self._pairs_last_refresh_time: dict[PairWithTimeframe, int] = {}
        # Timestamp of last markets refresh
        self._last_markets_refresh: int = 0

        self._cache_lock = Lock()
        # Cache for 10 minutes ...
        self._fetch_tickers_cache: TTLCache = TTLCache(maxsize=4, ttl=60 * 10)
        # Cache values for 300 to avoid frequent polling of the exchange for prices
        # Caching only applies to RPC methods, so prices for open trades are still
        # refreshed once every iteration.
        # Shouldn't be too high either, as it'll freeze UI updates in case of open orders.
        self._exit_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=300)
        self._entry_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=300)

        # Holds candles
        self._klines: dict[PairWithTimeframe, DataFrame] = {}
        self._expiring_candle_cache: dict[tuple[str, int], PeriodicCache] = {}

        # Holds public_trades
        self._trades: dict[PairWithTimeframe, DataFrame] = {}

        # Holds all open sell orders for dry_run
        self._dry_run_open_orders: dict[str, Any] = {}

        if config["dry_run"]:
            logger.info("Instance is running with dry_run enabled")
        logger.info(f"Using CCXT {ccxt.__version__}")
        exchange_conf: dict[str, Any] = exchange_config if exchange_config else config["exchange"]
        remove_exchange_credentials(exchange_conf, config.get("dry_run", False))
        self.log_responses = exchange_conf.get("log_responses", False)

        # Leverage properties
        self.trading_mode: TradingMode = config.get("trading_mode", TradingMode.SPOT)
        self.margin_mode: MarginMode = (
            MarginMode(config.get("margin_mode")) if config.get("margin_mode") else MarginMode.NONE
        )
        self.liquidation_buffer = config.get("liquidation_buffer", 0.05)

        # Deep merge ft_has with default ft_has options
        self._ft_has = deep_merge_dicts(self._ft_has, deepcopy(self._ft_has_default))
        if self.trading_mode == TradingMode.FUTURES:
            self._ft_has = deep_merge_dicts(self._ft_has_futures, self._ft_has)
        if exchange_conf.get("_ft_has_params"):
            self._ft_has = deep_merge_dicts(exchange_conf.get("_ft_has_params"), self._ft_has)
            logger.info("Overriding exchange._ft_has with config params, result: %s", self._ft_has)

        # Assign this directly for easy access
        self._ohlcv_partial_candle = self._ft_has["ohlcv_partial_candle"]

        self._max_trades_limit = self._ft_has["trades_limit"]

        self._trades_pagination = self._ft_has["trades_pagination"]
        self._trades_pagination_arg = self._ft_has["trades_pagination_arg"]

        # Initialize ccxt objects
        ccxt_config = self._ccxt_config
        ccxt_config = deep_merge_dicts(exchange_conf.get("ccxt_config", {}), ccxt_config)
        ccxt_config = deep_merge_dicts(exchange_conf.get("ccxt_sync_config", {}), ccxt_config)

        self._api = self._init_ccxt(exchange_conf, True, ccxt_config)

        ccxt_async_config = self._ccxt_config
        ccxt_async_config = deep_merge_dicts(
            exchange_conf.get("ccxt_config", {}), ccxt_async_config
        )
        ccxt_async_config = deep_merge_dicts(
            exchange_conf.get("ccxt_async_config", {}), ccxt_async_config
        )
        self._api_async = self._init_ccxt(exchange_conf, False, ccxt_async_config)
        _has_watch_ohlcv = self.exchange_has("watchOHLCV") and self._ft_has["ws_enabled"]
        if (
            self._config["runmode"] in TRADE_MODES
            and exchange_conf.get("enable_ws", True)
            and _has_watch_ohlcv
        ):
            self._ws_async = self._init_ccxt(exchange_conf, False, ccxt_async_config)
            self._exchange_ws = ExchangeWS(self._config, self._ws_async)

        logger.info(f'Using Exchange "{self.name}"')
        self.required_candle_call_count = 1
        # Converts the interval provided in minutes in config to seconds
        self.markets_refresh_interval: int = (
            exchange_conf.get("markets_refresh_interval", 60) * 60 * 1000
        )

        if validate:
            # Initial markets load
            self.reload_markets(True, load_leverage_tiers=False)
            self.validate_config(config)
            self._startup_candle_count: int = config.get("startup_candle_count", 0)
            self.required_candle_call_count = self.validate_required_startup_candles(
                self._startup_candle_count, config.get("timeframe", "")
            )

        if self.trading_mode != TradingMode.SPOT and load_leverage_tiers:
            self.fill_leverage_tiers()
        self.additional_exchange_init()

   

 


  

    @property
    def _ccxt_config(self) -> dict:
        # Parameters to add directly to ccxt sync/async initialization.
        if self.trading_mode == TradingMode.MARGIN:
            return {"options": {"defaultType": "margin"}}
        elif self.trading_mode == TradingMode.FUTURES:
            return {"options": {"defaultType": self._ft_has["ccxt_futures_name"]}}
        else:
            return {}

    @property
    def name(self) -> str:
        """exchange Name (from ccxt)"""
        return self._api.name

    @property
    def id(self) -> str:
        """exchange ccxt id"""
        return self._api.id

    @property
    def timeframes(self) -> list[str]:
        return list((self._api.timeframes or {}).keys())

    @property
    def markets(self) -> dict[str, Any]:
        """exchange ccxt markets"""
        if not self._markets:
            logger.info("Markets were not loaded. Loading them now..")
            self.reload_markets(True)
        return self._markets

    @property
    def precisionMode(self) -> int:
        """Exchange ccxt precisionMode"""
        return self._api.precisionMode

    @property
    def precision_mode_price(self) -> int:
        """
        Exchange ccxt precisionMode used for price
        Workaround for ccxt limitation to not have precisionMode for price
        if it differs for an exchange
        Might need to be updated if https://github.com/ccxt/ccxt/issues/20408 is fixed.
        """
        return self._api.precisionMode






   

   

    
   

 

    

 



  

    def create_order(
        self,
        *,
        pair: str,
        ordertype: str,
        side: BuySell,
        amount: float,
        rate: float,
        leverage: float,
        reduceOnly: bool = False,
        time_in_force: str = "GTC",
    ) -> CcxtOrder:
        if self._config["dry_run"]:
            dry_order = self.create_dry_run_order(
                pair, ordertype, side, amount, self.price_to_precision(pair, rate), leverage
            )
            return dry_order

        params = self._get_params(side, ordertype, leverage, reduceOnly, time_in_force)

        try:
            # Set the precision for amount and price(rate) as accepted by the exchange
            amount = self.amount_to_precision(pair, self._amount_to_contracts(pair, amount))
            needs_price = self._order_needs_price(side, ordertype)
            rate_for_order = self.price_to_precision(pair, rate) if needs_price else None

            if not reduceOnly:
                self._lev_prep(pair, leverage, side)

            order = self._api.create_order(
                pair,
                ordertype,
                side,
                amount,
                rate_for_order,
                params,
            )
            if order.get("status") is None:
                # Map empty status to open.
                order["status"] = "open"

            if order.get("type") is None:
                order["type"] = ordertype

            self._log_exchange_response("create_order", order)
            order = self._order_contracts_to_amount(order)
            return order

        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(
                f"Insufficient funds to create {ordertype} {side} order on market {pair}. "
                f"Tried to {side} amount {amount} at rate {rate}."
                f"Message: {e}"
            ) from e
        except ccxt.InvalidOrder as e:
            raise InvalidOrderException(
                f"Could not create {ordertype} {side} order on market {pair}. "
                f"Tried to {side} amount {amount} at rate {rate}. "
                f"Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not place {side} order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    

    

    @retrier(retries=0)
    def create_stoploss(
        self,
        pair: str,
        amount: float,
        stop_price: float,
        order_types: dict,
        side: BuySell,
        leverage: float,
    ) -> CcxtOrder:
        """
        creates a stoploss order.
        requires `_ft_has['stoploss_order_types']` to be set as a dict mapping limit and market
            to the corresponding exchange type.

        The precise ordertype is determined by the order_types dict or exchange default.

        The exception below should never raise, since we disallow
        starting the bot in validate_ordertypes()

        This may work with a limited number of other exchanges, but correct working
            needs to be tested individually.
        WARNING: setting `stoploss_on_exchange` to True will NOT auto-enable stoploss on exchange.
            `stoploss_adjust` must still be implemented for this to work.
        """
        if not self._ft_has["stoploss_on_exchange"]:
            raise OperationalException(f"stoploss is not implemented for {self.name}.")

        user_order_type = order_types.get("stoploss", "market")
        ordertype, user_order_type = self._get_stop_order_type(user_order_type)
        round_mode = ROUND_DOWN if side == "buy" else ROUND_UP
        stop_price_norm = self.price_to_precision(pair, stop_price, rounding_mode=round_mode)
        limit_rate = None
        if user_order_type == "limit":
            limit_rate = self._get_stop_limit_rate(stop_price, order_types, side)
            limit_rate = self.price_to_precision(pair, limit_rate, rounding_mode=round_mode)

        if self._config["dry_run"]:
            dry_order = self.create_dry_run_order(
                pair,
                ordertype,
                side,
                amount,
                stop_price_norm,
                stop_loss=True,
                leverage=leverage,
            )
            return dry_order

        try:
            params = self._get_stop_params(
                side=side, ordertype=ordertype, stop_price=stop_price_norm
            )
            if self.trading_mode == TradingMode.FUTURES:
                params["reduceOnly"] = True
                if "stoploss_price_type" in order_types and "stop_price_type_field" in self._ft_has:
                    price_type = self._ft_has["stop_price_type_value_mapping"][
                        order_types.get("stoploss_price_type", PriceType.LAST)
                    ]
                    params[self._ft_has["stop_price_type_field"]] = price_type

            amount = self.amount_to_precision(pair, self._amount_to_contracts(pair, amount))

            self._lev_prep(pair, leverage, side, accept_fail=True)
            order = self._api.create_order(
                symbol=pair,
                type=ordertype,
                side=side,
                amount=amount,
                price=limit_rate,
                params=params,
            )
            self._log_exchange_response("create_stoploss_order", order)
            order = self._order_contracts_to_amount(order)
            logger.info(
                f"stoploss {user_order_type} order added for {pair}. "
                f"stop price: {stop_price}. limit: {limit_rate}"
            )
            return order
        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(
                f"Insufficient funds to create {ordertype} {side} order on market {pair}. "
                f"Tried to {side} amount {amount} at rate {limit_rate} with "
                f"stop-price {stop_price_norm}. Message: {e}"
            ) from e
        except (ccxt.InvalidOrder, ccxt.BadRequest, ccxt.OperationRejected) as e:
            # Errors:
            # `Order would trigger immediately.`
            raise InvalidOrderException(
                f"Could not create {ordertype} {side} order on market {pair}. "
                f"Tried to {side} amount {amount} at rate {limit_rate} with "
                f"stop-price {stop_price_norm}. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not place stoploss order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

 

    @retrier(retries=API_FETCH_ORDER_RETRY_COUNT)
    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        if self._config["dry_run"]:
            return self.fetch_dry_run_order(order_id)
        if params is None:
            params = {}
        try:
            if not self.exchange_has("fetchOrder"):
                return self.fetch_order_emulated(order_id, pair, params)
            order = self._api.fetch_order(order_id, pair, params=params)
            self._log_exchange_response("fetch_order", order)
            order = self._order_contracts_to_amount(order)
            return order
        except ccxt.OrderNotFound as e:
            raise RetryableOrderError(
                f"Order not found (pair: {pair} id: {order_id}). Message: {e}"
            ) from e
        except ccxt.InvalidOrder as e:
            raise InvalidOrderException(
                f"Tried to get an invalid order (pair: {pair} id: {order_id}). Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

  

    def fetch_order_or_stoploss_order(
        self, order_id: str, pair: str, stoploss_order: bool = False
    ) -> CcxtOrder:
        """
        Simple wrapper calling either fetch_order or fetch_stoploss_order depending on
        the stoploss_order parameter
        :param order_id: OrderId to fetch order
        :param pair: Pair corresponding to order_id
        :param stoploss_order: If true, uses fetch_stoploss_order, otherwise fetch_order.
        """
        if stoploss_order:
            return self.fetch_stoploss_order(order_id, pair)
        return self.fetch_order(order_id, pair)

    def check_order_canceled_empty(self, order: CcxtOrder) -> bool:
        """
        Verify if an order has been cancelled without being partially filled
        :param order: Order dict as returned from fetch_order()
        :return: True if order has been cancelled without being filled, False otherwise.
        """
        return order.get("status") in NON_OPEN_EXCHANGE_STATES and order.get("filled") == 0.0

    @retrier
    def cancel_order(self, order_id: str, pair: str, params: dict | None = None) -> dict[str, Any]:
        if self._config["dry_run"]:
            try:
                order = self.fetch_dry_run_order(order_id)

                order.update({"status": "canceled", "filled": 0.0, "remaining": order["amount"]})
                return order
            except InvalidOrderException:
                return {}

        if params is None:
            params = {}
        try:
            order = self._api.cancel_order(order_id, pair, params=params)
            self._log_exchange_response("cancel_order", order)
            order = self._order_contracts_to_amount(order)
            return order
        except ccxt.InvalidOrder as e:
            raise InvalidOrderException(f"Could not cancel order. Message: {e}") from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not cancel order due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def cancel_stoploss_order(self, order_id: str, pair: str, params: dict | None = None) -> dict:
        return self.cancel_order(order_id, pair, params)

    def is_cancel_order_result_suitable(self, corder) -> TypeGuard[CcxtOrder]:
        if not isinstance(corder, dict):
            return False

        required = ("fee", "status", "amount")
        return all(corder.get(k, None) is not None for k in required)

    def cancel_order_with_result(self, order_id: str, pair: str, amount: float) -> CcxtOrder:
        """
        Cancel order returning a result.
        Creates a fake result if cancel order returns a non-usable result
        and fetch_order does not work (certain exchanges don't return cancelled orders)
        :param order_id: Orderid to cancel
        :param pair: Pair corresponding to order_id
        :param amount: Amount to use for fake response
        :return: Result from either cancel_order if usable, or fetch_order
        """
        try:
            corder = self.cancel_order(order_id, pair)
            if self.is_cancel_order_result_suitable(corder):
                return corder
        except InvalidOrderException:
            logger.warning(f"Could not cancel order {order_id} for {pair}.")
        try:
            order = self.fetch_order(order_id, pair)
        except InvalidOrderException:
            logger.warning(f"Could not fetch cancelled order {order_id}.")
            order = {
                "id": order_id,
                "status": "canceled",
                "amount": amount,
                "filled": 0.0,
                "fee": {},
                "info": {},
            }

        return order

    def cancel_stoploss_order_with_result(
        self, order_id: str, pair: str, amount: float
    ) -> CcxtOrder:
        """
        Cancel stoploss order returning a result.
        Creates a fake result if cancel order returns a non-usable result
        and fetch_order does not work (certain exchanges don't return cancelled orders)
        :param order_id: stoploss-order-id to cancel
        :param pair: Pair corresponding to order_id
        :param amount: Amount to use for fake response
        :return: Result from either cancel_order if usable, or fetch_order
        """
        corder = self.cancel_stoploss_order(order_id, pair)
        if self.is_cancel_order_result_suitable(corder):
            return corder
        try:
            order = self.fetch_stoploss_order(order_id, pair)
        except InvalidOrderException:
            logger.warning(f"Could not fetch cancelled stoploss order {order_id}.")
            order = {"id": order_id, "fee": {}, "status": "canceled", "amount": amount, "info": {}}

        return order

  

    def _fetch_orders_emulate(self, pair: str, since_ms: int) -> list[CcxtOrder]:
        orders = []
        if self.exchange_has("fetchClosedOrders"):
            orders = self._api.fetch_closed_orders(pair, since=since_ms)
            if self.exchange_has("fetchOpenOrders"):
                orders_open = self._api.fetch_open_orders(pair, since=since_ms)
                orders.extend(orders_open)
        return orders

    @retrier(retries=0)
    def fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        """
        Fetch all orders for a pair "since"
        :param pair: Pair for the query
        :param since: Starting time for the query
        """
        if self._config["dry_run"]:
            return []

        try:
            since_ms = int((since.timestamp() - 10) * 1000)

            if self.exchange_has("fetchOrders"):
                if not params:
                    params = {}
                try:
                    orders: list[CcxtOrder] = self._api.fetch_orders(
                        pair, since=since_ms, params=params
                    )
                except ccxt.NotSupported:
                    # Some exchanges don't support fetchOrders
                    # attempt to fetch open and closed orders separately
                    orders = self._fetch_orders_emulate(pair, since_ms)
            else:
                orders = self._fetch_orders_emulate(pair, since_ms)
            self._log_exchange_response("fetch_orders", orders)
            orders = [self._order_contracts_to_amount(o) for o in orders]
            return orders
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not fetch positions due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def fetch_trading_fees(self) -> dict[str, Any]:
        """
        Fetch user account trading fees
        Can be cached, should not update often.
        """
        if (
            self._config["dry_run"]
            or self.trading_mode != TradingMode.FUTURES
            or not self.exchange_has("fetchTradingFees")
        ):
            return {}
        try:
            trading_fees: dict[str, Any] = self._api.fetch_trading_fees()
            self._log_exchange_response("fetch_trading_fees", trading_fees)
            return trading_fees
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not fetch trading fees due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def fetch_bids_asks(self, symbols: list[str] | None = None, *, cached: bool = False) -> dict:
        """
        :param symbols: List of symbols to fetch
        :param cached: Allow cached result
        :return: fetch_bids_asks result
        """
        if not self.exchange_has("fetchBidsAsks"):
            return {}
        if cached:
            with self._cache_lock:
                tickers = self._fetch_tickers_cache.get("fetch_bids_asks")
            if tickers:
                return tickers
        try:
            tickers = self._api.fetch_bids_asks(symbols)
            with self._cache_lock:
                self._fetch_tickers_cache["fetch_bids_asks"] = tickers
            return tickers
        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching bids/asks in batch. "
                f"Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load bids/asks due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def get_tickers(
        self,
        symbols: list[str] | None = None,
        *,
        cached: bool = False,
        market_type: TradingMode | None = None,
    ) -> Tickers:
        """
        :param symbols: List of symbols to fetch
        :param cached: Allow cached result
        :param market_type: Market type to fetch - either spot or futures.
        :return: fetch_tickers result
        """
        tickers: Tickers
        if not self.exchange_has("fetchTickers"):
            return {}
        cache_key = f"fetch_tickers_{market_type}" if market_type else "fetch_tickers"
        if cached:
            with self._cache_lock:
                tickers = self._fetch_tickers_cache.get(cache_key)  # type: ignore
            if tickers:
                return tickers
        try:
            # Re-map futures to swap
            market_types = {
                TradingMode.FUTURES: "swap",
            }
            params = {"type": market_types.get(market_type, market_type)} if market_type else {}
            tickers = self._api.fetch_tickers(symbols, params)
            with self._cache_lock:
                self._fetch_tickers_cache[cache_key] = tickers
            return tickers
        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching tickers in batch. "
                f"Message: {e}"
            ) from e
        except ccxt.BadSymbol as e:
            logger.warning(
                f"Could not load tickers due to {e.__class__.__name__}. Message: {e} ."
                "Reloading markets."
            )
            self.reload_markets(True)
            # Re-raise exception to repeat the call.
            raise TemporaryError from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load tickers due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_proxy_coin(self) -> str:
        """
        Get the proxy coin for the given coin
        Falls back to the stake currency if no proxy coin is found
        :return: Proxy coin or stake currency
        """
        return self._config["stake_currency"]

    def get_conversion_rate(self, coin: str, currency: str) -> float | None:
        """
        Quick and cached way to get conversion rate one currency to the other.
        Can then be used as "rate * amount" to convert between currencies.
        :param coin: Coin to convert
        :param currency: Currency to convert to
        :returns: Conversion rate from coin to currency
        :raises: ExchangeErrors
        """

        if (proxy_coin := self._ft_has["proxy_coin_mapping"].get(coin, None)) is not None:
            coin = proxy_coin
        if (proxy_currency := self._ft_has["proxy_coin_mapping"].get(currency, None)) is not None:
            currency = proxy_currency
        if coin == currency:
            return 1.0
        tickers = self.get_tickers(cached=True)
        try:
            for pair in self.get_valid_pair_combination(coin, currency):
                ticker: Ticker | None = tickers.get(pair, None)
                if not ticker:
                    tickers_other: Tickers = self.get_tickers(
                        cached=True,
                        market_type=(
                            TradingMode.SPOT
                            if self.trading_mode != TradingMode.SPOT
                            else TradingMode.FUTURES
                        ),
                    )
                    ticker = tickers_other.get(pair, None)
                if ticker:
                    rate: float | None = safe_value_fallback2(ticker, ticker, "last", "ask", None)
                    if rate and pair.startswith(currency) and not pair.endswith(currency):
                        rate = 1.0 / rate
                    return rate
        except ValueError:
            return None
        return None

    @retrier
    def fetch_ticker(self, pair: str) -> Ticker:
        try:
            if pair not in self.markets or self.markets[pair].get("active", False) is False:
                raise ExchangeError(f"Pair {pair} not available")
            data: Ticker = self._api.fetch_ticker(pair)
            return data
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load ticker due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @staticmethod
    def get_next_limit_in_list(
        limit: int,
        limit_range: list[int] | None,
        range_required: bool = True,
        upper_limit: int | None = None,
    ):
        """
        Get next greater value in the list.
        Used by fetch_l2_order_book if the api only supports a limited range
        if both limit_range and upper_limit is provided, limit_range wins.
        """
        if not limit_range:
            return min(limit, upper_limit) if upper_limit else limit

        result = min([x for x in limit_range if limit <= x] + [max(limit_range)])
        if not range_required and limit > result:
            # Range is not required - we can use None as parameter.
            return None
        return result

    @retrier
    def fetch_l2_order_book(self, pair: str, limit: int = 100) -> OrderBook:
        """
        Get L2 order book from exchange.
        Can be limited to a certain amount (if supported).
        Returns a dict in the format
        {'asks': [price, volume], 'bids': [price, volume]}
        """
        limit1 = self.get_next_limit_in_list(
            limit,
            self._ft_has["l2_limit_range"],
            self._ft_has["l2_limit_range_required"],
            self._ft_has["l2_limit_upper"],
        )
        try:
            return self._api.fetch_l2_order_book(pair, limit1)
        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching order book. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get order book due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _get_price_side(self, side: str, is_short: bool, conf_strategy: dict) -> BidAsk:
        price_side = conf_strategy["price_side"]

        if price_side in ("same", "other"):
            price_map = {
                ("entry", "long", "same"): "bid",
                ("entry", "long", "other"): "ask",
                ("entry", "short", "same"): "ask",
                ("entry", "short", "other"): "bid",
                ("exit", "long", "same"): "ask",
                ("exit", "long", "other"): "bid",
                ("exit", "short", "same"): "bid",
                ("exit", "short", "other"): "ask",
            }
            price_side = price_map[(side, "short" if is_short else "long", price_side)]
        return price_side

    def get_rate(
        self,
        pair: str,
        refresh: bool,
        side: EntryExit,
        is_short: bool,
        order_book: OrderBook | None = None,
        ticker: Ticker | None = None,
    ) -> float:
        """
        Calculates bid/ask target
        bid rate - between current ask price and last price
        ask rate - either using ticker bid or first bid based on orderbook
        or remain static in any other case since it's not updating.
        :param pair: Pair to get rate for
        :param refresh: allow cached data
        :param side: "buy" or "sell"
        :return: float: Price
        :raises PricingError if orderbook price could not be determined.
        """
        name = side.capitalize()
        strat_name = "entry_pricing" if side == "entry" else "exit_pricing"

        cache_rate: TTLCache = self._entry_rate_cache if side == "entry" else self._exit_rate_cache
        if not refresh:
            with self._cache_lock:
                rate = cache_rate.get(pair)
            # Check if cache has been invalidated
            if rate:
                logger.debug(f"Using cached {side} rate for {pair}.")
                return rate

        conf_strategy = self._config.get(strat_name, {})

        price_side = self._get_price_side(side, is_short, conf_strategy)

        if conf_strategy.get("use_order_book", False):
            order_book_top = conf_strategy.get("order_book_top", 1)
            if order_book is None:
                order_book = self.fetch_l2_order_book(pair, order_book_top)
            rate = self._get_rate_from_ob(pair, side, order_book, name, price_side, order_book_top)
        else:
            logger.debug(f"Using Last {price_side.capitalize()} / Last Price")
            if ticker is None:
                ticker = self.fetch_ticker(pair)
            rate = self._get_rate_from_ticker(side, ticker, conf_strategy, price_side)

        if rate is None:
            raise PricingError(f"{name}-Rate for {pair} was empty.")
        with self._cache_lock:
            cache_rate[pair] = rate

        return rate

    def _get_rate_from_ticker(
        self, side: EntryExit, ticker: Ticker, conf_strategy: dict[str, Any], price_side: BidAsk
    ) -> float | None:
        """
        Get rate from ticker.
        """
        ticker_rate = ticker[price_side]
        if ticker["last"] and ticker_rate:
            if side == "entry" and ticker_rate > ticker["last"]:
                balance = conf_strategy.get("price_last_balance", 0.0)
                ticker_rate = ticker_rate + balance * (ticker["last"] - ticker_rate)
            elif side == "exit" and ticker_rate < ticker["last"]:
                balance = conf_strategy.get("price_last_balance", 0.0)
                ticker_rate = ticker_rate - balance * (ticker_rate - ticker["last"])
        rate = ticker_rate
        return rate

    def _get_rate_from_ob(
        self,
        pair: str,
        side: EntryExit,
        order_book: OrderBook,
        name: str,
        price_side: BidAsk,
        order_book_top: int,
    ) -> float:
        """
        Get rate from orderbook
        :raises: PricingError if rate could not be determined.
        """
        logger.debug("order_book %s", order_book)
        # top 1 = index 0
        try:
            obside: OBLiteral = "bids" if price_side == "bid" else "asks"
            rate = order_book[obside][order_book_top - 1][0]
        except (IndexError, KeyError) as e:
            logger.warning(
                f"{pair} - {name} Price at location {order_book_top} from orderbook "
                f"could not be determined. Orderbook: {order_book}"
            )
            raise PricingError from e
        logger.debug(
            f"{pair} - {name} price from orderbook {price_side.capitalize()}"
            f"side - top {order_book_top} order book {side} rate {rate:.8f}"
        )
        return rate

    def get_rates(self, pair: str, refresh: bool, is_short: bool) -> tuple[float, float]:
        entry_rate = None
        exit_rate = None
        if not refresh:
            with self._cache_lock:
                entry_rate = self._entry_rate_cache.get(pair)
                exit_rate = self._exit_rate_cache.get(pair)
            if entry_rate:
                logger.debug(f"Using cached buy rate for {pair}.")
            if exit_rate:
                logger.debug(f"Using cached sell rate for {pair}.")

        entry_pricing = self._config.get("entry_pricing", {})
        exit_pricing = self._config.get("exit_pricing", {})
        order_book = ticker = None
        if not entry_rate and entry_pricing.get("use_order_book", False):
            order_book_top = max(
                entry_pricing.get("order_book_top", 1), exit_pricing.get("order_book_top", 1)
            )
            order_book = self.fetch_l2_order_book(pair, order_book_top)
            entry_rate = self.get_rate(pair, refresh, "entry", is_short, order_book=order_book)
        elif not entry_rate:
            ticker = self.fetch_ticker(pair)
            entry_rate = self.get_rate(pair, refresh, "entry", is_short, ticker=ticker)
        if not exit_rate:
            exit_rate = self.get_rate(
                pair, refresh, "exit", is_short, order_book=order_book, ticker=ticker
            )
        return entry_rate, exit_rate

    # Fee handling

    @retrier
    def get_trades_for_order(
        self, order_id: str, pair: str, since: datetime, params: dict | None = None
    ) -> list:
        """
        Fetch Orders using the "fetch_my_trades" endpoint and filter them by order-id.
        The "since" argument passed in is coming from the database and is in UTC,
        as timezone-native datetime object.
        From the python documentation:
            > Naive datetime instances are assumed to represent local time
        Therefore, calling "since.timestamp()" will get the UTC timestamp, after applying the
        transformation from local timezone to UTC.
        This works for timezones UTC+ since then the result will contain trades from a few hours
        instead of from the last 5 seconds, however fails for UTC- timezones,
        since we're then asking for trades with a "since" argument in the future.

        :param order_id order_id: Order-id as given when creating the order
        :param pair: Pair the order is for
        :param since: datetime object of the order creation time. Assumes object is in UTC.
        """
        if self._config["dry_run"]:
            return []
        if not self.exchange_has("fetchMyTrades"):
            return []
        try:
            # Allow 5s offset to catch slight time offsets (discovered in #1185)
            # since needs to be int in milliseconds
            _params = params if params else {}
            my_trades = self._api.fetch_my_trades(
                pair,
                int((since.replace(tzinfo=timezone.utc).timestamp() - 5) * 1000),
                params=_params,
            )
            matched_trades = [trade for trade in my_trades if trade["order"] == order_id]

            self._log_exchange_response("get_trades_for_order", matched_trades)

            matched_trades = self._trades_contracts_to_amount(matched_trades)

            return matched_trades
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get trades due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_order_id_conditional(self, order: CcxtOrder) -> str:
        return order["id"]

    @retrier
    def get_fee(
        self,
        symbol: str,
        order_type: str = "",
        side: str = "",
        amount: float = 1,
        price: float = 1,
        taker_or_maker: MakerTaker = "maker",
    ) -> float:
        """
        Retrieve fee from exchange
        :param symbol: Pair
        :param order_type: Type of order (market, limit, ...)
        :param side: Side of order (buy, sell)
        :param amount: Amount of order
        :param price: Price of order
        :param taker_or_maker: 'maker' or 'taker' (ignored if "type" is provided)
        """
        if order_type and order_type == "market":
            taker_or_maker = "taker"
        try:
            if self._config["dry_run"] and self._config.get("fee", None) is not None:
                return self._config["fee"]
            # validate that markets are loaded before trying to get fee
            if self._api.markets is None or len(self._api.markets) == 0:
                self._api.load_markets(params={})

            return self._api.calculate_fee(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                takerOrMaker=taker_or_maker,
            )["rate"]
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get fee info due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @staticmethod
    def order_has_fee(order: CcxtOrder) -> bool:
        """
        Verifies if the passed in order dict has the needed keys to extract fees,
        and that these keys (currency, cost) are not empty.
        :param order: Order or trade (one trade) dict
        :return: True if the fee substructure contains currency and cost, false otherwise
        """
        if not isinstance(order, dict):
            return False
        return (
            "fee" in order
            and order["fee"] is not None
            and (order["fee"].keys() >= {"currency", "cost"})
            and order["fee"]["currency"] is not None
            and order["fee"]["cost"] is not None
        )

    def calculate_fee_rate(
        self, fee: dict, symbol: str, cost: float, amount: float
    ) -> float | None:
        """
        Calculate fee rate if it's not given by the exchange.
        :param fee: ccxt Fee dict - must contain cost / currency / rate
        :param symbol: Symbol of the order
        :param cost: Total cost of the order
        :param amount: Amount of the order
        """
        if fee.get("rate") is not None:
            return fee.get("rate")
        fee_curr = fee.get("currency")
        if fee_curr is None:
            return None
        fee_cost = float(fee["cost"])

        # Calculate fee based on order details
        if fee_curr == self.get_pair_base_currency(symbol):
            # Base currency - divide by amount
            return round(fee_cost / amount, 8)
        elif fee_curr == self.get_pair_quote_currency(symbol):
            # Quote currency - divide by cost
            return round(fee_cost / cost, 8) if cost else None
        else:
            # If Fee currency is a different currency
            if not cost:
                # If cost is None or 0.0 -> falsy, return None
                return None
            try:
                fee_to_quote_rate = self.get_conversion_rate(
                    fee_curr, self._config["stake_currency"]
                )
                if not fee_to_quote_rate:
                    raise ValueError("Conversion rate not found.")
            except (ValueError, ExchangeError):
                fee_to_quote_rate = self._config["exchange"].get("unknown_fee_rate", None)
                if not fee_to_quote_rate:
                    return None
            return round((fee_cost * fee_to_quote_rate) / cost, 8)

    def extract_cost_curr_rate(
        self, fee: dict[str, Any], symbol: str, cost: float, amount: float
    ) -> tuple[float, str, float | None]:
        """
        Extract tuple of cost, currency, rate.
        Requires order_has_fee to run first!
        :param fee: ccxt Fee dict - must contain cost / currency / rate
        :param symbol: Symbol of the order
        :param cost: Total cost of the order
        :param amount: Amount of the order
        :return: Tuple with cost, currency, rate of the given fee dict
        """
        return (
            float(fee["cost"]),
            fee["currency"],
            self.calculate_fee_rate(fee, symbol, cost, amount),
        )

    # Historic data

    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        is_new_pair: bool = False,
        until_ms: int | None = None,
    ) -> DataFrame:
        """
        Get candle history using asyncio and returns the list of candles.
        Handles all async work for this.
        Async over one pair, assuming we get `self.ohlcv_candle_limit()` candles per call.
        :param pair: Pair to download
        :param timeframe: Timeframe to get data for
        :param since_ms: Timestamp in milliseconds to get history from
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        :param is_new_pair: used by binance subclass to allow "fast" new pair downloading
        :param until_ms: Timestamp in milliseconds to get history up to
        :return: Dataframe with candle (OHLCV) data
        """
        with self._loop_lock:
            pair, _, _, data, _ = self.loop.run_until_complete(
                self._async_get_historic_ohlcv(
                    pair=pair,
                    timeframe=timeframe,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    candle_type=candle_type,
                    raise_=True,
                )
            )
        logger.debug(f"Downloaded data for {pair} from ccxt with length {len(data)}.")
        return ohlcv_to_dataframe(data, timeframe, pair, fill_missing=False, drop_incomplete=True)

    async def _async_get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: CandleType,
        raise_: bool = False,
        until_ms: int | None = None,
    ) -> OHLCVResponse:
        """
        Download historic ohlcv
        :param candle_type: Any of the enum CandleType (must match trading mode!)
        """

        one_call = timeframe_to_msecs(timeframe) * self.ohlcv_candle_limit(
            timeframe, candle_type, since_ms
        )
        logger.debug(
            "one_call: %s msecs (%s)",
            one_call,
            dt_humanize_delta(dt_now() - timedelta(milliseconds=one_call)),
        )
        input_coroutines = [
            self._async_get_candle_history(pair, timeframe, candle_type, since)
            for since in range(since_ms, until_ms or dt_ts(), one_call)
        ]

        data: list = []
        # Chunk requests into batches of 100 to avoid overwhelming ccxt Throttling
        for input_coro in chunks(input_coroutines, 100):
            results = await asyncio.gather(*input_coro, return_exceptions=True)
            for res in results:
                if isinstance(res, BaseException):
                    logger.warning(f"Async code raised an exception: {repr(res)}")
                    if raise_:
                        raise res
                    continue
                else:
                    # Deconstruct tuple if it's not an exception
                    p, _, c, new_data, _ = res
                    if p == pair and c == candle_type:
                        data.extend(new_data)
        # Sort data again after extending the result - above calls return in "async order"
        data = sorted(data, key=lambda x: x[0])
        return pair, timeframe, candle_type, data, self._ohlcv_partial_candle

    def _try_build_from_websocket(
        self, pair: str, timeframe: str, candle_type: CandleType
    ) -> Coroutine[Any, Any, OHLCVResponse] | None:
        """
        Try to build a coroutine to get data from websocket.
        """
        if self._can_use_websocket(self._exchange_ws, pair, timeframe, candle_type):
            candle_ts = dt_ts(timeframe_to_prev_date(timeframe))
            prev_candle_ts = dt_ts(date_minus_candles(timeframe, 1))
            candles = self._exchange_ws.ohlcvs(pair, timeframe)
            half_candle = int(candle_ts - (candle_ts - prev_candle_ts) * 0.5)
            last_refresh_time = int(
                self._exchange_ws.klines_last_refresh.get((pair, timeframe, candle_type), 0)
            )

            if candles and candles[-1][0] >= prev_candle_ts and last_refresh_time >= half_candle:
                # Usable result, candle contains the previous candle.
                # Also, we check if the last refresh time is no more than half the candle ago.
                logger.debug(f"reuse watch result for {pair}, {timeframe}, {last_refresh_time}")

                return self._exchange_ws.get_ohlcv(pair, timeframe, candle_type, candle_ts)
            logger.info(
                f"Couldn't reuse watch for {pair}, {timeframe}, falling back to REST api. "
                f"{candle_ts < last_refresh_time}, {candle_ts}, {last_refresh_time}, "
                f"{format_ms_time(candle_ts)}, {format_ms_time(last_refresh_time)} "
            )
        return None

    def _can_use_websocket(
        self, exchange_ws: ExchangeWS | None, pair: str, timeframe: str, candle_type: CandleType
    ) -> TypeGuard[ExchangeWS]:
        """
        Check if we can use websocket for this pair.
        Acts as typeguard for exchangeWs
        """
        if exchange_ws and candle_type in (CandleType.SPOT, CandleType.FUTURES):
            return True
        return False

    def _build_coroutine(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        since_ms: int | None,
        cache: bool,
    ) -> Coroutine[Any, Any, OHLCVResponse]:
        not_all_data = cache and self.required_candle_call_count > 1
        if cache:
            if self._can_use_websocket(self._exchange_ws, pair, timeframe, candle_type):
                # Subscribe to websocket
                self._exchange_ws.schedule_ohlcv(pair, timeframe, candle_type)

        if cache and (pair, timeframe, candle_type) in self._klines:
            candle_limit = self.ohlcv_candle_limit(timeframe, candle_type)
            min_ts = dt_ts(date_minus_candles(timeframe, candle_limit - 5))

            if ws_resp := self._try_build_from_websocket(pair, timeframe, candle_type):
                # We have a usable websocket response
                return ws_resp

            # Check if 1 call can get us updated candles without hole in the data.
            if min_ts < self._pairs_last_refresh_time.get((pair, timeframe, candle_type), 0):
                # Cache can be used - do one-off call.
                not_all_data = False
            else:
                # Time jump detected, evict cache
                logger.info(
                    f"Time jump detected. Evicting cache for {pair}, {timeframe}, {candle_type}"
                )
                del self._klines[(pair, timeframe, candle_type)]

        if not since_ms and (self._ft_has["ohlcv_require_since"] or not_all_data):
            # Multiple calls for one pair - to get more history
            one_call = timeframe_to_msecs(timeframe) * self.ohlcv_candle_limit(
                timeframe, candle_type, since_ms
            )
            move_to = one_call * self.required_candle_call_count
            now = timeframe_to_next_date(timeframe)
            since_ms = dt_ts(now - timedelta(seconds=move_to // 1000))

        if since_ms:
            return self._async_get_historic_ohlcv(
                pair, timeframe, since_ms=since_ms, raise_=True, candle_type=candle_type
            )
        else:
            # One call ... "regular" refresh
            return self._async_get_candle_history(
                pair, timeframe, since_ms=since_ms, candle_type=candle_type
            )

    def _build_ohlcv_dl_jobs(
        self, pair_list: ListPairsWithTimeframes, since_ms: int | None, cache: bool
    ) -> tuple[list[Coroutine], list[PairWithTimeframe]]:
        """
        Build Coroutines to execute as part of refresh_latest_ohlcv
        """
        input_coroutines: list[Coroutine[Any, Any, OHLCVResponse]] = []
        cached_pairs = []
        for pair, timeframe, candle_type in set(pair_list):
            if timeframe not in self.timeframes and candle_type in (
                CandleType.SPOT,
                CandleType.FUTURES,
            ):
                logger.warning(
                    f"Cannot download ({pair}, {timeframe}) combination as this timeframe is "
                    f"not available on {self.name}. Available timeframes are "
                    f"{', '.join(self.timeframes)}."
                )
                continue

            if (
                (pair, timeframe, candle_type) not in self._klines
                or not cache
                or self._now_is_time_to_refresh(pair, timeframe, candle_type)
            ):
                input_coroutines.append(
                    self._build_coroutine(pair, timeframe, candle_type, since_ms, cache)
                )

            else:
                logger.debug(
                    f"Using cached candle (OHLCV) data for {pair}, {timeframe}, {candle_type} ..."
                )
                cached_pairs.append((pair, timeframe, candle_type))

        return input_coroutines, cached_pairs

    def _process_ohlcv_df(
        self,
        pair: str,
        timeframe: str,
        c_type: CandleType,
        ticks: list[list],
        cache: bool,
        drop_incomplete: bool,
    ) -> DataFrame:
        # keeping last candle time as last refreshed time of the pair
        if ticks and cache:
            idx = -2 if drop_incomplete and len(ticks) > 1 else -1
            self._pairs_last_refresh_time[(pair, timeframe, c_type)] = ticks[idx][0]
        # keeping parsed dataframe in cache
        ohlcv_df = ohlcv_to_dataframe(
            ticks, timeframe, pair=pair, fill_missing=True, drop_incomplete=drop_incomplete
        )
        if cache:
            if (pair, timeframe, c_type) in self._klines:
                old = self._klines[(pair, timeframe, c_type)]
                # Reassign so we return the updated, combined df
                ohlcv_df = clean_ohlcv_dataframe(
                    concat([old, ohlcv_df], axis=0),
                    timeframe,
                    pair,
                    fill_missing=True,
                    drop_incomplete=False,
                )
                candle_limit = self.ohlcv_candle_limit(timeframe, self._config["candle_type_def"])
                # Age out old candles
                ohlcv_df = ohlcv_df.tail(candle_limit + self._startup_candle_count)
                ohlcv_df = ohlcv_df.reset_index(drop=True)
                self._klines[(pair, timeframe, c_type)] = ohlcv_df
            else:
                self._klines[(pair, timeframe, c_type)] = ohlcv_df
        return ohlcv_df

    async def refresh_latest_ohlcv(
        self,
        pair_list: ListPairsWithTimeframes,
        *,
        since_ms: int | None = None,
        cache: bool = True,
        drop_incomplete: bool | None = None,
    ) -> dict[PairWithTimeframe, DataFrame]:
        """
        Refresh in-memory OHLCV asynchronously and set `_klines` with the result
        Loops asynchronously over pair_list and downloads all pairs async (semi-parallel).
        Only used in the dataprovider.refresh() method.
        :param pair_list: List of 2 element tuples containing pair, interval to refresh
        :param since_ms: time since when to download, in milliseconds
        :param cache: Assign result to _klines. Useful for one-off downloads like for pairlists
        :param drop_incomplete: Control candle dropping.
            Specifying None defaults to _ohlcv_partial_candle
        :return: Dict of [{(pair, timeframe): Dataframe}]
        """
        logger.debug("Refreshing candle (OHLCV) data for %d pairs", len(pair_list))

        # Gather coroutines to run
        ohlcv_dl_jobs, cached_pairs = self._build_ohlcv_dl_jobs(pair_list, since_ms, cache)

        results_df = {}
        # Chunk requests into batches of 100 to avoid overwhelming ccxt Throttling
        for dl_jobs_batch in chunks(ohlcv_dl_jobs, 100):

            async def gather_coroutines(coro):
                return await asyncio.gather(*coro, return_exceptions=True)

            with self._loop_lock:
                results = await gather_coroutines(dl_jobs_batch)

            for res in results:
                if isinstance(res, Exception):
                    logger.warning(f"Async code raised an exception: {repr(res)}")
                    continue
                # Deconstruct tuple (has 5 elements)
                pair, timeframe, c_type, ticks, drop_hint = res
                drop_incomplete_ = drop_hint if drop_incomplete is None else drop_incomplete
                ohlcv_df = self._process_ohlcv_df(
                    pair, timeframe, c_type, ticks, cache, drop_incomplete_
                )

                results_df[(pair, timeframe, c_type)] = ohlcv_df

        # Return cached klines
        for pair, timeframe, c_type in cached_pairs:
            results_df[(pair, timeframe, c_type)] = self.klines(
                (pair, timeframe, c_type), copy=False
            )

        return results_df

    def refresh_ohlcv_with_cache(
        self, pairs: list[PairWithTimeframe], since_ms: int
    ) -> dict[PairWithTimeframe, DataFrame]:
        """
        Refresh ohlcv data for all pairs in needed_pairs if necessary.
        Caches data with expiring per timeframe.
        Should only be used for pairlists which need "on time" expirarion, and no longer cache.
        """

        timeframes = {p[1] for p in pairs}
        for timeframe in timeframes:
            if (timeframe, since_ms) not in self._expiring_candle_cache:
                timeframe_in_sec = timeframe_to_seconds(timeframe)
                # Initialise cache
                self._expiring_candle_cache[(timeframe, since_ms)] = PeriodicCache(
                    ttl=timeframe_in_sec, maxsize=1000
                )

        # Get candles from cache
        candles = {
            c: self._expiring_candle_cache[(c[1], since_ms)].get(c, None)
            for c in pairs
            if c in self._expiring_candle_cache[(c[1], since_ms)]
        }
        pairs_to_download = [p for p in pairs if p not in candles]
        if pairs_to_download:
            candles = self.refresh_latest_ohlcv(pairs_to_download, since_ms=since_ms, cache=False)
            for c, val in candles.items():
                self._expiring_candle_cache[(c[1], since_ms)][c] = val
        return candles

    def _now_is_time_to_refresh(self, pair: str, timeframe: str, candle_type: CandleType) -> bool:
        # Timeframe in seconds
        interval_in_sec = timeframe_to_msecs(timeframe)
        plr = self._pairs_last_refresh_time.get((pair, timeframe, candle_type), 0) + interval_in_sec
        # current,active candle open date
        now = dt_ts(timeframe_to_prev_date(timeframe))
        return plr < now

    @retrier_async
    async def _async_get_candle_history(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        since_ms: int | None = None,
    ) -> OHLCVResponse:
        """
        Asynchronously get candle history data using fetch_ohlcv
        :param candle_type: '', mark, index, premiumIndex, or funding_rate
        returns tuple: (pair, timeframe, ohlcv_list)
        """
        try:
            # Fetch OHLCV asynchronously
            s = "(" + dt_from_ts(since_ms).isoformat() + ") " if since_ms is not None else ""
            logger.debug(
                "Fetching pair %s, %s, interval %s, since %s %s...",
                pair,
                candle_type,
                timeframe,
                since_ms,
                s,
            )
            params = deepcopy(self._ft_has.get("ohlcv_params", {}))
            candle_limit = self.ohlcv_candle_limit(
                timeframe, candle_type=candle_type, since_ms=since_ms
            )

            if candle_type and candle_type != CandleType.SPOT:
                params.update({"price": candle_type.value})
            if candle_type != CandleType.FUNDING_RATE:
                data = await self._api_async.fetch_ohlcv(
                    pair, timeframe=timeframe, since=since_ms, limit=candle_limit, params=params
                )
            else:
                # Funding rate
                data = await self._fetch_funding_rate_history(
                    pair=pair,
                    timeframe=timeframe,
                    limit=candle_limit,
                    since_ms=since_ms,
                )
            # Some exchanges sort OHLCV in ASC order and others in DESC.
            # Ex: Bittrex returns the list of OHLCV in ASC order (oldest first, newest last)
            # while GDAX returns the list of OHLCV in DESC order (newest first, oldest last)
            # Only sort if necessary to save computing time
            try:
                if data and data[0][0] > data[-1][0]:
                    data = sorted(data, key=lambda x: x[0])
            except IndexError:
                logger.exception("Error loading %s. Result was %s.", pair, data)
                return pair, timeframe, candle_type, [], self._ohlcv_partial_candle
            logger.debug("Done fetching pair %s, %s interval %s...", pair, candle_type, timeframe)
            return pair, timeframe, candle_type, data, self._ohlcv_partial_candle

        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching historical "
                f"candle (OHLCV) data. Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not fetch historical candle (OHLCV) data "
                f"for {pair}, {timeframe}, {candle_type} due to {e.__class__.__name__}. "
                f"Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(
                f"Could not fetch historical candle (OHLCV) data for "
                f"{pair}, {timeframe}, {candle_type}. Message: {e}"
            ) from e

    async def _fetch_funding_rate_history(
        self,
        pair: str,
        timeframe: str,
        limit: int,
        since_ms: int | None = None,
    ) -> list[list]:
        """
        Fetch funding rate history - used to selectively override this by subclasses.
        """
        # Funding rate
        data = await self._api_async.fetch_funding_rate_history(pair, since=since_ms, limit=limit)
        # Convert funding rate to candle pattern
        data = [[x["timestamp"], x["fundingRate"], 0, 0, 0, 0] for x in data]
        return data

    # fetch Trade data stuff

    def needed_candle_for_trades_ms(self, timeframe: str, candle_type: CandleType) -> int:
        candle_limit = self.ohlcv_candle_limit(timeframe, candle_type)
        tf_s = timeframe_to_seconds(timeframe)
        candles_fetched = candle_limit * self.required_candle_call_count

        max_candles = self._config["orderflow"]["max_candles"]

        required_candles = min(max_candles, candles_fetched)
        move_to = (
            tf_s * candle_limit * required_candles
            if required_candles > candle_limit
            else (max_candles + 1) * tf_s
        )

        now = timeframe_to_next_date(timeframe)
        return int((now - timedelta(seconds=move_to)).timestamp() * 1000)

    def _process_trades_df(
        self,
        pair: str,
        timeframe: str,
        c_type: CandleType,
        ticks: list[list],
        cache: bool,
        first_required_candle_date: int,
    ) -> DataFrame:
        # keeping parsed dataframe in cache
        trades_df = trades_list_to_df(ticks, True)

        if cache:
            if (pair, timeframe, c_type) in self._trades:
                old = self._trades[(pair, timeframe, c_type)]
                # Reassign so we return the updated, combined df
                combined_df = concat([old, trades_df], axis=0)
                logger.debug(f"Clean duplicated ticks from Trades data {pair}")
                trades_df = DataFrame(
                    trades_df_remove_duplicates(combined_df), columns=combined_df.columns
                )
                # Age out old candles
                trades_df = trades_df[first_required_candle_date < trades_df["timestamp"]]
                trades_df = trades_df.reset_index(drop=True)
            self._trades[(pair, timeframe, c_type)] = trades_df
        return trades_df

    async def _build_trades_dl_jobs(
        self, pairwt: PairWithTimeframe, data_handler, cache: bool
    ) -> tuple[PairWithTimeframe, DataFrame | None]:
        """
        Build coroutines to refresh trades for (they're then called through async.gather)
        """
        pair, timeframe, candle_type = pairwt
        since_ms = None
        new_ticks: list = []
        all_stored_ticks_df = DataFrame(columns=[*DEFAULT_TRADES_COLUMNS, "date"])
        first_candle_ms = self.needed_candle_for_trades_ms(timeframe, candle_type)
        # refresh, if
        # a. not in _trades
        # b. no cache used
        # c. need new data
        is_in_cache = (pair, timeframe, candle_type) in self._trades
        if (
            not is_in_cache
            or not cache
        ):
            logger.debug(f"Refreshing TRADES data for {pair}")
            # fetch trades since latest _trades and
            # store together with existing trades
            try:
                until = None
                from_id = None
                if is_in_cache:
                    from_id = self._trades[(pair, timeframe, candle_type)].iloc[-1]["id"]
                    until = dt_ts()  # now

                else:
                    until = int(timeframe_to_prev_date(timeframe).timestamp()) * 1000
                    all_stored_ticks_df = data_handler.trades_load(
                        f"{pair}-cached", self.trading_mode
                    )

                    if not all_stored_ticks_df.empty:
                        if (
                            all_stored_ticks_df.iloc[-1]["timestamp"] > first_candle_ms
                            and all_stored_ticks_df.iloc[0]["timestamp"] <= first_candle_ms
                        ):
                            # Use cache and populate further
                            last_cached_ms = all_stored_ticks_df.iloc[-1]["timestamp"]
                            from_id = all_stored_ticks_df.iloc[-1]["id"]
                            # only use cached if it's closer than first_candle_ms
                            since_ms = (
                                last_cached_ms
                                if last_cached_ms > first_candle_ms
                                else first_candle_ms
                            )
                        else:
                            # Skip cache, it's too old
                            all_stored_ticks_df = DataFrame(
                                columns=[*DEFAULT_TRADES_COLUMNS, "date"]
                            )

                # from_id overrules with exchange set to id paginate
                [_, new_ticks] = await self._async_get_trade_history(
                    pair,
                    since=since_ms if since_ms else first_candle_ms,
                    until=until,
                    from_id=from_id,
                )

            except Exception:
                logger.exception(f"Refreshing TRADES data for {pair} failed")
                return pairwt, None

            if new_ticks:
                all_stored_ticks_list = all_stored_ticks_df[DEFAULT_TRADES_COLUMNS].values.tolist()
                all_stored_ticks_list.extend(new_ticks)
                trades_df = self._process_trades_df(
                    pair,
                    timeframe,
                    candle_type,
                    all_stored_ticks_list,
                    cache,
                    first_required_candle_date=first_candle_ms,
                )
                data_handler.trades_store(
                    f"{pair}-cached", trades_df[DEFAULT_TRADES_COLUMNS], self.trading_mode
                )
                return pairwt, trades_df
            else:
                logger.error(f"No new ticks for {pair}")
        return pairwt, None

    async def refresh_latest_trades(
        self,
        pair_list: ListPairsWithTimeframes,
        *,
        cache: bool = True,
    ) -> dict[PairWithTimeframe, DataFrame]:
        """
        Refresh in-memory TRADES asynchronously and set `_trades` with the result
        Loops asynchronously over pair_list and downloads all pairs async (semi-parallel).
        Only used in the dataprovider.refresh() method.
        :param pair_list: List of 3 element tuples containing (pair, timeframe, candle_type)
        :param cache: Assign result to _trades. Useful for one-off downloads like for pairlists
        :return: Dict of [{(pair, timeframe): Dataframe}]
        """
        from freqtrade.data.history import get_datahandler

        data_handler = get_datahandler(
            self._config["datadir"], data_format=self._config["dataformat_trades"]
        )
        logger.debug("Refreshing TRADES data for %d pairs", len(pair_list))
        results_df = {}
        trades_dl_jobs = []
        for pair_wt in set(pair_list):
            trades_dl_jobs.append(self._build_trades_dl_jobs(pair_wt, data_handler, cache))

        async def gather_coroutines(coro):
            return await asyncio.gather(*coro, return_exceptions=True)

        for dl_job_chunk in chunks(trades_dl_jobs, 100):
            
            results =  await gather_coroutines(dl_job_chunk)

            for res in results:
                if isinstance(res, Exception):
                    logger.warning(f"Async code raised an exception: {repr(res)}")
                    continue
                pairwt, trades_df = res
                if trades_df is not None:
                    results_df[pairwt] = trades_df

        return results_df


    # Fetch historic trades

    @retrier_async
    async def _async_fetch_trades(
        self, pair: str, since: int | None = None, params: dict | None = None
    ) -> tuple[list[list], Any]:
        """
        Asynchronously gets trade history using fetch_trades.
        Handles exchange errors, does one call to the exchange.
        :param pair: Pair to fetch trade data for
        :param since: Since as integer timestamp in milliseconds
        returns: List of dicts containing trades, the next iteration value (new "since" or trade_id)
        """
        try:
            trades_limit = self._max_trades_limit
            # fetch trades asynchronously
            if params:
                logger.debug("Fetching trades for pair %s, params: %s ", pair, params)
                trades = await self._api_async.fetch_trades(pair, params=params, limit=trades_limit)
            else:
                logger.debug(
                    "Fetching trades for pair %s, since %s %s...",
                    pair,
                    since,
                    "(" + dt_from_ts(since).isoformat() + ") " if since is not None else "",
                )
                trades = await self._api_async.fetch_trades(pair, since=since, limit=trades_limit)
            trades = self._trades_contracts_to_amount(trades)
            pagination_value = self._get_trade_pagination_next_value(trades)
            return trades_dict_to_list(trades), pagination_value
        except ccxt.NotSupported as e:
            raise OperationalException(
                f"Exchange {self._api.name} does not support fetching historical trade data."
                f"Message: {e}"
            ) from e
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load trade history due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(f"Could not fetch trade data. Msg: {e}") from e

    def _valid_trade_pagination_id(self, pair: str, from_id: str) -> bool:
        """
        Verify trade-pagination id is valid.d
        Workaround for odd Kraken issue where ID is sometimes wrong.
        """
        return True

    def _get_trade_pagination_next_value(self, trades: list[dict]):
        """
        Extract pagination id for the next "from_id" value
        Applies only to fetch_trade_history by id.
        """
        if not trades:
            return None
        if self._trades_pagination == "id":
            return trades[-1].get("id")
        else:
            return trades[-1].get("timestamp")

    async def _async_get_trade_history_id_startup(
        self, pair: str, since: int
    ) -> tuple[list[list], str]:
        """
        override for initial trade_history_id call
        """
        return await self._async_fetch_trades(pair, since=since)

    async def _async_get_trade_history_id(
        self, pair: str, *, until: int, since: int, from_id: str | None = None
    ) -> tuple[str, list[list]]:
        """
        Asynchronously gets trade history using fetch_trades
        use this when exchange uses id-based iteration (check `self._trades_pagination`)
        :param pair: Pair to fetch trade data for
        :param since: Since as integer timestamp in milliseconds
        :param until: Until as integer timestamp in milliseconds
        :param from_id: Download data starting with ID (if id is known). Ignores "since" if set.
        returns tuple: (pair, trades-list)
        """

        trades: list[list] = []
        # DEFAULT_TRADES_COLUMNS: 0 -> timestamp
        # DEFAULT_TRADES_COLUMNS: 1 -> id
        has_overlap = self._ft_has.get("trades_pagination_overlap", True)
        # Skip last trade by default since its the key for the next call
        x = slice(None, -1) if has_overlap else slice(None)

        if not from_id or not self._valid_trade_pagination_id(pair, from_id):
            # Fetch first elements using timebased method to get an ID to paginate on
            # Depending on the Exchange, this can introduce a drift at the start of the interval
            # of up to an hour.
            # e.g. Binance returns the "last 1000" candles within a 1h time interval
            # - so we will miss the first trades.
            t, from_id = await self._async_get_trade_history_id_startup(pair, since=since)
            trades.extend(t[x])
        while True:
            try:
                t, from_id_next = await self._async_fetch_trades(
                    pair, params={self._trades_pagination_arg: from_id}
                )
                await asyncio.sleep(0)
                if t:
                    trades.extend(t[x])
                    if from_id == from_id_next or t[-1][0] > until:
                        logger.debug(
                            f"Stopping because from_id did not change. Reached {t[-1][0]} > {until}"
                        )
                        # Reached the end of the defined-download period - add last trade as well.
                        if has_overlap:
                            trades.extend(t[-1:])
                        break

                    from_id = from_id_next
                else:
                    logger.debug("Stopping as no more trades were returned.")
                    break
            except asyncio.CancelledError:
                logger.debug("Async operation Interrupted, breaking trades DL loop.")
                break

        return (pair, trades)

    async def _async_get_trade_history_time(
        self, pair: str, until: int, since: int
    ) -> tuple[str, list[list]]:
        """
        Asynchronously gets trade history using fetch_trades,
        when the exchange uses time-based iteration (check `self._trades_pagination`)
        :param pair: Pair to fetch trade data for
        :param since: Since as integer timestamp in milliseconds
        :param until: Until as integer timestamp in milliseconds
        returns tuple: (pair, trades-list)
        """

        trades: list[list] = []
        # DEFAULT_TRADES_COLUMNS: 0 -> timestamp
        # DEFAULT_TRADES_COLUMNS: 1 -> id
        while True:
            try:
                t, since_next = await self._async_fetch_trades(pair, since=since)
                if t:
                    # No more trades to download available at the exchange,
                    # So we repeatedly get the same trade over and over again.
                    if since == since_next and len(t) == 1:
                        logger.debug("Stopping because no more trades are available.")
                        break
                    since = since_next
                    trades.extend(t)
                    # Reached the end of the defined-download period
                    if until and since_next > until:
                        logger.debug(f"Stopping because until was reached. {since_next} > {until}")
                        break
                else:
                    logger.debug("Stopping as no more trades were returned.")
                    break
            except asyncio.CancelledError:
                logger.debug("Async operation Interrupted, breaking trades DL loop.")
                break

        return (pair, trades)

    async def _async_get_trade_history(
        self,
        pair: str,
        since: int,
        until: int | None = None,
        from_id: str | None = None,
    ) -> tuple[str, list[list]]:
        """
        Async wrapper handling downloading trades using either time or id based methods.
        """

        logger.debug(
            f"_async_get_trade_history(), pair: {pair}, "
            f"since: {since}, until: {until}, from_id: {from_id}"
        )

        if until is None:
            until = ccxt.Exchange.milliseconds()
            logger.debug(f"Exchange milliseconds: {until}")

        if self._trades_pagination == "time":
            return await self._async_get_trade_history_time(pair=pair, since=since, until=until)
        elif self._trades_pagination == "id":
            return await self._async_get_trade_history_id(
                pair=pair, since=since, until=until, from_id=from_id
            )
        else:
            raise OperationalException(
                f"Exchange {self.name} does use neither time, nor id based pagination"
            )

    def get_historic_trades(
        self,
        pair: str,
        since: int,
        until: int | None = None,
        from_id: str | None = None,
    ) -> tuple[str, list]:
        """
        Get trade history data using asyncio.
        Handles all async work and returns the list of candles.
        Async over one pair, assuming we get `self.ohlcv_candle_limit()` candles per call.
        :param pair: Pair to download
        :param since: Timestamp in milliseconds to get history from
        :param until: Timestamp in milliseconds. Defaults to current timestamp if not defined.
        :param from_id: Download data starting with ID (if id is known)
        :returns List of trade data
        """
        if not self.exchange_has("fetchTrades"):
            raise OperationalException("This exchange does not support downloading Trades.")

        with self._loop_lock:
            task = asyncio.ensure_future(
                self._async_get_trade_history(pair=pair, since=since, until=until, from_id=from_id)
            )

            for sig in [signal.SIGINT, signal.SIGTERM]:
                try:
                    self.loop.add_signal_handler(sig, task.cancel)
                except NotImplementedError:
                    # Not all platforms implement signals (e.g. windows)
                    pass
            return self.loop.run_until_complete(task)

    @retrier
    def _get_funding_fees_from_exchange(self, pair: str, since: datetime | int) -> float:
        """
        Returns the sum of all funding fees that were exchanged for a pair within a timeframe
        Dry-run handling happens as part of _calculate_funding_fees.
        :param pair: (e.g. ADA/USDT)
        :param since: The earliest time of consideration for calculating funding fees,
            in unix time or as a datetime
        """
        if not self.exchange_has("fetchFundingHistory"):
            raise OperationalException(
                f"fetch_funding_history() is not available using {self.name}"
            )

        if type(since) is datetime:
            since = dt_ts(since)

        try:
            funding_history = self._api.fetch_funding_history(symbol=pair, since=since)
            self._log_exchange_response(
                "funding_history", funding_history, add_info=f"pair: {pair}, since: {since}"
            )
            return sum(fee["amount"] for fee in funding_history)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not get funding fees due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier
    def get_leverage_tiers(self) -> dict[str, list[dict]]:
        try:
            return self._api.fetch_leverage_tiers()
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load leverage tiers due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    @retrier_async
    async def get_market_leverage_tiers(self, symbol: str) -> tuple[str, list[dict]]:
        """Leverage tiers per symbol"""
        try:
            tier = await self._api_async.fetch_market_leverage_tiers(symbol)
            return symbol, tier
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not load leverage tiers for {symbol}"
                f" due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def load_leverage_tiers(self) -> dict[str, list[dict]]:
        if self.trading_mode == TradingMode.FUTURES:
            if self.exchange_has("fetchLeverageTiers"):
                # Fetch all leverage tiers at once
                return self.get_leverage_tiers()
            elif self.exchange_has("fetchMarketLeverageTiers"):
                # Must fetch the leverage tiers for each market separately
                # * This is slow(~45s) on Okx, makes ~90 api calls to load all linear swap markets
                markets = self.markets

                symbols = [
                    symbol
                    for symbol, market in markets.items()
                    if (
                        self.market_is_future(market)
                        and market["quote"] == self._config["stake_currency"]
                    )
                ]

                tiers: dict[str, list[dict]] = {}

                tiers_cached = self.load_cached_leverage_tiers(self._config["stake_currency"])
                if tiers_cached:
                    tiers = tiers_cached

                coros = [
                    self.get_market_leverage_tiers(symbol)
                    for symbol in sorted(symbols)
                    if symbol not in tiers
                ]

                # Be verbose here, as this delays startup by ~1 minute.
                if coros:
                    logger.info(
                        f"Initializing leverage_tiers for {len(symbols)} markets. "
                        "This will take about a minute."
                    )
                else:
                    logger.info("Using cached leverage_tiers.")

                async def gather_results(input_coro):
                    return await asyncio.gather(*input_coro, return_exceptions=True)

                for input_coro in chunks(coros, 100):
                    with self._loop_lock:
                        results = self.loop.run_until_complete(gather_results(input_coro))

                    for res in results:
                        if isinstance(res, Exception):
                            logger.warning(f"Leverage tier exception: {repr(res)}")
                            continue
                        symbol, tier = res
                        tiers[symbol] = tier
                if len(coros) > 0:
                    self.cache_leverage_tiers(tiers, self._config["stake_currency"])
                logger.info(f"Done initializing {len(symbols)} markets.")

                return tiers
        return {}

    def cache_leverage_tiers(self, tiers: dict[str, list[dict]], stake_currency: str) -> None:
        filename = self._config["datadir"] / "futures" / f"leverage_tiers_{stake_currency}.json"
        if not filename.parent.is_dir():
            filename.parent.mkdir(parents=True)
        data = {
            "updated": datetime.now(timezone.utc),
            "data": tiers,
        }
        file_dump_json(filename, data)

    def load_cached_leverage_tiers(
        self, stake_currency: str, cache_time: timedelta | None = None
    ) -> dict[str, list[dict]] | None:
        """
        Load cached leverage tiers from disk
        :param cache_time: The maximum age of the cache before it is considered outdated
        """
        if not cache_time:
            # Default to 4 weeks
            cache_time = timedelta(weeks=4)
        filename = self._config["datadir"] / "futures" / f"leverage_tiers_{stake_currency}.json"
        if filename.is_file():
            try:
                tiers = file_load_json(filename)
                updated = tiers.get("updated")
                if updated:
                    updated_dt = parser.parse(updated)
                    if updated_dt < datetime.now(timezone.utc) - cache_time:
                        logger.info("Cached leverage tiers are outdated. Will update.")
                        return None
                return tiers.get("data")
            except Exception:
                logger.exception("Error loading cached leverage tiers. Refreshing.")
        return None

    def fill_leverage_tiers(self) -> None:
        """
        Assigns property _leverage_tiers to a dictionary of information about the leverage
        allowed on each pair
        """
        leverage_tiers = self.load_leverage_tiers()
        for pair, tiers in leverage_tiers.items():
            pair_tiers = []
            for tier in tiers:
                pair_tiers.append(self.parse_leverage_tier(tier))
            self._leverage_tiers[pair] = pair_tiers

    def parse_leverage_tier(self, tier) -> dict:
        info = tier.get("info", {})
        return {
            "minNotional": tier["minNotional"],
            "maxNotional": tier["maxNotional"],
            "maintenanceMarginRate": tier["maintenanceMarginRate"],
            "maxLeverage": tier["maxLeverage"],
            "maintAmt": float(info["cum"]) if "cum" in info else None,
        }

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        """
        Returns the maximum leverage that a pair can be traded at
        :param pair: The base/quote currency pair being traded
        :stake_amount: The total value of the traders margin_mode in quote currency
        """

        if self.trading_mode == TradingMode.SPOT:
            return 1.0

        if self.trading_mode == TradingMode.FUTURES:
            # Checks and edge cases
            if stake_amount is None:
                raise OperationalException(
                    f"{self.name}.get_max_leverage requires argument stake_amount"
                )

            if pair not in self._leverage_tiers:
                # Maybe raise exception because it can't be traded on futures?
                return 1.0

            pair_tiers = self._leverage_tiers[pair]

            if stake_amount == 0:
                return pair_tiers[0]["maxLeverage"]  # Max lev for lowest amount

            # Find the appropriate tier based on stake_amount
            prior_max_lev = None
            for tier in pair_tiers:
                min_stake = tier["minNotional"] / (prior_max_lev or tier["maxLeverage"])
                max_stake = tier["maxNotional"] / tier["maxLeverage"]
                prior_max_lev = tier["maxLeverage"]
                # Adjust notional by leverage to do a proper comparison
                if min_stake <= stake_amount <= max_stake:
                    return tier["maxLeverage"]

            #     else:  # if on the last tier
            if stake_amount > max_stake:
                # If stake is > than max tradeable amount
                raise InvalidOrderException(f"Amount {stake_amount} too high for {pair}")

            raise OperationalException(
                "Looped through all tiers without finding a max leverage. Should never be reached"
            )

        elif self.trading_mode == TradingMode.MARGIN:  # Search markets.limits for max lev
            market = self.markets[pair]
            if market["limits"]["leverage"]["max"] is not None:
                return market["limits"]["leverage"]["max"]
            else:
                return 1.0  # Default if max leverage cannot be found
        else:
            return 1.0

    def _get_max_notional_from_tiers(self, pair: str, leverage: float) -> float | None:
        """
        get max_notional from leverage_tiers
        :param pair: The base/quote currency pair being traded
        :param leverage: The leverage to be used
        :return: The maximum notional value for the given leverage or None if not found
        """
        if self.trading_mode != TradingMode.FUTURES:
            return None
        if pair not in self._leverage_tiers:
            return None
        pair_tiers = self._leverage_tiers[pair]
        for tier in reversed(pair_tiers):
            if leverage <= tier["maxLeverage"]:
                return tier["maxNotional"]
        return None

    @retrier
    def _set_leverage(
        self,
        leverage: float,
        pair: str | None = None,
        accept_fail: bool = False,
    ):
        """
        Set's the leverage before making a trade, in order to not
        have the same leverage on every trade
        """
        if self._config["dry_run"] or not self.exchange_has("setLeverage"):
            # Some exchanges only support one margin_mode type
            return
        if self._ft_has.get("floor_leverage", False) is True:
            # Rounding for binance ...
            leverage = floor(leverage)
        try:
            res = self._api.set_leverage(symbol=pair, leverage=leverage)
            self._log_exchange_response("set_leverage", res)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected, ccxt.InsufficientFunds) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set leverage due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def get_interest_rate(self) -> float:
        """
        Retrieve interest rate - necessary for Margin trading.
        Should not call the exchange directly when used from backtesting.
        """
        return 0.0

    def funding_fee_cutoff(self, open_date: datetime) -> bool:
        """
        Funding fees are only charged at full hours (usually every 4-8h).
        Therefore a trade opening at 10:00:01 will not be charged a funding fee until the next hour.
        :param open_date: The open date for a trade
        :return: True if the date falls on a full hour, False otherwise
        """
        return open_date.minute == 0 and open_date.second == 0

    @retrier
    def set_margin_mode(
        self,
        pair: str,
        margin_mode: MarginMode,
        accept_fail: bool = False,
        params: dict | None = None,
    ):
        """
        Set's the margin mode on the exchange to cross or isolated for a specific pair
        :param pair: base/quote currency pair (e.g. "ADA/USDT")
        """
        if self._config["dry_run"] or not self.exchange_has("setMarginMode"):
            # Some exchanges only support one margin_mode type
            return

        if params is None:
            params = {}
        try:
            res = self._api.set_margin_mode(margin_mode.value, pair, params)
            self._log_exchange_response("set_margin_mode", res)
        except ccxt.DDoSProtection as e:
            raise DDosProtection(e) from e
        except (ccxt.BadRequest, ccxt.OperationRejected) as e:
            if not accept_fail:
                raise TemporaryError(
                    f"Could not set margin mode due to {e.__class__.__name__}. Message: {e}"
                ) from e
        except (ccxt.OperationFailed, ccxt.ExchangeError) as e:
            raise TemporaryError(
                f"Could not set margin mode due to {e.__class__.__name__}. Message: {e}"
            ) from e
        except ccxt.BaseError as e:
            raise OperationalException(e) from e

    def _fetch_and_calculate_funding_fees(
        self,
        pair: str,
        amount: float,
        is_short: bool,
        open_date: datetime,
        close_date: datetime | None = None,
    ) -> float:
        """
        Fetches and calculates the sum of all funding fees that occurred for a pair
        during a futures trade.
        Only used during dry-run or if the exchange does not provide a funding_rates endpoint.
        :param pair: The quote/base pair of the trade
        :param amount: The quantity of the trade
        :param is_short: trade direction
        :param open_date: The date and time that the trade started
        :param close_date: The date and time that the trade ended
        """

        if self.funding_fee_cutoff(open_date):
            # Shift back to 1h candle to avoid missing funding fees
            # Only really relevant for trades very close to the full hour
            open_date = timeframe_to_prev_date("1h", open_date)
        timeframe = self._ft_has["mark_ohlcv_timeframe"]
        timeframe_ff = self._ft_has["funding_fee_timeframe"]
        mark_price_type = CandleType.from_string(self._ft_has["mark_ohlcv_price"])

        if not close_date:
            close_date = datetime.now(timezone.utc)
        since_ms = dt_ts(timeframe_to_prev_date(timeframe, open_date))

        mark_comb: PairWithTimeframe = (pair, timeframe, mark_price_type)
        funding_comb: PairWithTimeframe = (pair, timeframe_ff, CandleType.FUNDING_RATE)

        candle_histories = self.refresh_latest_ohlcv(
            [mark_comb, funding_comb],
            since_ms=since_ms,
            cache=False,
            drop_incomplete=False,
        )
        try:
            # we can't assume we always get histories - for example during exchange downtimes
            funding_rates = candle_histories[funding_comb]
            mark_rates = candle_histories[mark_comb]
        except KeyError:
            raise ExchangeError("Could not find funding rates.") from None

        funding_mark_rates = self.combine_funding_and_mark(funding_rates, mark_rates)

        return self.calculate_funding_fees(
            funding_mark_rates,
            amount=amount,
            is_short=is_short,
            open_date=open_date,
            close_date=close_date,
        )

   