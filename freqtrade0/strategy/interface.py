"""
IStrategy interface
This module defines the interface to apply for strategies
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from math import isinf, isnan
import re
from typing import Literal

from pandas import DataFrame
from pydantic import ValidationError

from freqtrade.constants import CUSTOM_TAG_MAX_LENGTH, Config, IntOrInf, ListPairsWithTimeframes
from freqtrade.data.converter import populate_dataframe_with_trades
from freqtrade.data.converter.converter import reduce_dataframe_footprint
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import (
    CandleType,
    ExitCheckTuple,
    ExitType,
    MarketDirection,
    RunMode,
    SignalDirection,
    SignalTagType,
    SignalType,
    TradingMode,
)
from freqtrade.exceptions import OperationalException, StrategyError
from freqtrade.exchange import timeframe_to_minutes, timeframe_to_next_date, timeframe_to_seconds
from freqtrade.exchange.exchange_types import OrderBook
from freqtrade.ft_types import AnnotationType
from freqtrade.misc import remove_entry_exit_signals
from freqtrade.persistence import Order, PairLocks, Trade
from freqtrade.strategy.hyper import HyperStrategyMixin
from freqtrade.strategy.informative_decorator import (
    InformativeData,
    PopulateIndicators,
    _create_and_merge_informative_pair,
    _format_pair_name,
)
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.util import dt_now
from freqtrade.wallets import Wallets
import freqtrade.strategy

logger = logging.getLogger(__name__)


class IStrategy(freqtrade.strategy.IStrategy):
    """
    Interface for freqtrade strategies
    Defines the mandatory structure must follow any custom strategies

    Attributes you can use:
        minimal_roi -> Dict: Minimal ROI designed for the strategy
        stoploss -> float: optimal stoploss designed for the strategy
        timeframe -> str: value of the timeframe to use with the strategy
    """

   


    loop_enable: bool = True
    def __init__(self, config: Config) -> None:
        self.config = config
        # Dict to determine if analysis is necessary
        self._last_candle_seen_per_pair: dict[str, datetime] = {}
        super().__init__(config)

        # Gather informative pairs from @informative-decorated methods.
        self._ft_informative: list[tuple[InformativeData, PopulateIndicators]] = []
        for attr_name in dir(self.__class__):
            cls_method = getattr(self.__class__, attr_name)
            if not callable(cls_method):
                continue
            informative_data_list = getattr(cls_method, "_ft_informative", None)
            if not isinstance(informative_data_list, list):
                # Type check is required because mocker would return a mock object that evaluates to
                # True, confusing this code.
                continue
            strategy_timeframe_minutes = timeframe_to_minutes(self.timeframe)
            for informative_data in informative_data_list:
                if timeframe_to_minutes(informative_data.timeframe) < strategy_timeframe_minutes:
                    raise OperationalException(
                        "Informative timeframe must be equal or higher than strategy timeframe!"
                    )
                if not informative_data.candle_type:
                    informative_data.candle_type = config["candle_type_def"]
                self._ft_informative.append((informative_data, cls_method))

  
    
    
    @abstractmethod
    def loop_entry(self,pair:str,timestamp:datetime) ->tuple|None:
        
        '''
        return tuple[Literal["long","short"]|None,float|None,float|None,str|None]|None:
        return a tuple of (side,amount,price,signal_name)|None for the entry signal
        *side: "long" or "short",If the side is none, no action will be performed.
        stack: float | None
        price: float | None
        signal_name: str
        '''
        pass
    
    def _loop_entry(
        self,pair:str,timestamp:datetime,
        **kwargs
    ) -> tuple[Literal["long","short"]|None,float|None,float|None,str|None]|None:
        """
        wrapper around adjust_trade_position to handle the return value
        """
        resp = strategy_safe_wrapper(
            self.loop_entry, default_retval=(None, ""), supress_error=True
        )(
           pair = pair,timestamp = timestamp,
            **kwargs,
        )
        
        order_tag = ""
        if resp is None:
            return resp
        elif isinstance(resp, tuple):
            match len(resp):
                case 4:
                    return resp
                case 3:
                    side, stake_amount,price_or_tag = resp
                    if isinstance(price_or_tag, str):
                        price ,order_tag= None,price_or_tag
                    else:
                        price = price_or_tag
                case 2:
                    side, stake_amount = resp
                    price = None
                case _:
                    return None
       
        return side,stake_amount,price,order_tag
  

    