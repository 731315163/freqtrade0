"""
IStrategy interface
This module defines the interface to apply for strategies
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from math import isinf, isnan
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

   


    loop_enable: bool = False
    

  
    
    
    @abstractmethod
    def loop_entry(self,pair:str,timestamp:int) -> tuple[Literal["long","short"]|None,str|None,float|None,float|None]|None:
        '''
        return a tuple of (side,signal_name,amount,price)|None for the entry signal
        *side: "long" or "short",If the side is none, no action will be performed.
        signal_name: str
        amount: float | None
        price: float | None

        '''
        pass
    
   
  

    