"""
IStrategy interface
This module defines the interface to apply for strategies
"""

import logging
from abc import abstractmethod
from datetime import datetime
from typing import Literal


from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.enums import (
    CandleType,
)
from freqtrade.exceptions import OperationalException
from freqtrade.exchange import timeframe_to_minutes
from freqtrade.strategy.informative_decorator import (
    InformativeData,
    PopulateIndicators,
    _format_pair_name,
)
from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
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

   

    can_hedge_mode: bool = False
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
    def informative_trade_pairs(self) -> ListPairsWithTimeframes:
        """
        Define additional, informative pair/interval combinations to be cached from the exchange.
        These pair/interval combinations are non-tradable, unless they are part
        of the whitelist as well.
        For more information, please consult the documentation
        :return: List of tuples in the format (pair, interval)
            Sample: return [("ETH/USDT", "5m"),
                            ("BTC/USDT", "15m"),
                            ]
        """
        return []
    def gather_informative_trade_pairs(self) -> ListPairsWithTimeframes:
        """
        Internal method which gathers all informative pairs (user or automatically defined).
        """
        informative_pairs = self.informative_trade_pairs()
        # Compatibility code for 2 tuple informative pairs
        informative_pairs = [
            (
                p[0],
                p[1],
                (
                    CandleType.from_string(p[2])
                    if len(p) > 2 and p[2] != ""
                    else self.config.get("candle_type_def", CandleType.SPOT)
                ),
            )
            for p in informative_pairs
        ]
        for inf_data, _ in self._ft_informative:
            # Get default candle type if not provided explicitly.
            candle_type = (
                inf_data.candle_type
                if inf_data.candle_type
                else self.config.get("candle_type_def", CandleType.SPOT)
            )
            if inf_data.asset:
                if any(s in inf_data.asset for s in ("{BASE}", "{base}")):
                    for pair in self.dp.current_whitelist():
                        pair_tf = (
                            _format_pair_name(self.config, inf_data.asset, self.dp.market(pair)),
                            inf_data.timeframe,
                            candle_type,
                        )
                        informative_pairs.append(pair_tf)

                else:
                    pair_tf = (
                        _format_pair_name(self.config, inf_data.asset),
                        inf_data.timeframe,
                        candle_type,
                    )
                    informative_pairs.append(pair_tf)
            else:
                for pair in self.dp.current_whitelist():
                    informative_pairs.append((pair, inf_data.timeframe, candle_type))
        informative_pairs.extend(self.__informative_pairs_freqai())
        return list(set(informative_pairs))
    
    
    
    def loop_entry(self,pair:str,timestamp:datetime) ->None| tuple[Literal["long","short"],float|None]|tuple[Literal["long","short"],float|None,float|None|str]|tuple[Literal["long","short"],float|None,float|None,str]:
        
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
        match resp:
            case (side, stake_amount, price, order_tag):
                return side, stake_amount, price, order_tag
            case (side, stake_amount, price_or_tag):
                if isinstance(price_or_tag, str):
                    return side, stake_amount, None, price_or_tag
                else:
                    return side, stake_amount, price_or_tag, ""
            case (side, stake_amount):
                return side, stake_amount, None, ""
            case _:
                return None
       
        return side,stake_amount,price,order_tag
  

    