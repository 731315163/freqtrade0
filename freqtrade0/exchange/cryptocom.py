"""Crypto.com exchange subclass"""

import logging

from freqtrade0.exchange import Exchange
from freqtrade0.exchange.exchange_types import FtHas


logger = logging.getLogger(__name__)


class Cryptocom(Exchange):
    """Crypto.com exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has: FtHas = {
        "ohlcv_candle_limit": 300,
    }
