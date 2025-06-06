# pragma pylint: disable=missing-docstring,C0103,protected-access

import logging
import time
from copy import deepcopy
from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

import pandas as pd
import pytest
import time_machine

from freqtrade.constants import AVAILABLE_PAIRLISTS
from freqtrade.data.dataprovider import DataProvider
from freqtrade.enums import CandleType, RunMode
from freqtrade.exceptions import OperationalException
from freqtrade.persistence import LocalTrade, Trade
from freqtrade.plugins.pairlist.pairlist_helpers import dynamic_expand_pairlist, expand_pairlist
from freqtrade.plugins.pairlistmanager import PairListManager
from freqtrade.resolvers import PairListResolver
from freqtrade.util.datetime_helpers import dt_now
from tests.conftest import (
    EXMS,
    create_mock_trades_usdt,
    generate_test_data,
    get_patched_exchange,
    get_patched_freqtradebot,
    log_has,
    log_has_re,
    num_log_has,
)


# Exclude RemotePairList from tests.
# It has a mandatory parameter, and requires special handling, which happens in test_remotepairlist.
TESTABLE_PAIRLISTS = [p for p in AVAILABLE_PAIRLISTS if p not in ["RemotePairList"]]


@pytest.fixture(scope="function")
def whitelist_conf(default_conf):
    default_conf["runmode"] = "dry_run"
    default_conf["stake_currency"] = "BTC"
    default_conf["exchange"]["pair_whitelist"] = [
        "ETH/BTC",
        "TKN/BTC",
        "TRST/BTC",
        "SWT/BTC",
        "BCC/BTC",
        "HOT/BTC",
    ]
    default_conf["exchange"]["pair_blacklist"] = ["BLK/BTC"]
    default_conf["pairlists"] = [
        {
            "method": "VolumePairList",
            "number_assets": 5,
            "sort_key": "quoteVolume",
        },
    ]
    default_conf.update(
        {
            "external_message_consumer": {
                "enabled": True,
                "producers": [],
            }
        }
    )
    return default_conf


@pytest.fixture(scope="function")
def whitelist_conf_2(default_conf):
    default_conf["runmode"] = "dry_run"
    default_conf["stake_currency"] = "BTC"
    default_conf["exchange"]["pair_whitelist"] = [
        "ETH/BTC",
        "TKN/BTC",
        "BLK/BTC",
        "LTC/BTC",
        "BTT/BTC",
        "HOT/BTC",
        "FUEL/BTC",
        "XRP/BTC",
    ]
    default_conf["exchange"]["pair_blacklist"] = ["BLK/BTC"]
    default_conf["pairlists"] = [
        # {   "method": "StaticPairList"},
        {
            "method": "VolumePairList",
            "number_assets": 5,
            "sort_key": "quoteVolume",
            "refresh_period": 0,
        },
    ]
    return default_conf


@pytest.fixture(scope="function")
def whitelist_conf_agefilter(default_conf):
    default_conf["runmode"] = "dry_run"
    default_conf["stake_currency"] = "BTC"
    default_conf["exchange"]["pair_whitelist"] = [
        "ETH/BTC",
        "TKN/BTC",
        "BLK/BTC",
        "LTC/BTC",
        "BTT/BTC",
        "HOT/BTC",
        "FUEL/BTC",
        "XRP/BTC",
    ]
    default_conf["exchange"]["pair_blacklist"] = ["BLK/BTC"]
    default_conf["pairlists"] = [
        {
            "method": "VolumePairList",
            "number_assets": 5,
            "sort_key": "quoteVolume",
            "refresh_period": -1,
        },
        {"method": "AgeFilter", "min_days_listed": 2, "max_days_listed": 100},
    ]
    return default_conf


@pytest.fixture(scope="function")
def static_pl_conf(whitelist_conf):
    whitelist_conf["pairlists"] = [
        {
            "method": "StaticPairList",
        },
    ]
    return whitelist_conf


def test_log_cached(mocker, static_pl_conf, markets, tickers):
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )
    freqtrade = get_patched_freqtradebot(mocker, static_pl_conf)
    logmock = MagicMock()
    # Assign starting whitelist
    pl = freqtrade.pairlists._pairlist_handlers[0]
    pl.log_once("Hello world", logmock)
    assert logmock.call_count == 1
    pl.log_once("Hello world", logmock)
    assert logmock.call_count == 1
    assert pl._log_cache.currsize == 1
    assert ("Hello world",) in pl._log_cache._Cache__data

    pl.log_once("Hello world2", logmock)
    assert logmock.call_count == 2
    assert pl._log_cache.currsize == 2


def test_load_pairlist_noexist(mocker, markets, default_conf):
    freqtrade = get_patched_freqtradebot(mocker, default_conf)
    mocker.patch(f"{EXMS}.markets", PropertyMock(return_value=markets))
    plm = PairListManager(freqtrade.exchange, default_conf, MagicMock())
    with pytest.raises(
        OperationalException,
        match=r"Impossible to load Pairlist 'NonexistingPairList'. "
        r"This class does not exist or contains Python code errors.",
    ):
        PairListResolver.load_pairlist(
            "NonexistingPairList", freqtrade.exchange, plm, default_conf, {}, 1
        )


def test_load_pairlist_verify_multi(mocker, markets_static, default_conf):
    freqtrade = get_patched_freqtradebot(mocker, default_conf)
    mocker.patch(f"{EXMS}.markets", PropertyMock(return_value=markets_static))
    plm = PairListManager(freqtrade.exchange, default_conf, MagicMock())
    # Call different versions one after the other, should always consider what was passed in
    # and have no side-effects (therefore the same check multiple times)
    assert plm.verify_whitelist(["ETH/BTC", "XRP/BTC"], print) == ["ETH/BTC", "XRP/BTC"]
    assert plm.verify_whitelist(["ETH/BTC", "XRP/BTC", "BUUU/BTC"], print) == ["ETH/BTC", "XRP/BTC"]
    assert plm.verify_whitelist(["XRP/BTC", "BUUU/BTC"], print) == ["XRP/BTC"]
    assert plm.verify_whitelist(["ETH/BTC", "XRP/BTC"], print) == ["ETH/BTC", "XRP/BTC"]
    assert plm.verify_whitelist(["ETH/USDT", "XRP/USDT"], print) == ["ETH/USDT"]
    assert plm.verify_whitelist(["ETH/BTC", "XRP/BTC"], print) == ["ETH/BTC", "XRP/BTC"]


def test_refresh_market_pair_not_in_whitelist(mocker, markets, static_pl_conf):
    freqtrade = get_patched_freqtradebot(mocker, static_pl_conf)

    mocker.patch(f"{EXMS}.markets", PropertyMock(return_value=markets))
    freqtrade.pairlists.refresh_pairlist()
    # List ordered by BaseVolume
    whitelist = ["ETH/BTC", "TKN/BTC"]
    # Ensure all except those in whitelist are removed
    assert set(whitelist) == set(freqtrade.pairlists.whitelist)
    # Ensure config dict hasn't been changed
    assert (
        static_pl_conf["exchange"]["pair_whitelist"]
        == freqtrade.config["exchange"]["pair_whitelist"]
    )


def test_refresh_static_pairlist(mocker, markets, static_pl_conf):
    freqtrade = get_patched_freqtradebot(mocker, static_pl_conf)
    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
        markets=PropertyMock(return_value=markets),
    )
    freqtrade.pairlists.refresh_pairlist()
    # List ordered by BaseVolume
    whitelist = ["ETH/BTC", "TKN/BTC"]
    # Ensure all except those in whitelist are removed
    assert set(whitelist) == set(freqtrade.pairlists.whitelist)
    assert static_pl_conf["exchange"]["pair_blacklist"] == freqtrade.pairlists.blacklist


@pytest.mark.parametrize(
    "pairs,expected",
    [
        (
            ["NOEXIST/BTC", r"\+WHAT/BTC"],
            ["ETH/BTC", "TKN/BTC", "TRST/BTC", "NOEXIST/BTC", "SWT/BTC", "BCC/BTC", "HOT/BTC"],
        ),
        (
            ["NOEXIST/BTC", r"*/BTC"],  # This is an invalid regex
            [],
        ),
    ],
)
def test_refresh_static_pairlist_noexist(mocker, markets, static_pl_conf, pairs, expected, caplog):
    static_pl_conf["pairlists"][0]["allow_inactive"] = True
    static_pl_conf["exchange"]["pair_whitelist"] += pairs
    freqtrade = get_patched_freqtradebot(mocker, static_pl_conf)
    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
        markets=PropertyMock(return_value=markets),
    )
    freqtrade.pairlists.refresh_pairlist()

    # Ensure all except those in whitelist are removed
    assert set(expected) == set(freqtrade.pairlists.whitelist)
    assert static_pl_conf["exchange"]["pair_blacklist"] == freqtrade.pairlists.blacklist
    if not expected:
        assert log_has_re(r"Pair whitelist contains an invalid Wildcard: Wildcard error.*", caplog)


def test_invalid_blacklist(mocker, markets, static_pl_conf, caplog):
    static_pl_conf["exchange"]["pair_blacklist"] = ["*/BTC"]
    freqtrade = get_patched_freqtradebot(mocker, static_pl_conf)
    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
        markets=PropertyMock(return_value=markets),
    )
    freqtrade.pairlists.refresh_pairlist()
    whitelist = []
    # Ensure all except those in whitelist are removed
    assert set(whitelist) == set(freqtrade.pairlists.whitelist)
    assert static_pl_conf["exchange"]["pair_blacklist"] == freqtrade.pairlists.blacklist
    log_has_re(r"Pair blacklist contains an invalid Wildcard.*", caplog)


def test_remove_logs_for_pairs_already_in_blacklist(mocker, markets, static_pl_conf, caplog):
    logger = logging.getLogger(__name__)
    freqtrade = get_patched_freqtradebot(mocker, static_pl_conf)
    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
        markets=PropertyMock(return_value=markets),
    )
    freqtrade.pairlists.refresh_pairlist()
    whitelist = ["ETH/BTC", "TKN/BTC"]
    caplog.clear()
    caplog.set_level(logging.INFO)

    # Ensure all except those in whitelist are removed.
    assert set(whitelist) == set(freqtrade.pairlists.whitelist)
    assert static_pl_conf["exchange"]["pair_blacklist"] == freqtrade.pairlists.blacklist
    # Ensure that log message wasn't generated.
    assert not log_has("Pair BLK/BTC in your blacklist. Removing it from whitelist...", caplog)

    for _ in range(3):
        new_whitelist = freqtrade.pairlists.verify_blacklist(
            [*whitelist, "BLK/BTC"], logger.warning
        )
        # Ensure that the pair is removed from the white list, and properly logged.
        assert set(whitelist) == set(new_whitelist)
    assert num_log_has("Pair BLK/BTC in your blacklist. Removing it from whitelist...", caplog) == 1


def test_refresh_pairlist_dynamic(mocker, shitcoinmarkets, tickers, whitelist_conf):
    mocker.patch.multiple(
        EXMS,
        get_tickers=tickers,
        exchange_has=MagicMock(return_value=True),
    )
    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    # Remock markets with shitcoinmarkets since get_patched_freqtradebot uses the markets fixture
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=shitcoinmarkets),
    )
    # argument: use the whitelist dynamically by exchange-volume
    whitelist = ["ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC", "HOT/BTC"]
    freqtrade.pairlists.refresh_pairlist()
    assert whitelist == freqtrade.pairlists.whitelist

    whitelist_conf["pairlists"] = [{"method": "VolumePairList"}]
    with pytest.raises(
        OperationalException,
        match=r"`number_assets` not specified. Please check your configuration "
        r'for "pairlist.config.number_assets"',
    ):
        PairListManager(freqtrade.exchange, whitelist_conf, MagicMock())


def test_refresh_pairlist_dynamic_2(mocker, shitcoinmarkets, tickers, whitelist_conf_2):
    tickers_dict = tickers()

    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
    )
    # Remove caching of ticker data to emulate changing volume by the time of second call
    mocker.patch.multiple(
        "freqtrade.plugins.pairlistmanager.PairListManager",
        _get_cached_tickers=MagicMock(return_value=tickers_dict),
    )
    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf_2)
    # Remock markets with shitcoinmarkets since get_patched_freqtradebot uses the markets fixture
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=shitcoinmarkets),
    )

    whitelist = ["ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC", "HOT/BTC"]
    freqtrade.pairlists.refresh_pairlist()
    assert whitelist == freqtrade.pairlists.whitelist

    # Delay to allow 0 TTL cache to expire...
    time.sleep(1)
    whitelist = ["FUEL/BTC", "ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC"]
    tickers_dict["FUEL/BTC"]["quoteVolume"] = 10000.0
    freqtrade.pairlists.refresh_pairlist()
    assert whitelist == freqtrade.pairlists.whitelist


def test_VolumePairList_refresh_empty(mocker, markets_empty, whitelist_conf):
    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
    )
    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    mocker.patch(f"{EXMS}.markets", PropertyMock(return_value=markets_empty))

    # argument: use the whitelist dynamically by exchange-volume
    whitelist = []
    whitelist_conf["exchange"]["pair_whitelist"] = []
    freqtrade.pairlists.refresh_pairlist()
    pairslist = whitelist_conf["exchange"]["pair_whitelist"]

    assert set(whitelist) == set(pairslist)


@pytest.mark.parametrize(
    "pairlists,base_currency,whitelist_result",
    [
        # VolumePairList only
        (
            [{"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"}],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC", "HOT/BTC"],
        ),
        (
            [{"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"}],
            "USDT",
            ["ETH/USDT", "NANO/USDT", "ADAHALF/USDT", "ADADOUBLE/USDT"],
        ),
        # No pair for ETH, VolumePairList
        ([{"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"}], "ETH", []),
        # No pair for ETH, StaticPairList
        ([{"method": "StaticPairList"}], "ETH", []),
        # No pair for ETH, all handlers
        (
            [
                {"method": "StaticPairList"},
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "AgeFilter", "min_days_listed": 2, "max_days_listed": None},
                {"method": "PrecisionFilter"},
                {"method": "PriceFilter", "low_price_ratio": 0.03},
                {"method": "SpreadFilter", "max_spread_ratio": 0.005},
                {"method": "ShuffleFilter"},
                {"method": "PerformanceFilter"},
            ],
            "ETH",
            [],
        ),
        # AgeFilter and VolumePairList (require 2 days only, all should pass age test)
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "AgeFilter", "min_days_listed": 2, "max_days_listed": 100},
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC", "HOT/BTC"],
        ),
        # AgeFilter and VolumePairList (require 10 days, all should fail age test)
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "AgeFilter", "min_days_listed": 10, "max_days_listed": None},
            ],
            "BTC",
            [],
        ),
        # AgeFilter and VolumePairList (all pair listed > 2, all should fail age test)
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "AgeFilter", "min_days_listed": 1, "max_days_listed": 2},
            ],
            "BTC",
            [],
        ),
        # AgeFilter and VolumePairList LTC/BTC has 6 candles - removes all
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "AgeFilter", "min_days_listed": 4, "max_days_listed": 5},
            ],
            "BTC",
            [],
        ),
        # AgeFilter and VolumePairList LTC/BTC has 6 candles - passes
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "AgeFilter", "min_days_listed": 4, "max_days_listed": 10},
            ],
            "BTC",
            ["LTC/BTC"],
        ),
        # Precisionfilter and quote volume
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "PrecisionFilter"},
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC"],
        ),
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "PrecisionFilter"},
            ],
            "USDT",
            ["ETH/USDT", "NANO/USDT"],
        ),
        # PriceFilter and VolumePairList
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "PriceFilter", "low_price_ratio": 0.03},
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "LTC/BTC", "XRP/BTC"],
        ),
        # PriceFilter and VolumePairList
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "PriceFilter", "low_price_ratio": 0.03},
            ],
            "USDT",
            ["ETH/USDT", "NANO/USDT"],
        ),
        # Hot is removed by precision_filter, Fuel by low_price_ratio, Ripple by min_price.
        (
            [
                {"method": "VolumePairList", "number_assets": 6, "sort_key": "quoteVolume"},
                {"method": "PrecisionFilter"},
                {"method": "PriceFilter", "low_price_ratio": 0.02, "min_price": 0.01},
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
        ),
        # Hot is removed by precision_filter, Fuel by low_price_ratio, Ethereum by max_price.
        (
            [
                {"method": "VolumePairList", "number_assets": 6, "sort_key": "quoteVolume"},
                {"method": "PrecisionFilter"},
                {"method": "PriceFilter", "low_price_ratio": 0.02, "max_price": 0.05},
            ],
            "BTC",
            ["TKN/BTC", "LTC/BTC", "XRP/BTC"],
        ),
        # HOT and XRP are removed because below 1250 quoteVolume
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "min_value": 1250,
                }
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
        ),
        # HOT, XRP and FUEL whitelisted because they are below 1300 quoteVolume.
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "max_value": 1300,
                }
            ],
            "BTC",
            ["XRP/BTC", "HOT/BTC", "FUEL/BTC"],
        ),
        # HOT, XRP whitelisted because they are between 100 and 1300 quoteVolume.
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "min_value": 100,
                    "max_value": 1300,
                }
            ],
            "BTC",
            ["XRP/BTC", "HOT/BTC"],
        ),
        # StaticPairlist only
        ([{"method": "StaticPairList"}], "BTC", ["ETH/BTC", "TKN/BTC", "HOT/BTC"]),
        # Static Pairlist before VolumePairList - sorting changes
        # SpreadFilter
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "SpreadFilter", "max_spread_ratio": 0.005},
            ],
            "USDT",
            ["ETH/USDT"],
        ),
        # ShuffleFilter
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "ShuffleFilter", "seed": 77},
            ],
            "USDT",
            ["ADADOUBLE/USDT", "ETH/USDT", "NANO/USDT", "ADAHALF/USDT"],
        ),
        # ShuffleFilter, other seed
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "ShuffleFilter", "seed": 42},
            ],
            "USDT",
            ["ADAHALF/USDT", "NANO/USDT", "ADADOUBLE/USDT", "ETH/USDT"],
        ),
        # ShuffleFilter, no seed
        (
            [
                {"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume"},
                {"method": "ShuffleFilter"},
            ],
            "USDT",
            4,
        ),  # whitelist_result is integer -- check only length of randomized pairlist
        # AgeFilter only
        (
            [{"method": "AgeFilter", "min_days_listed": 2}],
            "BTC",
            "filter_at_the_beginning",
        ),  # OperationalException expected
        # PrecisionFilter after StaticPairList
        (
            [{"method": "StaticPairList"}, {"method": "PrecisionFilter"}],
            "BTC",
            ["ETH/BTC", "TKN/BTC"],
        ),
        # PrecisionFilter only
        (
            [{"method": "PrecisionFilter"}],
            "BTC",
            "filter_at_the_beginning",
        ),  # OperationalException expected
        # PriceFilter after StaticPairList
        (
            [
                {"method": "StaticPairList"},
                {
                    "method": "PriceFilter",
                    "low_price_ratio": 0.02,
                    "min_price": 0.000001,
                    "max_price": 0.1,
                },
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC"],
        ),
        # PriceFilter only
        (
            [{"method": "PriceFilter", "low_price_ratio": 0.02}],
            "BTC",
            "filter_at_the_beginning",
        ),  # OperationalException expected
        # ShuffleFilter after StaticPairList
        (
            [{"method": "StaticPairList"}, {"method": "ShuffleFilter", "seed": 42}],
            "BTC",
            ["TKN/BTC", "ETH/BTC", "HOT/BTC"],
        ),
        # ShuffleFilter only
        (
            [{"method": "ShuffleFilter", "seed": 42}],
            "BTC",
            "filter_at_the_beginning",
        ),  # OperationalException expected
        # PerformanceFilter after StaticPairList
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "HOT/BTC"],
        ),
        # PerformanceFilter only
        (
            [{"method": "PerformanceFilter"}],
            "BTC",
            "filter_at_the_beginning",
        ),  # OperationalException expected
        # SpreadFilter after StaticPairList
        (
            [{"method": "StaticPairList"}, {"method": "SpreadFilter", "max_spread_ratio": 0.005}],
            "BTC",
            ["ETH/BTC", "TKN/BTC"],
        ),
        # SpreadFilter only
        (
            [{"method": "SpreadFilter", "max_spread_ratio": 0.005}],
            "BTC",
            "filter_at_the_beginning",
        ),  # OperationalException expected
        # Static Pairlist after VolumePairList, on a non-first position (appends pairs)
        (
            [
                {"method": "VolumePairList", "number_assets": 2, "sort_key": "quoteVolume"},
                {"method": "StaticPairList"},
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "TRST/BTC", "SWT/BTC", "BCC/BTC", "HOT/BTC"],
        ),
        (
            [
                {"method": "VolumePairList", "number_assets": 20, "sort_key": "quoteVolume"},
                {"method": "PriceFilter", "low_price_ratio": 0.02},
            ],
            "USDT",
            ["ETH/USDT", "NANO/USDT"],
        ),
        (
            [
                {"method": "VolumePairList", "number_assets": 20, "sort_key": "quoteVolume"},
                {"method": "PriceFilter", "max_value": 0.000001},
            ],
            "USDT",
            ["NANO/USDT"],
        ),
        (
            [
                {"method": "StaticPairList"},
                {
                    "method": "RangeStabilityFilter",
                    "lookback_days": 10,
                    "min_rate_of_change": 0.01,
                    "refresh_period": 1440,
                },
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC", "HOT/BTC"],
        ),
        (
            [
                {"method": "StaticPairList"},
                {
                    "method": "RangeStabilityFilter",
                    "lookback_days": 10,
                    "max_rate_of_change": 0.01,
                    "refresh_period": 1440,
                },
            ],
            "BTC",
            [],
        ),  # All removed because of max_rate_of_change being 0.017
        (
            [
                {"method": "StaticPairList"},
                {
                    "method": "RangeStabilityFilter",
                    "lookback_days": 10,
                    "min_rate_of_change": 0.018,
                    "max_rate_of_change": 0.02,
                    "refresh_period": 1440,
                },
            ],
            "BTC",
            [],
        ),  # All removed - limits are above the highest change_rate
        (
            [
                {"method": "StaticPairList"},
                {
                    "method": "VolatilityFilter",
                    "lookback_days": 3,
                    "min_volatility": 0.002,
                    "max_volatility": 0.004,
                    "refresh_period": 1440,
                },
            ],
            "BTC",
            ["ETH/BTC", "TKN/BTC"],
        ),
        # VolumePairList with no offset = unchanged pairlist
        (
            [
                {"method": "VolumePairList", "number_assets": 20, "sort_key": "quoteVolume"},
                {"method": "OffsetFilter", "offset": 0, "number_assets": 0},
            ],
            "USDT",
            ["ETH/USDT", "NANO/USDT", "ADAHALF/USDT", "ADADOUBLE/USDT"],
        ),
        # VolumePairList with offset = 2
        (
            [
                {"method": "VolumePairList", "number_assets": 20, "sort_key": "quoteVolume"},
                {"method": "OffsetFilter", "offset": 2},
            ],
            "USDT",
            ["ADAHALF/USDT", "ADADOUBLE/USDT"],
        ),
        # VolumePairList with offset and limit
        (
            [
                {"method": "VolumePairList", "number_assets": 20, "sort_key": "quoteVolume"},
                {"method": "OffsetFilter", "offset": 1, "number_assets": 2},
            ],
            "USDT",
            ["NANO/USDT", "ADAHALF/USDT"],
        ),
        # VolumePairList with higher offset, than total pairlist
        (
            [
                {"method": "VolumePairList", "number_assets": 20, "sort_key": "quoteVolume"},
                {"method": "OffsetFilter", "offset": 100},
            ],
            "USDT",
            [],
        ),
    ],
)
def test_VolumePairList_whitelist_gen(
    mocker,
    whitelist_conf,
    shitcoinmarkets,
    tickers,
    ohlcv_history,
    pairlists,
    base_currency,
    whitelist_result,
    caplog,
) -> None:
    whitelist_conf["runmode"] = "util_exchange"
    whitelist_conf["pairlists"] = pairlists
    whitelist_conf["stake_currency"] = base_currency

    ohlcv_history_high_vola = ohlcv_history.copy()
    ohlcv_history_high_vola.loc[ohlcv_history_high_vola.index == 1, "close"] = 0.00090

    ohlcv_data = {
        ("ETH/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("TKN/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("LTC/BTC", "1d", CandleType.SPOT): pd.concat([ohlcv_history, ohlcv_history]),
        ("XRP/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("HOT/BTC", "1d", CandleType.SPOT): ohlcv_history_high_vola,
    }

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    mocker.patch.multiple(
        EXMS, get_tickers=tickers, markets=PropertyMock(return_value=shitcoinmarkets)
    )
    mocker.patch.multiple(
        EXMS,
        refresh_latest_ohlcv=MagicMock(return_value=ohlcv_data),
    )

    # Provide for PerformanceFilter's dependency
    mocker.patch.multiple(
        "freqtrade.persistence.Trade", get_overall_performance=MagicMock(return_value=[])
    )

    # Set whitelist_result to None if pairlist is invalid and should produce exception
    if whitelist_result == "filter_at_the_beginning":
        with pytest.raises(
            OperationalException,
            match=r"This Pairlist Handler should not be used at the first position "
            r"in the list of Pairlist Handlers.",
        ):
            freqtrade.pairlists.refresh_pairlist()
    else:
        freqtrade.pairlists.refresh_pairlist()
        whitelist = freqtrade.pairlists.whitelist

        assert isinstance(whitelist, list)

        # Verify length of pairlist matches (used for ShuffleFilter without seed)
        if isinstance(whitelist_result, list):
            assert whitelist == whitelist_result
        else:
            assert len(whitelist) == whitelist_result

        for pairlist in pairlists:
            if (
                pairlist["method"] == "AgeFilter"
                and pairlist["min_days_listed"]
                and len(ohlcv_history) < pairlist["min_days_listed"]
            ):
                assert log_has_re(
                    r"^Removed .* from whitelist, because age .* is less than " r".* day.*", caplog
                )
            if (
                pairlist["method"] == "AgeFilter"
                and pairlist["max_days_listed"]
                and len(ohlcv_history) > pairlist["max_days_listed"]
            ):
                assert log_has_re(
                    r"^Removed .* from whitelist, because age .* is less than "
                    r".* day.* or more than .* day",
                    caplog,
                )
            if pairlist["method"] == "PrecisionFilter" and whitelist_result:
                assert log_has_re(
                    r"^Removed .* from whitelist, because stop price .* "
                    r"would be <= stop limit.*",
                    caplog,
                )
            if pairlist["method"] == "PriceFilter" and whitelist_result:
                assert (
                    log_has_re(r"^Removed .* from whitelist, because 1 unit is .*%$", caplog)
                    or log_has_re(
                        r"^Removed .* from whitelist, " r"because last price < .*%$", caplog
                    )
                    or log_has_re(
                        r"^Removed .* from whitelist, " r"because last price > .*%$", caplog
                    )
                    or log_has_re(
                        r"^Removed .* from whitelist, " r"because min value change of .*", caplog
                    )
                    or log_has_re(
                        r"^Removed .* from whitelist, because ticker\['last'\] " r"is empty.*",
                        caplog,
                    )
                )
            if pairlist["method"] == "VolumePairList":
                logmsg = (
                    "DEPRECATED: using any key other than quoteVolume for "
                    "VolumePairList is deprecated."
                )
                if pairlist["sort_key"] != "quoteVolume":
                    assert log_has(logmsg, caplog)
                else:
                    assert not log_has(logmsg, caplog)
            if pairlist["method"] == "VolatilityFilter":
                assert log_has_re(r"^Removed .* from whitelist, because volatility.*$", caplog)


@pytest.mark.parametrize(
    "pairlists,base_currency,exchange,volumefilter_result",
    [
        # default refresh of 1800 to small for daily candle lookback
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_days": 1,
                }
            ],
            "BTC",
            "binance",
            "default_refresh_too_short",
        ),  # OperationalException expected
        # ambiguous configuration with lookback days and period
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_days": 1,
                    "lookback_period": 1,
                }
            ],
            "BTC",
            "binance",
            "lookback_days_and_period",
        ),  # OperationalException expected
        # negative lookback period
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_timeframe": "1d",
                    "lookback_period": -1,
                }
            ],
            "BTC",
            "binance",
            "lookback_period_negative",
        ),  # OperationalException expected
        # lookback range exceedes exchange limit
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_timeframe": "1m",
                    "lookback_period": 2000,
                    "refresh_period": 3600,
                }
            ],
            "BTC",
            "binance",
            "lookback_exceeds_exchange_request_size",
        ),  # OperationalException expected
        # expecting pairs as given
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_timeframe": "1d",
                    "lookback_period": 1,
                    "refresh_period": 86400,
                }
            ],
            "BTC",
            "binance",
            ["LTC/BTC", "ETH/BTC", "TKN/BTC", "XRP/BTC", "HOT/BTC"],
        ),
        # expecting pairs as input, because 1h candles are not available
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_timeframe": "1h",
                    "lookback_period": 2,
                    "refresh_period": 3600,
                }
            ],
            "BTC",
            "binance",
            ["ETH/BTC", "LTC/BTC", "NEO/BTC", "TKN/BTC", "XRP/BTC"],
        ),
        # TKN/BTC is removed because it doesn't have enough candles
        (
            [
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_timeframe": "1d",
                    "lookback_period": 6,
                    "refresh_period": 86400,
                }
            ],
            "BTC",
            "binance",
            ["LTC/BTC", "XRP/BTC", "ETH/BTC", "HOT/BTC", "NEO/BTC"],
        ),
        # VolumePairlist in range mode as filter.
        # TKN/BTC is removed because it doesn't have enough candles
        (
            [
                {"method": "VolumePairList", "number_assets": 5},
                {
                    "method": "VolumePairList",
                    "number_assets": 5,
                    "sort_key": "quoteVolume",
                    "lookback_timeframe": "1d",
                    "lookback_period": 2,
                    "refresh_period": 86400,
                },
            ],
            "BTC",
            "binance",
            ["LTC/BTC", "XRP/BTC", "ETH/BTC", "TKN/BTC", "HOT/BTC"],
        ),
        # ftx data is already in Quote currency, therefore won't require conversion
        # ([{"method": "VolumePairList", "number_assets": 5, "sort_key": "quoteVolume",
        #    "lookback_timeframe": "1d", "lookback_period": 1, "refresh_period": 86400}],
        #  "BTC", "ftx", ['HOT/BTC', 'LTC/BTC', 'ETH/BTC', 'TKN/BTC', 'XRP/BTC']),
    ],
)
def test_VolumePairList_range(
    mocker,
    whitelist_conf,
    shitcoinmarkets,
    tickers,
    ohlcv_history,
    pairlists,
    base_currency,
    exchange,
    volumefilter_result,
    time_machine,
) -> None:
    whitelist_conf["pairlists"] = pairlists
    whitelist_conf["stake_currency"] = base_currency
    whitelist_conf["exchange"]["name"] = exchange
    # Ensure we have 6 candles
    ohlcv_history_long = pd.concat([ohlcv_history, ohlcv_history])

    ohlcv_history_high_vola = ohlcv_history_long.copy()
    ohlcv_history_high_vola.loc[ohlcv_history_high_vola.index == 1, "close"] = 0.00090

    # create candles for medium overall volume with last candle high volume
    ohlcv_history_medium_volume = ohlcv_history_long.copy()
    ohlcv_history_medium_volume.loc[ohlcv_history_medium_volume.index == 2, "volume"] = 5

    # create candles for high volume with all candles high volume, but very low price.
    ohlcv_history_high_volume = ohlcv_history_long.copy()
    ohlcv_history_high_volume["volume"] = 10
    ohlcv_history_high_volume["low"] = ohlcv_history_high_volume.loc[:, "low"] * 0.01
    ohlcv_history_high_volume["high"] = ohlcv_history_high_volume.loc[:, "high"] * 0.01
    ohlcv_history_high_volume["close"] = ohlcv_history_high_volume.loc[:, "close"] * 0.01

    ohlcv_data = {
        ("ETH/BTC", "1d", CandleType.SPOT): ohlcv_history_long,
        ("TKN/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("LTC/BTC", "1d", CandleType.SPOT): ohlcv_history_medium_volume,
        ("XRP/BTC", "1d", CandleType.SPOT): ohlcv_history_high_vola,
        ("HOT/BTC", "1d", CandleType.SPOT): ohlcv_history_high_volume,
    }

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    if volumefilter_result == "default_refresh_too_short":
        with pytest.raises(
            OperationalException,
            match=r"Refresh period of [0-9]+ seconds is smaller than one timeframe "
            r"of [0-9]+.*\. Please adjust refresh_period to at least [0-9]+ "
            r"and restart the bot\.",
        ):
            freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
        return
    elif volumefilter_result == "lookback_days_and_period":
        with pytest.raises(
            OperationalException,
            match=r"Ambiguous configuration: lookback_days and lookback_period both "
            r"set in pairlist config\..*",
        ):
            freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    elif volumefilter_result == "lookback_period_negative":
        with pytest.raises(
            OperationalException, match=r"VolumeFilter requires lookback_period to be >= 0"
        ):
            freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    elif volumefilter_result == "lookback_exceeds_exchange_request_size":
        with pytest.raises(
            OperationalException,
            match=r"VolumeFilter requires lookback_period to not exceed "
            r"exchange max request size \([0-9]+\)",
        ):
            freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    else:
        freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
        mocker.patch.multiple(
            EXMS, get_tickers=tickers, markets=PropertyMock(return_value=shitcoinmarkets)
        )
        start_dt = dt_now()
        time_machine.move_to(start_dt)
        # remove ohlcv when looback_timeframe != 1d
        # to enforce fallback to ticker data
        if "lookback_timeframe" in pairlists[0]:
            if pairlists[0]["lookback_timeframe"] != "1d":
                ohlcv_data = {}

        ohclv_mock = mocker.patch(f"{EXMS}.refresh_latest_ohlcv", return_value=ohlcv_data)

        freqtrade.pairlists.refresh_pairlist()
        whitelist = freqtrade.pairlists.whitelist
        assert ohclv_mock.call_count == 1

        assert isinstance(whitelist, list)
        assert whitelist == volumefilter_result
        # Test caching
        ohclv_mock.reset_mock()
        freqtrade.pairlists.refresh_pairlist()
        # in "filter" mode, caching is disabled.
        assert ohclv_mock.call_count == 0
        whitelist = freqtrade.pairlists.whitelist
        assert whitelist == volumefilter_result

        time_machine.move_to(start_dt + timedelta(days=2))
        ohclv_mock.reset_mock()
        freqtrade.pairlists.refresh_pairlist()
        assert ohclv_mock.call_count == 1
        whitelist = freqtrade.pairlists.whitelist
        assert whitelist == volumefilter_result


def test_PrecisionFilter_error(mocker, whitelist_conf) -> None:
    whitelist_conf["pairlists"] = [{"method": "StaticPairList"}, {"method": "PrecisionFilter"}]
    del whitelist_conf["stoploss"]

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    with pytest.raises(
        OperationalException, match=r"PrecisionFilter can only work with stoploss defined\..*"
    ):
        PairListManager(MagicMock, whitelist_conf, MagicMock())


def test_PerformanceFilter_error(mocker, whitelist_conf, caplog) -> None:
    whitelist_conf["pairlists"] = [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}]
    if hasattr(Trade, "session"):
        del Trade.session
    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    exchange = get_patched_exchange(mocker, whitelist_conf)
    pm = PairListManager(exchange, whitelist_conf, MagicMock())
    pm.refresh_pairlist()

    assert log_has("PerformanceFilter is not available in this mode.", caplog)


def test_VolatilityFilter_error(mocker, whitelist_conf) -> None:
    volatility_filter = {"method": "VolatilityFilter", "lookback_days": -1}
    whitelist_conf["pairlists"] = [{"method": "StaticPairList"}, volatility_filter]

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    exchange_mock = MagicMock()
    exchange_mock.ohlcv_candle_limit = MagicMock(return_value=1000)

    with pytest.raises(
        OperationalException, match=r"VolatilityFilter requires lookback_days to be >= 1*"
    ):
        PairListManager(exchange_mock, whitelist_conf, MagicMock())

    volatility_filter = {"method": "VolatilityFilter", "lookback_days": 2000}
    whitelist_conf["pairlists"] = [{"method": "StaticPairList"}, volatility_filter]
    with pytest.raises(
        OperationalException,
        match=r"VolatilityFilter requires lookback_days to not exceed exchange max",
    ):
        PairListManager(exchange_mock, whitelist_conf, MagicMock())

    volatility_filter = {"method": "VolatilityFilter", "sort_direction": "Random"}
    whitelist_conf["pairlists"] = [{"method": "StaticPairList"}, volatility_filter]
    with pytest.raises(
        OperationalException,
        match=r"VolatilityFilter requires sort_direction to be either " r"None .*'asc'.*'desc'",
    ):
        PairListManager(exchange_mock, whitelist_conf, MagicMock())


@pytest.mark.parametrize(
    "pairlist,expected_pairlist",
    [
        (
            {"method": "VolatilityFilter", "sort_direction": "asc"},
            ["XRP/BTC", "ETH/BTC", "LTC/BTC", "TKN/BTC"],
        ),
        (
            {"method": "VolatilityFilter", "sort_direction": "desc"},
            ["TKN/BTC", "LTC/BTC", "ETH/BTC", "XRP/BTC"],
        ),
        (
            {"method": "VolatilityFilter", "sort_direction": "desc", "min_volatility": 0.4},
            ["TKN/BTC", "LTC/BTC", "ETH/BTC"],
        ),
        (
            {"method": "VolatilityFilter", "sort_direction": "asc", "min_volatility": 0.4},
            ["ETH/BTC", "LTC/BTC", "TKN/BTC"],
        ),
        (
            {"method": "VolatilityFilter", "sort_direction": "desc", "max_volatility": 0.5},
            ["LTC/BTC", "ETH/BTC", "XRP/BTC"],
        ),
        (
            {"method": "VolatilityFilter", "sort_direction": "asc", "max_volatility": 0.5},
            ["XRP/BTC", "ETH/BTC", "LTC/BTC"],
        ),
        (
            {"method": "RangeStabilityFilter", "sort_direction": "asc"},
            ["ETH/BTC", "XRP/BTC", "LTC/BTC", "TKN/BTC"],
        ),
        (
            {"method": "RangeStabilityFilter", "sort_direction": "desc"},
            ["TKN/BTC", "LTC/BTC", "XRP/BTC", "ETH/BTC"],
        ),
        (
            {"method": "RangeStabilityFilter", "sort_direction": "asc", "min_rate_of_change": 0.4},
            ["XRP/BTC", "LTC/BTC", "TKN/BTC"],
        ),
        (
            {"method": "RangeStabilityFilter", "sort_direction": "desc", "min_rate_of_change": 0.4},
            ["TKN/BTC", "LTC/BTC", "XRP/BTC"],
        ),
    ],
)
def test_VolatilityFilter_RangeStabilityFilter_sort(
    mocker, whitelist_conf, tickers, time_machine, pairlist, expected_pairlist
) -> None:
    whitelist_conf["pairlists"] = [{"method": "VolumePairList", "number_assets": 10}, pairlist]

    df1 = generate_test_data("1d", 10, "2022-01-05 00:00:00+00:00", random_seed=42)
    df2 = generate_test_data("1d", 10, "2022-01-05 00:00:00+00:00", random_seed=2)
    df3 = generate_test_data("1d", 10, "2022-01-05 00:00:00+00:00", random_seed=3)
    df4 = generate_test_data("1d", 10, "2022-01-05 00:00:00+00:00", random_seed=4)
    df5 = generate_test_data("1d", 10, "2022-01-05 00:00:00+00:00", random_seed=5)
    df6 = generate_test_data("1d", 10, "2022-01-05 00:00:00+00:00", random_seed=6)

    assert not df1.equals(df2)
    time_machine.move_to("2022-01-15 00:00:00+00:00")

    ohlcv_data = {
        ("ETH/BTC", "1d", CandleType.SPOT): df1,
        ("TKN/BTC", "1d", CandleType.SPOT): df2,
        ("LTC/BTC", "1d", CandleType.SPOT): df3,
        ("XRP/BTC", "1d", CandleType.SPOT): df4,
        ("HOT/BTC", "1d", CandleType.SPOT): df5,
        ("BLK/BTC", "1d", CandleType.SPOT): df6,
    }
    ohlcv_mock = MagicMock(return_value=ohlcv_data)
    mocker.patch.multiple(
        EXMS,
        exchange_has=MagicMock(return_value=True),
        refresh_latest_ohlcv=ohlcv_mock,
        get_tickers=tickers,
    )

    exchange = get_patched_exchange(mocker, whitelist_conf)
    exchange.ohlcv_candle_limit = MagicMock(return_value=1000)
    plm = PairListManager(exchange, whitelist_conf, MagicMock())

    assert exchange.ohlcv_candle_limit.call_count == 2
    plm.refresh_pairlist()
    assert ohlcv_mock.call_count == 1
    assert exchange.ohlcv_candle_limit.call_count == 2
    assert plm.whitelist == expected_pairlist

    plm.refresh_pairlist()
    assert exchange.ohlcv_candle_limit.call_count == 2
    assert ohlcv_mock.call_count == 1


def test_ShuffleFilter_init(mocker, whitelist_conf, caplog) -> None:
    whitelist_conf["pairlists"] = [
        {"method": "StaticPairList"},
        {"method": "ShuffleFilter", "seed": 43},
    ]
    whitelist_conf["runmode"] = "backtest"

    exchange = get_patched_exchange(mocker, whitelist_conf)
    plm = PairListManager(exchange, whitelist_conf)
    assert log_has("Backtesting mode detected, applying seed value: 43", caplog)

    with time_machine.travel("2021-09-01 05:01:00 +00:00") as t:
        plm.refresh_pairlist()
        pl1 = deepcopy(plm.whitelist)
        plm.refresh_pairlist()
        assert plm.whitelist == pl1

        t.shift(timedelta(minutes=10))
        plm.refresh_pairlist()
        assert plm.whitelist != pl1

    caplog.clear()
    whitelist_conf["runmode"] = RunMode.DRY_RUN
    plm = PairListManager(exchange, whitelist_conf)
    assert not log_has("Backtesting mode detected, applying seed value: 42", caplog)
    assert log_has("Live mode detected, not applying seed.", caplog)


@pytest.mark.usefixtures("init_persistence")
def test_PerformanceFilter_lookback(mocker, default_conf_usdt, fee, caplog) -> None:
    default_conf_usdt["exchange"]["pair_whitelist"].extend(["ADA/USDT", "XRP/USDT", "ETC/USDT"])
    default_conf_usdt["pairlists"] = [
        {"method": "StaticPairList"},
        {"method": "PerformanceFilter", "minutes": 60, "min_profit": 0.01},
    ]
    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    exchange = get_patched_exchange(mocker, default_conf_usdt)
    pm = PairListManager(exchange, default_conf_usdt)
    pm.refresh_pairlist()

    assert pm.whitelist == ["ETH/USDT", "XRP/USDT", "NEO/USDT", "TKN/USDT"]

    with time_machine.travel("2021-09-01 05:00:00 +00:00") as t:
        create_mock_trades_usdt(fee)
        pm.refresh_pairlist()
        assert pm.whitelist == ["XRP/USDT", "NEO/USDT"]
        assert log_has_re(r"Removing pair .* since .* is below .*", caplog)

        # Move to "outside" of lookback window, so original sorting is restored.
        t.move_to("2021-09-01 07:00:00 +00:00")
        pm.refresh_pairlist()
        assert pm.whitelist == ["ETH/USDT", "XRP/USDT", "NEO/USDT", "TKN/USDT"]


@pytest.mark.usefixtures("init_persistence")
def test_PerformanceFilter_keep_mid_order(mocker, default_conf_usdt, fee, caplog) -> None:
    default_conf_usdt["exchange"]["pair_whitelist"].extend(["ADA/USDT", "ETC/USDT"])
    default_conf_usdt["pairlists"] = [
        {"method": "StaticPairList", "allow_inactive": True},
        {
            "method": "PerformanceFilter",
            "minutes": 60,
        },
    ]
    mocker.patch(f"{EXMS}.exchange_has", return_value=True)
    exchange = get_patched_exchange(mocker, default_conf_usdt)
    pm = PairListManager(exchange, default_conf_usdt)
    pm.refresh_pairlist()

    assert pm.whitelist == [
        "ETH/USDT",
        "LTC/USDT",
        "XRP/USDT",
        "NEO/USDT",
        "TKN/USDT",
        "ADA/USDT",
        "ETC/USDT",
    ]

    with time_machine.travel("2021-09-01 05:00:00 +00:00") as t:
        create_mock_trades_usdt(fee)
        pm.refresh_pairlist()
        assert pm.whitelist == [
            "XRP/USDT",
            "NEO/USDT",
            "ETH/USDT",
            "LTC/USDT",
            "TKN/USDT",
            "ADA/USDT",
            "ETC/USDT",
        ]
        # assert log_has_re(r'Removing pair .* since .* is below .*', caplog)

        # Move to "outside" of lookback window, so original sorting is restored.
        t.move_to("2021-09-01 07:00:00 +00:00")
        pm.refresh_pairlist()
        assert pm.whitelist == [
            "ETH/USDT",
            "LTC/USDT",
            "XRP/USDT",
            "NEO/USDT",
            "TKN/USDT",
            "ADA/USDT",
            "ETC/USDT",
        ]


def test_gen_pair_whitelist_not_supported(mocker, default_conf, tickers) -> None:
    default_conf["pairlists"] = [{"method": "VolumePairList", "number_assets": 10}]

    mocker.patch.multiple(
        EXMS,
        get_tickers=tickers,
        exchange_has=MagicMock(return_value=False),
    )

    with pytest.raises(
        OperationalException, match=r"Exchange does not support dynamic whitelist.*"
    ):
        get_patched_freqtradebot(mocker, default_conf)


def test_pair_whitelist_not_supported_Spread(mocker, default_conf, tickers) -> None:
    default_conf["pairlists"] = [{"method": "StaticPairList"}, {"method": "SpreadFilter"}]

    mocker.patch.multiple(
        EXMS,
        get_tickers=tickers,
        exchange_has=MagicMock(return_value=False),
    )

    with pytest.raises(OperationalException, match=r"Exchange does not support fetchTickers, .*"):
        get_patched_freqtradebot(mocker, default_conf)

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    mocker.patch(f"{EXMS}.get_option", MagicMock(return_value=False))
    with pytest.raises(OperationalException, match=r".*requires exchange to have bid/ask data"):
        get_patched_freqtradebot(mocker, default_conf)


@pytest.mark.parametrize("pairlist", TESTABLE_PAIRLISTS)
def test_pairlist_class(mocker, whitelist_conf, markets, pairlist):
    whitelist_conf["pairlists"][0]["method"] = pairlist
    mocker.patch.multiple(
        EXMS, markets=PropertyMock(return_value=markets), exchange_has=MagicMock(return_value=True)
    )
    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)

    assert freqtrade.pairlists.name_list == [pairlist]
    assert pairlist in str(freqtrade.pairlists.short_desc())
    assert isinstance(freqtrade.pairlists.whitelist, list)
    assert isinstance(freqtrade.pairlists.blacklist, list)


@pytest.mark.parametrize("pairlist", TESTABLE_PAIRLISTS)
@pytest.mark.parametrize(
    "whitelist,log_message",
    [
        (["ETH/BTC", "TKN/BTC"], ""),
        # TRX/ETH not in markets
        (["ETH/BTC", "TKN/BTC", "TRX/ETH"], "is not compatible with exchange"),
        # wrong stake
        (["ETH/BTC", "TKN/BTC", "ETH/USDT"], "is not compatible with your stake currency"),
        # BCH/BTC not available
        (["ETH/BTC", "TKN/BTC", "BCH/BTC"], "is not compatible with exchange"),
        # BTT/BTC is inactive
        (["ETH/BTC", "TKN/BTC", "BTT/BTC"], "Market is not active"),
        # XLTCUSDT is not a valid pair
        (["ETH/BTC", "TKN/BTC", "XLTCUSDT"], "is not tradable with Freqtrade"),
    ],
)
def test__whitelist_for_active_markets(
    mocker, whitelist_conf, markets, pairlist, whitelist, caplog, log_message, tickers
):
    whitelist_conf["pairlists"][0]["method"] = pairlist
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )
    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    caplog.clear()

    # Assign starting whitelist
    pairlist_handler = freqtrade.pairlists._pairlist_handlers[0]
    new_whitelist = pairlist_handler._whitelist_for_active_markets(whitelist)

    assert set(new_whitelist) == set(["ETH/BTC", "TKN/BTC"])
    assert log_message in caplog.text


@pytest.mark.parametrize("pairlist", TESTABLE_PAIRLISTS)
def test__whitelist_for_active_markets_empty(mocker, whitelist_conf, pairlist, tickers):
    whitelist_conf["pairlists"][0]["method"] = pairlist

    mocker.patch(f"{EXMS}.exchange_has", return_value=True)

    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    mocker.patch.multiple(EXMS, markets=PropertyMock(return_value=None), get_tickers=tickers)
    # Assign starting whitelist
    pairlist_handler = freqtrade.pairlists._pairlist_handlers[0]
    with pytest.raises(OperationalException, match=r"Markets not loaded.*"):
        pairlist_handler._whitelist_for_active_markets(["ETH/BTC"])


def test_volumepairlist_invalid_sortvalue(mocker, whitelist_conf):
    whitelist_conf["pairlists"][0].update({"sort_key": "asdf"})

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    with pytest.raises(OperationalException, match=r"key asdf not in .*"):
        get_patched_freqtradebot(mocker, whitelist_conf)


def test_volumepairlist_caching(mocker, markets, whitelist_conf, tickers):
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )
    freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
    assert len(freqtrade.pairlists._pairlist_handlers[0]._pair_cache) == 0
    assert tickers.call_count == 0
    freqtrade.pairlists.refresh_pairlist()
    assert tickers.call_count == 1

    assert len(freqtrade.pairlists._pairlist_handlers[0]._pair_cache) == 1
    freqtrade.pairlists.refresh_pairlist()
    assert tickers.call_count == 1


def test_agefilter_min_days_listed_too_small(mocker, default_conf, markets, tickers):
    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "AgeFilter", "min_days_listed": -1},
    ]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )

    with pytest.raises(
        OperationalException, match=r"AgeFilter requires min_days_listed to be >= 1"
    ):
        get_patched_freqtradebot(mocker, default_conf)


def test_agefilter_max_days_lower_than_min_days(mocker, default_conf, markets, tickers):
    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "AgeFilter", "min_days_listed": 3, "max_days_listed": 2},
    ]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )

    with pytest.raises(
        OperationalException, match=r"AgeFilter max_days_listed <= min_days_listed not permitted"
    ):
        get_patched_freqtradebot(mocker, default_conf)


def test_agefilter_min_days_listed_too_large(mocker, default_conf, markets, tickers):
    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "AgeFilter", "min_days_listed": 99999},
    ]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )

    with pytest.raises(
        OperationalException,
        match=r"AgeFilter requires min_days_listed to not exceed "
        r"exchange max request size \([0-9]+\)",
    ):
        get_patched_freqtradebot(mocker, default_conf)


def test_agefilter_caching(mocker, markets, whitelist_conf_agefilter, tickers, ohlcv_history):
    with time_machine.travel("2021-09-01 05:00:00 +00:00") as t:
        ohlcv_data = {
            ("ETH/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("TKN/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("LTC/BTC", "1d", CandleType.SPOT): ohlcv_history,
        }
        mocker.patch.multiple(
            EXMS,
            markets=PropertyMock(return_value=markets),
            exchange_has=MagicMock(return_value=True),
            get_tickers=tickers,
            refresh_latest_ohlcv=MagicMock(return_value=ohlcv_data),
        )

        freqtrade = get_patched_freqtradebot(mocker, whitelist_conf_agefilter)
        assert freqtrade.exchange.refresh_latest_ohlcv.call_count == 0
        freqtrade.pairlists.refresh_pairlist()
        assert len(freqtrade.pairlists.whitelist) == 3
        assert freqtrade.exchange.refresh_latest_ohlcv.call_count > 0

        freqtrade.pairlists.refresh_pairlist()
        assert len(freqtrade.pairlists.whitelist) == 3
        # Call to XRP/BTC cached
        assert freqtrade.exchange.refresh_latest_ohlcv.call_count == 2

        ohlcv_data = {
            ("ETH/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("TKN/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("LTC/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("XRP/BTC", "1d", CandleType.SPOT): ohlcv_history.iloc[[0]],
        }
        mocker.patch(f"{EXMS}.refresh_latest_ohlcv", return_value=ohlcv_data)
        freqtrade.pairlists.refresh_pairlist()
        assert len(freqtrade.pairlists.whitelist) == 3
        assert freqtrade.exchange.refresh_latest_ohlcv.call_count == 1

        # Move to next day
        t.move_to("2021-09-02 01:00:00 +00:00")
        mocker.patch(f"{EXMS}.refresh_latest_ohlcv", return_value=ohlcv_data)
        freqtrade.pairlists.refresh_pairlist()
        assert len(freqtrade.pairlists.whitelist) == 3
        assert freqtrade.exchange.refresh_latest_ohlcv.call_count == 1

        # Move another day with fresh mocks (now the pair is old enough)
        t.move_to("2021-09-03 01:00:00 +00:00")
        # Called once for XRP/BTC
        ohlcv_data = {
            ("ETH/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("TKN/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("LTC/BTC", "1d", CandleType.SPOT): ohlcv_history,
            ("XRP/BTC", "1d", CandleType.SPOT): ohlcv_history,
        }
        mocker.patch(f"{EXMS}.refresh_latest_ohlcv", return_value=ohlcv_data)
        freqtrade.pairlists.refresh_pairlist()
        assert len(freqtrade.pairlists.whitelist) == 4
        # Called once (only for XRP/BTC)
        assert freqtrade.exchange.refresh_latest_ohlcv.call_count == 1


def test_OffsetFilter_error(mocker, whitelist_conf) -> None:
    whitelist_conf["pairlists"] = [
        {"method": "StaticPairList"},
        {"method": "OffsetFilter", "offset": -1},
    ]

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    with pytest.raises(OperationalException, match=r"OffsetFilter requires offset to be >= 0"):
        PairListManager(MagicMock, whitelist_conf)


def test_rangestabilityfilter_checks(mocker, default_conf, markets, tickers):
    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "RangeStabilityFilter", "lookback_days": 99999},
    ]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )

    with pytest.raises(
        OperationalException,
        match=r"RangeStabilityFilter requires lookback_days to not exceed "
        r"exchange max request size \([0-9]+\)",
    ):
        get_patched_freqtradebot(mocker, default_conf)

    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "RangeStabilityFilter", "lookback_days": 0},
    ]

    with pytest.raises(
        OperationalException, match="RangeStabilityFilter requires lookback_days to be >= 1"
    ):
        get_patched_freqtradebot(mocker, default_conf)

    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "RangeStabilityFilter", "sort_direction": "something"},
    ]

    with pytest.raises(
        OperationalException,
        match="RangeStabilityFilter requires sort_direction to be either None.*",
    ):
        get_patched_freqtradebot(mocker, default_conf)


@pytest.mark.parametrize(
    "min_rate_of_change,max_rate_of_change,expected_length",
    [
        (0.01, 0.99, 5),
        (0.05, 0.0, 0),  # Setting min rate_of_change to 5% removes all pairs from the whitelist.
    ],
)
def test_rangestabilityfilter_caching(
    mocker,
    markets,
    default_conf,
    tickers,
    ohlcv_history,
    min_rate_of_change,
    max_rate_of_change,
    expected_length,
):
    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {
            "method": "RangeStabilityFilter",
            "lookback_days": 2,
            "min_rate_of_change": min_rate_of_change,
            "max_rate_of_change": max_rate_of_change,
        },
    ]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )
    ohlcv_data = {
        ("ETH/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("TKN/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("LTC/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("XRP/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("HOT/BTC", "1d", CandleType.SPOT): ohlcv_history,
        ("BLK/BTC", "1d", CandleType.SPOT): ohlcv_history,
    }
    mocker.patch.multiple(
        EXMS,
        refresh_latest_ohlcv=MagicMock(return_value=ohlcv_data),
    )

    freqtrade = get_patched_freqtradebot(mocker, default_conf)
    assert freqtrade.exchange.refresh_latest_ohlcv.call_count == 0
    freqtrade.pairlists.refresh_pairlist()
    assert len(freqtrade.pairlists.whitelist) == expected_length
    assert freqtrade.exchange.refresh_latest_ohlcv.call_count > 0

    previous_call_count = freqtrade.exchange.refresh_latest_ohlcv.call_count
    freqtrade.pairlists.refresh_pairlist()
    assert len(freqtrade.pairlists.whitelist) == expected_length
    # Should not have increased since first call.
    assert freqtrade.exchange.refresh_latest_ohlcv.call_count == previous_call_count


def test_spreadfilter_invalid_data(mocker, default_conf, markets, tickers, caplog):
    default_conf["pairlists"] = [
        {"method": "VolumePairList", "number_assets": 10},
        {"method": "SpreadFilter", "max_spread_ratio": 0.1},
    ]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )

    ftbot = get_patched_freqtradebot(mocker, default_conf)
    ftbot.pairlists.refresh_pairlist()

    assert len(ftbot.pairlists.whitelist) == 5

    tickers.return_value["ETH/BTC"]["ask"] = 0.0
    del tickers.return_value["TKN/BTC"]
    del tickers.return_value["LTC/BTC"]
    mocker.patch.multiple(EXMS, get_tickers=tickers)

    ftbot.pairlists.refresh_pairlist()
    assert log_has_re(r"Removed .* invalid ticker data.*", caplog)

    assert len(ftbot.pairlists.whitelist) == 2


@pytest.mark.parametrize(
    "pairlistconfig,desc_expected,exception_expected",
    [
        (
            {
                "method": "PriceFilter",
                "low_price_ratio": 0.001,
                "min_price": 0.00000010,
                "max_price": 1.0,
            },
            "[{'PriceFilter': 'PriceFilter - Filtering pairs priced below "
            "0.1% or below 0.00000010 or above 1.00000000.'}]",
            None,
        ),
        (
            {"method": "PriceFilter", "low_price_ratio": 0.001, "min_price": 0.00000010},
            "[{'PriceFilter': 'PriceFilter - Filtering pairs priced below 0.1% "
            "or below 0.00000010.'}]",
            None,
        ),
        (
            {"method": "PriceFilter", "low_price_ratio": 0.001, "max_price": 1.00010000},
            "[{'PriceFilter': 'PriceFilter - Filtering pairs priced below 0.1% "
            "or above 1.00010000.'}]",
            None,
        ),
        (
            {"method": "PriceFilter", "min_price": 0.00002000},
            "[{'PriceFilter': 'PriceFilter - Filtering pairs priced below 0.00002000.'}]",
            None,
        ),
        (
            {"method": "PriceFilter", "max_value": 0.00002000},
            "[{'PriceFilter': 'PriceFilter - Filtering pairs priced Value above 0.00002000.'}]",
            None,
        ),
        (
            {"method": "PriceFilter"},
            "[{'PriceFilter': 'PriceFilter - No price filters configured.'}]",
            None,
        ),
        (
            {"method": "PriceFilter", "low_price_ratio": -0.001},
            None,
            "PriceFilter requires low_price_ratio to be >= 0",
        ),  # OperationalException expected
        (
            {"method": "PriceFilter", "min_price": -0.00000010},
            None,
            "PriceFilter requires min_price to be >= 0",
        ),  # OperationalException expected
        (
            {"method": "PriceFilter", "max_price": -1.00010000},
            None,
            "PriceFilter requires max_price to be >= 0",
        ),  # OperationalException expected
        (
            {"method": "PriceFilter", "max_value": -1.00010000},
            None,
            "PriceFilter requires max_value to be >= 0",
        ),  # OperationalException expected
        (
            {"method": "RangeStabilityFilter", "lookback_days": 10, "min_rate_of_change": 0.01},
            "[{'RangeStabilityFilter': 'RangeStabilityFilter - Filtering pairs with rate "
            "of change below 0.01 over the last days.'}]",
            None,
        ),
        (
            {
                "method": "RangeStabilityFilter",
                "lookback_days": 10,
                "min_rate_of_change": 0.01,
                "max_rate_of_change": 0.99,
            },
            "[{'RangeStabilityFilter': 'RangeStabilityFilter - Filtering pairs with rate "
            "of change below 0.01 and above 0.99 over the last days.'}]",
            None,
        ),
        (
            {"method": "OffsetFilter", "offset": 5, "number_assets": 10},
            "[{'OffsetFilter': 'OffsetFilter - Taking 10 Pairs, starting from 5.'}]",
            None,
        ),
        (
            {"method": "ProducerPairList"},
            "[{'ProducerPairList': 'ProducerPairList - default'}]",
            None,
        ),
        (
            {
                "method": "RemotePairList",
                "number_assets": 10,
                "pairlist_url": "https://example.com",
            },
            "[{'RemotePairList': 'RemotePairList - 10 pairs from RemotePairlist.'}]",
            None,
        ),
    ],
)
def test_pricefilter_desc(
    mocker, whitelist_conf, markets, pairlistconfig, desc_expected, exception_expected
):
    mocker.patch.multiple(
        EXMS, markets=PropertyMock(return_value=markets), exchange_has=MagicMock(return_value=True)
    )
    whitelist_conf["pairlists"] = [pairlistconfig]

    if desc_expected is not None:
        freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)
        short_desc = str(freqtrade.pairlists.short_desc())
        assert short_desc == desc_expected
    else:  # OperationalException expected
        with pytest.raises(OperationalException, match=exception_expected):
            freqtrade = get_patched_freqtradebot(mocker, whitelist_conf)


def test_pairlistmanager_no_pairlist(mocker, whitelist_conf):
    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    whitelist_conf["pairlists"] = []

    with pytest.raises(OperationalException, match=r"No Pairlist Handlers defined"):
        get_patched_freqtradebot(mocker, whitelist_conf)


@pytest.mark.parametrize(
    "pairlists,pair_allowlist,overall_performance,allowlist_result",
    [
        # No trades yet
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
            [],
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
        ),
        # Happy path: Descending order, all values filled
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            ["ETH/BTC", "TKN/BTC"],
            [
                {"pair": "TKN/BTC", "profit_ratio": 0.05, "count": 3},
                {"pair": "ETH/BTC", "profit_ratio": 0.04, "count": 2},
            ],
            ["TKN/BTC", "ETH/BTC"],
        ),
        # Performance data outside allow list ignored
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            ["ETH/BTC", "TKN/BTC"],
            [
                {"pair": "OTHER/BTC", "profit_ratio": 0.05, "count": 3},
                {"pair": "ETH/BTC", "profit_ratio": 0.04, "count": 2},
            ],
            ["ETH/BTC", "TKN/BTC"],
        ),
        # Partial performance data missing and sorted between positive and negative profit
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
            [
                {"pair": "ETH/BTC", "profit_ratio": -0.05, "count": 100},
                {"pair": "TKN/BTC", "profit_ratio": 0.04, "count": 2},
            ],
            ["TKN/BTC", "LTC/BTC", "ETH/BTC"],
        ),
        # Tie in performance data broken by count (ascending)
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
            [
                {"pair": "LTC/BTC", "profit_ratio": -0.0501, "count": 101},
                {"pair": "TKN/BTC", "profit_ratio": -0.0501, "count": 2},
                {"pair": "ETH/BTC", "profit_ratio": -0.0501, "count": 100},
            ],
            ["TKN/BTC", "ETH/BTC", "LTC/BTC"],
        ),
        # Tie in performance and count, broken by prior sorting sort
        (
            [{"method": "StaticPairList"}, {"method": "PerformanceFilter"}],
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
            [
                {"pair": "LTC/BTC", "profit_ratio": -0.0501, "count": 1},
                {"pair": "TKN/BTC", "profit_ratio": -0.0501, "count": 1},
                {"pair": "ETH/BTC", "profit_ratio": -0.0501, "count": 1},
            ],
            ["ETH/BTC", "TKN/BTC", "LTC/BTC"],
        ),
    ],
)
def test_performance_filter(
    mocker,
    whitelist_conf,
    pairlists,
    pair_allowlist,
    overall_performance,
    allowlist_result,
    tickers,
    markets,
    ohlcv_history_list,
):
    allowlist_conf = whitelist_conf
    allowlist_conf["pairlists"] = pairlists
    allowlist_conf["exchange"]["pair_whitelist"] = pair_allowlist

    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    freqtrade = get_patched_freqtradebot(mocker, allowlist_conf)
    mocker.patch.multiple(EXMS, get_tickers=tickers, markets=PropertyMock(return_value=markets))
    mocker.patch.multiple(
        EXMS,
        get_historic_ohlcv=MagicMock(return_value=ohlcv_history_list),
    )
    mocker.patch.multiple(
        "freqtrade.persistence.Trade",
        get_overall_performance=MagicMock(return_value=overall_performance),
    )
    freqtrade.pairlists.refresh_pairlist()
    allowlist = freqtrade.pairlists.whitelist
    assert allowlist == allowlist_result


@pytest.mark.parametrize(
    "wildcardlist,pairs,expected",
    [
        (["BTC/USDT"], ["BTC/USDT"], ["BTC/USDT"]),
        (["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"]),
        (["BTC/USDT", "ETH/USDT"], ["BTC/USDT"], ["BTC/USDT"]),  # Test one too many
        ([".*/USDT"], ["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"]),  # Wildcard simple
        (
            [".*C/USDT"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT"],
            ["BTC/USDT", "ETC/USDT"],
        ),  # Wildcard exclude one
        (
            [".*UP/USDT", "BTC/USDT", "ETH/USDT"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT", "BTCUP/USDT", "XRPUP/USDT", "XRPDOWN/USDT"],
            ["BTC/USDT", "ETH/USDT", "BTCUP/USDT", "XRPUP/USDT"],
        ),  # Wildcard exclude one
        (
            ["BTC/.*", "ETH/.*"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT", "BTC/USD", "ETH/EUR", "BTC/GBP"],
            ["BTC/USDT", "ETH/USDT", "BTC/USD", "ETH/EUR", "BTC/GBP"],
        ),  # Wildcard exclude one
        (
            ["*UP/USDT", "BTC/USDT", "ETH/USDT"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT", "BTCUP/USDT", "XRPUP/USDT", "XRPDOWN/USDT"],
            None,
        ),
        (["BTC/USD"], ["BTC/USD", "BTC/USDT"], ["BTC/USD"]),
    ],
)
def test_expand_pairlist(wildcardlist, pairs, expected):
    if expected is None:
        with pytest.raises(ValueError, match=r"Wildcard error in \*UP/USDT,"):
            expand_pairlist(wildcardlist, pairs)
    else:
        assert sorted(expand_pairlist(wildcardlist, pairs)) == sorted(expected)
        conf = {
            "pairs": wildcardlist,
            "freqai": {
                "enabled": True,
                "feature_parameters": {
                    "include_corr_pairlist": [
                        "BTC/USDT:USDT",
                        "XRP/BUSD",
                    ]
                },
            },
        }
        assert sorted(dynamic_expand_pairlist(conf, pairs)) == sorted(
            [*expected, "BTC/USDT:USDT", "XRP/BUSD"]
        )


@pytest.mark.parametrize(
    "wildcardlist,pairs,expected",
    [
        (["BTC/USDT"], ["BTC/USDT"], ["BTC/USDT"]),
        (["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"]),
        (["BTC/USDT", "ETH/USDT"], ["BTC/USDT"], ["BTC/USDT", "ETH/USDT"]),  # Test one too many
        ([".*/USDT"], ["BTC/USDT", "ETH/USDT"], ["BTC/USDT", "ETH/USDT"]),  # Wildcard simple
        (
            [".*C/USDT"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT"],
            ["BTC/USDT", "ETC/USDT"],
        ),  # Wildcard exclude one
        (
            [".*UP/USDT", "BTC/USDT", "ETH/USDT"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT", "BTCUP/USDT", "XRPUP/USDT", "XRPDOWN/USDT"],
            ["BTC/USDT", "ETH/USDT", "BTCUP/USDT", "XRPUP/USDT"],
        ),  # Wildcard exclude one
        (
            ["BTC/.*", "ETH/.*"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT", "BTC/USD", "ETH/EUR", "BTC/GBP"],
            ["BTC/USDT", "ETH/USDT", "BTC/USD", "ETH/EUR", "BTC/GBP"],
        ),  # Wildcard exclude one
        (
            ["*UP/USDT", "BTC/USDT", "ETH/USDT"],
            ["BTC/USDT", "ETC/USDT", "ETH/USDT", "BTCUP/USDT", "XRPUP/USDT", "XRPDOWN/USDT"],
            None,
        ),
        (["HELLO/WORLD"], [], ["HELLO/WORLD"]),  # Invalid pair kept
        (["BTC/USD"], ["BTC/USD", "BTC/USDT"], ["BTC/USD"]),
        (["BTC/USDT:USDT"], ["BTC/USDT:USDT", "BTC/USDT"], ["BTC/USDT:USDT"]),
        (
            ["BB_BTC/USDT", "CC_BTC/USDT", "AA_ETH/USDT", "XRP/USDT", "ETH/USDT", "XX_BTC/USDT"],
            ["BTC/USDT", "ETH/USDT"],
            ["XRP/USDT", "ETH/USDT"],
        ),
    ],
)
def test_expand_pairlist_keep_invalid(wildcardlist, pairs, expected):
    if expected is None:
        with pytest.raises(ValueError, match=r"Wildcard error in \*UP/USDT,"):
            expand_pairlist(wildcardlist, pairs, keep_invalid=True)
    else:
        assert sorted(expand_pairlist(wildcardlist, pairs, keep_invalid=True)) == sorted(expected)


def test_ProducerPairlist_no_emc(mocker, whitelist_conf):
    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))

    whitelist_conf["pairlists"] = [
        {
            "method": "ProducerPairList",
            "number_assets": 10,
            "producer_name": "hello_world",
        }
    ]
    del whitelist_conf["external_message_consumer"]

    with pytest.raises(
        OperationalException,
        match=r"ProducerPairList requires external_message_consumer to be enabled.",
    ):
        get_patched_freqtradebot(mocker, whitelist_conf)


def test_ProducerPairlist(mocker, whitelist_conf, markets):
    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
    )
    whitelist_conf["pairlists"] = [
        {
            "method": "ProducerPairList",
            "number_assets": 2,
            "producer_name": "hello_world",
        }
    ]
    whitelist_conf.update(
        {
            "external_message_consumer": {
                "enabled": True,
                "producers": [
                    {
                        "name": "hello_world",
                        "host": "null",
                        "port": 9891,
                        "ws_token": "dummy",
                    }
                ],
            }
        }
    )

    exchange = get_patched_exchange(mocker, whitelist_conf)
    dp = DataProvider(whitelist_conf, exchange, None)
    pairs = ["ETH/BTC", "LTC/BTC", "XRP/BTC"]
    # different producer
    dp._set_producer_pairs([*pairs, "MEEP/USDT"], "default")
    pm = PairListManager(exchange, whitelist_conf, dp)
    pm.refresh_pairlist()
    assert pm.whitelist == []
    # proper producer
    dp._set_producer_pairs(pairs, "hello_world")
    pm.refresh_pairlist()

    # Pairlist reduced to 2
    assert pm.whitelist == pairs[:2]
    assert len(pm.whitelist) == 2
    whitelist_conf["exchange"]["pair_whitelist"] = ["TKN/BTC"]

    whitelist_conf["pairlists"] = [
        {"method": "StaticPairList"},
        {
            "method": "ProducerPairList",
            "producer_name": "hello_world",
        },
    ]
    pm = PairListManager(exchange, whitelist_conf, dp)
    pm.refresh_pairlist()
    assert len(pm.whitelist) == 4
    assert pm.whitelist == ["TKN/BTC", *pairs]


@pytest.mark.usefixtures("init_persistence")
def test_FullTradesFilter(mocker, default_conf_usdt, fee, caplog) -> None:
    default_conf_usdt["exchange"]["pair_whitelist"].extend(["ADA/USDT", "XRP/USDT", "ETC/USDT"])
    default_conf_usdt["pairlists"] = [{"method": "StaticPairList"}, {"method": "FullTradesFilter"}]
    default_conf_usdt["max_open_trades"] = -1
    mocker.patch(f"{EXMS}.exchange_has", MagicMock(return_value=True))
    exchange = get_patched_exchange(mocker, default_conf_usdt)
    pm = PairListManager(exchange, default_conf_usdt)
    pm.refresh_pairlist()

    assert pm.whitelist == ["ETH/USDT", "XRP/USDT", "NEO/USDT", "TKN/USDT"]

    with time_machine.travel("2021-09-01 05:00:00 +00:00") as t:
        create_mock_trades_usdt(fee)
        pm.refresh_pairlist()

        # Unlimited max open trades, so no change to whitelist
        pm.refresh_pairlist()
        assert pm.whitelist == ["ETH/USDT", "XRP/USDT", "NEO/USDT", "TKN/USDT"]

        # Set max_open_trades to 4, the filter should empty the whitelist
        default_conf_usdt["max_open_trades"] = 4
        pm.refresh_pairlist()
        assert pm.whitelist == []
        assert log_has_re(r"Whitelist with 0 pairs: \[]", caplog)

        list_trades = LocalTrade.get_open_trades()
        assert len(list_trades) == 4

        # Move to 1 hour later, close a trade, so original sorting is restored.
        t.move_to("2021-09-01 07:00:00 +00:00")
        list_trades[2].close(12)
        Trade.commit()

        # open trades count below max_open_trades, whitelist restored
        list_trades = LocalTrade.get_open_trades()
        assert len(list_trades) == 3
        pm.refresh_pairlist()
        assert pm.whitelist == ["ETH/USDT", "XRP/USDT", "NEO/USDT", "TKN/USDT"]

        # Set max_open_trades to 3, the filter should empty the whitelist
        default_conf_usdt["max_open_trades"] = 3
        pm.refresh_pairlist()
        assert pm.whitelist == []
        assert log_has_re(r"Whitelist with 0 pairs: \[]", caplog)


@pytest.mark.parametrize(
    "pairlists,trade_mode,result,coin_market_calls",
    [
        (
            [
                # Get 2 pairs
                {"method": "StaticPairList", "allow_inactive": True},
                {"method": "MarketCapPairList", "number_assets": 2},
            ],
            "spot",
            ["BTC/USDT", "ETH/USDT"],
            1,
        ),
        (
            [
                # Get 6 pairs
                {"method": "StaticPairList", "allow_inactive": True},
                {"method": "MarketCapPairList", "number_assets": 6},
            ],
            "spot",
            ["BTC/USDT", "ETH/USDT", "XRP/USDT", "ADA/USDT"],
            1,
        ),
        (
            [
                # Get 3 pairs within top 6 ranks
                {"method": "StaticPairList", "allow_inactive": True},
                {"method": "MarketCapPairList", "max_rank": 6, "number_assets": 3},
            ],
            "spot",
            ["BTC/USDT", "ETH/USDT", "XRP/USDT"],
            1,
        ),
        (
            [
                # Get 4 pairs within top 8 ranks
                {"method": "StaticPairList", "allow_inactive": True},
                {"method": "MarketCapPairList", "max_rank": 8, "number_assets": 4},
            ],
            "spot",
            ["BTC/USDT", "ETH/USDT", "XRP/USDT"],
            1,
        ),
        (
            [
                # MarketCapPairList as generator
                {"method": "MarketCapPairList", "number_assets": 5}
            ],
            "spot",
            ["BTC/USDT", "ETH/USDT", "XRP/USDT"],
            1,
        ),
        (
            [
                # MarketCapPairList as generator - low max_rank
                {"method": "MarketCapPairList", "max_rank": 2, "number_assets": 5}
            ],
            "spot",
            ["BTC/USDT", "ETH/USDT"],
            1,
        ),
        (
            [
                # MarketCapPairList as generator - futures - low max_rank
                {"method": "MarketCapPairList", "max_rank": 2, "number_assets": 5}
            ],
            "futures",
            ["ETH/USDT:USDT"],
            1,
        ),
        (
            [
                # MarketCapPairList as generator - futures - low number_assets
                {"method": "MarketCapPairList", "number_assets": 2}
            ],
            "futures",
            ["ETH/USDT:USDT", "ADA/USDT:USDT"],
            1,
        ),
        (
            [
                # MarketCapPairList as generator - futures, 1 category
                {"method": "MarketCapPairList", "number_assets": 2, "categories": ["layer-1"]}
            ],
            "futures",
            ["ETH/USDT:USDT", "ADA/USDT:USDT"],
            ["layer-1"],
        ),
        (
            [
                # MarketCapPairList as generator - futures, 1 category
                {
                    "method": "MarketCapPairList",
                    "number_assets": 2,
                    "categories": ["layer-1", "protocol"],
                }
            ],
            "futures",
            ["ETH/USDT:USDT", "ADA/USDT:USDT"],
            ["layer-1", "protocol"],
        ),
    ],
)
def test_MarketCapPairList_filter(
    mocker, default_conf_usdt, trade_mode, markets, pairlists, result, coin_market_calls
):
    test_value = [
        {"symbol": "btc"},
        {"symbol": "eth"},
        {"symbol": "usdt"},
        {"symbol": "bnb"},
        {"symbol": "sol"},
        {"symbol": "xrp"},
        {"symbol": "usdc"},
        {"symbol": "steth"},
        {"symbol": "ada"},
        {"symbol": "avax"},
    ]

    default_conf_usdt["trading_mode"] = trade_mode
    if trade_mode == "spot":
        default_conf_usdt["exchange"]["pair_whitelist"].extend(["BTC/USDT", "ETC/USDT", "ADA/USDT"])
    default_conf_usdt["pairlists"] = pairlists
    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
    )
    mocker.patch(
        "freqtrade.plugins.pairlist.MarketCapPairList.FtCoinGeckoApi.get_coins_categories_list",
        return_value=[
            {"category_id": "layer-1"},
            {"category_id": "protocol"},
            {"category_id": "defi"},
        ],
    )

    gcm_mock = mocker.patch(
        "freqtrade.plugins.pairlist.MarketCapPairList.FtCoinGeckoApi.get_coins_markets",
        return_value=test_value,
    )

    exchange = get_patched_exchange(mocker, default_conf_usdt)

    pm = PairListManager(exchange, default_conf_usdt)
    pm.refresh_pairlist()
    if isinstance(coin_market_calls, int):
        assert gcm_mock.call_count == coin_market_calls
    else:
        assert gcm_mock.call_count == len(coin_market_calls)
        for call in coin_market_calls:
            assert any(
                "category" in c.kwargs and c.kwargs["category"] == call
                for c in gcm_mock.call_args_list
            )

    assert pm.whitelist == result


def test_MarketCapPairList_timing(mocker, default_conf_usdt, markets, time_machine):
    test_value = [
        {"symbol": "btc"},
        {"symbol": "eth"},
        {"symbol": "usdt"},
        {"symbol": "bnb"},
        {"symbol": "sol"},
        {"symbol": "xrp"},
        {"symbol": "usdc"},
        {"symbol": "steth"},
        {"symbol": "ada"},
        {"symbol": "avax"},
    ]

    default_conf_usdt["trading_mode"] = "spot"
    default_conf_usdt["exchange"]["pair_whitelist"].extend(["BTC/USDT", "ETC/USDT", "ADA/USDT"])
    default_conf_usdt["pairlists"] = [{"method": "MarketCapPairList", "number_assets": 2}]

    markets_mock = MagicMock(return_value=markets)
    mocker.patch.multiple(
        EXMS,
        get_markets=markets_mock,
        exchange_has=MagicMock(return_value=True),
    )

    mocker.patch(
        "freqtrade.plugins.pairlist.MarketCapPairList.FtCoinGeckoApi.get_coins_markets",
        return_value=test_value,
    )

    start_dt = dt_now()

    exchange = get_patched_exchange(mocker, default_conf_usdt)
    time_machine.move_to(start_dt)

    pm = PairListManager(exchange, default_conf_usdt)
    markets_mock.reset_mock()
    pm.refresh_pairlist()
    assert markets_mock.call_count == 3
    markets_mock.reset_mock()

    time_machine.move_to(start_dt + timedelta(hours=20))
    pm.refresh_pairlist()
    # Cached pairlist ...
    assert markets_mock.call_count == 1

    markets_mock.reset_mock()
    time_machine.move_to(start_dt + timedelta(days=2))
    pm.refresh_pairlist()
    # No longer cached pairlist ...
    assert markets_mock.call_count == 3


def test_MarketCapPairList_filter_special_no_pair_from_coingecko(
    mocker,
    default_conf_usdt,
    markets,
):
    default_conf_usdt["pairlists"] = [{"method": "MarketCapPairList", "number_assets": 2}]

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
    )

    # Simulate no pair returned from coingecko
    gcm_mock = mocker.patch(
        "freqtrade.plugins.pairlist.MarketCapPairList.FtCoinGeckoApi.get_coins_markets",
        return_value=[],
    )

    exchange = get_patched_exchange(mocker, default_conf_usdt)

    pm = PairListManager(exchange, default_conf_usdt)
    pm.refresh_pairlist()
    assert gcm_mock.call_count == 1
    assert pm.whitelist == []


def test_MarketCapPairList_exceptions(mocker, default_conf_usdt, caplog):
    exchange = get_patched_exchange(mocker, default_conf_usdt)
    default_conf_usdt["pairlists"] = [{"method": "MarketCapPairList"}]
    with pytest.raises(OperationalException, match=r"`number_assets` not specified.*"):
        # No number_assets
        PairListManager(exchange, default_conf_usdt)

    default_conf_usdt["pairlists"] = [
        {"method": "MarketCapPairList", "number_assets": 20, "max_rank": 500}
    ]
    with caplog.at_level(logging.WARNING):
        PairListManager(exchange, default_conf_usdt)
    assert log_has_re("The max rank you have set \\(500\\) is quite high", caplog)
    # Test invalid coinmarkets list
    mocker.patch(
        "freqtrade.plugins.pairlist.MarketCapPairList.FtCoinGeckoApi.get_coins_categories_list",
        return_value=[
            {"category_id": "layer-1"},
            {"category_id": "protocol"},
            {"category_id": "defi"},
        ],
    )
    default_conf_usdt["pairlists"] = [
        {
            "method": "MarketCapPairList",
            "number_assets": 20,
            "categories": ["layer-1", "defi", "layer250"],
        }
    ]
    with pytest.raises(
        OperationalException, match="Category layer250 not in coingecko category list."
    ):
        PairListManager(exchange, default_conf_usdt)


@pytest.mark.parametrize(
    "pairlists,expected_error,expected_warning",
    [
        (
            [{"method": "StaticPairList"}],
            None,  # Error
            None,  # Warning
        ),
        (
            [{"method": "VolumePairList", "number_assets": 10}],
            "VolumePairList",  # Error
            None,  # Warning
        ),
        (
            [{"method": "MarketCapPairList", "number_assets": 10}],
            None,  # Error
            r"MarketCapPairList.*lookahead.*",  # Warning
        ),
        (
            [{"method": "StaticPairList"}, {"method": "FullTradesFilter"}],
            None,  # Error
            r"FullTradesFilter do not generate.*",  # Warning
        ),
        (  # combi, fails and warns
            [
                {"method": "VolumePairList", "number_assets": 10},
                {"method": "MarketCapPairList", "number_assets": 10},
            ],
            "VolumePairList",  # Error
            r"MarketCapPairList.*lookahead.*",  # Warning
        ),
    ],
)
def test_backtesting_modes(
    mocker, default_conf_usdt, pairlists, expected_error, expected_warning, caplog, markets, tickers
):
    default_conf_usdt["runmode"] = "dry_run"
    default_conf_usdt["pairlists"] = pairlists

    mocker.patch.multiple(
        EXMS,
        markets=PropertyMock(return_value=markets),
        exchange_has=MagicMock(return_value=True),
        get_tickers=tickers,
    )
    exchange = get_patched_exchange(mocker, default_conf_usdt)

    # Dry run mode - works always
    PairListManager(exchange, default_conf_usdt)

    default_conf_usdt["runmode"] = "backtest"
    if expected_error:
        with pytest.raises(OperationalException, match=f"Pairlist Handlers {expected_error}.*"):
            PairListManager(exchange, default_conf_usdt)

    if not expected_error:
        PairListManager(exchange, default_conf_usdt)

    if expected_warning:
        assert log_has_re(f"Pairlist Handlers {expected_warning}", caplog)
