# flake8: noqa: F401
# isort: off
from freqtrade.resolvers.iresolver import IResolver


# isort: on
# Don't import HyperoptResolver to avoid loading the whole Optimize tree
# from freqtrade.resolvers.hyperopt_resolver import HyperOptResolver
from freqtrade.resolvers.pairlist_resolver import PairListResolver
from freqtrade.resolvers.protection_resolver import ProtectionResolver


from freqtrade0.resolvers.strategy_resolver import StrategyResolver
from freqtrade0.resolvers.exchange_resolver import ExchangeResolver