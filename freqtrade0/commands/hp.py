all = "all"
buy = "buy"
sell = "sell"
roi = "roi"
stoploss = "stoploss"
trailing = "trailing"
protection = "protection"
trades = "trades"
default = "default"

ProfitDrawDownHyperOptLoss = "ProfitDrawDownHyperOptLoss"
SharpeHyperOptLoss = "SharpeHyperOptLoss"
SharpeHyperOptLossDaily = "SharpeHyperOptLossDaily"
SortinoHyperOptLossDaily = "SortinoHyperOptLossDaily"
ProfitDrawDownHyperOptLoss = "ProfitDrawDownHyperOptLoss"


def join_hpo(*params: str):
    return " ".join(params)
