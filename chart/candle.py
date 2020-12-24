import numpy as np
from datenano import MINUTE, time_as_nano
import statistics
from collections import OrderedDict


class CandleExtended:
    def __init__(self, time=0., period: int = MINUTE):

        self.Time = time_as_nano(time)

        self.period = period
        self.open = np.float(0)
        self.high = np.float(0)
        self.low = np.float(0)
        self.close = np.float(0)
        self.price_avg = np.float(0)
        self.trade_avg = np.float(0)
        self.sell_avg = np.float(0)
        self.buy_avg = np.float(0)
        self.trade_med = np.float(0)
        self.volume = np.float(0)
        self.trade_cnt = np.float(0)

        self.last_time = self.Time
        self.totals = []
        self.buy_totals = []
        self.sell_totals = []
        self.amounts = []
        self.prices = []
        self.end_time = self.Time + period - 1

    def add_trade(self, time, price: float, volume: float, sell=False):

        time = time_as_nano(time)

        if time > self.end_time:
            return False

        trade_cost = price * volume
        self.trade_cnt += 1
        self.prices.append(price)
        self.amounts.append(volume)
        self.amounts.sort()
        self.totals.append(trade_cost)
        self.totals.sort()

        if self.open == 0 or time < self.last_time:
            self.open = price
        if self.close == 0 or time > self.last_time:
            self.close = price
        if self.high == 0 or price > self.high:
            self.high = price
        if self.low == 0 or price < self.low:
            self.low = price

        self.volume += volume
        if sell:
            self.sell_totals.append(trade_cost)
            self.sell_avg = statistics.fmean(self.sell_totals)
        else:
            self.buy_totals.append(trade_cost)
            self.buy_avg = statistics.fmean(self.buy_totals)

        # calc totals
        self.trade_med = statistics.median(self.totals)
        self.trade_avg = statistics.fmean(self.totals)
        self.price_avg = statistics.fmean(self.prices)
        self.last_time = time

    def time_in_candle(self, time: int) -> bool:
        return self.Time <= time <= self.end_time

    @property
    def ordered_dict(self) -> OrderedDict:
        return OrderedDict([
            ("Time", self.Time),
            ("Period", self.period),
            ("Open", self.open),
            ("High", self.high),
            ("Low", self.low),
            ("Close", self.close),
            ("PriceAverage", self.price_avg),
            ("TradeAverage", self.trade_avg),
            ("SellAverage", self.sell_avg),
            ("BuyAverage", self.buy_avg),
            ("TradeMedian", self.trade_med),
            ("Volume", self.volume),
            ("TradeCount", self.trade_cnt),
        ])
