import requests
import json
import numpy as np
import pandas as pd
from datenano import datetime, SECOND, MINUTE, time_as_nano
from datetime import datetime as pydatetime
from typing import Dict, List, Any
import statistics
from collections import OrderedDict
import math


class CandleExtended:
    def __init__(self, time=0., period: int = MINUTE):

        self.Time = time_as_nano(time)

        self.Period = period
        self.Open = np.float(0)
        self.High = np.float(0)
        self.Low = np.float(0)
        self.Close = np.float(0)
        self.PriceAverage = np.float(0)
        self.TradeAverage = np.float(0)
        self.SellAverage = np.float(0)
        self.BuyAverage = np.float(0)
        self.TradeMedian = np.float(0)
        self.Volume = np.float(0)
        self.TradeCount = np.float(0)

        self.lastTime = self.Time
        self.totals = []
        self.buy_totals = []
        self.sell_totals = []
        self.amounts = []
        self.prices = []
        self.endTime = self.Time + period - 1

    def add_trade(self, time, price: float, volume: float, sell=False):

        time = time_as_nano(time)

        if time > self.endTime:
            return False

        trade_cost = price * volume
        self.TradeCount += 1
        self.prices.append(price)
        self.amounts.append(volume)
        self.amounts.sort()
        self.totals.append(trade_cost)
        self.totals.sort()

        if self.Open == 0 or time < self.lastTime:
            self.Open = price
        if self.Close == 0 or time > self.lastTime:
            self.Close = price
        if self.High == 0 or price > self.High:
            self.High = price
        if self.Low == 0 or price < self.Low:
            self.Low = price

        self.Volume += volume
        if sell:
            self.sell_totals.append(trade_cost)
            self.SellAverage = statistics.fmean(self.sell_totals)
        else:
            self.buy_totals.append(trade_cost)
            self.BuyAverage = statistics.fmean(self.buy_totals)

        # calc totals
        self.TradeMedian = statistics.median(self.totals)
        self.TradeAverage = statistics.fmean(self.totals)
        self.PriceAverage = statistics.fmean(self.prices)
        self.lastTime = time

    def time_in_candle(self, time: int) -> bool:
        return self.Time <= time <= self.endTime

    @property
    def ordered_dict(self) -> OrderedDict:
        return OrderedDict([
            ("Time", self.Time),
            ("Period", self.Period),
            ("Open", self.Open),
            ("High", self.High),
            ("Low", self.Low),
            ("Close", self.Close),
            ("PriceAverage", self.PriceAverage),
            ("TradeAverage", self.TradeAverage),
            ("SellAverage", self.SellAverage),
            ("BuyAverage", self.BuyAverage),
            ("TradeMedian", self.TradeMedian),
            ("Volume", self.Volume),
            ("TradeCount", self.TradeCount),
        ])
