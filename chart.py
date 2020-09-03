from typing import Dict, Any, Union
from collections import OrderedDict
from candle import CandleExtended
from datenano import datetime, time_as_nano
import pandas as pd


class Chart:
    def __init__(self, data=None, period: int=None):
        self.period = period
        if data is not None:
            self.__data = data
        else:
            self.__data: Dict[float, CandleExtended] = {}

    @property
    def dataframe(self):
        return pd.DataFrame(self.ordered_dict.values())

    @property
    def ordered_dict(self) -> OrderedDict:
        od = OrderedDict()
        for k in sorted(self.__data.keys()):
            od[k] = self.__data[k].ordered_dict
        return od

    def add_candle(self, candle: CandleExtended):
        self.__data[candle.Time] = candle.ordered_dict

    def add_trade(self, time: Union[int, datetime], price: float, volume, sell=False):
        # round the time
        if isinstance(time, int):
            time = datetime.from_ns(time)

        start_time = time.floor(self.period).timestamp_nano()

        if start_time not in self.__data.keys():
            self.__data[start_time] = CandleExtended(start_time, self.period)

        self.__data[start_time].add_trade(time, price, volume, sell)

