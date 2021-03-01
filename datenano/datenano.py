from __future__ import annotations

import pandas as pd
from datetime import datetime as _datetime, timedelta
import math

NANOSECOND = 1
MICROSECOND = NANOSECOND * 1000
MILLISECOND = MICROSECOND * 1000
SECOND = MILLISECOND * 1000
MINUTE = SECOND * 60
HOUR = MINUTE * 60
DAY = HOUR * 24


def time_as_nano(time) -> float:
    if isinstance(time, datetime):
        return time.timestamp_nano()
    elif isinstance(time, _datetime):
        return time.timestamp() * SECOND
    elif isinstance(time, float):
        if time > 1e17:
            # already nano second
            return time
        else:
            return time * SECOND
    else:
        return time


class datetime(_datetime):

    def timestamp_nano(self) -> float:
        return self.timestamp() * SECOND

    @staticmethod
    def from_ns(nanoseconds) -> datetime:
        return datetime.fromtimestamp(nanoseconds / SECOND)

    @staticmethod
    def from_pydatetime(pydatetime: _datetime) -> datetime:
        return datetime.fromtimestamp(pydatetime.timestamp())

    @staticmethod
    def from_pdtimestamp(timestamp: pd.Timestamp) -> datetime:
        return datetime.from_pydatetime(timestamp.to_pydatetime())

    def __float__(self):
        return self.timestamp_nano

    def __round__(self, n) -> datetime:
        """
        :param n: round to this value. If N is a float, assume float is the time in seconds
        :return: a new ``datetime`` with rounded value
        """
        if isinstance(n, timedelta):
            # use microseconds if timedelta
            n = n.microseconds * MICROSECOND
        elif isinstance(n, float):
            # if float, assume seconds
            n = n * SECOND
        return self.from_ns(round(self.timestamp_nano() / n) * n)

    def floor(self, n) -> datetime:
        if isinstance(n, timedelta):
            # use microseconds if timedelta
            n = n.microseconds * MICROSECOND
        elif isinstance(n, float):
            # if float, assume seconds
            n = n * SECOND
        return self.from_ns(math.floor(self.timestamp_nano() / n) * n)

    def ceil(self, n) -> datetime:
        if isinstance(n, timedelta):
            # use microseconds if timedelta
            n = n.microseconds * MICROSECOND
        elif isinstance(n, float):
            # if float, assume seconds
            n = n * SECOND
        return self.from_ns(math.ceil(self.timestamp_nano() / n) * n)
