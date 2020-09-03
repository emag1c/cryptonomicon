import requests
import json
import numpy as np
import pandas as pd
from datenano import datetime, HOUR
from typing import Dict, List, Any, Union
import statistics
from collections import OrderedDict
import math
from time import sleep
from chart import Chart

kraken_trades_api_url = "https://api.kraken.com/0/public/Trades?pair={pair}&since={since}"


class RequestError(Exception):
    def __init__(self, values):
        self.values = values


def get_kraken_ohlc(chart: Chart, start: Union[datetime, bool], end: Union[datetime, bool], pair: str, ratelimit=1) -> Chart:
    if start is not False:
        req = requests.get(kraken_trades_api_url.format(pair=pair, since=int(start.timestamp_nano())))
    else:
        req = requests.get(kraken_trades_api_url.format(pair=pair, since=0))

    body: dict = json.loads(str(req.content.decode("utf-8")))

    if "error" in body.keys() and len(body["error"]) > 0:
        print(body["error"])
        sleep(ratelimit * 2)
        get_kraken_ohlc(chart, start, end, pair)

    if "result" not in body.keys():
        return chart

    lastdt = datetime.from_ns(float(body["result"].pop("last", 0)))
    print(int(lastdt.timestamp_nano()))

    if len(body["result"]) > 0:
        for results in body["result"].values():
            if len(results) < 1:
                return chart

            for v in results:
                date = datetime.fromtimestamp(v[2])
                if end is not False and date >= end:
                    return chart

                price = float(v[0])
                vol = float(v[1])
                sell = v[3] == "s"
                chart.add_trade(date, price, vol, sell)
    else:
        return chart

    if end is False or lastdt < end:
        sleep(ratelimit)
        return get_kraken_ohlc(chart, lastdt, end, pair)

    return chart


if __name__ == '__main__':
    # todo: add argparsing and save chart to csv when done
    chart = Chart(period=HOUR)
    chart = get_kraken_ohlc(chart, start=datetime(2019, 1, 1), end=False, pair="xbtusd")

    print(chart.dataframe.tail(100))
