import requests
import json
import numpy as np
import pandas as pd
from datenano import datetime
from typing import Dict, List, Any
import statistics
from collections import OrderedDict
import math

kraken_trades_api_url = "https://api.kraken.com/0/public/Trades?pair={pair}&since={since}"


class RequestError(Exception):
    def __init__(self, values):
        self.values = values



def get_kraken_ohlc(start: datetime, end: datetime, pair: str, since, p: int = 60):
    if since is not None:
        req = requests.get(kraken_trades_api_url.format(pair=pair, since=since.timestamp_nano()))
    else:
        req = requests.get(kraken_trades_api_url.format(pair=pair, since=0))

    body: dict = json.loads(str(req.content))

    if "error" in body.keys() and len(body["error"]) > 0:
        raise RequestError(body["error"])

    if "last" in body.keys():
        lastdt = datetime.from_ns(int(body["last"]))

    candle = None

    if "result" in body.keys() and len(body["result"]) > 0:
        for results in body["results"].values():
            for v in results:
                date = datetime.fromtimestamp(v[2])
                price =


if __name__ == '__main__':
