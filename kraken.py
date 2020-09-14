from datenano import datetime
from typing import Union
from time import sleep
from chart import Chart
import logging
import requester

log = logging.getLogger("kraken")


class Client:
    trades_api_url = "https://api.kraken.com/0/public/Trades?pair={pair}&since={since}"

    def __init__(self, req: requester.Base, retry_delay: int = 1):
        self.__r = req
        self.__retry_delay = retry_delay

    def ohlc(self,
             pair: str,
             period: int,
             start: Union[datetime, bool],
             end: Union[datetime, bool],
             file: str):

        if not file:
            log.warning(f"file not set! Will only print data to stdout")

        if start is not False:
            body = self.__r.get_json(self.trades_api_url.format(pair=pair, since=int(start.timestamp_nano())),
                                     {})
        else:
            body = self.__r.get_json(self.trades_api_url.format(pair=pair, since=0), {})

        if "error" in body.keys() and len(body["error"]) > 0:
            log.error(body["error"])
            sleep(self.__retry_delay)
            self.ohlc(pair, period, start, end, file)

        if "result" not in body.keys():
            return

        lastdt = datetime.from_ns(float(body["result"].pop("last", 0)))
        log.info(f"Last date: {lastdt.isoformat()}")

        chart = Chart(period=period)
        if len(body["result"]) > 0:
            for results in body["result"].values():
                if len(results) < 1:
                    return

                cnt = 0
                for v in results:
                    date = datetime.fromtimestamp(v[2])
                    if end is not False and date >= end:
                        return

                    price = float(v[0])
                    vol = float(v[1])
                    sell = v[3] == "s"
                    chart.add_trade(date, price, vol, sell)
                    cnt += 1

                log.info(f"Added {cnt} trades to chart")

            df = chart.dataframe
            if file:
                df.to_csv(file, mode="a", header=False)
            else:
                print(df)
        else:
            return

        if end is False or lastdt < end:
            return self.ohlc(pair, period, lastdt, end, file)

        return
