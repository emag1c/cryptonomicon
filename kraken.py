from datenano import datetime
from typing import Union
from time import sleep
from chart import Chart
import logging
import requester

log = logging.getLogger("kraken")


class KrakenAPIException(Exception):
    pass


class OHLC:
    trades_api_url = "https://api.kraken.com/0/public/Trades?pair={pair}&since={since}"

    def __init__(self,
                 req: requester.Base,
                 pair: str,
                 period: int,
                 start: Union[datetime, bool],
                 end: Union[datetime, bool],
                 file: str,
                 retry_delay: int = 1):
        self.r = req
        self.pair = pair
        self.period = period
        self.retry_delay = retry_delay
        self.start = start
        self.end = end
        self.file = file

        if not file:
            log.warning(f"file not set! Will only print data to stdout")

        self.__attempt = 0
        self.__file_saved = False
        self.__last = start

    def next(self) -> int:
        if self.__last:
            body = self.r.get_json(self.trades_api_url.format(pair=self.pair,
                                                              since=int(self.__last.timestamp_nano())), {})
        else:
            body = self.r.get_json(self.trades_api_url.format(pair=self.pair, since=0), {})

        if "error" in body.keys() and len(body["error"]) > 0:
            log.error("Kaken API error getting trade history: " + body["error"])
            self.__attempt += 1
            sleep(self.retry_delay)
            if self.__attempt > 3:
                raise KrakenAPIException(body["error"])
            # try again
            return self.next()

        # reset attempt count
        self.__attempt = 0

        if "result" not in body.keys():
            return 0

        self.__last = datetime.from_ns(float(body["result"].pop("last", 0)))
        log.info(f"Last date: {self.__last.isoformat()}")

        chart = Chart(period=self.period)

        if len(body["result"]) > 0:
            for results in body["result"].values():
                if len(results) < 1:
                    return False

                cnt = 0
                for v in results:
                    date = datetime.fromtimestamp(v[2])
                    if self.end is not False and date >= self.end:
                        return False

                    price = float(v[0])
                    vol = float(v[1])
                    sell = v[3] == "s"
                    chart.add_trade(date, price, vol, sell)
                    cnt += 1

                log.info(f"Added {cnt} trades to chart")

            df = chart.dataframe

            if self.file:
                if not self.__file_saved:
                    df.to_csv(self.file, mode="w", header=True)
                    self.__file_saved = True
                else:
                    df.to_csv(self.file, mode="a", header=False)
            else:
                print(df)

            return df.size

        return 0

    def iterator(self):
        out = self.next()
        yield out
        while out > 0:
            out = self.next()
            yield out


class Client:
    trades_api_url = "https://api.kraken.com/0/public/Trades?pair={pair}&since={since}"

    def __init__(self, req: requester.Base, retry_delay: int = 1):
        self.__r = req
        self.__retry_delay = retry_delay

    def __ohlc(self,
               pair: str,
               period: int,
               start: Union[datetime, bool],
               end: Union[datetime, bool],
               file: str):
        t = OHLC(self.__r, pair=pair, period=period, start=start, end=end, file=file)
        cnt = 0
        for next_cnt in t.iterator():
            cnt += next_cnt
        log.info(f"download finished. saved {cnt} {pair} trades to {file}")
        return

    def ohlc(self,
             pair: str,
             period: int,
             start: Union[datetime, bool],
             end: Union[datetime, bool],
             file: str):
        return self.__ohlc(pair, period, start, end, file)
