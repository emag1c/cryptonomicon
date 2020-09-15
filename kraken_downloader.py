import pandas as pd
from datenano import datetime, SECOND
from typing import Union
import kraken
import argparse
import logging
import requester
import sys

log = logging.getLogger("kraken_downloader")


class RequestError(Exception):
    def __init__(self, values):
        self.values = values


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pair", type=str, required=True, help="the pair to download")
    parser.add_argument("-f", "--file", type=str, required=True, help="the file to save the file")
    parser.add_argument("-s", "--start", type=str, required=False, help="the start date")
    parser.add_argument("-e", "--end", type=str, required=False, help="the end date")
    parser.add_argument("--period", type=str, required=True, help="the period to use")
    parser.add_argument("--proxy-host", type=str, required=False, default=None, help="the proxy host")
    parser.add_argument("--proxy-auth", type=str, required=False, default=None, help="the proxy authorization")
    parser.add_argument("--proxy-proto", type=str, required=False, default=None, help="the proxy protocol")
    parser.add_argument("--scraperapi-key", type=str, required=False, default=None,
                        help="scraperapi key if using scaperapi")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    setup_logging()
    args = parse_args()
    # use pandas to parse the timedelta
    _start_date = False
    _end_date = False
    if args.start:
        _start_date = datetime.from_pdtimestamp(pd.to_datetime(args.start))
    if args.end:
        _end_date = datetime.from_pdtimestamp(pd.to_datetime(args.end))

    _period = pd.to_timedelta(args.period).seconds * SECOND
    _pair = args.pair.lower().strip()
    _file = args.file.strip()

    print(f"SETTINGS:\n"
          f"pair: {_pair}\n"
          f"period: {_period}\n"
          f"file: {args.file}\n"
          f"start: {_start_date.isoformat() if isinstance(_start_date, datetime) else _start_date}\n"
          f"end: {_end_date.isoformat() if isinstance(_end_date, datetime) else _end_date}\n"
          f"proxy host: {args.proxy_host}\n"
          f"proxy host: {args.proxy_proto}\n"
          f"proxy auth: {args.proxy_auth}\n"
          f"scraperapi key: {args.scraperapi_key}")

    req: Union[None, requester.Base] = None
    if args.scraperapi_key:
        req = requester.ScraperApi(key=args.scraperapi_key.strip(), max_retry=3)
    else:
        req = requester.UrlLib(proxy_auth=args.proxy_auth.strip(),
                               proxy_host=args.proxy_host.strip(),
                               proxy_proto=args.proxy_proto.strip())

    getter = kraken.Client(req=req, retry_delay=1)
    getter.ohlc(pair=_pair, period=_period, start=_start_date, end=_end_date, file=_file)
