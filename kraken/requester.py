import json
import urllib3 as ul3
import logging
from random import randint
from urllib.error import HTTPError
from typing import Dict, Union
import time
from abc import abstractmethod
from scraper_api import ScraperAPIClient

_log = logging.getLogger("proxy")

USER_AGENTS = [
    "Mozilla/5.0 CK={} (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; KTXN)",
    "Mozilla/5.0 (Windows NT 5.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1"
]


class Base:

    @abstractmethod
    def get(self, url: str, headers: Dict[str, str]):
        pass

    @abstractmethod
    def get_json(self, url: str, headers: Union[Dict[str, str], None]):
        pass


class UrlLib(Base):
    def __init__(self,
                 proxy_host: Union[None, str] = None,
                 proxy_proto: str = "http",
                 proxy_auth: str = "",
                 max_attempts: int = 10,
                 num_pools: int = 10,
                 retry_delay: int = 1):
        self.proxy_host = proxy_host
        self.__pool = None
        self.__max_attempts = max_attempts
        self.retry_delay = retry_delay
        if self.proxy_host:
            proxy_url = proxy_proto + "://" + proxy_host
            headers = ul3.make_headers(proxy_basic_auth=proxy_auth) if proxy_auth else None
            self.__pool = ul3.ProxyManager(proxy_url, num_pools=num_pools, headers=headers)
            _log.info(f"{self.__name__}: using proxy f{proxy_host}")
        else:
            self.__pool = ul3.PoolManager(num_pools=num_pools, headers=ul3.make_headers())

    def get(self, url: str, headers: Dict[str, str]):
        return self.__get(url, headers, 0)

    def __get(self, url: str, headers: Dict[str, str], attempt: int = 0):
        if not headers:
            headers = {}

        if "User-Agent" not in headers.keys():
            headers["User-Agent"] = USER_AGENTS[randint(0, len(USER_AGENTS) - 1)]

        try:
            req = self.__pool.urlopen("GET", url, headers=headers)
            return req.data
            # return req.urlopen(req).read()
        except HTTPError as e:
            attempt += 1
            if attempt > self.__max_attempts:
                raise e
            else:
                _log.debug(f"error trying to GET {url}: {e}")
                time.sleep(self.retry_delay)
                self.get(url, headers, attempt)

        return False

    def get_json(self, url: str, headers: Union[Dict[str, str], None]):
        out = self.get(url, headers)
        if out is not False:
            return json.loads(out.decode("utf-8"))
        return False


class ScraperApi(Base):

    def __init__(self, key: str, max_retry):
        self.max_retry = max_retry
        self.client = ScraperAPIClient(key)

    def get(self, url: str, headers: Dict[str, str]):
        if not headers:
            headers = {}

        if "User-Agent" not in headers.keys():
            headers["User-Agent"] = USER_AGENTS[randint(0, len(USER_AGENTS) - 1)]
        return self.client.get(url, headers, retry=self.max_retry).text

    def get_json(self, url: str, headers: Union[Dict[str, str], None]):
        return json.loads(self.get(url, headers))
