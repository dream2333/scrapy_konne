from scrapy.dupefilters import BaseDupeFilter


from scrapy.http.request import Request

from scrapy.utils.request import (
    RequestFingerprinter,
    RequestFingerprinterProtocol,
)
from scrapy.crawler import Crawler
from mmh3 import hash128

class SourceUrlDupeFilter(BaseDupeFilter):

    def __init__(self,crawler:Crawler) -> None:
        self.file = None
        self.fingerprinter: RequestFingerprinterProtocol = RequestFingerprinter()
        self.fingerprints = set()
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        return object
    
    @property
    def redis_client(self):
        if not getattr(self, "_redis_client", None):
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    def request_seen(self, request: Request) -> bool:
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        
        return False

    def request_fingerprint(self, request: Request) -> str:
        return self.fingerprinter.fingerprint(request).hex()

    def in_zset(self, url: str) -> str:
        ...
