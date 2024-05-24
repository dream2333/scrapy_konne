from scrapy import Request, signals
from scrapy.crawler import Crawler
import logging
import mmh3

logger = logging.getLogger(__name__)


class SourceUrlDupefilterMiddleware:

    def __init__(self, crawler: Crawler):
        self._redis_client = None
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        return object

    @property
    def redis_client(self):
        if not self._redis_client:
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_spider_output(self, response, result, spider):
        async for r in result:
            if isinstance(r, Request):
                url = r.meta.get("filter_url")
                if url and r.dont_filter is False:
                    hash_value = mmh3.hash128(url)
                    if await self.redis_client.zscore(self.redis_key, hash_value):
                        self.crawler.stats.inc_value("redis_dupefilter", spider=spider)
                        logger.debug(f"URL 已存在 <{url}>")
                        continue
            yield r
