from scrapy import Request, signals
from scrapy.crawler import Crawler
import logging
import mmh3

logger = logging.getLogger(__name__)


class SourceUrlDupefilterMiddleware:

    # __slots__ = ("_redis_client", "crawler", "redis_key")

    def __init__(self, crawler):
        self._redis_client = None
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name
        # TODO:去重开关添加

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        crawler.signals.connect(object.spider_opened, signal=signals.spider_opened)
        return object

    async def spider_opened(self, spider):
        client = getattr(spider.crawler, "redis_client", None)
        if client:
            self.redis_client = client

    @property
    def redis_client(self):
        if not self._redis_client:
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_spider_output(self, response, result, spider):
        async for r in result:
            if isinstance(r, Request):
                url = r.meta.get("filter_url")
                if url:
                    hash_value = mmh3.hash128(url)
                    if await self.redis_client.zscore(self.redis_key, hash_value):
                        logger.debug(f"URL 已存在 <{url}>")
                        continue
                    # 这里有问题，只有item有添加权
                    # else:
                    #     hash_mapping = {hash_value: int(time.time() * 1000)}
                    #     add_count = await self.redis_client.zadd(self.redis_key, hash_mapping, nx=True)
                    #     if add_count:  # 新增成功
                    #         logger.debug(f"URL 添加成功 <{url}>")
            yield r
