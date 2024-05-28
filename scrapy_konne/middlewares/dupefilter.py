import asyncio
from scrapy import Request
from scrapy.crawler import Crawler
import logging
import mmh3

logger = logging.getLogger(__name__)


class UrlRedisDupefilterMiddleware:

    def __init__(self, crawler: Crawler):
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name
        self.loop = asyncio.get_event_loop()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        return object

    def get_redis_client(self, sync=False):
        """获取redis连接，同步或异步"""
        if not sync:
            if not getattr(self, "_async_redis_client", None):
                self._async_redis_client = getattr(self.crawler, "redis_client", None)
            return self._async_redis_client
        else:
            if not getattr(self, "_sync_redis_client", None):
                self._sync_redis_client = getattr(self.crawler, "sync_redis_client", None)
            return self._sync_redis_client

    def process_start_requests(self, start_requests, spider):
        """处理start_requests,只能用同步redis连接"""
        for request in start_requests:
            if self.is_dup_request_sync(request):
                self.crawler.stats.inc_value("dupefilter/redis", spider=spider)
                logger.debug(f"URL 已存在 <{request.meta['filter_url']}>")
                continue
            yield request
        self.get_redis_client(True).close()
        logger.info("关闭同步redis连接")

    async def process_spider_output(self, response, result, spider):
        async for r in result:
            if isinstance(r, Request):
                if await self.is_dup_request(r):
                    self.crawler.stats.inc_value("dupefilter/redis", spider=spider)
                    logger.debug(f"URL 已存在 <{r.meta['filter_url']}>")
                    continue
            yield r

    async def is_dup_request(self, request):
        if request.dont_filter is False:
            cursor = request.meta.get("cursor")
            if cursor:
                if self.get_redis_client().zscore(self.redis_key, str(cursor)):
                    return True
                return False
            url = request.meta.get("filter_url")
            if url:
                hash_value = mmh3.hash128(request.meta["filter_url"])
                if self.get_redis_client().zscore(self.redis_key, hash_value):
                    return True
        return False

    def is_dup_request_sync(self, request):
        if request.dont_filter is False:
            cursor = request.meta.get("cursor")
            if cursor:
                if self.get_redis_client(sync=True).zscore(self.redis_key, str(cursor)):
                    return True
                return False
            url = request.meta.get("filter_url")
            if url:
                hash_value = mmh3.hash128(request.meta["filter_url"])
                if self.get_redis_client(sync=True).zscore(self.redis_key, hash_value):
                    return True
        return False
