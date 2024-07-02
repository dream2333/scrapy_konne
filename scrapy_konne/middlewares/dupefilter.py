import asyncio
from typing_extensions import deprecated
from scrapy import Request
from scrapy.crawler import Crawler
import logging
from scrapy_konne.exceptions import RedisDuplicateRequest
from scrapy_konne.utils.fingerprint import get_url_fp


logger = logging.getLogger(__name__)


class UrlRedisDupefilterDownloaderMiddleware:
    def __init__(self, crawler: Crawler):
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name
        self.loop = asyncio.get_event_loop()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        return object

    async def is_dup_request(self, request):
        if request.dont_filter is False:
            # 如果是自增id，则判断id是否存在
            cursor = request.meta.get("cursor")
            if cursor:
                if await self.get_redis_client().zscore(self.redis_key, cursor):
                    return cursor
                return None
            else:
                # 如果是url，则判断url是否存在
                url = request.meta.get("filter_url") or request.url
                fp = get_url_fp(url)
                if await self.get_redis_client().zscore(self.redis_key, fp):
                    return url
        return None

    async def process_request(self, request, spider):
        if key := await self.is_dup_request(request):
            self.crawler.stats.inc_value("dupefilter/redis", spider=spider)
            raise RedisDuplicateRequest(f"请求key已存在 <{key}> : {request.url}")

    def get_redis_client(self, sync=False):
        """获取redis连接，同步或异步"""
        if not getattr(self, "_async_redis_client", None):
            self._async_redis_client = getattr(self.crawler, "redis_client", None)
        return self._async_redis_client


@deprecated("使用 UrlRedisDupefilterDownloaderMiddleware 请求中间件以替代")
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
            if key := self.is_dup_request_sync(request):
                self.crawler.stats.inc_value("dupefilter/redis", spider=spider)
                logger.debug(f"去重key已存在 <{key}>")
                continue
            yield request
        self.get_redis_client(True).close()
        logger.info("关闭同步redis连接")

    async def process_spider_output(self, response, result, spider):
        async for r in result:
            if isinstance(r, Request):
                if key := await self.is_dup_request(r):
                    self.crawler.stats.inc_value("dupefilter/redis", spider=spider)
                    logger.debug(f"去重key已存在 <{key}>")
                    continue
            yield r

    async def is_dup_request(self, request):
        if request.dont_filter is False:
            cursor = request.meta.get("cursor")
            if cursor:
                if await self.get_redis_client().zscore(self.redis_key, cursor):
                    return cursor
                return None
            url = request.meta.get("filter_url")
            if url:
                hash_value = get_url_fp(request.meta["filter_url"])
                if await self.get_redis_client().zscore(self.redis_key, hash_value):
                    return url
        return None

    def is_dup_request_sync(self, request: Request):
        if request.dont_filter is False:
            cursor = request.meta.get("cursor")
            if cursor:
                if self.get_redis_client(sync=True).zscore(self.redis_key, cursor):
                    return cursor
                return None
            url = request.meta.get("filter_url")
            if url:
                hash_value = get_url_fp(request.meta["filter_url"])
                if self.get_redis_client(sync=True).zscore(self.redis_key, hash_value):
                    return cursor
        return None
