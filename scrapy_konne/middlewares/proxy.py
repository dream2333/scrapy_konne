import asyncio
from collections import OrderedDict
import logging
import time

import redis.asyncio as redis
from scrapy.crawler import Crawler
from scrapy.core.downloader.handlers.http11 import TunnelError
from twisted.internet.error import TimeoutError

logger = logging.getLogger(__name__)


class ProxyPoolDownloaderMiddleware:

    def __init__(self, redis_url, prefetch_nums, expired_duration_ms, empty_wait_time) -> None:
        self.redis_client = redis.Redis.from_url(redis_url, protocol=3)
        self.proxy_change_exception = [
            TunnelError,
            TimeoutError,
        ]
        self._proxies_cache = OrderedDict()
        self.expired_duration_ms = expired_duration_ms
        self.prefetch_nums = prefetch_nums
        self.empty_wait_time = empty_wait_time
        self._sem = asyncio.Semaphore(1)

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        redis_url = crawler.settings.get("REDIS_URL")
        prefetch_nums = crawler.settings.get("PROXY_PREFETCH_NUMS", 64)
        expired_duration = crawler.settings.get("PROXY_EXPRIED_TIME", 30) * 1000
        empty_wait_time = crawler.settings.get("PROXY_EMPTY_WAIT_TIME", 3)
        s = cls(redis_url, prefetch_nums, expired_duration, empty_wait_time)
        return s

    async def get_proxy(self):
        async with self._sem:
            while True:
                if self._proxies_cache:
                    # 从代理缓存中取代理，如果代理过期则丢弃
                    proxy_url, proxy_timestamp = self._proxies_cache.popitem(last=False)
                    elapsed_time = int(time.time() * 1000) - proxy_timestamp
                    if elapsed_time > self.expired_duration_ms:
                        logger.debug(
                            "代理过期 %(proxy_url)s: 来自%(elapsed_time)dms前",
                            {"proxy_url": proxy_url, "elapsed_time": elapsed_time},
                        )
                        continue
                    return proxy_url
                else:
                    # 代理缓存为空时，从redis中取代理,并加入缓存
                    result = await self.redis_client.zpopmin("proxies_pool", self.prefetch_nums)
                    if not result:
                        logger.warning("代理池为空，等待%(wait_time)s秒", {"wait_time": self.empty_wait_time})
                        await asyncio.sleep(self.empty_wait_time)
                        continue
                    logger.debug("从远程代理池拉取代理%(count)d个", {"count": len(result)})
                    for proxy_url, proxy_timestamp in result:
                        proxy_url = proxy_url.decode("utf-8")
                        proxy_timestamp = int(proxy_timestamp)
                        self._proxies_cache[proxy_url] = proxy_timestamp

    async def process_request(self, request, spider):
        if request.meta.get("use_proxy", False) and not request.meta.get("proxy"):
            request.meta["proxy"] = await self.get_proxy()

    async def process_exception(self, request, exception, spider):
        for ex in self.proxy_change_exception:
            if isinstance(exception, ex) and request.meta.get("use_proxy", False):
                request.meta["proxy"] = await self.get_proxy()
                logger.debug(
                    "请求异常，切换代理 %(request)s: \n%(exception)s",
                    {"request": request, "exception": exception},
                    extra={"spider": spider},
                )
                break
        else:
            request.meta["proxy"] = await self.get_proxy()
            logger.warning(
                "未捕获的异常，切换代理 %(request)s: \n%(exception)s",
                {"request": request, "exception": exception},
                extra={"spider": spider},
            )
