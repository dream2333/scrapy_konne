import asyncio
from collections import OrderedDict
import logging
import time

from scrapy_konne.utils.connection import GlobalRedis
from scrapy.crawler import Crawler
from scrapy.core.downloader.handlers.http11 import TunnelError
from twisted.internet import reactor
from scrapy import signals

logger = logging.getLogger(__name__)


class ProxyPoolDownloaderMiddleware:

    def __init__(self, crawler: Crawler) -> None:
        self.redis_client = None
        self._proxies_cache = OrderedDict()
        self.expired_duration_ms = crawler.settings.getfloat("PROXY_EXPRIED_TIME", 30) * 1000
        self.prefetch_nums = crawler.settings.getint("PROXY_PREFETCH_NUMS", 64)
        self.empty_wait_time = crawler.settings.getfloat("PROXY_EMPTY_WAIT_TIME", 3)
        self._sem = asyncio.Semaphore(1)

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        crawler.signals.connect(object.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(object.spider_closed, signal=signals.spider_closed)
        return object

    async def spider_opened(self, spider):
        redis_url = spider.crawler.settings.get("REDIS_URL")
        self.redis_client = await GlobalRedis.from_url(redis_url)
        if not self.redis_client:
            reactor.callLater(0, spider.crawler.engine.close_spider, spider, reason="无法连接到Redis服务器")

    def spider_closed(self, spider, reason):
        if self.redis_client:
            self.redis_client.close()

    async def get_proxy(self):
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
                # 使用信号量控制拉取代理的协程数量，且不阻塞其他模块协程
                async with self._sem:
                    # 代理缓存为空时，从redis中取代理,并加入缓存
                    result = await self.redis_client.zpopmin("proxies_pool", self.prefetch_nums)
                    if not result:
                        logger.error("代理池为空，等待%(wait_time)s秒", {"wait_time": self.empty_wait_time})
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
        if request.meta.get("use_proxy"):
            logger.debug(
                "请求异常，切换代理 %(request)s: %(exception)s",
                {"request": request, "exception": exception},
                extra={"spider": spider},
            )
            request.meta["proxy"] = await self.get_proxy()
            if isinstance(exception, TunnelError):
                return request
