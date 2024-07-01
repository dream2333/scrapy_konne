import asyncio
from collections import OrderedDict
import logging
import time

from scrapy.crawler import Crawler
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.exceptions import NotConfigured
from scrapy_konne.constants import LOCALE


logger = logging.getLogger("代理池中间件")


class ProxyPoolDownloaderMiddleware:

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        spider_locale = getattr(crawler.spider, "locale", LOCALE.CN)
        match (spider_locale):
            case LOCALE.CN:
                logger.info("选择境内代理池")
                proxy_cls = RedisProxyPoolDownloaderMiddleware
            case _:
                logger.info("选择境外代理池，所有请求默认全部走代理")
                proxy_cls = ExtraTerritoryProxyDownloaderMiddleware
        return proxy_cls.from_crawler(crawler)


class ExtraTerritoryProxyDownloaderMiddleware(object):

    def __init__(self, v2ray_tunnel_url: str) -> None:
        self.v2ray_tunnel_url = v2ray_tunnel_url

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        v2ray_tunnel_url = crawler.settings.get("OVERSEA_PROXY_URL")
        if not v2ray_tunnel_url:
            raise NotConfigured("请配置境外代理地址")
        return cls(v2ray_tunnel_url)

    def process_request(self, request, spider):
        request.meta["proxy"] = self.v2ray_tunnel_url


class RedisProxyPoolDownloaderMiddleware:

    def __init__(self, crawler: Crawler) -> None:
        self.catch_exceptions = [TunnelError]
        self._proxies_cache = OrderedDict()
        self.expired_duration_ms = crawler.settings.getfloat("PROXY_EXPRIED_TIME", 30) * 1000
        self.prefetch_nums = crawler.settings.getint("PROXY_PREFETCH_NUMS", 64)
        self.empty_wait_time = crawler.settings.getfloat("PROXY_EMPTY_WAIT_TIME", 5)
        self._sem = asyncio.Semaphore(1)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        return object

    @property
    def redis_client(self):
        if not getattr(self, "_redis_client", None):
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_request(self, request, spider):
        if request.meta.get("rotate_proxy", False) and not request.meta.get("proxy"):
            request.meta["proxy"] = await self.get_proxy()

    async def process_exception(self, request, exception, spider):
        if request.meta.get("rotate_proxy"):
            logger.debug(f"请求异常，切换代理 {request}: {exception}")
            for Exce in self.catch_exceptions:
                if isinstance(exception, Exce):
                    return request
            # 如果不是指定的异常，直接切换代理
            request.meta["proxy"] = await self.get_proxy()

    async def get_proxy(self):
        fetch_failed_times = 0
        while True:
            # 使用信号量控制拉取代理的协程数量，且不阻塞其他模块协程
            async with self._sem:
                if self._proxies_cache:
                    # 从代理缓存中取代理，如果代理过期则丢弃
                    proxy_url, proxy_timestamp = self._proxies_cache.popitem(last=False)
                    elapsed_time = int(time.time() * 1000) - proxy_timestamp
                    if elapsed_time > self.expired_duration_ms:
                        logger.debug(f"代理过期{proxy_url}: 来自{elapsed_time}ms前")
                        continue
                    return proxy_url
                # 代理缓存为空时，从redis中取代理,并加入缓存
                if not await self.fetch_proxies():
                    fetch_failed_times += 1
                    if fetch_failed_times >= 5:
                        logger.error("代理池拉取失败次数大于等于5次")
                        fetch_failed_times = 0
                    logger.warn(f"代理池为空，等待{self.empty_wait_time}秒")
                    await asyncio.sleep(self.empty_wait_time)

    async def fetch_proxies(self):
        """
        从远程代理池拉取代理, 并加入缓存
        拉取到代理返回True，否则返回False
        """
        result = await self.redis_client.zpopmin("proxies_pool", self.prefetch_nums)
        if not result:
            return False
        logger.debug(f"从远程代理池拉取代理{len(result)}个")
        for proxy_url, proxy_timestamp in result:
            proxy_url = proxy_url.decode("utf-8")
            proxy_timestamp = int(proxy_timestamp)
            self._proxies_cache[proxy_url] = proxy_timestamp
        return True
