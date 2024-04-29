import asyncio

from time import time
from scrapy import signals
import redis.asyncio as redis
from scrapy.crawler import Crawler
from scrapy.utils.log import logger


class ProxyPoolDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self, redis_url) -> None:
        self.redis_client = redis.Redis.from_url(redis_url)
        self.proxies_buffer = asyncio.Queue()

    async def spider_opened(self, spider):
        spider.logger.info(f"爬虫打开，等待3秒运行: {spider.name}")
        self.proxies_getter_task = asyncio.create_task(self.add_proxy_to_queue())
        await asyncio.sleep(3)

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        # This method is used by Scrapy to create your spiders.
        redis_url = crawler.settings.get("PROXY_POOL_REDIS_URL")
        s = cls(redis_url)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    async def add_proxy_to_queue(self):
        while True:
            if self.proxies_buffer.qsize() <= 10:
                logger.debug("代理池小于10，正在补充")
                datas = await self.redis_client.zpopmax("proxies_pool", 20)
                if datas:
                    for data in datas:
                        proxy = data[0].decode("utf-8")
                        timestamp = int(data[1])
                        self.proxies_buffer.put_nowait((proxy, timestamp))
                else:
                    logger.warning("redis代理池为空")
                    await asyncio.sleep(1)
            else:
                # 代理池大于32，不需要补充，休息一阵子
                await asyncio.sleep(0.2)

    async def get_valid_proxy(self, expire=20000):
        """获取一个代理，如果代理过期则重新获取"""
        proxy, timestamp = await self.proxies_buffer.get()
        if timestamp < int(time() * 1000) - expire:
            logger.debug("代理过期，重新获取")
            return await self.get_valid_proxy()
        return proxy

    async def process_request(self, request, spider):
        # 从redis中获取一个代理
        proxy = await self.get_valid_proxy()
        request.meta["proxy"] = proxy
        return None

    async def process_exception(self, request, exception, spider):
        proxy = await self.get_valid_proxy()
        request.meta["proxy"] = proxy
        return request


class ProxyPoolNoBufferDownloaderMiddleware:
    """使用代理池，但取出时不使用缓冲区，直接从redis中取出"""

    def __init__(self, redis_url) -> None:
        self.redis_client = redis.Redis.from_url(redis_url, protocol=3)

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        # This method is used by Scrapy to create your spiders.
        redis_url = crawler.settings.get("PROXY_POOL_REDIS_URL")
        s = cls(redis_url)
        return s

    async def process_request(self, request, spider):
        # 从redis中获取一个代理
        proxy = await self.get_proxy()
        request.meta["proxy"] = proxy
        return None

    async def get_proxy(self):
        while True:
            result = await self.redis_client.zpopmax("proxies_pool", 1)
            if result:
                proxy = result[0][0].decode("utf-8")
                return proxy
            else:
                logger.warning("redis代理池为空,休息一秒")
                await asyncio.sleep(1)

    async def process_exception(self, request, exception, spider):
        proxy = await self.get_proxy()
        request.meta["proxy"] = proxy
        request.dont_filter = True
        logger.info("换代理 " + request.url)
        return request
