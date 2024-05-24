from logging import getLogger
from redis.asyncio import Redis, RedisError
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy.exceptions import NotConfigured
from asyncio import Lock

logger = getLogger(__name__)


class RedisExtensionError(Exception):
    pass


class GlobalRedisExtension:
    def __init__(self, redis_url):
        self.redis_url = redis_url
        self.redis_client = None
        self._lock = Lock()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        redis_url = crawler.settings.get("REDIS_URL")
        if not redis_url:
            raise NotConfigured("REDIS_URL 未设置")
        ext = cls(redis_url)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    async def create_connection(self, redis_url):
        """创建redis连接"""
        logger.info("开始连接redis...")
        try:
            redis_client = Redis.from_url(redis_url, socket_timeout=10, protocol=3)
            if await self.is_connection_alive(redis_client):
                return redis_client
        except RedisError as e:
            raise RedisExtensionError(f"Redis连接错误: {e}")
        except (ValueError, TypeError):
            raise RedisExtensionError("Redis连接参数错误")
        except Exception as e:
            raise RedisExtensionError(f"连接redis失败，未知错误: {e}")

    async def is_connection_alive(self, client):
        """检查是否有可用的redis连接和连接是否正常"""
        pong = await client.ping()
        return bool(pong)

    async def spider_opened(self, spider):
        async with self._lock:
            try:
                self.redis_client = await self.create_connection(self.redis_url)
                spider.crawler.redis_client = self.redis_client
                logger.info("挂载redis全局连接成功")
            except Exception as e:
                logger.error("%(error)s", {"error": e})
                spider.crawler.engine.close_spider(spider, "redis_error")

    async def spider_closed(self, spider):
        if self.redis_client:
            await self.redis_client.close()
