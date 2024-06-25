import time
from aiohttp import ClientSession
import mmh3
import logging
from datetime import datetime, timedelta
from scrapy import Spider,signals
from scrapy.crawler import Crawler
from scrapy_konne.items import DetailDataItem, IncreamentItem
from scrapy_konne.exceptions import MemorySetDuplicateItem, RemoteDuplicateItem
from scrapy_konne.exceptions import ExpriedItem


logger = logging.getLogger(__name__)


async def add_fp_to_redis(redis_key: str, redis_client, item: DetailDataItem | IncreamentItem):
    """将指纹添加到redis缓存"""
    if isinstance(item, IncreamentItem):
        fp = item.increment_id
    else:
        fp = mmh3.hash128(item.source_url)
    hash_mapping = {fp: int(time.time() * 1000)}
    await redis_client.zadd(redis_key, hash_mapping, nx=True)


class RedisFilteredUrlUploaderPipeline:
    """
    提交上传成功的Url到redis缓存，用于请求过滤。
    """

    def __init__(self, crawler: Crawler):
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

    async def process_item(self, item: DetailDataItem, spider: Spider):
        await add_fp_to_redis(self.redis_key, self.redis_client, item)
        return item


class SetFilterPipeline:
    """在本地进行预去重，避免重复上传。"""

    url_seen = set()

    def process_item(self, item: DetailDataItem, spider: Spider):
        url = item.source_url
        if url in self.url_seen:
            raise MemorySetDuplicateItem(f"url已经在本地存在，不需要上传: {url}")
        self.url_seen.add(url)
        return item


class TimeFilterPipeline:
    """超过时效的数据预过滤，并加入redis缓存"""

    def __init__(self, crawler: Crawler) -> None:
        self.expired_time = crawler.settings.getint("ITEM_FILTER_TIME", 72)
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(crawler)

    @property
    def redis_client(self):
        if not getattr(self, "_redis_client", None):
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_item(self, item: DetailDataItem, spider: Spider):
        # 时区转换
        dis_time = datetime.now().astimezone() - timedelta(hours=self.expired_time)
        if item.publish_time < dis_time:
            await add_fp_to_redis(self.redis_key, self.redis_client, item)
            raise ExpriedItem(f"发布时间超过{self.expired_time}，不需要上传: {item}")
        return item


class KonneHttpFilterPipeline:
    """对konne库中已存在的url进行过滤, 并加入redis缓存"""

    uri_deduplication_api: str

    def open_spider(self, spider: Spider):
        self.crawler = spider.crawler
        self.redis_key = "dupefilter:" + spider.name
        self.session = ClientSession()

    async def spider_closed(self, spider: Spider):
        logger.info("正在关闭数据上传管道")
        await self.session.close()
        logger.info("数据上传管道已关闭")
        self.session = ClientSession()


    @classmethod
    def from_crawler(cls, crawler: Crawler):
        upload_ip = crawler.settings.get("UPLOAD_DATA_IP")
        filter = cls()
        filter.uri_deduplication_api = f"http://{upload_ip}/QuChong/ExistUrl"
        crawler.signals.connect(filter.spider_closed, signal=signals.spider_closed)
        return filter

    async def is_url_exist(self, url):
        filter_url = self.uri_deduplication_api
        params = {"url": url}
        async with self.session.get(filter_url, params=params) as response:
            result = await response.json()
            if isinstance(result, int):
                return bool(result)

    @property
    def redis_client(self):
        if not getattr(self, "_redis_client", None):
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_item(self, item: DetailDataItem, spider: Spider):
        url = item.source_url
        if await self.is_url_exist(url):
            await add_fp_to_redis(self.redis_key, self.redis_client, item)
            raise RemoteDuplicateItem(f"url已经在http去重库存在，不需要上传: {url}")
        return item
