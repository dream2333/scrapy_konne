import time
import mmh3
import logging
from datetime import datetime, timedelta
from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy_konne.items import DetailDataItem
from scrapy_konne.exceptions import MemorySetDuplicateItem, RemoteDuplicateItem
from scrapy_konne.exceptions import ExpriedItem
from scrapy_konne.pipelines.konnebase import BaseKonneHttpPipeline


logger = logging.getLogger(__name__)


class RedisFilteredUrlUploaderPipeline:
    """
    提交上传成功的Url到redis，用于请求过滤。
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
        url = item.source_url
        hash_value = mmh3.hash128(url)
        hash_mapping = {hash_value: int(time.time() * 1000)}
        await self.redis_client.zadd(self.redis_key, hash_mapping, nx=True)
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
    """超过时效的数据预过滤，并加入redis去重库"""

    def __init__(self, crawler) -> None:
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
        dis_time = datetime.now() - timedelta(hours=self.expired_time)
        if item.publish_time < dis_time:
            url = item.source_url
            hash_value = mmh3.hash128(url)
            hash_mapping = {hash_value: int(time.time() * 1000)}
            await self.redis_client.zadd(self.redis_key, hash_mapping, nx=True)
            raise ExpriedItem(f"发布时间超过{self.expired_time}，不需要上传: {item}")
        return item


class KonneHttpFilterPipeline(BaseKonneHttpPipeline):
    """对konne库中已存在的url进行过滤"""

    async def is_url_exist(self, url):
        filter_url = self.uri_is_exist_url
        params = {"url": url}
        async with self.session.get(filter_url, params=params) as response:
            result = await response.json()
            if isinstance(result, int):
                return bool(result)

    def open_spider(self, spider: Spider):
        super().open_spider(spider)
        self.crawler = spider.crawler
        self.redis_key = "dupefilter:" + spider.name

    @property
    def redis_client(self):
        if not getattr(self, "_redis_client", None):
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_item(self, item: DetailDataItem, spider: Spider):
        url = item.source_url
        if await self.is_url_exist(url):
            hash_value = mmh3.hash128(url)
            hash_mapping = {hash_value: int(time.time() * 1000)}
            await self.redis_client.zadd(self.redis_key, hash_mapping, nx=True)
            raise RemoteDuplicateItem(f"url已经在http去重库存在，不需要上传: {url}")
        return item
