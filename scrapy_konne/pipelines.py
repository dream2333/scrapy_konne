import asyncio
import csv
from datetime import datetime, timedelta
import re
import time
from aiohttp import ClientSession
from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy_konne.items import DetailDataItem
from scrapy_konne.exceptions import (
    ItemUploadError,
    LocalDuplicateItem,
    ItemFieldError,
    RemoteDuplicateItem,
    ExpriedItem,
)
from w3lib.html import replace_entities
from twisted.internet.defer import Deferred
import mmh3
import logging

logger = logging.getLogger(__name__)


class LogItemPipeline:
    def process_item(self, item: DetailDataItem, spider: Spider):
        logger.info(item)
        return item


class ItemValidatorPipeline:
    def __init__(self, expired_time) -> None:
        self.expired_time = expired_time
        self.time_pattern = re.compile(
            r"(?P<full>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|(?P<partial>\d{4}-\d{2}-\d{2} \d{2}:\d{2})"
        )

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        expired_time = crawler.settings.getint("ITEM_FILTER_TIME", 72)
        return cls(expired_time)

    def process_item(self, item: DetailDataItem, spider: Spider):
        self.has_none_field(item)
        self.is_time_format_valid(item.publish_time)
        item.publish_time = self.publish_time_to_datetime(item.publish_time)
        dis_time = datetime.now() - timedelta(hours=self.expired_time)
        if item.publish_time < dis_time:
            raise ExpriedItem(f"发布时间超过3天，不需要上传: {item['source_url']}")
        return item

    def timestamp_to_datetime(self, timestamp):
        # 如果是13位时间戳，那么转换成10位时间戳
        if timestamp > 10000000000:
            timestamp /= 1000
        publish_time = datetime.fromtimestamp(timestamp)
        return publish_time

    def str_to_datetime(self, time_str):
        matches = self.time_pattern.match(time_str)
        if matches:
            full = matches.group("full")
            if full:
                # 尝试使用包含秒的格式来解析时间字符串
                return datetime.strptime(full, "%Y-%m-%d %H:%M:%S")
            else:
                partial = matches.group("partial")
                # 如果上面的格式失败，那么尝试使用不包含秒的格式
                return datetime.strptime(partial, "%Y-%m-%d %H:%M")
        else:
            ItemFieldError(f"时间字符串格式错误：{repr(time_str)}")

    def publish_time_to_datetime(self, publish_time: int | str | datetime):
        if isinstance(publish_time, int):
            return self.timestamp_to_datetime(publish_time)
        elif isinstance(publish_time, str):
            return self.str_to_datetime(publish_time)
        return publish_time

    def is_time_format_valid(self, publish_time: int | str | datetime):
        if isinstance(publish_time, int):
            # 必须是10位或13位时间戳
            if publish_time < 0 or len(str(publish_time)) not in (10, 13):
                raise ItemFieldError(f"unix时间戳错误：{publish_time}")
        elif isinstance(publish_time, str):
            if not self.time_pattern.match(publish_time):
                raise ItemFieldError(f"时间字符串错误：{repr(publish_time)}")
        elif not isinstance(publish_time, datetime):
            raise ItemFieldError(
                f"publish_time字段类型错误: {type(publish_time)},仅允许Datetime、10/13位Unix时间戳、日期字符串"
            )

    def has_none_field(self, item: DetailDataItem):
        if item.source_url is None:
            raise ItemFieldError("字段source_url不能为空")
        if item.title is None:
            raise ItemFieldError("字段title不能为空")
        if item.content is None:
            raise ItemFieldError("字段content不能为空")
        if item.source is None:
            raise ItemFieldError("字段source不能为空")
        if item.publish_time is None:
            raise ItemFieldError("字段publish_time不能为空")
        return True


class BaseKonneHttpPipeline:
    """
    基类，康奈的上传及去重接口的基本设置。
    """

    uri_is_exist_url: str
    upload_and_filter_url: str

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.settings
        upload_ip = settings.get("UPLOAD_DATA_IP")
        cls.uri_is_exist_url = f"http://{upload_ip}/QuChong/ExistUrl"
        cls.upload_and_filter_url = f"http://{upload_ip}/Data/AddDataAndQuChong"
        return cls()

    def open_spider(self, spider: Spider):
        self.session = ClientSession()

    def close_spider(self, spider: Spider):
        loop = asyncio.get_event_loop()
        return Deferred.fromFuture(loop.create_task(self.session.close()))


class MemoryItemDupefilterPipeline:
    """在本地进行预去重，避免重复上传。"""

    url_seen = set()

    def process_item(self, item: DetailDataItem, spider: Spider):
        url = item.source_url
        if url in self.url_seen:
            raise LocalDuplicateItem(f"url已经在本地存在，不需要上传: {url}")
        self.url_seen.add(url)
        return item


class HttpItemDupefilterPipeline(BaseKonneHttpPipeline):
    async def is_url_exist(self, url):
        filter_url = self.uri_is_exist_url
        params = {"url": url}
        async with self.session.get(filter_url, params=params) as response:
            result = await response.json()
            if isinstance(result, int):
                return bool(result)

    async def process_item(self, item: DetailDataItem, spider: Spider):
        url = item.source_url
        if await self.is_url_exist(url):
            raise RemoteDuplicateItem(f"url已经在http去重库存在，不需要上传: {url}")
        return item


class UploadDataPipeline(BaseKonneHttpPipeline):
    """
    数据上传pipeline，用于上传数据到数据库。
    """

    async def process_item(self, item: DetailDataItem, spider: Spider):
        data = {
            "Title": item.title,  # 标题
            "PublishTime": item.publish_time.strftime("%Y-%m-%d %H:%M:%S"),  # 文章的发布时间
            "Author": item.video_url,  # 作者
            "SourceUrl": item.source_url,  # 网址
            "VideoUrl": item.video_url,
            "Source": item.source,  # 来源
            "Content": item.content,
            "AuthorID": item.author_id,  # 作者id
            "MediaType": item.media_type,  # 固定值为8
            "PageCrawlID": item.page_crawl_id,  # 不同的项目不同
            "SearchCrawID": item.search_crawl_id,  # 不同的项目不同
        }
        if not await self.upload(self, data):
            raise ItemUploadError(f"item上传失败: {data}")
        return item

    async def upload(self, data):
        async with self.session.post(self.upload_and_filter_url, data=data) as response:
            result = await response.json()
            if isinstance(result, int):
                return bool(result)


class UploadFilteredUrlPipeline:
    """
    提交上传成功的Url的数据，用于请求过滤。
    """

    def __init__(self, crawler: Crawler):
        self._redis_client = None
        self.crawler = crawler
        self.redis_key = "dupefilter:" + crawler.spider.name

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        object = cls(crawler)
        return object

    @property
    def redis_client(self):
        if not self._redis_client:
            self._redis_client = getattr(self.crawler, "redis_client", None)
        return self._redis_client

    async def process_item(self, item: DetailDataItem, spider: Spider):
        url = item.source_url
        hash_value = mmh3.hash128(url)
        hash_mapping = {hash_value: int(time.time() * 1000)}
        await self.redis_client.zadd(self.redis_key, hash_mapping, nx=True)
        return item


class ReplaceHtmlEntityPipeline:
    """
    ReplaceHtmlEntityPipeline类用于替换item中的html实体。
    """

    def process_item(self, item: DetailDataItem, spider: Spider):
        item.content = replace_entities(item.content)
        return item


class CSVWriterPipeline:
    """
    CSVWriterPipeline类用于将item写入csv文件。
    """

    def open_spider(self, spider: Spider):
        self.file = open("items.csv", "w", encoding="utf-8-sig", newline="")
        self.writer = csv.writer(self.file)

    def close_spider(self, spider: Spider):
        self.file.close()

    def process_item(self, item: DetailDataItem, spider: Spider):
        item.publish_time = item.publish_time.strftime("%Y-%m-%d %H:%M:%S")
        self.writer.writerow(
            [
                item.title,
                item.video_url,
                item.publish_time,
                item.content,
                item.source,
                item.source_url,
            ]
        )
        return item
