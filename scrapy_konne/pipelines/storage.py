import csv
import logging
from aiohttp import ClientSession
from scrapy import signals
from scrapy.crawler import Crawler
import aio_pika
from scrapy import Spider
from scrapy_konne.exceptions import ItemUploadError
from scrapy_konne.constants import LOCALE
from scrapy_konne.items import DetailDataItem
import orjson

logger = logging.getLogger(__name__)


class PrintItemPipeline:
    """
    仅输出item到日志。
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger("管道输出")
        self.oldest_item = None
        self.newest_item = None

    def process_item(self, item: DetailDataItem, spider: Spider):
        # 记录最早和最新的item
        if not self.oldest_item:
            self.oldest_item = item
        elif item.publish_time < self.oldest_item.publish_time:
            self.oldest_item = item
        if not self.newest_item:
            self.newest_item = item
        elif item.publish_time > self.newest_item.publish_time:
            self.newest_item = item
        # 输出当前item到日志
        message = f"文章时间： {item.publish_time} | [{item.title}] | {item.source} | {item.source_url} | 作者: {item.author} | {repr(item.content)} | 视频链接： {item.video_url} | media_type: {item.media_type} page_crawl_id:{item.page_crawl_id}"
        self.logger.info(message)
        return item

    def close_spider(self, spider: Spider):
        if self.oldest_item:
            self.logger.info(f"[{self.oldest_item.publish_time}] 本轮最早item: {self.oldest_item}")
        if self.newest_item:
            self.logger.info(f"[{self.newest_item.publish_time}] 本轮最新item: {self.newest_item}")


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
        self.writer.writerow(
            [
                item.title,
                item.video_url,
                item.publish_time.strftime("%Y-%m-%d %H:%M:%S"),
                item.content,
                item.source,
                item.source_url,
            ]
        )
        return item


class KonneUploaderPipeline:
    """
    数据上传pipeline，根据境外境内选择不同的上传Uploader
    """

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        spider_locale = getattr(crawler.spider, "locale", LOCALE.CN)
        match (spider_locale):
            case LOCALE.CN:
                logger.info("选择境内http上传器")
                uploader = KonneTerritoryUploaderPipeline
            case _:
                logger.info("选择境外rabbitmq上传器")
                uploader = KonneExtraTerritoryUploaderPipeline
        return uploader.from_crawler(crawler)


class KonneTerritoryUploaderPipeline:
    """
    数据上传pipeline，用于上传板块和自增数据到数据库。
    """

    upload_and_filter_api: str

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        upload_ip = crawler.settings.get("UPLOAD_DATA_IP")
        cls.upload_and_filter_api = f"http://{upload_ip}/Data/AddDataAndQuChong"
        uploader = cls()
        crawler.signals.connect(uploader.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(uploader.spider_closed, signal=signals.spider_closed)
        return uploader

    async def spider_opened(self, spider: Spider):
        self.session = ClientSession()

    async def spider_closed(self, spider: Spider):
        await self.session.close()

    async def process_item(self, item: DetailDataItem, spider: Spider):
        data = {
            "Title": item.title,  # 标题
            "PublishTime": item.publish_time.strftime("%Y-%m-%d %H:%M:%S"),  # 文章的发布时间
            "Author": item.author,  # 作者
            "SourceUrl": item.source_url,  # 网址
            "VideoUrl": item.video_url,
            "Source": item.source,  # 来源
            "Content": item.content,
            "AuthorID": item.author_id,  # 作者id
            "MediaType": item.media_type,  # 固定值为8
            "PageCrawlID": item.page_crawl_id,  # 不同的项目不同
            "SearchCrawID": item.search_crawl_id,  # 不同的项目不同
        }
        if item.ip_area:
            data["IpArea"] = item.ip_area
        if item.video_image:
            data["VideoImage"] = item.video_image
        if not await self.upload(data):
            raise ItemUploadError(f"item上传失败: {data}")
        return item

    async def upload(self, data):
        async with self.session.post(self.upload_and_filter_api, data=data) as response:
            result = await response.json()
            if isinstance(result, int):
                return bool(result)


class KonneExtraTerritoryUploaderPipeline:
    """
    境外数据上传pipeline，用于上传数据到境外rabbitmq。
    """

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.settings
        cls.upload_url = settings.get("EXTRATERRITORIAL_RABBITMQ_URL")
        cls.exchange_name = settings.get("EXTRATERRITORIAL_EXCHANGE_NAME")
        cls.routing_key = settings.get("EXTRATERRITORIAL_ROUTING_KEY")
        uploader = cls()
        crawler.signals.connect(uploader.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(uploader.spider_closed, signal=signals.spider_closed)
        return uploader

    async def spider_opened(self, spider):
        logger.info("正在打开rabbitmq上传管道")
        for _ in range(3):
            try:
                self.pika_connection = await aio_pika.connect_robust(url=self.upload_url)
                self.channel = await self.pika_connection.channel()
                self.exchange = await self.channel.get_exchange(self.exchange_name)
                logger.info("rabbitmq上传管道已打开")
                return
            except Exception as e:
                logger.warning(f"连接失败，重试中... ({_ + 1}/{3})")
        spider.crawler.engine.close_spider(spider, "redis_error")

    async def spider_closed(self, spider, reason):
        logger.info("正在关闭rabbitmq上传管道")
        await self.pika_connection.close()
        logger.info("rabbitmq上传管道已关闭")

    async def upload(self, data):
        data = await self.exchange.publish(data, routing_key=self.routing_key)
        return data

    def make_data(self, item: DetailDataItem, spider):
        info = {
            "accountId": spider.site_id,
            "title": item.title,
            "content": item.content,
            "author": item.author,
            "publishTime": item.publish_time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": item.source,
            "sourceSite": "",  # 来源网站
            "sourceUrl": item.source_url,
            "columnId": item.page_crawl_id,  # 采集栏目ID
            "mediaType": item.media_type,
        }
        if getattr(spider, "language", None):  # 语言
            info["language"] = spider.language.value
        return aio_pika.Message(body=orjson.dumps(info))

    async def process_item(self, item: DetailDataItem, spider: Spider):
        data = self.make_data(item, spider)
        if not await self.upload(data):
            raise ItemUploadError(f"item上传失败: {data}")
        return item
