import csv
import logging

import aio_pika
from scrapy import Spider
from scrapy_konne.exceptions import ItemUploadError
from scrapy_konne.items import DetailDataItem
from scrapy_konne.pipelines.konnebase import BaseKonneRemotePipeline
import orjson


class PrintItemPipeline:
    """
    仅输出item到日志。
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger("管道末端")

    def process_item(self, item: DetailDataItem, spider: Spider):
        message = f"{item.publish_time} | [{item.title}] | {item.source} | {item.source_url} | 作者: {item.author} | {repr(item.content)} | {item.video_url} media_type: {item.media_type} page_crawl_id:{item.page_crawl_id} page_crawl_id:{item.search_crawl_id}"
        self.logger.info(message)
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


class KonneUploaderPipeline(BaseKonneRemotePipeline):
    """
    数据上传pipeline，用于上传板块和自增数据到数据库。
    """

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
        async with self.session.post(self.upload_and_filter_url, data=data) as response:
            result = await response.json()
            if isinstance(result, int):
                return bool(result)


class KonneExtraTerritoryUploaderPipeline:
    """
    境外数据上传pipeline，用于上传数据到境外rabbitmq。
    """

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        cls.upload_url = settings.get("EXTRATERRITORIAL_RABBITMQ_URL")
        cls.exchange_name = settings.get("EXTRATERRITORIAL_EXCHANGE_NAME")
        cls.routing_key = settings.get("EXTRATERRITORIAL_ROUTING_KEY")

    async def open_spider(self, spider):
        self.pika_connection = await aio_pika.connect_robust(url=self.upload_url)
        self.channel = await self.pika_connection.channel()
        self.exchange = await self.channel.get_exchange(self.exchange_name)

    async def upload(self, data):
        return await self.exchange.publish(data, routing_key=self.routing_key)

    def make_data(self, item: DetailDataItem, spider):
        info = {
            "accountId": spider.site_id,
            "title": item.title,
            "content": item.content,
            "author": item.author,
            "publishTime": item.publish_time,
            "source": item.source,
            "sourceSite": "",  # 来源网站
            "sourceUrl": item.source_url,
            "mediaType": 1,  # 媒体类型
            "columnId": 0,  # 采集栏目ID
            "language": spider.language.value,
        }
        return aio_pika.Message(body=orjson.loads(info))

    async def process_item(self, item: DetailDataItem, spider: Spider):
        data = self.make_data(item, spider)
        if not await self.upload(data):
            raise ItemUploadError(f"item上传失败: {data}")
        return item
