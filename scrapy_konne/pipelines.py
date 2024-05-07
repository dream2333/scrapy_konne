import asyncio
import csv
import datetime
from itemadapter import ItemAdapter
from aiohttp import ClientSession
from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy_konne.items import DetailDataItem
from twisted.internet.defer import Deferred
from scrapy_konne.exceptions import LocalDuplicateItem, LoseItemField, RemoteDuplicateItem, ExpriedItem
from w3lib.html import replace_entities


class BaseSettingsPipeline:
    """
    BaseSettingsPipeline类用于处理爬虫的基本设置。
    """

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


class TimeValidPipeline(BaseSettingsPipeline):
    def process_item(self, item, spider: Spider):
        item_adapter: DetailDataItem = ItemAdapter(item)
        publish_time = item_adapter["publish_time"]
        if not publish_time:
            raise LoseItemField("发布时间字段缺失")
        time_now = datetime.datetime.now()
        dis_time = time_now - datetime.timedelta(days=3)
        time_dis_str = dis_time.strftime("%Y-%m-%d %H:%M:%S")
        if publish_time < time_dis_str:
            raise ExpriedItem(f"发布时间超过3天，不需要上传: {item_adapter['source_url']}")
        return item


class LocalDuplicatePipeline(BaseSettingsPipeline):
    cache = set()

    def process_item(self, item, spider: Spider):
        item_adapter: DetailDataItem = ItemAdapter(item)
        url = item_adapter["source_url"]
        if url in self.cache:
            raise LocalDuplicateItem(f"url已经在本地存在，不需要上传: {url}")
        self.cache.add(url)
        return item


class RemoteDuplicatePipeline(BaseSettingsPipeline):
    async def process_item(self, item, spider: Spider):
        item_adapter: DetailDataItem = ItemAdapter(item)
        url = item_adapter["source_url"]
        if await self.is_url_exist(url):
            raise RemoteDuplicateItem(f"url已经在去重库存在，不需要上传: {url}")
        return item


class ReplaceHtmlEntityPipeline:
    def process_item(self, item, spider: Spider):
        item_adapter: DetailDataItem = ItemAdapter(item)
        item_adapter["content"] = replace_entities(item_adapter["content"])
        return item


class UploadDataPipeline(BaseSettingsPipeline):
    """
    数据上传pipeline，用于上传数据到数据库。
    """

    async def process_item(self, item, spider: Spider):
        item_adapter: DetailDataItem = ItemAdapter(item)
        data = {
            "Title": item_adapter["title"],  # 标题
            "Author": item_adapter["author"],  # 作者
            "AuthorID": item_adapter["author_id"],  # 作者id
            "Content": item_adapter["content"],
            "PublishTime": item_adapter["publish_time"],  # 文章的发布时间
            "MediaType": item_adapter["media_type"],  # 固定值为8
            "VideoUrl": item_adapter["video_url"],
            "Source": item_adapter["source"],  # 来源
            "SourceUrl": item_adapter["source_url"],  # 网址
            "PageCrawlID": item_adapter["page_crawl_id"],  # 不同的项目不同
            "SearchCrawID": item_adapter["search_crawl_id"],  # 不同的项目不同
        }
        async with self.session.post(self.upload_and_filter_url, data=data) as response:
            spider.logger.info(data)
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

    def process_item(self, item, spider: Spider):
        item_adapter: DetailDataItem = ItemAdapter(item)
        spider.logger.info(item_adapter)
        self.writer.writerow(
            [
                item_adapter["title"],
                item_adapter["author"],
                item_adapter["publish_time"],
                item_adapter["content"],
                item_adapter["source"],
                item_adapter["source_url"],
            ]
        )
        return item
