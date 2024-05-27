import asyncio
from aiohttp import ClientSession
from scrapy import Spider
from scrapy.crawler import Crawler
from twisted.internet.defer import Deferred
import logging

logger = logging.getLogger(__name__)


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