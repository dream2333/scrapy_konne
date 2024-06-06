import logging
from aiohttp import ClientSession
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy_konne.constants import LOG_TYPE
from scrapy_konne.items import IncreamentItem
from scrapy_konne.pipelines.formator import TimeFormatorPipeline
import asyncio

logger = logging.getLogger(__name__)


class LogUploader:
    def __init__(self, site_id, client_id, log_url) -> None:
        self.site_id = site_id
        self.client_id = client_id
        self.log_url = log_url
        self.session = ClientSession()
        self.logger_type = None

    async def send_log(self, stat):
        """提交日志"""
        async with self.session.post(self.log_url, data=stat) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    return True
        return False

    async def spider_opened(self, spider): ...

    async def spider_closed(self, spider, reason):
        await self.session.close()


class IncreaseLogUploader(LogUploader):
    formator = TimeFormatorPipeline()

    def spider_opened(self, spider):
        connect_signal = spider.crawler.signals.connect
        connect_signal(self.item_scraped, signal=signals.item_passed)
        self.logger_type = LOG_TYPE.INCREASE
        self.log_success_count = 0
        logger.info("开启自增日志拓展")

    async def item_scraped(self, item: IncreamentItem, response, spider):
        pubtime_str = item.publish_time.strftime("%Y-%m-%d %H:%M:%S")
        stats = {
            "LogID": "",
            "SiteID": self.site_id,
            "PlusNum": item.increment_id,
            "PublishTime": pubtime_str,
            "CreateTime": "",
            "ClientID": self.client_id,
        }
        await self.send_log(stats)
        self.log_success_count += 1


class SectionLogUploader(LogUploader):

    def __init__(self, site_id, client_id, log_url, interval) -> None:
        super().__init__(site_id, client_id, log_url)
        self.interval = interval

    @property
    def stats(self):
        _stats = getattr(self, "_stats", None)
        if _stats is None:
            self._stats = {
                "LogID": "",
                "SiteID": self.site_id,
                "TotalCount": 0,
                "AddCount": 0,
                "Message": "scrapy-log",
                "CreateTime": "",
                "ClientID": self.client_id,
            }
        return self._stats

    async def spider_opened(self, spider):
        connect_signal = spider.crawler.signals.connect
        connect_signal(self.item_scraped, signal=signals.item_passed)
        connect_signal(self.request_scheduled, signal=signals.request_scheduled)
        self.looping_task = asyncio.create_task(self.log_stats())
        self.logger_type = LOG_TYPE.SECTION
        self.log_success_count = 0
        logger.info("开启板块日志拓展")

    async def log_timer(self):
        """定时提交日志"""
        while True:
            await asyncio.sleep(self.interval)
            await self.log_stats()

    async def spider_closed(self, spider, reason):
        # 取消定时任务并提交日志
        self.looping_task.cancel()
        await self.log_stats()
        await super().spider_closed(spider, reason)

    def reset_stats(self):
        self.stats["AddCount"] = 0
        self.stats["TotalCount"] = 0

    def item_scraped(self, item, response, spider):
        self.stats["AddCount"] += 1

    def request_scheduled(self, request, spider):
        self.stats["TotalCount"] += 1

    async def log_stats(self):
        """提交日志并输出"""
        formatted_stats = f'item数：{self.stats["AddCount"]} 请求数：{self.stats["TotalCount"]}'
        result = await self.send_log(self.stats)
        if result:
            self.reset_stats()
            logger.info(f"[{self.client_id}] 日志提交成功: {formatted_stats}")
            self.log_success_count += 1
        else:
            logger.error(f"[{self.client_id}] 日志提交失败,当前状态为: {formatted_stats}")


class KonneHttpLogExtension:
    """用于向公司接口提交日志"""

    def __init__(self, log_uploader: SectionLogUploader):
        self.log_uploader = log_uploader

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        # 添加信号
        log_uploader = cls.get_log_uploader(crawler)
        extension = cls(log_uploader)
        crawler.signals.connect(log_uploader.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(log_uploader.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        return extension

    @classmethod
    def get_log_uploader(cls, crawler: Crawler):
        site_id = getattr(crawler.spider, "site_id")
        client_id = getattr(crawler.spider, "client_id")
        log_ip = crawler.settings.get("LOG_IP")
        log_type = getattr(crawler.spider, "log_type", LOG_TYPE.SECTION)
        match (log_type):
            case LOG_TYPE.SECTION:
                log_url = f"http://{log_ip}/Log/AddSectionLog"
                interval = crawler.settings.getfloat("UPLOAD_LOG_INTERVAL")
                return SectionLogUploader(site_id, client_id, log_url, interval)
            case LOG_TYPE.INCREASE:
                log_url = f"http://{log_ip}/PlusNum/AddPlusNumLog"
                return IncreaseLogUploader(site_id, client_id, log_url)
            case _:
                logger.error(f"<{type(log_type)} : {log_type}> log_type参数错误，为了防止错误提交，停止程序")
                crawler.engine.close_spider(crawler.spider, "log_type_error")

    async def spider_closed(self, spider, reason):
        logger.info(
            f"日志类型<{self.log_uploader.logger_type}> " f"上传次数：{self.log_uploader.log_success_count}"
        )
