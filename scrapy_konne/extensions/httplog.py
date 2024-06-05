import asyncio

from aiohttp import ClientSession
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy.utils.log import logger
from scrapy_konne.constants import LOG_TYPE


class LogUploader:
    def __init__(self, site_id, client_id, log_url) -> None:
        self.site_id = site_id
        self.client_id = client_id
        self.log_url = log_url
        self.session = ClientSession()

    @property
    def stats(self):
        raise NotImplementedError("stats属性未实现")

    async def send_log(self):
        """提交日志"""
        async with self.session.post(self.log_url, data=self.stats) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    # 提交成功后，重置stats
                    return True
        return False

    async def close(self):
        await self.session.close()


class SectionLogUploader(LogUploader):

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

    def reset_stats(self):
        self._stats["AddCount"] = 0
        self._stats["TotalCount"] = 0

    def item_scraped(self, item, response, spider):
        self.stats["AddCount"] += 1

    def request_scheduled(self, request, spider):
        self.stats["TotalCount"] += 1


class IncreaseLogUploader(LogUploader): ...


class KonneHttpLogExtension:
    """用于向公司接口提交日志"""

    def __init__(self, interval: int, log_uploader: SectionLogUploader):
        self.interval = interval
        self.log_uploader = log_uploader
        self.log_task = None
        self.loop = asyncio.get_event_loop()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        # 添加信号
        interval = crawler.settings.getfloat("UPLOAD_LOG_INTERVAL")
        log_uploader: SectionLogUploader = cls.get_log_uploader(crawler)
        extension = cls(interval, log_uploader)
        crawler.signals.connect(log_uploader.item_scraped, signal=signals.item_passed)
        crawler.signals.connect(log_uploader.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(log_uploader.close, signal=signals.spider_closed)
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
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
                logger.info("开启板块日志拓展")
                log_url = f"http://{log_ip}/Log/AddSectionLog"
                return SectionLogUploader(site_id, client_id, log_url)
            case LOG_TYPE.INCREASE:
                logger.info("开启自增日志拓展")
                log_url = f"http://{log_ip}/PlusNum/AddPlusNumLog"
                return IncreaseLogUploader(site_id, client_id, log_url)
            case _:
                logger.error(f"<{type(log_type)} : {log_type}> log_type参数错误，为了防止错误提交，停止程序")
                crawler.engine.close_spider(crawler.spider, "log_type_error")

    def spider_opened(self, spider):
        # 定时任务，30秒提交一次日志
        self.log_task = self.loop.create_task(self.log_timer(spider))

    async def spider_closed(self, spider, reason):
        # 取消定时任务
        self.log_task.cancel()
        if reason == "finished":
            await self.log_stats()
            logger.info(f"康奈日志记录器[{spider.name}]已提交，爬虫状态:{reason}")
        else:
            logger.error(f"爬虫关闭原因为:{reason}, 不提交康奈日志")

    async def log_timer(self, spider):
        """打点计时器，每隔interval秒提交一次日志"""
        while True:
            await asyncio.sleep(self.interval)
            await self.log_stats()

    async def log_stats(self):
        """提交日志并输出"""
        log_data = self.log_uploader.stats
        formatted_stats = f'item数：{log_data["AddCount"]} 请求数：{log_data["TotalCount"]}'
        result = await self.log_uploader.send_log()
        if result:
            self.log_uploader.reset_stats()
            logger.info(f"[{log_data['ClientID']}] 日志提交成功: {formatted_stats}")
        else:
            logger.error(f"[{log_data['ClientID']}] 日志提交失败,当前状态为: {formatted_stats}")
