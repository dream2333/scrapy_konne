from aiohttp import ClientSession
from abc import abstractmethod, ABCMeta
from scrapy_konne.constants import LOG_TYPE
from scrapy_konne.items import IncreamentItem
from scrapy import signals
import logging
import asyncio


class BaseLogUploader(metaclass=ABCMeta):
    """抽象基类，日志上传器必须继承于此类"""

    logger_type: LOG_TYPE
    logger: logging.Logger

    @abstractmethod
    async def send_log(self, stat):
        pass

    @abstractmethod
    async def spider_opened(self, spider):
        pass

    @abstractmethod
    async def spider_closed(self, spider, reason):
        pass


class NoLogUploader(BaseLogUploader):
    """无远程日志上传器"""

    logger_type = LOG_TYPE.NO_LOG
    logger = logging.getLogger("无日志")

    def __init__(self) -> None:
        self.log_success_count = 0

    def send_log(self, stat):
        self.logger.error("当前爬虫未开启远程日志记录，不上传日志")

    def spider_opened(self, spider):
        self.logger.info("当前爬虫未开启日志拓展")

    def spider_closed(self, spider, reason):
        self.logger.info("当前爬虫未开启远程日志记录，不上传日志")


class IncreaseLogUploader(BaseLogUploader):
    """自增日志上传器"""

    logger_type = LOG_TYPE.INCREASE
    logger = logging.getLogger("自增日志")

    def __init__(self, site_id, client_id, log_url, reset_id_url) -> None:
        self.site_id = site_id
        self.client_id = client_id
        self.log_url = log_url
        self.session = None
        self.reset_id_url = reset_id_url

    def spider_opened(self, spider):
        connect_signal = spider.crawler.signals.connect
        self.session = ClientSession()
        connect_signal(self.item_scraped, signal=signals.item_passed)
        self.log_success_count = 0
        self.logger.info("开启自增日志拓展")

    async def spider_closed(self, spider, reason):
        if spider._has_greater_cursor:
            result = await self.reset_max_num(spider.cursor)
            if result:
                self.logger.info(f"重置自增长id为{spider.cursor}成功")
            else:
                self.logger.warn(f"重置自增长id为{spider.cursor}失败")
        else:
            self.logger.info("本轮无新数据，无需重置自增长id")
        await self.session.close()

    async def reset_max_num(self, num):
        """
        重置自增长id
        """
        params = {"siteId": self.site_id, "num": num}
        async with self.session.get(url=self.reset_id_url, params=params) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    return True
        return False

    async def send_log(self, stat):
        """提交日志"""
        async with self.session.post(self.log_url, data=stat) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    return True
        return False

    async def item_scraped(self, item: IncreamentItem, response, spider):
        """提交日志"""
        pubtime_str = item.publish_time.strftime("%Y-%m-%d %H:%M:%S")
        stats = {
            "LogID": "",
            "SiteID": self.site_id,
            "PlusNum": item.increment_id,
            "PublishTime": pubtime_str,
            "CreateTime": "",
            "ClientID": self.client_id,
        }
        result = await self.send_log(stats)
        if result:
            self.log_success_count += 1
        else:
            self.logger.warn(f"[{self.client_id}] 自增日志提交失败")


class SectionLogUploader(BaseLogUploader):
    """板块日志上传器"""

    logger_type = LOG_TYPE.SECTION
    logger = logging.getLogger("板块日志")

    def __init__(self, site_id, client_id, log_url, interval) -> None:
        self.site_id = site_id
        self.client_id = client_id
        self.log_url = log_url
        self.session = None
        self.interval = interval
        self.log_success_count = 0
        self.timer = None

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
        self.session = ClientSession()
        connect_signal(self.item_scraped, signal=signals.item_passed)
        connect_signal(self.request_scheduled, signal=signals.request_scheduled)
        self.timer = asyncio.get_event_loop().create_task(self.log_timer())
        self.logger.info("开启板块日志拓展")

    async def log_timer(self):
        """定时提交日志"""
        while True:
            await asyncio.sleep(self.interval)
            await self.log_stats()

    async def spider_closed(self, spider, reason):
        # 取消定时任务并提交日志
        if self.timer:
            self.timer.cancel()
        await self.log_stats()
        await self.session.close()

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
            self.logger.info(f"[{self.client_id}] 日志提交成功: {formatted_stats}")
            self.log_success_count += 1
        else:
            self.logger.warn(f"[{self.client_id}] 日志提交失败,当前状态为: {formatted_stats}")

    async def send_log(self, stat):
        """提交日志"""
        async with self.session.post(self.log_url, data=stat) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    return True
        return False
