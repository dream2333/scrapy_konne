import asyncio
from aiohttp import ClientSession

from scrapy.crawler import Crawler
from scrapy import signals
from weakref import WeakKeyDictionary
from scrapy.utils.log import logger

class HttpLogStats:
    """用于向公司接口提交日志"""

    def __init__(self, interval: int):
        self.spiders_stats = WeakKeyDictionary()
        self.interval = interval
        self.session = ClientSession()
        self.log_task = None
        self.loop = asyncio.get_event_loop()

    @classmethod
    def from_crawler(cls: "HttpLogStats", crawler: Crawler):
        section_log_ip = crawler.settings.get("SECTION_LOG_IP")
        log_interval = crawler.settings.get("UPLOAD_LOG_INTERVAL")
        extension: "HttpLogStats" = cls(log_interval)
        # 添加信号
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(extension.item_scraped, signal=signals.item_passed)
        crawler.signals.connect(extension.request_scheduled, signal=signals.request_scheduled)
        cls.section_log_url = f"http://{section_log_ip}/Log/AddSectionLog"
        return extension

    def spider_opened(self, spider):
        self.spiders_stats[spider] = {
            "LogID": "",
            "SiteID": spider.site_id,
            "TotalCount": 0,
            "AddCount": 0,
            "Message": "scrapy-log",
            "CreateTime": "",
            "ClientID": spider.client_id,
        }
        # 定时任务，30秒提交一次日志
        self.log_task = self.loop.create_task(self.log_timer(spider))

    async def spider_closed(self, spider):
        # 取消定时任务
        self.log_task.cancel()
        logger.info(f"爬虫{spider.name}已关闭")
        await self.log_stats(spider)
        await self.session.close()

    def request_scheduled(self, request, spider):
        # 每次请求，stats加1
        self.spiders_stats[spider]["TotalCount"] += 1

    def item_scraped(self, item, response, spider):
        # 每次成功收取一个item，stats加1
        self.spiders_stats[spider]["AddCount"] += 1

    async def log_timer(self, spider):
        """打点计时器，每隔interval秒提交一次日志"""
        while True:
            await asyncio.sleep(self.interval)
            await self.log_stats(spider)

    async def log_stats(self, spider):
        """提交日志并输出"""
        result = await self.send_log(spider)
        upload_log = self.spiders_stats[spider]
        formatted_stats = f'添加数：{upload_log["AddCount"]} 请求数：{upload_log["TotalCount"]}'
        if result:
            logger.info(f"[{upload_log['ClientID']}] 日志提交成功: {formatted_stats}")
            # 提交成功后，重置stats
            upload_log["AddCount"] = 0
            upload_log["TotalCount"] = 0
        else:
            logger.error(f"[{upload_log['ClientID']}] 日志提交失败,当前状态为: {formatted_stats}")

    async def send_log(self, spider):
        """提交日志"""
        upload_log = self.spiders_stats[spider]
        async with self.session.post(self.section_log_url, data=upload_log) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    return True
        return False
