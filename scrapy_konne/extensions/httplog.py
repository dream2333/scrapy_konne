import asyncio
from aiohttp import ClientSession
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy.utils.log import logger


class KonneHttpLogExtension:
    """用于向公司接口提交日志"""

    def __init__(self, crawler: Crawler):
        self.spiders_stats = {}
        section_log_ip = crawler.settings.get("SECTION_LOG_IP")
        self.section_log_url = f"http://{section_log_ip}/Log/AddSectionLog"
        self.interval = crawler.settings.getfloat("UPLOAD_LOG_INTERVAL")
        self.session = ClientSession()
        self.log_task = None
        self.loop = asyncio.get_event_loop()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        extension = cls(crawler)
        # 添加信号
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(extension.item_scraped, signal=signals.item_passed)
        crawler.signals.connect(extension.request_scheduled, signal=signals.request_scheduled)
        return extension

    def spider_opened(self, spider):
        self.spiders_stats = {
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

    async def spider_closed(self, spider, reason):
        # 取消定时任务
        self.log_task.cancel()
        logger.info(f"康奈日志记录器[{spider.name}]已关闭，爬虫状态:{reason}")
        if reason == "finished":
            await self.log_stats()
        await self.session.close()

    def request_scheduled(self, request, spider):
        # 每次请求，stats加1
        self.spiders_stats["TotalCount"] += 1

    def item_scraped(self, item, response, spider):
        # 每次成功收取一个item，stats加1
        self.spiders_stats["AddCount"] += 1

    async def log_timer(self, spider):
        """打点计时器，每隔interval秒提交一次日志"""
        while True:
            await asyncio.sleep(self.interval)
            await self.log_stats()

    async def log_stats(self):
        """提交日志并输出"""
        log_data = self.spiders_stats
        formatted_stats = f'添加数：{log_data["AddCount"]} 请求数：{log_data["TotalCount"]}'
        result = await self.send_log(log_data)
        if result:
            logger.info(f"[{log_data['ClientID']}] 日志提交成功: {formatted_stats}")
        else:
            logger.error(f"[{log_data['ClientID']}] 日志提交失败,当前状态为: {formatted_stats}")

    async def send_log(self, log_data):
        """提交日志"""
        async with self.session.post(self.section_log_url, data=log_data) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("code") == 0:
                    # 提交成功后，重置stats
                    log_data["AddCount"] = 0
                    log_data["TotalCount"] = 0
                    return True
        return False
