import asyncio
from math import inf
import os
from zoneinfo import ZoneInfo
from aiohttp import ClientSession
from scrapy.crawler import Crawler
from scrapy import signals
from weakref import WeakKeyDictionary
from scrapy.utils.log import logger
from tabulate import tabulate
from wcwidth import wcswidth


class KonneHttpLogExtension:
    """用于向公司接口提交日志"""

    def __init__(self, interval: int):
        self.spiders_stats = WeakKeyDictionary()
        self.interval = interval
        self.session = ClientSession()
        self.log_task = None
        self.loop = asyncio.get_event_loop()

    @classmethod
    def from_crawler(cls: "KonneHttpLogExtension", crawler: Crawler):
        section_log_ip = crawler.settings.get("SECTION_LOG_IP")
        log_interval = crawler.settings.get("UPLOAD_LOG_INTERVAL")
        extension: "KonneHttpLogExtension" = cls(log_interval)
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

    async def spider_closed(self, spider, reason):
        # 取消定时任务
        self.log_task.cancel()
        logger.info(f"康奈日志记录器[{spider.name}]已关闭，爬虫状态:{reason}")
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


class KonneWechatBotExtension:

    def __init__(self, crawler: Crawler):
        self.bot_url = crawler.settings.get("WECHAT_BOT_URL")
        self.failure_threshold = crawler.settings.get("WECHAT_BOT_REQ_FAILURE_THRESHOLD", 0)
        self.log_error_threshold = crawler.settings.get("WECHAT_BOT_LOG_ERROR_THRESHOLD", 0)
        self.elapsed_time_threshold = crawler.settings.get("ELAPSED_TIME_THRESHOLD", float("inf"))
        if self.elapsed_time_threshold <= 0 or self.elapsed_time_threshold is None:
            self.elapsed_time_threshold = inf
        self.admin_url = crawler.settings.get("ADMIN_PAGE_URL")
        self.stats_collector = crawler.stats

    @classmethod
    def from_crawler(cls: "KonneWechatBotExtension", crawler: Crawler):
        extension: "KonneHttpLogExtension" = cls(crawler)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        return extension

    async def spider_closed(self, spider, reason):
        # 获取本地时区
        local_tz = ZoneInfo("Asia/Shanghai")
        # 将 UTC 时间转换为本地时间
        jobid = os.getenv("SCRAPYD_JOB", "N/A")
        project_name = os.getenv("SCRAPY_PROJECT", "N/A")
        stats = self.stats_collector.get_stats(spider)
        start_time = stats.get("start_time")
        finish_time = stats.get("finish_time")
        elapsed_time_seconds = int(stats.get("elapsed_time_seconds", 0))
        item_scraped_count = stats.get("item_scraped_count", 0)
        error_log = stats.get("log_count/ERROR", 0)
        retry_max_reached = stats.get("retry/max_reached", 0)
        item_dropped_count = stats.get("item_dropped_count", 0)
        total_scraped_count = item_dropped_count + item_scraped_count
        if (
            total_scraped_count == 0
            or retry_max_reached > self.failure_threshold
            or error_log > self.log_error_threshold
            or elapsed_time_seconds > self.elapsed_time_threshold
        ):
            data = [
                ["总耗时", elapsed_time_seconds, self.elapsed_time_threshold],
                ["item提交", item_scraped_count, "无"],
                ["item过滤", item_dropped_count, "无"],
                ["item总计", total_scraped_count, 0],
                ["失败请求", retry_max_reached, self.failure_threshold],
                ["错误日志", error_log, self.log_error_threshold],
            ]

            # 表头
            headers = ["指标", "结果", "阈值"]
            col_widths = [max(wcswidth(str(x)) for x in col) for col in zip(*data, headers)]
            table = tabulate(data, headers=headers, tablefmt="simple", colalign=("left",) * len(col_widths))
            md_content = f"""
<font color="warning">{spider.name}</font>相关统计数据异常，请相关同事注意。

<font color="comment">{project_name}</font>

<font color="comment">{jobid}</font>

开始: <font color="comment">{start_time.astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")}</font>

结束: <font color="comment">{finish_time.astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")}</font>

结束原因,  <font color="comment">{reason}</font>


{table}


[📓点击查看日志]({self.admin_url}/#/logs/{project_name}/{spider.name}/{jobid})
"""
            data = {
                "msgtype": "markdown",
                "markdown": {"content": md_content},
                "mentioned_list": ["@all"],
            }
            logger.info(f"爬虫{spider.name}有异常状态，已发送到企业微信")
            async with ClientSession() as session:
                async with session.post(self.bot_url, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("errcode") == 0:
                            return True
        else:
            logger.info(f"爬虫{spider.name}正常结束，无需发送到企业微信")
