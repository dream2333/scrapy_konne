import logging
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy_konne.constants import LOG_TYPE
from scrapy_konne.extensions.log_uploader import NoLogUploader, SectionLogUploader, IncreaseLogUploader

logger = logging.getLogger(__name__)


class KonneHttpLogExtension:
    """用于向公司接口提交日志"""

    def __init__(self, log_uploader: SectionLogUploader):
        self.log_uploader = log_uploader

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        # 添加信号
        log_uploader = cls.get_log_uploader(crawler)
        extension = cls(log_uploader)
        if log_uploader:
            crawler.signals.connect(log_uploader.spider_opened, signal=signals.spider_opened)
            crawler.signals.connect(log_uploader.spider_closed, signal=signals.spider_closed)
            crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        return extension

    @classmethod
    def get_log_uploader(cls, crawler: Crawler):
        """
        根据爬虫的配置生成不同的日志上传器
        """
        site_id = getattr(crawler.spider, "site_id")
        client_id = getattr(crawler.spider, "client_id")
        log_ip = crawler.settings.get("LOG_IP")
        # 默认为板块日志
        log_type = getattr(crawler.spider, "log_type", LOG_TYPE.SECTION)
        # 根据不同的日志类型选择不同的上传器
        match (log_type):
            case LOG_TYPE.NO_REMOTE_LOG:
                return NoLogUploader()
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
                return NoLogUploader()

    async def spider_closed(self, spider, reason):
        logger.info(
            f"日志类型<{self.log_uploader.logger_type}> " f"上传次数：{self.log_uploader.log_success_count}"
        )
