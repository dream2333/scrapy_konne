from scrapy.logformatter import LogFormatter
import logging
from scrapy_konne.exceptions import SilentDropItem


class PoliteLogFormatter(LogFormatter):
    """用于隐去DropItem"""
    def dropped(self, item, exception, response, spider):
        log_format = LogFormatter.dropped(self, item, exception, response, spider)
        if isinstance(exception, SilentDropItem):
            log_format["level"] = logging.DEBUG  # default is warning but I want to change it to debug
        return log_format

