import logging
import re
from datetime import datetime
from scrapy import Spider
from scrapy_konne.items import DetailDataItem
from scrapy_konne.exceptions import ItemFieldError
from w3lib.html import replace_entities

logger = logging.getLogger(__name__)


class TimeFormatorPipeline:
    def __init__(self) -> None:
        self.time_pattern = re.compile(
            r"(?P<full>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|(?P<partial>\d{4}-\d{2}-\d{2} \d{2}:\d{2})"
        )

    def process_item(self, item: DetailDataItem, spider: Spider):
        if isinstance(item.publish_time, int):
            item.publish_time = self.timestamp_to_datetime(item.publish_time)
            return item
        elif isinstance(item.publish_time, str):
            item.publish_time = self.str_to_datetime(item.publish_time)
            return item
        elif isinstance(item.publish_time, datetime):
            return item
        raise ItemFieldError("publish_time字段类型错误")
    
    def timestamp_to_datetime(self, timestamp):
        # 如果是13位时间戳，那么转换成10位时间戳
        if timestamp > 10000000000:
            timestamp /= 1000
        publish_time = datetime.fromtimestamp(timestamp)
        return publish_time

    def str_to_datetime(self, time_str):
        matches = self.time_pattern.match(time_str)
        if matches:
            full = matches.group("full")
            if full:
                # 尝试使用包含秒的格式来解析时间字符串
                return datetime.strptime(full, "%Y-%m-%d %H:%M:%S")
            else:
                partial = matches.group("partial")
                # 如果上面的格式失败，那么尝试使用不包含秒的格式
                return datetime.strptime(partial, "%Y-%m-%d %H:%M")
        else:
            raise ItemFieldError(f"时间字符串格式错误：{repr(time_str)}")


class ReplaceHtmlEntityPipeline:
    """
    ReplaceHtmlEntityPipeline类用于替换item中的html实体。
    """

    def process_item(self, item: DetailDataItem, spider: Spider):
        item.content = replace_entities(item.content)
        return item
