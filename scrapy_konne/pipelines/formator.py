import logging
from datetime import datetime
from scrapy import Spider

from scrapy_konne.items import DetailDataItem
from scrapy_konne.exceptions import ItemFieldError
from w3lib.html import replace_entities
from dateutil.parser import parse as time_parse
from scrapy_konne.utils.tools import format_time

logger = logging.getLogger(__name__)


class TimeFormatorPipeline:

    def process_item(self, item: DetailDataItem, spider: Spider):
        if isinstance(item.publish_time, str):
            item.publish_time = self.str_to_datetime_utc8(item.publish_time)
            return item
        elif isinstance(item.publish_time, int):
            item.publish_time = self.timestamp_to_datetime_utc8(item.publish_time)
            return item
        elif isinstance(item.publish_time, datetime):
            item.publish_time = self.datetime_to_utc8(item.publish_time)
            return item
        else:
            raise ItemFieldError("publish_time字段类型错误")

    def datetime_to_utc8(self, date_time: datetime) -> datetime:
        """将给定的datetime对象转换为UTC+8时区的datetime对象。"""
        return date_time.astimezone()

    def timestamp_to_datetime_utc8(self, timestamp: int):
        """将给定的时间戳转换为UTC+8时区的datetime对象。"""
        # 如果是13位时间戳，那么转换成10位时间戳
        if timestamp > 10000000000:
            timestamp /= 1000
        publish_time = datetime.fromtimestamp(timestamp)
        return publish_time.astimezone()

    def str_to_datetime_utc8(self, time_str: str) -> datetime:
        """
        尝试将给定的时间字符串转换为UTC+8时区datetime对象。

        首先尝试使用dateutil.parser.parse进行解析，

        如果失败，则尝试使用自定义的format_time函数进行解析。

        如果两种方法都失败，则抛出ItemFieldError异常。

        :param time_str: 需要转换的时间字符串
        :return: 转换后的datetime对象
        """
        try:
            # 使用dateutil.parser.parse来解析大部分ISO 8601格式和标准的时间字符串
            date_time = time_parse(time_str)
        except ValueError:
            try:
                # 尝试使用自定义的format_time函数进行解析
                date_time_str = format_time(time_str)
                date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                raise ItemFieldError(f"时间字符串无法被智能转换：{repr(time_str)}")
        return date_time.astimezone()


class ReplaceHtmlEntityPipeline:
    """
    ReplaceHtmlEntityPipeline类用于替换item中的html实体。
    """

    def process_item(self, item: DetailDataItem, spider: Spider):
        item.content = replace_entities(item.content)
        return item
