import logging
import re
from datetime import datetime
from scrapy import Spider
from scrapy_konne.items import DetailDataItem
from scrapy_konne.exceptions import ItemFieldError
from dataclasses import fields
from typing import get_type_hints

logger = logging.getLogger(__name__)


class FieldValidatorPipeline:
    def __init__(self) -> None:
        self.time_pattern = re.compile(
            r"(?P<full>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|(?P<partial>\d{4}-\d{2}-\d{2} \d{2}:\d{2})"
        )

    def process_item(self, item: DetailDataItem, spider: Spider):
        self.has_none_field(item)
        self.is_time_format_valid(item.publish_time)
        self.type_check(item)
        return item

    def is_time_format_valid(self, publish_time: int | str | datetime):
        if isinstance(publish_time, int):
            # 必须是10位或13位时间戳
            if publish_time < 0 or len(str(publish_time)) not in (10, 13):
                raise ItemFieldError(f"unix时间戳错误：{publish_time}")
        elif isinstance(publish_time, str):
            if not self.time_pattern.match(publish_time):
                raise ItemFieldError(f"时间字符串格式错误：{repr(publish_time)}")
        elif not isinstance(publish_time, datetime):
            raise ItemFieldError(
                f"publish_time字段类型错误: {type(publish_time)},仅允许Datetime、10/13位Unix时间戳、日期字符串"
            )

    def has_none_field(self, item: DetailDataItem):
        if not item.source_url:
            raise ItemFieldError("字段source_url不能为空")
        if not item.title:
            raise ItemFieldError("字段title不能为空")
        if item.content is None:
            raise ItemFieldError("字段content不能为空")
        if not item.source:
            raise ItemFieldError("字段source不能为空")
        if not item.publish_time:
            raise ItemFieldError("字段publish_time不能为空")
        return True

    def type_check(self, item: DetailDataItem):
        for field in fields(item):
            value = getattr(item, field.name)
            expected_type = get_type_hints(item.__class__)[field.name]
            if not isinstance(value, expected_type):
                raise ItemFieldError(f"字段{field.name}类型错误：{type(value)}，必须为{expected_type}")
