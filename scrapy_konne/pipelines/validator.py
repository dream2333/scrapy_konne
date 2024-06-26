import logging
from scrapy import Spider
from scrapy_konne.items import DetailDataItem
from scrapy_konne.exceptions import ItemFieldError
from dataclasses import fields
from typing import get_type_hints

logger = logging.getLogger(__name__)


class FieldValidatorPipeline:
    """
    用于验证DetailDataItem的字段。
    
    该类会检查DetailDataItem的字段是否符合规范，包括字段类型检查、字段非空检查、时间戳检查。
    """
    __not_empty_fields__ = ["source_url", "title", "source", "publish_time"]

    def process_item(self, item: DetailDataItem, spider: Spider):
        self.type_check(item)
        self.empty_check(item)
        self.unix_time_check(item)
        return item

    def unix_time_check(self, item: DetailDataItem):
        if isinstance(item.publish_time, int):
            # 必须是10位或13位时间戳
            if item.publish_time < 0 or len(str(item.publish_time)) not in (10, 13):
                raise ItemFieldError(f"unix时间戳错误,必须是10位或13位时间戳：{item.publish_time}")

    def empty_check(self, item: DetailDataItem):
        for field in self.__not_empty_fields__:
            value = getattr(item, field)
            if not value:
                raise ItemFieldError(f"字段{field}不能为空")

    def type_check(self, item: DetailDataItem):
        for field in fields(item):
            value = getattr(item, field.name)
            expected_type = get_type_hints(item.__class__)[field.name]
            if not isinstance(value, expected_type):
                raise ItemFieldError(f"字段{field.name}类型错误：{type(value)}，必须为{expected_type}")
