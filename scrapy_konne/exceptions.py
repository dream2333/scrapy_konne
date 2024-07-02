from scrapy.exceptions import DropItem, IgnoreRequest


# item字段错误
class ItemFieldError(Exception):
    """item字段错误"""


class ItemUploadError(Exception):
    """item上传失败"""


class SilentDropItem(DropItem):
    """静默丢弃item"""


class ExpriedItem(DropItem):
    """item时间过期"""


class MemorySetDuplicateItem(SilentDropItem):
    """item在内存set中去重时重复"""


class RedisDuplicateItem(SilentDropItem):
    """item在redis库中重复"""


class RemoteDuplicateItem(SilentDropItem):
    """item在康奈的http接口中重复"""


class RedisDuplicateRequest(IgnoreRequest):
    """reuqest在redis库中重复"""
