from scrapy.exceptions import DropItem

# item字段错误
class ItemFieldError(Exception):
    pass

class ItemUploadError(Exception):
    pass

class SilentDropItem(DropItem):
    pass

# item时间过期
class ExpriedItem(SilentDropItem):
    pass

# 在本地去重时重复
class MemorySetDuplicateItem(SilentDropItem):
    pass

# 在康奈的http接口中重复 
class RemoteDuplicateItem(SilentDropItem):
    pass
