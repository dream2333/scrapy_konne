from scrapy.exceptions import DropItem

class ItemFieldError(Exception):
    pass

class SilentDropItem(DropItem):
    pass

class ExpriedItem(SilentDropItem):
    pass

class LocalDuplicateItem(SilentDropItem):
    pass

class RemoteDuplicateItem(SilentDropItem):
    pass
