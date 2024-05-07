from scrapy.exceptions import DropItem

class LoseItemField(Exception):
    pass

class SilentDropItem(DropItem):
    pass

class ExpriedItem(SilentDropItem):
    pass

class LocalDuplicateItem(SilentDropItem):
    pass

class RemoteDuplicateItem(SilentDropItem):
    pass
