from scrapy.exceptions import DropItem

class SilentDropItem(DropItem):
    pass

class ExpriedItem(SilentDropItem):
    pass

class DuplicateItem(SilentDropItem):
    pass