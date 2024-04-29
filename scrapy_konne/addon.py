class Addons:
    def update_settings(self, settings):
        settings["SPIDER_MIDDLEWARES"]["scrapy_konne.core.ack.RequestAckMiddleware"] = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls()