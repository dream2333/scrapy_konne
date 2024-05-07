from scrapy.crawler import Crawler
from scrapy_konne.core.signals import Event


class AckSignalMiddleware:

    def __init__(self, crawler: Crawler) -> None:
        self.crawler = crawler

    def process_spider_output(self, response, result, spider):
        self.crawler.signals.send_catch_log(
            signal=Event.REQUEST_ACK,
            request=response.request,
            spider=spider,
        )
        return result

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)
