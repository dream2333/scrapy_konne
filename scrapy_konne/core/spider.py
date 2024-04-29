from scrapy.crawler import Crawler

class SpiderMixin:
    @classmethod
    def from_crawler(cls, crawler:Crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        crawler
        print("from_crawler")
        return spider