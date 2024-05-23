from scrapy import Request
from scrapy.crawler import Crawler
from scrapy_konne.utils.connection import GlobalRedis

class SourceUrlDupefilterMiddleware:
    def __init__(self, crawler: Crawler) -> None:
        self.crawler = crawler

    async def process_spider_output_async(self, response, result, spider):
        for i in result:
            if isinstance(i, Request):
                spider.cursor = response.meta["cursor"]

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(crawler)
    
