from scrapy import Request
from scrapy.crawler import Crawler


# class SourceUrlDupefilterMiddleware:
#     def __init__(self, crawler: Crawler) -> None:
#         self.crawler = crawler

#     async def process_spider_output_async(self, response, result, spider):
#         for i in result:
#             if isinstance(i, Request):
#                 spider.cursor = response.meta["cursor"]

    
#     async 