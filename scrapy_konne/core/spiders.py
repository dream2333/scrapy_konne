from typing import Any, Iterable
import pymongo
from scrapy_konne.items import DetailDataItem
from scrapy import Request, Spider
from scrapy.crawler import Crawler


class DistrubuteSpiderMixin:
    @classmethod
    def from_crawler(cls, crawler: Crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        crawler
        print("from_crawler")
        return spider


class IncreaseSpiderMiddleware:
    def process_spider_output(self, response, result, spider):
        for i in result:
            if isinstance(i, DetailDataItem):
                spider.cursor = response.meta["cursor_id"]
            yield i


class IncreaseSpider(Spider):
    name: str
    site_id: int
    offset: tuple[int, int]
    """爬虫通过文章的最新id和offset来确定更新时爬取的范围
    :必须设置offset以确定抓取范围
    :offset必须是一个长度为2的列表或元组，分别代表向前探查数量和向后探查数量"""

    url_template: str
    """url模板，格式化时传入cursor，eg:"https://example.com?tid={cursor}"""

    def __init__(self, name: str | None = None, **kwargs: Any):
        super().__init__(name, **kwargs)
        self._previous_round_cursor = None
        self._cursor: int = None
        self._has_greater_cursor = False
        if not getattr(self, "offset", None):
            raise ValueError(
                """爬虫通过文章的最新id和offset来确定更新时爬取的范围，必须设置offset以确定抓取范围
                offset必须是一个长度为2的列表，分别代表向前探查数量和向后探查数量"""
            )
        elif len(self.offset) != 2:
            raise ValueError("offset必须是一个长度为2的列表、元组、或切片，代表向前探查数量和向后探查数量")
        elif not getattr(self, "url_template", None):
            raise ValueError("必须设置url_template来确定自增的url模板")

    def start_requests(self) -> Iterable[Request]:
        for i in self.start_ids:
            url = f"https://share.dz169.com/wap/thread/view-thread/tid/{i}"
            yield Request(url, dont_filter=True, meta={"cursor_id": i})

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IncreaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        cls.mongo_client = pymongo.MongoClient(crawler.settings.get("MONGO_URI"))
        cls.collection = spider.mongo_client["Scrapy"]["ids_state"]
        spider.settings["SPIDER_MIDDLEWARES"][IncreaseSpiderMiddleware] = 0
        return spider

    @property
    def start_ids(self) -> Iterable[int]:
        for i in range(self.cursor - self.offset[0], self.cursor + self.offset[1]):
            yield i

    @property
    def cursor(self) -> int:
        if self._cursor is None:
            meta = self.collection.find_one({"site_id": self.site_id})
            self._cursor = meta["cursor"]
            self._previous_round_cursor = self._cursor
            self.logger.info(f"数据库id游标: {self._cursor}")
        return self._cursor  # getter方法

    @cursor.setter
    def cursor(self, value: int):
        if self.cursor < value:
            self._cursor = value
            self._has_greater_cursor = True

    def close(self):
        try:
            if self._has_greater_cursor:
                self.collection.update_one({"site_id": self.site_id}, {"$set": {"cursor": self.cursor}})
                self.logger.info(
                    f"更新游标到: {self.cursor}, 与上轮差值 +{self.cursor-self._previous_round_cursor}"
                )
            else:
                self.logger.info(f"本轮没有更新游标, 现在的游标是：{self.cursor}")
        finally:
            self.mongo_client.close()
