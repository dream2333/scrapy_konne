from typing import Any, Iterable
import pymongo
from scrapy_konne import DetailDataItem
from scrapy import Request, Spider
from scrapy.exceptions import CloseSpider


class IncreaseSpiderMiddleware:
    def process_spider_output(self, response, result, spider):
        for i in result:
            if isinstance(i, DetailDataItem):
                spider.cursor = response.meta["cursor"]
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
    cursor_name: str = "cursor"
    """数据库中游标的字段名，默认为cursor"""

    __mongo_client = None
    __collection = None

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
        elif (
            getattr(self, "url_template", None) is None
            and self.start_requests == IncreaseSpider.start_requests
        ):
            raise ValueError("url_template和start_requests之中至少有一个需要被重载")

    def start_requests(self) -> Iterable[Request]:
        for i in self.start_ids:
            url = self.url_template.format(cursor=i)
            yield Request(url, dont_filter=True, meta={"cursor": i})

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IncreaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        if cls.__mongo_client is None:
            mongo_url = crawler.settings.get("MONGO_URL")
            cls.__mongo_client = pymongo.MongoClient(mongo_url, timeoutMS=10000)
            cls.__collection = spider.__mongo_client["Scrapy"]["ids_state"]
        spider.settings["SPIDER_MIDDLEWARES"][IncreaseSpiderMiddleware] = 0
        return spider

    @property
    def start_ids(self) -> Iterable[int]:
        """获取游标前后范围内的id"""
        for i in range(self.cursor - self.offset[0], self.cursor + self.offset[1]):
            yield i

    @property
    def cursor(self) -> int:
        if self._cursor is None:
            # 初始化时从数据库中获取游标
            db_meta = self.__collection.find_one({"site_id": self.site_id})
            if db_meta is None:
                raise CloseSpider("请先在数据库中初始化游标")
            self._cursor = db_meta[self.cursor_name]
            self._previous_round_cursor = self._cursor
            self.logger.info(f"数据库id游标: {self._cursor}，前后偏移范围：{self.offset}")
        return self._cursor  # getter方法

    @cursor.setter
    def cursor(self, value: int):
        if self.cursor < value:
            self._cursor = value
            self._has_greater_cursor = True

    def close(self):
        try:
            if self._has_greater_cursor:
                self.__collection.update_one(
                    {"site_id": self.site_id}, {"$set": {self.cursor_name: self.cursor}}
                )
                self.logger.info(
                    f"更新游标到: {self.cursor}, 与上轮差值 +{self.cursor-self._previous_round_cursor}"
                )
            else:
                self.logger.info(f"本轮没有更新游标, 现在的游标是：{self.cursor}")
        finally:
            self.__mongo_client.close()
