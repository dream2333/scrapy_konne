import traceback
from typing import Any, Iterable, Union, cast
from pymongo import MongoClient
from twisted.internet.defer import Deferred

from scrapy_konne.http import KRequest
from scrapy_konne.items import IncreamentItem
from scrapy import Spider
from scrapy.exceptions import CloseSpider


class IncreaseSpiderMiddleware:
    async def process_spider_output(self, response, result, spider):
        async for i in result:
            if isinstance(i, IncreamentItem):
                spider.cursor = i.increment_id
            yield i


class IncreaseSpider(Spider):
    name: str
    site_id: int
    offset: tuple[int, int]
    """爬虫通过文章的最新id和offset来确定更新时爬取的范围
    :必须设置offset以确定抓取范围
    :offset必须是一个长度为2的列表或元组，分别代表向前探查数量和向后探查数量"""
    mock_cursor: int = 0
    """在非生产环境下，用于模拟游标的id，用于测试"""
    url_template: str
    """url模板，格式化时传入cursor，eg:"https://example.com?tid={cursor}"""
    cursor_name: str = "cursor"
    """数据库中游标的字段名，默认为cursor"""

    __mongo_client = None
    __collection = None

    def __init__(self, name: str | None = None, is_production=False, mongo_url=None, **kwargs: Any):
        super().__init__(name, **kwargs)
        self._previous_round_cursor = None
        self._cursor: int = None
        self._has_greater_cursor = False
        self._is_production = is_production
        self.mongo_url = mongo_url
        self.attr_check()

    def attr_check(self):
        if not getattr(self, "offset", None):
            raise ValueError(
                """爬虫通过文章的最新id和offset来确定更新时爬取的范围，必须设置offset以确定抓取范围
                offset必须是一个长度为2的列表，分别代表向前探查数量和向后探查数量"""
            )
        if not isinstance(self.offset, Iterable) or len(self.offset) != 2:
            raise ValueError("offset必须是一个长度为2的列表、元组、或切片，代表向前探查数量和向后探查数量")
        if (
            getattr(self, "url_template", None) is None
            and self.start_requests == IncreaseSpider.start_requests
        ):
            raise ValueError("url_template和start_requests之中至少有一个需要被重载")
        if self._is_production:
            if not getattr(self, "site_id", None):
                raise ValueError("请设置site_id")
            if not getattr(self, "cursor_name", None):
                raise ValueError("请设置cursor_name")
        else:
            if self.mock_cursor == 0:
                self.logger.warn("测试环境下，mock_cursor默认为0，建议设置一个非0的mock_cursor")

    def start_requests(self):
        self.logger.info("未重写start_requests方法,使用默认url_template生成自增链接进行请求")
        for i in self.start_ids:
            url = self.url_template.format(cursor=i)
            yield KRequest(url, cursor=i)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        is_production = crawler.settings.getbool("PROJECT_ENV_IS_PRODUCTION")
        mongo_url = crawler.settings.get("MONGO_URL")
        spider = super().from_crawler(crawler, cls.name, is_production, mongo_url, **kwargs)
        spider.settings["SPIDER_MIDDLEWARES"][IncreaseSpiderMiddleware] = 0
        return spider

    @property
    def start_ids(self) -> Iterable[int]:
        """获取游标前后范围内的id"""
        for i in range(self.cursor - self.offset[0], self.cursor + self.offset[1]):
            yield i

    @property
    def mongo_cursor(self):
        if self.__mongo_client is None:
            if self.mongo_url is None:
                raise ValueError("请设置MONGO_URL")
            self.__mongo_client = MongoClient(self.mongo_url, timeoutMS=10000)
            self.__collection = self.__mongo_client["Scrapy"]["ids_state"]
        db_meta = self.__collection.find_one({"site_id": self.site_id})
        if db_meta is None:
            raise CloseSpider("请先在数据库中初始化游标")
        cursor = db_meta[self.cursor_name]
        return cursor

    @mongo_cursor.setter
    def mongo_cursor(self, value: int):
        self.__collection.update_one({"site_id": self.site_id}, {"$set": {self.cursor_name: value}})

    @property
    def cursor(self) -> int:
        if self._cursor is None:
            if self._is_production:
                self.init_mongo_cursor()
            else:
                self.init_mock_cursor()
        return self._cursor  # getter方法

    @cursor.setter
    def cursor(self, value: int):
        if self.cursor < value:
            self._cursor = value
            self._has_greater_cursor = True

    def init_mongo_cursor(self):
        self._cursor = self.mongo_cursor
        self._previous_round_cursor = self._cursor
        self.logger.info(f"当前id游标: {self._cursor}，前后偏移范围：{self.offset}")

    def init_mock_cursor(self):
        self._cursor = self.mock_cursor
        self._previous_round_cursor = self._cursor
        self.logger.info(
            f"当前为测试环境，使用mock_cursor模拟初始游标: {self.mock_cursor}，前后偏移范围：{self.offset}"
        )

    def update_mongo_cursor(self):
        try:
            if self._has_greater_cursor:
                self.mongo_cursor = self.cursor
                self.logger.info(
                    f"更新游标到: {self.cursor}, 与上轮差值 +{self.cursor - self._previous_round_cursor}"
                )
            else:
                self.logger.info(f"本轮没有更新游标, 现在的游标是：{self.cursor}")
        except Exception as e:
            self.logger.error(f"更新mongo游标失败: {e}")
            traceback.print_exc()

    def update_course(self):
        if not self._is_production:
            self.logger.info(f"测试环境，不更新游标: {self.cursor}")
        else:
            self.update_mongo_cursor()
            self.__mongo_client.close()

    @staticmethod
    def close(spider: "IncreaseSpider", reason: str) -> Union[Deferred, None]:
        closed = getattr(spider, "closed", None)
        if callable(closed):
            return cast(Union[Deferred, None], closed(reason))
        spider.update_course()
