import logging

from scrapy import Spider
from scrapy.core.scheduler import BaseScheduler
from scrapy.crawler import Crawler
from scrapy.http.request import Request
from twisted.internet.defer import Deferred

from typing import Optional, Type, TypeVar


from scrapy.dupefilters import BaseDupeFilter

from scrapy.statscollectors import StatsCollector
from scrapy.utils.misc import create_instance, load_object
from scrapy_konne.core.signals import Event

logger = logging.getLogger(__name__)
SchedulerTV = TypeVar("SchedulerTV", bound="RedisScheduler")


class RedisScheduler(BaseScheduler):
    def __init__(
        self,
        dupefilter: BaseDupeFilter,
        rqclass=None,
        logunser: bool = False,
        stats: Optional[StatsCollector] = None,
        crawler: Optional[Crawler] = None,
    ):
        self.df: BaseDupeFilter = dupefilter
        self.rqclass = rqclass
        self.logunser: bool = logunser
        self.stats: Optional[StatsCollector] = stats
        self.crawler: Optional[Crawler] = crawler
        crawler.signals.connect(self.request_callback_done, signal=Event.REQUEST_ACK)

    @classmethod
    def from_crawler(cls: Type[SchedulerTV], crawler: Crawler) -> SchedulerTV:
        """
        Factory method, initializes the scheduler with arguments taken from the crawl settings
        """
        dupefilter_cls = load_object(crawler.settings["DUPEFILTER_CLASS"])
        return cls(
            dupefilter=create_instance(dupefilter_cls, crawler.settings, crawler),
            rqclass=load_object(crawler.settings["SCHEDULER_MESSAGE_QUEUE_CLASS"]),
            logunser=crawler.settings.getbool("SCHEDULER_DEBUG"),
            stats=crawler.stats,
            crawler=crawler,
        )

    def has_pending_requests(self) -> bool:
        return len(self) > 0

    def open(self, spider: Spider) -> Optional[Deferred]:
        self.spider = spider
        self.redis_mq = self._mq()
        return self.df.open()

    def close(self, reason: str) -> Optional[Deferred]:
        if self.redis_mq is not None:
            self.redis_mq.close()
        return self.df.close(reason)

    def enqueue_request(self, request: Request) -> bool:
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        self.redis_mq.push(request)
        assert self.stats is not None
        self.stats.inc_value("scheduler/enqueued/redis_mq", spider=self.spider)
        self.stats.inc_value("scheduler/enqueued", spider=self.spider)
        return True

    def next_request(self) -> Optional[Request]:
        request: Optional[Request] = self.redis_mq.pop()
        assert self.stats is not None
        if request is not None:
            self.stats.inc_value("scheduler/dequeued/redis_mq", spider=self.spider)
            self.stats.inc_value("scheduler/dequeued", spider=self.spider)
        return request

    def __len__(self) -> int:
        return len(self.redis_mq)

    def _mq(self):
        return create_instance(
            self.rqclass,
            settings=None,
            crawler=self.crawler,
            key=f"request_queue:{self.spider.name}",
        )

    def request_callback_done(self, signal, sender, request, spider):
        # self.redis_mq.ack(request.bindata)
        ...
