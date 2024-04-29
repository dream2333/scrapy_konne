from abc import ABCMeta, abstractmethod
import time
import redis
from scrapy.utils.misc import load_object


class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class BaseMessageQueue(metaclass=ABCMeta):
    def __init__(self, crawler, key, client, serializer=None) -> None:
        self.client = client
        self.key = key
        self.serializer = serializer
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        raise NotImplementedError("from_crawler method must be implemented")

    @abstractmethod
    def push(self, value):
        raise NotImplementedError("push method must be implemented")

    @abstractmethod
    def pop(self):
        raise NotImplementedError("pop method must be implemented")


class RedisQueue(BaseMessageQueue):

    def __init__(self, crawler, key, client: redis.Redis, serializer=None) -> None:
        BaseMessageQueue.__init__(self, crawler, key, client, serializer)
        self._register_scripts()
        self.send_buffer = client.pipeline()
        self.recevie_buffer = client.pipeline()

    @classmethod
    def from_crawler(cls, crawler, key, *args, **kwargs):
        redis_url = crawler.settings.get("REDIS_URL")
        client = redis.Redis.from_url(redis_url)
        serializer_class = load_object(crawler.settings.get("REDIS_SERIALIZER"))
        serializer = serializer_class(crawler.spider)
        return cls(crawler=crawler, client=client, key=key, serializer=serializer)

    def _register_scripts(self):
        pop_lt_score_scrpits = """
        local elements = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
        if #elements > 0 then
            redis.call('ZADD', KEYS[1], ARGV[2], unpack(elements))
        end
        return elements
        """
        self._execute_pop_lt_score = self.client.register_script(pop_lt_score_scrpits)

    def pop_lt_score(self, key, score, count):
        elements = self._execute_pop_lt_score(
            keys=[key],
            args=[score, score + 20000, count],
            # client=self.client,
        )
        return elements[0] if elements else None

    def push(self, request):
        bindata = self.serializer.serialize(request)
        timestamp = int(time.time() * 1000)
        data = {bindata: timestamp}
        self.client.zadd(self.key, data)

    def pop(self):
        bindata = self.pop_lt_score(self.key, int(time.time() * 1000), 1)
        if bindata:
            request = self.serializer.deserialize(bindata)
            request.bindata = bindata
            return request

    def ack(self, bin_request):
        self.client.zrem(self.key, bin_request)

    @property
    def redis_mq_size(self):
        return self.client.zcard(self.key)

    def __len__(self):
        return self.redis_mq_size

    def close(self):
        self.client.close()
