import ormsgpack
from abc import ABCMeta, abstractmethod
from scrapy import Request
import pickle
from scrapy.utils.request import request_from_dict


class Serializer(metaclass=ABCMeta):
    __slots__ = ("spider",)

    def __init__(self, spider) -> None:
        self.spider = spider

    @abstractmethod
    def serialize(self, request: Request):
        raise NotImplementedError("serialize method must be implemented")

    @abstractmethod
    def deserialize(cls, request: Request):
        raise NotImplementedError("deserialize method must be implemented")


class MsgpackSerializer(Serializer):
    __slots__ = ("spider",)

    def serialize(self, request: Request):
        data = request.to_dict(spider=self.spider)
        return ormsgpack.packb(data, option=ormsgpack.OPT_NON_STR_KEYS)

    def deserialize(self, msg: bytes):
        msg = ormsgpack.unpackb(msg)
        request = request_from_dict(msg, spider=self.spider)
        return request


class PickleSerializer(Serializer):
    __slots__ = ("spider",)

    def serialize(self, request: Request):
        data = request.to_dict(spider=self.spider)
        return pickle.dumps(data, protocol=5)

    def deserialize(self, msg: bytes):
        msg = pickle.loads(msg)
        request = request_from_dict(msg, spider=self.spider)
        return request
