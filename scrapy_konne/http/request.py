from typing import Callable, List, Optional, Tuple, Union
from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.request.json_request import JsonRequest as ScrapyJsonRequest
from scrapy.http.request.form import FormRequest as ScrapyFormRequest


class KRequest(ScrapyRequest):
    """重写Scrapy的Request类，增加了filter_url、cursor、rotate_proxy属性。"""

    attributes: Tuple[str, ...] = ScrapyRequest.attributes + ("filter_url", "cursor", "rotate_proxy")

    def __init__(
        self,
        url: str,
        callback: Optional[Callable] = None,
        method: str = "GET",
        headers: Optional[dict] = None,
        body: Optional[Union[bytes, str]] = None,
        cookies: Optional[Union[dict, List[dict]]] = None,
        meta: Optional[dict] = None,
        encoding: str = "utf-8",
        priority: int = 0,
        dont_filter: bool = False,
        errback: Optional[Callable] = None,
        flags: Optional[List[str]] = None,
        cb_kwargs: Optional[dict] = None,
        filter_url: Optional[str] = None,
        cursor: Optional[int] = None,
        rotate_proxy: bool = False,
    ) -> None:
        """
        Args:
            url (str): 要请求的URL。
            callback (Optional[Callable], optional): 接收到响应时要调用的回调函数。
            method (str, optional): 请求使用的HTTP方法。
            headers (Optional[dict], optional): 请求中要包含的头部信息。
            body (Optional[Union[bytes, str]], optional): 请求的主体内容。
            cookies (Optional[Union[dict, List[dict]]], optional): 请求中要包含的Cookie信息。
            meta (Optional[dict], optional): 请求的附加元数据。
            encoding (str, optional): 请求使用的编码方式。
            priority (int, optional): 请求的优先级。
            dont_filter (bool, optional): 是否应用请求过滤。
            errback (Optional[Callable], optional): 发生错误时要调用的回调函数。
            flags (Optional[List[str]], optional): 请求的附加标志。
            cb_kwargs (Optional[dict], optional): 回调函数的附加关键字参数。
            filter_url (Optional[str], optional): 如果当前请求是``板块详情页``，会yield一个``item``出来，``filter_url`` 可以设置为item的``source_url``，框架会自动根据redis里存在的``source_url``对请求进行去重节省带宽。
            cursor (Optional[int], optional): 如果当前请求是``自增详情页``，会yield一个``item``出来，``cursor`` 可以设置为自增爬虫当前页的游标，框架会自动根据redis里存在的``游标``对请求进行去重节省带宽。
            rotate_proxy (bool, optional): 是否使用代理池对请求进行代理轮换。
        """
        super().__init__(
            url,
            callback,
            method,
            headers,
            body,
            cookies,
            meta,
            encoding,
            priority,
            dont_filter,
            errback,
            flags,
            cb_kwargs,
        )
        self.filter_url = filter_url
        self.cursor = cursor
        self.rotate_proxy = rotate_proxy

    @property
    def filter_url(self) -> Optional[str]:
        return self._filter_url

    @filter_url.setter
    def filter_url(self, filter_url: Optional[str]) -> None:
        self._filter_url = filter_url
        self.meta["filter_url"] = filter_url

    @property
    def cursor(self) -> Optional[int]:
        return self._cursor

    @cursor.setter
    def cursor(self, cursor: Optional[int]) -> None:
        self._cursor = cursor
        self.meta["cursor"] = cursor

    @property
    def rotate_proxy(self) -> bool:
        return self._rotate_proxy

    @rotate_proxy.setter
    def rotate_proxy(self, rotate_proxy: bool) -> None:
        self._rotate_proxy = rotate_proxy
        self.meta["rotate_proxy"] = rotate_proxy


class KJsonRequest(ScrapyJsonRequest, KRequest):
    attributes = tuple(set(KRequest.attributes + ScrapyJsonRequest.attributes))


class KFormRequest(ScrapyFormRequest, KRequest):
    attributes = tuple(set(KRequest.attributes + ScrapyFormRequest.attributes))
