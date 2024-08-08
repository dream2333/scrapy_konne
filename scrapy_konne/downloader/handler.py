from scrapy import Request, Spider
from scrapy_impersonate.handler import ImpersonateDownloadHandler as CurlImpersonateDownloadHandler
from twisted.internet.defer import Deferred


class ImpersonateDownloadHandler(CurlImpersonateDownloadHandler):
    def download_request(self, request: Request, spider: Spider) -> Deferred:
        if "impersonate" in request.meta and request.meta["impersonate"] is None:
            return super().download_request(request, spider)
        return self._download_request(request, spider)
