from scrapy import Request, Spider
from twisted.internet.defer import Deferred

class ImpersonateDownloaderMiddleware:
    def download_request(self, request: Request, spider: Spider) -> Deferred:
        request.meta["impersonate"] = "chrome110"