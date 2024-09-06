import logging
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


class FakeUADownloaderMiddleware:
    def __init__(self):
        self.ua = UserAgent()

    async def process_request(self, request, spider):
        ua = self.ua.random
        request.headers.setdefault(
            "User-Agent",ua
        )
