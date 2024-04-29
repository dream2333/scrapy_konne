from scrapy.dupefilters import BaseDupeFilter


class SourceUrlDupeFilter(BaseDupeFilter):
    def request_seen(self, request):
        return False