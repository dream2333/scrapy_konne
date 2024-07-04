from scrapy.spiders import SitemapSpider as ScrapySitemapSpider
from scrapy.spiders.sitemap import Sitemap, sitemap_urls_from_robots
from dateutil.parser import parse
from datetime import datetime, timedelta
from scrapy_konne.http import KRequest


class SitemapSpider(ScrapySitemapSpider):
    expired_days: int = 5
    """sitemap中的url如果距离现在超过expired_days天，则不再抓取"""

    def _parse_sitemap(self, response):
        if response.url.endswith("/robots.txt"):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield KRequest(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                self.logger.warning(
                    "空sitemap: %(response)s",
                    {"response": response},
                    extra={"spider": self},
                )
                return
            s = Sitemap(body)
            it = self.sitemap_filter(s)
            if s.type == "sitemapindex":
                for entry in it:
                    loc = entry["loc"]
                    modtime_str = entry.get("lastmod")
                    if modtime_str:
                        modtime = parse(entry["lastmod"])
                        if datetime.now().astimezone() - modtime > timedelta(days=self.expired_days):
                            continue
                    if any(x.search(loc) for x in self._follow):
                        yield KRequest(loc, callback=self._parse_sitemap)
            elif s.type == "urlset":
                for entry in it:
                    modtime_str = entry.get("lastmod")
                    modtime = None
                    if modtime_str:
                        modtime = parse(entry["lastmod"])
                        if datetime.now().astimezone() - modtime > timedelta(days=self.expired_days):
                            continue
                    for r, c in self._cbs:
                        if r.search(entry["loc"]):
                            yield KRequest(entry["loc"], callback=c, meta={"lastmod": modtime})
                            break

    def closed(self, reason):
        self.logger.info(f"sitemap结束，原因: {reason}")
