from scrapy.spiders import SitemapSpider as ScrapySitemapSpider
from scrapy.spiders.sitemap import Sitemap as ScrapySitemap, sitemap_urls_from_robots
from dateutil.parser import parse
from datetime import datetime, timedelta
from scrapy_konne.http import KRequest
from typing import Any, Dict, Iterator


class Sitemap(ScrapySitemap):

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        for elem in self._root.getchildren():
            d: Dict[str, Any] = {}
            for el in elem.getchildren():
                tag = el.tag
                name = tag.split("}", 1)[1] if "}" in tag else tag

                if name == "link":
                    if "href" in el.attrib:
                        d.setdefault("alternate", []).append(el.get("href"))
                elif name == "news":
                    ns = {"news": el.nsmap["news"]}
                    pub_date = el.xpath("./news:publication_date/text()", namespaces=ns)
                    title = el.xpath("./news:title/text()", namespaces=ns)
                    if pub_date:
                        d["publication_date"] = pub_date[0]
                    if title:
                        d["title"] = title[0]
                else:
                    d[name] = el.text.strip() if el.text else ""

            if "loc" in d:
                yield d


class SitemapSpider(ScrapySitemapSpider):
    expired_days: int = 5
    """sitemap中的url如果距离现在超过expired_days天，则不再抓取"""

    def expired_check(self, time_str: str) -> bool:
        if time_str:
            modtime = parse(time_str).astimezone()
            if datetime.now().astimezone() - modtime > timedelta(days=self.expired_days):
                return True
        return False

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
                    if self.expired_check(modtime_str):
                        continue
                    if any(x.search(loc) for x in self._follow):
                        yield KRequest(loc, callback=self._parse_sitemap)
            elif s.type == "urlset":
                for entry in it:
                    modtime_str = entry.get("lastmod")
                    publication_date_str = entry.get("publication_date")
                    time_str = publication_date_str or modtime_str
                    if self.expired_check(modtime_str):
                        continue
                    title = entry.get("title")
                    for r, c in self._cbs:
                        if r.search(entry["loc"]):
                            yield KRequest(
                                entry["loc"],
                                callback=c,
                                meta={"lastmod": time_str, "title": title},
                            )
                            break

    def closed(self, reason):
        self.logger.info(f"sitemap结束，原因: {reason}")
