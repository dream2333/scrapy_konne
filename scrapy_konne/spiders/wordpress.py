import re
from scrapy import Request, Spider
from scrapy.http import HtmlResponse
from scrapy_konne import DetailDataItem


class WordPressSpider(Spider):
    name: str
    newest_count = 100
    exclude_pattern = []
    posts_url: str
    index_url: str
    site_id: int
    page_crawl_id: int
    media_type: int = 1

    def start_requests(self):
        if not getattr(self, "posts_url", None) and not getattr(self, "index_url", None):
            raise ValueError("请设置posts_url或index_url")
        if getattr(self, "index_url", None):
            url = f"{self.index_url}/wp-json/wp/v2/posts?page=1&per_page={self.newest_count}"
        else:
            url = f"{self.posts_url}?page=1&per_page={self.newest_count}"
        yield Request(url)

    def parse(self, response: HtmlResponse):
        for article in response.json():
            url = article["link"]
            if any(re.search(pattern, url) for pattern in self.exclude_pattern):
                self.logger.info(f"跳过不需要的文章: {url}")
                continue
            title = article["title"]["rendered"]
            content = article["content"]["rendered"]
            publish_time = article["date_gmt"] + "Z"
            item = DetailDataItem(
                title=title,
                publish_time=publish_time,
                content=content,
                source=self.name,
                source_url=url,
                page_crawl_id=self.page_crawl_id,
                media_type=self.media_type,
            )
            yield item
