# 全球眼境外爬虫示例
from parsel import Selector
from scrapy.http import HtmlResponse
from scrapy.spiders import Spider
from scrapy_konne import DetailDataItem
from scrapy_konne.constants import LANG, LOG_TYPE, LOCALE


class 对华援助新闻(Spider):
    name = "对华援助新闻"
    site_id = 224
    log_type = LOG_TYPE.NO_LOG # 日志类型
    language = LANG.ZHS # 爬虫爬取的网站语言
    locale = LOCALE.OTHER # 爬虫是海外还是国内，这只会决定上传到哪个库
    start_urls = ["https://www.chinaaid.net/feeds/posts/default/"]


    def parse(self, response: HtmlResponse):
        # 使用命名空间查询
        response.selector.register_namespace("atom", "http://www.w3.org/2005/Atom")
        for entry in response.xpath("//atom:feed/atom:entry"):
            author = entry.xpath(".//atom:author/atom:name/text()").get()
            title = entry.xpath(".//atom:title/text()").get()
            link = entry.xpath(".//atom:link[@rel='alternate']/@href").get()
            raw_content = entry.xpath(".//atom:content/text()").get()
            selector = Selector(text=raw_content)
            content = selector.xpath("string()").get()
            pub_time = entry.xpath(".//atom:published/text()").get()
            item = DetailDataItem(
                source_url=link,
                author=author,
                source="对华援助新闻",
                title=title,
                content=content,
                publish_time=pub_time,
                media_type=1,
            )
            yield item
