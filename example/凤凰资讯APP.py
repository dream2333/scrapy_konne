import json
from typing import Iterable
from scrapy.http import HtmlResponse, Request
from scrapy_konne import DetailDataItem, KRequest
from scrapy import Spider

from scrapy_konne.constants import LOG_TYPE


# 含有三级页面的板块爬虫示例，不需要关心日志、去重、上传等逻辑，只需要关心数据解析和请求的发起
class FengHuangShanXiaIncreament(Spider):
    name = "凤凰资讯APP"
    site_id = 36
    client_id = "fenghuangzixun_app_section_1"
    # 可不填，默认为LOG_TYPE.SECTION
    log_type = LOG_TYPE.SECTION
    # 如果添加start_urls，那么scrapy会默认以get方法请求start_urls中的url，并调用parse回调方法
    # start_urls = ["https://i.ifeng.com/", "https://ient.ifeng.com/"]

    # 如果不使用start_urls，可以重写start_requests方法,并将Request对象yield出去
    def start_requests(self) -> Iterable[Request]:
        sources = [
            {"name": "资讯", "url": "https://inews.ifeng.com/?srctag=xzydh1"},
            {"name": "娱乐", "url": "https://ient.ifeng.com/?srctag=xzydh2"},
            {"name": "财经", "url": "https://ifinance.ifeng.com/?srctag=xzydh4"},
            {"name": "军事", "url": "https://imil.ifeng.com/?srctag=xzydh6"},
            {"name": "科技", "url": "https://itech.ifeng.com/?srctag=xzydh10"},
            {"name": "体育", "url": "https://isports.ifeng.com/?srctag=xzysp1"},
            {"name": "时尚", "url": "https://ifashion.ifeng.com/?srctag=xzydh9"},
            {"name": "历史", "url": "https://ihistory.ifeng.com/?srctag=xzydh12"},
            {"name": "台湾", "url": "https://itaiwan.ifeng.com/index.shtml"},
            {"name": "港澳", "url": "https://ihistory.ifeng.com/?srctag=xzydh12"},
            {"name": "公益", "url": "https://igongyi.ifeng.com/?srctag=igongyi"},
            {"name": "旅游", "url": "https://itravel.ifeng.com/?srctag=itravel1"},
            {"name": "健康", "url": "https://ihealth.ifeng.com/?srctag=ihealth2"},
            {"name": "文化", "url": "https://iculture.ifeng.com/?srctag=iculture"},
            {"name": "国学", "url": "https://iguoxue.ifeng.com/?srctag=iguoxue"},
            {"name": "NBA", "url": "https://isports.ifeng.com/nba/?srctag=sfnbadh1"},
        ]
        for source in sources:
            # yield出去的Request对象会被scrapy调度器调度，然后发送请求到下载器中异步下载
            # 下载完成会调用回调方法，此处未填写回调方法，默认的回调方法是parse方法
            # cb_kwargs参数可以传递额外的参数到回调方法中
            yield KRequest(source["url"], cb_kwargs={"section_name": source["name"]})

    # 对板块页面进行解析,section_name是通过cb_kwargs传递过来的
    def parse(self, response: HtmlResponse, section_name: str):
        data = response.selector.re_first(r"var allData =([\s\S]*);[\s\S]+var adKeys =")
        json_data = json.loads(data)
        article_list = json_data["newsstream"]
        for article in article_list:
            # 通过DetailDataItem类封装数据，这样可以自动进行数据校验和类型转换
            # 时间可以填入10位、13位时间戳、字符串，也可以填入datetime对象
            item = DetailDataItem(
                title=article["title"],
                source_url=article["url"],
                publish_time=article["newsTime"],
                source=f"凤凰资讯APP-{section_name}",
            )
            # 将item也携带到下一级页面，这样在解析详情页时可以直接使用item
            yield KRequest(
                article["url"],
                filter_url=item.source_url,
                cb_kwargs={"item": item},
                callback=self.parse_detail,
            )

    # 解析详情页，补全仅剩的字段
    def parse_detail(self, response: HtmlResponse, item: DetailDataItem):
        item.content = response.xpath("string(//div[contains(@class,'index_main_content')])").get()
        item.author = response.xpath("string(//span[contains(@class,'index_source')])").get().strip()
        yield item
