# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import re
from scrapy.http import Response, Request
from scrapy_konne.middlewares.proxy import ProxyPoolDownloaderMiddleware
from scrapy.utils.log import logger


class JslDownloaderMiddleware(ProxyPoolDownloaderMiddleware):
    async def process_response(self, request: Request, response: Response, spider):
        if response.status == 521:
            # 更换cookies、代理、去重标签，重新请求
            logger.warning(f"加速乐521，生成cookies：{request.url}")
            chars = re.findall(r"\('(.*?)'\)", response.text)
            cookie = "".join(chars)
            cookie_value = re.search(r"__jsl_clearance_s=(.*?);", cookie).group(1)
            request.cookies = {"__jsl_clearance_s": cookie_value}
            request.meta["pass_jsl"] = True
            request.meta["origin_filter_flag"] = request.dont_filter
            request.dont_filter = True
            proxy = await self.get_proxy()
            request.meta["proxy"] = proxy
            return request
        return response
