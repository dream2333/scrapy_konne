# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import re
from scrapy.http import Response, Request
from scrapy.utils.log import logger


class JslDownloaderMiddleware:
    async def process_response(self, request: Request, response: Response, spider):
        if response.status == 521:
            # 更换cookies、代理、去重标签，重新请求
            logger.warning(f"加速乐521，生成cookies：{request.url}")
            chars = re.findall(r"\('(.*?)'\)", response.text)
            cookie = "".join(chars)
            cookie_value = re.search(r"__jsl_clearance_s=(.*?);", cookie).group(1)
            request.cookies.update({"__jsl_clearance_s": cookie_value}) 
            return request
        return response
