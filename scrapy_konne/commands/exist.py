from datetime import datetime
from redis import Redis
import requests
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError
from scrapy.utils.project import get_project_settings
from scrapy_konne.utils.fingerprint import get_url_fp

settings = get_project_settings()


def redis_url_filter(url, source):
    redis_url = settings.get("REDIS_URL")
    client = Redis.from_url(redis_url)
    hash_value = get_url_fp(url)
    result = client.zscore(f"dupefilter:{source}", hash_value)
    return result//1000



def redis_cursor_filter(cursor, source):
    redis_url = settings.get("REDIS_URL")
    client = Redis.from_url(redis_url)
    result = client.zscore(f"dupefilter:{source}", cursor)
    return result//1000


def url_is_exist(url):
    """
    判断url是否存在  1是有, 0是没有
    :return: True or False
    """
    upload_ip = settings.get("DEDUP_DATA_IP")
    base_url = f"http://{upload_ip}/BanKuaiQuChong/ExistUrl"
    params = {"url": url}
    resp = requests.get(url=base_url, params=params, timeout=10)
    if resp.text == "1":
        return True
    else:
        return False


class Exist(ScrapyCommand):
    requires_project = True
    default_settings = {"LOG_ENABLED": False}

    def syntax(self):
        return "<去重链接或自增id>  <爬虫类变量name>"

    def short_desc(self):
        return "查看某个链接或id是否已进入去重库"

    def run(self, args, opts):
        is_url = True
        try:
            if len(args) != 0:
                raise UsageError("参数数量不对")
            if len(args) == 0:
                print(
                    "\033[91m框架在去重时采用 内存->redis->公司接口 三级去重方案确保去重稳定性和速度\n此功能可以查询数据是否在redis或公司去重库中存在\033[0m"
                )
                dup_value = input("请输入url或自增id:")
            try:
                int(dup_value)
                print("输入的是自增id，仅进行redis去重查询，如需继续确认是否在康奈库中存在，请输入url")
                is_url = False
            except ValueError:
                print("输入的是url，进行url查询")
                result = url_is_exist(dup_value)
                if result:
                    print("\033[95m在康奈去重库已存在\033[0m")
                else:
                    print("\033[96m在康奈去重库不存在\033[0m")
            spider_name = input("请输入爬虫类变量name，以继续进行redis查询: ")
            if is_url:
                result = redis_url_filter(dup_value, spider_name)
            else:
                result = redis_cursor_filter(dup_value, spider_name)
            if result:
                dt_object = datetime.fromtimestamp(result)
                # 将datetime对象格式化为字符串
                date_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                print(f"\033[92m在redis去重库[{spider_name}]已存在: {date_string}\033[0m")
            else:
                print(f"\033[91m在redis去重库[{spider_name}]不存在\033[0m:")
        except KeyboardInterrupt:
            print("\n用户主动中断")
