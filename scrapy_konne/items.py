from dataclasses import dataclass
from dataclasses import field
from datetime import datetime


@dataclass
class DetailDataItem:
    """用于表示详情页提交数据的dataclass。

    能够提供字段合法性校验、数据过滤和自动类型转换功能

    它可以自动识别和转换以下几种常见的日期时间格式：

    1. 10位或13位int类型`unix时间戳`。
    2. 不带时区的`ISO 8601`时间戳，会自动转换时区到本地时间，例如 `2023-04-12 23:20:50`
    3. 带时区的`ISO 8601`时间戳，会自动转换时区到本地时间，例如 `2023-04-12T23:20:50.52Z`， `2023-04-12T23:20:50.52+02:00`等
    4. 相对时间字符串：例如 `刚刚`，`3小时前`，`1天前`，`1周前`，`1月前`，`1年前`等。
    5. 美式日期格式：例如 `04/12/2023`（月/日/年）。
    6. 完整缩写的月份名称：可以识别像 `April 12, 2023` 或 `12 Apr 2023` 这样的格式。
    7. 英文周日年格式：可以处理包含星期几的日期，如 `Wednesday, April 12, 2023`。
    8. 无分隔符的数字串：如 `20230412` 也可以被解析为日期。


    Attributes:
        title (str): 详情页标题。
        publish_time (str 或 int): 发布时间，可以填入纯数字时间戳或日期字符串，日期字符串会被智能解析。
        content (str): 详情页内容。
        source (str): 内容来源。
        source_url (str): 详情页URL。
        author (str, optional): 作者。默认为空字符串。
        author_id (str, optional): 作者ID。默认为空字符串。
        video_url (str, optional): 视频URL。默认为空字符串。
        media_type (int, optional): 媒体类型。默认为8。
        page_crawl_id (int, optional): 页面爬取ID。默认为0。
        search_crawl_id (int, optional): 搜索爬取ID。默认为0。
        ip_area (str, optional): IP所在地。默认为None。
    """

    title: str = field(default=None)
    publish_time: str | int | datetime = field(default=None)
    content: str = field(default=None)
    source: str = field(default=None)
    source_url: str = field(default=None, hash=True)
    author: str = field(default="")
    author_id: str = field(default="")
    video_url: str = field(default="")
    media_type: int = field(default=8)
    page_crawl_id: int = field(default=0)
    search_crawl_id: int = field(default=0)
    ip_area: str = field(default="")
    video_image: str = field(default="")


@dataclass
class IncreamentItem(DetailDataItem):
    """
    用于表示一个自增详情页，继承自DetailDataItem。
    """

    increment_id: int = field(default=None)


@dataclass
class HeartbeatLogItem:
    """
    心跳日志项类，用于表示一个心跳日志。
    """

    site_id: int
    client_id: str
    heartbeat_time: str
