from dataclasses import dataclass
from dataclasses import field
from datetime import datetime


@dataclass
class DetailDataItem:
    """
    用于表示一个详情页的信息。

    Attributes:
        title (str): 详情页标题。
        publish_time (str 或 int): 发布时间，可以填入纯数字时间戳或日期字符串。
        content (str): 详情页内容。
        source (str): 内容来源。
        source_url (str): 详情页URL。
        author (str, optional): 作者。默认为空字符串。
        author_id (str, optional): 作者ID。默认为空字符串。
        video_url (str, optional): 视频URL。默认为空字符串。
        media_type (int, optional): 媒体类型。默认为8。
        page_crawl_id (int, optional): 页面爬取ID。默认为0。
        search_crawl_id (int, optional): 搜索爬取ID。默认为0。
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


@dataclass
class HeartbeatLogItem:
    """
    心跳日志项类，用于表示一个心跳日志。
    """

    site_id: int
    client_id: str
    heartbeat_time: str
