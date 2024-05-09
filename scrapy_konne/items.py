from dataclasses import dataclass


@dataclass
class DetailDataItem:
    """
    用于表示一个详情页的信息。

    Attributes:
        title (str): 详情页标题。
        publish_time (str): 发布时间。
        content (str): 详情页内容。
        source (str): 内容来源。
        source_url (str): 详情页URL。
        site_id (int): 网站ID。
        client_id (str): 客户端ID。
        author (str, optional): 作者。默认为空字符串。
        author_id (str, optional): 作者ID。默认为空字符串。
        video_url (str, optional): 视频URL。默认为空字符串。
        media_type (int, optional): 媒体类型。默认为8。
        page_crawl_id (int, optional): 页面爬取ID。默认为0。
        search_crawl_id (int, optional): 搜索爬取ID。默认为0。
    """

    title: str = ""
    publish_time: str = ""
    content: str = ""
    source: str = ""
    source_url: str = ""
    author: str = ""
    author_id: str = ""
    video_url: str = ""
    media_type: int = 8
    page_crawl_id: int = 0
    search_crawl_id: int = 0



@dataclass
class HeartbeatLogItem:
    """
    心跳日志项类，用于表示一个心跳日志。
    """

    site_id: int
    client_id: str
    heartbeat_time: str
