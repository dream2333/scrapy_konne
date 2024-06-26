from enum import Enum
import json


class LOG_TYPE(Enum):
    """
    日志类型

    - NO_LOG: 不开启远程日志记录
    - SECTION: 板块
    - INCREASE: 自增
    """

    NO_LOG = -1
    SECTION = 0
    INCREASE = 1


class LANG(Enum):
    """
    网站的语言

    每个成员代表一种语言，成员值用于唯一标识语言。

    语言列表:
    - ZHS: 中文（简体）
    - ZHT: 中文（繁体）
    - BO: 藏语
    - UYG: 维语
    - EN: 英语
    - KO: 韩语
    - FR: 法语
    - JA: 日语
    - DE: 德语
    - RU: 俄语
    - AR: 阿拉伯语
    - OTHER: 其他语言
    """

    ZHS = 1
    """中文（简体）"""
    ZHT = -71
    """中文（繁体）"""
    BO = 3
    """藏语"""
    UYG = 2
    """维语"""
    EN = 4
    """英语"""
    KO = 16
    """韩语"""
    FR = 15
    """法语"""
    JA = 13
    """日语"""
    DE = 14
    """德语"""
    RU = 7
    """俄语"""
    AR = 10
    """阿拉伯语"""
    OTHER = 99
    """其他语言"""


class LOCALE(Enum):
    """
    网站的地区，如为境外则会使用固定境外代理下载中间件

    每个成员代表一种地区，成员值用于唯一标识地区。

    地区列表:
    - CN: 中国
    - US: 美国
    - JP: 日本
    - KR: 韩国
    - FR: 法国
    - DE: 德国
    - RU: 俄罗斯
    - SA: 沙特阿拉伯
    - OTHER: 其他海外地区
    """

    CN = 1
    """中国"""
    US = 2
    """美国"""
    JP = 3
    """日本"""
    KR = 4
    """韩国"""
    FR = 5
    """法国"""
    DE = 6
    """德国"""
    RU = 7
    """俄罗斯"""
    SA = 8
    """沙特阿拉伯"""
    OTHER = -1
    """其他海外地区"""
