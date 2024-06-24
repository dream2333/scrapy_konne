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
    定义支持的语言枚举。

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

