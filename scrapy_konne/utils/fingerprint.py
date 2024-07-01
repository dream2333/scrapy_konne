import mmh3
from w3lib.url import canonicalize_url


def get_url_fp(url: str):
    """对url标准化后进行hash计算，返回128位的hash值"""
    return mmh3.hash128(canonicalize_url(url, keep_fragments=True))
