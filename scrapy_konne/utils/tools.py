import re
import datetime
from lxml.html.clean import Cleaner

tag_cleaner = Cleaner(
    style=True,
    scripts=True,
    page_structure=False,
    safe_attrs_only=False,
    # remove_tags=("br", "p"),
)


_regexs = {}


def timestamp_to_date(timestamp: int):
    # 将时间戳转换为datetime对象
    dt = datetime.datetime.fromtimestamp(timestamp)
    # 将datetime对象转换为字符串格式的时间
    time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    return time_str


def get_current_date(date_format="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.now().strftime(date_format)


def get_info(html, regexs, allow_repeat=True, fetch_one=False, split=None):
    regexs = isinstance(regexs, str) and [regexs] or regexs

    infos = []
    for regex in regexs:
        if regex == "":
            continue

        if regex not in _regexs.keys():
            _regexs[regex] = re.compile(regex, re.S)

        if fetch_one:
            infos = _regexs[regex].search(html)
            if infos:
                infos = infos.groups()
            else:
                continue
        else:
            infos = _regexs[regex].findall(str(html))

        if len(infos) > 0:
            # print(regex)
            break

    if fetch_one:
        infos = infos if infos else ("",)
        return infos if len(infos) > 1 else infos[0]
    else:
        infos = allow_repeat and infos or sorted(set(infos), key=infos.index)
        infos = split.join(infos) if split else infos
        return infos


def transform_lower_num(data_str: str):
    num_map = {
        "一": "1",
        "二": "2",
        "两": "2",
        "三": "3",
        "四": "4",
        "五": "5",
        "六": "6",
        "七": "7",
        "八": "8",
        "九": "9",
        "十": "0",
    }
    pattern = f'[{"|".join(num_map.keys())}|零]'
    res = re.search(pattern, data_str)
    if not res:
        #  如果字符串中没有包含中文数字 不做处理 直接返回
        return data_str

    data_str = data_str.replace("0", "零")
    for n in num_map:
        data_str = data_str.replace(n, num_map[n])

    re_data_str = re.findall(r"\d+", data_str)
    for i in re_data_str:
        if len(i) == 3:
            new_i = i.replace("0", "")
            data_str = data_str.replace(i, new_i, 1)
        elif len(i) == 4:
            new_i = i.replace("10", "")
            data_str = data_str.replace(i, new_i, 1)
        elif len(i) == 2 and int(i) < 10:
            new_i = int(i) + 10
            data_str = data_str.replace(i, str(new_i), 1)
        elif len(i) == 1 and int(i) == 0:
            new_i = int(i) + 10
            data_str = data_str.replace(i, str(new_i), 1)

    return data_str.replace("零", "0")


def format_date(date, old_format="", new_format="%Y-%m-%d %H:%M:%S"):
    """
    @summary: 格式化日期格式
    ---------
    @param date: 日期 eg：2017年4月17日 3时27分12秒
    @param old_format: 原来的日期格式 如 '%Y年%m月%d日 %H时%M分%S秒'
        %y 两位数的年份表示（00-99）
        %Y 四位数的年份表示（000-9999）
        %m 月份（01-12）
        %d 月内中的一天（0-31）
        %H 24小时制小时数（0-23）
        %I 12小时制小时数（01-12）
        %M 分钟数（00-59）
        %S 秒（00-59）
    @param new_format: 输出的日期格式
    ---------
    @result: 格式化后的日期，类型为字符串 如2017-4-17 03:27:12
    """
    if not date:
        return ""

    if not old_format:
        regex = r"(\d+)"
        numbers = get_info(date, regex, allow_repeat=True)
        formats = ["%Y", "%m", "%d", "%H", "%M", "%S"]
        old_format = date
        for i, number in enumerate(numbers[:6]):
            if i == 0 and len(number) == 2:  # 年份可能是两位 用小%y
                old_format = old_format.replace(
                    number, formats[i].lower(), 1
                )  # 替换一次 '2017年11月30日 11:49' 防止替换11月时，替换11小时
            else:
                old_format = old_format.replace(number, formats[i], 1)  # 替换一次

    try:
        date_obj = datetime.datetime.strptime(date, old_format)
        if "T" in date and "Z" in date:
            date_obj += datetime.timedelta(hours=8)
            date_str = date_obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            date_str = datetime.datetime.strftime(date_obj, new_format)

    except Exception as e:
        print("日期格式化出错，old_format = %s 不符合 %s 格式" % (old_format, date))
        date_str = date

    return date_str


def format_time(release_time, date_format="%Y-%m-%d %H:%M:%S"):
    """
    >>> format_time("2个月前")
    '2021-08-15 16:24:21'
    >>> format_time("2月前")
    '2021-08-15 16:24:36'
    """
    release_time = transform_lower_num(release_time)
    release_time = release_time.replace("日", "天").replace("/", "-")

    if "年前" in release_time:
        years = re.compile(r"(\d+)\s*年前").findall(release_time)
        years_ago = datetime.datetime.now() - datetime.timedelta(days=int(years[0]) * 365)
        release_time = years_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "月前" in release_time:
        months = re.compile(r"(\d+)[\s个]*月前").findall(release_time)
        months_ago = datetime.datetime.now() - datetime.timedelta(days=int(months[0]) * 30)
        release_time = months_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "周前" in release_time:
        weeks = re.compile(r"(\d+)\s*周前").findall(release_time)
        weeks_ago = datetime.datetime.now() - datetime.timedelta(days=int(weeks[0]) * 7)
        release_time = weeks_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "天前" in release_time:
        ndays = re.compile(r"(\d+)\s*天前").findall(release_time)
        days_ago = datetime.datetime.now() - datetime.timedelta(days=int(ndays[0]))
        release_time = days_ago.strftime("%Y-%m-%d %H:%M:%S")
    elif "半小时前" in release_time:
        hours_ago = datetime.datetime.now() - datetime.timedelta(hours=0.5)
        release_time = hours_ago.strftime("%Y-%m-%d %H:%M:%S")
    elif "小时前" in release_time:
        nhours = re.compile(r"(\d+)\s*小时前").findall(release_time)
        hours_ago = datetime.datetime.now() - datetime.timedelta(hours=int(nhours[0]))
        release_time = hours_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "分钟前" in release_time:
        nminutes = re.compile(r"(\d+)\s*分钟前").findall(release_time)
        minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=int(nminutes[0]))
        release_time = minutes_ago.strftime("%Y-%m-%d %H:%M:%S")
    elif "秒前" in release_time:
        nsec = re.compile(r"(\d+)\s*秒前").findall(release_time)
        secs_ago = datetime.datetime.now() - datetime.timedelta(seconds=int(nsec[0]))
        release_time = secs_ago.strftime("%Y-%m-%d %H:%M:%S")
    elif "前天" in release_time:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=2)
        release_time = release_time.replace("前天", str(yesterday))

    elif "昨天" in release_time:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        release_time = release_time.replace("昨天", str(yesterday))

    elif "今天" in release_time:
        release_time = release_time.replace("今天", get_current_date("%Y-%m-%d"))

    elif "刚刚" in release_time:
        release_time = get_current_date()

    elif re.search(r"^\d\d:\d\d", release_time):
        release_time = get_current_date("%Y-%m-%d") + " " + release_time

    elif not re.compile(r"\d{4}").findall(release_time):
        month = re.compile(r"\d{1,2}").findall(release_time)
        if month and int(month[0]) <= int(get_current_date("%m")):
            release_time = get_current_date("%Y") + "-" + release_time
        else:
            release_time = str(int(get_current_date("%Y")) - 1) + "-" + release_time

    # 把日和小时粘在一起的拆开
    template = re.compile(r"(\d{4}-\d{1,2}-\d{2})(\d{1,2})")
    release_time = template.sub(r"\1 \2", release_time)
    release_time = format_date(release_time, new_format=date_format)
    return release_time
