from pprint import pprint
import pymongo
from scrapy.commands import ScrapyCommand
from scrapy.utils.project import get_project_settings

settings = get_project_settings()


def add_increament_id(site_id: int, name: str, cursor: int, cursor_name="cursor"):
    """_summary_

    Args:
        site_id (int): site_id
        name (str): 非必要，用于标识
        cursor (int): 自增id
        cursor_name (str, optional): 自增id在数据库中的键名，默认为"cursor"，一般不需要修改，某个site_id下可能有多个自增id时需要修改
    """
    mongo_url = settings.get("MONGO_URL")
    client = pymongo.MongoClient(mongo_url)
    db = client["Scrapy"]
    collection = db["ids_state"]
    data = collection.find_one({"site_id": site_id}, {"_id": 0, "name": 1, cursor_name: 1, "site_id": 1})
    if data:
        pprint(data)
        stdstr = input("\033[91m>>>已存在以上数据，需要更新请按y，否则按Ctrl+C或任意键退出\033[0m")
        if stdstr.lower() == "y":
            result = collection.update_one(
                {"site_id": site_id}, {"$set": {cursor_name: cursor, "name": name}}
            )
        else:
            result = None
    else:
        result = collection.insert_one({"name": name, cursor_name: cursor, "site_id": site_id})
    if result:
        print(">>>数据更新成功")
    else:
        print(">>>数据未更新")


def upload():
    try:
        site_id = int(input("请输入site_id:"))
        name = input("请输入爬虫名（不影响业务，仅用于人工提示）:")
        cursor_name = input(
            "请输入自增id的业务名（默认为cursor，无需干预，当一个网站可能有多个自增需求时才需要人工添加）:"
        )
        cursor = int(input("请输入初始的自增id:"))
        if not cursor_name:
            cursor_name = "cursor"
        add_increament_id(
            site_id=site_id,
            name=name,
            cursor_name=cursor_name,
            cursor=cursor,
        )
    except KeyboardInterrupt:
        print("\n>>>已退出")
        return


class Increase(ScrapyCommand):
    requires_project = True
    default_settings = {"LOG_ENABLED": False}

    def syntax(self):
        return ""

    def short_desc(self):
        return "添加或维护自增项目的id"

    def run(self, args, opts):
        upload()
