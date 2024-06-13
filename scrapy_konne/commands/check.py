from scrapy.commands.check import Command


def get_all_spiders(process):
    spider_loader = process.spider_loader
    spider_names = spider_loader.list()
    for name in spider_names:
        spider_cls = spider_loader.load(name)
        yield spider_cls


def check_spider(process):
    site_ids_dup = {}
    site_name_dup = {}
    client_ids_dup = {}
    for cls in get_all_spiders(process):
        package_name = cls
        if cls.site_id in site_ids_dup:
            print(f"{package_name} / {site_ids_dup[cls.site_id]} site_id重复：{cls.site_id}")
        if cls.name in site_name_dup:
            print(f"{package_name} / {site_name_dup[cls.name]} name重复：{cls.name}")
        if cls.client_id in client_ids_dup:
            print(f"{package_name} / {client_ids_dup[cls.client_id]} client_id重复：{cls.client_id}")
        site_ids_dup[cls.site_id] = package_name
        site_name_dup[cls.name] = package_name
        client_ids_dup[cls.client_id] = package_name


class Check(Command):
    requires_project = True
    default_settings = {"LOG_ENABLED": False}

    def syntax(self):
        return "[option] 爬虫名"

    def short_desc(self):
        return "检查scrapy爬虫的合约、以及康奈爬虫的合约是否符合规范"

    def run(self, args, opts):
        print("检查site_id、name、client_id等字段是否在多个爬虫中存在重复")
        check_spider(self.crawler_process)
        print("\n\n")
        print("检查scrapy爬虫中是否含有错误")
        super().run(args, opts)
