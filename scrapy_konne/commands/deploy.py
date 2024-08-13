import subprocess
import json

from scrapy.commands import ScrapyCommand


def deploy_project():
    command = ["scrapyd-deploy","-a"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = process.communicate()

    if error:
        print(f"执行命令失败: {error}")
    else:
        data = json.loads(output)
        print("节点名称：", data["node_name"])
        if data["status"] == "ok":
            print(
                "部署成功：",
                data["project"],
                "版本：",
                data["version"],
                "爬虫个数：",
                data["spiders"],
            )
        else:
            print(data["message"])
            print("部署失败")


class Deploy(ScrapyCommand):
    requires_project = True
    default_settings = {"LOG_ENABLED": False}
    
    def syntax(self):
        return "[option] 自定义项目名"

    def short_desc(self):
        return "部署项目到远端服务器，默认为scrapy.cfg的项目"

    def run(self, args, opts):
        deploy_project()
