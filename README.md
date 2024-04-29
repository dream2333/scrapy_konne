# scrapy_konne

小组内部使用Scrapy项目


## 主要组件

- `core/`: 包含了项目的核心功能，如中间件、调度器、序列化器、信号和爬虫。
- `middlewares/`: 包含了项目的中间件，如 JavaScript 和代理中间件。
- `utils/`: 包含了项目的工具函数。

## 如何运行

首先，确保你已经安装了 Poetry。然后，你可以使用以下命令安装项目的依赖：

```sh
poetry add git+https://github.com/dream2333/scrapy_konne.git#main
```

然后配置好settings.py，添加相应的中间件和pipeline

运行项目：

```sh
scrapy crawl <spider_name>
```


## 贡献

欢迎对项目进行贡献。如果你有任何问题或建议，请提交 issue 或 pull request。