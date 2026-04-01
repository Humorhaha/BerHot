"""
Scrapy 设置文件
"""

# 项目设置
BOT_NAME = "zhihu"
SPIDER_MODULES = ["zhihu.spiders"]
NEWSPIDER_MODULE = "zhihu.spiders"

# 遵守 robots.txt
ROBOTSTXT_OBEY = False  # 知乎的 robots.txt 会限制爬虫，但 API 端点是公开的

# 并发设置（匿名模式保守策略）
CONCURRENT_REQUESTS = 1              # 匿名模式下串行
CONCURRENT_REQUESTS_PER_DOMAIN = 1

# 下载延迟
DOWNLOAD_DELAY = 3                   # 基础延迟 3 秒
RANDOMIZE_DOWNLOAD_DELAY = True      # 实际延迟 = 0.5×~1.5× DOWNLOAD_DELAY

# 自动节流
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 3
AUTOTHROTTLE_MAX_DELAY = 30          # 被限速时自动退避到 30 秒
AUTOTHROTTLE_TARGET_CONCURRENCY = 0.5

# Cookie 设置
# 匿名模式: COOKIES_ENABLED = False
# 登录模式: COOKIES_ENABLED = True 并设置 ZHIHU_COOKIES
COOKIES_ENABLED = False

# 知乎登录 Cookie（从浏览器开发者工具复制）
# 格式: "_zap=xxx; d_c0=xxx; ..."
# ZHIHU_COOKIES = "_zap=xxx; d_c0=xxx; ..."

# Telnet 控制台
TELNETCONSOLE_ENABLED = False

# 默认请求头
DEFAULT_REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.zhihu.com/",
    "Connection": "keep-alive",
}

# 爬虫中间件
SPIDER_MIDDLEWARES = {
    "zhihu.middlewares.ExponentialBackoffMiddleware": 100,
}

# 下载器中间件
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "zhihu.middlewares.RandomUserAgentMiddleware": 400,
    "zhihu.middlewares.ZhihuHeadersMiddleware": 410,
    "zhihu.middlewares.ZhihuCookieMiddleware": 415,
    "zhihu.middlewares.ProxyMiddleware": 420,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 90,
}

# 重试设置
RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [403, 429, 500, 502, 503, 504]

# Pipeline
ITEM_PIPELINES = {
    "zhihu.pipelines.ZhihuCleanPipeline": 100,
    "zhihu.pipelines.ZhihuDeduplicationPipeline": 200,
    "zhihu.pipelines.BERTopicExportPipeline": 300,
}

# 输出目录
OUTPUT_DIR = "data"

# 日志设置
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(levelname)s: %(message)s"

# 请求超时
DOWNLOAD_TIMEOUT = 30

# 禁用重定向
REDIRECT_ENABLED = False

# 调度器队列
SCHEDULER_PRIORITY_QUEUE = "scrapy.pqueues.ScrapyPriorityQueue"

# DNS 缓存
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
DNSCACHE_TIMEOUT = 60 * 60 * 24  # 24 小时

# 代理池 URL（可选）
# PROXY_POOL_URL = "http://localhost:5555/random"
