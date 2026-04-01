# 知乎 Scrapy 工程化爬虫方案

> 目标：基于 Scrapy 框架，爬取知乎**话题/问题下的回答**与**特定用户的回答/文章**，
> 产出数据既可独立存储（JSON/CSV），也可无缝接入现有 BERTopic 管道。

---

## 一、技术选型与架构总览

```
zhihu_crawler/                      ← Scrapy 项目根目录（放在 crawl/ 下）
├── scrapy.cfg
├── zhihu/
│   ├── __init__.py
│   ├── settings.py                 ← 全局配置（并发、延迟、中间件）
│   ├── middlewares.py              ← 反爬中间件（UA轮换、代理、重试）
│   ├── items.py                    ← 数据模型（ZhihuAnswerItem / ZhihuArticleItem）
│   ├── pipelines.py               ← 数据清洗 + JSON/CSV 输出 + BERTopic 格式转换
│   ├── spiders/
│   │   ├── __init__.py
│   │   ├── question_spider.py     ← 按问题ID爬取所有回答
│   │   ├── topic_spider.py        ← 按话题ID爬取热门问题 → 回答
│   │   └── user_spider.py         ← 按用户url_token爬取回答+文章
│   └── utils/
│       ├── __init__.py
│       ├── zhihu_parser.py        ← API JSON 解析（对标 tweet_parser.py）
│       └── text_clean_zhihu.py    ← 知乎特有清洗（HTML标签、LaTeX、图片链接）
└── config/
    ├── questions.yaml             ← 待爬问题ID列表
    ├── topics.yaml                ← 待爬话题ID列表
    └── users.yaml                 ← 待爬用户url_token列表
```

### 核心决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 数据源 | 知乎 API v4 (`/api/v4/`) | 结构化JSON，无需解析HTML，分页稳定 |
| 框架 | Scrapy | 内置并发控制、重试、Pipeline、中间件体系 |
| 匿名访问 | 不登录，纯匿名 + 浏览器级 Headers | v4 API 部分端点匿名可读（问题回答、用户公开内容） |
| 输出格式 | 与 twitter_crawler 同构的 JSON | 无缝对接 `pipeline_io.load_docs()` |

---

## 二、知乎 API v4 端点梳理

### 2.1 问题回答（核心端点）

```
GET https://www.zhihu.com/api/v4/questions/{question_id}/answers
    ?include=data[*].content,voteup_count,comment_count,created_time,
             updated_time,author.name,author.url_token,excerpt,
             is_collapsed,collapse_reason
    &limit=20
    &offset=0
    &sort_by=default          # default(默认排序) | created(时间排序)
```

**分页机制**：响应中 `paging.next` 字段包含下一页完整URL，`paging.is_end=true` 时停止。

### 2.2 话题下的热门问题

```
GET https://www.zhihu.com/api/v4/topics/{topic_id}/feeds/top_activity
    ?limit=10&offset=0
```

> 注意：此端点返回混合内容（问题、文章、想法），需按 `target.type` 过滤。

### 2.3 用户回答

```
GET https://www.zhihu.com/api/v4/members/{url_token}/answers
    ?include=data[*].content,voteup_count,comment_count,created_time,
             updated_time,question.title,excerpt
    &limit=20&offset=0
    &sort_by=voteups           # voteups(高赞优先) | created(时间排序)
```

### 2.4 用户文章

```
GET https://www.zhihu.com/api/v4/members/{url_token}/articles
    ?include=data[*].content,voteup_count,comment_count,created,updated
    &limit=20&offset=0
    &sort_by=voteups
```

---

## 三、数据模型设计（items.py）

与现有 Twitter 数据结构对齐，确保 BERTopic 管道零改动接入：

```python
import scrapy

class ZhihuAnswerItem(scrapy.Item):
    """一条知乎回答，字段对齐 twitter_crawler 输出格式"""
    # —— 核心标识 ——
    post_id       = scrapy.Field()   # answer_id (str)
    author        = scrapy.Field()   # 用户名
    author_token  = scrapy.Field()   # url_token（知乎特有）
    date          = scrapy.Field()   # created_time ISO 8601
    url           = scrapy.Field()   # https://www.zhihu.com/question/{qid}/answer/{aid}

    # —— 内容 ——
    content_text  = scrapy.Field()   # 纯文本（HTML已清洗）— pipeline 读取此字段
    content_html  = scrapy.Field()   # 原始HTML（保留备用）
    excerpt       = scrapy.Field()   # 知乎自带摘要

    # —— 元数据 ——
    question_id   = scrapy.Field()
    question_title= scrapy.Field()
    voteup_count  = scrapy.Field()
    comment_count = scrapy.Field()
    updated_time  = scrapy.Field()

    # —— 预处理保留字段（由 Pipeline 填充）——
    clean_text      = scrapy.Field()
    urls            = scrapy.Field()
    lang            = scrapy.Field()
    translated_text = scrapy.Field()


class ZhihuArticleItem(scrapy.Item):
    """一条知乎专栏文章"""
    post_id       = scrapy.Field()
    author        = scrapy.Field()
    author_token  = scrapy.Field()
    date          = scrapy.Field()
    url           = scrapy.Field()
    content_text  = scrapy.Field()
    content_html  = scrapy.Field()
    title         = scrapy.Field()
    voteup_count  = scrapy.Field()
    comment_count = scrapy.Field()
    updated_time  = scrapy.Field()
    clean_text      = scrapy.Field()
    urls            = scrapy.Field()
    lang            = scrapy.Field()
    translated_text = scrapy.Field()
```

---

## 四、Spider 实现设计

### 4.1 QuestionSpider —— 问题回答爬虫

```python
# zhihu/spiders/question_spider.py
import scrapy
import yaml
from zhihu.items import ZhihuAnswerItem
from zhihu.utils.zhihu_parser import parse_answer

class QuestionSpider(scrapy.Spider):
    name = "zhihu_question"
    custom_settings = {
        "DOWNLOAD_DELAY": 3,        # 每请求间隔 3 秒（匿名保守策略）
        "CONCURRENT_REQUESTS": 1,   # 串行请求避免封IP
    }

    def __init__(self, questions_file=None, question_ids=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if question_ids:
            self.question_ids = question_ids.split(",")
        elif questions_file:
            with open(questions_file) as f:
                self.question_ids = yaml.safe_load(f)
        else:
            self.question_ids = []

    def start_requests(self):
        for qid in self.question_ids:
            url = (
                f"https://www.zhihu.com/api/v4/questions/{qid}/answers"
                f"?include=data[*].content,voteup_count,comment_count,"
                f"created_time,updated_time,author.name,author.url_token,excerpt"
                f"&limit=20&offset=0&sort_by=default"
            )
            yield scrapy.Request(
                url,
                callback=self.parse_answers,
                meta={"question_id": qid},
                headers=self._default_headers(),
            )

    def parse_answers(self, response):
        data = response.json()
        for answer_data in data.get("data", []):
            yield parse_answer(answer_data, response.meta["question_id"])

        # 自动分页
        paging = data.get("paging", {})
        if not paging.get("is_end", True):
            next_url = paging.get("next")
            if next_url:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_answers,
                    meta=response.meta,
                    headers=self._default_headers(),
                )

    @staticmethod
    def _default_headers():
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...",
            "Referer": "https://www.zhihu.com/",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
```

### 4.2 TopicSpider —— 话题 → 问题 → 回答（二级爬取）

```python
class TopicSpider(scrapy.Spider):
    name = "zhihu_topic"

    def start_requests(self):
        for topic_id in self.topic_ids:
            url = f"https://www.zhihu.com/api/v4/topics/{topic_id}/feeds/top_activity?limit=10&offset=0"
            yield scrapy.Request(url, callback=self.parse_topic_feed, ...)

    def parse_topic_feed(self, response):
        data = response.json()
        for item in data.get("data", []):
            target = item.get("target", {})
            if target.get("type") == "answer":
                # 直接产出回答
                yield parse_answer(target, target["question"]["id"])
            elif target.get("type") == "question":
                # 发现新问题 → 跳转到问题回答接口
                qid = target["id"]
                yield scrapy.Request(
                    f"https://www.zhihu.com/api/v4/questions/{qid}/answers?...",
                    callback=self.parse_answers,
                    meta={"question_id": qid},
                )
        # 分页...
```

### 4.3 UserSpider —— 用户内容爬虫

```python
class UserSpider(scrapy.Spider):
    name = "zhihu_user"

    def start_requests(self):
        for user_token in self.user_tokens:
            # 并行爬取回答和文章
            yield scrapy.Request(
                f"https://www.zhihu.com/api/v4/members/{user_token}/answers?...",
                callback=self.parse_user_answers,
                meta={"user_token": user_token},
            )
            yield scrapy.Request(
                f"https://www.zhihu.com/api/v4/members/{user_token}/articles?...",
                callback=self.parse_user_articles,
                meta={"user_token": user_token},
            )
```

---

## 五、反爬策略（Middlewares）

匿名爬取最关键的三道防线：

### 5.1 User-Agent 轮换

```python
# middlewares.py
import random

class RandomUserAgentMiddleware:
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ...",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ...",
        # 15-20 个真实浏览器 UA
    ]

    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(self.USER_AGENTS)
```

### 5.2 请求频率控制（settings.py 核心配置）

```python
# settings.py
DOWNLOAD_DELAY = 3                  # 基础延迟 3 秒
RANDOMIZE_DOWNLOAD_DELAY = True     # 实际延迟 = 0.5×~1.5× DOWNLOAD_DELAY
CONCURRENT_REQUESTS = 1             # 匿名模式下串行
CONCURRENT_REQUESTS_PER_DOMAIN = 1
AUTOTHROTTLE_ENABLED = True         # 自适应节流
AUTOTHROTTLE_START_DELAY = 3
AUTOTHROTTLE_MAX_DELAY = 30         # 被限速时自动退避到 30 秒
AUTOTHROTTLE_TARGET_CONCURRENCY = 0.5
RETRY_TIMES = 5                     # 最多重试 5 次
RETRY_HTTP_CODES = [403, 429, 500, 502, 503]
```

### 5.3 指数退避重试

```python
class ExponentialBackoffMiddleware:
    """遇到 403/429 时指数退避"""

    def process_response(self, request, response, spider):
        if response.status in (403, 429):
            retry_count = request.meta.get("retry_count", 0)
            if retry_count < 5:
                delay = min(2 ** retry_count * 5, 120)  # 5s → 10s → 20s → 40s → 80s
                spider.logger.warning(
                    f"Got {response.status}, backing off {delay}s (retry {retry_count+1})"
                )
                request.meta["retry_count"] = retry_count + 1
                # 通过 Twisted reactor 延迟调度
                from twisted.internet import reactor, defer
                d = defer.Deferred()
                reactor.callLater(delay, d.callback, None)
                return d.addCallback(lambda _: request)
        return response
```

### 5.4 可选：代理池支持

```python
# settings.py（如果后续需要扩大规模，接入代理池）
# PROXY_POOL_URL = "http://localhost:5555/random"

class ProxyMiddleware:
    def process_request(self, request, spider):
        proxy_url = spider.settings.get("PROXY_POOL_URL")
        if proxy_url:
            import requests as req
            proxy = req.get(proxy_url).text.strip()
            request.meta["proxy"] = f"http://{proxy}"
```

---

## 六、数据处理 Pipeline

### 6.1 HTML → 纯文本清洗

```python
# utils/text_clean_zhihu.py
import re
from html import unescape

def html_to_text(html_content: str) -> str:
    """知乎回答 HTML → 纯文本"""
    if not html_content:
        return ""
    text = html_content
    # 1. 移除图片标签（保留 alt 文本）
    text = re.sub(r'<img[^>]*alt="([^"]*)"[^>]*>', r' \1 ', text)
    text = re.sub(r'<img[^>]*>', '', text)
    # 2. 链接 → 保留文本
    text = re.sub(r'<a[^>]*>(.*?)</a>', r'\1', text)
    # 3. 数学公式 LaTeX → [公式]
    text = re.sub(r'<span class="ztext-math"[^>]*>(.*?)</span>', ' [公式] ', text)
    # 4. 代码块 → [代码]
    text = re.sub(r'<code[^>]*>.*?</code>', ' [代码] ', text, flags=re.DOTALL)
    # 5. 段落/换行 → 空格
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    # 6. 移除所有剩余标签
    text = re.sub(r'<[^>]+>', '', text)
    # 7. HTML 实体解码
    text = unescape(text)
    # 8. 规范化空白
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()
```

### 6.2 Scrapy Pipeline（输出 + BERTopic 格式化）

```python
# pipelines.py
import json
from datetime import datetime, timezone
from pathlib import Path
from zhihu.utils.text_clean_zhihu import html_to_text

class ZhihuCleanPipeline:
    """HTML清洗 + 基础文本处理"""

    def process_item(self, item, spider):
        # HTML → 纯文本
        if item.get("content_html"):
            item["content_text"] = html_to_text(item["content_html"])
        # 复用现有 text_clean 工具
        from utils.text_clean import extract_urls, remove_emoji
        if item.get("content_text"):
            clean, urls = extract_urls(item["content_text"])
            clean = remove_emoji(clean)
            item["clean_text"] = clean.strip()
            item["urls"] = urls
        return item


class ZhihuDeduplicationPipeline:
    """基于 post_id 去重"""

    def __init__(self):
        self.seen_ids = set()

    def process_item(self, item, spider):
        post_id = item.get("post_id")
        if post_id in self.seen_ids:
            from scrapy.exceptions import DropItem
            raise DropItem(f"Duplicate: {post_id}")
        self.seen_ids.add(post_id)
        return item


class BERTopicExportPipeline:
    """输出为 BERTopic 管道兼容格式"""

    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        # 生成与 twitter_crawler 同构的 JSON
        output_dir = Path(spider.settings.get("OUTPUT_DIR", "data"))
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        snapshot_path = output_dir / "snapshots" / f"zhihu_{timestamp}.json"
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "site": "知乎 / Zhihu",
            "source": spider.name,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "total_count": len(self.items),
            "texts": self.items,
        }

        with open(snapshot_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        spider.logger.info(f"Saved {len(self.items)} items → {snapshot_path}")

        # 同时写入主数据文件（供 pipeline.py 直接读取）
        main_path = output_dir / "zhihu_texts.json"
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
```

---

## 七、BERTopic 管道对接

### 现有管道零改动使用

```bash
# 爬取完成后，直接调用现有 pipeline：
python pipeline.py \
  --input-json data/zhihu_texts.json \
  --n-clusters 10 \
  --save-visualizations
```

`pipeline_io.load_docs()` 会读取 `texts[].content_text` 字段，这正是我们 Item 中填充的字段。

### scheduled.py 扩展（可选）

在 `scheduled.py` 中新增 `zhihu` 子命令，复用已有的 `analyze` 逻辑：

```python
# scheduled.py 新增
def cmd_zhihu_crawl(args):
    """调用 Scrapy 爬虫"""
    import subprocess
    subprocess.run([
        "scrapy", "crawl", "zhihu_question",
        "-a", f"questions_file={args.questions_file}",
        "-s", f"OUTPUT_DIR={args.output_dir}",
    ], cwd="crawl/zhihu_crawler")
```

---

## 八、配置文件示例

### questions.yaml
```yaml
# 待爬取的知乎问题ID列表
# 从 URL 提取：zhihu.com/question/[这个数字]
- 19551997    # 如何评价 ChatGPT？
- 585901765   # 2025年大模型发展方向？
- 316837423   # 深度学习入门
```

### topics.yaml
```yaml
# 知乎话题ID
# 从 URL 提取：zhihu.com/topic/[这个数字]
- 19554298    # 人工智能
- 19556592    # 深度学习
- 20069025    # 大语言模型
```

### users.yaml
```yaml
# 用户 url_token
# 从 URL 提取：zhihu.com/people/[这个字符串]
- excited-vczh
- kaiyuan-zhongwen
```

---

## 九、运行流程

```bash
# 1. 创建 Scrapy 项目
cd crawl
scrapy startproject zhihu_crawler
cd zhihu_crawler

# 2. 按问题爬取
scrapy crawl zhihu_question -a question_ids="19551997,585901765"

# 3. 按话题爬取（先拿问题列表，再逐个爬回答）
scrapy crawl zhihu_topic -a topic_ids="19554298"

# 4. 按用户爬取
scrapy crawl zhihu_user -a user_tokens="excited-vczh"

# 5. 从 YAML 配置批量爬取
scrapy crawl zhihu_question -a questions_file=config/questions.yaml

# 6. 爬取完成后运行 BERTopic 分析
cd ../..
python pipeline.py --input-json data/zhihu_texts.json --n-clusters 10 --save-visualizations
```

---

## 十、匿名爬取的限制与应对

| 限制 | 表现 | 应对策略 |
|------|------|----------|
| **频率限制** | 403/429 响应 | AutoThrottle + 指数退避（5s→80s） |
| **内容截断** | `content` 字段为空 | 匿名状态部分回答无法获取完整HTML，退回使用 `excerpt` |
| **分页上限** | offset 超过一定值后返回空 | 记录断点，切换 `sort_by` 参数获取不同排序的数据 |
| **IP封禁** | 连续403 | 暂停爬取，等待数小时或切换代理 |
| **部分接口不可用** | 话题Feed可能需要登录 | 回退到直接使用问题ID列表 |

### 规模预估（匿名、单IP、无代理）

- 每请求间隔 ~3秒，每页 20 条
- 每小时约 **400 条回答**
- **1000-10000 条目标**：约 2.5 ~ 25 小时
- 建议分批运行，每批 500-1000 条后暂停 30 分钟

---

## 十一、后续可选增强

1. **Cookie 登录支持**：在 `settings.py` 中配置 `DEFAULT_REQUEST_HEADERS` 添加 Cookie，可解除 content 截断限制
2. **代理池集成**：接入 `proxy_pool` 或商业代理服务，突破 IP 限制
3. **增量爬取**：基于 `updated_time` 字段实现，只爬取上次快照之后更新的内容
4. **评论爬取**：扩展 `GET /answers/{id}/comments` 端点，获取评论维度数据
5. **多语言 Embedding**：知乎内容以中文为主，可考虑切换 `shibing624/text2vec-base-chinese` 模型替代 MiniLM
