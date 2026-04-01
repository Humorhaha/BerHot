# 知乎 Scrapy 爬虫

基于 Scrapy 框架的工程化知乎爬虫，支持爬取问题回答、话题内容和用户内容。

## 特性

- **免登录支持**：通过 HTML 页面爬取，无需登录/Cookie
- **三种爬虫类型**：问题回答、话题、用户（回答+文章）
- **搜索功能**：通过关键词搜索获取内容
- **自动登录**：支持 Playwright 自动获取 Cookie（可选）
- **反爬策略**：User-Agent 轮换、指数退避、自动节流
- **数据对接**：输出格式与 `twitter_crawler` 同构，可直接接入 BERTopic 管道

## 快速开始

### 1. 安装依赖

```bash
# 在项目根目录
uv sync

# 可选：安装 Playwright（用于自动登录）
uv add playwright
playwright install chromium
```

### 2. 选择爬取方式

#### 方式 A: 免登录（推荐简单场景）

```bash
cd crawl/zhihu_crawler

# 免登录爬取问题回答（限制：每个问题只能获取前 20-50 个回答）
python run.py question --ids "19551997" --no-login

# 免登录搜索
python run.py search --query "人工智能" --max 30
```

#### 方式 B: 使用 Cookie（完整功能）

```bash
# 自动登录获取 Cookie
python run.py question --ids "19551997" --auto-login

# 或使用已有 Cookie
python run.py question --ids "19551997"
```

### 3. 运行爬虫

```bash
cd crawl/zhihu_crawler

# ========== 免登录模式 ==========

# 免登录爬取问题（只能获取前 20-50 个回答）
python run.py question --ids "19551997" --no-login

# 免登录搜索关键词
python run.py search --query "ChatGPT" --max 20

# ========== 使用 Cookie（完整功能） ==========

# 使用启动脚本
python run.py question --ids "19551997,585901765"
python run.py topic --ids "19554298"
python run.py user --tokens "excited-vczh"

# 自动登录并爬取
python run.py question --ids "19551997" --auto-login

# 直接使用 scrapy 命令
scrapy crawl zhihu_question -a question_ids="19551997"
scrapy crawl zhihu_question_html -a question_ids="19551997"  # 免登录版
scrapy crawl zhihu_search -a query="人工智能" -a max_results=30  # 搜索

# 从配置文件批量爬取
scrapy crawl zhihu_question -a questions_file=config/questions.yaml
```

### 4. BERTopic 分析

```bash
cd ../..
python pipeline.py \
  --input-json crawl/zhihu_crawler/data/zhihu_texts.json \
  --n-clusters 10 \
  --save-visualizations
```

## 爬虫模式对比

| 模式 | 登录要求 | 数据量 | 速度 | 稳定性 | 使用场景 |
|------|---------|--------|------|--------|----------|
| **API 模式** | 需要 Cookie | 完整（无限制） | 快 | 中 | 大规模爬取 |
| **HTML 免登录** | 不需要 | 有限（前 20-50 条） | 慢 | 中 | 简单场景 |
| **搜索免登录** | 不需要 | 有限 | 中 | 低 | 关键词探索 |

## 免登录方案详解

### 1. HTML 免登录爬虫

通过爬取知乎公开页面获取回答，**完全不需要登录**。

```bash
# 使用启动脚本
python run.py question --ids "19551997" --no-login

# 或使用 scrapy 直接运行
scrapy crawl zhihu_question_html -a question_ids="19551997"

# 限制数量
scrapy crawl zhihu_question_html -a question_ids="19551997" -a max_answers=10
```

**限制：**
- 每个问题只能获取前 **20-50** 个回答（知乎页面懒加载限制）
- 无法获取完整的回答列表
- 速度较慢（HTML 解析比 API 慢）
- 仍然可能遇到 IP 限制

### 2. 搜索免登录

通过知乎搜索功能获取内容。

```bash
# 使用启动脚本
python run.py search --query "人工智能" --max 30

# 或使用 scrapy
scrapy crawl zhihu_search -a query="ChatGPT" -a max_results=20
```

**特点：**
- 完全免登录
- 可以搜索任意关键词
- 结果包含回答和文章
- 适合探索性数据收集

## 配置文件

### questions.yaml - 问题 ID 列表

```yaml
# 纯列表格式
- 19551997     # 如何评价 ChatGPT？
- 585901765    # 2025 年大模型发展方向？
```

### topics.yaml - 话题 ID 列表

```yaml
- 19554298     # 人工智能
- 19556592     # 深度学习
```

### users.yaml - 用户 url_token 列表

```yaml
- excited-vczh         # 轮子哥
- kaiyuan-zhongwen     # 开源中文
```

## 获取 ID

| 类型 | URL 示例 | 提取方法 |
|------|----------|----------|
| 问题 | `zhihu.com/question/19551997` | 数字 `19551997` |
| 话题 | `zhihu.com/topic/19554298` | 数字 `19554298` |
| 用户 | `zhihu.com/people/excited-vczh` | 字符串 `excited-vczh` |

## 爬虫参数

### QuestionSpider (API 模式)

```bash
-a question_ids="id1,id2"    # 逗号分隔的问题 ID
-a questions_file=path.yaml   # YAML 配置文件
-a max_answers=100            # 每个问题最多爬取的回答数
```

### QuestionHtmlSpider (免登录模式)

```bash
-a question_ids="id1,id2"    # 逗号分隔的问题 ID
-a questions_file=path.yaml   # YAML 配置文件
-a max_answers=20             # 最大回答数（建议不超过 50）
```

### SearchSpider (免登录搜索)

```bash
-a query="关键词"             # 搜索关键词
-a max_results=20             # 最大结果数
```

### TopicSpider

```bash
-a topic_ids="id1,id2"        # 逗号分隔的话题 ID
-a topics_file=path.yaml      # YAML 配置文件
-a max_items=100              # 每个话题最多爬取的内容数
```

### UserSpider

```bash
-a user_tokens="token1,token2"  # 逗号分隔的用户 url_token
-a users_file=path.yaml         # YAML 配置文件
-a crawl_answers=1              # 是否爬回答（默认 1）
-a crawl_articles=1             # 是否爬文章（默认 1）
-a max_per_user=50              # 每个用户最多爬取的内容数
```

## Cookie 管理（API 模式需要）

### 自动登录

```bash
# 单独获取 Cookie（保存到 cookies.json）
python get_cookies.py

# 获取并导出为 txt
python get_cookies.py --export-txt
```

### 手动设置

```bash
# 命令行方式
scrapy crawl zhihu_question \
  -a question_ids="19551997" \
  -s COOKIES_ENABLED=True \
  -s ZHIHU_COOKIES="_zap=xxx; d_c0=xxx; ..."
```

## 反爬策略

爬虫内置以下反爬措施：

1. **User-Agent 轮换**：15+ 个真实浏览器 UA
2. **下载延迟**：基础 3-5 秒，随机 0.5×~1.5×
3. **自动节流**：根据响应自动调整延迟
4. **指数退避**：遇到 403/429 时自动退避
5. **请求头模拟**：添加知乎所需的 Headers

## 访问限制

| 模式 | 限制 | 应对策略 |
|------|------|----------|
| **免登录 HTML** | 只能获取前 20-50 条 | 多个问题汇总 |
| **免登录搜索** | 结果可能不完整 | 更换关键词 |
| **API + Cookie** | IP 频率限制 | 增加延迟、使用代理 |

### 规模预估

| 模式 | 每小时数据量 | 建议场景 |
|------|-------------|----------|
| 免登录 HTML | ~200 条 | 小规模测试 |
| 免登录搜索 | ~300 条 | 关键词探索 |
| API + Cookie | ~600-800 条 | 大规模爬取 |

## 项目结构

```
zhihu_crawler/
├── scrapy.cfg
├── run.py                    # 启动脚本
├── get_cookies.py            # Cookie 获取工具
├── README.md
├── config/
│   ├── questions.yaml
│   ├── topics.yaml
│   └── users.yaml
└── zhihu/
    ├── __init__.py
    ├── items.py              # 数据模型
    ├── middlewares.py        # 反爬中间件
    ├── pipelines.py          # 数据处理
    ├── settings.py           # 配置
    ├── spiders/
    │   ├── __init__.py
    │   ├── question_spider.py      # API 模式
    │   ├── question_html_spider.py # 免登录 HTML 模式
    │   ├── search_spider.py        # 免登录搜索
    │   ├── topic_spider.py
    │   └── user_spider.py
    └── utils/
        ├── __init__.py
        ├── auth.py           # 自动登录工具
        ├── zhihu_parser.py   # API JSON 解析
        └── text_clean_zhihu.py  # HTML 清洗
```

## 注意事项

1. **遵守法律法规**：爬取数据仅供学习和研究使用
2. **尊重目标网站**：控制爬取频率，避免对知乎服务器造成压力
3. **数据隐私**：注意保护用户隐私，不要公开敏感信息
4. **免登录限制**：免登录模式数据量有限，适合简单场景
