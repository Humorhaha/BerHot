# X 数据采集 × BERTopic Pipeline 集成方案（方案 C）

> 使用 [twtapi.com](https://www.twtapi.com/zh/docs/) 采集 X 推文，打通到现有 BERTopic 舆论分析 pipeline。

---

## 前置准备

### 1. 获取 twtapi.com API Key

注册 [twtapi.com](https://www.twtapi.com) 并获取 API Key，写入 `.env`：

```env
TWTAPI_KEY=your_api_key_here
```

### 2. 目录结构（集成后）

```
BerTopic/
├── twitter_crawler.py      ← 新增：twtapi.com 采集模块
├── run_C_scheduled.py      ← 新增：方案 C 入口
├── pipeline.py             ← 原有：BERTopic pipeline
├── pipeline_io.py          ← 原有：数据加载/保存
├── pipeline_builders.py    ← 原有：模型构建
├── pipeline_config.py      ← 原有：配置解析
├── data/
│   ├── twitter_texts.json          ← 当前分析用的合并数据（中间文件）
│   ├── .twitter_since_ids.json     ← 增量采集状态
│   └── snapshots/                  ← 时间戳快照存档
│       ├── twitter_20260330_060000.json
│       ├── twitter_20260330_063000.json
│       └── ...
└── .env                    ← 添加 TWTAPI_KEY=xxx
```

---

## 方案 C：定时增量模式

**数据流**

```
[cron: 每 30 分钟]                  [cron: 每 6 小时]
       ↓                                   ↓
  run_C_scheduled.py crawl      run_C_scheduled.py analyze
       ↓                                   ↓
  data/snapshots/                 合并最近 N 个快照
  twitter_YYYYMMDD_HHMMSS.json    → pipeline → topics.csv / analysis.json
```

**核心设计**：爬取和分析完全解耦，按独立频率运行。每次爬取写带时间戳的快照文件，分析时用滑动窗口合并最近 N 个快照。

---

## 子命令

### `crawl` — 只爬取，写时间戳快照

```bash
python run_C_scheduled.py crawl \
  --usernames elonmusk sama \
  --api-key $TWTAPI_KEY \
  --max-tweets 200
```

### `analyze` — 合并快照，运行 pipeline

```bash
python run_C_scheduled.py analyze \
  --window 5 \
  --pipeline-args --n-clusters 20
```

`--window 5` 表示合并最近 5 个快照文件。**注意**：`window` 是按快照文件数量计，不是按时间计。若某次爬取未写入快照（无新数据），实际时间跨度会超过预期，请结合 cron 频率估算合适的 `window` 值。

### `all` — 一次性完成爬取 + 分析

```bash
python run_C_scheduled.py all \
  --usernames elonmusk sama \
  --api-key $TWTAPI_KEY \
  --window 5
```

---

## 配合 cron 部署（Linux / macOS）

> **注意**：cron 不继承 shell 环境变量。务必使用绝对路径，并依赖脚本内的 `load_dotenv()` 读取 `.env`，**不要**在 cron 命令里直接写 `$TWTAPI_KEY`。

```cron
# 每 30 分钟爬取一次（增量）
*/30 * * * * cd /path/to/BerTopic && /path/to/.venv/bin/python run_C_scheduled.py crawl \
  --usernames elonmusk sama >> logs/crawl.log 2>&1

# 每 6 小时分析一次（合并最近 12 个快照 ≈ 最近 6 小时数据）
0 */6 * * * cd /path/to/BerTopic && /path/to/.venv/bin/python run_C_scheduled.py analyze \
  --window 12 >> logs/analyze.log 2>&1
```

---

## 增量采集原理

`since_id` 机制：twtapi.com 支持 `since_id` 参数，只返回 ID 大于该值的推文。

```
上次最新 tweet_id: 1774000000000000001
         ↓
本次请求: GET /twitter/user/tweets?username=elonmusk&since_id=1774000000000000001
         ↓
只返回比该 ID 更新的推文
```

状态持久化到 `data/.twitter_since_ids.json`：

```json
{
  "elonmusk": "1774000000000000099",
  "sama":     "1774000000000000042"
}
```

---

## 数据格式兼容性

`twitter_crawler.py` 输出的 JSON 与现有 `pipeline_io.load_docs()` **完全兼容**，无需修改任何 pipeline 代码：

```json
{
  "site": "X / Twitter",
  "source_accounts": ["elonmusk", "sama"],
  "fetched_at_utc": "2026-03-30T14:30:00+00:00",
  "total_count": 350,
  "texts": [
    {
      "post_id": "1774000000000000001",
      "author": "elonmusk",
      "date": "2026-03-30T10:00:00+00:00",
      "url": "https://x.com/elonmusk/status/1774000000000000001",
      "content_text": "推文正文（pipeline 实际使用的字段）",
      "retweet_count": 1200,
      "like_count": 45000
    }
  ]
}
```

`load_docs()` 读取 `texts[i]["content_text"]`，与现有 MIT Technology Review 数据结构一致。

---

## 中文账号注意事项

`pipeline_config.py` 中 `stop_words` 默认为 `"english"`，对中文文本无效。监控中文账号时，建议透传参数：

```bash
python run_C_scheduled.py analyze \
  --window 12 \
  --pipeline-args --stop-words none
```

---

## 快速开始（验证流程）

```bash
# 1. 配置 API Key
echo "TWTAPI_KEY=your_key_here" >> .env

# 2. 首次全量爬取
python run_C_scheduled.py crawl \
  --usernames elonmusk \
  --max-tweets 50

# 3. 运行分析
python run_C_scheduled.py analyze \
  --window 1 \
  --pipeline-args --n-clusters 5 --disable-llm

# 4. 查看输出
ls -la topics.csv document_info.csv analysis.json
```

---

## twtapi.com 接口速查

| 接口 | 路径 | 关键参数 |
|------|------|----------|
| 用户推文 | `GET /twitter/user/tweets` | `username`, `count`, `cursor`, `since_id` |
| 用户信息 | `GET /twitter/user/info` | `username` |
| 搜索推文 | `GET /twitter/tweet/search` | `query`, `count`, `cursor` |

认证方式：`Authorization: Bearer YOUR_API_KEY`

完整文档：https://www.twtapi.com/zh/docs/
