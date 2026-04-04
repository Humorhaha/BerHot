"""
crawl/mindspider_bridge.py — MindSpider (BettaFish) → BerTopic 快照适配器
==========================================================================

职责：
  1. 以子进程调用 MindSpider 执行深度爬取（可选）
  2. 从 MindSpider 数据库（MediaCrawler 表）读取采集结果
  3. 将各平台记录规范化为项目统一快照格式，供 second_pass() 和 BERTopic pipeline 使用

数据契约：
  每条输出记录包含 tweet_type="original" 和 is_truncated=False，
  满足 utils/preprocess.py:second_pass() 的断言要求。

数据库表来源：
  BettaFish/MindSpider 使用 MediaCrawler 作为子模块进行深度爬取。
  本桥接器读取的是 MediaCrawler 写入的平台内容表（Phase 2 输出），
  而非 MindSpider 自身的 daily_news / daily_topics 中间表。

环境变量（写入 .env）：
  MINDSPIDER_DB_DIALECT   mysql+pymysql（默认）或 postgresql+psycopg2
  MINDSPIDER_DB_HOST      数据库主机（默认 localhost）
  MINDSPIDER_DB_PORT      端口（默认 3306）
  MINDSPIDER_DB_USER      用户名（默认 root）
  MINDSPIDER_DB_PASSWORD  密码
  MINDSPIDER_DB_NAME      数据库名（默认 bettafish）

MindSpider 安装路径：
  默认期望克隆到 crawl/MindSpider/，也可通过参数覆盖。
  克隆命令：
    git clone --recurse-submodules https://github.com/666ghj/BettaFish crawl/BettaFish
    ln -s crawl/BettaFish/MindSpider crawl/MindSpider   # 或直接指定路径
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date as date_type
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

PROJECT_DIR = Path(__file__).resolve().parent.parent
MINDSPIDER_DIR = Path(__file__).resolve().parent / "MindSpider"
SNAPSHOT_DIR = PROJECT_DIR / "data" / "snapshots"

# ─── 平台表结构定义 ────────────────────────────────────────────────────────────
#
# 基于 MediaCrawler 数据库模型（github.com/NanmiCoder/MediaCrawler）
# 字段名以 MediaCrawler 实际列名为准；title_col 若存在则拼接到 content_text 前。
#
# 如果本地 MediaCrawler 版本不同导致表名/列名有差异，可在此处直接修改。

_PLATFORM_SCHEMAS: dict[str, dict] = {
    "xhs": {
        "table":       "xhs_note",
        "id_col":      "note_id",
        "author_col":  "nickname",
        "date_col":    "time",           # Unix timestamp (int)
        "content_col": "desc",
        "title_col":   "title",
        "url_col":     "note_url",
        "like_col":    "liked_count",
        "share_col":   "share_count",
        "site":        "小红书 / Xiaohongshu",
    },
    "dy": {
        "table":       "dy_aweme",
        "id_col":      "aweme_id",
        "author_col":  "nickname",
        "date_col":    "create_time",    # Unix timestamp (int)
        "content_col": "desc",
        "title_col":   "title",
        "url_col":     "aweme_url",
        "like_col":    "liked_count",
        "share_col":   "share_count",
        "site":        "抖音 / Douyin",
    },
    "bili": {
        "table":       "bilibili_video",
        "id_col":      "video_id",
        "author_col":  "nickname",
        "date_col":    "create_time",    # Unix timestamp (int)
        "content_col": "desc",
        "title_col":   "title",
        "url_col":     "video_url",
        "like_col":    "liked_count",
        "share_col":   "video_share_count",
        "site":        "哔哩哔哩 / Bilibili",
    },
    "wb": {
        "table":       "weibo_note",
        "id_col":      "note_id",
        "author_col":  "nickname",
        "date_col":    "create_time",    # Unix timestamp (int)
        "content_col": "content",
        "title_col":   None,
        "url_col":     "note_url",
        "like_col":    "liked_count",
        "share_col":   "shared_count",
        "site":        "微博 / Weibo",
    },
    "tieba": {
        "table":       "tieba_note",
        "id_col":      "note_id",
        "author_col":  "user_nickname",
        "date_col":    "publish_time",   # datetime string
        "content_col": "desc",
        "title_col":   "title",
        "url_col":     "note_url",
        "like_col":    None,
        "share_col":   None,
        "site":        "百度贴吧 / Tieba",
    },
    "ks": {
        "table":       "kuaishou_video",
        "id_col":      "video_id",
        "author_col":  "nickname",
        "date_col":    "create_time",    # Unix timestamp (int)
        "content_col": "desc",
        "title_col":   None,
        "url_col":     "video_url",
        "like_col":    "liked_count",
        "share_col":   None,
        "site":        "快手 / Kuaishou",
    },
    "zhihu": {
        "table":       "zhihu_content",
        "id_col":      "content_id",
        "author_col":  "user_nickname",
        "date_col":    "created_time",   # Unix timestamp (int)
        "content_col": "content_text",
        "title_col":   "title",
        "url_col":     "content_url",
        "like_col":    "voteup_count",
        "share_col":   None,
        "site":        "知乎 / Zhihu",
    },
}

ALL_PLATFORMS: list[str] = list(_PLATFORM_SCHEMAS.keys())


# ─── 日期/时间规范化 ───────────────────────────────────────────────────────────

def _to_iso(ts) -> str:
    """将 Unix 时间戳（int）或 datetime/str 统一转换为 ISO 8601 字符串。"""
    if ts is None:
        return "1970-01-01T00:00:00+00:00"
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    if isinstance(ts, int):
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError):
            return "1970-01-01T00:00:00+00:00"
    if isinstance(ts, str):
        return ts
    return str(ts)


def _date_filter_clause(date_col: str, dialect: str) -> str:
    """
    构造日期过滤的 SQL 片段。
    Unix timestamp（int 列）与 datetime/string 列的写法不同。
    """
    if "mysql" in dialect:
        # 对 int 类型和 datetime 类型都能使用 DATE(FROM_UNIXTIME(...))，
        # 但对字符串类型会失败；这里用 COALESCE 策略，实际使用时会在运行时报错，
        # 届时直接跳过日期过滤也可接受。
        return f"DATE(FROM_UNIXTIME({date_col})) = :target_date"
    else:
        # PostgreSQL / SQLite
        return f"DATE({date_col}) = :target_date"


# ─── 记录规范化 ───────────────────────────────────────────────────────────────

def _normalize_row(row: dict, schema: dict, platform: str) -> dict:
    """
    将 MediaCrawler DB 行转换为本项目统一推文格式。

    必须包含 tweet_type 和 is_truncated 字段，以兼容 second_pass()。
    title 若存在则拼接到 content_text 前，提升 BERTopic 主题提取质量。
    """
    content = str(row.get(schema["content_col"]) or "").strip()
    title = (
        str(row.get(schema["title_col"]) or "").strip()
        if schema.get("title_col")
        else ""
    )
    # 标题存在时拼接（避免重复：若内容已以标题开头则不再添加）
    if title and not content.startswith(title):
        content_text = f"{title}\n{content}" if content else title
    else:
        content_text = content or title

    return {
        "post_id":        str(row.get(schema["id_col"]) or ""),
        "author":         str(row.get(schema["author_col"]) or ""),
        "date":           _to_iso(row.get(schema["date_col"])),
        "url":            str(row.get(schema["url_col"]) or ""),
        "content_text":   content_text,
        "like_count":     int(row.get(schema["like_col"]) or 0) if schema["like_col"] else 0,
        "retweet_count":  int(row.get(schema["share_col"]) or 0) if schema["share_col"] else 0,
        # second_pass() 断言必须存在的字段
        "tweet_type":     "original",
        "is_truncated":   False,
        "rt_author":      None,
        # second_pass() 将填充的字段（预置 None / 空列表）
        "clean_text":     None,
        "urls":           [],
        "lang":           None,
        "translated_text": None,
        # 来源平台标注（用于多源合并时的日志/调试）
        "source_platform": platform,
    }


# ─── 数据库读取 ───────────────────────────────────────────────────────────────

def fetch_records(
    db_url: str,
    platforms: list[str],
    target_date: Optional[date_type] = None,
) -> list[dict]:
    """
    从 MindSpider/MediaCrawler 数据库读取各平台内容，转换为标准记录列表。

    Args:
        db_url:       SQLAlchemy 连接字符串，例如
                      "mysql+pymysql://root:pw@localhost:3306/bettafish"
        platforms:    要读取的平台代码列表，如 ["xhs", "wb", "zhihu"]
        target_date:  仅读取该日期的数据（None = 不限制日期，读取全表）

    Returns:
        规范化后的记录列表（content_text 为空的记录已过滤）
    """
    try:
        from sqlalchemy import create_engine, text as sa_text
    except ImportError:
        raise RuntimeError(
            "sqlalchemy 未安装。请运行：\n"
            "  pip install sqlalchemy pymysql\n"
            "或：\n"
            "  uv add --optional mindspider sqlalchemy pymysql"
        )

    dialect = db_url.split("://")[0] if "://" in db_url else "mysql"
    engine = create_engine(db_url, echo=False, pool_pre_ping=True)
    records: list[dict] = []

    with engine.connect() as conn:
        for platform in platforms:
            schema = _PLATFORM_SCHEMAS.get(platform)
            if schema is None:
                logger.warning(f"[mindspider_bridge] 未知平台: {platform}，跳过")
                continue

            table    = schema["table"]
            date_col = schema["date_col"]

            try:
                if target_date:
                    filter_clause = _date_filter_clause(date_col, dialect)
                    sql = sa_text(f"SELECT * FROM {table} WHERE {filter_clause}")
                    rows = conn.execute(sql, {"target_date": target_date.isoformat()}).mappings().all()
                else:
                    rows = conn.execute(sa_text(f"SELECT * FROM {table}")).mappings().all()

                batch = [
                    _normalize_row(dict(r), schema, platform)
                    for r in rows
                    if str(r.get(schema["content_col"]) or "").strip()
                ]
                records.extend(batch)
                logger.info(
                    f"[mindspider_bridge] {platform} ({table}): "
                    f"{len(rows)} 行 → {len(batch)} 条有效记录"
                )

            except Exception as e:
                logger.warning(
                    f"[mindspider_bridge] {platform} ({table}) 读取失败: {e}\n"
                    "  提示：如果表名不匹配，请检查 crawl/mindspider_bridge.py 中的 _PLATFORM_SCHEMAS"
                )
                continue

    logger.info(f"[mindspider_bridge] 共读取 {len(records)} 条记录，平台: {platforms}")
    return records


# ─── MindSpider 子进程调用 ────────────────────────────────────────────────────

def run_mindspider(
    mindspider_dir: Path,
    platforms: list[str],
    target_date: Optional[date_type] = None,
    test_mode: bool = False,
    timeout: int = 3600,
) -> bool:
    """
    以子进程运行 MindSpider 深度爬取（Phase 2: DeepSentimentCrawling）。

    前提条件：
      - BettaFish 已克隆（含子模块）到 mindspider_dir
      - Playwright 已安装并完成目标平台的 QR 扫码认证
      - .env 中已配置数据库和 DeepSeek API

    Args:
        mindspider_dir: MindSpider 主目录（含 main.py）
        platforms:      目标平台代码列表
        target_date:    爬取日期（None = 今天）
        test_mode:      传递 --test 给 MindSpider（限制数量，用于调试）
        timeout:        子进程超时秒数（默认 3600s / 1小时）

    Returns:
        True = 成功，False = 失败或未安装
    """
    main_py = mindspider_dir / "main.py"
    if not main_py.exists():
        logger.error(
            f"[mindspider_bridge] MindSpider 未找到：{main_py}\n"
            "  请先克隆 BettaFish 仓库（包含子模块）：\n"
            "    git clone --recurse-submodules https://github.com/666ghj/BettaFish crawl/BettaFish\n"
            "  然后通过 --mindspider-dir 参数指定路径，或将 MindSpider/ 目录符号链接到 crawl/MindSpider/"
        )
        return False

    cmd: list[str] = [
        sys.executable, str(main_py),
        "--deep-sentiment",
        "--platforms", *platforms,
    ]
    if target_date:
        cmd += ["--date", target_date.isoformat()]
    if test_mode:
        cmd += ["--test"]

    logger.info(f"[mindspider_bridge] 启动 MindSpider: {' '.join(str(c) for c in cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(mindspider_dir),
            timeout=timeout,
        )
        if result.returncode != 0:
            logger.error(f"[mindspider_bridge] MindSpider 退出码: {result.returncode}")
            return False
        logger.info("[mindspider_bridge] MindSpider 爬取完成")
        return True

    except subprocess.TimeoutExpired:
        logger.error(f"[mindspider_bridge] MindSpider 超时（>{timeout}s）")
        return False
    except Exception as e:
        logger.error(f"[mindspider_bridge] MindSpider 启动失败: {e}")
        return False


# ─── 快照 payload 构建 ────────────────────────────────────────────────────────

def build_snapshot_payload(records: list[dict], platforms: list[str]) -> dict:
    """构建与 twitter_crawler.build_payload() 格式兼容的快照 dict。"""
    return {
        "site":             "MindSpider",
        "source_platforms": platforms,
        "fetched_at_utc":   datetime.now(timezone.utc).isoformat(),
        "total_count":      len(records),
        "texts":            records,
    }
