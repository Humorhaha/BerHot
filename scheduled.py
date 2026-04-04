"""
定时增量采集 + BERTopic 分析 Pipeline
========================================

数据流:
  [定时任务 / cron / 手动触发]
         ↓
  twitter_crawler    → data/snapshots/twitter_YYYYMMDD_HHMMSS.json
  mindspider_bridge  → data/snapshots/mindspider_YYYYMMDD_HHMMSS.json
         ↓  (second_pass: 去截断RT、清洗、语言检测、可选翻译)
  scheduled.py analyze → 合并最近 N 个快照（所有信源）→ 运行 pipeline → 输出结果

快速开始:

  # 1. 从 users.yaml 随机抽取 40 个信源测试（Twitter）
  uv run python scheduled.py crawl --sample 40 --api-key YOUR_KEY

  # 2. 加载全部信源（Twitter）
  uv run python scheduled.py crawl --from-yaml --api-key YOUR_KEY

  # 3. 只分析（合并最近 5 个快照，含所有信源）
  uv run python scheduled.py analyze --window 5

  # 4. 一次性完成（测试模式）
  uv run python scheduled.py all --sample 40 --api-key YOUR_KEY

  # 5. 手动指定用户名
  uv run python scheduled.py crawl --usernames elonmusk sama --api-key YOUR_KEY

  # 6. 从 MindSpider（BettaFish）爬取中文社交平台
  uv run python scheduled.py mindspider --platforms xhs wb zhihu --run-spider

  # 7. 仅从 MindSpider 数据库读取（MindSpider 已运行过）
  uv run python scheduled.py mindspider --platforms xhs wb --date 2026-04-04
"""

from __future__ import annotations

import json
import os
import random
import sys
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from twitter_crawler import (
    build_payload,
    fetch_multiple_users,
    save_json,
)
from pipeline import run_pipeline
from pipeline_config import parse_config
from utils.user_utils import _extract_username, load_usernames_from_yaml
from utils.preprocess import second_pass

load_dotenv(dotenv_path=PROJECT_DIR / ".env")

SNAPSHOT_DIR = PROJECT_DIR / "data" / "snapshots"
USERS_YAML   = PROJECT_DIR / "users.yaml"


# ─── MindSpider 数据库连接字符串 ──────────────────────────────────────────────

def _mindspider_db_url() -> str:
    """
    从环境变量构建 MindSpider 数据库的 SQLAlchemy 连接字符串。

    所需 .env 条目（示例）：
      MINDSPIDER_DB_DIALECT=mysql+pymysql
      MINDSPIDER_DB_HOST=localhost
      MINDSPIDER_DB_PORT=3306
      MINDSPIDER_DB_USER=root
      MINDSPIDER_DB_PASSWORD=your_password
      MINDSPIDER_DB_NAME=bettafish
    """
    dialect  = os.getenv("MINDSPIDER_DB_DIALECT",  "mysql+pymysql")
    host     = os.getenv("MINDSPIDER_DB_HOST",     "localhost")
    port     = os.getenv("MINDSPIDER_DB_PORT",     "3306")
    user     = os.getenv("MINDSPIDER_DB_USER",     "root")
    password = os.getenv("MINDSPIDER_DB_PASSWORD", "")
    name     = os.getenv("MINDSPIDER_DB_NAME",     "bettafish")
    return f"{dialect}://{user}:{password}@{host}:{port}/{name}"


# ─── Step 1: 爬取快照 ────────────────────────────────────────────────────────

def cmd_crawl(
    usernames: list[str],
    api_key: str,
    max_tweets: int = 200,
    translate: bool = False,
) -> Path:
    """
    采集推文，经 second_pass 预处理后写入带时间戳的快照文件。
    返回写入的快照路径。
    """
    raw_tweets = fetch_multiple_users(usernames, api_key, max_tweets)
    if not raw_tweets:
        logger.warning("no tweets fetched, skip snapshot")
        return Path()

    # second_pass：过滤截断RT、清洗、语言检测、可选翻译
    tweets = second_pass(raw_tweets, translate=translate)

    if not tweets:
        logger.warning("all tweets filtered by second_pass, skip snapshot")
        return Path()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = SNAPSHOT_DIR / f"twitter_{ts}.json"
    payload = build_payload(tweets, usernames)
    save_json(payload, snapshot_path)

    return snapshot_path


# ─── Step 1b: MindSpider 爬取快照 ────────────────────────────────────────────

def cmd_crawl_mindspider(
    platforms: list[str] | None = None,
    target_date: date_type | None = None,
    run_spider: bool = False,
    test_mode: bool = False,
    translate: bool = False,
    mindspider_dir: Path | None = None,
) -> Path:
    """
    从 MindSpider（BettaFish）获取多平台内容，经 second_pass 预处理后写入快照。

    Args:
        platforms:      目标平台代码列表（None = 全部平台）
                        支持：xhs, dy, bili, wb, tieba, ks, zhihu
        target_date:    读取该日期的 DB 数据（None = 不限制日期）
        run_spider:     True 则先运行 MindSpider 爬虫，再读 DB
        test_mode:      传递 --test 给 MindSpider（限制数量，用于调试）
        translate:      对中文内容调用翻译 API（需配置 OPENAI_API_KEY）
        mindspider_dir: MindSpider 安装目录（None = crawl/MindSpider/）

    Returns:
        写入的快照文件路径；失败时返回 Path()。
    """
    from crawl.mindspider_bridge import (
        ALL_PLATFORMS,
        MINDSPIDER_DIR,
        build_snapshot_payload,
        fetch_records,
        run_mindspider,
    )

    platforms = platforms or ALL_PLATFORMS
    mindspider_dir = mindspider_dir or MINDSPIDER_DIR
    db_url = _mindspider_db_url()

    if run_spider:
        ok = run_mindspider(mindspider_dir, platforms, target_date, test_mode)
        if not ok:
            logger.warning("[mindspider] 爬虫运行失败，尝试读取现有 DB 数据")

    records = fetch_records(db_url, platforms, target_date)
    if not records:
        logger.warning("[mindspider] DB 中无有效数据，跳过快照")
        return Path()

    # second_pass：清洗、语言检测、可选翻译（与 Twitter 流程一致）
    records = second_pass(records, translate=translate)
    if not records:
        logger.warning("[mindspider] 所有记录被 second_pass 过滤，跳过快照")
        return Path()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = SNAPSHOT_DIR / f"mindspider_{ts}.json"
    payload = build_snapshot_payload(records, platforms)
    save_json(payload, snapshot_path)

    return snapshot_path


# ─── Step 2: 合并快照并分析 ──────────────────────────────────────────────────

def _load_snapshots(
    window: int,
    max_age_days: int | None = None,
) -> tuple[list[str], list[dict]]:
    """
    读取最近 `window` 个快照（所有信源：twitter_*.json、mindspider_*.json 等），
    合并去重，返回 (docs, tweets)。
    docs 优先使用 translated_text，其次 clean_text，再次 content_text。
    """
    snapshots = sorted(SNAPSHOT_DIR.glob("*.json"), reverse=True)[:window]
    if not snapshots:
        raise FileNotFoundError(f"no snapshots in {SNAPSHOT_DIR}")

    logger.info(f"merging {len(snapshots)} snapshots")
    all_tweets: list[dict] = []
    seen_ids: set[str] = set()

    cutoff: datetime | None = None
    if max_age_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        logger.info(f"age filter: >{max_age_days}d tweets dropped")

    for snap in sorted(snapshots):
        logger.info(f"  {snap.name}")
        with snap.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        site = payload.get("site", snap.stem.split("_")[0])
        for item in payload.get("texts", []):
            # 多信源去重：用 "信源:post_id" 作为唯一键，避免不同平台 ID 碰撞
            source_key = item.get("source_platform") or site
            pid = f"{source_key}:{item.get('post_id', '')}"
            text = item.get("clean_text") or item.get("content_text", "").strip()
            if not text or pid in seen_ids:
                continue
            if cutoff:
                try:
                    if datetime.fromisoformat(item.get("date", "")) < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass
            all_tweets.append(item)
            seen_ids.add(pid)

    all_tweets.sort(
        key=lambda x: datetime.fromisoformat(x.get("date", "1970-01-01")),
        reverse=True,
    )

    # 下游模型优先用 translated_text（非英文已翻译），其次 clean_text，再次 content_text
    all_docs = [
        t.get("translated_text") or t.get("clean_text") or t.get("content_text", "")
        for t in all_tweets
    ]

    if all_tweets:
        newest = all_tweets[0]["date"][:10]
        oldest = all_tweets[-1]["date"][:10]
        logger.info(f"merged {len(all_docs)} unique docs ({oldest} ~ {newest})")
    else:
        logger.info("merged 0 docs")

    return all_docs, all_tweets


def cmd_analyze(
    window: int = 5,
    max_age_days: int | None = None,
    pipeline_argv: list[str] | None = None,
) -> None:
    """合并最近 window 个快照，运行 BERTopic 全量分析。"""
    docs, tweets = _load_snapshots(window, max_age_days)
    if not docs:
        logger.warning("no docs available, abort analysis")
        return

    merged_json = PROJECT_DIR / "data" / "twitter_texts.json"

    # 关键修改：将处理后的文本写入 content_text 字段
    # 优先级：translated_text > clean_text > content_text
    for t in tweets:
        t["content_text"] = t.get("translated_text") or t.get("clean_text") or t.get("content_text", "")

    payload = build_payload(tweets, [])
    save_json(payload, merged_json)

    argv = ["--input-json", str(merged_json)] + (pipeline_argv or [])
    config = parse_config(argv=argv)

    logger.info("BERTopic pipeline start")
    run_pipeline(config, quiet=True)

    from pipeline_io import load_docs as _load_docs_for_stats
    analyzed_docs = _load_docs_for_stats(config.input_json)
    logger.info(f"pipeline done | docs={len(analyzed_docs)} → {config.output_dir}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="定时增量爬取 + 滑动窗口分析")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    # --- crawl ---
    p_crawl = subparsers.add_parser("crawl", help="仅爬取，写时间戳快照")
    p_crawl.add_argument("--usernames", nargs="+")
    p_crawl.add_argument("--from-yaml", action="store_true")
    p_crawl.add_argument("--sample", type=int, metavar="N")
    p_crawl.add_argument("--max-tweets", type=int, default=10)
    p_crawl.add_argument("--translate", action="store_true", default=False,
                         help="对非英文 original 推文调用翻译 API")
    p_crawl.add_argument("--api-key", default=os.getenv("TWTAPI_KEY"))

    # --- analyze ---
    p_analyze = subparsers.add_parser("analyze", help="合并快照并运行 pipeline")
    p_analyze.add_argument("--window", type=int, default=5)
    p_analyze.add_argument("--max-age-days", type=int, metavar="D")
    p_analyze.add_argument("--pipeline-args", nargs=argparse.REMAINDER, default=[])

    # --- mindspider ---
    p_ms = subparsers.add_parser(
        "mindspider",
        help="从 MindSpider (BettaFish) 爬取中文社交平台，写时间戳快照",
    )
    from crawl.mindspider_bridge import ALL_PLATFORMS as _ALL_PLATFORMS
    p_ms.add_argument(
        "--platforms", nargs="+", default=_ALL_PLATFORMS,
        metavar="PLATFORM",
        help=f"目标平台，可选：{', '.join(_ALL_PLATFORMS)}（默认全部）",
    )
    p_ms.add_argument(
        "--date", metavar="YYYY-MM-DD", default=None,
        help="读取指定日期的 DB 数据（默认不限制日期）",
    )
    p_ms.add_argument(
        "--run-spider", action="store_true", default=False,
        help="先运行 MindSpider 爬虫，再读取 DB（需已完成平台认证）",
    )
    p_ms.add_argument(
        "--test", action="store_true", default=False,
        help="以测试模式运行 MindSpider（限制数量）",
    )
    p_ms.add_argument(
        "--translate", action="store_true", default=False,
        help="对中文内容调用翻译 API（需配置 OPENAI_API_KEY）",
    )
    p_ms.add_argument(
        "--mindspider-dir", type=Path, default=None,
        metavar="PATH",
        help="MindSpider 安装目录（默认 crawl/MindSpider/）",
    )

    # --- all ---
    p_all = subparsers.add_parser("all", help="爬取 + 分析（一次性完成）")
    p_all.add_argument("--usernames", nargs="+")
    p_all.add_argument("--from-yaml", action="store_true")
    p_all.add_argument("--sample", type=int, metavar="N")
    p_all.add_argument("--max-tweets", type=int, default=10)
    p_all.add_argument("--max-age-days", type=int, metavar="D")
    p_all.add_argument("--translate", action="store_true", default=False)
    p_all.add_argument("--api-key", default=os.getenv("TWTAPI_KEY"))
    p_all.add_argument("--window", type=int, default=5)
    p_all.add_argument("--pipeline-args", nargs=argparse.REMAINDER, default=[])

    args = parser.parse_args()

    def resolve_usernames(cmd_args) -> list[str]:
        if getattr(cmd_args, "sample", None):
            all_usernames = load_usernames_from_yaml(USERS_YAML)
            selected = random.sample(all_usernames, min(cmd_args.sample, len(all_usernames)))
            logger.info(f"sampled {len(selected)}/{len(all_usernames)} from {USERS_YAML.name}")
            return selected
        elif getattr(cmd_args, "from_yaml", False):
            return load_usernames_from_yaml(USERS_YAML)
        elif getattr(cmd_args, "usernames", None):
            return [u for u in (_extract_username(u) for u in cmd_args.usernames) if u]
        else:
            parser.error("请指定 --usernames、--from-yaml 或 --sample N")

    if args.cmd == "crawl":
        if not args.api_key:
            parser.error("请设置 TWTAPI_KEY 或通过 --api-key 传入")
        usernames = resolve_usernames(args)
        cmd_crawl(usernames, args.api_key, args.max_tweets, args.translate)

    elif args.cmd == "mindspider":
        target_date = None
        if args.date:
            try:
                target_date = date_type.fromisoformat(args.date)
            except ValueError:
                parser.error(f"日期格式错误：{args.date!r}，请使用 YYYY-MM-DD")
        cmd_crawl_mindspider(
            platforms=args.platforms,
            target_date=target_date,
            run_spider=args.run_spider,
            test_mode=args.test,
            translate=args.translate,
            mindspider_dir=args.mindspider_dir,
        )

    elif args.cmd == "analyze":
        cmd_analyze(
            window=args.window,
            max_age_days=args.max_age_days,
            pipeline_argv=args.pipeline_args,
        )

    elif args.cmd == "all":
        if not args.api_key:
            parser.error("请设置 TWTAPI_KEY 或通过 --api-key 传入")
        usernames = resolve_usernames(args)
        cmd_crawl(usernames, args.api_key, args.max_tweets, args.translate)
        cmd_analyze(
            window=args.window,
            max_age_days=args.max_age_days,
            pipeline_argv=getattr(args, "pipeline_args", []),
        )


if __name__ == "__main__":
    main()
