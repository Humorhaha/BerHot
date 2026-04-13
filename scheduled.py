"""
定时增量采集 + BERTopic 分析 Pipeline
========================================

数据流:
  [定时任务 / cron / 手动触发]
         ↓
  twitter_crawler    → data/snapshots/twitter_YYYYMMDD_HHMMSS.json
  mindspider_bridge  → data/snapshots/mindspider_YYYYMMDD_HHMMSS.json
  zhihu_crawler      → data/snapshots/zhihu_YYYYMMDD_HHMMSS.json
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

  # 8. 抓取知乎问题（Firecrawl）
  uv run python scheduled.py zhihu-crawl --from-yaml --max-answers 20

定时调度（cron）：

  # 查看会安装的 crontab 条目（dry-run）
  uv run python scheduled.py cron-show

  # 安装到系统 crontab（自动追加，不覆盖已有条目）
  uv run python scheduled.py cron-install

  # 启动长期运行的 Python 调度器守护进程（无需系统 cron）
  uv run python scheduled.py scheduler

  # 手动执行 sources.yaml 中的某个信源
  uv run python scheduled.py run-source --id twitter_ai_researchers
"""

from __future__ import annotations

import heapq
import json
import os
import random
import subprocess
import sys
import time
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv
from loguru import logger

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from twitter_crawler import (
    build_payload,
    fetch_multiple_users,
    save_json,
)
from utils.user_utils import _extract_username, load_usernames_from_yaml


def _repo_root_from_gitfile(project_dir: Path) -> Path | None:
    gitfile = project_dir / ".git"
    if not gitfile.is_file():
        return None
    try:
        content = gitfile.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if not content.startswith("gitdir:"):
        return None
    gitdir = Path(content.split(":", 1)[1].strip()).expanduser()
    if not gitdir.is_absolute():
        gitdir = (project_dir / gitdir).resolve()
    # worktree gitdir 形如 <repo>/.git/worktrees/<name>
    parts = gitdir.parts
    if "worktrees" not in parts:
        return None
    try:
        return gitdir.parents[2]
    except IndexError:
        return None


def _load_env_chain(project_dir: Path) -> None:
    """优先加载 worktree .env，其次回退加载仓库根 .env。"""
    load_dotenv(dotenv_path=project_dir / ".env")
    repo_root = _repo_root_from_gitfile(project_dir)
    if repo_root and repo_root != project_dir:
        fallback_env = repo_root / ".env"
        if fallback_env.exists():
            load_dotenv(dotenv_path=fallback_env, override=False)


_load_env_chain(PROJECT_DIR)

SNAPSHOT_DIR         = PROJECT_DIR / "data" / "snapshots"
LOGS_DIR             = PROJECT_DIR / "logs"
USERS_YAML           = PROJECT_DIR / "users.yaml"
SOURCES_CONFIG       = PROJECT_DIR / "crawl" / "sources.yaml"
ZHIHU_QUESTIONS_YAML = PROJECT_DIR / "crawl" / "zhihu_crawler" / "config" / "questions.yaml"


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

    from utils.preprocess import second_pass

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

    from utils.preprocess import second_pass

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


# ─── Step 1c: 知乎 Firecrawl 爬取快照 ────────────────────────────────────────

def cmd_zhihu_crawl(
    questions: list[str],
    max_answers: int | None = None,
) -> Path:
    """
    使用 Firecrawl 抓取知乎问题，写入带时间戳的快照文件。
    返回写入的快照路径。
    """
    ZHIHU_CRAWLER_DIR = PROJECT_DIR / "crawl" / "zhihu_crawler"
    sys.path.insert(0, str(ZHIHU_CRAWLER_DIR))
    from zhihu.providers.firecrawl import FirecrawlConfig, ZhihuFirecrawlCrawler

    config = FirecrawlConfig.from_env(output_dir=PROJECT_DIR / "data", max_answers=max_answers)
    crawler = ZhihuFirecrawlCrawler(config)
    payload = crawler.crawl_questions(questions, max_answers=max_answers)
    if payload["total_count"] == 0:
        manifest_path = crawler.last_manifest_path
        logger.warning(
            "no zhihu answers fetched, skip snapshot | manifest={}",
            manifest_path if manifest_path else "unknown",
        )
        questions_meta = (crawler.last_manifest or {}).get("questions", [])
        for meta in questions_meta[:10]:
            logger.warning(
                "zhihu empty result | qid={} | mode={} | displayed={} | discovered={} | fetched={} | partial={}{}{}{}",
                meta.get("question_id"),
                meta.get("discovery_mode", "unknown"),
                meta.get("displayed_count"),
                meta.get("discovered_count"),
                meta.get("fetched_count"),
                meta.get("partial"),
                f" | error={meta.get('error')}" if meta.get("error") else "",
                f" | warning={meta.get('warning')}" if meta.get("warning") else "",
                f" | batch_error={meta.get('batch_error')}" if meta.get("batch_error") else "",
            )
        return Path()
    snapshot_path = crawler.last_export_paths.get("snapshot", Path())
    logger.info(f"zhihu crawl done | answers={payload['total_count']} snapshot={snapshot_path}")
    return snapshot_path


# ─── Step 2: 合并快照并分析 ──────────────────────────────────────────────────

def _load_snapshots(
    window: int,
    max_age_days: int | None = None,
    sources: str = "*",
) -> tuple[list[str], list[dict]]:
    """
    读取最近 `window` 个快照，合并去重，返回 (docs, items)。

    Args:
        window:       读取最新的 N 个快照文件
        max_age_days: 丢弃超过 N 天的记录（None = 不限制）
        sources:      文件前缀过滤，"*" = 全部信源，"twitter" = 仅推文快照，
                      "mindspider" = 仅 MindSpider 快照，"zhihu" = 仅知乎快照，以此类推

    docs 优先使用 translated_text，其次 clean_text，再次 content_text。
    """
    pattern = "*.json" if sources == "*" else f"{sources}_*.json"
    snapshots = sorted(SNAPSHOT_DIR.glob(pattern), reverse=True)[:window]
    if not snapshots:
        raise FileNotFoundError(f"no snapshots matching '{pattern}' in {SNAPSHOT_DIR}")

    logger.info(f"merging {len(snapshots)} snapshots (sources={sources!r})")
    all_items: list[dict] = []
    seen_ids: set[str] = set()

    cutoff: datetime | None = None
    if max_age_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        logger.info(f"age filter: >{max_age_days}d records dropped")

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
            all_items.append(item)
            seen_ids.add(pid)

    all_items.sort(
        key=lambda x: datetime.fromisoformat(x.get("date", "1970-01-01")),
        reverse=True,
    )

    # 下游模型优先用 translated_text（非英文已翻译），其次 clean_text，再次 content_text
    all_docs = [
        t.get("translated_text") or t.get("clean_text") or t.get("content_text", "")
        for t in all_items
    ]

    if all_items:
        newest = all_items[0]["date"][:10]
        oldest = all_items[-1]["date"][:10]
        logger.info(f"merged {len(all_docs)} unique docs ({oldest} ~ {newest})")
    else:
        logger.info("merged 0 docs")

    return all_docs, all_items


def cmd_analyze(
    window: int = 5,
    max_age_days: int | None = None,
    pipeline_argv: list[str] | None = None,
    sources: str = "*",
) -> None:
    """
    合并最近 window 个快照（所有信源），运行 BERTopic 全量分析。

    Args:
        window:        合并最近 N 个快照
        max_age_days:  丢弃超过 N 天的记录
        pipeline_argv: 额外传给 pipeline.py 的参数列表
        sources:       快照前缀过滤，"*" = 全部，"twitter" = 仅推文
    """
    docs, items = _load_snapshots(window, max_age_days, sources=sources)
    if not docs:
        logger.warning("no docs available, abort analysis")
        return

    from pipeline import run_pipeline
    from pipeline_config import parse_config

    # 合并输出文件名：全量合并用 merged_texts.json，单信源保留原名
    if sources == "*":
        merged_json = PROJECT_DIR / "data" / "merged_texts.json"
    else:
        merged_json = PROJECT_DIR / "data" / f"{sources}_texts.json"

    # 将最终文本写入 content_text 字段（优先级：translated > clean > raw）
    for item in items:
        item["content_text"] = (
            item.get("translated_text") or item.get("clean_text") or item.get("content_text", "")
        )

    payload = build_payload(items, [])
    save_json(payload, merged_json)

    argv = ["--input-json", str(merged_json)] + (pipeline_argv or [])
    config = parse_config(argv=argv)

    logger.info(f"BERTopic pipeline start | sources={sources!r} docs={len(docs)}")
    run_pipeline(config, quiet=True)

    from pipeline_io import load_docs as _load_docs_for_stats
    analyzed_docs = _load_docs_for_stats(config.input_json)
    logger.info(f"pipeline done | docs={len(analyzed_docs)} → {config.output_dir}")


def cmd_zhihu_analyze(
    window: int = 5,
    max_age_days: int | None = None,
    pipeline_argv: list[str] | None = None,
) -> None:
    """合并最近 window 个知乎快照，运行 BERTopic 全量分析。"""
    ZHIHU_CRAWLER_DIR = PROJECT_DIR / "crawl" / "zhihu_crawler"
    sys.path.insert(0, str(ZHIHU_CRAWLER_DIR))
    from zhihu.exporter import build_payload as build_zhihu_payload

    docs, items = _load_snapshots(window, max_age_days, sources="zhihu")
    if not docs:
        logger.warning("no zhihu docs available, abort analysis")
        return

    from pipeline import run_pipeline
    from pipeline_config import parse_config

    merged_json = PROJECT_DIR / "data" / "zhihu_texts.json"
    for item in items:
        item["content_text"] = (
            item.get("translated_text") or item.get("clean_text") or item.get("content_text", "")
        )

    payload = build_zhihu_payload(items, "scheduled.zhihu")
    save_json(payload, merged_json)

    argv = ["--input-json", str(merged_json)] + (pipeline_argv or [])
    config = parse_config(argv=argv)

    logger.info("BERTopic pipeline start (zhihu)")
    run_pipeline(config, quiet=True)

    from pipeline_io import load_docs as _load_docs_for_stats
    analyzed_docs = _load_docs_for_stats(config.input_json)
    logger.info(f"zhihu pipeline done | docs={len(analyzed_docs)} → {config.output_dir}")


# ─── 多信源调度：sources.yaml 驱动 ───────────────────────────────────────────

def load_sources_config(config_path: Path = SOURCES_CONFIG) -> dict:
    """加载 crawl/sources.yaml，返回解析后的配置字典。"""
    if not config_path.exists():
        raise FileNotFoundError(
            f"信源配置文件不存在：{config_path}\n"
            "  请确认 crawl/sources.yaml 已创建（参考项目文档）"
        )
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def cmd_run_source(source_id: str, config_path: Path = SOURCES_CONFIG) -> None:
    """
    执行 sources.yaml 中指定 id 的单个信源采集任务。
    主要供系统 cron / 调度器调用，也可手动触发测试。

    Args:
        source_id:   sources.yaml 中 source 的 id 字段
        config_path: 信源配置文件路径（默认 crawl/sources.yaml）
    """
    cfg = load_sources_config(config_path)
    source = next((s for s in cfg.get("sources", []) if s["id"] == source_id), None)
    if source is None:
        raise ValueError(
            f"未知信源 id: {source_id!r}\n"
            f"  可用 id: {[s['id'] for s in cfg.get('sources', [])]}"
        )

    source_type = source["type"]
    params = source.get("params", {})
    logger.info(f"[run-source] 执行信源: id={source_id!r} type={source_type!r}")

    if source_type == "twitter":
        api_key = (
            os.getenv(params.get("api_key_env", "TWTAPI_KEY"))
            or os.getenv("TWT_API_KEY")
            or ""
        )
        if not api_key:
            logger.error(f"[run-source] Twitter 信源 '{source_id}': TWTAPI_KEY 未设置，跳过")
            return
        if params.get("from_yaml"):
            usernames = load_usernames_from_yaml(USERS_YAML)
        else:
            usernames = [
                u for u in (_extract_username(u) for u in params.get("usernames", [])) if u
            ]
        if params.get("sample"):
            usernames = random.sample(usernames, min(params["sample"], len(usernames)))
        cmd_crawl(
            usernames=usernames,
            api_key=api_key,
            max_tweets=params.get("max_tweets", 200),
            translate=params.get("translate", False),
        )

    elif source_type == "mindspider":
        date_str = params.get("date")
        target_date = date_type.fromisoformat(date_str) if date_str else None
        mindspider_dir = params.get("mindspider_dir")
        cmd_crawl_mindspider(
            platforms=params.get("platforms"),
            target_date=target_date,
            run_spider=params.get("run_spider", False),
            test_mode=params.get("test_mode", False),
            translate=params.get("translate", False),
            mindspider_dir=Path(mindspider_dir) if mindspider_dir else None,
        )

    elif source_type == "zhihu":
        if params.get("from_yaml"):
            ZHIHU_CRAWLER_DIR = PROJECT_DIR / "crawl" / "zhihu_crawler"
            sys.path.insert(0, str(ZHIHU_CRAWLER_DIR))
            from zhihu.providers.firecrawl import load_question_targets
            questions = load_question_targets(questions_file=str(ZHIHU_QUESTIONS_YAML))
        else:
            questions = params.get("question_ids", [])
        cmd_zhihu_crawl(questions=questions, max_answers=params.get("max_answers"))

    else:
        raise ValueError(f"不支持的信源类型: {source_type!r}，支持: twitter, mindspider, zhihu")


# ─── Cron 安装辅助 ────────────────────────────────────────────────────────────

def _generate_crontab_lines(config_path: Path = SOURCES_CONFIG) -> list[str]:
    """
    根据 sources.yaml 生成对应的 crontab 条目列表。
    每个条目调用 `python scheduled.py run-source --id <id>`，日志写入 logs/。
    """
    cfg = load_sources_config(config_path)
    python = sys.executable
    script = str(PROJECT_DIR / "scheduled.py")
    cwd = str(PROJECT_DIR)
    logs = str(LOGS_DIR)

    lines: list[str] = []

    for source in cfg.get("sources", []):
        if not source.get("enabled", True):
            continue
        cron_expr = source.get("cron", "")
        if not cron_expr:
            logger.warning(f"[cron] 信源 '{source['id']}' 缺少 cron 字段，跳过")
            continue
        sid = source["id"]
        log_file = f"{logs}/cron_{sid}.log"
        line = (
            f"{cron_expr}  "
            f"cd {cwd} && {python} {script} run-source --id {sid} "
            f">> {log_file} 2>&1"
        )
        lines.append(line)

    analysis = cfg.get("analysis", {})
    if analysis.get("enabled", True) and analysis.get("cron"):
        window = analysis.get("window", 5)
        sources_filter = analysis.get("sources", "*")
        extra_args = " ".join(str(a) for a in analysis.get("pipeline_args", []))
        log_file = f"{logs}/cron_analysis.log"
        line = (
            f"{analysis['cron']}  "
            f"cd {cwd} && {python} {script} analyze "
            f"--window {window} --sources {sources_filter}"
            + (f" {extra_args}" if extra_args else "")
            + f" >> {log_file} 2>&1"
        )
        lines.append(line)

    return lines


def cmd_cron_show(config_path: Path = SOURCES_CONFIG) -> None:
    """
    打印将被安装的 crontab 条目（dry-run，不修改系统 crontab）。

    示例：
      uv run python scheduled.py cron-show
    """
    lines = _generate_crontab_lines(config_path)
    if not lines:
        print("# 没有启用的信源，无条目生成")
        return
    print("# BerTopic 自动生成的 crontab 条目")
    print("# 手动安装：将以下内容粘贴到 `crontab -e`")
    print("# 或运行：  uv run python scheduled.py cron-install")
    print()
    for line in lines:
        print(line)


def cmd_cron_install(config_path: Path = SOURCES_CONFIG) -> None:
    """
    将 crontab 条目安装到系统 crontab。

    - 自动读取现有 crontab，过滤掉旧 BerTopic 条目后追加新条目。
    - 安装前会先创建 logs/ 目录。
    - 需要系统已安装 `crontab` 命令（Linux / macOS）。

    示例：
      uv run python scheduled.py cron-install
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    new_lines = _generate_crontab_lines(config_path)
    if not new_lines:
        logger.warning("[cron-install] 没有启用的信源，crontab 未修改")
        return

    # 读取现有 crontab
    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True, text=True,
        )
        existing = result.stdout if result.returncode == 0 else ""
    except FileNotFoundError:
        logger.error("[cron-install] 系统未安装 crontab，请手动将条目添加到调度系统")
        cmd_cron_show(config_path)
        return

    # 过滤掉旧的 BerTopic 自动生成条目（避免重复安装）
    kept_lines = [
        line for line in existing.splitlines()
        if "scheduled.py" not in line and "BerTopic" not in line
    ]

    all_lines = (
        kept_lines
        + ["", "# BerTopic scheduled crawl (auto-generated — do not edit manually)"]
        + new_lines
        + [""]
    )
    new_crontab = "\n".join(all_lines)

    proc = subprocess.run(["crontab", "-"], input=new_crontab, text=True)
    if proc.returncode == 0:
        logger.info(f"[cron-install] 成功安装 {len(new_lines)} 个 crontab 条目")
        for line in new_lines:
            logger.info(f"  {line}")
    else:
        logger.error("[cron-install] crontab 安装失败，请手动运行 `crontab -e` 并粘贴以下条目：")
        cmd_cron_show(config_path)


# ─── Python 调度器守护进程 ────────────────────────────────────────────────────

def _next_run(cron_expr: str, after: datetime) -> datetime:
    """使用 croniter 计算 cron 表达式在 after 时间之后的下一次触发时刻。"""
    try:
        from croniter import croniter
    except ImportError:
        raise RuntimeError(
            "croniter 未安装。请运行：\n"
            "  uv add croniter\n"
            "或：pip install croniter"
        )
    return croniter(cron_expr, after).get_next(datetime)


def cmd_scheduler(config_path: Path = SOURCES_CONFIG) -> None:
    """
    启动长期运行的 Python 调度器守护进程。

    读取 crawl/sources.yaml，按各信源的 cron 表达式自动触发采集任务，
    无需系统级 cron（适合容器或无 crontab 权限的环境）。

    - 使用最小堆按下次触发时间排序，精度：分钟级
    - Ctrl+C 安全退出
    - 单次任务失败不影响其他任务继续调度

    示例：
      uv run python scheduled.py scheduler
      # 或后台运行：
      nohup uv run python scheduled.py scheduler > logs/scheduler.log 2>&1 &
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    cfg = load_sources_config(config_path)
    now = datetime.now()

    # 最小堆元素：(next_run: datetime, task_id: str, task_cfg: dict)
    heap: list[tuple[datetime, str, dict]] = []

    for source in cfg.get("sources", []):
        if not source.get("enabled", True):
            logger.info(f"[scheduler] 跳过已禁用信源: {source['id']}")
            continue
        cron_expr = source.get("cron", "")
        if not cron_expr:
            logger.warning(f"[scheduler] 信源 '{source['id']}' 缺少 cron 字段，跳过")
            continue
        next_t = _next_run(cron_expr, now)
        heapq.heappush(heap, (next_t, source["id"], source))
        logger.info(f"[scheduler] 注册信源: {source['id']} | 下次运行: {next_t.strftime('%Y-%m-%d %H:%M')}")

    analysis_cfg = cfg.get("analysis", {})
    if analysis_cfg.get("enabled", True) and analysis_cfg.get("cron"):
        next_t = _next_run(analysis_cfg["cron"], now)
        heapq.heappush(heap, (next_t, "_analysis", analysis_cfg))
        logger.info(f"[scheduler] 注册分析任务 | 下次运行: {next_t.strftime('%Y-%m-%d %H:%M')}")

    if not heap:
        logger.warning("[scheduler] 没有任何启用的任务，退出")
        return

    logger.info(f"[scheduler] 已启动，监控 {len(heap)} 个任务（Ctrl+C 退出）")

    try:
        while heap:
            next_run_time, task_id, task_cfg = heapq.heappop(heap)
            sleep_secs = (next_run_time - datetime.now()).total_seconds()

            if sleep_secs > 0:
                logger.info(
                    f"[scheduler] 休眠 {sleep_secs/60:.1f} min → "
                    f"下次任务: {task_id!r} @ {next_run_time.strftime('%Y-%m-%d %H:%M')}"
                )
                time.sleep(sleep_secs)

            logger.info(f"[scheduler] ▶ 开始执行: {task_id!r}")
            try:
                if task_id == "_analysis":
                    cmd_analyze(
                        window=task_cfg.get("window", 5),
                        pipeline_argv=task_cfg.get("pipeline_args") or [],
                        sources=task_cfg.get("sources", "*"),
                    )
                else:
                    cmd_run_source(task_id, config_path)
                logger.info(f"[scheduler] ✓ 完成: {task_id!r}")
            except Exception as exc:
                logger.error(f"[scheduler] ✗ 任务失败: {task_id!r} — {exc}")

            # 重新计算下次触发时间并入堆
            next_t = _next_run(task_cfg["cron"], datetime.now())
            heapq.heappush(heap, (next_t, task_id, task_cfg))

    except KeyboardInterrupt:
        logger.info("[scheduler] 收到 Ctrl+C，调度器退出")


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
    p_analyze.add_argument("--sources", default="*",
                           help="快照前缀过滤：'*'=全部, 'twitter', 'mindspider', 'zhihu'（默认 *）")
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
    p_all.add_argument("--sources", default="*",
                       help="分析时的快照前缀过滤（默认 *）")
    p_all.add_argument("--pipeline-args", nargs=argparse.REMAINDER, default=[])

    # --- zhihu-crawl ---
    p_zhihu_crawl = subparsers.add_parser("zhihu-crawl", help="抓取知乎问题，写时间戳快照")
    p_zhihu_crawl.add_argument("--question-ids", nargs="+")
    p_zhihu_crawl.add_argument("--questions-file")
    p_zhihu_crawl.add_argument("--from-yaml", action="store_true")
    p_zhihu_crawl.add_argument("--max-answers", type=int)

    # --- zhihu-analyze ---
    p_zhihu_analyze = subparsers.add_parser("zhihu-analyze", help="合并知乎快照并运行 pipeline")
    p_zhihu_analyze.add_argument("--window", type=int, default=5)
    p_zhihu_analyze.add_argument("--max-age-days", type=int, metavar="D")
    p_zhihu_analyze.add_argument("--pipeline-args", nargs=argparse.REMAINDER, default=[])

    # --- zhihu-all ---
    p_zhihu_all = subparsers.add_parser("zhihu-all", help="抓取知乎 + 分析（一次性完成）")
    p_zhihu_all.add_argument("--question-ids", nargs="+")
    p_zhihu_all.add_argument("--questions-file")
    p_zhihu_all.add_argument("--from-yaml", action="store_true")
    p_zhihu_all.add_argument("--max-answers", type=int)
    p_zhihu_all.add_argument("--max-age-days", type=int, metavar="D")
    p_zhihu_all.add_argument("--window", type=int, default=5)
    p_zhihu_all.add_argument("--pipeline-args", nargs=argparse.REMAINDER, default=[])

    # --- run-source ---
    p_run_source = subparsers.add_parser(
        "run-source",
        help="执行 sources.yaml 中指定 id 的单个信源（供 cron / scheduler 调用）",
    )
    p_run_source.add_argument(
        "--id", required=True, metavar="SOURCE_ID",
        help="sources.yaml 中的信源 id，例如 twitter_ai_researchers",
    )
    p_run_source.add_argument(
        "--config", type=Path, default=SOURCES_CONFIG,
        metavar="PATH",
        help=f"信源配置文件路径（默认 {SOURCES_CONFIG}）",
    )

    # --- scheduler ---
    p_scheduler = subparsers.add_parser(
        "scheduler",
        help="启动 Python 调度器守护进程，按 sources.yaml 中的 cron 定时采集（无需系统 cron）",
    )
    p_scheduler.add_argument(
        "--config", type=Path, default=SOURCES_CONFIG,
        metavar="PATH",
        help=f"信源配置文件路径（默认 {SOURCES_CONFIG}）",
    )

    # --- cron-show ---
    p_cron_show = subparsers.add_parser(
        "cron-show",
        help="打印将被安装的 crontab 条目（dry-run）",
    )
    p_cron_show.add_argument(
        "--config", type=Path, default=SOURCES_CONFIG,
        metavar="PATH",
    )

    # --- cron-install ---
    p_cron_install = subparsers.add_parser(
        "cron-install",
        help="将 sources.yaml 中启用的信源安装到系统 crontab",
    )
    p_cron_install.add_argument(
        "--config", type=Path, default=SOURCES_CONFIG,
        metavar="PATH",
    )

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

    def resolve_questions(cmd_args) -> list[str]:
        ZHIHU_CRAWLER_DIR = PROJECT_DIR / "crawl" / "zhihu_crawler"
        sys.path.insert(0, str(ZHIHU_CRAWLER_DIR))
        from zhihu.providers.firecrawl import load_question_targets

        questions_file = getattr(cmd_args, "questions_file", None)
        if getattr(cmd_args, "from_yaml", False):
            questions_file = str(ZHIHU_QUESTIONS_YAML)

        if getattr(cmd_args, "question_ids", None):
            return [qid for qid in cmd_args.question_ids if qid]
        if questions_file:
            return load_question_targets(questions_file=questions_file)
        parser.error("请指定 --question-ids、--questions-file 或 --from-yaml")

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
            sources=args.sources,
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
            sources=args.sources,
        )

    elif args.cmd == "zhihu-crawl":
        questions = resolve_questions(args)
        cmd_zhihu_crawl(questions, args.max_answers)

    elif args.cmd == "zhihu-analyze":
        cmd_zhihu_analyze(
            window=args.window,
            max_age_days=args.max_age_days,
            pipeline_argv=args.pipeline_args,
        )

    elif args.cmd == "zhihu-all":
        questions = resolve_questions(args)
        cmd_zhihu_crawl(questions, args.max_answers)
        cmd_zhihu_analyze(
            window=args.window,
            max_age_days=args.max_age_days,
            pipeline_argv=getattr(args, "pipeline_args", []),
        )

    elif args.cmd == "run-source":
        cmd_run_source(source_id=args.id, config_path=args.config)

    elif args.cmd == "scheduler":
        cmd_scheduler(config_path=args.config)

    elif args.cmd == "cron-show":
        cmd_cron_show(config_path=args.config)

    elif args.cmd == "cron-install":
        cmd_cron_install(config_path=args.config)


if __name__ == "__main__":
    main()
