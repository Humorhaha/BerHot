"""
agent_tree/pipeline.py — AAT 主流水线
=======================================

从现有 snapshot 文件加载数据，运行完整的 AgentAttentionTree，
输出 FinalReport JSON 并打印人类可读摘要。

与现有 scheduled.py 的集成点：
  - 输入格式与 BERTopic pipeline 兼容（同一 snapshot 目录）
  - 输出 aat_report_YYYYMMDD_HHMMSS.json 到 data/aat_reports/
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from openai import AsyncOpenAI

from .head_agent import HeadAgent
from .llm import reset_api_call_count, get_api_call_count
from .models import FinalReport, ModelConfig


PROJECT_DIR = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = PROJECT_DIR / "data" / "snapshots"
REPORT_DIR = PROJECT_DIR / "data" / "aat_reports"


# ─── 数据加载 ─────────────────────────────────────────────────────────────────

def _load_platform_posts(
    snapshot_dir: Path,
    platforms: list[str],
    window: int = 5,
) -> dict[str, list[dict]]:
    """
    从 snapshot 目录加载最近 N 个快照，按平台分组。

    Args:
        snapshot_dir: snapshot JSON 文件目录
        platforms: 要加载的平台列表，如 ["twitter", "zhihu"]
        window: 每个平台最多加载最近几个快照
    """
    platform_posts: dict[str, list[dict]] = {p: [] for p in platforms}

    for platform in platforms:
        # 找出该平台的所有快照，按时间戳降序
        snapshots = sorted(
            snapshot_dir.glob(f"{platform}_*.json"),
            reverse=True,
        )[:window]

        if not snapshots:
            logger.warning(f"未找到平台 {platform} 的快照文件（{snapshot_dir}/{platform}_*.json）")
            continue

        for snap_path in snapshots:
            try:
                with snap_path.open("r", encoding="utf-8") as f:
                    payload = json.load(f)
                texts = payload.get("texts", [])
                platform_posts[platform].extend(texts)
                logger.info(f"加载快照 {snap_path.name} | {len(texts)} 条")
            except Exception as e:
                logger.error(f"加载快照失败 {snap_path}: {e}")

        logger.info(f"平台 {platform} 共加载 {len(platform_posts[platform])} 条帖子")

    # 过滤掉没有内容的平台
    return {p: posts for p, posts in platform_posts.items() if posts}


# ─── 主入口 ───────────────────────────────────────────────────────────────────

async def run_aat(
    query_anchor: str,
    platforms: list[str] | None = None,
    snapshot_dir: Path | None = None,
    window: int = 5,
    model_config: ModelConfig | None = None,
    # 兼容旧接口：单一模型名会自动构建 ModelConfig.from_single()
    model: str | None = None,
    openai_api_key: str | None = None,
    openai_base_url: str | None = None,
    save_dir: Path | None = None,
    max_concurrent_batches: int = 8,
) -> FinalReport:
    """
    运行完整的 AgentAttentionTree 分析。

    Args:
        query_anchor:         分析目标（例如："AI 监管政策的公众态度"）
        platforms:            要分析的平台，默认 ["twitter", "zhihu"]
        snapshot_dir:         快照目录，默认 data/snapshots/
        window:               每个平台最多加载最近 N 个快照
        model_config:         分层模型配置（推荐），优先级高于 model
        model:                单一模型名（兼容旧接口，所有层使用同一模型）
        openai_api_key:       API key（默认读取 OPENAI_API_KEY 环境变量）
        openai_base_url:      自定义 API 地址（用于兼容接口）
        save_dir:             保存中间产物和报告的目录
        max_concurrent_batches: 并发 BatchAgent 数量上限

    Returns:
        FinalReport
    """
    if platforms is None:
        platforms = ["twitter", "zhihu"]
    if snapshot_dir is None:
        snapshot_dir = SNAPSHOT_DIR
    if save_dir is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        save_dir = REPORT_DIR / f"run_{ts}"

    # 解析模型配置：model_config 优先，其次 model 单一值，最后用默认哑铃配置
    if model_config is None:
        model_config = ModelConfig.from_single(model) if model else ModelConfig()

    reset_api_call_count()

    # ── 加载数据 ──
    platform_posts = _load_platform_posts(snapshot_dir, platforms, window)
    if not platform_posts:
        raise ValueError("没有找到任何数据，请先运行爬虫采集数据")

    total_posts = sum(len(p) for p in platform_posts.values())
    logger.info(
        f"AAT 启动 | query='{query_anchor}' | 总帖子={total_posts} | "
        f"模型配置: head={model_config.head_model} / "
        f"platform={model_config.platform_model} / "
        f"meeting={model_config.meeting_model} / "
        f"batch={model_config.batch_model}"
    )

    # ── 构建 OpenAI 客户端 ──
    api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("需要 OPENAI_API_KEY 环境变量或 --openai-api-key 参数")

    client_kwargs = {"api_key": api_key}
    if openai_base_url:
        client_kwargs["base_url"] = openai_base_url
    elif os.getenv("OPENAI_BASE_URL"):
        client_kwargs["base_url"] = os.getenv("OPENAI_BASE_URL")

    client = AsyncOpenAI(**client_kwargs)

    # ── 运行 HeadAgent ──
    head = HeadAgent(
        query_anchor=query_anchor,
        platform_posts=platform_posts,
        client=client,
        model_config=model_config,
        save_dir=save_dir,
        max_concurrent_batches=max_concurrent_batches,
    )

    report = await head.run()
    report.total_api_calls = get_api_call_count()
    report.total_posts_analyzed = total_posts

    # ── 保存报告 ──
    save_dir.mkdir(parents=True, exist_ok=True)
    report_path = save_dir / "final_report.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    _print_summary(report)
    logger.info(f"AAT 完成 | API调用={report.total_api_calls} | 报告: {report_path}")

    return report


def _print_summary(report: FinalReport) -> None:
    """人类可读摘要输出。"""
    sep = "═" * 60
    print(f"\n{sep}")
    print(f"  AgentAttentionTree 分析报告")
    print(f"  目标: {report.query_anchor}")
    print(f"  置信度: {report.confidence:.0%}  |  总API调用: {report.total_api_calls}")
    print(sep)

    if report.cross_platform_signals:
        print("\n【跨平台收敛信号】（X + 知乎同时出现，最高置信度）")
        for s in report.cross_platform_signals[:5]:
            print(f"  ▶ {s.topic}  [收敛×{s.convergence_count}, 强度{s.strength:.2f}]")
            for q in s.representative_quotes[:1]:
                print(f'      \u201c{q[:80]}\u201d')

    for platform, signals in report.platform_specific.items():
        if signals:
            print(f"\n【{platform} 特有信号】")
            for s in signals[:3]:
                print(f"  ▷ {s.topic}  [强度{s.strength:.2f}]")

    if report.cross_platform_contradictions:
        print("\n【跨平台观点分歧】")
        for c in report.cross_platform_contradictions[:3]:
            print(f"  ⚡ {c.get('topic', '')}")
            print(f"     X:    {c.get('twitter_view', '')[:60]}")
            print(f"     知乎: {c.get('zhihu_view', '')[:60]}")

    if report.key_insights:
        print("\n【关键洞察】")
        for i, insight in enumerate(report.key_insights, 1):
            print(f"  {i}. {insight}")

    print(f"\n{sep}\n")
