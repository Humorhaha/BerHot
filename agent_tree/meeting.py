"""
agent_tree/meeting.py — 稀疏指纹会议机制
==========================================

核心设计：
  - 避免 O(n²) 全对全通信
  - 用关键词指纹匹配确定会议参与者：只有指纹有交集的 Agent 才开会
  - 每个会议产出 MeetingResult，更新参与者信号的 convergence_count
  - 会议同时检测收敛（相似观点）和矛盾（对立观点）

时间复杂度：O(n·k)，k = 平均每个 Agent 匹配到的其他 Agent 数
"""

from __future__ import annotations

from collections import defaultdict
from openai import AsyncOpenAI
from loguru import logger
import asyncio

from .models import AgentSummary, MeetingResult, Signal, ModelConfig
from .llm import llm_json

# ─── Prompt ──────────────────────────────────────────────────────────────────

_MEETING_SYSTEM = """你是一名会议主持人。多个分析 Agent 发现了相似的话题，你需要主持讨论：
1. 判断这些话题是否真正收敛（同一议题的不同表达），还是只是表面相似
2. 如果收敛：精炼统一的话题描述，合并最佳引用，评估综合强度
3. 如果矛盾：识别并记录对立的观点
4. 输出标准 JSON，不要任何额外文字

分析目标：{query_anchor}
"""

_MEETING_USER = """共享关键词：{fingerprint}

各 Agent 的发现：
{summaries_text}

请输出 JSON：
{{
  "is_genuine_convergence": true或false,
  "refined_topic": "精炼后的话题描述",
  "convergence_strength": 0.0到1.0,
  "combined_quotes": ["最佳引用1", "最佳引用2", "最佳引用3"],
  "contradictions": ["矛盾点1", "矛盾点2"],
  "reasoning": "判断依据（50字以内）"
}}
"""


# ─── 指纹匹配逻辑 ─────────────────────────────────────────────────────────────

def _build_fingerprint_groups(
    summaries: list[AgentSummary],
    min_overlap: int = 1,
) -> list[tuple[str, list[tuple[AgentSummary, Signal]]]]:
    """
    根据信号指纹对 summaries 分组，返回：
      [(共享关键词, [(summary, signal), ...]), ...]

    只有至少两个不同 Agent 共享同一关键词才形成会议组。
    """
    # keyword → [(summary, signal), ...]
    keyword_map: dict[str, list[tuple[AgentSummary, Signal]]] = defaultdict(list)

    for summary in summaries:
        for signal in summary.signals:
            for kw in signal.topic_fingerprint:
                kw_norm = kw.strip().lower()
                keyword_map[kw_norm].append((summary, signal))

    # 过滤：只保留涉及多个不同 Agent 的关键词
    groups = []
    seen_pairs: set[frozenset] = set()  # 防止相同 Agent 对重复开会

    for kw, entries in keyword_map.items():
        # 去重：同一 agent 对同一关键词只取第一个信号
        agent_seen: dict[str, tuple[AgentSummary, Signal]] = {}
        for summary, signal in entries:
            if summary.agent_id not in agent_seen:
                agent_seen[summary.agent_id] = (summary, signal)

        unique_entries = list(agent_seen.values())
        if len(unique_entries) < 2:
            continue

        # 检查这组 agent pair 是否已经因其他关键词开过会
        pair_key = frozenset(s.agent_id for s, _ in unique_entries)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        groups.append((kw, unique_entries))

    logger.info(f"指纹匹配产生 {len(groups)} 个会议组（共 {len(summaries)} 个 BatchAgent）")
    return groups


# ─── 单次会议 ─────────────────────────────────────────────────────────────────

async def _run_single_meeting(
    fingerprint: str,
    entries: list[tuple[AgentSummary, Signal]],
    query_anchor: str,
    client: AsyncOpenAI,
    model: str,
) -> MeetingResult | None:
    """执行单次指纹会议，返回 MeetingResult。"""
    summaries_lines = []
    for i, (summary, signal) in enumerate(entries, 1):
        quotes = "; ".join(signal.representative_quotes[:2])
        summaries_lines.append(
            f"Agent {i} ({summary.platform} batch {summary.batch_index}):\n"
            f"  话题: {signal.topic}\n"
            f"  强度: {signal.strength:.2f}\n"
            f"  代表引用: {quotes}"
        )

    system = _MEETING_SYSTEM.format(query_anchor=query_anchor)
    user = _MEETING_USER.format(
        fingerprint=fingerprint,
        summaries_text="\n\n".join(summaries_lines),
    )

    raw = await llm_json(client, system, user, model=model, temperature=0.1)
    if not raw:
        return None

    try:
        return MeetingResult(
            fingerprint_topic=fingerprint,
            participating_agent_ids=[s.agent_id for s, _ in entries],
            is_genuine_convergence=bool(raw.get("is_genuine_convergence", False)),
            refined_topic=raw.get("refined_topic", fingerprint),
            convergence_strength=float(raw.get("convergence_strength", 0.5)),
            combined_quotes=raw.get("combined_quotes", [])[:3],
            contradictions=raw.get("contradictions", []),
            reasoning=raw.get("reasoning", ""),
        )
    except Exception as e:
        logger.warning(f"MeetingResult parse failed for '{fingerprint}': {e}")
        return None


# ─── 主入口：运行全部会议并更新信号 convergence_count ─────────────────────────

async def run_meetings(
    summaries: list[AgentSummary],
    query_anchor: str,
    client: AsyncOpenAI,
    model_config: ModelConfig | None = None,
    max_concurrent_meetings: int = 5,
) -> tuple[list[MeetingResult], list[AgentSummary]]:
    """
    运行所有稀疏指纹会议，并将结果反映回 summaries 中的信号。

    Returns:
        (meeting_results, updated_summaries)
    """
    cfg = model_config or ModelConfig()
    meeting_model = cfg.meeting_model

    groups = _build_fingerprint_groups(summaries)
    if not groups:
        logger.info("无指纹重叠，跳过会议阶段")
        return [], summaries

    # 并行运行所有会议（限制并发数避免超 rate limit）
    semaphore = asyncio.Semaphore(max_concurrent_meetings)

    async def _guarded_meeting(fingerprint, entries):
        async with semaphore:
            return await _run_single_meeting(fingerprint, entries, query_anchor, client, meeting_model)

    tasks = [_guarded_meeting(fp, entries) for fp, entries in groups]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[MeetingResult] = []
    for r in raw_results:
        if isinstance(r, MeetingResult):
            results.append(r)
        elif isinstance(r, Exception):
            logger.error(f"Meeting task exception: {r}")

    # ── 将会议结论写回 summaries 的信号 ──
    # 为加速查找，建 agent_id → summary 索引
    summary_map = {s.agent_id: s for s in summaries}

    for result in results:
        if not result.is_genuine_convergence:
            continue  # 未收敛的会议不增加权重
        for agent_id in result.participating_agent_ids:
            summary = summary_map.get(agent_id)
            if not summary:
                continue
            # 找到该 agent 中与 fingerprint 匹配的信号，更新 convergence_count
            for signal in summary.signals:
                if any(
                    result.fingerprint_topic in kw or kw in result.fingerprint_topic
                    for kw in signal.topic_fingerprint
                ):
                    signal.convergence_count += 1
                    # 将会议精炼后的引用合并到信号中（去重）
                    existing = set(signal.representative_quotes)
                    for q in result.combined_quotes:
                        if q not in existing:
                            signal.representative_quotes.append(q)
                            existing.add(q)
                    break

    logger.info(f"会议完成 | 总场次={len(results)} | 收敛={sum(1 for r in results if r.is_genuine_convergence)}")
    return results, list(summary_map.values())
