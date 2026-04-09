"""
agent_tree/meeting_agent.py — 稀疏指纹会议（对话优先版）
==========================================================

核心变化（相对旧版）：
  1. 输入：接收完整 AgentSummary（含 narrative + notable_posts）
  2. 输出：discussion_transcript 是主体，指标从对话导出
  3. open_questions：会议无法解决的悬案，传递给 PlatformAgent
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from openai import AsyncOpenAI
from loguru import logger

from .models import AgentSummary, MeetingResult, Signal, ModelConfig
from .llm import llm_json

# ─── Prompts（占位符，用户将填写精确版本）──────────────────────────────────────

_MEETING_SYSTEM = """\
【用户将填写 Meeting system prompt】

当前占位符结构：
你是会议书记员和分析师。多个 Agent 因共享关键词"{fingerprint}"被召集开会。

三阶段任务：
  [陈述] 每个 Agent 基于自己的实际观察发言（引用 narrative/notable_posts 的具体内容）
  [交叉质疑] Agent 之间相互回应（不是重复自己的陈述）
  [综合判断] 诚实地总结：共识、矛盾、或悬而未决

关键规则：
  - 每个发言必须有具体内容支撑，不能泛泛而谈
  - 矛盾 = 同一话题上的立场对立；话题根本不同 = 误报，直接标记
  - 无法确定时，记录为 open_question，不强行给结论

分析目标：{query_anchor}
"""

_MEETING_USER = """\
共享触发关键词：{fingerprint}

参会 Agent 的完整分析材料：

{agents_context}

---

请输出以下 JSON：

{{
  "discussion_transcript": "完整多 Agent 对话记录（这是主体，必须体现真实引用和交叉讨论）\\n格式：\\n[Agent {first_agent_id} 陈述]\\n...（引用自己 narrative/notable_posts 的具体内容）...\\n\\n[Agent {second_agent_id} 陈述]\\n...\\n\\n[交叉质疑]\\n{first_agent_id}：你提到的...我这边看到的是...\\n{second_agent_id}：...\\n\\n[综合判断]\\n...",

  "meeting_conclusion": "3-5句话总结：达成了什么共识，确认了什么矛盾，还有什么悬而未决。不超过150字。",

  "open_questions": [
    "具体疑点（格式：发现了X，但无法判断Y，原因是Z）"
  ],

  "is_genuine_convergence": true或false,
  "convergence_strength": 0.0到1.0,
  "refined_topic": "会议后更准确的话题描述",
  "contradictions": ["立场对立描述，格式：A方认为...，B方认为..."],
  "reasoning": "支持上述指标的一句话依据"
}}
"""


# ─── 指纹分组（返回完整 AgentSummary）──────────────────────────────────────────

def _build_fingerprint_groups(
    summaries: list[AgentSummary],
) -> list[tuple[str, list[AgentSummary]]]:
    keyword_map: dict[str, list[AgentSummary]] = defaultdict(list)

    for summary in summaries:
        for signal in summary.signals:
            for kw in signal.topic_fingerprint:
                kw_norm = kw.strip().lower()
                existing_ids = {s.agent_id for s in keyword_map[kw_norm]}
                if summary.agent_id not in existing_ids:
                    keyword_map[kw_norm].append(summary)

    groups = []
    seen_pairs: set[frozenset] = set()

    for kw, agent_list in keyword_map.items():
        if len(agent_list) < 2:
            continue
        pair_key = frozenset(s.agent_id for s in agent_list)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)
        groups.append((kw, agent_list))

    logger.info(f"指纹匹配产生 {len(groups)} 个会议组（共 {len(summaries)} 个 BatchAgent）")
    return groups


def _build_agents_context(summaries: list[AgentSummary], fingerprint: str) -> str:
    """为每个参会 Agent 构建完整上下文：narrative + notable_posts + 相关 signals。"""
    parts = []
    for i, s in enumerate(summaries, 1):
        related = [
            sig for sig in s.signals
            if any(fingerprint in kw or kw in fingerprint for kw in sig.topic_fingerprint)
        ]
        related_str = ""
        if related:
            sig = related[0]
            related_str = (
                f"\n  触发信号：{sig.topic}（强度{sig.strength:.2f}）"
                f"\n  信号引用：{sig.representative_quotes[:1]}"
            )

        notable_str = ""
        if s.notable_posts:
            lines = [
                f'    - "{np.content[:120]}" → {np.observation}'
                for np in s.notable_posts[:3]
            ]
            notable_str = "\n  值得注意的帖子：\n" + "\n".join(lines)

        weak_str = ""
        if s.weak_signals:
            weak_str = "\n  弱信号：" + "；".join(s.weak_signals[:2])

        narrative_preview = s.narrative[:400] + ("…" if len(s.narrative) > 400 else "")

        parts.append(
            f"【Agent {i}: {s.agent_id}】"
            f"\n  平台：{s.platform} | batch质量：{s.batch_quality:.2f}"
            f"{related_str}"
            f"\n  完整分析：{narrative_preview}"
            f"\n  讨论质感：{s.discourse_texture}"
            f"{notable_str}"
            f"{weak_str}"
        )
    return "\n\n".join(parts)


async def _run_single_meeting(
    fingerprint: str,
    summaries: list[AgentSummary],
    query_anchor: str,
    client: AsyncOpenAI,
    model: str,
) -> MeetingResult | None:
    agent_ids = [s.agent_id for s in summaries]
    agents_context = _build_agents_context(summaries, fingerprint)

    system = _MEETING_SYSTEM.format(fingerprint=fingerprint, query_anchor=query_anchor)
    user = _MEETING_USER.format(
        fingerprint=fingerprint,
        agents_context=agents_context,
        first_agent_id=agent_ids[0] if agent_ids else "A",
        second_agent_id=agent_ids[1] if len(agent_ids) > 1 else "B",
    )

    raw = await llm_json(client, system, user, model=model, temperature=0.3)
    if not raw:
        return None

    try:
        return MeetingResult(
            fingerprint_topic=fingerprint,
            participating_agent_ids=agent_ids,
            discussion_transcript=str(raw.get("discussion_transcript", "")),
            meeting_conclusion=str(raw.get("meeting_conclusion", "")),
            open_questions=list(raw.get("open_questions", []))[:2],
            is_genuine_convergence=bool(raw.get("is_genuine_convergence", False)),
            convergence_strength=float(raw.get("convergence_strength", 0.0)),
            refined_topic=str(raw.get("refined_topic", fingerprint)),
            contradictions=list(raw.get("contradictions", [])),
            reasoning=str(raw.get("reasoning", "")),
        )
    except Exception as e:
        logger.warning(f"MeetingResult parse failed for '{fingerprint}': {e}")
        return None


async def run_meetings(
    summaries: list[AgentSummary],
    query_anchor: str,
    client: AsyncOpenAI,
    model_config: ModelConfig | None = None,
    max_concurrent_meetings: int = 5,
) -> tuple[list[MeetingResult], list[AgentSummary]]:
    cfg = model_config or ModelConfig()
    groups = _build_fingerprint_groups(summaries)
    if not groups:
        logger.info("无指纹重叠，跳过会议阶段")
        return [], summaries

    semaphore = asyncio.Semaphore(max_concurrent_meetings)

    async def _guarded(fp: str, agents: list[AgentSummary]):
        async with semaphore:
            return await _run_single_meeting(fp, agents, query_anchor, client, cfg.meeting_model)

    tasks = [_guarded(fp, agents) for fp, agents in groups]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[MeetingResult] = []
    for r in raw_results:
        if isinstance(r, MeetingResult):
            results.append(r)
        elif isinstance(r, Exception):
            logger.error(f"Meeting task exception: {r}")

    # ── 将收敛结论写回 convergence_count ──
    summary_map = {s.agent_id: s for s in summaries}
    for result in results:
        if not result.is_genuine_convergence:
            continue
        for agent_id in result.participating_agent_ids:
            summary = summary_map.get(agent_id)
            if not summary:
                continue
            for signal in summary.signals:
                if any(
                    result.fingerprint_topic in kw or kw in result.fingerprint_topic
                    for kw in signal.topic_fingerprint
                ):
                    signal.convergence_count += 1
                    break

    logger.info(
        f"会议完成 | 总={len(results)} "
        f"收敛={sum(1 for r in results if r.is_genuine_convergence)} "
        f"悬案={sum(len(r.open_questions) for r in results)}"
    )
    return results, list(summary_map.values())
