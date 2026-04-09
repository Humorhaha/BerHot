"""
agent_tree/platform_agent.py — 平台级 Agent
============================================

职责：
  1. 按平台感知策略将帖子切分成 batch（X 按条数，知乎按字数）
  2. 并行运行所有 BatchAgent
  3. 召集稀疏指纹会议（meeting.py）
  4. 合并所有 AgentSummary → PlatformSummary
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path
from openai import AsyncOpenAI
from loguru import logger

from .models import AgentSummary, PlatformSummary, Signal, ModelConfig
from .batch_agent import BatchAgent
from .meeting_agent import run_meetings, _build_fingerprint_groups
from .llm import llm_json

# 平台感知切分参数
_BATCH_PARAMS = {
    "twitter": {"batch_size": 30, "mode": "count"},   # 按条数
    "zhihu":   {"batch_size": 3000, "mode": "chars"},  # 按字符数
}
_DEFAULT_BATCH = {"batch_size": 30, "mode": "count"}

# ─── Prompt ──────────────────────────────────────────────────────────────────

_PLATFORM_SYSTEM = """你是平台级分析师，负责整合同一平台上所有批次分析的结果。
你收到了：
  - 各 BatchAgent 的信号汇总（附带收敛计数）
  - 跨批次会议的结论

任务：
  1. 识别高收敛度信号（convergence_count 高）作为主要话题
  2. 识别低收敛但高质量的信号作为小众但值得关注的信号
  3. 识别平台内部的观点矛盾
  4. 总结平台整体情绪和核心叙事
  5. 输出标准 JSON

分析目标：{query_anchor}
"""

_PLATFORM_USER = """平台：{platform}
总帖子数：{total_posts}
Batch 数：{batch_count}
会议场次：{meeting_count}

信号概览（按 convergence_count × strength 降序，前15个）：
{signals_overview}

会议记录（高收敛会议含完整对话摘录，其余仅含结论）：
{meeting_context}

批次富文本分析（质量最高的批次）：
{batch_context}

待关注悬案（来自各会议）：
{open_questions_text}

请输出 JSON：
{{
  "top_signals": [
    {{"topic": "话题", "strength": 0.0-1.0, "convergence_count": int,
      "representative_quotes": ["引用1", "引用2"], "quality": 0.0-1.0}}
  ],
  "niche_signals": [...],
  "contradictions": [
    {{"topic": "话题", "side_a": "观点A", "side_b": "观点B", "intensity": 0.0-1.0}}
  ],
  "overall_sentiment": "positive/negative/mixed/neutral",
  "key_narratives": ["叙事1", "叙事2", "叙事3"],
  "analysis_quality": 0.0-1.0
}}

top_signals 最多 6 个，niche_signals 最多 4 个。
"""


# ─── PlatformAgent ───────────────────────────────────────────────────────────

class PlatformAgent:
    def __init__(
        self,
        platform: str,
        posts: list[dict],
        query_anchor: str,
        client: AsyncOpenAI,
        model_config: ModelConfig | None = None,
        save_dir: Path | None = None,
        max_concurrent_batches: int = 8,
        seed: int = 42,
    ) -> None:
        self.platform = platform
        self.query_anchor = query_anchor
        self._client = client
        self._model_config = model_config or ModelConfig()
        self._model = self._model_config.platform_model
        self._save_dir = save_dir
        self._max_concurrent = max_concurrent_batches

        # 随机打乱（防止位置偏差）
        random.seed(seed)
        self._posts = list(posts)
        random.shuffle(self._posts)

    def _split_batches(self) -> list[list[dict]]:
        """平台感知 batch 切分。"""
        params = _BATCH_PARAMS.get(self.platform, _DEFAULT_BATCH)
        mode = params["mode"]
        size = params["batch_size"]

        if mode == "count":
            return [
                self._posts[i: i + size]
                for i in range(0, len(self._posts), size)
            ]
        else:  # chars
            batches, cur_batch, cur_chars = [], [], 0
            for post in self._posts:
                text = post.get("content_text") or post.get("text") or ""
                cur_batch.append(post)
                cur_chars += len(text)
                if cur_chars >= size:
                    batches.append(cur_batch)
                    cur_batch, cur_chars = [], 0
            if cur_batch:
                batches.append(cur_batch)
            return batches

    async def orchestrate(self) -> PlatformSummary:
        batches = self._split_batches()
        logger.info(f"PlatformAgent [{self.platform}] | posts={len(self._posts)} | batches={len(batches)}")

        batch_save_dir = (self._save_dir / self.platform) if self._save_dir else None

        # ── 1. 并行运行所有 BatchAgent ──
        semaphore = asyncio.Semaphore(self._max_concurrent)

        async def _run_batch(idx: int, batch_posts: list[dict]) -> AgentSummary:
            agent_id = f"{self.platform}_batch_{idx:03d}"
            agent = BatchAgent(
                agent_id=agent_id,
                platform=self.platform,
                posts=batch_posts,
                query_anchor=self.query_anchor,
                client=self._client,
                model_config=self._model_config,
                save_dir=batch_save_dir,
            )
            agent.batch_index = idx  # type: ignore[attr-defined]
            async with semaphore:
                summary = await agent.analyze()
            summary.batch_index = idx
            return summary

        batch_tasks = [_run_batch(i, batch) for i, batch in enumerate(batches)]
        summaries: list[AgentSummary] = await asyncio.gather(*batch_tasks)

        # ── 2. 稀疏指纹会议 ──
        meeting_results, updated_summaries = await run_meetings(
            summaries,
            query_anchor=self.query_anchor,
            client=self._client,
            model_config=self._model_config,
        )

        # ── 3. 整合所有信号，构建 PlatformSummary ──
        platform_summary = await self._synthesize(updated_summaries, meeting_results)
        logger.info(f"PlatformAgent [{self.platform}] done | top_signals={len(platform_summary.top_signals)}")
        return platform_summary

    @staticmethod
    def _build_meeting_context(meeting_results, top_k_full: int = 3) -> str:
        """
        分级构建会议摘要文本：
          - 高收敛度（前 top_k_full 个）：传入完整 discussion_transcript
          - 其余：只传 meeting_conclusion + open_questions
        这样在保留最重要信息的同时控制 token 消耗。
        """
        if not meeting_results:
            return "无会议记录"

        # 按收敛强度排序
        sorted_results = sorted(
            meeting_results,
            key=lambda r: r.convergence_strength,
            reverse=True,
        )

        parts = []
        for i, r in enumerate(sorted_results):
            if i < top_k_full and r.is_genuine_convergence:
                # 完整对话记录
                transcript_preview = r.discussion_transcript[:600] + (
                    "…（截断）" if len(r.discussion_transcript) > 600 else ""
                )
                parts.append(
                    f"[会议: {r.refined_topic}] 收敛强度={r.convergence_strength:.2f}\n"
                    f"  对话摘录：{transcript_preview}\n"
                    f"  结论：{r.meeting_conclusion}"
                )
            else:
                # 只传结论 + 悬案
                open_q = "；".join(r.open_questions) if r.open_questions else "无"
                status = "收敛" if r.is_genuine_convergence else "未收敛/误报"
                parts.append(
                    f"[会议: {r.refined_topic}] {status} 强度={r.convergence_strength:.2f}\n"
                    f"  结论：{r.meeting_conclusion}\n"
                    f"  悬案：{open_q}"
                )

        return "\n\n".join(parts)

    @staticmethod
    def _build_batch_context(summaries: list[AgentSummary], top_k: int = 5) -> str:
        """
        选取质量最高的 top_k 个 batch 的富文本分析传给 PlatformAgent LLM。
        其余 batch 只用结构化信号。
        """
        sorted_summaries = sorted(
            summaries, key=lambda s: s.batch_quality, reverse=True
        )
        parts = []
        for s in sorted_summaries[:top_k]:
            weak = "；".join(s.weak_signals[:2]) if s.weak_signals else "无"
            parts.append(
                f"[Batch {s.agent_id}] 质量={s.batch_quality:.2f} 噪声={s.noise_ratio:.2f}\n"
                f"  分析：{s.narrative[:300]}{'…' if len(s.narrative) > 300 else ''}\n"
                f"  质感：{s.discourse_texture}\n"
                f"  弱信号：{weak}"
            )
        return "\n\n".join(parts) if parts else "无"

    async def _synthesize(
        self,
        summaries: list[AgentSummary],
        meeting_results,
    ) -> PlatformSummary:
        """将所有 BatchAgent 的结果整合为 PlatformSummary。"""

        # 收集所有信号，按 convergence_count × strength 排序
        all_signals: list[Signal] = []
        for s in summaries:
            all_signals.extend(s.signals)
        all_signals.sort(key=lambda s: s.convergence_count * s.strength, reverse=True)

        # ── 分级构建上下文（核心改变）──
        meeting_context = self._build_meeting_context(meeting_results, top_k_full=3)
        batch_context = self._build_batch_context(summaries, top_k=5)

        # 结构化信号概览（供排序参考）
        signals_overview = "\n".join(
            f"  [{s.convergence_count}x收敛 强度{s.strength:.2f} 质量{s.quality:.2f}] {s.topic}"
            for s in all_signals[:15]
        )

        # 收集所有 open_questions
        all_open_questions = []
        for r in meeting_results:
            for q in r.open_questions:
                all_open_questions.append(f"[{r.refined_topic}] {q}")

        system = _PLATFORM_SYSTEM.format(query_anchor=self.query_anchor)
        user = _PLATFORM_USER.format(
            platform=self.platform,
            total_posts=sum(s.raw_post_count for s in summaries),
            batch_count=len(summaries),
            meeting_count=len(meeting_results),
            signals_overview=signals_overview,
            meeting_context=meeting_context,
            batch_context=batch_context,
            open_questions_text="\n".join(all_open_questions) or "无",
        )

        raw = await llm_json(self._client, system, user, model=self._model)

        def _parse_signals(raw_list: list, fallback: list[Signal]) -> list[Signal]:
            result = []
            for s in (raw_list or []):
                try:
                    result.append(Signal(
                        topic=s.get("topic", ""),
                        topic_fingerprint=[],
                        strength=float(s.get("strength", 0.5)),
                        convergence_count=int(s.get("convergence_count", 0)),
                        representative_quotes=s.get("representative_quotes", [])[:3],
                        quality=float(s.get("quality", 0.5)),
                        platform=self.platform,
                    ))
                except Exception:
                    pass
            return result or fallback[:6]

        return PlatformSummary(
            platform=self.platform,
            top_signals=_parse_signals(raw.get("top_signals", []), all_signals),
            niche_signals=_parse_signals(raw.get("niche_signals", []), []),
            contradictions=raw.get("contradictions", []),
            overall_sentiment=raw.get("overall_sentiment", "neutral"),
            key_narratives=raw.get("key_narratives", [])[:5],
            batch_count=len(summaries),
            meeting_count=len(meeting_results),
            analysis_quality=float(raw.get("analysis_quality", 0.5)),
        )
