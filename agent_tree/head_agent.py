"""
agent_tree/head_agent.py — 首脑 Agent
======================================

职责：
  1. 广播 query_anchor 给所有下层 Agent
  2. 接收各 PlatformSummary
  3. 跨平台交叉分析：收敛信号、平台特有信号、跨平台矛盾
  4. 输出最终 FinalReport（含可直接用于预测的 key_insights）
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from openai import AsyncOpenAI
from loguru import logger

from .models import FinalReport, PlatformSummary, Signal, ModelConfig
from .llm import llm_json, get_api_call_count

# ─── Prompt ──────────────────────────────────────────────────────────────────

_HEAD_SYSTEM = """你是首席舆情分析师，负责整合多个社交平台的分析结果，输出最终情报报告。

分析框架：
  1. 跨平台收敛信号 = 在所有平台都出现的话题，置信度最高
  2. 平台特有信号 = 只在某一平台出现，可能反映用户群差异或平台文化
  3. 跨平台矛盾 = 同一话题在不同平台有对立观点，这本身是重要信号
  4. key_insights = 可直接用于预测/决策的具体洞察

要求：
  - 洞察必须具体，避免空泛
  - 标注置信度
  - 指出哪些信号值得持续监控
  - 严格输出 JSON

分析目标：{query_anchor}
"""

_HEAD_USER = """收到以下平台分析报告：

{platform_summaries_text}

请输出以下 JSON：
{{
  "cross_platform_signals": [
    {{"topic": "话题", "strength": 0.0-1.0, "convergence_count": int,
      "representative_quotes": ["引用1", "引用2"], "quality": 0.0-1.0,
      "topic_fingerprint": []}}
  ],
  "platform_specific": {{
    "twitter": [{{"topic": "...", "strength": ..., "convergence_count": 0,
                  "topic_fingerprint": [], "representative_quotes": [], "quality": ...}}],
    "zhihu": [...]
  }},
  "cross_platform_contradictions": [
    {{"topic": "话题", "twitter_view": "X上的观点", "zhihu_view": "知乎上的观点",
      "divergence_intensity": 0.0-1.0, "implication": "分歧的潜在含义"}}
  ],
  "key_insights": [
    "洞察1（具体、可操作）",
    "洞察2",
    "洞察3",
    "洞察4",
    "洞察5"
  ],
  "confidence": 0.0-1.0,
  "monitoring_recommendations": ["建议1", "建议2"]
}}
"""


# ─── HeadAgent ────────────────────────────────────────────────────────────────

class HeadAgent:
    def __init__(
        self,
        query_anchor: str,
        platform_posts: dict[str, list[dict]],
        client: AsyncOpenAI,
        model_config: ModelConfig | None = None,
        save_dir: Path | None = None,
        max_concurrent_batches: int = 8,
    ) -> None:
        self.query_anchor = query_anchor
        self._platform_posts = platform_posts
        self._client = client
        self._model_config = model_config or ModelConfig()
        self._model = self._model_config.head_model  # 顶层使用最强模型
        self._save_dir = save_dir
        self._max_concurrent = max_concurrent_batches

    async def run(self) -> FinalReport:
        """顶层编排：并行运行所有平台，然后综合分析。"""
        from .platform_agent import PlatformAgent

        logger.info(f"HeadAgent 启动 | query='{self.query_anchor}' | platforms={list(self._platform_posts.keys())}")

        platform_save = (self._save_dir / "batch_summaries") if self._save_dir else None

        # ── 并行运行所有 PlatformAgent ──
        async def _run_platform(platform: str, posts: list[dict]) -> tuple[str, PlatformSummary]:
            agent = PlatformAgent(
                platform=platform,
                posts=posts,
                query_anchor=self.query_anchor,
                client=self._client,
                model_config=self._model_config,
                save_dir=platform_save,
                max_concurrent_batches=self._max_concurrent,
            )
            summary = await agent.orchestrate()
            return platform, summary

        tasks = [
            _run_platform(platform, posts)
            for platform, posts in self._platform_posts.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        platform_summaries: dict[str, PlatformSummary] = {}
        for r in results:
            if isinstance(r, tuple):
                platform, summary = r
                platform_summaries[platform] = summary
            else:
                logger.error(f"PlatformAgent failed: {r}")

        if not platform_summaries:
            raise RuntimeError("所有 PlatformAgent 均失败，无法生成报告")

        # ── HeadAgent 综合分析 ──
        report = await self._synthesize(platform_summaries)

        # 保存最终报告
        if self._save_dir:
            self._save_dir.mkdir(parents=True, exist_ok=True)
            report_path = self._save_dir / "final_report.json"
            report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
            logger.info(f"最终报告已保存：{report_path}")

        return report

    async def _synthesize(
        self,
        platform_summaries: dict[str, PlatformSummary],
    ) -> FinalReport:
        """整合多平台结果，生成 FinalReport。"""

        # 构建 LLM 输入文本
        platform_texts = []
        total_posts = 0

        for platform, ps in platform_summaries.items():
            top_sigs = "\n".join(
                f"    [{s.convergence_count}x] {s.topic} (强度{s.strength:.2f})"
                for s in ps.top_signals[:5]
            )
            niche_sigs = "\n".join(
                f"    {s.topic} (强度{s.strength:.2f})"
                for s in ps.niche_signals[:3]
            )
            platform_texts.append(
                f"=== {platform.upper()} ===\n"
                f"情绪：{ps.overall_sentiment}\n"
                f"主要话题：\n{top_sigs}\n"
                f"小众话题：\n{niche_sigs}\n"
                f"核心叙事：{'; '.join(ps.key_narratives[:3])}\n"
                f"分析质量：{ps.analysis_quality:.2f}"
            )

        system = _HEAD_SYSTEM.format(query_anchor=self.query_anchor)
        user = _HEAD_USER.format(
            platform_summaries_text="\n\n".join(platform_texts),
        )

        raw = await llm_json(self._client, system, user, model=self._model, temperature=0.1)

        def _parse_signal_list(raw_list) -> list[Signal]:
            result = []
            for s in (raw_list or []):
                try:
                    result.append(Signal(
                        topic=s.get("topic", ""),
                        topic_fingerprint=s.get("topic_fingerprint", []),
                        strength=float(s.get("strength", 0.5)),
                        convergence_count=int(s.get("convergence_count", 0)),
                        representative_quotes=s.get("representative_quotes", [])[:3],
                        quality=float(s.get("quality", 0.5)),
                        platform="cross-platform",
                    ))
                except Exception:
                    pass
            return result

        platform_specific: dict[str, list[Signal]] = {}
        for platform, raw_sigs in (raw.get("platform_specific") or {}).items():
            sigs = _parse_signal_list(raw_sigs)
            for s in sigs:
                s.platform = platform
            platform_specific[platform] = sigs

        for platform in self._platform_posts:
            if platform not in platform_specific:
                platform_specific[platform] = []

        return FinalReport(
            query_anchor=self.query_anchor,
            cross_platform_signals=_parse_signal_list(raw.get("cross_platform_signals", [])),
            platform_specific=platform_specific,
            cross_platform_contradictions=raw.get("cross_platform_contradictions", []),
            key_insights=raw.get("key_insights", [])[:8],
            confidence=float(raw.get("confidence", 0.5)),
            platform_summaries=platform_summaries,
            total_posts_analyzed=sum(
                sum(s.raw_post_count for s in []) for _ in platform_summaries.values()
            ),
            total_api_calls=get_api_call_count(),
        )
