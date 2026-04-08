"""
agent_tree/batch_agent.py — 叶子节点 Agent
===========================================

每个 BatchAgent 负责：
  1. 独立分析分配到的一批帖子
  2. 输出结构化 AgentSummary（含信号指纹，供会议匹配用）
  3. 分析完成后"销毁"（对象不再被引用），但 summary 持久化到磁盘

平台感知 batch 策略（由 PlatformAgent 负责切分，传入已切好的 posts）：
  - X/Twitter：按条数切（30 条/batch）
  - 知乎：按字符数切（3000 字/batch）
"""

from __future__ import annotations

import json
from pathlib import Path
from openai import AsyncOpenAI
from loguru import logger

from .models import AgentSummary, Signal
from .llm import llm_json

# ─── Prompt 模板 ─────────────────────────────────────────────────────────────

_SYSTEM = """你是一名舆情分析专家。你将收到一批社交媒体帖子，任务是：
1. 识别帖子中讨论的主要话题/观点
2. 为每个话题提取 3-5 个关键词指纹（用于跨批次匹配）
3. 评估每个话题的强度和帖子整体质量
4. 提取最具代表性的原文引用

注意：
- 区分原创内容和转发/引用（转发权重降低）
- 具体、有论据的帖子比情绪宣泄质量更高
- 相同意思的不同表达算同一信号
- 严格输出 JSON，不要加任何解释文字

分析目标：{query_anchor}
"""

_USER = """帖子数量：{post_count}
平台：{platform}

帖子内容：
{posts_text}

请输出以下 JSON 格式（字段名必须完全一致）：
{{
  "signals": [
    {{
      "topic": "话题描述（15字以内）",
      "topic_fingerprint": ["关键词1", "关键词2", "关键词3"],
      "strength": 0.0到1.0,
      "representative_quotes": ["引用1", "引用2"],
      "quality": 0.0到1.0
    }}
  ],
  "batch_quality": 0.0到1.0,
  "noise_ratio": 0.0到1.0
}}

signals 最多输出 8 个，按 strength 降序排列。
"""


# ─── BatchAgent ───────────────────────────────────────────────────────────────

class BatchAgent:
    def __init__(
        self,
        agent_id: str,
        platform: str,
        posts: list[dict],
        query_anchor: str,
        client: AsyncOpenAI,
        model: str = "gpt-4o-mini",
        save_dir: Path | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.platform = platform
        self.posts = posts
        self.query_anchor = query_anchor
        self._client = client
        self._model = model
        self._save_dir = save_dir
        self._summary: AgentSummary | None = None

    @staticmethod
    def _format_posts(posts: list[dict], platform: str) -> str:
        """将帖子列表格式化为 LLM 可读的文本。"""
        lines = []
        for i, p in enumerate(posts, 1):
            text = p.get("content_text") or p.get("text") or ""
            author = p.get("author") or p.get("username") or "匿名"
            # 知乎回答截断到 500 字，避免超出 context
            if platform == "zhihu" and len(text) > 500:
                text = text[:500] + "…（已截断）"
            lines.append(f"[{i}] @{author}: {text}")
        return "\n".join(lines)

    async def analyze(self) -> AgentSummary:
        """执行分析，返回 AgentSummary。调用完成后可安全丢弃本对象。"""
        logger.info(f"BatchAgent {self.agent_id} | platform={self.platform} | posts={len(self.posts)}")

        posts_text = self._format_posts(self.posts, self.platform)
        system = _SYSTEM.format(query_anchor=self.query_anchor)
        user = _USER.format(
            post_count=len(self.posts),
            platform=self.platform,
            posts_text=posts_text,
        )

        raw = await llm_json(self._client, system, user, model=self._model)

        signals = []
        for s in raw.get("signals", []):
            try:
                sig = Signal(
                    topic=s.get("topic", "未知话题"),
                    topic_fingerprint=s.get("topic_fingerprint", [])[:5],
                    strength=float(s.get("strength", 0.5)),
                    representative_quotes=s.get("representative_quotes", [])[:3],
                    quality=float(s.get("quality", 0.5)),
                    platform=self.platform,
                    source_batch_ids=[self.agent_id],
                )
                signals.append(sig)
            except Exception as e:
                logger.warning(f"Signal parse error in {self.agent_id}: {e}")

        self._summary = AgentSummary(
            agent_id=self.agent_id,
            level="batch",
            platform=self.platform,
            signals=signals,
            batch_quality=float(raw.get("batch_quality", 0.5)),
            noise_ratio=float(raw.get("noise_ratio", 0.0)),
            raw_post_count=len(self.posts),
        )

        self._persist()
        logger.info(f"BatchAgent {self.agent_id} done | signals={len(signals)}")
        return self._summary

    def _persist(self) -> None:
        """将 summary JSON 持久化到磁盘（节点销毁后仍可审计）。"""
        if self._save_dir is None or self._summary is None:
            return
        self._save_dir.mkdir(parents=True, exist_ok=True)
        path = self._save_dir / f"{self.agent_id}.json"
        path.write_text(self._summary.model_dump_json(indent=2), encoding="utf-8")
