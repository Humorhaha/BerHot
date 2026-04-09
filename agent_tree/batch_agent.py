"""
agent_tree/batch_agent.py — 叶子节点 Agent（信息量最大层）
============================================================

输出结构分两部分，服务两类下游消费者：

  富文本部分（LLM 消费）：
    narrative         → 完整文字分析，供 Meeting 和 PlatformAgent 理解上下文
    notable_posts     → 3-5 条值得单独记录的帖子（原文 + 观察）
    weak_signals      → 不确定的苗头，传给上层决定是否关注
    discourse_texture → 讨论质感（情绪温度、极化程度、参与者类型）

  结构化部分（算法消费）：
    signals           → 指纹匹配用，从叙事中导出，与 narrative 保持一致
    batch_quality     → 排序过滤用
    noise_ratio       → 质量评估用

Prompt 设计原则：先叙事，后提炼。
  LLM 先做自由文字分析（narrative），再从分析中导出结构化 signals。
  信号必须与叙事一致，不允许 signals 里有叙事没提到的话题。
"""

from __future__ import annotations

from pathlib import Path
from openai import AsyncOpenAI
from loguru import logger

from .models import AgentSummary, Signal, NotablePost, ModelConfig
from .llm import llm_json

# ─── Prompts ─────────────────────────────────────────────────────────────────
# 以下为占位符结构。用户将根据商定的设计填写精确版本。
# 保留的结构约束：
#   1. JSON 字段顺序 = 思考顺序（先 narrative 后 signals）
#   2. signals 里有的话题，narrative 里必须有解释（一致性约束）
#   3. weak_signals 格式：观察 + 不确定原因
#   4. notable_posts：精确原文（非改写）+ 为什么值得注意

_SYSTEM = """\
【用户将填写 BatchAgent system prompt】

当前占位符结构：
你是分析目标"{query_anchor}"的一线情报员。
你刚刚读完了 {post_count} 条来自 {platform} 的帖子。

你的任务是按照"先理解，后提炼"的顺序输出分析：
第一步：写 narrative（自由分析，不受格式约束，这是你的主要工作）
第二步：记录 notable_posts（原文 + 为什么值得注意）
第三步：记录 weak_signals（不确定的苗头）
第四步：从 narrative 中提炼 signals（结构化，必须与叙事一致）

输出纯 JSON，无需其他文字。
"""

_USER = """\
分析目标：{query_anchor}
平台：{platform} | 帖子数：{post_count}

{posts_text}

---

请输出以下 JSON（字段顺序即思考顺序，请严格遵守）：

{{
  "narrative": "对这个batch帖子的完整文字分析{narrative_length_hint}。描述整体讨论主题、主要立场分布、叙事逻辑，以及你最确定和最不确定的观察。signals 里有的所有话题都必须在这里有解释。",

  "notable_posts": [
    {{
      "content": "帖子精确原文（知乎截取前200字，Twitter保留全文，不改写）",
      "observation": "为什么值得单独记录：它揭示了什么，或与其他帖子有何不同",
      "signal_hint": "它可能指向的信号话题"
    }}
  ],

  "weak_signals": [
    "我看到了[具体观察]，但不确定是否是真实信号，因为[原因]"
  ],

  "discourse_texture": "一两句话：情绪温度（冷静/情绪化）、极化程度、参与者类型（专业/大众）、是否围绕近期具体事件",

  "signals": [
    {{
      "topic": "从 narrative 提炼（15字以内，必须在 narrative 中有解释）",
      "topic_fingerprint": ["名词1", "名词2", "名词3"],
      "strength": 0.0,
      "representative_quotes": ["精确原文，不超过100字"],
      "quality": 0.0
    }}
  ],

  "batch_quality": 0.0,
  "noise_ratio": 0.0
}}

规则：
- signals 最多 8 个，按 strength 降序；noise_ratio > 0.7 时 signals 可为空列表
- notable_posts 选 3-5 条：最能揭示信号本质、最令人意外、或最能体现分歧的
- weak_signals 最多 3 条
- strength 计算：讨论此话题的帖子占比 × 情绪系数（冷静=1/明显立场=2/强烈情绪=3），归一化到0-1
- quality 计算：原创性（非转发）× 具体性（有事实数据）× 证据充分度（有论证）的综合
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
        model_config: ModelConfig | None = None,
        save_dir: Path | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.platform = platform
        self.posts = posts
        self.query_anchor = query_anchor
        self._client = client
        self._model = (model_config or ModelConfig()).batch_model
        self._save_dir = save_dir
        self._summary: AgentSummary | None = None

    @staticmethod
    def _format_posts(posts: list[dict], platform: str) -> str:
        lines = []
        for i, p in enumerate(posts, 1):
            text = p.get("content_text") or p.get("text") or ""
            author = p.get("author") or p.get("username") or "匿名"
            if platform == "zhihu" and len(text) > 500:
                text = text[:500] + "…（已截断）"
            lines.append(f"[{i}] @{author}: {text}")
        return "\n".join(lines)

    @staticmethod
    def _narrative_length_hint(platform: str) -> str:
        return "（约200-300字）" if platform == "twitter" else "（约400-600字）"

    async def analyze(self) -> AgentSummary:
        """执行分析，返回 AgentSummary。调用完成后可安全丢弃本对象。"""
        logger.info(
            f"BatchAgent {self.agent_id} | platform={self.platform} "
            f"| posts={len(self.posts)} | model={self._model}"
        )

        posts_text = self._format_posts(self.posts, self.platform)
        system = _SYSTEM.format(
            query_anchor=self.query_anchor,
            post_count=len(self.posts),
            platform=self.platform,
        )
        user = _USER.format(
            query_anchor=self.query_anchor,
            platform=self.platform,
            post_count=len(self.posts),
            posts_text=posts_text,
            narrative_length_hint=self._narrative_length_hint(self.platform),
        )

        raw = await llm_json(self._client, system, user, model=self._model)

        # ── 解析富文本部分（主体）──
        narrative = str(raw.get("narrative", ""))
        discourse_texture = str(raw.get("discourse_texture", ""))

        weak_signals_raw = raw.get("weak_signals", [])
        weak_signals = (
            [weak_signals_raw] if isinstance(weak_signals_raw, str)
            else list(weak_signals_raw)
        )[:3]

        notable_posts: list[NotablePost] = []
        for np in raw.get("notable_posts", [])[:5]:
            try:
                notable_posts.append(NotablePost(
                    content=str(np.get("content", "")),
                    observation=str(np.get("observation", "")),
                    signal_hint=str(np.get("signal_hint", "")),
                ))
            except Exception as e:
                logger.warning(f"NotablePost parse error in {self.agent_id}: {e}")

        # ── 解析结构化信号部分（从叙事导出）──
        signals: list[Signal] = []
        for s in raw.get("signals", []):
            try:
                signals.append(Signal(
                    topic=str(s.get("topic", "未知话题")),
                    topic_fingerprint=list(s.get("topic_fingerprint", []))[:5],
                    strength=float(s.get("strength", 0.5)),
                    representative_quotes=list(s.get("representative_quotes", []))[:3],
                    quality=float(s.get("quality", 0.5)),
                    platform=self.platform,
                    source_batch_ids=[self.agent_id],
                ))
            except Exception as e:
                logger.warning(f"Signal parse error in {self.agent_id}: {e}")

        self._summary = AgentSummary(
            agent_id=self.agent_id,
            level="batch",
            platform=self.platform,
            narrative=narrative,
            notable_posts=notable_posts,
            weak_signals=weak_signals,
            discourse_texture=discourse_texture,
            signals=signals,
            batch_quality=float(raw.get("batch_quality", 0.5)),
            noise_ratio=float(raw.get("noise_ratio", 0.0)),
            raw_post_count=len(self.posts),
        )

        self._persist()
        logger.info(
            f"BatchAgent {self.agent_id} done | "
            f"signals={len(signals)} notable={len(notable_posts)} "
            f"weak={len(weak_signals)} quality={self._summary.batch_quality:.2f}"
        )
        return self._summary

    def _persist(self) -> None:
        """将完整 summary 持久化到磁盘（节点销毁后仍可审计）。"""
        if self._save_dir is None or self._summary is None:
            return
        self._save_dir.mkdir(parents=True, exist_ok=True)
        path = self._save_dir / f"{self.agent_id}.json"
        path.write_text(self._summary.model_dump_json(indent=2), encoding="utf-8")
