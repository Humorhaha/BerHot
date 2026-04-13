"""
agent_tree/models.py — 所有 Agent 间传递的结构化数据契约
=========================================================

设计原则：
  - 每个 Agent 只输出 JSON，上游 Agent 只读 JSON，杜绝自然语言耦合
  - convergence_count 是唯一的"注意力权重"载体：每经过一次会议验证就 +1
  - contradictions 是一等公民：观点分裂与观点收敛同等重要
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


# ─── 分层模型配置（哑铃形参数分配）────────────────────────────────────────────

@dataclass
class ModelConfig:
    """
    各层 Agent 使用的模型配置。

    设计原则：哑铃形，而非严格递减。

    认知难度分析：
      BatchAgent    ← 最脏的任务：原始噪声社交媒体 NLU（slang/截断/讽刺/隐含）
      Meeting       ← 较简单：比较两份结构化 JSON 是否真正重叠
      PlatformAgent ← 中等：结构化数据上的编排与汇总
      HeadAgent     ← 战略推理：输入已干净，但最终报告质量最关键

    推荐配置（成本 vs 质量的 Pareto 前沿）：
      head_model     = "gpt-4o"        大模型，终稿质量最重要
      platform_model = "gpt-4o-mini"   结构化编排，mini 足够
      meeting_model  = "gpt-4o-mini"   JSON 比对，最简单任务
      batch_model    = "gpt-4o-mini"   量大，mini 为基础但可升级到 gpt-4o

    如果预算充足：batch_model 升到 "gpt-4o" 效果提升最明显，
    因为底层提取质量决定了整棵树的信息天花板。
    """
    head_model:     str = "gpt-4o"
    platform_model: str = "gpt-4o-mini"
    meeting_model:  str = "gpt-4o-mini"
    batch_model:    str = "gpt-4o-mini"

    @classmethod
    def economy(cls) -> "ModelConfig":
        """全部使用 mini，适合测试/预算有限场景。"""
        return cls(
            head_model="gpt-4o-mini",
            platform_model="gpt-4o-mini",
            meeting_model="gpt-4o-mini",
            batch_model="gpt-4o-mini",
        )

    @classmethod
    def performance(cls) -> "ModelConfig":
        """头部和批次都用大模型，质量最优。"""
        return cls(
            head_model="gpt-4o",
            platform_model="gpt-4o-mini",
            meeting_model="gpt-4o-mini",
            batch_model="gpt-4o",
        )

    @classmethod
    def from_single(cls, model: str) -> "ModelConfig":
        """所有层使用同一模型（兼容旧接口）。"""
        return cls(
            head_model=model,
            platform_model=model,
            meeting_model=model,
            batch_model=model,
        )


# ─── 原子信号（结构化指标，从叙事中导出）───────────────────────────────────────
#
# Signal 不再是信息的主体，而是 AgentSummary.narrative 的机器可读摘要。
# 它的主要用途是：Meeting 的指纹匹配、PlatformAgent 的排序与过滤。

class Signal(BaseModel):
    topic: str
    """话题的简短描述（15字以内）"""

    topic_fingerprint: list[str]
    """3-5 个关键词，用于跨 Agent 指纹匹配。
    规范：名词或名词短语，不含情感词/动词，粒度适中（能区分话题且足够稳定）。"""

    strength: float = Field(ge=0.0, le=1.0)
    """本 batch 内信号强度。计算方式：讨论此话题的帖子占比 × 情绪强度系数
    （1=冷静陈述, 2=明显立场, 3=强烈情绪），归一化到 0-1。"""

    convergence_count: int = 0
    """经会议验证的跨 batch 收敛次数（每次 +1）。越高代表越是全局信号。"""

    representative_quotes: list[str] = Field(default_factory=list)
    """精确原文片段（非改写），每条不超过 100 字，能直接证明信号存在。"""

    quality: float = Field(ge=0.0, le=1.0, default=0.5)
    """帖子质量：原创性（非转发）× 具体性（有事实/数据）× 证据充分度（有论证）的综合。"""

    platform: str = ""
    source_batch_ids: list[str] = Field(default_factory=list)


# ─── BatchAgent 中值得单独记录的帖子 ─────────────────────────────────────────

class NotablePost(BaseModel):
    content: str
    """帖子原文（知乎截取前 200 字，Twitter 保留全文）"""

    observation: str
    """为什么值得注意：它揭示了什么、与其他帖子有何不同、或代表了什么边缘信号"""

    signal_hint: str
    """它可能指向的信号话题（与 Signal.topic 对应，但允许不确定）"""


# ─── BatchAgent 的完整输出（信息量最大的层）──────────────────────────────────
#
# 设计原则：自下而上信息量逐步减少，BatchAgent 是信息最丰富的节点。
# 分两部分：
#   1. 富文本分析（narrative + notable_posts + weak_signals + discourse_texture）
#      → 供 Meeting 和 PlatformAgent 的 LLM 消费，保留最大信息量
#   2. 结构化信号（signals + 指标）
#      → 供 Meeting 指纹匹配、PlatformAgent 排序过滤等算法消费
# 两部分必须相互一致：signals 里有的，narrative 里应有解释。

class AgentSummary(BaseModel):
    agent_id: str
    level: str  # "batch" | "platform" | "head"
    platform: str
    batch_index: int = -1

    # ── 富文本部分（主体，信息量最大）────────────────────────────────────────
    narrative: str = ""
    """对本 batch 的完整文字分析。
    应包含：整体讨论主题、主要立场分布、叙事逻辑、观察到的模式。
    字数参考：Twitter batch ~200-300 字，知乎 batch ~400-600 字。"""

    notable_posts: list[NotablePost] = Field(default_factory=list)
    """3-5 条最值得注意的帖子，含具体原文和观察。
    选择标准：最能揭示信号本质、或最能体现分歧、或最令人意外的。"""

    weak_signals: list[str] = Field(default_factory=list)
    """观察到但尚不确定的苗头（不足以形成 Signal，但值得传递给上层）。
    格式：一句话描述观察 + 不确定原因，例如"有2条帖子提到X，
    但不确定是真实趋势还是偶发事件"。"""

    discourse_texture: str = ""
    """一两句话描述讨论的整体质感：极化程度、情绪温度、是否专业圈子、
    是否围绕近期事件等。供 PlatformAgent 判断 batch 的参考价值。"""

    # ── 结构化信号部分（从叙事导出，供算法使用）──────────────────────────────
    signals: list[Signal] = Field(default_factory=list)
    batch_quality: float = Field(ge=0.0, le=1.0, default=0.5)
    noise_ratio: float = Field(ge=0.0, le=1.0, default=0.0)

    # ── 元数据 ────────────────────────────────────────────────────────────────
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_post_count: int = 0


# ─── 一次稀疏指纹会议的产出（对话优先，指标导出）────────────────────────────
#
# 设计原则：
#   - discussion_transcript 是主体，LLM 模拟每个 Agent 轮流发言
#   - meeting_conclusion 是会议的结论摘要（几句话）
#   - 以下指标字段全部从上面两个文本字段导出，是机器可读的摘要
#   - open_questions 是新增字段：会议无法解决的悬案，传递给 PlatformAgent

class MeetingResult(BaseModel):
    fingerprint_topic: str = ""
    """触发此次会议的共享指纹关键词"""

    participating_agent_ids: list[str] = Field(default_factory=list)

    # ── 主体：对话记录（最丰富）──────────────────────────────────────────────
    discussion_transcript: str = ""
    """模拟的多 Agent 对话记录。
    结构：[Agent X 发言] 具体观察... → [Agent Y 发言] 回应/质疑... → [综合判断]
    每个 Agent 发言应基于其 narrative 和 notable_posts，引用具体原文。"""

    meeting_conclusion: str = ""
    """会议结论摘要（3-5 句话）：达成了什么共识、确认了什么矛盾、
    还有什么悬而未决。供 PlatformAgent 快速消费。"""

    open_questions: list[str] = Field(default_factory=list)
    """会议中发现但无法解决的疑点，传递给 PlatformAgent 关注。
    例如："两个 batch 都提到了 X，但一个认为是支持，一个认为是反对，
    需要更多样本才能判断"。"""

    # ── 导出指标（从对话记录中提炼，供算法使用）──────────────────────────────
    is_genuine_convergence: bool = False
    """从 discussion_transcript 导出：会议是否确认真实收敛。"""

    convergence_strength: float = Field(ge=0.0, le=1.0, default=0.0)
    """从会议强度导出：参与 Agent 数量、共识程度、证据质量的综合。"""

    refined_topic: str = ""
    """会议后精炼的话题描述（比原指纹词更准确）。"""

    contradictions: list[str] = Field(default_factory=list)
    """同一话题内部的立场对立，从 discussion_transcript 中明确标记。"""

    reasoning: str = ""
    """支持上述指标判断的一句话依据。"""


# ─── 平台级汇总 ───────────────────────────────────────────────────────────────

class PlatformSummary(BaseModel):
    platform: str
    top_signals: list[Signal]
    """高收敛度 + 高质量信号（主要话题）"""

    niche_signals: list[Signal]
    """仅出现在单一 batch 但质量高的信号（小众但值得关注）"""

    contradictions: list[dict]
    """平台内部的观点对立，格式：{topic, side_a, side_b, intensity}"""

    overall_sentiment: str  # "positive" | "negative" | "mixed" | "neutral"
    key_narratives: list[str]
    """3-5 条平台核心叙事，供 HeadAgent 跨平台比较"""

    batch_count: int
    meeting_count: int
    analysis_quality: float = Field(ge=0.0, le=1.0)


# ─── 最终报告（HeadAgent 输出）───────────────────────────────────────────────

class FinalReport(BaseModel):
    query_anchor: str
    """本次分析的目标问题"""

    cross_platform_signals: list[Signal]
    """在 X 和知乎上同时出现的强信号（最高置信度）"""

    platform_specific: dict[str, list[Signal]]
    """仅在某平台出现的信号，可能反映用户群差异"""

    cross_platform_contradictions: list[dict]
    """X 与知乎在同一话题上的观点分歧"""

    key_insights: list[str]
    """5-8 条最终洞察（可直接用于预测/决策）"""

    confidence: float = Field(ge=0.0, le=1.0)
    """整体置信度（基于数据质量和收敛度）"""

    platform_summaries: dict[str, PlatformSummary]
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_posts_analyzed: int = 0
    total_api_calls: int = 0
