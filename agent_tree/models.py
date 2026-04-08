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


# ─── 原子信号（一个 Agent 发现的一个话题/观点）────────────────────────────────

class Signal(BaseModel):
    topic: str
    """话题的简短描述（15字以内）"""

    topic_fingerprint: list[str]
    """3-5 个关键词，用于跨 Agent 指纹匹配"""

    strength: float = Field(ge=0.0, le=1.0)
    """在本 batch 内的强度（帖子比例 × 情感强度）"""

    convergence_count: int = 0
    """经会议验证的跨 batch 收敛次数（每次 +1）；越高越是全局信号"""

    representative_quotes: list[str] = Field(default_factory=list)
    """最能代表该信号的 1-3 条原文引用"""

    quality: float = Field(ge=0.0, le=1.0, default=0.5)
    """帖子质量评分（原创性 × 具体性 × 证据充分度的综合）"""

    platform: str = ""
    """来源平台（填充后不可修改）"""

    source_batch_ids: list[str] = Field(default_factory=list)
    """产生此信号的 batch agent ID 列表（会议后合并）"""


# ─── 单个 BatchAgent 的输出 ───────────────────────────────────────────────────

class AgentSummary(BaseModel):
    agent_id: str
    level: str  # "batch" | "platform" | "head"
    platform: str
    batch_index: int = -1
    signals: list[Signal]
    batch_quality: float = Field(ge=0.0, le=1.0, default=0.5)
    """本 batch 整体质量（噪声比、信息密度）"""
    noise_ratio: float = Field(ge=0.0, le=1.0, default=0.0)
    """估计的低质帖子比例"""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_post_count: int = 0


# ─── 一次稀疏指纹会议的产出 ──────────────────────────────────────────────────

class MeetingResult(BaseModel):
    fingerprint_topic: str
    """触发此次会议的共享指纹关键词"""

    participating_agent_ids: list[str]
    is_genuine_convergence: bool
    """是否确认为真实收敛（而非表面相似）"""

    refined_topic: str
    """会议后精炼的话题描述"""

    convergence_strength: float = Field(ge=0.0, le=1.0)
    combined_quotes: list[str] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    """同一话题内部的观点矛盾"""
    reasoning: str = ""


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
