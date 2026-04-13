"""
agent_tree — AgentAttentionTree 舆情分析框架
=============================================

架构概览：
  HeadAgent
    ├── PlatformAgent [twitter]
    │     └── BatchAgent × N  →  稀疏指纹会议  →  PlatformSummary
    └── PlatformAgent [zhihu]
          └── BatchAgent × M  →  稀疏指纹会议  →  PlatformSummary
"""

from .models import Signal, AgentSummary, MeetingResult, PlatformSummary, FinalReport, ModelConfig
from .pipeline import run_aat
from .x_analyst import XAnalyst, analyze_x_posts

__all__ = [
    "Signal",
    "AgentSummary",
    "MeetingResult",
    "PlatformSummary",
    "FinalReport",
    "ModelConfig",
    "run_aat",
    "XAnalyst",
    "analyze_x_posts",
]
