"""
run_aat.py — AgentAttentionTree CLI 入口
=========================================

用法示例：

  # 默认配置（哑铃形：head=gpt-4o, 其余=gpt-4o-mini）
  uv run python run_aat.py --query "AI监管政策的公众态度"

  # 预算模式：全部 mini（测试用）
  uv run python run_aat.py --query "大模型竞争格局" --preset economy

  # 旗舰模式：head 和 batch 都用 gpt-4o
  uv run python run_aat.py --query "AI安全争论" --preset performance

  # 精细控制每层模型（覆盖 preset）
  uv run python run_aat.py --query "开源vs闭源" \\
      --head-model gpt-4o \\
      --batch-model gpt-4o \\
      --platform-model gpt-4o-mini \\
      --meeting-model gpt-4o-mini

  # 使用 DeepSeek 等兼容接口
  uv run python run_aat.py --query "大模型评测" \\
      --base-url https://api.deepseek.com/v1 \\
      --head-model deepseek-chat \\
      --batch-model deepseek-chat
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

load_dotenv(dotenv_path=PROJECT_DIR / ".env")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AgentAttentionTree — 多平台舆情分析（无传统ML）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--query",
        required=True,
        metavar="TOPIC",
        help="分析目标问题，例如：'AI监管政策的公众态度'",
    )
    parser.add_argument(
        "--platforms",
        nargs="+",
        default=["twitter", "zhihu"],
        choices=["twitter", "zhihu"],
        help="要分析的平台（默认：twitter zhihu）",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=5,
        metavar="N",
        help="每个平台最多加载最近 N 个快照（默认：5）",
    )
    # ── 分层模型配置 ──────────────────────────────────────────────────────────
    model_group = parser.add_argument_group(
        "分层模型配置",
        "各层 Agent 可独立指定模型。--preset 快捷设置全组，单独参数可覆盖 preset 的某一层。"
    )
    model_group.add_argument(
        "--preset",
        choices=["default", "economy", "performance"],
        default="default",
        help=(
            "default   = head:gpt-4o, 其余:gpt-4o-mini（哑铃形，推荐）\n"
            "economy   = 全部 gpt-4o-mini（节约成本/测试）\n"
            "performance = head+batch:gpt-4o, 其余:gpt-4o-mini（质量最优）"
        ),
    )
    model_group.add_argument("--head-model",     default=None, metavar="MODEL",
                             help="HeadAgent 模型（覆盖 preset）")
    model_group.add_argument("--platform-model", default=None, metavar="MODEL",
                             help="PlatformAgent 模型（覆盖 preset）")
    model_group.add_argument("--meeting-model",  default=None, metavar="MODEL",
                             help="Meeting 模型（覆盖 preset）")
    model_group.add_argument("--batch-model",    default=None, metavar="MODEL",
                             help="BatchAgent 模型（覆盖 preset）")
    # 兼容旧接口：--model 将所有层设为同一模型
    parser.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="（兼容旧接口）所有层使用同一模型，会被 --preset 和分层参数覆盖",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        metavar="KEY",
        help="OpenAI API Key（默认读取 OPENAI_API_KEY 环境变量）",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        metavar="URL",
        help="自定义 API 地址（兼容 OpenAI 协议的接口，如 DeepSeek）",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="快照目录（默认：data/snapshots/）",
    )
    parser.add_argument(
        "--save-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="保存报告和中间产物的目录（默认：data/aat_reports/run_TIMESTAMP/）",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=8,
        metavar="N",
        help="同时运行的最大 BatchAgent 数量（默认：8）",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="输出 DEBUG 级别日志",
    )

    return parser


async def _main(args: argparse.Namespace) -> None:
    from agent_tree.pipeline import run_aat
    from agent_tree.models import ModelConfig

    # ── 构建 ModelConfig ──
    # 优先级：单层参数 > --preset > --model（兼容旧接口）
    if args.model:
        cfg = ModelConfig.from_single(args.model)
    elif args.preset == "economy":
        cfg = ModelConfig.economy()
    elif args.preset == "performance":
        cfg = ModelConfig.performance()
    else:
        cfg = ModelConfig()  # default 哑铃形

    # 单层覆盖
    if args.head_model:     cfg.head_model     = args.head_model
    if args.platform_model: cfg.platform_model = args.platform_model
    if args.meeting_model:  cfg.meeting_model  = args.meeting_model
    if args.batch_model:    cfg.batch_model    = args.batch_model

    report = await run_aat(
        query_anchor=args.query,
        platforms=args.platforms,
        snapshot_dir=args.snapshot_dir,
        window=args.window,
        model_config=cfg,
        openai_api_key=args.api_key or os.getenv("OPENAI_API_KEY"),
        openai_base_url=args.base_url or os.getenv("OPENAI_BASE_URL"),
        save_dir=args.save_dir,
        max_concurrent_batches=args.max_concurrent,
    )

    print(f"\n报告已保存，置信度 {report.confidence:.0%}，共 {len(report.key_insights)} 条洞察")
    print(f"API 调用总计：{report.total_api_calls} 次")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # 日志级别
    logger.remove()
    level = "DEBUG" if args.verbose else "INFO"
    logger.add(sys.stderr, level=level, format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}")

    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"运行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
