"""
run_aat.py — AgentAttentionTree CLI 入口
=========================================

用法示例：

  # 分析 AI 监管话题（默认加载最近 5 个快照）
  uv run python run_aat.py --query "AI监管政策的公众态度"

  # 指定平台和窗口
  uv run python run_aat.py --query "大模型竞争格局" --platforms twitter zhihu --window 3

  # 使用更强的模型
  uv run python run_aat.py --query "AI安全争论" --model gpt-4o

  # 控制并发和输出目录
  uv run python run_aat.py --query "开源vs闭源" --max-concurrent 5 --save-dir data/aat_reports/test_run

  # 使用兼容 OpenAI API 的自定义端点（如 DeepSeek）
  uv run python run_aat.py --query "大模型评测" --base-url https://api.deepseek.com/v1 --model deepseek-chat
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
    parser.add_argument(
        "--model",
        default="gpt-4o-mini",
        help="OpenAI 模型（默认：gpt-4o-mini，推荐节约成本）",
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

    report = await run_aat(
        query_anchor=args.query,
        platforms=args.platforms,
        snapshot_dir=args.snapshot_dir,
        window=args.window,
        model=args.model,
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
