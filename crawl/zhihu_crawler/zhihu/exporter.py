"""
知乎数据导出工具
复用统一的 payload / snapshot 导出逻辑，供 Scrapy 与 Firecrawl 共用。
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_payload(items: list[dict[str, Any]], source: str) -> dict[str, Any]:
    """构建 BERTopic 管道兼容的输出 payload。"""
    return {
        "site": "知乎 / Zhihu",
        "source": source,
        "source_meta": {
            "platform": "zhihu",
            "extractor": source,
            "targets": [],
        },
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_count": len(items),
        "texts": items,
    }


def export_payload(
    payload: dict[str, Any],
    output_dir: Path | str,
    *,
    timestamp: str | None = None,
    logger: Any | None = None,
    csv_prefix: str = "zhihu",
    main_filename: str = "zhihu_texts.json",
) -> dict[str, Path]:
    """
    导出主 JSON、时间戳快照和 CSV。

    Returns:
        各输出路径，键包括 snapshot/main/csv（csv 仅在成功时返回）。
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = timestamp or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    snapshot_path = out_dir / "snapshots" / f"{csv_prefix}_{ts}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    main_path = out_dir / main_filename
    with main_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    paths: dict[str, Path] = {
        "snapshot": snapshot_path,
        "main": main_path,
    }

    items = payload.get("texts", [])
    if items:
        try:
            csv_path = out_dir / f"{csv_prefix}_{ts}.csv"
            headers = list(items[0].keys())
            with csv_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(items)
            paths["csv"] = csv_path
        except Exception as exc:  # pragma: no cover - CSV 不是关键路径
            if logger:
                logger.warning(f"Failed to save CSV: {exc}")

    if logger:
        logger.info(f"Saved snapshot: {payload.get('total_count', 0)} items → {snapshot_path}")
        logger.info(f"Saved main data: {payload.get('total_count', 0)} items → {main_path}")
        if "csv" in paths:
            logger.info(f"Saved CSV: {payload.get('total_count', 0)} items → {paths['csv']}")

    return paths
