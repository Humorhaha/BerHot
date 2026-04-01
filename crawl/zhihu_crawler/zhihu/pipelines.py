"""
知乎爬虫数据处理 Pipeline
- HTML 清洗 → 纯文本
- 文本清洗（URL 提取、Emoji 移除）
- 基于 post_id 去重
- BERTopic 格式导出
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from scrapy.exceptions import DropItem

# 添加项目根目录到路径，以便导入 utils.text_clean
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.text_clean import extract_urls, remove_emoji

from zhihu.items import ZhihuAnswerItem, ZhihuArticleItem
from zhihu.utils.text_clean_zhihu import html_to_text


class ZhihuCleanPipeline:
    """HTML 清洗 + 基础文本处理"""

    def process_item(self, item, spider=None):
        # HTML → 纯文本（如果还没有转换）
        if isinstance(item, (ZhihuAnswerItem, ZhihuArticleItem)):
            if item.get("content_html") and not item.get("content_text"):
                item["content_text"] = html_to_text(item["content_html"])
            
            # 复用现有 text_clean 工具进行进一步清洗
            if item.get("content_text"):
                # 提取 URL
                clean, urls = extract_urls(item["content_text"])
                # 移除 emoji
                clean = remove_emoji(clean)
                # 压缩空白
                clean = " ".join(clean.split())
                
                item["clean_text"] = clean.strip()
                item["urls"] = urls
            else:
                item["clean_text"] = ""
                item["urls"] = []
        
        return item


class ZhihuDeduplicationPipeline:
    """基于 post_id 去重"""

    def __init__(self):
        self.seen_ids: set[str] = set()

    def process_item(self, item, spider=None):
        post_id = item.get("post_id")
        if post_id:
            if post_id in self.seen_ids:
                raise DropItem(f"Duplicate item: post_id={post_id}")
            self.seen_ids.add(post_id)
        return item


class BERTopicExportPipeline:
    """输出为 BERTopic 管道兼容格式"""

    def __init__(self):
        self.items: list[dict] = []
        self._spider = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider=None):
        self.items = []
        if spider:
            self._spider = spider

    def process_item(self, item, spider=None):
        self.items.append(dict(item))
        if spider:
            self._spider = spider
        return item

    def close_spider(self, spider=None):
        s = spider or self._spider
        
        if not self.items:
            if s:
                s.logger.warning("No items collected, skipping export")
            return
        
        # 获取输出目录
        if s:
            output_dir = Path(s.settings.get("OUTPUT_DIR", "data"))
        else:
            output_dir = Path("data")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成时间戳
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        # 保存快照（带时间戳的历史版本）
        snapshot_path = output_dir / "snapshots" / f"zhihu_{timestamp}.json"
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 构建输出数据
        payload = {
            "site": "知乎 / Zhihu",
            "source": s.name if s else "unknown",
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "total_count": len(self.items),
            "texts": self.items,
        }
        
        # 保存快照
        with open(snapshot_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        
        if s:
            s.logger.info(f"Saved snapshot: {len(self.items)} items → {snapshot_path}")
        
        # 同时写入主数据文件（供 pipeline.py 直接读取）
        main_path = output_dir / "zhihu_texts.json"
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        
        if s:
            s.logger.info(f"Saved main data: {len(self.items)} items → {main_path}")
        
        # 同时写入 CSV 格式（方便查看）
        try:
            import csv
            csv_path = output_dir / f"zhihu_{timestamp}.csv"
            if self.items:
                headers = self.items[0].keys()
                with open(csv_path, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(self.items)
                if s:
                    s.logger.info(f"Saved CSV: {len(self.items)} items → {csv_path}")
        except Exception as e:
            if s:
                s.logger.warning(f"Failed to save CSV: {e}")
