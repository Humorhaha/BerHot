"""
知乎爬虫数据处理 Pipeline
- HTML 清洗 → 纯文本
- 文本清洗（URL 提取、Emoji 移除）
- 基于 post_id 去重
- BERTopic 格式导出
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

from scrapy.exceptions import DropItem

# 添加项目根目录到路径，以便导入 utils.text_clean
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.text_clean import extract_urls, remove_emoji

from zhihu.exporter import build_payload, export_payload
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
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        payload = build_payload(self.items, s.name if s else "unknown")
        export_payload(payload, output_dir, timestamp=timestamp, logger=s.logger if s else None)
