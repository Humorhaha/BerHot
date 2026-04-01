"""
TopicSpider - 知乎话题爬虫
按话题 ID 爬取热门问题及其回答
"""

from pathlib import Path

import scrapy
import yaml

from zhihu.items import ZhihuAnswerItem
from zhihu.utils.zhihu_parser import parse_answer


class TopicSpider(scrapy.Spider):
    """
    知乎话题爬虫
    
    爬取话题下的热门内容，包括回答和文章。
    注意：话题 API 可能需要登录，匿名访问可能受限。
    
    使用方法:
        scrapy crawl zhihu_topic -a topic_ids="19554298,19556592"
        scrapy crawl zhihu_topic -a topics_file=config/topics.yaml
    """
    
    name = "zhihu_topic"
    
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }
    
    # API 端点模板
    TOPIC_FEED_API_TEMPLATE = (
        "https://www.zhihu.com/api/v4/topics/{topic_id}/feeds/top_activity"
        "?limit=10&offset=0"
    )
    
    ANSWERS_API_TEMPLATE = (
        "https://www.zhihu.com/api/v4/questions/{question_id}/answers"
        "?include=data[*].content,voteup_count,comment_count,"
        "created_time,updated_time,author.name,author.url_token,excerpt,"
        "is_collapsed,collapse_reason,question.title,question.id"
        "&limit=20&offset=0&sort_by=default"
    )
    
    def __init__(self, topics_file=None, topic_ids=None, max_items=None, **kwargs):
        """
        初始化
        
        Args:
            topics_file: YAML 配置文件路径，包含话题 ID 列表
            topic_ids: 逗号分隔的话题 ID 字符串
            max_items: 每个话题最多爬取的内容数
        """
        super().__init__(**kwargs)
        
        self.topic_ids: list[str] = []
        self.max_items = int(max_items) if max_items else None
        self._seen_questions: set[str] = set()  # 去重：已爬取的问题
        
        if topic_ids:
            self.topic_ids = [tid.strip() for tid in topic_ids.split(",") if tid.strip()]
            self.logger.info(f"Loaded {len(self.topic_ids)} topic IDs from argument")
        elif topics_file:
            topics_path = Path(topics_file)
            if not topics_path.is_absolute():
                topics_path = Path(self.settings.get("CONFIG_DIR", "config")) / topics_file
            
            try:
                with open(topics_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, list):
                        self.topic_ids = [str(tid) for tid in data]
                    elif isinstance(data, dict) and "topics" in data:
                        self.topic_ids = [str(tid) for tid in data["topics"]]
                self.logger.info(f"Loaded {len(self.topic_ids)} topic IDs from {topics_path}")
            except Exception as e:
                self.logger.error(f"Failed to load topics file: {e}")
        
        if not self.topic_ids:
            self.logger.warning("No topic IDs provided. Use -a topic_ids=... or -a topics_file=...")
    
    def start_requests(self):
        """生成初始请求"""
        for tid in self.topic_ids:
            url = self.TOPIC_FEED_API_TEMPLATE.format(topic_id=tid)
            yield scrapy.Request(
                url,
                callback=self.parse_topic_feed,
                meta={
                    "topic_id": tid,
                    "item_count": 0,
                },
            )
    
    def parse_topic_feed(self, response):
        """解析话题 Feed"""
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return
        
        topic_id = response.meta["topic_id"]
        item_count = response.meta.get("item_count", 0)
        
        for feed_item in data.get("data", []):
            # 检查是否达到最大数量限制
            if self.max_items and item_count >= self.max_items:
                self.logger.info(f"Reached max_items limit ({self.max_items}) for topic {topic_id}")
                return
            
            target = feed_item.get("target", {})
            target_type = target.get("type", "")
            
            if target_type == "answer":
                # 直接是回答
                question = target.get("question", {})
                qid = str(question.get("id", ""))
                item = parse_answer(target, qid)
                yield item
                item_count += 1
                
            elif target_type == "question":
                # 是问题，需要进一步爬取回答
                qid = str(target.get("id", ""))
                if qid and qid not in self._seen_questions:
                    self._seen_questions.add(qid)
                    url = self.ANSWERS_API_TEMPLATE.format(question_id=qid)
                    yield scrapy.Request(
                        url,
                        callback=self.parse_question_answers,
                        meta={
                            "topic_id": topic_id,
                            "question_id": qid,
                            "item_count": item_count,
                        },
                    )
                    item_count += 1
                    
            elif target_type == "article":
                # 专栏文章
                # TODO: 如果需要支持文章，可以在这里解析
                self.logger.debug(f"Skipping article: {target.get('title', '')}")
        
        # 分页
        paging = data.get("paging", {})
        if not paging.get("is_end", True):
            next_url = paging.get("next")
            if next_url:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_topic_feed,
                    meta={
                        "topic_id": topic_id,
                        "item_count": item_count,
                    },
                )
        else:
            self.logger.info(f"Finished crawling topic {topic_id}, total items: {item_count}")
    
    def parse_question_answers(self, response):
        """解析问题下的回答（简版，只取第一页）"""
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return
        
        topic_id = response.meta["topic_id"]
        question_id = response.meta["question_id"]
        
        # 只取前几页，避免单个问题占用过多配额
        answers = data.get("data", [])[:5]  # 每问题最多 5 个回答
        
        for answer_data in answers:
            item = parse_answer(answer_data, question_id)
            yield item
        
        self.logger.info(f"Collected {len(answers)} answers from question {question_id}")
