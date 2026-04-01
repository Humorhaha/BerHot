"""
QuestionSpider - 知乎问题回答爬虫
按问题 ID 爬取所有回答
"""

from pathlib import Path

import scrapy
import yaml

from zhihu.items import ZhihuAnswerItem
from zhihu.utils.zhihu_parser import parse_answer


class QuestionSpider(scrapy.Spider):
    """
    知乎问题回答爬虫
    
    使用方法:
        # 直接指定问题 ID
        scrapy crawl zhihu_question -a question_ids="19551997,585901765"
        
        # 从 YAML 文件加载
        scrapy crawl zhihu_question -a questions_file=config/questions.yaml
        
        # 限制爬取数量
        scrapy crawl zhihu_question -a question_ids="19551997" -a max_answers=100
    """
    
    name = "zhihu_question"
    
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }
    
    # API 端点模板
    ANSWERS_API_TEMPLATE = (
        "https://www.zhihu.com/api/v4/questions/{question_id}/answers"
        "?include=data[*].content,voteup_count,comment_count,"
        "created_time,updated_time,author.name,author.url_token,excerpt,"
        "is_collapsed,collapse_reason,question.title,question.id"
        "&limit=20&offset=0&sort_by=default"
    )
    
    def __init__(self, questions_file=None, question_ids=None, max_answers=None, **kwargs):
        """
        初始化
        
        Args:
            questions_file: YAML 配置文件路径，包含问题 ID 列表
            question_ids: 逗号分隔的问题 ID 字符串
            max_answers: 每个问题最多爬取的回答数（默认 None 表示无限制）
        """
        super().__init__(**kwargs)
        
        self.question_ids: list[str] = []
        self.max_answers = int(max_answers) if max_answers else None
        
        if question_ids:
            # 直接从参数解析
            self.question_ids = [qid.strip() for qid in question_ids.split(",") if qid.strip()]
            self.logger.info(f"Loaded {len(self.question_ids)} question IDs from argument")
        elif questions_file:
            # 从 YAML 文件加载
            questions_path = Path(questions_file)
            if not questions_path.is_absolute():
                questions_path = Path(self.settings.get("CONFIG_DIR", "config")) / questions_file
            
            try:
                with open(questions_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, list):
                        self.question_ids = [str(qid) for qid in data]
                    elif isinstance(data, dict) and "questions" in data:
                        self.question_ids = [str(qid) for qid in data["questions"]]
                self.logger.info(f"Loaded {len(self.question_ids)} question IDs from {questions_path}")
            except Exception as e:
                self.logger.error(f"Failed to load questions file: {e}")
        
        if not self.question_ids:
            self.logger.warning("No question IDs provided. Use -a question_ids=... or -a questions_file=...")
    
    def start_requests(self):
        """生成初始请求"""
        for qid in self.question_ids:
            url = self.ANSWERS_API_TEMPLATE.format(question_id=qid)
            yield scrapy.Request(
                url,
                callback=self.parse_answers,
                meta={
                    "question_id": qid,
                    "answer_count": 0,
                },
            )
    
    def parse_answers(self, response):
        """解析回答列表"""
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return
        
        question_id = response.meta["question_id"]
        answer_count = response.meta.get("answer_count", 0)
        
        # 解析回答数据
        for answer_data in data.get("data", []):
            # 检查是否达到最大数量限制
            if self.max_answers and answer_count >= self.max_answers:
                self.logger.info(f"Reached max_answers limit ({self.max_answers}) for question {question_id}")
                return
            
            item = parse_answer(answer_data, question_id)
            yield item
            answer_count += 1
        
        # 自动分页
        paging = data.get("paging", {})
        if not paging.get("is_end", True):
            next_url = paging.get("next")
            if next_url:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_answers,
                    meta={
                        "question_id": question_id,
                        "answer_count": answer_count,
                    },
                )
        else:
            self.logger.info(f"Finished crawling question {question_id}, total answers: {answer_count}")
