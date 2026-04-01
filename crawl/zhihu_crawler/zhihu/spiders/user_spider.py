"""
UserSpider - 知乎用户内容爬虫
按用户 url_token 爬取回答和文章
"""

from pathlib import Path

import scrapy
import yaml

from zhihu.items import ZhihuAnswerItem, ZhihuArticleItem
from zhihu.utils.zhihu_parser import parse_answer, parse_article


class UserSpider(scrapy.Spider):
    """
    知乎用户内容爬虫
    
    爬取用户的回答和专栏文章。
    
    使用方法:
        # 指定用户 token
        scrapy crawl zhihu_user -a user_tokens="excited-vczh,kaiyuan-zhongwen"
        
        # 从 YAML 文件加载
        scrapy crawl zhihu_user -a users_file=config/users.yaml
        
        # 只爬回答
        scrapy crawl zhihu_user -a user_tokens="excited-vczh" -a crawl_answers=1 -a crawl_articles=0
    """
    
    name = "zhihu_user"
    
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }
    
    # API 端点模板
    USER_ANSWERS_API_TEMPLATE = (
        "https://www.zhihu.com/api/v4/members/{url_token}/answers"
        "?include=data[*].content,voteup_count,comment_count,"
        "created_time,updated_time,author.name,author.url_token,"
        "excerpt,question.title,question.id"
        "&limit=20&offset=0&sort_by=voteups"
    )
    
    USER_ARTICLES_API_TEMPLATE = (
        "https://www.zhihu.com/api/v4/members/{url_token}/articles"
        "?include=data[*].content,voteup_count,comment_count,"
        "created,updated,author.name,author.url_token,title"
        "&limit=20&offset=0&sort_by=voteups"
    )
    
    def __init__(
        self,
        users_file=None,
        user_tokens=None,
        crawl_answers=1,
        crawl_articles=1,
        max_per_user=None,
        **kwargs
    ):
        """
        初始化
        
        Args:
            users_file: YAML 配置文件路径
            user_tokens: 逗号分隔的用户 url_token 字符串
            crawl_answers: 是否爬取回答（1/0）
            crawl_articles: 是否爬取文章（1/0）
            max_per_user: 每个用户最多爬取的内容数
        """
        super().__init__(**kwargs)
        
        self.user_tokens: list[str] = []
        self.crawl_answers = int(crawl_answers) == 1
        self.crawl_articles = int(crawl_articles) == 1
        self.max_per_user = int(max_per_user) if max_per_user else None
        
        if user_tokens:
            self.user_tokens = [token.strip() for token in user_tokens.split(",") if token.strip()]
            self.logger.info(f"Loaded {len(self.user_tokens)} user tokens from argument")
        elif users_file:
            users_path = Path(users_file)
            if not users_path.is_absolute():
                users_path = Path(self.settings.get("CONFIG_DIR", "config")) / users_file
            
            try:
                with open(users_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, list):
                        self.user_tokens = [str(token) for token in data]
                    elif isinstance(data, dict) and "users" in data:
                        self.user_tokens = [str(token) for token in data["users"]]
                self.logger.info(f"Loaded {len(self.user_tokens)} user tokens from {users_path}")
            except Exception as e:
                self.logger.error(f"Failed to load users file: {e}")
        
        if not self.user_tokens:
            self.logger.warning("No user tokens provided. Use -a user_tokens=... or -a users_file=...")
    
    def start_requests(self):
        """生成初始请求"""
        for token in self.user_tokens:
            meta = {
                "user_token": token,
                "answer_count": 0,
                "article_count": 0,
            }
            
            if self.crawl_answers:
                url = self.USER_ANSWERS_API_TEMPLATE.format(url_token=token)
                yield scrapy.Request(
                    url,
                    callback=self.parse_user_answers,
                    meta={**meta, "type": "answers"},
                )
            
            if self.crawl_articles:
                url = self.USER_ARTICLES_API_TEMPLATE.format(url_token=token)
                yield scrapy.Request(
                    url,
                    callback=self.parse_user_articles,
                    meta={**meta, "type": "articles"},
                )
    
    def parse_user_answers(self, response):
        """解析用户回答"""
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return
        
        user_token = response.meta["user_token"]
        answer_count = response.meta.get("answer_count", 0)
        
        for answer_data in data.get("data", []):
            # 检查最大数量限制
            if self.max_per_user and answer_count >= self.max_per_user:
                self.logger.info(
                    f"Reached max_per_user limit ({self.max_per_user}) for answers of user {user_token}"
                )
                return
            
            # 从回答数据中提取问题信息
            question = answer_data.get("question", {})
            qid = str(question.get("id", ""))
            
            item = parse_answer(answer_data, qid)
            yield item
            answer_count += 1
        
        # 分页
        paging = data.get("paging", {})
        if not paging.get("is_end", True):
            next_url = paging.get("next")
            if next_url:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_user_answers,
                    meta={
                        "user_token": user_token,
                        "answer_count": answer_count,
                        "type": "answers",
                    },
                )
        else:
            self.logger.info(f"Finished crawling answers for user {user_token}, total: {answer_count}")
    
    def parse_user_articles(self, response):
        """解析用户文章"""
        try:
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return
        
        user_token = response.meta["user_token"]
        article_count = response.meta.get("article_count", 0)
        
        for article_data in data.get("data", []):
            # 检查最大数量限制
            if self.max_per_user and article_count >= self.max_per_user:
                self.logger.info(
                    f"Reached max_per_user limit ({self.max_per_user}) for articles of user {user_token}"
                )
                return
            
            item = parse_article(article_data)
            yield item
            article_count += 1
        
        # 分页
        paging = data.get("paging", {})
        if not paging.get("is_end", True):
            next_url = paging.get("next")
            if next_url:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_user_articles,
                    meta={
                        "user_token": user_token,
                        "article_count": article_count,
                        "type": "articles",
                    },
                )
        else:
            self.logger.info(f"Finished crawling articles for user {user_token}, total: {article_count}")
