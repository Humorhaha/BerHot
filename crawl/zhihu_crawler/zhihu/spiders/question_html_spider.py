"""
QuestionHtmlSpider - 知乎问题回答爬虫（免登录版）
通过爬取公开 HTML 页面获取回答，无需登录

限制：
- 只能获取前 ~20-50 个回答（知乎页面懒加载）
- 速度较慢（HTML 解析比 API 慢）
- 需要处理反爬（IP 限制仍然存在）
"""

from pathlib import Path

import scrapy
import yaml
from scrapy.selector import Selector

from zhihu.items import ZhihuAnswerItem
from zhihu.utils.text_clean_zhihu import clean_zhihu_excerpt


class QuestionHtmlSpider(scrapy.Spider):
    """
    知乎问题回答爬虫（免登录 HTML 版）
    
    通过爬取公开 HTML 页面获取回答，完全不需要登录/Cookie
    
    使用方法:
        scrapy crawl zhihu_question_html -a question_ids="19551997,585901765"
        scrapy crawl zhihu_question_html -a questions_file=config/questions.yaml
    
    限制:
        - 每个问题只能获取前 20-50 个回答（知乎页面懒加载限制）
        - 速度比 API 慢
        - 知乎可能封 IP，建议使用代理
    """
    
    name = "zhihu_question_html"
    
    custom_settings = {
        "DOWNLOAD_DELAY": 5,        # 免登录需要更保守的延迟
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "COOKIES_ENABLED": False,   # 不需要 Cookie
        "RETRY_TIMES": 3,
    }
    
    # 知乎问题页面 URL 模板
    QUESTION_URL_TEMPLATE = "https://www.zhihu.com/question/{question_id}"
    
    def __init__(self, questions_file=None, question_ids=None, max_answers=None, **kwargs):
        super().__init__(**kwargs)
        
        self.question_ids: list[str] = []
        self.max_answers = int(max_answers) if max_answers else None
        
        if question_ids:
            self.question_ids = [qid.strip() for qid in question_ids.split(",") if qid.strip()]
            self.logger.info(f"Loaded {len(self.question_ids)} question IDs from argument")
        elif questions_file:
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
            self.logger.warning("No question IDs provided")
    
    def start_requests(self):
        """生成初始请求"""
        for qid in self.question_ids:
            url = self.QUESTION_URL_TEMPLATE.format(question_id=qid)
            yield scrapy.Request(
                url,
                callback=self.parse_question_page,
                meta={
                    "question_id": qid,
                    "answer_count": 0,
                },
                dont_filter=True,
            )
    
    def parse_question_page(self, response):
        """解析问题页面 HTML"""
        question_id = response.meta["question_id"]
        
        if response.status == 403:
            self.logger.error(f"访问被限制 (403)，请增加延迟或使用代理: {response.url}")
            return
        
        if response.status != 200:
            self.logger.error(f"请求失败: {response.status} - {response.url}")
            return
        
        # 提取问题标题
        question_title = self._extract_title(response)
        self.logger.info(f"问题 [{question_id}]: {question_title}")
        
        # 提取回答
        answers = response.css(".AnswerCard, .List-item, .ContentItem.AnswerItem")
        self.logger.info(f"找到 {len(answers)} 个回答")
        
        answer_count = response.meta.get("answer_count", 0)
        
        for answer in answers:
            # 检查最大数量限制
            if self.max_answers and answer_count >= self.max_answers:
                self.logger.info(f"Reached max_answers limit ({self.max_answers})")
                return
            
            item = self._parse_answer(answer, question_id, question_title, response.url)
            if item:
                yield item
                answer_count += 1
        
        self.logger.info(f"问题 [{question_id}] 已提取 {answer_count} 个回答")
        
        # 注意：知乎页面使用懒加载，无法通过分页获取更多回答
        # 如需获取更多，需要模拟滚动或使用 API
    
    def _extract_title(self, response) -> str:
        """提取问题标题"""
        # 尝试多种选择器
        selectors = [
            "h1.QuestionHeader-title::text",
            "h1::text",
            "meta[property='og:title']::attr(content)",
        ]
        for selector in selectors:
            title = response.css(selector).get("").strip()
            if title:
                # 移除知乎后缀
                title = title.replace(" - 知乎", "").strip()
                return title
        return ""
    
    def _parse_answer(self, selector, question_id: str, question_title: str, question_url: str) -> ZhihuAnswerItem | None:
        """解析单个回答"""
        item = ZhihuAnswerItem()
        
        # 提取回答 ID
        answer_id = selector.css("::attr(data-zop)").re_first(r'"itemId":\s*(\d+)')
        if not answer_id:
            # 尝试其他属性
            answer_id = selector.css("::attr(name)").get("")
        
        if not answer_id:
            return None
        
        item["post_id"] = str(answer_id)
        item["question_id"] = question_id
        item["question_title"] = question_title
        
        # 作者信息
        author_name = selector.css(".AuthorInfo-name::text").get("")
        if not author_name:
            author_name = selector.css("[itemprop='author'] [itemprop='name']::text").get("")
        item["author"] = author_name.strip() if author_name else "匿名用户"
        
        author_token = selector.css(".AuthorInfo-link::attr(href)").re_first(r"/people/([^/]+)")
        item["author_token"] = author_token or ""
        
        # 内容（HTML 中提取纯文本）
        content_html = selector.css(".RichContent-inner, .RichContent, .ContentItem-richText").get("")
        item["content_html"] = content_html
        item["content_text"] = self._extract_text_from_html(content_html)
        
        # 摘要
        excerpt = selector.css(".RichContent-inner .RichContent-collapsedText::text").get("")
        item["excerpt"] = clean_zhihu_excerpt(excerpt)
        
        # 赞同数
        voteup_text = selector.css(".VoteButton--up, .ContentItem-action[title*='赞同']").attrib.get("title", "")
        if not voteup_text:
            voteup_text = selector.css(".VoteButton--up::text").get("")
        item["voteup_count"] = self._parse_count(voteup_text)
        
        # 评论数
        comment_text = selector.css(".ContentItem-action[title*='评论']::text").get("")
        if not comment_text:
            comment_text = selector.css("button:contains('评论')::text").get("")
        item["comment_count"] = self._parse_count(comment_text)
        
        # 时间和 URL
        item["date"] = selector.css("time::attr(datetime)").get("")
        item["url"] = f"https://www.zhihu.com/question/{question_id}/answer/{item['post_id']}"
        
        # 预处理字段
        item["clean_text"] = ""
        item["urls"] = []
        item["lang"] = "zh"
        item["translated_text"] = ""
        
        return item
    
    def _extract_text_from_html(self, html: str) -> str:
        """从 HTML 提取纯文本"""
        if not html:
            return ""
        try:
            from html.parser import HTMLParser
            
            class TextExtractor(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.texts = []
                    self.skip_tags = {"script", "style", "noscript"}
                    self.in_skip = False
                
                def handle_starttag(self, tag, attrs):
                    if tag in self.skip_tags:
                        self.in_skip = True
                
                def handle_endtag(self, tag):
                    if tag in self.skip_tags:
                        self.in_skip = False
                
                def handle_data(self, data):
                    if not self.in_skip:
                        self.texts.append(data)
            
            extractor = TextExtractor()
            extractor.feed(html)
            text = " ".join(extractor.texts)
            # 清理空白
            text = " ".join(text.split())
            return text.strip()
        except:
            # 简单清理
            import re
            text = re.sub(r'<[^>]+>', '', html)
            return text.strip()
    
    def _parse_count(self, text: str) -> int:
        """解析数量文本"""
        if not text:
            return 0
        import re
        # 匹配数字
        match = re.search(r'(\d+(?:\.\d+)?)(?:\s*万)?', text)
        if not match:
            return 0
        
        num = float(match.group(1))
        # 处理 "万"
        if "万" in text:
            num *= 10000
        
        return int(num)
