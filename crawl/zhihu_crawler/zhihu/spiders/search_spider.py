"""
SearchSpider - 知乎搜索爬虫（免登录版）
通过搜索 API 获取内容，无需登录

可以获取：
- 搜索结果中的回答
- 搜索结果中的文章
"""

from pathlib import Path
from urllib.parse import quote

import scrapy

from zhihu.items import ZhihuAnswerItem, ZhihuArticleItem
from zhihu.utils.text_clean_zhihu import html_to_text


class SearchSpider(scrapy.Spider):
    """
    知乎搜索爬虫（免登录版）
    
    通过知乎搜索功能获取内容，完全不需要登录
    
    使用方法:
        scrapy crawl zhihu_search -a query="ChatGPT"
        scrapy crawl zhihu_search -a query="人工智能" -a max_results=50
    
    特点:
        - 完全免登录
        - 可以搜索任意关键词
        - 结果包含回答和文章
    
    限制:
        - 搜索结果可能不完整
        - 每页数量有限
        - 知乎可能限制搜索频率
    """
    
    name = "zhihu_search"
    
    custom_settings = {
        "DOWNLOAD_DELAY": 4,        # 搜索更保守
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "COOKIES_ENABLED": False,
        "RETRY_TIMES": 3,
    }
    
    # 知乎搜索 URL
    SEARCH_URL_TEMPLATE = "https://www.zhihu.com/search?type=content&q={query}"
    
    def __init__(self, query=None, max_results=20, **kwargs):
        super().__init__(**kwargs)
        
        self.query = query
        self.max_results = int(max_results)
        self.collected = 0
        
        if not self.query:
            self.logger.warning("No search query provided. Use -a query=...")
    
    def start_requests(self):
        """生成搜索请求"""
        if not self.query:
            return
        
        encoded_query = quote(self.query)
        url = self.SEARCH_URL_TEMPLATE.format(query=encoded_query)
        
        yield scrapy.Request(
            url,
            callback=self.parse_search_results,
            meta={"query": self.query},
            dont_filter=True,
        )
    
    def parse_search_results(self, response):
        """解析搜索结果"""
        if response.status == 403:
            self.logger.error("访问被限制 (403)，请增加延迟")
            return
        
        self.logger.info(f"搜索: {response.meta['query']}")
        
        # 搜索页面使用 JavaScript 渲染，内容在 script 标签中
        # 尝试从 initialState 中提取
        import json
        import re
        
        # 查找 initialState
        scripts = response.css("script::text").getall()
        data = None
        
        for script in scripts:
            # 查找 initialState
            if "initialState" in script or "INITIAL_STATE" in script:
                # 尝试提取 JSON
                matches = re.search(r'window\._?_INITIAL_STATE__?\s*=\s*({.+?});', script, re.DOTALL)
                if matches:
                    try:
                        data = json.loads(matches.group(1))
                        break
                    except:
                        continue
        
        if not data:
            # 尝试从页面直接提取内容卡片
            self.logger.warning("无法从页面提取 JSON 数据，尝试直接解析 HTML")
            yield from self._parse_html_cards(response)
            return
        
        # 从 initialState 提取搜索结果
        try:
            search_result = data.get("entities", {}).get("search", {})
            
            for key, item in search_result.items():
                if self.collected >= self.max_results:
                    break
                
                obj_type = item.get("object", {}).get("type", "")
                
                if obj_type == "answer":
                    answer_item = self._parse_search_answer(item)
                    if answer_item:
                        yield answer_item
                        self.collected += 1
                        
                elif obj_type == "article":
                    article_item = self._parse_search_article(item)
                    if article_item:
                        yield article_item
                        self.collected += 1
        
        except Exception as e:
            self.logger.error(f"解析搜索结果失败: {e}")
    
    def _parse_html_cards(self, response):
        """直接从 HTML 解析内容卡片（备用方案）"""
        # 搜索结果卡片
        cards = response.css(".SearchResult-Card, .ContentItem")
        
        for card in cards:
            if self.collected >= self.max_results:
                break
            
            # 判断是回答还是文章
            link = card.css("a::attr(href)").get("")
            
            if "/answer/" in link:
                item = self._parse_card_as_answer(card)
                if item:
                    yield item
                    self.collected += 1
            elif "/p/" in link:
                item = self._parse_card_as_article(card)
                if item:
                    yield item
                    self.collected += 1
    
    def _parse_search_answer(self, data: dict) -> ZhihuAnswerItem | None:
        """从搜索结果解析回答"""
        obj = data.get("object", {})
        
        item = ZhihuAnswerItem()
        
        item["post_id"] = str(obj.get("id", ""))
        
        # 作者
        author = obj.get("author", {})
        item["author"] = author.get("name", "匿名用户")
        item["author_token"] = author.get("url_token", "")
        
        # 问题信息
        question = obj.get("question", {})
        item["question_id"] = str(question.get("id", ""))
        item["question_title"] = question.get("title", "")
        
        # 内容
        content = obj.get("content", "")
        item["content_html"] = content
        item["content_text"] = html_to_text(content)
        item["excerpt"] = obj.get("excerpt", "")
        
        # 统计
        item["voteup_count"] = obj.get("voteup_count", 0)
        item["comment_count"] = obj.get("comment_count", 0)
        
        # 时间和链接
        item["date"] = obj.get("created_time", "")
        item["url"] = obj.get("url", "")
        
        # 预处理
        item["clean_text"] = ""
        item["urls"] = []
        item["lang"] = "zh"
        item["translated_text"] = ""
        
        return item
    
    def _parse_search_article(self, data: dict) -> ZhihuArticleItem | None:
        """从搜索结果解析文章"""
        obj = data.get("object", {})
        
        item = ZhihuArticleItem()
        
        item["post_id"] = str(obj.get("id", ""))
        
        # 作者
        author = obj.get("author", {})
        item["author"] = author.get("name", "匿名用户")
        item["author_token"] = author.get("url_token", "")
        
        # 标题和内容
        item["title"] = obj.get("title", "")
        content = obj.get("content", "")
        item["content_html"] = content
        item["content_text"] = html_to_text(content)
        
        # 统计
        item["voteup_count"] = obj.get("voteup_count", 0)
        item["comment_count"] = obj.get("comment_count", 0)
        
        # 时间和链接
        item["date"] = obj.get("created", "")
        item["url"] = obj.get("url", f"https://zhuanlan.zhihu.com/p/{item['post_id']}")
        
        # 预处理
        item["clean_text"] = ""
        item["urls"] = []
        item["lang"] = "zh"
        item["translated_text"] = ""
        
        return item
    
    def _parse_card_as_answer(self, card) -> ZhihuAnswerItem | None:
        """从 HTML 卡片解析回答（备用）"""
        item = ZhihuAnswerItem()
        
        # 提取信息
        link = card.css("a::attr(href)").get("")
        item["url"] = response.urljoin(link) if hasattr(self, 'response') else link
        
        # 尝试提取回答 ID
        import re
        match = re.search(r'/answer/(\d+)', link)
        item["post_id"] = match.group(1) if match else ""
        
        item["question_title"] = card.css("h2::text, .ContentItem-title::text").get("")
        
        # 作者
        item["author"] = card.css(".AuthorInfo-name::text").get("匿名用户")
        
        # 内容
        content = card.css(".RichContent-inner").get("")
        item["content_html"] = content
        item["content_text"] = html_to_text(content)
        
        # 统计
        voteup_text = card.css(".VoteButton--up::text").get("0")
        item["voteup_count"] = self._parse_count(voteup_text)
        
        # 预处理
        item["clean_text"] = ""
        item["urls"] = []
        item["lang"] = "zh"
        item["translated_text"] = ""
        item["excerpt"] = ""
        item["author_token"] = ""
        item["question_id"] = ""
        item["date"] = ""
        item["comment_count"] = 0
        
        return item
    
    def _parse_card_as_article(self, card) -> ZhihuArticleItem | None:
        """从 HTML 卡片解析文章（备用）"""
        item = ZhihuArticleItem()
        
        link = card.css("a::attr(href)").get("")
        item["url"] = response.urljoin(link) if hasattr(self, 'response') else link
        
        import re
        match = re.search(r'/p/(\d+)', link)
        item["post_id"] = match.group(1) if match else ""
        
        item["title"] = card.css("h2::text, .ContentItem-title::text").get("")
        item["author"] = card.css(".AuthorInfo-name::text").get("匿名用户")
        
        content = card.css(".RichContent-inner").get("")
        item["content_html"] = content
        item["content_text"] = html_to_text(content)
        
        voteup_text = card.css(".VoteButton--up::text").get("0")
        item["voteup_count"] = self._parse_count(voteup_text)
        
        item["clean_text"] = ""
        item["urls"] = []
        item["lang"] = "zh"
        item["translated_text"] = ""
        item["author_token"] = ""
        item["date"] = ""
        item["comment_count"] = 0
        
        return item
    
    def _parse_count(self, text: str) -> int:
        """解析数量"""
        if not text:
            return 0
        import re
        match = re.search(r'(\d+(?:\.\d+)?)', text)
        if not match:
            return 0
        return int(float(match.group(1)))
