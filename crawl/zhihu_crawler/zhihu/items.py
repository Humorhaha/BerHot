"""
知乎爬虫数据模型
与现有 Twitter 数据结构对齐，确保 BERTopic 管道零改动接入
"""

import scrapy


class ZhihuAnswerItem(scrapy.Item):
    """一条知乎回答，字段对齐 twitter_crawler 输出格式"""
    # —— 核心标识 ——
    post_id = scrapy.Field()       # answer_id (str)
    author = scrapy.Field()        # 用户名
    author_token = scrapy.Field()  # url_token（知乎特有）
    date = scrapy.Field()          # created_time ISO 8601
    url = scrapy.Field()           # https://www.zhihu.com/question/{qid}/answer/{aid}

    # —— 内容 ——
    content_text = scrapy.Field()  # 纯文本（HTML已清洗）— pipeline 读取此字段
    content_html = scrapy.Field()  # 原始HTML（保留备用）
    excerpt = scrapy.Field()       # 知乎自带摘要

    # —— 元数据 ——
    question_id = scrapy.Field()
    question_title = scrapy.Field()
    voteup_count = scrapy.Field()
    comment_count = scrapy.Field()
    updated_time = scrapy.Field()

    # —— 预处理保留字段（由 Pipeline 填充）——
    clean_text = scrapy.Field()
    urls = scrapy.Field()
    lang = scrapy.Field()
    translated_text = scrapy.Field()


class ZhihuArticleItem(scrapy.Item):
    """一条知乎专栏文章"""
    post_id = scrapy.Field()
    author = scrapy.Field()
    author_token = scrapy.Field()
    date = scrapy.Field()
    url = scrapy.Field()
    content_text = scrapy.Field()
    content_html = scrapy.Field()
    title = scrapy.Field()
    voteup_count = scrapy.Field()
    comment_count = scrapy.Field()
    updated_time = scrapy.Field()
    clean_text = scrapy.Field()
    urls = scrapy.Field()
    lang = scrapy.Field()
    translated_text = scrapy.Field()
