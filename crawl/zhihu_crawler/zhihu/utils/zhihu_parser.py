"""
知乎 API JSON 解析器
将知乎 API 返回的原始数据解析为 Item 对象
"""

import re
from datetime import datetime, timezone
from typing import Any

from zhihu.items import ZhihuAnswerItem, ZhihuArticleItem
from zhihu.utils.text_clean_zhihu import html_to_text, clean_zhihu_excerpt


def parse_timestamp(ts: int | float | None) -> str:
    """
    将知乎时间戳转换为 ISO 8601 格式
    知乎时间戳是 Unix 时间戳（秒）
    """
    if not ts:
        return ""
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, TypeError):
        return ""


def parse_answer(data: dict[str, Any], question_id: str | None = None) -> ZhihuAnswerItem:
    """
    解析知乎回答数据
    
    Args:
        data: 知乎 API 返回的 answer 对象
        question_id: 问题ID（如果从问题列表获取）
        
    Returns:
        ZhihuAnswerItem
    """
    item = ZhihuAnswerItem()
    
    # 基础信息
    item["post_id"] = str(data.get("id", ""))
    
    # 作者信息
    author = data.get("author", {})
    item["author"] = author.get("name", "匿名用户")
    item["author_token"] = author.get("url_token", "")
    
    # 时间
    item["date"] = parse_timestamp(data.get("created_time"))
    item["updated_time"] = parse_timestamp(data.get("updated_time"))
    
    # 链接
    aid = item["post_id"]
    qid = question_id or data.get("question", {}).get("id", "")
    item["url"] = f"https://www.zhihu.com/question/{qid}/answer/{aid}" if qid and aid else ""
    
    # 内容处理
    content_html = data.get("content", "")
    # 如果 content 为空（匿名访问限制），尝试使用 excerpt
    if not content_html or content_html.strip() == "":
        excerpt = data.get("excerpt", "")
        item["content_html"] = excerpt
        item["content_text"] = clean_zhihu_excerpt(excerpt)
    else:
        item["content_html"] = content_html
        item["content_text"] = html_to_text(content_html)
    
    item["excerpt"] = clean_zhihu_excerpt(data.get("excerpt", ""))
    
    # 问题信息
    question = data.get("question", {})
    item["question_id"] = str(question.get("id", "")) if isinstance(question, dict) else str(qid)
    item["question_title"] = question.get("title", "") if isinstance(question, dict) else ""
    
    # 统计信息
    item["voteup_count"] = data.get("voteup_count", 0) or data.get("votes_up", 0) or 0
    item["comment_count"] = data.get("comment_count", 0) or 0
    
    # 预处理字段（由 Pipeline 填充）
    item["clean_text"] = ""
    item["urls"] = []
    item["lang"] = "zh"
    item["translated_text"] = ""
    
    return item


def parse_article(data: dict[str, Any]) -> ZhihuArticleItem:
    """
    解析知乎专栏文章数据
    
    Args:
        data: 知乎 API 返回的 article 对象
        
    Returns:
        ZhihuArticleItem
    """
    item = ZhihuArticleItem()
    
    # 基础信息
    item["post_id"] = str(data.get("id", ""))
    
    # 作者信息
    author = data.get("author", {})
    item["author"] = author.get("name", "匿名用户")
    item["author_token"] = author.get("url_token", "")
    
    # 时间（文章使用 'created' 字段）
    item["date"] = parse_timestamp(data.get("created"))
    item["updated_time"] = parse_timestamp(data.get("updated"))
    
    # 链接
    item["url"] = data.get("url", f"https://zhuanlan.zhihu.com/p/{item['post_id']}")
    
    # 内容处理
    content_html = data.get("content", "")
    item["content_html"] = content_html
    item["content_text"] = html_to_text(content_html)
    
    # 标题
    item["title"] = data.get("title", "")
    
    # 统计信息
    item["voteup_count"] = data.get("voteup_count", 0) or data.get("votes_up", 0) or 0
    item["comment_count"] = data.get("comment_count", 0) or 0
    
    # 预处理字段
    item["clean_text"] = ""
    item["urls"] = []
    item["lang"] = "zh"
    item["translated_text"] = ""
    
    return item


def extract_question_id_from_url(url: str) -> str | None:
    """
    从知乎问题 URL 中提取问题 ID
    
    支持的格式:
    - https://www.zhihu.com/question/123456
    - https://www.zhihu.com/question/123456/answer/789
    """
    if not url:
        return None
    
    patterns = [
        r'zhihu\.com/question/(\d+)',
        r'zhihu\.com/questions/(\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def extract_topic_id_from_url(url: str) -> str | None:
    """
    从知乎话题 URL 中提取话题 ID
    
    支持的格式:
    - https://www.zhihu.com/topic/19554298
    """
    if not url:
        return None
    
    match = re.search(r'zhihu\.com/topic/(\d+)', url)
    if match:
        return match.group(1)
    
    return None


def extract_user_token_from_url(url: str) -> str | None:
    """
    从知乎用户主页 URL 中提取用户 url_token
    
    支持的格式:
    - https://www.zhihu.com/people/excited-vczh
    - https://www.zhihu.com/people/excited-vczh/answers
    """
    if not url:
        return None
    
    # 移除查询参数
    url = url.split('?')[0]
    
    # 匹配 /people/{token} 格式
    match = re.search(r'zhihu\.com/people/([^/]+)', url)
    if match:
        return match.group(1)
    
    return None
