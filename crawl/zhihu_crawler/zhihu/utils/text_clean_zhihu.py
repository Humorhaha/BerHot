"""
知乎特有文本清洗工具
HTML → 纯文本，处理知乎特有的格式（LaTeX、代码块等）
"""

import re
from html import unescape


def html_to_text(html_content: str) -> str:
    """
    知乎回答 HTML → 纯文本
    
    Args:
        html_content: 知乎 API 返回的 HTML 格式内容
        
    Returns:
        清洗后的纯文本
    """
    if not html_content:
        return ""
    
    text = html_content
    
    # 1. 移除 noscript 标签及其内容（通常包含图片懒加载代码）
    text = re.sub(r'<noscript>.*?</noscript>', '', text, flags=re.DOTALL)
    
    # 2. 移除图片标签（保留 alt 文本）
    text = re.sub(r'<img[^>]*alt="([^"]*)"[^>]*>', r' \1 ', text)
    text = re.sub(r'<img[^>]*>', '', text)
    
    # 3. 链接 → 保留文本，移除标签
    text = re.sub(r'<a[^>]*>(.*?)</a>', r'\1', text)
    
    # 4. 数学公式 LaTeX → [公式]
    text = re.sub(r'<span class="ztext-math"[^>]*>.*?</span>', ' [公式] ', text, flags=re.DOTALL)
    text = re.sub(r'\\\([^)]+\\\)', ' [公式] ', text)  # \( ... \)
    text = re.sub(r'\\\[[^\]]+\\\]', ' [公式] ', text)  # \[ ... \]
    
    # 5. 代码块 → [代码]
    text = re.sub(r'<pre[^>]*>.*?</pre>', ' [代码] ', text, flags=re.DOTALL)
    text = re.sub(r'<code[^>]*>.*?</code>', ' [代码] ', text, flags=re.DOTALL)
    
    # 6. 段落/换行处理
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<p[^>]*>', '', text)
    
    # 7. 列表处理
    text = re.sub(r'<li[^>]*>', '\n• ', text)
    text = re.sub(r'</li>', '', text)
    text = re.sub(r'<ul[^>]*>|</ul>|<ol[^>]*>|</ol>', '\n', text)
    
    # 8. 标题处理
    text = re.sub(r'<h[1-6][^>]*>', '\n', text)
    text = re.sub(r'</h[1-6]>', '\n', text)
    
    # 9. 移除所有剩余标签
    text = re.sub(r'<[^>]+>', '', text)
    
    # 10. HTML 实体解码
    text = unescape(text)
    
    # 11. 规范化空白
    text = re.sub(r'\n{3,}', '\n\n', text)  # 多个空行合并为两个
    text = re.sub(r'[ \t]+', ' ', text)     # 多个空格/制表符合并为一个
    
    # 12. 移除知乎特有的占位符
    text = re.sub(r'\[图片\]', ' [图片] ', text)
    text = re.sub(r'\[视频\]', ' [视频] ', text)
    
    return text.strip()


def clean_zhihu_excerpt(excerpt: str) -> str:
    """
    清洗知乎摘要（excerpt）
    摘要通常包含"...阅读全文"等后缀，需要清理
    """
    if not excerpt:
        return ""
    
    # 移除"...阅读全文"等后缀
    text = re.sub(r'…?\s*阅读全文\s*?$', '', excerpt)
    text = re.sub(r'…$', '', text)
    
    # 移除 HTML 标签
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    
    return text.strip()
