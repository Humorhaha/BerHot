"""
utils/text_clean.py — 推文文本清洗

职责（纯函数，无副作用，无网络调用）：
  - 提取并移除 URL
  - 移除 emoji
  - 组合输出 clean_text 与 urls 列表

注意：先提取 URL 再去 emoji，避免 emoji 出现在展开 URL 路径中被误删。
"""

from __future__ import annotations

import re

try:
    import emoji
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    emoji = None

# t.co 短链 + 通用 URL
_URL_RE = re.compile(r"https?://\S+", re.UNICODE)

# RT 前缀（"RT @username: "），清洗时保留该前缀，只清洗后面的内容
_RT_PREFIX_RE = re.compile(r"^(RT @\w+:\s*)")


def extract_urls(text: str) -> tuple[str, list[str]]:
    """
    从文本中提取所有 URL，返回 (去掉URL后的净文本, URL列表)。

    >>> extract_urls("Check this https://t.co/abc and https://t.co/xyz out")
    ('Check this  and  out', ['https://t.co/abc', 'https://t.co/xyz'])
    """
    urls = _URL_RE.findall(text)
    clean = _URL_RE.sub("", text)
    return clean, urls


def remove_emoji(text: str) -> str:
    """
    移除文本中的所有 emoji。

    使用 emoji 库的 replace_emoji，覆盖标准 emoji 及肤色修饰符等变体。

    >>> remove_emoji("Hello 🤖 world 🌍")
    'Hello  world '
    """
    if emoji is not None:
        return emoji.replace_emoji(text, replace="")

    # fallback: 覆盖常见 emoji / pictograph unicode block
    return re.sub(
        r"[\U0001F1E0-\U0001F1FF\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0000FE00-\U0000FE0F]",
        "",
        text,
    )


def clean_text(raw: str) -> tuple[str, list[str]]:
    """
    完整清洗流程：提取URL → 去emoji → 合并连续空白。

    对于 RT 推文，RT 前缀（"RT @username: "）被保留在 clean_text 中，
    只清洗前缀之后的正文部分，确保 rt_author 信息不丢失。

    Returns:
        (clean_text, urls)
        - clean_text: 去掉 URL 和 emoji 后的净文本，多余空白已压缩
        - urls: 原文中提取出的 URL 列表（保留顺序）

    >>> clean_text("RT @karpathy: Great paper 🔥 https://t.co/abc #AI")
    ('RT @karpathy: Great paper  #AI', ['https://t.co/abc'])
    """
    # 1. 提取 RT 前缀（保留），对正文部分单独处理
    m = _RT_PREFIX_RE.match(raw)
    if m:
        prefix = m.group(1)
        body = raw[m.end():]
    else:
        prefix = ""
        body = raw

    # 2. 提取 URL（在去 emoji 之前）
    body_no_url, urls = extract_urls(body)

    # 3. 去 emoji
    body_clean = remove_emoji(body_no_url)

    # 4. 压缩连续空白、去首尾空格
    body_clean = re.sub(r"[ \t]+", " ", body_clean).strip()
    result = (prefix + body_clean).strip()

    return result, urls
