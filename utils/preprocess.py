"""
utils/preprocess.py — 推文二次处理（second_pass）

职责：
  在爬取+解析之后，对推文列表做第二次遍历：
    1. 填入 clean_text 和 urls（调用 text_clean）
    2. 语言检测（在 clean_text 上，不在原始文本上）
    3. 按类型+语言差异化过滤极短文本
    4. 非英文 original 推文翻译（填 translated_text，不覆盖 content_text）
    5. 统计并输出 URL 频次 top-20

分类后的推文通过 tweet_type 字段供下游差异化处理：
  "original"  — 原创推文，内容完整，语言已检测，需要时有翻译
  "retweet"   — 转推，内容完整（截断已过滤），不翻译
  "reply"     — 回复，与 original 同等处理
"""

from __future__ import annotations

from collections import Counter

from langdetect import detect, LangDetectException
from loguru import logger

from utils.text_clean import clean_text


# 过滤阈值：clean_text 词数低于此值的推文被丢弃
# original/reply 阈值较宽松；retweet 完整内容普遍较长，阈值可更严
MIN_WORDS_ORIGINAL = 5
MIN_WORDS_RETWEET = 5

# URL 统计输出数量
TOP_URLS_COUNT = 20



def detect_lang(text: str) -> str:
    """
    检测文本语言，返回 ISO 639-1 代码（如 "en", "zh-cn", "ja"）。
    若是混合语言, 返回概率最高的语言

    必须在 clean_text（去URL、去emoji后）上调用，否则 URL 域名会干扰检测。
    极短文本（<3词）检测不可靠，直接返回 "und"（undetermined）。
    """
    if len(text.split()) < 3:
        return "und"
    try:
        return detect(text)
    except LangDetectException:
        return "und"



def _translate_to_english(text: str, openai_client) -> str | None:
    """
    用 OpenAI 将文本翻译为英文，失败返回 None。

    仅被 second_pass 内部调用，不对外暴露。
    调用者需保证 openai_client 不为 None。
    """
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Translate the following text to English. "
                        "Output only the translated text, no explanation."
                    ),
                },
                {"role": "user", "content": text},
            ],
            temperature=0,
            max_tokens=512,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"translation failed: {e}")
        return None



def second_pass(
    tweets: list[dict],
    translate: bool = False,
    openai_client=None,
) -> list[dict]:
    """
    对推文列表做二次处理，原地填入字段并返回过滤后的新列表。

    Args:
        tweets:        爬取+解析后的推文列表（必须含 tweet_type / is_truncated）
        translate:     是否对非英文 original 推文调用翻译
        openai_client: translate=True 时必须传入，OpenAI client 实例

    Returns:
        过滤后的推文列表（每条推文的 clean_text / urls / lang / translated_text 已填入）

    数据契约检查：
        每条输入推文必须已包含 tweet_type 和 is_truncated 字段，
        否则 assert 失败，说明 parse 阶段未完成。
    """
    for t in tweets:
        assert "tweet_type" in t, \
            f"post_id={t.get('post_id')} missing tweet_type — parse stage incomplete"
        assert "is_truncated" in t, \
            f"post_id={t.get('post_id')} missing is_truncated — parse stage incomplete"
        assert isinstance(t["is_truncated"], bool), \
            f"post_id={t.get('post_id')} is_truncated must be bool"

    kept: list[dict] = []
    url_counter: Counter = Counter()

    stats = {"truncated_rt": 0, "too_short": 0, "translated": 0, "kept": 0}

    for t in tweets:
        tweet_type = t["tweet_type"]

        if t["is_truncated"]:
            stats["truncated_rt"] += 1
            continue

        ct, urls = clean_text(t["content_text"])
        t["clean_text"] = ct
        t["urls"] = urls
        url_counter.update(urls)

        word_count = len(ct.split())
        threshold = (
            MIN_WORDS_RETWEET if tweet_type == "retweet" else MIN_WORDS_ORIGINAL
        )
        if word_count < threshold:
            stats["too_short"] += 1
            continue

        t["lang"] = detect_lang(ct)

        if (
            translate
            and openai_client is not None
            and t["lang"] not in ("en", "und")
            and tweet_type in ("original", "reply")
        ):
            translated = _translate_to_english(ct, openai_client)
            if translated:
                t["translated_text"] = translated
                stats["translated"] += 1

        kept.append(t)
        stats["kept"] += 1

    logger.info(
        f"second_pass done | "
        f"input={len(tweets)} kept={stats['kept']} "
        f"dropped_truncated={stats['truncated_rt']} "
        f"dropped_short={stats['too_short']} "
        f"translated={stats['translated']}"
    )
    if url_counter:
        top = url_counter.most_common(TOP_URLS_COUNT)
        logger.info(f"top-{min(TOP_URLS_COUNT, len(top))} URLs by frequency:")
        for url, count in top:
            logger.info(f"  {count:>4}x  {url}")

    return kept
