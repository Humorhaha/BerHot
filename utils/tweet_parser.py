"""
utils/tweet_parser.py — Twitter GraphQL 响应解析层

职责：
  - 从 API 原始响应中提取推文结构（纯解析，无网络调用）
  - 分类推文类型（original / retweet / reply）
  - 标记 RT 截断状态

数据契约：_parse_entry() 返回的 dict 字段定义见 TWEET_SCHEMA。
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from loguru import logger

# ─── 数据 schema（文档用，非运行时校验）────────────────────────────────────────
# {
#   "post_id":        str,
#   "author":         str,           # 发推账号（RT 时是转发者）
#   "date":           str,           # ISO 8601 UTC
#   "url":            str,
#   "content_text":   str,           # 原始全文，永不修改
#   "retweet_count":  int,
#   "like_count":     int,
#   "tweet_type":     "original" | "retweet" | "reply",
#   "rt_author":      str | None,    # 仅 retweet，从 "RT @username:" 解析
#   "is_truncated":   bool,          # True = RT 修复失败，内容不完整
#   # 预留字段，由 preprocess.py 填入
#   "clean_text":     None,
#   "urls":           [],
#   "lang":           None,
#   "translated_text": None,
# }

_RT_PREFIX = re.compile(r"^RT @(\w+):")


def classify_tweet(text: str) -> tuple[str, str | None, bool]:
    """
    判断推文类型。

    Returns:
        (tweet_type, rt_author, is_truncated)
        - tweet_type: "original" | "retweet" | "reply"
        - rt_author:  被转推的用户名，仅 retweet 时有值
        - is_truncated: RT 内容是否被截断（以 … 结尾）
    """
    if text.startswith("RT @"):
        m = _RT_PREFIX.match(text)
        rt_author = m.group(1) if m else None
        return "retweet", rt_author, text.endswith("…")
    if text.startswith("@"):
        return "reply", None, False
    return "original", None, False


def _parse_entry(entry: dict) -> dict | None:
    """
    解析单个 GraphQL entry，提取推文数据。

    entry 结构：
      content.__typename == "TimelineTimelineItem"
      content.content.__typename == "TimelineTweet"
      content.content.tweet_results.result.legacy → 核心字段
    """
    try:
        content = entry.get("content", {})
        if content.get("__typename") != "TimelineTimelineItem":
            return None

        inner = content.get("content", {})
        if inner.get("__typename") != "TimelineTweet":
            return None

        result = inner.get("tweet_results", {}).get("result", {})
        legacy = result.get("legacy", {})
        if not legacy:
            return None

        # ── 提取文本（优先 note_tweet，兜底 legacy.full_text）──
        note = (
            result.get("note_tweet", {})
            .get("note_tweet_results", {})
            .get("result", {})
        )
        full_text = note.get("text", "").strip() or legacy.get("full_text", "").strip()
        if not full_text:
            return None

        # ── RT 截断修复 ──
        #  重复检查
        if full_text.startswith("RT @") and full_text.endswith("…"):
            rt_result = legacy.get("retweeted_status_results", {}).get("result", {})
            if rt_result:
                rt_note = (
                    rt_result.get("note_tweet", {})
                    .get("note_tweet_results", {})
                    .get("result", {})
                )
                rt_full = rt_note.get("text", "").strip() or \
                          rt_result.get("legacy", {}).get("full_text", "").strip()
                if rt_full:
                    rt_core = (
                        rt_result.get("core", {})
                        .get("user_results", {})
                        .get("result", {})
                        .get("core", {})
                    )
                    rt_author_name = rt_core.get("screen_name", "")
                    prefix = f"RT @{rt_author_name}: " if rt_author_name else "RT: "
                    full_text = prefix + rt_full

        # ── 用户信息 ──
        user_core = (
            result.get("core", {})
            .get("user_results", {})
            .get("result", {})
            .get("core", {})
        )
        screen_name = user_core.get("screen_name", "")

        # ── tweet_id ──
        tweet_id = str(legacy.get("id_str") or result.get("rest_id", ""))

        # ── 时间解析 ──
        created_at = legacy.get("created_at", "")
        try:
            dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
            date_iso = dt.replace(tzinfo=timezone.utc).isoformat()
        except (ValueError, TypeError):
            date_iso = created_at

        # ── 类型分类 ──
        tweet_type, rt_author, is_truncated = classify_tweet(full_text)

        return {
            "post_id":        tweet_id,
            "author":         screen_name,
            "date":           date_iso,
            "url":            f"https://x.com/{screen_name}/status/{tweet_id}"
                              if tweet_id and screen_name else "",
            "content_text":   full_text,
            "retweet_count":  legacy.get("retweet_count", 0),
            "like_count":     legacy.get("favorite_count", 0),
            # 类型字段
            "tweet_type":     tweet_type,
            "rt_author":      rt_author,
            "is_truncated":   is_truncated,
            # 预留（由 preprocess.py 填入）
            "clean_text":     None,
            "urls":           [],
            "lang":           None,
            "translated_text": None,
        }
    except Exception as e:
        logger.debug(f"parse entry failed: {e}")
        return None


def _extract_tweets_from_response(data: dict) -> list[dict]:
    """从 Twitter GraphQL 嵌套响应中提取推文列表。"""
    tweets: list[dict] = []
    try:
        timeline = (
            data.get("data", {})
            .get("user_result_by_rest_id", {})
            .get("result", {})
            .get("profile_timeline_v2", {})
            .get("timeline", {})
        )
        instructions = timeline.get("instructions", [])

        for instruction in instructions:
            typename = instruction.get("__typename", "")
            if typename == "TimelinePinEntry" and "entry" in instruction:
                tweet = _parse_entry(instruction["entry"])
                if tweet:
                    tweets.append(tweet)
            elif typename == "TimelineAddEntries" and "entries" in instruction:
                for entry in instruction["entries"]:
                    tweet = _parse_entry(entry)
                    if tweet:
                        tweets.append(tweet)

        # 调用侧断言：函数必须返回 list，绝不返回 None
        assert isinstance(tweets, list)
        return tweets
    except AssertionError:
        raise
    except Exception as e:
        logger.debug(f"extract tweets failed: {e}")
        return []


def _extract_cursor(data: dict) -> str | None:
    """从响应中提取下一页游标。"""
    try:
        instructions = (
            data.get("data", {})
            .get("user_result_by_rest_id", {})
            .get("result", {})
            .get("profile_timeline_v2", {})
            .get("timeline", {})
            .get("instructions", [])
        )
        for instruction in instructions:
            if instruction.get("__typename") == "TimelineAddEntries":
                for entry in reversed(instruction.get("entries", [])):
                    if "cursor" in entry.get("entry_id", "").lower():
                        content = entry.get("content", {})
                        if content.get("__typename") == "TimelineTimelineCursor":
                            return content.get("value")
        return None
    except Exception:
        return None
