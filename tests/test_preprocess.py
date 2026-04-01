"""
tests/test_preprocess.py — 预处理模块单元测试

覆盖范围：
  - utils/tweet_parser.py: classify_tweet, _parse_entry
  - utils/text_clean.py:   extract_urls, remove_emoji, clean_text
  - utils/preprocess.py:   detect_lang, second_pass
"""

import pytest
from utils.tweet_parser import classify_tweet, _parse_entry
from utils.text_clean import clean_text, extract_urls, remove_emoji
from utils.preprocess import detect_lang, second_pass


# ─── classify_tweet ───────────────────────────────────────────────────────────

class TestClassifyTweet:

    def test_original(self):
        t, rt_author, truncated = classify_tweet("Great paper on transformers.")
        assert t == "original"
        assert rt_author is None
        assert truncated is False

    def test_retweet_complete(self):
        t, rt_author, truncated = classify_tweet(
            "RT @karpathy: Scaling laws are fascinating."
        )
        assert t == "retweet"
        assert rt_author == "karpathy"
        assert truncated is False

    def test_retweet_truncated(self):
        t, rt_author, truncated = classify_tweet(
            "RT @karpathy: This is a very long tweet that gets cut off…"
        )
        assert t == "retweet"
        assert rt_author == "karpathy"
        assert truncated is True

    def test_reply(self):
        t, rt_author, truncated = classify_tweet("@sama interesting point!")
        assert t == "reply"
        assert rt_author is None
        assert truncated is False

    def test_rt_missing_author(self):
        # 非标准 RT 格式，无法解析作者
        t, rt_author, truncated = classify_tweet("RT some text without @ prefix")
        # 不以 "RT @" 开头，视为 original
        assert t == "original"


# ─── _parse_entry ─────────────────────────────────────────────────────────────

def _make_entry(full_text: str, screen_name: str = "testuser", tweet_id: str = "123") -> dict:
    """构建标准 GraphQL entry 结构。"""
    return {
        "content": {
            "__typename": "TimelineTimelineItem",
            "content": {
                "__typename": "TimelineTweet",
                "tweet_results": {
                    "result": {
                        "rest_id": tweet_id,
                        "core": {
                            "user_results": {
                                "result": {
                                    "core": {"screen_name": screen_name}
                                }
                            }
                        },
                        "legacy": {
                            "id_str": tweet_id,
                            "full_text": full_text,
                            "created_at": "Mon Mar 30 10:00:00 +0000 2026",
                            "retweet_count": 10,
                            "favorite_count": 50,
                        },
                    }
                },
            },
        }
    }


class TestParseEntry:

    def test_original_tweet(self):
        entry = _make_entry("This is a test tweet about AI.")
        parsed = _parse_entry(entry)
        assert parsed is not None
        assert parsed["tweet_type"] == "original"
        assert parsed["rt_author"] is None
        assert parsed["is_truncated"] is False
        assert parsed["clean_text"] is None    # 预留字段，parse 阶段不填
        assert parsed["urls"] == []
        assert parsed["lang"] is None

    def test_retweet_entry(self):
        entry = _make_entry("RT @researcher: Exciting new results!")
        parsed = _parse_entry(entry)
        assert parsed is not None
        assert parsed["tweet_type"] == "retweet"
        assert parsed["rt_author"] == "researcher"
        assert parsed["is_truncated"] is False

    def test_truncated_rt(self):
        entry = _make_entry("RT @researcher: This is a very long tweet…")
        parsed = _parse_entry(entry)
        assert parsed is not None
        assert parsed["is_truncated"] is True

    def test_empty_text_returns_none(self):
        entry = _make_entry("   ")
        assert _parse_entry(entry) is None

    def test_non_tweet_typename_returns_none(self):
        entry = {"content": {"__typename": "TimelineTimelineCursor"}}
        assert _parse_entry(entry) is None

    def test_required_fields_present(self):
        entry = _make_entry("Hello world from AI research.")
        parsed = _parse_entry(entry)
        required = ["post_id", "author", "date", "url", "content_text",
                    "retweet_count", "like_count", "tweet_type",
                    "rt_author", "is_truncated", "clean_text", "urls",
                    "lang", "translated_text"]
        for field in required:
            assert field in parsed, f"missing field: {field}"


# ─── text_clean ───────────────────────────────────────────────────────────────

class TestExtractUrls:

    def test_single_url(self):
        text, urls = extract_urls("Check https://t.co/abc out")
        assert urls == ["https://t.co/abc"]
        assert "https" not in text

    def test_multiple_urls(self):
        _, urls = extract_urls("https://t.co/a and https://t.co/b")
        assert len(urls) == 2

    def test_no_url(self):
        text, urls = extract_urls("No links here")
        assert urls == []
        assert text == "No links here"


class TestRemoveEmoji:

    def test_removes_emoji(self):
        result = remove_emoji("Hello 🔥 world 🤖")
        assert "🔥" not in result
        assert "🤖" not in result
        assert "Hello" in result

    def test_no_emoji(self):
        result = remove_emoji("Plain text")
        assert result == "Plain text"


class TestCleanText:

    def test_rt_prefix_preserved(self):
        raw = "RT @karpathy: Great paper 🔥 https://t.co/abc #AI"
        ct, urls = clean_text(raw)
        assert ct.startswith("RT @karpathy:")
        assert "🔥" not in ct
        assert urls == ["https://t.co/abc"]

    def test_pure_url_returns_empty(self):
        ct, urls = clean_text("https://t.co/RzNuclNfsR")
        assert ct == ""
        assert len(urls) == 1

    def test_whitespace_compressed(self):
        ct, _ = clean_text("Hello   world  test")
        assert "  " not in ct

    def test_original_with_emoji_and_url(self):
        raw = "New paper 🚀 on LLMs https://t.co/xyz check it out"
        ct, urls = clean_text(raw)
        assert "🚀" not in ct
        assert urls == ["https://t.co/xyz"]
        assert "New paper" in ct


# ─── preprocess ───────────────────────────────────────────────────────────────

class TestDetectLang:

    def test_english(self):
        lang = detect_lang("This is a well-written English sentence about machine learning.")
        assert lang == "en"

    def test_too_short_returns_und(self):
        lang = detect_lang("AI")
        assert lang == "und"


def _make_tweet(content_text: str, tweet_type: str = None,
                rt_author: str = None, is_truncated: bool = False,
                post_id: str = "1") -> dict:
    """构建 second_pass 输入格式的推文。"""
    if tweet_type is None:
        from utils.tweet_parser import classify_tweet
        tweet_type, rt_author, is_truncated = classify_tweet(content_text)
    return {
        "post_id": post_id,
        "author": "testuser",
        "date": "2026-03-30T10:00:00+00:00",
        "url": "",
        "content_text": content_text,
        "retweet_count": 0,
        "like_count": 0,
        "tweet_type": tweet_type,
        "rt_author": rt_author,
        "is_truncated": is_truncated,
        "clean_text": None,
        "urls": [],
        "lang": None,
        "translated_text": None,
    }


class TestSecondPass:

    def test_truncated_rt_dropped(self):
        tweets = [_make_tweet(
            "RT @user: This is a truncated tweet…",
            tweet_type="retweet", is_truncated=True
        )]
        result = second_pass(tweets)
        assert result == []

    def test_too_short_dropped(self):
        # 少于 5 词
        tweets = [_make_tweet("Hi there", tweet_type="original", is_truncated=False)]
        result = second_pass(tweets)
        assert result == []

    def test_valid_original_kept(self):
        tweets = [_make_tweet(
            "New supply chain attack for npm axios the most popular HTTP client library",
            tweet_type="original", is_truncated=False
        )]
        result = second_pass(tweets)
        assert len(result) == 1
        assert result[0]["clean_text"] is not None
        assert result[0]["lang"] is not None

    def test_clean_text_filled(self):
        raw = "Check this amazing paper 🔥 https://t.co/abc about scaling"
        tweets = [_make_tweet(raw, tweet_type="original", is_truncated=False)]
        result = second_pass(tweets)
        assert len(result) == 1
        assert "🔥" not in result[0]["clean_text"]
        assert "https://t.co/abc" in result[0]["urls"]

    def test_assert_missing_tweet_type(self):
        """上游未填 tweet_type 时 assert 应触发。"""
        bad = {
            "post_id": "1", "author": "x", "date": "", "url": "",
            "content_text": "test", "retweet_count": 0, "like_count": 0,
            # 故意缺少 tweet_type / is_truncated
        }
        with pytest.raises(AssertionError):
            second_pass([bad])

    def test_complete_rt_kept(self):
        tweets = [_make_tweet(
            "RT @karpathy: Great new results on scaling laws for transformers in NLP",
            tweet_type="retweet", is_truncated=False
        )]
        result = second_pass(tweets)
        assert len(result) == 1
        assert result[0]["tweet_type"] == "retweet"

    def test_url_counter_runs(self):
        """second_pass 不应因 URL 计数逻辑抛异常。"""
        tweets = [
            _make_tweet(
                "Paper https://t.co/abc on transformers is worth reading carefully",
                tweet_type="original", is_truncated=False, post_id="1"
            ),
            _make_tweet(
                "Another paper https://t.co/abc with the same link cited again here",
                tweet_type="original", is_truncated=False, post_id="2"
            ),
        ]
        result = second_pass(tweets)
        assert len(result) == 2
