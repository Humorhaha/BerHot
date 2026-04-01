"""
单点测试：elonmusk 推文采集

运行:
    uv run pytest tests/test_twitter_crawler.py -v
    
需要设置环境变量:
    export TWTAPI_KEY=your_api_key
"""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(Path(__file__).parent.parent / ".env")

from twitter_crawler import (
    API_BASE,
    get_user_id,
    get_tweets,
    fetch_user_tweets,
    build_payload,
)
from utils.tweet_parser import _parse_entry

# 测试配置
TEST_USERNAME = "elonmusk"
TEST_API_KEY = os.getenv("TWTAPI_KEY") or os.getenv("TWT_API_KEY")


@pytest.fixture(scope="module")
def api_key():
    """获取 API Key，跳过测试如果未设置。"""
    key = os.getenv("TWTAPI_KEY") or os.getenv("TWT_API_KEY")
    if not key:
        pytest.skip("未设置 TWTAPI_KEY 环境变量")
    return key


class TestGetUserId:
    """测试用户名转 user_id。"""

    def test_get_user_id_success(self, api_key):
        """正常获取 elonmusk 的 user_id。"""
        user_id = get_user_id(TEST_USERNAME, api_key)
        
        assert user_id is not None
        assert user_id.isdigit()
        assert len(user_id) > 5  # 数字 ID 通常较长
        print(f"\n{TEST_USERNAME} -> user_id: {user_id}")

    def test_get_user_id_not_found(self, api_key):
        """测试不存在的用户名。"""
        user_id = get_user_id("this_user_definitely_not_exist_12345", api_key)
        assert user_id is None


class TestGetTweets:
    """测试获取推文接口。"""

    @pytest.fixture(scope="class")
    def elon_user_id(self, api_key):
        """获取 elonmusk 的 user_id。"""
        return get_user_id(TEST_USERNAME, api_key)

    def test_get_tweets_success(self, api_key, elon_user_id):
        """正常获取推文列表。"""
        data = get_tweets(elon_user_id, api_key)
        
        assert isinstance(data, dict)
        # 检查是否有 timeline 字段
        assert "timeline" in data or "data" in data or "tweets" in data
        
        timeline = data.get("timeline", [])
        print(f"\n获取到 {len(timeline)} 条推文")
        
        if timeline:
            first = timeline[0]
            print(f"第一条推文 ID: {first.get('id')}")
            print(f"第一条推文内容: {first.get('text', '')[:100]}...")

    def test_get_tweets_with_cursor(self, api_key, elon_user_id):
        """测试分页游标。"""
        # 第一页
        data1 = get_tweets(elon_user_id, api_key)
        timeline1 = data1.get("timeline", [])
        
        cursor = data1.get("cursor") or data1.get("next_cursor")
        print(f"\n第一页推文数: {len(timeline1)}, cursor: {cursor}")
        
        if not cursor:
            pytest.skip("API 未返回 cursor（可能只有一页数据）")
        
        # 第二页
        data2 = get_tweets(elon_user_id, api_key, cursor=cursor)
        timeline2 = data2.get("timeline", [])
        print(f"第二页推文数: {len(timeline2)}")
        
        # 两页内容应该不同（比较第一条推文 ID）
        if timeline1 and timeline2:
            id1 = str(timeline1[0].get("id", ""))
            id2 = str(timeline2[0].get("id", ""))
            assert id1 != id2, "分页返回相同推文"
        else:
            # 有一页为空也是合理的（最后一页）
            pass


class TestParseEntry:
    """测试推文解析（内部函数）。"""

    def test_parse_entry_normal(self):
        """解析正常推文 entry。"""
        # 构建模拟的 entry 结构
        raw = {
            "content": {
                "__typename": "TimelineTimelineItem",
                "content": {
                    "__typename": "TimelineTweet",
                    "tweet_results": {
                        "result": {
                            "__typename": "Tweet",
                            "rest_id": "1234567890",
                            "core": {
                                "user_results": {
                                    "result": {
                                        "core": {
                                            "screen_name": "testuser"
                                        }
                                    }
                                }
                            },
                            "legacy": {
                                "id_str": "1234567890",
                                "full_text": "This is a test tweet about AI and machine learning.",
                                "created_at": "Mon Mar 30 10:00:00 +0000 2026",
                                "retweet_count": 100,
                                "favorite_count": 500,
                            }
                        }
                    }
                }
            }
        }
        
        parsed = _parse_entry(raw)
        
        assert parsed is not None
        assert parsed["post_id"] == "1234567890"
        assert parsed["author"] == "testuser"
        assert "AI" in parsed["content_text"]
        assert parsed["retweet_count"] == 100
        assert parsed["like_count"] == 500
        assert parsed["url"] == "https://x.com/testuser/status/1234567890"
        print(f"\n解析结果: {parsed}")

    def test_parse_entry_empty_text(self):
        """空文本应返回 None。"""
        raw = {
            "content": {
                "__typename": "TimelineTimelineItem",
                "content": {
                    "__typename": "TimelineTweet",
                    "tweet_results": {
                        "result": {
                            "legacy": {
                                "full_text": "   ",
                                "created_at": "Mon Mar 30 10:00:00 +0000 2026",
                            }
                        }
                    }
                }
            }
        }
        parsed = _parse_entry(raw)
        assert parsed is None

    def test_parse_entry_non_tweet_type(self):
        """非推文类型应返回 None。"""
        raw = {
            "content": {
                "__typename": "TimelineTimelineCursor",  # 不是 TimelineTimelineItem
            }
        }
        parsed = _parse_entry(raw)
        assert parsed is None


class TestFetchUserTweets:
    """测试完整采集流程。"""

    def test_fetch_user_tweets(self, api_key):
        """采集 elonmusk 的推文。"""
        max_tweets = 10  # 单点测试只采 10 条
        
        tweets = fetch_user_tweets(TEST_USERNAME, api_key, max_tweets)
        
        assert isinstance(tweets, list)
        assert len(tweets) <= max_tweets
        
        if tweets:
            print(f"\n成功采集 {len(tweets)} 条推文")
            
            # 检查字段完整性
            first = tweets[0]
            required_fields = ["post_id", "author", "date", "content_text", "url"]
            for field in required_fields:
                assert field in first, f"缺少字段: {field}"
            
            # 检查内容
            assert first["author"] == TEST_USERNAME
            assert len(first["content_text"]) > 0
            print(f"第一条: {first['content_text'][:80]}...")
        else:
            print("\n未采集到推文（可能账号无推文或 API 限制）")


class TestBuildPayload:
    """测试 payload 构建。"""

    def test_build_payload(self):
        """构建 pipeline 兼容的 payload。"""
        tweets = [
            {
                "post_id": "1",
                "author": "user1",
                "date": "2026-03-30T10:00:00+00:00",
                "content_text": "Test tweet",
                "url": "https://x.com/user1/status/1",
                "retweet_count": 10,
                "like_count": 50,
            }
        ]
        usernames = ["user1", "user2"]
        
        payload = build_payload(tweets, usernames)
        
        assert payload["site"] == "X / Twitter"
        assert payload["source_accounts"] == usernames
        assert payload["total_count"] == 1
        assert len(payload["texts"]) == 1
        assert "fetched_at_utc" in payload
        print(f"\nPayload 结构: {list(payload.keys())}")


class TestDateFilter:
    """测试日期过滤功能。"""

    def test_fetch_with_days_ago(self, api_key):
        """测试只采集最近N天的数据。"""
        from datetime import datetime, timedelta, timezone
        
        # 采集最近2天的数据
        days = 2
        max_tweets = 20
        
        tweets = fetch_user_tweets(TEST_USERNAME, api_key, max_tweets, days_ago=days)
        
        assert isinstance(tweets, list)
        
        # 计算时间阈值
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        # 验证所有推文都在时间范围内
        for tweet in tweets:
            tweet_date = datetime.fromisoformat(tweet["date"].replace("Z", "+00:00"))
            assert tweet_date >= cutoff, f"推文时间 {tweet_date} 早于阈值 {cutoff}"
        
        print(f"\n✅ 成功采集 {len(tweets)} 条推文，全部在{days}天范围内")


if __name__ == "__main__":
    # 允许直接运行: python tests/test_twitter_crawler.py
    pytest.main([__file__, "-v", "-s"])
