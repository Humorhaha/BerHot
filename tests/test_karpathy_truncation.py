"""
测试 karpathy 推文采集，验证文本截断问题是否解决。

重点测试场景：
  1. 转推(RT)内容是否完整（不再被截断为140字符）
  2. 长推文(>280字符)是否完整
  3. 普通推文是否正常

运行:
    uv run pytest tests/test_karpathy_truncation.py -v -s
    
需要环境变量:
    export TWTAPI_KEY=your_api_key
"""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

# 加载环境变量
load_dotenv(Path(__file__).parent.parent / ".env")

from twitter_crawler import (
    get_user_id,
    get_tweets,
    _extract_tweets_from_response,
    _parse_entry,
    fetch_user_tweets,
)

# 测试配置
TEST_USERNAME = "karpathy"
TEST_API_KEY = os.getenv("TWTAPI_KEY") or os.getenv("TWT_API_KEY")

# 截断检测阈值（Twitter传统限制是140字符，新限制是280字符）
LEGACY_LIMIT = 140
NEW_LIMIT = 280


@pytest.fixture(scope="module")
def api_key():
    """获取 API Key，跳过测试如果未设置。"""
    key = os.getenv("TWTAPI_KEY") or os.getenv("TWT_API_KEY")
    if not key:
        pytest.skip("未设置 TWTAPI_KEY 或 TWT_API_KEY 环境变量")
    return key


@pytest.fixture(scope="module")
def karpathy_user_id(api_key):
    """获取 karpathy 的 user_id。"""
    user_id = get_user_id(TEST_USERNAME, api_key)
    if not user_id:
        pytest.skip(f"无法获取 {TEST_USERNAME} 的 user_id")
    return user_id


@pytest.fixture(scope="module")
def karpathy_tweets(api_key, karpathy_user_id):
    """获取 karpathy 的推文列表（最多50条用于分析）。"""
    return fetch_user_tweets(TEST_USERNAME, api_key, max_tweets=50)


class TestTruncation:
    """测试文本截断问题是否解决。"""

    def test_no_truncation_ellipsis(self, karpathy_tweets):
        """
        测试没有推文以'…'或'...'结尾（截断标志）。
        
        这是检测文本截断的关键指标：如果推文以'…'结尾，
        说明API返回了截断版本。
        """
        truncated_tweets = []
        
        for tweet in karpathy_tweets:
            content = tweet.get("content_text", "")
            # 检测以省略号结尾的推文（转推截断的典型特征）
            if content.endswith("…") or content.endswith("..."):
                truncated_tweets.append({
                    "post_id": tweet.get("post_id"),
                    "content_preview": content[:100] + "...",
                    "length": len(content),
                })
        
        # 断言：不应该有截断的推文
        assert len(truncated_tweets) == 0, (
            f"发现 {len(truncated_tweets)} 条可能截断的推文:\n"
            + "\n".join([f"  - {t['post_id']}: {t['content_preview']}" for t in truncated_tweets[:5]])
        )
        
        print(f"\n✅ 检查了 {len(karpathy_tweets)} 条推文，没有发现截断")

    def test_retweet_content_complete(self, karpathy_tweets):
        """
        测试转推(RT)内容是否完整。
        
        RT格式应该是: "RT @username: 完整推文内容"
        而不是: "RT @username: 截断内容…"
        """
        rt_tweets = [t for t in karpathy_tweets if t.get("content_text", "").startswith("RT @")]
        
        if not rt_tweets:
            pytest.skip("未找到转推内容")
        
        incomplete_rts = []
        
        for tweet in rt_tweets:
            content = tweet.get("content_text", "")
            # 检查是否以省略号结尾（截断标志）
            if content.endswith("…") or content.endswith("..."):
                incomplete_rts.append({
                    "post_id": tweet.get("post_id"),
                    "content_preview": content[:120] + "...",
                })
        
        # 断言：所有RT都应该是完整的
        assert len(incomplete_rts) == 0, (
            f"发现 {len(incomplete_rts)} 条不完整的转推:\n"
            + "\n".join([f"  - {rt['post_id']}: {rt['content_preview']}" for rt in incomplete_rts[:5]])
        )
        
        print(f"\n✅ 检查了 {len(rt_tweets)} 条转推，全部完整")
        # 打印一些RT示例
        for rt in rt_tweets[:3]:
            content = rt["content_text"][:100]
            print(f"   RT示例: {content}...")

    def test_retweet_length_distribution(self, karpathy_tweets):
        """
        分析转推内容长度分布。
        
        如果截断问题已修复，应该能看到：
        - 部分RT长度 > 140字符（原截断限制）
        - 平均长度应该比较合理
        """
        rt_tweets = [t for t in karpathy_tweets if t.get("content_text", "").startswith("RT @")]
        
        if not rt_tweets:
            pytest.skip("未找到转推内容")
        
        lengths = [len(t["content_text"]) for t in rt_tweets]
        avg_length = sum(lengths) / len(lengths)
        max_length = max(lengths)
        min_length = min(lengths)
        
        # 统计超过旧版限制(140)的RT数量
        over_legacy = sum(1 for l in lengths if l > LEGACY_LIMIT)
        
        print(f"\n📊 RT长度统计:")
        print(f"   总数: {len(rt_tweets)}")
        print(f"   平均长度: {avg_length:.1f} 字符")
        print(f"   最小长度: {min_length} 字符")
        print(f"   最大长度: {max_length} 字符")
        print(f"   超过140字符: {over_legacy} 条 ({over_legacy/len(rt_tweets)*100:.1f}%)")
        
        # 如果截断问题修复，应该至少有一些RT超过140字符
        # （当然，也可能所有RT都本来就短，所以这是警告而非错误）
        if over_legacy == 0:
            print("   ⚠️ 警告: 没有RT超过140字符（可能都是短推文，或仍有问题）")

    def test_long_tweets_preserved(self, karpathy_tweets):
        """
        测试长推文(>280字符)是否被正确保留。
        
        长推文应该使用 note_tweet 字段获取完整内容。
        """
        long_tweets = [t for t in karpathy_tweets if len(t.get("content_text", "")) > NEW_LIMIT]
        
        print(f"\n📜 发现 {len(long_tweets)} 条长推文(>{NEW_LIMIT}字符)")
        
        # 打印长推文示例
        for tweet in long_tweets[:3]:
            content = tweet["content_text"]
            print(f"   长推文({len(content)}字符): {content[:100]}...")

    def test_tweet_content_structure(self, karpathy_tweets):
        """
        测试推文内容结构是否正确。
        
        验证字段完整性和内容格式。
        """
        if not karpathy_tweets:
            pytest.skip("未采集到推文")
        
        required_fields = ["post_id", "author", "date", "content_text", "url"]
        
        for tweet in karpathy_tweets:
            # 检查必需字段
            for field in required_fields:
                assert field in tweet, f"推文缺少字段: {field}"
            
            # 检查内容非空
            assert len(tweet["content_text"].strip()) > 0, "推文内容为空"
            
            # 检查URL格式
            assert tweet["url"].startswith("https://x.com/"), "URL格式错误"
        
        print(f"\n✅ 所有 {len(karpathy_tweets)} 条推文结构正确")


class TestRawResponseParsing:
    """测试原始响应解析（更底层的测试）。"""

    def test_extract_from_raw_response(self, api_key, karpathy_user_id):
        """
        直接从原始响应提取推文，验证解析逻辑。
        
        这可以帮助调试解析问题。
        """
        data = get_tweets(karpathy_user_id, api_key)
        tweets = _extract_tweets_from_response(data)
        
        print(f"\n📥 从原始响应提取到 {len(tweets)} 条推文")
        
        # 检查是否有截断的
        truncated = [t for t in tweets if t.get("content_text", "").endswith(("…", "..."))]
        
        if truncated:
            print(f"   ⚠️ 发现 {len(truncated)} 条截断推文:")
            for t in truncated[:3]:
                print(f"      - {t['content_text'][:80]}...")
        else:
            print("   ✅ 没有发现截断推文")
        
        # 检查RT
        rts = [t for t in tweets if t.get("content_text", "").startswith("RT @")]
        print(f"   其中 {len(rts)} 条是转推")
        
        # 打印第一条推文详情
        if tweets:
            first = tweets[0]
            print(f"\n📄 第一条推文详情:")
            print(f"   ID: {first.get('post_id')}")
            print(f"   作者: {first.get('author')}")
            print(f"   长度: {len(first.get('content_text', ''))} 字符")
            print(f"   内容: {first.get('content_text', '')[:150]}...")


class TestSpecificTweets:
    """测试特定推文（如果存在）。"""

    def test_print_sample_tweets(self, karpathy_tweets):
        """
        打印一些推文样本用于人工检查。
        """
        if not karpathy_tweets:
            pytest.skip("未采集到推文")
        
        print(f"\n📝 karpathy 推文样本 (共 {len(karpathy_tweets)} 条):")
        print("=" * 80)
        
        # 分类统计
        normal_tweets = [t for t in karpathy_tweets if not t["content_text"].startswith("RT @")]
        rt_tweets = [t for t in karpathy_tweets if t["content_text"].startswith("RT @")]
        
        print(f"\n📊 分类统计:")
        print(f"   原创推文: {len(normal_tweets)} 条")
        print(f"   转推: {len(rt_tweets)} 条")
        
        # 打印原创推文
        if normal_tweets:
            print(f"\n📢 原创推文示例 (前3条):")
            for i, tweet in enumerate(normal_tweets[:3], 1):
                content = tweet["content_text"]
                print(f"\n   [{i}] ({len(content)}字符)")
                print(f"   {content[:200]}{'...' if len(content) > 200 else ''}")
        
        # 打印转推
        if rt_tweets:
            print(f"\n🔁 转推示例 (前3条):")
            for i, tweet in enumerate(rt_tweets[:3], 1):
                content = tweet["content_text"]
                print(f"\n   [{i}] ({len(content)}字符)")
                print(f"   {content[:200]}{'...' if len(content) > 200 else ''}")
        
        print("\n" + "=" * 80)


if __name__ == "__main__":
    # 允许直接运行: python tests/test_karpathy_truncation.py
    pytest.main([__file__, "-v", "-s"])
