"""
twitter_crawler.py — 基于 twtapi.com 的 X 推文采集模块

职责：纯 API 调用层（网络请求 + 分页控制 + 去重）。
解析逻辑在 utils/tweet_parser.py，预处理在 utils/preprocess.py。

流程：username → user_id → tweets（自动分页 + 日期过滤 + 账号粒度去重）
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from loguru import logger

from utils.tweet_parser import (
    _extract_cursor,
    _extract_tweets_from_response,
)

API_BASE = "https://api.twtapi.com"
_HEADERS = {"X-Lang": "zh"}


# ─── API 调用 ─────────────────────────────────────────────────────────────────

def get_user_id(username: str, api_key: str) -> str | None:
    """用户名 → 数字 user_id。"""
    username = username.strip().lstrip("@").split("?")[0].split("/")[-1]
    url = f"{API_BASE}/api/v1/twitter/UsernameToUserId"
    try:
        resp = requests.get(
            url,
            params={"username": username},
            headers={**_HEADERS, "X-API-Key": api_key},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        user_id = data.get("id") or data.get("id_str") or data.get("user_id")
        if user_id:
            logger.info(f"{username} → user_id={user_id}")
            return str(user_id)
        logger.warning(f"{username} no user_id in response")
        return None
    except Exception as e:
        logger.error(f"{username} get_user_id failed: {e}")
        return None


def get_tweets(user_id: str, api_key: str, cursor: str | None = None) -> dict:
    """获取用户推文（单次请求），返回原始响应。"""
    url = f"{API_BASE}/api/v1/twitter/UserTweets"
    params: dict = {"user_id": user_id}
    if cursor:
        params["cursor"] = cursor
    resp = requests.get(
        url,
        params=params,
        headers={**_HEADERS, "X-API-Key": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ─── 采集逻辑 ─────────────────────────────────────────────────────────────────

def fetch_user_tweets(
    username: str,
    api_key: str,
    max_tweets: int = 200,
    days_ago: int = 2,
) -> list[dict]:
    """
    采集单个用户的推文（自动分页 + 日期过滤 + 账号粒度去重）。

    Returns:
        推文列表，每条已包含 tweet_type / rt_author / is_truncated 字段。
    """
    user_id = get_user_id(username, api_key)
    if not user_id:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_ago)
    logger.info(f"{username} crawl start | max={max_tweets} days={days_ago}")

    tweets: list[dict] = []
    seen_ids: set[str] = set()   # 账号粒度去重
    cursor: str | None = None
    stop = False

    while len(tweets) < max_tweets and not stop:
        try:
            data = get_tweets(user_id, api_key, cursor)
        except Exception as e:
            logger.error(f"{username} request failed: {e}")
            break

        batch = _extract_tweets_from_response(data)
        assert isinstance(batch, list)   # _extract_tweets_from_response 不得返回 None

        if not batch:
            logger.info(f"{username} no more data")
            break

        for tweet in batch:
            pid = tweet.get("post_id", "")

            # 账号粒度去重
            if pid in seen_ids:
                continue
            seen_ids.add(pid)

            # 日期过滤
            try:
                tweet_date = datetime.fromisoformat(
                    tweet.get("date", "").replace("Z", "+00:00")
                )
                if tweet_date < cutoff:
                    stop = True   # 遇到旧推文，后续分页更旧，停止
                    continue
            except (ValueError, TypeError):
                pass   # 解析失败不过滤

            tweets.append(tweet)
            if len(tweets) >= max_tweets:
                break

        if stop:
            logger.info(f"{username} reached cutoff ({days_ago}d)")

        cursor = _extract_cursor(data)
        if not cursor:
            logger.info(f"{username} no more pages")
            break

        time.sleep(1)

    logger.info(f"{username} done | tweets={len(tweets)}")
    return tweets[:max_tweets]


def fetch_multiple_users(
    usernames: list[str],
    api_key: str,
    max_tweets_per_user: int = 200,
    days_ago: int = 2,
) -> list[dict]:
    """批量采集多个用户。"""
    all_tweets: list[dict] = []
    for username in usernames:
        tweets = fetch_user_tweets(username, api_key, max_tweets_per_user, days_ago)
        all_tweets.extend(tweets)
    logger.info(f"fetch_multiple_users done | total={len(all_tweets)}")
    return all_tweets


# ─── 输出工具 ─────────────────────────────────────────────────────────────────

def build_payload(tweets: list[dict], usernames: list[str]) -> dict:
    """构建 pipeline 兼容的 payload。"""
    return {
        "site": "X / Twitter",
        "source_accounts": usernames,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_count": len(tweets),
        "texts": tweets,
    }


def save_json(payload: dict, path: Path) -> None:
    """保存 JSON 文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"saved {path} | count={len(payload['texts'])}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    import os
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(description="采集 X 推文")
    parser.add_argument("--usernames", nargs="+", required=True)
    parser.add_argument("--max-tweets", type=int, default=10)
    parser.add_argument("--days-ago", type=int, default=2)
    parser.add_argument("--output", type=Path, default=Path("data/twitter_texts.json"))
    parser.add_argument("--api-key", default=os.getenv("TWTAPI_KEY"))
    args = parser.parse_args()

    if not args.api_key:
        parser.error("请设置 TWTAPI_KEY")

    tweets = fetch_multiple_users(args.usernames, args.api_key, args.max_tweets, args.days_ago)
    if tweets:
        payload = build_payload(tweets, args.usernames)
        save_json(payload, args.output)


if __name__ == "__main__":
    main()
