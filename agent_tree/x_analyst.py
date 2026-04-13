from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import random
import re
import sys
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger
from openai import AsyncOpenAI

PROJECT_DIR = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = PROJECT_DIR / "data" / "snapshots"

if __package__ in (None, ""):
    if str(PROJECT_DIR) not in sys.path:
        sys.path.insert(0, str(PROJECT_DIR))
    from agent_tree.llm import FatalLLMAuthError, llm_json
    from agent_tree.models import ModelConfig
else:
    from .llm import FatalLLMAuthError, llm_json
    from .models import ModelConfig


_BATCH_SYSTEM = """\
你是舆论分析助手。舆论指：在一定社会范围内，消除个人意见差异，反映社会知觉和集合意识的、多数人的共同意见。
当前没有外部 query。你必须只基于给定帖子，自行判断这批帖子在讨论什么，再提炼主要观点、情绪倾向和话题焦点。
你要做“放大提取”：优先保留技术细节、争议点、证据原文和潜在影响，不要只给笼统结论。
输出纯 JSON，字段说明见 user prompt。
"""

_BATCH_USER = """\
本批帖子数：{post_count}

{posts_text}

---
请输出以下 JSON：

{{
  "key_opinions": "4-8句话，描述这批帖子的主要观点和立场分布（举具体例子）",
  "sentiment": "positive | negative | mixed | neutral",
  "top_topics": ["话题1", "话题2", "话题3", ...](按主题热度降序),
  "stance_map": [
    {{"stance": "立场名称", "share": "high | medium | low", "reasoning": "该立场的核心论据"}}
  ],
  "technical_signals": ["本批出现的关键技术点/术语/指标，不少于2条"],
  "risk_signals": ["本批暴露的风险或争议，不少于1条"],
  "opportunity_signals": ["本批体现的机会或积极趋势，不少于1条"],
  "representative_quotes": ["2-3条最有代表性的原文，精确引用，不改写"],
  "notable_quote": "最能代表本批讨论的一句原文（精确引用，不改写）"
}}
"""

_SUMMARIZE_SYSTEM = """\
你是资深舆情分析师。舆论指：在一定社会范围内，消除个人意见差异，反映社会知觉和集合意识的、多数人的共同意见。
你将收到对 X (Twitter) 上多批次帖子的分析摘要。
当前没有外部 query。你需要先识别整份数据集围绕的核心事件/议题，再将关键热点信息整合，输出一句话的最终舆论总结。
"""

_SUMMARIZE_USER = """\
数据集画像：
{dataset_brief}

共 {batch_count} 批帖子，总计 {total_posts} 条。

各批分析如下：
{batch_analyses}

---
请输出以下 JSON：

{{
  "one_sentence_summary": ""
}}
"""

_MARKER_PATTERN = re.compile(r"(?:#[^\s#]{1,50}#?|@[A-Za-z0-9_]{1,50}|\$[A-Za-z]{1,10})")
_LATIN_TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_-]{2,20}")
_CJK_TOKEN_PATTERN = re.compile(r"[\u4e00-\u9fff]{2,6}")
_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "about", "have",
    "has", "had", "are", "was", "were", "will", "would", "should", "could", "can",
    "not", "you", "your", "our", "their", "they", "them", "his", "her", "its",
    "but", "out", "all", "just", "now", "new", "how", "what", "when", "why",
    "who", "where", "after", "before", "over", "more", "than", "also", "only",
    "some", "such", "very", "much", "many", "here", "there", "about", "because",
    "http", "https", "www", "com", "amp", "tco", "via", "retweet", "tweet", "rt",
}

MINIMAX_BASE_URL = "https://api.minimax.io/v1"


class XAnalyst:
    BATCH_SIZE = 20
    LLM_MAX_RETRIES = 3
    SYNTH_GROUP_SIZE = 5

    def __init__(
        self,
        posts: list[dict[str, Any]],
        client: AsyncOpenAI,
        model_config: ModelConfig | None = None,
        batch_size: int = BATCH_SIZE,
        max_concurrent: int = 5,
        max_api_calls: int | None = None,
        llm_max_retries: int = LLM_MAX_RETRIES,
        synth_group_size: int = SYNTH_GROUP_SIZE,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于 0")
        if max_concurrent <= 0:
            raise ValueError("max_concurrent 必须大于 0")
        if llm_max_retries <= 0:
            raise ValueError("llm_max_retries 必须大于 0")
        if max_api_calls is not None and max_api_calls <= 0:
            raise ValueError("max_api_calls 必须大于 0")
        if synth_group_size < 2:
            raise ValueError("synth_group_size 必须大于等于 2")

        config = model_config or ModelConfig()
        self.posts = self._prepare_posts(posts)
        self._dropped_posts = len(posts) - len(self.posts)
        self._client = client
        self._model = config.batch_model
        self._head_model = config.head_model
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.max_api_calls = max_api_calls
        self.llm_max_retries = llm_max_retries
        self.synth_group_size = synth_group_size
        self._remaining_api_calls = max_api_calls
        self._api_budget_lock = asyncio.Lock()

        self._dataset_brief = self._build_dataset_brief(self.posts)
        self._effective_batch_size = self._resolve_effective_batch_size()
        self._planned_batch_count = (
            math.ceil(len(self.posts) / self._effective_batch_size)
            if self.posts
            else 0
        )
        self._planned_synthesis_calls = (
            self._estimate_synthesis_calls(self._planned_batch_count, self.synth_group_size)
            if self.posts
            else 0
        )
        self._planned_logical_calls = self._planned_batch_count + self._planned_synthesis_calls

    async def run(self) -> str:
        if not self.posts:
            logger.warning("XAnalyst.run(): 没有有效帖子，返回空结果")
            return ""

        batches = self._split_batches()
        logger.info(
            "XAnalyst"
            f" | total_posts: {len(self.posts)}"
            f" | dropped_empty: {self._dropped_posts}"
            f" | batches: {len(batches)}"
            f" | batch_size: {self._effective_batch_size}"
            f" | planned_calls: {self._planned_logical_calls}"
            f" | synth_calls: {self._planned_synthesis_calls}"
            f" | synth_group_size: {self.synth_group_size}"
            f" | api_budget: {self.max_api_calls or 'unbounded'}"
            f" | model: {self._model}"
        )

        try:
            analyses = await self._run_batches_concurrent(batches)
        except FatalLLMAuthError as exc:
            raise RuntimeError(
                "LLM 鉴权失败（401/403），请检查 API Key 与 base URL 是否来自同一服务商。"
            ) from exc
        valid = [a for a in analyses if a]
        if not valid:
            logger.error("XAnalyst: 所有批次分析均失败，无法生成总结")
            return ""

        try:
            summary = await self._synthesize(valid)
        except FatalLLMAuthError as exc:
            raise RuntimeError(
                "LLM 鉴权失败（401/403），请检查 API Key 与 base URL 是否来自同一服务商。"
            ) from exc
        logger.info(
            f"XAnalyst | summary='{summary}'"
            f" | api_budget_left={self._remaining_api_calls if self.max_api_calls is not None else 'unbounded'}"
        )
        return summary

    @staticmethod
    def _extract_text(post: dict[str, Any]) -> str:
        return str(post.get("content_text") or post.get("text") or "").strip()

    @staticmethod
    def _extract_author(post: dict[str, Any]) -> str:
        author = str(post.get("author") or post.get("username") or "匿名").strip()
        return author or "匿名"

    @classmethod
    def _prepare_posts(cls, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [post for post in posts if cls._extract_text(post)]

    @classmethod
    def _parse_post_date(cls, post: dict[str, Any]) -> datetime | None:
        raw_date = post.get("date")
        if not raw_date:
            return None
        try:
            return datetime.fromisoformat(str(raw_date).replace("Z", "+00:00"))
        except ValueError:
            return None

    @classmethod
    def _build_dataset_brief(cls, posts: list[dict[str, Any]]) -> str:
        if not posts:
            return "无有效帖子。"

        unique_authors = len({cls._extract_author(post) for post in posts})
        markers = cls._top_markers(posts)
        keywords = cls._top_keywords(posts)
        dates = sorted(dt for dt in (cls._parse_post_date(post) for post in posts) if dt)

        parts = [
            f"样本量 {len(posts)} 条",
            f"作者数约 {unique_authors}",
        ]
        if dates:
            if dates[0].date() == dates[-1].date():
                parts.append(f"时间范围 {dates[0].date().isoformat()}")
            else:
                parts.append(
                    f"时间范围 {dates[0].date().isoformat()} 至 {dates[-1].date().isoformat()}"
                )
        if markers:
            parts.append("显式标记 " + "、".join(markers))
        if keywords:
            parts.append("高频词 " + "、".join(keywords))
        return "；".join(parts) + "。"

    @classmethod
    def _top_markers(cls, posts: list[dict[str, Any]], limit: int = 5) -> list[str]:
        counter: Counter[str] = Counter()
        for post in posts:
            text = cls._extract_text(post)
            counter.update(marker.strip() for marker in _MARKER_PATTERN.findall(text))
            hashtags = post.get("hashtags")
            if isinstance(hashtags, list):
                counter.update(str(tag).strip() for tag in hashtags if str(tag).strip())
        return [marker for marker, _ in counter.most_common(limit)]

    @classmethod
    def _top_keywords(cls, posts: list[dict[str, Any]], limit: int = 5) -> list[str]:
        counter: Counter[str] = Counter()
        for post in posts:
            text = cls._extract_text(post)
            for token in _LATIN_TOKEN_PATTERN.findall(text):
                lowered = token.lower()
                if lowered not in _STOPWORDS:
                    counter[lowered] += 1
            for token in _CJK_TOKEN_PATTERN.findall(text):
                counter[token] += 1
        return [
            token
            for token, count in counter.most_common(limit)
            if count > 1
        ]

    def _resolve_effective_batch_size(self) -> int:
        if not self.posts or self.max_api_calls is None:
            return self.batch_size

        logical_capacity = self.max_api_calls // self.llm_max_retries
        if logical_capacity < 2:
            raise ValueError(
                "max_api_calls 在当前 llm_max_retries 下不足以完成"
                "至少 1 个 batch 分析 + 1 次总结"
            )
        effective_batch_size = min(max(1, self.batch_size), len(self.posts))
        while effective_batch_size <= len(self.posts):
            batch_count = math.ceil(len(self.posts) / effective_batch_size)
            synth_calls = self._estimate_synthesis_calls(batch_count, self.synth_group_size)
            total_calls = batch_count + synth_calls
            if total_calls <= logical_capacity:
                if effective_batch_size != self.batch_size:
                    logger.info(
                        "XAnalyst | 调整 batch_size"
                        f" {self.batch_size} -> {effective_batch_size}"
                        f" 以满足 max_api_calls={self.max_api_calls}"
                    )
                return effective_batch_size
            effective_batch_size += 1

        raise ValueError(
            "max_api_calls 在当前 llm_max_retries 下不足以完成最小化后的分层总结流程"
        )

    @staticmethod
    def _estimate_synthesis_calls(item_count: int, group_size: int) -> int:
        """估算分层合成所需调用次数。"""
        if item_count <= 0:
            return 0
        if item_count == 1:
            return 1

        calls = 0
        current = item_count
        while current > 1:
            current = math.ceil(current / group_size)
            calls += current
        return calls

    @classmethod
    def _stable_seed(cls, posts: list[dict[str, Any]]) -> int:
        fingerprint = "\x1e".join(
            (
                f"{cls._extract_author(post)}\x1f"
                f"{post.get('post_id') or post.get('id') or ''}\x1f"
                f"{cls._extract_text(post)[:160]}"
            )
            for post in posts
        )
        digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()
        return int(digest[:16], 16)

    def _rebalance_posts(self) -> list[dict[str, Any]]:
        if len(self.posts) <= 1:
            return list(self.posts)

        grouped: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        rng = random.Random(self._stable_seed(self.posts))

        bucket_items: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for post in self.posts:
            bucket_items[self._extract_author(post)].append(post)

        authors = list(bucket_items.keys())
        rng.shuffle(authors)
        for author in authors:
            items = bucket_items[author]
            rng.shuffle(items)
            grouped[author] = deque(items)

        balanced: list[dict[str, Any]] = []
        active_authors = authors
        while active_authors:
            next_round: list[str] = []
            for author in active_authors:
                bucket = grouped[author]
                if not bucket:
                    continue
                balanced.append(bucket.popleft())
                if bucket:
                    next_round.append(author)
            rng.shuffle(next_round)
            active_authors = next_round

        return balanced

    def _split_batches(self) -> list[list[dict[str, Any]]]:
        """按有效 batch_size 切分，并先打散作者聚集造成的批次偏斜。"""
        balanced_posts = self._rebalance_posts()
        return [
            balanced_posts[i : i + self._effective_batch_size]
            for i in range(0, len(balanced_posts), self._effective_batch_size)
        ]

    @classmethod
    def _format_posts(cls, posts: list[dict[str, Any]]) -> str:
        lines = []
        for i, post in enumerate(posts, 1):
            text = re.sub(r"\s+", " ", cls._extract_text(post))
            author = cls._extract_author(post)
            lines.append(f"[{i}] @{author}: {text}")
        return "\n".join(lines)

    async def _consume_api_budget(self) -> bool:
        if self._remaining_api_calls is None:
            return True
        async with self._api_budget_lock:
            if self._remaining_api_calls <= 0:
                return False
            self._remaining_api_calls -= 1
            return True

    async def _call_llm_json(self, system: str, user: str, model: str) -> dict[str, Any]:
        if self.max_api_calls is None:
            return await llm_json(
                self._client,
                system,
                user,
                model=model,
                max_retries=self.llm_max_retries,
                fail_fast_on_auth=True,
            )

        for attempt in range(1, self.llm_max_retries + 1):
            if not await self._consume_api_budget():
                logger.warning("XAnalyst: API 调用预算已耗尽，停止后续请求")
                return {}

            raw = await llm_json(
                self._client,
                system,
                user,
                model=model,
                max_retries=1,
                fail_fast_on_auth=True,
            )
            if raw:
                return raw

            if attempt < self.llm_max_retries:
                await asyncio.sleep(float(attempt))

        return {}

    async def _analyze_batch(
        self,
        batch_idx: int,
        posts: list[dict[str, Any]],
        semaphore: asyncio.Semaphore,
    ) -> str:
        """
        对单个批次调用 LLM，返回该批次的舆论分析摘要文本。
        失败时返回空字符串，不向上抛异常，保证其他批次继续运行。
        """
        async with semaphore:
            logger.info(
                f"XAnalyst batch-{batch_idx}"
                f" | posts={len(posts)}"
                f" | model={self._model}"
            )
            user = _BATCH_USER.format(
                post_count=len(posts),
                posts_text=self._format_posts(posts),
            )
            raw = await self._call_llm_json(_BATCH_SYSTEM, user, model=self._model)
            if not raw:
                logger.warning(f"XAnalyst batch-{batch_idx}: LLM 返回空结果")
                return ""

            def _as_list(value: Any) -> list[str]:
                if isinstance(value, list):
                    return [str(item).strip() for item in value if str(item).strip()]
                if value:
                    text = str(value).strip()
                    return [text] if text else []
                return []

            def _format_stance_map(value: Any) -> list[str]:
                if not isinstance(value, list):
                    return []
                items: list[str] = []
                for entry in value:
                    if not isinstance(entry, dict):
                        item_text = str(entry).strip()
                        if item_text:
                            items.append(item_text)
                        continue
                    stance = str(entry.get("stance", "")).strip()
                    share = str(entry.get("share", "")).strip()
                    reasoning = str(entry.get("reasoning", "")).strip()
                    text = stance or "未命名立场"
                    if share:
                        text += f"（占比:{share}）"
                    if reasoning:
                        text += f": {reasoning}"
                    if text.strip():
                        items.append(text.strip())
                return items

            key_opinions = str(raw.get("key_opinions", "")).strip()
            sentiment = str(raw.get("sentiment", "unknown")).strip() or "unknown"
            top_topics = _as_list(raw.get("top_topics", []))
            stance_map = _format_stance_map(raw.get("stance_map", []))
            technical_signals = _as_list(raw.get("technical_signals", []))
            risk_signals = _as_list(raw.get("risk_signals", []))
            opportunity_signals = _as_list(raw.get("opportunity_signals", []))
            representative_quotes = _as_list(raw.get("representative_quotes", []))
            notable_quote = str(raw.get("notable_quote", "")).strip()
            if notable_quote and notable_quote not in representative_quotes:
                representative_quotes.append(notable_quote)

            parts = [f"[批次{batch_idx + 1}]"]
            parts.append(f"情绪倾向: {sentiment}")
            if top_topics:
                parts.append("核心话题: " + "；".join(top_topics[:6]))
            if key_opinions:
                parts.append("观点放大: " + key_opinions)
            if stance_map:
                parts.append("立场分布: " + "；".join(stance_map[:5]))
            if technical_signals:
                parts.append("技术信号: " + "；".join(technical_signals[:6]))
            if risk_signals:
                parts.append("风险信号: " + "；".join(risk_signals[:4]))
            if opportunity_signals:
                parts.append("机会信号: " + "；".join(opportunity_signals[:4]))
            if representative_quotes:
                quote_text = " | ".join(f"“{quote}”" for quote in representative_quotes[:3])
                parts.append("证据原文: " + quote_text)

            analysis = "\n".join(parts)
            logger.debug(f"XAnalyst batch-{batch_idx} done")
            return analysis

    async def _run_batches_concurrent(self, batches: list[list[dict[str, Any]]]) -> list[str]:
        """并发执行所有批次分析，受 max_concurrent 信号量控制。"""
        if not batches:
            return []

        semaphore = asyncio.Semaphore(min(self.max_concurrent, len(batches)))
        tasks = [
            self._analyze_batch(i, batch, semaphore)
            for i, batch in enumerate(batches)
        ]
        return await asyncio.gather(*tasks)

    async def _synthesize_chunk(
        self,
        chunk: list[str],
        level: int,
        group_index: int,
        group_count: int,
    ) -> str:
        chunk_text = "\n".join(
            f"{i + 1}. {analysis}" for i, analysis in enumerate(chunk)
        )
        user = _SUMMARIZE_USER.format(
            dataset_brief=self._dataset_brief,
            batch_count=len(chunk),
            total_posts=len(self.posts),
            batch_analyses=chunk_text,
        )
        logger.info(
            "XAnalyst synthesize"
            f" | level={level}"
            f" | group={group_index + 1}/{group_count}"
            f" | inputs={len(chunk)}"
            f" | model={self._head_model}"
        )
        raw = await self._call_llm_json(_SUMMARIZE_SYSTEM, user, model=self._head_model)
        return str(raw.get("one_sentence_summary", "")).strip()

    async def _synthesize(self, batch_analyses: list[str]) -> str:
        """分层合成：每次只合并少量摘要，逐层压缩到一句话。"""
        current = [item for item in batch_analyses if item]
        if not current:
            return ""

        level = 1
        while True:
            chunks = [
                current[i : i + self.synth_group_size]
                for i in range(0, len(current), self.synth_group_size)
            ]
            next_level: list[str] = []
            for idx, chunk in enumerate(chunks):
                summary = await self._synthesize_chunk(chunk, level, idx, len(chunks))
                if summary:
                    next_level.append(summary)

            if not next_level:
                logger.error(f"XAnalyst synthesize level-{level}: 全部分组返回空结果")
                return ""

            if len(next_level) == 1:
                return next_level[0]

            current = next_level
            level += 1


async def analyze_x_posts(
    posts: list[dict[str, Any]],
    client: AsyncOpenAI,
    model_config: ModelConfig | None = None,
    batch_size: int = XAnalyst.BATCH_SIZE,
    max_concurrent: int = 5,
    max_api_calls: int | None = None,
    llm_max_retries: int = XAnalyst.LLM_MAX_RETRIES,
    synth_group_size: int = XAnalyst.SYNTH_GROUP_SIZE,
) -> str:
    """
    对 X (Twitter) 帖子做分批舆论分析，返回一句话总结。

    Args:
        posts:           帖子列表，每条 dict 至少包含 content_text/text 与 author/username
        client:          AsyncOpenAI 实例
        model_config:    模型配置，默认使用 ModelConfig 默认值
        batch_size:      每批帖子数，默认 20
        max_concurrent:  并发批次数上限，默认 5
        max_api_calls:   当前 run 的 API 尝试总上限（包含重试）；None 表示不限制
        llm_max_retries: 单次 LLM 调用最大尝试次数，默认 3
        synth_group_size: 每层合成最多合并多少条摘要，默认 5

    Returns:
        一句话舆论总结字符串；失败时返回空字符串
    """
    analyst = XAnalyst(
        posts=posts,
        client=client,
        model_config=model_config,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        max_api_calls=max_api_calls,
        llm_max_retries=llm_max_retries,
        synth_group_size=synth_group_size,
    )
    return await analyst.run()


def load_snapshot_posts(snapshot_path: Path) -> list[dict[str, Any]]:
    """
    从 snapshot JSON 中加载帖子列表。

    支持两种输入：
      1. {"texts": [...]} 的标准 payload
      2. 直接为帖子数组 [...]
    """
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        texts = payload.get("texts", [])
        if not isinstance(texts, list):
            raise ValueError(f"snapshot 缺少有效的 texts 列表: {snapshot_path}")
        return texts
    if isinstance(payload, list):
        return payload
    raise ValueError(f"不支持的 snapshot 格式: {snapshot_path}")


def _resolve_model_config(
    model: str | None,
    head_model: str | None,
    batch_model: str | None,
) -> ModelConfig:
    cfg = ModelConfig.from_single(model) if model else ModelConfig()
    if head_model:
        cfg.head_model = head_model
    if batch_model:
        cfg.batch_model = batch_model
    return cfg


def _is_minimax_model(model: str | None) -> bool:
    return bool(model and str(model).startswith("MiniMax-"))


def _uses_minimax(model_config: ModelConfig) -> bool:
    return _is_minimax_model(model_config.head_model) or _is_minimax_model(model_config.batch_model)


def _first_non_empty(*values: str | None) -> str:
    for value in values:
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned:
            return cleaned
    return ""


def _resolve_api_key(explicit_api_key: str | None, model_config: ModelConfig) -> str:
    if _uses_minimax(model_config):
        return _first_non_empty(
            explicit_api_key,
            os.getenv("MINIMAX_API_KEY"),
            os.getenv("OPENAI_API_KEY"),
        )
    return _first_non_empty(
        explicit_api_key,
        os.getenv("OPENAI_API_KEY"),
        os.getenv("MINIMAX_API_KEY"),
    )


def _resolve_base_url(explicit_base_url: str | None, model_config: ModelConfig) -> str:
    if _uses_minimax(model_config):
        return _first_non_empty(
            explicit_base_url,
            os.getenv("MINIMAX_BASE_URL"),
            os.getenv("OPENAI_BASE_URL"),
            os.getenv("OPENAI_URL"),
            MINIMAX_BASE_URL,
        )
    return _first_non_empty(
        explicit_base_url,
        os.getenv("OPENAI_BASE_URL"),
        os.getenv("OPENAI_URL"),
        os.getenv("MINIMAX_BASE_URL"),
    )


def _resolve_snapshot_path(explicit_snapshot: Path | None) -> Path:
    if explicit_snapshot is not None:
        return explicit_snapshot.expanduser().resolve()

    candidates = sorted(SNAPSHOT_DIR.glob("twitter_*.json"))
    if candidates:
        return candidates[-1].resolve()

    raise FileNotFoundError(
        "未提供 --snapshot，且未在 data/snapshots 下发现 twitter_*.json。"
        "请显式传入 --snapshot /path/to/twitter_snapshot.json。"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="对单个 Twitter snapshot 运行 XAnalyst",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=None,
        metavar="PATH",
        help="要分析的 snapshot 文件；未传入时自动选择 data/snapshots 下最新的 twitter_*.json",
    )
    parser.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="所有阶段统一使用的模型",
    )
    parser.add_argument(
        "--head-model",
        default=None,
        metavar="MODEL",
        help="汇总阶段模型，覆盖 --model",
    )
    parser.add_argument(
        "--batch-model",
        default=None,
        metavar="MODEL",
        help="批分析阶段模型，覆盖 --model",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=XAnalyst.BATCH_SIZE,
        metavar="N",
        help=f"每批帖子数，默认 {XAnalyst.BATCH_SIZE}",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=5,
        metavar="N",
        help="批分析最大并发数，默认 5",
    )
    parser.add_argument(
        "--max-api-calls",
        type=int,
        default=None,
        metavar="N",
        help="总 API 调用硬上限（包含重试）",
    )
    parser.add_argument(
        "--llm-max-retries",
        type=int,
        default=XAnalyst.LLM_MAX_RETRIES,
        metavar="N",
        help=f"单次 LLM 调用最大尝试次数，默认 {XAnalyst.LLM_MAX_RETRIES}",
    )
    parser.add_argument(
        "--synth-group-size",
        type=int,
        default=XAnalyst.SYNTH_GROUP_SIZE,
        metavar="N",
        help=f"每层合成最多合并的摘要数，默认 {XAnalyst.SYNTH_GROUP_SIZE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="仅分析前 N 条帖子，便于低成本测试",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        metavar="KEY",
        help="API Key，默认读取 OPENAI_API_KEY / MINIMAX_API_KEY",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        metavar="URL",
        help="OpenAI 兼容接口地址，默认读取 OPENAI_BASE_URL / OPENAI_URL / MINIMAX_BASE_URL",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="输出 DEBUG 日志",
    )
    return parser


async def _main_async(args: argparse.Namespace) -> None:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=PROJECT_DIR / ".env")

    snapshot_path = _resolve_snapshot_path(args.snapshot)
    if not snapshot_path.exists():
        raise FileNotFoundError(f"snapshot 不存在: {snapshot_path}")

    posts = load_snapshot_posts(snapshot_path)
    if args.limit is not None:
        posts = posts[: args.limit]

    model_config = _resolve_model_config(
        model=args.model,
        head_model=args.head_model,
        batch_model=args.batch_model,
    )

    api_key = _resolve_api_key(args.api_key, model_config)
    if not api_key:
        raise ValueError("需要 OPENAI_API_KEY / MINIMAX_API_KEY 环境变量或 --api-key 参数")

    base_url = _resolve_base_url(args.base_url, model_config)
    client_kwargs: dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url

    client = AsyncOpenAI(**client_kwargs)

    summary = await analyze_x_posts(
        posts=posts,
        client=client,
        model_config=model_config,
        batch_size=args.batch_size,
        max_concurrent=args.max_concurrent,
        max_api_calls=args.max_api_calls,
        llm_max_retries=args.llm_max_retries,
        synth_group_size=args.synth_group_size,
    )

    print(f"snapshot: {snapshot_path}")
    print(f"posts: {len(posts)}")
    print(
        "models:"
        f" batch={model_config.batch_model}"
        f" head={model_config.head_model}"
    )
    print("\nsummary:")
    print(summary or "<empty>")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logger.remove()
    level = "DEBUG" if args.verbose else "INFO"
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}",
    )

    try:
        asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        print("\n用户中断")
        raise SystemExit(0)
    except Exception as exc:
        logger.error(f"XAnalyst CLI 运行失败: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
