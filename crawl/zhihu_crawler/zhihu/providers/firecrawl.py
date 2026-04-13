"""
Firecrawl 驱动的知乎问题抓取 provider。

设计原则：
- 发现阶段依赖 Firecrawl browser session 的远程浏览器执行
- 详情阶段依赖 Firecrawl scrape / batch scrape
- 字段提取以本地 HTML 解析为主，避免将 LLM 提取作为主路径
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
import yaml
from requests import Response
from scrapy.selector import Selector
from loguru import logger

from zhihu.exporter import build_payload, export_payload
from zhihu.items import ZhihuAnswerItem, ZhihuArticleItem
from zhihu.pipelines import ZhihuCleanPipeline
from zhihu.utils.text_clean_zhihu import clean_zhihu_excerpt, html_to_text
from zhihu.utils.zhihu_parser import (
    extract_article_id_from_url,
    extract_answer_id_from_url,
    extract_question_id_from_url,
    extract_user_token_from_url,
    normalize_question_identifier,
    parse_timestamp,
)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_SOURCE = "zhihu_question_firecrawl"
USER_SOURCE = "zhihu_user_firecrawl"
DISCOVERY_WAIT_MS = 1200
DISCOVERY_INITIAL_WAIT_MS = 1500
DEFAULT_BROWSER_BASE = "https://api.firecrawl.dev/v2"
CJK_LOCATION = {
    "country": "CN",
    "languages": ["zh-CN", "zh"],
}


class FirecrawlError(RuntimeError):
    """Firecrawl API / parsing 统一异常。"""


class FirecrawlNonRetryableError(FirecrawlError):
    """不可重试错误（例如题目已不可用）。"""


@dataclass
class FirecrawlConfig:
    api_key: str
    output_dir: Path = Path("data")
    api_base: str = DEFAULT_BROWSER_BASE
    proxy: str = "auto"
    location: dict[str, Any] = field(default_factory=lambda: dict(CJK_LOCATION))
    only_main_content: bool = False
    max_age: int = 0
    max_scroll_rounds: int = 12
    discovery_idle_rounds: int = 2
    max_answers: int | None = None
    crawl_timeout_s: int = 180
    debug_artifacts: bool = False
    zero_data_retention: bool = False
    request_timeout_s: int = 60
    batch_poll_interval_s: int = 2
    browser_ttl_s: int = 180
    question_retry_attempts: int = 2
    detail_retry_attempts: int = 2
    scrape_wait_for_ms: int = 2500
    scrape_timeout_ms: int = 120000
    discovery_mode: str = "auto"  # auto | browser | scrape
    disable_browser_after_failure: bool = True

    def __post_init__(self) -> None:
        self.api_base = _normalize_firecrawl_api_base(self.api_base)

    @classmethod
    def from_env(
        cls,
        *,
        output_dir: Path | str = "data",
        max_answers: int | None = None,
    ) -> "FirecrawlConfig":
        api_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
        if not api_key:
            raise ValueError("缺少 FIRECRAWL_API_KEY，无法使用 Firecrawl provider")

        env_max_answers = os.getenv("ZHIHU_MAX_ANSWERS", "").strip()
        resolved_max_answers = max_answers
        if resolved_max_answers is None and env_max_answers.isdigit():
            resolved_max_answers = int(env_max_answers)

        return cls(
            api_key=api_key,
            output_dir=Path(output_dir),
            api_base=os.getenv("ZHIHU_FIRECRAWL_API_BASE", os.getenv("FIRECRAWL_API_BASE", DEFAULT_BROWSER_BASE)),
            max_scroll_rounds=int(os.getenv("ZHIHU_MAX_SCROLL_ROUNDS", "12")),
            discovery_idle_rounds=int(os.getenv("ZHIHU_DISCOVERY_IDLE_ROUNDS", "2")),
            max_answers=resolved_max_answers,
            crawl_timeout_s=int(os.getenv("ZHIHU_CRAWL_TIMEOUT_S", "180")),
            debug_artifacts=_env_flag("ZHIHU_DEBUG_ARTIFACTS"),
            zero_data_retention=_env_flag("ZHIHU_FIRECRAWL_ZERO_DATA_RETENTION"),
            scrape_wait_for_ms=int(os.getenv("ZHIHU_SCRAPE_WAIT_MS", "2500")),
            scrape_timeout_ms=int(os.getenv("ZHIHU_SCRAPE_TIMEOUT_MS", "120000")),
            discovery_mode=os.getenv("ZHIHU_DISCOVERY_MODE", "auto").strip().lower(),
            disable_browser_after_failure=(
                _env_flag("ZHIHU_DISABLE_BROWSER_AFTER_FAILURE")
                if os.getenv("ZHIHU_DISABLE_BROWSER_AFTER_FAILURE") is not None
                else True
            ),
        )


@dataclass
class QuestionDiscoveryResult:
    question_id: str
    question_title: str
    displayed_count: int | None
    answer_urls: list[str]
    dismissed_modal: bool = False
    expanded_any: bool = False
    page_url: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    discovery_mode: str = "browser"


@dataclass
class UserDiscoveryResult:
    user_token: str
    answer_urls: list[str] = field(default_factory=list)
    article_urls: list[str] = field(default_factory=list)
    page_urls: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)
    discovery_mode: str = "browser"


class FirecrawlClient:
    """最小 Firecrawl REST client。"""

    def __init__(self, config: FirecrawlConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def create_browser(self) -> dict[str, Any]:
        return _unwrap_firecrawl_data(self._request("POST", "/browser", json_payload={}))

    def execute_browser(
        self,
        session_id: str,
        code: str,
        *,
        language: str = "node",
        timeout_s: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"code": code, "language": language}
        if timeout_s is not None:
            body["timeout"] = max(1, min(int(timeout_s), 300))
        return _unwrap_firecrawl_data(
            self._request(
                "POST",
                f"/browser/{session_id}/execute",
                json_payload=body,
                timeout=self.config.crawl_timeout_s,
            )
        )

    def delete_browser(self, session_id: str) -> None:
        self._request("DELETE", f"/browser/{session_id}", allow_empty=True)

    def batch_scrape(self, urls: list[str], *, formats: list[str]) -> list[dict[str, Any]]:
        if not urls:
            return []

        body = self._scrape_body(formats=formats)
        body["urls"] = urls

        start = _unwrap_firecrawl_data(
            self._request("POST", "/batch/scrape", json_payload=body, timeout=self.config.request_timeout_s)
        )
        if "data" in start:
            return list(start.get("data", []))

        poll_ref = start.get("url") or start.get("statusUrl") or start.get("next") or start.get("id")
        if not poll_ref:
            raise FirecrawlError(f"Firecrawl batch scrape 返回缺少轮询地址: {start}")

        deadline = time.time() + self.config.crawl_timeout_s
        while time.time() < deadline:
            status = _unwrap_firecrawl_data(self._request("GET", poll_ref, timeout=self.config.request_timeout_s))
            state = str(status.get("status", "")).lower()
            if state in {"completed", "done", "success"}:
                return self._collect_paginated_batch_data(status)
            if state in {"failed", "error", "cancelled"}:
                raise FirecrawlError(f"Firecrawl batch scrape 失败: {status}")
            time.sleep(self.config.batch_poll_interval_s)

        raise FirecrawlError("Firecrawl batch scrape 轮询超时")

    def scrape(
        self,
        url: str,
        *,
        formats: list[str],
        wait_for_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        body = self._scrape_body(formats=formats)
        body["url"] = url
        if wait_for_ms is not None:
            body["waitFor"] = max(0, int(wait_for_ms))
        if timeout_ms is not None:
            body["timeout"] = max(1000, min(int(timeout_ms), 300000))
        return _unwrap_firecrawl_data(
            self._request("POST", "/scrape", json_payload=body, timeout=self.config.request_timeout_s)
        )

    def _scrape_body(self, *, formats: list[str]) -> dict[str, Any]:
        body: dict[str, Any] = {
            "formats": formats,
            "maxAge": self.config.max_age,
            "onlyMainContent": self.config.only_main_content,
            "proxy": self.config.proxy,
            "location": self.config.location,
        }
        if self.config.zero_data_retention:
            body["zeroDataRetention"] = True
        return body

    def _collect_paginated_batch_data(self, status: dict[str, Any]) -> list[dict[str, Any]]:
        data = list(status.get("data", []))
        next_ref = status.get("next")
        while next_ref:
            page = _unwrap_firecrawl_data(self._request("GET", next_ref, timeout=self.config.request_timeout_s))
            data.extend(page.get("data", []))
            next_ref = page.get("next")
        return data

    def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        json_payload: dict[str, Any] | None = None,
        timeout: int | None = None,
        allow_empty: bool = False,
    ) -> dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self.config.api_base.rstrip('/')}{path_or_url}"
        last_error: Exception | None = None

        for attempt in range(self.config.detail_retry_attempts):
            try:
                response = self.session.request(
                    method,
                    url,
                    json=json_payload,
                    timeout=timeout or self.config.request_timeout_s,
                )
                if response.status_code in RETRYABLE_STATUS_CODES and attempt + 1 < self.config.detail_retry_attempts:
                    time.sleep(2**attempt)
                    continue
                self._raise_for_status(response)
                if allow_empty and not response.content:
                    return {}
                if not response.content:
                    return {}
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt + 1 >= self.config.detail_retry_attempts:
                    break
                time.sleep(2**attempt)

        raise FirecrawlError(f"Firecrawl 请求失败: {url} | {last_error}")

    @staticmethod
    def _raise_for_status(response: Response) -> None:
        if response.ok:
            return
        detail = response.text[:500]
        hint = ""
        if response.status_code == 403 and "api.firecrawl.dev/v1/" in response.url:
            hint = " | detected legacy Firecrawl v1 endpoint, switch api_base to https://api.firecrawl.dev/v2"
        raise FirecrawlError(f"Firecrawl HTTP {response.status_code}: {detail}{hint}")


def _normalize_firecrawl_api_base(value: str) -> str:
    base = str(value or "").strip()
    if not base:
        return DEFAULT_BROWSER_BASE

    parsed = urlparse(base)
    if parsed.scheme and parsed.netloc == "api.firecrawl.dev":
        path = parsed.path.rstrip("/")
        if path in {"", "/v1", "/v2"}:
            return f"{parsed.scheme}://{parsed.netloc}/v2"

    return base.rstrip("/")


class ZhihuFirecrawlCrawler:
    """知乎问题 Firecrawl 抓取器。"""

    def __init__(self, config: FirecrawlConfig, client: FirecrawlClient | None = None):
        self.config = config
        self.client = client or FirecrawlClient(config)
        self.clean_pipeline = ZhihuCleanPipeline()
        self.last_manifest: dict[str, Any] | None = None
        self.last_manifest_path: Path | None = None
        self.last_export_paths: dict[str, Path] = {}
        self._browser_disabled_reason: str = ""

    def crawl_questions(self, questions: list[str], *, max_answers: int | None = None) -> dict[str, Any]:
        normalized = []
        for value in questions:
            qid = normalize_question_identifier(value)
            if qid:
                normalized.append(qid)

        if not normalized:
            raise ValueError("未提供有效的 question id / URL")

        effective_max_answers = max_answers if max_answers is not None else self.config.max_answers

        all_items: list[dict[str, Any]] = []
        manifests: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for question_id in normalized:
            logger.info(
                "zhihu question start | qid={} | max_answers={} | scroll_rounds={} | discovery_idle_rounds={}",
                question_id,
                effective_max_answers,
                self.config.max_scroll_rounds,
                self.config.discovery_idle_rounds,
            )
            items, manifest = self._crawl_single_question(question_id, effective_max_answers)
            manifests.append(manifest)
            logger.info(
                "zhihu question done | qid={} | mode={} | displayed={} | discovered={} | fetched={} | failed={} | partial={}{}",
                question_id,
                manifest.get("discovery_mode", "unknown"),
                manifest.get("displayed_count"),
                manifest.get("discovered_count"),
                manifest.get("fetched_count"),
                manifest.get("failed_count"),
                manifest.get("partial"),
                f" | error={manifest.get('error')}" if manifest.get("error") else "",
            )
            for item in items:
                dedupe_key = str(item.get("record_id") or item.get("post_id") or "")
                if dedupe_key and dedupe_key not in seen_ids:
                    all_items.append(item)
                    seen_ids.add(dedupe_key)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        payload = build_payload(all_items, DEFAULT_SOURCE)
        payload["source_meta"] = {
            "platform": "zhihu",
            "extractor": "firecrawl",
            "targets": normalized,
        }
        self.last_export_paths = export_payload(payload, self.config.output_dir, timestamp=timestamp)

        manifest_payload = {
            "source": DEFAULT_SOURCE,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "questions": manifests,
        }
        manifest_path = self.config.output_dir / "manifests" / f"zhihu_firecrawl_{timestamp}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump(manifest_payload, f, ensure_ascii=False, indent=2)

        self.last_manifest = manifest_payload
        self.last_manifest_path = manifest_path
        return payload

    def crawl_users(
        self,
        user_tokens: list[str],
        *,
        max_items: int | None = None,
        crawl_answers: bool = True,
        crawl_articles: bool = True,
    ) -> dict[str, Any]:
        normalized = []
        for value in user_tokens:
            token = extract_user_token_from_url(value) or str(value).strip().lstrip("@")
            if token:
                normalized.append(token)

        if not normalized:
            raise ValueError("未提供有效的知乎用户 token / URL")
        if not crawl_answers and not crawl_articles:
            raise ValueError("crawl_answers 和 crawl_articles 不能同时为 false")

        all_items: list[dict[str, Any]] = []
        manifests: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for user_token in normalized:
            items, manifest = self._crawl_single_user(
                user_token,
                max_items=max_items,
                crawl_answers=crawl_answers,
                crawl_articles=crawl_articles,
            )
            manifests.append(manifest)
            for item in items:
                dedupe_key = str(item.get("record_id") or item.get("post_id") or "")
                if dedupe_key and dedupe_key not in seen_ids:
                    all_items.append(item)
                    seen_ids.add(dedupe_key)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        payload = build_payload(all_items, USER_SOURCE)
        payload["source_meta"] = {
            "platform": "zhihu",
            "extractor": "firecrawl",
            "targets": normalized,
            "target_kind": "user",
        }
        self.last_export_paths = export_payload(payload, self.config.output_dir, timestamp=timestamp)

        manifest_payload = {
            "source": USER_SOURCE,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "users": manifests,
        }
        manifest_path = self.config.output_dir / "manifests" / f"zhihu_firecrawl_users_{timestamp}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump(manifest_payload, f, ensure_ascii=False, indent=2)

        self.last_manifest = manifest_payload
        self.last_manifest_path = manifest_path
        return payload

    def _crawl_single_question(
        self,
        question_id: str,
        max_answers: int | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        question_url = f"https://www.zhihu.com/question/{question_id}"
        manifest: dict[str, Any] = {
            "question_id": question_id,
            "question_url": question_url,
            "question_title": "",
            "displayed_count": None,
            "discovered_count": 0,
            "fetched_count": 0,
            "failed_count": 0,
            "completeness_ratio": 0.0,
            "partial": False,
            "dismissed_modal": False,
            "expanded_any": False,
        }

        discovery: QuestionDiscoveryResult | None = None
        discovery_error = ""
        for attempt in range(self.config.question_retry_attempts):
            logger.info(
                "zhihu discovery attempt | qid={} | attempt={}/{}",
                question_id,
                attempt + 1,
                self.config.question_retry_attempts,
            )
            try:
                discovery = self.discover_question(question_url, max_answers=max_answers)
                break
            except Exception as exc:
                discovery_error = str(exc)
                logger.warning(
                    "zhihu discovery failed | qid={} | attempt={}/{} | error={}",
                    question_id,
                    attempt + 1,
                    self.config.question_retry_attempts,
                    discovery_error,
                )
                if isinstance(exc, FirecrawlNonRetryableError):
                    logger.warning(
                        "zhihu discovery non-retryable | qid={} | reason={}",
                        question_id,
                        discovery_error,
                    )
                    break
                if attempt + 1 >= self.config.question_retry_attempts:
                    break

        if not discovery:
            manifest["partial"] = True
            manifest["error"] = discovery_error or "question discovery failed"
            self._maybe_save_debug_artifacts(question_id, question_url, manifest)
            return [], manifest

        manifest["question_title"] = discovery.question_title
        manifest["displayed_count"] = discovery.displayed_count
        manifest["dismissed_modal"] = discovery.dismissed_modal
        manifest["expanded_any"] = discovery.expanded_any
        manifest["discovery_mode"] = discovery.discovery_mode

        answer_urls = discovery.answer_urls
        if max_answers:
            answer_urls = answer_urls[:max_answers]
        manifest["discovered_count"] = len(answer_urls)

        if not answer_urls:
            logger.warning(
                "zhihu discovered zero answer urls | qid={} | mode={} | trying direct question-page parsing",
                question_id,
                discovery.discovery_mode,
            )
            direct_items = parse_answers_from_question_page(
                raw_html=str(discovery.raw.get("raw_html") or ""),
                question_url=question_url,
                question_id=question_id,
                question_title=discovery.question_title,
                max_answers=max_answers,
                clean_pipeline=self.clean_pipeline,
            )
            if direct_items:
                expected_count = _expected_count(
                    displayed_count=discovery.displayed_count,
                    discovered_count=len(direct_items),
                    max_answers=max_answers,
                )
                manifest["discovered_count"] = len(direct_items)
                manifest["fetched_count"] = len(direct_items)
                manifest["failed_count"] = 0
                manifest["completeness_ratio"] = round((len(direct_items) / expected_count), 4) if expected_count else 0.0
                manifest["partial"] = bool(
                    discovery.displayed_count
                    and len(direct_items) < min(discovery.displayed_count, max_answers or discovery.displayed_count)
                )
                if manifest["partial"]:
                    manifest["warning"] = "used direct question-page parsing fallback"
                    self._maybe_save_debug_artifacts(question_id, question_url, manifest)
                return direct_items, manifest

            manifest["partial"] = True
            manifest["error"] = "no answer urls discovered"
            self._maybe_save_debug_artifacts(question_id, question_url, manifest)
            return [], manifest

        pages: list[dict[str, Any]] = []
        batch_error = ""
        try:
            logger.info(
                "zhihu batch scrape start | qid={} | urls={} | mode={}",
                question_id,
                len(answer_urls),
                discovery.discovery_mode,
            )
            pages = self.client.batch_scrape(answer_urls, formats=["rawHtml", "markdown"])
        except Exception as exc:
            batch_error = str(exc)
            logger.warning(
                "zhihu batch scrape failed | qid={} | urls={} | error={}",
                question_id,
                len(answer_urls),
                batch_error,
            )

        page_map = {
            _normalize_answer_url(_page_source_url(page)): page
            for page in pages
            if _page_source_url(page)
        }

        items: list[dict[str, Any]] = []
        failed_urls: list[str] = []

        for answer_url in answer_urls:
            normalized_url = _normalize_answer_url(answer_url)
            page = page_map.get(normalized_url)
            item: dict[str, Any] | None = None

            if page is not None:
                item = self._parse_answer_page(
                    page,
                    question_id=question_id,
                    question_title=discovery.question_title,
                    answer_url=answer_url,
                )

            if item is None:
                item = self._retry_single_answer_fetch(
                    answer_url,
                    question_id=question_id,
                    question_title=discovery.question_title,
                )

            if item is None:
                failed_urls.append(answer_url)
                continue

            items.append(item)

        expected_count = _expected_count(
            displayed_count=discovery.displayed_count,
            discovered_count=len(answer_urls),
            max_answers=max_answers,
        )
        manifest["fetched_count"] = len(items)
        manifest["failed_count"] = len(failed_urls)
        manifest["failed_urls"] = failed_urls
        if batch_error:
            manifest["batch_error"] = batch_error
        manifest["completeness_ratio"] = round((len(items) / expected_count), 4) if expected_count else 0.0
        manifest["partial"] = bool(
            failed_urls
            or (
                discovery.displayed_count
                and len(answer_urls) < min(discovery.displayed_count, max_answers or discovery.displayed_count)
            )
        )

        if manifest["partial"]:
            self._maybe_save_debug_artifacts(question_id, question_url, manifest)

        return items, manifest

    def _crawl_single_user(
        self,
        user_token: str,
        *,
        max_items: int | None,
        crawl_answers: bool,
        crawl_articles: bool,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        profile_url = f"https://www.zhihu.com/people/{user_token}"
        manifest: dict[str, Any] = {
            "user_token": user_token,
            "profile_url": profile_url,
            "discovered_answers": 0,
            "discovered_articles": 0,
            "fetched_count": 0,
            "failed_count": 0,
            "failed_urls": [],
            "partial": False,
            "crawl_answers": crawl_answers,
            "crawl_articles": crawl_articles,
        }

        discovery: UserDiscoveryResult | None = None
        discovery_error = ""
        for attempt in range(self.config.question_retry_attempts):
            try:
                discovery = self.discover_user_content(
                    user_token,
                    max_items=max_items,
                    crawl_answers=crawl_answers,
                    crawl_articles=crawl_articles,
                )
                break
            except Exception as exc:
                discovery_error = str(exc)
                if attempt + 1 >= self.config.question_retry_attempts:
                    break

        if not discovery:
            manifest["partial"] = True
            manifest["error"] = discovery_error or "user discovery failed"
            return [], manifest

        answer_urls = discovery.answer_urls[: max_items or len(discovery.answer_urls)] if crawl_answers else []
        article_urls = discovery.article_urls[: max_items or len(discovery.article_urls)] if crawl_articles else []
        target_urls = answer_urls + article_urls
        manifest["discovered_answers"] = len(answer_urls)
        manifest["discovered_articles"] = len(article_urls)
        manifest["discovery_mode"] = discovery.discovery_mode
        manifest["page_urls"] = discovery.page_urls

        if not target_urls:
            manifest["partial"] = True
            manifest["error"] = "no answer/article urls discovered"
            return [], manifest

        pages: list[dict[str, Any]] = []
        batch_error = ""
        try:
            pages = self.client.batch_scrape(target_urls, formats=["rawHtml", "markdown"])
        except Exception as exc:
            batch_error = str(exc)

        page_map = {
            _normalize_detail_url(_page_source_url(page)): page
            for page in pages
            if _page_source_url(page)
        }

        items: list[dict[str, Any]] = []
        failed_urls: list[str] = []

        for target_url in target_urls:
            normalized_url = _normalize_detail_url(target_url)
            page = page_map.get(normalized_url)
            item: dict[str, Any] | None = None

            if page is not None:
                item = self._parse_detail_page(page, target_url=target_url)

            if item is None:
                item = self._retry_single_detail_fetch(target_url)

            if item is None:
                failed_urls.append(target_url)
                continue

            items.append(item)

        manifest["fetched_count"] = len(items)
        manifest["failed_count"] = len(failed_urls)
        manifest["failed_urls"] = failed_urls
        if batch_error:
            manifest["batch_error"] = batch_error
        manifest["partial"] = bool(failed_urls)

        return items, manifest

    def discover_question(self, question_url: str, *, max_answers: int | None = None) -> QuestionDiscoveryResult:
        browser_error = ""
        if self._should_try_browser_discovery():
            try:
                return self._discover_question_via_browser(question_url, max_answers=max_answers)
            except Exception as exc:
                browser_error = str(exc)
                self._maybe_disable_browser_discovery(browser_error)
                logger.warning(
                    "zhihu discovery browser failed, fallback to scrape | url={} | error={}",
                    question_url,
                    browser_error,
                )
        else:
            logger.info(
                "zhihu browser discovery skipped | url={} | mode={}{}",
                question_url,
                self.config.discovery_mode,
                f" | reason={self._browser_disabled_reason}" if self._browser_disabled_reason else "",
            )
        return self._discover_question_via_scrape(
            question_url,
            max_answers=max_answers,
            browser_error=browser_error,
        )

    def discover_user_content(
        self,
        user_token: str,
        *,
        max_items: int | None = None,
        crawl_answers: bool = True,
        crawl_articles: bool = True,
    ) -> UserDiscoveryResult:
        browser_error = ""
        if self._should_try_browser_discovery():
            try:
                return self._discover_user_content_via_browser(
                    user_token,
                    max_items=max_items,
                    crawl_answers=crawl_answers,
                    crawl_articles=crawl_articles,
                )
            except Exception as exc:
                browser_error = str(exc)
                self._maybe_disable_browser_discovery(browser_error)
        else:
            logger.info(
                "zhihu user browser discovery skipped | user={} | mode={}{}",
                user_token,
                self.config.discovery_mode,
                f" | reason={self._browser_disabled_reason}" if self._browser_disabled_reason else "",
            )
        return self._discover_user_content_via_scrape(
            user_token,
            max_items=max_items,
            crawl_answers=crawl_answers,
            crawl_articles=crawl_articles,
            browser_error=browser_error,
        )

    def _should_try_browser_discovery(self) -> bool:
        mode = (self.config.discovery_mode or "auto").strip().lower()
        if mode not in {"auto", "browser", "scrape"}:
            mode = "auto"
        if mode == "scrape":
            return False
        if self._browser_disabled_reason:
            return False
        return True

    def _maybe_disable_browser_discovery(self, browser_error: str) -> None:
        if not self.config.disable_browser_after_failure:
            return
        text = str(browser_error).lower()
        if (
            "failed to execute code in browser session" in text
            or "http 502" in text
            or "http 429" in text
            or "rate limit exceeded" in text
            or "execution timed out" in text
            or ("timed out" in text and "browser" in text)
        ):
            self._browser_disabled_reason = browser_error[:240]
            logger.warning(
                "zhihu browser discovery disabled for this run | reason={}",
                self._browser_disabled_reason,
            )

    def _discover_question_via_browser(
        self,
        question_url: str,
        *,
        max_answers: int | None = None,
    ) -> QuestionDiscoveryResult:
        logger.info(
            "zhihu browser discovery start | url={} | max_answers={} | timeout_s={}",
            question_url,
            max_answers or self.config.max_answers,
            self.config.crawl_timeout_s,
        )
        session = self.client.create_browser()
        session_id = str(session.get("id") or session.get("sessionId") or "")
        if not session_id:
            raise FirecrawlError(f"Firecrawl browser session 返回缺少 id: {session}")

        try:
            code = _build_discovery_script(
                question_url=question_url,
                max_scroll_rounds=self.config.max_scroll_rounds,
                idle_rounds=self.config.discovery_idle_rounds,
                max_answers=max_answers or self.config.max_answers,
            )
            result = self.client.execute_browser(session_id, code, timeout_s=self.config.crawl_timeout_s)
            payload = _parse_browser_execute_result(result)
            question_id = payload.get("questionId") or extract_question_id_from_url(question_url) or ""
            answer_urls = [
                _normalize_answer_url(urljoin(question_url, url))
                for url in payload.get("answerUrls", [])
                if "/answer/" in str(url)
            ]
            deduped_urls = list(dict.fromkeys([u for u in answer_urls if u]))
            return QuestionDiscoveryResult(
                question_id=question_id,
                question_title=str(payload.get("questionTitle") or "").strip(),
                displayed_count=_safe_int(payload.get("displayedCount")),
                answer_urls=deduped_urls,
                dismissed_modal=bool(payload.get("dismissedModal")),
                expanded_any=bool(payload.get("expandedAny")),
                page_url=str(payload.get("pageUrl") or question_url),
                raw=payload,
                discovery_mode="browser",
            )
        finally:
            try:
                self.client.delete_browser(session_id)
            except Exception:
                pass

    def _discover_question_via_scrape(
        self,
        question_url: str,
        *,
        max_answers: int | None = None,
        browser_error: str = "",
    ) -> QuestionDiscoveryResult:
        logger.info(
            "zhihu scrape discovery start | url={} | wait_ms={} | timeout_ms={}",
            question_url,
            self.config.scrape_wait_for_ms,
            self.config.scrape_timeout_ms,
        )
        page = self.client.scrape(
            question_url,
            formats=["rawHtml", "markdown"],
            wait_for_ms=self.config.scrape_wait_for_ms,
            timeout_ms=self.config.scrape_timeout_ms,
        )
        _raise_if_zhihu_verification(page, question_url)
        raw_html = str(page.get("rawHtml") or page.get("raw_html") or page.get("html") or "")
        markdown = str(page.get("markdown") or "")
        parsed = discover_answer_urls_from_question_page(
            raw_html=raw_html,
            markdown=markdown,
            question_url=question_url,
            max_answers=max_answers or self.config.max_answers,
        )
        parsed.raw["browser_error"] = browser_error
        parsed.raw["scrape_metadata"] = page.get("metadata", {})
        parsed.raw["raw_html"] = raw_html
        parsed.raw["markdown"] = markdown
        parsed.discovery_mode = "scrape"
        metadata = page.get("metadata", {}) if isinstance(page.get("metadata"), dict) else {}
        logger.info(
            "zhihu scrape discovery | url={} | title={} | final_url={} | discovered_urls={} | wait_ms={} | timeout_ms={}",
            question_url,
            metadata.get("title") or parsed.question_title or "",
            metadata.get("url") or page.get("url") or "",
            len(parsed.answer_urls),
            self.config.scrape_wait_for_ms,
            self.config.scrape_timeout_ms,
        )
        return parsed

    def _discover_user_content_via_browser(
        self,
        user_token: str,
        *,
        max_items: int | None = None,
        crawl_answers: bool = True,
        crawl_articles: bool = True,
    ) -> UserDiscoveryResult:
        session = self.client.create_browser()
        session_id = str(session.get("id") or session.get("sessionId") or "")
        if not session_id:
            raise FirecrawlError(f"Firecrawl browser session 返回缺少 id: {session}")

        user_url = f"https://www.zhihu.com/people/{user_token}"
        try:
            code = _build_user_discovery_script(
                user_url=user_url,
                max_scroll_rounds=self.config.max_scroll_rounds,
                idle_rounds=self.config.discovery_idle_rounds,
                max_items=max_items,
                crawl_answers=crawl_answers,
                crawl_articles=crawl_articles,
            )
            result = self.client.execute_browser(session_id, code, timeout_s=self.config.crawl_timeout_s)
            payload = _parse_browser_execute_result(result)
            answer_urls = [
                _normalize_answer_url(urljoin(user_url, url))
                for url in payload.get("answerUrls", [])
                if "/answer/" in str(url)
            ]
            article_urls = [
                _normalize_article_url(urljoin("https://zhuanlan.zhihu.com", url))
                for url in payload.get("articleUrls", [])
                if _is_article_url(str(url))
            ]
            return UserDiscoveryResult(
                user_token=user_token,
                answer_urls=list(dict.fromkeys([url for url in answer_urls if url])),
                article_urls=list(dict.fromkeys([url for url in article_urls if url])),
                page_urls=[str(value) for value in payload.get("pageUrls", []) if str(value).strip()],
                raw=payload,
                discovery_mode="browser",
            )
        finally:
            try:
                self.client.delete_browser(session_id)
            except Exception:
                pass

    def _discover_user_content_via_scrape(
        self,
        user_token: str,
        *,
        max_items: int | None = None,
        crawl_answers: bool = True,
        crawl_articles: bool = True,
        browser_error: str = "",
    ) -> UserDiscoveryResult:
        page_urls = []
        answer_urls: list[str] = []
        article_urls: list[str] = []
        raw: dict[str, Any] = {"browser_error": browser_error}

        if crawl_answers:
            answers_url = f"https://www.zhihu.com/people/{user_token}/answers"
            page = self.client.scrape(
                answers_url,
                formats=["rawHtml", "markdown"],
                wait_for_ms=self.config.scrape_wait_for_ms,
                timeout_ms=self.config.scrape_timeout_ms,
            )
            _raise_if_zhihu_verification(page, answers_url)
            answer_urls = discover_answer_urls_from_user_page(
                raw_html=str(page.get("rawHtml") or page.get("raw_html") or page.get("html") or ""),
                markdown=str(page.get("markdown") or ""),
                page_url=answers_url,
                max_items=max_items,
                content_type="answer",
            )
            page_urls.append(answers_url)
            raw["answers_metadata"] = page.get("metadata", {})

        if crawl_articles:
            posts_url = f"https://www.zhihu.com/people/{user_token}/posts"
            page = self.client.scrape(
                posts_url,
                formats=["rawHtml", "markdown"],
                wait_for_ms=self.config.scrape_wait_for_ms,
                timeout_ms=self.config.scrape_timeout_ms,
            )
            _raise_if_zhihu_verification(page, posts_url)
            article_urls = discover_answer_urls_from_user_page(
                raw_html=str(page.get("rawHtml") or page.get("raw_html") or page.get("html") or ""),
                markdown=str(page.get("markdown") or ""),
                page_url=posts_url,
                max_items=max_items,
                content_type="article",
            )
            page_urls.append(posts_url)
            raw["posts_metadata"] = page.get("metadata", {})

        return UserDiscoveryResult(
            user_token=user_token,
            answer_urls=answer_urls,
            article_urls=article_urls,
            page_urls=page_urls,
            raw=raw,
            discovery_mode="scrape",
        )

    def _retry_single_answer_fetch(
        self,
        answer_url: str,
        *,
        question_id: str,
        question_title: str,
    ) -> dict[str, Any] | None:
        for attempt in range(self.config.detail_retry_attempts):
            try:
                page = self.client.scrape(
                    answer_url,
                    formats=["rawHtml", "markdown"],
                    wait_for_ms=self.config.scrape_wait_for_ms,
                    timeout_ms=self.config.scrape_timeout_ms,
                )
                item = self._parse_answer_page(
                    page,
                    question_id=question_id,
                    question_title=question_title,
                    answer_url=answer_url,
                )
                if item is not None:
                    return item
            except Exception:
                if attempt + 1 >= self.config.detail_retry_attempts:
                    break
                time.sleep(2**attempt)
        return None

    def _retry_single_detail_fetch(self, target_url: str) -> dict[str, Any] | None:
        for attempt in range(self.config.detail_retry_attempts):
            try:
                page = self.client.scrape(
                    target_url,
                    formats=["rawHtml", "markdown"],
                    wait_for_ms=self.config.scrape_wait_for_ms,
                    timeout_ms=self.config.scrape_timeout_ms,
                )
                item = self._parse_detail_page(page, target_url=target_url)
                if item is not None:
                    return item
            except Exception:
                if attempt + 1 >= self.config.detail_retry_attempts:
                    break
                time.sleep(2**attempt)
        return None

    def _parse_detail_page(self, page: dict[str, Any], *, target_url: str) -> dict[str, Any] | None:
        raw_html = str(page.get("rawHtml") or page.get("raw_html") or page.get("html") or "")
        markdown = str(page.get("markdown") or "")
        if _is_article_url(target_url):
            item = parse_article_page(raw_html=raw_html, markdown=markdown, article_url=target_url)
        else:
            item = parse_answer_page(raw_html=raw_html, markdown=markdown, answer_url=target_url)
        if item is None:
            return None
        item = self.clean_pipeline.process_item(item, spider=None)
        return dict(item)

    def _parse_answer_page(
        self,
        page: dict[str, Any],
        *,
        question_id: str,
        question_title: str,
        answer_url: str,
    ) -> dict[str, Any] | None:
        raw_html = str(page.get("rawHtml") or page.get("raw_html") or page.get("html") or "")
        markdown = str(page.get("markdown") or "")
        item = parse_answer_page(
            raw_html=raw_html,
            markdown=markdown,
            answer_url=answer_url,
            question_id=question_id,
            question_title=question_title,
        )
        if item is None:
            return None
        cleaned = self.clean_pipeline.process_item(item, spider=None)
        return dict(cleaned)

    def _maybe_save_debug_artifacts(self, question_id: str, question_url: str, manifest: dict[str, Any]) -> None:
        if not self.config.debug_artifacts:
            return

        debug_dir = self.config.output_dir / "debug" / question_id
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact: dict[str, Any] = {
            "manifest": manifest,
            "question_url": question_url,
            "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        try:
            scrape = self.client.scrape(question_url, formats=["rawHtml", "screenshot"])
            artifact["screenshot"] = scrape.get("screenshot")
            raw_html = str(scrape.get("rawHtml") or scrape.get("raw_html") or "")
            if raw_html:
                html_path = debug_dir / "question.raw.html"
                html_path.write_text(raw_html, encoding="utf-8")
                artifact["raw_html_path"] = str(html_path)
        except Exception as exc:
            artifact["debug_error"] = str(exc)

        manifest_path = debug_dir / "manifest.json"
        manifest_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")


def load_question_targets(question_ids: str | None = None, questions_file: str | None = None) -> list[str]:
    """加载问题列表，支持逗号分隔参数和 YAML。"""
    if question_ids:
        return [value.strip() for value in question_ids.split(",") if value.strip()]

    if not questions_file:
        return []

    questions_path = Path(questions_file)
    if not questions_path.is_absolute() and not questions_path.exists():
        questions_path = Path("config") / questions_file

    with questions_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if isinstance(data, list):
        return [str(value).strip() for value in data if str(value).strip()]
    if isinstance(data, dict) and "questions" in data:
        return [str(value).strip() for value in data["questions"] if str(value).strip()]
    return []


def load_user_targets(user_tokens: str | None = None, users_file: str | None = None) -> list[str]:
    """加载知乎用户 token 列表，支持逗号分隔参数和 YAML。"""
    if user_tokens:
        targets = []
        for value in user_tokens.split(","):
            token = extract_user_token_from_url(value) or str(value).strip().lstrip("@")
            if token:
                targets.append(token)
        return list(dict.fromkeys(targets))

    if not users_file:
        return []

    users_path = Path(users_file)
    if not users_path.is_absolute() and not users_path.exists():
        users_path = Path("config") / users_file

    with users_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    rows: list[Any] = []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("users", "user_tokens", "tokens", "accounts", "sources"):
            if isinstance(data.get(key), list):
                rows = data[key]
                break

    targets = []
    for value in rows:
        token = extract_user_token_from_url(str(value)) or str(value).strip().lstrip("@")
        if token:
            targets.append(token)
    return list(dict.fromkeys(targets))


def parse_answer_page(
    *,
    raw_html: str,
    markdown: str,
    answer_url: str,
    question_id: str | None = None,
    question_title: str | None = None,
) -> ZhihuAnswerItem | None:
    """将 Firecrawl 返回的 rawHtml/markdown 解析为 ZhihuAnswerItem。"""
    if not raw_html and not markdown:
        return None

    selector = Selector(text=raw_html or f"<html><body><article>{markdown}</article></body></html>")
    answer_id = extract_answer_id_from_url(answer_url) or ""
    qid = question_id or extract_question_id_from_url(answer_url) or ""

    json_ld = _extract_json_ld_answer(selector, answer_id)
    content_html = ""
    content_text = ""
    author = "匿名用户"
    author_token = ""
    created_at = ""
    updated_at = ""
    voteup_count = 0
    comment_count = 0

    if json_ld:
        content_html = str(json_ld.get("content_html") or "")
        content_text = str(json_ld.get("content_text") or "")
        author = str(json_ld.get("author") or author)
        author_token = str(json_ld.get("author_token") or author_token)
        created_at = str(json_ld.get("date") or "")
        updated_at = str(json_ld.get("updated_time") or "")
        voteup_count = _safe_int(json_ld.get("voteup_count")) or 0
        comment_count = _safe_int(json_ld.get("comment_count")) or 0

    container = _find_answer_container(selector, answer_id)
    if container is not None:
        author = _first_text(
            container,
            [
                ".AuthorInfo-name::text",
                ".UserLink-link::text",
                "[itemprop='author'] [itemprop='name']::text",
            ],
            default=author,
        )
        author_href = _first_text(
            container,
            [
                ".AuthorInfo-link::attr(href)",
                ".UserLink-link::attr(href)",
                "[itemprop='author'] a::attr(href)",
            ],
            default="",
        )
        author_token = author_token or (extract_user_token_from_url(urljoin(answer_url, author_href)) or "")

        content_html = content_html or _first_text(
            container,
            [
                ".RichContent-inner",
                ".RichContent",
                ".RichText",
                ".Post-RichTextContainer",
            ],
            default="",
            html_mode=True,
        )
        if not created_at:
            created_at = _normalize_datetime(
                _first_text(
                    container,
                    [
                        "time::attr(datetime)",
                        "meta[itemprop='dateCreated']::attr(content)",
                    ],
                    default="",
                )
            )
        if not updated_at:
            updated_at = _normalize_datetime(
                _first_text(
                    container,
                    [
                        "meta[itemprop='dateModified']::attr(content)",
                    ],
                    default="",
                )
            )
        voteup_count = voteup_count or _extract_count_from_text(container.get(), [r"赞同\s*([\d,.万]+)", r"VoteUp[^0-9]*([\d,.万]+)"])
        comment_count = comment_count or _extract_count_from_text(container.get(), [r"评论\s*([\d,.万]+)"])

    question_title = (question_title or "").strip() or _extract_question_title(selector)
    excerpt = clean_zhihu_excerpt(
        _first_text(
            selector,
            [
                "meta[name='description']::attr(content)",
                "meta[property='og:description']::attr(content)",
            ],
            default="",
        )
    )

    if not content_html and markdown:
        content_text = markdown.strip()
    elif content_html and not content_text:
        content_text = html_to_text(content_html)
    elif not content_text and raw_html:
        content_text = html_to_text(raw_html)

    content_text = content_text.strip()
    if not content_text:
        return None

    item = ZhihuAnswerItem()
    item["post_id"] = answer_id
    item["record_id"] = f"zhihu:{answer_id}" if answer_id else ""
    item["author"] = author or "匿名用户"
    item["author_token"] = author_token
    item["date"] = created_at
    item["updated_time"] = updated_at
    item["url"] = answer_url
    item["content_html"] = content_html
    item["content_text"] = content_text
    item["excerpt"] = excerpt or clean_zhihu_excerpt(content_text[:140])
    item["question_id"] = qid
    item["question_title"] = question_title
    item["voteup_count"] = voteup_count
    item["like_count"] = voteup_count
    item["share_count"] = 0
    item["comment_count"] = comment_count
    item["source_platform"] = "zhihu"
    item["source_extractor"] = "firecrawl"
    item["content_type"] = "answer"
    item["tweet_type"] = "original"
    item["rt_author"] = None
    item["is_truncated"] = False
    item["clean_text"] = ""
    item["urls"] = []
    item["lang"] = "zh"
    item["translated_text"] = ""
    return item


def parse_article_page(
    *,
    raw_html: str,
    markdown: str,
    article_url: str,
) -> ZhihuArticleItem | None:
    """将 Firecrawl 返回的专栏页 rawHtml/markdown 解析为 ZhihuArticleItem。"""
    if not raw_html and not markdown:
        return None

    selector = Selector(text=raw_html or f"<html><body><article>{markdown}</article></body></html>")
    article_id = extract_article_id_from_url(article_url) or ""

    json_ld = _extract_json_ld_article(selector)
    content_html = ""
    content_text = ""
    author = "匿名用户"
    author_token = ""
    title = ""
    created_at = ""
    updated_at = ""
    voteup_count = 0
    comment_count = 0

    if json_ld:
        content_html = str(json_ld.get("content_html") or "")
        content_text = str(json_ld.get("content_text") or "")
        author = str(json_ld.get("author") or author)
        author_token = str(json_ld.get("author_token") or author_token)
        title = str(json_ld.get("title") or title)
        created_at = str(json_ld.get("date") or "")
        updated_at = str(json_ld.get("updated_time") or "")
        voteup_count = _safe_int(json_ld.get("voteup_count")) or 0
        comment_count = _safe_int(json_ld.get("comment_count")) or 0

    article_container = _find_article_container(selector)
    if article_container is not None:
        title = _first_text(
            article_container,
            [
                "h1.Post-Title::text",
                "h1.ArticleItem-Title::text",
                "h1::text",
            ],
            default=title,
        )
        author = _first_text(
            article_container,
            [
                ".AuthorInfo-name::text",
                ".UserLink-link::text",
                "[itemprop='author'] [itemprop='name']::text",
                ".PostIndex-authorName::text",
            ],
            default=author,
        )
        author_href = _first_text(
            article_container,
            [
                ".AuthorInfo-link::attr(href)",
                ".UserLink-link::attr(href)",
                "[itemprop='author'] a::attr(href)",
            ],
            default="",
        )
        author_token = author_token or (extract_user_token_from_url(urljoin(article_url, author_href)) or "")
        content_html = content_html or _first_text(
            article_container,
            [
                ".RichText.ztext",
                ".Post-RichTextContainer",
                ".RichContent-inner",
                "article",
            ],
            default="",
            html_mode=True,
        )
        if not created_at:
            created_at = _normalize_datetime(
                _first_text(
                    article_container,
                    [
                        "time::attr(datetime)",
                        "meta[itemprop='datePublished']::attr(content)",
                    ],
                    default="",
                )
            )
        if not updated_at:
            updated_at = _normalize_datetime(
                _first_text(
                    article_container,
                    [
                        "meta[itemprop='dateModified']::attr(content)",
                    ],
                    default="",
                )
            )
        voteup_count = voteup_count or _extract_count_from_text(article_container.get(), [r"赞同\s*([\d,.万]+)"])
        comment_count = comment_count or _extract_count_from_text(article_container.get(), [r"评论\s*([\d,.万]+)"])

    title = title.strip() or _first_text(
        selector,
        [
            "meta[property='og:title']::attr(content)",
            "title::text",
        ],
        default="",
    ).replace(" - 知乎", "").strip()

    if not content_html and markdown:
        content_text = markdown.strip()
    elif content_html and not content_text:
        content_text = html_to_text(content_html)
    elif not content_text and raw_html:
        content_text = html_to_text(raw_html)

    content_text = content_text.strip()
    if not content_text:
        return None

    item = ZhihuArticleItem()
    item["post_id"] = article_id
    item["record_id"] = f"zhihu:{article_id}" if article_id else ""
    item["author"] = author or "匿名用户"
    item["author_token"] = author_token
    item["date"] = created_at
    item["updated_time"] = updated_at
    item["url"] = article_url
    item["content_html"] = content_html
    item["content_text"] = content_text
    item["title"] = title
    item["voteup_count"] = voteup_count
    item["like_count"] = voteup_count
    item["share_count"] = 0
    item["comment_count"] = comment_count
    item["source_platform"] = "zhihu"
    item["source_extractor"] = "firecrawl"
    item["content_type"] = "article"
    item["tweet_type"] = "original"
    item["rt_author"] = None
    item["is_truncated"] = False
    item["clean_text"] = ""
    item["urls"] = []
    item["lang"] = "zh"
    item["translated_text"] = ""
    return item


def discover_answer_urls_from_question_page(
    *,
    raw_html: str,
    markdown: str,
    question_url: str,
    max_answers: int | None = None,
) -> QuestionDiscoveryResult:
    """从 question page 的 HTML/markdown 中发现回答链接。"""
    selector = Selector(text=raw_html or f"<html><body><article>{markdown}</article></body></html>")
    question_id = extract_question_id_from_url(question_url) or ""
    initial_state = _extract_initial_state(raw_html)
    question_title = _extract_question_title(selector)
    if not question_title and markdown:
        for line in markdown.splitlines():
            line = line.strip()
            if line.startswith("# "):
                question_title = line[2:].strip()
                break

    answer_urls: list[str] = []
    for href in selector.css("a::attr(href)").getall():
        if "/answer/" not in href:
            continue
        normalized = _normalize_answer_url(urljoin(question_url, href))
        if normalized:
            answer_urls.append(normalized)
    answer_urls.extend(
        _extract_answer_urls_from_initial_state(
            initial_state=initial_state,
            question_id=question_id,
        )
    )
    if raw_html or markdown:
        answer_urls.extend(
            _extract_answer_urls_from_text_blob(
                raw_html=raw_html,
                markdown=markdown,
                question_url=question_url,
                question_id=question_id,
            )
        )
    answer_urls = list(dict.fromkeys(answer_urls))

    if max_answers:
        answer_urls = answer_urls[:max_answers]

    displayed_count = _extract_displayed_answer_count(
        raw_html,
        markdown,
        question_id=question_id,
        initial_state=initial_state,
    )
    return QuestionDiscoveryResult(
        question_id=question_id,
        question_title=question_title,
        displayed_count=displayed_count,
        answer_urls=answer_urls,
        page_url=question_url,
        raw={
            "answer_urls_found": len(answer_urls),
            "initial_state_answer_entities": len(
                (
                    initial_state.get("entities", {}).get("answers", {})
                    if isinstance(initial_state, dict)
                    else {}
                )
            ),
        },
        discovery_mode="scrape",
    )


def _extract_answer_urls_from_text_blob(
    *,
    raw_html: str,
    markdown: str,
    question_url: str,
    question_id: str,
) -> list[str]:
    """从 markdown/raw_html 文本中补充提取回答链接（含转义 URL 与非 a 标签场景）。"""
    text = f"{raw_html}\n{markdown}".replace("\\/", "/")
    if not text.strip():
        return []

    urls: list[str] = []

    absolute = re.findall(r"https?://www\.zhihu\.com/question/\d+/answer/\d+", text)
    urls.extend(_normalize_answer_url(value) for value in absolute)

    relative_q = re.findall(r"/question/\d+/answer/\d+", text)
    urls.extend(_normalize_answer_url(urljoin(question_url, value)) for value in relative_q)

    # 某些页面只暴露 /answer/<id>，需要依赖当前问题 id 补齐。
    if question_id:
        for answer_id in re.findall(r"/answer/(\d+)", text):
            urls.append(f"https://www.zhihu.com/question/{question_id}/answer/{answer_id}")

    return [url for url in urls if url]


def _extract_initial_state(raw_html: str) -> dict[str, Any]:
    if not raw_html:
        return {}
    match = re.search(r'<script id="js-initialData" type="text/json">(.*?)</script>', raw_html, flags=re.S)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    initial_state = payload.get("initialState", {})
    return initial_state if isinstance(initial_state, dict) else {}


def _extract_answer_urls_from_initial_state(
    *,
    initial_state: dict[str, Any],
    question_id: str,
) -> list[str]:
    if not initial_state:
        return []

    entities = initial_state.get("entities", {}) if isinstance(initial_state.get("entities"), dict) else {}
    questions = entities.get("questions", {}) if isinstance(entities.get("questions"), dict) else {}
    answers = entities.get("answers", {}) if isinstance(entities.get("answers"), dict) else {}

    urls: list[str] = []

    # 优先从 answers 实体恢复 URL（知乎无明文 anchor 时的主路径）。
    for answer_id, payload in answers.items():
        answer_id_text = str(answer_id).strip()
        question_ref = ""
        if isinstance(payload, dict):
            question_obj = payload.get("question", {})
            if isinstance(question_obj, dict):
                question_ref = str(question_obj.get("id") or "").strip()
            if (not answer_id_text or not answer_id_text.isdigit()) and payload.get("url"):
                extracted = extract_answer_id_from_url(str(payload.get("url")))
                if extracted:
                    answer_id_text = extracted

        if not answer_id_text or not answer_id_text.isdigit():
            continue
        if question_id and question_ref and question_ref != question_id:
            continue

        resolved_qid = question_id or question_ref
        if not resolved_qid:
            continue
        urls.append(f"https://www.zhihu.com/question/{resolved_qid}/answer/{answer_id_text}")

    # 部分页面仅提供 answerIds（无 answers map），这里补齐。
    if question_id and not urls:
        question_payload = questions.get(question_id, {}) if isinstance(questions.get(question_id), dict) else {}
        answer_ids = question_payload.get("answerIds")
        if isinstance(answer_ids, list):
            for answer_id in answer_ids:
                aid = str(answer_id).strip()
                if aid.isdigit():
                    urls.append(f"https://www.zhihu.com/question/{question_id}/answer/{aid}")

    return [url for url in urls if url]


def discover_answer_urls_from_user_page(
    *,
    raw_html: str,
    markdown: str,
    page_url: str,
    content_type: str,
    max_items: int | None = None,
) -> list[str]:
    """从用户 answers/posts 页的 HTML/markdown 中发现内容链接。"""
    selector = Selector(text=raw_html or f"<html><body><article>{markdown}</article></body></html>")
    urls: list[str] = []
    for href in selector.css("a::attr(href)").getall():
        href = str(href)
        if content_type == "answer":
            if "/answer/" not in href:
                continue
            normalized = _normalize_answer_url(urljoin(page_url, href))
        else:
            if not _is_article_url(href):
                continue
            normalized = _normalize_article_url(urljoin("https://zhuanlan.zhihu.com", href))
        if normalized:
            urls.append(normalized)

    urls = list(dict.fromkeys(urls))
    if max_items:
        urls = urls[:max_items]
    return urls


def parse_answers_from_question_page(
    *,
    raw_html: str,
    question_url: str,
    question_id: str,
    question_title: str,
    max_answers: int | None = None,
    clean_pipeline: ZhihuCleanPipeline | None = None,
) -> list[dict[str, Any]]:
    """直接从 question page 提取已渲染的回答卡片。"""
    if not raw_html:
        return []

    selector = Selector(text=raw_html)
    cards = selector.css(".AnswerCard, .ContentItem.AnswerItem, .List-item")
    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for card in cards:
        answer_id = _extract_answer_id_from_fragment(card.get())
        if not answer_id or answer_id in seen_ids:
            continue
        seen_ids.add(answer_id)
        answer_url = f"https://www.zhihu.com/question/{question_id}/answer/{answer_id}"
        item = parse_answer_page(
            raw_html=card.get(),
            markdown="",
            answer_url=answer_url,
            question_id=question_id,
            question_title=question_title,
        )
        if item is None:
            continue
        if clean_pipeline is not None:
            item = clean_pipeline.process_item(item, spider=None)
        items.append(dict(item))
        if max_answers and len(items) >= max_answers:
            break

    return items


def _extract_json_ld_answer(selector: Selector, answer_id: str) -> dict[str, Any] | None:
    scripts = selector.css("script[type='application/ld+json']::text").getall()
    question_title = _extract_question_title(selector)
    fallback_answer: dict[str, Any] | None = None

    for script in scripts:
        try:
            data = json.loads(script)
        except json.JSONDecodeError:
            continue

        for obj in _walk_json(data):
            obj_type = obj.get("@type")
            types = obj_type if isinstance(obj_type, list) else [obj_type]
            if "Answer" not in types:
                continue

            candidate_url = str(obj.get("url") or obj.get("@id") or "")
            identifier = str(obj.get("identifier") or "")
            if answer_id and answer_id not in f"{candidate_url} {identifier}":
                if fallback_answer is None:
                    fallback_answer = _build_json_ld_answer_payload(obj, question_title)
                continue
            return _build_json_ld_answer_payload(obj, question_title)

    return fallback_answer


def _extract_json_ld_article(selector: Selector) -> dict[str, Any] | None:
    scripts = selector.css("script[type='application/ld+json']::text").getall()
    for script in scripts:
        try:
            data = json.loads(script)
        except json.JSONDecodeError:
            continue

        for obj in _walk_json(data):
            obj_type = obj.get("@type")
            types = obj_type if isinstance(obj_type, list) else [obj_type]
            if "Article" not in types and "NewsArticle" not in types and "BlogPosting" not in types:
                continue
            return _build_json_ld_article_payload(obj)

    return None


def _build_json_ld_answer_payload(obj: dict[str, Any], question_title: str) -> dict[str, Any]:
    author_data = obj.get("author", {}) if isinstance(obj.get("author"), dict) else {}
    author_url = str(author_data.get("url") or "")
    raw_text = str(obj.get("text") or "")
    content_html = raw_text if "<" in raw_text else ""
    content_text = html_to_text(raw_text) if content_html else raw_text
    return {
        "author": str(author_data.get("name") or "匿名用户"),
        "author_token": extract_user_token_from_url(author_url) or "",
        "content_html": content_html,
        "content_text": content_text.strip(),
        "date": _normalize_datetime(obj.get("dateCreated")),
        "updated_time": _normalize_datetime(obj.get("dateModified")),
        "voteup_count": _safe_int(obj.get("upvoteCount") or obj.get("upvotes")),
        "comment_count": _safe_int(obj.get("commentCount")),
        "question_title": question_title,
    }


def _build_json_ld_article_payload(obj: dict[str, Any]) -> dict[str, Any]:
    author_data = obj.get("author", {}) if isinstance(obj.get("author"), dict) else {}
    if isinstance(obj.get("author"), list) and obj["author"]:
        author_data = obj["author"][0] if isinstance(obj["author"][0], dict) else {}
    author_url = str(author_data.get("url") or "")
    raw_text = str(obj.get("articleBody") or obj.get("text") or obj.get("description") or "")
    content_html = raw_text if "<" in raw_text else ""
    content_text = html_to_text(raw_text) if content_html else raw_text
    return {
        "title": str(obj.get("headline") or obj.get("name") or ""),
        "author": str(author_data.get("name") or "匿名用户"),
        "author_token": extract_user_token_from_url(author_url) or "",
        "content_html": content_html,
        "content_text": content_text.strip(),
        "date": _normalize_datetime(obj.get("datePublished") or obj.get("dateCreated")),
        "updated_time": _normalize_datetime(obj.get("dateModified")),
        "voteup_count": _safe_int(obj.get("upvoteCount") or obj.get("interactionStatistic", {}).get("userInteractionCount") if isinstance(obj.get("interactionStatistic"), dict) else None),
        "comment_count": _safe_int(obj.get("commentCount")),
    }


def _find_answer_container(selector: Selector, answer_id: str) -> Selector | None:
    candidates = selector.css(".AnswerCard, .ContentItem.AnswerItem, .List-item, article")
    if not candidates:
        return None
    if not answer_id:
        return candidates[0]

    needle = answer_id.strip()
    needle_patterns = [f"/answer/{needle}", f'"itemId":{needle}', f'"itemId": {needle}']
    for node in candidates:
        html = node.get()
        if any(pattern in html for pattern in needle_patterns):
            return node
    return candidates[0]


def _find_article_container(selector: Selector) -> Selector | None:
    candidates = selector.css(
        "article, .Post-content, .Post-Main, .Post-RichTextContainer, .ArticleItem, .RichText"
    )
    if not candidates:
        return None
    return candidates[0]


def _extract_question_title(selector: Selector) -> str:
    title = _first_text(
        selector,
        [
            "h1.QuestionHeader-title::text",
            "h1::text",
            "meta[property='og:title']::attr(content)",
            "title::text",
        ],
        default="",
    )
    return title.replace(" - 知乎", "").strip()


def _first_text(selector: Selector, css_paths: list[str], *, default: str = "", html_mode: bool = False) -> str:
    for css_path in css_paths:
        value = selector.css(css_path).get("")
        if value:
            return value.strip() if not html_mode else value
    return default


def _normalize_answer_url(url: str) -> str:
    parsed = urlparse(url)
    normalized = parsed._replace(query="", fragment="")
    return normalized.geturl()


def _normalize_article_url(url: str) -> str:
    parsed = urlparse(url)
    normalized = parsed._replace(query="", fragment="", scheme=parsed.scheme or "https")
    if not normalized.netloc:
        normalized = normalized._replace(netloc="zhuanlan.zhihu.com")
    return normalized.geturl()


def _normalize_detail_url(url: str) -> str:
    return _normalize_article_url(url) if _is_article_url(url) else _normalize_answer_url(url)


def _is_article_url(url: str) -> bool:
    return "/p/" in str(url) or "zhuanlan.zhihu.com" in str(url)


def _page_source_url(page: dict[str, Any]) -> str:
    metadata = page.get("metadata", {}) if isinstance(page.get("metadata"), dict) else {}
    return str(page.get("url") or metadata.get("sourceURL") or metadata.get("url") or "")


def _raise_if_zhihu_verification(page: dict[str, Any], source_url: str) -> None:
    metadata = page.get("metadata", {}) if isinstance(page.get("metadata"), dict) else {}
    title = str(metadata.get("title") or "")
    final_url = str(metadata.get("url") or page.get("url") or "")
    barren_markers = ("你似乎来到了没有知识存在的荒原", "没有知识存在的荒原")
    if any(marker in title for marker in barren_markers):
        raise FirecrawlNonRetryableError(
            f"知乎问题页不可用/不存在: source={source_url} final={final_url or 'unknown'} title={title or 'unknown'}"
        )
    if "安全验证" in title or "/account/unhuman" in final_url:
        raise FirecrawlError(
            f"知乎返回安全验证页: source={source_url} final={final_url or 'unknown'} title={title or 'unknown'}"
        )


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.endswith("万"):
        try:
            return int(float(text[:-1]) * 10000)
        except ValueError:
            return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _normalize_datetime(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)) or (isinstance(value, str) and str(value).isdigit()):
        return parse_timestamp(int(value))
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            return text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text).isoformat()
        except ValueError:
            return text
    return str(value)


def _extract_count_from_text(html: str, patterns: list[str]) -> int:
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return _safe_int(match.group(1)) or 0
    return 0


def _extract_answer_id_from_fragment(fragment_html: str) -> str | None:
    patterns = [
        r"/answer/(\d+)",
        r'"itemId"\s*:\s*(\d+)',
        r'data-za-extra-module="[^"]*answer[^"]*?(\d+)',
        r'name="(\d+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, fragment_html)
        if match:
            return match.group(1)
    return None


def _extract_displayed_answer_count(
    raw_html: str,
    markdown: str,
    *,
    question_id: str = "",
    initial_state: dict[str, Any] | None = None,
) -> int | None:
    state = initial_state or _extract_initial_state(raw_html)
    if question_id and state:
        entities = state.get("entities", {}) if isinstance(state.get("entities"), dict) else {}
        questions = entities.get("questions", {}) if isinstance(entities.get("questions"), dict) else {}
        question_payload = questions.get(question_id, {}) if isinstance(questions.get(question_id), dict) else {}
        answer_count = _safe_int(question_payload.get("answerCount"))
        if answer_count is not None:
            return answer_count

    merged = f"{raw_html}\n{markdown}"
    if "暂时还没有回答" in merged:
        return 0

    for source in (raw_html, markdown):
        if not source:
            continue
        match = re.search(r"(\d[\d,]*)\s*个回答", source)
        if match:
            return _safe_int(match.group(1))
    return None


def _expected_count(displayed_count: int | None, discovered_count: int, max_answers: int | None) -> int:
    candidates = [discovered_count]
    if displayed_count:
        candidates.append(displayed_count)
    if max_answers:
        candidates.append(max_answers)
    return min(value for value in candidates if value)


def _walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_json(item)


def _parse_browser_execute_result(result: dict[str, Any]) -> dict[str, Any]:
    candidate = result.get("result") if isinstance(result, dict) else result
    if isinstance(result, dict):
        error = str(result.get("error") or result.get("stderr") or "").strip()
        if error and not candidate:
            raise FirecrawlError(f"browser execute failed: {error}")
    if isinstance(candidate, dict):
        return candidate
    if isinstance(candidate, str):
        if not candidate.strip():
            raise FirecrawlError(f"browser execute returned empty result: {result}")
        return json.loads(candidate)
    if isinstance(result, str):
        return json.loads(result)
    raise FirecrawlError(f"无法解析 browser execute 结果: {result}")


def _build_discovery_script(
    *,
    question_url: str,
    max_scroll_rounds: int,
    idle_rounds: int,
    max_answers: int | None,
) -> str:
    quoted_url = json.dumps(question_url)
    max_answers_literal = "null" if max_answers is None else int(max_answers)
    script = f"""
const questionUrl = {quoted_url};
const maxRounds = {int(max_scroll_rounds)};
const idleLimit = {int(idle_rounds)};
const maxAnswers = {max_answers_literal};
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function clickSelectors(selectors) {{
  let clicked = false;
  for (const selector of selectors) {{
    try {{
      const locator = page.locator(selector).first();
      if (await locator.count()) {{
        await locator.click({{ timeout: 1000 }});
        clicked = true;
        await wait(200);
      }}
    }} catch (err) {{}}
  }}
  return clicked;
}}

async function dismissModal() {{
  let clicked = false;
  clicked = (await clickSelectors([
    'button[aria-label="关闭"]',
    'button:has-text("关闭")',
    'button:has-text("以后再说")',
    'button:has-text("取消")',
    'button:has-text("知道了")',
    '.Modal-closeButton'
  ])) || clicked;
  try {{
    await page.keyboard.press('Escape');
  }} catch (err) {{}}
  return clicked;
}}

async function expandContent() {{
  return await clickSelectors([
    'button:has-text("阅读全文")',
    'button:has-text("展开阅读全文")',
    'button:has-text("显示全部")',
    'button:has-text("展开")',
    'div:has-text("阅读全文")',
    'span:has-text("阅读全文")'
  ]);
}}

function normalize(url) {{
  try {{
    const parsed = new URL(url, questionUrl);
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString();
  }} catch (err) {{
    return '';
  }}
}}

function collectAnswerUrls() {{
  const urls = new Set();
  const anchors = document.querySelectorAll('a[href*="/answer/"]');
  for (const anchor of anchors) {{
    const href = anchor.getAttribute('href') || '';
    if (!href.includes('/answer/')) continue;
    const normalized = normalize(href);
    if (normalized) urls.add(normalized);
  }}
  return Array.from(urls);
}}

function questionTitle() {{
  const h1 = document.querySelector('h1.QuestionHeader-title') || document.querySelector('h1');
  if (h1 && h1.textContent) return h1.textContent.trim();
  const og = document.querySelector('meta[property="og:title"]');
  if (og) return (og.getAttribute('content') || '').replace(' - 知乎', '').trim();
  return document.title.replace(' - 知乎', '').trim();
}}

function displayedCount() {{
  const text = document.body ? document.body.innerText : '';
  const match = text.match(/([\\d,]+)\\s*个回答/);
  if (!match) return null;
  return Number.parseInt(match[1].replace(/,/g, ''), 10);
}}

await page.goto(questionUrl, {{ waitUntil: 'domcontentloaded', timeout: 60000 }});
await wait({DISCOVERY_INITIAL_WAIT_MS});
let dismissedModal = await dismissModal();
let expandedAny = false;
let idleRounds = 0;
let previousCount = 0;
const discovered = new Set();

for (let round = 0; round < maxRounds; round += 1) {{
  if (await expandContent()) {{
    expandedAny = true;
  }}
  const urls = collectAnswerUrls();
  for (const url of urls) discovered.add(url);
  if (maxAnswers && discovered.size >= maxAnswers) break;
  if (discovered.size === previousCount) {{
    idleRounds += 1;
  }} else {{
    idleRounds = 0;
    previousCount = discovered.size;
  }}
  if (idleRounds >= idleLimit) break;
  await page.mouse.wheel(0, 2000);
  await wait({DISCOVERY_WAIT_MS});
  dismissedModal = (await dismissModal()) || dismissedModal;
}}

return JSON.stringify({{
  questionId: (page.url().match(/question\\/(\\d+)/) || [])[1] || '',
  questionTitle: questionTitle(),
  displayedCount: displayedCount(),
  answerUrls: Array.from(discovered),
  dismissedModal,
  expandedAny,
  pageUrl: page.url()
}});
""".strip()
    return f"await (async () => {{\n{script}\n}})()"


def _build_user_discovery_script(
    *,
    user_url: str,
    max_scroll_rounds: int,
    idle_rounds: int,
    max_items: int | None,
    crawl_answers: bool,
    crawl_articles: bool,
) -> str:
    quoted_url = json.dumps(user_url.rstrip("/"))
    max_items_literal = "null" if max_items is None else int(max_items)
    crawl_answers_literal = "true" if crawl_answers else "false"
    crawl_articles_literal = "true" if crawl_articles else "false"
    script = f"""
const userUrl = {quoted_url};
const maxRounds = {int(max_scroll_rounds)};
const idleLimit = {int(idle_rounds)};
const maxItems = {max_items_literal};
const crawlAnswers = {crawl_answers_literal};
const crawlArticles = {crawl_articles_literal};
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function clickSelectors(selectors) {{
  let clicked = false;
  for (const selector of selectors) {{
    try {{
      const locator = page.locator(selector).first();
      if (await locator.count()) {{
        await locator.click({{ timeout: 1000 }});
        clicked = true;
        await wait(200);
      }}
    }} catch (err) {{}}
  }}
  return clicked;
}}

async function dismissModal() {{
  let clicked = false;
  clicked = (await clickSelectors([
    'button[aria-label="关闭"]',
    'button:has-text("关闭")',
    'button:has-text("以后再说")',
    'button:has-text("取消")',
    'button:has-text("知道了")',
    '.Modal-closeButton'
  ])) || clicked;
  try {{
    await page.keyboard.press('Escape');
  }} catch (err) {{}}
  return clicked;
}}

function normalizeAnswer(url) {{
  try {{
    const parsed = new URL(url, userUrl);
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString();
  }} catch (err) {{
    return '';
  }}
}}

function normalizeArticle(url) {{
  try {{
    const parsed = new URL(url, 'https://zhuanlan.zhihu.com');
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString();
  }} catch (err) {{
    return '';
  }}
}}

function collectAnswerUrls() {{
  const urls = new Set();
  const anchors = document.querySelectorAll('a[href*="/answer/"]');
  for (const anchor of anchors) {{
    const href = anchor.getAttribute('href') || '';
    if (!href.includes('/answer/')) continue;
    const normalized = normalizeAnswer(href);
    if (normalized) urls.add(normalized);
  }}
  return Array.from(urls);
}}

function collectArticleUrls() {{
  const urls = new Set();
  const anchors = document.querySelectorAll('a[href*="/p/"]');
  for (const anchor of anchors) {{
    const href = anchor.getAttribute('href') || '';
    if (!href.includes('/p/')) continue;
    const normalized = normalizeArticle(href);
    if (normalized) urls.add(normalized);
  }}
  return Array.from(urls);
}}

async function collectFromPage(targetUrl, collectFn) {{
  await page.goto(targetUrl, {{ waitUntil: 'domcontentloaded', timeout: 60000 }});
  await wait({DISCOVERY_INITIAL_WAIT_MS});
  await dismissModal();
  let idleRounds = 0;
  let previousCount = 0;
  const discovered = new Set();

  for (let round = 0; round < maxRounds; round += 1) {{
    const urls = collectFn();
    for (const url of urls) discovered.add(url);
    if (maxItems && discovered.size >= maxItems) break;
    if (discovered.size === previousCount) {{
      idleRounds += 1;
    }} else {{
      idleRounds = 0;
      previousCount = discovered.size;
    }}
    if (idleRounds >= idleLimit) break;
    await page.mouse.wheel(0, 2000);
    await wait({DISCOVERY_WAIT_MS});
    await dismissModal();
  }}

  return Array.from(discovered);
}}

const answerPageUrl = `${{userUrl}}/answers`;
const articlePageUrl = `${{userUrl}}/posts`;
const pageUrls = [];
let answerUrls = [];
let articleUrls = [];

if (crawlAnswers) {{
  pageUrls.push(answerPageUrl);
  answerUrls = await collectFromPage(answerPageUrl, collectAnswerUrls);
}}

if (crawlArticles) {{
  pageUrls.push(articlePageUrl);
  articleUrls = await collectFromPage(articlePageUrl, collectArticleUrls);
}}

return JSON.stringify({{
  userToken: (userUrl.match(/people\\/([^/]+)/) || [])[1] || '',
  pageUrls,
  answerUrls,
  articleUrls,
}});
""".strip()
    return f"await (async () => {{\n{script}\n}})()"


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _unwrap_firecrawl_data(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Firecrawl 常见返回形态：
    - {"success": true, "data": {...}}
    - {"success": true, "data": {"data": [...], "status": "..."}}
    - 直接返回业务字段
    """
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        inner = payload["data"]
        if payload.keys() <= {"success", "data", "warning"} or "success" in payload:
            return inner
    return payload
