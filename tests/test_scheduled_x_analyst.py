import asyncio
import inspect
import sys
from pathlib import Path

import pytest

import scheduled


def _call_cmd_x_analyze(cmd):
    """Call cmd_x_analyze with only supported kwargs to keep tests shape-tolerant."""
    sig = inspect.signature(cmd)
    candidates = {
        "window": 3,
        "max_age_days": None,
        "sources": "twitter",
        "model": "gpt-4o-mini",
        "head_model": None,
        "batch_model": None,
        "batch_size": 2,
        "max_concurrent": 2,
        "max_api_calls": 8,
        "llm_max_retries": 2,
        "synth_group_size": 2,
        "api_key": "test-key",
        "base_url": "https://example.com/v1",
        "limit": None,
    }
    kwargs = {k: v for k, v in candidates.items() if k in sig.parameters}
    result = cmd(**kwargs)
    if inspect.iscoroutine(result):
        return asyncio.run(result)
    return result


def _install_fake_x_runtime(monkeypatch):
    records = {"posts": None, "client_kwargs": None}

    class FakeModelConfig:
        def __init__(self, head_model="gpt-4o-mini", batch_model="gpt-4o-mini"):
            self.head_model = head_model
            self.batch_model = batch_model

    class FakeClient:
        def __init__(self, **kwargs):
            records["client_kwargs"] = kwargs

    def fake_resolve_model_config(model=None, head_model=None, batch_model=None):
        return FakeModelConfig(
            head_model=head_model or model or "gpt-4o-mini",
            batch_model=batch_model or model or "gpt-4o-mini",
        )

    def fake_resolve_api_key(explicit_api_key, _model_config):
        return explicit_api_key or "test-key"

    def fake_resolve_base_url(explicit_base_url, _model_config):
        return explicit_base_url or "https://example.com/v1"

    async def fake_analyze_x_posts(
        posts,
        client,
        model_config=None,
        batch_size=20,
        max_concurrent=5,
        max_api_calls=None,
        llm_max_retries=3,
        synth_group_size=5,
    ):
        records["posts"] = posts
        return "FAKE_X_SUMMARY"

    # Preferred path: patch scheduled-side loader helper (if implementation provides it).
    loader_names = [
        "_load_x_analyst_modules",
        "_load_x_analyst_helpers",
        "_load_x_analyst_runtime",
    ]
    for name in loader_names:
        if hasattr(scheduled, name):
            monkeypatch.setattr(
                scheduled,
                name,
                lambda: (
                    fake_analyze_x_posts,
                    fake_resolve_model_config,
                    fake_resolve_api_key,
                    fake_resolve_base_url,
                    FakeClient,
                ),
            )

    # Fallback path: patch direct names if cmd_x_analyze imports/binds directly.
    monkeypatch.setattr(scheduled, "analyze_x_posts", fake_analyze_x_posts, raising=False)
    monkeypatch.setattr(scheduled, "_resolve_model_config", fake_resolve_model_config, raising=False)
    monkeypatch.setattr(scheduled, "_resolve_api_key", fake_resolve_api_key, raising=False)
    monkeypatch.setattr(scheduled, "_resolve_base_url", fake_resolve_base_url, raising=False)
    monkeypatch.setattr(scheduled, "AsyncOpenAI", FakeClient, raising=False)

    return records


def test_cmd_x_analyze_merges_text_priority_and_handles_summary_artifact(monkeypatch, tmp_path):
    assert hasattr(scheduled, "cmd_x_analyze"), "scheduled.py should define cmd_x_analyze(...)"

    items = [
        {
            "post_id": "1",
            "author": "alice",
            "translated_text": "translated-1",
            "clean_text": "clean-1",
            "content_text": "raw-1",
        },
        {
            "post_id": "2",
            "author": "bob",
            "translated_text": "",
            "clean_text": "clean-2",
            "content_text": "raw-2",
        },
        {
            "post_id": "3",
            "author": "carol",
            "translated_text": None,
            "clean_text": "",
            "content_text": "raw-3",
        },
    ]

    monkeypatch.setattr(
        scheduled,
        "_load_snapshots",
        lambda window, max_age_days=None, sources="*": (["doc-a", "doc-b", "doc-c"], items),
    )

    records = _install_fake_x_runtime(monkeypatch)

    saved = {}

    def fake_save_json(payload, out_path):
        saved["payload"] = payload
        saved["path"] = Path(out_path)

    monkeypatch.setattr(scheduled, "save_json", fake_save_json, raising=False)

    result = _call_cmd_x_analyze(scheduled.cmd_x_analyze)

    assert records["posts"] is not None, "cmd_x_analyze should pass merged posts into XAnalyst"
    assert [post["content_text"] for post in records["posts"]] == [
        "translated-1",
        "clean-2",
        "raw-3",
    ]

    # Shape-tolerant assertion:
    # - either function returns summary directly
    # - or function persists artifact via save_json
    if isinstance(result, str):
        assert "SUMMARY" in result
    else:
        assert "payload" in saved or result is None

    if "payload" in saved:
        payload_text = str(saved["payload"])
        assert "FAKE_X_SUMMARY" in payload_text


def test_main_dispatches_x_analyze_command(monkeypatch):
    assert hasattr(scheduled, "main")

    called = {"count": 0}

    def fake_cmd_x_analyze(*args, **kwargs):
        called["count"] += 1
        return "ok"

    monkeypatch.setattr(scheduled, "cmd_x_analyze", fake_cmd_x_analyze, raising=False)
    monkeypatch.setattr(sys, "argv", ["scheduled.py", "x-analyze"])

    result = scheduled.main()

    assert called["count"] == 1, "main() should dispatch x-analyze to cmd_x_analyze(...)"
    assert result in (None, 0, "ok")

