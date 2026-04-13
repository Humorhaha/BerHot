import asyncio
import json

import pytest

import agent_tree.x_analyst as x_analyst_module
from agent_tree.x_analyst import (
    XAnalyst,
    analyze_x_posts,
    load_snapshot_posts,
    _resolve_api_key,
    _resolve_base_url,
    _resolve_model_config,
)


def _make_post(author: str, text: str, post_id: str, date: str = "2026-04-11T08:00:00+00:00") -> dict:
    return {
        "post_id": post_id,
        "author": author,
        "content_text": text,
        "date": date,
    }


class TestXAnalystBatching:

    def test_split_batches_rebalances_authors(self):
        posts = [
            _make_post("alice", f"Alice post {i}", f"a-{i}")
            for i in range(4)
        ] + [
            _make_post("bob", f"Bob post {i}", f"b-{i}")
            for i in range(4)
        ]

        analyst = XAnalyst(posts=posts, client=object(), batch_size=2)
        batches = analyst._split_batches()

        assert len(batches) == 4
        for batch in batches:
            authors = {post["author"] for post in batch}
            assert authors == {"alice", "bob"}

    def test_max_api_calls_expands_batch_size(self):
        posts = [_make_post("alice", f"post {i}", str(i)) for i in range(10)]

        analyst = XAnalyst(
            posts=posts,
            client=object(),
            batch_size=2,
            max_api_calls=6,
            llm_max_retries=2,
        )

        assert analyst._effective_batch_size == 5
        assert analyst._planned_batch_count == 2
        assert analyst._planned_logical_calls == 3

    def test_max_api_calls_too_small_raises(self):
        posts = [_make_post("alice", "hello", "1")]

        with pytest.raises(ValueError):
            XAnalyst(
                posts=posts,
                client=object(),
                max_api_calls=1,
                llm_max_retries=1,
            )

    def test_max_api_calls_accounts_for_multilevel_synthesis(self):
        posts = [_make_post("alice", f"post {i}", str(i)) for i in range(10)]

        analyst = XAnalyst(
            posts=posts,
            client=object(),
            batch_size=2,
            max_api_calls=6,
            llm_max_retries=1,
            synth_group_size=2,
        )

        assert analyst._planned_logical_calls <= 6


class TestAnalyzeXPosts:

    def test_analyze_x_posts_no_longer_requires_query(self, monkeypatch):
        calls = []

        async def fake_llm_json(client, system, user, model="gpt-4o-mini", max_retries=3, **kwargs):
            calls.append(
                {
                    "system": system,
                    "user": user,
                    "model": model,
                    "max_retries": max_retries,
                    **kwargs,
                }
            )
            if "one_sentence_summary" in user:
                return {"one_sentence_summary": "讨论集中在 AI 安全合作与产业落地。"}
            return {
                "key_opinions": "多数帖子把话题集中在政府与企业合作推进 AI 安全。",
                "sentiment": "mixed",
                "top_topics": ["AI 安全", "政府合作"],
                "notable_quote": "We signed an MOU with the Australian Government.",
            }

        monkeypatch.setattr(x_analyst_module, "llm_json", fake_llm_json)

        posts = [
            _make_post("alice", "We signed an MOU with the Australian Government.", "1"),
            _make_post("bob", "AI safety needs industry cooperation.", "2"),
            _make_post("carol", "   ", "3"),
        ]

        result = asyncio.run(analyze_x_posts(posts=posts, client=object(), batch_size=10))

        assert result == "讨论集中在 AI 安全合作与产业落地。"
        assert len(calls) == 2
        assert "没有外部 query" in calls[0]["system"]
        assert "数据集画像" in calls[1]["user"]
        assert "样本量 2 条" in calls[1]["user"]
        assert all(call.get("fail_fast_on_auth") is True for call in calls)

    def test_run_respects_hard_api_budget_including_retries(self, monkeypatch):
        responses = iter([
            {},
            {
                "key_opinions": "帖子围绕模型发布后的体验反馈展开。",
                "sentiment": "mixed",
                "top_topics": ["模型发布"],
                "notable_quote": "The launch is impressive but still rough.",
            },
            {},
            {"one_sentence_summary": "讨论聚焦模型发布后的效果认可与稳定性质疑并存。"},
        ])
        call_count = 0

        async def fake_llm_json(client, system, user, model="gpt-4o-mini", max_retries=3, **kwargs):
            nonlocal call_count
            call_count += 1
            return next(responses)

        monkeypatch.setattr(x_analyst_module, "llm_json", fake_llm_json)

        posts = [
            _make_post("alice", "The launch is impressive but still rough.", "1"),
            _make_post("bob", "Latency is down, but reliability is not there yet.", "2"),
        ]

        result = asyncio.run(
            analyze_x_posts(
                posts=posts,
                client=object(),
                batch_size=10,
                max_api_calls=4,
                llm_max_retries=2,
            )
        )

        assert result == "讨论聚焦模型发布后的效果认可与稳定性质疑并存。"
        assert call_count == 4

    def test_synthesize_runs_in_multiple_levels(self, monkeypatch):
        synth_call_count = 0

        async def fake_llm_json(client, system, user, model="gpt-4o-mini", max_retries=3, **kwargs):
            nonlocal synth_call_count
            if "one_sentence_summary" in user:
                synth_call_count += 1
                return {"one_sentence_summary": f"synth-{synth_call_count}"}
            return {
                "key_opinions": "多数帖子围绕同一事件展开讨论。",
                "sentiment": "mixed",
                "top_topics": ["事件进展"],
                "notable_quote": "sample quote",
            }

        monkeypatch.setattr(x_analyst_module, "llm_json", fake_llm_json)

        posts = [_make_post(f"user{i}", f"post {i}", str(i)) for i in range(9)]
        result = asyncio.run(
            analyze_x_posts(
                posts=posts,
                client=object(),
                batch_size=2,
                synth_group_size=2,
            )
        )

        assert synth_call_count > 1
        assert result == f"synth-{synth_call_count}"


class TestXAnalystCliHelpers:

    def test_load_snapshot_posts_from_standard_payload(self, tmp_path):
        snapshot = tmp_path / "twitter.json"
        snapshot.write_text(
            json.dumps(
                {
                    "site": "X / Twitter",
                    "texts": [
                        _make_post("alice", "hello world", "1"),
                        _make_post("bob", "another post", "2"),
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        posts = load_snapshot_posts(snapshot)

        assert len(posts) == 2
        assert posts[0]["author"] == "alice"

    def test_resolve_model_config_allows_layer_override(self):
        cfg = _resolve_model_config(
            model="qwen-plus",
            head_model="qwen-max",
            batch_model="qwen-turbo",
        )

        assert cfg.platform_model == "qwen-plus"
        assert cfg.meeting_model == "qwen-plus"
        assert cfg.head_model == "qwen-max"
        assert cfg.batch_model == "qwen-turbo"

    def test_resolve_api_key_prefers_minimax_for_minimax_models(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "openai-k")
        monkeypatch.setenv("MINIMAX_API_KEY", " minimax-k ")
        cfg = _resolve_model_config(model="MiniMax-M2.5", head_model=None, batch_model=None)

        api_key = _resolve_api_key(None, cfg)

        assert api_key == "minimax-k"

    def test_resolve_base_url_uses_minimax_default_for_minimax_models(self, monkeypatch):
        monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
        monkeypatch.delenv("OPENAI_URL", raising=False)
        monkeypatch.delenv("MINIMAX_BASE_URL", raising=False)
        cfg = _resolve_model_config(model="MiniMax-M2.5", head_model=None, batch_model=None)

        base_url = _resolve_base_url(None, cfg)

        assert base_url == "https://api.minimax.io/v1"
