from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING

from pipeline_builders import build_embedding_model, build_topic_model
from pipeline_config import parse_config
from pipeline_io import load_docs, save_artifacts, save_visualizations

if TYPE_CHECKING:
    from pipeline_config import PipelineConfig

# ─── 计时工具 ────────────────────────────────────────────────────────────────

_stage_start: float = 0.0
_pipeline_start: float = 0.0


def _log(message: str, quiet: bool = False) -> None:
    if quiet:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def _stage_begin(name: str, quiet: bool = False) -> None:
    global _stage_start
    _stage_start = time.perf_counter()
    _log(f"▶ START  {name}", quiet=quiet)


def _stage_end(name: str, quiet: bool = False) -> float:
    elapsed = time.perf_counter() - _stage_start
    _log(f"✔ DONE   {name}  ({elapsed:.2f}s)", quiet=quiet)
    return elapsed


# ─── 核心 Pipeline 逻辑（可被外部调用）────────────────────────────────────────

def run_pipeline(config: PipelineConfig, quiet: bool = False) -> None:
    """
    执行完整的 BERTopic pipeline。

    Args:
        config: PipelineConfig 配置对象
        quiet: 为 True 时抑制打印输出（适合被其他模块调用）
    """
    global _pipeline_start
    _pipeline_start = time.perf_counter()

    # ── 诊断：打印关键配置，方便排查性能参数 ──
    _log("=" * 60, quiet=quiet)
    _log("PIPELINE CONFIG SUMMARY", quiet=quiet)
    _log(f"  cluster_backend  : {config.cluster_backend}", quiet=quiet)
    _log(f"  n_clusters       : {config.n_clusters}  (kmeans only)", quiet=quiet)
    _log(f"  umap_n_neighbors : {config.umap_n_neighbors}", quiet=quiet)
    _log(f"  umap_min_dist    : {config.umap_min_dist}", quiet=quiet)
    _log(f"  top_n_words      : {config.top_n_words}  ← 越大 KeyBERT 越慢", quiet=quiet)
    _log(f"  llm_nr_docs      : {config.llm_nr_docs}  (LLM 每 topic 使用的文档数)", quiet=quiet)
    _log(f"  enable_llm       : {config.enable_llm}", quiet=quiet)
    _log(f"  show_progress_bar: {config.show_progress_bar}", quiet=quiet)
    _log("=" * 60, quiet=quiet)

    # ── Stage 1: 加载数据 ──
    _stage_begin("Load documents", quiet=quiet)
    docs = load_docs(config.input_json)
    _stage_end("Load documents", quiet=quiet)
    avg_len = sum(len(d) for d in docs) // len(docs) if docs else 0
    _log(f"  → {len(docs)} documents loaded, avg length {avg_len} chars", quiet=quiet)

    # ── Stage 2: 加载 embedding 模型 ──
    _stage_begin("Load embedding model", quiet=quiet)
    embedding_model = build_embedding_model(config)
    _stage_end("Load embedding model", quiet=quiet)

    # ── Stage 3: 编码文档（embed） ──
    _stage_begin("Encode documents (embedding)", quiet=quiet)
    embeddings = embedding_model.encode(docs, show_progress_bar=config.show_progress_bar)
    t_embed = _stage_end("Encode documents (embedding)", quiet=quiet)
    if docs:
        _log(f"  → embeddings shape: {embeddings.shape}, {t_embed/len(docs)*1000:.1f} ms/doc", quiet=quiet)

    # ── Stage 4: 构建 BERTopic 流水线 ──
    _stage_begin("Build BERTopic pipeline", quiet=quiet)
    topic_model, llm_enabled = build_topic_model(config, embedding_model)
    _stage_end("Build BERTopic pipeline", quiet=quiet)
    if config.enable_llm and not llm_enabled:
        _log("  ⚠ LLM requested but disabled — OPENAI_API_KEY not found", quiet=quiet)

    # ── Stage 5: fit_transform（最耗时阶段，内部细分） ──
    _log("─" * 60, quiet=quiet)
    _log("▶ START  fit_transform  ← 通常最慢，各子阶段说明：", quiet=quiet)
    _log("         5a. UMAP 降维        — n_neighbors 越大越慢", quiet=quiet)
    _log("         5b. 聚类             — KMeans 比 HDBSCAN 快", quiet=quiet)
    _log("         5c. c-TF-IDF        — 通常很快", quiet=quiet)
    _log(f"         5d. KeyBERTInspired — top_n_words={config.top_n_words}, 这是慢的主因之一", quiet=quiet)
    if llm_enabled:
        _log(f"         5e. LLM 调用        — nr_docs={config.llm_nr_docs}, 网络延迟会累加", quiet=quiet)
    _log("─" * 60, quiet=quiet)

    # 开启 BERTopic 内部 verbose 输出，方便看到子阶段卡在哪里
    topic_model.verbose = not quiet

    t0 = time.perf_counter()
    topics, probs = topic_model.fit_transform(docs, embeddings)
    t_fit = time.perf_counter() - t0

    topic_model.verbose = False
    _log("─" * 60, quiet=quiet)
    _log(f"✔ DONE   fit_transform  ({t_fit:.2f}s total)", quiet=quiet)
    n_topics_found = len(set(topics)) - (1 if -1 in topics else 0)
    _log(f"  → {n_topics_found} topics found", quiet=quiet)
    outlier_count = sum(1 for t in topics if t == -1)
    if outlier_count:
        pct = outlier_count / len(docs) * 100 if docs else 0
        _log(f"  → {outlier_count} outlier documents (topic=-1, {pct:.1f}%)", quiet=quiet)

    # 性能警告
    if t_fit > 60 and not quiet:
        _log("  ⚠ SLOW WARNING: fit_transform 超过 60s，建议检查：", quiet=quiet)
        _log(f"    - top_n_words={config.top_n_words} → 尝试降到 10~15", quiet=quiet)
        _log(f"    - umap_n_neighbors={config.umap_n_neighbors} → 尝试降到 5~10", quiet=quiet)
        if llm_enabled:
            _log(f"    - LLM 已启用 (nr_docs={config.llm_nr_docs}) → 尝试 --llm-nr-docs 3 或 --disable-llm 对比", quiet=quiet)

    # ── Stage 6: 保存产物 ──
    _stage_begin("Save artifacts", quiet=quiet)
    save_artifacts(topic_model, docs, topics, probs, config, llm_enabled)
    _stage_end("Save artifacts", quiet=quiet)

    if config.save_visualizations:
        _stage_begin("Save HTML visualizations", quiet=quiet)
        save_visualizations(topic_model, config.output_dir, docs)
        _stage_end("Save HTML visualizations", quiet=quiet)

    if config.save_model:
        _stage_begin(f"Save model → {config.model_output_path}", quiet=quiet)
        topic_model.save(
            str(config.model_output_path),
            serialization="safetensors",
            save_ctfidf=True,
            save_embedding_model=False,
        )
        _stage_end("Save model", quiet=quiet)

    total = time.perf_counter() - _pipeline_start
    _log("=" * 60, quiet=quiet)
    _log(f"✅ Pipeline completed successfully.  Total: {total:.2f}s", quiet=quiet)


# ─── CLI 入口（命令行调用）───────────────────────────────────────────────────

def main() -> None:
    """命令行入口，解析参数并执行 pipeline。"""
    config = parse_config()
    run_pipeline(config, quiet=False)


if __name__ == "__main__":
    main()
