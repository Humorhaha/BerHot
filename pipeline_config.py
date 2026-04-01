from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


DEFAULT_SUMMARY_PROMPT = """
你会收到一个主题的关键词和代表文档。
请输出“恰好一句话”总结该主题核心内容。
输出语言和[DOCUMENTS]严格保持一致。如果输入是英文, 则全部用英语输出.
不要标题，不要列表，不要解释。

关键词:
[KEYWORDS]

代表文档:
[DOCUMENTS]

按下面格式输出：
“a short label of the topic”: <one-sentence summary>

""".strip()


@dataclass
class PipelineConfig:
    project_dir: Path
    input_json: Path
    output_dir: Path
    embedding_model_name: str
    embedding_model_path: Optional[Path]
    prefer_local_embedding: bool
    cluster_backend: str
    n_clusters: int
    min_cluster_size: int
    min_samples: int
    umap_n_neighbors: int
    umap_min_dist: float
    vectorizer_max_df: float
    stop_words: str
    top_n_words: int
    mmr_diversity: float
    random_state: int
    show_progress_bar: bool
    enable_llm: bool
    llm_model: str
    llm_prompt: str
    llm_nr_docs: int
    llm_diversity: float
    openai_api_key: Optional[str]
    openai_base_url: Optional[str]
    save_model: bool
    model_output_path: Path
    save_visualizations: bool


def _build_parser(default_project_dir: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train BERTopic pipeline and export topic/document analysis artifacts."
    )
    parser.add_argument(
        "--input-json",
        type=Path,
        default=default_project_dir / "data" / "technologyreview_texts.json",
        help="Input JSON path (expects a top-level `texts` array).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_project_dir,
        help="Output directory for CSV/JSON/HTML artifacts.",
    )
    parser.add_argument("--embedding-model-name", default="all-MiniLM-L6-v2")
    parser.add_argument(
        "--embedding-model-path",
        type=Path,
        default=default_project_dir / "models" / "all-MiniLM-L6-v2",
        help="Local embedding model path. Falls back to model name if this path does not exist.",
    )
    parser.add_argument(
        "--prefer-local-embedding",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Try loading local SentenceTransformer files first (use --no-prefer-local-embedding to disable).",
    )
    parser.add_argument(
        "--cluster-backend",
        choices=["kmeans", "hdbscan"],
        default="kmeans",
        help="Clustering backend used in BERTopic.",
    )
    # ── 聚类参数 ──────────────────────────────────────────────────────────────
    # n_clusters=15: 单快照约1700~1800条文档，AI/科技领域话题域宽
    #   (LLM/模型/Agent/机器人/研究/应用/代码/多模态/安全等至少8大类，细分后约15个热点最合适)
    #   低于10会漏覆盖热点，高于20会过度碎片化
    parser.add_argument("--n-clusters", type=int, default=8)
    parser.add_argument("--min-cluster-size", type=int, default=8)
    parser.add_argument("--min-samples", type=int, default=5)

    # ── UMAP 参数 ─────────────────────────────────────────────────────────────
    parser.add_argument("--umap-n-neighbors", type=int, default=5)
    parser.add_argument("--umap-min-dist", type=float, default=0.0)


    parser.add_argument("--vectorizer-max-df", type=float, default=0.85)
    parser.add_argument("--stop-words", default="english")

    parser.add_argument("--top-n-words", type=int, default=5)
    parser.add_argument("--mmr-diversity", type=float, default=0.5)
    parser.add_argument("--random-state", type=int, default=0)
    parser.add_argument("--show-progress-bar", action="store_true", default=False)

    parser.add_argument("--enable-llm", dest="enable_llm", action="store_true", default=True)
    parser.add_argument("--disable-llm", dest="enable_llm", action="store_false")
    parser.add_argument("--llm-model", default="qwen-plus")
    parser.add_argument("--llm-prompt", default=DEFAULT_SUMMARY_PROMPT)
    parser.add_argument("--llm-nr-docs", type=int, default=5)
    parser.add_argument("--llm-diversity", type=float, default=0.7)
    parser.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    parser.add_argument("--openai-base-url", default=os.getenv("OPENAI_URL"))

    parser.add_argument("--save-model", action="store_true", default=False)
    parser.add_argument(
        "--model-output-path",
        type=Path,
        default=default_project_dir / "topic_model",
    )
    parser.add_argument("--save-visualizations", action="store_true", default=False)
    return parser


def parse_config(argv: Optional[list[str]] = None) -> PipelineConfig:
    project_dir = Path(__file__).resolve().parent
    # 必须在 _build_parser 之前加载 .env，否则 argparse default= 中的
    # os.getenv() 会在环境变量注入前求值，导致拿到 None
    load_dotenv(dotenv_path=project_dir / ".env", override=True)
    parser = _build_parser(project_dir)
    args = parser.parse_args(argv)

    embedding_model_path = args.embedding_model_path
    if embedding_model_path and not embedding_model_path.exists():
        embedding_model_path = None

    return PipelineConfig(
        project_dir=project_dir,
        input_json=args.input_json,
        output_dir=args.output_dir,
        embedding_model_name=args.embedding_model_name,
        embedding_model_path=embedding_model_path,
        prefer_local_embedding=args.prefer_local_embedding,
        cluster_backend=args.cluster_backend,
        n_clusters=args.n_clusters,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
        umap_n_neighbors=args.umap_n_neighbors,
        umap_min_dist=args.umap_min_dist,
        vectorizer_max_df=args.vectorizer_max_df,
        stop_words=args.stop_words,
        top_n_words=args.top_n_words,
        mmr_diversity=args.mmr_diversity,
        random_state=args.random_state,
        show_progress_bar=args.show_progress_bar,
        enable_llm=args.enable_llm,
        llm_model=args.llm_model,
        llm_prompt=args.llm_prompt,
        llm_nr_docs=args.llm_nr_docs,
        llm_diversity=args.llm_diversity,
        openai_api_key=args.openai_api_key,
        openai_base_url=args.openai_base_url,
        save_model=args.save_model,
        model_output_path=args.model_output_path,
        save_visualizations=args.save_visualizations,
    )

if __name__ == "__main__":
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=True)
    key = os.getenv("OPENAI_API_KEY")
    print(f"API_KEY:{key}")