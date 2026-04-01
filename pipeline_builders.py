from __future__ import annotations

from typing import Any, Tuple

import hdbscan
import openai
import umap
from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, OpenAI
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import CountVectorizer

from pipeline_config import PipelineConfig


def build_embedding_model(config: PipelineConfig) -> SentenceTransformer:
    if config.embedding_model_path is not None:
        return SentenceTransformer(str(config.embedding_model_path))

    if config.prefer_local_embedding:
        try:
            return SentenceTransformer(config.embedding_model_name, local_files_only=True)
        except OSError:
            pass
    return SentenceTransformer(config.embedding_model_name)


def build_umap_model(config: PipelineConfig) -> umap.UMAP:
    # n_components=5：BERTopic 官方推荐的聚类维度。
    # 原先缺失此参数，UMAP 默认 n_components=2（适合可视化但损失聚类精度）。
    # metric="cosine"：sentence-transformer 输出的向量用余弦相似度更合适，
    # 原先缺失此参数，UMAP 默认 metric="euclidean"，对高维 embedding 效果差。
    return umap.UMAP(
        n_components=5,
        n_neighbors=config.umap_n_neighbors,
        min_dist=config.umap_min_dist,
        metric="cosine",
        random_state=config.random_state,
        low_memory=False,
    )


def build_cluster_model(config: PipelineConfig) -> Any:
    if config.cluster_backend == "hdbscan":
        return hdbscan.HDBSCAN(
            min_cluster_size=config.min_cluster_size,
            metric="euclidean",
            cluster_selection_method="eom",
            min_samples=config.min_samples,
            prediction_data=True,
        )
    return KMeans(n_clusters=config.n_clusters, random_state=config.random_state, n_init=10)


def build_vectorizer_model(config: PipelineConfig) -> CountVectorizer:
    return CountVectorizer(max_df=config.vectorizer_max_df, stop_words=config.stop_words)


def build_representation_model(config: PipelineConfig) -> Tuple[dict[str, Any], bool]:
    main_chain = [
        KeyBERTInspired(top_n_words=config.top_n_words),
        MaximalMarginalRelevance(diversity=config.mmr_diversity),
    ]
    representation_model: dict[str, Any] = {"Main": main_chain}

    if not config.enable_llm:
        return representation_model, False

    if not config.openai_api_key:
        return representation_model, False

    client = openai.OpenAI(api_key=config.openai_api_key, base_url=config.openai_base_url)
    llm_model = OpenAI(
        client,
        model=config.llm_model,
        prompt=config.llm_prompt,
        nr_docs=config.llm_nr_docs,
        diversity=config.llm_diversity,
    )
    representation_model["LLM"] = llm_model
    return representation_model, True


def build_topic_model(config: PipelineConfig, embedding_model: SentenceTransformer) -> Tuple[BERTopic, bool]:
    representation_model, llm_enabled = build_representation_model(config)
    # BERTopic < 0.16 使用 hdbscan_model 参数名（尽管名称含 hdbscan，实际也接受 KMeans 等任意聚类器）。
    # BERTopic >= 0.16 将其重命名为 cluster_model；如升级版本后报 unexpected keyword argument，
    # 请将下方 hdbscan_model 改为 cluster_model。
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=build_umap_model(config),
        hdbscan_model=build_cluster_model(config),
        vectorizer_model=build_vectorizer_model(config),
        representation_model=representation_model,
    )
    return topic_model, llm_enabled
