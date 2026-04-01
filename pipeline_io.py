from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from bertopic import BERTopic

from pipeline_config import PipelineConfig


def _generate_wordcloud(topic_model: BERTopic, output_dir: Path) -> None:
    """为主题生成词云图"""
    try:
        from wordcloud import WordCloud
        import matplotlib.pyplot as plt
        
        topic_info = topic_model.get_topic_info()
        # 排除 -1 (离群值)
        topic_ids = [t for t in topic_info.Topic if t != -1]
        
        for topic_id in topic_ids[:10]:  # 只生成前10个主题的词云
            words = topic_model.get_topic(topic_id)
            if not words:
                continue
            
            # 构建词频字典
            word_freq = {word: weight for word, weight in words}
            
            wc = WordCloud(
                background_color="white",
                max_words=100,
                width=800,
                height=400,
            ).generate_from_frequencies(word_freq)
            
            plt.figure(figsize=(10, 5))
            plt.imshow(wc, interpolation="bilinear")
            plt.axis("off")
            plt.title(f"Topic {topic_id}", fontsize=16)
            plt.tight_layout()
            plt.savefig(output_dir / f"wordcloud_topic_{topic_id}.png", dpi=150, bbox_inches="tight")
            plt.close()
    except ImportError:
        pass  # wordcloud 未安装则跳过


def load_docs(input_json: Path, min_words: int = 8) -> list[str]:
    """
    加载并过滤文档。

    Args:
        input_json: 包含 `texts` 数组的 JSON 文件路径。
        min_words: 最少词数阈值（默认 8）。
            推文类数据中存在大量 RT 截断文本、纯 URL、表情符号推文等噪声，
            这类极短文本（<8词）无法形成有意义的语义向量，
            会分散 UMAP/聚类的注意力，导致主题质量下降。
            8 词约对应最短完整句，既能过滤噪声，又不损失有价值的短推文。
    """
    with input_json.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    texts = payload.get("texts", [])
    docs: list[str] = []
    skipped = 0
    for item in texts:
        value = item.get("content_text")
        if isinstance(value, str):
            text = value.strip()
            if text:
                if len(text.split()) >= min_words:
                    docs.append(text)
                else:
                    skipped += 1

    if not docs:
        raise ValueError(f"No valid `content_text` records found in {input_json}")
    if skipped:
        print(f"  [load_docs] 过滤了 {skipped} 条极短文本 (<{min_words} 词)，保留 {len(docs)} 条")
    return docs


def save_artifacts(
    topic_model: BERTopic,
    docs: list[str],
    topics: list[int],
    probs: Any,
    config: PipelineConfig,
    llm_enabled: bool,
) -> None:
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # topics.csv — 主题统计与关键词表示
    topic_info = topic_model.get_topic_info()
    topic_info.to_csv(output_dir / "topics.csv", index=False, encoding="utf-8")

    # document_info.csv — 文档到主题的映射
    doc_info = topic_model.get_document_info(docs)
    doc_info.to_csv(output_dir / "document_info.csv", index=False, encoding="utf-8")

    # analysis.json — topic_aspects_ (Main/LLM 表示通道)
    topic_aspects = getattr(topic_model, "topic_aspects_", {})
    with (output_dir / "analysis.json").open("w", encoding="utf-8") as f:
        json.dump(topic_aspects, f, ensure_ascii=False, indent=2, default=str)

    # run_summary.json — 本次运行配置摘要
    run_summary = {
        "run_at": datetime.now().isoformat(),
        "n_docs": len(docs),
        "cluster_backend": config.cluster_backend,
        "llm_enabled": llm_enabled,
        "embedding_model": config.embedding_model_name,
        "n_clusters": config.n_clusters if config.cluster_backend == "kmeans" else None,
        "min_cluster_size": config.min_cluster_size if config.cluster_backend == "hdbscan" else None,
        "umap_n_neighbors": config.umap_n_neighbors,
        "umap_min_dist": config.umap_min_dist,
        "random_state": config.random_state,
    }
    with (output_dir / "run_summary.json").open("w", encoding="utf-8") as f:
        json.dump(run_summary, f, ensure_ascii=False, indent=2)


def _generate_report_index(topic_model: BERTopic, output_dir: Path, docs: list[str]) -> None:
    """生成综合报告首页"""
    topic_info = topic_model.get_topic_info()
    n_topics = len([t for t in topic_info.Topic if t != -1])
    n_outliers = len([t for t in topic_model.topics_ if t == -1])
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>BERTopic Analysis Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
        .stat-card {{ background: #f5f5f5; padding: 15px; border-radius: 8px; text-align: center; }}
        .stat-value {{ font-size: 2em; font-weight: bold; color: #4CAF50; }}
        .stat-label {{ color: #666; margin-top: 5px; }}
        .nav {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin: 30px 0; }}
        .nav-card {{ border: 1px solid #ddd; border-radius: 8px; padding: 20px; text-decoration: none; color: #333; transition: box-shadow 0.2s; }}
        .nav-card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
        .nav-card h3 {{ margin-top: 0; color: #2196F3; }}
        .wordcloud-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }}
        .wordcloud-item {{ text-align: center; }}
        .wordcloud-item img {{ max-width: 100%; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        h2 {{ color: #555; margin-top: 30px; }}
    </style>
</head>
<body>
    <h1>📊 BERTopic 主题分析报告</h1>
    <p>生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    
    <div class="stats">
        <div class="stat-card">
            <div class="stat-value">{len(docs):,}</div>
            <div class="stat-label">文档总数</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{n_topics}</div>
            <div class="stat-label">主题数</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{n_outliers:,}</div>
            <div class="stat-label">离群文档</div>
        </div>
    </div>
    
    <h2>📈 交互式可视化</h2>
    <div class="nav">
        <a href="topics_map.html" class="nav-card">
            <h3>🗺️ 主题分布图</h3>
            <p>查看主题在降维空间中的分布，相似主题会聚集在一起。</p>
        </a>
        <a href="topics_barchart.html" class="nav-card">
            <h3>📊 关键词条形图</h3>
            <p>每个主题最重要的关键词及其 c-TF-IDF 分数。</p>
        </a>
        <a href="topics_heatmap.html" class="nav-card">
            <h3>🔥 主题相似度热力图</h3>
            <p>主题之间的相似度矩阵，深色表示主题更相似。</p>
        </a>
        <a href="topics_hierarchy.html" class="nav-card">
            <h3>🌳 主题层次聚类</h3>
            <p>主题的层次聚类结构，展示主题的层级关系。</p>
        </a>
    </div>
    
    <h2>☁️ 主题词云</h2>
    <div class="wordcloud-grid">
"""
    
    # 添加词云图片
    for i in range(min(n_topics, 10)):
        img_path = f"wordcloud_topic_{i}.png"
        if (output_dir / img_path).exists():
            html += f'''        <div class="wordcloud-item">
            <h4>Topic {i}</h4>
            <img src="{img_path}" alt="WordCloud Topic {i}">
        </div>
'''
    
    html += """    </div>
</body>
</html>"""
    
    with open(output_dir / "report.html", "w", encoding="utf-8") as f:
        f.write(html)


def save_visualizations(topic_model: BERTopic, output_dir: Path, docs: list[str] | None = None) -> None:
    """生成所有可视化图表"""
    # 1. 主题分布图 (降维后)
    vis_topics = topic_model.visualize_topics()
    vis_topics.write_html(output_dir / "topics_map.html")

    # 2. 关键词条形图
    vis_barchart = topic_model.visualize_barchart()
    vis_barchart.write_html(output_dir / "topics_barchart.html")

    # 3. 主题相似度热力图
    vis_heatmap = topic_model.visualize_heatmap()
    vis_heatmap.write_html(output_dir / "topics_heatmap.html")

    # 4. 层次聚类图
    vis_hierarchy = topic_model.visualize_hierarchy()
    vis_hierarchy.write_html(output_dir / "topics_hierarchy.html")
    
    # 5. 词云图
    _generate_wordcloud(topic_model, output_dir)
    
    # 6. 综合报告首页
    if docs:
        _generate_report_index(topic_model, output_dir, docs)
