# BERTopic Pipeline Technical Report

## 1. Objective

根据 `analysis.ipynb` 的实验流程，已将 BERTopic 主题建模过程工程化为可复现、可参数化的 CLI pipeline，实现如下目标：

- 复用 notebook 核心建模链路（Embedding -> UMAP -> Clustering -> Representation）。
- 支持多表示输出：
  - `Main`: 关键词链（`KeyBERTInspired + MMR`）。
  - `LLM`: 通过 prompt 生成一句话主题总结（可选）。
- 统一导出主题级与文档级结果，便于下游分析与可视化。

## 2. Delivered Files

- `pipeline.py`
  - 主入口，负责串联训练、导出和可选可视化/模型保存。
- `pipeline_config.py`
  - CLI 参数与环境变量解析（含默认值）。
- `pipeline_builders.py`
  - 模型构建层（Embedding/UMAP/Cluster/Representation/BERTopic）。
- `pipeline_io.py`
  - 数据读取与产物导出层。

## 3. Pipeline Architecture

### 3.1 Input

默认输入文件：`data/technologyreview_texts.json`。

预期结构：

```json
{
  "texts": [
    {"content_text": "..."}
  ]
}
```

读取逻辑会过滤空字符串，若无有效文本将抛错。

### 3.2 Embedding Layer

- 默认模型名：`all-MiniLM-L6-v2`
- 默认优先加载本地路径：`../models/all-MiniLM-L6-v2`
- 若本地路径不存在，则回退到模型名加载。

### 3.3 Topic Modeling Layer

- UMAP 默认参数：
  - `n_neighbors=5`
  - `min_dist=0.01`
  - `random_state=0`
- 聚类后端（可切换）：
  - `kmeans`（默认，`n_clusters=10`）
  - `hdbscan`（`min_cluster_size=15`, `min_samples=5`）
- Vectorizer：`CountVectorizer(max_df=0.8, stop_words="english")`

### 3.4 Representation Layer

默认开启多表示：

- `Main`: `[KeyBERTInspired(top_n_words=30), MaximalMarginalRelevance(diversity=0.5)]`
- `LLM`（可选）: `OpenAI(...)`，使用一句话总结 prompt。

LLM 自动降级策略：

- 若 `--enable-llm` 但未提供 `OPENAI_API_KEY`，则自动关闭 `LLM` 表示并继续运行，仅保留 `Main`。

## 4. CLI Usage

在 `BerTopic/` 目录下执行：

```bash
python pipeline.py
```

常见参数：

```bash
python pipeline.py \
  --cluster-backend hdbscan \
  --save-visualizations \
  --save-model \
  --model-output-path ./topic_model
```

禁用 LLM（仅关键词表示）：

```bash
python pipeline.py --disable-llm
```

## 5. Outputs

运行后默认输出到当前目录（`--output-dir` 可改）：

- `topics.csv`
  - 主题统计与表示结果。
- `document_info.csv`
  - 文档到主题的映射信息。
- `analysis.json`
  - `topic_aspects_` 导出（包括 `Main`/`LLM` 等表示通道）。
- `run_summary.json`
  - 本次运行配置摘要（文档数、聚类后端、是否启用 LLM 等）。
- 可选 HTML（`--save-visualizations`）：
  - `topics_map.html`
  - `topics_barchart.html`
  - `topics_heatmap.html`
  - `topics_hierarchy.html`

## 6. Why This Design

- 将 notebook 的线性脚本拆为“配置/构建/I/O/入口”四层，方便：
  - 参数实验（不改代码，仅改 CLI）。
  - 后续扩展（增加新 representation、新输出格式）。
  - 稳定运行（LLM 不可用时自动降级，不阻断主题建模主流程）。
- 通过始终传入 `embedding_model`，规避 `dict representation + KeyBERTInspired/MMR` 在预编码场景下的常见报错。

## 7. Known Constraints

- 运行依赖本地 Python 环境已安装：`bertopic`, `sentence-transformers`, `umap-learn`, `hdbscan`, `scikit-learn`, `python-dotenv`, `openai`。
- 若启用 LLM，需可访问对应 OpenAI 兼容接口（`OPENAI_API_KEY` / `OPENAI_URL`）。
- `topics.csv` 与 `document_info.csv` 中可能包含较长文本字段，文件体积会随语料增长明显增大。

## 8. Suggested Next Iterations

1. 增加 YAML 配置文件模式，替代长命令行参数。
2. 增加评估指标导出（主题一致性、主题多样性）。
3. 增加批量语料输入与分批增量训练能力。
4. 将 prompt 模板独立到单独文件，便于版本管理与 A/B 对比。
