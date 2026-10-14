# 原生 LOTUS CLIP 图像检索接入

本轮接通固定 LOTUS1.2.4 的图片建索引及图文检索。图库、原生 Faiss 索引、在线图像／文本编码、
完整 top10 输出均经过 LOTUS；Direct、SemLoom static、SemLoom HSE static 和 Ray Data 只接到公开 RM
执行接口。作者算子、索引、检索和框架源码保持。五条路径的 12／64／256 图及已有索引新进程查询
资格已通过；独立 Daft 接入接续。当前结果用于语义与数值资格，不作性能排名。

## 实际语义及模型身份

[官方多模态测试](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/.github/tests/multimodality_tests.py)
使用 `SentenceTransformersRM('clip-ViT-B-32')`、`FaissVS` 和 `sem_index`→`sem_search('bird')`。
该版本 `sem_index` 将列转成 list；`ImageArray` 的路径字符串因而不会保留图片 dtype。
原生 RM 不对这个 list 解码，SentenceTransformers 会把 str 当文本。本轮实际 SDK 负例证明这条分支；
不能仅运行路径示例就认定图片已编码。自有入口显式解码为 RGB PIL，放入一维 object 列，保留原始
RangeIndex 并另存稳定业务 ID。图片查询传 PIL，`bird` 查询传文本，原生算法继续拥有排序和输出组装。

模型与预处理资格来自实际文件、398 个加载参数、有效输入张量和完整向量，见
[模型身份](embedding_identity.json)。固定 ST revision 为 `327ab6726d33c0e22f920c83f2ff9e4bd38ca37f`，
cached OpenAI revision 为 `3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268`。两个权重文件 SHA 不同，
加载参数内容摘要相同：`968f8926f4522526f88da56b4d5f47aa41d7cc28220136e3ab830666a70c56e6`。
同 slow PIL 预处理的有效像素差记录为 0，12×512 stock／Direct 保存向量独立重算差为 0。
三个短文本探针的 token IDs 一致；这不代表所有 tokenizer 配置都相同。改名但同像素结果相同，
改为 magenta 内容后向量差记录为 0.1600715071。参数、像素及内容控制的原始张量未纳入紧凑 NPZ，
相应结论来自已执行源码和运行摘要；不称为对这些控制张量的离线重算。

使用单卡 RTX4090、FP32、512 维 projected／L2-normalized 完整输出，Torch 线程池 1／1。
LOTUS1.2.4、SentenceTransformers3.4.1、Transformers4.57.6、Torch2.12.1+cu130、NumPy1.26.4、
Pillow12.2.0、Faiss1.15.0；其他包按逐次原始观测记录。图库来自 COCO val2017 JPEG，先 eager RGB PIL
解码，再无损 PNG 传入批次执行器。输入文件摘要见[公开源清单](source_manifest.json)，不保存图片或权重。
stock 的外层 max_batch_size=64，原 ST encode 默认 batch_size=32；替换臂模型 batch_size=32。

## 公共接点与执行职责

[RM 协议](../../../../code/src/semantic_methods/lotus/image_rm.py)提供
`embed_batch(doc_ids: tuple[str,...], images: Sequence[bytes]) -> ImageEmbeddingResult`、`semantics` 和 `close()`。
bytes 为同一 RGB 像素的 PNG；RM 接收完整 float32 `[N,512]`、按 ID 恢复输入顺序，并检查 finite／单位范数。
完整图库矩阵是原生索引的要求，本入口显式限制至 5000 行，不是正式流式图像 runner 或 PG 接入。

| 接点 | 原行为及自有采用 |
|---|---|
| LOTUS RM／ST wrapper | stock 委托原 `_embed`；替换臂仅提供公开 `_embed`，原 query conversion 保持 |
| LOTUS `sem_index`／`sem_search`／FaissVS | 原生 Flat inner-product、持久化与完整输出保持；有序像素／模型身份及 index／vecs 文件哈希用 sidecar 核对 |
| Direct | 复用已有 slow `ClipImagePreprocessor` 和 `ClipTensorActor` |
| SemLoom static／HSE static | 复用已有 image driver、typed pool 与 HSE driver，不复制内部调度 |
| Ray Data | 原生 `map_batches` 解码／CLIP UDF 和 actor pool；原生图拥有执行与调度，自写图记录为诊断用途 |
| Daft | 公共 CLI 已有惰性 `daft_native` 接点，独立 owner 交付及本入口资格另记 |

这些采用为工程决策；架构参照沿用[现有接入计划](../../../plans/系统架构.md)。本入口串行拥有本进程 LOTUS
settings，原生 cache 关闭。未资格跨查询并发、filtered refill 或 `sem_sim_join` 的子集及缺邻居行为。
本任务是 CLIP 检索；VLM `sem_filter/map`、相关性质量、PG、跨 Job 调度与新的优化算法均未验证。

## 验证状态与计时

`lotus-retrieval-20261014-d`：五臂各 12 图，一次组合工作流、三个在线图片查询和一条 `bird`。
保存的五份图库及在线图片向量相对 stock 全部差为 0；文本 CPU／CUDA 最大差为 5.96e-8、分数差为
2.98e-8。20 组完整 top10 的顺序、位置、ID 与分数通过独立复算；三个图片查询各自 top1 找回源 ID。
没有 caption 或相关性标签，因此不据 `bird` 排名声称任务质量。
26 项实际 SDK／确定性 fixture 检查通过、0 跳过；八次受监督调用全部成功，257.747 秒含 16.806 秒清理，
逐次自有物理退出确认、GPU compute 空、共享 lease 释放。
[独立审核重算](small_d_independent_review.json)记录原件与源码校验。

`lotus-retrieval-20261014-e`：64／256 图五臂各一次组合工作流，以及五臂共享 stock256 索引的新进程 search，
15 个模型 CLI 加一次预检全部成功。完整图库及在线图查询与同形状 stock 向量差为 0，
完整排序、位置、ID 和分数通过；已有索引路径只编码在线查询，未再次编码图库。
累计 763.510／1200 秒含 33.394 秒逐次清理，16 次物理退出确认、GPU compute 空、共享 lease 释放。
两轮共 20 个模型 CLI、60 图片／20 文本完整查询；全部原始值留存，不将重复源图片算作独立性能样本。
[规模与公开恢复独立审核](scale_e_independent_review.json)核对 49／124／9 原件成员、97 个完整矩阵及
全部 80 条查询。图片向量差为 0，文本向量最大差 5.96e-8、分数最大差 4.47e-8。

每格一次语义资格、无预热；前期准备方案中的性能重复在规模 run 前明确延后，不改变数值容差或失败判定。
每次最多 180 秒，e 累计最多 1200 秒。GPU 使用严格共享 lease，`recover_stale=False`；不回收 foreign lease、
不停止 foreign 进程。清理未确认时监督 owner 保持存活，不启动新工作，避免死 owner 被其他旧 runner 自动回收。

CLI 分开记录 `index_s`、已有索引 `index_load_s`／每条完整 `sem_search`、`native_workflow_s`、
source 内容核查／解码至完整查询输出的 `application_e2e_s`、启动及 evidence 写出。
原 `sem_search` 实际加载索引的时间仍计入查询；`index_load_s` 只记录 `load_sem_index` 注册行为。
默认使用低开销单调时钟，可选细计时借用已有模块，不新增计时平台。Ray Data 每次图片 RM 图含冷模型加载，
其他内置图片路径 workflow 内驻留；非stock文本 RM 位于 CPU，stock 文本位于 CUDA。
startup 还含身份核查、stock 构造和 runtime 启动。替换 RM 包含完整图片 PNG 编码、执行器内解码与传输；
stock 直接使用 PIL，这些额外成本保持在实际计时内，不归因于调度。这些配置不可直接做性能排名。
样本少，P99／P99.99 为 unavailable；失败、超时和实际输出在相应运行身份下保留。

下表为 source-d 单次 256 图观察，单位秒；组合应用含源核查／解码、建索引及四条查询，已有索引应用
含源核查／解码及四条查询。模型／runtime 启动另计，原始每条 `sem_search` 时间见压缩观测。

| 路径 | 建索引 | 组合应用 | 已有索引应用 |
|---|---:|---:|---:|
| stock | 2.132 | 3.692 | 1.745 |
| Direct | 30.752 | 32.517 | 2.276 |
| SemLoom static | 35.271 | 37.431 | 4.009 |
| SemLoom HSE static | 33.703 | 35.829 | 5.855 |
| Ray Data | 42.183 | 73.228 | 29.483 |

[可选计时接点本机核查](timing_hook_local.json)使用 `bbbd8a1b` 的原模块，两个实际 span 的成功／失败四事件
保留，首个业务异常保持，默认未创建 writer。该核查只有本机 API／事件写出，无 LOTUS 模型或 GPU；
可选模块需要先由整合分支接纳，本轮 source-d 的实际模型资格始终关闭细计时。

同图 stock 图库 batch12 与在线 batch1 的向量最大差为 9.4e-5～2.4e-4，各臂匹配实际 batch 形状仍为 0 差。
因此资格只要求同样 batch 形状的跨执行器数学等价，不宣称 CLIP 跨 batch 形状恒等。

## 原件、失败与复现

[精简原始观测](observations.json.gz)保留完整逐次时间、RM 计数、top10 输出与监督状态。
[向量 NPZ](vectors.npz)按实际内容 SHA 无损去重，[逐运行映射](vector_map.json)可以恢复所有保存的完整矩阵；
没有挑选有利元素或只比较截断输出。运行原件、命令、日志、原生 index／vecs、未公开的控制材料在独立
artifact 目录保留；[原件映射](originals.json)按运行 ID、成员名、字节数与 SHA 定位，不上传私有大包。

首次 `lotus-retrieval-20261014-a` 在资产预检失败，源 tar 中的 macOS AppleDouble JSON sidecar 导致
UnicodeDecodeError，未进入模型／GPU；原失败保留。新 source-b 排除该元数据，预检及实际 SDK 通过，
下载固定公开 ST snapshot 后释放自己的 lease。固定 source-d 的运行代码与测试摘要见
[源码身份](source_identity.json)，基线 SHA 不代替未提交适配代码的身份。

公开证据可在安装 NumPy 的环境重算：

```bash
python experiments/results/diagnostics/lotus_native_image_retrieval_20261014/verify_evidence.py
```

[任务 CLI](../../../../code/scripts/experiments/run_lotus_image_retrieval.py)支持 `index/search/combined`；
模型及运行路径放 Git 外，[模型资格 CLI](../../../../code/scripts/experiments/qualify_lotus_clip.py)
先生成实际摘要匹配的 embedding identity。完整调用说明见[LOTUS 目录](../../../../code/src/semantic_methods/lotus/README.md)。
