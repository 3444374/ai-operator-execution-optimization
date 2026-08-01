# 实验 Baseline 参考矩阵

整理日期：2026-07-16；文献、官方 benchmark 与指标合同复审：2026-08-02

> **2026-07-17 口径更新**：本文中的"跨层决策""写回瓶颈""RC3"等旧术语已统一。最新 baseline 分级、研究内容定义和优先级以 `AGENTS.md` §1、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md` 为准。
用途：正式实验设计时，从正式论文、官方系统和可审计工程默认中提取 baseline，避免使用 strawman 对照
来源：`research/ai_operator_literature_inventory.md` 与 `research/top15_ranked_papers.md`

> **2026-07-16 方向更新**：vLLM 已定位为部署平台（非竞争对手），其 continuous batching 是 S 级 baseline——课题研究上游调度优化，不修改 vLLM 内部。新增 baseline 候选：Ray 2.49+ PrefixCacheAffinityRouter、Ray Serve batch_size_fn 等。详细背景见 `research/knowledge_hub.md`。

---

## 使用规则

1. 每个实验方向（GPU 调度 / 写回 / 数据组织）在选择 baseline 时，**优先从本矩阵中选取**已有文献中的最优策略。
2. 非文献来源的工程 baseline（如 COPY、unlogged table）必须先在 B 系列实验中确认其为当前最优实践。
3. 最终论文的对照表必须标注每个 baseline 的来源论文或系统。

## Baseline / benchmark 的检索、筛选与维护流程

本矩阵不是凭系统知名度罗列名称。新增或替换 baseline 时必须按以下流程留下可复核
记录；未完成来源核验的候选只能放 Related Work，不能直接进入性能排名。

### 1. 先定义问题，再检索系统

每次检索前先填写四项：算子语义（AI_COMPLETE / AI_EMBED / AI_CLASSIFY）、比较层级
（服务上限 / 执行框架 / 数据库产品 / 策略）、输入与输出合同、要回答的因果问题。
例如“Daft staged vs ours”回答执行框架差异，“vLLM Bench”只回答服务容量，
“Snowflake AI_CLASSIFY”回答产品 SQL 的质量-成本-时间，不允许互相替代。

### 2. 来源按优先级搜索

1. **官方产品能力文档**：确认算子是否真实存在、输入类型、模型/endpoint、配额与
   计费；用于 OceanBase、PolarDB、Snowflake、BigQuery 等 capability gate。
2. **官方 benchmark 页面与可运行代码**：提取 workload、模型、数据表示、预处理、
   硬件、版本、生命周期、重复次数和原始计时边界；当前 Ray Data 与 PolarDB
   多模态公开结果属于此层。
3. **数据库系统论文与 benchmark paper**：从 PVLDB/SIGMOD/CIDR/USENIX/ACM 官方
   论文页检索 semantic operator、AI query processing、multimodal batch inference、
   serving/scheduling；再沿论文的 baseline 和引用向前/向后追踪。SemBench、LOTUS、
   Palimpzest、Cortex AISQL 属于此层。
4. **部署平台与执行框架论文/文档**：补充服务上限和诊断指标，如 vLLM、Ray Data、
   Daft；它们不能自动代表数据库产品 baseline。
5. **博客、二手报告和搜索摘要**：只用于发现线索，结论必须回到官方文档、论文、代码
   或本项目同机复现；无法回溯时不进入矩阵。

推荐检索式至少覆盖 `operator + database + benchmark`、`system + workload + metrics`、
`AI_CLASSIFY/AI_EMBED + performance`、`multimodal inference + data pipeline`，并检查
论文/页面发布日期、软件版本和后续更新。项目已有精读材料优先复用
`research/reading_notes/`，但性能数字仍回查原文或官方结果页。

### 3. 每个候选的来源卡片

候选进入矩阵前必须能回答并记录：

| 字段 | 必填内容 |
|---|---|
| 身份 | 系统/论文、官方 URL/DOI、发布日期与访问日期、代码/版本 |
| 语义 | 算子、输入/输出、是否改变模型调用数或结果质量 |
| workload | 数据集/split/规模、unique data 与重复 pass、数据表示和 source/sink |
| compute | 模型、processor、dtype、CPU/GPU/内存/节点和并发配置 |
| execution | fused/staged/service、batch/actor/task、缓存、cold/warm 生命周期 |
| measurement | 计时起止、warmup/repeats、汇总统计、质量/成本/失败定义 |
| tuning | 默认点、独立校准、matched-resource 或 best-achievable |
| reproducibility | 可同机运行、仅外部数字、仅 capability，及缺失字段 |

### 4. 证据等级和准入

| 等级 | 定义 | 可以声称什么 |
|---|---|---|
| A：同机正式复现 | 同输入、模型、质量、物理资源和计时边界；独立校准；原始 CSV 可审计 | 可进入主性能排名 |
| B：同机 capability/gate | 代码可运行且正确，但规模、稳态或重复不足 | 只证明可行性，不排名 |
| C：外部官方 benchmark | 官方配置和数字可核验，但硬件/边界与本项目不同 | 行业参照和复现目标，不与本机 raw time 排名 |
| D：产品 capability / 论文 Related Work | 功能存在，但闭源或无法对齐模型/硬件 | 比语义、质量/成本口径或定位，不比内部 MFU |
| E：二手或缺失合同 | 无法回到原文，或关键配置/质量/计时边界缺失 | 不采用 |

正式 baseline 的最低集合遵循“少而强”：一个 compute/service ceiling、一个无项目
调度层的强因果对照、同栈官方 runtime、至少一个不同栈开源 runtime、冻结最佳静态
项目配置。产品和学术系统只在算子语义与工作量可对齐时进入数字排名，不能为了表格
数量实现无关系统。

### 4.1 原生执行准入门禁

“使用某框架的 API”不自动等于“官方原生 baseline”。正式主排名只接受以下两类：

1. **vendor-code parity**：固定官方 benchmark/example 的仓库、commit、依赖和入口，
   只修改路径、凭证、硬件规模和指标输出；保存适配 diff。
2. **vendor-native API graph**：官方没有可直接复用的相同 workload 时，使用官方推荐
   的 built-in AI Function 或执行图，由框架拥有 batching、backpressure 和调度；项目
   只写模型 workload UDF、输入/sink adapter 和审计，不得注入项目 credit、router、
   active-window 或 actor 编排。

项目自写 `@daft.cls`、Ray actor pool 或重构后的 staged pipeline 即使“参考官方文档”，
也只能是 `diagnostic_reference`。每个正式 run 必须记录 `implementation_provenance`、
`scheduler_owner`、`custom_scheduling_code`、`formal_baseline_eligible`、upstream URL/commit
和 adapter diff；字段缺失则 fail closed，不进入 baseline 排名。

当前 Daft image-classification vendor-code parity 已固定到
`Eventual-Inc/Daft@3f5bdd175b7de3dcdf35765e1ba604b5c1cb8e15`，入口为
`benchmarking/ai/image_classification/{daft_main.py,ray_data_main.py}`。文件 SHA256、
官方 803,580-row workload 和允许适配白名单见
`code/configs/image_vendor_baselines.json`；该 pin 尚未在本项目双 4090 上执行，不能把
Daft README 中的 AWS 8×g6.xlarge 数字与本机结果直接排名。

文本轨道采用同一 fail-closed 规则：每个 summary 由
`code/src/baselines/provenance.py` 写入 comparison role、implementation provenance、
scheduler owner、custom scheduling、formal eligibility、upstream source 和资格门禁。
`vLLM Bench` 是 service ceiling，项目 `bounded_*` 是 direct controls，二者均不标记为
native baseline；Daft built-in `functions.prompt`、Ray Data HTTP Processor 和通过
capability gate 的 OceanBase `AI_COMPLETE` 才可进入原生系统排名。执行合同见
`text_native_baseline_rerun_20260802.md`。

### 5. 过期清理规则

候选状态统一使用 `candidate → capability-verified → gated → calibrated → formal`；
失败或超出 scope 使用 `blocked` / `related-work-only` / `retired`。发生以下任一情况时
必须复审：官方文档或 benchmark 更新、软件/模型版本变化、计时边界变化、同名 arm
实现变化、出现相反的公开排名、项目新结果推翻旧结论。旧数字若仍有诊断价值，移到
带日期的结果报告并标成历史证据；当前入口不得继续写成“下一步”或“当前默认”。

`baseline_reference.md` 负责来源、分层、准入与指标合同；
`database_ai_operator_baseline_matrix_20260729.md` 是文本轨道预注册和历史执行记录；
`image_clip_workload_lock_20260731.md` 是当前图像执行合同；各 `results/README.md` 和
CSV 才是实验数字的权威来源。三者发生冲突时，不能自行拼接数字，必须回到结果目录。

---

## 一、GPU 调度侧 Baseline

2026-07-29 起，正式端到端实验保留四类参照，不能混称为一个 baseline：

- **direct-vLLM service-only capacity** 是物理上界参照。使用相同模型、请求
  trace、输出设置和 endpoint 数，绕过 PostgreSQL/Daft/Ray 测服务可实现容量；
  上游 infra 的目标是逼近而不是超过它。
- **现有无 Daft/Ray 数据库 AI 算子** 是产品级核心 baseline。当前首选
  OceanBase `AI_COMPLETE`，使用同机 OpenAI-compatible vLLM endpoint；
  若 Community Edition/endpoint/观测门禁未通过，只作工业参考。
- **同 PostgreSQL bounded AsyncIO** 是因果 control。它不是原生 baseline 或产品竞争对手，
  但能隔离 OceanBase/PostgreSQL 差异与 Daft/Ray 的净贡献；必须独立标定，
  不能使用串行 strawman。
- **Daft/Ray 官方 runtime** 是框架归因 baseline：Daft `prompt()` Native/Ray
  和 Ray Data HTTP Processor，用于判断自定义 token-work/refill 是否超越
  现有官方实现。
- **naive DB→Daft→Ray→vLLM** 是必须显著击败的工程 baseline，例如固定行数、
  job-local/global K、无 workload-aware organization。正式策略还必须对比经
  静态 sweep 得到的最佳强 baseline，不能只赢单行串行 strawman。

报告 `capacity_efficiency = pipeline_tokens_s / direct_service_tokens_s`，并在
相同 P99/SLO guardrail 下比较 goodput；若 direct 上界未测，不能用 MFU 单独
声称“GPU 已压满”或“仍有 70% 优化空间”。

上述 baseline 使用两个互不交叉排名的协议轨道：

- **Chat 产品兼容轨道**：vLLM Bench、bounded Chat、Daft Native/Ray、
  Ray Data、OceanBase capability 与项目 Ray async dispatch；
- **Completions 机制轨道**：bounded fixed-row multi-prompt 与项目原始
  multi-prompt token-budget/length-align/flush 路径。

两条轨道分别使用同一 request manifest、同一双 endpoint、同一输出上限和
独立 calibration。不得用 Completions 数值直接声称超过 Chat baseline。
详细预注册见 `database_ai_operator_baseline_matrix_20260729.md`。

### 图像 AI_EMBED / AI_CLASSIFY 的 baseline 层级

图像轨道不能直接复用文本的 Chat/Completions 排名，也不能把所有 Daft 路径称为
一个 baseline。按以下层级分开报告：

| 层 | 对照 | 作用 |
|---|---|---|
| compute ceiling | GPU-resident CLIP tensor→forward | 模型计算平台，不是项目对手 |
| direct service | bounded direct CLIP、vLLM pooling | 绕过 DB/Daft/Ray 的容量与部署参照 |
| vendor built-in | Daft `decode_image` + `embed_image` / `classify_image` | 由 Daft provider 拥有 batching、concurrency 和 backpressure 的主 baseline |
| vendor-code parity | Daft 官方 803,580-row ResNet18 benchmark 中的 Daft/Ray Data 原脚本 | 公开 file/object track；固定 upstream commit，只做环境适配 |
| native API graph | Ray Data `read_sql → map_batches(CPU) → map_batches(GPU)` | 数据库输入轨道；Ray Data 自己调度，UDF 只定义 workload kernel |
| diagnostic reference | 项目自写 Daft fused/staged UDF | 只作阶段边界与资源机制诊断，不进入 official/native 主排名 |
| product-integrated data engine | PolarDB Daft AI Functions `classify_image` / `embed_image` | 同一 Daft/Ray 架构家族的工业集成，不冒充独立数据库内核执行方式 |
| managed product SQL | Snowflake / BigQuery image `AI_CLASSIFY` | 闭源托管路径只比较 E2E、成本、质量和失败率，不跨硬件比较 MFU |
| project static | 冻结最佳 frame budget/active batches/actor shape | 动态策略的唯一主对照 |
| project adaptive | state-aware request shaping/shared credit | 只报告相对 frozen static 的增量与 oracle regret |

当前 1.296×/1.138× 只属于 `project static vs project-authored Daft UDF diagnostic`
历史结果，不能写成优于 Daft 内置 AI Function、官方 benchmark 或 PolarDB 异构
流水线。每个系统同时报告 matched-resource 与 independently
calibrated best-achievable；统一输入、模型、输出、生命周期、计时边界和 sink。

OceanBase 4.5/4.6 官方 AI Function 当前确证的是文本 `AI_COMPLETE`、文本
`AI_EMBED` 与 `AI_RERANK`，不把它冒充图像分类 baseline；其 CE 4.5.0 动态部署
仍受当前 AutoDL 容器门禁阻塞。公开 ImageNet/ResNet18 系统 benchmark 只提供方法模板：
PolarDB 与 Ray 官方页面对 Daft/Ray Data 的 raw 排名方向并不一致，因此外部数字不跨
硬件排名，必须在本项目机器上按同数据、同模型、同版本分别校准重跑。

### 外部多模态公开 benchmark：事实、冲突与复现合同

这组公开结果必须保留，因为它同时提供了行业参照和“为什么必须同机重跑”的直接证据，
但不得把厂商 raw time 与本项目 AutoDL 数字直接排名。

| 发布方 | workload / 数据规模 | Daft | Ray Data | Spark |
|---|---|---:|---:|---:|
| Ray Data 官方 | Image classification，约 800K ImageNet、ResNet18 | `195.3±2.5s` | `111.2±1.2s` | 未列 |
| Ray Data 官方 | Document embedding，10K PDF | `51.3±1.3s` | `29.4±0.8s` | 未列 |
| Ray Data 官方 | Audio transcription，113,800 FLAC、Whisper | `510.5±10.4s` | `312.6±3.1s` | 未列 |
| Ray Data 官方 | Video object detection，10K frames、YOLO | `735.3±7.6s` | `623.0±1.4s` | 未列 |
| PolarDB 官方 | Image classification，803,580 图 | `4m23s`（263s） | `23m30s`（1410s） | `45m07s`（2707s） |
| PolarDB 官方 | Document embedding，10K PDF | `1m54s`（114s） | `14m32s`（872s） | `8m04s`（484s） |
| PolarDB 官方 | Audio transcription，113,800 audio files | `6m22s`（382s） | `29m20s`（1760s） | `25m46s`（1546s） |
| PolarDB 官方 | Video object detection，1,000 videos | `11m46s`（706s） | `25m54s`（1554s） | `3h36m`（12960s） |

Ray 官方在其当前实现与资源下四项均为 Ray Data 更快；PolarDB 官方在 8 workers、
每节点 1×24GB GPU、4 vCPU、16GB RAM 的环境中四项均为 Daft 更快。两边 workload
名称相近但数据表示、pipeline、实例形状和测量边界未完全统一，因此这些数字不是一张
可直接排名的总榜。它们共同证明：外部公开 benchmark 必须保留，但必须在项目机器上
复现才能评价本项目。

Ray 官方还报告同一 image-classification workload 的 Ray Data E2E 随单节点 CPU
规格从 4 CPU 的 `456.2±39.9s`、8 CPU 的 `195.5±7.6s`、16 CPU 的
`144.8±1.9s`，降到 32 CPU 的 `111.2±1.2s`；Daft 对应为
`315.0±31.2s`、`202.0±2.2s`、`195.0±6.6s`、`195.3±2.5s`。这说明 CPU
供给、pipeline 边界和 actor/batch 实现足以改变排名，公开 raw time 只能作为外部
证据，不能替代同硬件复现。

来源：[Ray Data Benchmarks](https://docs.ray.io/en/master/data/benchmark.html)、
[PolarDB Daft 性能报告](https://help.aliyun.com/en/polardb/polardb-for-postgresql/daft-performance-benchmark)。

正式复现分成两个输入轨道：

1. **公开 file/object track**：尽量复用公开 ImageNet/ResNet18 数据、变换、模型、
   输出和版本，比较官方 Daft、Ray Data、可运行时 Spark，以及项目 adapter。该轨道
   回答“项目执行引擎相对公开 AI batch pipeline 如何”。
2. **database-operator track**：相同图像写入同一 PostgreSQL 输入，所有 arm 统一
   BYTEA 读取、结果物化和可选 sink，比较 Daft Native/Ray、Ray Data、强 bounded
   pipeline 和项目。该轨道回答“数据库 AI 算子外部链路是否更好”。

每个系统既报告 matched physical CPU/GPU budget，也报告 independently calibrated
best-achievable；后者允许各系统使用自己的 batch、actor/task 和 pipeline 参数，不能
强迫不同技术栈共享 Ray 概念。正式结果要求至少 1 warmup + 3 interleaved repeats、
相同模型权重、相同预处理、相同输出集合和质量门禁，同时报告 E2E/JCT、images/s、
first-output、P95/P99、CPU-core-seconds、GPU utilization/starvation、host/device
memory、失败率和 top-1/top-5 accuracy。

### 不同技术栈的产品与学术比较

Daft/Ray 是本项目实现手段，不是 baseline 准入条件。外部系统按算子语义入选：

| 系统族 | 候选 | 比较口径 |
|---|---|---|
| 同栈官方 runtime | Daft Native/Ray | 同机同模型严格排名，判断自定义调度相对官方实现的净收益 |
| 不同栈开源 runtime | Ray Data；可运行时 Spark | 同机同模型严格排名，防止只赢同栈弱实现 |
| 数据库调用外部 endpoint | OceanBase `AI_COMPLETE` / text `AI_EMBED` | 文本轨道同 endpoint 严格比较；当前容器门禁失败则保留工业参考 |
| 工业同类集成 | PolarDB Daft AI Functions | 作为 Daft/Ray 架构家族的产品实现，不称为独立数据库内核方案 |
| 闭源托管 SQL | Snowflake / BigQuery image `AI_CLASSIFY` | 同数据/标签比较查询 E2E、$/1K rows、质量、错误率和配额；不比较内部 GPU/MFU |
| 学术语义系统 | LOTUS、Palimpzest、ThalamusDB | 使用 SemBench 对齐 operator、数据、ground truth、runtime/cost/F1；不与固定-work CLIP 只比吞吐 |

SemBench 已对 LOTUS、Palimpzest、ThalamusDB 和 BigQuery 运行多模态 semantic
filter/join/map/rank/classification。项目只接入与课题语义一致的 image classification、
semantic map/filter 和 text generation 子集，并保留其 runtime、cost、quality、memory、
failure 与扩展性协议；不为了扩大表格而实现与研究问题无关的全部算子。

来源：[SemBench](https://sembench.github.io/SemBench/)、
[Snowflake AI_CLASSIFY](https://docs.snowflake.com/en/sql-reference/functions/ai_classify)、
[BigQuery AI.CLASSIFY](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-ai-classify)、
[PolarDB AI Functions](https://help.aliyun.com/en/polardb/polardb-for-postgresql/ai-functions-and-supported-model-providers)、
[OceanBase AI Function](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975)。

### 数据库 AI 算子评价指标合同

本节是后续文本与图像实验的最低指标合同。它不是把所有论文的字段机械相加，而是
要求每个实验报告与算子语义相适用的完整证据链。任何 headline 性能结果若缺少质量、
计时边界、工作量或失败记录，不得进入正式系统排名。

#### 外部系统实际采用的指标

| 来源 | 主要指标 | 对本项目的约束 |
|---|---|---|
| SemBench（PVLDB 2026） | result quality、execution time、monetary cost、memory、scaling；5 次运行的标准差；timeout/failure | 数据库语义算子必须同时报告质量、时间、成本/调用 work、内存和失败，不能只比吞吐 |
| LOTUS（PVLDB 2025） | accuracy / nDCG / RP@K、runtime、model calls，以及准确率目标和失败概率 | 若不同方案改变调用数、模型或近似程度，必须把质量和调用 work 一起报告 |
| Palimpzest（CIDR 2025） | runtime、financial cost、F1，比较 Pareto 计划 | 允许比较 quality-cost-time Pareto，但不能把降低质量换来的时间写成纯性能收益 |
| Cortex AISQL（SIGMOD 2026） | latency/speedup、throughput、inference work/cost、F1/相对 oracle quality | 数据库产品比较至少覆盖执行时间、容量、推理 work 和任务质量 |
| vLLM / LLM serving | request/token throughput、normalized latency、TTFT、TPOT/ITL、E2E tail、SLO goodput、KV/显存 | AI_COMPLETE 必须同时报告容量与 P50/P95/P99/SLO，不能只看 tokens/s |
| Ray Data / PolarDB 多模态 benchmark | job completion time、rows/images processed、均值/方差、资源规格、scale-out、完成/失败/OOM | 多模态 batch pipeline 必须报告 JCT、吞吐、扩展效率、资源规模和稳定完成率 |

外部闭源产品通常不暴露 GPU/MFU、队列或 PCIe 指标。因此 Snowflake/BigQuery 等
managed SQL 只要求可观测的 query E2E、成本、质量、调用/配额和失败率；内部资源指标
不得伪造或由客户端 wall time 反推。相反，本项目与同机开源 baseline 必须额外记录
CPU/GPU/内存/传输和队列指标，用于解释为什么快，而不是只给最终排名。

#### 每个正式 run 的必记字段

| 类别 | 必须记录 | 适用说明 |
|---|---|---|
| 实验身份 | git commit、系统/库/模型/processor revision、数据集版本与 split、硬件、CPU/GPU 配额、随机种子、warm/cold 生命周期 | 所有实验 |
| 工作量 | 输入 rows、unique objects、输入 bytes、调用/请求/batch 数、prompt/output tokens 或 image/frame 数、实际输出 rows | 所有实验；重复 pass 与 unique data 分开 |
| 正确性 | exactly-once、行 ID 集合、完整输出 digest、错误/重试/timeout/OOM 数 | 所有实验；失败 run 也必须落盘，禁止只保留成功样本 |
| 任务质量 | AI_COMPLETE 的任务指标；AI_CLASSIFY 的 top-1/top-5 或 mAP/F1/precision/recall；AI_EMBED 检索的 Recall@K/MRR/nDCG | 有 ground truth 的正式比较必选；embedding norm/checksum 只是执行门禁 |
| 时间 | operator E2E、system E2E/JCT、first output、per-row/request P50/P95/P99、各 pipeline stage wall | system E2E 必须包含统一 sink；stage time 用于归因 |
| 容量 | rows/images/requests/s、input/output/total tokens/s；SLO-compliant goodput | 按算子报告，不用 tokens/s 描述图像分类 |
| 成本 | model calls、token/image work、GPU-seconds、CPU-core-seconds、Joules 与单位工作能耗；云/API 可比时报告 $/1K rows 或 $/query | 金钱成本不可比时报告资源成本，不填虚假美元值 |
| 内存与 I/O | host/VRAM peak、Ray object-store/spill、disk/network bytes、H2D/D2H bytes 与时间 | 同机开源 baseline 必选；闭源产品仅记录可观察项 |
| 调度诊断 | queue/wait/active work、batch-size 分布、GPU starvation/bubble、preemption/cache 指标、scheduler overhead | 与策略因果相关时必选；不是产品 headline |
| 扩展与公平 | 1/2 GPU speedup 与 scaling efficiency；1/2/4 job aggregate throughput、per-job JCT/P99、slowdown、Jain fairness | 多 GPU、多 job 正式实验 |
| 统计 | 至少 1 warmup + 3 个交错 formal repeats；raw values、median、mean、std/CV，必要时 CI | 公开 Ray/SemBench 已报告重复方差，本项目不得只给最好一次 |

任务质量按算子分别冻结：ImageNet 单标签分类用 top-1/top-5；COCO 多标签分类用
mAP、micro/macro-F1、precision/recall；embedding 检索用 Recall@K、MRR、nDCG；
文本生成必须选择与具体 workload 对应的任务指标，不能统一用一个无语义的字符串相似度。
若当前数据集没有 labels/captions/ground truth，该 run 只能承担性能/执行正确性门禁，
报告中必须写明“任务质量未评价”，不能声称更准确或质量等价。

#### 当前采集覆盖与缺口

`code/scripts/run_image_clip_e2e.py` schema v9 已覆盖 operator E2E、first output、
images/s、batch/stage P50/P95/P99、输入/输出 bytes、H2D/D2H、CPU-core-seconds、
GPU util/power/energy/显存、exactly-once、digest/norm、资源版本与 CPU 预算。它仍然
缺少或只部分覆盖以下正式指标：

1. **任务质量**：当前 PostgreSQL COCO 表没有 annotations/captions，不能计算
   mAP/F1 或 Recall@K；正式 AI_CLASSIFY/AI_EMBED 前必须加入 ground truth evaluator。
2. **失败样本落盘**：进程异常时目前可能只有日志，没有结构化 run status、error type、
   retry/timeout/OOM 计数；正式 matrix runner 必须 fail-closed 并保留失败行。
3. **system E2E**：当前图像 gate 排除 pgvector sink；正式系统比较要为所有 arm 接入
   相同 COPY + deferred-index sink，并同时保留 operator E2E。
4. **内存与 Ray 数据面**：已有 host/VRAM peak，但缺 object-store peak、spill bytes、
   task retry 和资源清理后的残留审计。
5. **请求级尾延迟**：当前主要是 batch latency；若产品语义是一行一个 AI 算子请求，
   需按 row ID 记录 submit→complete 分布，避免 batch P99 冒充 request P99。
6. **扩展/成本派生量**：已有 CPU/GPU/energy 原始量，但仍需统一生成 scaling efficiency、
   $/1K rows（仅在价格可比时）、failure rate 和 oracle regret summary。

上述缺口分两类处理：任务质量 evaluator、system sink 与失败记录是正式排名前的
**阻断项**；object-store、逐行 latency、queue/bubble 等是策略或瓶颈归因需要时的
**解释项**。不得因为解释项很多而遗漏质量、JCT、吞吐和失败这四类核心证据。

当前 COCO/CLIP host-path matrix 只承担动机画像、actor/batch/active-window 校准和
收益归因，不承担外部 baseline headline。若使用 COCO AI_CLASSIFY，必须补 annotations
并报告 mAP、micro/macro-F1、precision/recall；若使用 ImageNet/ResNet18，报告
top-1/top-5；若使用 embedding 做检索，报告 Recall@K、MRR/nDCG。digest、norm 和
exactly-once 只是执行正确性门禁，不能代替任务质量。

正式策略比较还必须区分：calibration 得到并在 held-out 上冻结的最佳静态
baseline、每个 workload 事后 sweep 的 static oracle，以及仅冻结候选边界、
运行时自动选择的 dynamic policy。动态策略应对比冻结静态点，并报告相对
per-workload oracle 的 regret；不能把为每个 workload 人工精调的 oracle
冒充可部署静态方案，也不能完全跳过安全容量边界校准。

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **G1** | Continuous Batching | vLLM (Kwon et al., SOSP 2023) | A | Iteration-level 动态组 batch；PagedAttention 内存管理 | 用 vLLM / Ray Serve 替代手动 HTTP endpoint；记录到达率→batch→完成的 latency 分布 |
| **G2** | Iteration-Level Scheduling | Orca (Yu et al., OSDI 2022) | A | 调度粒度从 request-level 降到 iteration-level；GPT-3 175B 上最高 36.9× 吞吐 | 同 G1，Orca 是 vLLM 的前身，选其一即可 |
| **G3** | Chunked-Prefills | Sarathi-Serve (Agrawal et al., OSDI 2024) | A | 将 prefill 拆成 chunks 避免阻塞 decode；2.6-5.6× 服务容量提升 | 仅在 AI_COMPLETE 场景中使用（embedding 无需 decode） |
| **G4** | Streaming Batch Model | Ray Data (Luan et al., arXiv 2025) | 预印本 | CPU/GPU 异构批处理 + pipeline 执行；3-8× 吞吐 | 作为 "naive pipelining" baseline：只做 compute-write overlap，不做 joint optimization |
| **G5** | Disaggregated Prefill/Decode | DistServe (Zhong et al., OSDI 2024) | A | Prefill 和 Decode 分离到不同 GPU | AI_COMPLETE 场景的可选对照 |
| **G6** | Phase Splitting | Splitwise (Patel et al., ISCA 2024) | 顶会 | 将推理拆分为 prompt 和 token generation 两个 phase，分别优化 | AI_COMPLETE 场景的可选对照 |

### 当前状态

当前已使用真实双 4090 vLLM endpoint，并建立 vLLM Bench、bounded HTTP、
Daft `prompt()` Native/Ray 和 Ray Data HTTP Processor 的同 manifest 功能/计数
门禁。2026-07-30 复核发现旧 project Chat 路径最佳约
5.9K tokens/s、31.2s，而同 manifest bounded Chat C256 为
14.5K tokens/s、12.6s。新增持久异步 HTTP/Ray dispatch 后，单次 worktree
smoke 的 Chat K256 model-request wall 为 12.552s；multi-prompt
Completions fixed16 project/direct 为 11.164/10.943s，功能上已接近同协议
上限。正式策略排名仍需 1 warm-up + 3 repeats feeding gate 达到至少 95%。
之后只做必要的独立 calibration，找到合理强且进入平台期的配置；不把每个
baseline 调成无限参数搜索，也不把未到 ceiling 的默认点当最终结果。

多 job 侧增加 VTC（OSDI 2024）作为算法基线：token-cost service counter、
work-conserving borrowing 和每 job service/JCT/fairness。Llumnix（OSDI 2024）
作为多实例 virtual usage 与在线纠偏参考，不要求实现 KV live migration。

---

## 二、写回侧 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **W1** | COPY + 延迟建索引 | PostgreSQL 官方文档 §14.4 + pgvector Issues #400/#430 | 官方文档 | 先 COPY 到 unlogged table → `CREATE INDEX HNSW`（事后建索引远比增量插入快） | 写回侧"工程最优"baseline 出处（已采纳） |
| **W2** | io_uring + 空间感知插入 | TurboVecDB (PVLDB 2025) | **A** | 并行 I/O + 空间感知重排插入顺序；HNSW index build 减少 98.4%；查询吞吐 11.1× | 若 pgvector 版本已包含此优化，自动成为写回 baseline |
| **W3** | Worker-Direct Blind Append | Delta Lake (Armbrust et al., PVLDB 2020) | **A** | 多 worker 各写各的，盲追加永不冲突；optimistic concurrency | 对应本项目的 A2 实验（worker-direct 写回） |
| **W4** | Queue-Worker Decoupled | pgai Vectorizer Worker (Timescale) | 工程 | 触发器→队列表→外部 worker 轮询→各自写回；`FOR UPDATE SKIP LOCKED` + advisory lock | 对应本项目的 A3 实验（queue-worker 写回） |
| **W5** | Lazy Materialization (Merge-on-Read) | Iceberg (Okolnychyi et al., PVLDB 2024) | **A** | 先写 delete file 标记，后台 compaction 时再物理合并；避免写时重写 | 可作为"最懒写回"理论 baseline |
| **W6** | KV 分离避免 Compaction 重写 | WiscKey (Lu et al., FAST 2016) | **A** | LSM-tree 只存 key，大 value（embedding 向量）存在独立 vLog | 论证 embedding 大 value 的存储引擎选择依据 |
| **W7** | 列式格式写入（Parquet/Lance） | ColStorEval (Zeng et al., PVLDB 2023) + Lance (Pace et al., arXiv 2025) | **A** + 预印本 | Parquet/ORC 写入性能系统对比；Lance 自适应编码 | Sink 对照实验（C 系列）的格式选择依据 |

### 当前状态

项目写回 baseline 为 PostgreSQL + pgvector 的 COPY + 延迟建索引（先 COPY 到 unlogged table，再 `CREATE INDEX HNSW`），已在动机 GPU 实验中建立（pgvector writeback 0.897s vs JSON 1.567s，见 `motivation/results/gpu/`）。按 `AGENTS.md` §1/§3，写回作为工程 baseline 处理，不作为独立实验阶段；本节 W1 仅作 baseline 出处登记，不再设为待跑门禁实验。

---

## 三、数据组织侧 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 实验配置 |
|---|---|---|---|---|---|
| **D1** | Fixed Partition + Fixed Batch | Daft 官方文档 + Spark SQL Tuning Guide | 官方文档 | 固定 partition 数 + 固定 batch size；不做 workload 感知 | 当前已部分覆盖（coalesced vs fine） |
| **D2** | Pre-Shuffle Merge | Daft Shuffle 文档 | 官方文档 | 先合并 input partitions 降低 slot count，再进行 shuffle | Daft 层的 object coalescing baseline |
| **D3** | Semantic Operator Optimization | LOTUS (Patel et al., PVLDB 2025) | **A** | 准确率约束下选择代理模型、cascade、join/ranking 算法 | 数据库 AI 系统 baseline；冻结模型调用 work 后再与运行时策略比较 |
| **D4** | ML 谓词推理重写 | Smart (Guo et al., VLDB Journal 2025) | **A** | 推理重写、渐进式推理、成本最优物理优化 | AI_FILTER/AI_CLASSIFY 场景的 selectivity-aware 策略参考 |
| **D5** | Declarative Plan Search | Palimpzest (Liu et al., CIDR 2025) | 非 CCF-A | 用样本估计 time/cost/quality 并选择 Pareto 计划 | 官方系统 baseline；不把单线程默认当强 serving baseline |
| **D6** | Semantic Query Benchmark | SemBench (Lao et al., PVLDB 2026) | **A** | 55 queries、文本/图像/音频、质量/时间/成本/内存/扩展性 | workload 和评价协议依据，不是调度算法 |

---

## 四、跨层决策 Baseline

| 编号 | Baseline 名称 | 来源 | CCF | 策略要点 | 与本课题的差异 |
|---|---|---|---|---|---|
| **X1** | 代价驱动的 Compute-vs-Storage Pushdown | FlexPushdownDB (Yang et al., PVLDB 2021) | **A** | 基于代价的 push-to-storage vs pull-to-compute 决策模型 | 只覆盖 compute↔storage 维度，不覆盖 GPU batch↔write batch 的 joint decision |
| **X2** | 稀疏物化（Sparse Materialization） | AIDB (Jin et al., SIGMOD 2024) | **A** | 不是所有 ML 推理结果都物化到数据库；350× 成本降低 | 从"是否写回"角度，不涉及"写回批量和 GPU 批量的 joint optimization" |
| **X3** | 延迟视图维护 | Deferred View Maintenance (Colby et al., SIGMOD 1996) | **A** | 攒批 → 批量维护物化视图，减少事务开销 | 经典理论，但不涉及 GPU 推理侧 |
| **X4** | Semantic Operator Pareto Optimization | Abacus (Russo et al., PVLDB 2026) | **A** | profile + MAB/Pareto 搜索 quality/cost/latency 计划 | 优化调用/实现选择；本项目只迁移 profile 与 ranking 方法 |
| **X5** | UDF/Placement Cost Estimation | GRACEFUL (ICDE 2025) + COSTREAM (ICDE 2024) | **A** | 估计 UDF runtime 与异构 operator placement | 本项目首版为解析模型 + profile + residual，不直接采用复杂 GNN |

---

## 五、端到端流程调优增强对照矩阵

该矩阵用于在阶段级调优完成后分析阶段间耦合；它是增强型对照，不作为当前开题主叙事的前置假设。

| 编号 | 组名 | GPU 策略 | 写回策略 | 来源 | 代表什么 |
|---|---|---|---|---|---|
| **BL1** | GPU-Only Optimal | G1/G2 最优 B_gpu, W, endpoint | 默认 driver 写回（当前方式） | vLLM/Orca | GPU 岛最优，不管写回 |
| **BL2** | Writeback-Only Optimal | 默认 coalesced batch | W1/W2 最优 mode, B_write, sink | TurboVecDB + COPY | 写回岛最优，不管 GPU |
| **BL3** | Independent Best | BL1 的 GPU 配置 | BL2 的写回配置 | 组合 BL1+BL2 | 增强对照：检查阶段级最优拼装是否等于端到端最优 |
| **BL4** | Naive Pipeline | 固定 B_gpu，流水线写回 | 固定 overlap | Ray Data (G4) | 只做 overlap，不做 joint optimization |
| **BL5** | Queue-Decoupled | 任意 GPU 策略 | Queue → worker 写回 | pgai (W4) | 解耦但无代价模型 |
| **BL6** | FlexPushdownDB-Style | 代价决策（compute/storage pushdown） | 代价决策（compute/storage pushdown） | FlexPushdownDB (X1) | 已有跨层决策模型，但不管 GPU batch |

**最低必跑集合**（硕士论文现实约束）：BL1, BL2, BL4，加上完整优化流程。BL3 可在阶段间耦合明显时加入，用于增强论证。

---

## 六、使用检查清单

设计新实验或新 baseline 时，逐项确认：

- [ ] GPU 调度侧 baseline 是否覆盖了 vLLM/Orca/Sarathi-Serve 中的至少一种？
- [ ] 写回 baseline 是否已采用 COPY + 延迟建索引（工程 baseline，非独立实验阶段）？
- [ ] 跨层对照是否包含了 FlexPushdownDB 或 AIDB 的决策模型？
- [ ] 每个 baseline 是否标注了来源论文/系统？
- [ ] 是否避免了"常识级 strawman"作为唯一 baseline？
- [ ] 数据库 AI 系统 baseline 是否覆盖 LOTUS/Palimpzest，评价协议是否参考 SemBench？
- [ ] 多 job 是否包含 VTC/shared-credit，并同时报告聚合吞吐、每 job JCT/P99、Jain fairness 和 idle borrowing？
- [ ] 代价估计是否用 ranking/regret 验证了决策价值，而不只报告 MAPE？
