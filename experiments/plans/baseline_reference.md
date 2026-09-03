# 实验 Baseline 参考矩阵

整理日期：2026-07-16；文献、官方 benchmark、厂商 AI 算子可安装性与指标合同复审：2026-08-05

2026-09-03 增补：§0.2 定义 PG semantic gateway 的分层开销对照，§0.3 定义前缀/表示候选的因果对照；
仅为设计，未运行或恢复旧矩阵。

> **2026-07-17 口径更新**：本文中的"跨层决策""写回瓶颈""RC3"等旧术语已统一。最新 baseline 分级、研究内容定义和优先级以根 `AGENTS.md`“项目范围/当前资格顺序”、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md` 为准。
用途：正式实验设计时，从正式论文、官方系统和可审计工程默认中提取 baseline，避免使用 strawman 对照
来源：`research/ai_operator_literature_inventory.md` 与 `research/top15_ranked_papers.md`

> **2026-08-20 SAOR 对照冻结**：当前 database-E2E 主矩阵只含五个完整系统：Daft
> Native + vLLM FCFS、Daft Native/Ray + vLLM FCFS、Ray Data native API graph +
> vLLM FCFS、project frozen-static + vLLM FCFS、SAOR + vLLM FCFS。前三臂不接
> bounded-ready、K/W、credit、inflight 或 project selector；frozen-static 不接 bounded-ready、
> 动态选择或 debt；只有 SAOR 使用 concrete-ready observation、shared credit、borrowing 和
> projected-debt recovery。FIFO/DRR/VTC-style/strict-priority 的既有数据保留为历史项目内消融，
> 不进入这张系统表。
>
> 官方 VTC 另设**服务机制组**：固定 `Ying1123/VTC-artifact@192c2e...`，只比较该 artifact
> 同一 S-LoRA 栈内 FCFS 与 VTC。它不经过 Daft/Ray Data/PostgreSQL/Project coordinator，
> 也不进入 database-E2E 排名。当前 4090、模型和 runtime 兼容性未证明，状态为
> `blocked_unverified_runtime / server_validation=not_run / formal_authorized=false`；禁止用 vLLM
> reproduction 替代或冒充官方 artifact。能力合同把每个 database Job 映射为一个 VTC client，冻结
> 同一两份 prompt SHA、`[0,5]` release 与 256-token/raw/temperature-0 输出语义；Qwen2.5-7B
> 在该 S-LoRA artifact 上的兼容性明确标 `unverified`。
>
> 另增一个不进入五臂排名的**跨层完整系统 capability**：Daft Ray + vLLM native FCFS、同数据路径
> + DRR-on-vLLM reproduction、同数据路径 + VTC-on-vLLM reproduction、SAOR + vLLM native
> FCFS。官方 artifact 只作 VTC 语义参考；reproduction 名称不得省略。当前 vLLM installed-source
> SHA、Daft→Request Job identity 与 custom-FCFS parity 均未验证，故 `blocked`，不能运行 GPU 或
> 发布性能表。详细合同见 `saor_cross_layer_scheduler_capability_20260820.md`。

> **2026-07-16 方向更新**：vLLM 已定位为部署平台（非竞争对手），其 continuous batching 是 S 级 baseline——课题研究上游调度优化，不修改 vLLM 内部。新增 baseline 候选：Ray 2.49+ PrefixCacheAffinityRouter、Ray Serve batch_size_fn 等。详细背景见 `research/knowledge_hub.md`。

---

## 0. 统一入口：三类算子、四层对照、当前状态

本文件是 baseline 选择、证据分级和指标合同的**唯一总入口**。为避免把证据综述、
执行命令和实验结果复制成多份，其他文件只承担专项职责：

| 专项文件 | 唯一职责 |
|---|---|
| `completed/text_native_baseline_rerun_20260802.md` | AI_COMPLETE 的历史 Chat/Completions 分轨与已完成开题范围合同 |
| `completed/image_clip_workload_lock_20260731.md` | AI_EMBED/AI_CLASSIFY 已完成的 workload、语义、质量和图像静态合同 |
| `experiment_status_and_gaps.md` | 当前完成度、阻断项和下一步，不复制 baseline 原理 |
| `../../research/evaluation_metrics_survey_20260731.md` | 厂商/论文指标的来源证据，不承担运行职责 |
| `../results/` 与 `../../motivation/results/` | 原始数据、七步分析和结论的权威来源 |
| `archive/database_ai_operator_baseline_matrix_20260729.md` | 2026-07-29 文本预注册与执行历史，仅供追溯，不指导新实验 |

三类算子统一使用四层对照，不能只列同机 runtime，也不能把外部厂商 raw time 与本机
结果直接混排：

| 算子 | 服务/计算上限 | 同机原生框架 | 产品/公开 benchmark | 项目主对照 |
|---|---|---|---|---|
| **AI_COMPLETE** | vLLM Bench；bounded Chat/Completions 为 direct control | Daft `prompt()` Native/Ray；Ray Data HTTP Processor | 可本地：Doris `AI_GENERATE`、ClickHouse `aiGenerate`、StarRocks `ai_query`、Oracle `UTL_TO_GENERATE_TEXT`、Db2 `TEXT_GENERATION`、OceanBase `AI_COMPLETE`；云端另列 Snowflake/BigQuery/Databricks 等 | calibration 后冻结的 static active-work/actor/flush；dynamic 只与其比较 |
| **AI_EMBED** | direct embedding service；CLIP R0/vLLM pooling | Daft `embed_image`；Ray Data native graph | 可本地：Doris `EMBED`、ClickHouse `aiEmbed`、Oracle `UTL_TO_EMBEDDING(S)`、Db2 `TO_EMBEDDING`、SQL Server `AI_GENERATE_EMBEDDINGS`；DuckDB/pgai 扩展另列；云端多模态产品另列 | 冻结 frame/work budget、batch、actor shape 的 project static |
| **AI_CLASSIFY** | GPU-resident/direct classifier | Daft `classify_image`；Ray Data native graph；可运行时 Spark | 可本地：Doris `AI_CLASSIFY`、ClickHouse `aiClassify`、DuckDB `ai_classify`（社区扩展）；官方 803,580-row ImageNet/ResNet18、Snowflake/BigQuery、SemBench | 冻结 project static；adaptive 报相对 static 与 oracle regret |

当前执行状态只作导航，数字仍以结果目录为准：

| 轨道 | 当前状态 | 下一门禁 |
|---|---|---|
| AI_COMPLETE service/direct | bounded C32/C64/C128/C256 饱和校准已完成；C128 达 C256 已测峰值 98.22% | 当前不扩矩阵；新签名重新独立校准 |
| AI_COMPLETE Daft/Ray Data native | 2,048-row 单 Job 16/16 cell、12 formal 已完成；五臂共同 gateway rehearsal 5/5 通过 | 五臂 formal 未运行；0s/5s isolation 仍需独立 control |
| OceanBase `AI_COMPLETE` | 普通 AutoDL 容器 observer init `-9100`，`blocked` | privileged/seccomp-unconfined 容器或 VM |
| Doris / ClickHouse AI SQL | 官方文档已确认可自托管和 OpenAI-compatible endpoint；尚未在本机安装 | 固定版本后一行协议 gate → 计数/缓存 gate → 独立 calibration |
| StarRocks `ai_query` | 4.1.1 已修复函数注册；独立函数文档和稳定性仍不足 | 固定 4.1.1+，关闭响应缓存后做一行 vLLM gate |
| Oracle / Db2 AI SQL | 官方确认本地版本与文本生成/embedding；尚未在本机安装 | 核对 Free/Community 镜像是否含功能、TLS/endpoint 协议和资源限制 |
| SQL Server AI embedding | 2025 可本地 Docker；仅 embedding，远端 endpoint 要求 HTTPS | TLS reverse proxy + 一行 embedding gate |
| DuckDB `ai`（已安装 + 接入框架）/ PostgreSQL pgai | DuckDB `ai` 已装 **duckdb 1.5.4**（1.5.5 无 ai 扩展二进制）+ `INSTALL ai FROM community`，adapter `duckdb_ai` 已接入 baseline 框架原生执行（`code/src/baselines/text/products/duckdb_ai.py`）；pgai 仍是已归档历史扩展 | DuckDB：固定 duckdb 版本 + ai 扩展版本/commit，记 `duckdb_ai_max_concurrent_requests` 等扩展自有旋钮，用 `openai_compatible` provider 指向 vLLM；单列 extension baseline，不冒充数据库 core |
| Daft built-in image embedding | 12K 能力/容量证据与原生图像四 Job 正式观察已完成；20K 起触发 object-store 容量失败，不能与 120K streaming 路径同规模排名 | 保留容量边界；不注入 Project 调度或扩容追排名 |
| Ray Data native image graph | 120K matched-resource 正式对照与原生图像四 Job 观察已完成 | 只按冻结官方 graph 报告；不注入 Project broker |
| vLLM CLIP pooling | 当前 0.25.1 环境两次 1-image offline gate 均 600s timeout，无 embedding 输出 | `blocked`；不运行在线/5K/60K，只在新隔离环境重新做 capability gate |
| 官方 ImageNet/ResNet18 parity | upstream commit、文件哈希和适配白名单已冻结；128-row native gate 已完成 | single controls/formal 尚未运行，且不阻塞当前主线 |
| project image static | 120K matched-resource 正式对照已完成；staged descriptor/observe-only 24/24 group 已归档 | HSE static GPU 非劣门后再接单一动态动作与质量检查 |
| Snowflake/BigQuery/PolarDB/学术系统 | external/capability evidence | 仅在语义、质量、模型和计时边界可对齐时升级为数字比较 |

### 0.1 多 Job fixed-envelope：原生 baseline、项目实验臂与内部消融（2026-08-13 纠偏）

`BoundedReadyWindow` 是**项目自有的上游执行机制**：每个 Project Job 把已经到达、payload 和
estimated work 均已确定的有限多个 concrete request 预注册给项目 coordinator，使 selector
不再只能看到每个 Job 的单个 head。它不修改 vLLM continuous batching，也不扩大已经授予的
active K/W；但会增加 active credit 之外的 host-side ready buffer，因此必须另报 request/work/
bytes、内存与 CPU。它不是 Daft、Ray Data、vLLM 或数据库产品的原生调度能力。

因此正式评价严格分层：**任何原生 baseline 都不得接入 bounded-ready**。只有为了归因项目内部
机制时，若比较不同 selector，才让这些 Project internal controls 使用同一个 bounded-ready
observation contract。当前身份冻结为：

| 层级与身份 | 对象 | scheduler owner | 是否使用项目 bounded-ready | 报告角色 |
|---|---|---|---|---|
| 服务上限 | bounded direct HTTP / vLLM Bench | vLLM + 原生/简单有界客户端 | 否 | saturation ceiling，不是公平 baseline |
| 原生系统 baseline | Daft `prompt()` Native/Ray、Ray Data native graph、通过门禁的数据库产品 | 被测框架/产品 | **否，禁止注入** | 同合同系统主比较；保留其原生 batching/backpressure/scheduling |
| 项目静态参照 | project frozen-static partition/reservation | Project | 否；保持冻结静态执行路径 | 同栈隔离/Pareto reference，不能简称“原生 baseline” |
| 项目内标准算法 control（no-bounded-ready） | single-head FIFO/DRR/VTC-style | **Project shared-credit coordinator 的本地实现** | 否 | 用已有算法作完整调度包/observation bridge 对照；不是 Daft/Ray/vLLM upstream-native 实现 |
| 项目历史诊断 | old single-head SAOR | Project | 否 | 只定位旧 SAOR observation gap，不作为 proposed |
| 项目 matched-observation FIFO control | project bounded-ready + global FIFO | **Project shared-credit coordinator 的本地实现** | 是 | FIFO control 的候选集配平变体；与 single-head FIFO 配对衡量 ready-state exposure |
| 项目 matched-observation 份额 control | project bounded-ready + DRR/WFQ、external VTC-style actual-work counter | **Project shared-credit coordinator 的本地实现** | 是 | 复用已有 DRR/VTC 思想的 control，判断复杂 selector 是否必要；不是上游 artifact |
| 项目优先级上界消融 | project bounded-ready + strict-priority/EDF | Project | 是 | 测 SLO 可达上界与 starvation 代价；属于 internal ablation |
| 项目 proposed | project bounded-ready + $H_B=0.125W_e$ guarded priority/debt | Project | 是 | 判断 debt guard 是否有独立 Pareto 增量 |
| 引擎内相关工作 | VTC、DLPM、JITServe、Llumnix、SCORPIO、ProServe | 原论文系统 | 不由本项目注入 | 理论/系统参照；没有原实现同层复现时不冒充 executable baseline |

若论文问题是“加入 SAOR 后，完整数据库 AI 多 Job 系统是否比原生 Daft/Ray 更好”，正式证据
必须同时包含下面两层，任一层都不能代替另一层：

| 证据层 | 回答的问题 | 最小对象 | 可以声称什么 |
|---|---|---|---|
| **系统级 matched comparison** | 完整 Project/SAOR 系统是否有实际经验价值 | Daft `prompt()` Native、Daft `prompt()` Ray（两种 runner 均可执行时分列）、Ray Data native graph、project frozen-static、project bounded-ready + guarded debt | 相同数据库 AI workload 与物理资源下，各完整系统的经验性 E2E 性能、SLO、资源与 correctness 差异 |
| **机制级 attribution** | Project 收益来自 ready-state exposure、共享容量还是 selector | Project coordinator 内的 no-bounded-ready FIFO/DRR/VTC-style 标准算法 controls（最小桥接为 single-head + shared FIFO）；另设 bounded-ready + FIFO、DRR、VTC-style、strict-priority、guarded debt matched controls | observation、共享容量和 selector 的项目内部因果分解；所有这些臂均复用项目执行栈，不得冒充 upstream-native/system baseline |

系统级原生臂保留各自官方 batching/backpressure/scheduler，不向其强塞 Project K/W、credit、
coordinator 或 bounded-ready；但仍须冻结相同 GPU/CPU、endpoint、模型与 vLLM FCFS 服务签名、
immutable workload/arrival、PostgreSQL source、协议/输出上限、正确性合同和 balanced run
order。原生臂只允许使用预注册的官方/原生旋钮及各自独立 calibration，避免以默认欠调优制造
strawman。Project frozen-static 与 SAOR 则冻结相同最大 K/W、ready-buffer 口径之外的项目栈和
服务配置。该层比较不同 scheduler owner，结论必须写“完整系统经验表现”，不能写成 SAOR
selector 的单因素因果收益。

当前 SAOR 系统级复测的共同到达形态冻结为 Job 级
`bulk@0s → foreground@5s`、Job 内 eager。Daft/Ray Data 官方 graph 若没有忠实逐请求 timed
replay 接口，不得用外部 feeder 接管其请求释放后仍称原生调度；Project 两臂也须使用同一 Job
级形态。五臂共同计时从 PostgreSQL scan/materialization 前开始，到两 Job 全部输出写入共同
`document_completions` JSON-text sink 并通过独立 readback digest/exactly-once 校验结束；计时前读取
JSONL 的旧 native path 只作诊断。group JCT/database-E2E 含共同 sink；per-Job JCT 截止该 Job
完成模型响应，避免把 group 末尾统一 sink barrier 伪装成两个 Job 各自的完成时刻。原生臂没有
真实共同 request clock 时，P99/SLO 必须写
`unavailable`。详细规格见
`../../code_doc/superpowers/specs/2026-08-13-saor-native-system-matched-comparison-design.md`。
历史 Project 内部 bounded-ready FIFO/DRR/VTC-style/SAOR same-regime 数据只保留为消融归档；
本轮五臂 runner 不再生成或排名该表，也不据其产生 selector winner/formal claim。
这里的 FIFO 全名是 `Project bounded-ready + global FIFO matched-control`：bounded-ready 不是
FIFO 算法的组成，而是让 FIFO 与 DRR/VTC-style/SAOR 共享同一个可见候选集 $R(t)$，从而只比较
selector score/order。完整旧方案仍是 single-head + shared FIFO；二者之间的差异属于 observation
effect，不能计入 FIFO/SAOR selector 差异。

这里必须区分“算法来源”和“实现/调度所有者”：FIFO、DRR、VTC 是已有算法思想，但本项目实验
中的可执行代码位于 Project `shared_credit.py`，不是 Daft、Ray Data、upstream vLLM 或 VTC
artifact 的原生实现；所有获准请求随后共同进入 upstream vLLM FCFS + continuous batching。
因此 Daft/Ray Data 才称**原生系统 baseline**，FIFO/DRR/VTC-style 只称**项目内标准算法
control**。为做 selector 单因素归因，可以另复制一个接入 Project bounded-ready 的
matched-control 版本：这不表示 bounded-ready 变成 FIFO/DRR/VTC 的组成，只是让各 selector
看到同一批 ready request。因此 `single-head/no-bounded-ready + FIFO/DRR/VTC-style` 承担标准
算法对照身份；
`bounded-ready + FIFO/DRR/VTC-style` 只承担 matched-observation control 身份，不能取代前者。
报告必须同时标注 `scheduler_owner`、observation contract 和实现来源，禁止省略前缀只写“DRR”。

“同一个 ready-window”只约束最后四类**项目内部 selector 归因臂**：每个臂内的所有 Job 都应用
同样的 ready request/work/bytes 上限，且共享 immutable manifest、arrival、vLLM FCFS、prefix-
cache 生命周期、active K/W、CPU/GPU/endpoint 和 balanced run order。原生 baseline 保持各自
调度，不能为了表面配平而套入项目 coordinator。若 proposed 只赢旧 single-head 路径而不赢
bounded-ready 简单 internal controls，不能写 SAOR selector 胜出。equal-share 场景用项目内
DRR/VTC-style 与 service lag；foreground/bulk differentiated-service 场景用项目内 strict-
priority/EDF、foreground SLO goodput 与 bulk reserved-share JCT/starvation guard。

当前 bounded-ready 五个 selector 同窗口只能回答“哪一个 selector 在相同 observation 下更好”；
其中 FIFO/DRR/VTC-style 是项目内标准算法 controls 的 matched-observation 版本，strict-priority 是
SLO 上界 control，guarded debt 是 proposed，而不是“五个 proposed internal controls”。
`project frozen-static` 同时与它们在 static/shared capacity、single-head/bounded-ready 和 selector
上不同，不能单独归因 ready-state exposure。若论文要把 bounded-ready 本身写成独立贡献，必须
增加 `single-head + shared FIFO`：`frozen-static → single-head + shared FIFO` 主要观察共享容量，
`single-head + shared FIFO → bounded-ready + FIFO` 主要观察 observation/execution path，随后才
比较同 bounded-ready 下的不同 selector。若要报告“完整 SAOR 包相对 DRR/VTC 包”的差值，还应
保留 no-bounded-ready DRR/VTC-style；它们可在签名完全一致时复用旧 formal，否则重跑。

历史原生多 Job 数据只有在 manifest SHA/顺序、arrival offset/scale/barrier、模型与服务签名、
PG source/sink 及计时边界、协议/输出 cap、硬件/endpoint、correctness 和所需指标 schema 全部
一致时才能严格复用；任一项不一致或缺失即重跑，禁止把历史数字与新 Project 结果硬拼进同表。

正式报告必须分别给出：① 外部公开 benchmark 锚点；② 同机原生系统排名；③ 数据库
AI 算子的 quality-cost-time；④ project frozen-static/dynamic 消融。只给 tokens/s 或
images/s 不能构成完整的数据库 AI 算子评价。

当前 baseline 主问题是单租户多 Job/workload class，Project 使用 `job_id` 公平键并按 intra-tenant
Job fairness/service differentiation 报告；不要求原生系统暴露 tenant resource-group，也不把
多租户能力作为当前准入门。多租户只保留 future-compatible 边界：届时原生系统使用官方 user/
resource-group 语义，Project 在现有 Job scheduler 外增加稳定 principal 聚合 entitlement/debt、
per-tenant buffer cap 与 anti-splitting 门，不能把 flat `job_id` 竞争直接改名为 tenant fairness。
当前仍将公平（按 Job 份额/lag）与隔离（固定 victim 在 aggressor normal→burst 下的 P99/goodput/
SLO 变化）分表报告。

---

<a id="gateway-layered-controls"></a>

### 0.2 PG semantic gateway：A/B/C/D 分层开销对照（设计，待执行）

问题是：额外 UDS/wire、PG carrier 与 SemLoom 执行控制各引入多少成本，在哪些请求规模和并发条件下
影响端到端表现。它支撑数据组织与提交/路由研究，不是新的研究内容，也不是原生产品的排名。
架构与实现条件见[主计划](postgresql_ai_semantic_operator_architecture_20260827.md#multi-session-execution)；
当前 fixture 资源检查不能回答性能开销。历史 observation-only HTTP gateway 与本处 semantic UDS
gateway 的协议/职责不同，其数字不直接移用。

| 臂 | 执行关系 | 用途与进入条件 |
|---|---|---|
| A：matched direct | 合成/公开任务客户端 → fixed HTTP Adapter → 同一模型服务 | 与 B 使用相同请求和 HTTP 实现/配置，隔离额外 semantic 协议路径；这一个点不是服务最大容量，上限另用匹配的原生 benchmark/并发扫描 |
| B：thin semantic gateway | 同一任务客户端 → UDS/wire → 固定 gateway → 同一模型服务 | 补足无 PG 的协议路径对照；仍遵守选定算子的完整 spec、sequence、messages 与 completion 校验，不把 v3/v4 当任意 prompt API |
| C：PG reference | PG18.3 ordinary child → 语义算子 → 同一 gateway/Adapter → 模型服务 | 衡量 PG 集成路径；只用已验收 SQL/协议，生成型 Map 须先完成其 PG 接线与资格 |
| D：PG + SemLoom | 同一 PG 输入与语义 → gateway → SemLoom → 同一模型服务 | 相应增量接入与资源检查通过后执行；同量 work 下比较执行控制，不将异步窗口或换模型收益混称调度算法贡献 |

先验证请求与结果一致，再分别作低负载单请求开销和同上限负载扫描：

1. 选定同一输入/规范消息 manifest、模型 revision、模板、有效生成参数、服务/硬件、输出检查、
   冷热状态、到达时间与重复方法。记录实际 model calls/attempts、prompt/output usage、输出与错误；
   token 数或模型工作不匹配时单列，不直接归因执行开销。固定模型 reference 可以用于工程诊断，
   但不会因此获得尚未通过的 Filter 质量资格。
2. A/B 对齐 HTTP 客户端、连接复用策略、DNS/TLS 条件与总 deadline，分列 gateway 启动/握手及稳定期。
   B 包含客户端编码、UDS、两端摘要/JSON/复制和返回校验等成本，B−A 不是“纯网络转发耗时”。
   连接池的收益另做同路径 on/off 对照，不把两个臂不同的连接策略隐藏在协议开销里。
3. 当前每 session 一个在途任务且 gateway 串行服务会话，只能按该实际能力定义 A/B/C 的匹配点。
   多会话正确性通过后才测多 backend，分别限制活跃 session、全局请求/estimated work 和队列 bytes；
   PG 一项同步与无界异步客户端的差值只能说明供给差异，不能据此评价 gateway 或方法优劣。
4. C/D 使用相同 PG 数据、ordinary child/snapshot、SQL 输出或 INSERT 写回与消费方式，结果有界读取。
   A/B 明确排除数据库取数，不冒充 database-E2E。分别报告服务窗口和含 source/结果校验的完整窗口；
   C−B 是这两条匹配路径的实测差异，须结合分阶段观测解释，不能称为一个恒定的 PG 税。
5. D 可运行后先保留同一 SemLoom 路径上固定顺序/固定上限的无策略控制，再逐项启用组织、准入、路由。
   D−C 是接入与方法的组合差异；方法消融须保持物理路径、观测、容量及 HTTP 复用相同。
   原生系统仍使用自己的执行与调度，此处的自有控制不冒充原生 baseline。
6. 记录序列化/摘要/拷贝、open、queue/admission、DNS/connect/TLS、HTTP/model、fan-in/reorder、
   PG result/writeback，报告全部重复的 JCT、吞吐、延迟分位、CPU/RSS/FD、调用/usage、取消浪费与错误。
   无可靠观测的细分阶段标 unavailable；重叠阶段、吞吐及分位数不能通过相减得到可加的独立成本。

fake/golden 先验证关联、并发进展、停止与资源上限，不能据人工 delay 推出真实模型吞吐或“两倍并发收益”。
正式运行另立具体切片计划，固定 workload、请求预算、配置、重复/交错顺序、资源与停止条件；
已有 A/B/C 所需部件不等于该比较已获授权或已可完整运行。D 尚未接入时写 pending，不用占位数字补齐。
本节不消耗四 D 的有限真实服务预算，不恢复校准、旧 SAOR formal 或新的模型下载。

<a id="semantic-prefix-causal-controls"></a>

### 0.3 前缀与表示候选：先证明损失，再证明方法（设计，待执行）

研究问题与最近邻限制见[设计审查](../../research/semantic_prefix_reuse_design_audit_20260903.md)，
工程位置见[主计划 §7](postgresql_ai_semantic_operator_architecture_20260827.md#research-mechanism-slices)。
本节定义可复用对照，不决定具体模型、数据、参数或运行预算；每个实验仍须另有当前切片计划。

**先做机会与上界检查。** 使用实际 tokenizer/chat template 与部署签名，区分潜在 token LCP、
完整共享 blocks、实际 cached work 与估计。短公共 JSON、相同行号或总 KV 使用率均不足以证明
机会。记录 source/编码/提交空隙、服务忙闲与可改善 prefill 占比；上界不足覆盖新组织成本时停止
该候选。同步 PG 是正确性 reference，不能作为唯一性能 baseline。

**强对照分层。** 同一执行栈的算法消融与作者原生系统分别报告，不以名称相同代替实现相同。

| 问题 | 最低对照 | 配平对象 |
|---|---|---|
| 不改 messages 的组织是否有效 | 固定顺序＋work budget、row-only 分组/重排、简单 prefix-first、最少 work/HRW；已有适用强静态点保留 | 同消息/调用与生成要求，同 ready/active/结果缓冲上限，同服务与 HTTP 栈 |
| 字段/程序改变是否值得 | canonical、一个明确候选；只有结构化字段合同成立后才比较 fixed field order 与 GGR-style row/field reorder | 质量适用性、完整任务、授权字段、真实 prompt/output work 和编译成本；不是机械宣称“同 work” |
| 依赖是否提供新信息 | 固定深度/广度优先、及时下游、prefix-first，以及适用的 Kalypso paper reproduction | 相同合法依赖/输入可见规则及资源；只仿照“立即 Map”不能叫完整 Kalypso；无作者 artifact 时不冒充 native |
| 联合选择是否必要 | 横向/纵向简单控制、调优集选定的固定组合/静态切换、候选联合策略、小规模受约束 oracle | 相同候选表示与调参预算；oracle 也受依赖、容量/内存、合法输出和质量要求限制，不是无限缓存/未来数据算法 |
| 数据库信息本身是否有效 | 相同算法去掉/打乱关系或依赖 hint 的信息消融，prompt-only prefix tree 对照 | 实际 payload 和合法依赖仍正确，只隐藏用于评分的信息；不能破坏正确性或减少候选集制造优势 |

同窗口意味着相同 source/到达输入、候选揭示规则与 request/work/bytes 上限，不是强制每秒的 R(t)
相同：不同先前动作会改变完成与 ready 集合。只比较决策函数时可另用相同快照 replay，标为离线
决策诊断；完整系统测试保留这种内生变化。原生系统保留自己的调度，不能注入 SemLoom 窗口后仍称原生。

**布局 × 执行时机的 2×2 只作为机制检查。**

| | 显式算子间屏障 | 依赖满足后流水 |
|---|---|---|
| canonical 程序 | A | B |
| 新程序 | C | D |

两种时机使用相同活跃容量；流水不等于一次只跑一行。屏障是实验控制，不能写成当前 PG Filter→Map
已有实现。对每个独立重复计算 I = (T_C − T_D) − (T_A − T_B)，报告配对差异及区间；I>0 只表示
布局放大了所测流水收益，不自动证明是 cache 所致。保留 cache-off 的对应控制、命中及排队时序；
没有命中时流水仍可能因重叠有收益，不能预设乘积关系或 cache-off 必须零差异。

不同程序可能改变 Filter 的存活行、Map 输出长度、失败率和调用数，必须分成两类结果：

- 自然语义执行：各臂运行自己的合法程序，报告实际质量、存活集合、调用/输出 work 与总耗时；
  较少正确行或更多失败不能当速度收益。质量合格也不表示结果集合必然相同。
- 相同任务图诊断：可用预先记录的相同 survivor mask/已定任务重放隔离执行时机，记录 mask 来源
  与所有实际调用；它不等于自然 Filter→Map 或 PG 端到端结果。不能只保留各臂共同成功的事后子集。
  若输出 work 仍不同，继续单列，不强迫生产 parser 截断/修补以制造配平。

相同 messages 与 temperature=0 也不保证真实模型逐字确定；并发配置改变后的自然结果差异照实
保留，区分系统正确性、任务质量和模型非确定性，不能只挑输出一致的重复进入性能统计。

**压力与反例在运行前确定。** 至少覆盖无/短前缀、嵌套兼容的横纵前缀、确有冲突的候选、缓存
足够与不足、不同 Filter selectivity、prefill/decode 比例、冷启动/暖态及队列压力。选择率依据任务
分布或诊断 mask，不按优化臂执行后的有利结果分桶。未来仍需复用的 unique prefix blocks 与可用 KV
bytes 的比率只作带来源的情景描述；共享块去重，使用实际部署容量，不把 gpu-memory-utilization
百分比或 sum(input tokens) 直接当 KV 分母/占用。未知量写 estimated/unavailable。
默认条件无收益时保留负结果；不临时缩缓存、换模型/模态、放大并发或补调参数使候选过线。

**评价选择，不只评价拟合。** cost/quality 的开发、选型和最终 held-out 数据分别管理，同一
文档/前缀家族不要因行级随机切分同时进入训练与测试。特征只来自决定前可见的记录；命中/输出/未来
survivor 真值不能作为线上输入。报告 pairwise ordering、top-1、实际选择额外 JCT 及最差 regret。
受约束 oracle 是已批准候选集合里事后最好的匹配控制，不证明全局最优；计算方法与重复噪声一并报告。
准备样本、tokenizer、质量评估、calibration、重排与候选搜索时间均计入总成本或明确给出摊销条件。

每次独立 run 以完整查询为重复单位，固定输入、到达、服务配置和预热/cache 起点；采用交错顺序、
保存全部重复与失败。预先确定效果阈值、置信方法、样本量与质量非劣要求，不把同一例多次生成当成
独立数据，也不从同一 trace 随意抽行 bootstrap 当作独立系统重复。多候选选择应说明多重比较处理。

报告至少包含 query JCT、首结果/尾延迟、合法完成行数和任务质量、model calls/usage、实际缓存证据
等级、CPU/PG/gateway 缓冲、FD、active/ready work、未消费或取消后仍计算的 work。GPU 利用率下降
可能来自省去计算，不能单看利用率判断效率；能耗或“正确工作/能耗”只有测量与质量分母都成立时报告。
逐任务 cached_tokens 缺失不使 SQL 失败，但该 run 不作逐任务命中归因；aggregate metric 不冒充
逐行证据，来自 sidecar 的观测也不冒充现有 wire 已绑定字段。

独立核心测试与 PG 多在途接入不以本候选实验成功为前提。反过来，fixture/独立 producer 的正结果
也不替代 PG18.3 新路径验收；正式 PG 比较仍按 §0.2 和主计划完成接入、正确性与资源检查。

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

2026-08-05 AutoDL 能力审计更新：upstream 脚本读取的是公开
`s3://daft-oss-public-datasets/imagenet/benchmark` parquet，而不是项目本地 COCO 或
Hugging Face imagefolder；HF token/许可通过不等于该 official workload 已准备好。
短下载仅证明对象公开且返回 parquet 字节，尚不能证明全 workload 的可用带宽。当前
`daft-062` venv 实测为 Daft 0.6.2 + Ray 2.56.1，未满足冻结的 Ray 2.49.2；服务器也未
保留三份已校验 upstream 脚本。因此状态是 `blocked-before-gate`，不是“环境/代码/数据
全部就绪”。下一步只运行版本隔离、三文件 SHA 和一个完整 parquet object 的计时门禁；
若 AutoDL 带宽不足，可在不改变 rows/labels/transforms 的前提下镜像 exact upstream
objects 到本地路径，并将该输入路径改动写入 adapter diff，不能用不同 HF 子集替代。

文本轨道采用同一 fail-closed 规则：每个 summary 由
`code/src/baselines/provenance.py` 写入 comparison role、implementation provenance、
scheduler owner、custom scheduling、formal eligibility、upstream source 和资格门禁。
`vLLM Bench` 是 service ceiling，项目 `bounded_*` 是 direct controls，二者均不标记为
native baseline；Daft built-in `functions.prompt`、Ray Data HTTP Processor 和通过
capability gate 的 OceanBase `AI_COMPLETE` 才可进入原生系统排名。执行合同见
`completed/text_native_baseline_rerun_20260802.md`。

### 5. 过期清理规则

候选状态统一使用 `candidate → capability-verified → gated → calibrated → formal`；
失败或超出 scope 使用 `blocked` / `related-work-only` / `retired`。发生以下任一情况时
必须复审：官方文档或 benchmark 更新、软件/模型版本变化、计时边界变化、同名 arm
实现变化、出现相反的公开排名、项目新结果推翻旧结论。旧数字若仍有诊断价值，移到
带日期的结果报告并标成历史证据；当前入口不得继续写成“下一步”或“当前默认”。

`baseline_reference.md` 负责来源、分层、准入与指标合同；
`archive/database_ai_operator_baseline_matrix_20260729.md` 是文本轨道预注册和历史记录；
`completed/image_clip_workload_lock_20260731.md` 是图像 workload 与静态执行的完成合同；当前动态执行以
`state_aware_work_unit_evaluation_20260808.md` 为准。各 `results/README.md` 和
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
已完成的文本复测合同见 `completed/text_native_baseline_rerun_20260802.md`；旧预注册仅在
`archive/database_ai_operator_baseline_matrix_20260729.md` 保留。

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
| product-integrated data engine | PolarDB Lakebase / OceanBase Lakebase 的 Daft-on-Ray 多模态计算 | 同一 Daft/Ray 架构家族的工业集成；与数据库 SQL AI Function 分开，不冒充独立内核执行方式 |
| managed product SQL | Snowflake / BigQuery image `AI_CLASSIFY` | 闭源托管路径只比较 E2E、成本、质量和失败率，不跨硬件比较 MFU |
| project static | 冻结最佳 frame budget/active batches/actor shape | 动态策略的唯一主对照 |
| project adaptive | state-aware request shaping/shared credit | 只报告相对 frozen static 的增量与 oracle regret |

当前 1.296×/1.138× 只属于 `project static vs project-authored Daft UDF diagnostic`
历史结果，不能写成优于 Daft 内置 AI Function、官方 benchmark 或 PolarDB 异构
流水线。每个系统同时报告 matched-resource 与 independently
calibrated best-achievable；统一输入、模型、输出、生命周期、计时边界和 sink。

OceanBase 需要拆成两条产品面：OceanBase Database 4.5/4.6 的 SQL AI Function 当前
确证的是文本 `AI_COMPLETE`、文本 `AI_EMBED` 与 `AI_RERANK`；OceanBase AI Database /
Lakebase 的公开架构另以共享对象存储、统一 catalog 和多模表为数据底座，由 **Daft on
Ray 执行多模态 AI inference**。后者是产品集成架构证据，但当前没有公开的、可固定
commit 并在本项目服务器运行的 OceanBase vendor benchmark，因此不能把 Lakebase 宣传页
冒充同机数字 baseline，也不能据此把 SQL AI Function 写成图像分类算子。OceanBase CE
4.5.0 动态部署仍受当前 AutoDL 容器门禁阻塞。公开 ImageNet/ResNet18 系统 benchmark 只提供方法模板：
PolarDB 与 Ray 官方页面对 Daft/Ray Data 的 raw 排名方向并不一致，因此外部数字不跨
硬件排名，必须在本项目机器上按同数据、同模型、同版本分别校准重跑。

来源：[OceanBase AI Database Lakebase architecture](https://en.oceanbase.com/blog/oceanbase-ai-database-lakebase-architecture)、
[OceanBase DataStudio Daft-on-Ray workflow](https://en.oceanbase.com/blog/oceanbase-datastudio-unified-ai-data-production)、
[OceanBase AI Function](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975)。

OceanBase 公开材料对实验合同的可用部分与禁用部分如下：

| 公开证据 | 可采用的评价项 | 不得替代的实验 |
|---|---|---|
| Cloud AI Services | 24h success rate、TTFT、token output rate、token/request quota 与 rate limit | database-E2E、DB fetch/queue/writeback、exactly-once |
| SQL AI Function | `AI_COMPLETE`/`AI_EMBED`/`AI_RERANK` 的真实 SQL 语义、endpoint 与批量表列调用形态 | 没有公开性能报告，不能从语法示例推导吞吐或延迟 |
| Lakebase/DataStudio | 常驻 Ray actor、micro-partition 调度、CPU/GPU pipeline、多模态回填场景 | 没有公开 OceanBase runner/数据/硬件/raw logs，不能声称其 Daft 快于 Ray Data |
| Sysbench/TPC-H/VectorDBBench | 分别作 DB 引擎与 retrieval closure 的相邻门禁 | 不能充当 AI Function 或 Daft-on-Ray AI pipeline benchmark |

因此 OceanBase 文本产品臂若恢复，应至少报告 correct rows/s、input/output/total tokens/s、
operator/database-E2E JCT、TTFT、request p50/p95/p99、row success/error/truncation、EM/F1、
实际调用/token、成本与数据库 CPU；endpoint 侧同步采集 running/waiting/KV/prefix-cache、
GPU/MFU/能耗。OceanBase Lakebase 不设未经证实的性能臂，Daft/Ray 性能由两方官方代码
同机复现。**IMLane** 的正式 PVLDB 19(12): 4223–4236 论文与作者 artifact 已公开。论文在
OceanBase Paetica 4.3 和 DuckDB 0.10.1 上拆分 process-level execution 与 batch-wise asynchronous
scheduling，并比较 IMBridge、pandas、SparkSQL 与 Ray Data；artifact commit `072f8257` 包含 DBEnd、
Coordinator、独立 backend executors 和 Ray adapter。它因此是 DB bridge、execution-batch formation、
异步提交与 Lane/resource scheduling 的直接可运行候选 baseline，不再只按 vendor summary 登记。
当前仍缺本项目同机 build/correctness/raw-repeat 证据，不能把论文汇总倍率直接写入项目排名；artifact
的简单 queue/core hint 也不能冒充项目 token/work-aware multi-Job policy。

**Kalypso** 当前只有 arXiv:2607.23815v2，未定位作者 artifact。它是 stage/dependency、prefix lease、
KV-aware admission 与 virtual pinning 的直接方法参考，但不是数据库 bridge 或 row batching system，
也不进入当前 baseline 矩阵。若未来另行实现等价 Algorithm 1，只能标为 paper reproduction/diagnostic，
不得标成 Kalypso native；任何后续 matched comparison 还必须区分其单 query、单 cache domain 设置与
本项目多 Job、多 endpoint 目标。

补充来源：[OceanBase Cloud AI Services release notes](https://en.oceanbase.com/docs/common-oceanbase-cloud-10000000003353421)、
[OceanBase performance testing](https://en.oceanbase.com/docs/common-oceanbase-cloud-10000000002694815)、
[VectorDBBench guide](https://en.oceanbase.com/docs/common-oceanbase-database-10000000002164117)、
[OceanBase publications](https://github.com/oceanbase/publications)、
[IMLane 正式论文](https://www.vldb.org/pvldb/vol19/p4223-xu.pdf)、
[IMLane 作者 artifact](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)、
[Kalypso arXiv v2](https://arxiv.org/abs/2607.23815)。

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

#### Daft / Ray 用户常测场景与可复用 workload

官方示例和 benchmark 显示，Daft-on-Ray 与 Ray Data 最常用于“对象/表数据读取 → CPU
解码或解析 → GPU 批推理 → 列式结果写出”的离线流水线，而不是只测一个模型 kernel。
可复用场景按与本课题的接近程度排序如下：

| 场景 | 官方 workload / 模型 | 对本项目的用途 | 边界 |
|---|---|---|---|
| 图像分类 | ImageNet、ResNet18，80,358 unique images 重复 10 次形成 803,580 rows | 当前最强 vendor-code parity；同时测 CPU decode、GPU inference 和写出 | 重复行可能放大缓存效应，必须另报 unique track |
| 文档 embedding | 10K PDF、`all-MiniLM-L6-v2` | 文本解析重、输出固定，适合验证 CPU/GPU overlap | 不是生成式 AI_COMPLETE |
| 音频转写 | Common Voice 17、Whisper-tiny | 变长输入、CPU decode 与 GPU 推理异质性 | 输出和错误指标需按 ASR 语义补 WER |
| 视频检测 | Hollywood2、YOLO11n | frame-budget、多级 decode/inference、长对象 | 当前双 4090 本地存储不足时只做小门禁 |
| 大图 embedding | 大型 Parquet/base64 image、ViT | 检验 source/decode/H2D/GPU 的木桶效应 | 公开规模可达 TiB，不能直接搬到当前 AutoDL |
| LLM 离线推理 | OpenOrca/ShareGPT/SQuAD 等、vLLM | 对齐项目 AI_COMPLETE、变长 output、SLO 和 token-work | 与图像官方 benchmark 分轨，不混 raw throughput |

Daft 官方四模态 benchmark 提供 Daft、Ray Data、Spark 的代码、集群配置和日志，适合作为
可复现厂商代码轨；Ray Data 官方 batch-inference 合同则是 `load → map_batches → consume/write`。
两者都不是独立第三方裁判。来源：[Daft Benchmarks](https://docs.getdaft.io/en/stable/benchmarks/)、
[Daft image-classification source](https://github.com/Eventual-Inc/Daft/tree/main/benchmarking/ai/image_classification)、
[Ray Data batch inference](https://docs.ray.io/en/latest/data/batch_inference.html)。

#### 第三方标准的采用边界

当前未找到一套被第三方维护、专门用于中立排名 Daft 与 Ray Data 的权威套件。正式证据
采用“第三方任务/质量合同 + 双厂商原生代码同机复现”，而不是挑选某家更有利的网页数字：

| 标准 | 可以复用 | 不能声称 |
|---|---|---|
| [MLPerf Inference](https://docs.mlcommons.org/inference/) | ImageNet/ResNet、SQuAD/BERT、OpenImages、Whisper 等数据/模型/质量阈值和 latency/throughput 定义 | 把 Daft/Ray wrapper 的结果称为官方 MLPerf submission；它不覆盖数据库 source/sink E2E |
| [TPCx-AI](https://www.tpc.org/tpcx-ai/) | 数据管理、训练/评分/服务、性能价格和审计思想；可选 scoring 子集 | 只跑推理子集却称 TPCx-AI compliant；未审计的改编只能叫 TPCx-AI-inspired |
| SemBench | 多模态 semantic operator 的 quality/time/cost/memory/failure/scale 协议 | 用 semantic query 结果替代固定-work 执行链路的同模型吞吐对照 |

因此图像主轨使用 ImageNet/ResNet18 vendor-code parity + 本项目 PostgreSQL database-E2E；
文本主轨使用 SQuAD/ShareGPT 等语义合同 + 数据库 AI_COMPLETE 原生算子。第三方标准负责
任务与质量，Daft/Ray 官方代码负责框架原生性，本项目同机重跑负责性能可比性。

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

#### 当前服务器的三次门禁与选型规则

GPU 服务器规格不得仅凭“模型能放下”或一次峰值决定。当前 AutoDL 的只读审计只证明
2×RTX 4090、约 32 个容器可用 CPU、约 240 GiB 内存和 120 GiB `/dev/shm` 可用于双
endpoint 门禁；同时存在 `vm.max_map_count=65530`、数据盘余量有限、无 NVLink，以及
审计时两卡被文本 vLLM 占用等约束。**尚未运行下列三次门禁，因此暂不下最终租机结论。**

1. **能力门禁（256–1K unique rows）**：固定 upstream commit/依赖，验证 source、模型、
   exactly-once、质量与 sink；只判可运行，不报性能结论。
2. **饱和门禁（每 cell ≥60s，1 warmup + 3 interleaved repeats）**：分别扫描 workload
   scale，再冻结规模扫描 native batch/source/preprocess/model worker；禁止两个维度同时上涨。
   用 CPU busy/core-seconds、source/decode、H2D、GPU active/power、memory/spill 和 write
   time 判定木桶，而不是只看 `nvidia-smi` 单点 util。
3. **规模/稳健性门禁（50K–80,358 unique rows）**：真实写出，cold/warm 分轨，并把
   80,358 unique 与 repeated-10× compatibility track 分开；记录结构化失败上界。

门禁后按瓶颈选机：CPU/source 饥饿优先增加 CPU 与本地 NVMe，显存不足再升 48GB GPU，
多 endpoint 实验优先增加独立 GPU 数；只有 tensor-parallel/VLM 才把 NVLink/NVSwitch
作为硬要求。若目标是复现厂商 raw number，应复刻官方 8 个单 GPU worker（每节点
4 vCPU/16GB/24GB GPU）及数据所在云区，而不是用更强单机直接比较。若目标是本项目
2–4 endpoint 方法实验，候选环境为 2–4×24/48GB GPU、64–96 可用 CPU、256–512GB RAM、
1–2TB NVMe、`/dev/shm≥128GB`、`vm.max_map_count≥262144`（建议 1048576）和 PCIe 4.0 x16；
该候选只用于筛选租机，最终规格必须由前三次门禁的阶段数据决定。

### 不同技术栈的产品与学术比较

Daft/Ray 是本项目实现手段，不是 baseline 准入条件。外部系统按算子语义入选：

| 系统族 | 候选 | 比较口径 |
|---|---|---|
| 同栈官方 runtime | Daft Native/Ray | 同机同模型严格排名，判断自定义调度相对官方实现的净收益 |
| 不同栈开源 runtime | Ray Data；可运行时 Spark | 同机同模型严格排名，防止只赢同栈弱实现 |
| 数据库调用外部 endpoint | OceanBase `AI_COMPLETE` / text `AI_EMBED` | 文本轨道同 endpoint 严格比较；当前容器门禁失败则保留工业参考 |
| 工业同类集成 | PolarDB Lakebase / OceanBase Lakebase 的 Daft-on-Ray | 作为开放存储 + Daft/Ray 多模态计算的产品架构证据；无可运行 vendor code 时不进同机排名 |
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

### 数据库厂商 AI 算子与可安装性清单（2026-08-04）

本清单回答三个不同问题：厂商是否提供由 SQL/数据库执行器发起模型调用的一等算子；
该产品线是否能在本地自托管；能否把算子接到项目拥有的同一 OpenAI-compatible vLLM
endpoint。只有三项都通过且完成同机 capability gate 的系统，才可能从产品参考升级为
正式性能 baseline。仅有 `VECTOR` 类型、向量索引、Python SDK 或可自写 HTTP UDF，均不
等于数据库原生 AI 算子。

各论文与数据库产品的**具体 workload 场景、数据模态、输入→算子→输出链路、论文实际
指标及可比边界**统一维护在
[`research/evaluation_metrics_survey_20260731.md` §9](../../research/evaluation_metrics_survey_20260731.md#9-按论文与数据库系统拆分的指标矩阵及本项目对比合同2026-08-04)。
本文件只维护 baseline 身份、可安装性、准入和运行合同，避免在两处复制会随产品版本
变化的场景说明。

兼容性措辞固定如下：**direct** 表示官方明确支持 OpenAI-compatible/vLLM；**likely**
表示 endpoint 可配置但 wire protocol 尚须一行请求验证；**prompt-emulated** 表示没有
专用分类算子，只能用生成算子改写任务。下表“可安装”只表示官方存在公开本地安装路径，
不表示本项目已经安装成功。

#### 可本地安装的数据库/扩展

| 产品与建议固定版本 | 一等 AI SQL 算子（与本项目相关） | 本地安装与同 vLLM | baseline 身份与当前判断 |
|---|---|---|---|
| **Apache Doris 4.1.3**（或 4.0.7 stable） | `AI_GENERATE`、`AI_CLASSIFY`、`AI_EXTRACT`、`AI_FILTER`、`AI_SUMMARIZE`、`AI_TRANSLATE`、`AI_SIMILARITY`、`AI_AGG`、`EMBED` | 官方 Docker/binary；AI Resource 支持 local/OpenAI-compatible，**direct**。`EMBED` 官方覆盖文本及图像/音视频文件引用 | **首选新增正式候选**。同时覆盖文本生成、文本分类、文本/多模态 embedding；固定 provider/model 后做同 vLLM gate |
| **ClickHouse 26.6** | `aiGenerate`、`aiClassify`、`aiExtract`、`aiTranslate`、`aiEmbed` | 官方 binary/DEB/RPM/Docker；支持 OpenAI-compatible/Ollama，**direct** | **高优先级候选**。功能较新且部分仍属 experimental，必须记录 feature flag、named collection、版本和缓存 |
| **StarRocks 4.1.1+** | `ai_query(prompt, config_json)` | 官方 all-in-one Docker；endpoint/config 为 OpenAI 风格，**likely** | **门禁候选**。4.1.1 修复函数注册；需验证 payload、关闭/清空 response cache，并记录 `llm_max_queue_size`、`llm_max_concurrent_queries` |
| **OceanBase CE 4.5.x** | `AI_COMPLETE`、`AI_PROMPT`、文本 `AI_EMBED`、`AI_RERANK` | CE/standalone 可装；自定义 endpoint **likely**。当前普通 AutoDL 容器因 seccomp/systemd/kernel 条件阻塞 | **正式文本候选但当前 blocked**。改用 systemd VM 或 privileged/seccomp-unconfined 容器；无图像 embedding 证据 |
| **Oracle AI Database 26ai Free** | `DBMS_VECTOR[_CHAIN].UTL_TO_GENERATE_TEXT`、`UTL_TO_EMBEDDING(S)`、`UTL_TO_SUMMARY`、`UTL_TO_RERANK` | RPM/Windows/官方容器；官方明确列 OpenAI-compatible 和 vLLM，**direct** | **正式文本生成/embedding 候选**。安装较重；Free 版 2 CPU/2 GB RAM/12 GB 数据限制必须单列，不能外推企业版吞吐 |
| **IBM Db2 12.1.5 Community** | `TEXT_GENERATION`、`TO_EMBEDDING`，模型由 `CREATE EXTERNAL MODEL` 注册 | Community Docker 可装；OPENAI provider 面向 OpenAI-compatible REST，**direct**，但 endpoint/TLS 需 gate | **正式文本候选**。先确认 Community 镜像实际包含 12.1.5 LLM 功能；容器通常需要 privileged |
| **SQL Server 2025 Developer** | `AI_GENERATE_EMBEDDINGS`、`AI_GENERATE_CHUNKS` | 官方 Docker；OpenAI/Ollama embedding endpoint，**direct**，但远端 URL 必须 HTTPS | **文本 embedding-only 候选**。需在 vLLM 前加 TLS reverse proxy；没有本地原生 AI_COMPLETE，不能补生成 baseline |
| **DuckDB 当前稳定版 + `ai` community extension** | `ai_complete`、`ai_classify`、`ai_extract`、`ai_embed`、`ai_similarity`、`ai_rerank`、`ai_agg` 等 | `INSTALL ai FROM community`；OpenAI-compatible/Ollama/llama.cpp，**direct** | **社区扩展 baseline**。安装最容易，但不是 DuckDB core；必须记录 extension version/commit、签名来源和扩展自有并发/重试设置 |
| **PostgreSQL + Timescale pgai 0.11.2** | `ai.openai_chat_complete`、`ai.openai_embed`、`ai.ollama_generate`、`ai.ollama_embed` | Docker/源码可装；`base_url` 可指向本地服务，**direct** | **历史扩展 baseline**。仓库已归档，固定最后版本复现，不作为首要长期对手 |
| **PostgresML 2.10.0** | `pgml.transform`、`pgml.embed`、`pgml.rank`、`pgml.predict` | Docker 可装；模型加载在数据库侧，不直接复用外部 vLLM | **不同机制对照**。改变模型副本、GPU 内存和 scheduler owner，只能单独报告 in-database inference |

本地安装执行顺序为：Doris → ClickHouse → StarRocks 一行门禁 → Oracle/Db2 → SQL Server
embedding；OceanBase 在获得合适 VM/特权容器后恢复。pgai 只补历史机制。每个系统必须先保存版本、镜像
digest、官方 URL/commit 和最小 SQL，再决定是否投入正式 calibration。

**DuckDB `ai` 已落地（2026-08-05）**：driver venv 装 `duckdb==1.5.4`（1.5.5 的 ai 扩展二进制尚未构建，
`INSTALL ai FROM community` 会 404），扩展默认 provider 是 ollama，指向 vLLM 必须显式
`SET duckdb_ai_provider='openai_compatible'` + `CREATE SECRET (TYPE duckdb_ai, AI_PROVIDER 'openai_compatible',
BASE_URL 'http://host/v1', API_KEY 'EMPTY')`，再 `SET duckdb_ai_model`。已接入 baseline 框架为
`duckdb_ai` adapter（`code/src/baselines/text/products/duckdb_ai.py`，set-oriented
`ai_try_complete` 并同时保留 `{response,error}`；任一 error 或 NULL response 均按失败处理，
不再把截断行伪装为 completed）。扩展原生拥有行内 HTTP 并发；**产品原生主轨只配置一个
`BASE_URL`，不由实验 harness 做跨 endpoint 分片**。provenance 标
`database_product_native_baseline` / `duckdb_community_ai_extension`，不得写成 DuckDB core
或官方 DuckDB benchmark。observability 为 `query_barrier`/`unavailable`，与 OceanBase
同形。计时与观测脚本
`code/scripts/baselines/time_duckdb_ai_baseline.py` 复用 `PeriodicSampler` + vLLM counter delta + nvidia-smi，
其中 `successful_rows_per_s` 只统计无 error 的非 NULL 输出。历史 harness-sharded diagnostic 的并行 wall
不得乘 endpoint 数，但该数字也不得进入产品原生排名。same-work 主轨固定 response cache=false、retry=0、rate limit=0，
同时保持 vLLM 服务端 prefix cache=on；产品优化的 response-cache-on 只能另列轨道。
服务器 capability probe 曾由实验 harness 预切 4 行并对两个单 endpoint shard 各跑一次；这只证明
DuckDB 1.5.4 / `ai` 0.4.14 的**两个独立单 endpoint 查询**可完成 1024-cap 请求，不能证明扩展自身拥有
多 endpoint 路由，现降级标为 `harness_pre_split_diagnostic`。官方扩展页只公开一个
`duckdb_ai_base_url` / secret `BASE_URL`；上游 README 建议多后端时接用户自己的 gateway。因此双 GPU
产品部署若要测试，必须另列“DuckDB-ai + 第三方 gateway”完整系统轨，记录 gateway scheduler owner，
不能注入项目 router，也不能冒充 DuckDB 原生能力。当前同一 ShareGPT workload 的 64 行在 256 和 1024
cap 均出现 length error。

服务器安装的 v0.4.14 capability probe 进一步观察到：`secret => CASE ...` 与 `base_url => CASE ...` 均报
“must be a constant expression”，即单条查询不能按行选择本地 endpoint。两条固定 `base_url` 的
`ai_complete` 配 `WHERE`/`UNION` 属于 SQL 作者人工静态切分，不是扩展调度。该 probe 当前只有运行侧汇报，
原始 SQL、版本输出和错误文本归档前只能作为**实测待归档**证据；正式依据仍以公开单 `BASE_URL` 接口为准。

还要避免另一种常见误读：当前 community `ai` extension 不是把 llama.cpp/CUDA runtime 嵌进 DuckDB 后
默认在 `cuda:0` 加载 GGUF。上游 provider 表写的是 `llamacpp` → 外部 `llama-server`、
`openai_compatible` → 外部 vLLM/LM Studio/LiteLLM/gateway；本项目使用后者访问 vLLM。因此 llama.cpp 的
tensor parallel、`CUDA_VISIBLE_DEVICES` 或多个 DuckDB 进程绑定 GPU 都属于**外部服务/业务部署层**，不是
DuckDB-ai 原生 GPU 调度能力。多进程把行分给不同 `BASE_URL` 仍须标作 harness/application sharding。
因此 DuckDB 当前只进入独立 bounded-output 产品轨，不进入默认 ShareGPT fixed-cap 主排名；
不得把少量 capability 成功外推为正式吞吐。
LOTUS 是语义算子 SDK，政策上不进 chat-track 吞吐榜，
需独立质量-成本-时间轨，本轮不提交。

官方依据：[Doris AI Functions](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/ai-functions/overview/)、
[Doris EMBED](https://doris.apache.org/docs/dev/key-features/embedding/)、
[Doris Docker quick start](https://doris.apache.org/docs/dev/getting-started/quick-start/)、
[ClickHouse 26.6 AI embedding](https://clickhouse.com/blog/clickhouse-release-26-06#aiembed)、
[ClickHouse 安装平台](https://clickhouse.com/support/platforms)、
[StarRocks 4.1 release notes](https://docs.starrocks.io/releasenotes/release-4.1/)、
[OceanBase AI Functions](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975)、
[Oracle chainable AI functions](https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/chainable-utility-functions-and-common-use-cases.html)、
[Oracle vLLM generation endpoint](https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/utl_to_generate_text-dbms_vector_chain.html)、
[Db2 LLM integration](https://www.ibm.com/docs/en/db2/12.1.x?topic=sql-llm-integration-db2)、
[SQL Server AI_GENERATE_EMBEDDINGS](https://learn.microsoft.com/en-us/sql/t-sql/functions/ai-generate-embeddings-transact-sql?view=sql-server-ver17)、
[DuckDB `ai` community extension](https://duckdb.org/community_extensions/extensions/ai)、
[pgai official repository](https://github.com/timescale/pgai)、
[PostgresML](https://postgresml.org/docs/)。

#### 可用云账号测试、但不进入本地同机排名的厂商产品

| 产品 | 主要 SQL AI 算子 | 可安装性/endpoint 限制 | 正确比较口径 |
|---|---|---|---|
| **PolarDB PostgreSQL Polar_AI** | `AI_CallModel`、`AI_Text_Embedding`、`AI_Text_Classification`、`AI_Text_Generation` | 云 AI node/商业产品；模型 URL 可配置但对 vLLM 仅 **likely** | 有账号时做云产品 E2E/成本/质量。用户给出的 PolarDB-X RPM **不是该产品线** |
| **PolarDB Lakebase Daft on Ray** | Daft `prompt`、`embed_text`、`classify_text`、`embed_image`、`classify_image` | 完整集成为阿里云托管；Daft 本身可本地 | 工业同栈参考；本地 Daft 已由现有 baseline 覆盖，不能重复包装成 PolarDB 本地 baseline |
| **Hologres** | `ai_gen`、`AI_EMBED`（含图像 CLIP）、`ai_classify`、`ai_rank`、`ai_extract`、`ai_similarity` 等 | 托管模型/AI node，无公开自托管 | 当前图像 CLIP 最贴近的国产云工业参考；比 E2E、质量、成本和配额 |
| **AnalyticDB MySQL / PostgreSQL** | MySQL 有 `ai_generate/classify/embed/...`；PG 有 `AI_GENERATE_TEXT`、`pgml.embed/transform` | 云/商业集群，模型或 PAI-EAS 契约受控 | 云产品/机制参考，不与本机 MFU 排名 |
| **TDSQL Boundless / TCHouse-X / TencentDB for PostgreSQL** | `LLM_INVOKE` 或 `AI_GENERATE/CLASSIFY/...`，以及 `call_model/get_embedding/run_rerank` | 腾讯云商业节点；部分 endpoint 可配置但协议 **likely**，其余绑定托管模型 | 有账号后做 capability、质量、成本；不得声称已兼容本地 vLLM |
| **TiDB Cloud Starter** | `EMBED_TEXT`、`VEC_EMBED_COSINE_DISTANCE`、`VEC_EMBED_L2_DISTANCE` | SQL Auto Embedding 仅 Cloud Starter on AWS | text-embedding 云参考；Self-Managed 仅 vector search，另见排除表 |
| **Snowflake Cortex AI** | `AI_COMPLETE`、`AI_EMBED`、`AI_CLASSIFY`、`AI_FILTER`、`AI_AGG`、`AI_EXTRACT`、`AI_TRANSCRIBE` 等 | 托管服务，原生函数不能注册任意本地 vLLM | 单列 query E2E、$/1K rows、质量、错误/配额；External Function 必须另标 generic UDF |
| **BigQuery** | `AI.GENERATE*`、`AI.EMBED*`、`AI.IF`、`AI.SCORE`、`AI.CLASSIFY`、`AI.AGG` | 托管服务；原生函数绑定 BigQuery/Vertex endpoint | 单列云面板；物化精确输入，记录实际模型调用行数、Vertex 配额和两侧费用 |
| **Databricks** | `ai_query`、`ai_gen`、`ai_classify`、`ai_extract`、`ai_summarize`、`ai_similarity` 等 | Serverless 云端；custom external model 支持 OpenAI-compatible 公网 HTTPS endpoint | 可做“同 endpoint、云端 scheduler”面板，但 WAN/AI Gateway/serverless 开销使其不能与本机 raw time 混排 |
| **SingleStore Helios** | `AI_COMPLETE`、`AI_CLASSIFY`、`AI_EXTRACT`、`EMBED_TEXT` 等 | AI Functions 属于 Helios/Aura managed Python UDF containers；本地 Dev Image 未证明含同功能 | 云 capability/成本参考 |
| **MySQL HeatWave** | `ML_GENERATE(_TABLE)`、`ML_EMBED_ROW/TABLE`、`ML_RAG(_TABLE)`、`HEATWAVE_CHAT` | HeatWave 托管云，普通 MySQL 安装不含这些算子 | 云 capability/多模态参考；不能称为 MySQL Community baseline |
| **Amazon Aurora PostgreSQL / Redshift** | Aurora `aws_bedrock.invoke_model*`；Redshift `CREATE EXTERNAL MODEL ... BEDROCK` 生成 SQL inference function | AWS 托管，绑定 Bedrock/SageMaker；自写 Lambda 代理属于 generic UDF | 单列 AWS 云面板，记录 region、quota、throttle/retry、provisioned throughput 和费用 |
| **Azure Database for PostgreSQL** | `azure_ai.generate/is_true/extract/rank`、`azure_openai.create_embeddings` | 云预览扩展，绑定 Azure AI/Foundry endpoint | capability/成本参考 |
| **MotherDuck** | `prompt()`、`embedding()` | MotherDuck 托管、无 on-prem、不能注册本地 endpoint | 轻量云数仓参考；不得称为 DuckDB native |
| **SAP HANA Cloud / Teradata Vantage** | `VECTOR_EMBEDDING`；`AI_AskLLM`、`AI_TextEmbeddings`、`AI_TextClassifier` 等 | 云/企业许可，无公开低成本同机安装路径 | 企业 capability/成本/质量参考 |

官方依据：[PolarDB Polar_AI SQL](https://help.aliyun.com/zh/polardb/polardb-for-postgresql/polar-ai-sql-reference/)、
[Hologres AI Functions](https://help.aliyun.com/zh/hologres/user-guide/ai-function-list)、
[AnalyticDB MySQL AI Functions](https://help.aliyun.com/zh/analyticdb/analyticdb-for-mysql/ai-function)、
[Snowflake Cortex AISQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)、
[BigQuery Generative AI overview](https://docs.cloud.google.com/bigquery/docs/generative-ai-overview)、
[Databricks AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions)、
[SingleStore AI Functions](https://docs.singlestore.com/cloud/ai/ai-ml-functions/ai-functions/)、
[HeatWave GenAI routines](https://dev.mysql.com/doc/heatwave/en/mys-hwgenai-routines.html)、
[Aurora PostgreSQL ML](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/postgresql-ml.html)、
[Azure PostgreSQL AI Functions](https://learn.microsoft.com/en-us/azure/postgresql/azure-ai/generative-ai-azure-ai-functions)、
[MotherDuck prompt](https://motherduck.com/blog/sql-llm-prompt-function-gpt-models/)。

#### 不纳入生成式数据库 AI 算子 baseline 的安装包

| 产品/安装包 | 核查结果 | 排除原因 |
|---|---|---|
| **PolarDB-X 标准版 RPM** | 可以在 CentOS 类 VM 安装；未在官方文档/仓库验证 `AI_COMPLETE`、`AI_EMBED` 或模型 endpoint | 与 PolarDB PostgreSQL Polar_AI/Lakebase 是不同产品，不能因同品牌误列 baseline |
| **TiDB Self-Managed、MariaDB 11.7+、普通 MySQL** | 提供向量类型/索引/距离函数；embedding 由客户端或外部模型生成 | vector-only，不是 DB-owned model-call scheduler |
| **MatrixOne** | 官方 RAG 示例由 Python/Ollama 生成 embedding/答案；数据库负责向量存取 | Python UDF/自写 HTTP glue 不等于 vendor-native AI operator |
| **openGauss/GaussDB DB4AI** | `CREATE MODEL` / `PREDICT BY` 覆盖回归、分类、聚类等经典 ML | 可本地但任务和部署机制不是 AI_COMPLETE/LLM/多模态 embedding |
| **ByteHouse、KingbaseES、Dameng** | 截至核查日未在官方公开资料验证一等模型调用 SQL 函数和 endpoint 契约 | 证据不足，保留观察，不写成绝对不存在 |
| **自写 External Function/Lambda/Python UDF** | 技术上可把任意数据库连到 vLLM | scheduler owner 是项目 glue，不是厂商 AI 算子，只能作 generic UDF control |

排除依据：[PolarDB-X RPM 部署](https://doc.polardbx.com/deployment/topics/deploy-by-rpm-std.html)、
[TiDB Self-Managed Vector Search](https://docs.pingcap.com/tidb/stable/vector-search-integration-overview/)、
[MariaDB Vector](https://mariadb.com/docs/server/reference/sql-structure/vectors/vector-overview)、
[MatrixOne RAG example](https://docs.matrixorigin.cn/v25.3.0.0/MatrixOne/Tutorial/rag-demo/)、
[openGauss DB4AI](https://docs.opengauss.org/en/docs/7.0.0-RC3/characteristic_description/aifeature_guide/native_db4ai_engine.html)。

#### 厂商 baseline 的统一准入与报告合同

1. **先过四道 capability gate**：安装并记录精确版本；一行调用返回正确 schema；N 行
   exactly-once 且调用数可审计；冻结并验证响应/prefix/result cache 状态后再测并发。
   vLLM 正式性能主轨使用 cache-on；cache-off 仅作独立机制消融。任何系统之间只有
   cache 配置、预热规则和输入前缀分布一致时才能排名。
2. **同机主排名只收可复现系统**：相同输入、模型权重、endpoint、输出上限、source/sink、
   CPU/GPU 上限和 warm/cold 生命周期；各系统独立 calibration，不能把同一 Ray 参数强塞
   给不同数据库。
3. **外部厂商 raw number 与本地分榜**：云产品只比较其可观察的 query E2E、质量、
   $/1K rows、token/image work、失败/429、quota 和 region；不推断内部 GPU/MFU。
4. **记录谁拥有调度**：每个 run 必须写 `scheduler_owner`、原生 AI Function 还是扩展/
   generic UDF、数据库内部队列/批大小/重试/缓存参数，以及项目 adapter diff。
5. **不同模型只做产品比较**：只有相同模型和 endpoint 才能讨论调度效率；厂商托管不同
   模型只能比较产品 E2E、质量和成本 Pareto，不能据此声称执行链路更快。

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

`code/scripts/experiments/run_image_clip_e2e.py` schema v12 已覆盖 operator E2E、first output、
images/s、batch/stage P50/P95/P99、输入/输出 bytes、H2D/D2H、CPU-core-seconds、
GPU util/power/energy/显存、exactly-once、digest/norm、资源版本与 CPU 预算，并增加
first-output/E2E 流式比例、60s duration gate、J/1K images、GPU-seconds/image、
images/CPU-core-second 和 host I/O bytes/image。历史 schema-v11 可旁置补算这些派生量，
但不能事后构造 engine-internal batch、逐图 latency 或 object-store/spill。它仍然
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
6. **扩展/成本派生量**：schema v12 已补单位图片资源量；仍需生成 scaling efficiency、
   $/1K rows（仅在价格可比时）、failure rate 和 oracle regret summary。

纯代数派生指标不触发历史性能实验重跑。可用
`code/scripts/analysis/augment_image_observability.py` 从 schema-v11 raw CSV 生成旁置增强
CSV；原始 CSV 保持不变。该工具缺字段时 fail closed，也不把 12K 与 60K 的 absolute
JCT/first-output 伪装成 matched-workload 比较。

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
公开 workload 的精确运行合同见
[`state_aware_work_unit_evaluation_20260808.md` §7.6](state_aware_work_unit_evaluation_20260808.md)：
VTC 两个 synthetic suite 只进入 direct/project/VTC-style 上游调度轨；BurstGPT v2.0
另补 Daft Native、Daft Ray、Ray Data 的 `eager_trace_shape` 原生观察，禁止跨计时边界做
absolute JCT 排名。

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

项目写回 baseline 为 PostgreSQL + pgvector 的 COPY + 延迟建索引（先 COPY 到 unlogged table，再 `CREATE INDEX HNSW`），已在动机 GPU 实验中建立（pgvector writeback 0.897s vs JSON 1.567s，见 `motivation/results/gpu/`）。按根 `AGENTS.md`“项目范围”和 `PROJECT_OUTLINE.md`，写回作为工程 baseline 处理，不作为独立实验阶段；本节 W1 仅作 baseline 出处登记，不再设为待跑门禁实验。

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
| **X6** | Uncertainty-aware Admission Cost | SFS、TIE（ICML 2026）、Past-Future（ASPLOS 2025）、JITServe（NSDI 2026）、Beyond Prediction（ICML 2026） | 正式论文 + 预印本 | serving simulation、输出长度分布、remaining-work 修正、SLO goodput 与 prediction-free tail 对照 | 多数机制修改 serving scheduler；本项目只迁移 admission-time 估计、区间、指标和回退合同，不修改 vLLM |

### 代价估计的可实施 baseline 与晋级合同

代价模型不是以“拟合秒数最好看”为目标，而是要改善数据组织、active-work、endpoint 和 submit/hold 决策。按实现复杂度依次比较：

1. 不使用估计器的固定静态 active-work/credit；
2. 输入 token/frame + output cap 的解析公式；
3. profile bucket/lookup；
4. 解析模型 + residual correction；
5. 输出 work 分位数/预测区间 + endpoint state；
6. oracle actual output length（只作不可部署上界）；
7. 只有拿到细粒度 running prefill/decode snapshot 时，才加入 SFS 式 what-if simulation。

每个估计器必须同时报告三层结果：

- **预测层**：MAE/RMSE、median/p95/p99/max Q-error、prediction interval coverage/width、tail underestimation；
- **排序层**：Spearman、pairwise accuracy、Top-K、pick rate；
- **决策层**：selected JCT/throughput/SLO goodput、oracle regret、SLO 违反、性能回退率、回退到静态策略的比例与估计器开销。

训练/验证至少采用配置组留出、独立时间段和 workload 留出；自然 EOS 场景另做长度分布漂移与 burst 留出。动态估计器只有显著优于**同最大 work/credit 上限的强静态策略**，且 P95/P99、公平性和失败率无明显回退，才进入正文主结果。Beyond Prediction 提供的反例要求保留 prediction-free 静态 arm，不能把 oracle output length 当作调度最优的充分条件。

### 条件性计划级验证：TPC-H-derived AI operator plans

TPC-H 原始 benchmark 不包含 AI 算子，因此当前 320-run 不使用 TPC-H，也不把 TPC-H raw
query time 当 AI cost baseline。项目保留一条 `planned-conditional` 计划级 held-out：只有
formal-only 局部代价模型通过 candidate ranking/regret 晋级门槛后，才在 TPC-H comment 列上
构造 bounded AI_COMPLETE，并比较 filter/join/materialize 位置与冻结运行配置的等价候选计划。

该轨只允许称 `TPC-H-derived`/`TPC-H-inspired`，不得声称 TPC-H compliant；TPCx-AI 仍只复用
scoring、质量、性能价格和审计合同。计划级 baseline 包括关系优化器原生 cost + 固定每行 AI
常数、token/output-cap 解析模型、profile lookup、Ridge/LightGBM、解析 + residual，以及
actual-runtime oracle 上界。主评价从点误差扩展到 whole-query Q-error、plan ranking/pick rate、
selected/oracle JCT 与 plan regret。完整启动条件、语义等价门禁和停止条件见
`completed/operator_cost_profile_dual4090_formal_20260804.md` §8。

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
- [ ] 单租户多 Job 是否包含 VTC-style/shared-credit，并同时报告聚合吞吐、每 Job JCT/P99、
      weighted service/empirical lag、最长无服务和 idle borrowing？Jain 只作描述量。
- [ ] 代价估计是否用 ranking/regret 验证了决策价值，而不只报告 MAPE？
