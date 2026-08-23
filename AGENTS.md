# AGENTS.md

本文件是项目级长期规则（方向、边界、工作方式）。详细内容见各目录的 `README.md`、`PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md`。

## 1. 项目目标

研究方向：数据库 AI 负载的执行优化与调度。不回到传统数据库内核或 GPU 查询算子。

**课题定位**：优化数据库 AI 算子外部执行链路的上游调度——数据如何组织为请求、以什么节奏发送、如何根据模型服务状态调节并发。vLLM 是文本 AI_COMPLETE 的部署平台；图像 AI_EMBED 主方法使用 typed Ray GPU actor，并以 vLLM pooling、Daft 内置 AI Function、官方多模态 benchmark 代码和 Ray Data native API graph 作分层 baseline；均不修改模型内部。Ray 作为架构设计空间，利用其 actor 模型和异步能力实现调度方案。Daft 作为数据引擎（Rust 核心 + Arrow 零拷贝 + `@daft.cls` GPU UDF），从文本阶段直接接入，多模态阶段复用同一套 pipeline 代码。项目自写 Daft UDF 只作 diagnostic reference；策略增量必须再对比冻结最佳项目静态点。

**方向已收敛，策略候选池开放**：经过 2026-07-16 的讨论与文献收集，优化方向已明确收敛到上游调度（数据组织 + 提交控制），但具体策略不提前锁定——动态 batching（token-budget/length-align/prefix-aware）、K_max 自适应、queue-adaptive flush、actor pool 分池路由等均为候选方案，最终采用哪些由后续实验数据决定。新增候选策略应记入 `research/knowledge_hub.md` §5 供以后参考。

**两项策略设计 + 多模态泛化验证 + 算子代价估计（共同使能组件）**：

1. **研究内容一：数据组织策略**。探索按计算量（token 量/frame 量）而非固定行数的动态组织方式，以及按计算量相似度分组对推理效率的影响。利用异构 actor pool 实现。引擎级参数（Daft `into_batches`、`batch_size`、`repartition`）与策略级决策（token-budget、length-align、prefix-aware）共同构成数据组织优化空间。
2. **研究内容二：调度与提交控制策略**。利用 Ray actor 的 stateful + async 能力，研究固定资源下的最小饱和 active work、request-level replenishment、endpoint-shared request/work credit、work-conserving idle borrowing 和多 job fair queue。固定静态 credit 是强 baseline；动态候选只有显著优于同上限静态策略才晋级。
   当前正式范围是**单租户内多个 Job/workload class**，按 Job 评价份额、SLO 与隔离；不把当前
   `job_id` 记账外推为多租户公平。多租户只作后续层次化扩展：外层 tenant entitlement/debt 与
   per-tenant buffer cap，内层复用现有 Job-level observation、priority/SLO、debt 和 borrowing/reclaim，
   不阻塞当前 formal。
3. **多模态泛化验证**（正文实验，验证策略抽象不依赖数据模态）。在图像 workload（AI_EMBED/AI_CLASSIFY，CLIP/Qwen2.5-VL）上使用同一套策略代码和配置逻辑，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。
4. **算子代价估计**（共同使能组件，不作为独立研究内容）。首版采用简单解析模型 + profile 校准 + residual correction，预测 prompt/output work、operator service time、JCT、remaining work 和 SLO slack，服务于 active-work/K 初始化、数据组织、endpoint 路由和提交策略；评价误差、配置 ranking、决策 regret 和预测区间。

写回使用 PostgreSQL + pgvector，COPY + deferred index 为工程 baseline。不作为独立研究内容，仅在实验设置中说明。

**验证方式**：研究内容一和二的每种策略通过消融实验对比（静态 baseline vs 动态策略）。两项策略分别独立搜索最优配置后拼接，再与联合 grid search 对比——联合显著优于拼接则说明需要联合调优，两者接近则分层独立优化即可。无论哪种结果，都不改变课题的核心贡献（上游优化策略设计）。多模态实验使用同一套策略代码，仅替换数据列类型（`df["prompt"]` → `df["image"]`）。

**主场景**：AI_COMPLETE（生成式 LLM，文本）+ AI_EMBED/AI_CLASSIFY（图像，多模态泛化验证）。AI_EMBED 文本预研已完成。文本服务使用 vLLM；图像主方法使用 tensor-input Ray GPU actor，vLLM pooling 是统一部署/服务 baseline；Daft 为统一数据引擎。

详细描述见 `PROJECT_OUTLINE.md` 和 `research/knowledge_hub.md`。

## 2. 当前边界

```text
PostgreSQL 18.3
  → Daft DataFrame（数据引擎，文本 df["prompt"] / 图像 df["image"]）
  → Ray 动态 Batching（token-budget / length-align / prefix-aware）
    + Ray actor 架构（异构 actor pool / queue-adaptive flush / 去中心化）
  → AI_COMPLETE（文本 LLM，主场景）/ AI_EMBED/AI_CLASSIFY（图像，多模态泛化验证）
  → 文本：vLLM generation / 图像：typed CLIP backend（Ray GPU actor ours；vLLM pooling baseline）
  → PostgreSQL + pgvector（写回）
```

不要把主线写成：改造 vLLM 或 continuous batching、改造 Ray 调度器、Daft/Ray 单纯集成、Arrow serialization 优化、传统 GPU 查询算子、模型 kernel 优化（GQA/MQA/Flash-Attention）、Python toy benchmark。

## 3. 当前证据与下一步

已有实验：GPU-backed 文本 AI_EMBED 预研链路（2026-07-12，文本向量，非图像 CLIP；fine vs coalesced：推理执行阶段约 37.5×、端到端约 13.4×；pgvector writeback 0.897s vs JSON 1.567s）+ vLLM + Qwen2.5-1.5B AI_COMPLETE baseline（已建立，详见 `experiments/results/local_vllm_qwen15b_baseline/`）。图像 CLIP 的动机是另一套（GPU 利用率仅 1–4%、瓶颈在 CPU 预处理，见 `motivation/results/gpu/image_*`）。详细数据见 `motivation/results/gpu/`。CPU/fake 实验仅历史参考。

**2026-08-08 开题证据冻结顺序**：开题材料冻结前，先统一研究 framing 与
`opening/claim_matrix.md`，再补 SQuAD 均匀控制组和 ShareGPT 受控异质组两套
三臂 database-E2E 正式实验。两组均比较 direct static-sharded、DuckDB AI
static-sharded 和 project frozen-static，统一 PostgreSQL source/sink、外部
database-E2E、质量和资源指标。两组通过后停止扩展数据库产品、workload 和参数搜索，不换 workload、模型或
数据库追正结果；随后只补两类不可替代的动机证据：① 同一 ShareGPT Chat manifest
上的 bounded control、Daft `prompt()` Native/Ray 和 Ray Data HTTP Processor 原生单 job
1+3 对照；② Daft/Ray Data short/long 两 job 错峰原生观察，以及项目
static-partition vs shared-work-credit 同上限 A/B。DuckDB 只留在 SQuAD/cap=64
有界输出产品轨，不与语义不兼容的 ShareGPT 框架轨交叉排名。已有 1/2/4-job、
图像 matched-resource、组织 regime 和 429-run cost decision-quality 证据直接复用；
phase-change、3:1 weighted、第二硬件与大规模参数搜索不阻塞开题。四条证据链
（Work Unit、状态感知、动态调度、共同使能代价估计）权重相同，都必须由实验现象导出设计。
开题不要求 proposed 全面胜出。当前暂停 PPT 成品、云文档和 Wiki
同步，只冻结内容大纲、实验报告与数据图。本顺序覆盖下述 2026-08-01 工程优先级，但不撤销
已有 image-first 证据和论文阶段计划。

**2026-08-01 工程优先级（开题冻结后恢复）**：内部已锁定 A（模型服务状态感知的请求成形/提交）+
B（算子代价估计），首个 workload 为 image AI_EMBED (CLIP)；文本遗留实验统一
`parked-conditional`。外部“DB↔GPU 经 Daft 桥接”scope 是否进入正式题目仍待
导师/学长确认。CLIP 5K 规范画像已通过门禁：实用 batch（≥16）CPU 准备/GPU
embed 比为 13.8–18.3，当前进入 path-B runner + image 强 baseline 建设；该画像
仅是 motivation，不是策略胜出证据。权威顺序见
`experiments/plans/experiment_status_and_gaps.md` §0。

文本轨道已完成项（vLLM baseline、Daft 文本接入、token-budget / K_max、flush
跨负载与 2048 留出、本地单 GPU 联合消融、受控 prefix cache-off 实验、
算子代价估计初版、request-level credit-release 双卡重复、active-work 八档
扩展曲线、固定资源 Actor Pool、complete-row service quantum 和 SLO-aware
EWMA flush 对照均已完成）：
① 已按预注册规则选择每 endpoint 65,536 active work；多 actor 与固定 quantum
均未达到 5% 晋升门槛，保留 `request + 1×256`，其价值是精确 completion/
credit 语义而非显著稳态提速；SLO-EWMA 相对 fixed-50 未过 5% 门槛 →
② 当前 2×4090 上完成 shared request/work credit 与 1/2/4 job 等量核心矩阵，以及
5s short/long guaranteed-overlap 开题最小证据：shared 相对 static 总吞吐 +21.03%、
long JCT −18.31%，但 short JCT +4.98%、Jain fairness 下降；冻结为效率—隔离—公平
权衡，不称动态全面胜出。weighted、完整 held-out/异构 offset 与故障迁移均是论文阶段
遗留项；③ prefix cache 开启后的
数据组织机制验证（07-31 系统重测 `rc1_data_organization/`，**取代 07-25/26
gropy；07-18/19 保留作历史动机参照**）：**regime-dependent**——2-ep/0.9（每
endpoint KV 池占 GPU 显存 0.9、无压力 max 7–10%）5 策略 50–56k 近似中性；
4-ep/0.43（2 endpoint/GPU 各占 0.43、KV 饱和 max 98–100%）分化 39–50k、排名
反转为 sequential>fixed>>row_cap≈best_fit>length_align。机制
`prefix_group_ratio`：重排序类 organizer 打散 prefix 组 → 4-ep 命中从 0.60–0.76
塌到 0.06–0.07。prefix-affinity routing 2-ep/7B 中性 −0.1%、4-ep/1.5B +5.9%
跨门禁；matched-KV 更支持 endpoint consolidation 是驱动，4-ep 饱和深度仍未完全
隔离→④ 多模态泛化验证（图像，同一套策略代码）——**2026-08-13 静态/观测证据已闭合，动态待接**：
5K CLIP 画像确认 binding 是 CPU prepare 与 driver/Ray submission 的组合，不是 H2D 或 PG bulk
read 单点；Daft built-in、Ray Data native 与 project frozen-static 的 operator-E2E/provenance 证据
已完成，原生图像 four-job 40/40、Project staged descriptor + observe-only 24/24 也已归档。当前只
证明静态阶段拆分、原生多 Job 干扰和低成本状态观测，不证明 state-aware 胜出；下一步先做 HSE
static GPU 非劣门，再接两级 stage controller、CE5 在线驱动与小规模 pgvector 质量闭环 → ⑤ 代价
模型增加独立时间段或新 workload 校准。当前证据
支持 sequential token-budget + static K8 + fixed 50ms；联合候选未显著优于
独立拼接，two-level adaptive 和 SLO-EWMA 均未显著优于 fixed-50，
prefix-only 在 cache-off 下无稳定收益；cache-ON 下 batching **regime-dependent**（2-ep 无压力近似中性、4-ep KV 饱和分化 27% 且排名反转），routing
2-ep/7B 中性（−0.1%）、4-ep/1.5B +5.9% 跨门禁但有条件待隔离消融。**RC1 数据组织 + #28 routing + KV-sweep 三向闭环：上游策略价值只在 4-ep KV 饱和 regime 显现，2-ep 是干净对照基线。** 写回使用 PostgreSQL + pgvector
（COPY + deferred index），不作为独立实验阶段。详见 `PROJECT_OUTLINE.md`
§近期优先级。

**Scope 缩减触发条件（2026-07-17 约定）**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion（✅ baseline 已建立，未触发）
- 文本 RC1+RC2 达到可转轨门槛后才启动 Daft 多模态 pipeline（✅ 2026-08-01 已满足，image build 已解除暂停）
- VLM 生成实验（AI_COMPLETE 多模态版）始终标记为 optional

## 4. 目录规则

| 目录 | 用途 |
|---|---|
| `overview/` | 项目总纲（`current_direction_and_plan.md` 为 TL;DR 快速参考卡片，以 `PROJECT_OUTLINE.md` 为权威总纲） |
| `research/` | 背景调研、文献依据（第一入口：`knowledge_hub.md`） |
| `motivation/` | 动机场景、端到端测试（脚本→`benchmarks/`，计划→`plans/`，结果→`results/`） |
| `feasibility/` | 组件、环境、脚本可用性验证（不承担实验大纲职责） |
| `experiments/` | 正式研究实验（方法有效性验证） |
| `code/` | 可复用工程代码 |
| `code_doc/` | 自动生成的代码文档（辅助参考，不承担规则职责） |
| `data/` | 本地 workload 数据（raw 负载被 git ignore） |
| `deploy/` | 跨机器 runtime/profile/资产合同 + Docker/AutoDL 平台部署配置 |
| `figures/` | 图资产（架构图、实验图、绘图脚本、审计） |
| `opening/` | 开题报告、PPT、飞书、文献 |
| `projects/` | PPT 自动生成工程（旧版，已作废；仅保留工具链经验） |
| `learning/` | 学习讲解材料 |
| `notes/` | 沟通记录、待确认问题 |

进入子目录前先读该目录的 `AGENTS.md`（规则），再读 `README.md`（内容）。

### 4.1 多机器运行、缺依赖或缺资产的强制入口

只要任务涉及下列任一情况：在多台机器间轮流实验、新机器/新容器、切换 GPU 或云环境、环境初始化、缺少
Python 包、模型或数据集、下载新 workload、准备本地单 GPU 或远端多 GPU 实验，agent
必须先读：

1. `deploy/runtime/AGENTS.md`；
2. `deploy/runtime/README.md`；
3. 所选平台专项 runbook（如 `deploy/autodl/README.md`）。

随后必须先运行 `manage_environment.py check` 的只读 preflight（默认自动选择机器 profile）并保存机器报告，再决定
是否显式安装或下载。禁止 clone 后直接全量 `pip install`、混装 driver/vLLM 环境、绕过
许可下载数据、沿用另一台机器的最优 K/batch/actor 参数，或在 correctness gate 前启动
正式实验。模型/数据下载完成不等于数据库 workload 已导入；必须继续运行对应 importer
和行数/schema/exactly-once 门禁。

环境自动识别不等于性能参数可以跨机器复用。batch/K/actor/active-work 必须绑定
“机器 + 模型/服务配置 + 协议 + workload 分布/稳态规模”的校准签名；签名不变可复用冻结合同，
签名变化必须重新 gate/校准。选择最小饱和点而非单次峰值，正式 run 禁止在线调参。
校准先固定 batch 做 workload scale ramp，确认至少 60 秒且速率进入平台，再固定规模扫描
batch/K/actor；禁止把两个维度同时上涨后归因。

## 5. 实验规则

- 正式结果放对应目录：动机 → `motivation/results/`，可行性 → `feasibility/results/`，方法 → `experiments/results/`
- GPU-backed E2E 优先于 CPU/fake；CPU/fake 仅供调试或历史对照
- 每条 CSV 记录 `server_version` 和 `pgvector_version`
- 新实验必须有明确问题、运行命令、CSV 输出、结果解释
- 区分数据生成、序列化、`ray.put`、fan-in、写回等阶段边界
- warm-up 忽略或标注；Python baseline 与 Ray baseline 共享数据读取和写回路径
- **正式 baseline 必须由被测系统拥有执行与调度**：优先直接运行官方 benchmark、
  内置 AI Function 或官方推荐 API graph；项目只允许做数据源、统一 sink、质量审计和
  指标采集适配。自写 actor pool、credit、inflight/backpressure 或重写框架执行器的
  路径只能标为 diagnostic reference，不能进入 baseline 主排名。每个 run 必须记录
  upstream URL/commit、实现来源、scheduler owner 和适配 diff。

## 6. 严谨性规则

遵循 `karpathy-guidelines`：不确定就问、先定义可验证目标、做最小实验、每个结论标注来源类型、方向选择先做 fatal-flaws audit。

禁止：
- 凭感觉定题；只用 microbenchmark 支撑完整结论；把 Ray 说成"很慢"无上下文
- 把 Daft/Ray/Lance 产品化适配写成既定事实；因写过 benchmark 就反向寻找论文问题
- **在正式材料（开题报告、论文、PPT、图表）中使用 `RC1/RC2/RC3`、`BL1/BL2`、`Phase 0/1/2/3`、`P0/P1/P2` 等内部代号**（内部工作文档可用缩写）

### 6.1 对外文档语言规则

- 开题报告、论文、PPT、图表、答辩讲稿和对外同步稿不得直接照搬项目管理用语。尤其避免把“冻结、门禁、闭环、边界、约束、合同、产品轨、框架轨、正式点、晋级、失效”等词当作无需解释的普通名词。
- 需要表达这些含义时，写出实际动作或条件。例如，将“冻结配置”写成“实验开始前选定配置，运行期间保持不变”；将“通过门禁”写成“正确性、资源使用和重复实验均满足预先规定的条件后，才纳入性能比较”；将“闭环”写成实际包含的读取、执行、写回和质量检查；将“适用边界”写成“该方法在哪些工作负载和运行条件下有效、在哪些条件下不再有效”。
- 这不是机械禁词表。数学约束、事务边界等有明确学术含义的术语可以使用，但首次出现时必须说明约束的对象、条件和作用。英文缩写、内部数据结构名和实验指标也要在首次出现时给出中文解释。
- 正式材料定稿前必须搜索上述高风险词并逐句检查，同时检查连续名词堆叠、只有结论没有读图方法、用抽象分类代替具体比较条件等问题。读者不应依赖项目内部文档才能理解正文。

## 6.5 文献优先设计规则

设计系统/算法/实验方案时，优先从项目 CCF-A 文献清单提取设计模式和策略，不凭空设计。完整方法论见 `research/README.md` §文献优先设计方法论，Baseline 矩阵见 `experiments/plans/baseline_reference.md`。

## 7. 实验结果讲解规则

按七步结构：实验设置 → 实验设计 → 严谨性自检 → 实验数据（基于 CSV）→ 结果解释（事实/推断/待确认/不能声称）→ 对课题含义 → 下一步。禁止把 microbenchmark 包装成完整论文结论。详见 `learning/AGENTS.md`。

## 7.5 实验执行与结果记录流程（每次实验必跑，自动执行）

**A. 跑前 pre-flight**：endpoint 健康 + PG + Ray 干净（主机重启过则先 `rm -f /tmp/ray/ray_current_cluster`，否则 `ray.init()` 会卡 14 分钟连死 GCS）；同协议 **bounded HTTP baseline** 可用（feeding-saturation 门禁的参照）；策略参数设到能测出效应的区间（非 trivial）。

**B. 跑**：统一干净合同（见 `experiments/plans/` RC1/RC2 模板）——2×4090 + 当前拓扑（2-ep 干净基线 / 4-ep consolidation）+ **最新修正 workload** + tokens/s + httpx async + token-IDs + prefix-cache ON + K256/W65536/fixed-50ms + 1 warmup + 3 formal（formal 由 runner 交错）。

**C. 跑完先合规自检（任一不过 → 不抽策略结论，诊断/重跑/丢弃）**：
1. **喂饱 GPU/vLLM**：`gpu_utilization_pct_mean` ≥ ~80% + `vllm_num_requests_running` 持续高 + `waiting` 低。**用 `*_mean/p50/p95/max` 系列，不用单次 snapshot 列 `gpu_utilization_pct`**（那是单点采样，曾显 0% 假象）。
2. **feeding-saturation 门禁**：E2E `tokens_per_s` ≥ 95% of 同协议 bounded client。baseline（vLLM Bench / bounded HTTP）按各自标准测，它们没跑满 vLLM 是它们自己的特性，不是本门禁要修的。
3. **策略到极限**：参数在效应区间；A/B 两臂同 config 仅策略不同。
4. **稳定**：formal repeats CV 合理、一致。

**D. 结果记录（全数据进 README，按此顺序）**：
1. **实验目的**（问题 + 方法 + 关系到哪个方向）。
2. **实验设置**（平台/拓扑/workload/调度合同/重复/指标/配置路径/原始数据路径）。
3. **合规性自检**（C 的四项 + 异常指标明确标注）。
4. **实验设计**。
5. **实验数据——全组件表格（不只主指标）**：吞吐+端到端延迟（E2E tok/s / 模型侧 tok/s / operator tok/s / rows/s / E2E wall / req p50-p99 / SLO / goodput）/ vLLM 模型服务（running·waiting mean-max / KV usage / e2e-queue-inference-prefill-decode / prompt-gen tokens / TTFT 分位 / TBT-ITL 分布 / prefix_cache_hit_rate）/ GPU+能耗+MFU（util mean-max / 显存 / 功耗 / 能耗 / J per 1k tok / MFU）/ pipeline 阶段计时（db_fetch·source_fetch·organizer·submit·fanin·bounded_wait·actor_ready·wall）/ Ray-actor-调度（max_inflight / actor slots / packing util / prefix_group_ratio / batch_tokens / finish_reason）。**每个指标标注含义/单位**；异常指标标"坏、不用"。**质量+成本补充**（AI_EMBED 写回→检索闭环报 recall@k/nDCG@10 证伪"批处理/写回引入质量偏差"；成本报 $/M tokens，input/output 分计并标注单价假设）——口径见 `research/evaluation_metrics_survey_20260731.md`。
6. **结果解释**（事实/推断/不能声称）。
7. **对课题含义**。
8. **下一步**。

**E. 存储**：`experiments/results/<方向>/<exp>_<date>/{README.md, raw/}`（`raw/` = runs.csv + manifest.json + per-run requests/submissions/resources CSV）。

**F. 指标注意**：优先用 time-series 聚合列（`*_mean/p50/p95/max`）；`vllm_kv_cache_usage_perc` 是**分数（0–1）非百分比**（vLLM HELP: "1 = 100%"），按分数读时正常可靠——曾把 0.06 误读成 0.06% 当"指标坏"，实为 6%、working set 本就只占 6–45%（见 `rc1_prefix_routing/kv_budget_sweep` 纠正）。量化 KV 压力时按分数读，并用 TTFT / 命中率 / bounded client 行为等信号交叉印证饱和。

## 8. 沟通规则

对外表述：**数据库内置 AI 算子的外部分布式数据处理执行链路优化**。待确认事项见 `notes/communication_notes.md`。

## 9. 更新规则

**影响项目结构、方向、实验结论或关键入口的操作，必须记入 `PROJECT_LOG.md`。**

| 变更类型 | 必须更新 |
|---|---|
| 目录结构变化 | `PROJECT_INDEX.md`、`README.md`、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md`、受影响目录的 `README.md` |
| 实验结论变化 | 结果报告、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md` |
| 方向/题目变化 | `AGENTS.md` §1、`opening/report/opening_report.md`、`opening/feishu/`、`PROJECT_OUTLINE.md`、`PROJECT_LOG.md` |
| 规则变化 | 对应目录 `AGENTS.md`；如影响全局同步根 `AGENTS.md`，记入 `PROJECT_LOG.md` |
| 新增/删除文件 | `PROJECT_INDEX.md`、所在目录 `README.md` |
| 新增/更新图表 | `figures/README.md`、`figures/audit/`；如影响主线同步 `opening/report/` |

## 10. Git 规则

**禁止在 commit message 中添加 Co-Authored-By 或任何形式的 AI 署名。** 所有 commit 的用户署名只能是项目开发者本人。

**禁止把隐私数据提交进 Git。** 包括但不限于：API key、token（HuggingFace `hf_`、OpenAI `sk-`、GitHub `ghp_`/`github_pat_`、Slack `xox*`、Google `AIza` 等）、外部服务器 IP/host、非 localhost 的用户名/口令、私钥（`-----BEGIN ... PRIVATE KEY-----`）、`sshpass -p <pw>` 形式的密码。要求：

- 真实密钥/口令只放在仓库外的 runtime env 文件（`.gitignore` 已覆盖 `*.env` / `*.env.local`，`!*.env.example` 例外）；
- 新代码/配置/文档/脚本里的连接串一律用环境变量引用（如 `$DATABASE_URL`、`${DATABASE_URL}`），不写明文；需要给默认值时只给 `postgresql://postgres:postgres@localhost:5432/...`（公开本地默认，仅绑 localhost，非外部凭据）或 `<DB_URL>` 占位符；
- 实验报告/evidence（`command` 字段、异常文本、traceback）必须经 `src/baselines/common/redact.py` 脱敏后再落盘；
- commit 前跑 `python code/scripts/environment/scan_git_secrets.py`（默认扫暂存区；高精度拦截 key/token/私钥/外部 `user:pw@<真实host或IP>`），建议启用 `git config core.hooksPath .githooks` 作为 pre-commit 自动拦截；
- 命中真·隐私必须立即轮换该密钥；review 过的误报（如模型生成文本里的占位符 URL）记入 `code/scripts/environment/secret_scan_baseline.txt`（每条一个正则，附原因，保持精简）。
- **本地默认 `postgres:postgres@localhost` 的历史存在不批量改写**：它是公开 PostgreSQL 默认口令、只连 localhost、非外部凭据；scanner 把它放行。新增文件仍优先用环境变量引用。

## 11. 知识库同步

项目有平级 Obsidian LLM Wiki 知识库（`../ai-operator-wiki/`）。项目是知识唯一来源，知识库是编译查询界面。

**触发条件**（满足任一即提醒）：
- 用户在对话中说"记住""记下来""同步到知识库""加到 wiki"等——**立即执行同步**
- 会话中**任何知识文件被创建或修改**（`research/`（含泛读 `reading_notes/`、精读 `精读文献笔记/`、`reference/`）、`opening/literature/`（精读清单与 Top15 拷贝）、`experiments/plans/` 下的 `.md`，或用户指定的新知识路径）——**会话结束前提醒**

**操作指南**——执行同步时读取 `research/knowledge_sync_guide.md`。
