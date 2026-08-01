# code/AGENTS.md

本目录用于可复用工程代码、实验驱动脚本和测试入口。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放可复用系统实现，而不是一次性 notebook 或临时 CSV。
- 承载 PostgreSQL connector、fake/real `AI_EMBED` pipeline、Ray/Daft/Lance 类外部执行链路、优化实现和测试入口。
- 为 `feasibility/` 和 `motivation/` 提供可复用脚本，但正式结果仍写入对应结果目录。

## 边界

- 不在 `code/` 堆临时 CSV、原始实验结果或一次性 notebook。
- 绘图、图表复现和素材筛选脚本归 `figures/scripts/`；`code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口。
- 连接验证结果归 `feasibility/results/`。
- 系统画像、瓶颈定位和动机结果归 `motivation/results/`。

## 规则

- 所有代码应有可运行入口、配置、结果输出和最小验证。
- 不提前引入复杂框架；只有当重复逻辑稳定出现时才抽象。
- 每条真实运行 CSV 必须记录实际 `server_version` 和 `pgvector_version`，不能靠目录名推断平台。
- Python baseline、Ray task、Ray actor、模型服务等 baseline 要尽量共享同一数据读取和写回路径，避免 baseline 不可比。
- baseline 共享的是输入、输出与计时合同，不共享项目调度实现。正式 baseline 必须
  直接使用官方 benchmark、内置函数或官方 native API graph，让 Daft/Ray Data/产品
  自己负责 batching、backpressure、actor/task 调度；项目 adapter 只能负责数据源、
  sink、质量审计和指标采集。自写 UDF 可以作为 workload kernel，但不得包含项目
  credit/inflight/router；自写框架执行图必须命名为 `diagnostic_reference`，不得冒充
  official/native baseline。
- 完成代码实现或功能测试后，按根规则同步更新 `learning/` 中的学习讲解。

## 代码质量总则（模块清晰 / 框架分明 / 低耦合 / 目标清晰）

所有新增代码（系统实现、实验驱动脚本、profiling/baseline 入口）必须同时满足下面四条总则。下方「编码规范」六条是这四条在具体场景下的落地，新代码**先过四条总则，再查六条专项**。

1. **目标清晰**——每个脚本/模块在文件顶部 docstring 用一两句写明它的**单一可验证目标**："回答什么问题 / 测什么 / 产出什么 / 什么结果算通过"。没有可验证目标的脚本不写。对应 `karpathy-guidelines`「先定义可验证目标、做最小实验」。实验脚本的目标必须对齐 `experiments/plans/` 或 `motivation/plans/` 里的问题，不自行发明问题。
2. **模块清晰**——每个函数/类/模块只做一件事，能脱离上下文独立理解。阅读者**不看内部实现**也应能从签名 + docstring 知道"它做什么、怎么用、依赖什么"。一个文件做太多事时拆分；一个函数超过~50 行时考虑拆分。
3. **框架分明**——分层边界显式，不跨层调用：
   - 系统实现：数据源（DB/Daft/Arrow 读）→ 数据组织/策略层 → 引擎/服务调用 → fan-in → 写回 sink，各层经抽象接口通信；
   - 脚本类（profiling/实验驱动/baseline）：按 `配置 → 阶段(stage) → 扫描/编排 → 输出` 组织，不把四者揉进一个长 `main`；
   - 每层的输入输出类型在签名/docstring 标注（如 `bytes → Tensor → embeddings`）。
4. **低耦合**——模块经**明确接口**通信，无隐式跨依赖：
   - 策略层只依赖抽象（如 `BatchRequest`），不感知底层 Arrow/Daft（编码规范 §3 的总则化）；
   - 新增引擎/模型后端时保留旧后端作对照/回退；
   - 不在策略/组织代码里直接调 `daft.*` / `pyarrow.*` / `psycopg.*` 引擎 API；
   - 能复用已有 helper（PG 连接、模型加载、计时）就复用，不重造；profiling 脚本不引入 scheduler/organizer 除非目标就是测它们。

**总则与编码规范的关系**：编码规范 §1（保持简单）= 总则 1+2 的复杂度上限；§3（策略层不依赖引擎层）= 总则 4 的具体化；§4（多模态复用）= 总则 3 的跨模态体现；§6（指标完整性）= 总则 1"产出什么"的指标要求。冲突时以四条总则为纲、六条专项为目。

## 编码规范（2026-07-23 综合评估产出）

以下规则从项目文献精读笔记、Wiki 知识库和已有实验教训中提取，新代码必须遵守。

### 1. 保持简单（来源：Ray ConcurrencyCapBackpressurePolicy 废弃教训）

Ray Data 的 `ConcurrencyCapBackpressurePolicy` 用 ~400 行实现了 EWMA + deadband 自适应并发控制，性能反而不如简单方案，已被官方废弃。

**要求**：
- 自适应策略第一版 <100 行（不含 dataclass 和 abstract 定义）
- 优先用简单规则（静态阈值、固定步长），只在数据证明必要时再增加复杂度
- 不要提前做"可配置性"——先用硬编码值跑通，确认有效后再参数化

**文献依据**：`research/ray_actor_dynamic_batching_reference.md` §3.7

### 2. 每行是一个独立完整的请求（来源：vLLM Chunked Prefill 语义安全分析）

vLLM 的 `--enable-chunked-prefill` 在 token 级做数学等价拆分；手动将一份 prompt 拆成多条请求会导致 KV cache 隔离、上下文断裂。

**要求**：
- Token-budget 策略决定"多少行合并为一个 batch"——每行仍是独立完整的 vLLM 请求
- **禁止**在 Ray actor/Daft UDF 中自动拆分单行 prompt 内容为多条请求
- 超长单行（超过模型 context window）只能：预处理截断、独占一个 batch、或从数据集排除
- 代码中涉及文本拆分的地方必须注释：这是"行间合并"还是"行内拆分"——前者 OK，后者禁止

**文献依据**：`experiments/plans/data_organization_batching.md` §2.5.7；vLLM SOSP 2023 论文

### 3. 策略层不依赖引擎层（来源：DataOrganizer 抽象设计）

当前 `organizers.py` 通过 `ArrowOrganizer` / `DaftOrganizer` + `BatchRequest` 元数据实现了引擎隔离。

**要求**：
- 策略代码（admission、routing、request_pool）只依赖 `BatchRequest` 抽象（`prompt_tokens_sum`、`row_count`、`prefix_key` 等），不感知底层是 Arrow 还是 Daft
- 新增引擎后端时保留旧后端作为对照/回退（如 Daft 接入时保留 Arrow）
- 不在策略代码中直接调用 `daft.*` 或 `pyarrow.*` API

**文献依据**：`experiments/plans/strategy_design_implementation_reference.md` §4.2

### 4. 多模态复用文本代码路径（来源：策略泛化性验证设计）

Token-budget 和 frame/image-work budget 本质都是"按计算量预算组织数据"——合并逻辑相同，仅代价估计器和模态适配器不同。

**要求**：
- 新的公共合同使用中性字段 `estimated_work_units` / `work_unit`；文本
  `prompt_tokens + output_tokens` 和图像 `frames/pixels/preprocess_cost` 只在模态
  adapter 中换算。迁移期间保留 token 字段兼容层，不在 scheduler 中增加
  `if image ... else text ...` 分支。
- budget packing、bin-packing、flush、credit、routing 只消费中性 work units；
  length-align/prefix-aware 等**不适用所有模态**的能力由显式 capability 声明，
  不能靠缺列时静默退化。
- 不为图像单独写一套 batch 构造代码
- `code/scripts/` 只做配置和编排；可被 profiler、runner、baseline 共同复用的
  decode/preprocess/model-output 逻辑必须放入 `code/src/`，禁止生产 runner
  反向 import profiling 脚本。

### 4.1 图像执行路径合同

图像路径新增代码必须先冻结下列合同，再实现 endpoint 或实验：

- **输入表示**：明确是 encoded bytes、共享 URI 还是预处理后的 tensor；不能在
  不同 baseline 间悄悄改变表示。
- **预处理归属**：decode/resize/normalize 位于 Daft/Ray 上游或模型服务内部必须
  写入 manifest。两种归属是不同实验边界，不能只比较一个总吞吐数。
- **embedding 语义**：记录模型 revision、processor revision、dtype、输出维度、
  projection 和 L2 normalization；写回前校验 shape、finite 和行 ID 对齐。
- **隐藏 batching**：记录服务端 dynamic/continuous batching 是否开启及参数；研究
  上游 batching 时，服务端 batching 要么冻结，要么作为独立实验因子。
- **流式边界**：正式 image runner 不得在 driver 上对全量图像调用
  `to_arrow()`、`list(to_arrow_iter())` 或等价 collect；仅 smoke/profile 可在文档
  明示规模边界后 materialize。
- **引擎隔离**：Daft/Ray pipeline 只依赖 typed `ImageEmbeddingBackend` 合同；
  Ray GPU actor、vLLM pooling、Triton、Daft `@daft.cls` 和测试 fake backend 均由 adapter 接入，
  不把供应商 API 写进 organizer/scheduler。

**文献依据**：`research/daft_ray_multimodal_reference.md`；Daft DataFrame 统一 API

### 5. 文献优先——新机制从精读笔记提取（来源：文献优先设计方法论）

项目已有分级文献基线 + 49 篇精读笔记。凭空设计的机制大概率重复已有工作或漏掉已知最优 practice。

**要求**：
- 新增策略机制前，先查 Wiki `文献地图` MOC 中对应研究岛的论文
- 代码注释标注每个非平凡设计决策的文献来源（如 `# Ref: Clipper NSDI 2017, AIMD adaptive batching`）
- 如果找不到文献依据，标注为"工程决策"并在实验报告中说明

**文献依据**：`research/README.md` §文献优先设计方法论；Wiki `设计方法论` MOC

### 6. 新实验指标完整性（来源：指标盲区与外部 benchmark 审计）

所有新实验遵循 `experiments/plans/baseline_reference.md` 的“数据库 AI 算子评价指标
合同”，按算子记录适用指标，不能把文本 serving 字段机械套到图像：

- AI_COMPLETE 记录 input/output/total tokens/s、TTFT、TPOT/ITL、request E2E
  P50/P95/P99 和 SLO goodput；
- AI_CLASSIFY 记录 images/s、JCT/tail，以及 top-1/top-5 或 mAP/F1；
- AI_EMBED 记录 embeddings/s、JCT/tail，若承担检索语义则记录 Recall@K/MRR/nDCG；
- 所有模态记录 exactly-once、失败/重试/timeout/OOM、CPU/GPU/内存/能耗和工作量；
- 涉及 adaptive 的实验记录 inflight/queue 时间序列，涉及分组策略的实验记录
  per-request/row E2E 分布；
- 失败 run 也必须结构化落盘。缺 ground truth 的 smoke 只能标记任务质量未评价，
  digest/norm 不能替代 accuracy/recall。

不只为补非阻断诊断指标重跑历史实验；但任务质量、统一 system sink、失败记录等
正式排名阻断项不能靠事后推断绕过。

**文献依据**：`experiments/plans/baseline_reference.md`“数据库 AI 算子评价指标合同”；
`experiments/plans/experiment_status_and_gaps.md` §3。

详细脚本说明见 `scripts/README.md`。依赖清单见 `requirements.txt`。
