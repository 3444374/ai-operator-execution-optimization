# 沟通记录与关键信息

生成日期：2026-07-09

> **2026-08-27 口径更新**：本文档为早期沟通记录。当前对外对象表述为“PostgreSQL 内置 LOTUS
> AI 语义算子的外部分布式物理执行与调度优化”；“内置”指 SQL/planner/query lifecycle 属于数据库，
> 模型 payload 仍由数据库管理的执行通道交给外部 backend。最新口径以根 `AGENTS.md`“项目范围/
> 文档受众与对外表达”和 `PROJECT_OUTLINE.md` 为准。

## 1. 最近沟通信息

来自截图和聊天记录的关键信息：

1. 后续公司可能推广 AI 开发、多模型监督；
2. 希望用软件工程思维明确目标、验证和纠偏步骤，避免跑偏；
3. 之前讨论过把数据库算子下推到 GPU，也讨论过 Daft + Ray 路线；
4. 同事确认：
   - 一些 AI 算子会用到 Ray；
   - 数据库内置 AI 算子也会用到 Ray；
   - 可以用 AI 生成代码快速验证；
   - 验证后再考虑产品化。

## 2. 这些信息对论文方向的影响

这些信息说明：

> 当前重点不一定是传统数据库查询算子，而可能是企业 AI 算子和数据库内置 AI 算子的执行基础设施。

需要特别区分：

| 概念 | 含义 | 和当前课题关系 |
|---|---|---|
| AI 算子 | 泛指 AI 开发或 AI pipeline 中的处理步骤，例如 embedding、特征处理、批量推理、数据预处理 | 可作为 Ray/Daft/Lance 的应用 workload |
| 数据库内置 AI 算子 | 数据库通过 SQL 函数、表函数、外部执行器或服务形式暴露的 AI 能力 | 更适合对导师/达梦表述，是数据库场景入口 |
| 数据库查询算子 GPU 下推 | 传统 filter/join/aggregate 等查询算子下推到 GPU | 不是当前主线，除非后续明确要求回到 GPU 查询 |
| Daft + Ray + Lance 优化 | 优化 AI/数据库 AI 算子背后的数据处理执行链路 | 当前最合适的技术路线 |

因此，当前题目不应改成“AI 算子优化”或“GPU 算子优化”。更准确的是：

> 面向 AI 算子/数据库内置 AI 算子的 Daft + Ray + Lance 数据处理执行链路优化。

因此，Daft + Ray + Lance 路线需要从以下角度理解：

- Daft：AI 数据处理和 DataFrame pipeline；
- Ray：AI 算子和分布式任务执行；
- Lance：多模态、向量或列式 AI 数据存储；
- 数据库：提供数据入口、算子调用和产品化场景；

## 3. 后续需要继续问同事的问题

### 3.1 关于 AI 算子

1. 目前说的 AI 算子具体包括哪些？
2. 是模型推理算子，还是数据预处理算子？
3. 输入输出是什么格式？
4. 是否天然是批处理？
5. 是否需要分布式执行？
6. 为什么会用 Ray，而不是普通线程池、进程池或数据库内执行器？

### 3.2 关于数据库内置 AI 算子

1. 数据库内置 AI 算子的调用方式是什么？
2. 是 SQL 函数、表函数、外部执行器，还是独立服务？
3. 数据从数据库到 Ray/Daft 的格式是什么？
4. 是否会使用 Arrow / Lance / Parquet？
5. 数据转换成本是否可接受？
6. 哪些算子适合走 Daft/Ray/Lance，哪些不适合？

### 3.3 关于 Ray

1. 当前企业内部为什么要用 Ray？
2. Ray 主要承担：
   - task scheduling？
   - actor？
   - object store？
   - distributed execution？
   - GPU resource scheduling？
3. 当前遇到的瓶颈是：
   - 调度慢；
   - object transfer 慢；
   - shuffle 慢；
   - 部署复杂；
   - 资源控制困难；
   - 和数据库集成困难？
4. 有没有现成 profiling 数据？

### 3.4 关于产品化

1. 最后希望产品化成什么形态？
   - 数据库插件；
   - AI 算子执行引擎；
   - 数据处理服务；
   - Daft/Ray/Lance 集成方案；
   - 外部分布式执行服务？
2. 产品化最看重：
   - 性能；
   - 稳定性；
   - 部署简单；
   - 可控性；
   - 国产化；
   - 数据库集成？
3. 个人硕士论文可以负责其中哪一块？

## 4. 当前建议提问话术

可以直接问：

> 我理解现在不是单纯为了跑通 Daft + Ray + Lance，而是想验证企业 AI 算子或数据库内置 AI 算子是否适合走这条执行链路。为了避免做无用功，我想先确认：目前最真实的痛点是 Ray 调度开销、object transfer/shuffle 开销、数据库到 Daft/Lance 的数据转换成本，还是 Lance 读取层开销？哪一个是公司后续产品化最可能遇到、也最值得我作为论文切入的？

也可以问：

> 如果我先做 Mac 上的小规模验证，您觉得最有价值的是验证小任务调度、对象传输、shuffle，还是数据库 AI 算子映射到 Daft pipeline 的可行性？

---

## 5. 2026-07-31 学长反馈 + PolarDB 新颖性边界（待确认）

### 5.1 学长反馈（原话要点）

学长对项目当前方向的判断：

> "你现在的问题是你本身其实数据未到，GPU 这个事儿在你的场景里其实并不慢。我理解你要慢只有两种可能：第一种，你这个数据本身需要非常大——很大的数据先从 SSD 再到 DRAM 再到 GPU，但是你说你只是 prompt，那其实应该不大。第二种，就是你数据搬运的这个过程没有那么简单，你可能需要 CPU 去做很多处理——比如数据库里要大量分析，有一张非常大的表，你得先分析出一些结果再传给 GPU，分析过程占了很多时间，导致 GPU 不是在等数据搬运，而是在等 CPU 做数据准备。如果你单纯只是想怎么把 prompt 送到 vLLM 再让 vLLM 推理，这个事儿已经能做的空间比较有限了，因为 vLLM 本身的优化已经迭代了很多次。"

### 5.1.1 完整反馈（含三痛点 + DB-GPU 桥接场景，2026-07-31 续）

学长进一步明确**场景**与**Daft 三痛点**（详查 `research/daft_db_gpu_bridge_direction_scope_20260731.md`）：

**场景**：数据库 ↔ GPU 经 **Daft 桥接**——Daft 是数据库和 GPU 之间的数据搬运/中转桥梁；GPU 旁插几张卡；任务挪到 GPU 上跑、经 Daft 管数据搬运；**GPU 侧算子多样**（以前写好的业务处理逻辑/复杂任务，不止一个 vLLM）；数据量大；流式执行。**不能用 ShareGPT 这种对话式 workload**——它本质上对 CPU 侧数据处理没有大需求，与场景不符。

**Daft 三痛点**（学长指出的可优化处）：
1. **GPU 数量写死**：`@daft.cls(gpus=N)` 的 N 是写死逻辑，不随数据量动态——同一算子昨天 100G、今天 10000G，Daft 都用同一个 N。
2. **多算子冷启动**：4 卡要跑 100 种算子，不可能每张卡提前把所有算子的模型都加载；Daft 执行一个算子时选个 GPU、加载模型、分片传数据、再算——冷启动问题。
3. **流式 pipeline**：1 万 G 的 embedding 不能一次放 GPU，分批流式（如 100G 分到 10 卡每张 10G，算完传回，再拿下 100G）——流水线有优化空间。

**原则**："不管搞科研还是公司，最开始最重要的就是先定 benchmark/workload——场景先被人认可，里面任何优化得到正指标大家都认可；找 workload 本身就是最重要的事；关注前面数据准备过程。"

**核实结论**（工作流 `w6xclfb0g`）：三痛点全部真实（Daft 源码 `daft/udf/__init__.py` L360-410 等一手核实），可防御性排序 cold-start(②) >> gpu-static(①) > streaming(③)；可防御界面 = 批 dataflow 的 plan 阶段 foreknowledge（DAG + 数据量）vs online serving 随机到达。

### 5.2 对照项目证据的核实

| 学长判断 | 项目证据 | 结论 |
|---|---|---|
| "GPU 不慢/数据未到" | feeding-saturation 条件：Completions project/direct = 97.7%；Chat 修 httpx 后 smoke wall ≈ bounded；数据组织层 organizer 开销 <1% | **学长对**——当前纯文本单 job 场景里数据搬运不是 bottleneck，GPU 已喂饱 |
| 两种 bottleneck（大数据搬运 / 重 CPU 准备） | 项目两个都不沾：prompt 小（非大数据）；organizer <1%（非重 CPU 准备） | **学长对**——所以"空间有限"成立 |
| "只是把 prompt 送到 vLLM 空间有限" | 半年动态策略均未达到预先规定的 5% 改善幅度（AIMD/EWMA/PID/flush/service quantum/actor pool） | **学长一句话预言了项目几十轮实验的结论** |

### 5.3 学长 (b) 与项目后续方向的连接

学长第二种 bottleneck（重 CPU 数据准备 → GPU 等）正是仍有空间的方向。项目当前数据准备太轻（<1%），要让"CPU 准备→GPU 等"成为真实可优化变量，workload 必须变重——**多模态前处理（图像 decode/resize）、RAG 检索、大表语义算子**。这与 PolarDB Lakebase 的卖点（音视频 vs Spark 4–10×、util 60→80%）指向同一结论：**异构调度的收益在重 CPU 准备场景里是真实的、巨大的。**

### 5.4 PolarDB Lakebase 新颖性边界（专项核查结论）

用户提示"阿里云 PolarDB 也用 Daft on Ray"，经专项 agent 核实（官方文档 + 开源仓库交叉）：

- **属实且重要**：PolarDB Lakebase 集成**开源 Eventual-Inc/Daft on Ray（非 fork）**，内置 embed/classify/prompt，是迄今最贴近本项目技术栈的工业产品（相关度 4.5/5）。项目 `code/AGENTS.md` 的 `@daft.cls` 编码规范被工业验证。
- **双刃**：PolarDB 的卖点（CPU/GPU 异构调度、morsel+backpressure、util 60→80%）**逐条对应项目研究方向**——"Daft on Ray + 异构调度 + 背压"已是产品，**项目不能把这一层当新颖性**。
- **新颖性边界因此切清**：PolarDB 做通用**数据流** backpressure（下游慢→减缓上游），**不观测 vLLM 内部状态**（queue/KV/prefix）。项目能占的切片 = **模型服务状态感知的请求成形 + 闭源产品未公开的上游调度策略开放消融**。
- **命名陷阱**：PolarDB **没有 `AI_COMPLETE`**（Snowflake 命名），等价物是 `polar_ai.*`（SQL，外挂 HTTP 调百炼）+ Daft `prompt()`（DataFrame）。项目要对标的 analog 专门是 **Lakebase 数据湖那条线**，不是 `polar_ai` SQL 扩展。

### 5.5 Scoop 检索结果（2026-07-31 工作流返回）

**总判定：partially-scooped（部分子切片已被直接发表）。**

**已被占据、不能再声称的子切片：**

| 子切片 | 已发表工作 | 性质 |
|---|---|---|
| "首次让上游层感知模型服务内部状态"（一般性声称） | **llm-d**（KV-cache-aware Endpoint Picker，2025 OSS）、**Preble**（ICLR 2025，global prefix-aware routing） | 在线 serving 网关 / 分布式推理场景占据 |
| "prefix-cache-aware 数据/请求重排用于 DB-LLM 算子"（研究内容一的 prefix 子切片） | **SOLO**（ICML 2026 poster，OpenReview VSY1nFjumI，报 90.3% prefill 吞吐增益）、**Liu et al.**（OpenReview R7bK9yycHp，被引 24×，request+field 重排最大化 prefix sharing） | **完全相同场景的直接 scoop**——本项目 prefix-affinity routing 在 4-ep/1.5B 上的 +5.9% 信号对应的正是这一切片 |
| "让 serving 层感知 query 结构做跨算子 KV 复用" | **Kalypso**（arXiv 2607.23815，2026-07-26 提交，仅 5 天新） | 严重重叠整体 framing；区别：Kalypso 改引擎（KV pinning），本项目不改 vLLM |
| Daft+Flotilla+vLLM 同栈的 prefix bucketing + 全局 prefix-aware router | **Daft v0.6.9 'vllm-prefix-caching' provider**（Eventual 工程博客 2025-11-04） | **在本项目用的同一个栈上产品化**；其 Future Work 明确列出"router 监控 replica 实际未完成请求数 + 读 serving-engine cache metrics"——即本切片正被 Daft 团队路线图化 |

**剩余可防御切片（窄，且实证支撑偏弱）：**

在数据库 AI 算子的【离线批处理数据管线】（Daft-on-Ray，区别于在线 serving 网关、区别于推理引擎内部），以【未修改的 vLLM 为黑盒外部观察对象】（区别于 Kalypso 的引擎内 KV pinning），将 live vLLM 内部状态耦合到【提交控制层】——active-work/K 上限、request/work credit replenishment、queue-adaptive flush、多作业公平排队（区别于 llm-d/Preble 的 routing 选副本、区别于 SOLO/Liu 的 batch 内数据/字段组织），并验证文本↔图像模态无关复用性。

**诚实风险（双重）：**
1. **scoop 风险**——prefix 子切片已失，剩余切片很窄，且 Daft 团队正在关闭它。
2. **regime-failure 风险**——项目自身证据显示动态状态感知策略在 2-endpoint/2×4090 饱和 regime 下，
   相对同资源上限的静态配置**未稳定达到预先规定的 5% 改善幅度**（AIMD/flush/service quantum/
   actor pool 均为负结果）。剩余切片的实证支撑本身偏弱。

二者叠加——**不宜单靠"未被直接 scoop"声称可防御**。必须：① Related Work 显式点名并区分 SOLO / Liu / Kalypso / Daft v0.6.9 / llm-d / Preble / Abacus 七篇；② 找到一个剩余切片确有显著收益的 regime（多 job 高压 / 重 CPU 准备多模态可能是最后机会）。

**仍需补的检索（工作流 remaining_search）：** SOLO/Kalypso/Liu 全文细读（确认是否 ONLY 数据重排，还是也覆盖 submission pacing）；submission control × live serving 状态的精确空白；多模态(VLM/CLIP) batch scheduling on Daft/Ray；Daft v0.6.9 之后 release 是否已出 true state-aware router；ICDE/OSDI/SIGMOD 2026 proceedings 扫描。

### 5.6 当前决定（不变题目）

- **题目不变**——仍是"数据库 AI 算子的外部执行链路优化"。
- **新颖性表述需精修**：不能写成"Daft on Ray 异构调度优化"（PolarDB 已做），要写成"**在通用 Daft-on-Ray pipeline 之上，加一层模型服务内部状态感知（vLLM queue/KV/prefix）的上游请求成形与提交控制**"。PolarDB Lakebase 列入 Related Work 必点名的最近、最同栈工业系统。
- **下一步**：① 等 scoop 工作流结果决定新颖性能否定稿；② 据附录 B.4 公开数据集清单锁定 workload 锚点（优先能体现重 CPU 数据准备的多模态/RAG 类，见 survey 文档附录 A.4）。
