# Code Architecture Refactor Plan

日期：2026-08-02

实施状态：`codex/code-architecture-refactor` 已完成第 0–3 阶段源码路径迁移，并完成
metrics、backend、shared-vLLM 三个第 4 阶段拆分；第 5 阶段 scripts/tests 物理分组
也已在审计分支完成，尚未合并到 main。

## 1. 结论

`src` 的主要职责层、import 边界、scripts 域分组和 tests 镜像目录已经落地。仍需另行
处理 `data/materializers/text.py` 等剩余大文件，并在服务器恢复后执行文本/图像 gate；
这些工作不与本次纯路径迁移混合。

重构采用两个正交维度：

1. **公共执行阶段**：数据源 → 数据物化/组织与代价 → 调度 → 服务后端 → 观测 → 写回 → 实验编排；
2. **模态适配**：文本与图像只保存各自的数据合同、代价特征、预处理、payload、模型后端和质量评价。

不建立两套完整的 text/image scheduler。公共 active-work、packing、credit、routing、flush
和资源观测只实现一次，模态通过 `estimated_work_units` 与 capability contract 接入。

## 2. 目标目录

```text
code/
├── src/
│   ├── data/
│   │   ├── sources/              # PostgreSQL/Daft/Arrow 读取与流式批次
│   │   ├── sinks/                # completion、embedding、pgvector 写回
│   │   ├── workloads/            # workload manifest/seed/split；不含实验策略
│   │   └── materializers/        # Arrow/Daft 将既定成员关系物化；允许引擎依赖
│   ├── planning/
│   │   ├── contracts.py          # WorkItem/WorkBatch/estimated_work_units
│   │   ├── costs/                # 中性 cost interface、profile 校准、residual
│   │   ├── packing/              # fixed/work-budget/BFD 等纯策略
│   ├── scheduling/
│   │   ├── organization/         # work-budget、service quantum
│   │   ├── submission_control/   # request/work credit、AIMD/PID/UCB、shared fair credit
│   │   ├── endpoint_routing/     # pinned/queue/work/prefix routing
│   │   ├── runtime/              # Ray adapter、actor pool、观测缓存
│   │   └── core/                 # state、lifecycle、topology、scheduler loop、errors
│   ├── serving/
│   │   ├── contracts.py          # Completion/Embedding backend protocols
│   │   ├── completion/           # OpenAI Chat/Completions、Ollama、fake
│   │   ├── embedding/            # OpenAI-compatible embedding、fake
│   │   └── probes/               # vLLM health/metrics/counter client
│   ├── modalities/
│   │   ├── text/
│   │   │   ├── contracts.py      # PromptRow/CompletionResult
│   │   │   ├── costs.py          # prompt/output-token work
│   │   │   ├── payloads.py       # Chat/Completions payload 与结果规范化
│   │   │   └── quality.py        # 输出长度/任务质量接口
│   │   └── image/
│   │       ├── contracts.py      # encoded bytes/tensor/embedding 语义
│   │       ├── source.py         # image_documents schema adapter
│   │       ├── preprocessing/    # decode/resize/normalize
│   │       ├── backends/         # CLIP/vLLM pooling/Triton adapter
│   │       ├── execution.py      # image-specific audit 与 stage boundary
│   │       └── quality.py        # top-k、mAP/F1、Recall@K/MRR/nDCG
│   ├── observability/
│   │   ├── timing.py             # stage/group/request wall
│   │   ├── resources/            # CPU/GPU/memory/power/energy/network/Ray spill
│   │   ├── schemas/              # 稳定 CSV/trace contract
│   │   └── profiling/            # profiler 配置、replay、Ray submission、trace
│   ├── baselines/
│   │   ├── common/               # manifest、result、provenance、validity gate
│   │   ├── text/
│   │   │   ├── ceilings/         # vLLM Bench
│   │   │   ├── controls/         # bounded clients（非 native）
│   │   │   ├── frameworks/       # Daft prompt、Ray Data
│   │   │   ├── products/         # OceanBase 等原生 SQL AI Function
│   │   │   └── orchestration/    # CLI、counter、双 endpoint runner
│   │   └── image/
│   │       ├── ceilings/         # GPU-resident/direct service ceiling
│   │       ├── controls/         # bounded direct CLIP
│   │       ├── frameworks/       # Daft built-in、Ray Data native graph
│   │       ├── vendor/           # 固定 upstream benchmark parity
│   │       └── orchestration/
│   ├── experiments/
│   │   ├── scenarios/            # 通用 scenario/config/resume
│   │   ├── shared_vllm/          # 多 job 配置、编排、公平性、汇总
│   │   └── calibration/          # 选择/冻结配置，不实现策略
│   └── infrastructure/
│       ├── runtime_env.py        # 线程/PYTHONPATH/Ray runtime env
│       └── runner_lease.py       # 单写者租约与恢复
├── scripts/
│   ├── data/                     # import workload
│   ├── services/                 # 本地服务启动
│   ├── baselines/                # baseline gate/matrix
│   ├── profiling/                # profile/smoke/transfer ceiling
│   ├── experiments/              # 正式 scenario/group runner
│   └── analysis/                 # summarize/select calibration
└── tests/
    ├── data/
    ├── planning/
    ├── scheduling/
    ├── serving/
    ├── modalities/{text,image}/
    ├── observability/
    ├── baselines/{text,image}/
    └── experiments/
```

## 3. 文本与图像如何共用公共核心

```text
PostgreSQL/Daft source
        ↓
text adapter: prompt/output token work
image adapter: pixels/frames/preprocess work
        ↓             （统一为 estimated_work_units）
planning + scheduling + Ray runtime
        ↓
text serving backend / image serving backend
        ↓
统一 lifecycle/resource trace
        ↓
completion sink / embedding sink
```

允许模态不同的部分：

| 能力 | 文本 | 图像 |
|---|---|---|
| 输入合同 | prompt/messages | encoded bytes/URI/tensor |
| work estimator | prompt + predicted output tokens | pixels/frames + preprocess/forward profile |
| 可选组织能力 | length-align、prefix-aware | aspect/resolution/frame-align |
| payload/backend | Chat/Completions/vLLM | CLIP/vLLM pooling/Triton |
| 质量 | task score、输出长度/一致性 | top-k、mAP/F1、Recall@K/MRR/nDCG |

禁止在公共 scheduler 中出现 `if modality == "image"`。不适用的策略通过 capability
声明拒绝，例如 image adapter 不声明 `prefix_key`，prefix-aware policy 就 fail closed。

## 4. 依赖方向

生产代码只允许以下方向：

```text
data ─┐
modalities ─→ planning ─→ scheduling ─→ serving contracts
serving implementations ───────────────┘
observability ← 各阶段只写事件/指标
sinks ← orchestration
experiments/baselines → 可以组合以上模块
```

硬边界：

- `planning/` 不 import Daft、Ray、Arrow、psycopg 或供应商 SDK；引擎相关物化归
  `data/materializers/`，避免原计划中“materializer 在 planning”与纯策略边界冲突；
- `scheduling/` 不 import `modalities.image`、`modalities.text`、Daft、Arrow、数据库连接；
- `modalities/` 不实现 credit、routing、flush 或 actor pool；
- `serving/` 不决定 batch membership 和 admission；
- `baselines/` 不 import 项目 scheduling；native arm 只能调用 vendor API graph；
- `scripts/` 只解析配置并调用 `src`，不能保存可复用业务逻辑；
- `experiments/` 可以编排但不能定义新的生产策略。

后续增加一个 AST import-boundary test，把这些约束变成 CI 门禁，而不是只写在文档中。

## 5. 现有文件的主要归属

| 当前文件 | 目标 |
|---|---|
| `sources.py` | `data/sources/postgres_text.py`；通用连接/stream helper 单独抽出 |
| `sinks.py` | `data/sinks/{completion,embedding}.py` |
| `workloads.py` | `data/workloads/text.py` |
| `request_costs.py` | `modalities/text/costs.py` |
| `packing.py` | `planning/packing/scalar.py` |
| `organizers.py` | `data/materializers/text.py`；后续再拆 Arrow/Daft adapter 与中性 contracts |
| `cost_estimation.py`、`calibration.py` | `planning/costs/` 与 `experiments/calibration/` |
| `model_backends.py` | `serving/backends/{common,completion,embedding}.py`（已完成） |
| `vllm_probe.py` | `serving/probes/vllm.py` |
| `metrics.py` | `observability/metrics/{timing,csv,statistics,resources,vllm}.py`（已完成） |
| `profiling/` | `observability/profiling/` |
| `image/` | 非 baseline 内容迁入 `modalities/image/`；baseline 内容迁入 `baselines/image/` |
| `baselines/` | 公共合同下沉 `common/`，现有实现迁入 `text/` |
| `experiment_scenarios.py` | `experiments/scenarios/` |
| `shared_vllm_experiment.py` | `experiments/shared_vllm/{config,runner,runtime,evidence,metrics}.py`（已完成） |
| `runtime_env.py`、`runner_lease.py` | `infrastructure/` |
| 根层 `profile_*.py`、`scheduling/*` 兼容文件 | 迁移调用方后删除，不长期保留双入口 |

## 6. 分阶段迁移

### 第 0 阶段：冻结行为和依赖边界

- 保存现有测试、CLI `--help`、dry-run schema 和关键 import 清单；
- 新增 architecture-boundary test；
- 不移动代码。

通过条件：当前专项测试、JSON schema 和 CLI 参数快照稳定。

### 第 1 阶段：清除重复入口

- 把调用方改到 `profiling/*`、`scheduling/{organization,submission_control,runtime,...}`；
- 删除根层 6 个 `profile_*.py` 和 scheduling compatibility shims；
- 不改算法、默认值和 CSV 字段。

通过条件：`rg` 无旧 import；全量单测通过；远端 dry-run 不变。

### 第 2 阶段：按模态整理 baseline 与 image

- `baselines/common|text|image` 落地；
- `image/` 拆成 `modalities/image` 与 `baselines/image`；
- provenance 与 native eligibility 逻辑保持不变。

通过条件：文本 64 行 gate、图像 256 行 gate 的命令和结果 schema 不变。

### 第 3 阶段：抽公共层

- 依次迁移 `data/`、`planning/`、`serving/`、`observability/`、`infrastructure/`；
- 每次只迁移一个域，旧路径保留一个提交周期的显式 deprecated shim；
- 下一提交更新全部调用方并删除 shim。

通过条件：每次迁移提交只包含路径/导入变化；相同输入的 digest、调度 trace 和 summary
完全一致。

### 第 4 阶段：拆大文件

按风险从低到高，一次只拆一个：

1. `metrics.py`；
2. `model_backends.py`；
3. `organizers.py`；
4. `baselines/cli.py` / `gate_runner.py`；
5. `profiling/replay.py` / `profiling/ray.py`；
6. `scheduling/scheduler.py`；
7. `shared_vllm_experiment.py`。

建议生产文件目标小于约 400 行；不是硬性凑行数，超过时必须能说明单一职责为何不可再拆。

### 第 5 阶段：整理 scripts/tests 和删除兼容层（审计分支已完成）

- scripts 按 data/services/baselines/profiling/experiments/analysis 分组；
- tests 镜像 `src`；
- 更新所有部署命令、结果复现命令和 README；
- 删除所有临时兼容模块。

通过条件：根层不再出现功能实现，只有包入口；所有文档路径可解析；本地全量测试与远端
文本/图像可运行性 gate 通过。

## 7. 提交纪律

- 路径重构和策略修改分开提交；
- 一个提交只迁移一个域或拆一个大文件；
- 使用 Git rename 保留历史；
- 不在目录调整时顺便格式化全仓；
- 服务器关机期间只做本地静态迁移和单测，GPU gate 等开机后执行；
- 任一阶段产生输出 schema、算法默认值或实验结果变化时立即停止，单独解释并复验。

## 8. 推荐下一步

源码域、metrics/backend/shared-vLLM 拆分和 scripts/tests 物理分组已经收敛；文档中的
可复现命令同步指向新路径，历史 raw manifest 保持原样以保全证据。下一步只处理
`data/materializers/text.py` 等剩余大文件和远端 gate，不再建立扁平兼容入口。
