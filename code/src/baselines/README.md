# Baseline and comparison harnesses

本包负责文本/图像 baseline 的共同合同、薄适配和证据落盘，不承载项目调度策略。

## 分层

| 层 | 模块 | 角色 |
|---|---|---|
| 共享合同 | `common/{contracts,manifests,results,gate,provenance}.py` | immutable input、exactly-once、公共指标与身份门禁 |
| 文本服务上限 | `text/ceilings/vllm_bench.py` | 官方 vLLM Bench；不是数据库/框架 baseline |
| 文本直接控制 | `text/controls/` | 项目自写强客户端；不是 native baseline |
| 文本框架原生 | `text/frameworks/` | Daft prompt / Ray Data vendor API graph |
| 文本数据库产品 | `text/products/oceanbase.py` | OceanBase 原生 SQL `AI_COMPLETE` adapter |
| 文本编排 | `text/orchestration/` | PostgreSQL manifest、双 endpoint gate、counter 和 CLI |
| 图像框架原生 | `image/frameworks/` | Daft built-in / Ray Data native graph |
| 图像身份门禁 | `image/provenance.py` | image arm scheduler owner 与 formal eligibility |

`text/frameworks/` 和 `image/frameworks/` 内只允许 payload/response adapter 与 vendor
API graph。需要 active-work、K、
router、flush、shared credit 或自定义 actor pool 的代码属于项目方法，应放在
`src/scheduling/` / `src/observability/profiling/`，不能倒流进 native baseline。

复测合同见
`experiments/plans/text_native_baseline_rerun_20260802.md`。
