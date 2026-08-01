# Text comparison harness

本包只负责文本 AI_COMPLETE 的共同输入/输出合同、薄适配和证据落盘，不承载项目调度策略。

## 分层

| 层 | 模块 | 角色 |
|---|---|---|
| 共享合同 | `contracts.py`、`manifests.py`、`results.py`、`postgres_manifest.py` | immutable input、exactly-once 与公共指标 |
| 身份门禁 | `provenance.py` | ceiling/control/native 的来源、调度所有者与正式资格 |
| 框架原生 | `runtime/daft_prompt.py`、`runtime/ray_data_http.py` | 调用 vendor API graph；不加入项目 credit/router |
| 数据库产品 | `products/oceanbase.py` | OceanBase 原生 SQL `AI_COMPLETE` adapter |
| 服务上限 | `ceilings/vllm_bench.py` | 官方 vLLM Bench；不是数据库/框架 baseline |
| 直接控制 | `controls/async_http.py`、`controls/batched_completions.py` | 项目自写强客户端；不是 native baseline |
| 编排 | `cli.py`、`gate.py`、`gate_runner.py` | 双 endpoint gate、counter、失败证据和 CLI |

`runtime/` 内只允许 payload/response adapter 和 vendor API 调用。需要 active-work、K、
router、flush、shared credit 或自定义 actor pool 的代码属于项目方法，应放在
`src/scheduling/` / `src/profiling/`，不能倒流进 native baseline。

复测合同见
`experiments/plans/text_native_baseline_rerun_20260802.md`。
