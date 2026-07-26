# 修复前 Output-aware BFD 64 行门禁

## 实验设置与问题

该门禁在真实 PostgreSQL 18.4、pgvector 0.8.2、Daft native、Ray task、vLLM/Qwen2.5-1.5B 和 RTX 5070 上运行，没有使用 fake backend。它比较 sequential/BFD 与 prompt-only、fixed-output-cap、trace-metadata 三种成本模式，共六个场景，每个场景一次 warm-up 和一次 formal。

问题是验证 output-aware packing 的执行链路、request/submission/resource trace 和 MFU 字段是否可用。它不是性能排名实验。

## 运行与原始数据

复现参数见 `scenario_config.json`，使用统一 runner：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\run_ai_operator_scenarios.py `
  --config experiments\results\output_aware_bfd_gate_20260726\scenario_config.json `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\output_aware_bfd_gate_20260726 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`manifest.json` 保存 12 条展开后的精确命令；`runs.csv` 保存 12 条成功运行；逐轮 `.requests.csv`、`.submissions.csv`、`.flush.csv`、`.control.csv` 和 `.resources.csv` 保存原始轨迹。

## 严谨性自检与结果

- 12/12 scheduled runs 完成，0 incident，`runs.csv` 状态均为 `ok`。
- 六个场景和 warm-up/formal 身份完整，真实组件版本、GPU、能耗和 MFU 字段可审计。
- 但 sequential token-budget 当时没有采用与 BFD 相同的 `ray_batch_rows` hard cap，因此两种算法的最大 batch 行数没有被控制。

## 结论边界

本目录只证明修复前的端到端观测链路能够工作。由于核心约束不一致，任何 sequential 与 BFD 的吞吐、延迟、能耗或 MFU 差异都不能作为算法性能结论。

修复后的门禁位于 `../output_aware_bfd_gate_v2_20260726/`，正式规模结果位于 `../output_aware_bfd_512_v2_20260726/` 和 `../output_aware_bfd_1024_20260726/`。保留本目录是为了审计旧数据被排除的原因。
