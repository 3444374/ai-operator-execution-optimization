# Output-aware BFD 64-row Infrastructure Gate

## 复现入口

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\run_ai_operator_scenarios.py `
  --config experiments\results\output_aware_bfd_gate_v2_20260726\scenario_config.json `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\output_aware_bfd_gate_v2_20260726 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`manifest.json` 保存 12 条展开后的精确命令和完成状态；`runs.csv` 以及逐轮 request/submission/flush/control/resource CSV 是原始证据。复现前应确认 health、metrics、模型、prefix-cache 和 eager/CUDA-Graph 配置与 manifest 一致。

## 实验设置与问题

本门禁只验证修复后的统一约束和观测链路，不用于比较策略性能。真实链路为
PostgreSQL 18.4 + pgvector 0.8.2 → Daft native organizer → Ray task →
vLLM 0.25.1 / Qwen2.5-1.5B BF16 → RTX 5070 12 GB。vLLM 禁用 prefix
cache 并开启 MFU counter；未使用 fake backend，未写回数据库。

六个场景为 sequential/BFD × prompt-only/fixed-output-cap/trace-metadata。
共同约束是 64 个相同 doc、token budget 6144、每 submission 最多 16 行、
K_max=8、输出上限 16 tokens。每场景 1 次 warm-up + 1 次 formal。

## 严谨性审计

- manifest `completed`，12/12 runs，0 incident；
- formal 6/6 runs 均为 `ok`，共 384/384 request successes；
- 每轮 64 个唯一 request/doc ID，request → submission 外键完整；
- lifecycle 时间非负，PostgreSQL/pgvector 版本一致；
- 六轮 `batch_rows_max <= 16`，修复后的 sequential/BFD 约束一致；
- vLLM FLOP delta 均非零，GPU、功耗、能耗和 MFU 字段可用。

## 结论边界

门禁证明基础设施可以进入 512 行正式重复。64 行运行太短，GPU 采样波动大，
不能据此声称任一 packing 策略更快。复现入口见 `scenario_config.json`，
原始结果见 `runs.csv`、`manifest.json` 及逐 run trace。
