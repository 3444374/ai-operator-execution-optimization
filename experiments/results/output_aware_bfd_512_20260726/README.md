# 修复前 512 行 Output-aware BFD 失败审计

## 实验设置与问题

该实验计划在真实 PostgreSQL→Daft→Ray→vLLM/Qwen2.5-1.5B→RTX 5070 链路上，对 sequential/BFD × prompt-only/fixed-output-cap/trace-metadata 六个场景做 512 行对照。目标原本是筛选 output-aware BFD，而不是验证 fake 或 CPU 路径。

## 运行与原始数据

参数入口为 `scenario_config.json`，统一 runner 命令如下：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\run_ai_operator_scenarios.py `
  --config experiments\results\output_aware_bfd_512_20260726\scenario_config.json `
  --profiler code\scripts\postgres_ai_operator_profile.py `
  --python-executable D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\output_aware_bfd_512_20260726 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics
```

`manifest.json` 保存实际命令和 incident；`runs.csv` 有 21 条 `ok` 记录，逐轮 request/submission/flush/control/resource trace 与 stdout/stderr 保留在本目录。

## 失败事实与严谨性审计

- 原计划 24 runs，实际完成 21；manifest 记录 1 个 incident。
- Sequential token-budget 当时只受 token budget 约束，单个 submission 可达约 71–94 行；BFD 同时受 `ray_batch_rows=16` 约束。
- 第 22 轮 `seq_fixed` 将大量序列同时提交到 vLLM，触发 180 秒 HTTP timeout。
- 因此失败不是“BFD 一定更快”或“sequential 一定更慢”的证据，而是实验变量未受控。

## 修复与结论边界

后续修复要求所有 token-budget 策略同时执行 token budget 与 row cap，并重新从小规模门禁开始。由于本目录的 batch membership 和最大行数不一致，21 条成功运行也全部排除在性能统计之外。

修复后的有效结果位于 `../output_aware_bfd_gate_v2_20260726/`、`../output_aware_bfd_512_v2_20260726/` 和 `../output_aware_bfd_1024_20260726/`。本目录只作为失败、timeout 和约束缺口的可追溯审计。

