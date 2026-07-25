# Superseded 512-row Output-aware BFD Run

本目录是约束缺口的失败审计，不能用于性能结论。

- 原计划 24 runs，实际完成 21，manifest 状态为 `failed`，1 个 incident。
- sequential token-budget 当时只受 token budget 约束，单 submission 达
  71--94 行；BFD 同时受 `ray_batch_rows=16` 约束，比较并未控制最大行数。
- 第 22 轮 `seq_fixed` 将大量序列同时提交到 vLLM，最终触发 180 秒 HTTP
  timeout。
- 该问题已由提交 `9adb86e` 按 TDD 修复：所有 token-budget 策略同时执行
  token budget 与 row cap。

修复后的有效结果位于 `../output_aware_bfd_512_v2_20260726/`。本目录仅保留
incident、stderr 和中间 trace，以便审计为什么旧数据被排除。

