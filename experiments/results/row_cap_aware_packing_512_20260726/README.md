# 512 行 row-cap-aware packing 筛选与重复验证

日期：2026-07-26

## 1. 实验设置

- 链路：PostgreSQL 18.4 + pgvector 0.8.2 → Daft → Arrow payload
  boundary → Ray task → vLLM 0.25.1 → Qwen2.5-1.5B。
- 硬件：单张 RTX 5070 12 GB。
- workload：`sharegpt_burstgpt` 前 512 行，`doc_id` 顺序，每个 prompt
  是一个完整请求。
- 输出上限：16 tokens；静态提交上限 `K_max=8`。
- vLLM：`--enable-mfu-metrics --no-enable-prefix-caching`。关闭 prefix
  cache 后先执行一次不计入正式结果的全链路冷启动。
- 主要比较：sequential token-budget、classic BFD、BFD-inspired
  row-cap-first placement。

## 2. 实验设计

最初设计为 row cap `{16,32,64}` × token budget
`{4096,6144,8192}` × 3 种算法。第一次完整筛选发现运行服务实际启用了
prefix cache，同一批 prompt 的重复次序会改变缓存命中，导致相同配置可从约
6 秒波动到 180 秒超时。该批数据保留在本目录根部和 `repeats/` 作为事故审计，
不用于性能结论。

修正服务配置后，先在 512 行上定向筛选四组有正向机制信号的配置：

- row cap 16 / budget 6144；
- row cap 32 / budget 6144；
- row cap 32 / budget 8192；
- row cap 64 / budget 6144。

每组同时运行 sequential、classic BFD、row-cap-aware。筛选结果位于
`nocache_targeted_screen/`。最强且没有明显能耗回退的
`row cap=64, budget=6144` 进入 1 次预热 + 3 次正式重复，结果位于
`nocache_repeats/`。

## 3. 严谨性自检

- 定向筛选 12/12 成功，重复验证 12/12 成功，均无 incident。
- 每个正式 run 均有 512 个 completed request、512 个唯一 request ID、
  512 个唯一 doc ID。
- request → submission 外键完整，`batch_rows_max <= 64`。
- resource trace 非空；vLLM FLOP delta 为正；`mfu_status=ok`。
- 正式均值仅来自 `phase=formal,status=ok`；预热和受 prefix-cache 污染的
  运行不进入汇总。

## 4. 实验数据

`row cap=64, budget=6144`，3 次正式重复，均值 ± 样本标准差：

| 指标 | Sequential | Classic BFD | Row-cap-aware |
|---|---:|---:|---:|
| E2E (s) | 8.1557 ± 0.0587 | 8.2018 ± 0.0813 | 8.1024 ± 0.1157 |
| Tokens/s | 8652.8 ± 62.3 | 8605.2 ± 85.3 | 8711.9 ± 123.4 |
| Request P95 (s) | 7.7831 ± 0.0325 | 7.8185 ± 0.0575 | 7.7400 ± 0.1205 |
| SLO goodput (req/s) | 62.780 ± 0.453 | 62.429 ± 0.616 | 63.200 ± 0.899 |
| Submissions | 13 | 16 | 16 |
| Packing utilization | 0.8838 | 0.7181 | 0.7181 |
| Energy / 1k tokens (J) | 11.4847 ± 0.1692 | 11.4739 ± 0.1551 | 11.1622 ± 0.0915 |
| Mean GPU util. (%) | 56.69 ± 2.62 | 58.65 ± 2.10 | 55.76 ± 3.83 |
| MFU | 0.4407 ± 0.0032 | 0.4436 ± 0.0039 | 0.4478 ± 0.0067 |

相对匹配 sequential，row-cap-aware 的 tokens/s `+0.68%`、request P95
`-0.55%`、SLO goodput `+0.67%`、energy/1k tokens `-2.81%`、MFU
`+1.62%`。Classic BFD 的 tokens/s `-0.55%`、request P95 `+0.45%`。

原始入口：

- `nocache_targeted_screen/runs.csv`
- `nocache_targeted_screen/summary_long.csv`
- `nocache_repeats/runs.csv`
- `nocache_repeats/summary_long.csv`

## 5. 结果解释

**事实**：在 512 行、无 prefix cache、固定 16-token 输出上限下，
row-cap-first placement 给出小幅、方向一致的吞吐、P95、能耗和 MFU 信号。

**推断**：row-cap-first 可能减少 classic BFD 的行槽碎片，但它产生 16 个
submission，而 sequential 只产生 13 个；收益并非来自更高 token-budget
利用率。

**不能声称**：3 次重复和单 workload 不能证明 row-cap-aware 普遍优于
sequential，也不能证明 decreasing-order 应成为默认策略。

## 6. 对课题的含义

完整 classic BFD 不应作为默认方案。row-cap-first 是值得保留的机制消融点，
但必须通过更大规模的 SLO guardrail。Sequential token-budget 继续作为默认。

## 7. 下一步

将同一配置不变地推进到 1024 行 held-out，对比 throughput、request
P50/P95/P99、SLO goodput、energy/1k tokens 与 MFU；出现任一实质性 SLO
回退即停止晋级。
