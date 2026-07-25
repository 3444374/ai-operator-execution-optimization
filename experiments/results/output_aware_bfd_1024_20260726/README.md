# Output-aware BFD 1024-row Confirmation

## 1. 实验设置

本实验检验 512 行中 `BFD trace` 的正向候选信号能否扩展到完整 1,024 doc。
硬件、软件、无 prefix cache、MFU 定义、token budget 6144、row cap 16、
K_max=8、输出上限 16 tokens 与 512 实验一致。三个场景各 1 次 warm-up +
3 次 seeded interleaved formal repeat：

- `seq_fixed`：当前 strongest practical baseline；
- `seq_trace`：与 BFD 保持相同成本模式的算法对照；
- `bfd_trace`：候选策略。

## 2. 严谨性自检

- manifest `completed`，12/12 runs，0 incident；
- 9 个 formal run，共 9,216/9,216 request successes；
- 每轮 1,024 个唯一 request/doc ID，外键与时间顺序通过；
- 每轮 `batch_rows_max <= 16`、FLOP delta 非零、MFU status `ok`；
- 真实 PostgreSQL → Daft → Ray → vLLM，不使用 fake，不写回。

## 3. 实验数据

| 场景 | rows/s | E2E (s) | request P95 (s) | SLO violation | submissions | budget util. | GPU util. | energy (J) | energy/1k tokens (J) | MFU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| seq fixed | 58.56 ± 6.34 | 17.63 | 16.84 | 0.621 | 65 | 0.351 | 83.1% | 2544.8 | 18.17 | 0.390 |
| seq trace | 52.92 ± 8.76 | 19.68 | 18.86 | 0.658 | 77 | 0.884 | 83.5% | 2874.8 | 20.53 | 0.350 |
| BFD trace | 50.19 ± 5.60 | 20.56 | 19.67 | 0.923 | 87 | 0.782 | 85.2% | 3070.3 | 21.92 | 0.332 |

`BFD trace` 相对同成本模式 `seq_trace`：rows/s -5.156%、E2E +4.510%、
request P95 +4.318%、energy +6.801%、energy/1k tokens +6.816%、MFU
-5.130%。相对 `seq_fixed`：rows/s -14.293%、E2E +16.672%、energy
+20.651%、MFU -14.740%。

## 4. 结果解释

**事实**：1024 行没有复现 512 行的正向趋势。BFD trace 三个 repeat 相对
seq fixed 的吞吐均为负；相对 seq trace 的平均吞吐也为负。BFD 产生 87 个
submission，较 seq trace 多 10 个，同时 budget utilization 更低。

**推断**：在 16 行硬上限和当前成本分布下，decreasing order 造成的 row-cap
fragmentation 随规模扩大，额外 submission 与尾部成本超过了潜在的负载均衡
收益。更高平均 GPU utilization 并不等价于更高有效吞吐；本实验中它伴随更长
运行时间、更高能耗和更低 MFU。

**待确认**：需要将 row cap、token budget 和 packing objective 联合搜索，
或设计 row-cap-aware/latency-aware packing，而不是直接宣称经典 BFD 是最终
策略。`trace_target_output` 仍只是未配对 metadata。

**不能声称**：不能声称当前 BFD 在完整规模优于 sequential；也不能用
512 单点结果替代规模曲线。10 秒 SLO 在 1024 offline job-start 时间原点下
导致较高 violation，只是统一观测阈值，不是业务承诺。

## 5. 对课题的含义与下一步

该负结果明确了研究内容一的设计边界：动态数据组织必须联合考虑 token
capacity、row cap、submission 数与服务尾部，经典 BFD 只能作为候选。
下一步应优先做：

1. row cap `{16, 32, 64}` × token budget `{4096, 6144, 8192}` 的小型联合搜索；
2. reward 同时包含 request P95、throughput、energy 和 MFU；
3. 只在 held-out 规模复验后选择最终 packing；
4. 将输出成本改为同 prompt/同模型校准值后，再评价 output-aware 收益。

复现入口为 `scenario_config.json`；原始与绘图数据见 `runs.csv`、
`summary_long.csv`、`summary_wide.csv` 和逐运行 trace。

