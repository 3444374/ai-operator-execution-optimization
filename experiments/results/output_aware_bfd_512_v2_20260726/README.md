# Output-aware BFD 512-row Comparison

## 1. 实验设置

研究问题：在相同 output-cost 语义和相同 token/row 双重约束下，离线
Best-Fit Decreasing 是否优于 sequential token-budget packing。

真实链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft native organizer →
Ray task → vLLM 0.25.1 / Qwen2.5-1.5B BF16 → RTX 5070 12 GB。vLLM
禁用 prefix cache 并开启 MFU counter；未使用 fake backend，未写回。
共同参数为同一批 512 doc、`source_order=doc_id`、token budget 6144、
row cap 16、K_max=8、输出上限 16 tokens。六场景各 1 次 warm-up + 3 次
seeded interleaved formal repeat。

MFU 使用 vLLM `estimated_flops_per_gpu_total` 的运行增量，除以
`operator_wall_s` 和 RTX 5070 密集 BF16 Tensor、FP32 accumulate 峰值
61.7 TFLOP/s。峰值来自 NVIDIA RTX Blackwell architecture 表；没有使用
988 AI TOPS 作为 BF16 分母。

## 2. 实验设计

| packing | output cost |
|---|---|
| sequential token budget | prompt-only / fixed 16-token cap / trace metadata |
| global deterministic BFD | prompt-only / fixed 16-token cap / trace metadata |

`trace_target_output` 来自未与 ShareGPT prompt 配对的 BurstGPT metadata，
只用于成本敏感性实验，不是当前 Qwen prompt 的真实输出 oracle。

## 3. 严谨性自检

- manifest `completed`，24/24 runs，0 incident；
- 18 个 formal run 共 9,216/9,216 request successes；
- 每轮 512 个唯一 request/doc ID，request → submission 外键与 lifecycle
  时间顺序通过审计；
- 所有场景 `batch_rows_max <= 16`，输入集合、模型、输出 cap、K_max 和
  writeback 均相同；
- 每轮均记录 server/pgvector 版本、GPU 利用率/显存、功耗/能耗、vLLM
  running/waiting/KV cache、FLOP delta 与 MFU。

## 4. 实验数据

下表为 3 次 formal repeat 的均值；`±` 后为样本标准差。

| 场景 | rows/s | E2E (s) | request P95 (s) | submissions | budget util. | GPU util. | energy (J) | MFU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| seq prompt | 49.40 ± 7.73 | 10.53 | 9.99 | 33 | 0.308 | 74.8% | 1380.4 | 0.340 |
| seq fixed | 56.28 ± 2.38 | 9.11 | 8.62 | 33 | 0.348 | 65.1% | 1000.5 | 0.392 |
| seq trace | 50.93 ± 5.30 | 10.12 | 9.70 | 39 | 0.868 | 68.5% | 1260.1 | 0.350 |
| BFD prompt | 52.97 ± 3.70 | 9.70 | 9.26 | 36 | 0.282 | 70.8% | 1198.9 | 0.368 |
| BFD fixed | 52.98 ± 8.99 | 9.87 | 9.39 | 36 | 0.319 | 64.0% | 1165.6 | 0.368 |
| BFD trace | 57.06 ± 2.91 | 8.99 | 8.61 | 44 | 0.769 | 62.6% | 1025.2 | 0.399 |

同成本模式下，BFD trace 相对 sequential trace：rows/s +12.019%、E2E
-11.187%、request P95 -11.203%、energy -18.639%、energy/1k observed
tokens -18.633%、MFU +13.906%。相对本规模最强 practical baseline
`seq_fixed`，BFD trace 仅 rows/s +1.384%，且 energy +2.474%。

## 5. 结果解释

**事实**：在 512 行、trace metadata 成本模式下，BFD 三个 paired repeat
的吞吐差值均为正（+21.982%、+15.078%、+0.927%），并降低端到端时间和
能耗。BFD 并未提高 packing budget utilization，且生成了更多 submission。

**推断**：该收益更可能来自 batch 内成本形状与服务尾部变化，而不是减少
submission 或提高 bin utilization。由于 n=3 且不同 repeat 波动明显，当前
只是正向候选证据。

**不能声称**：不能据此声称 BFD 普遍优于 sequential，也不能把未配对 trace
metadata 写成输出预测准确性。1024 行确认实验必须决定该趋势是否外推。

## 6. 对课题的含义

数据组织算法与成本模式存在交互，单看 packing utilization 不足以预测端到端
性能；GPU、能耗和 MFU 必须与 request E2E 一起评估。这为后续
batching × submission 联合搜索提供了真实 reward 字段。

## 7. 复现入口与下一步

- 配置：`scenario_config.json`
- 调度/incident：`manifest.json`
- run 指标：`runs.csv`
- 绘图长表/宽表：`summary_long.csv`、`summary_wide.csv`
- 原始轨迹：`*.requests.csv`、`*.submissions.csv`、`*.resources.csv`

下一步使用同一约束在完整 1,024 doc 上确认，并保留 `seq_fixed` 作为强基线。

