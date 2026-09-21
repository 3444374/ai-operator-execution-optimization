# AI_COMPLETE 2048 请求留出验证（2026-07-26）

## 实验设置

本实验检查 512 请求上选出的简单 fixed 50ms flush 是否能在更长的单 GPU
运行中保持正确性和策略优势，并以 queue-adaptive 25/50ms 作为复杂策略对照。

本地原始 ShareGPT/BurstGPT 数据重新构造为独立 workload
`sharegpt_burstgpt_2048_ctx1536`。使用 vLLM `/tokenize` 对每个 prompt 做
Qwen2.5-1.5B 的真实 token 计数，仅保留 `prompt_tokens + 512 <= 2048`
的完整行；没有截断、拆分或复制 prompt。最终 2048 行的 prompt token 范围为
1–1471。

原始到达跨度为 66710s，replay scale 设为 0.001194，压缩后跨度约
79.65s、平均回放强度约 25.70 req/s，与 512 请求锚点一致。其他共同参数为
token budget 6144、静态 admission `K_max=8`、ChatML、temperature 0、
max output 512、无写回、prefix cache 关闭、MFU 指标开启。

## 严谨性自检

- manifest 状态为 `completed`，2/2 场景完成，0 incident；
- 两个场景各有 2048 条 request trace 和 2048 个唯一 request id；
- 4096/4096 请求状态均为 `completed`；
- 两个场景均观测到 2048 个实际输出 token 计数和 finish reason；
- 两个场景 SLO violation ratio 均为 0；
- 两个场景 `vllm_metrics_status=ok`、`mfu_status=ok`；
- 本轮每个策略仅一次筛选，不提供显著性声明。

## 实验数据

| 策略 | E2E (s) | tokens/s | request P99 (s) | submissions | energy/1k tokens (J) | MFU |
|---|---:|---:|---:|---:|---:|---:|
| fixed 50ms | 456.555 | 2003.618 | 368.064 | 654 | 65.476 | 0.09921 |
| adaptive 25/50ms | 464.814 | 1968.478 | 377.677 | 657 | 65.452 | 0.09748 |

adaptive 相对 fixed 50ms：

- tokens/s：-1.75%；
- E2E：+1.81%；
- request P99：+2.61%；
- submissions：+0.46%；
- energy/1k tokens：-0.04%；
- MFU：-1.75%。

输出分布也接近：fixed 50ms 的实际输出 token P50/P95/P99 为
373/512/512，adaptive 为 371/512/512；达到长度上限的比例分别为
33.54% 和 34.08%。

## 结果解释

**事实**：fixed 50ms 在 2048 请求留出中仍同时获得更高 tokens/s、更低
E2E、更低 request P99 和更少 submissions；adaptive 没有出现规模扩大后的
排序反转。

**事实**：相对既有 512 请求 fixed 50ms 三次重复均值，2048 fixed 50ms 的
tokens/s 由 2226.656 降至 2003.618（-10.02%）；请求数扩大 4 倍时 E2E 和
P99 分别扩大约 4.45 倍和 4.54 倍。这说明长运行可完成且没有请求丢失，但
持续积压仍会放大端到端尾延迟。

**推断**：当前单 GPU 默认应采用 fixed 50ms，而不是为近似相同行为承担
adaptive 控制复杂度。下一阶段更值得研究的是减少持续积压、改进数据组织和
服务容量匹配，而不是继续微调 25/50ms 二档切换。

**不能声称**：单次 2048 筛选不能证明吞吐下降具有统计显著性，也不能推广到
其他模型、GPU、多 endpoint 或真实生产到达过程。

## 决策

不追加 adaptive 重复。fixed 50ms 作为当前单 GPU 文本主线提交策略；保留
queue-adaptive 实现与 trace，用于未来负载范围或窗口集合改变时重新评估。

原始入口：

- `scenario_config.json`
- `screen/manifest.json`
- `screen/runs.csv`
- `screen/*.requests.csv`
- `screen/*.submissions.csv`
- `screen/*.flush.csv`
- `screen/*.resources.csv`
