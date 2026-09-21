# 受控 prefix-aware batching 筛选（2026-07-26）

## 实验设置

本实验回答：当共享 prefix 比例受控变化时，prefix-aware token-budget 组织是否
优于 sequential token-budget。

从同一批 512 个独立 ShareGPT/BurstGPT session 构造四个 workload：

| workload | 共享 prefix 行数 | 比例 |
|---|---:|---:|
| `sharegpt_prefix_0` | 0 | 0% |
| `sharegpt_prefix_30` | 154 | 30.08% |
| `sharegpt_prefix_70` | 358 | 69.92% |
| `sharegpt_prefix_100` | 512 | 100% |

共享集合按稳定哈希选择，30% 集合是 70% 集合的子集。构造仅在完整原 prompt
前增加公共指令，不修改原 prompt 后缀；每行重新通过 vLLM `/tokenize` 计数，
不截断、不拆分。

真实链路为 PostgreSQL → Daft → Arrow → Ray task → vLLM
Qwen2.5-1.5B → RTX 5070。输出固定为 16 tokens，以减少 decode 方差并隔离
prefill/组织机制；不是 fake backend。共同参数为 token budget 6144、静态
`K_max=8`、无写回。vLLM prefix cache 关闭，因此本轮不测试缓存命中收益。

## 代码门禁

初版筛选发现原 `prefix_aware` 会按每行唯一的 `prefix_key` 哈希重排 0% 共享
workload，造成无意义退化。随后按红—绿测试修正为：

1. 只聚合实际重复的非空 prefix；
2. 唯一 prefix 保持原始相对顺序；
3. 重复 prefix 组在第一次出现位置一次性发出；
4. 组内保持原始顺序；length-align 仍由独立策略负责。

最终 v3 语义避免把 prefix grouping 和 length alignment 隐式耦合。v1/v2
原始结果保留用于代码演进审计，策略结论只使用 `screen_v3/`。

## 严谨性自检

- v3 manifest 状态为 `completed`，8/8 场景完成，0 incident；
- 每个场景 512 条 request trace、512 个唯一 request id；
- 4096/4096 请求状态为 `completed`，且都有实际 output token 与 finish reason；
- 所有场景 SLO violation ratio 为 0；
- 所有场景 vLLM metrics 和 MFU 状态正常；
- 每个场景仅一次机制筛选，不提供显著性结论。

## v3 实验数据

| prefix 比例 | 策略 | E2E (s) | tokens/s | P99 (s) | submissions | prefill mean (s) | energy/1k tokens (J) | MFU |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 0% | sequential | 8.235 | 8746.750 | 7.890 | 12 | 0.2582 | 11.158 | 0.44479 |
| 0% | prefix-aware | 8.306 | 8672.188 | 7.944 | 12 | 0.2575 | 11.176 | 0.44211 |
| 30% | sequential | 8.575 | 9102.680 | 8.212 | 13 | 0.2591 | 10.962 | 0.45861 |
| 30% | prefix-aware | 8.545 | 9134.132 | 8.134 | 13 | 0.2662 | 11.212 | 0.46635 |
| 70% | sequential | 8.998 | 9559.494 | 8.629 | 15 | 0.2642 | 11.031 | 0.47959 |
| 70% | prefix-aware | 9.151 | 9400.112 | 8.709 | 14 | 0.2626 | 11.110 | 0.47726 |
| 100% | sequential | 9.283 | 9911.954 | 8.910 | 15 | 0.2636 | 10.657 | 0.49342 |
| 100% | prefix-aware | 9.426 | 9760.801 | 9.004 | 15 | 0.2636 | 10.866 | 0.49180 |

prefix-aware 相对 sequential：

| prefix 比例 | tokens/s | E2E | P99 | prefill mean |
|---:|---:|---:|---:|---:|
| 0% | -0.85% | +0.86% | +0.69% | -0.28% |
| 30% | +0.35% | -0.34% | -0.95% | +2.74% |
| 70% | -1.67% | +1.70% | +0.93% | -0.61% |
| 100% | -1.53% | +1.55% | +1.05% | -0.02% |

## 结果解释

**事实**：修复语义后，prefix-aware 在四个比例下都没有稳定的吞吐、E2E、
P99 或 prefill 优势。100% 共享时两策略组织结果相同，观测差异属于单次运行
噪声范围。

**事实**：v1 中“组内 length-align”可降低 token spread 和 prefill，但增加
batch 数并降低端到端吞吐；因此已从 prefix-aware 中移除，后续应作为显式联合
策略单独消融。

**推断**：在 prefix cache 关闭时，仅相邻组织共享前缀不足以减少实际模型工作。
当前默认继续采用 sequential token-budget。

**不能声称**：本轮不能判断 vLLM prefix cache 开启后的收益，也不能否定带有
缓存容量、命中率和路由亲和性的 prefix-aware 系统。

## 下一步

若启动 prefix cache 机制实验，必须单独记录 cache 配置和命中相关指标，并比较：

1. cache off + sequential；
2. cache on + sequential；
3. cache on + prefix-aware。

在该门禁完成前，prefix-aware 保留为候选实现，不进入当前单 GPU 默认配置。

原始入口：

- `scenario_config.json`
- `screen/`：初版哈希重排语义；
- `screen_v2/`：只聚合重复 prefix，但隐式组内 length-align；
- `screen_v3/`：最终职责单一语义及本文结论。
