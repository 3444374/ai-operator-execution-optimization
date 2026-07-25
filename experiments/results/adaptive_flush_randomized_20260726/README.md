# 随机化变长输出 flush 实验（2026-07-26）

## 1. 实验设置

本实验回答两个问题：

1. 在真实、可观测的变长输出下，queue-adaptive flush 是否稳定优于 25ms
   fixed-timeout；
2. 若有收益，收益来自动态窗口选择，还是来自压力期采用更长的 50ms 等待。

真实链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft 0.7.20 → Arrow →
Ray 2.56 task → vLLM 0.25.1 / Qwen2.5-1.5B → RTX 5070 12GB。vLLM
开启 MFU 指标、关闭 prefix cache。没有使用 fake backend。

正式 workload 为按 `arrival_time_s` 排序的 `sharegpt_burstgpt`。为了在
2048-token context 下给输出保留 512 tokens，数据源显式排除
`prompt_tokens > 1500` 的行；没有截断或拆分任何 prompt。共同参数为
token budget 6144、静态 admission `K_max=8`、accelerated replay
`scale=0.0005`、temperature 0、无写回。

## 2. 实验设计

### 输出语义门禁

旧 raw-completion 路径即使把上限提高到 128 tokens，64/64 请求仍因
`finish_reason=length` 结束，不能称为自然 EOS workload。

因此增加显式 `chatml` prompt envelope，但保持原始数据库 prompt 内容不变。
在 max tokens 512 的 64 请求门禁中：

- 48/64 请求自然停止；
- 16/64 请求达到长度上限；
- 实际输出范围为 2–512 tokens，均值 288.73 tokens。

### 正式随机化对比

- 策略：25ms fixed-timeout、queue-adaptive（25ms base / 50ms pressure）；
- 每个策略 1 次 warm-up + 5 次 formal；
- formal 在每个 repeat 内随机化策略顺序；
- 每次运行前等待 vLLM running=waiting=0；
- SLO 为 180s；
- 每请求记录实际 token IDs、finish reason、arrival/flush/submit/completion
  和 submission identity。

### 机制探针

正式实验后额外运行一次 fixed-timeout 50ms。该探针只用于识别机制，不作为
重复统计结论。

## 3. 严谨性自检

- 正式主实验 12/12 运行成功，manifest 无 incident；
- 10/10 formal run 均为 512 个唯一请求，exactly-once；
- 每个 request 都有 endpoint 返回的实际 output token 数和 finish reason；
- 每次运行均有 request/submission/flush/resource trace；
- tokens/s 使用 vLLM prompt + generation token counter delta；
- MFU 使用开启 `--enable-mfu-metrics` 后的正 FLOP delta；
- 5 次 formal 的策略顺序为 3 次 adaptive-first、2 次 fixed-first；
- 两组平均 generation tokens 分别为 165451.2 和 165258.6，仅相差约
  0.12%。

即使 temperature=0，同一请求在不同 HTTP batch shape 下仍可能产生不同 token
序列；因此本实验不是逐请求输出配对试验。它通过随机化重复、实际 tokens/s 和
真实输出长度分布控制该变量。

## 4. 实验数据

均值 ± 95% t 置信区间半宽，n=5：

| 策略 | E2E (s) | tokens/s | request P99 (s) | submissions | batch rows mean | energy/1k tokens (J) | MFU |
|---|---:|---:|---:|---:|---:|---:|---:|
| fixed 25ms | 133.283 ± 0.915 | 1719.899 ± 11.915 | 111.751 ± 0.633 | 200 | 2.560 | 71.698 ± 0.227 | 0.08534 ± 0.00057 |
| queue adaptive | 102.564 ± 2.189 | 2237.369 ± 46.476 | 81.153 ± 2.080 | 139 | 3.683 | 59.354 ± 0.649 | 0.11121 ± 0.00233 |

配对相对变化：

| 指标 | adaptive vs fixed 25ms |
|---|---:|
| tokens/s | +30.09% ± 2.66% |
| E2E | -23.05% ± 1.60% |
| request P99 | -27.38% ± 1.87% |
| submissions | -30.50% |

两组 180s SLO violation 均为 0；adaptive 的 SLO goodput 为 4.993 req/s，
fixed 为 3.842 req/s。

单次 fixed-50ms 机制探针：

| 策略 | E2E (s) | tokens/s | request P99 (s) | submissions |
|---|---:|---:|---:|---:|
| fixed 50ms | 102.228 | 2250.139 | 80.804 | 137 |

## 5. 结果解释

**事实**：queue-adaptive 相对 fixed-25ms 在全部 5 个 repeat 中同时提高
tokens/s、降低 E2E 和 P99，并减少 30.5% submissions。

**事实**：adaptive flush trace 绝大多数窗口由 `running_pressure` 选择 50ms；
单次 fixed-50ms 的结果与 adaptive 几乎相同。

**推断**：本 workload 的主要收益来自压力期扩大 coalescing window，而不是
复杂的逐次动态切换。当前两档 adaptive 更像自动选择 50ms 的保护逻辑。

**待确认**：fixed-50ms 尚只有一次机制探针；若要声称 adaptive 与最佳静态窗口
等价或更差，需要随机化重复 fixed-25 / fixed-50 / adaptive 三组。

**不能声称**：

- 不能声称 queue-adaptive 已优于最佳静态 timeout；
- 不能推广到多 GPU、多 endpoint 或其他模型；
- 不能把 accelerated replay 当作生产到达率；
- 不能把单次 fixed-50ms 探针写成显著性结论。

## 6. 对研究内容的含义

研究内容二现在有更清晰的完成边界：

- static `K_max=8` 仍是 admission guardrail；
- 50ms pressure-window coalescing 在变长输出下具有稳定正向证据；
- 当前 queue-adaptive 实现可以自动进入该窗口，但尚未证明动态性优于最佳静态
  timeout；
- 因此论文贡献应表述为“服务压力感知的等待窗口选择及其适用边界”，不能只用
  adaptive 标签包装更长 timeout。

本轮同时补齐了此前缺失的真实 per-request output tokens 和 finish reason，使
后续控制器、代价估计和多模态实验能区分估计代价与实际服务代价。

## 7. 下一步

1. token budget × Kmax × flush 联合筛选及固定 16-token cap 的候选重复已经
   完成，详见 `../joint_batching_submission_512_20260726/README.md`；
2. 在相同 8192/K8 下，adaptive 相对 fixed-50 tokens/s
   `-0.75% ± 0.97%`，没有可分辨增量，因此当前 workload 采用简单 fixed
   50ms；
3. 下一步在本目录的自然 EOS workload 上正式随机化重复 fixed-25 /
   fixed-50 / adaptive，并改变 arrival rate；
4. 只有跨负载结果支持 adaptive 后，才进入 2048 行 held-out。

## 数据入口

- `output_variability_gate/`：raw completion 16/64/128 上限门禁；
- `chatml_eos_gate_v2/`：ChatML 256-token 门禁；
- `chatml_eos_max512_gate/`：ChatML 512-token 自然 EOS 门禁；
- `chatml_flush_gate_deterministic/`：64 请求 temperature=0 策略门禁；
- `chatml_flush_formal_512/`：512 请求随机化正式重复及 `summary_long.csv`；
- `chatml_fixed50_probe/`：单次 fixed-50ms 机制探针。
