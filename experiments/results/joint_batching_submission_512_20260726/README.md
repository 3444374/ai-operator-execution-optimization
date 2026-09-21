# Batching 与提交控制联合实验（2026-07-26）

## 1. 实验设置

本实验回答：数据组织参数与提交控制参数是否必须联合搜索，还是分别搜索后
拼接即可。

真实链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft 0.7.20 → Arrow →
Ray 2.56 task → vLLM 0.25.1 / Qwen2.5-1.5B → RTX 5070 12GB。vLLM
开启 MFU 指标、关闭 prefix cache；没有使用 fake backend。

共同设置：

- `sharegpt_burstgpt`，按 `arrival_time_s` 排序，512 个请求；
- sequential token-budget batching；
- token budget `{4096, 6144, 8192}`；
- `K_max={4, 8, 16}`；
- flush 为 fixed 25ms 或 queue-adaptive 25/50ms；
- accelerated replay `scale=0.0005`；
- raw completion，max output 16，temperature 0；
- 10 秒 request SLO，无写回；
- 每个请求记录 endpoint 返回的实际 output token 数与 finish reason。

## 2. 实验设计

### 第一阶段：18 单元筛选

运行 `3 token budgets × 3 K_max × 2 flush policies`，顺序由固定 seed
随机化。每个单元运行一次，用于筛选而非显著性结论。

选择规则为：

1. correctness 与 exactly-once 必须通过；
2. request SLO violation 必须不超过 1%；
3. 在合格配置中优先 tokens/s，再检查 goodput、能耗和 MFU。

“独立搜索”采用明确的锚点：

- 数据组织：固定 `K_max=8 + fixed 25ms`，只选择 token budget；
- 提交控制：固定 token budget 6144，选择 K_max 与 flush；
- 将两个独立选择拼接为一个实际配置。

“联合搜索”直接在全部 18 个单元中选择。

### 第二阶段：候选随机化重复

对以下四项各运行 1 次 warm-up + 3 次 formal，formal 顺序按 repeat
随机化：

- 基线：6144 / K8 / fixed 25ms；
- 独立拼接：6144 / K8 / adaptive 25/50ms；
- 联合筛选最优：8192 / K8 / adaptive 25/50ms；
- 机制对照：8192 / K8 / fixed 50ms。

## 3. 严谨性自检

- 第一阶段 18/18 成功，manifest 无 incident；
- 第二阶段 16/16 成功，manifest 无 incident；
- 12/12 formal run 均为 512 个唯一请求，exactly-once；
- 每次运行均有 request/submission/flush/resource trace；
- tokens/s 来自 vLLM prompt + generation token counter delta；
- MFU 使用开启 `--enable-mfu-metrics` 后的正 FLOP delta；
- 配置顺序随机化，warm-up 不进入正式统计；
- 95% 区间为 n=3 的 t 区间，P99 波动很大，只作边界检查。

## 4. 实验数据

### 4.1 单次筛选

吞吐最高的 K16 配置均未通过 SLO 门槛：

| 配置 | tokens/s | request P99 (s) | SLO violation |
|---|---:|---:|---:|
| 6144 / K16 / adaptive | 3267.625 | 11.607 | 2.148% |
| 8192 / K16 / adaptive | 3264.835 | 12.164 | 2.539% |
| 4096 / K16 / adaptive | 3250.131 | 11.545 | 2.734% |

通过门槛后的独立与联合选择：

| 选择 | 配置 | tokens/s | request P99 (s) | SLO violation |
|---|---|---:|---:|---:|
| 数据组织独立最优 | 6144 / K8 / fixed 25ms | 3057.037 | 7.173 | 0 |
| 提交控制独立最优 | 6144 / K8 / adaptive | 3164.873 | 7.796 | 0 |
| 独立拼接 | 6144 / K8 / adaptive | 3164.873 | 7.796 | 0 |
| 联合筛选最优 | 8192 / K8 / adaptive | 3175.568 | 6.193 | 0 |

联合筛选比独立拼接仅高 0.338%，需要重复验证。

### 4.2 候选重复

均值 ± 95% t 置信区间半宽，n=3：

| 配置 | E2E (s) | tokens/s | request P99 (s) | submissions | energy/1k tokens (J) | MFU |
|---|---:|---:|---:|---:|---:|---:|
| 6144 / K8 / fixed 25ms | 23.456 ± 0.304 | 3009.000 ± 38.953 | 7.554 ± 4.745 | 200.0 | 28.322 ± 0.962 | 0.14294 ± 0.00141 |
| 6144 / K8 / adaptive | 22.391 ± 0.264 | 3152.097 ± 37.088 | 7.061 ± 2.945 | 154.7 | 27.299 ± 1.116 | 0.14976 ± 0.00193 |
| 8192 / K8 / adaptive | 22.450 ± 0.248 | 3143.894 ± 34.805 | 6.587 ± 3.492 | 154.3 | 26.987 ± 0.194 | 0.14937 ± 0.00221 |
| 8192 / K8 / fixed 50ms | 22.281 ± 0.057 | 3167.579 ± 8.074 | 7.535 ± 3.354 | 135.0 | 26.436 ± 0.947 | 0.15011 ± 0.00024 |

配对相对变化：

| 对比 | tokens/s | E2E | 说明 |
|---|---:|---:|---|
| 独立拼接 vs fixed-25 baseline | +4.758% ± 2.293% | -4.538% ± 2.082% | 三轮均为正向 |
| 联合候选 vs fixed-25 baseline | +4.485% ± 1.999% | -4.289% ± 1.840% | 三轮均为正向 |
| 联合候选 vs 独立拼接 | -0.258% ± 2.071% | +0.264% ± 2.075% | 无可分辨增量 |
| adaptive 8192 vs fixed-50 8192 | -0.748% ± 0.973% | +0.754% ± 0.986% | fixed-50 略优但区间跨零 |

独立拼接三轮中有一轮出现 2/512 请求超过 10 秒，三轮总体 violation
为 0.130%；其余候选正式运行均为 0。

## 5. 结果解释

**事实**：K16 带来最高单次吞吐，但所有 K16 单元均出现 1.76%–3.13%
SLO violation，因此不应只按吞吐选它。

**事实**：独立拼接与联合候选在重复实验中 tokens/s 相差 -0.258%，95%
区间跨零；能耗与 MFU 同样没有可分辨差异。

**事实**：adaptive 相对 fixed 25ms 稳定减少约 22.7% submissions 并提高
约 4.8% tokens/s。

**事实**：相同 8192 / K8 下，fixed 50ms 的均值略优于 adaptive，而且只产生
135 次 submissions，少于 adaptive 的 154.3 次。

**推断**：在当前固定 workload 上，主要收益仍来自采用更长的 50ms
coalescing window，而非动态切换本身。

**不能声称**：

- 不能声称联合调优显著优于分层独立调优；
- 不能声称 adaptive 优于最佳静态 timeout；
- 不能把 16-token capped output 结果推广到自然 EOS；
- 不能推广到多 GPU、多 endpoint、其他模型或生产到达率。

## 6. 对研究内容的含义

本地单 GPU 当前证据支持分层优化：

1. 数据组织先选择满足 batch token-tail 与 SLO 的 budget；
2. admission 使用静态 `K_max=8` guardrail；
3. flush window 在当前 workload 可采用简单 fixed 50ms；
4. adaptive 保留为跨 workload 自动选择候选，但不作为默认性能结论。

联合搜索仍有方法学价值：它暴露了 K16 的吞吐/SLO 冲突，并验证了独立拼接
没有错过明显更优的局部组合。但当前数据不支持为 0.3% 的筛选差异引入更复杂
的联合控制器。

## 7. 下一步

1. 默认配置保持 sequential token-budget + static K8；
2. 在线 accelerated-replay 的当前候选采用 fixed 50ms；
3. 在自然 EOS workload 上正式重复 fixed 25 / fixed 50 / adaptive；
4. 若不同 arrival rate 的最佳静态窗口发生变化，再验证 adaptive 是否能跨负载
   自动接近各自最优；
5. 文本两项策略结论稳定后，再进入同一策略接口的图像 workload 泛化实验。

## 数据入口

- `screen_config.json`、`screen/`：18 单元筛选、原始 trace 与
  `summary_long.csv`；
- `candidate_repeat_config.json`、`candidate_repeat/`：4 个候选的随机化
  重复、原始 trace 与 `summary_long.csv`。
