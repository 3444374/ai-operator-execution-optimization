# AI_EMBED vs AI_COMPLETE：观察变量选择方法论

Date: 2026-07-20

本文解释为什么从 AI_EMBED 转向 AI_COMPLETE 后，实验观察变量需要从"阶段时延拆分"转向"请求形状 + 服务端压力 + 端到端分布"的多维指标集。

## 1. 为什么 AI_EMBED 测"时延"有意义

AI_EMBED（text → 384-dim vector）的核心特征：**每行计算量几乎相等**。

```text
row_1: "hello world" → embedding → 384-dim vector  (≈ 相同计算量)
row_2: "some text here" → embedding → 384-dim vector  (≈ 相同计算量)
row_N: "another sentence" → embedding → 384-dim vector  (≈ 相同计算量)
```

在这种 workload 下：
- "一行"就是一个有意义的、可比较的工作单位
- `rows/s` 直接反映系统效率
- 按阶段拆分的 wall time（DB fetch / Arrow build / Ray submit / model inference / fan-in / writeback）可以回答"时间花在哪"
- `per-row latency` 可以跨行比较

所以 AI_EMBED 时期的实验设计——拆分阶段耗时、对比 fine vs coalesced、分析 fan-in 占比——是完全合理的。

## 2. AI_COMPLETE 的根本差异

AI_COMPLETE（LLM 文本生成）的核心特征：**每行计算量差异悬殊**。

```text
row_1: 38 tokens prompt → LLM generate 16 tokens →  (轻量)
row_2: 4663 tokens prompt → LLM generate 16 tokens → (重量，~122× row_1)
```

一个 batch=8 的请求，token 量范围可以是 335~4663（跨度 13.9×）。

此时：
- **"一行"不再是可比较的单位**——处理 row_1 和处理 row_2 的代价完全不同
- `rows/s` 仍然有用（反映整体吞吐），但**不能单独用来解释"为什么快或慢"**
- 阶段拆分仍然可以做，但主要时间在模型推理，DB fetch 和 Daft organize 占比很小
- **需要把"行数"和"计算量"拆开观察**

## 3. AI_COMPLETE 需要的分层变量体系

### 第一层：请求形状变量（数据组织策略的核心）

回答"你发给模型服务的请求是什么样的"：

| 变量 | 含义 | 对应研究问题 |
|---|---|---|
| `batch_tokens_p50/p95/p99/max` | 每个请求的 token 量分布 | token-budget 是否真的约束了 tail？ |
| `batch_prompt_token_spread` | 同一 batch 内 prompt token 的 min-max 差距 | length-align 是否减少了 batch 内不平衡？ |
| `mean_rows_per_request` | 每个请求包含几行 | 不同 budget 对应不同请求粒度 |
| `model_calls` | 总共发了多少次 HTTP 请求 | 策略的核心 tradeoff：少发大请求 vs 多发小请求 |
| **`tokens/s`** ⚠️ | 每秒处理多少 token | **比 rows/s 更公平的效率指标**——归一化了不同行的计算量差异 |

### 第二层：服务端压力变量（提交控制策略的核心）

回答"模型服务正在承受多大压力"：

| 变量 | 含义 | 对应研究问题 |
|---|---|---|
| `vllm_request_queue_time_mean_s` | vLLM 内部请求排队时间 | K_max 是否把等待从客户端转移到了服务端？ |
| `service_p50/p95/p99` | 模型服务响应时间分布 | batch 大小对服务延迟的非线性影响 |
| `bounded_wait_s` | 客户端侧等待 inflight slot 的时间 | K_max 的直接效果 |
| **inflight/queue 时间序列** ⚠️ | running/waiting requests 随时间变化 | 诊断自适应策略的关键：overshoot、恢复、稳态 |
| `vllm_prefill_time` vs `decode_time` | prefill 和 decode 各自的时间占比 | 大 batch → prefill 压力大；长生成 → decode 压力大 |

### 第三层：端到端质量变量

回答"用户（数据库查询）实际感受到了什么"：

| 变量 | 含义 | 对应研究问题 |
|---|---|---|
| `e2e_s` / `rows/s` / `tokens/s` | 整体效率 | 策略对比 |
| **per-request e2e latency 分布** ⚠️ | 每个请求的端到端延迟分布 | batch-level P95 掩盖了 batch 内部单个请求的真实延迟 |

### 第四层：控制器行为变量（adaptive 策略专有）

回答"adaptive 控制器到底在做什么"：

| 变量 | 含义 | 对应研究问题 |
|---|---|---|
| `adaptive_upshifts/downshifts` | 调高/调低次数 | 控制器活跃度 |
| `adaptive_limit_mean` | 实际生效的平均 K_max | 稳态行为 |
| **K_max 时间序列** ⚠️ | K_max 值随时间变化 | **最关键缺失**——没有时间序列无法判断 downshift 发生在初始 overshoot 后还是稳态微调中 |

⚠️ = 当前未采集或未系统化使用的指标。

## 4. 从"测时延"到"测分布"的思维转变

AI_EMBED 的逻辑：
```
行数固定 → 每行计算量固定 → 测阶段时延 → 找瓶颈在哪个阶段
```

AI_COMPLETE 的逻辑：
```
行数固定 → 每行计算量差异大 → 测 token 分布 + 服务端压力分布 + 延迟分布 → 判断策略是否改善了"形状"和"节奏"
```

核心转变：**从"时间花在哪"（阶段拆分）转向"请求长什么样 + 服务端压力有多大 + 延迟分布如何"（多维分布表征）**。

阶段拆分在 AI_COMPLETE 中仍然可以做，但主要价值不再是找瓶颈（瓶颈几乎一定是模型推理），而是验证**优化没有在非目标阶段引入新开销**（例如 token-budget 是否增加了 Daft organize 时间）。

## 5. 每个实验的最低推荐变量集

所有 AI_COMPLETE 实验共同：
- `tokens/s`、`rows/s`、`e2e_s`
- `batch_tokens_p50/p95/max`、`model_calls`
- `service_p50/p95/p99`、`vllm_request_queue_time_mean_s`
- `max_inflight_seen`

K_max / adaptive 实验追加：
- inflight/queue 时间序列
- K_max 时间序列（adaptive 模式）
- `bounded_wait_s`

分组策略（length-align / prefix-aware）实验追加：
- `batch_prompt_token_spread`
- per-request e2e latency 分布

Shared-vLLM 实验追加：
- 两个 job 各自的 `tokens/s`
- foreground per-request P99 latency
