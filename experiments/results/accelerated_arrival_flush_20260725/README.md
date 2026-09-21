# Accelerated arrival flush experiment (2026-07-25)

## 1. 实验设置

本实验验证研究内容二中的独立 flush 等待策略。真实执行链为
`PostgreSQL 18.4 -> Daft 0.7.20 -> Arrow -> Ray 2.56 task -> vLLM
0.25.1 -> Qwen2.5-1.5B`，运行在单张 NVIDIA GeForce RTX 5070 12 GB
GPU 上。没有使用 fake backend。

workload 为 PostgreSQL 中的 `sharegpt_burstgpt` 前 512 条请求，按
`arrival_time` 顺序重放。原始时间跨度为 39,757 秒；实验将到达间隔乘以
`0.0005`，得到 19.8785 秒的受控加速到达窗口。该数据只能解释为加速
arrival screening，不能等同于原始生产流量强度。

共同参数：

- token budget：6,144；
- static admission：`K_max=8`；
- completion cap：16 tokens；
- flush timeout：25 ms；
- hard max wait：50 ms；
- 每种策略 1 次预热、5 次正式重复。

完整环境和参数见 `manifest.json`。

## 2. 实验设计

比较三种只改变 flush 决策的策略：

1. `immediate`：每个到达请求立即提交，作为不等待 baseline；
2. `fixed_timeout`：等待固定的 25 ms 合并窗口；
3. `queue_adaptive`：根据服务负载决定低负载立即提交或等到 50 ms
   hard max wait。

正式运行前先用每策略 64 条请求做真实链路门禁。门禁要求状态为 `ok`、
vLLM 成功增量为 64、三类轨迹非空，且提交轨迹覆盖 64 个唯一文档且无
重复。门禁通过后才运行 512 条正式矩阵。

## 3. 严谨性自检

- 18/18 次运行状态为 `ok`，其中 3 次预热不进入统计，15 次正式运行进入统计。
- 每个正式重复均覆盖 512 个唯一文档 ID，未发现缺失或重复。
- 每行原始 run 记录 PostgreSQL 和 pgvector 真实版本。
- `tokens/s` 使用 vLLM Prometheus 的
  `(prompt_tokens_delta + generation_tokens_delta) / e2e_s` 计算，不使用
  tokenizer 估计值。
- 报告均值、样本标准差和 95% t 置信区间半宽（n=5，t=2.776）。
- 第一次 queue-adaptive 预热因 7 个服务端请求在客户端 180 秒超时后仍
  卡在 running 状态而失败。该次没有写入汇总 CSV，也未进入统计。恢复时仅
  重启实验 vLLM 容器，等待 health=200、running=waiting=0 后完整重跑该策略。

限制：策略顺序没有随机化，且成功的 queue-adaptive 组之前发生过服务重启；
因此本轮适合筛选和诊断，不足以支持显著性或论文最终结论。submission trace
是 batch-level，不是 per-request e2e latency。

## 4. 实验数据

下表为 5 次正式重复的均值 ± 95% CI 半宽：

| 策略 | E2E (s) | rows/s | observed tokens/s | submissions | batch service P99 (s) |
|---|---:|---:|---:|---:|---:|
| immediate | 28.430 ± 0.243 | 18.010 ± 0.154 | 2482.5 ± 21.3 | 512.0 ± 0.0 | 0.2811 ± 0.0106 |
| fixed timeout | 28.379 ± 0.295 | 18.042 ± 0.188 | 2487.1 ± 26.0 | 466.0 ± 1.8 | 0.2838 ± 0.0066 |
| queue adaptive | 28.714 ± 0.710 | 17.837 ± 0.436 | 2458.5 ± 60.0 | 512.0 ± 0.0 | 0.2901 ± 0.0191 |

相对 immediate：

- fixed timeout：E2E -0.178%，rows/s +0.180%，tokens/s +0.185%，
  submissions -8.984%；
- queue adaptive：E2E +1.000%，rows/s -0.963%，tokens/s -0.966%，
  submissions 不变，batch service P99 +3.219%。

batch-level submission 汇总：

| 策略 | 总 submissions | 平均 batch rows | P95 batch rows | 最大 batch rows |
|---|---:|---:|---:|---:|
| immediate | 2,560 | 1.000 | 1 | 1 |
| fixed timeout | 2,330 | 1.099 | 2 | 5 |
| queue adaptive | 2,560 | 1.000 | 1 | 1 |

queue-adaptive 的 2,560 次正式 flush 中，596 次（23.281%）由
`service_underloaded` 触发，1,964 次（76.719%）由 `hard_max_wait`
触发；但两类触发最终都没有形成多行 batch。

资源轨迹显示三种策略 GPU 平均利用率约为 66.2%–67.6%，峰值为
92%–93%；vLLM waiting 峰值均为 0。该结果说明当前负载下服务端没有持续
排队，但不能单独用于证明 GPU 饱和。

### 1024 条同密度规模探针

为判断 512 条负结果是否只是规模过小，又在完全相同的 arrival scale
`0.0005` 下对 1024 条请求各运行一次。该探针不含重复，只用于行为诊断：

| 策略 | E2E (s) | rows/s | tokens/s | submissions | 平均 batch rows |
|---|---:|---:|---:|---:|---:|
| immediate | 51.014 | 20.073 | 2744.952 | 1024 | 1.000 |
| fixed timeout | 51.505 | 19.881 | 2719.149 | 985 | 1.040 |
| queue adaptive | 49.108 | 20.852 | 2851.680 | 1024 | 1.000 |

queue-adaptive 的单次吞吐数值最高，但它仍然没有形成任何多行 batch。由于这是
固定顺序的单次探针，不能把差异归因于策略；可以确认的是，把请求数从 512
增加到 1024 并未修复 batch formation。故暂不直接投入 2048 条正式重复，
先修正窗口选择和 deadline 前到达请求的吸收顺序。

## 5. 结果解释

**事实**：fixed timeout 减少约 9% 的 Ray/vLLM 提交次数，但吞吐均值只提升
约 0.18%；三种策略的 95% CI 明显重叠。

**事实**：当前 queue-adaptive 没有合并任何请求，提交次数与 immediate
完全相同，同时均值和尾延迟略差。

**推断**：现有规则在 `service_underloaded` 时立即 flush，而在其他情况只等
到 hard max wait；对于当前加速间隔分布，这一决策没有创造有效的 coalescing
窗口，因此控制复杂度没有转化为批处理收益。

**不能声称**：

- 不能声称 fixed timeout 显著优于 immediate；
- 不能声称 queue-adaptive 在所有 workload 上无效；
- 不能从单 GPU、单模型和固定 16-token 输出推广到多 endpoint、多 GPU；
- 不能把 batch service P99 当作 per-request E2E P99。

## 6. 对课题的含义

这轮结果给出了一个负结果但可操作的设计判据：queue-adaptive flush 的下一版
必须实际改变 batch formation，不能只改变 flush reason。若策略无法在不明显
增加尾延迟的前提下形成大于 1 的 batch，它就没有继续进入联合搜索的价值。

fixed timeout 可作为后续 queue-adaptive 改进的强 baseline；当前版本的
queue-adaptive 不应作为“动态策略优于静态策略”的论文证据。

## 7. 下一步

1. 将低负载动作从“立即 flush”改为有上下界的短 coalescing window，并保留
   pressure/budget 到达时立即 flush。
2. 在 64 条真实门禁中首先验证平均 batch rows > 1、exactly-once 和 P99
   guardrail；未通过不运行 512 条矩阵。
3. 通过门禁后按随机化策略顺序复验，并在每个策略组前恢复一致的服务初态。
4. 增加真正的 per-request arrival/submission/completion 时间戳，补齐
   per-request E2E P95/P99 和 SLO violation。
5. queue-adaptive 只有超过 fixed timeout 或给出明确 tail/throughput tradeoff
   后，才进入 batching × submission 联合搜索。

## 数据文件

- `gate_runs.csv`：64 条真实链路门禁；
- `formal_runs.csv`：18 条原始运行记录；
- `formal_run_metrics.csv`：15 条正式运行的绘图友好指标，补充 observed tokens/s；
- `formal_metric_summary.csv`：逐策略、逐指标统计；
- `formal_*_flush_trace.csv`：flush 决策时序；
- `formal_*_submission_trace.csv`：batch-level 提交与服务时间；
- `formal_*_resource_trace.csv`：250 ms GPU/vLLM 资源采样；
- `formal_resource_run_metrics.csv` / `formal_resource_summary.csv`：资源派生指标；
- `formal_flush_reason_summary.csv`：flush reason 分布；
- `formal_submission_summary.csv`：batch formation 汇总；
- `scale_probe_runs.csv`：1024 条同密度、每策略单次的规模诊断；
- `scale_probe_1024_*_trace.csv`：规模诊断的三类原始轨迹；
- `preinstrumentation_gate/`：缺少 submission/resource trace 的早期预备尝试，
  仅保留审计，不进入任何统计。
