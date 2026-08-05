# DuckDB `ai` 文本算子语义与能力门禁（2026-08-05）

> 结论类型：可行性与语义负结果。这里没有正式性能排名，也不能用本目录中的
> `tokens/s` 与项目、Daft/Ray 或 vLLM 上限比较。

## 1. 实验设置

- 硬件：AutoDL，2×RTX 4090；两个独立 vLLM endpoint。
- 模型与协议：Qwen2.5-7B，Chat Completions。
- 服务配置：prefix cache 开启，`max_num_seqs=256`，
  `max_num_batched_tokens=8192`。
- 数据库算子：DuckDB v1.5.4 + community `ai` v0.4.14，调用
  `ai_try_complete`；调度归 DuckDB SQL executor 和 `ai` 扩展所有，没有注入项目
  credit、router 或 actor pool。
- 扩展控制：response cache 关闭、provider prompt-cache hints 关闭、retry=0、
  request interval=0、timeout=120s、max concurrent requests=32。
- 原始 prompt 未入库；`raw/` 只保存配置、命令、退出状态、服务计数、逐分片摘要和
  失败日志，故障排查所需证据可复核而不重复保存 workload 内容。

## 2. 实验设计

本轮只回答两个问题：

1. DuckDB `ai` 能否在当前双 endpoint 环境执行真实 Chat Completion，并被 runner
   记录为 exactly-once、零行级错误？
2. DuckDB `ai` 对 `finish_reason=length` 的语义是否与现有 ShareGPT fixed-cap 主轨
   兼容？

依次执行：

| 门禁 | 行数 | `max_tokens` | 目的 |
|---|---:|---:|---|
| `duckdb_ai_semantic_gate_64_20260805_v3` | 64 | 256 | 检查现有主轨输出上限的行级语义 |
| `duckdb_ai_capability_gate_4_cap1024_20260805` | 4 | 1024 | 用极小样本确认扩展、双 endpoint 和观测链路可运行 |
| `duckdb_ai_semantic_gate_64_cap1024_20260805` | 64 | 1024 | 检查提高上限后能否消除 ShareGPT 语义失败 |

## 3. 严谨性自检

- adapter 同时读取 `ai_try_complete` 的 `response` 和 `error`；error 或无解释的
  NULL response 均 fail-closed，不把失败行计作 completed。
- runner 在 cell 前等待 endpoint 空闲，采集前后 vLLM token/prefix-cache counters，
  并核对运行中服务的 cache、sequence 与 batched-token 配置。
- 4 行门禁不是稳定性能实验；其工作量偏斜阈值也不适合这么小的样本。
- 64 行测试没有 warmup + 交错三重复，因此即便语义通过，也不能成为 formal 数字。
- DuckDB 扩展不暴露逐行 output-token accounting；摘要中的
  `token_accounting=unavailable` 和 `output_tokens=0` 不能解释为模型没有生成 token，
  应以服务 counter delta 判断实际生成工作。

## 4. 实验数据

| 门禁 | 分片结果 | 行级结果 | gate 结果 | 可直接观察的事实 |
|---|---|---|---|---|
| 64 行，cap=256 | 两分片均 exit 2 | 分别 21/32、22/32 行失败 | failed | 错误均为 provider response 达到 `max_tokens` |
| 4 行，cap=1024 | 两分片均执行完成 | 4/4 completed，0 failed，exactly-once | failed | 唯一 incident 是 4 行分配造成的 endpoint work skew=0.1357，高于正式阈值 0.02 |
| 64 行，cap=1024 | endpoint 0：32/32；endpoint 1：31/32 | 1 行达到 `max_tokens` | failed | 增大 cap 仍未使 ShareGPT fixed-cap 轨达到零错误 |

4 行 capability run 同时记录到：

- service prompt/generation/total token delta：319 / 1287 / 1606；
- prefix-cache query/hit delta：endpoint 0 为 119/112，endpoint 1 为 200/128；
- DuckDB、扩展版本和来源均成功写入逐分片摘要；
- 服务端生成计数非零，证明不是空响应或纯客户端假运行。

这些数据来自 `raw/*/run_status.json`、`raw/*/duckdb_ai/gate.json`、
`service_counters.json` 和逐分片 `summary.json`，没有根据对话估算。

## 5. 结果解释

### 事实

- 当前环境中的 DuckDB community `ai` 扩展能够调用两个 vLLM endpoint；4 行能力请求
  真实生成了 token，且 4/4 exactly-once。
- 对本轮 ShareGPT prompts，DuckDB `ai_try_complete` 把达到 `max_tokens` 作为行级
  error；cap=256 大量触发，cap=1024 仍有 1/64 触发。

### 推断

- 现有 ShareGPT fixed-cap 主轨接受截断文本，而 DuckDB 扩展返回行级 error，二者的
  完成语义不同。继续在同一表中排名会把语义差异误当性能差异。
- 单纯继续提高 cap 会同时改变工作量和服务成本，属于事后调参，不能修复比较口径。

### 待确认

- 需要预注册一个所有 comparator 都能零错误完成的 bounded-output manifest，才能确认
  DuckDB 的原生并发拐点与正式吞吐。
- DuckDB 扩展内部逐请求延迟、output token 和调度队列仍不可见；正式报告只能使用共同
  可观测的服务 counters、JCT 和正确性门禁。

### 不能声称

- 不能用 4 行 run 的 183.54 service tokens/s 排名；该数受冷启动、样本偏斜和极小规模影响。
- 不能声称 DuckDB 比项目慢或快，也不能声称 vLLM prefix cache 带来某一幅度收益。
- 不能把本结果外推为 DuckDB `ai` 普遍无法运行；被否决的是当前 ShareGPT fixed-cap
  正式比较语义。

## 6. 对课题的含义

DuckDB `ai` 仍是有价值的数据库产品原生 baseline，但应进入单独的
**bounded-output product track**。默认 ShareGPT 主门禁继续保留语义兼容的 core arms；
DuckDB 只有在独立 bounded-output 门禁通过后，才进入校准与正式实验。这样既保留权威
产品对照，也不通过隐藏失败或放宽 cap 制造虚假的吞吐数字。

## 7. 下一步

1. 构造并冻结 bounded-output manifest；同轨所有 comparator 使用完全相同的 prompt、
   model、protocol、output cap、cache-on 服务和计时边界。
2. 先运行独立 capability gate，要求零行级错误、exactly-once、服务身份一致。
3. 门禁通过后，仅校准 DuckDB 原生 `max_concurrent_requests`（8/16/32/64）；不加入项目调度。
4. 只有 1 warmup + 3 formal repeats、稳定性与语义门禁全部通过，才生成正式排名。
