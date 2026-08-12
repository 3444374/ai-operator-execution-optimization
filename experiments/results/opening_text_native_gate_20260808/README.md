# Text native framework capability gate and Ray Data calibration (2026-08-08)

## 1. 实验目的

在启动同机正式矩阵前，确认 bounded Chat control、Daft `functions.prompt`
Native/Ray 和 Ray Data `HttpRequestProcessorConfig` 四条入口能在同一份 256-row
Chat manifest、同一双 vLLM 服务上正确执行，并只对 Ray Data 做最小并发筛选。

本实验是 capability/calibration gate，不回答框架正式性能排名，也不证明项目方法、
状态感知或动态调度优于原生框架。

## 2. 实验设置

- 机器：2× NVIDIA RTX 4090；vLLM 0.25.1；Qwen2.5-7B；双 endpoint。
- 服务合同：Chat Completions，prefix cache ON，`max_num_seqs=256`，
  `max_num_batched_tokens=8192`。
- workload：256 rows，每 endpoint 128 rows；estimated work 41,942 / 41,943。
- 入口：bounded HTTP C32；Daft Native vendor default；Daft Ray vendor default；
  Ray Data batch 16、concurrency 4/8/16。
- 重复：每点一次 validity gate。该重复数只允许做能力门和冻结候选，不允许稳定性或
  正式性能排名。
- 原始服务器目录：
  - `/root/autodl-tmp/experiment-artifacts/opening_text_native_gate256_20260808/`
  - `/root/autodl-tmp/experiment-artifacts/opening_text_native_raydata_c8_gate256_20260808/`
  - `/root/autodl-tmp/experiment-artifacts/opening_text_native_raydata_c16_gate256_20260808/`
- 服务器静态归档：
  `/root/autodl-tmp/experiment-artifacts/archives/opening_text_native_scans_20260808.tar.gz`，
  SHA256 `0bfe22fec0f477a70e805b0237efdf09b6cc7d4d80640ddd2ba8e5d4a7c8c7e7`。
- 仓库汇总：[`capability_and_raydata_calibration.csv`](capability_and_raydata_calibration.csv)。

## 3. 严谨性自检

所有 6 个 gate 均满足：256/256 rows、0 failed、exactly-once、两个 endpoint 均使用、
服务 token counter 一致、末态 running/waiting 为 0，且 manifest estimated-work skew
为 `2.384e-5`。

work skew 使用代码口径 `(max(work)-min(work))/max(work)`，实现见
`code/src/baselines/common/manifests.py:207-223` 的 `partition_summary`；不是除以总 work。

provenance 审计结果：Daft Native、Daft Ray 和 Ray Data 均为
`framework_native_baseline`、`custom_scheduling_code=false`；scheduler owner 分别为
`daft_native_runner`、`daft_ray_runner` 和 `ray_data`。bounded HTTP 是
`direct_client_control`，由项目 asyncio control 驱动，因此只作协议容量 control，
不是 framework-native baseline。

## 4. 实验数据

| 入口 | 配置 | group wall (s) | service total tok/s | rows | failed | 计时粒度 |
|---|---:|---:|---:|---:|---:|---|
| bounded HTTP | C32, B1 | 20.236 | 4,961.10 | 256 | 0 | request |
| Daft Native | vendor default | 8.497 | 11,782.73 | 256 | 0 | shard barrier |
| Daft Ray | vendor default | 10.933 | 9,161.65 | 256 | 0 | shard barrier |
| Ray Data | C4, B16 | 126.816 | 789.64 | 256 | 0 | shard barrier |
| Ray Data | C8, B16 | 123.038 | 813.10 | 256 | 0 | shard barrier |
| Ray Data | C16, B16 | 131.177 | 764.28 | 256 | 0 | shard barrier |

Ray Data C8 是三个已测试点中的单次 measured peak；C4、C16 相对 C8 分别为
−2.89%、−6.00%。因此正式矩阵冻结 C8/B16，但这里只称“筛选点”，不称稳定最优或
最小饱和点。

本 gate 的 bounded 4,961 tok/s 与后续 2,048-row formal 的 17,800 tok/s **不可直接
横比**：前者是 256-row 短 gate 的 request 粒度并包含 client 启动效应，后者是 ≥60 s
正式任务的 shard-barrier/服务容量口径。该 3.6× 差异不是服务容量变化，也不能用于跨报告
推导加速比。

## 5. 结果解释

### 事实

- 三个原生框架入口都能在不注入项目 scheduler/credit/router 的条件下执行同一 Chat
  manifest，并保留 exactly-once、provenance 和 service-counter 证据。
- Ray Data 在 C4→C8 有小幅单次改善，C16 回退；继续扩大扫描没有开题价值。

### 推断

- capability gate 已消除“入口不可运行”这一 fatal flaw，允许进入 2048-row、
  1 warmup + 3 formal 的同机矩阵。
- Ray Data 当前点可能有框架级启动、批处理或 HTTP processor 开销；原因需要正式矩阵和
  原生 trace 才能判断，不能由本 gate 归因。

### 不能声称

- 不能按本表宣称 Daft 比 Ray Data 快多少；各原生入口只有一次短 gate，且只有 Ray Data
  超过 60 秒。
- 不能把本 gate 的 bounded 4,961 tok/s 与后续 formal 的 17,800 tok/s 当同一计时口径。
- 不能把 bounded HTTP 当 vendor-native baseline。
- 不能由 capability gate 推出 Work Unit、状态感知或动态调度已经带来性能收益。

## 6. 对开题的含义

该 gate 只为后续“现有原生框架单 job 观察”和“原生多 job 错峰观察”提供可执行、可审计
入口。开题动机仍由固定行隐藏 work、运行状态差异、多 job 错峰和代价选择 regret 等
独立证据支撑。

## 7. 后续图表规划（暂不绘制）

- 本 gate 不单独画主图；最多在附录放一张 provenance/capability 紧凑表。
- 单 job 正式矩阵完成后再画 workload 固定下的 service throughput/JCT 分面图，并把
  timing granularity 明示在图注中。
- 多 job 完成后画三阶段时间线（独占窗、重叠窗、drain），而不是把本 gate 数字混入
  多 job 因果图。
