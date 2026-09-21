# Project 单 short all-at-t0 计时对齐诊断（2026-08-09）

## 1. 实验目的

本诊断回答：Project single-short 的 71.24 s 是否意味着其执行路径比 Daft Native 的
11.06 s 慢约 6.4 倍。唯一实验变量是输入可见性：关闭 66.875 s 的逐请求 arrival
span，让同一 512-row short manifest 在 Job 开始时全部可见；Project 的 K128/W65,536、
token budget 6144、8×32 actor、双 endpoint 和服务配置保持不变。

该 short cell 用于定位时间与供给问题，不要求达到 60 s，不作稳态容量排名。

## 2. 实验设置

- 2×RTX 4090；两个 Qwen2.5-7B vLLM endpoint；prefix cache、chunked prefill 和
  MFU metrics 开启；`max_num_batched_tokens=8192`、`max_num_seqs=256`。
- 输入为既有 short manifest，512 行、每 endpoint 256 行，SHA256
  `85b3f90c...c971`；`source-order=doc_id`，不启用 arrival replay。
- 1 warm-up + 3 formal；无 writeback，以模型响应 gather 为终点。
- 运行代码 commit `282bf71`；服务重启后先完成 endpoint、PostgreSQL、Ray 和 runtime
  preflight，再执行 64-row correctness gate 和完整矩阵。

## 3. 严谨性自检

- 首个 64-row gate v1 因 eager 模式仍沿用 `source-order=arrival_time` 而 fail closed，
  未发出请求；修正为 `doc_id` 后 v2 通过。两份失败/成功证据均保留。
- 三次 formal 均为 512 requests、512 unique doc IDs、512 submissions，两个 endpoint
  各 256；vLLM success counter 均为 512，resource/MFU 状态均为 `ok`。
- E2E、model-request tokens/s、MFU 的 sample CV 分别为 0.552%、0.110%、0.063%。
- 原始目录和压缩包均保留在服务器；压缩包 SHA256 为
  `21280849...6944`，本地回读副本校验一致。

## 4. 统一计时标准

后续 short/multi-job 数据统一使用五层计时，不再把不同层级混成一个 `wall_s`：

| 层级 | 起止边界 | 当前能否跨 Project/Daft 比较 |
|---|---|---|
| T0 full-pipeline wall | source/framework 准备前 → 输出完成（有 sink 时含 sink） | **不能**；Daft 未记录准备前的外层起点 |
| T1 offered-work JCT | 第一条请求对被测路径可见 → 最后一条完成 | 只在 offered-arrival trace 完全相同时比较 |
| T2 framework execute | 声明的 execute/collect 起点 → 框架结果 materialize | 仅诊断；Project operator 与 Daft collect 的准备归属不同 |
| T3 model-request window | 最早模型请求提交 → 最晚模型响应完成 | 可作同 manifest short-job 诊断 |
| T4 vLLM request service mean | vLLM 同一 counter 边界 | 可作同服务诊断，需与 work rate/MFU 联读 |

权威机器可读定义见 `data/timing_contract.csv`，每个 run 的映射见
`data/timing_alignment.csv`。缺失值保持为空，不使用文件 mtime 或 Job barrier 反推。

## 5. 实验数据

### 5.1 Project all-at-t0 三次 formal

| repeat | T0 profiler E2E (s) | T1 request JCT (s) | T3 submit→completion (s) | model tokens/s | MFU | GPU util mean |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 14.895 | 14.720 | 11.323 | 14,378.9 | 42.96% | 75.85% |
| 2 | 15.051 | 14.870 | 11.379 | 14,348.6 | 42.92% | 75.83% |
| 3 | 14.925 | 14.739 | 11.359 | 14,356.2 | 42.91% | 75.61% |
| mean | **14.957** | **14.776** | **11.354** | **14,361.2** | **42.93%** | **75.76%** |

服务状态均值：running 155.36、P95 253.33、max 256，waiting 0；KV mean/max 为
16.17%/36.79%。request E2E P50/P95/P99 为 9.580/14.733/14.775 s。vLLM 内部
request mean 为4.652 s，其中 queue 46 µs、inference 4.631 s、prefill 0.050 s、
decode 4.581 s。

`source_fetch_s=2.916`、`actor_ready_s=4.300`、`operator_wall_s=11.359`；这些字段
存在包含/重叠关系，不能相加。

### 5.2 与 arrival-replay Project 的同路径反事实

| 指标 | arrival replay | all-at-t0 | 变化 |
|---|---:|---:|---:|
| T0 profiler E2E | 79.394 s | 14.957 s | −81.16% |
| operator wall | 71.245 s | 11.359 s | −84.06% |
| model service tokens/s | 2,218.3 | 14,361.2 | **6.47×** |
| MFU | 6.63% | 42.93% | **6.47×** |
| running mean | 26.10 | 155.36 | 5.95× |

该反事实只改变输入可见性，直接确认 71.24 s 的主要原因是冻结的在线 arrival replay，
而不是 K/W 或 Project 模型请求路径无法利用 GPU。

### 5.3 与 Daft Native 的公平边界

| 指标 | Daft Native | Project all-at-t0 | Project 变化 | 结论资格 |
|---|---:|---:|---:|---|
| T0 full-pipeline wall | 未采集 | 14.957 s | — | 不可排名 |
| T2 framework execute/collect | 11.059 s | 11.359 s | +2.71% | 近似边界，仅诊断 |
| **T3 model-request window** | **11.059 s** | **11.354 s** | **+2.67%** | 同 manifest short 诊断可比 |
| model service tokens/s | 14,726.9 | 14,361.2 | −2.48% | 同服务表征 |
| MFU | 44.04% | 42.93% | −2.52% | 同口径表征 |
| vLLM request mean | 6.654 s | 4.652 s | −30.09% | 单请求服务表征 |

Daft 的三次 T3 为10.640/11.533/11.004 s。Project 的完整 T0 比 Daft 已记录窗口多
3.898 s，但其中3.599 s 是 Project T0 到 operator 起点之间的可见时间；这个 92.3%
只是计时层级的算术分解，不能写成 Daft 一定没有同类准备成本。

## 6. 结果解释

### 事实

1. Project 在完整输入可见时，T3、service tokens/s 与 MFU 均只比 Daft Native 相差约
   2.5%–2.7%，没有“模型请求路径慢 6.4×”的证据。
2. Project all-at-t0 的 running P95 接近 256、GPU P95/max 为100%，短 Job 已经进入高
   active-work 状态；mean GPU 75.76%包含有限 Job 的 ramp/drain，不要求人为拉长到60 s。
3. Project 的 T0 完整路径仍为14.957 s，但 Daft 缺匹配的 T0，所以不能把 +35.25%
   全部归因于 Project 框架开销。

### 推断

- 原 71.24 s 与 11.06 s 的巨大差异主要来自 input visibility 和 timer placement；若研究
  offline/eager short 性能，应使用本诊断的 T3/T4，而不是 replay JCT。
- 现在没有依据扫描 K256/K512 或重写 scheduler 来修复 single-short；先保持冻结静态点。

### 不能声称

- 不能把本 short cell 当作 ≥60 s 稳态容量、database-E2E 或框架正式排名。
- 不能说 Project 的完整 E2E 已与 Daft 持平；Daft T0 未采集。
- 不能用 all-at-t0 数据替换在线 arrival-replay 多 Job 结论；二者回答不同 offered-load
  问题。

## 7. 对多 Job 与后续数据的处理

现有多 Job 的系统内 `single→short+long` 反事实仍有效，因为每个系统内部的输入与 timer
保持一致。为了公平回答 eager 条件下 Project short 受 long 的影响，将只补一个
Project-only 配对矩阵：同一 runner 下的 eager single、eager static+long、eager
shared+long；原生 Daft/Ray Data 数据复用，不重跑。

暂不画图。后续若需要图，只画两项：

1. T0–T4 计时边界/缺失值示意，防止再次混用 14.957、11.354 和11.059 s；
2. replay→all-at-t0 的同路径吞吐/MFU变化，以及 T3 下 Project/Daft 的小差异。

完整数值在 `data/formal_runs.csv`、`data/summary.csv`、`data/comparisons.csv`；审计合同在
`data/audit.json`。
