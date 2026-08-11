# SAOR capacity-only development gate（2026-08-11）

## 结论

本轮证明了 SAOR 的最小真实执行路径可以在 PostgreSQL→Daft→Ray→未经修改的 vLLM 上安全
改变每 endpoint 的 request/work capacity，并在降档时保持已有 lease 自然排空；但**没有通过
性能晋级门**。

- 相对 frozen K128，SAOR 吞吐提高 4.36%、group duration 缩短 4.18%，低于预注册约 5%
  晋级线；Job A/B JCT 分别降低 15.06%/4.12%，但 Jain 从 0.805 降到 0.776，Job B P99
  上升 26.94%。
- 相对 frozen K160，SAOR 吞吐只提高 0.52%，group duration 只缩短 0.52%。
- 相对简单 threshold controller，SAOR 吞吐降低 1.46%、duration 增加 1.48%；Job A P99
  改善 15.45%，但 Job B JCT/P99 分别恶化 1.94%/4.79%，Jain 下降 1.25%。
- 因此 capacity-only SAOR 在该持续高压 workload 上没有可复现的额外价值。按门禁停止，
  不追加专门公平实验，也不通过继续调权重或提高负载追正。

证据等级为 **development / one repeat / not rankable as formal**。它可以支持代码可执行性、
控制动作和失败模式分析，不能支持论文性能结论或“SAOR 已完成数学验证”。

## 1. 实验目的

回答三个最小问题：

1. SAOR 是否能在不修改 vLLM 的前提下，通过 Ray 共享 credit 动态执行 K128/W131072 与
   K160/W163840 两个离线安全容量档；
2. capacity-only DPP 相对 frozen lower、frozen upper 和现有简单 threshold controller 是否
   有至少约 5% 的增量；
3. 若没有增量，失败来自正确性、控制开销、workload 形态、观测/估计还是算法目标错配。

本轮不检验完整 SAOR。真实 runner 只接入 aggregate capacity action；Job-head ordered
release、公平/SLO 虚拟债务、endpoint routing 和图像两级 stage queue 均未启用。

## 2. 实验设置

| 项目 | 冻结值 |
|---|---|
| 硬件 | 2×NVIDIA RTX 4090 24 GiB；128 CPU slots |
| 数据库 | PostgreSQL 18.4；pgvector 0.8.5 |
| 数据/Ray | Daft 0.7.21；Ray 2.56.1；Python 3.12.3 |
| 模型服务 | vLLM 0.25.1；Qwen2.5-7B；2 endpoint；未经修改 |
| vLLM 能力 | continuous batching；chunked prefill；prefix cache；max batched tokens 8192；max seqs 256；GPU memory utilization 0.9 |
| vLLM 内部策略 | 服务进程未显式传 `--scheduling-policy`，实际使用 upstream 默认 FCFS；因未显式冻结，本轮只算 development |
| 数据路径 | PostgreSQL→Daft native organizer→Ray async actor→HTTP chat completions |
| 请求语义 | 每行保持完整请求；token-budget 6144 只组织多行 cohort，不把单 prompt 按 token 拆请求 |
| workload | phase-change A=20、B=4.5 合同；Job A 4,713 行，Job B 553 行；B offset 60.335 s；output 512、ignore EOS |
| 容量 | lower K128/W131072；upper K160/W163840，均为每 endpoint |
| 重复 | 0 warm-up + 1 measured run/arm，顺序 threshold→upper→SAOR→lower |
| 主要指标 | correct prompt+generation tokens/s、group duration、per-Job JCT/P99/SLO、weighted-service Jain、actions、running/waiting/KV、GPU/MFU |

SAOR 使用 completion-service EWMA、aggregate organizer ready work 和
`0.5×waiting/160 + 0.5×KV` tail proxy；`V=1`、switch weight 0.01、8-sample dwell。
完整展开值见 `raw/expanded_control_parameters.json`。参数均由环境/JSON 注入，没有在策略代码
中写死 K、W、endpoint 数或 vLLM 阈值。

## 3. 严谨性与正确性自检

### 3.1 通过项

- runtime preflight 状态 `ok`；2 GPU、数据库、Ray/Daft/Python 依赖均通过。
- v3 manifest 状态 `completed`，4/4 arm、每臂 5,266/5,266 请求完成，0 failure、0 incident。
- 四臂 prompt/generation token delta 完全相同：1,706,742 / 2,696,192，避免用更多完成 work
  制造吞吐收益。
- 所有 arm 终态 `active_requests/work=0`、`waiting_requests/work=0`；SAOR 两 endpoint 峰值
  K 分别达到 160，证明增档真实生效。
- SAOR 产生 7 次 increase、7 次 decrease；降档不撤销已有 lease，终态验证按配置中的最大
  安全臂检查峰值，再按当前 lower arm 检查终态归零。
- 四臂 GPU utilization mean 98.997%–99.364%，资源采样状态 `ok`；结果不是上游没有喂饱造成。

### 3.2 限制与排除项

- 只有一次、且 arm 顺序未交错，不能计算 CV/置信区间，也不能排除时间漂移。
- 服务器隔离 worktree 以 commit `feb8a7f` 为 HEAD，再同步未提交代码运行；manifest 的 commit
  只记录基座，不代表完整源码。完整归档保留在服务器并记录 SHA，但本轮不升级为 formal。
- v3 manifest 的旧 `_redacted_config` 漏记 `saor_capacity_control`；config fingerprint 仍覆盖
  原配置，展开参数已从运行 shell 的冻结环境另存。该 provenance 缺口已经加回代码并加入
  回归测试，但不能倒推修复既有 manifest。
- v1 因 Ray Client 初始化失败而排除；v2 请求都成功，但终态 validator 错把 SAOR 的初始
  K128 当峰值上限，拒绝合法 K160，故排除。v3 修正为“最大配置安全臂验证峰值 + 当前臂验证
  终态”，从干净目录完整重跑。
- `service_disparity_bound_status` 明确为 unavailable；Jain 是经验指标，不是 VTC bound。
- 单次 `gpu_utilization_pct` snapshot 为 0 的字段无效；本文只使用 during-run mean/P95/max。
- 最终代码在服务器通过 69 个 SAOR/shared-vLLM 聚焦测试；排除需要自行启动本地 Ray 的
  `test_scheduling_daft_ray_contract.py` 后，其余全量为 1,127 passed + 253 subtests。该 5-test
  文件未在已有正式 Ray/vLLM 服务上强行启动第二个 local Ray，属于环境隔离边界，不是本次
  修改失败。

## 4. 实验数据

### 4.1 主要结果

| arm | tokens/s | duration (s) | Job A/B JCT (s) | Job A/B P99 (s) | Jain | actions up/down |
|---|---:|---:|---:|---:|---:|---:|
| legacy threshold | **13,458.48** | **327.15** | 298.21 / 265.03 | 58.45 / 186.60 | 0.786 | 2 / 0 |
| frozen K160 | 13,192.97 | 333.73 | 297.01 / 271.71 | 57.01 / 190.90 | 0.779 | 0 / 0 |
| SAOR | 13,261.84 | 332.00 | **291.79** / 270.17 | **49.42** / 195.54 | 0.776 | 7 / 7 |
| frozen K128 | 12,707.93 | 346.47 | 343.52 / 281.79 | 103.74 / **154.04** | **0.805** | 0 / 0 |

相对变化使用 `(SAOR / baseline - 1) × 100%`；负 duration/JCT/P99 表示更好。

| baseline | throughput | duration | Job A/B JCT | Job A/B P99 | Jain |
|---|---:|---:|---:|---:|---:|
| frozen K128 | +4.36% | −4.18% | −15.06% / −4.12% | −52.36% / **+26.94%** | −3.59% |
| frozen K160 | +0.52% | −0.52% | −1.75% / −0.57% | −13.31% / +2.43% | −0.39% |
| legacy threshold | **−1.46%** | **+1.48%** | −2.15% / +1.94% | −15.45% / +4.79% | −1.25% |

### 4.2 模型服务与资源状态

| arm | GPU mean | running mean | waiting mean / P95 / max | KV mean / P95 / max | MFU estimate | prefix hit |
|---|---:|---:|---:|---:|---:|---:|
| legacy threshold | 99.36% | 271.28 | 1.22 / 10 / 51 | 0.700 / 0.993 / 0.999 | 0.501 | 0.337 |
| frozen K160 | 99.33% | 266.53 | 2.02 / 17 / 70 | 0.690 / 0.997 / 1.000 | 0.498 | 0.345 |
| SAOR | 99.34% | 263.38 | 1.92 / 17 / 61 | 0.685 / 0.996 / 1.000 | 0.495 | 0.333 |
| frozen K128 | 99.00% | 228.89 | 0.07 / 0 / 13 | 0.603 / 0.826 / 0.940 | 0.473 | 0.320 |

K128 明显降低 engine pressure，但也降低 correct throughput；三条可达 K160 的 arm 都把 KV
P95 推到约 1。高 GPU utilization 同时出现在安全和高压力状态，进一步说明不能单看 GPU。

固定 K160 本身不是失败点。相对 K128，它仍有 +3.82% 吞吐、−3.68% duration 和两个 Job
JCT −13.54%/−3.58%；本轮也没有 OOM、request failure 或 credit leak。可测代价是 Job B P99
+23.93%、SLO violation +1.085 个百分点、Jain −3.22%，以及 waiting/KV 更接近上限。因此
K160 是强静态效率 baseline，而不是应被动态策略刻意打败的稻草人；SAOR 必须证明 Pareto
改善，不能仅以“发生过降档”晋级。

### 4.3 控制路径 microbenchmark 与 trace replay

- 服务器 32-Job SAOR decision P50/P95 为 107.257/108.701 μs，约 8,908 ops/s；相对 250 ms
  control slot 只占约 0.0435%。这只排除了 Python decision 本身是主要瓶颈，不代表 GPU E2E
  性能。
- 优化后的 scorer 预计算 aggregate fairness share，复杂度从重复求和的 O(J²) 降为 O(J)。
- 旧 phase 聚合 trace 的 paired non-causal replay 有 8 个样本、6 个可计 regret 样本，5 次匹配
  事后 oracle，累计归一化 regret 0.0141、2 次切换。它没有产生降档，支持“旧 workload 缺少
  稳定降档状态”，但不能当在线因果证据。

## 5. 为什么 SAOR 没有胜过强静态/简单阈值

### 5.1 workload 没有提供足够的动态可利用空间

这次是持续高压并带历史累积，不是 recovery gate 保证复位的 low↔high 重复。所有 arm GPU
mean 都约 99%。在这种近稳态下，若 K160 已接近最优，动态策略的上界本来就是“多数时间等同
K160”，再减去切换、估计和执行滞后成本。

SAOR 的 1,876 个 endpoint-state 样本中 1,551 个应用 K160（82.7%）；threshold 为
1,632/1,850（88.2%）。所以 SAOR 的实际行为是“多数时间 upper + 末段振荡”，不是能利用
phase 的新 operating trajectory。

### 5.2 实现只是 capacity-only，不是完整数学模型

runner 没有接 per-Job queue、fairness debt、SLO virtual queue 或 ordered release，两个 Job
仍由同一个 DRR credit coordinator 执行。DPP 控制的只是 aggregate K/W；因此不能期待理论中
Job service debt 项改善 Jain 或隔离。

### 5.3 评分量纲与观测模型错配

MaxWeight 队列项随 ready backlog 放大，而当前 tail proxy 是 0–约 1 的归一化 waiting/KV，
且 `V=1`。ready work 一旦很大，服务项压倒风险项，控制器会长期选 upper；直到 KV/等待已经
形成才降档。当前线上只更新所处 arm 的 EWMA，另一 arm 保留 prior，也没有同一状态下的
counterfactual service estimate。

因此当前实现既不是 conditional mean 已知的 exact oracle DPP，也尚未证明是
`alpha`-approximate MaxWeight。一般 bounded service-estimation error 乘上无界 backlog 后不再是
与队列无关的常数，不能直接套用 `(B+C)/V` 定理。

### 5.4 外部 action 的作用有延迟

Ray/vLLM 上游降档不会撤销已接纳请求，只阻止新 lease；continuous batching、长 decode 与 KV
working set 需要时间排空。14 次 SAOR 切换集中在约 286–330 s 的末段，此时高压力已形成，
降档无法追溯改善早期 tail；随后短时 pressure 回落又触发增档，形成有限臂振荡。

### 5.5 aggregate proxy 没有表达公平/SLO 目标

`waiting + KV` 看不到 Job B 的 P99、Ray oldest age、per-Job attained-service lag 和 phase
recovery。它改善 Job A P99，却同时恶化 Job B P99/Jain；这不是“公平与吞吐综合最优”，而是
代理目标与评价目标不一致。

## 6. 对课题的含义与不能声称的内容

可以声称：

- 模块化 SAOR core、config adapter、Ray shared-credit actuation、state/action trace 和最大安全臂
  validation 已跑通；
- K128 与可达 K160 的 arm 是不同 operating point；更高容量带来吞吐与压力/公平权衡；
- 当前 capacity-only SAOR 没超过强静态或简单 threshold，复杂动态不会自动产生收益。

不能声称：

- SAOR 已完成定理证明或数学验证；
- 完整 SAOR 已被否定；本轮没有执行 ordered release/fairness/stage queues；
- SAOR、threshold 或 K160 在统计上显著更优；只有一次且顺序固定；
- Jain 改善、VTC bound、SLO 公平或跨 workload 泛化已经成立。

## 7. 下一步与停止规则

1. 不在该持续高压合同上扫描 `V`、threshold 或更多 K；capacity-only 分支在此 workload 记为
   `not-promoted`。
2. 若继续容量验证，只使用新的独立 burst/recovery workload：进入下一 phase 前满足
   `active/waiting/backlog 清空 + KV 回到预注册基线带`，并交错 1 warm-up + 3 formal。
3. 先跑 offline oracle。若 oracle 在新 workload 相对 frozen-static 仍小于约 5% 或伴随 tail/
   fairness 退化，直接淘汰 dynamic capacity；oracle 有收益而 online 无收益，才研究估计器。
4. 完整 SAOR 另作为单因素增量接 per-Job ordered release/fairness debt，不能与 capacity、routing、
   priority 和图像 pipeline 一次性混合。
5. 图像 CPU 木桶走独立两级 broker 与 work-reduction 消融；不能用本轮文本 capacity 结果声称
   SAOR 能消除图像 decode/resize 成本。

## 原始数据与归档

- `raw/capacity_summary.csv`：本文紧凑主表。
- `raw/capacity_v3_group_runs.csv`、`raw/capacity_v3_records/`：完整 group record。
- `raw/capacity_v3_saor_states.csv`：SAOR 1,876 行 state/action trace。
- `raw/capacity_v3_manifest.json`：4/4 completed manifest；已知漏记 SAOR config 的 provenance
  缺口见 §3.2。
- `raw/capacity_v1_*`、`raw/capacity_v2_*`：两次无效运行及失败原因。
- `raw/control_benchmark_{local,server}.csv`：CPU decision microbenchmark。
- `raw/replay_rows.csv`、`raw/replay_summary.json`：非因果 paired trace replay。
- 服务器完整归档：
  `/root/autodl-tmp/experiment-artifacts/saor_capacity_development_20260811.tar.gz`，9.0 MiB，
  SHA256 `59aa4a3013a79107842f8deec4d827c105b48c0ba98c3518b05d24eb715ab134`。
