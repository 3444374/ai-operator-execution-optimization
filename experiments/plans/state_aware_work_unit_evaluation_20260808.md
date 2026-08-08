# Work Unit、状态感知、动态调度与代价估计的实验计划

日期：2026-08-08

算子代价估计是下述 organization 与 admission/routing/multi-job 的共同使能部件：
同一 calibration signature 下输出 stage/service/remaining work、SLO slack 与预测区间。
它不单列为第三项研究内容，但必须先通过 work estimate/decision gate，才允许驱动两项策略。

## 1. 四条同等严格的可证伪链路

1. **Work Unit**：现有 rows/images/batch 是否无法表示可比 AI work？分阶段 work、locality、job/SLO、uncertainty 字段分别由哪个实验现象导出？
2. **状态感知**：相同静态上限下，stage/service/job 状态是否随到达、work mix 和 job 存活集变化？哪些信号必须是 fresh 且绑定 calibration signature？
3. **动态调度**：静态分区或独立 job 并发何时出现空闲份额、全局压力叠加、tail 或公平性问题？有界准入、路由和 shared work credit 是否在同最大 K/work 下超过 frozen-static？
4. **算子代价估计**：简单均值、解析 proxy 或 lookup 是否能稳定选对 organization/active-work/routing 候选？解析 + profile + residual 是否降低 ranking/regret？

四条链路均要记录“已证明 / 条件性支持 / 待验证 / 不能声称”。文本与图像是外部有效性轴，不把 token 机械改名为 frame。

## 2. 成功标准

- correctness、exactly-once、任务质量、feeding-saturation、稳定性与资源门禁全部通过；
- 相对同资源、同上限 frozen-static，correct throughput、SLO goodput、P99/JCT 或 fairness 至少一项改善约 5%，且其他关键指标和 failure 不出现不可接受退化；
- steady control 不要求动态胜出，但控制开销和回退不得造成约 5% 以上退化；
- 若所有目标指标均未改善，记录动态控制失效边界，不更换弱 baseline。

## 3. 公共实验合同

- immutable workload manifest、相同 source、模型/processor revision、输出语义、随机种子和服务 flags；若 headline 是 database-E2E，再要求相同 sink；
- 1 warm-up + 3 个交错 formal，失败 run 结构化保留；
- 离线容量校准和正式运行使用相同 calibration signature；
- baseline 由对应框架拥有 batching/backpressure/scheduling；项目只做 source、必要的 sink、质量与指标适配；
- project static 与 project dynamic 使用同一最大 request credit、stage work credit、actor/GPU/CPU 资源和 buffer bytes 上限；
- 全部指标使用 during-run time series 聚合，不使用单点 idle snapshot。

## 4. 因子分解

### 4.1 Work estimate gate

| 模态 | 对照 | 候选 | 指标 |
|---|---|---|---|
| 文本 | rows、input tokens | input+cap、input+predicted output、calibrated service work | rank、MAPE、pairwise、regret、区间覆盖 |
| 图像 | images、encoded bytes、pixels | prepare work、model work、two-stage vector | stage service rank、regret、区间覆盖 |

只在估计器能稳定排序候选配置后，才允许驱动在线控制；否则只作 tracing 字段。

### 4.2 Organization gate

保持总 work、服务与静态 credit 不变，逐一比较：

1. fixed rows/images；
2. scalar token/frame budget；
3. stage-aware work budget；
4. balance-aware；
5. locality-aware；
6. balance + locality 受控组合。

报告 batch work CV、packing ratio、oversize rows、endpoint/stage work skew、prefix/locality preservation、queue age、throughput、tail、quality 和 energy。

### 4.3 Dynamic gate

动态控制仅在离线校准动作集合内选择，按以下顺序运行：

1. steady underload；
2. steady near-saturation；
3. steady overload guardrail；
4. two-phase low→high 与 high→low；
5. burst arrival；
6. short/long 或 easy/hard work mix；
7. 1/2/4 job equal work；
8. staggered overlap；
9. weighted 3:1；
10. heterogeneous job mix/offset。

每个场景比较：

- frozen-static；
- state-observed but no-op（测量观测开销）；
- admission-only；
- routing-only；
- fair-sharing-only（多 job）；
- 最小联合候选。

禁止一开始把 organization、K、routing、actor 数和资源同时动态化。

## 5. 动态控制器最小设计

第一版只使用：

- offline `safe_min_work`、`safe_max_work`；
- arrival-work EWMA；
- completion-work EWMA；
- oldest queue age / SLO slack；
- stage active/ready work；
- 观测 freshness。

动作：从冻结的 work-credit 候选集合中单步升降；deadband 内保持；最短驻留时间内不重复动作；观测过期、误差异常或 failure 时回退 frozen-static。vLLM waiting/KV 和 GPU util 作为 guardrail/诊断，不单独驱动 AIMD。

## 6. 图像正式矩阵

### 6.1 强 baseline

1. bounded direct ceiling；
2. Daft built-in AI function；
3. Ray Data native API graph；
4. project typed Ray actor frozen-static；
5. project state-aware proposed（只在前四项统一合同通过后进入排名）。

统一 PostgreSQL BYTEA source、CLIP model/processor/dtype/L2 normalization、CPU/GPU reservation、到 gather 完成的 operator-E2E 边界、task quality 和 failure accounting。pgvector sink 只运行小规模 exactly-once/检索质量闭环，不进入性能主排名。vLLM pooling 只有在相同模型/语义可部署且通过 correctness gate 后才进入，否则保持 blocked 并说明原因。

### 6.2 图像 workload 形态

- uniform resized control；
- encoded-size/decode-cost controlled skew；
- easy→hard/hard→easy phase shift；
- burst arrival；
- two-job staggered 与 weighted mix。

图像 work descriptor 至少记录 encoded bytes、original dimensions、prepare-work estimate、model-work estimate、tensor bytes、locality/shape bucket 和 uncertainty。

### 6.3 图像指标

- correct embeddings/s、operator-E2E、first output、P50/P95/P99 batch/row JCT；
- CPU preprocess busy/queue、ready-tensor queue work、GPU queue/service/busy、H2D、buffer bytes；
- GPU utilization/MFU（若公式适用）、power/energy、J per 1k embeddings；
- exactly-once、retry/timeout/OOM、embedding finite/norm/digest；
- Recall@K、MRR 或 nDCG（使用冻结 ground truth）；
- multi-job JCT、SLO goodput、Jain fairness、isolation 和 borrowed work。

## 7. 开题使用边界

开题需要四条证据链都有动机现象和可执行对照，但不要求论文方法已经全面胜出。原生单 job 1+3 已完成：Daft Native/Ray 在当前官方 graph 下稳定 overqueue，Ray Data 当前冻结路径稳定 underfeed；它只证明问题形态，不证明项目方法胜出。当前只剩一类不可替代的文本证据：

1. Daft Native/Ray、Ray Data 的两个 short/long 错峰独立 job 观察，以及项目 `static_partition` vs `shared_work` 的同上限因果 A/B。

现有 token-work、active-work、图像 stage profile/matched-resource、1/2/4-job 和 429-run cost decision-quality 直接复用。图像 cost held-out 只在已有 profile 无法组成决策对照时才新跑；phase-change、3:1 weighted、第二硬件和更多 workload 不阻塞本轮开题证据闭环。

### 7.1 开题最小矩阵的冻结规模

| 组 | 冻结 workload/shape | arms/scenarios | 重复 | 目的 |
|---|---|---|---|---|
| 原生单 job（已完成） | ShareGPT controlled-skew held-out Chat manifest；同 model/service/output cap | bounded Chat、Daft Native、Daft Ray、Ray Data | 每臂 1+3，平衡交错 | 已冻结 underfeed/minimum-saturation/overqueue 三类外部状态；见 `opening_text_native_single_job_formal_20260808/` |
| 原生两 job 观察 | 两个 512 行 short/long job；offset 在 concurrent smoke 后冻结；互斥 manifest | Daft Native、Daft Ray、Ray Data 各自启动两个独立 job；不注入项目 credit | 每臂 1+3 | job/group JCT、服务计数、vLLM running/waiting/KV/TTFT delta、逐 GPU 利用率/功耗；观察全局压力、干扰和资源超卖 |
| 两作业 | 两个 512 行 short/long job；15 s stagger；互斥 manifest 与 source offset | static partition 与 shared work-credit/fair queue 成对比较；相同 endpoint 总 K/work | 每场景 1+3 group runs | JCT、公平、隔离、idle borrowing；不得把独立 full credit 当公平 baseline；weighted 留论文阶段 |

两作业必须使用冻结的 short/long manifest 与互斥 source offset，并在结果中报告各 job 实际 predicted/observed work。原生框架观察不得命名为 `static_partition`；只有项目 A/B 可计算 `borrowed_work_seconds`。该最小矩阵不声称 3:1 weighted fairness 已验证。
