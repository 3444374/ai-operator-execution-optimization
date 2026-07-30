# 数据库 AI 算子与官方 Runtime Baseline 矩阵

日期：2026-07-30
状态：f203257 双协议 feeding formal 已完成。Completions fixed16 project
达到同协议 bounded direct 的 97.7%，Chat async K256 达到 bounded Chat
的同量级服务窗口，feeding 门禁已通过。32K 是当前 throughput-oriented
token-budget 冻结点；49K 是保留吞吐下的 SLO-goodput 候选。旧 8K 数据组织
和 K32/K64 提交消融只作机制证据，必须在冻结合同下重跑才能进入最终排名。

## 0. 一句话目标与成功条件

本项目不是要超过绕过数据库和 Ray 的 vLLM Bench 服务上限，而是要在相同物理
环境、相同请求语义和相同工作量下：

1. 先使项目提交路径稳定达到同协议强客户端上限的至少 95%，避免把客户端欠载
   误判成调度策略效果；
2. 再证明数据组织或提交控制相对“冻结的最佳静态配置”改善 operator E2E/JCT、
   tokens/s、P99/SLO 或达到同等吞吐所需的 active work；
3. 最后在 database E2E、多 job 和多模态负载上验证收益仍成立。

任何阶段只汇报该阶段可回答的问题。服务上限不是竞争 baseline，默认参数不是
强 baseline，事后逐 workload 搜索出的最优点也不能冒充可在线部署的动态策略。

## 1. 研究问题

本实验先于新的调度策略，回答三个不同问题：

1. **服务上限**：同一双 GPU vLLM 对固定请求集能提供多大容量？
2. **外部系统价值**：相对无 Daft/Ray 的现有数据库 `AI_COMPLETE` 算子，
   本项目是否改善吞吐、JCT、尾延迟或达到饱和所需的压力？
3. **自定义策略价值**：相对 Daft/Ray 的官方批推理实现，本项目的 token-work、
   request-level refill 和共享 credit 是否有独立收益？

三个问题分层报告。vLLM Bench 不是数据库 baseline；Daft/Ray 官方实现也不
替代“无 Daft/Ray”的数据库算子 baseline。

## 2. 第一层：无 Daft/Ray 核心矩阵

| Arm | 链路 | 角色 |
|---|---|---|
| B0 | vLLM Bench → 双 vLLM | serving ceiling |
| B1 | OceanBase `AI_COMPLETE` → 双 vLLM | 现有数据库 AI 算子产品 baseline |
| B2 | PostgreSQL → bounded AsyncIO → 双 vLLM → PostgreSQL | 同数据库强因果 baseline |
| B3 | PostgreSQL → Daft+Ray static → 双 vLLM → PostgreSQL | 框架成本消融 |
| B4 | PostgreSQL → Daft+Ray token-work/refill → 双 vLLM → PostgreSQL | proposed 单 job arm |

B2 不是用来替代 B1，而是防止 B1/B4 的差异被 OceanBase/PostgreSQL 数据库
差异污染。B2 必须独立标定并发，不得使用串行或弱默认实现。

OceanBase B1 的 formal 前置门禁：

- Community Edition 实例包含 `AI_COMPLETE`、`DBMS_AI_SERVICE`；
- 自定义 OpenAI-compatible endpoint 能直连同机 vLLM；
- 无额外云 AI 网关；
- 固定 messages、model、temperature、top_p、max_tokens 和 chat template；
- 明确 AI function 的原生 SQL 并行能力；
- exactly-once、错误、token 和完成时间可以审计。

门禁失败时，保留失败证据并把 OceanBase 降为工业参考，不伪造等价 arm。

## 3. 第二层：现有官方框架矩阵

| Arm | 链路 | 角色 |
|---|---|---|
| F0 | vLLM Bench | serving ceiling |
| F1 | Daft `prompt()` + Native Runner | Daft 官方无 Ray AI Function |
| F2 | Daft `prompt()` + Ray Runner | Daft 官方分布式 AI Function |
| F3 | Ray Data HTTP Processor | Ray 官方外部 HTTP batch inference |
| F4 | 本项目 Daft+Ray token-work/refill | proposed runtime |
| F5 | LOTUS semantic operator | PVLDB 正式数据库 AI 系统 baseline，非首轮 runtime ceiling 必跑 |
| F6 | Palimpzest declarative pipeline | CIDR 官方系统 baseline，非首轮 runtime ceiling 必跑 |

F0–F4 必测。F5/F6 只有在关闭 cache、cascade/helper model 和计划级 work
reduction，固定一行一次调用，且 messages/input/output token 与其他 arm 等价后
才进入“相同 work 的 runtime formal”。若保留系统自身优化，则另列为
system-level quality/cost baseline，同时报告调用数和质量，不能与 F0–F4 只比
wall time。

SemBench（PVLDB 2026）不作为一个可执行 arm，而作为扩展 workload、质量/成本/
内存/失败指标和五次重复协议的依据。

第一轮只测固定 manifest 的 operator-only；随后 F1–F4 接回相同 PostgreSQL
读取与写回，形成 database-e2e 对照。

## 4. 同条件契约与双协议实验轨道

保留两个互不交叉排名的协议轨道：

| 轨道 | 项目请求语义 | 同协议强对照 | 回答的问题 |
|---|---|---|---|
| Completions 机制主线 | 一个 HTTP body 可含多个完整 prompt；每个 prompt 仍是一行独立请求 | 持久异步 bounded multi-prompt client，固定 `batch_rows × concurrency` | token-budget、length-align、动态关批等数据组织是否降低上游开销并改善 E2E |
| Chat 产品兼容线 | 一行一次 Chat 请求；Ray actor 内异步并发 dispatch | vLLM Bench、bounded Chat、Daft Native/Ray、Ray Data、可用时 OceanBase | 与现有产品/官方 runtime 的同语义比较及通用性 |

Completions 不是为了“换协议制造更高吞吐”，而是项目原始的多 prompt 设计；
Chat 也不是被删除的慢路径，而是官方 runtime 和数据库产品普遍可比的兼容合同。
任何数值比较都必须协议内进行：Completions 只对 Completions，Chat 只对 Chat。
跨协议结果只能解释接口/封装代价，不能声称策略胜出。

两条轨道都保持“一行是一个完整 prompt”，严禁把一行 prompt 内容切成多个
请求。multi-prompt 只是在同一个 HTTP body 中批量提交多行，vLLM 仍为每个
prompt 建立独立 sequence。

固定：

- 同一台 AutoDL、两个单 GPU vLLM endpoint；
- 同一模型、vLLM 版本、精度、服务参数和 prefix-cache 状态；
- 同一 ordered request manifest 与 SHA-256；
- 同一 messages/chat template、temperature、top_p、max_tokens、EOS；
- 同一 warm-up、请求顺序、总行数和 endpoint 分片；
- 两 endpoint 预测 token work 差异不超过 2%；
- 0 failure、exactly-once、结束后 vLLM running/waiting 为 0。

实际 output token 总量跨 arm 差异超过 1% 时，不得只凭 JCT 声称加速。
工作量主口径使用每个 endpoint 在 cell 前后的 vLLM prompt/generation cumulative
counter 差分。客户端 token 字段只作交叉核验：server-usage arm 两项必须与
服务端差分一致；official bench 只核对 generation/output；Daft 不以缺失的
client usage 伪造 output token。

## 5. Workload

### 5.1 小作业瞬态

规模：32/64/128/256 行，分别运行。回答：

- time-to-95%-ceiling；
- ramp regret；
- 任务是否有足够独立请求暴露并行；
- 原 15 秒任务能否因更快喂饱 vLLM 而显著缩短。

### 5.2 稳态 held-out

规模：2,048 行；每 arm 1 warm-up + 3 formal repeats。回答：

- capacity efficiency；
- 稳态吞吐/JCT/P99/SLO；
- minimum saturating active work；
- 达到同吞吐所需的 upstream/vLLM 排队压力。

多 job 不与本轮同时启动。先锁定单 job 强 baseline，之后用同一批 arm 扩展到
1/2/4-job；4-job 先通过独立宿主/进程能力门禁，再进入同一正式矩阵。

## 6. Calibration、冻结静态对照与动态策略

calibration 与 held-out 数据严格分离：

- B0/F0：per-endpoint concurrency `{16,32,64,128,256}`；
- B1：OceanBase 原生单 SQL、官方并行执行、必要时多 SQL session；
- B2：bounded concurrency `{16,32,64,128,256}`；
- B3：static request-count capacity curve；
- B4/F4：复核 Chat 下 65,536 active work 饱和点；
- F1/F2：Daft partition/batch/concurrency；
- F3：`batch_size × concurrency`，同时核对真实 HTTP body；
- F5：通过语义门禁后标定 `max_batch_size`。

选取达到本 arm 最大安全吞吐至少 97%、下一档增益小于 3% 的最小压力点。

四类参数必须明确区分：

- **服务/传输容量校准**：在 calibration manifest 上找连接数、actor pool、
  K/active work 和 HTTP batch 的可行平台，只用于消除欠载；
- **冻结的最佳静态 baseline**：从 calibration 选择一个参数点，随后在
  held-out、小作业、多 job 中保持不变，是动态策略必须击败的主对照；
- **per-workload static oracle**：每个测试 workload 都事后 sweep 得到的
  最优点，只用于报告 dynamic regret/上界，不能作为可部署 baseline；
- **动态策略**：冻结候选集合、上下界、控制周期和反馈信号，运行时自动选择。
  它的意义不是“免除一切离线校准”，而是用一次安全边界校准替代每个 workload
  的人工精调，并在 workload 改变时逼近 oracle。

token budget 不是越大越好，也不存在脱离目标函数的单一“最佳预算”。正式曲线保持每 endpoint active work 和请求总量
一致，扫描 2K/4K/8K/16K/32K/49K/65K，记录实际每 HTTP body 的行数/token
利用率、关批原因、JCT、P99 和 vLLM running/waiting。只有在固定预算曲线证明
存在随负载变化的不同甜点后，才评价动态 token budget 是否能逼近各 workload
oracle；不能只拿一个人工调到最优的静态点证明动态策略无意义。冻结合同分别
记录 throughput-oriented 点和“吞吐至少为峰值 95% 时最大 SLO goodput”的
SLO-oriented 点；动态策略必须与二者构成的静态 Pareto 对照比较。

## 7. 指标与结论门槛

时间指标按以下层级报告，tokens/s 不能单独代表系统效果：

1. service-only JCT：vLLM Bench/直接客户端完成固定 manifest 的时间；
2. operator E2E/JCT：数据已在 PostgreSQL 中，到最后一行 AI 结果回到 runtime；
3. database E2E：读取、AI 执行、fan-in、写回和 commit；
4. per-request P50/P95/P99 与小 job JCT、多 job slowdown/SLO。

共同指标：

- output tokens/s、JCT、P50/P95/P99、SLO；
- GPU utilization/MFU、vLLM running/waiting/KV；
- active/pending request 与 token work；
- CPU、内存、DB fetch、AI execute、fan-in、writeback、commit；
- exactly-once、失败和 endpoint 分布。

新增：

```text
capacity_efficiency
time_to_95pct_ceiling_s
ramp_regret_tokens
minimum_saturating_active_work
```

性能晋级必须满足：

- 相对独立标定后的对照，tokens/s 或 JCT 至少改善 5%；
- 至少 2/3 repeats 同方向；
- 最差 repeat 不退化超过 3%；
- P99、失败和 exactly-once 不退化。

若吞吐在 ±3% 内，但饱和 work 至少降低 20%、P99 至少降低 10%，或
time-to-ceiling/ramp-regret 至少改善 10%，只声称压力效率或瞬态改善，不声称
“加速 GPU 推理”。

## 8. 实施顺序

| 阶段 | 比较对象 | 首要指标 | 停止/放行条件 |
|---|---|---|---|
| 正确性 gate | 所有 adapter | exactly-once、0 failure、同 manifest/counter、空队列 | 任一失败即停 |
| 服务上限 | vLLM Bench/同协议 direct client | service JCT、tokens/s、平台曲线 | 记录 ceiling；不要求项目超越 |
| 喂饱门禁 | project fixed submission vs 同协议 bounded client | model-request JCT/throughput、headers wait、running；另报 operator/E2E | warmed model-request capacity ≥95% bounded；否则禁止策略排名 |
| 官方 runtime | Daft Native/Ray、Ray Data、OceanBase capability | operator/database E2E、真实 HTTP 粒度 | 同语义、同计时边界后才排名 |
| 数据组织 | fixed rows vs token-budget/length/prefix | operator E2E、P99、packing/HTTP body | 相对冻结静态 baseline 做单因素消融 |
| 提交控制 | static K/flush vs adaptive | E2E、ramp regret、SLO、active work | 控制参数冻结，不能 per-workload 手调 |
| 单/双 GPU | 相同 per-endpoint 压力 | JCT、吞吐、scaling efficiency | 每卡 workload/并发足够且同协议 |
| 1/2/4 job | independent/partition/shared | aggregate throughput、job JCT、P99、Jain/slowdown | 0 starvation；j4 先过有界 actor/VMA 能力门禁 |
| DB E2E/多模态 | PostgreSQL+writeback；图像同策略 | database E2E、策略收益保留率 | 前述机制已通过才启动 |

当前实际顺序：

1. 冻结 Chat 与 Completions manifest/hash、公共结果 schema；
2. 修复 Ray worker 线程上限、失败 trace、持久异步 client；
3. 分别运行 Chat feeding gate 和 Completions fixed-row feeding gate；
4. 门禁通过后冻结 Chat actor/K 和 Completions `batch_rows × concurrency`；
5. 才运行 token budget、length-align、动态 K/flush 单因素消融；
6. 再运行小作业、2,048 held-out、单/双 GPU 和 1/2/4-job；4-job 独立门禁后再进入 formal；
7. 最后接回数据库写回、多模态，以及可用时 OceanBase；LOTUS/Palimpzest 仅按
   system-level quality/cost 合同扩展。

远端遵循 `deploy/autodl/README.md`：先检查 runner/lease/endpoint/git，使用
全新输出目录，gate 未通过禁止 formal，保留失败证据和所有未跟踪结果。

### 8.1 2026-07-29 功能门禁状态

固定 64 行 manifest 已生成并冻结；两个 endpoint 的预测 token work 为
11,713/11,712，偏差 0.0085%。在
`dual_gpu_official_baseline_core_gate_20260729_1730` 中，vLLM Bench、
bounded HTTP、Daft Native 与 Daft Ray 均通过 64/64 exactly-once、双 endpoint
与最终空队列门禁。

`dual_gpu_official_baseline_core_gate_20260729_1725_fix5708e85` 已通过全部
5 个 core adapter：每项 64/64 exactly-once、0 incident、两 endpoint 均使用、
work skew 0.0085%，最终 vLLM 队列归零。此前 `_1730` 的 Ray worker
`PYTHONPATH` 失败目录继续保留为证据。

通过后的等价性审计又发现三项 fatal flaw：

1. vLLM CustomDataset 默认先套 chat template，`openai-chat` 再交给服务端套
   第二次；必须使用 `--skip-chat-template`。修复后 bench `input_lens` 是裸
   prompt，不能与服务端套一次模板后的 usage 强制逐行相等；
2. Ray Data 的整数 concurrency 在该版本实际形成 `1..n` autoscaling，小作业
   只起 1 actor；必须显式使用 `(n,n)` 固定池再独立 calibration；
3. Daft 只有 shard barrier 时间和裸 prompt token，Ray Data 当前也只有 shard
   barrier 时间；summary 必须标注观测粒度，不能把公共 barrier 复制出的 P95
   与 request-level P95 横比。

提交 `f2e82bd` 的等价性修复已在
`dual_gpu_official_baseline_equivalence_gate_20260729_f2e82bd` 使用全新目录
再次通过 5/5、最终队列归零。Ray Data 固定池创建 4 actor，但 64 行双 shard
每端只有两个 16 行 task，实际只有 1 actor 执行；因此 actor 使用率留到
calibration 由 `batch_size × actor_count × task_count` 联合判断，不能把小 gate
当并行扩展证据。

当前最后一个正式性能前置门禁是统一服务端 token 计数：每个 cell、每个 endpoint
在运行前后采样 vLLM prompt/generation cumulative counters，保存原值和差分；
server-usage arm 与差分不一致、差分非正、counter 回退或存在并发污染时均失败。
该门禁通过后才允许进入 calibration。

### 8.2 256 行 scale gate 与直接客户端校准

`dual_gpu_official_baseline_scale_gate_256_20260729_3b4aef0` 已完成 5/5
core arm 单次 scale gate：每项 256/256 exactly-once、0 incident、最终队列
归零。按统一服务端 counter 计算：

| arm | per-endpoint 配置 | JCT (s) | total tokens/s | generation tokens/s |
|---|---:|---:|---:|---:|
| vLLM Bench | C32 | 20.37 | 4930 | 2698 |
| bounded HTTP | C32 | 20.37 | 4926 | 2694 |
| Daft Native | official default | 10.20 | 9818 | 5359 |
| Daft Ray | official default | 16.35 | 6125 | 3345 |
| Ray Data HTTP | batch16 × actor4 | 128.53 | 780 | 426 |

这是一次 scale gate，不是统计有效的性能排名。vLLM Bench 与 bounded HTTP 在
相同 C32 下几乎重合，说明当前直接客户端对照实现一致，但二者都未证明达到服务
ceiling。Daft CLI 的 `concurrency=1` 没有成为实际 `prompt()` 并发控制，官方
执行保持 `concurrency=None`；Ray Data 包装器则在 UDF 内逐行请求，每 endpoint
只有约四个服务请求并行。Daft/Ray Data 的 barrier 延迟也不能与 request-level
P99 横比。

因此下一步只校准直接客户端 C64，再在门禁通过后运行 C128；使用同一 256 行
manifest、同一双 endpoint、全新输出目录与 runner 的 `--include-cell` /
`--concurrency-override`，不重跑 Daft/Ray Data。若 C64→C128 的 total 或
generation tokens/s 增益低于 3%，选择达到最大安全吞吐 97% 的最小档位；若仍
增长，再依据预注册矩阵决定是否运行 C256。该校准完成前，不把 Daft Native
单次高值解释为框架加速，也不与历史 2,048 行 arrival-replay 的约 8K tokens/s
直接比较。

C64 与 C128 随后完成。C64 两个直接客户端均通过 256/256 exactly-once、0
incident 与空队列门禁：vLLM Bench/bounded 分别为 8,342/8,333 total
tokens/s、JCT 12.021/12.019s，较 C32 约提升 69%，证明 C32 明显欠载。
vLLM Bench C128 日志确认 peak concurrency=128，得到 12,762 total
tokens/s、JCT 7.849s，较 C64 再提升 53%，尚未达到 3% 平台阈值。

bounded C128 虽通过完整性门禁，但仅 8,711 total tokens/s；fatal-flaw audit
确认 httpx 0.28.1 默认 `max_connections=100`、keepalive=20，配置 C128 被
客户端隐式截断，因此该点作废。客户端连接池现已测试先行改为显式匹配配置并发，
并在全新目录完成 bounded-only C128 re-gate：endpoint running 峰值观测到
124/125，256/256 exactly-once、0 incident、最终队列归零；修复后 JCT
8.048s、total/generation tokens/s 为 12,472/6,823。相对旧污染点吞吐
+43.2%、JCT -30.1%；相对有效 vLLM Bench C128 只低 2.27%/2.11%，JCT
高 2.53%，已无明显协议分叉。

现有 256 行 manifest 每 endpoint 只有 128 行，不能执行有效 C256。下一容量点
必须使用至少 512 行、每 endpoint 256 行的同构冻结 manifest。更重要的是，
12,762 total tokens/s 已否定“历史约 8.0–8.2K 是双 4090 物理极限”的解释；
历史 active-work 平台只属于当时 project profiler、arrival replay、请求协议和
workload。下一强制对照是把 project profiler 映射到同一 manifest、Chat
Completions、no replay，再比较 direct ceiling、ours 的 JCT/吞吐以及达到同一
吞吐所需的 active work，不能跨口径判断谁更快。

### 8.3 512 行 C256 结果与 project runtime 前置状态

512 行 immutable manifest 已冻结在远端，SHA-256 为
`7205f7ec2b9d52d8f0a4546a044cbbdaff644c0f88d06e9fc11a9a0c86077ced`。
两 endpoint 各 256 行，预测 work 为 73,329/73,328，偏斜 0.00136%。
四个直接客户端 cell 均满足 512/512 exactly-once、0 incident、双 endpoint、
服务端 counter 一致和最终空队列：

| arm | per-endpoint concurrency | JCT (s) | total tokens/s | generation tokens/s |
|---|---:|---:|---:|---:|
| vLLM Bench | 128 | 14.864 | 12,354 | 7,327 |
| bounded HTTP | 128 | 16.762 | 10,929 | 6,471 |
| vLLM Bench | 256 | 11.931 | 15,351 | 9,088 |
| bounded HTTP | 256 | 12.569 | 14,532 | 8,587 |

C128→C256 的 total tokens/s 增益分别为 +24.3% 和 +33.0%，因此 C128
明确不是 ceiling。C256 是当前 `max_num_seqs=256` 的配置硬上限；因为没有更高
可执行相邻点，不能把它写成已经满足“下一档增益 <3%”的经验平台。

project profiler 使用
`dual_gpu_same_condition_project_calibration.example.json` 在同一 512 行
manifest 上扫描 per-endpoint static K `{32,64,128,256}` 与 active work
`{16384,32768,49152,65536,98304}`。契约强制 Chat Completions、原始 prompt、
`temperature=0`、trace-target output cost、no arrival replay、request-level
replenishment、同一固定 endpoint 分片和显式 Ray address；任一源行或 token
字段不匹配即失败。

远端 PostgreSQL 当前只有 `doc_id=0..2047` 的 2,048 行。校准占用 `0..511`
后，`512..2047` 仅 1,536 行，不能冒充 disjoint 2,048-row formal。正式矩阵
启动前必须新增并冻结 `doc_id=2048..2559`，或导入独立的 2,048 行 held-out
workload；profiler formal 模板固定 `--source-row-offset 512`。在此阻塞解除前，
只允许执行 512 行 project calibration，不允许重用校准行启动 formal。

### 8.4 Project 64 行首次门禁故障与修复

首次 project gate
`dual_gpu_same_condition_project_gate64_20260729_33c278b` 在任何 HTTP 请求
前停止。`doc_id=2` 的数据库原始 `target_output_tokens=276`，official
manifest 按请求 `max_output_tokens=256` 记录有效
`estimated_output_tokens=256`；`source_row_hash` 一致，因此不是数据库或
manifest 漂移。

根因是 project 原有 `trace_target_output` 把未裁剪的 276 用于 active work，
manifest guard 也直接比较 raw 276 与 effective 256。统一合同现改为
`min(target_output_tokens, completion_max_tokens)`：调度估计和 guard 使用
同一有效 work；guard 还会用 raw target 等完整源字段重算
`source_row_hash`，所以两个不同的 above-cap raw rows 仍不等价，校验没有
移除或放宽。旧失败目录、stderr、lease 证据均保留；
双 endpoint 最终队列为 0，512 校准未启动。只有修复通过本地完整测试并在全新
目录通过 64 行 re-gate 后，才恢复 512 校准。

补齐 formal 数据时，`source-row-offset` 按全部过滤后的 eligible rows 计数。
远端已核对 raw hash、2,048 行文本/session 和 Qwen2.5-7B token 数，但历史
shell 命令没有留存，不能声称 exact CLI provenance。使用正式 source
predicate 的显式 `max_prompt_tokens=1500` 重建并逐字段核验
`doc_id=0..2047`，随后用
append-only 插入 `2048..2559`；任一 prefix 不一致或 doc ID 冲突都阻止写入，
禁止旧 upsert 路径覆盖已有实验数据。

### 8.5 Project active-work 背压故障与重新放行条件

`beeee20` 的全新 64 行 re-gate 已通过 64/64 exactly-once、双 endpoint
32/32、0 incident 和最终空队列。随后 512 行校准首个
`work16384` 场景在 position/doc_id=89 停止：固定 endpoint-0 已有
16,161 work，新请求 work=234，加入后 16,395 超过 cap；endpoint-1 仍可接收。
旧实现把这个 request-specific capacity 结果覆盖到 `healthy`，pinned router
因而误报 endpoint-0 不健康。两个服务的 `/health` 始终正常，失败完成
0/9、无 `runs.csv`，现场未覆盖且没有自动重试。

修复合同如下：

- `healthy` 只表示服务健康，`available` 表示当前请求是否还有 endpoint-local
  request/work credit；
- manifest-pinned endpoint 暂满时收集完成并重试，不允许改投；
- preferred endpoint 同时确定 pool，pool fallback 不能改变冻结分片；
- fixed pool 中有健康 endpoint 但暂时无 capacity 时走 typed retry；真正无健康
  endpoint 才是终止故障；
- 空 endpoint 上的 oversized local work 可独占执行；若同时启用更严格的
  shared-credit work limit，则在提交前 fail fast，不能静默越界。

本地 512 tests、相关 170 tests 与独立审阅已通过。远端必须使用包含该修复的新
提交和全新输出目录重新跑 64 行 gate；只有 gate 再次通过才恢复 512 行校准。
旧 `beeee20` 校准目录只作为 incident 证据，不参与参数选择。

### 8.6 单次 512 校准失效与等价性门禁

提交 `0c370ce` 的全新 64 行 gate 已通过：64/64 exactly-once、endpoint
32/32、0 incident/failure、最终队列归零。随后 9 个 512 行单次 calibration
cell 均正确完成，但结果非单调：static K256 为 11,736 total tokens/s，
nonbinding work98K 为 4,153；两者均达到 512 max inflight、endpoint work
73,329/73,328、bounded wait 0，理论上应等价。

只读 trace 诊断排除了 active-work 限制、actor 并行度、manifest/payload、
output work 和统计口径。W98K 相对 K256 的额外约 28.6 秒位于
`model_request_wall`；actor ramp 仅多约 3 秒。W98K 是首个 full-concurrency
cell，vLLM running 峰值只有 332 且两 endpoint 分波接纳；K256 running 峰值
510。当前最可能是 Ray threaded urllib、OS connection 或 vLLM HTTP ingress
的首次高并发冷路径，尚不能进一步归因。

因此该 9-cell 数据只作诊断，不选择 K256/W65K 进入 formal。下一步固定为：

1. actor ready barrier 放在 measured E2E timer 之前，单独记录
   `actor_ready_s`；
2. 非流式 HTTP 请求记录 request-start、headers-ready、body-complete、
   headers-wait 和 body-read；不改变 streaming 语义；
3. 使用
   `dual_gpu_same_condition_project_equivalence_gate.example.json`，只比较
   K256 与 nonbinding W98K，每臂 1 warm-up + 3 interleaved repeats；
4. mean throughput/JCT 差异均不超过 5%、至少 2/3 repeats 在界内且所有
   正确性门禁通过后，才启动完整 calibration。

完整 baseline 按因果阶梯推进：单 job steady-state → 32/64/128/256 小作业
transient/ramp → 单/双 GPU scaling → shared-vLLM 1/2/4-job。单 job 要求 ours
吞吐不低于 bounded HTTP 95%；压力效率要求在至少 97% ceiling 下 active
work/inflight 少 20%；transient 要求 time-to-ceiling/ramp regret 改善 20%；
多 job 要求聚合吞吐不低于 95%，同时 P99/SLO/fairness 至少改善 10% 且无
饥饿。均不通过时，结论为当前 workload 下无可证明优势。

### 8.7 2026-07-30 远端结果复核：先修 feeding，不再扩大策略网格

同一 512 行 Chat manifest 的三次 formal 汇总显示：

| 路径 | 最佳/代表配置 | operator/service JCT (s) | total tokens/s |
|---|---|---:|---:|
| vLLM Bench | C256 | 11.931 | 15,351 |
| bounded Chat | C256 | 12.569 | 14,532 |
| project profiler | static K32K active work | 31.227 | 5,884 |
| project profiler | static K64 | 33.328 | 5,509 |
| project profiler | static K128 | 33.015 | 5,551 |
| project profiler | static K256 | 41.053 | 4,592 |

这是“项目提交路径尚未通过 feeding gate”的证据，不是 token-budget 或动态控制
策略较差的正式结论。K 从 64/128 增至 256 后，bounded wait 归零但 JCT 反而
上升，说明容量不是越大越好；project 与 bounded 的约 18.7 秒差距主要仍在
Ray actor、HTTP connection/headers 和 vLLM ingress 所见的提交形态。旧 threaded
`urllib` 每次调用不能提供持久异步连接池，因此新增以下最小因果对照：

1. **Chat feeding**：相同 manifest、request granularity 和 K，比较旧
   `urllib` 与每 actor 一个持久 `httpx.AsyncClient`；再只改变 1×256、
   2×128、4×64、8×32、16×16 actor 形状。Chat 每行仍独立，actor 内使用
   async dispatch；该曲线只解释 Chat feeding，不能冻结 Completions actor；
2. **Completions transport ceiling**：无 Ray bounded client 比较固定
   `batch_rows={1,4,16,32}`，并令 `batch_rows × HTTP concurrency=256`
   per endpoint，隔离 HTTP packing 本身；
3. **project Completions feeding**：使用相同 fixed-row 组合和持久 async
   actor，保留原 multi-prompt body。只有它接近对应 bounded Completions，
   才进入 token-budget/length-align/adaptive flush；
4. feeding gate 先比较不含 source fetch/organize/writeback 的
   `model_request_wall_s` 与
   `model_request_tokens_per_s=(server prompt+generation tokens) /
   model_request_wall_s`。project 不低于同协议 bounded 95%，且模型请求 JCT
   不高于 1.05×、0 failure、exactly-once、最终空队列才通过。完整
   `operator_wall_s`、`operator_tokens_per_s`、`e2e_s` 和 `tokens_per_s`
   仍必须同时报告，但不能把数据库/Daft 固有时间归因成 feeding 缺口；
5. **Completions actor-shape 校准**：在已经选定的 token budget、K 和 active
   work 上固定每 endpoint 256 slots/0.5 CPU，扫描 1/2/4/8/16 actors。
   按“达到正式重复峰值中位数 97% 的最小 actor 数”冻结；16 actors 的单次
   高点不覆盖中位数和离散度。只有这份同协议证据能进入 calibration contract。

此前 shared-vLLM 1/2/4-job `_v3` 只完成 1-job 和 2-job warm-up，4-job
independent warm-up 因 Ray worker 创建失败及 OpenBLAS 每进程尝试 32 线程而
终止；随后 trace writer 对缺失结果调用 `.get` 又遮蔽了根因。这批数据不能作为
formal 多 job 结论。运行环境现统一限制 OMP/OpenBLAS/MKL/NumExpr 为单线程，
trace 对失败 lifecycle 写出证据后再抛原始错误；必须在新提交、新 worktree、
新输出目录重跑功能 gate 后才恢复 multi-job formal。

### 8.8 2026-07-30 worktree 单次真实请求 smoke

提交 `24d6fe3` 在远端独立 worktree 完成 524/524 单元测试、ruff 和
compileall。512 行 immutable manifest、双 endpoint 的三项真实 smoke 均
512/512 success、0 worker failure、exactly-once、最终空队列：

| 路径 | 配置 | model/direct JCT (s) | model-request 或 direct tokens/s | 完整 project E2E (s) / tokens/s |
|---|---|---:|---:|---:|
| bounded Completions | fixed16 × C16/endpoint | 10.943 | 16,624 | service-only |
| project Completions | fixed16、async、K16/endpoint | 11.164 | 16,265 | 14.211 / 12,778 |
| project Chat | async、K64/endpoint | 23.464 | 7,807 | 25.884 / 7,077 |
| project Chat | async、K256/endpoint | 12.552 | 14,581 | 13.916 / 13,152 |
| bounded Chat（既有） | C256/endpoint | 12.569 | 14,532 | service-only |

事实：Completions model-request capacity 为同协议 direct 的约 97.8%；Chat
async K256 的模型请求窗口与 bounded Chat 基本重合，而 K64 明显欠载。这支持
“持久 async dispatch + 足够的 per-endpoint K 修复 feeding”的机制判断，也
说明 K 必须按负载校准而不是越小越稳。

边界：以上是一次 worktree smoke，project E2E 还包含约 0.6–2.3s source fetch
以及组织/编排时间；不能把 model-request 接近 direct 写成 database E2E 已等价，
也不能据此选择正式最优 batch/K。下一步只运行已预注册的 1 warm-up + 3
repeats feeding 配置；重复门禁通过后，Completions 轨道才进入 token-budget
曲线，Chat 轨道才进入官方 runtime/多 job 对照。

### 8.9 f203257 正式结果复核与下一轮冻结合同

同协议 formal 已证明 feeding 缺口关闭：Completions fixed16 project/direct
model-request throughput 为 16,036/16,416 tokens/s（97.7%）；Chat async
K256 的 model-request throughput 中位数为 14,588 tokens/s，与既有 bounded
Chat C256 14,532 tokens/s 同量级。direct 只计 service JCT，project 必须另报
model-request、operator 和 database E2E，禁止用不同计时边界排名。

固定 K256/endpoint、active work 65,536 的 token-budget 曲线并非单调。32K
的 model-request throughput 中位数最高（15,007 tokens/s）；49K 为
14,316 tokens/s，仍达峰值 95.4%，但 30s SLO 违约率稳定为 51.5%，低于
32K 的 57.5%，SLO goodput 18.59/s 高于 17.03/s。因此：

- 32K 冻结为 throughput-oriented static baseline；
- 49K 只冻结为 SLO-oriented 候选，不把它写成已跨 workload 成立的最优；
- 动态 token budget 需同时比较吞吐、P99/SLO 与 static Pareto frontier，
  不能只追一个 tokens/s reward。

旧 8K 数据组织消融内部条件一致，能够提供机制信号：length-align 相对
sequential 的 model-request throughput 仅 -1.8%，但 request P50
32.93→23.56s、30s SLO 违约 58.6%→44.8%、SLO goodput 15.33→20.23/s；
P95 53.64→50.40s。row-cap-aware 则同时降低吞吐并把 SLO 违约提高到 81.4%。
这支持“length-align 可能改善 multi-prompt body 内长短请求拖尾”的假设，
但最终结论仍需在冻结 32K、相同 manifest 上重跑，并增加 512/1024/2048
规模头对头；当前不能把推断写成已证明的 vLLM 内部 HOL 机制。

旧 submission-policy 数据中 active-work 为 K64、request-credit 为 K32，
两者 P50/P95 差异不是受控策略效应。新模板统一 K256、32K、actor shape 和
active-work，只改变 credit/routing/flush 单因素后再排名。

f203257 的 OMP 单线程修复使 j2 通过，但 j4 `ray_task` 仍创建 200+ worker，
在 `vm.max_map_count=65530` 的只读容器触发 raylet `SIGABRT`。下一轮 shared
矩阵统一改为每 job、每 endpoint 一个 persistent async Ray actor。独立
64-row j4 gate 已在相同 `vm.max_map_count=65530` 容器完成三臂、0 actor
failure、0 failure record，正式矩阵恢复 j1/j2/j4。若未来 actor gate
仍失败，j4 标记为宿主能力阻塞，不把失败解释为 shared-credit 策略结果。

## 9. 详细工程设计

适配器边界、fatal-flaw audit 和实现模块见：

`../../code_doc/superpowers/plans/2026-07-29-same-condition-official-baselines-design.md`

本轮批准的 staged validation 与实施清单见：

- `../../code_doc/superpowers/specs/2026-07-29-daft-ray-baseline-advantage-validation-design.md`
- `../../code_doc/superpowers/plans/2026-07-29-daft-ray-baseline-advantage-validation-implementation.md`
