# 数据库 AI 算子与官方 Runtime Baseline 矩阵

日期：2026-07-29
状态：预注册完成；64 行双 GPU 功能与等价性 re-gate 均通过，统一服务端
token counter gate 待完成，尚无可用于性能结论的结果

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

## 4. 同条件契约

核心矩阵统一使用：

```text
POST /v1/chat/completions
```

现有 `/v1/completions` 结果仅作历史机制证据，不与本矩阵直接比较。

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
1/2/4-job。

## 6. Calibration

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

## 7. 指标与结论门槛

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

1. 冻结 Chat manifest、hash、公共结果 schema；
2. OceanBase 版本/函数/endpoint/单行 SQL gate；
3. B0/B1/B2 双 endpoint gate；
4. B3/B4 Chat 适配 gate；
5. F1/F2/F3 request-body、batch、exactly-once gate；
6. 各 arm 独立 calibration；
7. 小作业瞬态 formal；
8. 2,048 held-out formal；
9. 七步结构分析后再决定 LOTUS/Palimpzest system-level baseline 和多 job 扩展。

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

## 9. 详细工程设计

适配器边界、fatal-flaw audit 和实现模块见：

`../../code_doc/superpowers/plans/2026-07-29-same-condition-official-baselines-design.md`
