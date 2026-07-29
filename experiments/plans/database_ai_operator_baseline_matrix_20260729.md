# 数据库 AI 算子与官方 Runtime Baseline 矩阵

日期：2026-07-29
状态：预注册完成；64 行双 GPU 功能门禁进行中，尚无可用于性能结论的结果

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
| F5 | LOTUS `sem_map` | 语义算子系统扩展，非首轮必跑 |

F0–F4 必测。F5 只有在关闭 cache、cascade/helper model，固定一行一次调用，
且 messages/input/output token 与其他 arm 等价后才进入 formal。

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
9. 七步结构分析后再决定 LOTUS 和多 job 扩展。

远端遵循 `deploy/autodl/README.md`：先检查 runner/lease/endpoint/git，使用
全新输出目录，gate 未通过禁止 formal，保留失败证据和所有未跟踪结果。

### 8.1 2026-07-29 功能门禁状态

固定 64 行 manifest 已生成并冻结；两个 endpoint 的预测 token work 为
11,713/11,712，偏差 0.0085%。在
`dual_gpu_official_baseline_core_gate_20260729_1730` 中，vLLM Bench、
bounded HTTP、Daft Native 与 Daft Ray 均通过 64/64 exactly-once、双 endpoint
与最终空队列门禁。

Ray Data HTTP 尚未通过：两个 driver 都已连接同一个 6380 cluster，但 Ray
worker 反序列化项目 UDF 时缺少仓库 `code/` 的 `PYTHONPATH`。这属于部署可移植性
缺陷，不是性能结果，也不能从 pending 资源告警推断 Ray 容量不足。修复必须先以
单元测试锁定 `runtime_env`，再在全新输出目录重跑整个 core gate；`_1730`
保留为失败证据。core gate 完整通过前，calibration 和 formal 继续禁止启动。

## 9. 详细工程设计

适配器边界、fatal-flaw audit 和实现模块见：

`../../code_doc/superpowers/plans/2026-07-29-same-condition-official-baselines-design.md`
