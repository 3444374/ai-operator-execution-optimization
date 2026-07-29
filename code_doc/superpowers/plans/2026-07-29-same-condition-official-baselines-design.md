# 无 Daft/Ray 数据库 AI 算子 Baseline 设计

日期：2026-07-29
状态：核心对照与官方框架补充层已确认；实现前仍需完成语义/可用性门禁

## 1. 研究问题

现有双 4090 实验主要比较 Daft+Ray 链路内部的组织、准入、补位、Actor Pool
和共享 credit 策略。它们不能单独回答：

> 与一个不使用 Daft/Ray 的现有数据库 AI 箔子相比，本项目的外部执行链路
> 是否更快、更早喂饱 vLLM，或以更少的排队压力达到相同吞吐？

因此，下一轮最高优先级不是继续增加 Ray 内部策略，而是建立同规模、同模型
服务条件下的外部系统 baseline。

## 2. 核心对照定义

### 2.1 产品级核心 baseline

优先使用 OceanBase `AI_COMPLETE`：

```text
OceanBase source table
  -> SQL AI_COMPLETE
  -> OpenAI-compatible /v1/chat/completions
  -> the same two vLLM endpoints
  -> OceanBase result table
```

它满足“现有数据库 AI 算子”与“无 Daft/Ray”两个条件。官方文档确认：

- `AI_COMPLETE(model_key, prompt[, parameters])` 是数据库内 SQL 表达式；
- 可注册自定义 HTTP/HTTPS 模型 endpoint；
- provider 可使用 OpenAI 格式；
- URL 可直接指向 Chat Completions endpoint；
- V4.4.1 起提供模型 endpoint 视图，便于核对配置。

OceanBase 必须在真实门禁中确认以下事实后才能晋升为正式 arm：

1. 当前可部署的 Community Edition 镜像确实包含 `AI_COMPLETE` 和
   `DBMS_AI_SERVICE`；
2. 能访问同机 vLLM，不经过额外云 AI 网关；
3. 能固定 model、temperature、max_tokens、EOS/chat template；
4. 一条表扫描 SQL 的 AI function 是否并行；若不并行，官方推荐的最强执行
   形态是什么；
5. 能记录 exactly-once、失败、真实 token、逐请求或至少逐行完成时间。

若任一关键条件不满足，OceanBase 降为“工业系统参考”，不得用不等价云端数字
替代同机实验。

### 2.2 必要的同数据库因果 baseline

产品对照会同时改变 PostgreSQL 与 OceanBase，不能单独归因于 Daft/Ray。
因此必须增加：

```text
PostgreSQL source table
  -> bounded AsyncIO HTTP client
  -> the same two vLLM endpoints
  -> PostgreSQL result table
```

该 arm 不使用 Daft/Ray，并保留与本项目相同的 PostgreSQL 读取和写回语义。
它不是产品创新对手，而是最小因果对照，用来隔离：

- 数据库种类差异；
- Daft DataFrame/Arrow 组织开销；
- Ray Actor/调度开销与收益；
- token-work 准入和 request-level refill 的净贡献。

这个 baseline 必须独立标定 bounded concurrency，不能故意使用串行客户端。

### 2.3 本项目 arm

```text
PostgreSQL source table
  -> Daft organization
  -> Ray Actor Pool
  -> token-work admission + request-level refill
  -> the same two vLLM endpoints
  -> PostgreSQL result table
```

先保留当前已验证的固定策略：

- request-level continuous replenishment；
- `1×256` Actor Pool；
- 每 endpoint 65,536 active work；
- fixed 50ms flush；
- 固定 endpoint shard。

动态 routing、shared credit、公平队列和 adaptive flush 不混入单 job 核心主比。

### 2.4 服务上限参照

vLLM `bench serve` 只回答同一请求集和服务配置下的可实现容量：

```text
capacity_efficiency = pipeline_tokens_s / direct_vllm_tokens_s
```

它不是数据库 AI 算子 baseline，不能替代 OceanBase 或同数据库因果对照。

### 2.5 现有官方框架 baseline

为排除“收益只是 Daft/Ray 官方能力，而不是本项目策略”的解释，增加第二层：

```text
frozen request manifest
  -> Daft prompt() Native Runner
  -> Daft prompt() Ray Runner
  -> Ray Data HTTP Processor
  -> the same two vLLM endpoints
```

这一层不替代 OceanBase。它回答的是框架内部归因：

- Daft 原生 AI Function 的 batching/concurrency/backpressure 是否已足够；
- Ray Data 官方 HTTP batch inference 是否已达到相同吞吐和爬坡速度；
- 自定义 Actor Pool、token-work credit 和 request refill 是否仍有独立收益。

LOTUS `sem_map` 作为扩展系统门禁。只有关闭 cache、cascade/helper model，
固定一行一次相同模型调用，并核对实际 messages/input/output tokens 后，才可
进入第二阶段正式结果。

## 3. 补充系统的定位

| 系统 | 定位 | 不作为核心的原因 |
|---|---|---|
| pgai SQL/model calling | PostgreSQL 生态补充 baseline | 仓库已归档；现有本项目验证是 Ollama `AI_EMBED`，尚未证明 AI_COMPLETE 可无适配地指向同机 vLLM |
| PolarDB AI Function | 工业系统参考 | 托管产品、数据库和云环境变化，当前没有同机可复现实例 |
| PolarDB Daft on Ray | 工业架构依据 | 使用 Daft/Ray，不属于用户要求的“无 Daft/Ray”对照 |
| Ray Data HTTP Processor | 必测的官方框架 baseline | 使用 Ray，因此回答框架内部归因，不替代核心外部 baseline |
| Daft `prompt()` Native/Ray | 必测的官方框架 baseline | 使用 Daft，因此回答官方 AI Function 是否已足够 |
| LOTUS `sem_map` | 第二阶段语义算子系统扩展 | prompt 模板、解析、缓存和语义优化默认值可能改变工作量 |
| Snowflake Cortex AISQL | 闭源工业参考 | 模型、硬件、内部执行和队列不可控 |

## 4. 协议决定

OceanBase 官方模型 endpoint 使用 OpenAI-compatible Chat Completions。因此核心
矩阵统一为：

```text
POST /v1/chat/completions
```

OceanBase、bounded AsyncIO、本项目和 vLLM Bench 必须全部重新运行 Chat
Completions。现有 `/v1/completions` 结果保留为机制与历史证据，但禁止与新的
核心矩阵直接横比。

所有 arm 固定：

- 相同 chat template 与 system/user message；
- 相同 model、temperature、top_p、max_tokens、EOS；
- 相同 prompt 原文和实际 prompt token；
- 相同 vLLM 版本、模型、精度、CUDA Graph、chunked prefill、
  `max_num_seqs`、`max_num_batched_tokens`、GPU memory utilization 和
  prefix-cache 状态。

## 5. 实验矩阵

### 5.1 无 Daft/Ray 核心主矩阵

| Arm | 数据库 | Daft | Ray | 提交控制 | 作用 |
|---|---|---:|---:|---|---|
| B0 | 无 | 否 | 否 | vLLM Bench static concurrency | 服务容量上限 |
| B1 | OceanBase | 否 | 否 | `AI_COMPLETE` 原生执行 | 现有数据库 AI 算子核心 baseline |
| B2 | PostgreSQL | 否 | 否 | bounded AsyncIO static concurrency | 同数据库强因果 baseline |
| B3 | PostgreSQL | 是 | 是 | static request-count | Daft/Ray 框架成本消融 |
| B4 | PostgreSQL | 是 | 是 | token-work + request refill | 当前 proposed 单 job arm |

B1 若门禁失败，正式主矩阵只保留 B0/B2/B3/B4，并明确论文暂缺可复现的产品级
数据库 AI 算子数值对照；OceanBase 只作功能与架构参考。

### 5.2 现有官方框架矩阵

| Arm | 系统 | Runner/控制 | 作用 |
|---|---|---|---|
| F0 | vLLM Bench | 最优 static concurrency | 同协议服务上限 |
| F1 | Daft `prompt()` | Native Runner | 无 Ray 的 Daft 官方 AI Function |
| F2 | Daft `prompt()` | Ray Runner | Daft 官方分布式执行 |
| F3 | Ray Data HTTP Processor | `batch_size × concurrency` | Ray 官方外部 HTTP 批推理 |
| F4 | 本项目 | token-work + request refill | 自定义策略 |
| F5 | LOTUS `sem_map` | 固定一行一调用 | 通过语义门禁后才运行的扩展 |

F0–F4 为必测；F5 为第二阶段。第一轮先做预加载 operator-only，避免数据库读写
差异掩盖 runtime 归因；通过后再让 F1–F4 接回相同 PostgreSQL 读取/写回边界。

## 6. 同规模同条件契约

### 6.1 固化请求

从同一源 workload 生成不可变清单：

```text
doc_id
prompt
messages
arrival_time
prompt_tokens
max_output_tokens
estimated_output_tokens
source_row_hash
```

生成 ordered JSONL、SHA-256、行数/token 分布、重复 ID 审计，以及互不重叠的
calibration 和 held-out 分片。OceanBase 与 PostgreSQL 分别导入相同内容；
不得由系统自行改写、重排或截断。

### 6.2 双 endpoint

第一轮使用确定性 token-work 平衡分片，为两个 endpoint 同步启动同一 arm。
不增加反向代理，避免新变量。每次 run 检查：

- 两 endpoint 均有请求；
- 预测 work 差异不超过 2%；
- 首提交 skew 不超过 2 秒；
- exactly-once；
- 结束后 vLLM running/waiting 为 0。

动态路由在核心矩阵之后单独消融。

### 6.3 计时边界

同时报告：

1. `operator_only`：请求可用到全部模型响应完成；
2. `database_e2e`：数据库读取、执行、结果写回和提交；
3. `service_only`：B0 的直接 vLLM 上限。

OceanBase 的 SQL 表达式若将读取/推理/返回绑定在一个查询中，必须报告该不可拆
边界，不得伪造细粒度分段。

## 7. 强 baseline 标定

每个 arm 使用 calibration 数据独立调优，参数冻结后才运行 held-out。

- B0：per-endpoint concurrency `{16, 32, 64, 128, 256}`；
- B1：先测试原生单 SQL，再测试 OceanBase 官方支持的并行 hint/并行执行；
  若 AI function 不进入并行计划，再标定多 SQL session 数；
- B2：per-endpoint bounded concurrency `{16, 32, 64, 128, 256}`；
- B3：static request-count capacity curve；
- B4：冻结当前 65,536 active work，另验证 Chat 协议下饱和点未漂移。
- F1/F2：独立扫描 Daft partition/batch/concurrency，并记录真实 HTTP 请求数；
- F3：扫描 `batch_size × concurrency`，先以 trace 确认 `batch_size` 是 Ray
  stage batch 还是一次 HTTP multi-prompt；
- F5：通过 cache/prompt/token 语义门禁后标定 `max_batch_size`。

选点规则：

1. 达到该 arm 最大安全吞吐的至少 97%；
2. 下一档吞吐增益小于 3%；
3. 0 failure、exactly-once、服务健康和输出语义门禁均通过；
4. 满足条件时选并发/active work 更小者。

正式 held-out 为 2,048 行，1 次 warm-up + 3 次 formal repeat。小作业
32/64/128/256 行单列运行，用于检验启动爬坡，不与 2,048 稳态结果混表。

## 8. 指标

共同指标：

- output tokens/s、JCT、P50/P95/P99、SLO violation；
- GPU utilization、MFU、vLLM running/waiting/KV cache；
- upstream pending、active request/work、CPU、内存；
- exactly-once、失败和 endpoint 分布；
- 数据库读取、AI 执行、写回与 commit 时间。

新增：

```text
time_to_95pct_ceiling_s
ramp_regret_tokens
capacity_efficiency
minimum_saturating_active_work
```

`ramp_regret_tokens` 对服务上限的 95% 积分缺口进行累计，用来回答“原本
15 秒的小任务，是否能通过更快暴露并行请求而显著接近 5 秒”。

## 9. 预注册结论

### 9.1 有效性硬门槛

- 0 worker/database/model-call failure；
- 0 manifest incident；
- exactly-once；
- 相同输入、chat template、token cap 和实际 output token 语义；
- 两 endpoint 元数据一致且均收到请求；
- traces 非空，run 后服务队列清空。

实际 output token 总量跨 arm 差异超过 1% 时，禁止只凭 JCT 声称加速。

### 9.2 性能门槛

相对独立标定后的 B1 和 B2 分别比较 B4：

- tokens/s 或 JCT 至少改善 5%；
- 至少 2/3 repeats 同方向；
- 最差 repeat 不退化超过 3%；
- P99、失败和 exactly-once 不退化。

相对独立标定后的 F1/F2/F3 分别比较 F4，使用相同 5%/2-of-3/最差 repeat
规则。只有这一组通过，才能声称自定义调度优于现有 Daft/Ray 官方执行能力。

若吞吐在 ±3% 内，但以下任一成立，可声称“压力效率/瞬态改善”，不能称为
“GPU 推理加速”：

- minimum saturating active work 降低至少 20%；
- P99 降低至少 10%；
- time-to-ceiling 或 ramp regret 改善至少 10%。

若 B2/B4 均能在相近并发达到 B0，且 B4 未过上述门槛，合法结论是：

> 对单 job 同质请求，强有界异步提交已经足够喂饱 vLLM；Daft/Ray 不提高
> 模型服务物理上限，其价值需要在多 job 隔离、公平性、异质 workload、
> 数据组织统一性或更低压力达到同吞吐上证明。

## 10. Fatal-flaw audit

| 风险 | 处理 |
|---|---|
| OceanBase 与 PostgreSQL 不同 | 同时保留 B2 同数据库因果 baseline |
| OceanBase AI function 版本/镜像不可用 | 先只读/最小 gate；失败则降为工业参考 |
| OceanBase 单 SQL 串行形成 strawman | 独立标定原生并行与多 session 强版本 |
| OceanBase 经云网关调用其他模型 | 只允许同机 vLLM URL；否则不进入数值矩阵 |
| Completions 与 Chat 混表 | 核心矩阵全部重跑 Chat |
| 不同 chat template/output 长度造成假加速 | 固定模板并审计真实 input/output tokens |
| Daft/Ray Data 默认参数过弱 | 每个官方系统独立 calibration，禁止默认值对已调优 ours |
| Ray Data `batch_size` 掩盖 multi-prompt HOL | 记录实际 HTTP body、请求数和每请求行数 |
| LOTUS 改写 prompt 或命中 cache | 先做 messages/token/hash 门禁；不等价则仅作定性参考 |
| 双 endpoint 客户端能力不同 | 统一预分片；动态 routing 另做消融 |
| 服务上限与数据库 E2E 混称 | B0、operator-only、database-e2e 分开报告 |
| “力大砖飞”掩盖策略价值 | 报告最小饱和 work、爬坡损失和队列压力 |

## 11. 实施顺序

1. 冻结 Chat Completions workload 与公共结果 schema；
2. 在本地/远端只做 OceanBase 版本、函数、endpoint、单行 SQL 可用性门禁；
3. 测 B0/B1/B2 的最小双 endpoint gate；
4. 测 B3/B4 Chat 协议适配门禁；
5. 测 F1/F2/F3 的 request-body、batch/concurrency 与 exactly-once 语义门禁；
6. 独立 calibration，冻结各 arm 参数；
7. 运行无 Daft/Ray 核心矩阵和官方框架矩阵的小作业瞬态实验；
8. 运行两层各自的 2,048 held-out 正式矩阵；
9. 结果按七步结构分析后，再决定是否运行 LOTUS、回到多 job 或设计新策略。

正式 AutoDL 操作继续遵循 `deploy/autodl/README.md`：远端操作前检查 runner、
lease、endpoint 和 git；使用全新输出目录；门禁未通过禁止 formal；保留失败
证据，不删除未跟踪结果。

## 12. 代码边界

- `workload_exporter`：生成不可变 Chat 请求与 hash；
- `baseline_adapters`：OceanBase、bounded AsyncIO、vLLM Bench、Daft prompt、
  Ray Data、可选 LOTUS 和 project runtime；
- `experiment_core`：配置展开、manifest、resume、同步启动与语义校验；
- `observation`：数据库、HTTP、vLLM、GPU 和 request lifecycle；
- `result_normalizer`：保留原始 artifact，只附加统一 schema；
- `deploy/autodl`：OceanBase gate、calibration 和 formal 模板分离。

不把 OceanBase 逻辑堆入现有 profiler 主脚本，也不复制项目 scheduler 代码给
baseline。
