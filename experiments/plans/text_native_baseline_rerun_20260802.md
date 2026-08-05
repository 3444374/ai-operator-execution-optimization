# 文本数据库 AI 算子原生 Baseline 复测合同

日期：2026-08-02
状态：64-row validity gate runner 与合同已准备；重构后的远端 smoke 已通过。calibration
和 held-out formal 目前只有机器可读的**预注册合同**，尚无统一 matrix runner，不得把
JSON 文件当成可执行命令。新 provenance 合同下的 calibration/formal 尚未执行。
服务器开关属于运行态，不写入长期计划状态。

baseline 的身份、证据等级和公共指标只以 `baseline_reference.md` 为准；本文只保留
AI_COMPLETE 的 Chat/Completions 执行合同，不另建总表。

## 1. 为什么文本阶段也必须调整

图像阶段暴露出的“使用某框架 API 不等于原生 baseline”问题同样存在于文本阶段。
旧 harness 把下列四种角色都叫作 official baseline，容易把服务上限、项目自写控制和
产品原生算子混为一谈：

| 角色 | 当前 arm | 能回答的问题 | 是否原生系统 baseline |
|---|---|---|---|
| 服务上限 | `vllm bench serve` | 两个 vLLM endpoint 的可实现服务容量 | 否；它是 ceiling |
| 直接客户端对照 | `bounded_http`、`bounded_completions` | 项目路径是否把服务喂饱；HTTP packing 的代价 | 否；它们含项目自写并发控制 |
| 框架原生 | Daft `functions.prompt` Native/Ray；Ray Data HTTP Processor | 不加入项目 credit/router 时，现有框架的执行能力 | 是 |
| 数据库产品原生 | DuckDB `ai` community extension；OceanBase SQL `AI_COMPLETE` | 已有数据库/扩展 AI 算子在同 endpoint 下的 E2E | DuckDB 只进入独立 bounded-output 轨且须先通过语义门禁；OceanBase 当前容器阻塞 |

代码现在为每个 run 强制写入 `comparison_role`、`implementation_provenance`、
`scheduler_owner`、`custom_scheduling_code`、`formal_baseline_eligible`、upstream source
和资格门禁。缺字段、或“原生 arm 同时含项目调度”会在 validity gate 中失败。

## 2. 复测要回答的五个问题

1. **服务容量**：固定模型、请求和双 endpoint 后，vLLM 官方 bench 的平台是多少？
2. **feeding overhead**：同协议 bounded control 能否达到官方 bench 的至少 95%？
3. **框架能力**：Daft Native、Daft Ray 和 Ray Data 的原生 API graph 在独立校准后，
   JCT、service tokens/s、尾延迟可观测性和资源效率分别怎样？
4. **数据库产品能力**：DuckDB `ai_try_complete` 与 OceanBase `AI_COMPLETE` 能否使用
   同一 vLLM endpoint 完成相同 SQL 语义？DuckDB 行级 `error` 或 NULL response 必须算
   失败；OceanBase 若不可部署，只报告 capability/blocker，不伪造替代实现。
5. **项目价值**：冻结项目最佳静态配置后，相对原生框架和 bounded control 在数据库
   E2E、多 job、SLO 或资源效率上增加了什么；动态策略只与冻结静态项目配置比较。

## 3. 两条协议轨道不能交叉排名

### Chat 产品/框架轨道（正式原生 baseline 主轨）

默认 ShareGPT 主轨：

`vLLM Bench → bounded Chat → Daft prompt Native/Ray → Ray Data HTTP → OceanBase（可用时）`

独立 bounded-output 产品轨：

`vLLM Bench/bounded Chat（同一 bounded-output manifest）→ DuckDB ai`

同一轨内所有 arm 必须是一行对应一次 Chat 请求、相同 messages、temperature=0、输出
上限、模型、chat template、endpoint 和 immutable manifest。两个轨道不能混表排名。

### Completions 机制轨道（项目内部机制轨）

`bounded multi-prompt Completions → 项目 fixed rows/token-budget/length-align/flush`

该轨目前没有同语义的数据库/框架原生 comparator。它用于解释 multi-prompt packing
与项目策略，不得用其 tokens/s 宣称超过 Chat 轨的 Daft、Ray Data 或 OceanBase。

Ray Data 的本地 `vLLMEngineProcessorConfig` 是有价值的可选原生轨道，但它把模型加载到
Ray Data actor 内，拓扑不同于“共享双 vLLM HTTP endpoint”。若加入，必须单独报告模型
副本、GPU 分配、冷启动和计时边界，不能直接并入 HTTP 排名。

### 产品/学术系统轨道（原生入口，不自行重写调度器）

| 候选 | 原生入口 | 当前用途 |
|---|---|---|
| OceanBase | 官方 SQL `AI_COMPLETE` + `DBMS_AI_SERVICE` | 能同机接相同 endpoint 时进入主排名；当前等待合适容器/VM |
| PolarDB-X / PolarDB | 官方 AI Function / 云产品实例 | 模型和硬件可冻结时比较 query E2E、质量、成本；否则只作 capability/外部官方 benchmark |
| LOTUS / Palimpzest / ThalamusDB | 作者官方仓库与 SemBench adapter | 只在 semantic map/filter/classify 语义、模型调用和质量合同对齐时进入 system-level quality-cost-time 轨 |

这些系统不能由本项目“照着论文再写一个简化版本”充当 baseline。可执行时固定官方
仓库/commit/入口，只允许数据路径、凭证、硬件和外部指标采集适配；无法同机运行时标成
external/capability evidence，不拿公开 raw time 与本机 tokens/s 排名。SemBench 类系统
可能通过减少模型调用换取不同质量，必须同时报告 F1/accuracy/nDCG、调用数、成本和
runtime，不能只比固定工作量吞吐。

## 4. 三阶段执行合同

### A. Validity gate

- 64 行 immutable manifest；双 endpoint 固定分片；每 arm 单次。
- exactly-once、0 failed/worker failure、两 endpoint 使用、服务配置一致、最终队列为空。
- vLLM 服务端 prompt/generation cumulative counter 前后差分必须为正且与可比较客户端
  token 统计一致。
- 主实验 vLLM prefix cache 开启；gate 同时核对声明值、同机服务进程参数以及可用时的
  query/hit counter。DuckDB response cache 关闭，避免缓存命中绕过模型调用；两类 cache
  不是一回事。
- 每个 cell 前先证明 vLLM 空闲，再取 counter snapshot；gate 采用 host-scope 互斥锁，
  双 shard 最长 900 秒，超时必须终止并留下 failed status，不能无限挂起。
- 每个 summary 必须有 provenance；原生 arm 不允许项目 credit、router、actor pool 或
  active-window。

入口仍为兼容文件名
`code/scripts/baselines/run_official_baseline_gate.py`，配置为
`deploy/autodl/dual_gpu_official_baseline_gate.example.json`。DuckDB 不进入默认 ShareGPT
core gate：服务器实测 256 cap 两端立即出现 length error，1024 cap 的 64 行仍有 length
error；其独立有界输出门禁使用
`deploy/autodl/dual_gpu_duckdb_ai_capability_gate.example.json`。这里的 “official” 只是历史
文件名；报告中的 scope 已改为 `text_comparison_validity_gate`。

### B. 独立 calibration

- 512 行 calibration manifest，1 warmup + 至少 2 repeats；各系统独立找运行点。
- vLLM Bench/bounded 扫 per-endpoint concurrency 32/64/128/256。
- Ray Data 扫官方 `batch_size × fixed concurrency`。
- DuckDB 只有在有界输出语义 gate 通过后，才扫扩展原生
  `duckdb_ai_max_concurrent_requests=8/16/32/64`；固定
  response cache=false、retry=0、rate limit=0。扩展运行于锁定 `duckdb==1.5.4` 的独立
  driver venv，不把项目 credit/router 注入 SQL 执行。
- Daft `functions.prompt` 当前公开 adapter 未暴露 `partition_count`；按 vendor default
  运行，禁止扫描一个代码没有接线的假参数。
- OceanBase 仅在 capability gate 通过后扫描其原生 SQL parallel degree。
- 选择“在正确性和 SLO guard 下最高 service tokens/s”；3% 峰值以内选资源更小的点。

预注册合同：`deploy/autodl/dual_gpu_official_baseline_calibration.example.json`。该 JSON
当前不由 `run_official_baseline_gate.py` 读取；正式 calibration runner 落地并通过单测前，
只能按 validity runner 的 `--include-cell/--concurrency-override` 做独立 screening，不能
称为完成了整套 calibration matrix。

### C. Held-out formal

- 2,048 条与 calibration 不重叠的行，目标每个 run 至少 60 秒；不足则停止排名，先为
  baseline 和 project 同时冻结更大的同一 immutable manifest，再共同增加 rows，不能只
  延长某一个 arm。
- 1 warmup + 3 formal repeats，arm 顺序做 balanced interleaving/randomization。
- 每个系统使用 calibration 冻结的自身最佳点；formal 阶段禁止按 arm 结果继续调参。
- native formal 合同只列 service/control/native comparator；project frozen-static 仍由
  project scenario runner 执行，但必须读取同一 2,048-row manifest、同一 Chat 协议和
  同一服务配置。两边任一 manifest SHA/rows/model/protocol 不一致时禁止合并排名。
- 结果目录：`experiments/results/text_native_baseline_formal_<timestamp_commit>/`，保存
  manifest/hash、resolved config、commands、版本、raw logs、requests、resource trace、
  service counters、失败证据和七步 README。

合同：`deploy/autodl/dual_gpu_text_native_baseline_formal.example.json`。该文件是 formal
预注册合同，不是现有 CLI 的输入；统一 formal runner、calibration selection 文件和
同 manifest 的 project-static 执行入口闭合前禁止运行或手工拼接结果。

## 5. 指标与可比边界

### 所有同机 arm 必记

- 身份：代码 commit、Daft/Ray/vLLM/PostgreSQL/OceanBase 版本、provenance、实际配置。
- 工作量/正确性：manifest hash、rows、prompt/output token 分位数、模型调用数、
  exactly-once、失败/重试、输出长度和抽样语义一致性。
- 时间/容量：JCT/operator E2E、requests/s、prompt/generation/total service tokens/s、
  first output/TTFT（可观测时）、P50/P95/P99 和 SLO goodput（逐请求可观测时）。
- 资源：CPU 利用率/core-seconds、RSS、网络字节、GPU utilization/MFU/显存/功耗/能耗、
  vLLM running/waiting/KV、Ray actor/task 数和 spill。
- 统计：每个 repeat 原值、中位数、CV、置信区间或误差条；失败 repeat 不删除。

Daft 当前只提供 shard barrier，不能把复制给每行的 barrier 时间冒充 request P99；此时
报告 JCT 和 service-counter throughput，并把 request tail 标成不可观测。不同 observability
级别不能硬凑同一张延迟排名表。

每个 shard summary 中的 `service_*_tokens_per_s` 是单 endpoint 速率。双 GPU headline
必须用“两端 token counter 差分之和 ÷ 两个并行 shard 的共同 group wall”重新计算；不能
把两个可能起止时间不同的 endpoint rate 直接相加。

## 6. 旧结果如何保留与是否需要重测

- 2026-07-29 的 64/256 行结果继续证明 adapter 可运行、请求等价和早期容量趋势；它们是
  gate/screening，不是论文正式排名。
- 2026-08-05 旧 DuckDB probe 把 `ai_try_complete.error` 丢弃并将 NULL response 标为
  completed，因此旧 DuckDB throughput 不具有效输出语义，只保留为故障证据；必须用
  新 gate 重跑后才能进入 calibration。
- `vLLM Bench` 继续作为服务 ceiling；`bounded_*` 重新标成直接客户端 control，不再称
  vendor-native baseline。
- Daft/Ray Data 旧绝对值需要重测：旧规模短、没有 60 秒稳态/交错三重复，而且 Daft
  曾被无效的 `partition_count` 配置误导。
- OceanBase 当前只保留 observer init `-9100` 的部署失败证据；服务器重开后也不应在普通
  AutoDL 容器反复消耗时间，应换 privileged/seccomp-unconfined 容器或 VM。
- PolarDB-X/PolarDB PostgreSQL 的 AI Function 属于云产品能力/外部官方 benchmark；无法
  固定同机模型和硬件时只作行业参照，不把云端 raw time 放入本机主排名。

## 7. 官方依据

- [vLLM Bench Serve](https://docs.vllm.ai/en/stable/cli/bench/serve/)
- [Daft AI Functions: prompt](https://docs.daft.ai/en/stable/ai-functions/prompt/)
- [Ray Data HttpRequestProcessorConfig](https://docs.ray.io/en/latest/data/api/doc/ray.data.llm.HttpRequestProcessorConfig.html)
- [Ray Data working with LLMs](https://docs.ray.io/en/master/data/working-with-llms.html)
- [OceanBase AI Functions](https://en.oceanbase.com/docs/common-oceanbase-database-10000000003678975)
- [DuckDB `ai` community extension](https://duckdb.org/community_extensions/extensions/ai)
- [PolarDB-X AI Functions](https://help.aliyun.com/en/polardb/polardb-for-xscale/ai-function)

## 8. 开机后的第一步

先执行远端只读状态检查、fast-forward 到本次提交、验证两个 endpoint/Ray/PostgreSQL
版本和空队列，再只跑 64 行 validity gate。gate 通过后停止并审计，不自动连跑 formal。
