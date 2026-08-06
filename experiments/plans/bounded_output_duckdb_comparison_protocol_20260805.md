# Bounded-output DuckDB 对比协议（2026-08-05）

> 权威方法论。DuckDB `ai` 与项目系统的 bounded-output 产品对比按本文件执行。原 ShareGPT
> 多轮实验**保持不变**（仍讲项目内部策略/长输出/服务容量/动静态对比），只是不与 DuckDB 新结果
> 直接并表。本协议源于"DuckDB 把 `finish_reason=length` 当行级 error、与 ShareGPT fixed-cap
> 主轨语义不兼容"这一发现。

## 0. 出发点：从数据库 AI_COMPLETE 的实际工作方式出发

**不**从"找一个不会截断的任务"出发，而**从数据库 `AI_COMPLETE` 实际在做什么**出发。典型链路：

```text
数据库表中多行数据
  → SQL 扫描与 prompt 模板拼接
  → AI_COMPLETE 每行产生一次模型调用
  → 数据库/扩展控制并发、重试、缓存与错误
  → 外部模型 endpoint 生成文本
  → 数据库把文本或错误重新物化为查询结果行
```

- OceanBase `AI_COMPLETE(model_key, prompt, parameters)` 返回生成文本，官方直接用它做情感分类——
  说明"分类 prompt"仍是 AI_COMPLETE workload，**不要求实现独立分类引擎**。
- DuckDB community `ai`：`ai_complete` / 返回 `{response,error}` 的 `ai_try_complete`，自带并发/缓存/
  重试/限流与 usage/cost 记录。
- BigQuery `AI.GENERATE_TEXT`、Snowflake `AI_COMPLETE(show_details=>TRUE)`（返回 prompt/completion/
  total token usage）同理。

"数据库内执行避免数据搬运"指**避免用户整表导出到应用再逐行调用**；只要模型 endpoint 在数据库进程外，
数据库仍要序列化 prompt 并经 HTTP/RPC 发给模型服务。

**项目侧**：现有 AI_COMPLETE pipeline（PostgreSQL/manifest → Daft 读 prompt → Ray 组织提交 → vLLM →
`(doc_id, output)`）能跑问答/摘要/情感分类/信息抽取/JSON 生成。报告写"**AI_COMPLETE 驱动的分类/抽取
workload**"，**不能**说成实现了独立 `AI_CLASSIFY` 算子。

## 1. 任务划分

| 任务 | 作用 | 是否进正式排名 |
|---|---|---|
| ShareGPT 多轮生成 | 项目内部策略、长输出、服务容量 | 是，但**不含 DuckDB** |
| 句子计数 cap=16 | DuckDB 能力 + 极短输出调用开销 | **仅 microbenchmark** |
| **SQuAD 短答案 cap=64（固定）** | DuckDB / direct / 项目的**主 bounded AI_COMPLETE 对比** | **是（主）** |
| SST-2 / AG News 标签输出 | 数据库批量文本分析扩展验证 | 可选 |

**主对比用 SQuAD 短答案**，因为：仍是文本生成（不改变项目方向）、有公开 reference answer、可统计
Exact Match 与 token-level F1、输出天然较短、比"数句子"有真实语义。

> 未归档的 64 行 screening 观察：句子计数在 ShareGPT 多轮对话上 accuracy 仅 ~5%（regex 数出 11+ 句、
> 模型从不输出 11+），
> 因对话里"句子数"本身歧义——故只作 microbenchmark，accuracy 信号在该语料上无意义。

## 2. 共同指标（5 类，所有 comparator 同口径）

### 2.1 正确性与语义（进性能排名前的硬门禁）

输入行数、输出行数；exactly-once；success/error/NULL/truncation 计数；`finish_reason=stop` 比例；
无效格式比例；SQuAD EM/F1（或分类 accuracy/macro-F1）。输出 digest 只查执行一致性，**不能代替任务质量**。

SQuAD 三臂必须共用 `src.observability.metrics.squad_quality_metrics`：官方式英文归一化
（lowercase / ASCII punctuation / `a|an|the` / whitespace）、多答案分别取 max；缺失/失败行
以 0 分进入全 manifest 分母。统一输出 `squad_exact_match_rows`、
`squad_exact_match_percent`、`squad_token_f1_percent`、prediction/missing 行数和状态。
评估模块不含时间；`correct rows/s` 必须由同一计时边界的 JCT 与
`squad_exact_match_rows` 在 runner 汇总层计算。

**真正有意义的 headline**：

```text
correct rows/s
SLO-compliant correct rows/s
cost/correct row
```

**不能只看 raw rows/s。**

### 2.2 数据库查询边界

SQL/operator JCT；成功 rows/s；首行返回时间；P50/P95/P99（**仅当真实逐行完成时间可见时**记录）；
SQL scan / prompt 构造 / AI 算子 / 结果物化 各段计时；数据库 CPU core-seconds、内存峰值、网络发送/接收字节。

DuckDB 当前只暴露 query barrier → **不能**把整条查询 JCT 复制成每行 P95/P99，**也不能**伪造 TTFT。

### 2.3 模型服务工作量（统一从 vLLM 侧记）

prompt tokens；output tokens；total tokens；prompt/output/total tokens/s；running/waiting 请求；
KV cache usage；prefix-cache queries/hits/hit rate；GPU utilization、MFU、显存、功率、能耗。

DuckDB 摘要里 `output_tokens=0` 表示**扩展未向 runner 暴露**，不是模型没生成。正式共同指标**必须用
vLLM counter delta**，并与 DuckDB `ai_usage()` / `ai_usage_summary()` 交叉核对。

### 2.4 系统效率

`capacity_efficiency = 系统 service tokens/s ÷ direct-service tokens/s`；CPU core-seconds/correct row；
Joules/correct row；successful rows/CPU-core-second；GPU seconds/correct row；网络 bytes/correct row；
有显式价格时报 cost/correct row。

### 2.5 稳定性与扩展性

独立并发校准；1 warmup + 3 个交错 formal repeats；CV、95% CI；单次稳态**至少 60 秒**；
单 endpoint 产品轨稳定性；双 endpoint 系统轨扩展效率与工作量偏斜；retry/timeout/rate-limit/cache 状态。

### 2.6 endpoint 拓扑必须分轨，不能用“主机有两张 GPU”代替“实际用了两个 endpoint”

DuckDB `ai` 的公开配置是一个全局或 secret 级 `BASE_URL`；官方扩展页没有 endpoint-list、
round-robin 或 least-loaded 路由设置。上游 README 对多后端部署的建议是把这个单一 URL 指向用户自己的
gateway。依据：[DuckDB community `ai` 扩展页](https://duckdb.org/community_extensions/extensions/ai)、
[duckdb-ai 上游 README](https://github.com/leonardovida/duckdb-ai)。因此以下三条证据轨必须分开：

| 轨道 | 实际拓扑 | comparator | 回答的问题 | 允许的结论 |
|---|---|---|---|---|
| **单 endpoint 产品语义轨** | 三臂都只访问一个 vLLM endpoint；另一张 GPU 即使存在也不算已使用 | DuckDB `ai` 原生 SQL；direct control；单 endpoint project diagnostic | 数据库 AI_COMPLETE 的正确性、质量、失败语义和单 endpoint 集成开销 | 可比较产品语义/单 endpoint E2E；**不能**验证项目多 endpoint 方法 |
| **双 endpoint 方法/control 轨** | direct/bounded control 与所有 project arms 都访问同一对 endpoint | 强 direct control；冻结最佳项目静态 control；per-endpoint credit/路由/共享信用等候选策略；动态臂仅在预门禁通过后加入 | endpoint-aware 方法是否有增益 | 这是项目方法贡献的主证据；direct 是 causal control，不冒充数据库产品 baseline |
| **可选 gateway 完整系统轨** | DuckDB `ai` 以一个 `BASE_URL` 访问冻结的第三方 gateway，再由 gateway 访问两个 vLLM endpoint；项目直接访问两个 endpoint | DuckDB+gateway vs project multi-endpoint | 现实部署下完整系统的容量、成本和 SLO | 只能下系统级结论；gateway 版本、算法、开销和 scheduler owner 必须计入，不属于 DuckDB 原生 baseline，也不是当前 formal 前置 |

禁止为了凑“双 GPU 三臂”而在 Python/SQL harness 中把 DuckDB 输入行预切两半并并行跑两个独立
DuckDB 查询，再把它称为产品原生多 endpoint。该做法只能标作
`harness_sharded_diagnostic`，scheduler owner 是实验 harness，不进产品原生主排名。也不能让三臂都经过
同一个 gateway 后声称验证了项目 endpoint-aware 路由——那会再次把项目方法旁路掉。

## 3. 两个计时边界（必须分开，不可混比）

当前 DuckDB adapter 先从 manifest 拿请求、再在 DuckDB 内建临时表执行 AI 查询——更接近 **operator-only**，
不是完整数据库系统 E2E。故正式结果分两套：

1. **Operator-only**：所有系统 prompt 已备好；从 AI 算子开始 → 结果物化结束。用于比较 **DuckDB 扩展 / direct
   client / 项目调度层**。
2. **Database E2E**：从持久表扫描开始；含 Daft/DuckDB 读取、prompt 构造、模型调用、统一 sink。用于比较
   用户实际提交整条数据库作业的总时间。

**不能**拿 DuckDB operator-only JCT 与旧项目（含 PostgreSQL/Daft 数据读取的 E2E）直接比较。

> **实现状态（2026-08-05）**：operator-only 计时已在 DuckDB adapter 实现（`submitted→started` = setup、
> `started→completed` = operator-only）。**database-E2E 顶层 runner 三臂均已实现**
> （`code/scripts/baselines/squad_database_e2e_runner.py`）：duckdb_ai/direct_client 是进程内臂（一个计时墙包住
> 持久表扫描 → prompt 构造 → 模型调用 → 统一 sink `document_completions`），DuckDB 扩展拥有 batching/concurrency，
> runner 不注入项目 credit/actor/backpressure；**project_static 是 shell-out 臂**——runner 在通用 scan 前分流，
> 子进程调用 `postgres_ai_operator_profile.py` 跑显式冻结的静态合同（token budget、per-endpoint K、
> per-endpoint active-work、actor topology；profiler 独占 scan+organize+model+sink）。profiler 另行输出
> completion evidence 与实际 source-scan fingerprints，runner 用独立 DB 完整性/评分读取核对 scan 身份及 sink。
> 报告层记 `database_e2e_wall_s` 与 scan/construct/adapter/sink 分段；runner 层算 `correct_rows_per_s`（主 headline）、
> `successful_rows_per_s`、failure rate，并对所有臂统一报 `truncation_count`/`truncation_rate`；状态字段解耦为
> `single_run_valid` / `formal_run_gate_passed`（单次 runner 恒 false）/ `comparison_admission`（`pending_formal_repeat`）。
> **注**：PG 连接建立按连接池惯例算 setup（不计入 E2E 墙）；DuckDB 连接+扩展加载在 adapter 内、计入 operator 段；
> project_static 的计时段来自 profiler `--output` CSV（`e2e_s`→`database_e2e_wall_s` 等），与进程内臂结构不同。
> 当前该段包含 trace IO / metrics scrape / finish_job，不能与进程内臂的 wall 直接排名；在统一计时边界落地前，
> project_static 只能通过正确性/可运行性门禁，`comparison_admission=blocked_unified_timing_boundary`。

## 4. 请求等价门禁

利用 DuckDB 的 `ai_completion_request_json()` 保存**实际请求体**，核对它与项目路径的 model、prompt、
temperature、max_tokens、消息角色一致；再用 **vLLM prompt-token counter** 验证没有隐藏 system prompt。

否则即使两边数据集相同，实际送进模型的请求也可能不同（比较口径失效）。

## 5. 执行顺序

主路径（SQuAD，**不等待句子计数门禁**）：

1. **SQuAD 短答案 workload**（主 bounded 对比）：导入 SQuAD → prompt(context+question) + reference answer
   → manifest → 三个 comparator（DuckDB `ai` / direct client / 项目冻结最佳静态）。
2. 先完成**单 endpoint 产品语义轨**：三臂同 manifest、同 model、同一个 vLLM endpoint、prefix cache、
   **同 cap=64**、同计时边界，按 §2 五类指标 + §3 两边界 + §4 请求等价门禁执行。这里的
   `project_static` 只作正确性/管线开销 diagnostic；`per_endpoint` 在一个 endpoint 时退化为 global，
   不授予方法结论。
3. 项目最终优化方案确定后补第四臂。
4. **database-E2E 顶层 runner（三臂可执行；§3）**：`code/scripts/baselines/squad_database_e2e_runner.py`
   已覆盖 scan→construct→operator→unified sink 的 E2E 计时墙与 runner 层指标（duckdb_ai/direct_client 进程内，
   project_static 经 profiler 子进程）；**project_static 统一计时墙 + 同运行签名静态校准完成后**，才能进入多臂
   `1w+3f` 单 endpoint 正式结果。该 runner 只有 singular `--endpoint-url`，证据必须写
   `active_endpoint_count=1`、`multi_endpoint_method_exercised=false`；此前不发布完整数据库系统排名。
5. 双 endpoint 方法/control 轨使用现有多 endpoint project/control runner，先独立校准冻结静态 control，再比较
   per-endpoint credit、路由、共享信用等候选策略；动态臂只有通过既有晋级门禁才加入。不等待 DuckDB
   单 endpoint 轨即可做 correctness gate，但最终报告必须分表，不能把 DuckDB 单卡数与双卡数直接排名。
6. 第三方 gateway 完整系统轨是可选扩展：只有在需要研究现实部署组合时才先做 capability/请求等价/
   故障归因门禁并冻结 gateway；它不是 DuckDB 原生 baseline，也不阻塞 A/B 两条主证据轨。

非阻塞 microbenchmark（可与主路径并行，**不是 SQuAD 的前置门禁**）：

- 句子计数 capability：64 行 screening 的对话外观察尚未归档；2048 行门禁未完成（归档证据目前只到
  `feasibility/results/duckdb_ai_semantic_gate_20260805/`）。因其 accuracy 在 ShareGPT 对话语料上是噪声，
  只作链路开销 microbenchmark；想补完时再补，不阻塞 SQuAD 主路径。

可选扩展：

- SST-2/AG News 标签轨、中等输出轨（短输入摘要，须全 manifest 零截断预检；达不到则诚实记录 DuckDB 产品
  语义限制，**不继续抬 cap 直到"碰巧通过"**）。
