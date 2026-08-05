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
| **SQuAD 短答案 cap=64/128** | DuckDB / direct / 项目的**主 bounded AI_COMPLETE 对比** | **是（主）** |
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
单 endpoint → 双 endpoint 扩展效率；双 endpoint 工作量偏斜；retry/timeout/rate-limit/cache 状态。

## 3. 两个计时边界（必须分开，不可混比）

当前 DuckDB adapter 先从 manifest 拿请求、再在 DuckDB 内建临时表执行 AI 查询——更接近 **operator-only**，
不是完整数据库系统 E2E。故正式结果分两套：

1. **Operator-only**：所有系统 prompt 已备好；从 AI 算子开始 → 结果物化结束。用于比较 **DuckDB 扩展 / direct
   client / 项目调度层**。
2. **Database E2E**：从持久表扫描开始；含 Daft/DuckDB 读取、prompt 构造、模型调用、统一 sink。用于比较
   用户实际提交整条数据库作业的总时间。

**不能**拿 DuckDB operator-only JCT 与旧项目（含 PostgreSQL/Daft 数据读取的 E2E）直接比较。

> **实现状态（2026-08-05）**：operator-only 计时已在 DuckDB adapter 实现（`submitted→started` = setup、
> `started→completed` = operator-only）；**database-E2E 尚未实现**——连接创建/扩展加载、持久表扫描、
> Daft/DuckDB 读取、统一 sink 都属**顶层 runner** 职责，目前无代码。

## 4. 请求等价门禁

利用 DuckDB 的 `ai_completion_request_json()` 保存**实际请求体**，核对它与项目路径的 model、prompt、
temperature、max_tokens、消息角色一致；再用 **vLLM prompt-token counter** 验证没有隐藏 system prompt。

否则即使两边数据集相同，实际送进模型的请求也可能不同（比较口径失效）。

## 5. 执行顺序

1. 句子计数 capability：**64 行 screening 的对话外观察尚未归档；2048 行门禁未完成**（归档证据目前
   只到 `feasibility/results/duckdb_ai_semantic_gate_20260805/` 的 ShareGPT cap=256 43/64 失败 /
   cap=1024 1/64 失败 / 4 行 4/4 成功）。须先归档完整 2048 行零失败 + ground-truth accuracy 证据。
2. **SQuAD 短答案 workload**（主 bounded 对比）：导入 SQuAD → prompt(context+question) + reference answer
   → manifest → 三个 comparator（DuckDB `ai` / direct client / 项目冻结最佳静态）。
3. 三臂同 manifest、同 model、双 GPU、vLLM 同配置、prefix cache、同 cap、同计时边界，按 §2 五类指标 +
   §3 两边界 + §4 请求等价门禁执行。
4. 项目最终优化方案确定后补第四臂。
5. （可选）SST-2/AG News 标签轨、中等输出轨（短输入摘要 cap 128/256，须全 manifest 零截断预检；达不到
   则诚实记录 DuckDB 产品语义限制，**不继续抬 cap 直到"碰巧通过"**）。
