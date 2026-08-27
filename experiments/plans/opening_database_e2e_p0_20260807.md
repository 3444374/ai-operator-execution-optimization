# 开题前统一 database-E2E 文本三臂计划

冻结日期：2026-08-07

状态：**主矩阵已完成；ShareGPT C128 双臂纠正补测为条件性待办，不是当前优先项。**
2026-08-08 替换重跑的 24/24 单元均通过 source/sink、identity、exactly-once 和稳定性检查；
SQuAD 三条静态路径可作均匀短输出的完成性与质量控制；项目路径计时还多含指标采集、记录写入
和结束处理，因此不到 1% 的时间/吞吐差异不排名。ShareGPT 只保留运行现象、正确性和产品语义
证据，不作方法性能排名。权威结果见 `experiments/results/opening_database_e2e_text_refeed_20260808/README.md`。
第 9 节仅在 LOTUS/PostgreSQL 资格项完成且双 RTX 4090 环境恢复后补 direct C128 与 project
K128/W65,536；不重跑 DuckDB、SQuAD 或已有容量扫描。

## 1. 准入问题

| 问题 | 答案 |
|---|---|
| 支持开题中的哪句话？ | 给出数据库触发后，从 PostgreSQL source 到统一 sink 的三种静态执行路径在均匀与异质 workload 下的可比数据；判断异质 work 是否会放大上游组织差异。 |
| 不运行会缺少什么？ | 现有结果无法支持数据库产品、直接控制和项目静态路径的统一 database-E2E 排名，也没有相同三臂的异质 workload 对照。 |
| 为什么现有正式结果不能回答？ | scale-ramp 的 gate 臂是 request/query-barrier 口径，project 臂的旧 `e2e_s` 还包含模型循环后的证据文件和指标抓取；source、sink、质量和 per-row 合同也未统一。 |

## 2. 冻结系统合同

| 项 | 冻结值 |
|---|---|
| 平台 | AutoDL 双 RTX 4090；实际 PostgreSQL 与 pgvector 版本逐 run 记录，当前预演为 PostgreSQL 18.4 + pgvector 0.8.5，不冒充目标 PG18.3 |
| 模型服务 | 两个独立 vLLM endpoint，各绑定一张 GPU；Qwen2.5-7B；raw Chat Completions；temperature=0；prefix cache ON；`max_num_seqs=256`；`max_num_batched_tokens=8192`；`max_model_len=8192`；`gpu_memory_utilization=0.90` |
| source | 同一 PostgreSQL `documents` 表、同一 `workload_name`、`ORDER BY doc_id`；每个 measured cell 都在 E2E 计时内重新扫描并校验 frozen manifest |
| sink | 同一 PostgreSQL `document_completions`；每个 cell 写入前按本次 doc-id 集合清除旧行，写后执行 count + `(doc_id, completion_text)` digest readback；清理不计入 E2E |
| request set | 两 endpoint 的 immutable JSONL manifest；`equal_rows`，seed=20260807；manifest SHA、sidecar、row count、每 endpoint work 逐 run 记录；三臂必须相同 |
| 并发 | direct 与 DuckDB AI 每 endpoint 32；project 首轮 K=32、8 actors × concurrency 4 未过 feeding 门。纠正重跑只采用校准后冻结的最小饱和 K/actor slots；不得在 formal 在线调参 |
| project static | token budget=6144，首轮 active work=65,536/endpoint，request-level，manifest-pinned routing，httpx async，固定 50 ms flush 标签沿用冻结静态合同；纠正校准统一使用 8×32=256 actor slots 并固定除 K 外全部变量，扫描 K32/64/128/256（包含既有正式合同 K256）；若四档均不满足才固定最佳 K 后单独扫描 active work |
| 重复 | 每 workload 每 arm 1 warmup + 3 formal；按确定性随机顺序交错执行；warmup 不进 headline |
| headline | `correct_rows / database_e2e_s`；同时报告 raw rows/s、database-E2E、service tokens/s、request latency、TTFT、failure、truncation、GPU、MFU、能耗和 sink 门禁 |

三臂固定命名：

- `direct_static_sharded`：项目实现的 bounded HTTP 静态直接控制；不是数据库产品。
- `duckdb_ai_static_sharded`：experiment harness 先按 manifest 切成两个 shard，再由两个独立 DuckDB AI extension 进程分别拥有本 shard 的 set-oriented 执行。它是 DuckDB AI 组件对照，但不是 DuckDB 原生多 endpoint 能力，不写成“产品原生双卡调度”。
- `project_frozen_static`：项目冻结的 Daft organizer + Ray actor + per-endpoint static K/work credit 路径；本轮不运行任何 adaptive/state-aware 策略。

## 3. P0-1 均匀控制组

- workload：`squad_v11_dev_short_answer` 全量 10,570 行。
- output cap：64。
- output work：manifest 使用 `fixed_cap`，防止真实输出泄漏到执行前分片。
- 质量：SQuAD v1.1 normalized EM、token F1、correct rows、空结果、失败、`finish_reason=length`；DuckDB 若不暴露 finish reason，明确标为 unavailable，不能按 0 处理。
- 目的：建立统一 database-E2E 三臂控制组，不预设性能排序。

## 4. P0-2 异质实验组

- workload：`sharegpt_multiturn` 全量 2,048 行，不再导入新数据。
- 冻结构成：按数据库 `prompt_tokens` 分桶，short `<256` 为 538 行，medium `256..1024` 为 1,175 行，long `>1024` 为 335 行；阈值与自然比例不因结果改变。
- output cap：256，降低 DuckDB AI 因 `finish_reason=length` 返回结构化失败的比例；manifest 的 estimated output 使用 `fixed_cap`，避免未来信息泄漏。该 cap 与控制组不同，因此跨 workload 只比较机制变化，不把绝对吞吐变化归因于 heterogeneity 单因素。
- 必报 workload 描述：prompt、target-output、estimated-work 的 histogram，P50/P95/P99、均值、标准差、CV；三桶行数和比例。
- 质量/有效性：exactly-once、非空完成、failure、finish reason、truncation 可观测性和实际 output-token 分布；该 workload 没有 SQuAD reference，不伪造 EM/F1。
- 追加机制指标：endpoint estimated/observed work imbalance，request P50/P95/P99，TTFT/ITL，cache hit，active work，running/waiting/KV，GPU/MFU 与 energy/correct row。

## 5. 统一 E2E 边界

```text
timer start
  PostgreSQL source scan
  manifest identity/content validation
  arm-owned model execution on two pinned endpoints
  unified PostgreSQL sink write
timer stop
```

timer 之外只允许：runner preflight、endpoint idle、旧 sink 行清理、metrics before snapshot、结果证据 CSV/JSON 写盘、metrics after settle/scrape、sink readback、聚合与画图。project profiler 使用 opt-in clean boundary，在 operator result 完整性检查后立即写 sink并停止计时，再写 trace/evidence 和抓取 after metrics；默认 profiler 时序保持不变。

## 6. 每个 formal cell 的 fail-closed 门禁

1. 两 endpoint 健康、身份和 service flags 与冻结合同一致；cell 前后 idle；同一时刻只有一个 experiment runner。
2. PostgreSQL 扫描行数、doc-id 集合、prompt、token metadata 和 manifest SHA 完全一致。
3. exactly-once，两个 endpoint 都有请求，0 worker/transport failure，sink count/content digest 一致。DuckDB AI 的 cap 语义失败不是基础设施失败，但必须进入总行数和 correct rows/s 分母并单独报告，不能删除。
4. `gpu_utilization_pct_mean`、running/waiting/KV 使用 during-cell time series；不用单点 `gpu_utilization_pct` 下结论。
5. feeding-saturation：E2E service tokens/s 至少达到同协议 bounded control 的 95%。若 DuckDB 的 query-barrier 不能提供 per-request finish reason/latency，字段标 unavailable，不虚填 0。
6. 三个 formal repeat 的 CV 和离群点透明报告；不因某臂表现差而删 run。服务崩溃、计时边界不一致、manifest/sink 门禁失败的 cell 丢弃并原配置重跑，保留 incident。

## 7. 输出与停止规则

- 结果目录：`experiments/results/opening_database_e2e_text_20260807/{README.md,raw/}`。
- `raw/` 保存在服务器 artifact root；Git 仅提交去敏、必要、体积受控的正式汇总和重建图表所需证据。
- 报告按项目八段结构：目的、设置、合规自检、设计、全组件数据、解释、课题含义、下一步。
- 两组完成后立即停止开题 baseline。小于 5% 的差异是有效结果，不触发第二数据库、更多文本引擎、更多 workload、模型替换或 scale × concurrency 扫描。

## 8. Feeding corrective rerun（2026-08-08）

首轮同时通过正确性与资源门禁，但项目臂未达到同协议 direct 的 95% service-token feeding 门，因此按下列预注册顺序纠正，不删除或覆盖首轮证据：

1. 重启后重新执行 runtime preflight，核对相同 2×4090、Qwen2.5-7B、Chat、prefix-cache ON、8192/256 service capacity、PostgreSQL source/sink 和 immutable manifest。
2. 使用 `deploy/autodl/opening_project_feeding_calibration.example.json`，对每个 workload 分别固定 token budget=6144、active work=65,536/endpoint、8 actor 与每 actor concurrency=32（每 endpoint 256 slots），只扫描 per-endpoint K `{32,64,128,256}`；K256 是既有正式合同点，不能从峰值参照中省略。bounded direct 固定每 endpoint 32，所有 cell 使用同一 manifest 并在测量前同协议 cache conditioning。此前 8×16 的 SQuAD 三档结果与未完成的 ShareGPT 预热只保留为诊断，不参与冻结。
3. 每档三个成功重复。候选必须满足 exactly-once、0 failure、双 endpoint、最终空队列；以 repeat 中位数选择同时达到该 workload 已测项目峰值 97% 且达到同 run direct service tokens/s 95% 的最小 K。单次峰值不能晋级。
4. 若 K 四档均不满足 feeding，固定最佳已测 K，再单独扫描 active work；禁止同时改变 K、active work、token budget 或 actor 数。
5. 两个 workload 的校准签名分别冻结；正式 runner 可按 workload 使用各自选择，但每个 workload 的三臂 formal 期间参数不变。
6. 冻结后以新 experiment ID 和新输出目录完整替换重跑两 workload × 三臂 × 1 warmup + 3 formal。新矩阵通过 correctness、feeding、stability 后才更新报告/PPT/飞书；首轮继续标为 failed-feeding diagnostic。

## 9. ShareGPT C128 database-E2E 双臂纠正补测（2026-08-24，待服务器执行）

### 9.1 补测问题与证据边界

| 问题 | 预注册答案 |
|---|---|
| 为什么需要补测？ | 现有 ShareGPT database-E2E 中 direct 每个服务实例为 C32，project 为 K128/W65,536。后续容量扫描证明 C32 只达到已测最高服务吞吐的 52.07%，因此 180.332 s 与 116.703 s 同时包含请求容量和执行结构差异。 |
| 现有 C128 结果为什么不能直接替代？ | 同一 manifest 的 bounded C128 已完成 1 warm-up + 3 formal，JCT 95.49 s、service throughput 17,800.21 tok/s，但其边界不含 PostgreSQL source scan 与统一 sink，不能填成 database-E2E 时间。 |
| 本次补测回答什么？ | 在每个服务实例都允许 128 个未完成请求时，观察 direct static 与 project frozen-static 的 PostgreSQL scan → 模型执行 → PostgreSQL sink 完整路径时间、服务吞吐和资源状态。 |
| 本次补测不回答什么？ | 不把两条不同执行结构的时间差归因于某个数据组织或调度方法；不恢复 DuckDB 的 ShareGPT 排名；不声称 C128 可跨机器、模型或 workload 复用。 |

DuckDB 不进入本次补测，原因彼此独立：其 `ai` 扩展当前并发配置上限为每实例 64；更重要的是，达到 256 词元上限时 `ai_try_complete` 返回空结果，而另外两条路径保留截断文本。提高 DuckDB 并发不能修复输出含义不一致。

### 9.2 两臂与固定合同

| 项 | 固定值 |
|---|---|
| 平台 | 与 2026-08-08 结果相同签名：2× RTX 4090、每卡一个 Qwen2.5-7B vLLM endpoint；实际 driver、vLLM、PostgreSQL、pgvector 与 git commit 逐 run 记录 |
| 服务 | raw Chat Completions、temperature=0、output cap=256、prefix cache ON、`max_num_seqs=256`、`max_num_batched_tokens=8192`、`max_model_len=8192`、`gpu_memory_utilization=0.90` |
| workload | `sharegpt_multiturn` 2,048 行；沿用 equal-row 双 endpoint manifest，SHA256=`54c97a2f10347d35b15ac5442da116dd7a8c56a8ef05e36c76a7120da783169b`；不得重导、重分片或修改输出上限 |
| direct static | `direct_static_sharded`；每 endpoint C128，batch=1，固定 semaphore；httpx `max_connections=max_keepalive_connections=128`；无 active-work credit |
| project static | `project_frozen_static`；每 endpoint K128、W65,536、token budget 6,144、8 actors × concurrency 32、request-level completion release、manifest-pinned routing、fixed 50 ms；不启用 adaptive/state-aware 策略 |
| source/sink 与计时 | 完全复用第 5 节：每 cell 在计时内重新扫描 PostgreSQL、校验 manifest、完成两 endpoint 执行并写入统一 `document_completions` sink；清理旧 sink 和写后 readback 位于 timer 外 |
| 其余执行参数 | `timeout_s=180`、`write_batch_rows=500`、`service_conditioning_before_cell=true`、estimated output=`fixed_cap`；正式运行期间保持不变 |
| 重复 | 两臂各 1 warm-up + 3 formal；按 seed=20260824 确定性地交错臂顺序，共 8 cells、6 formal；不从 warm-up 选配置 |
| 主要指标 | database-E2E s、correct/raw rows/s、service prompt+generation tok/s、request P50/P95/P99、TTFT/ITL、running/waiting/KV、prefix hit、GPU utilization、MFU、能耗、source/sink/exactly-once |

本实验只把**请求数上限**匹配到每 endpoint 128。project 的 W65,536 仍是其冻结物理路径的一部分，因此补测后只能比较两条完整静态路径，不能宣称已经做成“只有组织或调度方法不同”的单因素实验。

### 9.3 服务器开机前的最小工程准备

只扩展现有 runner 的配置能力，不复制 source/sink、指标采集或请求实现：

1. 修改 `code/scripts/baselines/opening_database_e2e_matrix.py`：
   - 新增 `enabled_arms`，默认仍为原三臂，且只接受 `ARMS` 的非空无重复子集；
   - 新增 `direct_concurrency_per_endpoint`，默认回退到旧 `concurrency_per_endpoint=32`；旧三臂模板的行为和结果合同保持不变；
   - direct 使用独立的 C128；仅在 `duckdb_ai_static_sharded` 被启用时才检查 DuckDB runtime，并继续要求其并发不超过 64；
   - arm 交错顺序只对 `enabled_arms` 生效；preflight、run summary 与每个 cell report 显式保存两臂列表和各自配置；
   - direct report 记录配置连接池上限，并从逐请求 started/completed 区间复算每 endpoint client peak inflight，防止“名义 C128、实际被连接池截断”。
2. 新增 `deploy/autodl/opening_sharegpt_c128_database_e2e.example.json`：只包含 ShareGPT、direct C128 和 project K128/W65,536；output root 必须来自新的环境变量，禁止覆盖 2026-08-08 目录。
3. 更新 `code/tests/baselines/test_opening_database_e2e_matrix.py`：覆盖旧三臂配置不变、两臂子集顺序稳定、direct override=128、禁用 DuckDB 时不执行 DuckDB preflight、未知/重复/空 arm fail-closed、direct C128 确实传入 `DirectClientConfig`。
4. 新增独立汇总入口 `code/scripts/analysis/summarize_opening_sharegpt_c128_database_e2e.py` 及对应测试。它复用现有 cell report 字段，只审计 1 workload × 2 arms × (1 warm-up + 3 formal)，把 project/direct feeding ratio 作为观察结果，而不把“project 必须达到 direct 的 95%”误写成该次运行是否有效的正确性条件。
5. 工程改动在本地通过上述测试后再提交；服务器只 checkout 已提交版本，不临时复制 JSON、手改 runner 或补丁式拼接 shard。

### 9.4 服务器恢复后的执行顺序

首先按 `deploy/runtime/AGENTS.md`、`deploy/runtime/README.md` 和 `deploy/autodl/README.md` 恢复环境并保存只读机器报告：

```bash
PYTHONPATH=code "$DRIVER_PYTHON" \
  code/scripts/environment/manage_environment.py check \
  --groups core,text,analysis \
  --json-out \
  "$ARTIFACT_ROOT/opening_sharegpt_c128_database_e2e_environment.json"
```

随后完成 PG、Ray、两个 vLLM endpoint、GPU、host runner lease 和最终空队列检查；若服务器重启导致 Ray 地址文件陈旧，按 runtime runbook 处理，不沿用另一台机器的参数。正式请求发出前检查 resolved config 必须同时满足：

```text
enabled_arms = [direct_static_sharded, project_frozen_static]
workload = sharegpt_controlled_skew, rows = 2048, max_tokens = 256
direct_concurrency_per_endpoint = 128
project K/W/token_budget/actors = 128/65536/6144/8x32
manifest_sha256 = 54c97a2f...3169b
output_root = 全新目录
```

运行入口冻结为：

```bash
PYTHONPATH=code "${VENV_ROOT}/text-baselines/bin/python" \
  code/scripts/baselines/opening_database_e2e_matrix.py \
  --config \
  deploy/autodl/opening_sharegpt_c128_database_e2e.example.json

PYTHONPATH=code "${VENV_ROOT}/text-baselines/bin/python" \
  code/scripts/analysis/summarize_opening_sharegpt_c128_database_e2e.py \
  --input-root "$OPENING_SHAREGPT_C128_OUTPUT_ROOT" \
  --output-dir "$OPENING_SHAREGPT_C128_OUTPUT_ROOT/summary"
```

正式运行期间只允许这一个 runner 使用两个 endpoint。任何 infrastructure failure 都保留原目录和 incident；确认原因后只按完全相同配置在新目录重跑，不覆盖、不删差点，也不因结果方向修改 K/W、manifest 或臂顺序。

### 9.5 有效性检查与结果判定

每个 cell 必须满足：2,048/2,048 行 exactly-once、0 transport/worker failure、两 endpoint 均有请求、sink count/content digest 一致、direct/project 都保留 `finish_reason=length` 的截断文本、cell 前后服务为空。direct formal 还必须记录每 endpoint client peak inflight=128，且连接池两个上限均为 128；否则该 cell 仍属于欠供给诊断，不进入两臂比较。

整个正式集合必须满足：8/8 cells、6/6 formal 完成；每个 formal 至少 60 s；两臂 database-E2E 与 service tok/s 的三次 sample CV 分别不超过 5%；direct C128 三次中位 service tok/s 至少达到既有同签名正式均值 17,800.21 tok/s 的 95%，即 16,910.20 tok/s。若 direct 未达到该复现下限，先检查服务签名、其它流量、连接池和 endpoint 不对称，本轮只记诊断，不比较路径时间。

project/direct 的 service ratio **不是本实验有效性的通过条件**，而是结果本身。报告三次单值、均值、中位数、sample SD/CV、配对 repeat 的差值，并按以下边界解释：

- direct C128 明显快于或接近 project：说明旧 116.703 s 相对 180.332 s 的表面优势主要受 C32 容量不足影响；不得反写成 project 方法负收益，除非另做同执行器单因素 A/B。
- project 仍快于 direct C128：只能称当前两条完整静态执行路径存在条件性差异；由于 Daft/Ray、token-budget 和 W65,536 同时存在，不能归因于某一数据组织或调度机制。
- 两臂 database-E2E 差异绝对值小于 5%：按预注册描述为当前签名下近似中性，不追加并发扫描追正。

无论结果方向如何，都不重跑 DuckDB、不增加 C64/C256、不更换 workload/模型/数据库、不调 project K/W/actor，也不把该补测升级为新的开题核心贡献。结果目录名由固定前缀 `opening_sharegpt_c128_database_e2e_` 加实际服务器运行日期组成，目录内保存 `README.md` 与 `raw/`；随后再更新旧结果报告、`opening/claim_matrix.md`、开题正文和 `PROJECT_LOG.md`，并明确旧 116.703 s 仍是有效历史配置结果。
