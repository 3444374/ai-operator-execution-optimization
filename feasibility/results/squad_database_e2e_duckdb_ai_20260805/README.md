# SQuAD database-E2E runner — DuckDB-ai arm（2026-08-05，单臂 E2E 测量）

> **角色：database-E2E 边界测量（单臂，非排名）。** `squad_database_e2e_runner.py`（`08d061c`）在
> DuckDB-ai 臂上跑通 scan→construct→operator→unified sink 的整段计时墙。`direct_client`/`project_static`
> 臂未实现，多臂齐全前不发布数据库系统排名。

## 1. 实验目的

补齐 `bounded_output_duckdb_comparison_protocol_20260805.md` §3 的 **database-E2E** 边界（持久表扫描 →
prompt 构造 → 模型调用 → 统一 sink），回答"用户提交整条数据库作业的总时间"在 DuckDB-ai 臂上到底花在哪。
runner 只做静态分片/计时/审计/写回；DuckDB 扩展继续拥有 batching/concurrency；不注入项目 credit/actor/backpressure。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090；hostname `autodl-container-a4884eab4c-22401421` |
| 模型服务 | vLLM **0.25.1**，单 endpoint 8000，qwen2.5-7b，prefix-cache enabled，max_model_len 8192 |
| DuckDB | v1.5.4 + community `ai` 0.4.14；temp 0.0、max_concurrent_requests 32、cache off、retry 0 |
| 数据库 | PostgreSQL 18.4 + **pgvector 0.8.5**（`vector` 扩展，探针已修）；workload `squad_v11_dev_short_answer`（10570） |
| 合同 | `--mode` full（runner 跑全集）、cap=64、`--strict-attribution`、`--service-config-hash 49cf2f803735b4a4`、`--metrics-settle-s 5` |
| 统一 sink | `write_completions(..., "json_text")` → `document_completions`，batch 500；tenant/category 从扫描行 sidecar（SQuAD: 0/'squad'） |
| 代码/路径 | runner @ `08d061c`；importer provenance `feasibility/results/squad_v11_dev_import_20260805/provenance.json`（content_hash `2c2301f2…`）；本目录 `report.json` + `per_row_evidence.csv` |

## 3. 合规性自检

| 检查 | 结果 |
|---|---|
| workload 完整性 + 三 hash 一致 | ✅ verified；`workload_content_hash == importer_content_hash == 2c2301f2…` |
| exactly-once（full-set） | ✅ result id set == input id set；10570/10570/10570；source_example_id 唯一非空 |
| 统一 sink 写回 | ✅ `document_completions` 写入 **10570 行**（category='squad'，psql COUNT 复核） |
| vLLM counter 归因（`--strict-attribution`） | ✅ attributable；运行前后 idle，`request_success_delta == requests_sent == 10570` |
| 行级 fail-closed | ❌ 1 error / 1 NULL（1 max_tokens）→ `status=failure`，EXIT=1（与 full capability gate 同源的偶发生成尾部事件，机制未定） |
| EM/F1 独立复算 | ✅ 从 `per_row_evidence.csv` 复算 = 报告值（80.32166509% / 89.41832377%） |
| 命令脱敏 + pgvector 探针 | ✅ `command` 脱敏（`postgres:***@`）；`pgvector_version=0.8.5`（探针修后正确） |

## 4. 实验设计

单臂（DuckDB-ai）、全集 10570、cap=64、prefix-cache on、temp 0.0、`--strict-attribution`。计时墙：
`t0 → scan（SELECT doc_id,text,tenant_id,category,source_example_id,reference_answers FROM documents）→
construct（建 ChatRequest + 完整性校验）→ scrape_before → run_duckdb_ai_complete（operator-only 时间戳保留）
→ sink（write_completions）→ t1`；settle + after-scrape 在墙外。`database_e2e_wall_s = t1−t0`。
PG 连接建立按连接池惯例算 setup（不计入墙）；DuckDB 连接+扩展加载在 adapter 内、计入 adapter 段。

## 5. 实验数据

**database-E2E 计时（秒）**
| wall | scan | construct | adapter(operator) | op_jct | sink |
|---|---|---|---|---|---|
| **93.899** | 0.143 | 0.231 | 93.212 | 87.835 | 0.258 |

> **关键观察**：DuckDB-ai 臂的 database 开销（scan+construct+sink ≈ **0.63s**）< E2E wall 的 **1%**——
> 模型调用（adapter）独占 99%。这是 DuckDB-ai 的 barrier 执行模型决定的（一条 set-oriented SELECT 跑完
> 10570 次模型调用，scan/sink 相对可忽略）。direct_client/project 臂的 E2E 拆分可能不同（待补）。

**runner 层指标**：`correct_rows_per_s` **90.4165**（主 headline）｜`successful_rows_per_s` 112.5573｜
`raw_rows_per_s` 112.568（不作排名键）｜`failure_rate` 0.000189｜sunk 10570。

**正确性/语义**：EM **80.32166509%**（8490/10570，missing 1）｜token-F1 **89.41832377%**｜
exactly-once True｜success 10569 / error 1 / NULL 1 / max_tokens 1。

**vLLM 工作量（attributable）**：prefix-cache hit-rate 0.7497。

## 6. 结果解释

- **事实**：DuckDB-ai 臂全量 10570 的 database-E2E wall 93.9s，其中模型调用 93.2s（99%），scan/construct/sink
  合计 0.63s（<1%）。整集 `correct_rows/s = 90.42`、EM 80.32%（独立复算一致）。1 行偶发 max_tokens 截断
  → fail-closed 标 failure（与 full capability gate 同源、机制未定的生成尾部事件）。
- **状态字段（分开）**：`capability_gate_status=failure` / `formal_run_gate_passed=false` /
  `comparison_admission=eligible_with_documented_failure`。zero-error validity gate **未被削弱**；失败 cell
  完整保留并仍计入 EM/F1、failure rate、successful/correct rows/s，但不冒充通过 validity gate 的 headline。
- **不能声称**：数据库系统排名（单臂）；DuckDB-ai 比直连/项目更快或更慢（无对照）；scan/sink 在所有臂都
  可忽略（仅本臂观测）；该 1 行截断「确定性」（已证伪，偶发）。

## 7. 对课题含义 + 下一步

- **含义**：database-E2E 边界在 DuckDB-ai 臂已可测、可归因、可复算；对本臂，E2E ≈ operator-dominated，
  上游 scan/sink 不是瓶颈。这给后续 direct_client/project 臂一个明确的对照基线（它们的 E2E 拆分是否也
  operator-dominated，还是会暴露 scan/sink/排队开销）。
- **下一步**：① 补 `direct_client` 臂（直连 vLLM，无 DuckDB 扩展）→ 看 E2E 拆分差异；② 补 `project_static`
  臂（项目冻结最佳静态）→ 形成三臂 E2E 正式排名；③ 正式前填全 `dual_gpu_squad_database_e2e.example.json`
  的 REPLACE_ME（vLLM launch cmd / revision / dtype / parallelism / VRAM / env），重算 `service-config-hash`。
