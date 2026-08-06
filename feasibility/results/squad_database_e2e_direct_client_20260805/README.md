# SQuAD database-E2E runner — direct_client arm（2026-08-05，单臂 E2E 测量）

> **角色：database-E2E 边界测量（direct_client 臂，单次 full single-shot，非正式排名）。**
> `direct_client` = 直连 vLLM `/v1/chat/completions`（httpx async + 固定并发 32），无 DuckDB 扩展。
> 与 DuckDB-ai 臂共享 PG scan / prompt / cap=64 / 模型配置 / 统一 sink。`formal_run_gate_passed=false`（单次 runner）。

## 1. 实验目的

补上 `bounded_output_duckdb_comparison_protocol_20260805.md` 三臂对比的第二臂：direct_client。
与 DuckDB-ai 的唯一差异 = 执行模型（per-request HTTP vs set-oriented barrier）；相同 endpoint/model/cap/concurrency/sink。
direct_client 暴露 `finish_reason` + `output_tokens` + per-request latency（DuckDB-ai 不暴露），
用于揭示两条路径对同一种截断事件的不同产品语义。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 主机有 2×RTX 4090；本臂**只激活 endpoint 8000 / GPU 0**，GPU 1 不属于本次实验资源；vLLM 0.25.1，qwen2.5-7b，prefix-cache enabled |
| arm | `direct_client`（httpx async + `asyncio.Semaphore(32)`，per-request，无项目 credit/backpressure） |
| 请求体 | direct_client 用 `build_completion_request_body`（chat_completions，temp 0.0，cap=64）；`request_equivalence_gate_20260805/` 验证两路径关键请求字段语义等价（DuckDB-ai 不复用该 builder，它有自己的 `ai_completion_request_json`） |
| 数据库 | PostgreSQL 18.4 + pgvector 0.8.5；workload `squad_v11_dev_short_answer`（10570） |
| 合同 | 全集 10570（无 --limit）、`--strict-attribution`、`--service-config-hash 49cf2f803735b4a4`、`--metrics-settle-s 5` |
| sink | `write_completions(..., "json_text")` → `document_completions`；content digest readback |
| 代码 | runner @ `28050e6`（`direct_client.py` + arm dispatch + arm-aware identity + --limit smoke） |

## 3. 合规性自检

| 检查 | 结果 |
|---|---|
| workload 完整性 + 三 hash 一致 | ✅ verified；`workload_content_hash == importer_content_hash == 2c2301f2…` |
| exactly-once | ✅ 10570/10570/10570 |
| 归因（`--strict-attribution`） | ✅ attributable；运行前后 idle，`request_success_delta == 10570` |
| **行级 fail-closed** | ✅ **0 error / 0 NULL → status=success**（截断行返回 partial text，非 error） |
| sink content readback | ✅ matched=True（10570 行 `(doc_id, completion_text)` digest 一致） |
| EM/F1 独立复算 | ✅ 从 `per_row_evidence.csv` 复算 = 报告值（80.21759697% / 89.32531727%） |
| 命令脱敏 | ✅ `postgres:***@` |

## 4. 实验设计

单臂（direct_client）、全集 10570、cap=64、prefix-cache on、temp 0.0、并发 32、`--strict-attribution`。
计时墙与 DuckDB-ai 臂完全一致：`t0 → scan → construct → scrape_before → run_direct_client → sink → t1`。
`run_direct_client` 用 `asyncio.Semaphore(32)` 固定并发，per-request `POST /v1/chat/completions`，
记录 `submitted`（队列前）→ `started`（获得 slot）→ `completed`（HTTP 响应）。
`_operator_span = max(completed) - min(started)`。

## 5. 实验数据

**finish_reason 分布**（direct_client 独有——DuckDB-ai 不暴露）：`{stop: 10569, length: 1}`。
**1 行 `finish_reason=length`**（截断），但 direct_client 返回 partial text（status=completed）→ 不触发 fail-closed。

**database-E2E 计时（秒）**：wall **91.872** = scan 0.133 + construct 0.228 + adapter 91.189（op_jct 90.909）+ sink 0.262。

**runner 层指标**：`correct_rows_per_s` **92.2913**（主 headline）｜`successful_rows_per_s` 115.0512｜
`raw_rows_per_s` 115.0512｜`failure_rate` **0.0**｜sunk 10570（readback matched）。

**正确性/语义**：EM **80.21759697%**（8479/10570）｜token-F1 **89.32531727%**｜exactly-once True。

## 6. 结果解释

- **事实**：direct_client 全量 10570 在 cap=64 下 **0 error / 0 NULL → status=success**。
  finish_reason 分布 `{stop: 10569, length: 1}`——1 行截断（length），但 direct_client 返回 partial text
  而非 NULL。E2E wall 91.9s（adapter 99.26%），`correct_rows/s = 92.29`，EM 80.22%（独立复算一致）。
  sink content readback matched=True。
- **与 DuckDB-ai 臂的对比**（核心发现）：

  | | DuckDB-ai full（c20240e） | direct_client full（本次） |
  |---|---|---|
  | status | **FAILURE**（1 NULL） | **success**（0 error/NULL） |
  | finish_reason | unavailable | `{stop: 10569, length: 1}` |
  | 截断处理 | `finish_reason=length` → DuckDB 扩展当 error → **NULL** → fail-closed | `finish_reason=length` → **partial text** → 非 error |
  | EM / F1 | 80.32% / 89.42% | 80.22% / 89.33% |
  | E2E wall | 93.9s | 91.9s |
  | correct_rows/s | 90.42 | 92.29 |

  **同一 source row（`572700c8…`，见 `squad_truncation_diag_572700c8_20260805/`）在两次独立 full 中都触顶
  cap=64**，两条路径给出不同的可靠性结论：DuckDB-ai 把它变成失败行（NULL），direct_client 把它变成
  截断但完成的行（partial text）。注意这是两次独立运行的同一行，不是同一次事件。EM 几乎相同（截断行
  两种口径都 0 分）；E2E wall direct 稍快约 2s（**单次观察，不能归因为"无扩展 barrier 开销"**）。
- **状态字段（解耦）**：`single_run_valid=true` / `formal_run_gate_passed=false`（单次 runner 恒 false）/
  `comparison_admission=pending_formal_repeat`。单次 E2E 测量，**非数据库系统排名**。
- **不能声称**：direct_client 比 DuckDB-ai 更快或更可靠（单次观测、偶发截断、不同语义口径）；
  scan/sink 在所有臂都可忽略（仅本臂观测）。

## 7. 对课题含义 + 下一步

- **含义**：direct_client 臂已可测、可归因、可复算、可读回。两臂的 E2E 拆分都 **adapter/operator-dominated**
  （本 direct 臂 adapter 占 wall 99.26%，含 per-request HTTP+排队+模型服务，不能单独归因给"模型调用"），
  scan/sink <1%。两臂的差异集中在"截断的产品语义"（NULL vs partial text）而非吞吐。
- **拓扑边界**：本报告属于**单 endpoint 产品语义轨**。它不能验证 per-endpoint credit、跨 endpoint 路由或
  双 GPU 扩展；主机上存在第二张 GPU 不等于实验使用了它。
- **下一步**：① 用 `single_endpoint_squad_database_e2e.example.json` 补齐同轨冻结配置与统一计时边界；
  ② 项目方法贡献在双 endpoint direct/bounded control vs 冻结静态/endpoint-aware 策略轨独立验证，不能由
  本报告外推；③ 第三方 gateway 只作可选完整系统轨，不是 DuckDB 原生 baseline 的前置。

## 8. 审计订正（codex direct_client 复核；不重跑、不覆写原始文件）

> 本节 `report.json` / `per_row_evidence.csv` 是 `535789d` 当时的原始输出，**保持不变**。以下订正
> 反映后续 runner 口径修复；正式三臂 rerun 时会直接产出订正后字段。

- **统一截断口径**：归档 `report.json` 的 `runner_metrics` 没有 `truncation_count`/`truncation_rate`；
  但从 `per_row_evidence.csv` 可复算（finish_reason=='length' 的行数 = 1）。订正后的 runner 对所有臂
  统一报 `truncation_count = count(finish_reason=='length' OR error contains 'max_tokens')`，
  使三臂比较不会误读 direct 的 `failure_rate=0` 为"无截断"。
- **per-request latency**：归档 CSV 没有 `submitted_at_s` / `started_at_s` / `completed_at_s` / `queue_wait_s` /
  `latency_s` 列。订正后的 runner 对每行记录这些时间戳，可算 P50/P95/P99（direct 臂是真正的 per-request；
  DuckDB 臂所有行共享 barrier 时间戳 → P50=P95=P99 = barrier span）。
- **共享 timeout**：归档 direct 用 timeout=180s，DuckDB 用 120s（不一致）。订正后用共享 CLI
  `--request-timeout-s`（默认 120s），两臂必须冻结同一值。
- **README §2/§6/§7 措辞订正（已直接改正文）**：以下 4 处措辞 codex 复核后要求改正，现已直接修在
  §2 请求体行、§6 对比段、§7 含义段，§8 仅留更正记录：
  - 是"同一 source row（`572700c8…`）在两次 full 中都触顶"，不是"同一个事件"（两次独立运行）。
  - 2s 差异只是**单次观察**，不能归因为"没有扩展 barrier 开销"。
  - 两臂是 **adapter/operator dominated**，不能写"模型调用 >99%"。
  - DuckDB **不使用** direct 的 `build_completion_request_body`；只能说请求等价门禁
    （`request_equivalence_gate`）验证了两路径关键字段语义等价。
- **sunk_status.csv**：本次跑（`535789d`）的服务器产出目录有 `sunk_status.csv`，但之前只取回了
  `report.json` + `per_row_evidence.csv`。现已补取（10570 行）。
- **provenance（订正）**：direct_client 复用已有角色 `comparison_role="direct_client_control"`（非 database product
  baseline）；因为它使用项目自写的 `asyncio.Semaphore(32)` 固定并发，`custom_scheduling_code=True`、
  `scheduler_owner="project_asyncio_semaphore_control"`、`formal_baseline_eligible=False`、`formal_control_eligible=True`。
  上一版误登记为不存在于 `ComparisonRole` Literal 的 `"direct_service_control"` 且 `custom_scheduling_code=False`
  （掩盖了项目调度代码），已订正。runner 现在对每个 arm 调 `adapter_provenance()` 并把 `summary_fields()` 写进
  每次 `report.json`；归档 `report.json`（`535789d`）早于此修复、无 `provenance` 字段，需正式 rerun 落盘。
- **smoke 空集**：`--limit` smoke 的 `_smoke_integrity` 已加空集拒绝（`all([])` 不再真空通过）。

## 9. 服务器副本 SHA 差异复核（2026-08-06）

- 服务器仓库受控文件 SHA256：`report.json=97dd6094…`、`per_row_evidence.csv=249aea93…`、
  `sunk_status.csv=8d00b78b…`。仓库外备份 `/root/autodl-tmp/evidence_backup_direct_client_1785981777/`
  中 `report.json` 字节 SHA 相同；两个 CSV 的原始字节 SHA 分别以 `9532…`、`fc05…` 开头，因而最初被误报为
  “不是同一次产出”。
- 复核同时用 Python `csv.DictReader` 解析两份 CSV：两侧均为 **10,570 行**，逐行字典完全相等，字段差异数
  **0**。唯一差异是服务器备份使用 CRLF 记录分隔符，Git 受控版本使用 LF；带引号字段里的内嵌换行不影响
  CSV 语义。
- **判决**：这是换行规范化导致的 byte-level SHA 差异，不是结果、顺序或 provenance 差异；不需要为此重跑。
  后续证据审计同时保存 `raw_file_sha256` 与基于解析记录的 `semantic_record_sha256`，禁止只凭原始文件 SHA
  推断运行身份。
