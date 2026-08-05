# SQuAD v1.1 dev FULL 10570 gate — DuckDB-ai arm（2026-08-05，fail-closed FAILURE）

> **结果：fail-closed 触发，门禁 FAILURE（非 pass）。** 10570/10570 exactly-once、workload 完整性
> verified、三个 content hash 一致、attribution 可归因，但 **1/10570 行在 cap=64 下被 DuckDB-ai
> 当作 max_tokens 截断错误返回 NULL**，fail-closed（error/NULL → status=failure + 非零退出）正确触发。
> 这是 DuckDB-ai「truncation-as-error」产品语义的真实边界，不是 baseline 缺陷；按协议不抬 cap。

## 1. 实验目的

在 DuckDB-ai 单臂上跑**全量** 10570 行 SQuAD dev（cap=64），确认 cap 是否对整集零截断、
整集 EM/F1、以及 full-mode 下 fail-closed / 归因 / 三 hash 一致是否成立。
属 baseline 建设阶段（`experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md`）。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090（24564 MiB，driver 595.58.03） |
| 模型服务 | vLLM 0.25.1，单 endpoint `http://127.0.0.1:8000`，qwen2.5-7b，prefix-cache enabled，max_model_len 8192 |
| service-config-hash | `49cf2f803735b4a4`（= sha256[:16] of {model, max_model_len, 2×RTX4090, prefix-cache, vLLM 0.25.1}） |
| DuckDB | v1.5.4 + community `ai` 0.4.14；temp 0.0、max_concurrent_requests 32、cache off、retry 0 |
| 数据库 | PostgreSQL 18.4；pgvector **not_installed**（SQuAD workload 不用向量扩展，如实记录） |
| 模式/合同 | `--mode full` + `--strict-attribution` + `--metrics-settle-s 5` |
| 计时边界 | operator-only（database-E2E runner 仍未实现） |
| 代码/路径 | gate @ `d724edc`（含 fail-closed + server/pgvector 版本字段修复） |

## 3. 合规性自检

| 检查 | 结果 |
|---|---|
| workload 完整性 | ✅ verified：10570 行、unique doc_id、unique source_example_id、非空 reference_answers |
| 三个 content hash 一致 | ✅ `sample == workload == importer == 2c2301f2…`（full 模式选全集，故 sample hash==workload hash） |
| exactly-once（full-set） | ✅ result id set == input id set；10570/10570/10570 |
| vLLM counter 归因（`--strict-attribution`） | ✅ attributable：运行前后 idle、`request_success_delta == requests_sent == 10570`、counter 单调 |
| **行级错误/NULL（fail-closed）** | ❌ **1 error / 1 NULL（1 max_tokens error）→ status=failure，EXIT=1** |
| EM/F1 独立复算 | ✅ 从 `per_row_evidence.csv` 复算 = 报告值（80.32166508987702% / 89.36120269884813%） |

## 4. 实验设计

`--mode full` 选全部 10570 行，单臂、同 model、cap=64、prefix-cache on、temp 0.0、`--strict-attribution`。
较 v4（256 sampled）增加：全量 + strict 归因 + 真实 service-config-hash。
无对照臂（capability gate，不做跨臂对比）。

## 5. 实验数据

- **行级**：success 10569 / error 1 / NULL 1 / max_tokens error 1；exactly-once True
- **失败行**：`source_example_id = 572700c8dd62a815002e976d`，error `"AI provider response stopped
  because max_tokens was reached"`，output_chars 0（NULL）。参考答案均为 5–7 词（"Europeans who were
  based in Britain" 等）—— 正确答案远低于 64 token，模型在该题（temp 0.0 确定性）生成了 >64 token
  （rambling/loop），触发 DuckDB-ai 把 finish_reason=length 当行级 error → NULL。
- **语义质量（10569 成功 + 1 NULL 计 missing→0 进分母）**：EM **8490/10570 = 80.32166509%**、
  token-F1 **89.36120270%**、missing 1
- **vLLM 工作量（attributable）**：avg 5.71 gen tokens/row、prefix-cache hit-rate **0.7446**
- **operator-only 计时**：adapter_wall 92.831s、operator_only_jct 88.515s、setup 0.668s
- 证据：`report.json`（status=failure + failure_reason）+ `per_row_evidence.csv`（含 server_version /
  pgvector_version 列）+ `sample_manifest.jsonl`

## 6. 结果解释

- **事实**：全量 10570 在 cap=64 下，DuckDB-ai 对 **1 行**（572700c8…）返回 max_tokens 截断 NULL；
  其余 10569 行成功。fail-closed 据此将门禁标为 failure（EXIT=1）。整集 EM 80.32%、F1 89.36%
  （独立复算一致）。归因严格通过（request_success_delta==10570，endpoint 独占）。
- **判断**：这是 DuckDB-ai「truncation-as-error」产品语义在 SQuAD dev 上的真实边界——cap=64 不能
  100% 覆盖整集（1/10570 ≈ 0.0095% 截断率）。失败行不是 baseline 缺陷、不是数据问题，而是模型在该题
  生成超长（确定性）。整集质量信号（EM/F1）在扣除该 missing 行的口径下仍强劲。
- **不能声称**：full gate 「通过」（codex 的 0 error/NULL 标准未达）；cap=64 全集零截断；
  DuckDB-ai 比直连/项目更快或更慢；database-E2E 性能。
- **按协议**：不抬 cap 到 128 去「碰巧通过」（协议明令）；如实记录该 1 行边界。

## 7. 对课题含义 + 下一步

- **含义**：DuckDB-ai 单臂在 cap=64 下对 SQuAD dev 的覆盖率是 10569/10570；fail-closed 机制有效
  拦住了「带 1 个 NULL 当成功」的假象。整集能力（质量 + 归因 + 完整性）基本成立，唯一边界是 1 行
  模型超长生成。
- **待定（交 codex/用户裁决）**：(a) 接受 cap=64 的 1/10570 截断边界，把 full 标为「capability
  demonstrated with 1 documented truncation」；(b) 单独诊断该 1 行（更高 cap 单跑确认是长度问题）；
  (c) 其它。协议禁止靠抬 cap 过门禁。
- **更后**：database-E2E 顶层 runner（结构性缺口）→ 三臂正式对比。
