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
  because max_tokens was reached"`，output_chars 0（NULL）。**归因见 §8 审计订正**：定点诊断证明该行
  在 cap=64 孤立重放时正常完成（46 token，`stop`），故全量跑里的这次截断是**单次、机制未定的生成尾部
  事件**（并发/批状态是候选解释，未隔离验证），被 DuckDB-ai truncation-as-error 语义硬转成 NULL。
  该行不论 NULL 还是孤立重放的 46-token 文本，SQuAD 质量都是 0 分（EM=0/F1=0）——截断只影响可靠性
  指标，不改变该行质量贡献。
- **并发规模**：10570 行的 full-set query，**DuckDB 扩展最大并发 32**（不是 10570 同时并发）。
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
- **定性（经定点诊断订正）**：**不是基础设施或数据故障，而是 DuckDB-ai baseline 在统一 cap=64 下、
  10570-row full-set query（扩展并发上限 32）中可测量的产品语义限制。** cap=64 孤立重放该行正常
  （见 `feasibility/results/squad_truncation_diag_572700c8_20260805/`）；full-set 跑里观察到一次
  截断，**机制未定**（并发/批状态、prefix-cache 状态、请求顺序、扩展并发均为候选，均未隔离验证），
  DuckDB-ai 把 `finish_reason=length` 当行级 error → NULL。这是 baseline 的可靠性行为（1/10570，
  单次、不可复现），需要被评价，而非掩盖。
- **状态字段（分开）**：`capability_gate_status = failure`；
  `comparison_admission = eligible_with_documented_failure`；
  `formal_run_gate_passed = false`（zero-error validity gate 未通过）。失败 cell 完整保留，仍计 EM/F1、
  failure rate、successful/correct rows/s，但**不得冒充通过 validity gate 的 headline 性能**。
- **不能声称**：full gate「通过」（codex 的 0 error/NULL 标准未达）；cap=64 全集零截断；该行「确定性
  rambling」（已证伪）；**根因是 batching/浮点抖动/logit 翻转（未验证，仅候选）**；DuckDB-ai 比直连/
  项目更快或更慢；database-E2E 性能。
- **按协议**：不抬 cap 到 128 去「碰巧通过」（协议明令）；接受该产品边界。

## 7. 对课题含义 + 下一步（已裁决：接受边界）

- **含义**：DuckDB-ai 单臂在 cap=64 下对 SQuAD dev 的成功率 10569/10570；fail-closed 有效拦住
  「带 1 个 NULL 当成功」的假象。整集能力（质量 + 归因 + 完整性）成立，唯一边界是 full-set query 下
  1 行偶发截断（机制未定，产品语义）。
  截断（产品语义）。
- **裁决（用户 + codex）**：**(a) 接受并保留该产品边界**——full zero-error gate 维持 `FAILURE`（不改写
  为 pass）；baseline 标 `eligible_with_documented_failure`，允许进入**失败感知**的系统比较。
- **database-E2E 可继续建设**。正式三臂比较对所有 arm 统一报：success/error/NULL/truncation rate、
  全 manifest EM/F1（失败行按 0 分）、`successful_rows/s`、`correct_rows/s`、exactly-once 与完整失败
  证据；**不以 raw rows/s 单独排名**。

## 8. 审计订正与 provenance 说明（不覆写原始证据）

> 本节的 `report.json` / `per_row_evidence.csv` / `sample_manifest.jsonl` 是 `c20240e` 当时的机器原始
> 输出，**保持不变**；以下订正只写在 README，供读者校准。

- **失败行归因订正**：§5/§6 原写「模型 rambling/loop、temp=0 确定性」**不成立**。定点诊断
  （`feasibility/results/squad_truncation_diag_572700c8_20260805/`，`9aefeba`）证明 cap=64 孤立重放
  该行 3×3（direct vLLM + DuckDB `ai_try_complete`）全部 `stop`、46 token、文本一致；两条路径请求体
  **语义等价**（model/messages/temperature=0/max_tokens 一致；direct 多显式 `stream=false`，非字节相同）。
  截断不可复现 → 改为「full-set query（并发 32）中的单次、机制未定的生成尾部事件」；并发/批状态是候选
  解释，**未隔离验证，不主张根因**。该 46-token 文本本身 EM=0/F1=0（错答），故该行质量贡献不论 NULL
  或该文本都是 0 分——截断只影响可靠性指标。
- **pgvector 版本字段不可信**：`report.json` 里 `pgvector_version="not_installed"` 是探针 bug（当时
  查 `extname='pgvector'`，实际扩展名是 `vector`）。已修（`9aefeba`）；服务器实测 `vector 0.8.5`。
  不影响 SQuAD/EM-F1/截断结论，仅环境 provenance。
- **service-config-hash 不完整**：`49cf2f803735b4a4` 是若干手填字段（model/max_model_len/2×4090/
  prefix-cache/vLLM 版本）的摘要，**不是完整可反演的服务配置**。正式实验前须归档：实际 vLLM 启动
  命令、模型 revision、dtype、并行配置（max_num_seqs 等）、显存比例、环境快照。
