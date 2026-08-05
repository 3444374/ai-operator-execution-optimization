# SQuAD v1.1 dev capability gate v4 — DuckDB-ai arm（2026-08-05，canonical sample）

> **角色：能力门禁，不是正式排名。** 单臂（DuckDB community `ai`）、operator-only 计时边界。
> database-E2E 顶层 runner 尚未实现，不产生数据库系统级排名。
> **v4 是 codex 第七轮修复（SQuAD-normalize 分桶 + sample_manifest.jsonl + /version 修复）后的 canonical 256 行样本，取代 v3。**

## 1. 实验目的

验证 bounded-output 主对比轨（SQuAD 短答案 cap=64）的 DuckDB-ai 单臂能力：
(a) DuckDB `ai` 0.4.14 经 OpenAI-compatible vLLM 逐行产出可解析答案；
(b) 共享 SQuAD evaluator 从逐行证据复算 EM/F1；
(c) vLLM counter delta 可归因到本轮（endpoint 独占、运行前后 idle）。
属于 **baseline 建设阶段**（见 `experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md`）。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090（24564 MiB，driver 595.58.03），hostname `autodl-container-a4884eab4c-22401421` |
| 模型服务 | vLLM **0.25.1**，单 endpoint `http://127.0.0.1:8000`，model `qwen2.5-7b`，prefix-cache enabled |
| DuckDB runtime | duckdb v1.5.4 + community `ai` 0.4.14；venv `/root/autodl-tmp/venvs/text-baselines` |
| 数据库 | PostgreSQL 18.4，workload `squad_v11_dev_short_answer`（10570 行） |
| Workload | SQuAD v1.1 dev，cap=64，prompt 模板见 `code/scripts/data/import_squad_workload.py` |
| 调度合同 | DuckDB-ai 扩展原生（`max_concurrent_requests=32`、cache off、retry 0、temp 0.0）；项目不注入调度 |
| 计时边界 | operator-only（submitted→started=setup；started→completed=operator-only JCT） |
| 代码/原始路径 | gate @ `735751b`；importer provenance `feasibility/results/squad_v11_dev_import_20260805/provenance.json`（content_hash `2c2301f2…`）；本目录 `report.json` + `per_row_evidence.csv` + `sample_manifest.jsonl` |

## 3. 合规性自检

| 检查 | 结果 |
|---|---|
| workload 完整性（两模式都跑） | ✅ verified：10570 行、unique doc_id、unique source_example_id、非空 reference_answers、`workload_content_hash == importer_content_hash == 2c2301f2…` |
| exactly-once（full-set） | ✅ result id set == input id set；256/256/256；source_example_id 全唯一非空 |
| 成功/错误/NULL/max_tokens error | 256 / 0 / 0 / 0 |
| vLLM counter 归因 | ✅ attributable：运行前后 running=waiting=0、scrape 非空、counter 单调、`request_success_delta(256) == requests_sent(256)` |
| DuckDB-ai finish_reason | 不可得（扩展不暴露）→ 不声称零截断，只报"未观察到 error/NULL/可识别 max_tokens error" + vLLM gen-token delta |
| 命令脱敏 | ✅ `command` 字段 `postgres:***@`、无明文 |
| EM/F1 独立复算 | ✅ 从 `per_row_evidence.csv` 经共享 `squad_quality_metrics` 复算 = 报告值（81.640625% / 89.82133685%） |

## 4. 实验设计

`--mode sampled --sample-count 256`：确定性分层抽样——按参考答案 **SQuAD-normalized** 最长答案词数分桶
（`normalize_squad_answer` 去冠词/标点/转小写后再计词数，short≤1/medium≤4/long>4，多答案取 max），
largest-remainder 配额（各桶之和严格=256，每桶至多补 1 席），桶内按 source_example_id 均匀间距。
**新增 `sample_manifest.jsonl`**：每行一条结构化 `{source_example_id, prompt, reference_answers}`，使
sample_content_hash 可脱离数据库独立复算。`sample_content_hash = d0e0e987…`（与 v3 的 `b154c46a…`
不同，因 SQuAD-normalize 分桶改变了样本选择——这正是 codex 第七轮修复的点）。

## 5. 实验数据

**正确性与语义**：EM **209/256 = 81.640625%**；token-F1 **89.82133685%**；evaluated 256、missing 0、status ok。

**vLLM 工作量（attribution=attributable）**：prompt_tokens_delta = 59811；generation_tokens_delta = 1438；
avg **5.62 gen tokens/row**；prefix_cache_queries 59811、hits 21648、hit-rate **0.3619**。

**operator-only 计时**：adapter_wall 5.072s；operator_only_jct 4.209s；setup 0.668s。

**证据**：`per_row_evidence.csv`（source_example_id/status/error/output_chars/prediction/reference_answers）+
`sample_manifest.jsonl`（逐行结构化，hash 可独立复算）。

## 6. 结果解释

- **事实**：256 行 SQuAD-normalize 确定性样本上，DuckDB-ai 返回 256/256 成功、0 error/NULL/可识别
  max_tokens error；EM 81.64%、F1 89.82%（独立复算一致）。endpoint 独占且运行前后 idle，vLLM delta
  可归因：avg 5.62 gen tokens/row、prefix-cache hit-rate 0.3619。vLLM 0.25.1、DuckDB ai 0.4.14。
- **与 v3 的差异**：v4 用 SQuAD-normalize 词数分桶（v3 用原始 split），样本不同（hash d0e0e987 vs
  b154c46a），EM 81.64% vs v3 80.86%。两者都有效；v4 是修复"分层未用 SQuAD normalize"后的 canonical 样本。
- **不能声称**：整个 10570 行已通过（256 样本）；cap=64 全集零截断（DuckDB-ai 不暴露 finish_reason）；
  DuckDB-ai 比直连/项目更快或更慢（单臂、operator-only、无可比对照）；database-E2E 性能。

## 7. 对课题含义 + 下一步

- **含义**：DuckDB-ai 单臂具备进入 bounded-output 正式三臂对比的前提（能力跑通、质量可比、指标可归因、
  样本可独立复算）。
- **下一步**（协议 §5）：① `--mode full` 全 10570 行语义门禁；② database-E2E 顶层 runner（结构性缺口）；
  ③ 三臂正式对比（DuckDB-ai / 直连 client / 项目冻结最佳静态）。
