# SQuAD v1.1 dev capability gate v3 — DuckDB-ai arm（2026-08-05）

> **角色：能力门禁，不是正式排名。** 单臂（DuckDB community `ai` 扩展）、operator-only 计时边界。
> database-E2E 顶层 runner 尚未实现，故不产生数据库系统级排名。本门禁回答的问题是：
> 在固定 cap=64 下，DuckDB-ai 能否正确跑通 SQuAD 短答案（逐行可复算 EM/F1），并把
> vLLM 侧 token/cache 计数**可归因**地记录下来。修复过 codex 第六轮 review + 4 审查员对抗审。

## 1. 实验目的

验证 bounded-output 主对比轨（SQuAD 短答案 cap=64）的 **DuckDB-ai 单臂能力**：
(a) DuckDB community `ai` 扩展在 OpenAI-compatible vLLM endpoint 下能否逐行产出可解析答案；
(b) 共享 SQuAD evaluator 能否从逐行证据复算 EM/F1；
(c) vLLM counter delta 是否可归因到本轮（endpoint 独占、运行前后 idle）。
关系到方向：**baseline 建设阶段**（DuckDB-ai 是数据库产品原生 baseline，见
`experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md`）。

## 2. 实验设置

| 项 | 值 |
|---|---|
| 平台 | AutoDL 2×RTX 4090（24564 MiB each，driver 595.58.03），hostname `autodl-container-a4884eab4c-22401421` |
| 模型服务 | vLLM，单 endpoint `http://127.0.0.1:8000`，model `qwen2.5-7b`（Qwen2.5-7B-Instruct），prefix-cache **enabled** |
| DuckDB runtime | duckdb **v1.5.4** + community `ai` 扩展 **0.4.14**（`installed_from=community`）；venv `/root/autodl-tmp/venvs/text-baselines` |
| 数据库 | PostgreSQL 18.4，`ai_operator.documents`，workload `squad_v11_dev_short_answer`（10570 行） |
| Workload | SQuAD v1.1 dev，cap=64，prompt 模板见 importer（`code/scripts/data/import_squad_workload.py`） |
| 调度合同 | DuckDB-ai 扩展原生：`max_concurrent_requests=32`、`cache=off`、`retry_count=0`、temperature=0.0；项目不注入调度 |
| 重复 | 单次（capability gate，非稳态排名；不做 warmup/formal 交错） |
| 计时边界 | **operator-only**：submitted→started=setup（configure+建表+灌 prompt），started→completed=operator-only JCT |
| 配置/原始路径 | 代码 `code/scripts/baselines/squad_capability_gate.py` @ `fd4f8bf`；importer provenance `feasibility/results/squad_v11_dev_import_20260805/provenance.json`（content_hash `2c2301f2…`）；本目录 `report.json` + `per_row_evidence.csv` |

## 3. 合规性自检（rigor gate）

| 检查 | 结果 |
|---|---|
| workload 完整性（两种模式都跑） | ✅ verified：行数 10570、unique doc_id、unique source_example_id、非空 reference_answers、`workload_content_hash == importer_content_hash == 2c2301f2…` |
| exactly-once（full-set） | ✅ result id set == input id set；result_count=unique_result_ids=input=256；source_example_id 全唯一非空 |
| 成功/错误/NULL/max_tokens error | 256 / 0 / 0 / 0 |
| vLLM counter 归因 | ✅ attributable：运行前 running=waiting=0、运行后 running=waiting=0、scrape 非空、counter 单调、`request_success_delta(256) == requests_sent(256)` |
| DuckDB-ai finish_reason | 不可得（扩展不暴露 finish_reason）→ 不声称"零截断"，只报"未观察到 error/NULL/可识别 max_tokens error" + vLLM gen-token delta |
| 命令/异常脱敏 | ✅ report.json `command` 字段已脱敏（`postgres:***@`） |

无异常指标。

## 4. 实验设计

`--mode sampled --sample-count 256`：对全 10570 行做**确定性分层抽样**——按参考答案的
**最长**答案的原始 whitespace 词数分桶（short≤1 / medium≤4 / long>4，多答案取 max 而非 answers[0]），
**largest-remainder** 配额（各桶配额之和严格等于 256，每桶至多补 1 席），桶内按
source_example_id 排序后均匀间距选取。sample 内容哈希为结构化 JSON-per-row SHA256，
记入 report.json（`sample_content_hash = b154c46a…`）。本目录当时未归档 prompt-bearing
sample manifest，因此该 sample hash **不能只靠已提交文件独立复算**；这是 v3 的证据边界。

单臂、同 manifest、同 model、同 cap、同 endpoint、prefix-cache on。无对照臂
（capability gate 不做跨臂对比；跨臂对比在 database-E2E runner 就绪后的正式三臂门禁进行）。

## 5. 实验数据（基于 report.json + per_row_evidence.csv）

**正确性与语义**
- EM 207/256 = **80.859375%**；token-F1 = **89.861139%**；evaluated 256、missing 0、status ok
- exactly-once True；success 256 / error 0 / NULL 0 / max_tokens error 0

**vLLM 模型服务工作量（attribution=attributable）**
- prompt_tokens_delta = 61609；generation_tokens_delta = 1364；avg **5.33 gen tokens/row**
- prefix_cache_queries_delta = 61609；prefix_cache_hits_delta = 22112；hit_rate = **0.3589**
- 注：prompt_tokens_delta == prefix_cache_queries_delta == 61609（vLLM 把每个 prompt token 计一次 cache query）

**operator-only 计时**
- adapter_wall = 4.907s；operator_only_jct = 4.26s；setup = 0.603s
- 边界：operator-only（不含连接创建/扩展加载/持久表扫描/统一 sink —— 那是 database-E2E 顶层 runner 的职责）

**逐行证据**：`per_row_evidence.csv`（source_example_id / status / error / output_chars / prediction /
reference_answers），EM/F1 可用共享 `squad_quality_metrics` 独立复算。

## 6. 结果解释

- **事实**：在固定的 256 行确定性样本（max-答案分桶 + largest-remainder）上，DuckDB-ai 0.4.14
  经 OpenAI-compatible provider 调用 vLLM qwen2.5-7b，返回 256/256 成功、0 错误/NULL/可识别
  max_tokens error；共享 evaluator 复算 EM 80.86%、token-F1 89.86%。endpoint 独占且运行前后 idle，
  vLLM counter delta 可归因：平均 5.33 gen tokens/row、prefix-cache hit-rate 0.3589。
- **推断**：DuckDB-ai 在 cap=64 下能正确驱动 SQuAD 短答案生成，答案语义质量（EM/F1）与
  直连 client / 项目侧可比；operator-only JCT 4.26s 是"prompt 已就绪 → AI 算子 → 结果物化"的
  算子级耗时，不含数据库读侧与统一 sink。
- **不能声称**：整个 10570 行 SQuAD 已通过（本门禁是 256 行样本，full-mode 10570 未跑）；
  cap=64 在整个 SQuAD 上零截断（DuckDB-ai 不暴露 finish_reason）；DuckDB-ai 比直连/项目更快或更慢
  （单臂、operator-only，无可比对照）；双 GPU 利用（单 endpoint）；database-E2E 性能。
- **与 v2 的差异**：v3 EM 80.86% 高于 v2 的 75.39%，原因是抽样规则变了（v3 用**最长**答案词数分桶
  + largest-remainder，v2 用 answers[0] + round 配额），选中的 256 行不同；两者都是有效 capability
  evidence，v3 抽样更严谨（多答案 max + 配额精确）。
- **后续审计修正**：v3 分桶实现使用 `answer.split()`，并非报告早先写的
  “SQuAD-normalized token count”。这不改变本目录 256 行输出、EM/F1 和能力门禁事实，但意味着
  v3 不能作为最终冻结的 canonical sample；后续重跑改用 SQuAD 官方 normalize 后的词数，并归档
  `sample_manifest.jsonl`。

## 7. 对课题含义 + 下一步

- **含义**：DuckDB-ai 作为数据库产品原生 baseline 已具备进入 bounded-output 正式三臂对比
  （DuckDB-ai / 直连 client / 项目冻结最佳静态）的前提条件——能力跑通、质量可比、指标可归因。
- **下一步**（按 `bounded_output_duckdb_comparison_protocol_20260805.md` §5 执行顺序）：
  1. （可选）`--mode full` 全 10570 行语义门禁，确认整集无截断/无质量塌陷；
  2. **database-E2E 顶层 runner**（结构性缺口）：实现持久表扫描 → prompt 构造 → 模型调用 → 统一 sink
     的端到端计时，才能发布数据库系统级排名；
  3. 三臂正式对比（同 manifest、同 model、双 GPU、同 cap=64、同计时边界、prefix-cache on）。
