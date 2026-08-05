# SQuAD 行 572700c8 截断定点诊断（2026-08-05）

> **结论：cap=64 下孤立重放该行 3×3 全部正常完成（`stop`，46 token，文本一致）；
> 全量 10570 跑里的 max_tokens 截断【不能复现】。** 因此 full gate 的 1 行 NULL 不是该行的
> 确定性属性，而是**全量并发服务下的偶发生成尾部风险**（vLLM 批处理解码在 temp=0 下的
> 非确定性），被 DuckDB-ai「truncation-as-error」产品语义硬转成 NULL。这是诊断，不抬正式 cap=64。

## 1. 目的

`c20240e` 的 full-10570 gate 里 1 行（`572700c8dd62a815002e976d`）在 cap=64 被 DuckDB-ai 当作
max_tokens 截断错误返回 NULL。原始 README 据此写了「模型 rambling/loop、temp=0 确定性」——但
DuckDB 返回 NULL 时实际超长内容不可见，该推断未经验证。本诊断按 codex 裁决复跑该行的**归档 prompt**
确认：是稳定超长，还是偶发尾部风险。

## 2. 设置

同一 `source_example_id` 的归档 prompt（sha256 `7342945119862b4e…`，775 字符），model `qwen2.5-7b`，
temperature 0.0。两条路径 × cap {64, 128, 256} × 重复 3 次：

- **direct vLLM** `POST /v1/chat/completions`（暴露 `finish_reason` / `completion_tokens` / 文本）；
- **DuckDB `ai_try_complete`**（暴露 `{response, error}` 产品语义）；并用
  `ai_completion_request_json` 抓取扩展实际请求体，证明两条路径是同一个请求。

DuckDB `cache=false`、`retry_count=0`；高 cap（128/256）仅用于诊断，不回灌正式门禁（cap=64 锁定）。
脚本：`code/scripts/baselines/squad_truncation_diagnostic.py`（`9aefeba`）。

## 3. 数据（`diagnostic.json`）

| 路径 | cap | 3 次重复 finish / completion_tokens / response |
|---|---:|---|
| direct vLLM | 64 | `stop` / `stop` / `stop`；46 / 46 / 46；文本三份完全一致 |
| direct vLLM | 128 | `stop`×3；46×3；一致 |
| direct vLLM | 256 | `stop`×3；46×3；一致 |
| DuckDB `ai_try_complete` | 64 | 3 次全部 `response`（140 字符）、`error=null` |
| DuckDB `ai_try_complete` | 128 | 3 次全部成功 |
| DuckDB `ai_try_complete` | 256 | 3 次全部成功 |

两条路径产出的文本完全相同：`"Nicholas Stone, Caius Gabriel Cibber, Grinling Gibbons, John Michael
Rysbrack, Louis-François Roubiliac, Peter Scheemakers, Agostino Carlini"`（46 token）。
DuckDB 抓到的请求体与 direct 请求体逐字段一致（model / messages / temperature=0 / max_tokens）。

`decision.stable_length_at_cap64 = false`、`duckdb_cap64_null = false` → **cap=64 不能稳定复现截断**。

## 4. 解释

- **事实**：cap=64 孤立重放该行（direct 与 DuckDB 各 3 次）全部 46 token 正常 `stop`，无截断、无 NULL。
  即该行在低并发下**不会**超长。full-10570 跑里的那 1 次 max_tokens 截断**不可复现**。
- **推断（机理）**：full gate 期间 10570 行以 `max_concurrent_requests=32` 灌入、vLLM 侧
  continuous batching 同时服务大量请求；temp=0 下 vLLM 的批处理解码**并非严格确定**（批组合影响
  规约的浮点累加，偶发 logit 翻转 → 不同 token → 发散 → 长度溢出）。该行在那次高并发批次里恰好发散
  到 >64 token，vLLM 返回 `finish_reason=length`，DuckDB-ai 据产品语义把整行当 error 返回 NULL。
- **不能声称**：该行「确定性 rambling」（已证伪）；该行本身需要 >64 token（实际 46）；cap=64 在
  全集「必然」截断某行（单次观察，不可复现，应记为偶发）。
- **正确措辞**：DuckDB-ai baseline 在统一 cap=64 + 全量并发服务下，存在**可测量的偶发生成尾部
  风险**（1/10570，单次），被 truncation-as-error 语义硬转成 NULL。这是 baseline 在负载下的可靠性
  行为，不是基础设施或数据故障。

## 5. 对 full gate 定性的修正

`c20240e` full-10570 README §6 原写「模型 rambling/loop、temp=0 确定性」**不成立，已在该 README
的审计订正节更正**。full gate 的核心结论不变（10570/10570 exactly-once、三 hash 一致、归因严格通过、
fail-closed 正确拦住假 pass、10569 成功 + 1 NULL），但 1 行 NULL 的归因从「确定性模型超长」改为
「全量并发下的偶发生成尾部风险」。full gate 仍为 FAILURE；baseline 标 `eligible_with_documented_failure`。

## 6. 下一步

接受该产品边界（不抬 cap、不重跑全量）；进入 database-E2E runner；正式三臂比较对所有 arm 统一报
success/error/NULL/truncation rate、全 manifest EM/F1（失败行 0 分）、successful_rows/s、
correct_rows/s、exactly-once 与完整失败证据；不以 raw rows/s 单独排名。
