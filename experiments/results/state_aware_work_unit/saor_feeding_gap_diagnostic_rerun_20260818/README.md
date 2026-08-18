# SAOR feeding-gap diagnostic D0/D1/P0 重跑完成——work_envelope_primary 判决（2026-08-18，commit 345bee2f + 2e4c2723 观测补丁）

> **性质**：冻结诊断实验（feeding-gap 归因专用），非 baseline 正式排名、非 native-system 实验、非 SAOR formal。
> **最终判决（官方 summarizer）**：`status=valid_diagnostic`、`evidence_valid=True`、**`classification=work_envelope_primary`**——D1/D0=0.9238（<0.95）且 P0/D1=0.9982（≥0.95）。**W（65536）work envelope 是项目路径与 direct 天花板之间差距的主要来源；Project 执行路径（bounded-ready FIFO + credit + Daft/Ray）几乎无额外损失（0.18%）。**
> **前序 incident 闭环**：08-17 运行的 D0 rep3 `ReadError` 已由独立 transport reliability gate 定位为 HTTP/1.1 持久连接竞态（`BrokenPipeError`），三轮 gate 中 1 轮复现（1/1024），非系统性故障；本次重跑 12/12 cell、0 incident。
> **SAOR formal 状态不变**：`locked_failed_feeding` / `formal_authorized=false`；`may_change_prior_feeding_decision=false`，本判决不重开任何旧锁。

## provenance

- **代码**：main@`345bee2f` + 观测补丁 `2e4c2723`（分支 `codex/saor-feeding-gap-transport-evidence-20260818`，只增异常 cause-chain 落盘、零请求行为变化、零重试合同不变；async_http 13/13 测试通过）。
- **诊断合同**：`deploy/autodl/saor_feeding_gap_diagnostic_contract.json`（manifests SHA `8e532819…`/`85b3f90c…` 逐字节一致）。
- **平台/服务**：AutoDL 2×RTX 4090；PG 18.4；2× vLLM endpoint 8000/8001（qwen2.5-7b，8192/256，prefix-cache ON，FCFS）；Ray head 6380；preflight `status=ok`（08-17 报告，SHA `0bb64848…`）。
- **output root**：`saor_feeding_gap_diagnostic_345bee2f_20260818_r2`（全新，运行前确认不存在）；pre-run clean gate passed。
- **run**：12/12 cell（3 warmup + 9 formal 交错）、0 incident、exactly-once、RUNNER_EXIT=0。
- **summarizer**：`summarize_saor_feeding_gap_diagnostic.py` → `diagnostic_validation.json`，`errors=[]`。
- **archive**：`saor_feeding_gap_diagnostic_345bee2f_20260818_r2.tar.gz`（2,647,583 B），SHA256 `7fdf8b65f7e1926d5a567497b0cac56e6bb6d5c2d6d50db5826a9a14488a1391`（服务器=本地镜像一致）。gate archive `52a763bf…`。
- **raw**：完整 raw 在服务器 root 与本地 tarball；git 内只放 compact evidence（7 文件）。

## 1. 实验设置

同 08-17 报告：三臂 D0（direct K-only ceiling）/ D1（direct K+W 诊断控制，非原生 baseline）/ P0（Project bounded-ready FIFO K+W），K=128/endpoint、W=65536/endpoint、1 warmup + 3 measured repeats、seed 20260815、chat_completions + token-IDs + keepalive 4s + 零重试。

## 2. 实验设计

预注册四分支判决（`paired_by_repeat_arithmetic_mean`）：`D1/D0<0.95 ∧ P0/D1≥0.95` → **work_envelope_primary**；其余三支见合同。本次落入第一支。

## 3. 严谨性自检

- **clean gate**：`pre_run_clean_gate.json status=passed`（双 endpoint health+idle、PG 无杂 session、无残留 named actor、Ray 32C/2G、capacity 与合同一致）。
- **完整性**：12/12 cell、`request_success_delta`=1024/cell、`job_failed_rows=0`、`actor_worker_failures=0`、0 incident；summarizer `errors=[]`、`evidence_valid=True`。
- **transport 前置**：结构化错误补丁后三轮独立 gate（同客户端合同、1024 请求/轮）：r1 复现 1 条 `httpx.ReadError`，cause chain=`httpx.ReadError('') ← httpcore.ReadError('') ← anyio.BrokenResourceError('') ← BrokenPipeError('[Errno 32] Broken pipe')`（fg job、endpoint-1，与 08-17 incident 同型）；r2/r3 零失败。定位为低概率持久连接竞态（客户端向服务端已关连接写入），非系统性故障、非算法失败。
- **观测口径**：time-series 聚合列；KV 按分数读。

## 4. 实验数据

**配对判决输入（formal，tokens/s）**：

| rep | D0 | D1 | P0 | D1/D0 | P0/D1 | P0/D0 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 13366.8 | 12516.6 | 12539.6 | 0.9364 | 1.0018 | 0.9381 |
| 2 | 13914.0 | 12719.0 | 12427.7 | 0.9141 | 0.9771 | 0.8932 |
| 3 | 13278.7 | 12227.9 | 12419.9 | 0.9209 | 1.0157 | 0.9353 |
| **均值** | 13519.8 | 12487.8 | 12462.4 | **0.9238** | **0.9982** | 0.9222 |

D1/D0 CV=1.01%，P0/D1 CV=1.60%。**D1/D0_passed=False（0.9238<0.95）；P0/D1_passed=True（0.9982≥0.95）→ work_envelope_primary。**

**全组件（formal 3-rep 均值）**：

| 指标 | D0 | D1 | P0 |
|---|---:|---:|---:|
| tok/s | 13520 | 12488 | 12462 |
| MFU | 0.547 | 0.472 | 0.468 |
| GPU util % | 98.2 | 98.1 | 96.4 |
| KV usage mean | 0.70（max 顶 1.0） | 0.43 | 0.41 |
| vLLM running/waiting mean | 164/39（wait max ~138） | 114/1 | 119/1 |
| TTFT p99 s | 18.6 | 2.5 | 2.7 |
| JCT bulk/fg s（rep1） | 52.0/60.1 | 63.5/64.5 | 67.9/47.2 |
| P99 bulk/fg s | 45.6/54.8 | 56.9/57.8 | 61.2/40.7 |
| SLO viol bulk/fg | 0.551/0.986 | 0.465/0.879 | 0.557/0.402 |
| direct occ req / work/W | 0.889 / 1.90 | 0.473 / 0.897 | project admission |
| admission wait p95 s | 42.2 | 50.3 | bounded（project 内） |
| Ray actor ready max | N/A | N/A | 4.9s |

## 5. 事实 / 推断 / 待确认 / 不能声称

**事实**：
1. 官方 summarizer 判决：`work_envelope_primary`，证据链完整（clean gate、exactly-once、occupancy/admission ledger、vLLM/MFU/TTFT-ITL/JCT-SLO/能耗全字段、SHA 绑定、errors=[]）。
2. W envelope 主导差距：D0→D1 掉 ~7.6% 吞吐，同时 KV 从 mean 0.70（max 1.0）压到 0.43、waiting 从 39（max ~138）压到 1、TTFT p99 从 18.6s 压到 2.5s。
3. Project 执行路径额外损失 0.18%（P0/D1=0.9982），三 repeat 全部 ≥0.977。
4. 08-17 的 ReadError incident 根因闭合：`BrokenPipeError` 持久连接竞态，gate 3 轮复现率 1 轮；重跑 0 incident。

**推断**（低置信）：W 不是纯工程开销，而是用 ~7.6% 容量换取模型服务压力显著下降（KV 去饱和、队列近乎清零、TTFT p99 降 7.4×）——保护成本与容量之间的显式权衡点。

**待确认**：`work_envelope_primary` 的 exit contract 要求"分开 Project K+W/direct K+W 实现效率 与 Project K+W/direct K-only 保护成本 两道门"——这是下一步实验设计，本轮未执行。

**不能声称**：
- SAOR 已胜出 / formal 已通过或已授权（`locked_failed_feeding` 原样）。
- native baseline 已完成；D1/P0 是原生 baseline。
- 本次诊断修改任何旧判决（`may_change_prior_feeding_decision=false`）。
- "W 应该调大/调小"——本轮只归因，不调参。
- 把 gate r1 的 1/1024 失败外推为普适故障率（n=3 轮）。

## 6. 对课题的含义

项目路径（bounded-ready + credit + Daft/Ray）在当前签名下**不是** feeding 差距的原因——92.3% 的 D1/D0 差距全部来自 W=65536 work envelope 本身。这把"项目链路慢"的嫌疑正式转移到"容量保护策略的代价"上：要么接受保护成本（低 KV 压力/低排队/低 TTFT），要么研究更聪明的 W 管理（状态感知的 W 动态化在 exit contract 的两道门框架下展开）。SAOR formal 负判决维持——它当初卡的就是这条链路的 92.9% feeding，现在知道这 7.1% 里几乎全是 W 的保护代价而非工程损耗。

## 7. 下一步

1. 按 exit contract 拆两道门：① Project K+W vs direct K+W（实现效率）；② Project K+W vs direct K-only（保护成本）——具体设计由 codex 决定。
2. transport 补丁 `2e4c2723`（结构化错误链）建议合入 main，作为后续所有 direct 实验的标准观测。
3. ReadError 竞态的工程缓解（如 keepalive 3s 或连接重建语义）需独立授权改合同后才可评估，本轮未动。
4. 原生 Daft/Ray Data 多 Job comparison 按既定顺序排在本诊断之后。

## 归档清单

| 项 | 值 |
|---|---|
| 执行 commit | `345bee2f` + 观测补丁 `2e4c2723`（服务器分支） |
| output root | `saor_feeding_gap_diagnostic_345bee2f_20260818_r2`（服务器，完整） |
| rerun archive SHA256 | `7fdf8b65f7e1926d5a567497b0cac56e6bb6d5c2d6d50db5826a9a14488a1391` |
| gate archive SHA256 | `52a763bfdd26a435c8b1233b954bf30e050b661a71d03f97cee3add980670ebf`（3 轮） |
| matrix 终态 | `completed`，12/12，incidents=0 |
| summarizer | `valid_diagnostic`，`classification=work_envelope_primary`，errors=[] |
| formal 合同 | `locked_failed_feeding` / `formal_authorized=false`（未触碰） |
| 本地镜像 | `C:\Users\ays\Desktop\results\`（两个 tarball，SHA 已校验） |
