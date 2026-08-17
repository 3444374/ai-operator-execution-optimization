# SAOR feeding-gap diagnostic D0/D1/P0 冻结执行——fail-closed 中止报告（2026-08-17，commit 345bee2f）

> **性质**：冻结诊断实验（feeding-gap 归因专用），非 baseline 正式排名、非 native-system 实验、非 SAOR formal。
> **最终状态**：runner 在第 11/12 cell fail-closed 中止——D0 formal rep3 出现 1/512 请求 `ReadError`（zero-retry 合同下任何 ReadError 即 incident）。完成 10/12 cell（3 warmup + 7 formal），**官方 summarizer 未运行**（其前置条件"runner 正常完成"未满足），四种 0.95 判决均未产生。root 已完整保留并归档，不重跑、不放宽门槛。
> **SAOR formal 状态不变**：`locked_failed_feeding` / `formal_authorized=false`。本报告不修改任何旧判决。

## provenance

- **代码**：git `345bee2f`（main，服务器与本地一致，服务器工作区干净、ff-only 同步）。
- **诊断合同**：`deploy/autodl/saor_feeding_gap_diagnostic_contract.json`（`status=locked_diagnostic_only_pending_run`，`may_change_prior_feeding_decision=false`）。
- **prior lock**：`saor_project_feeding_ceiling_c988622a_20260814_retry2`（validation SHA `b439fe08…`，archive SHA `ebf5c35a…`，required `locked_failed_feeding/formal_authorized=false`——本次运行前后均未触碰）。
- **平台/服务**：AutoDL 2×RTX 4090；PG 18.4 + pgvector；2× vLLM endpoint（8000/8001，qwen2.5-7b，`max_num_batched_tokens=8192`、`max_num_seqs=256`、gpu-mem-util 0.9、prefix-cache ON、FCFS）；Ray head 127.0.0.1:6380（32 CPU/2 GPU）；driver `/root/miniconda3/bin/python`。
- **冻结合同**：K=128/endpoint、W=65536/endpoint、1 warmup + 3 measured repeats、manifests=long_512/short_512（SHA `8e532819…`/`85b3f90c…`，与合同逐字节一致）、seed 20260815、`COMPLETION_HTTP_KEEPALIVE_EXPIRY_S=4`（zero-retry：任何 ReadError = incident）。
- **preflight**：`manage_environment.py check --groups core,text,analysis` → `status=ok`，profile 自动选择 `autodl_2x4090`，无失败检查。报告 `saor_feeding_gap_environment.json`，SHA256 `0bb648489da8754affefffad99ca6ac6355f5760f22c06afeb628c6fc9474fe7`。
- **output root**：`saor_feeding_gap_diagnostic_345bee2f_20260817`（运行前确认不存在）。
- **archive**：`saor_feeding_gap_diagnostic_345bee2f_20260817.tar.gz`（2,498,384 B），SHA256 `f4b9793d8ec49d1de6712706d5e8abb4fdf9d97364cc6cc8b7878f72f9f9b4c6`（服务器与本地镜像一致）。本地镜像：`C:\Users\ays\Desktop\results\`。
- **raw**：完整 raw（records/10 + jobs/ + logs/ + traces/ + manifest.json + group_runs.csv + pre_run_clean_gate.json）只在服务器 root 与本地 tarball 镜像；本目录 `raw/` 仅含 compact evidence（4 个身份/汇总文件）。

## 1. 实验设置

三臂冻结矩阵（同 manifest、同服务签名、同 K/W，唯一差异为 request/work envelope 归属）：

| 臂 | 身份 | envelope | 说明 |
|---|---|---|---|
| D0 `feeding_gap_d0_direct_k_only` | service_capacity_ceiling | endpoint-local K only，无 work 上限 | direct 直连天花板 |
| D1 `feeding_gap_d1_direct_k_work` | project_diagnostic_control_not_native_baseline | endpoint-local K + endpoint-local typed-work（estimated work acquire/completion release） | **诊断控制，非原生 baseline**；不含 job fairness/bounded-ready/SAOR selector |
| P0 `feeding_gap_p0_project_bounded_ready_fifo` | project_execution_path_control | endpoint-shared K + W，bounded concrete pre-registration ready 观测 | Project bounded-ready FIFO 执行路径 |

执行顺序由 runner 冻结为 balanced interleaved by repeat（3 warmup → 交错 formal rep1-3）。

## 2. 实验设计

预注册判决合同（本次**未执行**）：配对逐 repeat 算术平均后，按 `D1/D0` 与 `P0/D1` 对 0.95 的四分支归类（work_envelope_primary / project_path_primary / both / original_gap_not_reproduced），且 `classification_never_reopens_locked_failed_feeding`。

## 3. 严谨性自检

- **pre-run clean gate**：`pre_run_clean_gate.json status=passed`（两 endpoint health 200 且 running/waiting=0、PG 无 non-idle session、诊断 namespace 无残留 named actor、Ray 32CPU/2GPU 完整、vLLM 进程 cmdline 与冻结合同一致）。
- **正确性**：完成的 7 个 formal cell 均 `request_success_delta=1024`、`job_failed_rows=0`（除 incident cell）、`actor_worker_failures=0`、metrics/resources 全 `ok`。
- **incident（fail-closed）**：cell `010_formal_3_feeding_gap_d0_direct_k_only`，D0 formal rep3，job1（foreground 短 manifest）512 条中 1 条失败——doc_id=301913，endpoint-1，`admission_wait 43.5s` 后进入请求、约 3.1s 后 `ReadError`（空响应体，`output_sha256=e3b0c442…` 为空流 SHA），0 input/0 output token。511/512 completed。runner 记 `recovered=false`，matrix `status=failed`，exit 1。**root 完整保留，未在同一 root 重跑**（合同要求）。
- **缺口**：D0 formal rep3（incident 中止）与 D1 formal rep3（未启动）缺失，12 cell 只完成 10。四判决的输入不完整。
- **观测口径**：全部指标取自 per-run time-series 聚合列（`*_mean/p95/max`）；`vllm_kv_usage` 按分数（0–1）读。

## 4. 实验数据（已完成 formal cell；**非判决输入**）

每臂吞吐（formal；D0/D1 只有 2 rep，P0 3 rep）：

| 臂 | n | tok/s 均值 | CV | 单次值 |
|---|---|---:|---:|---|
| D0 direct K-only | 2 | 13239.2 | 0.30% | [13211.1, 13267.2] |
| D1 direct K+W | 2 | 12612.3 | 0.21% | [12630.7, 12593.8] |
| P0 project bounded-ready FIFO | 3 | 12456.5 | 0.27% | [12449.7, 12492.8, 12427.0] |

（warmup 不入列：D0 13279.2 / D1 12533.5 / P0 12441.3，与 formal 同档。）

**描述性配对比值（仅 rep1/rep2 两对，非预注册 3 对，不能当判决）**：D1/D0 = 0.9561/0.9492（均值 0.9527）；P0/D1 = 0.9857/0.9920（均值 0.9888）。若未来完成 rep3 后由官方 summarizer 落在同侧，倾向"work envelope 主导 + project path 无额外损失"分支，但**本报告不预支该结论**。

全组件（formal rep 均值）：

| 指标 | D0 | D1 | P0 |
|---|---:|---:|---:|
| GPU util mean % | 98.4 | 98.8 | 96.1 |
| MFU（分数） | 0.537 | 0.477 | 0.468 |
| KV usage max（分数） | 1.00 | 0.55 | 0.54 |
| vLLM running mean | 160 | 116 | 119 |
| vLLM waiting max | 139 | 18 | 26 |
| TTFT p50/p95 s | 2.36/13.3 | 0.21/2.1 | 0.37/2.2 |
| ITL p50/p95 s | 0.031/0.102 | 0.019/0.113 | 0.018/0.109 |
| JCT bulk/fg s | 52.4/60.8 | 65.9/63.7 | 68.0/47.4 |
| P99 bulk/fg s | 45.8/55.5 | 58.7/57.9 | 61.1/40.7 |
| SLO viol bulk/fg | 0.555/0.986 | 0.468/0.857 | 0.561/0.403 |
| energy J/1k tok | 65.4 | 67.3 | 67.3 |
| direct occupancy req/work max | 128 / ~176.9K | 128 / 65536 | N/A（project admission） |
| direct admission wait p50/p95 s | 30.6/42.9 | 34.3/50.4 | N/A |

occupancy 语义：D0 无 work envelope → estimated work occupancy 顶到 ~177K（2.7×W）；D1 work gate 恰好压在 65536；P0 走 project admission（occupancy 列 not_applicable，机制量在 credit trace）。

## 5. 事实 / 推断 / 待确认 / 不能声称

**事实**：
1. 三臂在前 2 个 formal repeat 上极其稳定（CV ≤0.30%），且排序 D0 > D1 > P0（13239 > 12612 > 12457 tok/s）。
2. D0（无 work 上限）KV 顶满 1.0、waiting 139；D1 加 W 后 KV 0.55、waiting 18——work envelope 是 KV/排队压力的直接控制变量。
3. incident 是 D0 臂、foreground job、1/512 的 HTTP `ReadError`（keepalive 4s 合同下 zero-retry fail-closed），不是 D1/P0 的调度行为。

**推断（低置信，须完整 3 rep + summarizer 确认）**：D1/D0 ≈ 0.95 边界、P0/D1 ≈ 0.99，若成立指向 work-envelope 主导差距、project 执行路径近乎无额外损失。

**待确认**：
1. incident 的根因（vLLM endpoint-1 连接被服务端断开？keepalive 过期竞态？）——本轮不诊断、不修复，证据在 root。
2. rep3 补齐后四判决的官方输出。

**不能声称**：
- 任何四种 0.95 判决结论（summarizer 未运行、配对不完整）。
- "SAOR 已胜出"、"native baseline 已完成"、"formal 已通过或已授权"。
- "D1/P0 是原生 baseline"（合同明示 D1 是 project diagnostic control）。
- 用描述性比值预支正式归因；GPU util ~100% 单独不证明 feeding 充分。

## 6. 对课题的含义

冻结诊断本轮**未产出归因判决**——中止原因是传输层单请求失败触发预注册 fail-closed，属于实验完整性事件而非策略结果。已完成 cell 的描述性信号（D0 与 D1 的差距 ≈4.8%、P0 与 D1 ≈1.1%、CV<0.3%）不改变任何已有结论，也不解锁 formal。`locked_failed_feeding` 维持原状。

## 7. 下一步（决策权在用户/codex，本轮不执行）

1. 用户/codex 决定 incident 处置：接受 root 为一次 fail-closed 诊断运行（保留现状），或授权在**全新 root** 重跑完整 12 cell（同合同、同 commit，不视为续跑）。
2. 若重跑完成，由官方 `summarize_saor_feeding_gap_diagnostic.py` 出四判决，再走第七/八步归档与结果分支。
3. ReadError 若复现于 D0 臂，建议 codex 独立审查 direct 客户端在 K-only 无 work 上限下的连接行为（本轮不诊断）。

## 归档清单

| 项 | 值 |
|---|---|
| 执行 commit | `345bee2f99079ea19e29599077c812c0a8c64cce`（服务器=本地=origin/main） |
| preflight SHA256 | `0bb648489da8754affefffad99ca6ac6355f5760f22c06afeb628c6fc9474fe7` |
| output root | `experiment-artifacts/saor_feeding_gap_diagnostic_345bee2f_20260817`（服务器，完整保留） |
| archive SHA256 | `f4b9793d8ec49d1de6712706d5e8abb4fdf9d97364cc6cc8b7878f72f9f9b4c6`（2,498,384 B，本地镜像一致） |
| matrix 终态 | `status=failed`，completed 10/12，incidents=1（D0 formal rep3 ReadError，recovered=false） |
| summarizer | **未运行**（前置条件不满足） |
| formal 合同 | `locked_failed_feeding` / `formal_authorized=false`（未触碰） |
