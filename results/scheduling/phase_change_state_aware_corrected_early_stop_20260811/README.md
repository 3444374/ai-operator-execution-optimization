# 两 Job phase-change 状态感知容量实验：修正后提前停止

## 1. 实验目的

验证项目控制器能否在两个离线标定容量点之间完成在线闭环：短 Job A 持续到达时从
K128 升到 K160；长 Job B 周期性开启后因服务压力降回 K128。本实验是
**VTC-shape-derived phase-change workload**，不是官方 VTC 复现，也不修改 vLLM
内部调度器。

## 2. 实验设置

- 日期：2026-08-11；服务器 commit：`feb8a7f673277cd586f1cb2192b692dbd2b3a214`。
- 硬件：2×RTX 4090；Ray 1 node、32 CPU、2 GPU。
- 服务：2×vLLM 0.25.1、Qwen2.5-7B-Instruct，`max_num_batched_tokens=8192`、
  `max_num_seqs=256`、prefix cache ON、GPU memory utilization 0.9。
- 数据链路：PostgreSQL 18.4 + pgvector 0.8.5 → Daft native PostgreSQL source →
  Ray HTTP actors → 两个 vLLM endpoint；不写回。
- 执行合同：每 endpoint 8 actors、actor concurrency 32、token budget 6144、
  `ignore_eos`、固定 512 output tokens、arrival scale 1、manifest-pinned routing。
- 容量臂：lower=K128/W131072，upper=K160/W163840。
- workload：240s，OFF/ON/OFF/ON 各 60s；A=20 req/s、约 256-token prompt；
  B 依次为 2.5/3.5/4.5 req/s、约 1024-token prompt。真实 prompt 来自
  SQuAD/ShareGPT，两个 endpoint 等工作量分配。
- 权威配置与停止规则：`deploy/autodl/phase_change_state_aware_RUNBOOK.md`。

## 3. 严谨性自检

- 修正后 A-only 和三个 pressure 点均为新目录；每个有效 cell exactly-once、0 incident。
- 排除三类无效诊断：旧 `organizer_queued_work>0` 不可满足门禁；修复前两次
  `httpx.ReadError`；遗漏 Job B 全局 arrival offset、使其从 t=0 启动的 pressure 跑。
- 修正后 Job B 首次到达保持在约 60s；OFF/ON 相位按 group-global clock 划分。
- OFF 安全使用 time-series P95，允许孤立边界 drain；ON 压力仍要求两个周期、两个
  endpoint 均出现 `waiting>0` 或 `KV>=0.85`。
- 最高预注册 B rate 失败后停止；未扩大 rate、未运行 action/formal、未追正结果。

## 4. 实验设计

顺序门禁为：环境与 immutable workload → A-only lower/upper → B=2.5/3.5/4.5
pressure → adaptive action → 三臂 formal。任一层失败即禁止进入下一层。

## 5. 实验数据

### 5.1 A-only 容量动机

| 指标 | K128 lower | K160 upper |
|---|---:|---:|
| 每 endpoint median service rate | 6602.29 tok/s | 7115.40 tok/s |
| upper 相对 lower | — | +7.77% |
| endpoint-0 0.8K 占用样本比例 | 0.937 | 0.884 |
| endpoint-1 0.8K 占用样本比例 | 0.884 | 0.856 |
| lower arrival→submit P95 | 22.92/23.01s | — |
| waiting max | 0/0 | 0/0 |
| KV max | 0.602/0.614 | 0.744/0.719 |

注：upper 的“0.8K 占用比例”以 K160 自身上限计算。A-only gate 通过，说明 K128
存在上游 admission backlog，且 K160 在安全区内提供可测容量增量。

### 5.2 pressure 顺序门禁

| A rate | B rate | 执行 | 门禁 | 首个确定失败原因 |
|---:|---:|---|---|---|
| 20 | 2.5 | 2/2、0 incident | FAIL | 第一 ON phase endpoint-0 无 upper pressure/relief |
| 20 | 3.5 | 2/2、0 incident | FAIL | 第一 ON phase endpoint-0 无 upper pressure/relief |
| 20 | 4.5 | 2/2、0 incident | FAIL | 第一 ON phase endpoint-0 无 upper pressure/relief |

最高档 B=4.5 的完整压力摘要见 [phase_metrics.csv](raw/phase_metrics.csv)。关键值：

| K160 阶段 | endpoint-0 KV max/P95 | endpoint-1 KV max/P95 | waiting max |
|---|---:|---:|---:|
| ON-1 (60–120s) | 0.749/0.707 | 0.734/0.693 | 0/0 |
| ON-2 (180–240s) | 0.874/0.841 | 0.837/0.787 | 0/0 |

只有第二轮 endpoint-0 的瞬时 KV 超过 0.85；它不是两个 endpoint、两个周期均可重复的
降档条件。第二轮压力强于第一轮，同时中间 OFF 段 KV P95 仍为 0.755/0.724，说明长请求
与 backlog 具有跨阶段历史累积。

## 6. 结果解释

**事实**：K160 相对 K128 有 7.77% A-only service-rate 增量，升档动机成立；三个
pressure rate 均未形成预注册的双 endpoint、双周期 upper 风险。

**推断**：当前 60s OFF/ON 形态没有形成可重复的低压→高压→低压稳态切换；第二轮的
更高 KV 更像前序 active work/backlog 累积，而非仅由当期 B arrival 决定。

**不能声称**：不能说 adaptive 有效或无效，不能比较 adaptive 与 frozen-static 的吞吐、
JCT、公平性或 SLO；action 与 formal 从未运行。也不能把单 endpoint、单周期 KV=0.874
包装成稳定降档证据。

## 7. 对课题含义与下一步

本轮确认动态容量设计的一半前提——低压时存在安全升档空间——但没有建立可验证的降档
工作区间。下一次必须注册为独立实验：保持硬件、模型、K/W、output cap 不变，只改变
phase 合同；采用显式 drain/recovery gate 或更长 OFF 阶段，并要求每轮 ON 前恢复到同一
KV/waiting 基线。不能把新 rate 或新 phase 与本轮数据合并。

服务器完整归档：
`/root/autodl-tmp/experiment-artifacts/phase_change_corrected_early_stop_feb8a7f.tgz`
（24 MiB），SHA256
`001600b6ef5d0261b4277af01349dcf07ff08b156eb169048fe171cd89be0bca`。
