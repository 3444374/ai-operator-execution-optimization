# SAOR Project mechanism 六臂 rehearsal（2026-08-14）

## 1. 实验目的

本次只验证最终 Project mechanism 合同是否具备可执行、可复算、fail-closed 的证据链，
不决定策略排名，也不授权 formal。六臂为 Project frozen-static，以及共享同一个
bounded-ready observation 的 FIFO、DRR、external VTC-style、strict-priority boundary control
和 SAOR guarded debt（$H_B=0.125W_e$）。这些都是项目内部机制比较，不是 Daft、Ray Data
或 upstream vLLM 原生 baseline。

## 2. 实验设置

| 项目 | 冻结值 |
|---|---|
| 代码 | `63d1730058923609808bec6e3b91ed26a2cd581a` |
| 硬件 | 2× RTX 4090 24 GiB；Ray 1 node、32 CPU、2 GPU |
| 模型服务 | vLLM 0.25.1，Qwen2.5-7B-Instruct，2 endpoint，内部 FCFS |
| vLLM 特性 | continuous batching、chunked prefill、prefix cache，`max_num_seqs=256` |
| 数据路径 | PostgreSQL 18.4 + pgvector 0.8.5 → Daft 0.7.21 → Ray 2.56.1 → vLLM |
| Job | bulk long 512 requests @ 0s；foreground short 512 requests @ 5s |
| 上游 envelope | 每 endpoint K=128、$W_e=65,536$ work，quantum=2,048 |
| ready payload | 每 Job 64 MiB logical payload limit |
| work cost | chat-completions；prompt overhead=29；`fixed_output_cap=256` |
| SLO | request E2E ≤30s |
| 重复 | 每臂 1 个 warm-up cell；没有 formal repeat |
| 写回 | none；PostgreSQL source → validated gather operator-E2E |

服务器 runtime env 原先缺少五个通用路径变量；本次仅在仓库外持久 env 中补入
`PROJECT_ROOT/ARTIFACT_ROOT/MODEL_ROOT/DATA_ROOT/VENV_ROOT` 并保留备份。最终
[environment preflight](raw/environment_preflight.json) 为 `status=ok`、0 missing。环境修复没有
改变冻结策略参数。

## 3. 严谨性自检

### 3.1 矩阵、correctness 与 work 来源

- 6/6 cells completed、0 incident，每 cell 2×512 requests exactly-once；
- endpoint delta 每 cell 1,024 successes；
- 6,144 条 request/submission 一对一 join；total/input/output token 来源均为 endpoint usage；
- endpoint prompt − raw prompt 分布为 `{29: 6144}`；
- audit 明确冻结 `output_bound_source=fixed_output_cap` 和 `completion_max_tokens=256`；
- 每条 request 的 `estimated_output_tokens` 必须恰好等于 256；六臂
  `actual_work≤estimated_work` 越界均为 0；
- 每 cell estimated work 均为 898,522；
- 输入证据清单 SHA256 为
  `84954a3a97a81e4607ead91d1b528057d099893736ac6a6e3dc179fb306479fb`。

独立 CLI 对同一原始文件重算所得 audit 与 wrapper audit byte-for-byte 相同。`+29` 只绑定当前
模型/tokenizer revision、chat template、single-user/no-system message shape 与 chat-completions
协议；签名变化必须重新校准。公平性 service work 使用 endpoint `submission.token_count`，raw
prompt 字段仍原样保留。

### 3.2 资源与饱和

六臂 `metrics_status=resource_metrics_status=mfu_status=ok`。GPU utilization mean 为
95.85%–97.90%，但 MFU 为 35.54%–47.91%、吞吐为 9.42K–12.71K tok/s，说明 GPU utilization
只能证明设备忙，不能替代吞吐、MFU、running/waiting、KV 和 tail 指标。SAOR 的 GPU utilization
mean/p95 为 97.03%/100%，running mean/p95 为 119.32/232，waiting mean/p95/max 为
1.16/8/35，KV mean/p95 为 0.418/0.521。

### 3.3 SAOR 机制门

- 3,244 个 lossless mechanism events，event sequence complete；
- 512 个 SLO-priority grants、96 个 recovery grants、96 个 recovery completions；
- 15/15 debt-repayment episodes completed，0 censored，0 unresolved；
- repayment P95/max 3.234s，低于冻结的 30s empirical gate；
- 1,108/1,108 projected-debt events 离线重算一致；
- projection violation、estimate overrun、recovery estimate overrun、离散 overshoot-bound
  violation 均为 0；
- recovery in-flight max 28 requests / 38,248 work；repayment 时 in-flight work max 32,294；
- actual/projected overshoot max 为 619.5/758 work，观测 bound max 为 876；
- avoidable-idle 与 debt-critical foreign grant 均为 0；
- post-drain work-conservation gate passed。

[rehearsal validation](raw/rehearsal_validation.json) 为 `passed`，但明确记录
`formal_authorized=false`、`performance_ranking_decided=false`。

## 4. 实验数据

以下都是单次 warm-up diagnostic。frozen-static 没有 registered-ready credit ledger，其
completion lag/no-service 为 N/A，不能把 CSV 的占位 0 当作公平证据。

| 臂 | tok/s | MFU | bulk JCT s | fg JCT s | fg P99 s | bulk/fg SLO miss | lag P95 work | no-service s | Jain |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| frozen-static | 9,424.13 | 35.54% | 90.619 | 35.389 | 28.673 | 68.16% / 0% | N/A | N/A | 0.9367 |
| bounded FIFO | 12,714.16 | 47.78% | 66.815 | 45.776 | 39.006 | 55.08% / 39.26% | 140,496.5 | 11.498 | 0.8017 |
| bounded DRR | 12,695.46 | 47.86% | 66.851 | 31.863 | 24.868 | 65.04% / 0% | 61,022.5 | 6.958 | 0.8912 |
| bounded VTC-style | 12,658.78 | 47.71% | 66.858 | 32.541 | 25.477 | 63.87% / 0% | 62,607.5 | 6.546 | 0.8864 |
| strict-priority | 11,967.47 | 45.18% | 70.809 | 19.828 | 13.264 | 80.08% / 0% | 55,210.5 | 16.837 | 0.9905 |
| **SAOR** | **12,713.03** | **47.91%** | **66.952** | **32.776** | **25.504** | **64.45% / 0%** | **54,376.0** | **6.547** | **0.8848** |

SAOR 相对冻结主参照 VTC-style 的单次差异：

- 吞吐 +0.43%，MFU +0.43%；
- bulk JCT +0.14%，foreground JCT +0.72%，foreground P99 +0.11%；
- completion service lag P95 −13.15%，max −8.86%；
- longest no-service +0.014%；
- bulk 30s SLO miss +0.586 percentage point；foreground miss 均为 0。

相对 DRR，SAOR 吞吐 +0.14%、bulk JCT +0.15%、foreground P99 +2.56%，同时 lag P95
−10.89%、no-service −5.91%。相对 FIFO，SAOR 吞吐近似相同（−0.009%），foreground P99
−34.61%、lag P95 −61.30%、no-service −43.06%。没有 comparator 在冻结向量上单次支配 SAOR。
strict-priority 把 foreground P99 降至 13.264s，但相对 SAOR 吞吐低约 5.87%、bulk JCT 高约
5.76%、no-service 达 16.837s；它是 SLO boundary control，不是 equal-share 公平 comparator。

### 4.1 Service lag 的归一化与机制对应

VTC-style 与 SAOR 的 lag P95 分别为 $62,607.5/65,536=0.955W_e$ 与
$54,376/65,536=0.830W_e$；绝对差为 8,231.5 work。SAOR 的冻结 debt cap 为
$H_B=0.125W_e=8,192$，故观测差值约为 $1.005H_B$。这与 projected-debt recovery 直接限制
累计服务欠账的设计方向一致，但不是数学恒等式：在线 debt 按 endpoint-local active set 更新，
离线 completion lag 按全局 registered-ready completion 回放。该接近关系属于**机制一致性证据**，
不能把 lag 这个目标邻近指标单独解释为用户 tail 收益；仍须同时检查 JCT、P99、SLO、
longest no-service 与吞吐保护。

## 5. 结果解释

### 事实

1. 修正后的六臂证据链能在真实 2×4090/vLLM 上完整运行，并对 endpoint work 来源、矩阵
   完整性、Job 身份、固定 admission output cap、repayment 和 work conservation fail closed。
2. SAOR 在该 workload 中形成 15 个有限完成的经验偿还 episode，没有用“发生 recovery grant”
   冒充“债务已经偿还”。
3. 该单次 warm-up 中，SAOR 相对 VTC-style 达到 service-lag 5% headline，但没有改善
   foreground P99；所有冻结保护门均未越界。

### 推断

这说明 SAOR 值得进入独立证据审核：候选价值不是吞吐显著提升，而是在基本保持 VTC-style
吞吐/JCT/tail 的同时降低 completion-accounted service lag。稳定性只能由位置平衡的
1 warm-up + 3 formal 验证。

### 不能声称

- 不能声称 SAOR 已胜过 FIFO/DRR/VTC-style；本次没有 formal repeats，validator 也禁止排名。
- 不能将 static 的 registered-ready lag/no-service 记为 0；它们是 N/A。
- 不能用 Jain 单指标替代 service lag、最长无服务、SLO/JCT 和隔离评价。
- 不能声称理论 repayment bound 已证明；这里只验证冻结假设下的经验 episode 和离散 overshoot。
- 不能外推到 4 Job、跨租户、其他硬件或原生 Daft/Ray baseline。
- strict-priority 只能称经验性 latency boundary control；当前没有可称“理论边界”的下界证明。

### 独立代码与证据审核（2026-08-14）

**证据审核已通过。** 审核从完整 archive 解包后，用当前代码重新执行 6-cell/6,144-request
work-cost audit，得到相同 input-files manifest SHA 与每 cell actual/estimated work；报告中的
配对百分比、五个登记 SHA、96/96 recovery、15/15 repayment、1,108/1,108 projection、
GPU utilization 97.03% 和 MFU 47.91% 均可由 raw 复核。相关本地测试 170 项通过。

**formal 启动审核尚未通过。** 这是授权与报告合同缺口，不推翻本 rehearsal 的机制证据：

1. post-run contract 写入 `rehearsal_validation.validation_sha256`，但当前授权 validator 读取
   `rehearsal_validation.sha256`；授权前必须统一字段，并同时绑定 `repository_commit`、`root_id`、
   `archive_sha256` 与 `valid_rehearsal=true`。独立审核已经完成；若状态机需要保留中间态，应命名为
   `locked_pending_formal_readiness`，不能继续写成“待独立审核”，再由单独提交切换
   `formal_ready/true`；
2. 公平 trace 不完整的 fail-closed 分支引用未定义的 `stem`，应修成可审计的 `ValueError` 并补
   反例测试；
3. 本报告尚未登记同签名 bounded-client 的 feeding-saturation ratio，也未把六臂 TTFT/ITL、
   queue/prefill/decode、KV/prefix、能耗和 pipeline stage 汇总成全组件表。GPU busy 证据不能
   代替 feeding 门；可从现有 raw 恢复的指标先重汇总，确实不可恢复的字段标明 unavailable，
   不从单点快照补造。
4. predecessor failed root 目前只在合同中登记名称和 SHA，仓库内没有可复核实物。若“失败 root
   永久保留”是硬要求，还须登记可访问归档位置或外部 manifest；这不影响当前有效 root 的核真。

## 6. 对课题的含义

结果支持当前层次划分：SAOR 是 vLLM FCFS/continuous batching 上游的 Project
bounded-ready + guarded-debt admission/release 控制，不是 vLLM 内部 token scheduler。static 与
bounded-ready 的吞吐差异也再次说明只看 GPU utilization 不可靠；动态调度必须同时评价效率、
SLO、隔离、公平和机制证据。

## 7. 下一步

1. 保留本 root 与所有 SHA 不变；它已通过独立证据复核，不因后续授权代码修正重写 raw；
2. 修复 formal 授权 schema/证据绑定与不完整公平 trace 的 `stem` 分支，用新 validator 对本封存
   artifact 重验；这些修改不改变 selector，无需自动重跑 rehearsal；
3. 补同签名 bounded-client feeding ratio 与六臂全组件重汇总；如既有 bounded 数据签名不一致，
   只补 ceiling 对照，不重跑或调参本六臂；
4. 上述门全部关闭后，由单独提交显式授权并重跑 readiness，再执行冻结的 position-balanced
   `1 warm-up + 3 formal`；不再调整门槛、workload、参数或 $0.125W_e$，失败即记录 valid negative；
5. 原生 Daft Native/Daft Ray/Ray Data matched comparison 作为独立系统层证据推进。

## 证据与完整性

- [group_runs.csv](raw/group_runs.csv)
- [manifest.json](raw/manifest.json)
- [six per-cell records](raw/records/)
- [work_cost_audit.json](raw/work_cost_audit.json)，SHA256
  `602dfc28e7b3f1dbbf1b1ad5c3d72bf559ef1aa481b2b871d332d2de28a2bb5e`
- [independent CLI re-audit](raw/work_cost_cli_reaudit.json)，与 wrapper audit 哈希相同
- [rehearsal_validation.json](raw/rehearsal_validation.json)，SHA256
  `4f19e0b70c13d4a67a24015ff33444a95a8bab4b773052b62716bfc39540b668`
- [pre-run contract snapshot](raw/project_mechanism_contract.json)
- [environment preflight](raw/environment_preflight.json)，SHA256
  `6a8d3f8e23c457aefd7609b43f0c97e221f34a022b425b50819828ed67ca9ffb`
- [完整 113-file archive](raw/full_artifact.tar.gz)，SHA256
  `5f267dc5847529e8dcea7a4415d52a3e1675a4a983c5190c164ef67af552cedd`

archive 内含所有 request/submission/flush/resource/release-event traces、per-cell records 与日志。
仓库中的 contract snapshot 是运行前的原始快照；`deploy/autodl/` 下的 post-run contract 只登记
本次证据和运行后当时的待独立审核状态，不反向改写已运行的 root；审核结论与剩余 formal
放行门见上文“独立代码与证据审核”。旧 `d6259f5f` root 缺少逐请求固定输出
上界门，仅作 diagnostic，不进入最终 rehearsal 证据。
