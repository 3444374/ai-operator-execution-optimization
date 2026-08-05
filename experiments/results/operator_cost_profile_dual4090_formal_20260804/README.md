# 双 4090 算子代价估计 formal 首次运行事故（2026-08-04）

本目录记录首次 320-run formal 的有效性审计。审计结论是：两套结果均不得进入
CE0–CE6 训练、排序或决策比较。这里保存的是实验事故证据，不是性能结果。

## 1. 实验设置

预注册设置为 PostgreSQL → Daft → Ray actor → 两个 Qwen2.5-7B/vLLM Completions
endpoint，80 个 candidate cells，每 cell 1 次 warmup + 3 次 formal，共 320 runs。完整的
workload、固定项、候选和指标见
[`../../plans/operator_cost_profile_dual4090_formal_20260804.md`](../../plans/operator_cost_profile_dual4090_formal_20260804.md)。

服务器产生了两个彼此独立的输出目录：

- `dual_gpu_cost_profile_formal_20260804_184835`，launcher PID 550681；
- `dual_gpu_cost_profile_formal_20260804_184931`，launcher PID 557118。

## 2. 实验设计

原设计要求同一台双 GPU 主机同一时刻只有一个 formal runner，并要求所有 run 连接预先
启动的共享 Ray。只有这样，同一 candidate 的三个 formal repeat 才共享相同物理资源
边界，且运行间的差异才可解释为重复波动，而不是另一个实验的竞争或 Ray 冷启动。

本次事后审计同时检查：行数和 phase、scenario/repeat 唯一性、manifest、逐 run 文件、
文件时间线、runner 命令以及 stdout/stderr 中的 Ray 启动证据。

## 3. 严谨性自检

表面完整性通过：两个目录各有 320 行、80 warmup + 240 formal、80 个 scenario、每个
scenario 4 行，全部 `status=ok`；各自 manifest 均写为 `completed_count=320`、
`incident_count=0`，并各有 320 份 request/submission/resource/stdout/stderr 文件。

但是两个更高优先级的独立性门禁失败：

1. 两个 runner 的逐请求证据时间区间几乎完全重叠。共同重叠区间为
   `2026-08-04 18:49:54.807946` 至 `22:04:55.987479`（服务器文件系统时间戳未携带
   时区）。因此两套运行同时竞争相同 vLLM endpoint 和两张 GPU。
2. 两份 manifest 的已完成命令均包含空值 `--ray-address ''`。两个目录合计 640 份
   stdout/stderr 中，640/640 均出现 `Started a local Ray instance`。因此每个子运行都
   重新启动本地 Ray，而不是复用预注册的共享 Ray。

这两项污染不是 `status` 字段中的子进程失败，所以旧 runner 的 `incident_count=0` 没有
检测到它们。不能据此写“0 incident 的有效 formal”。

## 4. 实验数据

紧凑审计数据见 [`invalid_run_summary.csv`](invalid_run_summary.csv)。关键文件哈希如下：

| 输出目录后缀 | `runs.csv` SHA256 | `manifest.json` SHA256 |
|---|---|---|
| `184835` | `56e7c6b1188009e3f91ca7a63900ec7f377840638cc56579817a6845719b396f` | `7456723a2a8a8ec5d99fd9d990a46dae5445d4a1521ad0c11d163be50e31ae2c` |
| `184931` | `d076a6554aa9292fc1cf348a3fc7153dc47d034f022dce46263b31b86e934ba9` | `19db18c8e8ed606d7f8e2a7030ba5bdd7d24cd80f8ab5c53133e1935e29f1134` |

两个哈希不同，说明它们是两套实际执行结果，不是同一目录的复制品。原始大体积 trace
继续留在服务器，不同步 Git；本目录只保存足以复核排除判决的紧凑事实和哈希。

## 5. 结果解释

### 实验事实

- 两个独立 runner 几乎全程并发；
- 所有子运行都使用各自的 local Ray；
- 行数、文件数和子进程退出状态完整，不能抵消资源独立性和执行边界失败。

### 合理推断

并发 runner 会改变 GPU/vLLM 排队和服务时间；每 run 的 Ray 冷启动会进入测量路径或
改变其前置状态。因此 E2E、吞吐、tail、资源利用率和 active-work 候选排名都可能同时
受到污染，且现有 trace 无法事后分离两种影响。

### 待确认

- 修复后的单 runner、共享 Ray 重跑是否能稳定完成 320 runs；
- 新数据上 CE0–CE5 的预测、排序和 regret 是否通过预注册门槛。

### 不能声称

- 不能从这两套数据报告 CE0–CE6 的 MAE、Q-error、ranking、pick rate 或 regret；
- 不能挑选其中一套或单独挑选不重叠的少数行恢复 formal；candidate 的跨时间竞争环境
  已被破坏；
- 不能把 `manifest incident_count=0` 解读成实验环境无事故。

## 6. 对课题的含义

这次事故不否定代价估计方法，也不提供任何方法优劣证据。它暴露的是运行器合同缺口：
原有 lease 只保护单个输出目录，无法阻止不同目录的 runner 同时使用同一主机；环境变量
展开只拒绝“缺失”，没有拒绝“显式存在但为空”。修复后，这两类错误应在首个 GPU run
之前 fail-closed。

## 7. 下一步

1. 本地和服务器验证 host-scope runner lease，以及空 `--ray-address` 的配置门禁；
2. 使用共享 Ray 运行一个最小的 4-run gate，并检查日志中 local-Ray 启动计数必须为 0；
3. 只有 gate 通过后，由远端 agent 在单一新目录重跑 320 runs；
4. 完成后先做独立性、exactly-once、trace 和 repeat 审计，再运行 formal-only
   context-LOO；审计未通过时不生成性能排名。
