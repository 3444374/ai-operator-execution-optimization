# 双 4090 算子代价估计 formal profile（2026-08-04）

> **状态：已预注册，暂缓执行。** 2026-08-04 按用户要求只完成门禁、语义审计和
> `main` 推送；本地 agent 不运行 320-run formal。只有远端 agent 在确认 `main`、服务、
> 数据和磁盘门禁后才可启动。此前一次启动因服务器没有 `/usr/bin/time` 在首个 run 前
> 退出，未产生实验数据，空目录已清理；正式命令不再依赖该可选工具。

## 1. 研究问题

在与旧单 5070 数据完全隔离的双 4090 环境中，现有 CE0–CE5 代价估计方法能否仅凭
执行前特征，对未见过的 workload/rows/output context 排序四个 active-work 候选，并
选择接近 oracle 的候选。该实验评价预测、排序和决策质量，不评价上游调度策略的论文增益。

## 2. 固定环境和执行链路

- PostgreSQL 18.4 → Daft PostgreSQL source → token-budget organizer → Ray actor →
  2× vLLM 0.25.1/Qwen2.5-7B Completions；
- `httpx_async`、request submission、1×256 actor/endpoint、token budget 8192、fixed
  50 ms、prefix cache off、no writeback；
- active-work 是唯一候选变量：32,768 / 49,152 / 65,536 / 98,304 per endpoint；
- 运行配置：`deploy/autodl/dual_gpu_cost_profile_formal.example.json`。

## 3. 二十个 decision contexts

在不查看 formal 结果的前提下冻结以下笛卡尔积：

- workload（5）：`short_prompt_lt50`、`long_prompt_ge150`、
  `sharegpt_concentrated`、`sharegpt_multiturn`、`lmcache_agent`；
- rows（2）：128 / 256；
- completion cap（2）：64 / 256；
- candidate（4）：上述 active-work credit。

共 20 contexts、80 candidate cells。每 cell 1 warmup + 3 formal，共 320 runs；固定 seed
后全局交错。prompt 上限 7000，保证观测到的长 prompt 加 256 输出仍低于 8192 model
length。五个 workload 在运行前均需验证可提供至少 256 行。

## 4. 数据有效性门禁

1. manifest 320/320 completed、0 unrecovered incident；summary 80 warmup + 240 formal；
2. 每条 formal 的 request/submission 数等于 context rows，`doc_id` exactly-once；
3. 两个 endpoint 均接收请求，Prometheus/resource trace 状态为 `ok`；
4. 非 replay 的 `flush_trace_status=not_applicable_non_replay`；不能伪造 flush CSV；
5. formal-only 加载后恰有 20 context × 4 candidate × 3 repeats；warmup 必须排除；
6. 每个 context 的四个 23 维特征向量与 candidate ID 均不同，机器/协议 context 一致；
7. 服务进程快照证明模型、端口、cache、max batched tokens 和 max seqs；
8. 任一 cell CV>5%、输出 token 或 endpoint 分布异常时单列并补跑，不静默删除离群值。

## 5. 预注册评价指标

### 预测层

MAE、RMSE、Q-error P50/P90/P95/P99/max、prediction interval coverage/width。

### 排序层

先聚合同 candidate 的 3 次 formal，再计算 within-context Spearman、pairwise accuracy、
Top-K precision。row-level pairwise 只保留历史兼容，不再作为新数据主指标。

### 决策层

pick rate、selected/oracle runtime、macro regret mean/median/max、pooled regret、selected
rank、fallback rate 和 estimator overhead。exact predicted tie 固定取字典序最小
candidate ID，并同时报告 tie-context 数；不允许依赖 CSV 行顺序。

## 6. Baseline 与晋级口径

- CE0：训练集均值；CE1：解析模型；CE2：lookup；CE3：Ridge；CE4：LightGBM；
  CE5：解析模型 + residual correction；CE6 oracle 只作上界。
- 新数据的候选主门槛在运行前冻结为：candidate pairwise ≥0.75、median regret ≤5%、
  macro mean regret ≤5%、max regret ≤15%。任何一项失败都不能接管计划选择。
- “CE5 优于 baseline”必须同时列 CE1/CE2/CE4，不只选一个弱对照；如果不同指标各有
  胜负，结论写成 Pareto/trade-off，不写“全面最好”。

## 7. 运行与时间预算

pilot 两次 8-run 墙钟为 312.719/317.620 s。线性外推 320 runs 约 3.5 小时；为服务
启动、长 prompt、失败补跑和证据审计预留约 4 小时。使用 runner lease 和独立 output
directory；SSH 断开不应由不受监管的前台任务承担，正式启动应进入 `screen` 或等价后台会话。

在仓库根目录先执行不触碰 GPU 的配置门禁：

```bash
/root/miniconda3/bin/python -c \
  "import sys; sys.path.insert(0, 'code'); from scripts.experiments.run_ai_operator_scenarios import _load_config; c=_load_config('deploy/autodl/dual_gpu_cost_profile_formal.example.json'); assert len(c.scenarios)==80; assert len({x.scenario_id for x in c.scenarios})==80; print(c.experiment_id, len(c.scenarios))"
```

再由远端 agent 在**新目录**启动；不要用 `/usr/bin/time` 包裹命令：

```bash
mkdir -p /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_20260804
screen -dmS cost-formal bash -lc '
  cd /root/autodl-tmp/ai-operator &&
  set -a && source /root/autodl-tmp/ai-operator-runtime.env && set +a &&
  /root/miniconda3/bin/python code/scripts/experiments/run_ai_operator_scenarios.py \
    --config deploy/autodl/dual_gpu_cost_profile_formal.example.json \
    --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
    --python-executable /root/miniconda3/bin/python \
    --output-dir /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_20260804 \
    --health-url http://127.0.0.1:8000/health \
    --metrics-urls "$MODEL_METRICS_URLS" \
    --idle-timeout-s 120 \
    > /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_20260804/runner.log 2>&1
'
```

启动前还必须确认 5 个 workload 各有至少 256 行、两个 health/metrics endpoint 正常、
`prefix_caching=false` 与服务进程参数一致、Ray/runner 无重复实例。上述命令依赖已加载的
`/root/autodl-tmp/ai-operator-runtime.env`；缺少任一环境变量时 config loader 会
fail-closed，不允许手填
默认值继续跑。

完成后结果进入 `experiments/results/operator_cost_profile_dual4090_formal_20260804/`，
包含七步 README、compact summary、formal-only LOO JSON、raw archive SHA256 和不能声称的
结论。若门禁失败，保留 incident，不生成性能排名。
