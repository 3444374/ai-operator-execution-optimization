# 双 4090 算子代价估计 formal profile（2026-08-04）

> **归档状态（2026-08-27）**：本合同已完成。首次 320-run 因共享 GPU 竞争和 local Ray
> 被排除；修复后的 v2 cache-on 正式运行 320/320 有效、0 incident，并已完成 CE0–CE5
> context-LOO 评估。有效结果见
> [`../../results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/`](../../results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/)；
> 首次无效证据继续单独保留。下文的“重跑”指事故后的原始执行合同，不是当前待办。
>
> **历史状态：首次运行无效；修复后的 cache-on 最小门禁已通过。**
> 2026-08-04 的两套 320-run 输出几乎全程
> 并发使用同一组 vLLM/GPU，且空 `--ray-address` 使每个子运行启动 local Ray；两套数据
> 均排除出 CE0–CE6 分析。事故证据见
> [`../../results/operator_cost_profile_dual4090_formal_20260804/README.md`](../../results/operator_cost_profile_dual4090_formal_20260804/README.md)。
> host-scope lease、非空共享 Ray 门禁和 cache-on 最小复跑已在提交 `2b7da6c` 上通过；
> 远端 agent 可在完成本页全部 preflight 后，于单一新目录重跑。不得从无效目录 resume
> 或挑选部分结果。门禁证据见
> [`../../../feasibility/results/cost_profile_cacheon_gate_20260805/README.md`](../../../feasibility/results/cost_profile_cacheon_gate_20260805/README.md)。
> 2026-08-05 按真实部署口径把 v2 主合同冻结为 **prefix cache on**；cache-off 只作
> 单独机制消融，不进入主性能排名，也不与 cache-on 行混合训练。

## 1. 研究问题

在与旧单 5070 数据完全隔离的双 4090 环境中，现有 CE0–CE5 代价估计方法能否仅凭
执行前特征，对未见过的 workload/rows/output context 排序四个 active-work 候选，并
选择接近 oracle 的候选。该实验评价预测、排序和决策质量，不评价上游调度策略的论文增益。

## 2. 固定环境和执行链路

- PostgreSQL 18.4 → Daft PostgreSQL source → token-budget organizer → Ray actor →
  2× vLLM 0.25.1/Qwen2.5-7B Completions；
- `httpx_async`、request submission、1×256 actor/endpoint、token budget 8192、fixed
  50 ms、prefix cache on、no writeback；
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
9. artifact root 的 host-scope lease 证明同一时刻只有一个实验 runner；启动前和完成后
   均检查没有 sibling output runner。
10. `--ray-address` 必须为非空共享 Ray 地址，且全部 stdout/stderr 中
    `Started a local Ray instance` 计数必须为 0。
11. manifest/live process/CSV 三处 cache 状态必须一致为 enabled；每条 formal 的
    cache query/hit delta 必须可用且满足 `0 ≤ hits ≤ queries`、hit rate∈[0,1]。

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

- 本节 CE0–CE5 是**算子代价估计方法 baseline**，不是 Daft/Ray Data/OceanBase 等
  数据库 AI 算子系统 baseline。该 320-run 实验不能替代系统原生 baseline formal，二者
  只共享 workload 与观测字段，不共享比较结论。
- CE0：训练集均值；CE1：解析模型；CE2：lookup；CE3：Ridge；CE4：LightGBM；
  CE5：解析模型 + residual correction；CE6 oracle 只作上界。
- `service_prefix_caching` 是 decision-context 身份字段，不是执行后特征。cache hit rate
  只用于解释误差和机制；禁止将本 run 的 hit rate 输入同一 run 的 pre-execution 预测。
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
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
test -n "${RAY_ADDRESS:-}" || {
  echo "RAY_ADDRESS is missing; start Ray and update the runtime env first" >&2
  exit 2
}
/root/miniconda3/bin/python -c \
  "import sys; from pathlib import Path; sys.path.insert(0, 'code'); from scripts.experiments.run_ai_operator_scenarios import _load_config; c=_load_config(Path('deploy/autodl/dual_gpu_cost_profile_formal.example.json')); assert len(c.scenarios)==80; assert len({x.scenario_id for x in c.scenarios})==80; print(c.experiment_id, len(c.scenarios))"
```

再由远端 agent 在**新目录**启动；不要用 `/usr/bin/time` 包裹命令：

```bash
mkdir -p /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260805
screen -dmS cost-formal-cache-on-v2 bash -lc '
  cd /root/autodl-tmp/ai-operator &&
  set -a && source /root/autodl-tmp/ai-operator-runtime.env && set +a &&
  /root/miniconda3/bin/python code/scripts/experiments/run_ai_operator_scenarios.py \
    --config deploy/autodl/dual_gpu_cost_profile_formal.example.json \
    --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
    --python-executable /root/miniconda3/bin/python \
    --output-dir /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260805 \
    --health-url http://127.0.0.1:8000/health \
    --metrics-urls "$MODEL_METRICS_URLS" \
    --idle-timeout-s 120 \
    > /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260805/runner.log 2>&1
'
```

启动前还必须确认 5 个 workload 各有至少 256 行、两个 health/metrics endpoint 正常、
`prefix_caching=true` 与两个服务进程的 `--enable-prefix-caching` 一致、Ray/runner 无重复实例。上述命令依赖已加载的
`/root/autodl-tmp/ai-operator-runtime.env`；缺少任一环境变量时 config loader 会
fail-closed，不允许手填默认值继续跑。

2026-08-04 首次 formal 已证实：仅检查变量“存在”不够，变量可能存在但为空。远端
agent 必须先按 `deploy/autodl/README.md` 启动 Ray，把**实际非空地址**写回该机器自己的
runtime env，并做连接门禁；不能仅为模板展开填入未监听地址。config loader 现在拒绝
显式空值，host-scope lease 拒绝不同输出目录上的并发 runner。

完成后有效重跑结果进入新的
`experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_<date>/`，不得覆盖或
混入保存首次无效运行证据的 `operator_cost_profile_dual4090_formal_20260804/`。新目录应包含
七步 README、compact summary、formal-only LOO JSON、raw archive SHA256 和不能声称的
结论。若门禁失败，保留 incident，不生成性能排名。

## 8. 条件性后续：TPC-H-derived AI 查询计划代价验证

### 8.1 定位与启动门禁

本方向保留为 `planned-conditional`：验证代价估计能否从“为 active-work 候选排序”继续
泛化到“为包含 AI 算子的数据库查询计划排序”。它仍是两项策略的共同使能组件，不新增第三项
研究内容，也**不改变或扩展本页 320-run 合同**。

只有本页重跑产生一套完全有效的数据，并且至少一个可部署估计器同时满足 §6 已冻结的
candidate pairwise、median/macro/max regret 门槛，才允许进入计划级 capability；否则先修复
局部估计器，不启动 TPC-H-derived GPU 长实验。

### 8.2 名称与合规边界

- 只能称 `TPC-H-derived AI operator plan validation` 或 `TPC-H-inspired`，不得称官方 TPC-H
  result/compliant benchmark；TPC-H 原始 schema/query 没有 AI 算子，本实验会增加 AI 调用和
  新的候选计划。
- TPCx-AI 仅提供数据管理、scoring、质量、性能价格和审计合同；除非完整满足官方规范，任何
  推理子集都只能称 `TPCx-AI-inspired`。
- TPC-H 原生查询只作关系执行与 cardinality/cost 采集的相邻控制，不能替代数据库 AI Function
  或本项目 AI pipeline baseline。

### 8.3 最小可验证设计

从 TPC-H 的 `orders`/`lineitem`/`part` 等 comment 字段构造 bounded AI_COMPLETE，所有候选
计划必须产生相同的最终关系结果和相同模型请求集合（除非实验变量就是减少 AI 调用数）。首个
capability 只覆盖三类等价计划对：

1. 独立关系 filter 在 AI 前执行 vs AI 后执行；
2. 对可复用维表文本先做 AI 并物化再 join vs join 后对重复行做 AI；
3. 同一关系计划下选择不同的冻结 active-work/endpoint 配置。

先运行小 scale-factor correctness/cost decomposition gate，再做 scale ramp；正式规模由至少
60 秒稳态、无 spill/资源死锁和模型服务饱和门禁决定，禁止凭机器名称预设 SF1/SF10 为正式点。

### 8.4 Baseline 与特征边界

计划级至少比较：关系优化器原生 cost + 固定每行 AI 常数、输入 token/output cap 解析模型、
profile lookup、Ridge/LightGBM、解析模型 + residual correction，以及 actual runtime oracle
（不可部署上界）。GRACEFUL/COSTREAM/Abacus 作为方法与指标依据；只有拿到可运行官方实现并
通过适配审计，才能进入数字排名，不能按论文描述自写近似实现后冒充原系统。

可部署预测只能使用提交前已知的 relation cardinality/selectivity estimate、prompt/token/frame
特征、模型/硬件/服务配置和候选 action。实际 output length、当前 run 的 cache hit、实际 service
time、最终 queue trace 均属于执行后信息，只能作误差解释或 oracle。

### 8.5 评价与停止条件

除 §5 三层指标外，计划级必须报告 relation/cardinality error、operator-level 与 whole-query
Q-error、plan pairwise accuracy/pick rate、selected/oracle query JCT、plan regret、AI invocation/
token work、估计器开销和最终结果集合一致性。若简单固定每行 cost 已与学习模型持平，或加入 AI
代价后仍不能稳定改善 plan regret，则把结果保留为负结果并停止扩矩阵，不把本方向包装成已实现的
查询优化器贡献。

### 8.6 Fatal-flaw 预注册

1. **Scope creep**：同时做调度系统、通用查询优化器和新 benchmark 会使贡献失焦。防御方式是
   仅在局部估计器过门槛后做最小 held-out 计划选择验证，并保持“共同使能组件”定位。
2. **不可归因/语义不等价**：关系计划可能改变 AI 调用集合、cache locality 和结果质量。防御方式是
   保存 canonical request manifest、关系结果 digest、阶段计时和 actual invocation/token work；
   不等价候选进入 system-level quality/cost track，不与 fixed-work runtime 排名混读。
