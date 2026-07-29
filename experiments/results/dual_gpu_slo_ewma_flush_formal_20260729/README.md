# 双 GPU SLO-aware EWMA flush 对照（2026-07-29）

## 1. 实验设置

本实验对应研究内容二“调度与提交控制”，研究问题是：在 request-level
completion、固定 active-work 上限和固定 Ray actor 资源下，使用
oldest-request SLO slack、arrival/service EWMA、独立饱和容量和 deadband
选择 25–50ms flush window，能否优于 fixed-50 和已有 two-level
queue-adaptive baseline。

链路为 PostgreSQL 18.4 + pgvector 0.8.5 → Daft → Arrow → Ray actor →
两个独立 vLLM endpoint（2× RTX 4090，Qwen2.5-7B，vLLM 0.25.1）。
固定：

- `token_budget=32768`；
- `submission_granularity=request`；
- `max_active_work_per_endpoint=65536`；
- 1 actor × concurrency 256/endpoint，0.5 Ray CPU/endpoint；
- `request_slo_ms=30000`、prefix cache off、fixed 256-token output cap；
- 2048 行 ShareGPT/BurstGPT workload；
- high replay scale 0.001，`near_*` 场景 replay scale 0.006。

比较 `fixed_timeout=50ms`、`queue_adaptive=25/50ms` 和
`slo_ewma=25/50ms`。每个场景 1 次 warm-up + 3 次 formal，formal 按
seed 交错。运行 checkout 为 `ec615ef`。

远端正式入口：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
nohup /root/miniconda3/bin/python \
  code/scripts/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_slo_ewma_flush.example.json \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir experiments/results/dual_gpu_slo_ewma_flush_formal_20260729 \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120
```

本目录归档 [`runs.csv`](runs.csv)、[`manifest.json`](manifest.json) 和
[`formal_summary.csv`](formal_summary.csv)。约 40MB 的 request/submission/
flush/resource/control traces 保留在远端同名目录。

## 2. 实验设计

SLO-EWMA 使用每 endpoint 独立标定的 4,000 tokens/s 饱和容量下界，避免把
arrival-limited achieved throughput 当作服务容量。策略在反馈缺失或 stale
时回退 fixed-50；有有效反馈时按全局 arrival EWMA / 聚合服务容量选择窗口，
并由 oldest-request slack 提供 hard deadline。

预注册晋升规则：相对同负载 fixed-50 至少取得 5% SLO goodput 或吞吐增益，
且 P99 退化不超过 5%；或者吞吐等价但有预先定义的显著 P99/SLO 改善。否则
保留 fixed-50。

正式矩阵前先修复并门禁了一个更基础的 Ray replay completion 回收缺陷：
arrival gap 期间 scheduler 现在持续轮询已完成 ObjectRef，不再把 backend
已完成请求的 credit 和 lifecycle completion 保留到下一次 arrival。512 行
回归门禁的 backend-end → scheduler-completion P95 从 31.20s/217.36s 降到
4ms/4ms，正式矩阵才获准启动。

## 3. 严谨性自检

- **固定 offered-work 上限**：全部 arm 使用每 endpoint 65,536 active work，
  不通过继续增大 batch、K 或 actor 数制造表面收益。
- **固定执行资源**：全部使用 request completion 和已选 1×256 actor pool；
  模型、endpoint、数据、output cap、source order 和资源口径一致。
- **重复与顺序**：24/24 runs 完成、0 incident、0 skipped，18 个 formal
  run 全部 `status=ok`；场景交错执行。
- **exactly-once**：18 个 formal run 共 36,864 request 和 36,864
  submission；逐 run 均为 2048 unique request、2048 unique doc、
  2048 unique submission，request↔submission 集合完全匹配，0 worker
  failure。
- **completion 语义**：backend service-end → scheduler completion P95
  在 high 三策略为 10.32–13.03ms，在 near 三策略为 4.01–4.13ms；旧的
  arrival-gap stale-credit 失真没有复现。
- **资源与版本**：所有 formal run 的 resource/MFU 状态为 `ok`，CSV 均记录
  PostgreSQL 18.4、pgvector 0.8.5、GPU 与 vLLM 服务配置。
- **命名边界**：`near_*` 是预注册 scenario ID。formal 实测该组仅约
  19 个 vLLM running、MFU 7.07%、active work 峰值约 18.7K–19.4K，
  因此应解释为 arrival-limited/underloaded，而不能事后称为真实临界容量。

## 4. 实验数据

| 负载 | 策略 | tokens/s | vs fixed | SLO goodput/s | P99 | SLO violation | GPU util | MFU | wait mean/P50 | fallback |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| high | fixed-50 | 8037.4 | baseline | 24.198 | 17.228s | 0% | 87.10% | 35.01% | 50.00/50ms | 0% |
| high | queue-25/50 | 8030.1 | -0.09% | 24.166 | 17.386s | 0% | 87.29% | 34.96% | 29.19/25ms | 73.44% |
| high | SLO-EWMA | 7995.6 | -0.52% | 24.072 | 17.066s | 0% | 87.25% | 35.07% | 48.84/50ms | 65.72% |
| near | fixed-50 | 1667.0 | baseline | 5.017 | 5.664s | 0% | 73.95% | 7.07% | 50.00/50ms | 0% |
| near | queue-25/50 | 1672.2 | +0.31% | 5.032 | 5.642s | 0% | 73.77% | 7.07% | 25.00/25ms | 54.73% |
| near | SLO-EWMA | 1668.6 | +0.10% | 5.022 | 5.636s | 0% | 74.11% | 7.07% | 42.55/50ms | 48.69% |

相对 fixed-50：

- high SLO-EWMA：tokens/s -0.52%，SLO goodput -0.52%，P99 -0.94%；
- near SLO-EWMA：tokens/s +0.10%，SLO goodput +0.10%，P99 -0.49%；
- 两组均没有 SLO violation，全部差异远低于 5% 晋升门槛；
- SLO-EWMA 的非 fallback 事件主要为 `busy_load_ewma` 及 hysteresis；
  formal trace 没有出现 SLO deadline 主导的 flush reason。

## 5. 结果解释

**事实**：SLO-EWMA 已形成可观测的负载相关动作，但未优于 fixed-50。
high 组仍主要选择 50ms；near 组平均窗口降到 42.55ms，但 P50/P95 仍为
50ms。即使 queue-adaptive 在 near 组始终选择 25ms，其吞吐也只变化
+0.31%，说明 25ms window 差异不是该组的一阶瓶颈。

**推断**：

1. 30s SLO 对 high/near 的 formal P99 分别仍有约 12.8s/24.4s slack，
   hard deadline 没有真实决策机会；
2. 25–50ms 的最大动作幅度只有 25ms，而请求 P99 为 5.6–17.4s，flush
   动作相对服务时标太小，无法预期 5% 级 E2E/SLO 改善；
3. high 组已达到 65K active-work 上限，vLLM 持续有约 169–172 个 running
   request；此时不同关批窗口都能填满服务；
4. near 组则由 arrival replay 限速，GPU 有约 21.7% 采样低于 10% 利用率。
   更早 flush 不能创造尚未到达的请求。

**不能声称**：

- 不能说 SLO-aware 控制普遍无效；本实验只否定当前 25–50ms flush 动作在
  这两个负载和 30s SLO 下的默认晋升。
- 不能把 `near_*` 名称当作真实 near-capacity 证据。
- 不能把零 SLO violation 解释为 SLO-EWMA 的收益；fixed-50 同样为零。
- 不能继续调 EWMA alpha/deadband 并把小于重复波动的差异包装成优化。

## 6. 对课题的含义

“给 vLLM 越多越好”的混淆已经被拆成三个层次：

1. Ray arrival-gap completion 缺陷已经修复，不再用已完成但未回收的 credit
   伪造 active work；
2. active-work 曲线已证明 65K 以上进入吞吐平台，继续加 work 只恶化尾延迟；
3. 本轮在固定 65K 上限和固定 actor 资源后，动态 flush 与 fixed-50 的差异
   只有 -0.52%～+0.31%。

因此当前结论不是“继续堆 batch”，而是：GPU 先被充分供给；供给达到平台后，
25–50ms 上游 flush 不是剩余瓶颈。SLO-EWMA 不晋升，简单
`request + active-work 65K + 1×256 + fixed-50` 继续作为单 job 基线。

## 7. 下一步

1. 不再在同一 25–50ms 动作空间调 alpha/deadband。仅收紧 SLO 也不能扩大
   25ms 的物理控制幅度，缺少通过 5% 门槛的可行上界。
2. 下一轮转向已有代码基础的 Shared-vLLM 1/2/4-job gate：比较 job-local
   与 endpoint-shared request/work credit、work-conserving 公平队列。该动作
   直接控制秒级排队与跨 job 隔离，比继续调毫秒级 flush 更有辨识力。
3. 只有多 job 最小门禁通过 exactly-once、共享 credit 上限、公平性 trace、
   0 failure 和租约释放后，才启动正式公平性矩阵。
4. SLO-aware flush 仅保留为未来具有毫秒级 TTFT SLO、显著网络/RPC 固定成本
   或更大可控 window 的独立机制实验，不作为当前默认策略。
