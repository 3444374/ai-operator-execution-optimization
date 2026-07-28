# 双 GPU request-level active-work 扩展饱和曲线（2026-07-29）

## 1. 实验设置

本实验对应研究内容二“调度与提交控制”，回答一个先于策略比较的问题：
在当前 2× RTX 4090、Qwen2.5-7B、vLLM 0.25.1 和 2048 行
ShareGPT/BurstGPT workload 上，每 endpoint 需要多少 predicted-token active
work 才能进入吞吐平台。

执行链路为 PostgreSQL 18.4 + pgvector 0.8.5 → Daft → Arrow →
request-level Ray actor → 两个独立 vLLM endpoint。固定
`token_budget=32768`、`max_inflight=256/endpoint`、1 actor/endpoint、
actor concurrency 256、arrival replay scale 0.001、fixed 50 ms flush、
prefix cache off；只改变
`max_active_work_per_endpoint={16384,24576,32768,49152,65536,81920,98304,131072}`。
每个场景 1 次 warm-up + 3 次 formal，seeded interleaving。

远端运行入口：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
nohup /root/miniconda3/bin/python \
  code/scripts/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_active_work_curve.example.json \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir experiments/results/dual_gpu_active_work_saturation_20260729 \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120
```

运行 checkout 在启动前核对为 `1774184`。完成后保留
[`runs.csv`](runs.csv)、[`manifest.json`](manifest.json) 和
[`formal_summary.csv`](formal_summary.csv)；大体积 request/submission/resource
trace 继续保留在远端结果目录。

## 2. 实验设计

预注册选择规则：在所有安全档中，选择第一个同时满足以下条件的最小 work：

1. formal 平均 tokens/s 达到最大已测安全档的 97%；
2. 下一个安全档的相对吞吐增益低于 3%。

若不存在该点，结论必须是 `saturation_not_reached`，不能把最高已测档改名为
饱和点。高负载 OOM、超时或失败必须保留 incident 并停止继续增压。

## 3. 严谨性自检

- 32/32 run 成功：8 warm-up + 24 formal；每档 formal `n=3`。
- manifest=`completed`，0 skipped、0 incident，runner lease 正常释放。
- 每个 formal 均有 2048 request events、2048 submission events；
  request-level exactly-once 完整。
- resource metrics 与 MFU 状态全部为 `ok`，每个 formal 至少 222 条 resource
  sample；两 endpoint 均收到请求。
- tokens/s CV 为 0.05%–1.73%，平台区的差异量级可与重复波动直接比较。
- 131K 档实际最高 active work 只有 112,306，说明该档 credit 没有被当前
  workload/slot 组合填满；这不是 131K 容量被完全施压后的证明。

## 4. 实验数据

| active work / endpoint | tokens/s | 相邻增益 | 最大值占比 | MFU | P99 | SLO violation |
|---:|---:|---:|---:|---:|---:|---:|
| 16,384 | 4,903 | — | 59.71% | 20.99% | 68.96s | 52.36% |
| 24,576 | 6,021 | +22.79% | 73.32% | 26.09% | 42.91s | 33.37% |
| 32,768 | 6,800 | +12.94% | 82.81% | 29.47% | 33.14s | 10.14% |
| 49,152 | 7,655 | +12.57% | 93.22% | 33.10% | 35.12s | 1.89% |
| **65,536** | **8,030** | **+4.91%** | **97.80%** | **35.08%** | **36.78s** | **2.17%** |
| 81,920 | 8,104 | +0.92% | 98.70% | 35.22% | 38.71s | 2.80% |
| 98,304 | 8,211 | +1.32% | 100.00% | 35.75% | 40.05s | 3.34% |
| 131,072 | 8,211 | -0.003% | 100.00% | 35.70% | 40.07s | 3.32% |

## 5. 结果解释

### 事实

- `BEST_TESTED_THROUGHPUT` 位于 98,304，均值 8,211 tokens/s；131,072 与它
  完全持平。
- 65,536 已达到最大均值的 97.80%，下一档只增加 0.92%，因此按预注册规则
  选择 `SATURATED_ACTIVE_WORK_PER_ENDPOINT=65536`。
- 从 65K 增至 98K（+50% credit）只增加 2.25% tokens/s，P99 却从
  36.78s 增至 40.05s，SLO violation 从 2.17% 增至 3.34%。
- MFU 从 65K 的 35.08% 到平台最高约 35.75%，同样只有很小增量。

### 推断

当前服务器与 workload 已在约 65K work/endpoint 进入服务能力平台。继续增加
上游 credit 主要增加在途工作和尾延迟，而不是按比例增加模型有效吞吐。因此后续
策略实验应固定 65,536，并比较在相同 offered-work 上限下谁能减少 HOL、
credit-held 空转和 Ray-to-service delay。

### 不能声称

- 65,536 不是跨模型、跨 workload 或跨硬件的常数。
- 98K 单点均值最高不代表它是更好的综合策略；其领先幅度接近重复波动且尾延迟
  更差。
- 当前约 35.7% MFU 不证明 GPU kernel 已达到硬件理论上限；本实验只标定当前
  完整服务链路的上游 work 平台。
- 本曲线不比较数据组织或 actor pool 算法，不能把容量标定写成策略贡献。

## 6. 对课题的含义

本结果解决了此前“给 vLLM 越多就越好”的因果混淆：在欠载区确实呈现
力大砖飞，但扩展到 65K 以后吞吐进入稳定平台，98K/131K 已没有有意义增量。
因此研究问题不再是无限增大 batch/K，而是：

- 以最小饱和 work 保持 GPU 供给；
- 在该固定 work 下缩短 completion/credit 周期；
- 降低 P99、SLO violation 和无效在途队列；
- 在多 job 时隔离公平性和干扰。

## 7. 下一步

1. 固定每 endpoint `active_work=65536` 和 256 actor slots，比较
   1×256、2×128、4×64 Actor Pool，Ray CPU reservation 同为 0.5/endpoint。
2. 固定最佳 pool、planning budget 与 active work，比较 whole batch、
   complete-row service quantum 512/1024/2048/4096 和 request diagnostic。
3. 只有观察到稳定 worker imbalance 时再比较 least-active-work routing。
4. 单项机制必须超过重复波动且不恶化 P99/failure 才能进入组合策略。
