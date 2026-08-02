# 双 GPU complete-row service quantum 对照（2026-07-29）

## 1. 实验设置

本实验对应研究内容二“调度与提交控制”，研究问题是：在 GPU 已饱和且
offered work 固定后，把 32,768-token planning batch 切成不拆单行的较小
HTTP/Ray completion 单元，能否消除 whole-submission HOL 和 credit 空转，
并转化为可重复的吞吐、尾延迟或 SLO goodput 收益。

链路为 PostgreSQL 18.4 + pgvector 0.8.5 → Daft → Arrow → Ray actor →
两个独立 vLLM endpoint（2× RTX 4090，Qwen2.5-7B，vLLM 0.25.1）。
固定：

- `token_budget=32768`；
- `max_active_work_per_endpoint=65536`；
- 1 actor × concurrency 256/endpoint；
- 256 slots、0.5 Ray CPU reservation/endpoint；
- arrival replay scale 0.001、fixed 50ms flush、prefix cache off；
- 2048 行 ShareGPT/BurstGPT workload。

比较 whole planning batch、complete-row quantum 512/1024/2048/4096 和
one-row request diagnostic。每个场景 1 次 warm-up + 3 次 formal，formal
按 seed 交错。运行 checkout 为 `c2528d4`。

远端正式入口：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
export ACTIVE_WORK_PER_ENDPOINT=65536
export ACTOR_WORKERS_PER_ENDPOINT=1
export RAY_ACTOR_MAX_CONCURRENCY=256
export RAY_WORKER_NUM_CPUS=0.5
nohup /root/miniconda3/bin/python \
  code/scripts/experiments/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_service_quantum.example.json \
  --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir experiments/results/dual_gpu_service_quantum_20260729 \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120
```

本目录归档 [`runs.csv`](runs.csv)、[`manifest.json`](manifest.json) 和
[`formal_summary.csv`](formal_summary.csv)。大体积 request/submission/
resource/flush trace 保留在远端同名目录。

## 2. 实验设计

量子只在行间切分，不切断单条 prompt。单行 predicted work 大于量子预算时，
该行独占一个 quantum 并标记 oversized。实验前 64 行 gate 6/6 通过：

- 每个场景 64 行 exactly-once，无丢失、无重复、无 worker failure；
- 512/1024 token 分别形成 64/48 个完整行 quantum；
- oversized 单行未被拆分；
- 两 endpoint、worker ID/PID、资源和 MFU 指标均正常。

预注册晋升规则：相对 planning-batch control 至少取得 5% 吞吐或 SLO
goodput 增益且 P99 退化不超过 5%，或者吞吐等价但有预先定义的显著尾延迟
改善。否则不把 service quantum 加入默认策略。

## 3. 严谨性自检

- **容量已固定**：八档 active-work 曲线已先选定 65,536，所有 arm 实际峰值
  均达到约 65,535–65,536；没有通过增加 offered work 制造收益。
- **Ray 资源已固定**：全部使用已选 1×256 actor pool、相同 slots/CPU。
- **控制变量**：数据、模型、endpoint、planning budget、flush、arrival replay、
  source order、随机种子和 output cap 一致；只改变真实 completion 边界。
- **重复性**：24/24 runs 完成，0 incident；formal 吞吐 CV 为
  0.16%–1.41%。
- **时延语义边界**：batch/quantum 的 request trace 使用真实
  submission-level 完成时间并复制到成员行；request diagnostic 才是独立
  request completion。P95/P99 可作 guardrail，但不能伪称为后端逐行完成时间。
- **启动事故**：第一次正式启动遗漏 runner 的 profiler/Python/health/metrics
  参数，在 argparse 阶段退出，未创建结果目录、未占用 GPU；失败日志保留。
  随后按 AutoDL runbook 的完整参数重新启动。

## 4. 实验数据

| 粒度 | submissions | rows/submission | tokens/s | vs batch | MFU | P99 | bounded wait | credit-held |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| planning batch | 570 | 3.593 | 7989.0 | baseline | 34.64% | 36.683s | 33.860s | 10.171s |
| quantum 512 | 2048 | 1.000 | 7986.8 | -0.03% | 35.01% | 36.992s | 27.832s | 8.529s |
| quantum 1024 | 1067 | 1.919 | 7997.9 | +0.11% | 34.63% | 36.612s | 32.689s | 9.170s |
| quantum 2048 | 703 | 2.913 | 7999.0 | +0.12% | 34.60% | 36.673s | 33.579s | 9.812s |
| quantum 4096 | 584 | 3.507 | 8031.9 | +0.54% | 34.70% | 36.914s | 33.732s | 10.123s |
| request diagnostic | 2048 | 1.000 | 8129.2 | +1.75% | 34.99% | 36.948s | 28.009s | 8.534s |

512-token quantum 将 credit-held 均值从 10.171s 降至 8.529s（约 -16.1%），
P95 从 31.474s 降至 11.508s；bounded wait 约 -17.8%。request diagnostic
有相同的 credit/bounded-wait 机制变化。1024/2048/4096 随聚合增大逐步退回
planning-batch 的 completion 行为。所有量子的吞吐、MFU、P99 和 SLO
goodput 均未达到晋升阈值。

## 5. 结果解释

**事实**：complete-row quantum 的实现确实缩短了 credit-held，说明原
whole-submission barrier 存在且已被修复；但最细粒度没有提高吞吐。所有 arm
都稳定落在约 8k tokens/s、约 35% MFU 的同一平台。

**推断**：在每 endpoint 已保有 65K active work 时，vLLM 的
iteration-level continuous batching 已持续有足够请求可执行。提前 1–2 秒
释放上游 credit 只改变“候补请求何时可进入”，没有让 GPU 获得额外可执行
工作；更多 HTTP/Ray submission 则抵消了可能的调度收益。

**文献边界**：Orca/vLLM 在服务内部按 iteration 加入/移出请求。上游量子
不能再实现一次 continuous batching，只能修正 HTTP 返回、Ray completion
和 credit 释放边界。本实验与该边界一致。

**不能声称**：

- 不能说 HOL 不存在；credit-held 的下降已证明它存在。
- 不能说“消除 HOL 必然提高稳态吞吐”；当前饱和库存掩盖了该收益。
- 不能把 submission-level P99 当作 batch 内每行真实完成时间。
- 不能把本轮负结果外推到低 active-work、网络 RTT 高、多 job SLO 干扰或
  endpoint 失衡场景。

## 6. 对课题的含义

“给 vLLM 越多越好”的问题已经被容量曲线和本实验分开解决：

1. 65K 以上吞吐平台不再增长，继续加 work 只恶化尾延迟；
2. 在该平台固定 work 后，改变 completion 粒度只带来 0%–1.75% 吞吐差异；
3. 因此当前 steady-state 瓶颈是模型服务容量，不是 Ray actor 数或批内
   credit barrier。

不晋升固定 service quantum 为默认性能策略。request-level 路径继续保留为
精确 credit 释放、真实逐请求观测和后续多 job 公平控制的基础设施；这是一项
控制语义选择，不包装成超过 5% 的性能贡献。

## 7. 下一步

1. 不运行 least-active-work worker routing：actor-pool trace 没有不平衡，
   service quantum 也未产生可放大的 worker 级收益。
2. 下一轮把“有无决策机会”作为变量：在仍能达到 GPU 饱和但包含 burst/gap、
   foreground/background 和不同 SLO 的负载中，验证 oldest-request slack、
   token backlog、arrival/service EWMA 与 hard deadline 驱动的 flush。
3. 多 job 阶段使用 request-level completion，比较 job-local 与 shared
   request/work credit、work-conserving 公平队列；此时提前释放 credit
   可能转化为隔离与公平收益，而不只是 steady-state 吞吐。
4. 若上述动态/多 job 场景仍无稳定收益，登记负结果并保留
   `request + static 65K + fixed-50ms + 1×256` 简单基线。
