# 双 GPU Ray actor-pool 形状对照（2026-07-29）

## 1. 实验设置

本实验对应研究内容二“调度与提交控制”，研究问题是：GPU 已处于
request-level active-work 饱和区后，在不增加 offered load 和 Ray CPU
reservation 的前提下，把每个 endpoint 的提交能力拆成多个 Ray actor，是否
能提高吞吐、降低尾延迟或减少批内等待。

链路为 PostgreSQL 18.4 + pgvector 0.8.5 → Daft → Arrow → Ray actor →
两个独立 vLLM endpoint（2× RTX 4090，Qwen2.5-7B，vLLM 0.25.1）。
固定 `token_budget=32768`、request-level replenishment、
`max_active_work_per_endpoint=65536`、每 endpoint 256 个 actor slots、
arrival replay scale 0.001、fixed 50 ms flush、round-robin worker routing；
只改变 actor-pool 形状：

- 1 actor × concurrency 256，0.5 Ray CPU/actor；
- 2 actor × concurrency 128，0.25 Ray CPU/actor；
- 4 actor × concurrency 64，0.125 Ray CPU/actor。

因此三个 arm 都是 256 slots/endpoint、0.5 Ray CPU/endpoint。每个场景
1 次 warm-up + 3 次 formal，formal 顺序按 seed 交错。运行 checkout 为
`c2528d4`。

远端正式入口：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
export ACTIVE_WORK_PER_ENDPOINT=65536
python code/scripts/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_actor_pool_shape.example.json \
  --profiler code/scripts/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir experiments/results/dual_gpu_actor_pool_shape_20260729 \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120
```

本目录归档 [`runs.csv`](runs.csv)、[`manifest.json`](manifest.json) 和
[`formal_summary.csv`](formal_summary.csv)。逐请求、逐 submission、资源和
flush trace 保留在远端同名结果目录。

## 2. 实验设计

正式实验前先运行 64 行真实链路 gate。三个形状均满足：

- 64/64 request 与 submission exactly-once；
- 两个 endpoint 都被使用；
- worker ID、worker PID、逐 worker submission count 可追踪；
- slots 和 CPU 契约与模板一致；
- worker failure 为 0，GPU/resource/MFU 指标状态正常。

预注册晋升规则是：相对 1×256 至少取得 5% 吞吐或 SLO goodput 增益且 P99
退化不超过 5%，或者吞吐等价但有预先定义的显著尾延迟改善。否则保留最简单
的 1×256。只有 trace 显示 worker 不平衡时，才继续比较
least-active-work worker routing。

## 3. 严谨性自检

- **控制 offered load**：三个形状都固定 65,536 work/endpoint 和
  256 slots/endpoint，避免把更多 actor 能接收更多请求误写成策略收益。
- **控制 CPU**：总 Ray CPU reservation 固定为 0.5/endpoint。
- **控制服务与数据**：模型、endpoint、token budget、arrival replay、
  flush、source order、随机种子和总行数一致。
- **重复性**：12/12 runs 完成，0 incident；三个 formal repeat 的吞吐 CV
  为 0.25%–0.96%。
- **测量边界**：这是单 job、两个独立 endpoint 的拓扑实验，不等于多 job
  公平性、故障迁移或异构 GPU 分池结果。

## 4. 实验数据

| Pool 形状 | tokens/s | 相对 1×256 | MFU | P95 | P99 | SLO violation | SLO goodput/s | Ray→service |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1×256 | 7983.9 | baseline | 35.02% | 17.203s | 36.760s | 2.214% | 23.496 | 4.027ms |
| 2×128 | 8143.8 | +2.00% | 35.03% | 17.179s | 36.819s | 2.279% | 23.955 | 24.281ms |
| 4×64 | 8043.8 | +0.75% | 35.05% | 17.153s | 36.700s | 2.132% | 23.704 | 10.362ms |

三个形状的 credit-held 均值约 8.52s，slot-held utilization 均约 0.418；
MFU、P95、P99 和 SLO violation 基本重合。2×128 的 2.00% 吞吐增量没有
达到 5% 晋升阈值，并伴随 Ray→service 均值从 4.0ms 增至 24.3ms。

## 5. 结果解释

**事实**：固定总 slots、active work 和 CPU 后，多 actor 没有稳定达到预注册
晋升门槛；worker 分配均衡、无失败，三组 MFU 和 credit-held 几乎相同。

**推断**：当前瓶颈主要位于 vLLM 服务容量与请求完成分布，而不是单个 Ray
actor 的并发入口。增加 actor 只重排同一组 HTTP 请求，没有改变每 endpoint
可见工作量，也没有消除 planning batch 内的 completion barrier。

**不能声称**：不能据此说 Ray actor pool 普遍无效。多 job 隔离、异构
endpoint、故障迁移和有状态分池仍需要 actor 架构；本实验只否定“在当前
单 job、同构双 endpoint 上仅增加 actor 数就能显著提速”。

## 6. 对课题的含义

Ray 的价值在这里首先是可观测、可约束的 stateful admission 和 completion
管理，而不是 actor 数量本身。当前默认保留 1×256：它满足饱和工作量，
拓扑最简单，Ray→service 延迟最低。后续优化应改变真正的 completion/credit
边界，例如 complete-row service quantum，而不是继续堆 actor。

## 7. 下一步

1. 固定 1×256、65,536 active work 和 32,768 planning budget，完成
   batch、512/1024/2048/4096 complete-row quantum 与 request diagnostic
   的三次重复对照。
2. 只有 service quantum 产生稳定收益且 trace 显示 worker 不平衡时，才恢复
   least-active-work worker routing 消融。
3. 多 job 阶段单独验证共享 request/work credit、公平队列和 actor 分池隔离；
   不把本轮负结果外推到该场景。
