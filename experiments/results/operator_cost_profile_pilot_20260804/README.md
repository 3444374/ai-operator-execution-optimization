# 双 4090 算子代价 profile 4-cell pilot（2026-08-04）

本结果对应算子代价估计的采样基础设施门禁。它验证“能否在当前机器上生成一个完整、
可区分的四候选 decision context”，不评价 estimator 精度，也不宣布某个 active-work
候选更快。

## 1. 实验设置

- 硬件：32 CPU、240 GiB RAM、2×NVIDIA GeForce RTX 4090；每个 vLLM endpoint
  固定一张 GPU。
- 软件：PostgreSQL 18.4、pgvector 0.8.5、vLLM 0.25.1、Qwen2.5-7B-Instruct。
- 服务：Completions + `httpx_async`，端口 8000/8001，`max-num-batched-tokens=8192`、
  `max-num-seqs=256`、prefix cache off。`v2_raw.tar.gz` 中的
  `service_process_snapshot.txt` 保存真实进程命令；命令没有
  `--enable-prefix-caching`。
- 数据：PostgreSQL `sharegpt_multiturn`，固定 512 行、按 `doc_id`，输出上限 256。
- 执行：Daft PostgreSQL source → token-budget organizer → Ray actor → 两个 vLLM endpoint；
  request-level submission，1×256 actor/endpoint，fixed 50 ms，no writeback。
- 历史 v2 冻结配置位于 `v2_raw.tar.gz` 的 manifest；当前
  `deploy/autodl/dual_gpu_cost_profile_pilot.example.json` 已升级为 cache-on v3，不能
  用来冒充复现本目录的 cache-off 数字。

## 2. 实验设计

唯一变量是每 endpoint active-work credit：32,768、49,152、65,536、98,304。
每臂 1 warmup + 1 formal，固定 seed 后交错执行，共 8 runs。v1 首次运行完成 8/8，
但预注册文档错误地要求非 arrival-replay 也产生 flush trace，因此只作为诊断轮。
修正 schema 和合同后，v2 用相同 workload/候选独立重跑。

实际命令：

```bash
python code/scripts/experiments/run_ai_operator_scenarios.py \
  --config deploy/autodl/dual_gpu_cost_profile_pilot.example.json \
  --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir /root/autodl-tmp/experiment-artifacts/dual_gpu_cost_profile_pilot_v2_20260804 \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120
```

## 3. 严谨性自检

- v2 manifest：8/8 completed、0 skipped、0 incident；8 条 summary 均为 `status=ok`。
- 每个 formal request trace 恰有 512 行和 512 个 unique `doc_id`，全部 completed。
- 两个 endpoint 每臂均有请求；单 endpoint 占比范围为 48.6%–50.8%，没有单卡失活。
- 每个 formal 有 512 request events、512 submission events 和 67–111 resource samples。
- 非 arrival-replay 没有逐次关批循环，故 `flush_trace_status=not_applicable_non_replay`、
  path 为空、events=0；这不是“观测到 0 次 flush”。
- 四臂由当前 estimator driver 产生 23 维有限特征；4 个向量、4 个 candidate ID 均不同，
  decision-context ID 同为 `a8dc0e30c7160074`。
- v2 formal 仍只有 n=1，不能计算 CV/CI，也不能据吞吐排序选择配置。

## 4. 实验数据

下表只列 v2 formal；完整字段见 `summary.csv` 和压缩 raw。

| active work/endpoint | E2E (s) | request P50/P95/P99 (s) | model-request tok/s | vLLM running/waiting mean | MFU |
|---:|---:|---:|---:|---:|---:|
| 32,768 | 40.824 | 22.416 / 39.565 / 40.467 | 10,432 | 68.9 / 0.1 | 0.329 |
| 49,152 | 31.747 | 17.664 / 31.033 / 31.342 | 13,279 | 101.1 / 1.4 | 0.408 |
| 65,536 | 28.844 | 20.414 / 28.496 / 28.516 | 15,499 | 123.5 / 3.4 | 0.483 |
| 98,304 | 24.402 | 15.999 / 23.985 / 23.987 | 17,621 | 182.7 / 12.4 | 0.561 |

v2 整套 8 runs 的实际墙钟为 312.719 s（5m12.719s）；v1 为 317.620 s。
相同 formal 单元的 v2-v1 E2E 差为 -3.76% 至 +0.95%，只用于粗略估时。

## 5. 结果解释

### 实验事实

1. 当前双 4090 链路能稳定产出四个候选、同一 context、逐请求和资源 trace 完整的
   cost-profile 单元，v2 合同通过。
2. 在这两个单次诊断轮中，active-work 增大时 E2E 下降、model-request tok/s 和 MFU
   上升；98,304 同时出现更高的 vLLM waiting（12.4 mean）。
3. request trace 的 prompt token 总量四臂相同（278,408），实际输出总量约
   113.8K–114.2K；候选没有通过改变输入工作量获得表面优势。

### 合理推断

98,304 可能尚未越过当前 workload 的吞吐饱和点，但 waiting 上升说明继续扩 credit
可能增加排队。必须用至少 3 次 formal 和更宽的 active-work 曲线才能定位平台；本 pilot
不能完成该判断。

### 不能声称

- 不能声称 98,304 是最优配置或显著优于 65,536；
- 不能把 GPU util 当 MFU，也不能用本轮 n=1 生成误差条；
- 不能把 pilot 加进旧单 5070 的 204 条 formal 数据直接训练普通 LOO；
- 不能把服务启动命令快照等同于 vLLM 内部所有 runtime feature 的逐项证明。

## 6. 对课题的含义

本轮闭合的是“新机器上如何采集可用于代价估计的候选数据”这一工程门禁。23 维
pre-execution schema 能区分 active-work 候选，并通过机器/协议 context 隔离避免把
单 5070 与双 4090 静默混合。它尚未证明 CE5 或任何估计器在新硬件上有效。

## 7. 下一步

两种补数目标不能混用：

1. 若继续补旧单 5070 的 17 个 context 到既有 20×4 合同，应回到同一 5070 环境；按
   152 runs 粗略外推约 99–101 分钟，但这不是当前双 4090 数据集。
2. 若建立独立双 4090 数据集，至少需要 20 contexts × 4 candidates ×
   (1 warmup + 3 formal) = 320 runs。按 pilot 的每-run 墙钟线性外推约 3.5 小时，
   还应为端点启动、失败重跑和审计预留到约 4 小时。

在任何 320-run 启动前，需先冻结 20 个 workload/rows/output context、候选 tie policy、
formal-only 加载规则和独立 held-out 设计。本目录不会用 pilot 结果反向选择有利 context。

## 原始证据

- `v1_diagnostic_raw.tar.gz`：第一轮诊断，SHA256 见 `MANIFEST.sha256`；
- `v2_raw.tar.gz`：修订合同后的完整 manifest、runs、request/submission/resource trace、
  stdout/stderr、GPU 与服务进程快照；
- `summary.csv`：v2 formal 的可读审计表，数值直接来自 raw CSV，并补入逐 trace
  unique rows 与 endpoint request counts。
