# 下一轮实验会记录哪些指标

## 为什么补这些指标

同一个 `tokens/s` 可能对应完全不同的系统状态：请求可能首 token 很慢、逐 token
间隔很抖、prefix cache 没命中，或者策略只是把更多超时请求塞进了系统。下一轮
实验因此同时记录“产出多少”“用户等多久”“资源是否有效工作”“策略为什么产生
这个结果”和“质量是否保持”。这些字段只增加观测，不修改调度策略。

## AI_COMPLETE 原始运行字段

- `vllm_time_to_first_token_{mean,p50,p95,p99}_s`：请求进入 vLLM 后，到首 token
  出现的分布。数据来自 vLLM Prometheus histogram 的运行前后 bucket delta。
- `vllm_inter_token_latency_{mean,p50,p95,p99}_s`：相邻输出 token 的时间间隔，
  用于区分“首 token 慢”和“生成过程卡顿”。
- `vllm_prefix_cache_{queries,hits}_delta` 与 `hit_rate`：prefix 策略是否真的减少
  prefill 重算的直接机制证据。
- `request_slo_{input,output,total}_tokens_goodput_per_s`：只统计 E2E SLO 内完成的
  token。它与普通 `tokens_per_s` 的区别是：迟到的工作不算有效产出。
- `observed_p99_slo_scale`：配置 `--ttft-slo-ms`/`--itl-slo-ms` 后，报告观测 P99
  是目标的多少倍。小于等于 1 表示该次运行满足这两个聚合 P99 目标；它不是逐请求
  联合 SLO attainment。
- `token_cost_*`：只有显式传入 input/output 的每百万 token 单价才计算。自建 GPU
  不应伪装成云 API 单价；未配置时状态为 `unavailable:prices_not_configured`。

## 数据组织与调度解释字段

- `packing_padding_waste_ratio`：每个组织 batch 按最长 prompt 补齐时，浪费 token
  slot 占总 padded slot 的比例。arrival-replay 若未保留逐行长度，状态明确不可用。
- `scheduling_control_overhead_{s,pct}`：只计 `organizer_plan_s + submit_s`，再除以
  `operator_wall_s`。`organizer_collect_s` 可能包含真实数据物化，不冒充纯调度开销。
- shared-vLLM 输出每 job 的实际 token work、SLO token goodput、最终累计服务差和
  job 活跃重叠期间的最大服务差。当前实现同时写
  `service_disparity_bound_status=unavailable:not_proven...`，因此这些数是描述性公平
  证据，不冒充 DRR/VTC 的理论上界。

## 正式重复与代价模型

正式 CSV 完成后运行：

```bash
python code/scripts/analysis/summarize_formal_repeats.py \
  --input-csv NEW_RESULT/runs.csv \
  --output NEW_RESULT/repeat_statistics.json \
  --baseline-scenario-id frozen_static \
  --regression-tolerance-pct 5
```

它输出 sample std、CV、Student-t 95% CI，以及按 `repeat_index` 配对的回退次数。
少于两次 formal 会明确标记没有区间，不能伪造误差条。

`estimate_operator_cost.py` 现在除 MAE/MAPE/RMSE/R² 外，还输出 Q-error
P50/P90/P95/P99/max、Spearman ρ，以及同一 workload 多候选配置中的 pick rate、
selected runtime、oracle runtime、regret、selected-plan rank 和 surpassed plans。
没有至少两个候选配置的 decision context 会被排除，不计作“选对”。

## AI_EMBED 质量门禁

Recall@K/nDCG/MRR 必须有显式相关性真值。先用 gate/capture 运行保存逐行向量，再执行：

```bash
python code/scripts/analysis/evaluate_embedding_retrieval.py \
  --embeddings gate_embeddings.npz \
  --relevance-csv relevance.csv \
  --k 1,5,10 \
  --output retrieval_quality.json
```

该脚本做精确 cosine 排序并排除 query 自身。它验证 embedding 的下游任务质量，
不计入性能 E2E；当前也不等于 pgvector ANN 写回后的 recall-vs-QPS 实验，后者仍需
索引、exact scan 真值和 disjoint query set。

## 文本指标不能机械套到图像

AI_COMPLETE 是逐 token 生成；CLIP AI_EMBED 是一次返回一个固定维向量。因此下表
按算子冻结口径：

| 文本生成指标 | 图像路径是否使用 | 图像中的正确对应量 |
|---|---|---|
| TTFT | 否 | `first_output_s`：冷启动到第一个完整 Arrow batch 返回 |
| ITL/TPOT | 否 | 可观测时使用 batch completion/service P50/P95/P99 |
| token goodput | 否 | 有逐图完成时间与显式 SLO 时才定义 SLO-compliant images/s |
| token padding waste | 否 | 另行定义 image/frame/tensor padding，不能复用 token 字段 |
| $/M-token | 否 | 价格合同明确时使用 $/1K images；否则报告资源成本 |
| actual token work | 否 | 多 job 图像实验使用显式 image/frame/pixel 或预测服务时间 work |

图像 runner schema v12 在已有 `first_output_s` 之外增加
`first_output_fraction_of_e2e`、`post_first_output_s/fraction`。前者回答“总工作结束前
多早开始返回”，可揭示先物化后输出与流式输出的结构差异；它仍受 workload 规模影响，
跨规模时只作描述性信号，不能写成归一化延迟或正式速度排名。

跨规模容量表只允许在每个 arm 独立进入稳定平台后比较速率和单位工作资源：
`images/s`、`images/J`、`J/1K images`、`CPU core-seconds/image`、
`images/CPU-core-second`、`GPU-seconds/image` 和 host I/O bytes/image。
`steady_state_duration_gate_met` 只检查单 run 是否达到预注册的 60 秒时长，不证明吞吐
已经达到平台；平台仍需由相邻规模或 logical-pass 复测确认。

历史 schema-v11 CSV 可在不改原始文件的前提下补算上述派生量：

```bash
python code/scripts/analysis/augment_image_observability.py \
  --input-csv OLD_RESULT/runs.csv \
  --output-csv OLD_RESULT/runs_with_derived_metrics.csv
```

该命令不会凭空补出 Daft 内部 batch、Ray object-store 或逐图完成时间。缺少原始字段时
写 `image_derived_metrics_status=unavailable:...`，不能填 0。

### 图像核心字段与英文缩写字典

| 字段/缩写 | 中文含义与公式 | 数据从哪里来 | 审计时最容易误读的地方 |
|---|---|---|---|
| E2E / `operator_e2e_s` | 算子端到端墙钟；project 为 query+显式 worker setup | 每条 arm 的 `perf_counter` | 不含 Ray cluster 启动和 pgvector 写回；不同 rows 不能直接比总秒数 |
| JCT | Job Completion Time，整个 job 完成时间 | 当前 operator track 等同 operator E2E；system track 还要加统一 sink | 必须先说清是 operator JCT 还是 system JCT |
| `first_output_s` | 冷边界到第一个完整 Arrow batch 返回 | stream iterator/首个 Ray batch 完成时刻 | 不是首图 latency，也不是文本 TTFT；first batch 大小可能不同 |
| `first_output_fraction_of_e2e` | `first_output_s / E2E` | 上述两个直接时钟值 | 越接近 1 只说明晚返回/偏物化；无量纲不等于与规模无关 |
| P50/P95/P99 | 50%、95%、99% 分位 | 仅来自真正可见的 batch/row 样本 | Daft 内部不可见时为空；不能把 batch P99 写成 per-image P99 |
| H2D | Host-to-Device，CPU/主存到 GPU 显存传输 | project detailed-stage 的 tensor bytes 与 CUDA 同步时段 | `logical_h2d_effective_gbps` 不是 PCIe 硬件计数器，开启细分计时会扰动性能 |
| D2H | Device-to-Host，GPU 结果回到主存 | project detailed-stage | CLIP embedding 很小，D2H 快不代表整个系统没有 I/O 瓶颈 |
| `cpu_core_seconds_estimate` | 平均等效忙 CPU 核数×E2E | psutil 主机级逐核采样 | 包含同机其它进程，不是某 actor 精确 CPU 时间 |
| `images_per_cpu_core_second` | rows/CPU 核秒 | CPU 核秒估计与处理行数 | 继承 host-wide 采样噪声 |
| GPU util / busy | nvidia-smi 采样点上 GPU 是否有 kernel 活动 | `utilization.gpu` | 不等于有效 FLOPs；低频采样可能漏掉短 kernel 和空洞 |
| MFU | Model FLOPs Utilization，实际/理论模型 FLOPs 比 | 只有显式提供可信 FLOPs/image 与 dtype 峰值才估算 | 默认应为空；不能用 GPU util 代替 MFU |
| `gpu_energy_estimate_j` | 活跃卡平均总功率×采样窗口秒数 | nvidia-smi `power.draw` | 是低频估算，不是硬件能量计；不含 CPU/整机能耗 |
| `J/1K images` | `GPU energy ×1000/rows` | 上述能耗估计 | 可跨规模描述单位工作，但必须保持计时和采样边界一致 |
| `gpu_seconds_per_image` | `gpu_workers×E2E/rows` | 声明的 GPU 数与 E2E | 是分配成本，不是 GPU 真正执行 kernel 的时间 |
| host I/O bytes/image | 主机磁盘/网络 counter 增量÷rows | psutil 主机总计数器 | 包含其它进程流量，不是 PostgreSQL/Ray 精确归属 |
| CV | Coefficient of Variation，标准差/均值 | formal repeats | n=2 很弱；本项目正式结果至少 3 repeats |
| CI | Confidence Interval，置信区间 | repeat summary 的 Student-t 95% CI | 小样本区间很宽；不能只报均值隐藏原始重复 |

runner 的每个 schema-v12 manifest 都保存同一份 `metric_definitions`，其中逐字段记录
`measurement_kind`（直接时钟/采样估算/代数派生）、单位、公式、source、适用范围和
limitations。历史增强 CSV 同目录生成 `*.metrics.json`。发现异常时先看定义，再回查
raw clock/counter，而不是直接依据列名解释。

## 运行边界

本次 profiler CSV 字段发生变化，下一轮必须使用新的结果目录，不能向旧 header
追加。vLLM 版本若不暴露 histogram bucket，TTFT/ITL 分位状态会是 unavailable；
价格、检索真值或多候选 decision context 缺失时同样 fail-closed。这样的“缺失”是
实验事实，不能用 0、checksum 或估计值替代。
