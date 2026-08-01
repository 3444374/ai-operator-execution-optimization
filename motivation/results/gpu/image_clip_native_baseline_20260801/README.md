# Image CLIP：fused Daft Native/Ray 基线与阶段拆分动机实验

日期：2026-08-01
代码：`ba1b7101ecfdaea989485f011f8e6d809e3ab68c`

## 1. 实验设置

本实验回答一个先于策略设计的问题：在数据库图像 `AI_EMBED` 链路中，把
JPEG decode/processor 与 CLIP GPU forward 从同一个 Daft GPU UDF 拆成有界的
CPU→GPU 流水线，是否比经过独立 actor-shape 校准的 Daft 原生执行更快？

- 机器：AutoDL，2× NVIDIA RTX 4090，32 vCPU。
- 数据：PostgreSQL `image_documents` 中 COCO val2017 的 5000 条 JPEG `BYTEA`。
- 模型：本地 CLIP ViT-B/32，Transformers 5.14.1，Torch 2.11.0+cu130。
- 执行栈：Daft 0.7.21、Ray 2.56.1、PostgreSQL 18.4、pgvector 0.8.5。
- 公共配置：fast torchvision tensor decode/processor、batch=64、float16
  forward、512d float32 L2-normalized embedding。
- operator-E2E 边界：每次 query 的模型 worker 建立/执行开始，到最后一个
  embedding batch 返回；Ray framework startup 排除，模型 worker cold setup
  包含，pgvector 写回排除。

因此，本报告是 GPU-backed **operator E2E 动机结果**，不是完整数据库
system-E2E，也不是只测 CLIP kernel 的 microbenchmark。

## 2. 实验设计

### 2.1 三种执行路径

| 路径 | 数据流 | 角色 |
|---|---|---|
| `daft_native` | PG lazy shards → `@daft.cls`，actor 内 preprocess+forward；Native runner | 单 GPU fused Daft baseline |
| `daft_ray` | 同一个 `@daft.cls` UDF，仅切 Daft Ray runner | 双 GPU fused Daft/Ray baseline |
| `project_ray` | PG lazy shards → Ray CPU preprocess actors → tensor-only CLIP GPU actors | 当前项目的静态有界阶段拆分路径 |

Daft 0.7.21 Native runner 无法可靠隔离双 GPU，故不把单卡 Native 与双卡结果
混排：单卡比较 `daft_native` vs `project_ray`，双卡比较 `daft_ray` vs
`project_ray`。

### 2.2 先校准 baseline，再跑 formal

未经校准的 one-actor-per-GPU Daft 不是强 baseline。筛选显示：

| track | actor/config | operator E2E | images/s | 判读 |
|---|---:|---:|---:|---|
| 1 GPU Native | 1 fused actor | 38.27s（早期 3-run median） | 130.64 | CPU prepare 并发不足 |
| 1 GPU Native | 2 fused actors | 24.21s | 206.51 | 改善 |
| 1 GPU Native | **4 fused actors** | **20.46s** | **244.44** | 筛选最佳，进入 formal |
| 2 GPU Daft Ray | 2 fused actors | 22.05s（早期 3-run median） | 226.80 | CPU prepare 并发不足 |
| 2 GPU Daft Ray | **4 fused actors** | **17.61s** | **283.97** | 筛选最佳，进入 formal |
| 2 GPU Daft Ray | 6 fused actors | 28.23s | 177.11 | 模型副本/调度竞争 |
| 2 GPU Daft Ray | 8 fused actors | 179.62s | 27.84 | 严重退化 |

项目路径也独立筛选 CPU workers/source shards。双卡中，2 CPU preprocess
workers 的 21.74s 明显欠供给；4 CPU workers 后 source=4 为 15.94s，source=6
为 15.19s。source=4→6 的单次收益约 4.7%，记录为配置效应，不解释成核心策略收益。

冻结配置后按 baseline→project 交错执行 3 个 formal repeats：

| track | baseline | project |
|---|---|---|
| 单卡 | Native：4 fused actors、4 source shards | 3 CPU preprocess + 1 GPU actor、4 shards |
| 双卡 | Daft Ray：4 fused actors、4 source shards | 4 CPU preprocess + 2 GPU actors、6 shards |

## 3. 严谨性自检

- 三条路径读取同一 PostgreSQL 表、5000 行集合和模型 revision。
- 每次 invocation 都先跑 64 行 preflight；formal 重新建立 cold model workers，
  没有让项目臂复用 warm actor 对比 cold Daft actor。
- 12/12 formal runs 均 `output_rows=5000`、`exactly_once=true`。
- 12/12 的最大归一化误差均为 `1.788139e-07`；embedding 第一维 checksum 全体
  相对 span 为 `2.91e-5`，未发现粗粒度数值异常。该字段不能证明完整逐行等价；
  exactly-once 只证明行集合完整。
- baseline 与 project 交错执行，避免连续跑同一臂造成单向时间漂移。
- 原始 CSV 与逐 run manifest 均保存在 `raw/`；`summary.csv` 仅为可复核派生表。

## 4. 实验数据

headline 使用 3-run median；CV 使用 sample standard deviation / mean。

| track | arm | operator E2E 三次 (s) | median E2E | median images/s | CV | 相对强基线 |
|---|---|---|---:|---:|---:|---:|
| 单卡 | Daft Native | 21.96 / 22.15 / 25.52 | 22.15s | 225.76 | 8.64% | 1.00× |
| 单卡 | **project-Ray** | 17.02 / 17.27 / 17.09 | **17.09s** | **292.54** | 0.76% | **1.296× / +29.6%** |
| 双卡 | Daft Ray | 17.74 / 19.17 / 17.95 | 17.95s | 278.50 | 4.23% | 1.00× |
| 双卡 | **project-Ray** | 15.36 / 15.82 / 15.77 | **15.77s** | **316.96** | 1.61% | **1.138× / +13.8%** |

换成 JCT 降幅口径，项目路径单卡降低 22.8%，双卡降低 12.1%。双卡
first-output median 由 Daft Ray 的 15.43s 降为 8.90s。

## 5. 结果解释

### 事实

1. baseline actor shape 是一级变量。若直接使用 one-actor-per-GPU，会把单卡
   Daft baseline 低估约 1.7×；正式比较必须使用校准后的 4-actor 配置。
2. 在同一物理机器、各自独立校准最佳配置下，阶段拆分在 3/3 repeats 中均快于
   对应校准后的 fused Daft 基线；单卡增益 29.6%，双卡增益 13.8%。
3. 继续复制 fused Daft actor 并非越多越好：双卡 6/8 actor 都退化，说明用模型
   副本换 CPU prepare 并发会很快撞上内存/调度竞争。
4. 项目路径只保留每 GPU 一个模型副本。观测到的每卡显存峰值约 930 MiB，Daft
   4-actor 形状约为单卡 3714 MiB、双卡每卡约 1855 MiB。

### 推断

结合独立 preprocessing profile，当前最符合数据的候选解释是：图像
decode/processor 是 CPU-heavy stage；fused UDF 只能通过复制整个模型 actor 增加
CPU prepare 并发，而阶段拆分允许增加 CPU workers 但不复制 GPU 模型。不过 formal
本身没有采集 CPU busy cores，也没有把 host copy/H2D/forward 拆开，因此这里只能称
**待验证机制推断**，不能由本轮单独确认 CPU 已饱和或排除数据搬运。它支持的是
阶段边界/资源配比动机，不是动态调度策略已经生效的证据。

`nvidia-smi` 500ms 采样得到的 active-device util 均值仅约 2%–5%，且峰值约
15%–22%。短 kernel 会被低频采样漏掉，因此不能据此计算 MFU；但这些数据至少不支持
“CLIP GPU 已被完全压满”的说法。当前 workload 更可能仍受 CPU prepare、短批 kernel
和调度间隙共同限制。

### 待确认

- formal 是“同一物理机器、各自最佳配置”，不是严格相同 Ray CPU reservation。
  双卡项目使用 4 CPU preprocess + 2 GPU actors，而最佳 Daft baseline 为 4 fused
  actors。相同总 4 个 actor reservation 的 screening 中，项目 21.74s 反而慢于
  Daft 17.61s；相同总 6 个 reservation 时项目 15.19s 快于 Daft 28.23s。正式论文
  需要补 CPU-budget-normalized curve，明确收益来自有效利用空闲 CPU，而非免费加资源。
- 尚未接 pgvector sink；writeback 可能缩小 operator E2E 增益。
- 尚未测 bounded direct CLIP ceiling、Daft-on-Ray staged pipeline、Ray Data staged
  pipeline 和 vLLM pooling；当前只完成 fused Daft Native/Ray 对照。PolarDB/Daft
  官方已支持按算子拆分 CPU preprocess 与 GPU UDF，因此本轮不能代表最强 Daft 路径。

### 不能声称

- 不能声称动态 frame-budget、K/active-work、queue-aware flush 或代价模型带来收益；
  本轮项目臂是固定 actor shape + 静态 bounded queue。
- 不能声称完整数据库 E2E 提升，不能声称 recall@10 不退化。
- 不能把低频 `nvidia-smi` util 当 MFU，也不能声称双 GPU 已达到线性扩展。
- 不能把 fused Daft Ray 当成 Daft staged 或 Ray Data baseline；两者都是后续独立
  框架归因臂。

## 6. 对课题的含义

这组结果提供了一个比旧 micro-profile 更直接的动机：在真实 PostgreSQL BYTEA→
Daft→CLIP operator E2E 中，Daft fused GPU UDF 即使独立调到强运行点，仍存在
阶段耦合与资源配比限制；CPU/GPU stage separation 能在固定物理主机上取得
13.8%–29.6% 的可复现改善。CPU prepare、Ray/host copy、PCIe H2D 各自贡献多少，
仍须由 R0→R4 表示阶梯复测确定。

它支持继续建设项目执行链路，但还不能证明后续“状态感知策略”优于最佳静态阶段拆分。
今后的策略主对照必须是本项目冻结的最佳静态 pipeline，而不是重新拿 Daft fused
当策略对照；fused Daft 的角色是系统/动机 baseline。是否存在架构级增量，还要先
与 Daft-on-Ray/Ray Data staged pipeline 比较。

## 7. 下一步

1. 先按 `motivation/plans/image_host_data_path_bottleneck.md` 运行 R0→R4 表示阶梯，
   以 schema v2 低扰动 headline + 短窗口侵入式诊断判定 CPU/Ray/PCIe/GPU 瓶颈。
2. 在同一复测中补 bounded direct CLIP ceiling和 CPU-budget-normalized actor curve；
   同时报告最佳可达性能与资源效率。
3. 三臂接同一个 pgvector COPY sink，补 system-E2E、写回占比和 recall@10。
4. 补 Daft-on-Ray staged 与 Ray Data staged baseline 作框架归因；vLLM pooling 因
   processor placement 不同，单列服务轨道，不能与 tensor-only actor 混成一个吞吐排名。
5. 上述门禁闭合后，才以“冻结最佳静态 project pipeline”为对照测试状态感知请求
   成形、frame-work credit、multi-job shared credit 和代价模型。

## 附：per-arm 指标的审计修订与资源缺口

本节只解释历史 `schema_version=1` 的 12 个 formal runs，不改写原始 CSV。
逐行复核 runner 后，早先追加说明里有三个语义错误：把外部 batch completion
误称为纯 GPU forward、把 Daft `worker_setup_s=0` 误解为冷启动未计入、把包含
闲置卡的 visible-GPU 平均值当成 active-GPU 平均值。以下为修订口径。

### 附.1 per-arm 全指标（3-run 中位）

| 指标 | Native 单卡 | project 单卡 | Daft Ray 双卡 | project 双卡 |
|---|---:|---:|---:|---:|
| operator E2E (s) | 22.15 | 17.09 | 17.95 | 15.77 |
| images/s | 225.8 | 292.5 | 278.5 | 317.0 |
| first complete output batch (s) | 9.18 | 8.48 | 15.43 | 8.90 |
| submission→result wall p50 (s) | — | 0.91 | — | 0.71 |
| submission→result wall p95 (s) | — | 1.10 | — | 0.89 |
| explicit project pool setup (s) | folded | 6.93 | folded | 7.51 |
| visible-GPU mean（旧 CSV 字段） | 1.7% | 1.9% | 2.2% | 2.0% |
| active-card mean（由 per-device JSON 复算） | 3.4% | 3.9% | 2.2% | 2.0% |
| GPU peak（500ms 采样） | 18% | 17% | 15% | 15% |
| 每卡显存峰值 | 3714 MiB | 930 MiB | 1855 MiB | 930 MiB |
| 模型副本数 | 4 | 1 | 4 | 2 |
| 每 worker GPU 配额 | 0.25 | 1.0 | 0.5 | 1.0 |
| checksum（仅第一维求和） | -36.447 | -36.446 | -36.446 | -36.446 |
| max norm error | 1.79e-7 | 1.79e-7 | 1.79e-7 | 1.79e-7 |
| exactly-once / rows | true / 5000 | true / 5000 | true / 5000 | true / 5000 |

公共配置：batch=64、float16、512d、CLIP ViT-B/32。单卡旧
`gpu_util_mean_pct` 把第二张闲置 GPU 也纳入平均，不能直接用于单/双卡比较。

### 附.2 字段的正确解释

1. `first_output_s` 是从 cold worker 边界到**第一个完整 Arrow record batch**返回，
   不是单图片延迟，也不是模型 TTFT。双卡 15.43→8.90s 说明项目更早产出首批，
   但不能仅凭该字段把全部差异归因于 pipeline fill。
2. 历史 `batch_service_p50/p95_s` 从 GPU actor 调用提交时开始计时；其输入
   ObjectRef 此时可能仍在 CPU preprocess。它包含依赖等待、actor queue、host copy、
   H2D、forward、D2H 和返回，不是纯 GPU forward。Daft fused UDF 没有对应分解。
3. project 的显式 pool setup 约 7s，随后被加回 E2E。Daft actor/model 在遍历
   timed query 时懒创建，因此 setup 已折叠在 `total_s/first_output_s` 内；
   `worker_setup_s=0` 只表示“未单独观测”，不能扣除 project 的 7s 后比较。
4. 单卡显存差约 4×（4 对 1 模型副本）；双卡每卡差约 2×（每卡 2 对 1），
   不能把所有 track 概括成统一“省 4×”。
5. exactly-once、shape、finite 和 norm 均通过；但 checksum 只累加 embedding 第一维，
   不是完整逐行等价证明。正式质量结论仍需逐行 cosine/max-abs 和 Recall@10。

### 附.3 当前数据能支持与不能支持的硬件判断

当前能支持：GPU 没有持续繁忙的证据；增加 fused Daft 模型副本会增加显存和竞争；
阶段拆分能利用更多 CPU 并发而不复制模型。

当前不能支持：CPU cores 已饱和、PCIe 已饱和、主机内存带宽是瓶颈、GPU MFU 约为
2%，或者传输可以忽略。旧 profile 的 `tensor bytes / transfer wall` 只能得到逻辑
有效速率，不能冒充 PCIe hardware counter；而且当前 E2E 没有把 Ray object-store
copy、pageable/pinned memory 和 H2D 分开。

### 附.4 schema v2 修复（等待同目的正式复测）

runner 已为下一次同目的实验改为 fail-explicit 记录：

- `worker_setup_accounting` 区分 explicit 与 folded，不再用 0 表示“没有 setup”；
- 保留旧 `batch_service_*` 兼容列，但增加明确的 completion-wall、actor-service、
  CPU preprocess、host-copy、H2D、forward 和 D2H 字段；详细 CUDA 分段计时是显式
  diagnostic 模式，因为同步本身会扰动 headline；
- 同时记录 system per-core CPU utilization、等价 busy cores、active-GPU 聚合、
  功耗/时钟/估算能耗、PCIe current/max link、pending batch peak、未归因 wait，以及
  encoded/tensor/device/output logical bytes；
- 带宽字段明确命名为 `logical_*_effective_gbps`，不称为 PCIe 实测；
- 只有同时提供经过校准的 per-image FLOPs 与相应 dtype 的 per-GPU peak FLOP/s，
  才填写 `estimated_e2e_mfu`；否则留空，不用 GPU util 冒充 MFU；
- 质量审计增加全维求和与按 doc_id 的 rounded embedding digest。

新复测通过后，schema v2 可覆盖本报告 headline；schema v1 原始文件保留作审计，
不回填不存在的指标。

## 复现入口

- runner：`code/scripts/run_image_clip_e2e.py`
- 部署/命令：`deploy/autodl/image_serving.md` §5.4
- 正式原始结果：`raw/image_clip_native_baseline_formal_ba1b710/`
- actor-shape screening：`raw/image_clip_daft_actor_shape_ba1b710/`
- 未校准 one-actor 初始结果：`raw/image_clip_e2e_formal_02e2261/`（只用于说明
  为什么必须校准，禁止作为最终 baseline）
