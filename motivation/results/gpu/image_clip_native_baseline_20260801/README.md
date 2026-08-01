# Image CLIP：Daft Native/Ray 强基线与阶段拆分动机实验

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
| `daft_native` | PG lazy shards → `@daft.cls`，actor 内 preprocess+forward；Native runner | 单 GPU 直接强基线 |
| `daft_ray` | 同一个 `@daft.cls` UDF，仅切 Daft Ray runner | 双 GPU 官方执行栈强基线 |
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
- 12/12 的最大归一化误差均为 `1.788139e-07`；embedding checksum 全体相对
  span 为 `2.91e-5`，未发现静默缺行或输出合同变化。
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
   对应 Daft 强基线；单卡增益 29.6%，双卡增益 13.8%。
3. 继续复制 fused Daft actor 并非越多越好：双卡 6/8 actor 都退化，说明用模型
   副本换 CPU prepare 并发会很快撞上内存/调度竞争。
4. 项目路径只保留每 GPU 一个模型副本。观测到的每卡显存峰值约 930 MiB，Daft
   4-actor 形状约为单卡 3714 MiB、双卡每卡约 1855 MiB。

### 推断

最符合数据的机制解释是：图像 decode/processor 是 CPU-heavy stage；fused UDF
只能通过复制整个模型 actor 增加 CPU prepare 并发，而阶段拆分允许增加 CPU workers
但不复制 GPU 模型，因此 pipeline 更快且更稳定。这是**阶段边界/资源配比动机**，
不是动态调度策略已经生效的证据。

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
- 尚未测 bounded direct CLIP ceiling、Ray Data 和 vLLM pooling；当前只完成最接近
  项目栈的 Daft Native/Ray 强基线。

### 不能声称

- 不能声称动态 frame-budget、K/active-work、queue-aware flush 或代价模型带来收益；
  本轮项目臂是固定 actor shape + 静态 bounded queue。
- 不能声称完整数据库 E2E 提升，不能声称 recall@10 不退化。
- 不能把低频 `nvidia-smi` util 当 MFU，也不能声称双 GPU 已达到线性扩展。
- 不能把 Daft Ray 当成纯 Ray Data baseline；Ray Data 是后续独立框架归因臂。

## 6. 对课题的含义

这组结果提供了一个比旧 micro-profile 更直接的动机：在真实 PostgreSQL BYTEA→
Daft→CLIP operator E2E 中，官方 Daft fused GPU UDF 即使独立调到强运行点，仍会受
CPU prepare 与模型副本绑定的限制；CPU/GPU stage separation 能在固定物理主机上
取得 13.8%–29.6% 的可复现改善。

它支持继续建设项目执行链路，但还不能证明后续“状态感知策略”优于最佳静态阶段拆分。
今后的策略主对照必须是本项目冻结的最佳静态 pipeline，而不是重新拿 Daft Native
当策略对照；Daft Native/Ray 的角色是系统/动机 baseline。

## 7. 下一步

1. 三臂接同一个 pgvector COPY sink，补 system-E2E、写回占比和 recall@10。
2. 补 bounded direct CLIP ceiling；用于判断 pipeline feeding 缺口，不要求超越。
3. 补 CPU-budget-normalized actor curve；同时报告最佳可达性能与资源效率。
4. 再补 Ray Data baseline作框架归因；vLLM pooling因 processor placement 不同，单列
   服务轨道，不能与 tensor-only actor 混成一个吞吐排名。
5. 上述门禁闭合后，才以“冻结最佳静态 project pipeline”为对照测试状态感知请求
   成形、frame-work credit、multi-job shared credit 和代价模型。

## 复现入口

- runner：`code/scripts/run_image_clip_e2e.py`
- 部署/命令：`deploy/autodl/image_serving.md` §5.4
- 正式原始结果：`raw/image_clip_native_baseline_formal_ba1b710/`
- actor-shape screening：`raw/image_clip_daft_actor_shape_ba1b710/`
- 未校准 one-actor 初始结果：`raw/image_clip_e2e_formal_02e2261/`（只用于说明
  为什么必须校准，禁止作为最终 baseline）
