# AI_EMBED operator 正式对比：project vs Ray Data native（2026-08-03）

> 性质：**同机正式系统对比**（campaign step 6 + step 8）。回答：在同一 PostgreSQL 图像输入、
> CLIP 模型、L2-normalized 输出质量和双 4090 上，**项目静态执行路径是否优于最强 native baseline
> （Ray Data），以及增益来自执行结构还是单纯更多 CPU**。
>
> Daft built-in 因物化执行无法 scale 到 60K×2（OutOfDisk），按 option-A **单列**（见 §6 + 另行
> 独立报告）。direct/GPU ceiling 单独作容量参照（step 7，待跑）。**本报告不把 external raw time
> 与本机数字混排。**

## 1. 实验目的

- **主问题**：project_ray 在 60K×2 上是否降低 AI_EMBED operator JCT 并优于 Ray Data native？增益是
  执行结构还是 CPU 数量？
- **唯一主指标**：`verified_operator_jct_s` = SQL source 开始 → 120K 输出全完成 + exactly-once + 输出
  合同审计通过。等于 runner 的 `operator_e2e_s`（exactly_once 过滤后），全 arm 同边界
  （`per_query_model_worker_setup_to_last_embedding_batch_returned`，含各 arm 自身 setup）。
- **两张表**：A 各自最优（best-achievable）；B matched-resource（同 CPU 下，隔离结构收益）。

## 2. 实验设置

- 服务器：AutoDL 2×RTX 4090（cgroup `nproc=32`，/dev/shm 120G，object store ~77G）。
- 数据：PostgreSQL `coco_train2017_heldout`（**58287 unique**，与 calibration 用的 `coco_train2017_60k`
  完全 disjoint——heldout 不在任何跨 workload doc_id dup 中）。`--limit 58287 --dataset-passes 2` =
  **116574 行/run**。
- 模型：CLIP ViT-B/32，float16，`--embedding-output-contract l2_normalized`（归一化计入各 arm E2E）。
- 引擎：Ray 2.56.1、PyTorch 2.11.0+cu130、Transformers 5.14.1；PG 18.4、pgvector 0.8.5；
  schema v11；代码 commit `37dc8fd`。
- 冻结合同：batch=64、gpu-workers=2、source-shards=4、1 warmup + 3 formal、alternate interleave
  （Latin-square 式，控制时间漂移）、关闭 embedding capture、不用 profiler。
- arm 配置（唯一差异是 cpu_workers / max_active_batches）：
  - Ray Data native：`implementation=official_api_with_workload_udfs`、`scheduler_owner=ray_data`。
  - project_ray：`implementation=project_implementation`、`scheduler_owner=project_ray`、`normalize=True`。
- 计时边界：runner 的 `operator_e2e_s` 对两 arm 都含自身 setup（project 显式记 `worker_setup_s`~8.7s
  再加回；Ray Data 把 model-worker 冷启动折叠进 `first_output`/total）。故两 arm operator_jct 同口径
  可比（都 cold、都含 setup）；`runtime_setup_s` 仅 project 可分离（Ray Data 折叠在内，报告标注）。

## 3. 合规性自检

- **正确性**：12/12 formal run `exactly_once=True`、`output_rows=116574`、`embedding_dimension=512`、
  `max_norm_error=0.0`（l2_normalized 合同生效，norm 在 float32 容差内）。
- **稳态**：所有 formal `operator_e2e ≥ 60s`（最短 project cpu16=70s，最长 Ray Data cpu8=129s）。
- **稳定**：每 cell 3 rep CV = 0.7–3.2%（全 <5%，**无需扩到 5 formal**）。
- **disjoint**：formal 用 heldout（58287），与 calibration/project-static 用的 60k 完全 disjoint。
- **provenance**：Ray Data 确为 framework-native（`formal_baseline_eligible=true`、无项目调度注入）；
  project 为项目方法（`custom_scheduling_code=true`）。

## 4. 实验设计

两阶段：
- **step 6（2-arm，各自 5K 校准配置）**：Ray Data(cpu8) vs project(cpu16)——直接复用各自 5K 校准冻
  结点。3 formal，alternate interleave。
- **step 8（matched-resource 2×2 补全）**：补 Ray Data(cpu16) + project(cpu8) 两 cell，与 step 6 合成
  完整 2×2。同 3 formal、同 interleave、同合同、同 commit。

## 5. 实验数据

### 5.1 Table B —— matched-resource 2×2（**隔离结构收益**，主判定依据）

operator_jct（低=好；每 cell 3 formal 中位，CV 见括号）：

| | Ray Data | project | project 优势 |
|---|---|---|---|
| **@cpu8** | 128.75s (CV 3.2%) | 112.24s (CV 0.7%) | **−12.8%** ✅ |
| **@cpu16** | 82.41s (CV 2.6%) | 69.95s (CV 2.0%) | **−15.1%** ✅ |

**同 CPU 下 project 在两档都显著更快（≥5%、方向一致）→ 执行结构收益成立，约 13–15%。**

### 5.2 全指标 4 cell（见 `summary.csv`）

| cell | operator_jct | img/s | first_out | GPU busy(mean/peak) | CPU busy | img/J |
|---|---:|---:|---:|---:|---:|---:|
| Ray Data cpu8 | 128.75s | 905 | 45.6s | 6.1% / 46% | 16.4 | 6.88 |
| Ray Data cpu16 | 82.41s | 1415 | 40.1s | 8.2% / 37% | 21.8 | 9.75 |
| project cpu8 | 112.24s | 1039 | 21.8s | 6.3% / 30% | 16.4 | 7.07 |
| project cpu16 | 69.95s | 1666 | 22.4s | 9.6% / 37% | 25.1 | 10.33 |

#### 5.2.1 schema-v12 派生观测补算（不重跑、不改 raw）

2026-08-04 使用 `augment_image_observability.py` 从 schema-v11 原始总量旁置补算；公式、
数据源和误差边界见脚本生成的 `*.metrics.json` 及
`learning/observability_metrics_guide.md`。仓库只保留下面的紧凑汇总；完整增强 CSV/JSON 可由
归档 raw 重建，避免重复保存同一批逐 run 数据。这些字段没有增加新的 runtime
instrumentation，不能补出 Ray/Daft 隐藏的逐 batch 或逐图时间。

| cell | first-output/E2E | J/1K images | CPU core-s/image | images/CPU-core-s | GPU-s/image |
|---|---:|---:|---:|---:|---:|
| Ray Data cpu8 | 0.354 | 145.33 | 0.01819 | 54.96 | 0.002209 |
| Ray Data cpu16 | 0.486 | 102.56 | 0.01542 | 64.84 | 0.001414 |
| project cpu8 | 0.196 | 141.52 | 0.01565 | 63.88 | 0.001926 |
| project cpu16 | 0.320 | 96.83 | 0.01539 | 64.98 | 0.001200 |

读法：`first-output/E2E` 是首个完整 Arrow batch 返回时已消耗的总时间比例，不是
per-image latency；比例越接近 1 越偏向晚返回/物化。`GPU-s/image` 是分配的 GPU 数×墙钟
除以图片数，不是 kernel active time。CPU 核秒、能耗与 host I/O 均来自低频或 host-wide
采样，适合辅助比较和发现 regime change，不应冒充精确进程归因。

同一 raw 还显示 Ray Data 的 host network receive bytes/image 约为 project 的 2 倍
（约 329KB vs 165KB）。这是**待确认观测**：计数器是整机总量，可能包含 SQL shard/
Ray 数据路径差异或同机背景流量；没有进程级网络归属前，不能写成“Ray Data 多传一倍数据”。

### 5.3 CPU scaling（两臂在 60K×2 都能 scale，与 5K 不同）

| arm | cpu8 img/s | cpu16 img/s | cpu8→16 增益 |
|---|---:|---:|---:|
| Ray Data | 905 | 1415 | **+56.2%** |
| project | 1039 | 1666 | +60.4% |

> 5K 校准时两臂都在 cpu8 平台（task 数少）；**60K×2 task 多，两臂都能用满 16 CPU**。这关键地修正了
> 下面 §5.4 的 best-achievable 口径。

### 5.4 Table A —— best-achievable（**修正后**）

| 口径 | 比较 | project 优势 | 评价 |
|---|---|---:|---|
| ❌ step-6 headline | project(cpu16) vs **Ray Data(cpu8)** | 45.7% | **虚高**：Ray Data 用了 5K 校准冻结的 cpu8（其 60K 弱配置）|
| ✅ **corrected best-achievable** | project(cpu16) vs **Ray Data(cpu16)** | **15.1%** | 公平的最强对最强 |

> **45.7% 是 Ray Data 被低估配（cpu8）造成的假象**。Ray Data 在 60K×2 的真实强配置是 cpu16
> （img/s +56%）。公平比较下 project 快 **15.1%**——与 matched-cpu16 完全一致（因为两者最优都在 cpu16）。

## 6. 结果解释

### 事实

1. **matched-CPU（结构）**：project 在 cpu8 快 12.8%、cpu16 快 15.1%，两档都 ≥5% 且方向一致 →
   **执行结构收益真实，约 13–15%**（非单纯"更多 CPU 换吞吐"）。
2. **best-achievable（修正）**：公平口径 project 快 **15.1%**（都 cpu16）。step-6 的 45.7% 是 Ray Data
   被困 cpu8 的伪差距。
3. **两臂都 scale CPU**（+56–60% cpu8→cpu16）；project 略胜（60.4% vs 56.2%），但差距小。
4. project **更早出首条**（~22s vs Ray Data ~40–46s，流式 vs Ray Data 较慢灌满）。
5. project **略更省能**（matched-cpu16 img/J 10.33 vs 9.75；matched-cpu8 7.07 vs 6.88）。
6. 两臂 **GPU 都饥饿**：busy mean 6–10%（peak 30–46%），双卡均 claim 但平均远未饱和——瓶颈在
   CPU 喂入侧（与 R0–R3 木桶判决一致）。project cpu16 也只到双卡 ~19K 天花板的 ~9%。
7. Daft built-in **不在本表**：物化执行在 60K×1 即 OutOfDisk（~78GB > 77GB object store），60K×2 需
   ~156GB 远超——无法 scale 到本规模（详见 §8 + Daft 独立报告）。

### 推断

- project 的 ~13–15% 结构收益来自**显式 CPU/GPU actor 分级 + 项目 admission/inflight 调度**让喂入与
  forward 更重叠、更早出首条；但**温和**（非数量级）。
- 两臂都 GPU 饥饿 → 上游喂入是共同瓶颈；project 的结构优势正是在"如何更高效地把 CPU 预处理喂给
   GPU"这一环上——这正是课题"上游调度"的价值区间，但当前静态点离 GPU 饱和仍远（~9%），留给状态
   感知策略（研究内容 A）的空间大。

### 不能声称

- **不能声称 45.7%**——那是 Ray Data 低估配的伪差距；正式 best-achievable 是 15.1%。
- 不能把 GPU busy mean（6–10%）写成 MFU（nvidia-smi 采样，非硬件 counter）。
- 不能声称 project 已最优——它只是 4 cell 里最优；状态感知策略（A）未叠加，GPU 仍 ~9% 饱和。
- Ray Data 的 per-batch 阶段不可见（`unavailable_engine_internal`）；只有 Ray Data 官方 `stats()` 的
  per-operator 视角。
- 不能把本机 15.1% 与外部厂商 raw time 混排。

## 7. 对课题含义

1. **正向但温和的结构收益**：在同输入/模型/合同/硬件下，project 比 Ray Data native 快 ~13–15%
   （matched-CPU 证实非纯资源），且更早出首条、略更省能。**结构/提交机制本身有增益，成立**。
2. **修正了虚高 headline**：matched-resource（Table B）抓出 best-achievable 用了 Ray Data 弱配置的
   问题——这正是 Table B 的设计目的。正式 claim 必须用 15.1%。
3. **GPU 仍是共同瓶颈**：两臂都 ~6–10% busy。project 当前静态点也只 ~9% GPU 饱和——**状态感知
   调度（A）有 ~10× headroom**，是后续真正的大头。
4. **Ray Data 5K 校准低估了它**：60K×2 下它 cpu16 比 cpu8 快 56%。后续若要更紧的 Ray Data 最优，
   可扫 cpu20+（本次未做）。

## 8. Daft built-in 处理（option-A，单列）—— 物化-cap 已实测闭合

Daft built-in 物化执行（`DistributedActorPoolProject` 把 decode 后图像缓冲进 Ray object store）。
**升序探针定 max**（`daft_max_probe2_20260803`，带磁盘状态日志）：

| N (rows, ×1 pass) | 结果 | /dev/shm 峰值 | spill |
|---:|---|---:|---|
| 5K（5K 校准）| OK | — | 无 |
| **12K** | **OK** | **68G（56%）** | 无（全装 /dev/shm）|
| 20K | **OutOfDisk 崩** | 70G + spill 9G→/root/autodl-tmp | symlink 生效但仍崩 |
| 30K / 60K | OutOfDisk 崩 | — | — |

- **Daft max ≈ 12–16K 行**（12K 跑通、20K 崩），img/s @12K ≈ **187**（与 5K 校准 177 一致）。
- **物化足迹 ~5.7MB/行**（Daft 在 object store 存多份副本：encoded + decoded + intermediate）。
- **cap 是 /dev/shm object store（RAM ~70–77G），不是磁盘**：12K 时 /dev/shm 到 68G；20K 时撞顶
  → spill。symlink 把 spill 引到大盘（/root/autodl-tmp +9G）**生效了，但照样崩**——因为 object store
  本身（/dev/shm）撞 RAM 上限。
- **扩容磁盘无效**：cap 是 RAM object store，不是 /root/autodl-tmp 磁盘。即便把 /dev/shm 120G 全配给
  object store（需改 runner `object_store_memory`，代码改），Daft 也只能 ~24K 行（仍 ≪ 60K×2）。

**3-arm 同规模一致性 run 不可行（最终判定）**：Daft max ~12K 下，project(1666 img/s)=**~7s**、
Ray Data(905)=**~13s** —— fast arm 远短于 first_output（~22s），稳态窗口≈0，img/s 全 transient 噪声。
Daft max（~12K）与 project 可靠测量规模（~100K）差 **~10×，无重叠**。故 **option-A 是唯一可行路径**：
Daft 单列。

**Daft 的发现本身有价值**：原生 materializing baseline 在 ~12K 行就撞 RAM object store、完全无法 scale，
而 streaming 的 Ray Data/project 轻松 120K——**量化了"执行结构（stream vs materialize）决定可扩展性"**，
比"Daft 在某规模慢多少"更有说服力，支持课题执行结构论点。远端：`daft_max_probe2_20260803/`（probe.log +
runs.csv）+ `daft_builtin_60k_gate_symlink_20260803/`（60K 崩溃证据）。

12K 三臂 clean consistency 的 Daft 两次 formal 旁置补算中位数为：
`first_output/E2E=0.951`、`J/1K-images=648.19`、`CPU-core-s/image=0.08061`、
`images/CPU-core-s=12.79`、`GPU-s/image=0.01064`。它说明结果几乎到 job 尾部才开始
返回，并且单位工作资源显著高；但只有 2 个 formal repeats，且与 60K fast arms 不是同规模，
因此只作容量/结构诊断，不进入 matched-workload 排名。

## 9. 容量参照（step 7）+ 下一步

**direct/GPU ceiling（容量参照，不进排名表）**：R0 GPU-resident forward 天花板 **~9.7K img/s（单卡）**
/ ~19K（双卡），见 `motivation/results/gpu/image_clip_transfer_ceiling_20260802/`（R0/R1/R2 profile）。
对照本实验 operator 臂（Ray Data ~905–1415、project ~1039–1666 img/s）→ **所有 operator 臂都只到双卡
天花板的 ~5–9%**，GPU 有 ~10× headroom——瓶颈在喂入侧（CPU preprocess），不在 GPU 算力。这与 §6 的
GPU busy 6–10% 一致。

**剩余 step**：
1. **system E2E + pgvector 写回**（campaign step 5/6 后续）：operator formal 有效后才接统一 pgvector sink，
   比较完整 source→operator→sink。
2. **策略消融**（研究内容 A）：在冻结 project static 上做 frame/work budget、状态感知调度；动态策略只和
   冻结静态点比。
3. **官方 ResNet18 复现**（独立 AI_CLASSIFY 轨道，并行准备，不阻塞 AI_EMBED）。

## 原始数据

- `raw/runs_step6_2arm_formal.csv`（8 行：Ray Data cpu8 + project cpu16，各 1 warmup + 3 formal）
- `raw/runs_step8_matched_resource.csv`（8 行：Ray Data cpu16 + project cpu8，各 1 warmup + 3 formal）
- `raw/runs_daft_12k_consistency.csv.gz`（clean schema-v11 三臂 12K，1 warmup + 2 formal；
  只作 capacity/consistency 诊断）
- `summary.csv`（4 cell 全指标中位；派生字段由 raw 代数补算）
- 完整 `*_with_derived.csv` 与 `*.metrics.json` 不重复入库；运行
  `code/scripts/analysis/augment_image_observability.py` 可从上述 raw 重建
- 远端：`/root/autodl-tmp/experiment-artifacts/ai_embed_formal_2arm_60kx2_20260803/` +
  `…/ai_embed_matched_resource_20260803/`（runs.csv + 16 per-run manifest + formal.log/matched.log）；
  `daft_max_probe2_20260803/`（Daft 物化-cap 探针）；`daft_builtin_60k_gate_symlink_20260803/`（60K 崩溃证据）
- 复现：commit `37dc8fd`；`deploy/autodl/image_serving.md` §5.4（ray_data_staged / project_ray 命令）
