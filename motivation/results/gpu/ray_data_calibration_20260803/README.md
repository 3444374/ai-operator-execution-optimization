# Ray Data native `map_batches` 校准（2026-08-03）

> 性质：**原生 baseline 独立校准**（campaign step 4 的后半）。只动 Ray Data 官方
> `map_batches` 暴露的原生参数 `batch_size` 与 actor pool 大小（`cpu_workers` 预处理池、
> `gpu_workers` GPU 池），Ray Data 自管 backpressure、task dispatch、actor scheduling
> （`scheduler_owner=ray_data`，`custom_scheduling_code=false`，`implementation=
> official_api_with_workload_udfs`）。**这不是 baseline 排名**——排名是 step 5，需统一
> L2-normalized 合同 + 匹配规模。本结果只报 Ray Data native 的独立校准：最佳操作点、img/s、
> GPU 饥饿现象、binding stage。

## 1. 实验目的

- **问题**：Ray Data native staged pipeline（`read_sql → CPU map_batches → GPU map_batches`）在固定
  输入/模型/GPU 下，其原生参数 `batch_size` 与 `cpu_workers`（预处理 actor 池）取多少让吞吐达平台？
  平台点绝对吞吐多少？GPU 是否被打满？哪一段 binding？
- **方法**：两阶段单因素扫描——Phase 1 扫 batch（cpu=4 固定），Phase 2 扫 cpu_workers（batch=64 固定），
  各 2 formal repeats；其余冻结；Ray Data 自管执行。
- **关系到哪个方向**：多模态泛化验证的 framework-native baseline 独立校准；为 step 5 正式排名选定
  Ray Data native 的配置；同时产出"原生 pipeline 是否喂饱 GPU / 哪段 binding"的动机信号，与
  Daft built-in 校准（`daft_builtin_calibration_20260803`）对照。

## 2. 实验设置

- 服务器：AutoDL 2×RTX 4090，本实验双卡 `CUDA_VISIBLE_DEVICES=0,1`。
- 数据：PostgreSQL `coco_train2017_60k`（60000 行，avg 159 kB/image），`--limit 5000`（前 5000 行）。
- 模型：CLIP ViT-B/32，float16，`normalize=True`（**Ray Data predictor 输出已 L2-normalize**，与 Daft built-in 的 raw 输出不同）。
- 引擎版本：Ray 2.56.1、Daft 0.7.21、PyTorch 2.11.0+cu130、Transformers 5.14.1；
  PostgreSQL 18.4、pgvector 0.8.5；schema v10；代码 commit `29b256b`。
- 冻结合同（每 run）：`--arm ray_data_staged --limit 5000 --dataset-passes 1 --warmup-rows 256
  --gpu-workers 2 --source-shards 4 --max-active-batches 8 --phase formal`，1 warmup + 2 formal。
- **Phase 1（batch 扫描）**：`--batch-size ∈ {16,32,64,128,256}`、`--cpu-workers 4` 固定。
- **Phase 2（cpu_workers 扫描）**：`--cpu-workers ∈ {2,4,8,12}`、`--batch-size 64` 固定（Phase 1 平台点）。
- Ray 资源账本：Ray Data 预留 `source_shards + cpu_workers + gpu_workers` CPU slot（Phase 1=10，
  Phase 2 随 cpu_workers 变 8/10/14/18）。容器 `nproc=32`（cgroup），所有配置 ≤18 slot，不超卖。
- 输出：`raw/runs_phase1_batch.csv`（10 行）、`raw/runs_phase2_cpu.csv`（8 行）+ per-run manifest。
- 远端 artifact：`/root/autodl-tmp/experiment-artifacts/ray_data_calib_5k_20260803/`（Phase 1）、
  `…/ray_data_calib_5k_phase2_cpu_20260803/`（Phase 2）。

## 3. 合规性自检

1. **provenance**：`implementation=official_api_with_workload_udfs`、`scheduler_owner=ray_data`、
   `formal_baseline_eligible=true`、`custom_scheduling_code=false`、
   `upstream_source=docs.ray.io/.../data/batch_inference.html`——确为 framework-native formal baseline，
   非项目自写 diagnostic。
2. **正确性**：18/18 run `exit=0`、`exactly_once=True`、`output_rows=5000`、`embedding_dimension=512`；
   `embedding_digest_xor_rounded5` 在同 config 2 rep 间一致。
3. **`max_norm_error≈1.2e-7–1.8e-7`**：Ray Data predictor `normalize=True`，输出已 L2-normalize，
   norm error 在 float32 容差内（对比 Daft built-in 的 11.85 raw）。
4. **稳定**：每 config 2 rep 的 img/s CV = 0.3–4.6%（见 `summary.csv`）。
5. **已知 Ray warning（非错误）**：`MapWorker(MapBatches(RayDataClipPreprocessor)) has constructor
   arguments in the object store and max_restarts > 0`——Ray 已知 issue（ray-project/ray#53727），
   单 pass 不影响正确性，所有 run `exit=0`、`exactly_once=True`。
6. **未跑 feeding-saturation 门禁**：Ray Data native 与 Daft built-in 一样是 GPU 喂入侧瓶颈
   （见 §5/§6），GPU 平均利用率 ~2%，是"原生没喂饱"的对照样本，不是要修正的违规。

## 4. 实验设计

两阶段单因素扫描，每点 2 formal repeats。Phase 1 找 batch 平台（判据：相邻增益 <3%），
Phase 2 在 batch 平台点上扫 cpu_workers 找预处理池平台。所有 run 共享同一 5000 行输入、同一模型、双卡拓扑。

## 5. 实验数据（全组件）

### 5.1 Phase 1——batch 扫描（cpu_workers=4 固定，per batch 2 rep 中位数）

| batch | img/s median | img/s CV | operator_e2e median | first_output median | gpu_util mean | gpu_util peak |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 337.6 | 0.3% | 14.81 s | 9.24 s | 3.2% | 35% |
| 32 | 321.1 | 1.2% | 15.57 s | 9.34 s | 2.7% | 28% |
| **64** | **344.4** | 2.3% | 14.53 s | 9.07 s | 2.1% | 30% |
| 128 | 328.2 | 3.9% | 15.26 s | 9.47 s | 3.7% | 36% |
| 256 | 329.9 | 2.2% | 15.16 s | 9.36 s | 1.8% | 27% |

> **batch 几乎无影响**：321–344 img/s（±7%），相邻增益 −4.9%/+7.2%/−4.7%/+0.5% 全在噪声内。
> batch=64 中位最高但与其余在 CV 范围内重合。**batch 不是 Ray Data 的杠杆**——因为 binding
> stage 在 CPU 预处理（见 5.3），不在 GPU forward。

### 5.2 Phase 2——cpu_workers 扫描（batch=64 固定，per cpu 2 rep 中位数）

| cpu_workers | img/s median | img/s CV | operator_e2e median | ray_cluster_num_cpus | 相邻增益 |
|---:|---:|---:|---:|---:|---:|
| 2 | 211.0 | 4.6% | 23.74 s | 8 | — |
| 4 | 329.3 | 3.5% | 15.20 s | 10 | +56.0% |
| **8** | **346.5** | **1.2%** | **14.43 s** | 14 | +5.2% |
| 12 | 330.9 | 0.6% | 14.79 s | 18 | −3.9% |

> **cpu_workers 平台在 8**（346.5 img/s，CV 最低 1.2%）。2→4 大跳（+56%，严重欠配），4→8 微升（+5%），
> 8→12 持平略降（−3.9%）。文档 formal 配置 cpu=4（329）在平台点 5% 以内。

### 5.3 per-operator throughput（最佳配置 batch=64/cpu=8/r1，Ray Data stats）

| operator | aggregate rows/s | single-task rows/s | UDF time total | wall |
|---|---:|---:|---:|---:|
| ReadSQL（4 shard union，3 cached） | 832 | 841 | 0 | 1.5 s |
| **MapBatches(RayDataClipPreprocessor)** | **632** | **171** | 29.0 s | 7.9 s |
| MapBatches(RayDataClipPredictor) | 808 | **1662** | 3.0 s | 6.2 s |

> **binding stage = CPU preprocess**（single-task 171 rows/s，最低；aggregate 632）。
> GPU predictor single-task 1662 rows/s（preprocess 的 ~10×），aggregate 808——**GPU 被 preprocess 喂不够**。
> 注：aggregate per-operator（632/808）高于 e2e 346 img/s，因 pipeline 各 stage 重叠，e2e 受最慢 stage +
> pipeline fill/drain 限制。

### 5.4 GPU + 能耗（两阶段汇总）——**核心观察：GPU 严重未打满，与 Daft 同量级**

- `gpu_util_mean_pct` 全程 **1.1–3.9%**（双卡均 claim，`gpu_active_device_count=2`），peak 22–60%。
- `gpu_active_power_mean_w` ~111–113 W（TDP 450 W 的 ~25%）。
- per-device（cpu=4 r2，peak 较高那 rep）：GPU0 util_mean 1.9% / GPU1 5.7%——两卡都未饱和。
- **与 Daft built-in 同量级**：Daft gpu_util_mean 1.2–4.1%，Ray Data 1.1–3.9%。两者都把两张 4090
  闲置到 ~97%。

### 5.5 流式边界——**比 Daft 早 ~3× 出首条**

`first_output_s` ~9.0–9.5 s（vs Daft ~27 s），`operator_e2e_s` ~14.4–15.6 s（vs Daft ~28 s）。
Ray Data 的 staged pipeline + `prefetch_batches=2` 真正流式，首条结果在 e2e 的 ~60% 时刻出现
（Daft 是 ~95%，近末端才发射）。

### 5.6 阶段计时——vendor-native 边界不可见

与 Daft 一样，runner 的 per-batch `batch_*_p50_s` 字段为空（`batch_service_semantics=
unavailable_engine_internal`）——Ray Data 自管执行内部，runner 看不到 per-batch 阶段。**但 Ray Data
自带 `dataset.stats()`**，本报告 5.3 的 per-operator throughput 即来自该 stats（语义
`ray_data_operator_stats`），这是 Ray Data 官方暴露的、可比 per-operator 视角。

## 6. 结果解释

### 事实

1. **batch 无影响**（Phase 1：321–344，±7%）；**cpu_workers 平台在 8**（Phase 2：211→346→331）。
2. **最佳操作点 batch=64 / cpu_workers=8 / gpu_workers=2 → ~346 img/s**（CV 1.2%）；文档 cpu=4 给 329（5% 内）。
3. **binding stage = CPU preprocess**（single-task 171 rows/s）；GPU predictor 1662 rows/s single-task，
   被 preprocess 喂不够 → GPU 平均利用率 1–4%。
4. Ray Data 真正流式（first_output ~9s），e2e ~15s。
5. 输出正确性闭环：exactly_once、normalize=True（norm error ~1e-7）、digest 确定性。

### 推断（标注为推断）

- Ray Data 与 Daft built-in **同属 CPU/喂入侧瓶颈**：两者 GPU 平均利用率都 ~2-4%，CLIP forward 很快
  （见 `image_clip_transfer_ceiling_20260803` R0 ~9.7K img/s）但原生 pipeline 喂不够。
- Ray Data 比 Daft built-in 快 ~2× e2e（346 vs 177 img/s）且早 ~3× 出首条，原因是 Ray Data 的显式
  CPU/GPU 分级 + backpressure 让两 stage 重叠；Daft built-in 的单 `PhysicalScan→actor pool` 漏斗
  喂得更慢。**但两者都没喂饱 GPU**——优化空间在 GPU 之前的喂入/组织，不在 GPU 内部。

### 待确认

- **5K 规模的 task-count 限制**：batch=64 时 5000 行只产生 ~8 个 preprocess task（Ray Data 按 batch_size
  coalesce），故 cpu_workers>4 的收益被 task 数压住（cpu=8 ≈ cpu=4）。更大规模（更多 task）下 cpu=8 是否
  显著优于 4，需 step 5 formal 验证。
- cpu_workers=12 略降（330 vs 346）是 5K task-count 限制还是真 contention，需更大规模区分。

### 不能声称

- **不能排名**：346 img/s（Ray Data，5K）vs 177 img/s（Daft built-in，5K）vs project_ray ~1681 img/s
  （60K×2）的差距**不是受控比较**——规模、CPU 分配、归一化均不同。任何"N×"结论必须等 step 5 统一
  L2-normalized contract + 匹配规模。（注：Ray Data 与 Daft built-in 同为 5K/双卡/cpu≈4，二者直接可比：
  Ray Data ~1.95× Daft built-in 在本校准条件下成立——但这是校准条件下的对照，非正式排名。）
- 不能声称 per-batch 阶段占比（runner 不可见）；per-operator 数据来自 Ray Data 官方 stats（§5.3 已标注语义）。
- 不能把聚合 img/s 写成干净稳态（5K 受 pipeline fill/drain + 近 9s 首输出影响；比 Daft 的近末端发射好，但仍非纯稳态）。

## 7. 对课题含义

1. **强动机信号（与 Daft 互补）**：两个 framework-native baseline（Daft built-in、Ray Data）在真实
   bytea-in-PG 链路上都把两张 4090 闲置到 ~2-4%，binding 在 CPU preprocess / 喂入侧。**GPU forward 天花板
   ~9.7K img/s，原生 pipeline 只跑到 177–346 img/s**——巨大 headroom，且优化点在 GPU 之前。这正是课题
   "上游数据组织/喂入调度"的价值，与 R0-R2/R3 木桶判决一致。
2. **为 step 5 选定 Ray Data native 配置**：batch=64、cpu_workers=8（或文档 cpu=4，5% 内）、gpu_workers=2。
   formal 排名时用此配置；注意 5K 的 task-count 限制，更大规模需复核 cpu_workers。
3. **Ray Data 比 Daft built-in 快 ~2×（校准条件下）**：显式 CPU/GPU 分级 + backpressure 优于单漏斗。
   但两者都没喂饱 GPU——项目策略（显式 CPU/GPU actor + 上游组织 + 状态感知调度）的潜在空间正于此。
4. **vendor-native 边界纪律**：Ray Data 的低 GPU 利用率与 binding preprocess 是**被观察的现象**，不是项目
   要"修"的 bug；不得为 Ray Data 注入项目自己的 inflight/credit/router，否则破坏 baseline 可比性。

## 8. 下一步

1. **step 4 完成**：两个 framework-native baseline（Daft built-in、Ray Data）均已独立校准。
2. **step 5 正式排名**（门禁：step 2 统一 L2-normalized 输出合同，codex WIP）：Daft built-in(batch 64) +
   Ray Data(batch 64 / cpu 8) + project_ray，统一归一化、匹配规模、feeding-saturation 门禁。
3.（可选）Ray Data 在更大规模（≥20K）复核 cpu_workers 平台，排除 5K task-count 限制的混淆。

## 原始数据

- `raw/runs_phase1_batch.csv`（10 行，batch 扫描，147 列 schema v10）
- `raw/runs_phase2_cpu.csv`（8 行，cpu_workers 扫描）
- `summary.csv`（两阶段 per-config 中位数 + CV + e2e + GPU）
- 远端：`/root/autodl-tmp/experiment-artifacts/ray_data_calib_5k_20260803/` +
  `…/ray_data_calib_5k_phase2_cpu_20260803/`（runs.csv + 18 个 per-run manifest + calibration.log）
- 复现：见 `deploy/autodl/image_serving.md` §5.4（ray_data_staged 命令，本校准扫 batch/cpu_workers、2 rep）
