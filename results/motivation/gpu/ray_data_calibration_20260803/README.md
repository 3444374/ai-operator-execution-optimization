# Ray Data native `map_batches` 校准（2026-08-03）

> 性质：**原生 baseline 独立校准**（campaign step 4 的后半）。只动 Ray Data 官方
> `map_batches` 暴露的原生参数 `batch_size` 与 actor pool 大小（`cpu_workers` 预处理池、
> `gpu_workers` GPU 池），Ray Data 自管 backpressure、task dispatch、actor scheduling
> （`scheduler_owner=ray_data`，`custom_scheduling_code=false`，`implementation=
> official_api_with_workload_udfs`）。**这不是 baseline 排名**——排名是 step 5，需统一
> L2-normalized 合同 + 匹配规模。本结果只报 Ray Data native 的独立校准：观测操作点、
> img/s、低采样 GPU busy 率和候选限制阶段。

> **2026-08-03 长稳态更新**：下文 Phase 1/2 是 5K screening；§9 的 60K unique×2
> passes、schema v11、交错长稳态复核是当前配置选择权威证据。长稳态确认 batch16/64
> 为同一近优平台，但 64 的吞吐、首输出、能效与重复稳定性均更好；batch256/512 稳定
> 退化，因此冻结 `batch64/cpu8/gpu2/source4`，不继续测 1024。

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
6. **未跑 feeding-saturation 门禁**：5K screening 的 GPU busy 采样均值约 2%，只能作为
   “可能未持续喂满”的候选信号；它不是 MFU，也不能单独确认喂入瓶颈。

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

> **screening 观察**：321–344 img/s（观测范围约 7%），batch=64 中位最高。每点只有两个
> repeat，不能据此做统计等价声明；60K×2 长稳态复核见 §9，它确认 16/64 近优而 256/512
> 退化。

### 5.2 Phase 2——cpu_workers 扫描（batch=64 固定，per cpu 2 rep 中位数）

| cpu_workers | img/s median | img/s CV | operator_e2e median | ray_cluster_num_cpus | 相邻增益 |
|---:|---:|---:|---:|---:|---:|
| 2 | 211.0 | 4.6% | 23.74 s | 8 | — |
| 4 | 329.3 | 3.5% | 15.20 s | 10 | +56.0% |
| **8** | **346.5** | **1.2%** | **14.43 s** | 14 | +5.2% |
| 12 | 330.9 | 0.6% | 14.79 s | 18 | −3.9% |

> **cpu_workers 平台在 8**（346.5 img/s，CV 最低 1.2%）。2→4 大跳（+56%，严重欠配），4→8 微升（+5%），
> 8→12 持平略降（−3.9%）。文档 formal 配置 cpu=4（329）在平台点 5% 以内。

### 5.3 per-operator throughput（候选配置 batch=64/cpu=8/r1，Ray Data stats）

| operator | aggregate rows/s | single-task rows/s | UDF time total | wall |
|---|---:|---:|---:|---:|
| ReadSQL（4 shard union，3 cached） | 832 | 841 | 0 | 1.5 s |
| **MapBatches(RayDataClipPreprocessor)** | **632** | **171** | 29.0 s | 7.9 s |
| MapBatches(RayDataClipPredictor) | 808 | **1662** | 3.0 s | 6.2 s |

> CPU preprocess 的 single-task 与 aggregate throughput 均低于 predictor，支持它是候选限制
> 阶段；但 aggregate operator stats 存在 pipeline overlap，不能与 E2E 直接相减，也不能仅由
> 本表确认 GPU 等待的因果比例。

### 5.4 GPU + 能耗（两阶段汇总）——低频 busy 采样信号

- `gpu_util_mean_pct` 全程 **1.1–3.9%**（双卡均可见，`gpu_active_device_count=2`），peak 22–60%。
- `gpu_active_power_mean_w` ~111–113 W（TDP 450 W 的 ~25%）。
- per-device（cpu=4 r2，peak 较高那 rep）：GPU0 util_mean 1.9% / GPU1 5.7%——两卡都未饱和。
- **与 Daft built-in 同量级**：Daft gpu_util_mean 1.2–4.1%，Ray Data 1.1–3.9%。这些是
  `nvidia-smi` 采样的 busy 指标，不等于 MFU，也不能换算成“严格空转 97%”。

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
3. CPU preprocess 的 single-task throughput 为 171 rows/s，predictor 为 1662 rows/s；
   同期 `nvidia-smi` busy 采样均值为 1%–4%。前者是官方 operator stats，后者不是 MFU。
4. Ray Data 真正流式（first_output ~9s），e2e ~15s。
5. 输出正确性闭环：exactly_once、normalize=True（norm error ~1e-7）、digest 确定性。

### 推断（标注为推断）

- Ray Data 与 Daft built-in **可能同属 CPU/喂入侧受限**：两者 GPU busy 采样均值都低，且
  R0 GPU-resident ceiling 明显更高；这是跨诊断证据形成的推断，不是 Nsight/硬件 counter
  已确认因果。
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

1. **动机候选信号（与 Daft 互补）**：两个 framework-native baseline 的低频 GPU busy
   采样均值都较低，Ray Data operator stats 又显示 preprocess 服务能力低于 predictor；结合
   R0 GPU-resident ceiling，支持继续检验上游喂入优化。正式贡献仍须由统一合同、同规模
   baseline/project 头对头证明，不能用 5K screening 直接成立。
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

## 9. 60K×2 原生 batch 上界复核（当前配置选择证据）

### 9.1 实验设置与目的

- 问题：5K screening 下 batch16–256 差异小，且 cpu8 是预处理池平台；在扩大 workload
  后，batch 最优点是否改变，512 是否仍可能提高吞吐？
- 代码：commit `d73fbfb`；实际 runner 输出 commit 与 matrix repository commit 均为该
  revision；runner schema v11。
- 数据：PostgreSQL `coco_train2017_60k`，60,000 unique images×2 logical passes，
  每 run 120,000 processed rows。
- 原生执行图：Ray Data 官方
  `read_sql→map_batches(RayDataClipPreprocessor)→map_batches(RayDataClipPredictor)`；
  `scheduler_owner=ray_data`、`custom_scheduling_code=false`。项目 credit、router、
  inflight 与 `ray.wait` 提交循环均未使用。
- 固定项：cpu_workers=8、gpu_workers=2、source_shards=4、float16 CLIP ViT-B/32、
  L2-normalized 输出合同；唯一变量为官方 `batch_size∈{16,64,256,512}`。
- 编排：固定 seed 交错 1 warmup+2 formal，共 12 runs；formal steady-state proxy≥60s、
  exactly-once 和 unique/processed row 门禁 fail closed。

25K×1 的先导运行因 formal 只有 30.309s 被 60 秒门禁拒绝，未进入本结果；它只用于
纠正 workload 规模。失败目录已清理，拒绝原因保留在 `PROJECT_LOG.md` 和部署文档。

复现命令（`DATABASE_URL` 由服务器 runtime env 提供，不写入 artifact）：

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a
PY=/root/autodl-tmp/venvs/vllm-4090/bin/python
OUT=/root/autodl-tmp/experiment-artifacts/image_ray_data_native_crosscheck_60k_x2_<run-id>
"$PY" code/scripts/experiments/run_image_clip_matrix.py \
  --config deploy/autodl/image_ray_data_native_crosscheck.example.json \
  --image-runner code/scripts/experiments/run_image_clip_e2e.py \
  --python-executable "$PY" --output-dir "$OUT"
```

### 9.2 严谨性自检

1. matrix `status=complete`，12/12 runs、0 incidents；8 个 formal 均为 120,000/120,000、
   60,000 unique、`exactly_once=true`。
2. 所有 formal 的 `embedding_output_contract_effective=l2_normalized`；同配置两个 repeat
   的吞吐 CV 为 0.03%–2.08%。本矩阵预先固定为 2 个 formal repeat，只用于配置选择；
   低 CV 不把它升级为跨系统正式排名，后者仍按 1 warmup+3 formal 执行。
3. formal E2E 为 125.1–136.7s，全部超过 60 秒；没有把 warmup 用于配置排名。
4. 四个 batch 的资源预算相同；只调整 Ray Data 官方 batch 参数，没有给 baseline 注入
   项目调度代码。
5. GPU utilization 是低频采样的 busy 指标，不是 MFU；本实验没有提供已验证模型 FLOPs，
   因此 `estimated_e2e_mfu` 为空，报告不声称 MFU 数值。

### 9.3 基于 raw CSV 的 formal 数据

| batch | images/s 两次原值 | 中位数 | CV | vs batch64 | E2E 中位 | first output | GPU util mean | 显存峰值中位 | images/J |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 | 948.830 / 921.388 | 935.109 | 2.075% | -2.298% | 128.355s | 42.319s | 7.614% | 822 MiB | 6.744 |
| **64** | **954.753 / 959.446** | **957.100** | **0.347%** | **0%** | **125.380s** | **38.742s** | **6.045%** | **931 MiB** | **6.998** |
| 256 | 918.981 / 919.406 | 919.193 | 0.033% | -3.961% | 130.549s | 42.513s | 5.415% | 1,312 MiB | 6.817 |
| 512 | 888.675 / 877.767 | 883.221 | 0.873% | -7.719% | 135.872s | 47.190s | 5.344% | 1,813 MiB | 6.575 |

完整派生列见 [`long_crosscheck_summary.csv`](long_crosscheck_summary.csv)，原始 12 行见
[`raw/runs_phase3_long_crosscheck.csv`](raw/runs_phase3_long_crosscheck.csv)，编排顺序、
fingerprint、commit 和 0 incident 见
[`raw/matrix_manifest_phase3_long_crosscheck.json`](raw/matrix_manifest_phase3_long_crosscheck.json)。

batch64 两个 formal 的 Ray Data stats 中，CPU preprocess aggregate throughput 为
1333.745/1354.235 rows/s、single-task 为 177.732/180.318 rows/s；GPU predictor
aggregate throughput 为 1361.693/1381.741 rows/s、single-task 为
2667.401/2627.867 rows/s。官方 stats 的 aggregate 值包含 stage overlap，不能与端到端
957.100 images/s 直接相减解释成等待时间；它只支持 preprocess aggregate 略低于 predictor、
且单 task 服务能力明显更低。

### 9.4 结果解释

**事实**：

1. batch64 是观测吞吐最高点，且 first output、E2E、images/J 和 CV 同时最好。
2. batch16 比 batch64 慢 2.30%，属于预注册的 3% 近优区；它只在显存上少约 109 MiB，
   但首输出慢 3.58s、能效低 3.62%、CV 更高，因此没有足够理由替换 batch64。
3. batch256/512 分别比 batch64 慢 3.96%/7.72%，且显存和 first output 同时变差。
4. batch512 没有达到“相对 batch64 改善≥3%”的继续条件，因此停止，不测 1024。
5. cpu busy cores 中位数约 14.1–14.8，与 source4+preprocess8+model2 的 14-slot 资源
   账本一致；没有 CPU 资源超卖。

**推断**：

- 结合官方 operator stats，batch64 下 CPU preprocess aggregate throughput 略低于 GPU
  predictor，且 single-task 速度约为 predictor 的 1/15，支持“预处理/喂入仍是主要限制”
  的解释。这个推断与 R0 GPU-resident ceiling 明显更高一致，但不是硬件 PCIe counter 或
  Nsight 因果证明。
- 5K screening 的 346.5 img/s 与长稳态 957.1 img/s 差异主要说明启动和 fill/drain 成本
  对小 workload 很大；不能把它写成 batch64 自身带来 2.76× 加速。

**待确认**：

- 正式系统排名仍需让 Daft built-in、Ray Data 和 project 在同一 60K×2、schema v11、
  统一输出合同下交错 1+3；本节只冻结 Ray Data 自身配置。
- 本实验只有 CPU preprocess actor pool 和 tensor predictor；若未来替换模型、分辨率、
  processor 或硬件，batch64 必须重新校准。

**不能声称**：

- 不能用本节直接声称项目比 Ray Data 快，也不能与 5K Daft 数字计算正式倍数。
- 不能把 5%–8% `nvidia-smi` util 写成 MFU，或据此宣称 GPU 92%–95% 时间严格空转。
- 不能把 embedding norm/digest 当作 AI_CLASSIFY accuracy 或检索 Recall@K。

### 9.5 对课题含义与下一步

Ray Data 原生配置已冻结为 `batch64/cpu8/gpu2/source4`。扩大 batch 不是改善原生链路的
有效杠杆；继续扫 1024 没有实验依据。下一步不是修改 Ray Data 调度，而是用该冻结点
参加统一合同 formal，并把原生系统观察结果与项目方法分开报告。
