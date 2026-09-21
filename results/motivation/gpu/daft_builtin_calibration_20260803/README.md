# Daft built-in `embed_image` batch 校准（2026-08-03）

> 性质：**原生 baseline 独立校准**（campaign step 4 的前半）。只扫 Daft 官方 `embed_image`
> 暴露的唯一参数 `batch_size`，找吞吐平台点。Daft 自管并发、GPU 放置、backpressure 与
> 调度（`scheduler_owner=daft`，`custom_scheduling_code=false`）。**这不是 baseline 排名**——
> 排名是 step 5，需统一 L2-normalized 输出合同 + 匹配规模。本结果只报 Daft built-in 的独立
> 校准：最佳 batch、img/s、GPU 饥饿现象。

## 1. 实验目的

- **问题**：Daft built-in `embed_image`（vendor-native AI Function）在固定输入/模型/GPU 下，
  其唯一可调参数 `batch_size` 取多少让吞吐达到平台？平台点的绝对吞吐是多少？GPU 是否被打满？
- **方法**：扫 batch {16, 32, 64, 128, 256} × 2 formal repeats，其余全部冻结；Daft 自管执行。
- **关系到哪个方向**：多模态泛化验证的 framework-native baseline 独立校准；为 step 5 正式排名
  选定 Daft built-in 的 batch 配置；同时产出"原生 pipeline 是否喂饱 GPU"的动机信号。

## 2. 实验设置

- 服务器：AutoDL 2×RTX 4090，本实验双卡 `CUDA_VISIBLE_DEVICES=0,1`。
- 数据：PostgreSQL `coco_train2017_60k`（60000 行，avg 159 kB/image），`--limit 5000`（前 5000 行）。
- 模型：CLIP ViT-B/32，`/root/autodl-tmp/models/clip-vit-base-patch32`；Daft provider 解析 processor/dtype。
- 引擎版本：Daft 0.7.21、Ray 2.56.1、PyTorch 2.11.0+cu130、Transformers 5.14.1；
  PostgreSQL 18.4、pgvector 0.8.5；schema v10；代码 commit `29b256b`。
- 冻结合同（每 run）：`--arm daft_builtin_embed --limit 5000 --dataset-passes 1 --warmup-rows 256
  --gpu-workers 2 --source-shards 4 --max-active-batches 8 --phase formal`，1 warmup + 2 formal。
  `--cpu-workers` 对该 arm 无效（Daft 自管），manifest 记 `cpu_workers=` 空。
- 唯一扫描因子：`batch_size ∈ {16,32,64,128,256}`。
- 输出：`raw/runs.csv`（10 行 formal）+ per-run manifest + 远端 `calibration.log`。
- 远端 artifact：`/root/autodl-tmp/experiment-artifacts/daft_builtin_calib_5k_20260803/`。

## 3. 合规性自检

1. **provenance**：`implementation=vendor_builtin_ai_function`、`scheduler_owner=daft`、
   `formal_baseline_eligible=true`、`custom_scheduling_code=false`——确为 vendor-native formal baseline，
   非项目自写 diagnostic。
2. **正确性**：10/10 run `exit=0`、`exactly_once=True`、`output_rows=5000`、`embedding_dimension=512`；
   每 batch 的 `embedding_digest_xor_rounded5` 在 2 个 rep 间一致（确定性），跨 batch 因 FP 求和顺序不同而异（预期）。
3. **`max_norm_error≈11.847` 是预期**：Daft built-in 输出**未 L2-normalize 的 raw embedding**
  （raw norm P50≈10.47，见 `image_embedding_parity_20260803`）。该字段衡量的是与单位 norm 的偏差，
   不是错误。parity 门禁已证 post-normalize cosine P1=0.9998。
4. **稳定**：每 batch 2 rep 的 img/s CV = 1.1–2.0%（见 `summary.csv`）。
5. **资源账本**：`ray_cluster_num_cpus=6`、`declared_total_cpus=6`、
   `resource_budget_semantics=ray_reserved_slots_includes_daft_sql_readers_and_model_actors;
   builtin_provider_owns_actual_concurrency`——runner 为 Daft 预留 6 CPU slot（source_shards 4 + GPU 2），
   实际并发由 Daft 自己决定。

> **本次校准未跑 feeding-saturation 门禁**（≥95% of bounded client）。原因见 §5/§6：Daft built-in
> 在该数据/拓扑下 GPU 平均利用率仅 ~3%，是喂入侧瓶颈，不是 GPU 饱和；feeding-saturation 门禁的
> 目的是防止"误把没喂饱 GPU 的策略当成优胜"，而 Daft built-in 恰恰是"原生就没喂饱"的对照样本，
> 其低 GPU 利用率本身就是被记录的现象，不是要修正的违规。

## 4. 实验设计

batch 单因素扫描，每点 2 formal repeats。平台判据：相邻 batch 的 img/s 中位数增益 <3% 即视为进入平台。
所有 run 共享同一 5000 行输入、同一模型 revision、双卡拓扑；warmup-rows=256 保证即使 batch=256 也至少
有 1 个 warmup batch。

## 5. 实验数据（全组件）

### 5.1 吞吐 + 端到端（per batch，2 rep 中位数）

| batch | img/s median | img/s CV | operator_e2e median | first_output median | 相邻增益 |
|---:|---:|---:|---:|---:|---:|
| 16 | 156.6 | 1.7% | 31.93 s | 30.42 s | — |
| 32 | 172.2 | 1.6% | 29.03 s | 27.76 s | +10.0% |
| **64** | **177.4** | **1.1%** | **28.19 s** | **26.59 s** | **+3.0%** |
| 128 | 176.2 | 1.7% | 28.38 s | 27.03 s | −0.6% |
| 256 | 169.6 | 2.0% | 29.48 s | 28.16 s | −3.8% |

> **最佳 batch = 64**（177.4 img/s，CV 最低 1.1%，e2e 最短 28.19 s）。64 之后吞吐持平或下降
> （256 甚至低于 batch 32），平台判据在 batch 64 满足。

### 5.2 GPU + 能耗（per batch）——**核心观察：GPU 严重未打满**

| batch | gpu_util mean | gpu_util peak | gpu_active_device_count | gpu_active_power mean | images_per_gpu_s | images_per_joule |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 3.34% | 13% | 2 | 100.9 W | 77–79 | 1.53–1.57 |
| 32 | 2.85% | 23% | 2 | 100.7 W | 85–87 | 1.69–1.72 |
| 64 | 3.24% | 21% | 2 | 99.2 W | 88–89 | 1.78–1.79 |
| 128 | 3.77% | 41% | 2 | 100.8 W | 87–90 | 1.72–1.76 |
| 256 | 2.65% | 76% | 2 | 100.2 W | 84–86 | 1.66–1.72 |

- `gpu_util_mean_pct` 全程 **1.2–4.1%**：两张 4090 都被 claim（`gpu_active_device_count=2`），
  但平均利用率仅 ~3%，即 **CLIP forwarder ~97% 时间在等数据**。
- `gpu_util_peak_pct` 随 batch 上升（13%→76%）：大 batch 的单次 forward 更久、能瞬时拉高利用率，
  但**均值仍 ~3%**——喂入间隙把均值压回低位。
- 功耗 mean ~100 W（TDP 450 W 的 ~22%），PCIe gen4×16 满宽。
- per-device（batch 256 r2，`gpu_per_device_json`）：GPU0 util_mean 2.13% / GPU1 util_mean **0.34%**
  ——Daft actor pool 对第二张卡的利用更弱。

### 5.3 CPU + 主机

- `cpu_busy_cores_mean` 5–9（of 128 logical / 32 physical cores）——CPU 也未饱和。
- `host_memory_mean_pct` ~4.3%、`host_memory_peak_pct` ~5.6%——内存无压力。

### 5.4 阶段计时——**vendor-native 边界不可见**

所有 `batch_*_p50_s`（forward/preprocess/host_copy/h2d/d2h/source_next）字段为空，
`batch_service_semantics=unavailable_engine_internal`。**这是预期且正确的**：Daft 自管执行内部，
runner 无法插桩 per-batch 阶段；只能看到聚合 `operator_e2e_s` + 主机/GPU 采样。**不能**因此声称
"Daft 的 forward/preprocess 各占多少"——那需要 Daft 自带 profiler，超出本次校准边界。

### 5.5 流式边界——**近末端发射**

`first_output_s` ≈ 26–31 s，而 `operator_e2e_s` ≈ 28–32 s——首条结果几乎在 run 末端才出现。
Daft 的 `DistributedActorPoolProject` 按 partition 缓冲、整段完成后才向 driver 发射 record batch，
几乎没有真正的流式。因此 5K 规模下的 img/s 是**整段聚合速率**（含缓冲/transient），不是干净的稳态流式吞吐。

## 6. 结果解释

### 事实

1. batch_size 平台点 = 64（177.4 img/s）；16→64 增益 +13%，64→256 持平或下降。
2. GPU 平均利用率 1.2–4.1%（双卡均 claim），峰值随 batch 升至 76% 但均值不升——**GPU 严重饥饿**。
3. CPU（5–9 busy cores）、内存、PCIe 均未饱和；既不是 GPU 算力瓶颈，也不是纯 CPU 饱和。
4. 近末端发射：first_output ≈ operator_e2e 的 90–97%。
5. 输出正确性闭环：exactly_once、digest 确定性、parity（归一化后 cosine P1=0.9998）均成立。

### 推断（标注为推断）

- Daft built-in 在该 bytea-in-PG 拓扑下是**喂入/流水线瓶颈**，不是 GPU 算力瓶颈：GPU forward
  本身很快（见 `image_clip_transfer_ceiling_20260803` R0 ~9.6–9.8K img/s、batch64 forward 6.5 ms），
  但原生 `PhysicalScan→decode→actor pool` 链路把数据喂给 GPU 的速率远低于 GPU 消费速率，
  故 GPU ~97% 空等。batch_size 是 Daft 暴露的唯一旋钮，但它作用于 GPU 侧 forward，对喂入侧瓶颈影响有限
  ——这解释了为何 64 之后吞吐不再升。
- 5K 规模下整段 run 受缓冲/transient 主导（近末端发射），img/s 是聚合值；更大规模（更多 partition）
  可能让稳态读数更干净，但喂入瓶颈的性质不会变。

### 待确认

- batch 64 的 177 img/s 在更大规模（如 60K×1）下是否一致——需 step 5 formal run 验证。
- Daft actor pool 对 GPU1 利用更弱（0.34% mean）是否可由 Daft 自身并发参数改善——**但那不是项目应改的**
  （会破坏 vendor-native 边界）；只作记录。

### 不能声称

- **不能排名**：177 img/s（Daft built-in，5K）vs project_ray ~1681 img/s（60K×2，cpu16/active32）的
  ~9.5× 差距**不是受控比较**——规模不同（5K vs 120K）、CPU 分配不同（Daft 自决 vs 16 CPU actor）、
  Daft 未归一化。任何"项目比 Daft 快 N×"的结论必须等 step 5 统一 L2-normalized contract + 匹配规模。
- 不能声称 batch 64 是绝对最优——只是 5K 校准的平台点。
- 不能声称 per-batch 阶段占比（vendor-native 边界不可见，§5.4）。
- 不能把聚合 img/s 写成干净稳态流式吞吐（§5.5 近末端发射）。

## 7. 对课题含义

1. **强动机信号**：Daft built-in 作为 framework-native baseline，在真实 bytea-in-PG 链路上把两张 4090
   闲置到 ~3% 利用率，而 GPU forward 天花板 ~9.7K img/s。**原生 pipeline 没喂饱 GPU**——这正是课题
   "上游数据组织/喂入调度"的价值所在：优化点在 GPU 之前，不在 GPU 内部。与 R0-R2/R3 木桶判决一致。
2. **为 step 5 选定 Daft built-in 配置**：batch=64（平台点）。formal 排名时 Daft built-in 用 batch 64，
   规模应放大以获得更干净稳态（注意 5K 是缓冲主导）；L2-normalize 须计入 Daft 计时边界（step 2 合同）。
3. **vendor-native 边界纪律**：Daft 的低 GPU 利用率是**被观察的现象**，不是项目要"修"的 bug；
   项目不得为 Daft 注入自己的并发/调度来抬利用率，否则破坏 baseline 可比性。

## 8. 下一步

1. **Ray Data native 校准**（campaign step 4 后半）：`ray_data_staged`，扫 batch + preprocess actors +
   GPU pool，找其平台点。同样不注入项目调度。
2. step 5 正式排名：Daft built-in(batch 64) + Ray Data native(校准后) + project_ray，统一 L2-normalized
   contract，匹配规模，feeding-saturation 门禁。
3.（可选）Daft built-in 在更大规模（60K×1）的单点复核，确认 5K 平台点外推稳定。

## 原始数据

- `raw/runs.csv`（10 行 formal，147 列 schema v10）
- `summary.csv`（per-batch 中位数 + CV + 相邻增益）
- 远端：`/root/autodl-tmp/experiment-artifacts/daft_builtin_calib_5k_20260803/`
  （`runs.csv` + 10 个 per-run manifest + `calibration.log`）
- 复现：见 `deploy/autodl/image_serving.md` §5.4（daft_builtin_embed 命令，本校准将 `--limit` 设 5000、
  扫 batch、2 rep）
