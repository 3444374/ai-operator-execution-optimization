# 图像 CLIP host-path 木桶 screening（2026-08-02）

> 性质：真实 2×RTX 4090、PostgreSQL 18.4、Daft source、Ray CPU/GPU actor、
> CLIP ViT-B/32 的 **1-run screening + 代表点侵入式诊断**。它用于选择正式实验点，
> 不是 formal headline，也不是与 Daft/Ray Data baseline 的最终性能排名。

## 1. 实验设置

- 数据：PostgreSQL `image_documents` 中 `coco_val2017` 的 5000 张不同 JPEG，编码输入
  约 814.7MB；每次 64 张一批，共 79 批。
- 模型：本地 `clip-vit-base-patch32`，两张 RTX 4090，各一个 tensor-only GPU actor，
  float16 forward，输出 512d float32 L2-normalized embedding。
- 线程合同：每个 Ray CPU/GPU worker 的 Torch intra/inter-op=`1/1`；Daft source
  threads、preprocess actor、GPU actor 分开计入 host 预算。
- 计时：`operator_e2e_s = 冷模型池建立 + 查询执行`；本报告另计算
  `post_setup_query_s = operator_e2e_s - worker_setup_s`，两者不能混读。
- correctness：全部有效行均为 5000 rows、exactly-once，digest 固定为
  `60ba2cec6fe8e38a3e4d382e1dcd044b`，max norm error约 `1.79e-7`。
- 代码：`c1484f2` 运行 screening，`084f08c` 运行代表点分段诊断；当前 main 已包含
  两者所需的资源与 telemetry 修复。

原始结果已随报告归档在 [`raw/`](raw/)；每个子目录同时保存该扫描的
`runs.csv` 和逐臂 manifest JSON，避免结论只依赖服务器临时文件：

```text
raw/cpu_actor_screen/                 # preprocess actor = 1/2/4/8
raw/source_thread_screen/             # source threads = 1/2/4/6
raw/active_batch_screen/              # active batches = 4/8/16/32/64
raw/cpu_actor_followup/               # preprocess actor = 16/32
raw/representative_stage_diagnostic/  # 16 actor + active32 的 schema v8 诊断
```

服务器 `/root/autodl-tmp/experiment-artifacts/` 中仍保留同内容的运行时副本，但它不再是
报告可复现性的唯一来源。`summary.csv` 是由上述原始记录整理出的紧凑读表，不替代原始证据。

## 2. 实验设计

依次只改变一类容量变量：

1. 固定 source=4、active=8，扫 preprocess actor=`1/2/4/8`；
2. 固定 preprocess=8、active=8，扫 source threads=`1/2/4/6`；
3. 固定 source=4、preprocess=8，扫 active batches=`4/8/16/32/64`；
4. 在 active=32 下补 preprocess actor=`16/32`，观察 actor 创建成本与查询平台；
5. 对当前冷 E2E 最佳的 `16 CPU actor + active32` 开启 CUDA 同步和 schema v8
   driver 分段，只用于机制归因。

典型命令：

```bash
python code/scripts/run_image_clip_e2e.py \
  --arm project_ray \
  --model /root/autodl-tmp/models/clip-vit-base-patch32 \
  --pg-dsn "$DATABASE_URL" --workload-name coco_val2017 \
  --limit 5000 --warmup-rows 64 --batch-size 64 \
  --cpu-workers 16 --gpu-workers 2 --source-shards 6 \
  --source-cpu-threads 4 --max-active-batches 32 \
  --torch-intraop-threads 1 --torch-interop-threads 1 \
  --detailed-stage-timing --phase gate --repeat-index 0 \
  --out-csv runs.csv --out-manifest representative.json
```

## 3. 严谨性自检

- 修复了两项会破坏公平性的隐藏变量：Ray `num_cpus` 不限制 Torch/OMP 线程；Daft
  source 位于 Ray cluster 外。schema v8 现在分别记录 worker thread pools、Ray slots、
  external source threads 和 host 总预算。
- source threads 与 preprocess actor 已解耦；不会在一次扫描中同时扩大两阶段。
- `nvidia-smi` 500ms util 只作低频旁证，不称 MFU；未提供经验证 FLOPs 时 MFU 留空。
- detailed timing 强制 CUDA synchronize，会改变 overlap，只解释阶段，不充当 headline。
- host busy cores 是整机指标，包含 driver/raylet/系统背景，不能冒充 actor attribution。
- 每点只有一次，5000 图导致冷启动占比很高；相对顺序仍可能受时间漂移影响。

## 4. 实验数据

完整紧凑数据见 [`summary.csv`](summary.csv)。关键结果如下。

### 4.1 preprocess actor 是强杠杆

| preprocess actor | active | 冷 E2E | 冷 images/s | post-setup images/s |
|---:|---:|---:|---:|---:|
| 1 | 8 | 34.87s | 143 | 180 |
| 2 | 8 | 23.76s | 210 | 302 |
| 4 | 8 | 16.91s | 296 | 551 |
| 8 | 8 | 13.79s | 363 | 834 |
| 16 | 32 | **11.45s** | **437** | 1661 |
| 32 | 32 | 15.73s | 318 | **1783** |

32 actor 的查询阶段仅比 16 actor 快约 7.3%，但 setup 从 8.44s 增至 12.92s，
first output 从 9.86s 恶化到 14.34s，所以对 5000-row 冷作业是净损失。

### 4.2 source threads 不是主要杠杆

在 preprocess=8、active=8 下，source threads=`1/2/4/6` 的冷吞吐为
`359/366/368/345 images/s`。4 相对 1 仅约 +2.4%，6 反而回落；它不足以解释 GPU
空转，不能把当前问题归因成数据库 read thread 不够。

### 4.3 active window 有最小饱和点，继续排队会伤害延迟

| active batches | 冷 E2E | 冷 images/s | post-setup images/s | completion p50 | 未归因 wait p50 |
|---:|---:|---:|---:|---:|---:|
| 4 | 17.94s | 279 | 498 | 0.43s | 0.03s |
| 8 | 14.29s | 350 | 786 | 0.47s | 0.03s |
| 16 | 13.33s | 375 | 995 | 0.79s | 0.36s |
| 32 | **12.57s** | **398** | **1015** | 1.35s | 0.95s |
| 64 | 13.95s | 359 | 945 | 1.81s | 1.44s |

16→32 的查询吞吐只增约 2%，64 已回落。若优先 SLO/尾延迟，16 更合理；若只看该
短作业 JCT，单次 screening 是 32 最好。必须用 formal repeats 冻结选择。

### 4.4 代表点的木桶分解

`16 CPU actor + source4 + active32` 的侵入式诊断：

- 冷 E2E 12.20s，其中模型池 setup 9.06s，post-setup query 3.13s；
- 79 批 driver 串行累计：source-next 0.668s、materialize 0.181s、submit 0.814s，
  合计 1.66s，约为 query wall 的 53%；三者有阶段交错，不能直接再相加为占比结论；
- preprocess p50 0.399s/batch；79 批 actor-time 总量约 31.5s，除以 16 actor 的
  理想下界约 1.97s；
- host copy/H2D/forward/D2H p50 约 5.8/7.4/7.0/0.08ms/batch；即使按 79 批、两卡
  粗略串行下界，GPU-side transfer+forward 也远小于 preprocess 与 driver path；
- active GPU util mean 2.5% 是低频旁证，不是 MFU。

## 5. 结果解释

### 事实

1. 增加 preprocess actor 能显著降低 E2E，CPU preprocess 是当前强瓶颈信号。
2. 增加 source threads 的收益很小；当前不是单纯 DB reader thread 木桶。
3. active window 从 4 增到 16/32 能改善 JCT，但 64 使吞吐与尾延迟同时变差。
4. 16→32 actor 呈边际递减，且 cold setup/first-output 明显恶化。
5. 代表点中 CPU preprocess 理想下界与 driver/source/submit 累计时间同量级；当前应
   判为 **CPU-preprocess + driver/Ray-submission mixed**，而不是单一 GPU compute 或
   PCIe bottleneck。

### 推断

- 现有路径仍没有持续喂满 GPU；优先优化/重构的对象应是 CPU preprocess pool、
  driver materialization/submit 串行路径和受控 in-flight，而不是继续增大 GPU queue。
- PCIe 优化在当前 CLIP/224×224/FP16 regime 下大概率 NO-GO，但还需 R0/R1 pinned
  对照与 3 repeats 才能按预注册门槛正式判决。

### 不能声称

- 不能把 16 actor 或 active32 称为稳定最优；它们尚未做交错 3 repeats。
- 不能声称项目已优于 Daft/Ray Data 官方 native baseline；本轮没有运行 Daft 内置
  AI Function、固定 upstream 的官方 benchmark code 或移除项目 inflight 后的 Ray Data graph。
- 不能把 `source_next` 称为纯 PostgreSQL 时间，也不能把 `submit` 全称为序列化。
- 不能声称 GPU MFU=2.5%；该字段只是 `nvidia-smi` 利用率。
- 不能从 CLIP ViT-B/32 外推到高分辨率 VLM、视频或 GPU decode workload。

## 6. 对课题的含义

这组数据支持“数据库图像 AI 算子的外部执行链路存在可优化 feeding gap”，但更精确
的故事不是“PCIe 限制 GPU”，而是：在真实 DB→Daft→Ray→CLIP 路径中，CPU 图像
预处理容量与 driver/Ray submission 串行开销共同决定吞吐；actor 数与 active window
存在 JCT/first-output/queue 的非单调权衡。它直接对应项目的数据组织与提交控制主线。

## 7. 下一步

1. 导入至少 20K unique images，让每个 formal 点持续至少 60s；对 8/16 CPU actor、
   active16/32 做交错 `1 warmup + 3 repeats`，按 JCT、first-output、P95/P99、能耗共同选点。
2. 在相同 host 总 CPU/GPU 预算下跑 Daft built-in、官方 ResNet18 parity、Ray Data native graph、project
   static，区分“资源更多”与“执行效率更高”。
3. 补 R0 GPU-resident、R1 pinned H2D、R2 pageable/Ray tensor 直接 ceiling；只有 H2D
   占关键路径≥20%且 pinned/overlap 改善 E2E≥5%，才推进 PCIe 优化。
4. 分别验证批量 driver submission、减少 Python bytes materialize、CPU actor 生命周期
   复用能否带来≥5% E2E/JCT 改善；不要一次叠加后失去归因。
