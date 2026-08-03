# CLIP R0/R1/R2 H2D ceiling 诊断（2026-08-02）

> 性质：单张 RTX 4090、CLIP ViT-B/32 的 synthetic mechanism diagnostic。
> 本实验解释 GPU-resident、pinned H2D 和 pageable tensor 边界，不是数据库/Daft/
> Ray Data 系统 baseline，也不是论文 headline。

## 1. 实验设置

- 硬件：AutoDL 2×RTX 4090，本实验仅暴露 GPU0；采样确认 PCIe 4.0 x16。
- 模型：本地 CLIP ViT-B/32，float16，输入形状 `B×3×224×224`。
- batch：16、64、256；每种表示先 5 warmups，再在每个 repeat 内随机排列三种表示，
  共 30 repeats/点、270 条 raw rows。
- 计时：模型加载排除；CUDA events 测 H2D/forward，同步 wall 测串行阶段总时长。
- 代码：`4cf5f67`；原始 CSV/manifest 位于 [`raw/`](raw/)，派生读表见
  [`summary.csv`](summary.csv)。

## 2. 实验设计

三个表示层只改变模型输入所在位置和内存属性：

1. R0 `gpu_resident`：float16 tensor 已在显存，只执行 forward，给出 compute ceiling；
2. R1 `pinned_fp16`：页锁定 CPU float16 tensor，non-blocking H2D 后执行 forward；
3. R2 `pageable_fp32`：只读 NumPy float32 tensor，先取得 writable ownership，再做
   dtype 转换/H2D 和 forward，近似项目 GPU actor 的输入边界。

batch64 的 host float32 tensor 为 38.54MB，device float16 tensor 为 19.27MB。
总数据集行数不会改变这个单批 payload；只有 batch、分辨率、dtype、crop/frame 数会改变。

## 3. 严谨性自检

- 30 repeats 保留原始行，不只保存平均值；每个 repeat 内模式顺序由固定 seed 打乱。
- 三种模式每个 batch 的 `output_sum` 完全一致，最大 norm error≤`5.96e-8`。
- CUDA events 能比 Python wall 更接近 device timeline，但仍不是 PCIe hardware byte
  counter；逻辑 GB/s 使用 host tensor bytes 除以 event time，R2 还包含 dtype 语义。
- R1/R2 串行执行 H2D→forward，没有测试双缓冲 overlap；结果是可分解上界，不是最优流水线。
- 输入是 synthetic zero tensor；R2 每次重新分配 ownership copy，不能把其 copy wall
  直接等同于 Ray object store、生产 actor 或 PostgreSQL 时间。
- batch16 只有约 4–6ms，wall CV约31–32%，太短；batch64/256 的方向更可靠。

## 4. 实验数据

中位数如下；完整 P95/CV 见 `summary.csv`。

| batch | 表示 | wall | ownership copy | H2D | forward | images/s | logical H2D |
|---:|---|---:|---:|---:|---:|---:|---:|
| 16 | R0 resident | 3.98ms | 0 | 0 | 3.94ms | 4,021 | — |
| 16 | R1 pinned FP16 | 4.20ms | 0 | 0.22ms | 3.92ms | 3,814 | 22.2GB/s |
| 16 | R2 pageable FP32 | 5.83ms | 0.57ms | 1.14ms | 4.01ms | 2,743 | 8.4GB/s |
| 64 | R0 resident | 6.90ms | 0 | 0 | 6.86ms | 9,281 | — |
| 64 | R1 pinned FP16 | 7.73ms | 0 | 0.80ms | 6.87ms | 8,281 | 24.0GB/s |
| 64 | R2 pageable FP32 | 32.07ms | 20.87ms | 4.14ms | 6.85ms | 1,996 | 9.3GB/s |
| 256 | R0 resident | 27.87ms | 0 | 0 | 27.83ms | 9,185 | — |
| 256 | R1 pinned FP16 | 31.08ms | 0 | 3.11ms | 27.91ms | 8,237 | 24.8GB/s |
| 256 | R2 pageable FP32 | 133.53ms | 87.41ms | 16.35ms | 27.94ms | 1,917 | 9.4GB/s |

## 5. 结果解释

### 事实

1. pinned FP16 H2D 随 payload 近似线性：batch64 为0.80ms、batch256 为3.11ms，
   逻辑带宽约24–25GB/s，接近 PCIe 4.0 x16 可达到的有效数量级。
2. pageable FP32 的 H2D/转换约为 pinned 的5倍，逻辑带宽约9.3GB/s。
3. R2 最大开销不是 forward，而是本 synthetic 实现中的 ownership allocation/copy；
   batch64/256 分别约20.9/87.4ms。
4. R0 的 compute throughput 在 batch64 已进入平台：9,281 images/s；batch256 为9,185。

### 推断

- 先前 project 代表点约7.4ms/batch 的“H2D”并非因为总 workload 只有5K而失真；
  它测的是 pageable FP32→float16 device 的同步 wall，而非纯 pinned PCIe copy。
- 纯 PCIe capacity 在当前 CLIP/224/FP16 下不像第一木桶；pageable ownership/copy 和
  dtype 边界更值得进入 R2→R4 受控验证。
- 但生产 representative 的 preprocess约399ms/batch，host-copy+H2D 约13ms/batch；
  即使缩短传输也未必给 operator E2E 带来≥5%，必须用完整流水线实测。

### 不能声称

- 不能声称 pinned memory 已让项目提速，或 PCIe 路线已经正式 NO-GO；尚无 R4 E2E 消融。
- 不能把 R2 的20–87ms ownership copy 写成 Ray 固有开销；它包含本脚本分配行为。
- 不能用 R0/R1/R2 images/s 与 Daft/Ray Data/operator-E2E 横向排名。
- 不能从 zero tensor、CLIP ViT-B/32 外推到高分辨率 VLM、视频或 GPU decode。

## 6. 对课题的含义

这组数据把“传输”拆成了两块：PCIe H2D 本身和 H2D 之前的 host ownership/dtype
准备。前者在 pinned 条件下很快，后者可能明显更大。这支持继续研究外部执行链路的
数据表示与阶段边界，但不支持预先把论文动机写成“PCIe 带宽不足”。正式贡献仍需在
matched-resource 的 PostgreSQL→Daft/Ray→GPU operator E2E 中体现。

## 7. 下一步

1. 完成 COCO train ≥20K unique、最快臂查询阶段≥60秒的 project formal；
2. 在冻结 project 点上加 pinned-input/ownership reuse 单因素 E2E 臂，要求≥5%且
   至少2/3 repeats 同向；
3. 用 Nsight/CUDA events 代表窗口核对实际 memcpy kind/bytes、NUMA 与 overlap；
4. 完成 R3 in-memory JPEG、R4 PostgreSQL/Daft，并与 Daft fused/staged、Ray Data
   staged 在相同物理预算下比较；再按预注册20% wall/5% E2E门槛判 PCIe GO/NO-GO。
