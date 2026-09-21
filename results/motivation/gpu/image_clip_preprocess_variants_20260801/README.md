# CLIP preprocessing implementation-boundary profile, 2026-08-01

## 1. 实验设置

- 代码：`f3d17af`；AutoDL 2×RTX 4090，单次实验仅暴露 GPU 0。
- 数据：PostgreSQL 18.4 + pgvector 0.8.5 中 COCO val2017 5000 图 BYTEA。
- 模型：本地 `openai/clip-vit-base-patch32`，float16 forward，float32 L2
  normalization；Torch 2.12.1+cu130，Transformers 5.14.1。
- batch size：1/16/32/64/128/256；每格 5 warmup + 30 formal。
- 原始数据：`raw_repeats.csv`（720 行）；运行合同：`manifest.json`；日志：
  `run.log`。

## 2. 实验设计

同一 batch、同一 repeat 内随机交错四条路径，全部经过同一
`ClipTensorActor`，避免把时间漂移或不同模型实现误认为 processor 收益：

| 变体 | decode / processor / 输出边界 | 角色 |
|---|---|---|
| `production_np` | PIL + 当前 `ClipImagePreprocessor` + NumPy | 当前项目实现 |
| `legacy_pt` | PIL + 历史 `CLIPProcessor` + Torch | 旧画像口径 |
| `torchvision_pil_pt` | PIL + torchvision processor + Torch | 只换 processor backend |
| `torchvision_tensor_pt` | torchvision tensor decode + torchvision processor | 真正 tensor fast-path 对照 |

每条输出与 `production_np` 做逐行 cosine 和 max-absolute-difference 检查。
`cpu_preprocess_s` 包含 encoded bytes 解码和 processor；`actor_call_wall_s` 包含
CPU tensor/array → GPU、CLIP forward、float32 normalization 和返回 NumPy。

## 3. 严谨性自检

- gate 先以 64 图、B=1/8、2 repeats 跑通，再运行 5000 图正式配置。
- 变体顺序按固定 seed 在每个 batch/repeat 内随机交错。
- raw repeats、版本、backend、output kind、GPU、PG/pgvector 均落盘。
- 四臂全部成功，无 silent skip；720/720 formal rows 完整。
- 真 cosine 最小值为 1.0，最大绝对 embedding 差为 0.0，质量门禁通过。
- B≥16 的 images/s CV 约 2.0%–6.9%；不使用单次最快值作结论。

## 4. 实验数据

下表为每格 30 次 formal 的中位数。时间单位均为 ms/image；`images/s` 是
`cpu_preprocess + actor_call` 严格串行的 profile 吞吐，不含 DB fetch、Daft/Ray
调度和 pgvector 写回。

| variant | B | CPU prepare | actor | CPU/actor | images/s |
|---|---:|---:|---:|---:|---:|
| production_np | 16 | 5.622 | 0.340 | 16.5 | 168.4 |
| production_np | 64 | 5.201 | 0.143 | 36.4 | 187.1 |
| production_np | 256 | 5.436 | 0.151 | 36.1 | 178.6 |
| legacy_pt | 64 | 5.245 | 0.144 | 36.3 | 185.5 |
| torchvision_pil_pt | 64 | 5.155 | 0.143 | 36.0 | 188.7 |
| torchvision_tensor_pt | 16 | 4.782 | 0.346 | 13.8 | 195.4 |
| torchvision_tensor_pt | 64 | 4.480 | 0.143 | 31.2 | 216.4 |
| torchvision_tensor_pt | 256 | 4.437 | 0.151 | 29.5 | 218.1 |

`torchvision_tensor_pt` 相对 `production_np` 的配对串行吞吐比：

| B | 中位 speedup | 2.5%–97.5% repeat 区间 | fast 胜出 repeats |
|---:|---:|---:|---:|
| 1 | 1.139× | 1.056–1.395× | 29/30 |
| 16 | 1.161× | 1.067–1.311× | 30/30 |
| 32 | 1.172× | 1.137–1.275× | 30/30 |
| 64 | 1.161× | 1.096–1.217× | 30/30 |
| 128 | 1.184× | 1.159–1.344× | 30/30 |
| 256 | 1.219× | 1.152–1.319× | 30/30 |

5000 行 bulk DB fetch 为 5.759s，只记录环境读入成本，不计入上表的重复循环。

## 5. 结果解释

**事实**：

1. 当前 `production_np` 与旧 `legacy_pt` 基本等价；旧 slow-pt 画像可以作为当前
   processor 量级的历史佐证，但不能替代本次 exact-path 数据。
2. 只把 processor 换成 torchvision、仍输入 PIL，收益很小；tensor decode 才形成
   稳定 13.9%–21.9% 的配对中位收益。
3. 即使使用 tensor fast path，实用 batch 的 CPU prepare 仍为
   4.44–4.78ms/image，约为 actor 的 13.8–31.2 倍；CPU/GPU 阶段失衡没有消失。
4. 四条路径在本次模型/数据/精度下输出完全一致，质量不是速度差异的混淆变量。

**推断**：tensor decode 减少了 PIL/NumPy/processor 转换开销，但 JPEG decode、
resize/normalize 和逐图准备仍占主要时间。该结果支持继续验证 CPU worker 与 GPU
actor overlap，不过实际收益取决于 Daft/Ray 传输、队列和 backpressure。

**不能声称**：

- 不能声称项目已经快于 Daft Native、Ray Data 或 vLLM pooling；它们尚未在同一
  E2E 边界运行。
- 不能把 1.14–1.22× 写成 path-B 系统收益；它只是串行 processor 实现差异。
- 不能继续写“GPU 实测空转 95%”；这里仍是阶段时间比，不是 GPU 时间序列。
- 不能外推到其他模型、分辨率、JPEG 实现或多 GPU。

## 6. 对课题含义

初始 slow-path 动机没有被更强 processor 对照推翻：改用 tensor fast path 后，CPU
prepare 仍显著重于 GPU actor。因而可以继续建设 PG→Daft→Ray CPU preprocess→
Ray CLIP GPU actor→pgvector E2E runner；但论文贡献必须来自 E2E overlap、调度、
SLO 或多 job 结果，而不是选择更快的 torchvision API。

## 7. 下一步

1. runner 默认加入 `production_np` 与 `torchvision_tensor_pt` 显式配置；正式主臂
   应采用通过质量门禁的更强 fast path，slow path 只作归因。
2. 先做单 GPU exactly-once E2E gate，记录 DB read、decode/preprocess、object
   transfer、actor queue/service、fan-in 和 writeback。
3. 再比较严格串行、bounded overlap、Daft Native、Ray Data 和 ours；只有 ours
   在相同 processor/模型/质量语义下超过强静态 overlap baseline 才能形成方法 claim。
