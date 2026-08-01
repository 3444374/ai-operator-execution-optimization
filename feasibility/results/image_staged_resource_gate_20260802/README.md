# 图像 staged baseline 显式资源门禁

日期：2026-08-02

提交：`c0b573313428fc565c324a7e33aa84fd8d22d875`

平台：AutoDL，2×RTX 4090，PostgreSQL 18.4 + pgvector 0.8.5，Ray 2.56.1，Daft 0.7.21

## 1. 实验设置

同一 `coco_val2017` PostgreSQL BYTEA 行集合、CLIP ViT-B/32、float16、256 行，
batch=32，4 个显式 SQL shards、4 个 CPU preprocess actors、2 个单 GPU model
actors。每臂先 warmup 64 行；Ray framework 启动不计入 operator wall，模型 worker
冷启动计入。原始 schema v4 CSV/manifest 保留在服务器：

```text
/root/autodl-tmp/experiment-artifacts/image_staged_gate_v4_c0b5733/
```

## 2. 实验设计

问题不是“哪个框架更快”，而是修复后两个 staged 强 baseline 能否同时满足：

1. SQL reader、CPU stage、GPU stage 都能推进，不再形成 0-row 资源死锁；
2. Ray cluster CPU 等于显式的 source + preprocess + model slot 总和，且不超过
   进程 affinity 可用 CPU；
3. 256 行 exactly-once，Daft staged 与 Ray Data staged 的完整 embedding digest 一致；
4. 两张 GPU 都被实际激活。

核心命令（两臂只替换 `--arm` 与 manifest 名）：

```bash
PYTHONPATH=code CUDA_VISIBLE_DEVICES=0,1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
  /root/autodl-tmp/venvs/vllm-4090/bin/python code/scripts/run_image_clip_e2e.py \
  --arm daft_staged --model /root/autodl-tmp/models/clip-vit-base-patch32 \
  --pg-dsn "$DATABASE_URL" --limit 256 --warmup-rows 64 --batch-size 32 \
  --cpu-workers 4 --gpu-workers 2 --daft-model-workers 2 --source-shards 4 \
  --max-active-batches 8 --phase gate --repeat-index 0 \
  --out-csv "$OUT/runs.csv" --out-manifest "$OUT/daft_staged_256.json"

# Ray Data 不使用 --daft-model-workers：
# --arm ray_data_staged ... --out-manifest "$OUT/ray_data_staged_256.json"
```

## 3. 严谨性自检

- 资源预算为 `4 + 4 + 2 = 10 CPU`，host affinity 检测为 128；没有用
  `ray.init(num_cpus=...)` 虚构超过物理可用量的容量。
- Daft staged 与 Ray Data staged 使用同一输入、processor、model、dtype 和输出审计。
- 这是单次小规模 gate，模型冷启动占主导；没有随机块顺序或 3 repeats，不能比较
  18.74 与 32.19 images/s 的性能高低。
- Ray Data 启动期仍出现一次瞬态 `resources not enough` 警告，但随后 SQL rows、
  4 个 preprocess tasks 和 4 个 predictor tasks 均完成；它不是原先永久 0-row 死锁，
  但正式实验必须继续把该警告和 operator stats 作为有效性检查。
- CPU、内存、disk/network、context switch 和 interrupt 都是 host delta/采样，可能含
  同机系统噪声；它们用于发现异常和资源效率趋势，不是进程级精确归因。PCIe 字段只
  记录链路代际/宽度，不是硬件传输 byte counter。

## 4. 实验数据

紧凑数据见 `runs_summary.csv`。两个 arm 都返回 256 行、`exactly_once=true`，
digest 都为 `f071d16276dee75f941b5a3cd60846ef`，两张 GPU 均有采样活动。

| arm | operator wall | images/s | first output | CPU 账本 | active GPU |
|---|---:|---:|---:|---:|---|
| Daft staged | 13.660 s | 18.741 | 13.557 s | 4+4+2=10 | `[0,1]` |
| Ray Data staged | 7.952 s | 32.192 | 7.948 s | 4+4+2=10 | `[0,1]` |

## 5. 结果解释

**事实**：原来“4 preprocess + 2 GPU actors 把 6 CPU 全占满，SQL reader 无法调度”
的永久死锁已消失；schema v4 能同时记录 host、cluster 和三段资源声明；两个 staged
arm 的输出逐行集合与 digest 等价。

**推断**：把 source slot 纳入资源合同是必要修复。Ray Data 的瞬态资源告警说明
“总量足够”不等于调度过程已经最优，后续仍要观察 operator-level pending/running。

**不能声称**：Ray Data 比 Daft 快 1.72×、GPU 已压满、PCIe 不是瓶颈，或项目优于
这两个 baseline。小 gate 的 active-GPU mean util 低于 1%，吞吐主要受冷启动和短作业影响。

## 6. 对课题的含义

staged Daft/Ray Data 现在具备进入独立 calibration 的最低条件。后续项目结果必须
同时对比 fused framework 和这两个 staged 强 baseline；资源账本不一致的 arm 无效。

## 7. 下一步

1. 分别扫描 staged Daft/Ray Data 的 batch、actor/source shape 与 in-flight；
2. 冻结各自最佳点后跑 COCO 5K、随机块交错、1 warmup + 3 formal repeats；
3. 补统一 pgvector sink 后重跑 system E2E；
4. Ray Data 每轮检查 SQL task 数、preprocess/predictor task 数、资源警告和 GPU 卡覆盖。
