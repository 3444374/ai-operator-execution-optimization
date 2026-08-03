# project_ray 静态配置选择证据（2026-08-02 / 08-03 两轮）

> **定位（重要）**：本结果是 **project_ray 静态操作点的选择证据**——冻结
> `cpu_workers=16 + max_active_batches=32 + batch=64`，供后续 step-4 跨系统正式排名使用。
> **它本身不是跨系统正式排名**。两轮均跑在**旧 commit**（`1f2e4fe`、`29b256b`）、
> **旧 schema**（无 `schema_version`、无 `embedding_output_contract` 字段），早于 codex 的
> `l2_normalized` 输出合同（`03b815d`/`6f0954b`）。因此它只能用于"选 project 自己的静态点"，
> 不能直接作为最终同机排名的 project_ray 行。最终排名须在**当前 commit + 统一合同**下重跑。

## 1. 实验目的

- **问题**：project_ray 在 60K×2 双卡下，CPU 预处理 actor 数（`cpu_workers`）与 in-flight 上限
  （`max_active_batches`）取哪个组合让吞吐最高且稳定？冻结它作为后续跨系统排名的 project 静态点。
- **方法**：4 配置 `cpu{8,16} × active{16,32}`（batch=64 固定），每配置 1 warmup + 3 formal，
  **重复两轮**（不同日期/commit），取 formal 中位。
- **关系到哪个方向**：campaign §10 step 1（project static 复验）——只冻结项目静态点，**不与
  Daft/Ray Data 横向排名**。

## 2. 实验设置

- 服务器：AutoDL 2×RTX 4090，双卡 `CUDA_VISIBLE_DEVICES=0,1`。
- 数据：PostgreSQL `coco_train2017_60k`（60000 行，avg 159 kB/image），`--limit 60000 --dataset-passes 2`（120K 行/run）。
- 模型：CLIP ViT-B/32，float16，`normalize=True`（project_ray **输出已 L2-normalize**，`max_norm_error=0`）。
- arm：`project_ray`（项目路径：显式 CPU preprocess actor + GPU actor + 项目 admission/inflight）。
- 两轮：
  - **round 1**：2026-08-02，commit `1f2e4fe`，`image_project_static_60k_x2_20260802/`。
  - **round 2**：2026-08-03，commit `29b256b`，`image_project_static_60k_x2_20260803_29b256b_r1/`。
- 冻结合同：`--arm project_ray --batch-size 64 --gpu-workers 2 --source-shards 4 --phase formal`，
  `cpu_workers ∈ {8,16}`、`max_active_batches ∈ {16,32}`。1 warmup + 3 formal。
- 输出：`raw/round1_20260802/`、`raw/round2_20260803/`（各 16 行 runs.csv + matrix_manifest）。

## 3. 合规性自检

1. **正确性**：两轮全部 formal `exactly_once=True`、`output_rows=120000`、`unique_images=60000`、
   `max_norm_error=0.000`（project_ray 归一化生效）。
2. **稳定**：冻结点两轮中位 1701.0 vs 1681.0 img/s，**差 ~1.2%**；每配置 3 formal rep。
3. **⚠️ schema 限制（不能进最终排名的原因）**：
   - 两轮 CSV **无 `schema_version` 字段、无 `embedding_output_contract` 字段**——早于
     `03b815d enforce image embedding output contract`。
   - project_ray 本身已归一化（norm_error=0），但**未经过当前 `l2_normalized` 合同的 fail-closed 门禁**。
   - 因此这些行只能选静态点，不能与 Ray Data native（`f450e07`/合同后）等 arm 并排排名。
4. **未跑 feeding-saturation 门禁**：本项目静态点的目的是"选配置"，不是"证喂饱 GPU"；
   GPU 平均利用率 ~9.6%（见 §5）只是参考信号，不是 MFU。

## 4. 实验设计

4 配置全因子（cpu×active），batch=64 固定。两轮独立跑（不同 commit/日期），用以确认冻结点跨
commit/日期稳定。选点规则：取两轮一致最优、且与次优差距清晰的配置。

## 5. 实验数据（全组件）

### 5.1 四配置 × 两轮 formal 中位 img/s

| 配置 | round1 (1f2e4fe) | round2 (29b256b) | 趋势 |
|---|---:|---:|---|
| cpu8 / active16 / batch64 | 1031.6 | 1026.6 | 基线（CPU 欠配） |
| cpu8 / active32 / batch64 | 1051.1 | 1027.2 | active↑ 对 cpu8 几乎无影响 |
| cpu16 / active16 / batch64 | 1484.9 | 1463.1 | cpu8→16 ~+45% |
| **cpu16 / active32 / batch64** | **1701.0** | **1681.0** | **冻结点**（active↑ 对 cpu16 +15%） |

> **cpu16 是主杠杆**（cpu8→16 约 +45–60%）；active32 只在 cpu16 时显著加成（+15%）。
> 两轮排名一致、数值差 ~1.2% → 冻结 `cpu16/active32/batch64` 稳健。

### 5.2 冻结点全指标（cpu16/active32/batch64，两轮 6 formal 合并/中位）

| 指标 | round1 | round2 | 含义 |
|---|---:|---:|---|
| images_per_s median | 1701.0 | 1681.0 | operator 级吞吐 |
| operator_e2e_s median | 70.5 | 71.4 | 120K 行全 pass 墙钟 |
| first_output_s median | 23.2 | 22.9 | 首条结果（e2e 的 ~32%） |
| gpu_util_mean_pct | 9.57% | 9.81% | nvidia-smi busy 采样均值（**非 MFU**） |
| gpu_util_peak_pct | 37% | 37% | 采样峰值 |
| cpu_busy_cores_mean | 19.5 | 19.5 | ~20/32 logical cores 忙 |
| images_per_gpu_s | 850.5 | 840.5 | per-GPU |
| images_per_joule | 8.43 | 10.27 | 能效（round2 更高，可能 GPU 功耗采样更低） |
| max_norm_error | 0.000 | 0.000 | project_ray 归一化（合同前） |
| exactly_once | True | True | — |

> GPU busy 均值 ~9.6%：高于 Ray Data native（~6%，cpu8）和 Daft built-in（~3%），因为 project
> 用了 16 个 CPU 预处理 actor（vs Ray Data 8）。但相对双卡 forward 天花板 ~19K img/s
> （`image_clip_transfer_ceiling_20260802` R0），1691 img/s 仍只占 ~9%——**GPU 仍有 ~10× headroom**，
> 与"原生 baseline 也喂不饱 GPU"的动机信号一致。

## 6. 结果解释

### 事实

1. **冻结点 = `cpu16/active32/batch64`**，两轮中位 1701.0 / 1681.0 img/s（~1.2% 差）。
2. cpu8→16 是主增益（~+45–60%），active16→32 在 cpu16 时再加 ~15%，在 cpu8 时无效。
3. 两轮跨 commit/日期排名一致、数值稳定 → 选点可靠。
4. project_ray 输出已归一化（norm_error=0），GPU busy 均值 ~9.6%，CPU busy ~20 cores。

### 推断

- project_ray 的增益主要来自**更多 CPU 预处理 actor（16 vs Ray Data 8）+ in-flight 上限放宽**，
  即上游喂入更激进，而非 GPU 侧差异——与"瓶颈在喂入侧"一致。
- 即便如此，GPU busy 仍只 ~10%，说明 project 当前静态点也**远未喂饱 GPU**；这正是后续状态感知
  调度（研究内容 A）的优化空间。

### 不能声称

- **不能排名**：本结果**不能**把 1691 img/s（project，旧 schema）与 Ray Data native 957（`f450e07`/合同后）
  或 Daft built-in（待合同后长规模）并排排名——schema/commit/合同不一致。受控排名是 step-4，须在
  当前 commit + 统一 `l2_normalized` 合同下重跑全部 arm。
- 不能把 GPU busy 均值 ~9.6% 写成 MFU 或"GPU 利用率"（nvidia-smi 采样，非硬件 counter）。
- 不能声称 project_ray 当前静态点已最优——它只是 4 配置里的最优点，状态感知策略（A）尚未叠加。

## 7. 对课题含义 + 下一步

- **冻结 project 静态点 `cpu16/active32/batch64`**，作为 step-4 四臂排名的 project_ray 配置
  （将在当前 commit + 统一合同下重跑，不直接复用本表数值）。
- 两轮稳定（~1.2%）说明静态点选择本身已闭合；缺的是**合同一致性**，不是选点。
- **下一步**（campaign §10 step 4）：
  1. Daft built-in 60K×1 长门禁（验长规模可运行 + 双卡激活 + 无内存爆）；
  2. 门禁过后四臂同机正式排名（Daft built-in / Ray Data native / project static / bounded-direct），
     统一 PG COCO 60K×2、CLIP FP16 512d L2-normalized、当前 commit、1 warmup+3 formal、固定块交错。

## 原始数据

- `raw/round1_20260802/runs.csv`（16 行）+ `matrix_manifest.json`
- `raw/round2_20260803/runs.csv`（16 行）+ `matrix_manifest.json`
- `summary.csv`（四配置 × 两轮中位 + 冻结点全指标 + 跨轮合并）
- 远端：`/root/autodl-tmp/experiment-artifacts/image_project_static_60k_x2_20260802/` +
  `…/image_project_static_60k_x2_20260803_29b256b_r1/`（runs.csv + 16 per-run manifest + matrix_manifest）
