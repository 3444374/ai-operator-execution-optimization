# 图像 Project staged descriptor + observe-only snapshot 正式门禁（2026-08-10）

> 结论先行：同一 2K short + 3×3K long 图像合同下，24/24 group run 全部通过，
> 99,000 行 formal Job 记录全部 exactly-once。3,114 个 formal submit/complete 事件的
> runtime snapshot 均为 `observe_only` 且 100% fresh，构建均值 0.141 ms、age P95
> 0.246 ms。four-job observe-only proposed-role 与 frozen static 的 group JCT 分别为
> 5.691 s 与 5.635 s，差 0.98%，未达到 5% 门槛。因此本实验通过的是 staged
> descriptor/状态观测接入门，不是 stage-aware 调度胜出。

## 1. 实验目的

在不改变既有 model-pixel credit 和调度决策的前提下，把图像请求的 source、prepare、
model、result 四阶段 `WorkDescriptor` 与 fresh `RuntimeStateSnapshot` 接入正式 Project
runner，并回答三个最小问题：

1. descriptor calibration signature 是否在全矩阵保持一致；
2. observe-only snapshot 是否足够新、构建成本是否可忽略；
3. 接线后 frozen static 与保留的 proposed-role 是否仍完成相同结果语义，而不会把观测
   误写成控制效果。

## 2. 实验设置

| 项 | 冻结合同 |
|---|---|
| 平台 | AutoDL，2×RTX 4090；Ray head `127.0.0.1:6380`；服务器重启后先执行 cold-start preflight |
| 数据 | PostgreSQL `coco_train2017_60k`；short 2,000 行，long1/2/3 各 3,000 行；manifest SHA `fd8cff32…a64765` |
| 到达 | short 0 s；三个 long 0.5 s；同原生图像正式矩阵 |
| Project runner | typed CLIP Ray GPU actors；static 每 Job 32 active batches；proposed-role 使用现有 fair endpoint credit coordinator |
| descriptor | `staged_v1_legacy_equivalent`；calibration signature `image-stage-work-v1:07b3c83b…84ce4d` |
| runtime state | `observe_only`，freshness 上限 50 ms；snapshot 只写 trace，不进入控制决策 |
| 重复 | 6 个场景各 1 warm-up + 3 formal，共 24 group runs、36 formal Job rows |
| 版本 | repository commit `939b7268502ee56447cdfc1bfa256a466a2bdbed` |

服务器原始证据：
`/root/autodl-tmp/experiment-artifacts/opening_image_project_fourjob_observe_only_formal_20260810/`。

## 3. 合规性自检

- matrix `passed`，24/24 group runs passed；6 warm-up + 18 formal。
- 36 条 formal Job rows 全部 exactly-once，共完成 99,000 行；所有场景共享同一 manifest
  SHA 和 descriptor calibration signature。
- 3,114 个 formal control-trace 事件全部 `observe_only`、全部 fresh；全事件 snapshot 构建
  均值 0.141 ms，age P95 0.246 ms，远低于 50 ms freshness 上限。
- four-job formal JCT CV：proposed-role 2.88%，frozen static 0.29%；两臂均稳定。
- staged descriptor 的 legacy-equivalent model work 恒等式由 15 个 image 定向单元测试覆盖；
  正式 trace 持久化 calibration signature 与 snapshot，未持久化逐事件 descriptor JSON，
  因而不把 raw 证据表述为逐事件字段恒等审计。
- GPU util 只有 10.48%–12.32%，该图像链路仍是 CPU/data-feeding 受限状态；本实验不是
  GPU feeding-saturated 容量排名。

## 4. 实验设计

single-full 四臂用于给每个 Job 建立独立完成基线；four-job 只比较 frozen static 与
observe-only proposed-role。两臂都构建相同 staged descriptor/snapshot，区别仍是既有
static partition 与 fair credit coordinator；snapshot 本身不驱动任何动作。因此本矩阵可
验收观测接入和现有策略角色的稳定性，但不能隔离“snapshot 的性能收益”。

## 5. 实验数据

### 5.1 组级 formal mean（n=3）

| 场景 | group JCT | CV | images/s | GPU util mean | MFU | 能耗 | CPU busy cores | snapshot build mean | fresh |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| single short | 2.319 s | 3.38% | 863.26 | 3.04% | 0.0456 | 281.5 J | 17.02 | 0.108 ms | 100% |
| single long1 | 2.844 s | 1.54% | 1054.97 | 4.30% | 0.0558 | 378.8 J | 15.89 | 0.105 ms | 100% |
| single long2 | 2.843 s | 1.47% | 1055.33 | 4.77% | 0.0558 | 383.5 J | 14.47 | 0.106 ms | 100% |
| single long3 | 2.926 s | 1.70% | 1025.56 | 3.87% | 0.0542 | 389.0 J | 15.53 | 0.101 ms | 100% |
| four-job frozen static | 5.635 s | 0.29% | 1952.14 | 10.48% | 0.1032 | 999.9 J | 21.82 | 0.159 ms | 100% |
| four-job observe-only proposed-role | 5.691 s | 2.88% | 1934.10 | 12.32% | 0.1022 | 1015.8 J | 22.36 | 0.159 ms | 100% |

MFU 为 `[0,1]` 分数，基于 CLIP 模型估算 FLOPs、group JCT 与 2×4090 峰值计算；不解释为
硬件计数器实测利用率。完整紧凑表见
[`data/group_summary.csv`](data/group_summary.csv)。

### 5.2 four-job 逐 Job JCT（formal mean，n=3）

| Job | frozen static | observe-only proposed-role | 相对 static |
|---|---:|---:|---:|
| short | 4.189 s | 2.756 s | −34.21% |
| long1 | 5.083 s | 3.174 s | −37.55% |
| long2 | 4.969 s | 4.345 s | −12.56% |
| long3 | 5.090 s | 5.184 s | +1.84% |

proposed-role 改变了逐 Job 完成顺序，但 aggregate group JCT 没有跨过 5% 门槛，并伴随收益
不均。该表描述既有 coordinator 与 static 的策略差异，不是 snapshot 驱动效果。逐 Job
queue/preprocess 分位与 exactly-once 见 [`data/job_summary.csv`](data/job_summary.csv)。

质量门覆盖 source identity、encoded-byte digest、embedding 输出合同和 exactly-once；本矩阵
无 pgvector sink/检索闭环，recall@k、nDCG@10、写回时间和 `$/M images` 为 N/A。

## 6. 结果解释与课题含义

**事实**：staged descriptor 和 observe-only runtime state 已进入 GPU-backed 正式 Project
链路；signature 单一、freshness 100%、构建开销约百微秒，结果语义完整。static 与
proposed-role group JCT 近似中性，逐 Job 服务份额却明显不同。

**推断**：现有 trace 足以低成本暴露 prepare/model 两阶段 active/capacity work，为下一步
做“状态是否能改善单一控制动作”的因果实验提供了可信观测面。逐 Job 差异也说明只看总
images/s/group JCT 会掩盖隔离与公平权衡。

**不能声称**：不能说 observe-only snapshot 提升性能；不能说 proposed 全面优于 static；
不能从本 CPU-bound 图像合同推出 GPU 饱和场景结论；不能称它为图像 VTC 官方 benchmark。

## 7. 下一步

1. 冻结本 manifest、descriptor signature 和同上限 static，在 `BoundedStageWorkController`
   只启用一个动作，独立验证 missing/stale fallback 与 5% 晋升门槛。
2. VTC-compatible 文本多 Job 矩阵继续串行运行，用 actual token-work、normalized service、
   backlog disparity 和 idle borrowing 补充公平性外部合同；不与本图像绝对性能交叉排名。
3. 若 stage-aware 单动作未跨门槛，保留 observe-only telemetry 作为共同使能组件，不把控制
   复杂度晋级为主方法。
