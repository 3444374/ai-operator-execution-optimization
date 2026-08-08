# 文本多 Job 前台干扰与共享调度权衡（2026-08-09）

## 1. 实验目的

本实验只回答一个开题动机问题：当一个 long Job 在 short Job 已经运行后到达，现有
执行路径会怎样影响前台 short Job；项目的固定分区和共享 work-credit 又呈现什么
效率、隔离与公平权衡。

这不是完整系统排名，也不验证图像、多模型、weighted fairness 或最终 state-aware
控制器。原 15 s offset 下 Daft Native 的 short 在 long 到达前结束，因而只保留为
arrival observation；本报告以所有系统共同使用的 5 s guaranteed-overlap 补充矩阵为准。

## 2. 实验设置

- 硬件与服务：2×RTX 4090，两个独立 vLLM endpoint，Qwen2.5-7B-Instruct，
  `max_num_batched_tokens=8192`、`max_num_seqs=256`、prefix cache on、MFU metrics on。
- workload：两个互斥 ShareGPT manifest，各 512 行；short SHA256 为
  `85b3f90c...c971`，long SHA256 为 `8e532819...e9c1`。short 在 0 s 启动，long 在
  5 s 启动。
- 重复：每个场景 1 warm-up + 3 formal，正式顺序由 seeded runner 交错。
- 项目路径：PostgreSQL/Daft source、token-budget 6144、request-level replay、两个
  endpoint；全局上限 K128/W65,536 per endpoint。比较 `static_partition` 与
  endpoint-shared DRR/work credit。
- 项目匹配控制：single-short full pool，以及只启用一个 short、但预留两个静态分区的
  half pool。后者让 short 获得 K64/W32,768，却不启动 synthetic competing Job。
- 原生路径：Daft `functions.prompt` Native、Daft `functions.prompt` Ray runner、Ray Data
  HTTP official graph。每个 Job 独立启动两个官方 endpoint shard；不注入项目 credit、
  router、actor pool 或调度器。
- 边界：`writeback-mode=none`。该实验测 serving-side multi-job interference，不承担
  database-E2E sink 排名；项目 request trace 可报告 short P99/work rate，原生 adapter
  未采集 request P95/P99，不能由 Job barrier JCT 伪造。

## 3. 严谨性自检

- 项目 5 s 矩阵：8/8 group、6/6 formal，manifest status completed；资源、MFU、
  exactly-once、零 worker failure/incident 和 manifest 隔离门全部通过。
- 原生 5 s 矩阵：12/12 cells、9/9 formal、0 error；exactly-once、provenance、服务
  counter 和两个 endpoint 使用门通过。
- 统一汇总：30 formal rows、10 summary rows、6 comparisons、18 project phase rows，
  short manifest 身份一致；所有 two-job arms 的实际 overlap 都大于 0。
- formal 稳定性：项目 service tok/s CV 为 0.065%（static）和 0.228%（shared）；原生
  two-job service tok/s CV 为 0.59%（Daft Native）、1.18%（Daft Ray）、0.60%
  （Ray Data）。
- 两次无效启动均单独保留：v1 在创建 cell 前因 offset 环境变量未展开而失败；v2 在
  static warm-up 后因一次 `httpx.ReadError` fail closed，不进入正式均值。endpoint 随后
  健康、服务日志无 5xx/CUDA/engine crash；残留的具名 shared-credit actor 已精确清理，
  异常路径 cleanup 已加入回归测试。

## 4. 实验数据

### 4.1 项目：先隔离 quota，再测 long 竞争

| 对比 | short JCT | short P99 | short work rate | 实际 overlap |
|---|---:|---:|---:|---:|
| full-pool single → half-pool single | −0.003% | −0.013% | −0.004% | 0 s |
| matched half single → static + long | **+3.79%** | **+90.80%** | **−3.57%** | 68.94 s |
| matched full single → shared + long | **+8.95%** | **+173.33%** | **−8.28%** | 72.62 s |

full/half single-short 的 JCT 均约 71.24 s，说明 K/W 减半在这个 short workload 上不是
退化来源；long 真正加入后，short 的尾延迟和完成 work rate 才发生变化。

### 4.2 项目：共享额度提高总效率，但没有免费隔离

| 指标 | static partition | shared work credit | shared 相对 static |
|---|---:|---:|---:|
| group service tokens/s | 7,028.59 | 8,506.81 | **+21.03%** |
| long JCT | 117.19 s | 95.73 s | **−18.31%** |
| short JCT | 73.94 s | 77.62 s | **+4.98%** |
| max request P99 | 50.89 s | 31.73 s | **−37.66%** |
| MFU | 26.50% | 32.01% | +5.51 pp |
| Jain fairness（median） | 0.759 | 0.707 | −0.052 |

5 s 前的 pre-long 窗口中，两臂都只有约 6.3 running requests，随后 overlap 窗口提高到
约 67.6（static）和 86.7（shared）。shared 把更多全局 work 投入服务，因此提高总吞吐
并缩短 long drain；与此同时 short JCT/work rate 和 Jain fairness 变差。动态/共享
调度的目标必须显式包含前台隔离和公平，而不能只优化 aggregate throughput。

### 4.3 原生框架：同一系统内的 single → overlap 观察

| 系统 | single short JCT | short+long short JCT | short JCT 变化 | overlap | two-job MFU | two-job waiting mean |
|---|---:|---:|---:|---:|---:|---:|
| Daft Native | 11.06 s | 20.17 s | **+82.42%** | 15.17 s | 58.72% | 167.18 |
| Daft Ray | 14.74 s | 30.19 s | **+104.84%** | 25.19 s | 50.85% | 147.16 |
| Ray Data HTTP | 128.91 s | 171.14 s | **+32.76%** | 166.14 s | 17.67% | 0.00 |

三条原生路径都发生真实重叠，且后到 long 与 short 的 JCT 退化同时出现。Daft 两臂表现为
高 running/high waiting/KV 接近满，Ray Data 表现为 low running/no waiting/低 MFU；
因此仅用“Job 数”或 GPU utilization 不能描述共享服务压力，需要联合 work、完成速率、
running/waiting、KV、MFU 和 tail 状态。

原生对比标记为 `observational:overlap_present`：它描述两个独立官方应用竞争同一 vLLM
服务后的外部现象，不把框架内部算法或项目未控制的提交语义作因果归因。

## 5. 结果解释与开题对应

### 事实

1. 没有 overlap 的 15 s Daft Native 结果不能证明前台干扰；统一 5 s 后三条原生路径与
   两条项目策略都发生真实 overlap。
2. 单 short 的 half quota 几乎不影响 JCT/P99/work rate；long 加入后才产生明显退化。
3. 项目 shared work credit 提升总吞吐并缩短 long JCT，但 short JCT/work rate 与 Jain
   fairness 变差，存在可重复的效率—隔离权衡。
4. 现有原生路径在相同任务下落入 overqueue 或 underfeed 等不同状态形态，且 short
   都受到后到 long 的影响。

### 对设计的支撑

- **Work Unit / WorkDescriptor**：quota 和竞争应按 prompt/output work，而不是只按 Job
  数或行数计量；还需携带 job、stage、deadline/SLO、uncertainty 与 locality 元数据。
- **感知**：至少观测 per-job remaining/completed work、arrival/active/drain 状态，以及
  endpoint completion rate、running/waiting、KV、MFU 和 tail。
- **动态调度**：需要 work-conserving idle borrowing，但必须同时设置 per-job floor/cap、
  fairness 或 SLO guard；本实验说明“共享更多”不是完整策略。
- **算子代价估计**：为 remaining work、service time、SLO slack 和 credit 大小提供共同
  输入；本实验本身不验证估计精度，精度与 selection regret 仍由 cost-profile 实验承担。

### 不能声称

- 不能说 shared/dynamic 全面优于 static；short isolation 和公平指标明确回退。
- 不能从原生 JCT 变化归因 Daft/Ray Data 内部调度算法，也不能称项目已优于三个框架。
- 不能把原生 short cell 当作 ≥60 s 稳态容量排名，不能伪造原生 request P99。
- 不能外推到 4+ Job、weighted/SLO、图像、音频、视频或故障恢复。
- 不能把本实验当 database-E2E sink 结果；它有意使用 no-writeback 来隔离 serving 竞争。

## 6. 待画图清单（本轮不画）

1. **前台干扰主图**：每个系统一组 `single short` 与 `short+long` 的 short JCT 点/误差线，
   同时标注实际 overlap；只画 within-system normalized delta，不混排原生 request P99。
2. **项目因果分解图**：full single、half single、static+long、shared+long 四点，分别显示
   short JCT、P99 和 work rate；突出“quota-only≈0，long competition>0”。
3. **效率—隔离权衡图**：static/shared 的 aggregate tok/s、long JCT、short JCT、Jain
   fairness 四个 aligned small multiples；避免双 y 轴和无解释散点。
4. **状态时间线图**：0–5 s pre-long、overlap、long-drain 三段，画 running/work rate/GPU
   util；MFU 只报 group aggregate，因为没有 interval FLOPs counter。
5. **原生状态指纹图**：Daft Native、Daft Ray、Ray Data 的 running、waiting、KV、MFU
   小倍图，用来解释相同“两个 Job”为什么处于不同服务压力形态。

这五项可压缩成开题正文两张组合图：一张回答“后到 Job 是否伤害前台”，一张回答
“为什么需要 work-aware、state-aware 的多 Job 调度”。

## 7. 数据与服务器归档

Git 只保存紧凑审计数据：

- `data/combined/`：统一 30-row formal、10-row summary、6 个对比和三阶段数据；
- `data/project/`：项目 5 s static/shared 的逐次、汇总、pairwise 与 audit；
- `data/native/`：三条原生路径的逐次、汇总与 audit。

服务器保留全部 manifest、commands、per-job requests/submissions/resources/credits、原生
shard log、GPU/service time series 和失败 incident：

| 归档 | SHA256 |
|---|---|
| `opening_multijob_forced_overlap_20260809_v3.tar.gz` | `f766faf7f91fb3a30a6dde8ab1b79c6cc02bae4678a5454bc4e533abae814cfa` |
| `opening_text_native_multijob_forced_overlap_20260809_v1.tar.gz` | `515b33a5a07e77c39131e02ba1ee8fcb1ff3c000b4f2a582cd117c6b5ca095a7` |
| `opening_short_job_interference_forced_overlap_20260809_v1.tar.gz` | `b7aa4c8b6cd728285fa3929acdec0a03ac5052492ef7f1dc99bf8419ea617e6d` |
| config-load failed v1 | `fbe52e3a53a76d0660b23253e6295d78f3d4dda64814ff5a06260122cf096c8e` |
| transient-ReadError failed v2 | `6c2bc324accfa92efbf7f1d2a7a25fee480a50e1ce773bca06da1380890ef77a` |

服务器原始目录不删除、不覆盖：三份全量目录位于
`/root/autodl-tmp/experiment-artifacts/<run_name>/`（当前约 11 MiB、16 MiB、36 KiB），
压缩包位于 `/root/autodl-tmp/experiment-artifacts/archives/`。2026-08-09 回读复核时三份
压缩包 SHA256 与上表完全一致；截至归档后仍约有 23 GiB 可用空间。
