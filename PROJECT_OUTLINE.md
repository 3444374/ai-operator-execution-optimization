# 项目大纲

本文件是根目录下的项目总纲入口，用于快速了解当前方向、实验主线和近期调整点。详细材料仍以各目录 README、结果报告和开题报告为准。

## 当前题目与方向

开题报告当前收敛后的正式题目：

> 数据库 AI 负载的执行优化与调度研究方向。

**2026-07-16 方向重大更新**：主场景从 AI_EMBED 转向 AI_COMPLETE（生成式 LLM 推理），上游 batching 从静态固定 batch_size 转向探索按 token 量动态组织的方式，Ray 从 task executor 升级为架构设计空间（异构 actor pool + 去中心化自适应提交）。vLLM 定位为部署平台。Daft 确认为数据引擎，文本阶段直接接入，多模态阶段复用同一套 pipeline。详见 `PROJECT_LOG.md` 2026-07-16 条目。

**2026-07-29 文献基线升级**：多模态仍是正文泛化验证。算子代价估计从“补充讨论”提升为数据组织和调度提交控制共同依赖的重要组件，但不单独扩张成第三项研究内容。首版采用简单解析模型 + profile 校准 + residual correction，用于 work/service/JCT、active-work/K、组织、路由和多 job remaining-work/SLO 判断。

**2026-08-01 image-first 执行方向锁定**：项目内部已锁定 **A（模型服务状态感知的请求成形/提交）+ B（算子代价估计）**，首个 workload 为 image AI_EMBED（CLIP）；文本 vLLM 遗留实验转为 `parked-conditional`，不是废弃。外部题目是否采用“DB↔GPU 经 Daft 桥接”的 scope reframe 仍待导师/学长确认，这与内部实施顺序分开记录。CLIP 5K 初始 slow-pt 画像和当前实现边界复测均通过门禁：production-np 的 CPU prepare 约 5.2–5.6ms/image；更强 torchvision tensor-decode 虽带来约 1.14–1.22× 串行 profile 加速，CPU prepare 仍为 actor 的 13.8–31.2×。当前进入 path-B E2E runner 与强 baseline 建设，见 `experiments/plans/experiment_status_and_gaps.md` §0、`experiments/plans/image_clip_workload_lock_20260731.md` 和 `motivation/results/gpu/image_clip_preprocess_variants_20260801/`。

当前重点不是传统数据库 GPU 查询算子，也不是模型 kernel 优化。数据库 AI 算子在本文中作为 workload 入口，研究重点是上游 Ray 数据执行层的调度优化——探索数据组织策略和提交控制策略，利用 Ray actor 实现去中心化自适应提交。Daft 作为数据引擎，提供 Rust 执行内核、Arrow 零拷贝、Morsel 流式背压和 `@daft.cls` GPU UDF 接口。

## 研究内容

当前开题报告采用两项策略设计 + 多模态泛化验证 + 算子代价估计（共同使能组件）：

1. **AI workload 感知的动态数据组织与批处理构造策略**（研究内容一）：对比 token-budget batching 与固定 batch_size 在端到端吞吐和 P99 延迟上的差异，以及 length-aligned/prefix-aware 分组与随机分组的效果差异。利用 Ray actor 异构化实现。引擎级参数（Daft `into_batches`、`batch_size`、`repartition`）与策略级决策共同构成优化空间。
2. **运行层调度与提交控制策略**（研究内容二）：利用 Ray actor 的 stateful + async 能力，研究固定资源下的最小饱和 active work、request-level replenishment、endpoint-shared request/work credit、work-conserving idle borrowing 和多 job fair queue。固定静态 credit 是强 baseline；动态候选只有显著优于同上限静态策略才晋级。
3. **多模态泛化验证**（正文实验，§5.3）：在图像 workload 上使用同一套策略代码和配置逻辑，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。
4. **算子代价估计**（共同使能组件，不作为独立研究内容）：预测 prompt/output work、operator service time、JCT、remaining work 和 SLO slack；初始化不同 GPU/模型/workload 的 active-work/K，并辅助数据组织、endpoint 路由和提交策略。除误差外报告配置 ranking、决策 regret 和预测区间。

两项方法下收敛为三个研究问题：

1. 固定硬件、模型和 workload 下，能否用更小 active work 更快达到 serving ceiling？
2. 相同 token/frame work 下，数据组织如何影响 JCT、尾延迟和 cache/active-work 波动？
3. 多 job 共享同一 vLLM 时，shared credit、idle borrowing 和 fair queue 如何平衡吞吐、JCT、SLO 与公平性？

写回使用 PostgreSQL + pgvector（COPY + deferred index baseline），不作为独立研究内容，仅在实验设置中说明。

**主场景：AI_COMPLETE**（生成式 LLM 推理，文本）+ **AI_EMBED/AI_CLASSIFY**（图像，多模态泛化验证）。AI_EMBED 文本预研已完成（真实 GPU-backed 链路）。

阶段划分、执行画像和瓶颈归因不是独立研究内容，而是动机测试、方案设计和评价依据。

## 实验主线

当前实验主线优先从 `motivation/` 进入：

| 文件 | 用途 |
|---|---|
| `motivation/README.md` | 动机测试目录总入口 |
| `motivation/plans/workloads.md` | 三类 AI 算子场景、动机测试和后续实验优先级 |
| `motivation/plans/integration.md` | PostgreSQL / 外部 worker / Ray / GPU model service / writeback 集成路线 |
| `motivation/plans/image_host_data_path_bottleneck.md` | 图像 R0→R4 容量/表示阶梯与 CPU/Ray/PCIe/GPU 瓶颈判决 |
| `motivation/results/README.md` | 动机测试结果阅读顺序和结论边界 |
| `motivation/results/gpu/README.md` | 真实 GPU-backed E2E 结果入口 |

`feasibility/` 当前只作为组件、环境、脚本可用性的验证入口。

## 实验结论写作标准

所有实验结论、实验数据分析、开题可行性分析和飞书实验摘要，都参考 `learning/AGENTS.md` 的讲解标准组织。写实验结论时至少说明：

1. 为什么做这个实验，验证哪个问题。
2. 实验链路是什么，数据从哪里来，经过哪些系统或进程，写回哪里。
3. 参数和指标分别代表什么，例如 rows、batch、task/actor、ObjectRef、queue wait、bounded wait、fan-in、writeback。
4. 真实数据结果是什么，数字来自哪个 CSV 或报告。
5. 结果能说明什么，不能说明什么。
6. 结论属于本地实验事实、模拟实验事实、合理推断、待确认问题，还是不能声称的内容。
7. 下一步实验或消融如何验证当前解释。

正式报告可以比学习材料更凝练，但分析精细程度不能低于上述标准。

## 当前最重要证据

正式论证优先引用：

1. `experiments/results/local_vllm_qwen15b_baseline/README.md`
   - **2026-07-18/19 本地 vLLM + Qwen2.5-1.5B AI_COMPLETE baseline 全系列**。
   - Token-tail revision：固定行 batch=8 时 token 跨度 13.9×，batch=128 时 token P95=26678——证明固定行数是计算量的弱代理。
   - Token-budget vs Fixed Row：token_budget=6144/8192 约束 token P95 至 ~6141/8171（vs fixed 64/128 的 16377/26677），吞吐接近。
   - Shared-vLLM K_max 干扰（07-26 复验）：static K=8 相对 K=16 将前台 E2E 降低 27.9%、P99 降低 40.0%（后台吞吐代价 27.9%）——证明 K_max 在共享 vLLM 下是必要的 guardrail。AIMD 三轮 0 次 decrease（窗口均值 15.953），与 static K=16 不可分辨。根因：vLLM waiting=0 但前台已慢 38.9%——AIMD 盯 vLLM waiting 做决策，但请求在 Ray 侧排队、waiting 始终为 0。
   - Queue-adaptive flush 已完成自然 EOS 三组随机化复验：fixed-50 与
     adaptive 相对 fixed-25 tokens/s 分别 `+32.23% ± 3.90%` 与
     `+32.09% ± 6.22%`；adaptive 相对 fixed-50
     `-0.10% ± 4.13%`。固定 16-token cap 的候选重复同样未显示 adaptive
     增量，因此当前证据只支持更长 coalescing window，不支持动态策略优于
     最佳静态窗口。详见
     `experiments/results/adaptive_flush_randomized_20260726/`。
   - AIMD、EWMA-AIMD、PID 已完成单作业 512 请求真实矩阵。三者相对
     static K=8 的 E2E 降低约 30–32%，但平均窗口均接近 16；追加的随机交错
     AIMD vs static K=16 对照显示 E2E +0.66%、tokens/s -0.69%，没有动态
     反馈增量。shared-vLLM 128/512 双作业复验进一步显示 AIMD 0 次 decrease、
     平均窗口 15.953；相对 static K16 前台 E2E +1.22%、P99 +1.98%、后台
     tokens/s -1.45%。追加 adaptive flush 后同样无稳定增量。
   - Output-aware deterministic BFD 已完成真实单 GPU 64→512→1024 分级验证。
     512 行 trace-metadata 成本模式相对同成本 sequential 吞吐 +12.019%，
     但 1024 行反转为 -5.156%，并产生更多 submission、较高能耗和较低 MFU。
     因此经典 BFD 仅保留为条件性候选；下一版必须联合搜索 row cap、token
     budget 与 packing objective，不能把 512 单点写成普遍收益。
   - **2026-07-31 RC1 数据组织系统重测（干净平台，取代 07-25/26 gropy；07-18/19 保留作历史动机参照）**：
     `experiments/results/rc1_data_organization/README.md`。5 策略（fixed_rows/sequential token_budget/
     length_align/best_fit BFD/row_cap_aware）× {2-ep/0.9, 4-ep/0.43}，1.5B，sharegpt_multiturn，cache-ON，
     含 P0 指标（prefix_cache_hit_rate/TTFT）。**regime-dependent 闭合**：2-ep/0.9（每 endpoint KV 池占 GPU 0.9、无压力 max 7–10%）
     5 策略 E2E 50–56k 紧凑、fixed≈seq>bestfit>rowcap>lenalign；4-ep/0.43（2 endpoint/GPU 各占 0.43、KV max 98–100% 饱和）分化 39–50k、
     **排名反转为 seq>fixed>>rowcap≈bestfit>lenalign**。机制（`prefix_group_ratio`）：重排序类 organizer 打散
     prefix 组（ratio 0.03）→ 4-ep 命中从 0.60–0.76 崩到 0.06–0.07 → TTFT 翻倍、best_fit/row_cap SLO 60%；
     保序类 ratio 0.13–0.29、命中 0.47–0.48。consolidation 是惩罚（4-ep −10～−26% + 能耗 +40%）。**与 #28 routing /
     KV-sweep 闭环：上游策略价值在 4-ep 饱和 regime 才显现，2-ep 是干净对照基线。** ✅ feeding 门禁已补
     （batched bounded，gate 放宽 ≥2 endpoint）：2-ep 真上限 79,488（策略 63–71%，缺口=准入节流非饿死）；
     4-ep bounded 24,733 病态（策略超过）→ 准入控制是吞吐杠杆、效应随 regime 反向（2-ep 可放开 W 提速、4-ep 防 thrash 应保留）。
   - 边界：本地 rehearsal，不代表 PG18.3 内部平台结果。
   - 状态与缺口审计：`experiments/plans/experiment_status_and_gaps.md`。
2. `motivation/results/gpu/image_clip_native_baseline_20260801/README.md`
   - **图像 CLIP operator-E2E 项目自写 Daft UDF 诊断（历史 image-first 动机入口）**。
   - 先校准 Daft fractional-GPU actor shape，再做 COCO 5000 图×3 formal：
     project-Ray 相对项目自写单卡 `daft_native` UDF 1.296×，相对项目自写双卡
     `daft_ray` UDF 1.138×。
   - 结论只支持静态阶段拆分在同物理机器上优于独立校准的项目自写 fused UDF；尚不包含
     Daft-on-Ray/Ray Data staged、pgvector sink、bounded direct ceiling，也不是动态
     状态感知策略收益。
3. `motivation/results/gpu/image_clip_bottleneck_profile_20260801.md`
   - **COCO val 5K × 100 iterations 的 image-CLIP fatal-flaw 门禁**。
   - 实用 batch（≥16）的 CPU 准备/GPU embed 比为 `13.8–18.3`，B=128 串行下 GPU 约 95% 时间等待上游。
   - production-np 总 CPU prepare 约 5.2–5.6ms/image；torchvision tensor-decode
     降至约 4.4–4.8ms，但仍显著重于 GPU actor。历史 method wrapper 只直接归因出
     resize 约 1.3ms，其余 processor 时间仍未细分。
   - 这是 motivation/profile 的 GO 证据，不是 path-B 策略胜过 Daft 原生实现的结果。
4. `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`
   - 真实 GPU-backed embedding 链路拆分（AI_EMBED 预研，已完成）。
   - 1024 行下 fine / coalesced 端到端约 `13.4x`。
   - 16384 行下 operator 和 writeback 均为大块成本。
5. `motivation/results/gpu/multi_endpoint_ray_motivation_20260712.md`
   - 双 endpoint 下 Ray task / actor 开始体现并发 routing 价值。
   - 端到端收益仍受 writeback 约束。

## 近期优先级

图像轨道在继续策略实验前，先按
`motivation/plans/image_host_data_path_bottleneck.md` 完成 R0→R4 表示阶梯与
schema v2 复测。R0–R2 已把 pageable FP32 ownership copy/dtype conversion 与 pinned H2D
分开；Ray Data 60K×2 原生长稳态复核冻结 `batch64/cpu8/gpu2/source4`（957.100 img/s，
2 repeats，CV 0.347%），batch512 相对慢 7.719%，不继续扫 1024。该结果支持 CPU
preprocess/喂入是当前候选限制，但不把 `nvidia-smi` util 写成 MFU 或硬件因果证明。
Daft built-in 与 Ray Data 均已独立校准，schema v11 计时内 normalized contract parity
已通过；下一步是同 60K×2、1+3 的原生 baseline/project 正式排名。项目自写 Daft
staged/fused 仅保留为诊断 reference；官方 ResNet18 已固定 upstream commit/SHA，但
AutoDL 仍需通过 exact S3 workload 带宽、Ray 2.49.2 隔离环境和 adapter diff 门禁。

**已完成**：
- ✅ vLLM + Qwen2.5-1.5B baseline 建立（07-18）
- ✅ Daft 文本阶段直接接入，`PostgreSQL → DaftPostgresSource → DaftOrganizer → Ray → vLLM` 链路跑通
- ✅ 固定行 batch token-tail revision（动机证据：行数是计算量的弱代理）
- ✅ Token-budget vs Fixed Row 对照（策略信号：token-budget 约束 token tail）
- ✅ Shared-vLLM 2-job K_max 干扰实验（动机证据：K_max 在共享 vLLM 下必要）
- ✅ Queue-adaptive flush 双窗口修正通过单 GPU 64/1024 门禁与 512 行重复筛选
- ✅ Length-align + Prefix-aware 初步 ablation
- ✅ Output-aware cost + deterministic BFD 基础设施、GPU/功耗/能耗/MFU 指标与
  512/1024 规模边界验证
- ✅ Row-cap-first 机制级消融与 prefix-cache-corrected 512/1024 验证：
  1024 行吞吐约 +1%，但 10 秒 SLO violation 从 50.39% 升到 88.67%，
  因此 sequential token-budget 保持默认
- ✅ 实验运行器支持可审计 resume、失败场景剪枝和 service metadata
  一致性校验
- ✅ vLLM 逐 choice token IDs / finish reason 观测、ChatML prompt envelope
  与上下文安全过滤
- ✅ Batching × submission 18 单元筛选与 4 候选重复：SLO-constrained
  联合候选相对独立拼接 `-0.26% ± 2.07%`，当前采用分层优化
- ✅ 跨 arrival-rate 真实筛选：约 51.4/25.7/12.85 req/s 三档均未出现
  fixed-25 反超；adaptive 相对 fixed-50 无增量，当前默认 fixed 50ms
- ✅ 2048 请求自然 EOS 留出：4096/4096 对照请求 exactly-once；fixed-50
  相对 adaptive tokens/s +1.75%、request P99 -2.61%
- ✅ Prefix 受控 workload 0/30/70/100% 与真实 vLLM 筛选；修复唯一 prefix
  哈希重排和隐式 length-align 耦合。prefix cache 关闭时无稳定收益
- ✅ 算子 E2E 代价估计审计：旧 283-row 结果混入 warmup，已归档；当前 formal-only
  23-feature context-LOO 使用 204 条 formal、17 contexts。CE5 MAE 7.91s、candidate
  pairwise 0.800、macro/pooled/max regret 4.58%/0.62%/26.23%，row pairwise 0.684
  未过既有门槛，仍不晋级。双 4090 4-cell pilot 8/8 通过；首次独立 20-context
  formal 因两个 runner 几乎全程重叠且 640/640 子运行启动 local Ray 而整体无效。
  已增加 host-scope lease 和空 Ray 地址门禁；重跑主合同改为实际部署的 cache-on，
  cache 状态写入 CSV/context，最小共享-Ray gate 通过前不重跑 formal
- ✅ vLLM eager vs CUDA Graph 同配置对照：512 请求、每侧 3 次 formal；
  CUDA Graph 的 E2E 均值 79.85s（-71.76%）、observed tokens/s
  2875.68（+254.05%）、MFU 14.51%（eager 4.02%）。该结果用于选定后续
  本地 steady-state baseline，不作为上游调度贡献
- ✅ AIMD/EWMA-AIMD/PID 单作业 GPU 矩阵与 static K=16 机制对照：
  控制器相对 K=8 的收益来自升至 K≈16；AIMD 未优于同上限静态策略
- ✅ Shared-vLLM 128/512 前台/后台 K8/K16/AIMD 三次重复与 adaptive-flush
  补充：K8 保护前台；AIMD 饱和至 K16 且没有 decrease；adaptive flush
  约 89.4% 决策选择 50ms，当前默认保持 static K8 + fixed 50ms
- ✅ 双 4090 request-level replenishment 三次重复：global K32 与
  per-endpoint K16 等价；等名义 offered work 的 request K48 与 batch K16
  吞吐持平。request K64 为最高已测吞吐点，但同时增加约 33% offered work
  且 P99 更差，不能归因为补位机制胜出或称为容量最优
- ✅ 双 4090 request-level active-work 八档扩展曲线：32/32 run 成功，
  65K 达到最大均值 97.80% 且下一档只增 0.92%；98K→131K 吞吐持平，
  P99 约 40s。按预注册规则选择 65,536，后续策略不再靠继续增大 offered
  work 获取表面收益
- ✅ 双 4090 固定资源 Actor Pool 形状对照：12/12 run 成功；固定
  65,536 work、256 slots 和 0.5 Ray CPU/endpoint 后，2×128/4×64 相对
  1×256 吞吐仅 +2.00%/+0.75%，MFU 与尾延迟基本重合，未达到 5% 晋升
  门槛；当前保留 1×256
- ✅ 双 4090 complete-row service quantum：24/24 run 成功；固定饱和
  work 后，512/1024/2048/4096 quantum 相对 batch 吞吐变化仅
  -0.03%/+0.11%/+0.12%/+0.54%，request 为 +1.75%。512/request 把
  credit-held 降约 16%，但未提高稳态 GPU 吞吐，固定 quantum 不晋升
- ✅ 双 4090 SLO-aware EWMA flush：24/24 run 成功；相对 fixed-50，
  high/arrival-limited 的吞吐为 -0.52%/+0.10%，P99 为 -0.94%/-0.49%，
  所有 30s SLO 均零违约。25–50ms 动作相对 5.6–17.4s P99 缺少一阶杠杆，
  SLO-EWMA 不晋升
- ✅ 双 4090 Shared-vLLM 1/2/4-job：36/36 group run、0 incident，
  endpoint-shared request/work credit 全程不越界并最终归零。1-job 协调
  开销 -0.02%，2-job shared 与 independent 不可分辨；4-job shared
  聚合吞吐 +9.57%、max P99 -22.52%、max JCT -15.89%，Jain median
  0.9961。4-job 三次吞吐变化为 +8.43%/-0.28%/+22.60%，因此仅记为
  高竞争条件性候选，需 held-out 复验
- ✅ service ceiling / direct-control C32/C64/C128/C256 校准：vLLM Bench total
  tokens/s 4,930→8,342→12,762→15,351；512 行 C256 的 bounded HTTP 为
  14,532 tokens/s。C128→C256 仍增长 24.3%/33.0%，因此 C256 只称当前
  `max_num_seqs=256` 配置硬上限；历史约
  8.0–8.2K 只属于当时 project profiler/arrival-replay 链路，不能再称为
  vLLM 或双 4090 物理上限。bounded C128 暴露 httpx 默认 100 连接上限，
  显式扩展连接池后 re-gate 达到 12,472 tokens/s，与 vLLM Bench 仅差 2.3%
- ✅ 文本 baseline 原生性审计与复测合同：vLLM Bench 固定为 service ceiling，
  bounded Chat/Completions 固定为项目 direct controls；Daft built-in prompt、Ray Data
  Processor 和通过 capability gate 的 OceanBase 才进入 native baseline。代码已增加
  provenance fail-closed、服务端统一 token throughput，并删除未接线的 Daft
  `partition_count` 扫描；native/project 同一 2,048-row held-out formal 合同已冻结，
  但统一 calibration/formal matrix runner 尚未实现，当前不得手工拼接执行
- ✅ baseline 文档入口收敛：`experiments/plans/baseline_reference.md` 统一维护三类算子的
  分层、原生性、证据等级、指标与门禁；旧 2026-07-29 文本矩阵已移入 archive，不再
  作为当前执行依据

**当前缺口与顺序（以 `experiments/plans/experiment_status_and_gaps.md` §0 为准）**：

1. **Image fused operator-E2E gate**：中性 work-unit、lazy source、fast processor、
   项目自写 fused Daft Native/Ray diagnostic 与 bounded `PG→Daft→Ray CPU preprocess→Ray CLIP GPU actor`
   runner 和 5K×3 formal 已完成。
2. **补 system E2E 与完整强 baseline**：Daft built-in 的 256 图逐行 schema v11 计时内
   normalized-contract parity 已通过（cosine P1=0.999800、非自身 overlap@10 mean=0.9949），正式比较采用统一
   normalized AI_EMBED contract 并把归一化成本计入各臂 E2E；继续补官方 ResNet18 parity
   和 Ray Data native graph runner/256 行门禁、独立校准及 60K×2 batch 上界复核均已完成；
   当前冻结 Ray Data `batch64/cpu8/gpu2/source4`，下一步做统一规模 1+3 正式重复，再给
   系统臂接同一 pgvector sink；同时补 bounded direct CLIP、naive 和 ours。vLLM
   pooling 当前环境的两次 1-image offline gate 均 600s 超时且无 embedding，只保留为
   blocked 的 direct-service ceiling 候选，不继续在线/5K/60K。OceanBase AI_EMBED
   等待可部署环境。当前 5K 结果只
   证明相对 fused UDF 的优化空间，尚未证明 ours 优于主流 staged pipeline。
3. **A+B 方法验证**：A 读取 CLIP endpoint queue/active-work 做状态感知请求成形；
   B 先实现 `<100 LOC` 解析代价模型 + profile 校准 + residual correction，并报告
   ranking、regret 和预测区间，不直接扩张成复杂 learned optimizer。
4. **正式指标闭环**：除吞吐/JCT/P95/P99/SLO 外，报告 CPU preprocess、H2D、
   GPU embed、overlap、GPU busy、能耗与写回后的 Recall@10；策略晋级需相对各自
   强静态 baseline 改善约 5%、重复方向一致且质量不退化。
5. **文本遗留项统一 parked-conditional**：feeding 等价性、short/long static-credit、
   4-job held-out、prefix routing 隔离和文本系统 baseline 不再阻塞 image build；仅在
   论文收录文本结果时按已有实验合同恢复。matched-KV 证据目前指向 endpoint
   consolidation，而不是单纯 per-endpoint KV 大小，但仍未完全隔离饱和深度。
6. 后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 AutoDL rehearsal 写成
   正式内部平台结论；最终 scope/题目 framing 仍需导师/学长确认。

**多机器执行合同**：代码与实验配置不再以 AutoDL 固定路径作为通用接口。
`deploy/runtime/` 统一描述 machine profile、Python 能力组和模型/数据资产；每台机器
保存自己的仓库外 runtime env，preflight 自动识别已知双 4090/单 5070，并为其他 Linux
NVIDIA GPU 选择保守通用 profile。单 5070 可复用执行与指标合同，但 actor、batch、
active work、显存和服务容量按“机器 + 模型/服务 + 协议 + workload”签名独立校准；
签名不变才复用冻结选择，不能继承双 4090 最优点。

文献机制的发现、迁移审计和晋级/放弃条件统一见
`experiments/plans/literature_driven_pipeline_optimization_guide.md`。

**指标状态**：
- 新实验已经系统采集 `tokens/s`、request P50/P95/P99、SLO
  violation/goodput、GPU/功耗/能耗、vLLM pressure、FLOP/MFU；
- typed adaptive 已有 inflight/queue/control trace 与 sample age；
- 旧实验历史数据仍存在指标缺口，不能与新口径直接拼接；
- vLLM 已通过每个 choice 的 token IDs 提供真实 per-request output tokens 与
  finish reason；generic compatible endpoint 缺少该能力时字段保持为空。

写回使用 PostgreSQL + pgvector（COPY + deferred index baseline），不作为独立实验阶段。

**Scope 缩减触发条件**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion（✅ 已建立，未触发）
- 文本 RC1+RC2 达到可转轨门槛后才启动 Daft 多模态 pipeline（✅ 2026-08-01 已满足，image build 已解除暂停）
- VLM 生成实验始终标记为 optional
- Adaptive 控制器 3 轮改进后不能超过 static K_max=8 → RC2 降级

## 同步规则

项目规划和开题材料采用双向同步：

- 开题报告必须基于当前项目进展、实验事实和后续规划撰写。
- 开题报告的题目、研究内容、技术路线、实验边界或侧重点变化时，项目整体规划、实验优先级和文档入口也要同步调整。
- 修改方向类内容时，至少检查 `README.md`、`PROJECT_INDEX.md`、本文件、`overview/current_direction_and_plan.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`opening/report/opening_report.md` 和 `opening/work_rules.md`。

## 日志入口

项目级简要操作日志见：

```text
PROJECT_LOG.md
```

开题材料的详细日志见：

```text
opening/logs/project_log.md
```
