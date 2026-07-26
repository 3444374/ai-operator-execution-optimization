# 项目日志

## 2026-07-26 Eager vLLM baseline captured after recovered preflight incident

- Recorded the exact running vLLM 0.25.1 eager service and added complete,
  validated 64/512 scenario configs for real ShareGPT/BurstGPT compatible-HTTP
  execution with ChatML, temperature 0, 512 output cap, token budget 6144,
  static K=8, fixed 50 ms flush, MFU/resource tracing, and no writeback.
- The 64-request gate completed with zero incidents and passed exact request,
  output-token, finish-reason, FLOP, MFU, energy, resource, database job, and
  CSV schema audits.
- The 512 warm-up completed, after which the first formal invocation failed
  before job creation because the new process's base preflight row keys did not
  match the fully expanded `runs.csv` header. After the stable-field preflight
  fix and an independent resume audit, `--resume` skipped the warm-up,
  completed all three formal repeats, and marked the retained incident
  recovered.
- The three formal repeats each contain 512 completed unique requests/docs,
  positive token/FLOP deltas, valid MFU/energy/resource traces, and finished
  database jobs. Mean E2E is 282.756 s, observed throughput 812.234 tokens/s,
  MFU 0.04025, and GPU energy 22.851 kJ.
- Boundary: this establishes only the eager baseline. The recovered preflight
  incident, overwritten original retry stderr path, and one formal-3 Ray
  shutdown access-violation stack fragment remain documented concerns; no
  eager-versus-CUDA-graph conclusion is made.

## 2026-07-26 Ray 执行基础整分支审查修复

- Ray actor 正式运行现在只构造一次 endpoint-local worker submitter 与 legacy
  endpoint round-robin 状态；跨 PostgreSQL fetch chunk 不再从首个 worker 或首个
  endpoint 重新开始。逐 worker 提交计数按单次 chunk 返回 delta，避免汇总时重复
  累加历史计数。
- 非 fake compatible HTTP backend 的 endpoint URL 校验提前到 dry-run、数据库连接
  和 Ray 初始化之前；Ollama completion 继续保留默认 `http://localhost:11434`。
- job 创建后的任意异常会先回滚失败事务，再将 job 标记为 `failed` 并写
  `finished_at`；状态更新失败只附注到原异常，不替换原始失败。
- 主 CSV 在任何非 dry-run 执行前用 dry-run 字段做 schema 预检；正式追加仍要求
  header 精确一致。K_max interference runner 默认写入新的 `20260726` 文件名，
  历史 `20260719` 结果未删除、未覆盖。
- 本次是执行正确性与失败可观测性修复，不新增或修改实验性能结论。

## 2026-07-26 Ray 执行契约结果 schema

- `postgres_ai_operator_profile.py` 的 dry-run 与真实 run row 新增
  `ray_version`、每 endpoint actor worker 数、actor 最大并发、Ray CPU/GPU
  资源、service endpoint 数、actor worker 总数和逐 worker 提交计数。
- 真实 Ray 版本仅在 Ray 可用后读取；Python executor 行保持空值。HTTP worker
  固定记录 Ray GPU 配额 0，正式 completion 的 task/actor 自动重试继续禁用。
- 后续回归补齐 Python executor 的非适用哨兵：actor concurrency/CPU 为 0/0.0，
  避免访问可选 `RayWorkerOptions`；Ray task 记录实际 CPU，但对外 actor-only
  concurrency 字段也为 0。
- review 修复让 fake Ray task/actor 统一应用 CPU、零 GPU、禁重试/重启 options；
  两处 submit metrics 合并循环抽为同一 helper，覆盖多 chunk 逐 worker 计数累加、
  缺字段兼容与 worker 宽度不一致拒绝。
- `append_metrics` 现在为新/空 CSV 写 header，并在非空 CSV 的既有 header 与 row
  keys 不精确一致时于追加前抛出 `ValueError`，防止 schema 演进造成静默列错位。
- 文档明确 service endpoint 不等于 actor worker，并给出
  `endpoint × workers/endpoint × actor max concurrency` 的配置并发上界。
  多 GPU 性能仍待独立 GPU-backed endpoint 验证，本次只建立执行与观测契约，
  不新增实验性能结论。

## 2026-07-26 Row-cap-aware packing 与非阻塞 adaptive 观测门禁

- **代码**：typed adaptive admission 的指标抓取从提交决策线程移到既有后台采样器，增加 `sample_age_s` 观测/控制轨迹字段和异常路径关闭测试；新增 BFD-inspired row-cap-first 纯装箱函数，Arrow 与 Daft 复用同一 membership 实现，顺序 token-budget 仍为默认。
- **验证**：完整测试 235/235 通过；真实 Daft→Ray task/actor 合约连续 3 轮、共 12/12 通过；64 行真实 PostgreSQL→Daft→Ray→vLLM 门禁最终 6/6 runs、384/384 requests、0 incident，行数/token 约束、外键、资源轨迹、FLOP 增量和 MFU 全部通过。
- **环境根因**：vLLM 0.25.1 未开启 `--enable-mfu-metrics` 时仍暴露零值 FLOP counter。首次门禁因此被判无效并废弃；保留旧容器为 `ai-operator-vllm-qwen-pre-mfu-20260726`，等价重建当前服务并只增加 MFU 开关，单请求确认 counter 增长后重跑。
- **边界**：64 行仅是基础设施正确性证据；单次 formal 数据不用于策略性能排序。下一步按预注册规则做 512 行 row cap × token budget × algorithm 筛选，未通过者不进入 1024。

## 2026-07-26 Output-aware BFD 真实 512/1024 规模验证

- 修复后 64 行门禁完成 12/12 runs；512 行六单元矩阵完成 24/24 runs、
  0 incident，18 个 formal run 共 9,216/9,216 request successes。1024 行
  三场景确认完成 12/12 runs、0 incident，9 个 formal run 也有 9,216 条
  逐请求记录。
- 正式服务为 vLLM 0.25.1 + Qwen2.5-1.5B BF16，禁用 prefix cache 并开启
  MFU counter。MFU 分母使用 NVIDIA 官方 RTX Blackwell 表中的 RTX 5070
  密集 BF16 Tensor、FP32 accumulate 61.7 TFLOP/s，不使用 988 AI TOPS。
- 512 行中，BFD trace 相对同成本 sequential trace：rows/s +12.019%、
  request P95 -11.203%、energy -18.639%、MFU +13.906%；但相对 strongest
  practical baseline `seq_fixed` 仅 rows/s +1.384% 且 energy +2.474%。
- 1024 行没有复现：BFD trace 相对 seq trace rows/s -5.156%、request P95
  +4.318%、energy +6.801%、MFU -5.130%；相对 seq fixed rows/s -14.293%。
  BFD submission 数为 87，seq trace 为 77，budget utilization 0.782 vs
  0.884。当前证据否定“经典 BFD 全规模更强”，支持 row-cap-aware 联合搜索。
- 报告与原始数据位于 `experiments/results/output_aware_bfd_*_20260726/`。
  分支继续隔离，未合并 `main`，本轮不自动同步 Wiki。
- 绘图汇总默认指标从 33 项扩展到 63 项，补齐 rows/s、SLO、stage time、
  submission/batch shape 和 vLLM latency；旧 CSV 缺失的新字段以 `n=0`
  输出，不破坏历史结果读取。512/1024 `summary_long.csv` 已重新生成。
- 最终回归在真实 arrival-replay→Ray contract 中复现 3.6ms 的时钟域竞态：
  理想 replay deadline 偶尔晚于实际 epoch-shaped flush，导致 trace 中 submit
  看似早于 flush。修复后以实际观测 flush 为边界，将超前的 intended arrival
  clamp 到该边界；不放宽 lifecycle 校验。54 项调度测试通过，真实
  Daft→Ray contract 连续 3 轮共 12 次通过。离线 512/1024 结果不受影响。

## 2026-07-26 Output-aware BFD、离线逐请求 E2E 与资源效率指标

- **数据组织**：新增严格的输出成本模式和确定性 best-fit-decreasing
  装箱；Arrow 与 Daft 共用同一 membership 逻辑，保持每行是一条完整请求，
  并显式记录 packing algorithm/scope、预算利用率、超预算行和成本分位数。
- **成本边界**：`trace_target_output` 明确标记为未配对的 BurstGPT trace
  metadata，不声称是当前 Qwen prompt/model 的 oracle；后端
  `completion_max_tokens` 不受成本估计模式影响。
- **逐请求时延**：request trace schema 升至 v2，区分
  `replayed_arrival` 与 `offline_job_start`；离线 BFD 现在可记录每个 prompt
  从作业开始（含读取与组织）到完成的 E2E，跨 fetch chunk 的 submission ID
  不复用。
- **资源效率**：正式 run row 新增 GPU 利用率、显存、vLLM 压力、功率、
  积分能耗和每千 observed token 能耗。MFU 仅在显式提供 reviewed
  GPU 峰值与精度时输出；优先使用 vLLM
  `estimated_flops_per_gpu_total` 增量，旧版服务才回退到显式
  FLOPs/token，保留估计方法与时间口径；
  GPU utilization 不冒充 MFU。
- **实验规模**：64 行仅作真实组件门禁；六组策略使用同一 512 文档、
  1 次 warm-up + 3 次正式重复；512 审计通过后，仅对选中的 baseline 与
  adaptive 配置做 1024 行、3 次正式复验，不混算不同规模的 effect size。
- **新增入口**：`code/scripts/summarize_output_aware_bfd.py` 输出
  scenario/metric 长表统计，覆盖吞吐、E2E/tail、packing、GPU、能耗与 MFU。
- **验证边界**：当前完成的是单元与真实本地 Daft→Ray task/actor contract；
  GPU-backed PostgreSQL+Daft+Ray+vLLM 的 64/512/1024 数据尚待运行，暂不形成
  性能优越性结论。

## 2026-07-25 加速 arrival replay 正式实验设计

- **问题**：BurstGPT 官方时间戳单位是秒；当前 1024 行覆盖 52,184 秒，
  前 512 行覆盖 39,757 秒，直接按原速回放不适合本地单 GPU 重复实验。
- **设计**：新增显式 `arrival_time_scale`，只缩放首行归零后的回放时钟偏移，
  不修改数据库原始时间戳，不缩放 flush timeout/hard max，并把比例写入所有
  运行与 manifest 产物。
- **矩阵**：64 行 × 0.0001 做一次快速产物门禁；随后 512 行 × 0.0005，
  immediate/fixed-timeout/queue-adaptive 各 1 warm-up + 5 正式重复。
- **边界**：结果属于单 GPU、受控加速的 BurstGPT-derived workload 对比，
  不代表原始生产到达率、多 GPU scaling 或内部 PostgreSQL 18.3 平台。
- **新增文件**：
  `code_doc/superpowers/specs/2026-07-25-accelerated-arrival-replay-design.md`。
- **实施计划**：
  `code_doc/superpowers/plans/2026-07-25-accelerated-arrival-replay-implementation.md`，
  分为回放时钟缩放、profiler/trace 接线、真实门禁与正式矩阵三项。

## 2026-07-25 Arrival replay 与独立 flush 运行链路完成

- **实现**：新增完整行粒度 pending batch builder、单调时钟 arrival replay、
  immediate/fixed-timeout/queue-adaptive flush 组合，以及独立 flush trace。
  batching、flush、admission 三层职责分离，策略层不依赖 Daft、Arrow、Ray
  或 HTTP。
- **生产接线**：`postgres_ai_operator_profile.py` 新增 `--arrival-replay`、
  flush 策略/超时/hard-max/trace 参数；仅显式开启时改变旧离线吞吐路径。
  queue-adaptive 指标由后台采样，回放循环不执行网络 I/O，正常关闭等待采样
  生命周期结束。
- **真实契约**：使用真实 Daft 和本地 Ray task/actor 验证
  `0,0,20,100 ms` 到达序列、固定超时边界、每行恰好一次和确定性 fan-in。
  测试发现并修复 Daft `RecordBatch` 与只接受 `Table` 的适配缺陷。
- **验证**：完整代码测试 161/161 通过；真实契约连续运行 3 轮通过；compileall
  与 diff check 通过。该证据属于单元/集成契约，不能作为 GPU 性能结论。
- **下一步门禁**：单 GPU 上以真实 PostgreSQL + Daft + Ray + vLLM，固定
  token budget 6144、静态 K_max=8，对 immediate/fixed-timeout/
  queue-adaptive 各做 1 warm-up + 1 smoke；全部运行、请求、flush、control、
  resource 和 manifest 产物非空后再进入正式重复。

## 2026-07-24 Top 15 精读按学术标准重排（Orca/DistServe 进，SABER/Multi-Bin 出）+ Clockwork 补入 inventory

- **触发**：用户质疑 Orca（continuous batching / iteration-level scheduling 开山、开题正文"vLLM/Orca"并称 5 次、8 个实验计划引用）竟不在精读 Top 15。核查发现 Orca 已精读，只是被旧"对本项目贡献度"标准以"vLLM 覆盖其机制"为由排到 #16——与项目把它当一等文献引用自相矛盾。
- **重排标准（用户定）**：从 66 篇按**学术研究标准**（基础工作/核心技术/相关工作）选前 15，CCF-A 优先，极重要 arXiv 可破例，正好 15 篇。
- **新 Top 15**（CCF-A/顶会 12 + 重要 arXiv 3）：基础 4——vLLM、**Orca**、Ray、Clipper；核心 7——Sarathi-Serve、SGLang、**DistServe**、Splitwise、CONCUR、Ray Data Streaming、BucketServe；相关 4——Cortex AISQL、NeurDB、Galois、DB Perspective。
- **与旧版差异**：进 Orca（iteration-scheduling 开山）、DistServe（goodput/prefill-decode，CCF-A）；出 SABER（USL 理论，AIMD 已被 Clipper+CONCUR 覆盖）、Multi-Bin（length-align 理论，已被 BucketServe 工程代表）。arXiv 由 5 降至 3，保留的每篇都是某核心策略无 CCF-A 替代的唯一来源。
- **Clockwork**：补入 `research/ai_operator_literature_inventory.md`（v5，65→66 篇，OSDI×6→7、CCF-A 37→38），归入推理服务系统组；knowledge_hub §5.2 已引其为 queue-adaptive flush 的调度思想来源。精确题录待用户放入 `research/reference/clockwork_osdi2020.pdf` 后以扉页核实并登记 REFERENCE_INDEX（67→68）。
- **更新文件**：`research/top15_ranked_papers.md`（按新标准重写）、`research/ai_operator_literature_inventory.md`（v5+计数+CCF 统计）、`opening/literature/top15_reading_notes/`（拷贝集删 saber/multibin、加 orca/distserve）+ 其 README 清单。
- **未做**：未 `git commit`；Clockwork PDF 未登记（待用户提供）。

## 2026-07-24 机制优先级并入 experiment_status_and_gaps §4（撤回新建文件）+ plans/ 文档维护纪律

- **撤回**：上一步新建的 `experiments/plans/mechanism_experiment_index.md` 是同一错误的第三次重复（前两次：`submission_control_autoregressive_basis.md` 被要求合并回现有文档、plans/ 结构治理时被指出文档增殖）。用户指出"现有文档不能记录这些内容吗"——确认应并入 `experiment_status_and_gaps.md` §4：该文档即"下一步实验第一参考"，索引回答的"先试哪个机制"与 §4 P0 改进方向是同一问题。机制优先级表已并入 §4 首「候选机制优先级（跨论文）」；fatal flaw 指向 `strategy_design_literature_basis.md` §3.1（不重复）；耦合/多模态两行删除（已有 `cross_layer_killer_experiment.md` / `daft_ray_multimodal_reference.md` 承接）。新建文件已删，`PROJECT_INDEX.md`、`plans/README.md` 登记回滚。
- **新增规则（plans/ 文档维护纪律，写入 `plans/README.md`「文档维护纪律」节）**：(1) **默认并入现有文档不新建**——某类内容找到自然归属就并入，深度进 reading_notes / `*_reference.md`；只有所有现有文档都不合适才新建，且必须在 PROJECT_LOG 说明理由。(2) **计划文档只保留待做内容**——实验完成（结果已记 results + experiment_status_and_gaps）后，设计/变量/矩阵从对应计划文档删除；前提是 results 报告自包含设计。
- **未做**：未对现有计划文档（`data_organization_batching.md`、`service_scheduling_backpressure.md` 等）做"完成内容清理"pass——其中可能仍有已完成实验的存量，待确认后另起。

## 2026-07-24 全项目文档新鲜度审计与批量修正

- **触发**：用户指出文献归位只是个例，要求检查全部规则/说明文档是否反映当前状态。
- **方法**：4 个并行审计 agent（索引文档 / 26 个 AGENTS.md / 断链机械扫描 / 数值版本漂移）+ 主线逐条复核，只报核实过的出入。
- **修正（全部 P0/P1/P2）**：
  - **P0**：根 `README.md` 状态冻结在 07-18 前（把已完成的 vLLM baseline 写成"下一步"、运行命令指向已弃用 fake 管道）→ 重写"当前证据/近期目标/运行命令"对齐 `PROJECT_OUTLINE.md`；目录树修 6 处断链 + 补 `code_doc/`、`data/`、扩 `research/` 子树。
  - **P1 篇数漂移**：inventory 已升 v4=65 篇、精读 33 篇，下游未跟随——修 9 处"57→65"（research/README、knowledge_hub ×3、current_direction_and_plan、PROJECT_INDEX、baseline_reference、code/AGENTS）、4 处"16/19→33"（inventory header、code/AGENTS、reference/README、strategy_design_implementation_reference）；top15"立即行动项"过期块重写；4 处"待精读→已精读"（Lance、SABER、vLLM×2：orca/serverlessllm）。
  - **P1 方向/状态**：`motivation/AGENTS.md` 当前状态/下一步从 fake/"方向未定"更新为 GPU-backed 已完成 + 方向已收敛；`experiments/AGENTS.md` 第三项从"写回瓶颈判定"改为"多模态泛化验证"；根 `AGENTS.md` §3"下一步①建立 baseline"过期 → 改为当前缺口。
  - **P1 断链（共 12 处）**：`overview/project_outline.md`（README 树 + overview/README + overview/AGENTS，按"删引用"处理）、`feasibility/guide.md`/`analysis.md`、`feasibility/results/feasibility_report.md`/`current_direction_analysis.md`、`opening/outline.md`、`opening/navigation.md` echarts_rules.md、`opening/slides`+`projects`+`templates` 旧 pptx 名、`code/README`+`deploy/postgres18.4` feasibility 旧文件名（→ pg18_4_connection_*）、`learning/experiment_walkthrough` 图路径缺 `../`。
  - **P2**：根 `AGENTS.md` §4 目录表补 `deploy/`/`projects/`/`code_doc/`/`data/`；`motivation/results/AGENTS` 补 `cpu/`；headline 37.5×（operator 阶段）与 13.4×（端到端）口径标注统一。
- **审计中 agent 漏报、由复核揪出的 2 处**：`code/README.md` bash 示例里第二处旧 CSV 名（L169）、`serverlessllm_osdi2024.md` 也有"vllm(待精读)"——已补修。
- **未改（历史/边界，正确保留）**：`PROJECT_LOG.md` 与 `archive/` 内的"57/16/19/44/69 篇"为当时真值；inventory L3 v3=57 版本史；PG 18.3/18.4 区分、模型版本——审计确认全部无矛盾。
- **未做**：未 `git commit`（等用户确认）。

## 2026-07-24 文献精读语料从 opening/ 迁至 research/；开题 Top 15 拷贝留 opening/

- **触发**：用户指出 opening/ 是开题（阶段性）工作区，但全部文献精读笔记（44 篇）、PDF（69 个）、文献清单与评估都放在 `opening/literature/`，与 `research/`（项目级"背景调研、文献依据"目录）职责错位——opening 自己的 README/navigation 都写着"文献参考 research/"，research/README 却要标注"扩展文献不在本目录"打补丁。
- **迁移**（`git mv`，保留历史）：`opening/literature/{reading_notes,reference}` → `research/{reading_notes,reference}`；`opening/literature/{ai_operator_literature_inventory,top15_ranked_papers,gpu_scheduler_data_placement_supplement_20260715,direction_assessment_20260715}.md` → `research/`。`research/` 成为文献唯一归属，knowledge_hub 与原料同目录。
- **opening 保留**：`opening/literature/reading_list.md`（开题精读优先级清单）+ 新增 `opening/literature/top15_reading_notes/`（开题要求精读的 15 篇笔记拷贝 + figs，自包含快照，权威版在 `research/reading_notes/`）。
- **链接/索引同步**：全仓库批量替换 6 类已搬走路径（`opening/literature/{reference,reading_notes}/` + 4 个 md）；手动修索引文档——`research/README.md`（"扩展文献不在本目录"段改为"本目录内"并补 reading_notes/reference 条目）、`research/knowledge_sync_guide.md`（手动映射表头 + 触发规则）、根 `AGENTS.md` §11 触发规则中的知识目录列表、`opening/README.md` 与 `opening/AGENTS.md` 的 `literature/` 职责、`figures/audit/strategy_figure_micro_design_points.md`、`PROJECT_INDEX.md`（补 `research/reading_notes/`、`research/reference/`、inventory、top15 等条目）。
- **连带影响（重要）**：同级 wiki 仓库 `../ai-operator-wiki/sync-wiki.sh` 的 reverse-sync 路由与 `[2/5]`、`[3/5]` 段全部耦合 `opening/literature/`，已同步改为 `research/`（`raw/inventory` 分流：`reading_list` 回 opening、其余回 research；`raw/analysis` 的 direction/gpu_scheduler 并入 research/ 分支）；否则下次同步会静默丢笔记/PDF。
- **未做**：未 `git commit`（等用户确认）；未改笔记内容语义（仅改路径）；笔记中指向项目外 `raw/papers/` 的 paper 库引用不动。

## 2026-07-23（第四次）PDF 全量规范化改名 + 误下载/重复清理 + 精读推荐 15 篇

- **触发**：用户要求精读下一批论文前，先把 `reference/` 下 69 个混乱命名的 PDF（arXiv 号、`pxxxx-author`、`osdi24-xxx`、中文标题混存）统一改名，并清理误下载/重复。
- **改名规范**：全部统一为 `短名_会议年份.pdf`，与 `research/reading_notes/` 精读笔记一一对应（如 `vllm_sosp2023.pdf` ↔ `vllm_sosp2023.md`）。15 个 git 跟踪文件用 `git mv` 暂存为 rename（保留历史），其余本地 `mv`。
- **清理误下载 3 篇**（arXiv ID 被重新分配导致内容错位）：`diskann_neurips2019.pdf`（实为凝聚态物理）、`milvus_sigmod2021.pdf`（实为 IR 词典翻译）、`dostoevsky_sigmod2018.pdf`（实为代数几何）。真 DiskANN/真 Milvus 已重新获取；Dostoevsky 暂不补（写回 LSM 背景，优先级低）。
- **清理重复 2 篇**：FlashAttention、FlexGen 各保留正式会议命名副本（NeurIPS/ICML），删除 arXiv 号重复副本。
- **补齐 3 篇**：真 Milvus（SIGMOD 2021, DOI:10.1145/3448016.3457550）、Clipper（NSDI 2017）、CoLoRA（ASP-DAC 2026, DOI:10.1109/ASP-DAC66049.2026.11420717）。
- **CoLoRA 撞名警示**：arXiv 上至少 4 个"CoLoRA/CoLoRa"不同领域论文（CNN-PEFT / PDE-降阶 / LoRa-无线网络 / 多租户 LLM 调度）。正确那篇仅 IEEE 付费墙可得（无 arXiv），已通过完整标题命中获取 `colora_aspdac2026.pdf`；三次 arXiv 搜索均命中撞名论文。
- **索引同步**：`REFERENCE_INDEX.md` 重写（67 篇按 7 类重组、计数 52→67、未下载清单修正、新增规范化记录附录）；`reference/README.md` 计数修正；全项目 `.md` 中旧 PDF 文件名引用经 sed 批量替换为新名（仅替换 `.pdf` 后缀的文件引用，纯 arXiv/DOI 文献引用不动）。
- **库现状**：67 个 PDF，全部规范命名，无错误/重复。
- **精读推荐**：用户要求按"全部未读"假设推荐 15 篇，应用 T1(综述)→T2(最近前人工作)→T3(核心技术) 排序；判断不可外包的 ⭐8 篇需用户亲自精读（Cortex AISQL、Galois、Ray Data Streaming Batch、vLLM、DB Perspective、Splitwise、Clipper、SGLang），其余交 agents 批量精读。
- **更新文件**：`research/reference/*.pdf`（改名）、`REFERENCE_INDEX.md`（重写）、`reference/README.md`、`PROJECT_LOG.md`、以及含旧文件名引用的若干 `.md`。

## 2026-07-23（第三次）编码规范与代码架构文档落地

- **触发**：用户确认综合评估与项目已有记录一致，要求将新增内容写入对应文档，并重申"设计优先使用知识库和精读论文中的知识"。
- **更新文件**：
  - `experiments/plans/strategy_design_implementation_reference.md` — 新增 §8 "目标代码架构与模块接口规范"，定义 4 个新模块（admission/routing/request_pool/pipeline）的接口规范、文献来源、实现优先级。每个设计决策标注文献出处（Clipper NSDI'17、CONCUR 2025、SABER 2025、CoLoRA 2026、SGLang NeurIPS'24、Parrot OSDI'24 等）。
  - `code/AGENTS.md` — 新增"编码规范"节，6 条规则：① 保持简单 <100 行（Ray ConcurrencyCap 废弃教训）；② 每行=独立完整请求（vLLM chunked prefill 语义安全）；③ 策略层不依赖引擎层（DataOrganizer 抽象）；④ 多模态复用文本代码路径；⑤ 文献优先——新机制从精读笔记提取；⑥ 新实验指标完整性（tokens/s + service_p99 + 时间序列）。每条规则标注文献来源。
- **设计原则重申**：所有机制设计、策略选择、基线对比，优先从项目 57 篇 CCF-A 文献 + 16 篇精读笔记中提取设计模式和候选方案，不凭空设计。方法论见 `research/README.md` §文献优先设计方法论 和 Wiki `设计方法论` MOC。

## 2026-07-23（第二次）全维度综合评估：Wiki 知识库 + 文献精读 + 代码架构 + 后续路线图

- **触发**：用户要求结合 Wiki 知识库（206 实体、4 MOC）、16 篇精读论文笔记、项目代码和实验状态，做全维度综合评估。
- **方法**：使用 idea-evaluator（五维打分）、nature-reviewer 视角、deep-research 文献验证、karpathy-guidelines 严谨性控制。
- **五维得分**：Higher=7, Faster=6, Stronger=5, Cheaper=6, Broader=7。Faster 和 Stronger 为当前瓶颈维度，需要 adaptive 控制器数据和 scale-out 验证来提升。
- **代码架构评估**：现有 6 模块（sources/organizers/model_backends/sinks/metrics/workloads）分离清晰，策略-引擎抽象合理。缺少 3 个核心模块：admission.py、routing.py、request_pool.py。pipeline.py 编排层缺失。`model_backends.py` 用 urllib 手工 HTTP 请求，无重试/连接池/streaming。bin-packing 分组策略未实现。无 CLIP/VLM backend。
- **目标架构**：8 模块（+ admission / routing / request_pool / pipeline），接口规范已定义。admission 使用 AIMD + EWMA + per-submission check，request_pool 按 operator_type 分 bucket。
- **代码实现注意事项**：(1) 保持简单 <100 行（Ray ConcurrencyCapBackpressurePolicy 废弃教训）；(2) 每行=独立完整请求（语义安全红线）；(3) 策略层不依赖引擎层（DataOrganizer 抽象）；(4) 多模态复用 token-budget 代码路径；(5) 文献优先——新机制从精读笔记提取；(6) 所有新实验包含 tokens/s + service_p99 + inflight 时间序列。
- **最短路径（6-8 周）**：Week 1-2 P0 修复 → Week 3-4 P1 补齐 → Week 5-6 多模态前置 + 代码补全 → Week 7-8 多模态实验 + 论文写作。
- **后备路径（adaptive 失败时）**：RC2 降级为"K_max 必要性论证 + 跨查询请求路由（方法补充）+ queue-adaptive 探索（Discussion）"。
- **风险最高项**：Adaptive 3 轮后仍不如 static（概率 40%）。后备路径已准备。
- **认知债务**：6 篇 2025-2026 新论文未精读、baseline_reference 20 个 baseline 仅 <5 运行、设计决策日志为空、Daft 引擎参数未探索、开题报告与实验状态不同步。
- **关键结论**：(1) 课题定位成立（四岛空白双重确认 + 57 篇 CCF-A 文献支撑）；(2) 最高优先级只有让 adaptive 工作，不成功则 4 周后切换后备路径；(3) 跨查询请求池 + 算子路由是多模态前置依赖，不是 afterthought；(4) 文献基础扎实但未充分利用，投稿前需补齐新论文精读笔记和精确的 Related Work 区分度论证。
- **文件**：`experiments/plans/experiment_status_and_gaps.md` §6（已有审计，可考虑补充代码架构部分）

## 2026-07-23 完整问题审计：P0/P1/P2 分级 + 认知债务清单

- **触发**：用户要求对项目当前状态做系统性审计，识别除"ML as Native Operator"叙事定位之外的全部问题。讨论中使用了 idea-evaluator（五维评分 + fatal-flaws audit + paradigm-shift probe）、nature-reviewer（三审稿人模拟）、deep-research、karpathy-guidelines 和 brainstorming 六种技能交叉评估。
- **"ML as Native Operator"叙事问题**：搁置至后续阶段。用户的三区别框架（语义感知查询重写 / 跨查询 continuous batching / 两层嵌套代价模型）是有价值的分析工具，但当前项目未实现区别 1（DB 内核改动），所有优化在 Ray 中间层。当前阶段聚焦外部执行链路优化，不涉及数据库内核。
- **跨查询 batching 澄清**：vLLM 内部做 continuous batching（隐式），但 Ray 层无显式的"跨查询请求融合"机制。Shared-vLLM K_max Interference 是两 job 共享同一 endpoint（跨查询共享服务），不是跨查询主动合并请求。如论文需 claim 此能力，需实现全局请求池 + 按算子类型/prefix hash 维度合并。
- **多模态场景下的跨查询 batching（2026-07-23 补充）**：纯文本场景下 vLLM 的 continuous batching 掩盖了"无跨查询请求池"的 gap——所有请求走同一 vLLM endpoint，vLLM 内部自动合并。但在多模态场景下，CLIP embedding 没有 continuous batching，不同查询的 AI_EMBED 请求必须显式合并才能保证 GPU 利用率。跨查询请求池 + 算子类型感知路由是多模态实验的工程前置条件，不是可选的。如果 RC2 adaptive 在 P0 阶段降级，跨查询合并可作为 RC2 的方法补充贡献。
- **核心发现（P0 阻塞级）**：
  1. RC2 核心策略为负结果：queue-adaptive flush 的 foreground E2E=10.2s vs static K_max=8 的 7.3s（~40% 差距）。放弃条件：3 轮改进后仍不达 static 的 90% → RC2 降级为"K_max 必要性论证 + adaptive 探索性讨论"。
  2. 两项策略联合消融（独立拼接 vs 联合 grid search）完全没有数据——AGENTS.md §1 写死的核心验证实验。
  3. `tokens/s` 指标缺失——`rows/s` 在 AI_COMPLETE 场景下是有偏指标（每行 token 量可差 13.9×）。此外 `service_p99`、inflight/queue 时间序列、per-request e2e latency 分布均缺失。
- **P1 严重级**：
  4. Prefix-aware 在自然 workload 上信号太弱（6.4% prefix ratio），受控 workload（0/30/70/100%）未做。
  5. Length-align + fixed rows 是负结果（token P95=33407），正确组合（length-align + token-budget）未做正式对照。
  6. 所有实验 512 行规模，无 scale-out 验证（2048 行）。
  7. Token-budget tradeoff 未系统表征（token tail vs HTTP call count）。
- **P2 方法论/设计问题**：
  8. Daft 引擎级参数实验空间为零——"策略级 + 引擎级"优化空间仅覆盖了策略级。
  9. 离线扫表（doc_id 序）与 arrival-aware 实验间存在叙事断层。
  10. Baseline 矩阵（baseline_reference.md 20 个 baseline）大量未实际运行。
  11. 无多 endpoint/多 GPU 实验。
  12. 跨查询 batching 是隐含效果而非显式策略（见上）。
- **认知债务**：文档承诺 vs 实际交付存在系统性差距——baseline 矩阵、引擎级参数表征、实验五阶段计划、actor pool 分池路由均存在文档写了但实验未做的情况。投稿前必须清理。
- **最短可交付路径**：第 1 周修 adaptive 控制器（≥90% static 或立即降级）+ 联合消融 + 补齐 `tokens/s`/`service_p99`；第 2 周后 prefix 受控 workload + 2048 行 scale-out。
- **更新文件**：`experiments/plans/experiment_status_and_gaps.md`（新增 §6 完整问题审计，含 P0/P1/P2 分级 + 认知债务清单）、`PROJECT_LOG.md`

## 2026-07-22 文献精读笔记批量完成（12 篇新增）

- **触发**：用户要求对 `research/reference/` 中的文献按 `tpl-文献精读-深度版.md` 模板做精读。
- **操作**：使用 12 个并行 Agent 同时阅读 PDF 并生成精读笔记，每篇严格遵循四层模板（基本信息 → 论文结构分析 → 批判性评估 → 与课题连接）。
- **新增笔记**：
  - DB4AI 组：`neurdb_cidr2025.md`、`leads_pvldb2024.md`、`inferdb_pvldb2024.md`、`smartlite_pvldb2024.md`
  - AI 推理服务组：`vllm_sosp2023.md`、`orca_osdi2022.md`、`sarathi_serve_osdi2024.md`、`serverlessllm_osdi2024.md`
  - 综述组：`llm4dm_pvldb2024.md`、`db_perspective_llm_pvldb2025.md`
  - 基础设施组：`ray_osdi2018.md`
  - 持久化组：`diskann_neurips2019.md`
- **总计**：`reading_notes/` 目录现有 16 篇精读笔记（4 篇旧 + 12 篇新）+ 2 模板，覆盖 `ai_operator_literature_inventory.md` 中 15 篇建议精读的全部文献 + DiskANN 补充精读。
- **已知问题（2026-07-23 已解决）**：原 `reference/diskann_neurips2019.pdf` 内容为凝聚态物理论文（arXiv ID 被重新分配）——当日已用真 DiskANN（arXiv:1811.01324）替换并清理误下载，详见 PROJECT_LOG 第四次条目。
- **更新文件**：`reading_notes/*.md`（16 篇精读笔记）、`reading_list.md`、`reference/README.md`、`PROJECT_LOG.md`

## 2026-07-21 Token 元数据来源与技术细节记录规范

- **触发**：导师追问 token-aware batching 中“每行 token 怎么获取、怎么用于分组”，用户要求把这类技术细节记录到合适文档，并明确后续代码完成时同步记录实现细节。
- **决策**：不新建单独技术细节文档；当前内容归入既有实验计划和实现参考，避免入口分散。
  - `experiments/plans/data_organization_batching.md` 记录每行 `prompt_tokens` 的来源、tokenizer 一致性要求、`prompt_tokens + completion_max_tokens` 组批公式、超长行边界和实验必须记录字段。
  - `experiments/plans/strategy_design_implementation_reference.md` 记录 Workload Profiler / DataOrganizer / `BatchRequest` 需要携带的 token 元数据，以及 CSV/审计指标口径。
- **后续规则**：以后完成涉及调度策略、workload 导入、DataOrganizer、CSV 字段或 vLLM 指标采集的代码时，同步检查上述两个文档；若新增字段、公式、fallback 或边界条件，必须在对应文档和具体结果 README 中记录。

## 2026-07-21 开题报告图位优化与正文分析强化

- **触发**：用户要求将图放到正文合适位置，在正文中提及并讲解分析每张图，报告可以比 PPT 图多、讲解更清晰。同时注意图文一致性。submission_control 的三张新图暂不写入报告。
- **操作**：对 `opening/report/opening_report.md` 中所有 9 张图进行了系统性的图位优化和正文分析强化（详见上一版记录）。随后将更新后的报告覆盖同步到飞书 docx（revision 244），上传 9 张图后在 XML 层获取 block ID，逐张移动到对应图注段落之后（revision 263–271），使每张图紧跟在正文中对应的图注文字下方。
- **飞书同步**：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`，revision 271，文本+图片均已到位。图片位于对应图注之后（"图注文字 → 图片"顺序，可读）。
- **更新文件**：`opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md`、`PROJECT_LOG.md`

## 2026-07-21 开题报告飞书 docx 同步

- **触发**：用户要求将开题报告最新修改同步到飞书，与答辩 PPT 内容一致。
- **操作**：
  - 将 `opening/report/opening_report.md` 的最新内容复制到本地源稿 `opening/feishu/opening_report_wiki.md`。
  - 使用 `lark-cli docs +update --command overwrite`（user 身份）覆盖写入飞书 docx：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`，飞书返回 `partial_success`（本地图片路径无法直接导入，预期行为），文档 revision 更新为 `221`。
  - 逐一上传 9 张图到飞书 docx（research_gap_three_islands / system_architecture_ai_data_execution / cross_layer_method_framework / runtime_strategy_control_loop / 10_e2e_operator_writeback_breakdown / 07_gpu_pgai_rerun_stage_writeback_20260714 / 08_gpu_pgai_rerun_endpoint_comparison_20260714 / 09_gpu_pgvector_writeback_comparison_20260714 / b26_arrow_vs_daft_stage_breakdown），均带中文图注。
- **本次同步的主要变更**（与旧版飞书内容相比）：
  - 题目从"面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究"改为"数据库 AI 负载的执行优化与调度研究方向"。
  - 写回 baseline 从"工程最优写入方案"统一改为"PostgreSQL + pgvector 的 COPY + deferred index"。
  - 研究内容中增加"多模态泛化验证"（图像 workload：AI_EMBED/AI_CLASSIFY，CLIP/Qwen2.5-VL）和"算子代价估计"补充讨论。
  - 研究方案新增"多模态泛化验证"实验段落，描述 token 预算到 frame 预算的参数迁移和 Daft pipeline 复用。
  - 进度安排更新：7 月已完成的 4 项打 ✅（开题报告、vLLM baseline、Daft 接入、token-tail revision）；10 月新增多模态泛化验证启动。
  - 新增 Daft vs Arrow 管线开销对比（图 4-7），支撑 Daft 作为后续 AI_COMPLETE 及多模态实验数据引擎的可行性。
- **图与正文一致性**：已检查图 4-3～4-7 的图注与正文描述一致，图 4-7 为新图，正文已包含对应的 arrow_postgres vs daft_postgres 分析段落。
- **未完成**：图片位于文档末尾，未移动到正文对应位置（与历史同步行为一致，受限于 markdown overwrite 不保留 block 级图片位置）。

## 2026-07-21 Ray 队列自适应提交机制调研与文献补充

- **触发**：用户提问"Ray 是否有现成的队列自适应提交机制，还是需要自己实现"。
- **调研方法**：WebSearch（Ray 官方文档、GitHub PR/issues）+ 学术文献搜索（2025-2026 LLM serving 论文）。
- **核心结论**：
  1. **Ray 无现成的 queue-adaptive flush 或 K_max 动态控制 API**——这些是课题需自建的核心贡献。
  2. Ray 提供丰富的 building blocks（`ray.wait()` 手动反压、`max_concurrency`、`max_tasks_in_flight` + `should_add_input()`、Serve queue-based autoscaling）。
  3. Ray Data 曾有一个几乎就是"自适应并发控制"的 `ConcurrencyCapBackpressurePolicy`（EWMA + deadband），但**已被废弃**——~400 行控制逻辑，性能反而不如简单方案。这是重要的 cautionary tale：我们的自适应策略必须保持简单。
  4. 发现 6 篇 2025-2026 年新论文与本课题直接相关——最相关的是 CONCUR（AIMD-based agent-level admission control, 4.09× throughput）和 BucketServe（按序列长度分组，3.58×，与我们的 length-aligned grouping 同构）。
- **更新文件**：
  - `research/ray_actor_dynamic_batching_reference.md`：
    - 新增 §1.6 `max_queued_requests` 准入控制
    - 新增 §1.7 Queue-Based Autoscaling（PRs #59430, #59548, #59351）
    - 新增 §1.8 Custom Autoscaling Policies（Ray 2.51+）
    - §3.7 大幅扩展：ConcurrencyCapBackpressurePolicy 废弃详情（EWMA/deadband/废弃原因）→ DownstreamCapacityBackpressurePolicy 替代方案 → `max_pending_calls` → `max_tasks_in_flight` + `should_add_input()` + `num_free_slots()` → `_actor_generator_backpressure_num_objects`
    - 新增 §6.7 CONCUR (2025)、§6.8 Scorpio (2025)、§6.9 SABER (2025)、§6.10 CoLoRA (2026)、§6.11 BucketServe (2025)、§6.12 ProServe (2025)
    - 附录 URL 清单扩充 15 条
  - `research/knowledge_hub.md`：
    - 新增 §5.5 从 6 篇新论文提取的设计原则
    - 新增 §5.6 Ray 现存机制的能力边界（building blocks vs 需自建）
    - §8 知识缺口新增 3 项（CONCUR 迁移可行性、USL 建模、ConcurrencyCap 教训）
    - §9 文件清单新增本次更新条目
- **对课题方向的影响**：无需改变方向——确认了"Ray 不提供现成机制，需要自己实现"的判断是正确的。新增文献进一步验证了 adaptive admission control + length-aligned grouping 两条线的研究活跃度。CONCUR 是最需要优先精读的论文。

## 2026-07-21 课题名称更新 + 实践计划表审查

- **触发**：用户审查实践计划表，要求将课题名称从"面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究"改为更精简的表述。
- **题目变更**：新题目为 **"数据库 AI 负载的执行优化与调度研究方向"**。
- **实践计划表发现**：四个阶段内容中存在多处与当前方向不一致的措辞：Phase Ⅱ "三项研究内容"应改为"两项"（写回已降为实验设置）、Phase Ⅱ "提交控制策略"应补全为"调度与提交控制策略"、Phase Ⅲ workload 列表（embedding 批量写入 / AI predicate 过滤与分类 / 离线 LLM 生成与抽取）需替换为当前主线（AI_COMPLETE 文本主场景 + AI_EMBED/AI_CLASSIFY 图像多模态泛化验证）、vLLM 不再是可选项。
- **更新文件**（题目替换）：
  - `AGENTS.md` §1
  - `README.md`
  - `PROJECT_OUTLINE.md`
  - `PROJECT_INDEX.md`
  - `opening/report/opening_report.md`
  - `opening/feishu/opening_report_wiki.md`
  - `opening/slides/opening_ppt.md`
  - `opening/practice_plan.md`
  - `research/literature_and_evidence_review.md`
- **未更新**：`PROJECT_LOG.md` 和 `opening/logs/project_log.md` 中历史条目保留旧题目（历史记录不应修改）。

## 2026-07-20 实验状态全面审计与缺口分析

- **触发**：用户要求对当前实验进展做系统性评估，回答"现在的实验能说明什么、还需要做什么"。
- **评估方法**：结合 idea-evaluator（五维评分 + fatal-flaws audit + paradigm-shift probe）、ars-reviewer（模拟三审稿人）、deep-research 和 vibe-research-workflow 四种视角。
- **核心发现**：
  1. **动机链完整**：token-tail revision 证明了"固定行 batch 是计算量的弱代理"，shared-vLLM interference 证明了"K_max 在共享 vLLM 下必要"——这两个动机实验质量好，可直接写进论文。
  2. **RC1 策略机制已验证**：token-budget batching 能约束 token tail（P95 从 26678→6144），但存在 tradeoff（4096 吞吐较低）。
  3. **RC2 策略未验证**：queue-adaptive flush 已实现但不如静态 K_max=8（foreground E2E 10.2s vs 7.3s）。这是当前最高风险的 gap。
  4. **两项策略联合消融缺失**：完全没跑过独立拼接 vs 联合 grid search。
  5. **指标盲区**：缺 `tokens/s`（比 rows/s 更公平的 AI_COMPLETE 效率指标）、缺 inflight/queue 时间序列、缺 per-request latency 分布、缺系统性 `service_p99`。
- **新建文件**：
  - `experiments/plans/experiment_status_and_gaps.md`：完整的状态-缺口-路线图文档，包含已完成/未完成实验表、证据链评估、指标盲区、P0/P1/P2 实验路线图、审稿人视角的拒绝风险。
  - `learning/metric_selection_methodology.md`：AI_EMBED vs AI_COMPLETE 观察变量选择方法论，解释为什么从"阶段时延拆分"转向"请求形状 + 服务端压力 + 端到端分布"的四层变量体系。
- **更新文件**：
  - `PROJECT_OUTLINE.md`：§当前最重要证据 重写为以本地 vLLM baseline 为首要证据；§近期优先级 重写为已完成项 + P0/P1/P2 缺口 + 指标盲区 + 新增 adaptive 放弃条件。
  - `experiments/plans/README.md`：新增状态审计文档入口。
  - `learning/README.md`：新增指标方法论文档入口。
  - `experiments/results/local_vllm_qwen15b_baseline/README.md`：§Remaining Formal Experiments 重写为结构化的下一步清单。
- **idea-evaluator 裁决**：Accept with Revisions。Higher 6, Faster 7, Stronger 8, Cheaper 5, Broader 6。Paradigm-shift potential possible（3.5/4）。两个 MAJOR flaw（adaptive < static、单 GPU 限制），均有明确修复路径。
- **ars-reviewer 共识**：动机实验扎实，但 adaptive < static 和缺乏联合消融是两个 MAJOR concern，如不修复在 VLDB/SIGMOD 级会议上大概率被拒。

## 2026-07-19 Shared-vLLM K_max interference experiment

- Added `code/scripts/run_kmax_interference_experiment.py`, a wrapper around
  `postgres_ai_operator_profile.py` that runs a foreground small job while a
  background bulk job shares the same local vLLM endpoint.
- Ran the first two-job `AI_COMPLETE` interference experiment:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv`
  and
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv`.
  Foreground: 128 rows, fixed16, `K_max=8`, `completion_max_tokens=64`.
  Background: 1024 rows observed in CSV, fixed16,
  `completion_max_tokens=64`, comparing `K_max=8` versus unbounded
  (`max_inflight=100000`). Both jobs share
  `http://localhost:8000/v1/completions`.
- Result: foreground small-job E2E averaged 4.923s solo, 6.535s during bounded
  bulk, and 11.384s during unbounded bulk. Foreground service P95 rose from
  2.580s under bounded bulk to 4.386s under unbounded bulk; vLLM queue mean
  rose from 0.001s to 0.445s.
- Interpretation boundary: this supports the K_max admission-control motivation
  under a shared vLLM service. It does not imply K_max is necessary when every
  job has an exclusive vLLM endpoint, and it is not yet a full fairness/SLO
  sweep.
- Added `figures/data/backup/b21_local_vllm_kmax_interference_small_job.*` and
  updated the local baseline README, code script README, figure README, audit,
  and `PROJECT_INDEX.md`.

## 2026-07-19 Batch policy x K_max AI_COMPLETE scheduling matrix

- Adjusted the scheduling baseline design after reviewing the role of `K_max`:
  `K_max` is admission control over already-formed Ray submissions, so it must
  be tested jointly with upstream batch shape rather than as a substitute for
  batch construction.
- Ran the local ShareGPT/BurstGPT `AI_COMPLETE` matrix:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv`.
  Matrix: fixed rows `32/64/128`, token budgets `4096/6144/8192`, and
  `K_max=2/4/8/16/unbounded`; 512 rows, `source_order=arrival_time`,
  `ray_task`, Daft source/organizer, local vLLM Qwen2.5-1.5B,
  `completion_max_tokens=16`, no writeback, warmup 1, formal repeats 3.
  All 120 rows completed with `status=ok`.
- Result boundary: too-small `K_max` increases Ray-side bounded wait and
  end-to-end time; larger `K_max` mostly improves or plateaus E2E in this local
  offline job, while raising vLLM queue/service-tail pressure at high inflight.
  This matrix does not prove that `K_max` is required for end-to-end
  improvement.
- Batch shape remains necessary in the analysis: `fixed128` creates only 4
  upstream requests, so `K_max>4` has little scheduling space, while token P95
  remains about 26680. Token-budget settings bound token P95 but create more
  requests, making admission control observable.
- Added `figures/data/backup/b18_local_vllm_batch_kmax_e2e.*`,
  `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.*`, and
  `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.*`.
  Updated the local baseline README, figure README, audit note, and
  `PROJECT_INDEX.md`. The earlier
  `sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv` / `b17` run is now
  documented as a preliminary single-shape K_max sweep.
- Started and then stopped a heavier offline stress sweep after determining it
  would still not establish the right motivation for `K_max`. The next
  scheduling experiment should instead use a real admission-control objective:
  multi-job or burst arrival workload, shared vLLM endpoint, SLO tail latency,
  timeout rate, queue-length peak, and fairness.

## 2026-07-19 Token-budget vs fixed-row AI_COMPLETE baseline

- Added upstream `--batching-policy fixed_rows|token_budget` and
  `--token-budget` support to `code/scripts/postgres_ai_operator_profile.py`
  through `code/src/organizers.py`. Token-budget batching greedily groups rows
  by estimated `prompt_tokens + completion_max_tokens` before Ray submission;
  it does not modify Ray or vLLM internals.
- Added CSV fields `batching_policy`, `token_budget`, and
  `model_request_timeout_s`, plus organizer unit tests for token-budget batch
  construction.
- Ran the local ShareGPT/BurstGPT `AI_COMPLETE` policy matrix:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv`.
  Matrix: fixed rows `16/32/64/128` versus token budgets `4096/6144/8192`,
  512 rows, `ray_task`, Daft source/organizer, local vLLM Qwen2.5-1.5B,
  `max_inflight=8`, no writeback, warmup 1, formal repeats 3, request timeout
  300s.
- Result boundary: token-budget controls request token tail and queue pressure
  (`4096/6144/8192` token P95 near budget, versus fixed 64/128 at 16377/26677),
  but throughput is a tradeoff. `4096` is most queue-stable but slower;
  `6144/8192` approach fixed 32/64 throughput while keeping token P95 much
  lower than fixed 64/128. This supports dynamic batching motivation, not the
  full method claim.
- Added `figures/data/backup/b15_local_vllm_token_budget_throughput.*` and
  `figures/data/backup/b16_local_vllm_token_budget_tail_queue.*`, then updated
  the local baseline README, figure README, audit note, and script README.
- Recorded the remaining local baseline follow-up list in
  `experiments/results/local_vllm_qwen15b_baseline/README.md`: arrival-aware
  `K_max` sweep next, queue-adaptive flush after that, length-align and
  prefix-aware ablations later, and COPY + deferred-index writeback deferred.

## 2026-07-19 PostgreSQL source-order mode for AI_COMPLETE profiles

- Added `--source-order doc_id|arrival_time` to
  `code/scripts/postgres_ai_operator_profile.py` and propagated the value into
  CSV rows.
- Updated `code/src/sources.py` so both `PostgresArrowSource` and
  `DaftPostgresSource` share the same source-order semantics:
  `doc_id` for offline throughput/data-organization scans, and
  `arrival_time_s NULLS LAST, doc_id` for arrival-aware service scheduling
  experiments.
- Updated `code/tests/test_sources.py`, `code/scripts/README.md`,
  `experiments/results/local_vllm_qwen15b_baseline/README.md`,
  `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md`,
  `learning/local_vllm_ray_baseline_walkthrough.md`, and `PROJECT_INDEX.md`.
- Boundary: existing 2026-07-18/2026-07-19 local baseline CSVs should be read
  as `doc_id` offline-throughput runs. Future K_max, queue-adaptive flush, and
  backpressure experiments should use `--source-order arrival_time` when the
  claim depends on request arrival rhythm.

## 2026-07-19 Local vLLM fixed-row baseline token-tail revision

- Added the 2026-07-19 Ray task token-tail sweep CSV:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv`.
- Revised the baseline interpretation from a plain row-batch sweep to a
  fixed-row proxy limitation test: larger row batches reduce request count, but
  token P95 and service P95 grow sharply and local throughput plateaus around
  16-32 rows and remains flat through the 64/128 stress points.
- Updated `figures/scripts/generate_local_vllm_ray_baseline_charts.py` to add
  `b11_local_vllm_token_tail_performance.*` and
  `b13_local_vllm_token_tail_penalty.*` as the main token-tail motivation
  figures, then added `b14_local_vllm_service_tail_gap.*` to isolate the
  service P50-to-P95 tail gap.
- Revised `b10` into `b10_local_vllm_request_count_inflight.*`, using two
  aligned panels for model-service call count and in-flight utilization instead
  of mixing both quantities on one axis.
- Updated `experiments/results/local_vllm_qwen15b_baseline/README.md`,
  `figures/README.md`, and
  `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` with the
  revised baseline question, command, result table, boundaries, and figure
  roles.

## 2026-07-18 Local vLLM Ray baseline figures and learning note

- Added `figures/scripts/generate_local_vllm_ray_baseline_charts.py` to
  regenerate backup figures from the ShareGPT/BurstGPT local
  `AI_COMPLETE` baseline CSVs.
- Generated separate single-purpose figures instead of a dashboard:
  `b07_local_vllm_ray_throughput.*`, `b08_local_vllm_ray_e2e_time.*`,
  `b09_local_vllm_ray_task_stage_timing.*`,
  `b10_local_vllm_request_count_inflight.*`,
  `b11_local_vllm_token_tail_performance.*`, and
  `b12_local_vllm_latency_probe_breakdown.*`.
- Added `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` and
  `learning/local_vllm_ray_baseline_walkthrough.md`.
- Boundary: these are local PG18.4 fixed row-batch baseline and metric
  observability support figures. They are not token-aware batching,
  queue-adaptive scheduling, writeback-inclusive, or PostgreSQL 18.3 internal
  platform results.

## 2026-07-18 AI_COMPLETE latency and vLLM metric probe

- Added batch-level result statistics to `code/src/metrics.py` and
  `code/scripts/postgres_ai_operator_profile.py`: batch row min/max/mean,
  batch token min/max/mean, and batch service latency P50/P95/P99.
- Added optional `--model-metrics-url` Prometheus scraping for vLLM run-level
  delta metrics: prompt/generation token deltas, request success delta, mean
  vLLM e2e/queue/inference/prefill/decode latency, and final running/waiting
  request gauges.
- Verified with
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv`:
  4 rows, 3 formal rows, all `status=ok`, all `vllm_metrics_status=ok`.
- Boundary: this validates metric collection on a small local Daft + Ray +
  vLLM probe. It is not a full optimized scheduling result.

## 2026-07-18 ShareGPT/BurstGPT tokenizer-filtered Ray rerun

- Updated `code/scripts/import_ai_complete_workload.py` so the imported
  `sharegpt_burstgpt` workload can use the local Qwen2.5-1.5B-Instruct
  tokenizer for `prompt_tokens` and filter rows by
  `prompt_tokens + completion_max_tokens <= max_model_len`.
- Re-imported 1024 `sharegpt_burstgpt` rows into the local PostgreSQL
  rehearsal database with `max_model_len=2048` and `completion_max_tokens=16`.
  Current prompt-token range is 1..1851.
- Reran the local `AI_COMPLETE` baseline through
  `PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> Ray -> vLLM` for
  `ray_task` and `ray_actor`, batch sizes 1/2/4/8/16/32. Results are recorded
  in
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv`.

## 2026-07-18 Local vLLM Qwen static batch baseline

- Established the first local `AI_COMPLETE` baseline for `PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> vLLM-compatible completion backend -> no writeback`.
- Used local `models/Qwen2.5-1.5B-Instruct` served by `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404` as `qwen2.5-1.5b`.
- Ran fixed row-batch sweep with `ray_batch_rows` in `1,2,4,8,16`, `total_rows=32`, `completion_max_tokens=8`, `executor=python`, `warmup_runs=1`, `repeats=3`.
- Result CSV and report: `experiments/results/local_vllm_qwen15b_baseline/static_batch_sweep.csv` and `experiments/results/local_vllm_qwen15b_baseline/README.md`.
- All 20 rows returned `status=ok`; formal rows only: mean throughput improved from 10.328 rows/s at batch size 1 to 91.976 rows/s at batch size 16.
- Boundary: this is a local fixed row-batch baseline, not a token-aware scheduling result, not an optimal batch-size claim, and not a PostgreSQL 18.3 internal-platform result.

## 2026-07-18 ShareGPT and BurstGPT raw workload downloads

- Downloaded ShareGPT Vicuna unfiltered prompt data to `data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json`; local check found 94,145 conversation records.
- Downloaded BurstGPT trace data to `data/raw/burstgpt/BurstGPT_1.csv`; local check confirmed columns `Timestamp`, `Model`, `Request tokens`, `Response tokens`, `Total tokens`, and `Log Type`.
- Added `data/README.md` to document raw data paths, intended use, and boundary.
- Updated `.gitignore` so `data/raw/**` payloads are not tracked by git.
- Boundary: this only establishes raw workload availability. Comparable baseline and optimized experiments should be generated from a normalized ShareGPT/BurstGPT workload table, not from the earlier synthetic seed rows.

## 2026-07-18 ShareGPT/BurstGPT workload import path

- Added `code/scripts/import_ai_complete_workload.py` to normalize ShareGPT prompts with BurstGPT timestamp/token metadata into the PostgreSQL `documents` table.
- Extended `documents` with workload metadata columns: `workload_name`, `prompt_tokens`, `target_output_tokens`, `arrival_time_s`, `session_id`, and `prefix_key`.
- Added `--source-workload-name` to `code/scripts/postgres_ai_operator_profile.py`, so different workloads can coexist in `documents` and profiling can select one explicitly.
- Imported local `sharegpt_burstgpt` workload rows into PostgreSQL with `start_doc_id=1000000`, `rows=1024`, `prompt_tokens=8..1797`, `target_output_tokens=2..2048`, and categories covering short/medium/long x ChatGPT/GPT-4.
- Verified a small `DaftPostgresSource -> DaftOrganizer -> Ray task -> vLLM` smoke under `tmp/sharegpt_burstgpt_daft_ray_vllm_smoke.csv` with `status=ok`, `total_rows=8`, `source_workload_name=sharegpt_burstgpt`.
- Boundary: this validates the final workload import/read path. It is not yet the full baseline sweep or an optimized scheduling result.

## 2026-07-18 vLLM local Qwen AI_COMPLETE smoke

- Started `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404` with local Hugging Face model files from `models/Qwen2.5-1.5B-Instruct`, avoiding runtime Hub downloads.
- Required local Windows/WSL Docker settings for this machine: `VLLM_WSL2_ENABLE_PIN_MEMORY=1`, `VLLM_USE_V2_MODEL_RUNNER=0`, and `--enforce-eager`; the default vLLM V1/V2 runner path previously failed with `RuntimeError: UVA is not available`.
- Verified OpenAI-compatible `/v1/models` returned `qwen2.5-1.5b`; verified `/v1/completions` with a minimal prompt.
- Verified project E2E smoke under `tmp/vllm_local_qwen15b_ai_complete_smoke.csv`: `operator=ai_complete`, `data_source=daft_postgres`, `organizer=daft`, `model_backend=compatible_http`, `model_name=qwen2.5-1.5b`, `writeback_mode=json_text`, `total_rows=2`, `written_rows=2`, `status=ok`.
- Ran the layer-2 structural matrix under `tmp/vllm_local_qwen15b_layer2_matrix.csv`: `data_source` (`arrow_postgres`, `daft_postgres`) x `organizer` (`arrow`, `daft`) x `executor` (`python`, `ray_task`, `ray_actor`) x `writeback_mode` (`none`, `json_text`). All 24 rows returned `status=ok`; all `json_text` rows wrote `written_rows=2`; all `none` rows wrote `written_rows=0`.
- Boundary: this establishes the local vLLM + Qwen + Daft + PostgreSQL completion path. It is not yet a formal performance experiment or token-aware/prefix-aware batching result.

## 2026-07-18 Ollama AI_COMPLETE backend

- Added `ollama` as an `AI_COMPLETE` backend in `code/src/model_backends.py`, using Ollama native `/api/generate`.
- Updated `code/scripts/postgres_ai_operator_profile.py` so `--operator ai_complete --model-backend ollama` defaults to `http://localhost:11434` when no completion endpoint URL is provided.
- Verified local PG18.4 smoke with Docker Ollama `qwen2.5:1.5b`: `ollama_ai_complete_smoke` completed with `written_rows=2`; `ollama_daft_ai_complete_smoke` completed with `data_source=daft_postgres`, `organizer=daft`, and `written_rows=2`.
- Ran the layer-3 structural matrix under `tmp/ollama_ai_complete_layer3_matrix.csv`: `data_source` (`arrow_postgres`, `daft_postgres`) x `organizer` (`arrow`, `daft`) x `executor` (`python`, `ray_task`, `ray_actor`) x `writeback_mode` (`none`, `json_text`). All 24 rows returned `status=ok`; all `json_text` rows wrote `written_rows=4`.
- This is a local Ollama completion smoke. It does not replace the future vLLM-compatible `/v1/completions` path and is not a token-aware/prefix-aware batching result.

## 2026-07-18 AI_COMPLETE runtime skeleton

- Added `--operator ai_embed|ai_complete` to `code/scripts/postgres_ai_operator_profile.py`; default remains `ai_embed`.
- Extended `code/src/model_backends.py` with fake and vLLM-compatible `/v1/completions` completion backends.
- Extended `code/src/sinks.py` with `write_completions` and added `document_completions` to the local schema.
- `AI_COMPLETE` supports `none/json_text` writeback. `pgvector` remains embedding-only and is rejected for `AI_COMPLETE`.
- Added Ray worker `PYTHONPATH` runtime env so Ray task/actor workers can import `code/src` modules after the runtime split.
- Verified PG18.4 local smoke under `tmp/postgres_ai_complete_fake_smoke.csv`: fake `AI_COMPLETE` completed with `total_rows=16`, JSON-text writeback completed with `written_rows=8`, and `ray_task` completed with `status=ok`. This is a local function smoke, not a vLLM performance result or token-aware batching conclusion. The Windows Ray run printed a raylet shutdown access-violation warning after producing the result row.

## 2026-07-18 Runtime code boundary cleanup

- Split reusable runtime helpers out of `code/scripts/postgres_ai_operator_profile.py`:
  - `code/src/model_backends.py`: fake debug embedding backend and compatible HTTP embedding backend.
  - `code/src/sinks.py`: existing `none/json_text/pgvector` PostgreSQL writeback.
  - `code/src/metrics.py`: stage timer, GPU snapshot, and CSV append helper.
- Kept `fake` only as an offline smoke/control backend. vLLM-compatible runs should use `--model-backend compatible_http`; `http_openai` remains accepted as a compatibility alias.
- Added `code/tests/test_model_backends.py` and `code/tests/test_sinks.py`.
- Updated `code/README.md`, `code/scripts/README.md`, and `PROJECT_INDEX.md` with the new code boundaries.

## 2026-07-17 Daft PostgreSQL data entry implementation

- Added `code/src/sources.py` with `PostgresArrowSource` and `DaftPostgresSource`, plus `code/tests/test_sources.py`.
- Updated `code/scripts/postgres_ai_operator_profile.py` with `--data-source arrow_postgres|daft_postgres`; default remains `arrow_postgres`.
- Kept writeback unchanged: `none/json_text/pgvector`. Lance remains a future optional sink and is not implemented in this step.
- Added Daft SQL runtime dependencies `sqlglot` and `connectorx` to `code/requirements.txt`.
- Verified local PG18.4 smoke under `tmp/postgres_daft_source_e2e.csv`: `source_arrow_smoke` and `source_daft_smoke` both completed with `total_rows=64` and `object_count=4`. This is a local smoke result, not a formal performance conclusion.

## 2026-07-17 Superpowers implementation plan for Daft PostgreSQL data entry

- **触发**：用户要求使用 `superpowers:brainstorming` / `superpowers:writing-plans` 构思后续代码，并明确当前写回按既有方案，Lance 仅作为后续可能方向。
- **新增**：
  - `code_doc/README.md`
  - `code_doc/superpowers/README.md`
  - `code_doc/superpowers/plans/README.md`
  - `code_doc/superpowers/plans/2026-07-17-daft-postgres-entry-existing-writeback.md`
- **范围**：计划聚焦 Daft 作为 PostgreSQL data entry；当前 writeback 保持 `none/json_text/pgvector`；Lance 仅作为 future optional sink，不进入本轮实现。

## 2026-07-17 Daft 文本 DataOrganizer smoke 接入

- **触发**：用户要求实际使用 Daft，并要求遵循 `karpathy-guidelines`、保证代码可维护性。
- **实现**：
  - 新增 `code/src/organizers.py`，实现 `ArrowOrganizer` 与 `DaftOrganizer`。两者接收 Arrow table，输出 downstream 可复用的 Arrow batch 列表和指标。
  - 新增 `code/scripts/daft_text_organizer_smoke.py`，通过 `--organizer arrow|daft` 验证 `rows -> Arrow Table -> organizer -> batches`，并支持显式 `--runner ray` 检查 Daft `into_partitions`。
  - 更新 `code/scripts/postgres_ai_operator_profile.py`：主链路的 `fetch_record_batch + split_batch` 已替换为 organizer 后端选择，新增 `--organizer arrow|daft`、`--organizer-partition-mode`、`--organizer-partitions`、`--daft-runner`。默认仍为 `arrow`，保留旧路径作为 baseline。
  - 新增 `code/tests/test_organizers.py`，覆盖 Arrow 后端和 Daft native 后端的 batch 输出一致性。
  - 更新 `code/requirements.txt`：新增 `daft`，并将 `pyarrow` 约束为 `>=16,<25`，匹配 Daft 0.7.20 的依赖边界。
  - 更新 `code/README.md`、`code/scripts/README.md`、`PROJECT_INDEX.md`，登记新增入口和运行命令。
- **本地验证**：
  - NativeRunner：`--rows 256 --batch-size 64` 生成 4 个 64 行 batch。
  - Ray runner：`--runner ray --rows 32 --batch-size 8 --partition-mode into_partitions --partitions 4` 生成 4 个 8 行 batch。
- **边界**：主脚本已具备 Daft organizer 后端选择，但这仍不是正式性能实验；真实 PostgreSQL/vLLM/GPU-backed 结论需要后续 E2E 运行数据。

## 2026-07-17 多模态正文实验 + Daft 文本阶段直接接入 + 优化空间扩展

- **触发**：与导师讨论后明确多模态实验进入正文（§5.3 策略泛化性验证），不是仅 Discussion；用户确认 Daft 从文本阶段直接接入（不再经过 Arrow 中间态）；用户明确"参数优化也可以作为贡献"。
- **六项关键决策**：
  1. **多模态正式进入正文**：在图像 workload（ImageNet/HuggingFace，CLIP + Qwen2.5-VL）上使用同一套策略代码，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。VLM 生成实验标记为 optional。
  2. **Daft 文本阶段直接接入**：取消 Arrow→Daft 过渡方案。Daft DataFrame API 对文本（`df["prompt"]`）和图像（`df["image"]`）是同一套接口，后续多模态实验只需替换列类型。
  3. **优化空间扩展为"策略级 + 引擎级"双层**：策略级（token-budget、queue-adaptive flush、routing）+ 引擎级（Daft `into_batches`/`batch_size`/`max_concurrency`/`gpus`/`repartition`）。论文贡献不是"发明了某个 knob"，而是"在数据库 AI 算子外部执行场景中系统表征了优化空间 + 提出了策略级决策方法 + 跨模态验证"。
  4. **算子代价估计定位**：作为 §6.1 补充讨论（不作为独立研究内容），基于实验阶段已采集的 profile 数据，不新增实验。
  5. **完整优化实验清单建立**：P0（batch 粒度/分组策略/提交节奏 3 消融）→ P1（Daft 引擎参数 + 耦合验证）→ P2（多模态泛化 + 算子代价估计）。
  6. **Scope 缩减触发条件写死**：Month 1 无 vLLM baseline → 多模态降 Discussion；文本 RC1+RC2 未完成前不启动多模态 pipeline；VLM 生成始终 optional。
  7. **写回降级**：写回不作为独立研究内容或实验阶段，降为实验设置中的工程细节。PostgreSQL + pgvector + COPY + deferred index 作为默认写回路径。
- **idea-evaluator 重评估结果**：Accept with Revisions（两个 MAJOR 但可防御）。评分：Higher 8, Faster 8, Stronger 8, Cheaper 6, Broader 7。范式转移潜力 possible（3.5/4）。最大风险是单 GPU 限制多 endpoint 并行实验深度 + 串行依赖过多导致周期膨胀（有 scope 缩减触发条件）。
- **更新文件**：
  - `AGENTS.md` §1/§2/§3 — 新增 Daft + 多模态 + 算子代价估计 + scope 缩减条件
  - `PROJECT_OUTLINE.md` — 研究内容扩展为 5 项、近期优先级重排、新增 scope 缩减条件
  - `research/knowledge_hub.md` §10.5.1 — 重写为"Daft 文本阶段直接接入 + 优化空间三层框架 + 完整实验清单"
  - `experiments/plans/strategy_design_implementation_reference.md` — 此前已完成口径统一（三层策略 → 两项策略 + 验证），§4.2 已更新 Daft 引擎抽象
- **涉及文件**：`AGENTS.md`, `PROJECT_OUTLINE.md`, `research/knowledge_hub.md`, `experiments/plans/strategy_design_implementation_reference.md`

## 2026-07-17 Daft+Ray 多模态与具身智能调研

- **新建** `research/daft_ray_multimodal_reference.md`：Daft+Ray 多模态执行引擎技术手册，涵盖 Swordfish 流式引擎、Flotilla 分布式架构、@daft.cls GPU UDF 机制、与具身智能的连接、及与本课题的关系分析。
- **更新** `research/knowledge_hub.md`：新增 §10 "Daft+Ray 多模态执行引擎与具身智能负载"，含架构对比、Snowflake Cortex 多模态 AI 算子、具身智能管线、及与本课题的互补关系论证。
- **更新** `research/ai_operator_literature_inventory.md`：新增 8 篇文献（Daft SciPy Talk、Ray Data Streaming Batch、Flotilla、@daft.cls、Snowflake Cortex Multimodal、阿里云 EMR Daft 具身智能、IBM 具身数据缺口、HeteroHub），总数 57→65 篇。
- **核心结论**：
  1. Daft+Ray 优化引擎层的物理资源调度（CPU/GPU 重叠、内存管理），本课题优化策略层的调度决策（按什么规则组 batch、按什么节奏发请求）——两者互补而非竞争。
  2. Snowflake Cortex 已 GA 多模态 AI SQL 算子，数据库 AI 算子处理多模态数据是工业现实。
  3. 本课题的调度策略框架（token-budget→frame-budget、queue-adaptive flush、actor pool 路由）对多模态负载具有自然泛化能力。
  4. 建议在论文 Discussion (§6) 中以具身智能为 generalization case，不做主实验。
- **涉及文件**：`research/knowledge_hub.md`, `research/daft_ray_multimodal_reference.md`, `research/ai_operator_literature_inventory.md`

## 2026-07-16 推理管线交互文献系统性收集

- **新建** `research/inference_pipeline_interaction_literature.md`：系统性搜索和收集 28 篇 CCF-A 论文、技术报告和工业系统文档。
- **覆盖五个方向**：
  1. LLM 推理服务与连续批处理（vLLM, Orca, Sarathi-Serve, FastServe, DistServe, Splitwise, Mooncake, S-LoRA）
  2. 自适应批处理与推理服务调度（Clipper, Nexus, Clockwork, Triton）
  3. 数据管线与推理服务交互（Ray Data Streaming Batch, Ray Data LLM integration, NeuStream, HedraRAG）
  4. Token/Prefix-Aware 优化（Parrot, SGLang, KVFlow, ChunkAttention, EPIC）
  5. Ray-Specific 推理服务模式（Ray Serve LLM, Ray Compiled Graphs）
- **核心发现**：确认存在研究空白——无任何已有工作系统性研究"上游数据管线 batch 参数（batch_size, partition_count, concurrency, token-aware/prefix-aware 分组）如何影响下游推理引擎 continuous batching 效率及最优协调策略"。
- **最新 2026 论文**：收录 BatchLLM (MLSys 2026)、PKAS (HPDC 2026)、PLA-Serve (MLSys 2026)、Load-Aware Prefill Deflection、PEACE 等。
- `research/README.md` 和 `PROJECT_INDEX.md` 已在先前 session 预添加了该文件的索引条目。

## 2026-07-16 方向重大调整：AI_COMPLETE 为主线 + 上游动态 Batching + Ray 架构设计空间

- **触发**：用户明确 AI_COMPLETE（生成式 LLM 推理）才是真正目标场景，AI_EMBED 只是"能跑的先跑"的过渡；用户不想要静态 batch，希望借鉴 vLLM continuous batching 思路做上游动态 batching，并充分利用 Ray 的 actor 灵活性做架构设计。
- **方向调整（七项共识）**：
  1. **RC3 降格**：从"研究内容三"降为"端到端验证实验：写回瓶颈判定"，只使用当前最优写回方法（COPY + deferred index）
  2. **"协同"操作化定义**：协同 = 上游数据组织的"形状"（batch token 分布）和提交"节奏"（K_max、queue-adaptive flush）共同影响下游 vLLM continuous batching 的调度效率。不再是模糊的"跨层协同"。
  3. **vLLM 重定位**：vLLM 不是竞争对手，是部署平台 + baseline。课题研究"在 vLLM continuous batching 之上，上游 Ray 数据执行层如何最优地组织请求"。
  4. **AI_COMPLETE 为主线**：AI_EMBED 降为预研验证；AI_COMPLETE（生成式 LLM）成为论文主体 workload，引入 token 长度分布、shared prefix、TTFT/TPOT、generation straggler 等更丰富的交互变量
  5. **上游动态 Batching（借鉴 Continuous Batching 原理）**：计划层不再是静态选择 `batch_size`，而是设计动态 batching policy——token-budget batching（类似 vLLM `max_num_batched_tokens`）、length-aligned grouping、prefix-aware grouping
  6. **Ray 作为架构设计空间**：不是只把 Ray 当 task executor，而是利用 actor 异构化（ShortTokenActor / LongTokenActor / PrefixAffinityActor）、运行时自适应（queue-adaptive flush）、去中心化协调（每个 actor 自主决策）、actor pool 分池路由等架构设计杠杆
  7. **耦合验证前置**：独立最优拼接 vs 联合 grid search 作为第一个关键消融实验；无交互效应时 fallback 为"分层独立优化框架"，仍为合格硕士论文
- **文献确认**：多源检索确认无 CCF-A 论文研究"上游数据管道 batch 参数 × 下游 continuous batching 性能"这一交叉点，研究空白判断成立。
- **用户三层划分**：模型结构层（GQA/MQA）→ 计算执行层（Flash-Attention）→ 服务部署层（PagedAttention + In-Flight-Batching）。课题聚焦层级 3，前两层为模型/实现选型，不进入优化范围。
- **需同步更新**：`AGENTS.md` §1/§2/§3/§5、`experiments/plans/strategy_design_literature_basis.md` §7、`motivation/plans/workloads.md`、`opening/report/opening_report.md`、`PROJECT_OUTLINE.md`
- **注意**：同期三个评估 skill（idea-evaluator / ars-reviewer / nature-reviewer）接收的是旧 framing（AI_EMBED + 静态 batch）；新 framing（AI_COMPLETE + 动态 batch + Ray 架构）更强。评估结果到达后应做 framing 对比再最终确认。

## 2026-07-15 开题报告移除 fake/CPU 主文证据

- 根据当前已经完成 pgai SQL 触发面集成和真实 GPU-backed `AI_EMBED` 完整链路复测的事实，更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`。
- 删除 4.2 中历史 fake/CPU 预研图、表和相关表述，避免读者误解课题仍停留在 toy/fake benchmark 阶段。
- 4.2 可行性证据现在只保留 PG18.4 + pgvector 环境、GPU-backed `AI_EMBED` 链路和双 endpoint Ray 动机测试；调优变量依据改为文献机制 + 当前真实 GPU-backed 复测。
- 已覆盖同步新版开题报告飞书 docx，并重新插入 8 张正式 PNG；回读确认 revision 更新到 `72`，未检出 fake/CPU、图 4-7、表 4-4、Mermaid 旧图或本地 `figures/` 路径残留。

## 2026-07-15 开题报告飞书新版 docx 同步

- 使用 user 身份将 `opening/feishu/opening_report_wiki.md` 覆盖同步到新版开题报告飞书 docx：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`。
- 覆盖写入后飞书返回 `partial_success`，原因是 Markdown 中的本地图片路径不能直接导入为图片资源；随后逐张上传并插入 8 张 PNG：研究缺口图、总体研究框架图、三层上游执行策略图、运行时策略闭环图、粒度对比图、阶段时延图、endpoint 对比图、pgvector 写回对比图。
- 回读线上文档确认 revision 更新到 `51`，关键图注附近为真实飞书图片 URL；关键词检查未发现本地 `figures/` 路径和旧的“三岛/Killer/联合最优/边界确认/阶段画像/Ours-v0”等表述残留。

## 2026-07-15 策略设计与实现参考沉淀

- 新增 `experiments/plans/strategy_design_implementation_reference.md`，把 Ray OSDI 2018、Ray Data / Ray Serve、vLLM / Orca、Triton、GPU 数据放置和 DB AI 算子文献机制沉淀为三层策略参考。
- 明确三层策略：计划层负责数据库侧 `batch_size` / `partition_count` / `object_merge`；运行层负责 `K_max` / routing / backpressure / actor pool；服务端层负责 dynamic / continuous `micro-batch`。
- 文档同时给出每层可观测信号、可调变量、实现边界、实验指标、baseline 顺序和实现优先级，供后续实验设计和原型实现使用。
- 进一步补充“系统优化蓝图”和“机制到实现任务优先级”，将 Workload Profiler、Plan-time Data Organizer、Ray Admission Controller、Endpoint Router、Service-side Micro-batcher、E2E Guardrail 拆成可实现模块，并给出每个模块的借鉴来源、最小实现、验证问题和放弃条件。
- 同步更新 `experiments/plans/README.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`。

## 2026-07-15 GPU 调度与数据放置补充调研

- 新增 `research/gpu_scheduler_data_placement_supplement_20260715.md`，补充 GPU / LLM 推理调度、异构数据管线、GPU 数据库算子、GPU-resident 数据放置和数据库 AI 算子几条文献线索。
- 明确当前策略不应写成“重新发明 GPU scheduler”或“改造 Ray 调度器”，而是位于数据库外部执行链路和模型服务入口之间的轻量级 runtime strategy controller。
- 同步 `opening/README.md`、`opening/literature/reading_list.md` 和 `PROJECT_INDEX.md`，将该补充调研纳入开题文献入口。

## 2026-07-15 策略设计重新评判与三层收窄

- 根据用户反馈和补充调研，将策略从“全运行时控制器”收窄为 three-layer upstream execution strategy：计划层在执行前选择 `batch_size` / `partition_count` / `object_merge`，运行层调整 `K_max`、routing、backpressure，服务端用 dynamic / continuous batching 形成推理 `micro-batch`。
- 明确当前不采用“运行时重切数据库侧已物化 RecordBatch”作为主方案；动态 batch 借鉴 vLLM / Ray Serve / Triton 思路，放在模型服务侧尚未执行的请求队列中。
- 补充 Ray OSDI 2018 调度思想映射：task/actor、resource-aware scheduling、local/global scheduler、object store locality 和 actor pool 可迁移为 task 粒度、actor 池、资源约束、placement/locality、`K_max` 与 routing 等实验变量。
- 同步更新 `figures/scripts/generate_runtime_strategy_control_loop.py`、`figures/audit/runtime_strategy_control_loop_audit.md`、`figures/audit/top_venue_strategy_figure_design_notes.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`；重新生成 PNG/SVG 并通过边框、箭头和禁用术语自检。

## 2026-07-16 实验计划与开题报告同步更新

- **开题报告对齐实验计划**：6 项修改——
  1. §4.1 新增 Killer Experiment 六组对照（BL1-BL6）定义，明确核心 claim 的验证条件
  2. §4.2 新增"合理默认 vs 诊断工具"区分：逐行调用和无界 in-flight 仅作为诊断工具，不作为论文 §7 方法对照 baseline
  3. §3.2 研究内容三 扩展：提及 B 系列工程实验和三路写回架构对比（driver / worker-direct / queue-worker）
  4. §4.1 末尾新增 FILTER/COMPLETE 模拟 workload 诚实声明（参照 Orca 合成权重做法）
  5. §6 新增统计严谨性指标（中位数、IQR、重复次数、Ray 重启、随机种子）
  6. §2.3 补上 ColStorEval[50] 引用
- **PROJECT_INDEX.md 同步更新**：§3 新增四个实验计划文件列表、§7 更新研究内容三标题、§8 更新下一步优先级（P0/P1/P2 结构）
- **PROJECT_OUTLINE.md 同步更新**：研究内容三标题降级为"边界分析与轻量写回优化"、近期优先级改为 P0/P1/P2 三阶段
- 上述修改使开题报告与 `experiments/plans/` 下四份实验计划在 BL 矩阵、baseline 分级、统计规范和 workload 标注上口径一致

## 2026-07-16 实验计划六项评估方法论修正

- **修正四个实验计划文件**，统一遵循从 vLLM/Orca/TurboVecDB/GaussML/FlexPushdownDB 五篇 CCF-A 论文提取的六项评估标准：
  1. **前置依赖声明**（§0）：每个文件写明 P0 必须先完成 vLLM + B 系列，否则所有 baseline 是 suboptimal
  2. **假设先行**（§2）：每个实验段在参数矩阵之前先写"要推翻什么假设"，不是盲目扫参——每个假设标注对应实验段和推翻后的含义
  3. **模型 batch scaling 前置实验**（研究内容一 §4）：在讨论 batch_size 选择之前，先跑模型自身的 batch_size→吞吐曲线
  4. **FILTER/COMPLETE 诚实标注**（研究内容一/二 §3）：标注为 simulated workload（参照 Orca 合成权重的做法）
  5. **统计规范**（各文件 §10）：重复次数、中位数（不取平均值）、IQR、Ray 状态重置、warm-up 策略、随机种子——全部标准化
  6. **可验证边界**（各文件 §11）："When does it NOT help?" 的每个边界条件对应一个可跑实验点，不是空洞自省
- 修改文件：`data_organization_batching.md`（重写）、`service_scheduling_backpressure.md`（新增 §0/§2/§10，修正 §9/§11）、`sink_writeback_coordination.md`（新增 §0/§2/§10，修正 §9）、`cross_layer_killer_experiment.md`（新增 §0/§2）

## 2026-07-16 实验计划骨架填充 + 评估方法论标准化

- **四个实验计划文件新建**：`experiments/plans/` 下三份研究内容实验计划 + 一份跨层 Killer Experiment 计划。
  - `data_organization_batching.md`（研究内容一）：Grid search、workload 对比、selectivity-aware 策略、模型 batch scaling 前置实验
  - `service_scheduling_backpressure.md`（研究内容二）：K_max sweep、routing 策略、adaptive vs static K_max、vLLM baseline 前置实验
  - `sink_writeback_coordination.md`（研究内容三）：B 系列工程 baseline（UPSERT vs COPY, logged vs unlogged, online vs deferred index）、三路架构对比、sink 对照
  - `cross_layer_killer_experiment.md`（跨层核心）：BL1-BL4 + 联合方案的完整矩阵、代价模型 R²、消融瀑布、跨 workload 泛化、统计严谨性要求
- **评估方法论标准化**：所有四个计划遵循从 vLLM (SOSP 2023)、Orca (OSDI 2022)、TurboVecDB (VLDB 2025)、GaussML (ICDE 2024)、FlexPushdownDB (VLDB 2021) 五篇 CCF-A 论文提取的共同原则——曲线 > 单点、先暴露瓶颈再优化、同硬件公平 baseline、消融拆开、诚实报告边界、统计严谨。
- **实验前置依赖明确**：P0 必须先跑 vLLM 接入 + B 系列写回工程 baseline，否则所有 Grid Search 都基于 suboptimal baseline。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 Baseline 分级重构：移除 strawman C 级

- **Baseline 分级重构**：`experiments/plans/research_design_catalog.md` §10.1-§10.4。
  - 移除 "C 级（Naive）"——row-by-row 调用、无界 in-flight 等故意劣化配置降级为"诊断工具"，只用于 §4 理解瓶颈机制，不作为 baseline 对照。
  - "合理默认配置"（coalesced batch=64、driver 写回）取代旧 C 级作为 §4 动机展示的参照点——这是正常工程师会写的第一版代码，不是 strawman。
  - S/A/B 三级保留：S（文献最优）→ A（工程最优）→ B（单维调优）。§7 方法对照至少包含 A 级。
  - §10.1 新增原则 5："动机展示不用 strawman"。§10.3 检查清单新增两条防 strawman 项。
  - A2.1 baseline 描述从 "Unbounded in-flight" 改为 "Ray 默认行为（无显式 K_max）"。
- **未同步到其他文件**：`baseline_reference.md`、`AGENTS.md` 和 `experiments/plans/README.md` 的 strawman 相关措辞已经合理，无需修改。

## 2026-07-15 研究方案候选目录 + Baseline 设计考量

- **新增研究方案候选目录**：`experiments/plans/research_design_catalog.md`，覆盖三个研究内容和跨层协同优化的 28 个候选方案，每个方案在六个维度（文献支撑、工程可行性、硬件可行性、开源依赖、创新空间、实验可验证性）上评分。
- **方案来源**：基于 57 篇文献清单 + 2026 年 7 月前沿检索（Ray Serve 2025 Custom Router/Async Inference/Autoscaling、NexusSched 两层调度、Multi-Bin Batching 队列理论、MAB 反馈控制、GFS/DARIS 优先级抢占、Arrow Flight Ballista/Spark SPIP、Iceberg v3 Deletion Vectors、COSTREAM/CONCERTO/GRACEFUL Learned Cost Models）。
- **Baseline 设计考量**：目录第 10 节为每个候选方案指定了对应 baseline（文献最优 S 级 / 工程最优 A 级 / 常见实践 B 级 / Naive C 级），并给出实现优先级（P0: COPY deferred index + vLLM 接入）。
- **分阶段路线图**：Phase 0-4，覆盖 2026-07 至 2026-10，Phase 3 的 Killer Experiment（BL1-BL6）是论文核心 claim 的验证点。
- **风险分析**：6 项风险（vLLM 消除外部调度收益 / 单 GPU 限制 / 写回优化边际 / workload 扩展 / Joint Opt 增量 < 10% / PG18.3 平台），每项标注了反证条件。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 写回文献调研 + Baseline 矩阵 + 文献优先设计规则

- **文献清单 v3**：`research/ai_operator_literature_inventory.md` 从 45 篇扩充至 57 篇，新增写回/持久化方向 12 篇 CCF-A 文献（第六组精读 + E 组补充）。
- **新增实验 Baseline 参考矩阵**：`experiments/plans/baseline_reference.md`，覆盖 GPU 调度侧（6 个）、写回侧（7 个）、数据组织侧（4 个）、跨层决策侧（3 个），所有 baseline 标注来源论文/系统。
- **新增文献优先设计规则（§6.5）**：根 `AGENTS.md` 加入"系统/算法/实验方案设计时，优先从 CCF-A 文献提取设计模式"的规则。完整方法论写入 `research/README.md` §文献优先设计方法论。
- **idea-evaluator 评估**：课题方向 Accept with Revisions，无 CRITICAL 缺陷，paradigm-shift probe 4/4 yes。五项调整建议已记录在对话中。
- 同步更新：`AGENTS.md` §6.5、`research/README.md`、`experiments/plans/README.md`、`experiments/plans/baseline_reference.md`（新建）。

## 2026-07-13 制图脚本目录归位

- 将 `code/scripts/make_chain_breakdown_figures.py` 迁移到 `figures/scripts/make_chain_breakdown_figures.py`。
- 明确 `code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口；绘图、图表复现和素材筛选脚本统一放入 `figures/scripts/`。
- 同步更新 `code/AGENTS.md`、`code/README.md`、`code/scripts/README.md`、`figures/scripts/README.md` 和 `figures/learning/README.md`，避免后续实验代码目录与图资产目录混用。

## 2026-07-13 图资产规则沉淀

- 根据今天系统架构图和实验数据图的多轮修改经验，扩展 `figures/AGENTS.md` 为项目级图表长期规则文件。
- 规则中明确：论文级核心图先用 `figure-designer` 判断图型和版式；投稿/论文级质检可用 `nature-figure`；报告转 PPT 可用 `nature-paper2ppt` 或 `ppt-master`；实验数据图优先用 Python + Matplotlib / Seaborn 从 CSV 可复现生成。
- 补充系统架构图常见返工点：箭头遮挡、编号越界、模块未对齐、框内内容不规整、观测层和执行层割裂、图文术语不一致。
- 补充实验图规则：哪些数据值得画图，哪些只适合表格或文字；正式图必须标注数据来源、warm-up 处理、证据层级和不能声称的结论。
- 在 `PROJECT_INDEX.md` 顶部补充图资产规则入口，提醒后续新增、修改、迁移或审查图表前先读 `figures/AGENTS.md`。

## 2026-07-13 项目目录一致性复核

- 复核根目录、`overview/`、`research/`、`motivation/`、`learning/`、`opening/` 和 `figures/` 中与当前开题方向相关的入口文件。
- 将 `overview/project_outline.md` 从旧的“数据库内置 AI 算子外部执行链路”口径重写为“数据库驱动 AI 工作负载的分布式数据执行与存储协同优化”口径。
- 同步更新 `AGENTS.md`、`PROJECT_INDEX.md`、`research/literature_and_evidence_review.md`、`research/existing_ai_operator_execution_chains.md`、`motivation/plans/integration.md` 和 `motivation/results/README.md` 中的旧表述。
- 对 `motivation/results/pg18_4_fake/system_profile.md` 与 `motivation/results/fake_cpu/analysis.md` 增加当前口径说明，保留历史实验语境，但明确真实瓶颈归因应优先引用 GPU-backed 结果。
- 本次复核只调整会影响项目规划、阅读入口和方向判断的文件；历史日志和旧实验过程记录不做大面积改写。

本文件记录项目级简要操作，便于日后复盘方向、入口和关键材料调整。详细实验日志仍放在对应结果目录；开题材料的详细修改记录见 `opening/logs/project_log.md`。

## 2026-07-13 开题主线调整为数据库驱动 AI workload

- 根据用户确认的判断，将开题题目从“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”调整为“面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究”。
- 同步更新项目级方向口径：数据库 AI 算子主要作为 workload 入口和验证场景，研究主体调整为 Daft/Arrow 数据组织、Ray 执行调度、GPU 模型服务和 Lance / pgvector / PostgreSQL sink 之间的数据执行与存储协同。
- 同步修改 `README.md`、`PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`AGENTS.md`、`overview/current_direction_and_plan.md`、`motivation/plans/integration.md` 以及 opening 相关源稿，避免项目规划与开题报告割裂。
- 已生成新的本地飞书源稿 `opening/feishu/opening_report_wiki.md`；飞书写入时 `lark-cli` 在用户目录刷新锁文件处返回 `Access is denied`，提升权限重试被自动审批拒绝，需后续获得权限后再同步线上 wiki。

## 2026-07-12 根目录总纲与项目日志

- 新增 `PROJECT_OUTLINE.md`，作为根目录项目总纲入口，汇总当前题目、研究内容、实验主线、关键证据、近期优先级和同步规则。
- 新增 `PROJECT_LOG.md`，作为项目级简要操作日志，用于记录跨目录、影响项目方向或入口结构的调整。
- 后续如果开题报告、实验主线、项目方向或关键入口发生变化，需要同步更新 `PROJECT_OUTLINE.md` 和本日志。

## 2026-07-12 实验主线入口调整

- 将项目实验主线入口从 `feasibility/guide.md` 调整到 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md`。
- 明确 `feasibility/` 只负责组件、环境和脚本可用性验证，不承担当前实验大纲、开题主线或 GPU-backed 性能结论职责。

## 2026-07-12 开题与项目规划双向同步

- 明确开题报告和项目规划不是单向关系：开题报告基于项目进展撰写；开题题目、研究内容、技术路线或侧重点调整后，也会反向影响项目规划、实验优先级和对外口径。
- 项目入口文档需要与 `opening/report/opening_report.md` 保持一致，不能长期出现不同方向。

## 2026-07-12 开题报告与飞书内容复核

- 按当前 `PROJECT_OUTLINE.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md` 复核开题报告与飞书源稿。
- 确认 `opening/report/opening_report.md` 当前主线基本合适：正式证据优先引用真实 GPU-backed 结果，PG18.4 / fake / CPU 结果有边界说明。
- 清理 `opening/feishu/opening_report_wiki.md` 的本地源稿说明，避免发布到飞书后出现工作流元话语。
- 补充飞书后续计划：后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地同构预演写成正式平台结论。
- 修正 `motivation/results/README.md` 中 GPU-backed 结果入口的过时措辞。

## 2026-07-12 实验结论写作标准

- 根据用户反馈，将 `learning/AGENTS.md` 的实验讲解标准提升为项目级实验结论写作参照。
- 更新 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md` 和 `opening/work_rules.md`，要求实验结论、数据分析、开题可行性分析和飞书实验摘要都说明实验目的、链路流程、参数含义、数据来源、结果读法、不能证明什么、结论类型和下一步验证。
- 后续正式报告可以比学习材料更凝练，但结论边界和分析精细程度不能低于 `learning/AGENTS.md` 的要求。

## 2026-07-12 开题实验飞书页与 PPT 生成

- 新增 `opening/feishu/motivation_feasibility_wiki.md`，按真实 GPU-backed 证据、fake/CPU 历史预研、可行性验证边界和下一步实验组织动机测试与可行性测试内容。
- 使用 user 身份覆盖写入动机测试与可行性测试飞书 wiki：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`，飞书返回成功并生成 5 个 Mermaid whiteboard。
- 基于学校 PPT 模板生成开题汇报 PPTX：`opening/slides/opening_defense_20260712.pptx`，内容来自开题报告、GPU-backed 动机实验和当前项目总纲。
- 已将 PPTX 以 user 身份导入为飞书在线幻灯片：`https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg`。
# 2026-07-14 pgvector(384) writeback comparison

- Updated `code/scripts/postgres_ai_operator_profile.py` so `--setup --embedding-dim 384` creates `document_embeddings.embedding_vector` as `vector(384)`.
- Ran the same GPU-backed Ray actor chain for no writeback, JSON text writeback, and pgvector `vector(384)` writeback.
- Added result report and CSV under `motivation/results/gpu/`.
- Added report-main figure `figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png`.
- Updated opening report, learning walkthrough, figure indexes, and result indexes. Boundary: PG18.4 local rehearsal, not PostgreSQL 18.3 internal platform.

# 2026-07-14 合并 agent/postgres18-local-profile 分支并全项目校准

- 将 `origin/agent/postgres18-local-profile` 合并到 `main`，恢复 `opening/` 开题材料目录。
- 分支带来的重构：`validation/` → `feasibility/`，`motivation/` 脚本 → `benchmarks/`、设计文档 → `plans/`、结果按 `fake_cpu/cpu/gpu/pg18_4_fake` 分类。
- 新增目录：`deploy/`、`experiments/`、`figures/`、`learning/`、`opening/`、`projects/`。
- 创建 `CLAUDE.md` 作为 Claude Code 环境规则入口，导入全部 `AGENTS.md`。
- 全项目文档路径校准（12 个文件）：
  - 根 `AGENTS.md`：§3 证据更新为 GPU-backed 结果，§4 目录加新结构，§5 实验规则更新。
  - 根 `README.md`：目录树重写、标题对齐 `PROJECT_OUTLINE.md`、证据和运行命令更新。
  - `PROJECT_INDEX.md`：全文重写，所有路径更新，新目录入口，当前证据优先级。
  - `overview/current_direction_and_plan.md`、`overview/project_outline.md`：加弃用声明，指向根 `PROJECT_OUTLINE.md`。
  - `motivation/results/README.md`：从扁平文件列表重写为子目录结构。
  - `feasibility/benchmarks/README.md`：命令路径和脚本引用全部更新。
  - `opening/ppt_rules.md`：图表规则重写，引用 `figures/` 为权威来源，Python+Matplotlib 优先于 ECharts。
  - `opening/work_rules.md`：过期引用更新。
  - `experiments/AGENTS.md`：新增 `karpathy-guidelines` 和图表 skill 引用。
- 镜像同步规则：`CLAUDE.md` 和 `AGENTS.md` §9 包含相同的 6 行变更→更新清单，互相指向对方。

# 2026-07-15 开题报告研究方案图补充

- 将 `figures/architecture/cross_layer_method_framework.png` / `.svg` 调整为研究方案图，明确三类数据库 AI 算子、阶段画像、数据组织策略、模型服务调度策略、联合调优验证和写回瓶颈判定实验。
- 重绘 workload 区块为三张卡片：场景名、SQL 算子名、调度压力三行排版；图中移除 `RC` / `BL` 缩写、`Workload 入口`、`边界确认` 和未解释的 `vs` 表达。
- 在 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 的 Killer Experiment 段落后插入该图作为图 4-1，并顺延后续第 4 章图号。
- 新增 `figures/audit/cross_layer_method_framework_audit.md`，并更新 `figures/README.md` 的正式图资产说明。

# 2026-07-15 研究方案图作图规则同步

- 将研究方案图的版式和审查经验同步到 `figures/AGENTS.md`：方案图必须回答“我要做什么”，并按 workload、阶段画像、策略设计、联合验证和写回瓶颈判定组织。
- 明确禁止在正式可见图中使用 `RC/BL` 内部缩写、未解释的 `vs`、`边界确认` 等模糊标签；workload 区块优先使用“三行卡片”排版。
- 补充遮挡和越界检查要求：卡片边框必须完整可见，文字不得裁切，生成后同时执行程序化像素/关键词残留检查和人工 PNG 预览。
- 同步更新 `opening/ppt_rules.md`，要求 PPT 中的研究方案图也遵守同一套语义和排版规则。

# 2026-07-15 开题主线调整为上游链路调优与端到端效果评估

- 根据用户确认，将开题主叙事从“独立最优组合 vs 跨层联合最优”调整为“上游执行链路调优 + 端到端效果评估”：优化侧重点在数据组织与模型服务调度，尤其是模型服务状态感知调度；写回纳入端到端效果评价。
- 更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`：研究路线改为“分阶段性能剖析 -> 上游执行链路调优 -> 加入写回的全链路验证 -> 多 workload 验证”，并将独立最优拼装对照降级为阶段间耦合明显时的增强对照。
- 更新 `figures/architecture/cross_layer_method_framework.*`：中心卡片改为“上游执行链路调优”，评价标准改为加入写回后的端到端耗时、吞吐、排队和写回占比整体改善。
- 同步调整 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`experiments/plans/` 和 `figures/audit/` 中的入口说明，避免把跨层联合优化写成当前唯一核心 claim。

# 2026-07-15 上游执行链路策略设计图

- 新增 `figures/architecture/upstream_strategy_design.png` / `.svg`，用于说明阶段画像之后的已定位瓶颈如何转化为数据组织优化、模型服务调度、写回约束处理、执行配置与端到端验证。
- 新增 `figures/scripts/generate_upstream_strategy_design.py`，统一生成 PNG/SVG，并检查所有核心卡片边界和箭头是否越界或穿过无关卡片。
- 新增 `figures/audit/upstream_strategy_design_audit.md`，记录该图不声称最终 learned optimizer，而采用“已定位瓶颈 -> 优化动作 -> 执行配置 -> 端到端验证”的保守方法图定位。

# 2026-07-15 策略设计文献依据与边界

- 新增 `experiments/plans/strategy_design_literature_basis.md`，将策略设计从文献中站住：区分 Cortex/Smart、vLLM/Orca、Ray/Daft、COPY/pgai/Delta/TurboVecDB 等工作中可借鉴的优化思想、只能作为 baseline/边界的部分，以及本文自己的上游执行链路策略定义。
- 明确当前策略推荐写成 “workload-aware 数据组织 + 模型服务状态感知调度 + 写回约束验证”，不提前声称 finalized learned optimizer、通用 Ray Serve 调度器或存储引擎优化。
- 同步更新 `experiments/plans/README.md` 和 `PROJECT_INDEX.md`，要求后续更新策略设计图或方法口径前先查阅该文件。

# 2026-07-15 Ours-v0 优化策略逻辑图

- 新增 `figures/architecture/optimization_strategy_logic.png` / `.svg`，将优化策略细化为“输入信号 -> 规则表选择器 -> 策略动作与配置 -> 端到端验证与回填”。
- 新增 `figures/scripts/generate_optimization_strategy_logic.py`，统一生成 PNG/SVG，并检查核心卡片边框、底部验证框和 7 条箭头是否越界或穿过无关卡片。
- 新增 `figures/audit/optimization_strategy_logic_audit.md`，记录该图的文献边界和可见标签要求：不声称 finalized learned optimizer，不把写回或跨层联合最优作为当前主 claim。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`，将该图纳入正式架构/方法图资产。

# 2026-07-15 顶会系统论文策略图范式整理

- 新增 `figures/audit/top_venue_strategy_figure_design_notes.md`，从 vLLM、Orca、Cortex AISQL、Ray Data 等系统论文图形中抽取方法图范式：优先画运行时机制、running example、data/control path 区分和紧凑规则表，而不是三列术语堆叠。
- 明确下一版策略图建议采用 “control-loop + running example + compact rule table”：上半部画 database AI query 到 batch queue、strategy selector、actor/endpoint、sink、E2E metrics 的控制循环，下半部画 Trigger -> Action -> Guardrail 规则表。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`，要求后续重绘策略图前先阅读该设计备忘。

# 2026-07-15 策略图小机制拆分与论文下载清单

- 新增 `figures/audit/strategy_figure_micro_design_points.md`，将后续策略设计图拆分为可独立绘制的小优化点：workload-aware batch/partition、bounded in-flight 反压、endpoint routing、写回约束和 Trigger -> Action -> Guardrail 规则表。
- 为每个小机制记录优化对象、参考论文图形范式、建议画法、所需实验证据和 reviewer 风险，避免继续画成“大而全”的术语堆叠图。
- 补充 vLLM、Orca、Ray Data Streaming Batch、Cortex AISQL、Sarathi-Serve、DistServe、Splitwise、FlexPushdownDB 等优先下载/精读链接，并同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`。

# 2026-07-15 本地参考文献 PDF 子集登记与图形阅读

- 新增 `research/reference/README.md`，登记用户已下载的 14 篇本地 PDF 子集，包括 Ray Data、vLLM、Ray、Sarathi-Serve、ServerlessLLM、GaussML、Galois、LEADS、NeurDB、Lance 等；明确该目录只是部分文献，不替代完整文献清单。
- 新增 `figures/audit/local_reference_figure_reading_notes.md`，记录从本地 PDF 图中提取的图形经验：用 `AI_EMBED` running example 锚定主图、把策略动作贴到执行位置、区分数据/控制/反馈流、用规则表或 mini timeline 补充机制。
- 更新 `opening/README.md`、`opening/literature/reading_list.md`、`figures/README.md` 和 `PROJECT_INDEX.md`，将本地 PDF 子集和图形阅读笔记纳入项目入口。

# 2026-07-15 Ours-v0 运行时策略闭环图

- 新增 `figures/architecture/runtime_strategy_control_loop.png` / `.svg`，用一个 `AI_EMBED` SQL 运行例子贯穿 RecordBatch queue、Ray submit gate、Endpoint queues、GPU model service、Results/sink 和 E2E metrics，直接展示 batch/partition、K_max、routing 和 writeback guardrail 的作用位置。
- 新增 `figures/scripts/generate_runtime_strategy_control_loop.py`，统一生成 PNG/SVG，并执行核心卡片边框、主数据流箭头和禁用术语自检；本次生成已通过程序化检查和 PNG 人工预览。
- 新增 `figures/audit/runtime_strategy_control_loop_audit.md`，记录该图为策略机制主图；`cross_layer_method_framework.*` 保留为研究方案总览，`upstream_strategy_design.*` 保留为过渡图，`optimization_strategy_logic.*` 降级为规则表草图。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md` 的图资产入口。
# 2026-07-15 策略图迭代版本归档

- 将暂时不用的策略图迭代版本移入 `figures/archive/architecture/20260715_strategy_iterations/`：`upstream_strategy_design.*`、`optimization_strategy_logic.*` 和内部字体测试图 `_font_test.png`。
- 当前策略设计说明优先使用 `figures/architecture/runtime_strategy_control_loop.*` 与 `figures/architecture/runtime_strategy_rule_table.*`。
- 同步更新 `figures/README.md`、`PROJECT_INDEX.md` 和相关审计文件中的路径说明，避免旧图继续出现在当前主图清单中。

# 2026-07-15 策略闭环图中文化与箭头修正

- 将 `runtime_strategy_control_loop.*` 和 `runtime_strategy_rule_table.*` 的可见标签尽量中文化，仅保留 `AI_EMBED`、`SQL`、`GPU`、`K_max`、`P99`、`token` 等必要技术记号。
- 将主流程框从泛泛的状态字段改为“观测量 / 调节项 / 判定项 / 约束项 / 评价项”，说明这些框是策略选择器读取的信号来源及其作用。
- 收窄主流程卡片、拉大间距并缩小箭头头部，使主数据流箭头有完整线段；重新生成 PNG/SVG 后通过边框、箭头和禁用术语自检。

# 2026-07-15 开题报告 architecture 图同步到三层策略

- 重绘 `figures/architecture/system_architecture_ai_data_execution.*`，将总体架构图同步为计划层数据组织、运行层入口调度、服务端 dynamic micro-batch 与写回瓶颈判定的当前口径。
- 重绘 `figures/architecture/cross_layer_method_framework.*`，将研究方案图从“上游链路调优”进一步明确为“三层上游执行策略与端到端评价”。
- 将 `figures/architecture/runtime_strategy_control_loop.*` 补入 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 作为图 4-2，替代原 Mermaid 链路示意，用于解释策略机制。
- 同步更新 `figures/README.md`、`figures/audit/*` 和 `PROJECT_INDEX.md`，去除当前主图入口中的 `Ours-v0`、`下一轮配置`、`边界确认` 等旧表述。

# 2026-07-15 architecture 图颜色语义修正

- 将 `cross_layer_method_framework.*` 中的三层策略改为三个并列中性卡片：计划层、运行层、服务端层，避免被误读为两个策略框或与上方 workload 颜色一一对应。
- 将 `system_architecture_ai_data_execution.*` 底部研究内容卡片统一改为中性色；上方系统阶段继续保留数据层、Ray 执行层、GPU 模型服务和结果存储的颜色编码。
- 将研究内容 2 标题调整为 `运行层调度与服务端批处理`，更准确表达当前方案横跨 Ray 入口调度、endpoint routing 和模型服务侧 `micro-batch`。

# 2026-07-15 研究缺口图与候选规则表口径修正

- 重绘 `research_gap_three_islands.*`，将底部研究内容同步为数据组织与批处理构造、运行层调度与服务端批处理、写回瓶颈判定，避免旧的“GPU 服务感知调度”与当前三层策略不一致。
- 将 `runtime_strategy_rule_table.*` 从“策略规则表”改为“候选策略规则表”，明确表中规则是待实验验证的触发逻辑，不代表已证明结论。
- 同步更新 `figures/README.md`、`PROJECT_INDEX.md` 和相关审计记录。

# 2026-07-15 开题报告正文同步三层策略口径

- 更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`，将文献综述、研究目标、研究内容、研究方案、进度安排和预期成果同步到当前方向。
- 研究内容二统一表述为“运行层调度与服务端批处理协同方法”，覆盖 `K_max`、endpoint routing、actor pool、backpressure 和服务端 `micro-batch`。
- 将方向三改为“写回瓶颈判定与端到端收益检查”，避免把写回写成当前独立主贡献。
- 清理旧的“岛”“GPU 调度优化”“联合最优/Killer Experiment”等主叙事表述，保留其作为后续增强对照的可能性。
## 2026-07-20 数据组织策略机制图正式化

- 使用 `figure-designer` 和 `nature-figure` 的论文图规则审计新增的数据组织策略机制图，将 `rc1_*` 草图口径调整为正式可引用的 `data_organization_*_mechanism.*` 系列。
- 新增三张 architecture 机制图：`data_organization_token_budget_mechanism.*`、`data_organization_length_align_mechanism.*`、`data_organization_prefix_aware_mechanism.*`，分别解释 token-budget batching、length-aligned grouping 和 prefix-aware grouping。
- 重写 `figures/scripts/generate_data_organization_strategy_mechanism.py`，输出 PNG/SVG，并对正式 SVG 执行 `RC/BL` 等禁用可见术语检查。
- 新增 `figures/audit/data_organization_strategy_mechanism_audit.md`，明确这些图是候选机制说明，不是实验结果图；prefix-aware 图仅声称创造 prefix locality，不提前声称 APC 收益。
- 更新 `figures/README.md` 和 `PROJECT_INDEX.md` 的图资产入口，说明旧 `rc1_*` 文件不再作为正式报告/PPT/论文入口。
- 根据 PPT 预览反馈修正 `data_organization_length_align_mechanism.*` 的标题字体混排问题，将含 `batch` 的粗体混排标签改为更稳定的纯中文机制标签，并同步替换 v5 PPT 中的对应图片。
## 2026-07-20 开题 PPT v5 增量版

- 新增 `opening/slides/opening_defense_20260720_v5.pptx`，由 v4 拷贝后增量修改生成，未重跑 `opening/slides/build_ppt.py`。
- 在研究内容一后新增三页数据组织机制图（token-budget、length-align、prefix-aware），图源来自 `figures/architecture/data_organization_*_mechanism.png`。
- 将原研究内容一中的 prefix-aware 表述收紧为候选验证口径，避免提前声称 KV-cache / APC 收益。
- 更新 `opening/README.md`、`opening/slides/README.md`、`opening/logs/project_log.md` 和 `PROJECT_INDEX.md`。

## 2026-07-21 提交控制策略机制图正式化

- 重写 `figures/scripts/generate_submission_control_mechanisms.py`，修复原脚本编码损坏问题，并统一生成三张提交控制策略机制图的 PNG/SVG。
- 更新 `submission_control_queue_adaptive_mechanism.*`、`submission_control_kmax_admission_mechanism.*` 和 `submission_control_pool_routing_mechanism.*`，将叙事收敛为“提交时机、提交数量、提交去向”三类上游提交控制决策。
- 图中统一使用保守表述：只说明候选机制和验证指标，不把当前设计写成已验证性能结论，也不暗示修改 vLLM 内部调度、Ray 调度器或数据库优化器。
- 新增 `figures/audit/submission_control_strategy_mechanism_audit.md`，记录图的角色、证据边界、验证指标和 QA 检查。
- 更新 `figures/README.md` 和 `PROJECT_INDEX.md`，将 submission-control 机制图纳入正式图资产入口。

## 2026-07-22 提交控制策略机制图重绘修正

- 根据图面反馈重绘 `submission_control_queue_adaptive_mechanism.*`、`submission_control_kmax_admission_mechanism.*` 和 `submission_control_pool_routing_mechanism.*`，将大面积红绿对照块改为白底卡片式机制图，降低草图感。
- 修复 K_max 图中请求槽位越出 vLLM 服务入口框的问题：所有槽位改为在父框内部按宽度居中计算。
- 修正分池路由图箭头语义：箭头从“请求形态判别”组件边界出发，并分别落到短请求池、长请求池、前缀相似池的左边界。
- 重新运行 SVG 坐标越界检查、PNG 非背景边界检查和 SVG 禁用词/乱码扫描，结果均通过。
- 更新 `figures/audit/submission_control_strategy_mechanism_audit.md` 记录本次 redesign QA。

## 2026-07-22 图表箭头边界 QA 规则补充

- 根据 submission-control 机制图反馈，在 `figures/AGENTS.md` 新增箭头方向与边界检查规则。
- 后续所有机制图、架构图、流程图除画布越界外，必须检查箭头是否从源组件边界出发、是否指向目标组件边界、方向语义是否一致，以及是否穿过无关卡片或文字。
- 同步更新 `figures/audit/submission_control_strategy_mechanism_audit.md`，将箭头边界关系纳入本组图的 QA 记录。

## 2026-07-23 P1/P2 文献精读批量完成（8 篇）+ 知识库同步

- 按用户给定的 P0/P1/P2 优先级清单，完成 **P1 四篇 + P2 四篇**深度精读（沿用 `tpl-文献精读-深度版` 四层模板），连同此前完成的 P0 四篇（Clipper / CONCUR / CoLoRA / SABER），精读笔记总数由 16 增至 **28 篇**。
- 新增笔记（`research/reading_notes/`）：
  - P1：`scorpio_llm_serving_2025`、`bucketserve_2025`、`sglang_neurips2024`、`splitwise_isca2024`
  - P2：`proserve_2025`、`distserve_osdi2024`、`flashattention_neurips2022`（自读全文）、`flexgen_icml2023`（自读全文）
- 全部笔记已两次同步至知识库 `../ai-operator-wiki/raw/papers/`（每完成四篇同步一次）。
- FlashAttention、FlexGen 两篇 PDF 此前未下载，本次补下到 `research/reference/`（arXiv 2205.14135 / 2303.06865，已校验 `%PDF` + `%%EOF`）。
- **精读勘误（重要，已写入 `reading_list.md`）**：原始任务描述两处与论文实际内容不符，精读代理据原文修正——(1) DistServe 全文用 simple FCFS，**无** AFGM fairness 与 prediction-based pairing（已 pdftotext 全文核实，§4.3 原文）；(2) ProServe 真实主题是**多优先级请求调度**（TDG + SlideBatching + GoRouting），**非** "预测式 prefill/decode 分离调度"。笔记均按论文真实内容撰写。
- **对课题的含义（策略补强方向，详见各笔记第四层）**：
  - RC2 自适应控制器形成三候选对比：Clipper AIMD（整体 batch size）/ Scorpio TRP+Credit（per-request 频率）/ CONCUR EWMA；Scorpio 的解析 ITL 模型同时是研究内容四（算子代价估计）的直接模板。
  - RC1 数据组织：BucketServe 的 padding 形式化（Eq.2/3）+ length 分桶、SGLang RadixAttention（Theorem 3.1 DFS 最优排序）+ 与 vLLM APC 互补、Splitwise/DistServe 的 Lm 饱和阈值（512 token 饱和 A100）共同支撑 token-budget / length-align / prefix-aware 分组。
  - 背景与对照：FlashAttention 提供理解 vLLM 内部 memory-bound 行为的底层理论链（→ Sarathi-Serve → 本课题 token-budget），并把 database join 与 GPU attention 并列于 IO-aware 谱系；FlexGen 作为"离线吞吐优先"对照锚点，明确本课题 online serving 定位。
- 更新 `opening/literature/reading_list.md` 精读笔记索引（16→28，新增 P0/P1/P2 三组 + 勘误说明）。
- 环境备注：本环境 Read 工具无法渲染 PDF（缺 pdftoppm），精读改用 `pdftotext`（xpdf 4.06）提取全文；已确认 `reference/` 与 `reading_notes/` 无 `.txt` 中间文件残留。

## 2026-07-24 提交控制（K_max/flush）与自回归生成特性：厘清与合并进现有文档

- 核心厘清：自回归生成的两个特性（decode 阶段 memory-bound、输出长度不可预测/完成时间异质）是**提交控制（K_max 自适应 / queue-adaptive flush）的物理前提**，而**数据组织（token-budget）依据已知输入 prompt、不依赖自回归**——由 `code/src/organizers.py:230` `_row_token_cost = prompt_tokens + completion_max_tokens` 代码确证。
- **关键增量（假设 H，待验证）**：RC2 adaptive 负结果（P0-1，foreground E2E 10.2s vs static K=8 的 7.3s）的现有归因是“控制器粗糙”；提出**未被识别的混淆变量——实验 `--completion-max-tokens 64` 固定 output** 消除了自回归变异源，adaptive 运行时动态优势可能无从发挥。建议 3 轮控制器改进前先用变长 output 重验。
- **内容归位（不另建文件，合并进现有文档）**：物理前提 + 架构边界 → `service_scheduling_backpressure.md` §0.5；adaptive 负结果归因 + 变长 output 实验方向 → `experiment_status_and_gaps.md` P0-1 + §4 P0；实现注意事项（EWMA 默认关闭 / AIMD 作对照 / 抓取节流 / flush 口径）→ `strategy_design_implementation_reference.md` §8.2；fatal flaws 缺口（Clipper Poisson / CONCUR 中段抖动 / BucketServe prefill-only）→ `strategy_design_literature_basis.md` §3.1。
- 同步修正：`PROJECT_INDEX.md:366` 补 adaptive 负结果限定（口径超前）。
- 待办（本次未动）：① 文献笔记因果链错误归因（`flexgen:201`/`sarathi:152,203,250`/`flashattention:149`/`top15:156` 把 token-budget 动机错挂 decode memory-bound）；② 开题材料 K_max/flush 段未引论文、未讲清与 vLLM 内部 continuous batching 互补关系——用户指示开题材料本次不动。

## 2026-07-24 plans/ 文档导航治理（方案 A，不移动文件）

- 问题：`experiments/plans/` 下混了三类性质不同的文档（实验计划 / 设计参考 / 状态审计），命名未体现性质，且两个 `strategy_design_*` 名字接近易混。
- 处理（导航治理，非物理移动/合并/改名）：
  - 重写 `experiments/plans/README.md`，按性质分三组（一、实验计划；二、设计参考；三、状态审计），并在设计参考组点明两个 strategy_design 的分工（`literature_basis`=边界论证 / `implementation_reference`=工程映射）。
  - 两个 `strategy_design_*.md` 开头各加"与对方分工"的交叉说明。
  - 不移动文件路径、不改名、不合并——避免破坏全项目引用路径（surgical changes）。
- 明确：不在 plans/ 再建技术文档层；技术基础（decode memory-bound / AIMD / continuous batching）单一来源在 `research/` 与 `research/reading_notes/`，plans/ 只引用、不重复。

## 2026-07-24 精读笔记配图重做（9 张，确定性抽取 + 完整性验证）

- 背景：`reading_notes` 引用的 9 张论文配图此前裁剪有问题（内容错位、切边、带正文、留白不均/歪斜）。本环境 Read 无法直接看小图、vision MCP 不可靠（对彩色图假报"文字"、坐标估计失准），改用**确定性像素分析**。
- 方法：①嵌入栅格图直抽（cortex_fig1/fig7，像素级精确）；②矢量图按"图题锚定底部 + 列/页面文本宽度定左右 + 彩色像素/水平墨线定图框"裁剪；③每张用墨迹 bbox 验证四周白边≥22px 确认不切边；④最后统一收紧到 ~28px 均匀留白（只裁白边，不动图内容）。
- 结果（9 张，三处一致：`research/reading_notes/figs/`、`opening/literature/top15_reading_notes/figs/`、wiki `raw/papers/figs/`）：cortex_fig1/fig7、galois_fig3、neurdb_fig2、orca_fig11、ray_fig8、sarathi_fig4/fig9、vllm_fig12——每张内容对应笔记引用的 Figure N，完整未切边。
- **orca 更正**：top15 清单已更新（orca/distserve 进，saber/multibin 出），orca 是 top15 成员，其 fig11 须保留——核实为**单栏左图**（plot 框仅在左栏 y72-220，右栏为正文），按左栏裁剪即完整。中途曾误删，已恢复。
- 修正 `opening/literature/top15_reading_notes/README.md` #10-15 顺序与权威源 `research/top15_ranked_papers.md` 一致（原 README 误把 Cortex/NeurDB/Galois/DB-Perspective 排在 Ray-Data/BucketServe 之前）。

## 2026-07-24 为 14 篇精读笔记补充论文配图（架构图/支撑图）

- 评估：15 篇精读笔记中 7 篇原有图，8 篇无图。逐篇核对"笔记讲解内容是否需要图 + 论文有无合适图"——**db_perspective** 是 perspective 论文、本身无图，跳过；其余 7 篇各选 1 张最贴合讲解的图：clipper Fig1（两层架构）、sglang Fig3（RadixAttention 操作）、distserve Fig3（两阶段吞吐，支撑"阶段特征不同"核心论点；该文无纯架构图）、splitwise Fig10（三池系统图）、concur Fig4（System Overview）、ray_data_streaming Fig1（Logical dataflow graphs）、bucketserve Fig4（Architecture）。
- 抽取方法（矢量图，无嵌入栅格）：图题锚定底部 + **彩色范围 ∪ 矢量范围**定图框（彩色覆盖 plot/栅格插图，矢量覆盖灰度架构图，单独用任一会漏）+ 列/页面宽度定左右 + 智能底部（图与图题间隙>40pt 则按矢量底，否则按图题）+ getbbox 收紧 + 28px 留白。
- 关键修正：图题查找器最初返回**正文里的"Figure N 引用"**（非真图题）——改用"上方 280pt 内有>5 个矢量对象"判定真图题，排除正文引用。此 bug 曾导致 sglang（误用 p3 正文引用，真图题在 p4）、concur 裁错。
- 验证：7 张墨迹 bbox 四周均 28px（完整不切）；逐张视觉确认内容与笔记讲解一致。
- 嵌入：7 篇笔记（权威版 `research/reading_notes/` + 快照 `opening/literature/top15_reading_notes/`）各加"## ▎配图（辅助讲解）"区块，嵌图 + 1-2 句说明 tying 到核心论点。同步 wiki `raw/papers/figs/`。
- 结果：15 篇中 14 篇有配图（仅 db_perspective 无）；三处一致 16 张图（原 9 + 新 7）。

## 2026-07-24 补 top15 精读的来源说明（provenance）

- 用户反馈："开题的 top15 文献精读来自 research 全量文献排名前 15，但项目内无文档说明此关系。"
- 核查：`research/top15_ranked_papers.md` 第 4 行已写"候选池：`ai_operator_literature_inventory.md`（66 篇）"，但开题交付面 `opening/literature/top15_reading_notes/README.md` 未说明选取链路，故用户在 opening/ 侧看不到来源。
- 处理（合并进现有 README，不新建文件）：在 `top15_reading_notes/README.md` 加"来源与选取链路（provenance）"段，显式写出三步链路：候选池 `ai_operator_literature_inventory.md`（66 篇，v5）→ `top15_ranked_papers.md` 学术排名选前 15 → `research/reading_notes/`（33 篇精读含此 15）权威版 / 本目录为快照拷贝。
- 文档链路现状：`reading_list.md` → 指向 `top15_ranked_papers.md` + 本目录；`top15_ranked_papers.md` → 标候选池；本 README → 完整 provenance。
- 用户进一步指出"`research/reading_notes/`（Top 15 的权威来源库）本身无 README"：新建 `research/reading_notes/README.md`，说明本目录作用（33 篇精读笔记 + `figs/` + 模板）、provenance 链路（inventory 66 → 精读 33 → Top 15 → 开题快照）、与 top15 快照及 wiki 的关系、配图与编辑规则；同步更新 `PROJECT_INDEX.md` 该目录条目。

## 2026-07-25 RC2 adaptive admission controller 设计确认

- 对现有 shared-vLLM 结果复核：静态 `K_max=8` foreground mean E2E 为 `6.602s`，现有两档 adaptive 为 `10.214s`；当前只能证明 admission guardrail 必要，不能证明 adaptive 优于静态策略。
- 确认采用“先可观测、再改控制律”的最小方案：控制器与 Prometheus/Ray/Daft 解耦，移除热路径 `sleep`，增加 K_max/inflight/queue/KV 时序记录，再实现无 EWMA 的死区非对称 AIMD。
- 新增 `code_doc/superpowers/plans/2026-07-25-adaptive-admission-controller-design.md`，明确范围、架构、异常语义、固定输出混淆变量实验、成功/放弃条件、TDD 与正式 GPU 验证边界。
- 本次只落盘已批准设计，尚未修改生产代码或产生新实验结果。

## 2026-07-25 运行层策略套件总体设计确认

- 用户将实现范围扩展为完整运行层策略套件：独立 queue-adaptive flush、actor pool 分池与动态路由、多 endpoint/未来多 GPU 拓扑、PID/EWMA/UCB 控制器，以及 batching × submission 联合搜索。
- 硬件边界确认：当前仅一张 RTX 5070 12GB。代码保留多 GPU endpoint topology 扩展，但当前正式实验只形成单 GPU 证据；同 GPU 多 endpoint 不用于声称多 GPU 扩展收益。
- 学习型控制器首版选择有限动作空间 UCB，不提前实现需要大量训练数据的监督式代价模型。
- 新增 `code_doc/superpowers/plans/2026-07-25-runtime-scheduling-strategy-suite-design.md`：采用分层策略接口，将 batching、flush、admission、pool routing、endpoint routing、topology、scheduler 和 search 解耦；明确失败/回退语义、TDD/不变量/集成测试、9 点核心联合矩阵、12 点含 flush 扩展矩阵、2048 行 held-out evaluation 和统计规则。
- 指标扩展为 `runs.csv`、`submissions.csv`、`requests.csv`、`control_trace.csv`、`resource_trace.csv` 与 `manifest.json`，区分 batch-level submission 与 row/model-sequence 粒度，并覆盖 tokens/s、全阶段 latency、tail、SLO、公平性、控制器轨迹、路由、endpoint/GPU 资源和 instrumentation overhead，为后续批量绘图保留原始数据。
- 原 `adaptive-admission-controller-design.md` 保留为 AIMD 子模块细化设计；本次仍仅完成总体设计，未修改生产代码或产生新 GPU 结果。

## 2026-07-25 运行层策略套件第一阶段实施计划

- 总体设计经用户审阅确认后，新增 `code_doc/superpowers/plans/2026-07-25-scheduling-foundation-implementation.md`。
- 第一阶段严格收敛为 typed request/topology schemas、静态 admission、round-robin routing 和 deterministic synchronous scheduler；先验证 exactly-once 与 bounded-inflight 不变量，不在同一变更中接入动态 flush、PID/EWMA/UCB 或生产 Ray 路径。
- 计划规定逐行为 RED/GREEN、项目 `.conda/pg-ai-profile` 测试环境、完整 test suite、compile/import 检查和分任务提交。
- 用户进一步确认所有后续设计与计划必须位于 Daft + Ray 框架内。总设计和实施计划已补充正式链路 `PostgreSQL -> Daft -> Arrow payload boundary -> Ray task/actor -> endpoint`；纯策略模块保持引擎无关仅为可测试性，fake/synchronous adapter 只用于测试。
- 第一阶段新增 Daft organizer -> Arrow payload -> 单节点 Ray task contract smoke；该 smoke 未通过前不进入 adaptive 策略实现。
- 执行 contract smoke 时确认当前 Ray 已移除 `local_mode=True`；测试改为 `ray.init(num_cpus=1)` 的本机单节点 Ray task，仍使用真实 Ray runtime，不以 mock 替代。

## 2026-07-25 Scheduling foundation 第一阶段实现与验证

- 在隔离分支 `feat/runtime-scheduling-foundation` 新增 `code/src/scheduling/`：不可变 `BatchRequest`/endpoint/topology schema、静态 K_max admission、健康 endpoint 过滤、deterministic round-robin 和 synchronous policy-composition scheduler。
- 策略包依赖扫描无 `daft`/`pyarrow`/`ray` import；正式 framework contract 仍为 `PostgreSQL -> Daft -> Arrow payload -> Ray task/actor -> endpoint`。
- 新增 4 个测试模块：schema 3 tests、policy 6 tests、scheduler 2 tests、真实 Daft→Arrow→单节点 Ray task contract 1 test。
- 项目 `.conda/pg-ai-profile` 环境全量验证：11 个测试模块、54 tests、0 failures；`compileall`、public import smoke 和策略层 engine-import 扫描通过。
- 当前完成的是 typed foundation 与真实 Daft/Ray contract，不是性能实验：尚未替换生产 Ray 提交循环，未实现 queue-adaptive flush/AIMD/PID/EWMA/UCB，也没有新增 vLLM 吞吐或延迟结论。

## 2026-07-25 Static Ray task/actor 接线实施计划

- 新增 `code_doc/superpowers/plans/2026-07-25-ray-static-wiring-implementation.md`，第二阶段只把 typed scheduler 接入 profiler 的静态 Ray task/actor 路径。
- 先扩展 typed collection timing 与通用 Ray adapter，再分别验证 task/actor 行为和原 CSV metric keys 等价。
- 旧 adaptive 分支原样隔离保留；本阶段不同时改变控制算法，避免把重构影响与策略收益混淆。
- 用户要求代码、测试和单 GPU 实验全部跑通并产出数据后再合并 `main`；开发使用项目内 `.worktrees/scheduling-foundation` 隔离 worktree，根 `.gitignore` 增加 `.worktrees/`。

## 2026-07-25 Static Ray task/actor 正式接线完成

- `postgres_ai_operator_profile.py` 的静态 `ray_task` 与 `ray_actor` 路径已统一委托给 typed `SynchronousScheduler` 和 `RaySubmissionAdapter`；旧 `queue_adaptive` 循环被隔离保留，尚未宣称控制算法改进。
- Arrow batch 在进入 Ray 前转换为 `BatchRequest` 元数据，但 payload 保持原 Arrow 对象；endpoint/actor 显式进入 `TopologySnapshot`，当前单卡实验统一标记 `gpu_id=0`。
- 保持既有 profiler 指标键：`operator_invocations`、`max_inflight`、bounded wait、fan-in、submit timing 与 adaptive 兼容字段。
- 项目 `.conda/pg-ai-profile` 环境验证：全量 15 个测试模块、70 tests、0 failures；真实 Daft→Arrow→单节点 Ray task/actor 契约均通过，`compileall`、public import 与策略层 engine-import 扫描通过。
- 该结果只证明静态执行接线和行为契约，不是 GPU 性能收益。queue-adaptive flush、AIMD/PID/EWMA/UCB、分池动态路由、联合搜索和正式单 GPU 数据仍待实现；完成前不合并 `main`。

## 2026-07-25 Adaptive controller family 实施计划

- 新增 `code_doc/superpowers/plans/2026-07-25-adaptive-controller-family-implementation.md`，按 typed observation/decision → AIMD/EWMA → PID → UCB → Ray scheduler 接入的顺序实施。
- 控制器保持引擎无关，不执行网络、sleep、Ray 或文件 I/O；缺失/陈旧指标统一 hold，静态策略不读取 adaptive metrics。
- 旧 two-level adaptive 暂时保留为显式 baseline；新控制器完成单元/契约测试后再进入单 GPU 正式对照，仍不合并 `main`。
- 复核 `experiments/plans/` 后补齐执行约束：正式 adaptive 对比前先做固定 64 与 EOS-permissive 256 output cap 混淆变量检查；优先无 EWMA AIMD，EWMA/PID/UCB 为后续对照；正式记录 tokens/s、service P99 与 inflight/queue/K_max 时序，并沿用三轮改进放弃条件。
- 联合搜索口径纠正为两层：先完成状态审计要求的 token_budget `{4096,6144,8192}` × K_max `{4,8,16}` 共 9 点核心实验，再做包含三种 flush 的 12 点扩展网格；UCB 单独报告，不替代独立拼接 vs joint grid。

## 2026-07-25 Adaptive controller core 与 profiler 接入

- 新增 typed `AdmissionObservation`、`WindowDecision` 与 diagnostics；实现无平滑 AIMD、可选 EWMA-AIMD、bounded PID、有限动作 UCB1 和 SLO-constrained reward。
- 新增 250ms 默认缓存 observation provider、stale/missing hold、无 sleep 的 dynamic admission gate 与逐决策 trace；动态降窗时 scheduler 的 bounded-inflight 不变量已有确定性测试。
- profiler CLI 新增 `aimd|ewma_aimd|pid`，在整个 run/多 DB fetch chunk 间保持控制器状态，并统一走 Daft→Arrow→Ray task/actor 路径；control trace 记录 inflight、K_max、running、waiting、KV、action、reason 与 allowed。
- legacy `queue_adaptive` 继续隔离作为对照。UCB 只完成策略/reward core，尚未在缺少 epoch 指标时暴露为 CLI，避免伪集成。
- 当阶段项目 `.conda/pg-ai-profile` 全量回归为 105 tests、0 failures，包含真实 Daft→Arrow→单节点 Ray task/actor 契约；compileall、public import 和策略层 engine-import 扫描通过。CLI AIMD dry-run 通过。
- 尚无新增 GPU 性能数据，不能声称 adaptive 优于静态 K=8；正式对比前仍须完成 output-length 混淆变量检查。分支继续隔离，不合并 `main`。

## 2026-07-25 Actor pool、endpoint topology 与独立 flush core

- 新增 request-cost pool router、least-queued endpoint router、确定性 rendezvous prefix affinity 与 unhealthy least-queued fallback；scheduler 可组合 pool/endpoint 两级路由。
- profiler 可记录并使用每个 Ray actor/task endpoint 的 pool ID 与 GPU ID；真实 Daft→Arrow→Ray actor 契约验证短/长 batch 进入不同逻辑 actor pool。当前两池仍共享 RTX 5070，只是行为证据。
- 新增 immediate、fixed-timeout、queue-adaptive 三类独立 flush policy，覆盖 budget、低负载、拥塞、missing/stale metrics 和 hard max-wait。
- fatal-flaw audit：现有 `source_order=arrival_time` 只排序不按时间 replay，不能产生真实 pending-wait/flush 证据；正式 flush 实验前必须增加 Ray pending queue 与 arrival-paced enqueue。
- 加入上述变更后的新鲜全量回归：122 tests、0 failures，包含真实单节点 Ray task/actor 与分池 actor contract。仍未产生 GPU 性能结果，未合并 `main`。

## 2026-07-25 加速到达 flush 策略真实单 GPU 实验

- 新增 arrival time scale、完整 flush/submission/resource trace，并在真实
  `PostgreSQL -> Daft -> Arrow -> Ray task -> vLLM` 链路完成三策略门禁。
- 正式矩阵共 18 次运行：immediate、fixed timeout、queue adaptive 各 1 次
  预热 + 5 次正式重复；每次 512 个文档均 exactly-once，未使用 fake。
- fixed timeout 相对 immediate 减少 8.984% submissions，但 tokens/s 仅提高
  0.185%，95% CI 重叠；当前 queue adaptive 没有形成多行 batch，tokens/s
  低 0.966%，不能声称动态策略有效。
- 第一次 queue-adaptive 预热触发 vLLM 请求超时和 7 个遗留 running 请求；
  失败数据未进入 CSV。仅重启实验 vLLM 容器并等待空闲后完整重跑，限制已写入
  manifest 和结果报告。
- profiler 新增直接输出 `tokens_per_s`，使用 vLLM 实际 token 增量；结果目录为
  `experiments/results/accelerated_arrival_flush_20260725/`。分支继续隔离，
  尚未合并 `main`。
- 后续 1024 条同密度单次探针中，queue adaptive 仍为 1024 submissions、
  平均 batch rows=1；扩大行数没有修复 batch formation，因此暂不直接运行
  2048 条正式重复，先修正 adaptive 窗口和事件时间处理。

## 2026-07-25 Adaptive flush 双窗口改进设计

- 根因复核确认：硬编码 `low_load_running=64` 与 `K_max=8` 不匹配，250 ms
  采样慢于 25–50 ms flush horizon，低负载立即 flush 放弃 fixed-timeout
  合并机会，且下游背压后 runtime 会在吸收 deadline 前到达行之前先超时关闭。
- 用户确认吞吐优先、P99 guardrail 的双窗口方向：低负载/缺失指标退化为 25 ms
  fixed baseline，服务压力下扩展到 50 ms；窗口在 batch 打开时选择并保持不变。
- 新增
  `code_doc/superpowers/specs/2026-07-25-adaptive-flush-window-design.md`，
  明确事件时间 catch-up、exactly-once、trace schema、64/1024/2048 分级门禁和
  claim boundary。本步骤只固化设计，尚未修改 adaptive flush 行为。

## 2026-07-25 Adaptive flush 双窗口实施计划

- 用户审阅并确认双窗口设计后，新增
  `code_doc/superpowers/plans/2026-07-25-adaptive-flush-window-implementation.md`。
- 计划分为显式窗口选择、event-time catch-up、profiler/trace 接线和真实单 GPU
  分级门禁四项；每项严格 RED→GREEN，64/1024 门禁失败即停止，不直接消耗
  2048 正式实验时间。

## 2026-07-25 Adaptive flush 双窗口实现与真实单 GPU 复验

- 实现显式 `FlushWindow`，queue-adaptive 在低负载/缺失指标时使用 25 ms
  fallback，在 waiting/KV/running 压力下使用 50 ms；running 阈值绑定本次
  `max_inflight`，窗口在 pending batch 打开时只选择一次。
- arrival replay 改为 event-time catch-up：下游 Ray 背压后，deadline 前已经
  到达的完整行仍进入当前 batch；`SystemReplayClock` 对系统提前醒来循环等待。
- flush trace 升级到 schema 2，新增 `selected_wait_s` 和 `window_reason`。
  全量回归 172 tests 通过，包含 3 条真实 Daft→Arrow→Ray task/actor 契约，
  `compileall` 通过。
- 64 行门禁和 1024 行行为探针均通过 exactly-once、batch formation、tokens/s
  与 service P99 guardrail；未使用 fake backend。
- 512 行正式重复共 18 次运行（每策略 1 次预热 + 5 次正式）。adaptive 相对
  新版 fixed timeout：observed tokens/s +3.671%、submissions -23.500%、
  平均 batch rows +30.732%、batch service P99 -8.010%；每轮 512 个文档
  exactly-once。
- 该结果是单 GPU、加速到达、固定 16-token 输出、固定策略组顺序下的正向候选
  证据；尚缺逐 repeat 随机化、变长输出、per-request E2E P99 和 2048 行
  held-out，不合并 `main`。结果见
  `experiments/results/adaptive_flush_window_20260725/`。

## 2026-07-25 实验基础设施与后续优化路线确认

- 复核总体计划、当前代码和项目文献后，确认 request lifecycle trace、随机化
  scenario runner、`target_output_tokens` 成本利用、Best-Fit bin-packing、
  UCB profiler 接线、联合搜索、actor-local async queue、多模态和代价估计仍是
  主要缺口。
- 用户要求先把 infra 搭稳，使后续多模态和代价估计容易接入；同时明确不需要为
  尚无真实调用方的功能提前创建空接口。
- 新增
  `code_doc/superpowers/specs/2026-07-25-ai-operator-execution-infra-design.md`：
  将总体定位调整为数据库 AI 算子外部执行 infra，分为数据进入与组织、运行时
  控制、Ray 执行、模型服务边界、可观测性与实验控制；定义
  request/submission/run 三层 schema、客户端可观测单 prompt E2E、seeded
  随机化 runner、output-aware BFD、控制器与联合搜索、actor runtime，以及
  多模态/代价估计/`@daft.cls` 的触发边界。
- 总体范围拆为四个独立子项目，第一项为 request lifecycle 与 scenario runner；
  每项单独设计、TDD、真实 Daft→Ray→vLLM 验证，不在同一提交同时改变计时、
  batch membership 和 admission control law。

## 2026-07-25 AI 算子执行 infra 第一阶段实施计划

- 用户确认按代码主要缺口和文献候选技术依次完善；先实施 request lifecycle 与
  seeded scenario runner，再进入 output-aware packing、控制器/联合搜索和 actor
  runtime。
- 新增
  `code_doc/superpowers/plans/2026-07-25-request-lifecycle-scenario-runner-implementation.md`，
  按 submission lifecycle → row seed/join → profiler request CSV/SLO → seeded
  runner → 真实 64 行门禁拆为五个可独立审查的 TDD 任务。
- 第一阶段明确不改变 batch membership、flush window、admission law 或 routing
  决策；batch endpoint 的逐行 E2E 标记为 `latency_granularity=submission`，
  不冒充 vLLM 内部单 sequence 完成时刻。
- compatible endpoint 仅提供 submission aggregate usage，因此逐请求
  `actual_output_tokens` 允许为空；客户端输出 token 估算使用独立字段和来源
  标签，不拆分 aggregate usage，不把估算值冒充服务端实际值。
- 完成 request lifecycle profiler 接线：新增 request CSV、client E2E
  P50/P95/P99、SLO violation/goodput、scenario/seed 字段，并保持既有 Ray
  submit API 的两值返回契约。
- Daft→Arrow→Ray task/actor 四行合同验证 request/submission exactly-once 映射。
  调试中发现直接混用多次 `time.time()` 会因 wall-clock 微调产生亚毫秒倒序，
  改为共享 monotonic-backed epoch clock；backend service epoch 单独标记为
  `service_clock_domain=backend`，跨时钟域无法可靠排序时不生成
  `submit_to_service_s`。

## 2026-07-25 Seeded 场景运行器

- 新增纯函数场景调度模块：warm-up 保持配置顺序，formal 每轮使用记录的 seed
  独立洗牌，输出连续 `order_index`，便于复现实验顺序。
- profiler 新增单次运行身份参数 `--run-phase` 与
  `--run-repeat-index`；场景运行器逐次启动独立 profiler 进程，并在每次运行前
  检查 vLLM health、running 与 waiting 空闲状态。
- runner 在非零退出或缺少预期 run CSV 行时立即停止并记录 incident；每次成功后
  原子更新 manifest。持久化命令与配置会脱敏 API key、认证 token、secret、
  password 和数据库 URL 密码，但保留 token budget 等实验控制量。
- 当前仅完成运行器代码与单元测试，真实 64 行
  PostgreSQL→Daft→Arrow→Ray→vLLM 门禁仍待执行；尚未产生新的性能结论，也未合并
  `main`。

## 2026-07-25 Request lifecycle 真实单 GPU 门禁

- 全量回归 191 tests 通过，包含 3 条真实本地 Daft→Arrow→Ray task/actor
  contract；`compileall` 与 diff check 通过后才启动真实门禁。
- 本机环境为 PostgreSQL 18.4、pgvector 0.8.2、vLLM 0.25.1、
  Qwen2.5-1.5B 和 RTX 5070 12GB；使用 64 个 ShareGPT/BurstGPT prompt，
  fixed timeout 与 queue-adaptive 各运行一次，未使用 fake backend。
- 首次 preflight 因错误参数名 `--model-request-timeout-s` 被 runner 以 exit
  code 2 阻断，未发送模型请求；incident 证据保留。修正为
  `--completion-request-timeout-s` 后继续。
- 首轮数据审计发现 legacy submission trace 没有显式 `submission_id`。按 TDD
  升级为 schema 2 后重新生成最终数据；两场景均为 64 request rows、64 唯一
  request/doc IDs、vLLM success delta=64，request→submission 外键、时间顺序、
  分位数重算、版本字段和最终 service idle 全部通过。
- 最终单次数据：fixed/adaptive request E2E P99 为 2.473097/2.351755s，
  observed tokens/s 为 2972.920/3073.893，submissions 为 22/19。该 64 行、
  每策略一次且 fixed 先运行的门禁只证明基础设施正确，不能声称 adaptive
  性能显著优于 fixed；1 秒 SLO 两者 violation ratio 均为 1.0。
- 结果位于 `experiments/results/request_lifecycle_gate_20260725/`；分支继续隔离，
  未合并 `main`，本次不自动同步 Wiki。

## 2026-07-26 输出成本与确定性 BFD 设计

- 用户确认下一阶段先完善 output-aware cost 与离线 Best-Fit Decreasing
  packing；global BFD 只用于完整可见的离线 organizer input，arrival replay
  保持到达顺序并仅复用成本计算。
- 新增
  `code_doc/superpowers/specs/2026-07-26-output-aware-bfd-design.md`，规定两个小型
  engine-independent core、Arrow/Daft 单一 BFD 实现、exactly-once/oversized
  不变量、packing scope 指标以及 64→512→1024→2048 分级门禁。
- 提交前 fatal-flaw 自检确认：当前 importer 将 ShareGPT prompt 与独立
  BurstGPT trace 逐行配对，因此 `target_output_tokens` 只能标记为
  `burstgpt_unpaired_trace_metadata`。它可用于成本敏感性和装箱验证，但不能称为
  当前 Qwen prompt 的真实输出 oracle，也不能单独支撑输出预测或 GPU 工作量匹配
  结论。
- 真正的离线 oracle 需要同 prompt、同模型、同 tokenizer、同生成参数和同停止条件
  的校准输出，并在不相交的 evaluation run 中回放；标签存在前不创建空接口。
- 根据用户后续会替换 prompt、模型和多模态输入的要求，BFD core 改用中性的
  `cost_units`/`capacity` 边界，不读取 prompt、tokenizer、模型 ID 或图像字段；
  当前仅实现真实文本 adapter，未来新增真实模态 adapter 而不重写 packing、Ray
  调度、lifecycle 和 scenario runner。多资源约束出现时新增真实算法，不把不可比
  单位强行压成伪标量。
- 实施计划映射时发现现有 request lifecycle 只覆盖 arrival replay，而 global BFD
  必须走离线 organizer。设计补充 `request_time_origin`：replay 保持
  `replayed_arrival`；离线 sequential/BFD 统一以 `offline_job_start` 为
  `arrival_epoch_s`、organized batch ready 为 `flush_epoch_s`。该补充只完善观测，
  不改变 batch membership、Ray 调度或模型请求。
- 本次仅固化设计与索引，尚未编写实施计划、生产代码或新增性能实验，分支继续隔离，
  不合并 `main`，不自动同步 Wiki。

## 2026-07-26 输出成本与确定性 BFD 实施计划

- 用户审阅设计并要求开始实施后，新增
  `code_doc/superpowers/plans/2026-07-26-output-aware-bfd-implementation.md`。
- 计划按共享 output cost、纯 `cost_units` BFD、Arrow/Daft 单实现、
  profiler/replay 接线、离线 request lifecycle、真实 Daft-Ray contract、
  64 行门禁与 512 行六单元矩阵七个任务执行；每个行为变更严格 RED→GREEN 并独立
  提交。
- 64 行真实 PostgreSQL→Daft→Ray→vLLM 门禁失败即停止，不直接消耗 512/1024/2048
  实验时间；本阶段最高只运行 512 行三次正式重复，1024/2048 留给重复证据通过后的
  独立确认阶段。
- 计划继续保持单 GPU、分支隔离、禁止 fake 正式数据、禁止把未配对 BurstGPT target
  称为 oracle，并要求记录模型/tokenizer/cost source、global/local packing scope、
  packing utilization、逐请求 E2E 和 service P99。
- 用户要求测试规模尽量一致：64 行仅作 infra 门禁，不进入性能比较；正式六单元矩阵
  全部固定为同一批 512 个 doc、相同 source order/fetch size/model/generation cap/
  token budget/max rows/K_max/writeback，仅改变 packing algorithm 与 output-cost
  mode，并逐 repeat 审计 `(doc_id, prompt_tokens)` 集合完全一致。

## 2026-07-26 Output-aware BFD 512 行预实验约束缺口

- 64 行真实链路门禁验证了 request/submission/resource trace、vLLM FLOP
  counter、功耗与 MFU 字段能够落盘；该规模只作基础设施检查，不作性能结论。
- 首次 512 行矩阵在第 22/24 轮停止。审计发现 sequential token-budget
  只应用 token 容量，而 BFD 同时应用 token 容量与 `ray_batch_rows=16`，
  导致前者单 submission 达 71--94 行，比较混入了不一致的行数上限；失败轮次还
  触发 180 秒 HTTP timeout。该批数据保留为 incident 证据，不进入正式结论。
- 按 TDD 为 sequential token-budget 补齐与 BFD、arrival replay 一致的
  `ray_batch_rows` 硬上限。新增回归测试先复现 `[5]` 与 `[2,2,1]` 的不一致，
  修复后 organizer、profiler scheduling、真实 Daft-to-Ray contract 与全量
  224 tests 均通过。正式矩阵必须从干净 vLLM 服务重新运行。

## 2026-07-26 Row-cap-aware packing 与非阻塞观测设计

- 用户确认继续优化，但明确 BFD 如果造成性能下降可以不使用；候选技术不要求全部
  进入最终系统。
- 新增
  `code_doc/superpowers/specs/2026-07-26-row-cap-aware-packing-and-observation-design.md`，
  将 classic BFD 固定为实验 baseline，默认继续使用 sequential token-budget，
  只有真实单 GPU 512 行筛选与 held-out 1024 行确认均支持时才提升新策略。
- 下一阶段只包含两个可独立验证的改动：把 AIMD/EWMA-AIMD/PID 正式路径接到已有
  非阻塞 metrics provider；新增同时尊重 row cap 与 token budget 的确定性
  row-cap-first best-fit 候选。Daft 流式化、多 endpoint、UCB、prefix 和多模态拆为
  后续独立阶段，避免一次改动引入不可归因的性能变化。
- 正式主比较使用同一模型、文档、固定 output cost、K_max 和测量配置，classic BFD
  与未配对 BurstGPT trace 只作 secondary sensitivity；每轮继续强制真实 vLLM
  FLOP delta、MFU、能耗、逐请求 E2E、submission 数和 exactly-once 审计。
- 用户进一步澄清 BFD 不应按“整体采用/整体删除”二选一。设计调整为机制级消融：
  即使 classic BFD 整体负向，仍可在 row-cap-aware 混合策略中复用经独立对照有效的
  cost 降序、确定性 tie-break、共享硬约束、oversized 单例和 packing diagnostics；
  只有 placement objective 等造成退化的部分被排除。

## 2026-07-26 Row-cap-aware packing 与非阻塞观测实施计划

- 用户确认机制级设计并要求继续，新增
  `code_doc/superpowers/plans/2026-07-26-row-cap-aware-packing-and-observation-implementation.md`。
- 计划分为 observation age/schema、typed adaptive 非阻塞生产接线、纯
  row-cap-first packing、Arrow/Daft 共享 organizer 接线、全量回归、真实 64 行门禁
  和 512→1024 筛选确认七个任务；所有生产行为严格先 RED 后 GREEN。
- 主比较固定 output cost，并以 sequential、classic BFD、BFD-inspired
  row-cap-aware 三组完成最小机制消融；未配对 BurstGPT target-output 只保留为
  secondary sensitivity，不再作为主配置。
- 512 行先单次筛选，只有同时通过 correctness/MFU 门禁且未出现无补偿的显著性能
  退化的候选才运行三次重复；1024 只验证 512 胜出配置，不重新调参。若无候选胜出，
  直接保留 sequential 并停止，不为使用复杂技术而继续扩展。

## 2026-07-26 Row-cap-aware 真实结果与 Infra 状态闭环

- 完成非阻塞 typed-adaptive metrics provider、sample-age control trace、
  row-cap-first packing、Arrow/Daft 共享接线以及 64 行真实门禁。
- vLLM 0.25.1 的 FLOP counter 必须显式启用 `--enable-mfu-metrics`；
  仅看到 metric 名称不足以证明 MFU 有效。本轮通过真实请求验证正 FLOP delta。
- 初始 512 筛选发现 prefix cache 实际开启，导致重复 prompt 的场景顺序依赖和
  180 秒超时。污染数据保留作 incident 审计，不进入性能结论；服务按相同配置
  重建并增加 `--no-enable-prefix-caching`。
- 无 prefix cache 的 512 行三次重复中，row-cap-first 相对 sequential：
  tokens/s +0.68%、request P95 -0.55%、energy/1k tokens -2.81%、
  MFU +1.62%，因此进入 1024 held-out。
- 1024 行三次重复中，row-cap-first tokens/s +0.82%，但 10 秒 SLO
  violation 从 50.39% 上升到 88.67%，SLO goodput 从 37.66 降到
  8.67 req/s。Sequential token-budget 保持默认，classic BFD 不采用，
  row-cap-first 仅保留为研究消融点。
- 场景运行器新增 TDD 覆盖的安全 resume、recovered incident、显式失败场景
  pruning，以及进入 manifest/resume 校验的 `service_metadata`。
- 新增 `code/INFRA_STATUS.md`，统一说明 batching、flush/admission、
  actor pool/endpoint routing、观测与实验基础设施的当前流程、完成度和后续
  实施顺序。
- 所有改动继续位于隔离特性分支，尚未合并 `main`；本次未自动同步 Wiki。

## 2026-07-26 变长输出观测、Adaptive Flush 与联合实验闭环

- Compatible vLLM completion 路径新增显式 token-ID opt-in、per-request
  actual output tokens、finish reason、ChatML prompt envelope、temperature
  与 context-safe source filter；generic compatible endpoint 默认行为不变。
- 自然 EOS 门禁确认 64 请求中 48 个 `stop`、16 个 `length`。512 请求、
  每策略 5 次随机化正式重复中，queue-adaptive 相对 fixed-25：
  tokens/s `+30.09% ± 2.66%`、E2E `-23.05% ± 1.60%`、request P99
  `-27.38% ± 1.87%`。单次 fixed-50 探针与 adaptive 相当，因此不声称
  动态性优于最佳静态窗口。
- 完成 18 单元 token budget × K_max × flush 真实筛选，所有 K16 配置均因
  1.76%–3.13% SLO violation 被 guardrail 排除。
- 完成 4 候选、每项 1 warm-up + 3 formal 的随机化重复。独立拼接相对
  fixed-25 tokens/s `+4.76% ± 2.29%`；联合候选相对独立拼接
  `-0.26% ± 2.07%`；adaptive 8192/K8 相对 fixed-50
  `-0.75% ± 0.97%`。
- 设计决策：本地单 GPU 当前采用 sequential token-budget + static K8 的
  分层优化；当前 accelerated-replay workload 使用 fixed-50。联合搜索保留为
  验证工具，adaptive 保留为跨 arrival-rate 候选，不增加联合在线控制器。
- 新结果位于
  `experiments/results/adaptive_flush_randomized_20260726/` 与
  `experiments/results/joint_batching_submission_512_20260726/`。仍未合并
  `main`，也未自动同步 Wiki。

## 2026-07-26 自然 EOS Fixed-25 / Fixed-50 / Adaptive 三组复验

- 在相同 512-request ChatML 自然 EOS workload 上，对 fixed-25、fixed-50、
  queue-adaptive 各运行 1 warm-up + 3 formal，formal 顺序按 repeat 随机化；
  12/12 成功、0 incident、逐请求 actual output/finish 与 MFU 审计通过。
- fixed-50 相对 fixed-25：tokens/s `+32.23% ± 3.90%`、E2E
  `-24.69% ± 2.72%`、request P99 `-29.27% ± 2.70%`、submissions
  `-31.50%`。
- adaptive 相对 fixed-25：tokens/s `+32.09% ± 6.22%`；adaptive 相对
  fixed-50：tokens/s `-0.10% ± 4.13%`、E2E `+0.13% ± 4.72%`，
  没有可分辨增量，且 submissions 平均多 1.70%。
- 当前单 GPU、当前 arrival rate 的在线候选正式收敛为 fixed-50；
  adaptive 仅保留为跨 arrival-rate 自动选择候选。下一步不继续增加控制器
  复杂度，先验证负载变化时最佳静态窗口是否改变。

## 2026-07-26 单 GPU 文本主线证据闭环

- 完成约 51.4/12.85 req/s 两档新增回放强度的 fixed-25/fixed-50/adaptive
  真实筛选；6/6 场景、3072/3072 请求、0 incident。两档 fixed-50 相对
  fixed-25 tokens/s 分别 +22.50%/+25.80%，adaptive 相对 fixed-50
  -0.61%/-1.32%，未显示跨负载动态切换价值。
- 新增 vLLM `/tokenize` workload 入口，不依赖本机 tokenizer 缓存；从原始
  ShareGPT/BurstGPT 生成 2048 条上下文安全独立请求，未截断或复制。
- 完成 2048 自然 EOS 留出：fixed-50 相对 adaptive tokens/s +1.75%、
  E2E -1.81%、P99 -2.61%；4096/4096 请求完成、0 incident。相对 512 锚点
  吞吐下降约 10%，持续积压仍放大尾延迟。
- 构造 0/30/70/100% 受控 prefix workload。真实筛选暴露并修复唯一 prefix
  哈希重排、prefix grouping 隐式叠加 length-align 两个语义问题；最终
  prefix-only 策略在 cache-off 下无稳定收益，sequential token-budget 保持默认。
- 新增无执行后特征泄漏的 E2E 代价估计：283 行真实 profile、70 个配置组。
  五个 grouped held-out seed 的 ridge 平均 MAE 11.68s、MAPE 50.60%、
  RMSE 25.89s、R² 0.776；MAPE 对小目标敏感，只作为粗粒度编排提示。
- 当前单 GPU 文本默认收敛为 sequential token-budget + static K_max=8 +
  fixed 50ms。adaptive、prefix-aware、BFD 均保留为有显式触发门槛的候选。
  分支继续隔离，未合并 `main`，未自动同步 Wiki。

## 2026-07-26 多 endpoint 路由就绪设计

- 用户确认未来会提供真实多 GPU 环境；当前不通过同一张 GPU 上的双 vLLM
  进程模拟多 GPU 性能，也不把两个逻辑 endpoint 指向同一服务的结果写成扩展性证据。
- 代码审查发现 `LeastQueuedEndpointRouter` 在多个 endpoint 的
  `running + waiting` 相同时固定选择 endpoint ID 最小者，可能使新鲜拓扑的初始
  burst 偏向单端点。该发现属于静态代码证据，当前单 endpoint 正式结果不能量化其影响。
- 新增
  `code_doc/superpowers/specs/2026-07-26-multi-endpoint-routing-readiness-design.md`：
  设计 tie-fair least-queued 和独立的 least-estimated-work 候选；当前用真实
  vLLM 的双逻辑 endpoint 仅验证路由、trace、exactly-once 和客户端开销，未来在
  独立 GPU endpoint 上完成吞吐、尾延迟、MFU、公平性与故障迁移矩阵后才能晋级策略。
- 本次仅固化设计与交接边界，尚未修改生产路由代码或运行新的性能实验。

## 2026-07-26 Ray 与 vLLM 执行层调优设计

- 用户确认把尚未系统使用的 Ray 参数优化加入主线，并要求继续审计其余可优化点。
  现有 2048 held-out 正式配置为 `ray_task`、Daft native、无 partition；
  Ray submit 约 1.54s、fan-in 约 0.17s，而 E2E 约 456.55s，因此不优先做
  object-store、显式 `ray.put` 或 fan-in 微优化。
- 只读审计当前 vLLM 0.25.1 容器确认服务使用 `--enforce-eager`、
  `--gpu-memory-utilization 0.75`、`--enable-mfu-metrics` 和
  `--no-enable-prefix-caching`，且没有显式记录/设置
  `max_num_batched_tokens` 与 `max_num_seqs`。当前服务配置适合作为 eager
  baseline，但不是唯一稳态性能 baseline。
- 代码审查进一步发现 Ray actor 数量当前被直接建模为 endpoint 数量，
  混淆了“独立 vLLM/GPU 服务容量”和“客户端 actor worker”。新增
  `code_doc/superpowers/specs/2026-07-26-ray-vllm-execution-tuning-design.md`，
  要求一个 endpoint 可拥有多个 actor worker，endpoint routing 与 endpoint-local
  worker selection 分层。
- 新设计按 CUDA Graph 门禁、Ray task/actor 有效并发、vLLM scheduling capacity、
  prefix-cache 机制和未来多 endpoint/multi-GPU 的顺序执行；Daft Ray runner
  sweep 仅在数据组织超过 E2E 5% 或多模态预处理到来时触发。
- 本次仍只更新设计与交接规则，尚未重启服务、修改生产代码或新增性能结论。

## 2026-07-26 Ray 执行基础与 vLLM 调优实施计划

- 用户审阅通过 Ray/vLLM 执行层设计后，新增两份相互独立、顺序执行的计划：
  `code_doc/superpowers/plans/2026-07-26-ray-execution-foundation-implementation.md`
  负责 endpoint-local actor pool、Ray CPU/并发/零 GPU/零自动重试契约和结果字段；
  `code_doc/superpowers/plans/2026-07-26-vllm-ray-tuning-experiments.md`
  负责真实 CUDA Graph、Ray task/actor 与 vLLM capacity 门禁和重复实验。
- 代码计划含六个独立 RED→GREEN 任务；实验计划固定同一 512-request workload，
  每一层只晋级上一层胜出配置，并要求 64-request correctness/MFU 门禁先通过。
- vLLM 容器切换采用停止并重命名 eager 容器的可恢复方式；不会删除原容器，
  不把编译/graph capture 启动成本混入 steady-state E2E。
- 本次仅编写计划，尚未执行生产代码改动或重启 vLLM。

## 2026-07-26 vLLM CUDA Graph 真实对照

- 保留式停止并重命名 eager 容器为
  `ai-operator-vllm-qwen-eager-backup`，使用同一 vLLM 0.25.1 镜像、
  同一只读 Qwen2.5-1.5B 挂载和 0.75 GPU memory utilization 启动
  CUDA Graph 服务；唯一执行变量为移除 `--enforce-eager`。
- 64-request graph gate 通过：64/64 请求 exactly-once，201 字段 formal
  schema、request/output/finish、resource/MFU/energy 和数据库 job 审计完整，
  0 incident。
- 完成 graph 1 warm-up + 3 formal 的 512-request 实验。三轮 formal
  相对 eager 均值：E2E 282.76s → 79.85s（-71.76%），observed tokens/s
  812.23 → 2875.68（+254.05%），request P99 259.82s → 57.63s
  （-77.82%），MFU 4.02% → 14.51%，每千 observed tokens 能耗
  99.95J → 55.52J（-44.46%）。
- 两侧每轮 prompt tokens=63,970、packing/submission=137，且相同 512 个
  doc ID 顺序；实际 generation tokens 相差约 0.57%，因此吞吐按各轮实际
  observed tokens 计算，不声称逐 token 输出完全相同。
- `graph_startup_evidence.json` 保存带 Docker 时间戳的启动证据：Created
  到 application startup complete 为 148.512s，模型加载 23.139s、
  compile 13.51s、graph capture 4s、engine init 72.26s；启动成本不计入
  steady-state E2E。两侧 service 记录均为可机器读取的单对象 JSON。
- Windows Ray shutdown stderr 在 eager formal 3 以及 graph warm-up、
  formal 1、formal 2 中出现非致命 `access violation` 文本，但对应
  profiler exit=0，manifest、vLLM success delta、exactly-once trace 和
  数据库 finished 状态均完整。该问题保留为两侧共有的独立 infra 缺陷，
  不能称任一侧日志完全干净。
- 后续本地 Ray task/actor 与 capacity 调优采用 CUDA Graph 作为部署 baseline；
  该提升属于 vLLM 配置选择，不作为论文上游调度策略贡献。结果与绘图数据见
  `experiments/results/vllm_cuda_graph_512_20260726/`。

## 2026-07-26 实验与机制证据文档收口

- 审计 `experiments/results/` 全部 19 个一级结果目录，确认每个目录均有
  `README.md`，并新增 `EXPERIMENT_EVIDENCE_REGISTRY.md` 作为统一入口。
- 台账逐项映射 fixed/token-budget/length/prefix/BFD/row-cap、K_max、adaptive
  flush、AIMD/EWMA/PID、UCB、多 endpoint routing、联合搜索、CUDA Graph、
  代价估计和多模态预留，明确区分设计、功能测试、真实门禁、GPU 筛选和重复/
  留出证据。
- 明确 UCB 目前只有纯控制器和 SLO reward 测试，未接入 profiler，也没有端到端
  GPU 结果；AIMD/EWMA/PID 同样不能由代码测试推出性能有效。真实多 endpoint/
  多 GPU、公平性和故障迁移仍待后续硬件验证。
- 修复 `output_aware_bfd_512_20260726/README.md` 等历史报告的编码和信息缺口，
  补充统一复现命令、原始证据路径、incident、排除理由和后继结果；根结果索引
  补登记修复前 `output_aware_bfd_gate_20260726/`。
- 本次没有改变任何实验数字或机制结论，因此不改研究计划和论文结论；只收口
  可追溯性、入口和结论边界。

## 2026-07-26 Adaptive admission 真实 GPU 矩阵

- 在现有 CUDA Graph vLLM 0.25.1/Qwen2.5-1.5B、RTX 5070 单 GPU 链路上，
  完成 64 请求 static K8/AIMD/EWMA-AIMD/PID 门禁，以及 512 请求每策略
  1 warm-up + 3 formal 的随机交错矩阵。
- 12 个四策略 formal runs 共 6,144/6,144 请求 completed；prompt tokens
  均为 63,970，vLLM success delta 均为 512，201 列 schema、request/control/
  resource/MFU/energy 证据完整。
- AIMD/EWMA-AIMD/PID 相对 static K8 的 E2E 分别 -32.04%/-31.94%/
  -30.32%，tokens/s 分别 +46.26%/+46.21%/+42.43%；但三个控制器平均
  admission limit 均为 15.78–15.93，暴露缺失 static K16 机制 control。
- 追加随机交错 AIMD vs static K16，各 1 warm-up + 3 formal。AIMD 相对
  static K16：E2E +0.66%、tokens/s -0.69%、request P99 -0.07%、goodput
  -0.66%、energy/1k tokens +0.37%、MFU -0.13%，均不可分辨。
- 当前结论是动态控制器相对 K8 的收益来自更高并发上限，不是反馈控制增量。
  单作业稳态优先简单静态窗口；shared-vLLM 仍以 K8 为 guardrail，后续如继续
  adaptive，只做 foreground/background 保护验证，不在稳态单作业上调 PID。
- 三组 runner 共 28/28 runs、0 incident。多轮 Windows Ray shutdown stderr
  保留非致命 access violation 文本，但两侧均出现，profiler exit=0、manifest、
  exactly-once 请求和数据库完成状态均通过。结果见
  `experiments/results/adaptive_admission_controller_20260726/`。
- 合并前完整测试在项目 Python 3.10 实验环境暴露错误清理路径直接调用
  Python 3.11 `BaseException.add_note()` 的兼容性缺口；增加 3.10 fallback，
  保持原始异常和 `__notes__` 语义。沙箱外完整 `code/tests` 为 311 passed。
