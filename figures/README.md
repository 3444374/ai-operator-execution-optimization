# 2026-07-14 Current Figure Set

For the current opening-report version, prefer these PG18.4
pgai-integrated GPU-backed rerun figures over older 2026-07-12 GPU charts:

```text
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.png
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.png
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.png
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
```

The older `03_invocation_granularity`, `04_executor_endpoint_comparison`,
and `05_actor_endpoint_scaling_writeback` figures are retained as historical
motivation assets, but should not be the first citation for the latest local
pgai-integrated GPU-backed rerun.

# 2026-07-20 Data Organization Mechanism Figures

Use these formal mechanism figures when explaining research content one
(data-organization strategy design):

```text
figures/architecture/data_organization_token_budget_mechanism.png
figures/architecture/data_organization_token_budget_mechanism.svg
figures/architecture/data_organization_length_align_mechanism.png
figures/architecture/data_organization_length_align_mechanism.svg
figures/architecture/data_organization_prefix_aware_mechanism.png
figures/architecture/data_organization_prefix_aware_mechanism.svg
figures/scripts/generate_data_organization_strategy_mechanism.py
figures/audit/data_organization_strategy_mechanism_audit.md
```

These figures replace the older `rc1_*` draft entry points for formal report,
PPT, and later thesis use. They are mechanism diagrams, not experimental-result
charts: token-budget, length-aligned, and prefix-aware grouping are presented
as candidate request-shaping policies whose throughput, tail-latency, queue,
and prefix-cache effects still require ablation evidence.

# 2026-07-21 Submission Control Mechanism Figures

Use these formal mechanism figures when explaining research content two
(scheduling and submission-control strategy design):

```text
figures/architecture/submission_control_queue_adaptive_mechanism.png
figures/architecture/submission_control_queue_adaptive_mechanism.svg
figures/architecture/submission_control_kmax_admission_mechanism.png
figures/architecture/submission_control_kmax_admission_mechanism.svg
figures/architecture/submission_control_pool_routing_mechanism.png
figures/architecture/submission_control_pool_routing_mechanism.svg
figures/scripts/generate_submission_control_mechanisms.py
figures/audit/submission_control_strategy_mechanism_audit.md
```

The set is organized as three upstream submission decisions: when to flush,
how many requests may remain in flight, and where each request is routed. These
figures are mechanism diagrams, not experimental-result charts. They should be
cited with validation metrics such as queue wait, P95/P99 latency, tokens/s,
foreground/background interference, queue balance, and prefix locality.

# Project Figures

做图、改图、迁移图或审查图表前，先读 `figures/AGENTS.md`。本文件只维护当前图资产入口、正式图清单和保留规则。

本目录是项目级图资产库，供 learning 材料、开题报告、开题 PPT、中期汇报和毕业论文共同复用。图不再分散在 `opening/assets/charts/` 和 `opening/assets/figures/` 中；后续新增图也优先放在本目录下，并按用途分子目录。

## 2026-08-08 开题叙事图（当前入口）

### 开题专用图集

开题 PPT 和开题报告不再直接从内容繁杂的 `data/report_main/` 选图。统一进入
`opening_figure_set/`：`main_png/` 与 `main_svg/` 按答辩页码保存 21 张 PPT 主讲候选图，
`editable_drawio/` 保存 10 张可编辑概念图，`backup_png/` 与 `backup_svg/` 只保存 2 张
单 Job 诊断备份图。文件采用 `P页码_用途_内容` 或 `B序号_用途_内容` 的中文可读命名。

完整目录、源文件映射和排除项见 `opening_figure_set/README.md`；选择与复制审计见
`audit/opening_figure_set_manifest_20260811.md`。权威源仍在 `architecture/editable/` 和
`data/report_main/`，图集只作为稳定选图入口。

2026-08-20 的开题报告 Markdown 从该图集中选择 11 张图，复制到
`opening/report/figures/` 以保证报告路径独立、后续转写 Word 方便。报告正文没有继续插入文本
原生多作业、图像 baseline 和图像多作业三张配套图，以控制第 4.2 节图量；选择与图号映射见
`opening/report/figures/README.md`。副本不构成新的事实源。

第一性原理复审后，正文不再从“已有模块”倒推故事，而是先用动机证据分别导出
WorkDescriptor、运行时感知和有界动态提交，再展示组织、图像与代价估计的先验证据：

| 图 | 角色与边界 |
|---|---|
| `data/report_main/opening_motivation_work_state.png` / `.svg` | 动机问题一和问题二的联合证据图。panel a 说明相同行数仍有 14.3× 模型工作量差异；panels b、c 说明固定上限不等于实际在途工作，并且吞吐增加时还要检查尾延迟。多 Job 干扰作为问题三由原生四作业图另行展示。 |
| `data/report_main/opening_motivation_work_state_part1_work.png` / `.svg` | 动机一的 16:9 图：相同行数可能对应不同模型工作量。数值与统计口径不变。 |
| `data/report_main/opening_motivation_work_state_part2_state_capacity.png` / `.svg` | 动机二的 16:9 图：判断是否继续提交，不能只看一个指标。左图说明固定上限与实际在途工作不同，右图说明吞吐和尾延迟需要一起检查；每个 Job 的进度另图展示。 |
| `data/report_main/opening_text_baseline_evidence_map.png` / `.svg` | 文本 baseline 分轨图：SQuAD database-E2E 产品轨比较 Direct/DuckDB/Project；ShareGPT 官方 Chat graph 轨比较直接调用容量参照、Daft Native、Daft Ray 与 Ray Data。只在 panel 内排名；Project 没有同一 2,048-row graph→gather 正式点，图中明确标注而不混入右侧排名。2026-08-10 修正多行 y 轴标签：文本块贴近轴，块内两行居中。 |
| `data/report_main/opening_native_fourjob_normalized_impact.png` / `.svg` | 现有原生框架的多 Job 动机图：三条 vendor-owned 执行图的 four-job/isolated-single JCT 影响矩阵；格内同时给出 slowdown 倍率与 JCT 增幅，Short 与全部 Long 均受共享服务竞争影响。只作各系统内部归一化，不作跨框架绝对性能排名，也不用于证明项目方法胜出。 |
| `architecture/opening_ai_data_execution_boundary.png` / `.svg` | 研究边界：数据库 AI 算子与模型/typed GPU backend 之间是 AI Data Execution Layer；两项研究内容并列，算子代价估计作为共同使能部件向二者供给 work/slack/uncertainty。 |
| `architecture/opening_work_to_schedule_overview.png` / `.svg` | 方案总览：共同代价估计器产生 stage/service/remaining work、SLO slack 和不确定区间，经 staged WorkDescriptor 同时使能组织与调度；组织器保留 work/locality，调度器再结合新鲜状态做 admission/routing/credit/fair queue。 |
| `data/report_main/opening_work_organization_regime_v2.png` / `.svg` | 吞吐与prefix-cache命中率双轨迹图：每条线连接同一策略从2-endpoint低压力到4-endpoint高压力的3次formal中位数；吞吐统一用 `k token/s`，高压力时保序策略命中约47%，重排/装箱策略降至6%–7%。不画误差线；严格feeding-saturation边界必须保留。 |
| `data/report_main/opening_image_stage_aware_evidence.png` / `.svg` | 图像动机图：CPU prepare/GPU actor 为 13.8–31.2×；batch64 的 R0/R1/R2 transfer ceiling 区分 GPU-resident、pinned FP16 与 pageable FP32；active-window screening 显示欠供给、近平台与过量排队。标题只概括阶段失衡、传输形态与提交窗口，不提前导出项目机制。 |
| `data/report_main/opening_image_stage_aware_evidence_part1_prepare.png` / `.svg` | 原图像动机图 panel a 的 16:9 拆分版；保留 batch 16/64/256 的中位数与 IQR。panel 标题为“图像也是分阶段工作量：张数描述不了阶段压力”，对齐动机“文本讲量、图像讲阶段”口径，不强调 CPU 主瓶颈。 |
| `data/report_main/opening_image_stage_aware_evidence_part2_transfer_window.png` / `.svg` | 原图像动机图 panels b–c 的 16:9 拆分版；保留 transfer ceiling 与 active-window screening 的原始口径。作为动机补充页“AI Work 需要分阶段描述”：panel b“输入表示改变阶段执行效率”（R0/R1/R2 表示对应不同阶段代价），panel c“阶段供给不匹配导致欠供给或等待堆积”；直接证据为 prepare/transfer/model 三阶段。 |
| `data/report_main/opening_image_baseline_evidence_map.png` / `.svg` | 图像 baseline 纯数据图：左为 12K 小规模能力检查（初始化开销占主导，不作排名），右为 120K 同资源比较；Daft 内置路径的未完成原因用自然中文说明。 |
| `data/report_main/opening_image_fourjob_normalized_impact.png` / `.svg` | 图像四 Job 归一化干扰矩阵：列为 Short/Long 1--3，行为 Daft Built-in、Ray Data、Project static 与 Project shared；格内直标并发/独立 JCT 倍数和增幅，画法与文本原生四 Job 图统一。只作各路径内部比较；Project shared 的状态快照仅观测、不驱动动作。 |
| `data/report_main/opening_cost_model_decision_quality_v2.png` / `.svg` | 左图呈现 estimator 级 candidate pairwise；右图逐估计器完整展开 20 个 leave-one-context-out decision regret，以小菱形标中位数、同尺寸深色点标最坏 context，并同时显示平均 5%/最坏 15% 门槛；Hybrid平均2.90%、max 14.72%。Ridge逐行MAE更低但最坏选择regret更高，只称 marginal pass。 |
| `data/report_main/opening_cost_model_decision_quality_v3.png` / `.svg` | 与 v2 相同的两 panel（配置排序 + 决策损失分布），唯一区别是 panel b 标题写全为“决策损失分布（模型预测最优与实际最优的偏离）（20 个场景）”：经校验该偏离（`argmin(predicted_mean_s)` 候选的实际偏离）在数值上等于 decision regret，二者是同一个量，故不单列第三 panel。v2 保留。 |
| `data/report_main/opening_cost_model_decision_quality_v4.png` / `.svg` / `.pdf` | 三组结果合成图。图 a 用六个统一坐标的小图分别展示六种估计方法的 80 组候选均值：同一横坐标上的空心点为真实时间、实心点为预测时间，竖线长度为两者相差的秒数；每幅图直标中位相对偏差和平均绝对误差。图 b 保留四种在途工作量上限的两两排序准确率，图 c 保留 20 个留出场景的选择损失；散点区域不放置统计数字，图外图例解释单个情境、中位数、最差情境和混合模型颜色。该图明确显示混合模型并非单点时间预测误差最小，只支持其在当前实验中的候选排序与选择结果较好。 |
| `data/report_main/opening_cost_prediction_time_report.png` / `.svg` / `.pdf` | 报告图 13a：从 v4 原合成图拆出的六种方法真实—预测时间图，使用 11.0×7.4 英寸画布和不小于 14 pt 的图内文字，适合 A4 正文宽度插入。数据和 80 组候选均值口径不变。 |
| `data/report_main/opening_cost_ranking_decision_loss_report.png` / `.svg` / `.pdf` | 报告图 13b：从 v4 原合成图拆出的配置排序与错误选择额外耗时图，使用 11.5×6.6 英寸画布和不小于 14 pt 的图内文字；排序准确率、20 个留出场景和 5%/15% 参考值均不变。散点区域不放统计摘要框，六项图外图例完整解释点型、混合模型颜色和参考线。 |
| `data/report_main/opening_native_single_job_request_latency.png` / `.svg` | 单 Job 主结果：Job JCT、vLLM waiting、单请求 queue time、TTFT 四项原单位对齐；说明相近 makespan 可掩盖请求级排队。Project 暂无同一 2,048-row graph→gather 正式点，故只在图注说明缺口，不用 database-E2E 或 512-row eager 诊断补位。 |
| `data/report_main/opening_native_single_job_state_fingerprint.png` / `.svg` | 单作业服务状态联合观察：吞吐、正在运行请求数、排队请求数、KV、MFU、GPU 活跃率六项原单位对齐；区分过量排队与供给不足。图内底部说明不再使用 `formal` 或 `graph→gather` 等内部措辞。 |
| `data/report_main/opening_multijob_interference_tradeoff.png` / `.svg` | 同一总上限下的四 Job 对照图：每条线固定代表同一个 Job，从独立运行、1/4 份额、四 Job 静态竞争到共享方式，直接显示份额减少、并发竞争和共享未用份额的影响；右侧表格给出总体效率变化，进度折线说明四个 Job 都加快但改善幅度不同。静态与共享是同上限的两个互斥对照。 |

统一生成脚本：`scripts/generate_opening_story_figures_20260808.py`。数据、claim、视觉和
禁止外推合同：`audit/opening_story_figures_contract_20260808.md`；第一性原理的选图依据见
`audit/opening_required_data_figures_20260810.md`。2026-08-10 已统一重建九张正文数据图
A/T/N/C/H/D/I/J/E，新增单 Job 任务—请求主图，并保留 F 状态备份图；十一张 PNG/SVG 均已打开复核，无裁切、
缺字或文字重叠，并通过 300 DPI、矢量、灰度与颜色外形状编码检查。当前仍未制作新的
PPT 成品；旧 PPT 只是历史底稿。

2026-08-22 为与开题报告的证据说明一致，重新生成 A/C/H/D/E 五张图并同步报告副本：A 把图内的
`active work`、`endpoint`、`formal` 等简写改为中文含义，并明确 29% 是运行期间峰值相对于配置上限的比例；C 明确比较的是
两种完整服务部署条件；H 的静态与共享完成进度统一使用作业独占完整资源的独立运行参照；D 不再把主机端
传输测量写成“PCIe 不是瓶颈”的单一归因；E 改为直接说明排序准确率和决策损失参考值。数据和统计方法未改变，详细记录见
`audit/opening_story_figures_contract_20260808.md`。

### 2026-08-11 可编辑概念图候选稿

`architecture/editable/` 新增五张按当前 20 页答辩主线重构的 Draw.io 候选图：研究空白与
方案概览、总体闭环、Work-unit 与数据组织、状态感知提交/路由/多作业、因果验证路线。每张同时
保留 `.drawio`、SVG、1600×900 PNG、逐元素审计和独立 SVG icon 资产；参考图只用于版式与
图形语言，不作为整图截图嵌入。多模态复用并入总体闭环，不再单独重复一张大架构图。

本批次是供用户逐图确认的 PPT-ready 候选稿，尚未替换报告当前引用的
`opening_ai_data_execution_boundary.*` 与 `opening_work_to_schedule_overview.*`。选择合同、
编辑方式和图标来源边界见 `audit/opening_editable_diagrams_manifest_20260811.md` 与
`architecture/editable/README.md`。

用户复核后完成源图层清理：01 删除面向内部的研究边界锁卡并将“嵌入/分类”改为“图像表征/分类”；
02 删除旧整页 raster base 和后加覆盖层，重建单一原生紫色模态面板与绿色 Sink；04 合并重复
completion 路径；05 删除七个圆角修补 mask，并把窄卡文字改为显式两行。五张图禁止用遮罩或
新卡片盖住错误对象，必须删除错误节点后再导出。

2026-08-17 再次修订 01/P05：将标题、分区、卡片标题、正文和结论分别提高到
42/30/25/20/26 px；跨栏请求/提交箭头改为固定像素小箭头头与 60 px 完整线身，反馈箭头同步使用
较小固定头和灰色虚线。权威源与开题专用图集副本均已同步，未修改 PPT。

2026-08-18 新增 03b/P12A（WorkDescriptor 定义页）与 03c/P13A（Work Organizer 定义页）：
按导师反馈把研究内容一拆成三页——03b 只讲 WorkDescriptor 本身（三层结构：Work Estimation
四阶段 → Work/Locality/Job-SLO/Confidence 四分类字段 → Consumers），03c 讲 Work Organizer
（三设计维度 + 五臂候选策略 + 统一比较条件框），原 03/P12（packing 区）保留不动。03b/03c
均由用户手调 drawio 后按本机 draw.io CLI + 图标内联 + headless Chrome 管线渲染，图集副本
命名为 P12A/P13A。

2026-08-12 又在 `architecture/editable/opening_background_20260812/` 增补第 2–4 页三张背景图：
数据库 AI 算子外部执行链路、传统数据库算子与 AI 语义算子外部物理执行的假设对照、相关工作分层。
三图已按项目事实重写并通过 Draw.io 结构检查、1600×900 与 PPT 缩放目视审计；图集副本命名为
`P02`、`P03`、`P04`。

2026-08-16 按“背景现象 → 相关工作 → 研究空白 → 项目方案”的叙事顺序再次清理图资产：P02
暂不修改；P03 改为 AI 语义算子的通用外部物理执行六阶段链路，删除 Work Unit、credit、状态反馈和项目后端实现；
P04 只保留相关工作分层与衔接不足；P05 只陈述三类研究空白，不提前展示方案。本轮没有重生成 PPT。

2026-08-25 根据开题报告最新精读综述更新 P04：数据库侧加入 IMBridge，数据执行侧加入 AYO，
模型服务侧更新为 Parrot、VTC、DLPM 和 BlendServe；Ray Data 与 Daft 继续作为数据执行框架代表。
底部结论改为数据库作业信息与模型服务状态共同指导上游数据组织和请求提交，并同步 Draw.io、SVG
和 4000×2250 PNG，供开题 PPT 第 5 页替换使用；未修改 PPT 文件。
各系统的会议与年份以 14 px 深灰字标在名称之后，不使用文献编号或括号；Cortex AISQL 和 Learned
Cost Models 因横向空间不足将出处另起一行。Ray Data 按作者公开论文列表标为 NSDI 2027。

02 后续按用户局部复核再次清理：Admission 标题改用卡片整行宽度，两个 Adapter 卡重排图标、
标题与正文安全边距；删除手绘 Daft 近似符号，改用 Daft 官方仓库的黑/洋红标识。官方 Logo 仅作
产品识别，保留原比例与颜色，来源、哈希与商标边界记录在 `02_system_architecture.audit.md`。

03 按用户反馈完成整体字体与版面密度提升：主标题 42 px、三栏标题 30 px、机制卡标题 24 px，
正文、字段、徽标和图例统一不低于 20 px；通过重分配盲点、Work Estimation、WorkDescriptor 和
评价区的文本框与内距解决放大后的边界问题，没有删减内容、增加遮罩或改变箭头语义。

本组图的视觉语法已冻结：颜色表示系统或策略，形状只在需要冗余编码时保留；圆点、
方框、三角、菱形若表示执行路径，必须由图内图例逐项映射；若表示统计量，图例或页脚
必须写明均值、中位数、macro 均值或最大值。误差线必须注明 SD，普通横线必须注明是
中位数至最大值范围或同一 Job 的成对变化。未承担上述语义的装饰性散点与线段不得保留。

2026-08-10 绘图完成后的冻结状态如下：

| 后续项 | 状态 | 用途与边界 |
|---|---|---|
| 原生文本单 Job 任务—请求主图 | `rendered-qa-pass` | 四臂 12 formal；JCT/waiting/queue time/TTFT 原单位对齐，Ray Data 欠供给单列诊断 |
| 原生文本单 Job 状态补充 | `rendered-qa-pass` | tok/s、running、waiting、KV、MFU、GPU utilization 解释 JCT 背后的供给与资源状态 |
| 文本 baseline 分轨图 | `rendered-qa-pass` | DuckDB 产品轨与 Daft/Ray Chat graph 轨均呈现；合同不同的 panel 禁止互排 |
| 原生文本四 Job 归一化干扰 | `rendered-qa-pass` | Daft Native/Ray、Ray Data 的 Short 与 3 个 Long；使用slowdown影响矩阵，格内直标倍率和百分比增幅，SD留在附录数据 |
| 四 Job 干扰与共享权衡 | `rendered-qa-pass` | Project按同一Job连接独立→1/4配额→静态竞争→共享调度；效率表与Static→Shared进度折线分别展示总收益和公平代价 |
| 图像 staged-work 动机 | `rendered-qa-pass` | prepare/model、R0/R1/R2 transfer 形态和 active-window screening 分开呈现；不把 microprofile 或 screening 写成系统排名 |
| 图像 baseline 数据图 | `rendered-qa-pass` | 图内只画 12K 诊断和 120K 正式对照；能力门禁和路径角色与数据图分离 |
| 图像四 Job 归一化干扰 | `rendered-qa-pass` | 4×4 路径/策略×Job slowdown 矩阵，与文本四 Job 图统一；不画误差线或跨系统绝对排名，Project 状态只作 observe-only 证据 |
| 同上限 static–dynamic phase change | `do-not-draw-no-result` | 只保留实验合同；正式 A/B 完成前不画示意结果曲线 |
| database-E2E replacement 三臂 | `appendix-table-only` | SQuAD 作静态 correctness 地基；ShareGPT 因 C32 direct 欠供给与 DuckDB cap 语义失败不作性能排名 |

本轮第一性原理数据图清单 A/T/N/C/H/D/I/J/E 与备份图 F 已完成；G 仍不画。
完整输入行数、关键字段、SHA256 与视觉 QA 记录见上述 audit 合同。
2026-08-09 cost LOO JSON 仅规范化 6 处 `§6` 的 UTF-8 编码，图 E 的冻结输入 SHA 前缀
相应更新为 `bbb2f2f8c5c1c07f`；字段、数值和既有图 E 均不变，无需重画。

## 2026-08-07 四图（被 2026-08-08 叙事重构取代）

下列图保留可复现性和历史引用，但不再作为新报告/PPT 的首选入口：

| 图 | 核心结论与边界 |
|---|---|
| `data/report_main/opening_serving_capacity_frontier.png` / `.svg` | 65K active work/endpoint 已达最大已测吞吐均值的 97.80%，继续增压时吞吐趋于平台而 P99 上升；这是当前配置的最小近饱和点，不是 vLLM 内部容量上限 |
| `data/report_main/opening_work_organization_regime.png` / `.svg` | 数据组织排名随 KV 压力反转；重排序破坏 prefix locality 的效应只在 4-endpoint 高 KV 压力 regime 明显，不能外推为某策略全局最优 |
| `data/report_main/opening_image_matched_resource.png` / `.svg` | 匹配 CPU/GPU 资源后，项目静态分级 actor 路径的 operator JCT 主实验改善 12.8%/15.1%，独立复测同向；禁止使用旧 45.7% 口径 |
| `data/report_main/opening_cost_model_decision_quality.png` / `.svg` | Hybrid 首次通过候选选择合同，但 max regret=14.72% 距 15% 线仅 0.28 pp，只能称 marginal pass |

统一生成脚本：`scripts/generate_opening_core_evidence_figures.py`。设计合同、数据来源、统计口径和渲染后 QA：`audit/opening_core_evidence_figures_contract_20260807.md`。四图均由正式结果 CSV/JSON 重建，排除 warm-up；SVG 保留可编辑文字，PNG 为报告/PPT 兼容副本。

## 目录结构

```text
figures/
  architecture/       系统架构图、流程图、方法框架图
  data/report_main/   报告、PPT、论文正文优先使用的数据图
  data/backup/        答辩备份、飞书补充和 learning 可选支撑图
  audit/              图表质检、图表选择说明、设计审计记录
  scripts/            可复现绘图脚本
```

## 正文主线图

```text
figures/architecture/
figures/data/report_main/
```

建议进入开题报告正文、PPT 正文，后续中期汇报和毕业论文也优先从这里选图：

| 文件 | 用途 |
|---|---|
| `architecture/system_architecture_ai_data_execution.png` / `.svg` | 课题总体研究框架，定义数据库 -> Daft/Arrow -> Ray -> GPU model service -> sink 的研究对象，并标出计划层、运行层、服务端动态批处理和写回判定位置 |
| `architecture/research_gap_three_islands.png` / `.svg` | 研究缺口图，说明三个成熟方向（DB4AI、推理服务、数据存储）之间的空白和本课题定位 |
| `architecture/cross_layer_method_framework.png` / `.svg` | 研究方案图，说明三类 AI workload、分阶段性能剖析、三层上游执行策略、结果写回瓶颈判定和端到端效果评估 |
| `architecture/runtime_strategy_control_loop.png` / `.svg` | 运行时策略闭环图，当前首选策略机制图；用一个 AI_EMBED SQL 例子说明计划层 batch/partition、运行层 K_max/routing/backpressure、服务端 micro-batch 和写回 guardrail 如何协同 |
| `data/report_main/02_gpu_stage_latency_stack.png` / `.svg` | 真实 GPU-backed 链路阶段耗时，说明端到端成本可拆解、可观测 |
| `data/report_main/03_invocation_granularity.png` / `.svg` | 调用粒度对比，说明 batch / invocation 粒度值得调 |
| `data/report_main/04_executor_endpoint_comparison.png` / `.svg` | single / dual endpoint 下执行方式对比，说明 Ray 的价值依赖模型服务并行条件 |
| `data/report_main/05_actor_endpoint_scaling_writeback.png` / `.svg` | actor endpoint scaling 和写回约束，说明只优化模型调用会被 writeback 限制 |
| `data/report_main/opening_serving_capacity_frontier.png` / `.svg` | 开题冻结证据一：serving capacity、最小近饱和 active work 与尾延迟边界 |
| `data/report_main/opening_work_organization_regime.png` / `.svg` | 开题冻结证据二：数据组织的 regime dependency 与 prefix locality 机制 |
| `data/report_main/opening_image_matched_resource.png` / `.svg` | 开题冻结证据三：图像 workload 的 matched-resource 静态结构收益 |
| `data/report_main/opening_cost_model_decision_quality.png` / `.svg` | 开题冻结证据四：代价估计的候选选择质量与 marginal-pass 边界 |

## 备份与补充图

目录：

```text
figures/data/backup/
```

建议用于 PPT 备份页、飞书补充说明、learning 讲解或答辩问答：

| 文件 | 用途 |
|---|---|
| `b01_workload_matrix_speedup.png` | 解释为什么保留 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 三类 workload |
| `b02_granularity_attribution.png` | 解释为什么调 batch / partition / task / invocation / object / fan-in |
| `b03_backpressure_queue_pressure.png` | 解释为什么调 bounded in-flight、queue wait 和 backpressure |
| `b04_writeback_batching.png` | 解释为什么 writeback 和持久化协同值得研究 |
| `b05_ray_arrow_fanout_fanin.png` | 解释 Ray / Arrow object 粒度和 fan-in 组件信号 |
| `b06_stage_share.png` / `.svg` | 补充展示 GPU request wall 和 writeback 的阶段占比变化 |

## 使用边界

- `report_main/` 中的 GPU 图来自真实 GPU-backed CSV，可作为开题主动机和可行性分析主证据。
- `data/backup/` 中部分图来自 fake/CPU、PG18.4 fake 或 feasibility benchmark，只能用于解释实验设计来源和变量选择，不能替代真实 GPU-backed 结论。
- 报告或 PPT 中引用这些图时，图注必须说明对应证据层级和不能声称的边界。

## 保留规则

图表资产长期只保留以下内容：

```text
figures/
motivation/results/
feasibility/results/
```

其中 `figures/` 保留最终图、绘图脚本和图表审计；`motivation/results/` 与 `feasibility/results/` 保留原始 CSV / 结果报告。生成过程中的中间 PNG / SVG 不长期保留。

可以删除或不再使用的旧目录包括：

```text
opening/assets/charts/python/
opening/assets/charts/all_meaningful/
opening/assets/charts/gpu_embed_*.png
opening/assets/charts/gpu_embed_*.svg
opening/assets/generate_echarts_experiment_charts.js
opening/assets/charts/selected/
opening/assets/figures/system_architecture_ai_data_execution.*
learning/figures/ 中与本目录重复的正式图副本
```

删除前需要确认：

1. `opening/report/opening_report.md` 不再引用旧图路径。
2. `opening/feishu/opening_report_wiki.md` 不再引用旧图路径。
3. learning、中期汇报、毕业论文草稿不再引用旧图路径。
4. 开题 PPT 源稿和 PPTX 不再引用旧图路径。
5. 线上飞书文档中的图片已经重新上传为 `figures/data/report_main/` 对应版本。
6. 若仍需要复现旧图，保留 Python 脚本和原始 CSV 即可，不保留旧 PNG / SVG 副本。

## 2026-07-14 pgai-integrated GPU rerun figures

Latest report-main figures generated from
`motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv`:

```text
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.png
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.svg
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.png
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.svg
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.png
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.svg
```

Audit:

```text
figures/audit/pgai_integrated_gpu_rerun_charts_audit_20260714.md
```

These figures should be cited before older 2026-07-12 GPU figures when the
opening report needs the latest local pgai-integrated GPU-backed rerun.

## 2026-07-14 pgvector(384) writeback figure

Latest sink-mode comparison generated from
`motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv`:

```text
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.svg
```

Audit:

```text
figures/audit/pgvector_writeback_chart_audit_20260714.md
```

This figure should be used when discussing whether JSON text writeback and
pgvector `vector(384)` writeback have the same cost in the local GPU-backed
chain.

## 2026-07-15 strategy figure design notes

Before redrawing the strategy-design figure, read:

```text
figures/audit/top_venue_strategy_figure_design_notes.md
figures/audit/strategy_figure_micro_design_points.md
figures/audit/local_reference_figure_reading_notes.md
```

This note extracts design patterns from top systems papers and recommends that
the next strategy figure use a control-loop plus compact rule-table structure,
rather than a three-column list of signals and actions.

`strategy_figure_micro_design_points.md` further decomposes the strategy figure
into smaller drawable mechanisms: workload-aware batch/partition selection,
bounded in-flight control, endpoint routing, writeback guardrails, and the
Trigger -> Action -> Guardrail rule table.

`local_reference_figure_reading_notes.md` records figure-design lessons from
the locally downloaded PDF subset under `research/reference/` and
connects them to the current runtime control-loop figure.

The generated control-loop figure is:

```text
figures/architecture/runtime_strategy_control_loop.png
figures/architecture/runtime_strategy_control_loop.svg
figures/architecture/runtime_strategy_rule_table.png
figures/architecture/runtime_strategy_rule_table.svg
figures/scripts/generate_runtime_strategy_control_loop.py
figures/audit/runtime_strategy_control_loop_audit.md
```

`runtime_strategy_control_loop.*` and `runtime_strategy_rule_table.*` are used
as a pair: the first figure explains the runtime feedback loop, and the second
figure explains the compact observed-signal -> candidate-action -> guardrail
table. The rule table records candidate logic to be validated, not final proven
rules. Visible labels are mostly Chinese, with only necessary technical tokens
such as `AI_EMBED`, `SQL`, `GPU`, `K_max`, `P99`, and `token` retained.

## 2026-07-15 archived strategy iterations

The following strategy-figure iterations are archived and should not be used as
current opening-report or PPT figures:

```text
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.png
figures/archive/architecture/20260715_strategy_iterations/upstream_strategy_design.svg
figures/archive/architecture/20260715_strategy_iterations/optimization_strategy_logic.png
figures/archive/architecture/20260715_strategy_iterations/optimization_strategy_logic.svg
figures/archive/architecture/20260715_strategy_iterations/_font_test.png
```

Use `runtime_strategy_control_loop.*` and `runtime_strategy_rule_table.*`
instead for the current strategy-design explanation.

## 2026-07-18 local vLLM Ray baseline support figures

Latest learning-support figures generated from the local ShareGPT/BurstGPT
`AI_COMPLETE` baseline:

```text
figures/data/backup/b07_local_vllm_ray_throughput.png
figures/data/backup/b07_local_vllm_ray_throughput.svg
figures/data/backup/b08_local_vllm_ray_e2e_time.png
figures/data/backup/b08_local_vllm_ray_e2e_time.svg
figures/data/backup/b09_local_vllm_ray_task_stage_timing.png
figures/data/backup/b09_local_vllm_ray_task_stage_timing.svg
figures/data/backup/b10_local_vllm_request_count_inflight.png
figures/data/backup/b10_local_vllm_request_count_inflight.svg
figures/data/backup/b11_local_vllm_token_tail_performance.png
figures/data/backup/b11_local_vllm_token_tail_performance.svg
figures/data/backup/b12_local_vllm_latency_probe_breakdown.png
figures/data/backup/b12_local_vllm_latency_probe_breakdown.svg
figures/data/backup/b13_local_vllm_token_tail_penalty.png
figures/data/backup/b13_local_vllm_token_tail_penalty.svg
figures/data/backup/b14_local_vllm_service_tail_gap.png
figures/data/backup/b14_local_vllm_service_tail_gap.svg
figures/data/backup/b15_local_vllm_token_budget_throughput.png
figures/data/backup/b15_local_vllm_token_budget_throughput.svg
figures/data/backup/b16_local_vllm_token_budget_tail_queue.png
figures/data/backup/b16_local_vllm_token_budget_tail_queue.svg
figures/data/backup/b17_local_vllm_arrival_kmax_sweep.png
figures/data/backup/b17_local_vllm_arrival_kmax_sweep.svg
figures/data/backup/b18_local_vllm_batch_kmax_e2e.png
figures/data/backup/b18_local_vllm_batch_kmax_e2e.svg
figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.png
figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.svg
figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.png
figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.svg
figures/data/backup/b21_local_vllm_kmax_interference_small_job.png
figures/data/backup/b21_local_vllm_kmax_interference_small_job.svg
figures/data/backup/b22_local_vllm_length_prefix_tail.png
figures/data/backup/b22_local_vllm_length_prefix_tail.svg
figures/data/backup/b23_local_vllm_length_prefix_signal.png
figures/data/backup/b23_local_vllm_length_prefix_signal.svg
figures/data/backup/b24_local_vllm_interference_sweep_small_job.png
figures/data/backup/b24_local_vllm_interference_sweep_small_job.svg
figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.png
figures/data/backup/b25_local_vllm_interference_sweep_bulk_tradeoff.svg
figures/scripts/generate_local_vllm_ray_baseline_charts.py
```

Audit:

```text
figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md
```

These are backup and learning figures for the local
`PostgreSQL -> Daft -> Ray -> vLLM` fixed row-batch baseline. Each figure has a
single purpose: throughput, end-to-end time, Ray task stage timing, request
in-flight utilization, token-tail performance, latency metric breakdown,
token-tail penalty, service-tail gap, token-budget throughput comparison,
token-budget tail/queue comparison, arrival-aware `K_max` sweep, or coupled
batch-policy x `K_max` matrix analysis.
They show batch-size overheads and why fixed row count is an imprecise proxy
for model request cost. `b15` and `b16` split the first direct token-budget
policy comparison into a throughput view and a token-tail/queue view. They are
still local single-endpoint, no-writeback results; they motivate the next
`K_max` and queue-adaptive experiments rather than proving the full optimized
method. `b17` is a preliminary single-request-shape scheduling support figure.
`b18`-`b20` are the corrected coupled scheduling figures: they vary fixed-row
and token-budget batch shapes together with `K_max`, showing end-to-end
plateaus, vLLM queue/service-tail pressure, and the request-granularity limit
that makes very large fixed batches leave little room for admission control.
`b21` is the first shared-service interference figure: a foreground small job
shares the same vLLM endpoint with a background bulk job, showing that
unbounded background inflight hurts foreground E2E, service P95, and queue
stability compared with bounded `K_max=8`.
`b22` and `b23` record the first length-align and prefix-aware data
organization ablation. `b22` separates token tail from service tail; `b23`
shows the organization signals. The current prefix-aware result only shows a
small prefix-group-ratio change, not a proven KV-cache or APC benefit.
`b24` and `b25` extend the shared-vLLM interference experiment into a formal
sweep over background `K_max={8,16,unbounded}` plus a tuned queue-adaptive
implementation test. In this run, `K_max=8` protects the foreground job better
than larger background inflight. Tuned adaptive does downshift, but it is not
yet better than static `K_max=8`.
# Figure asset updates

- 2026-08-25: 开题报告图 2 删除右上角“开题第 5 页｜研究问题”幻灯片页码残留，保留三项跨层能力、左右两侧已有能力和底部研究问题的原有内容与布局；同步 `architecture/editable/01_research_gap.{drawio,svg}` 和报告 1600×900 PNG。图 7、图 15 按用户要求保持不变；视觉与源文件检查补记于 `audit/opening_report_figure_readability_audit_20260824.md`。
- 2026-08-25: 为 Parrot 精读笔记从正式 OSDI 2024 proceedings PDF 裁剪正文全部 Figure 1–19，输出到 `research/精读文献笔记/parrot_osdi2024/figures/`；配图覆盖四类应用工作流、Semantic Variable 依赖、连续请求开销、应用级调度动机、prompt 结构、系统架构、API 与跨请求分析、目标推导、baseline capacity 校准，以及 Chain/Map-Reduce/Bing/GPTs/Multi-agent/Mixed workloads 六类结果。Table、Algorithm 与公式已在正文转写，不重复截图；正式版页码、SHA256、读图方法和视觉 QA 见 `audit/parrot_deep_reading_figures_audit_20260825.md`。
- 2026-08-24: 为 DLPM/D²LPM 精读笔记从用户本地 arXiv:2501.14312v1 PDF 选择并裁剪正文全部 Figure 1–12，输出到 `research/精读文献笔记/dlpm_2025/figures/`；配图覆盖 Qᵘ 吞吐—公平权衡、LPM/VTC/DLPM 冲突、两层问题空间、centralized overhead、D²LPM 架构、workload graph、synthetic 主结果、真实 trace、公平时间序列以及 Qʷ/client-scaling/mixed-workload 消融。Table 与 Algorithm 已由正文转写，不重复截图；原 Word 转换稿中重复的 Figure 1 只保留一份。版本、页码、SHA256、缺失点/Long-Context 反例与视觉 QA 见 `audit/dlpm_deep_reading_figures_audit_20260824.md`。
- 2026-08-24: 为 IMBridge 精读笔记从用户本地 4 页 SIGMOD-Companion ’24 PDF 选择并裁剪全部 Figure 1–6，输出到 `research/精读文献笔记/IMBridge_sigmod2024/figures/`；配图覆盖 prediction query 用户接口、系统架构、两类 impedance mismatch、函数生命周期改写及两项机制的演示结果。原文没有编号 Table 或 Algorithm，已转写内容不重复截图；本地文件名 `IMBridge_2026.pdf` 不作为发表年份，Figure 5/6 只按 demo 截图解读。版本、页码、SHA256 与视觉 QA 见 `audit/imbridge_deep_reading_figures_audit_20260824.md`。
- 2026-08-24: 根据开题报告计时口径复核，更新文本路径对照图 `opening_text_baseline_evidence_map`。左图不再写“SQuAD 结果可直接比较”，改为“质量可核对，性能暂不排名”，并说明项目路径的计时还包含指标采集和记录处理。三条柱形、误差线和原始实验数值均未改变；同步权威 PNG/SVG/PDF、开题图集与报告副本，审计见 `audit/opening_report_figure_readability_audit_20260824.md`。
- 2026-08-24: 为 Sema 精读笔记从本地 arXiv v1 PDF 裁剪正文全部 Figure 1–8，输出到 `research/精读文献笔记/sema_vldb2026/figures/`，并替换笔记中失效的 `/mnt/data/sema_figures/` 临时路径。配图覆盖系统架构、SemaSQL 示例、端到端 workflow、总体 latency/quality、execution optimization、AQE breakdown 与 Q6 case study；Table 1、Algorithm 1 和附录 Figure 9–40 已有等价文字转写，不重复截图。版本边界、页码、SHA256 与视觉 QA 见 `audit/sema_deep_reading_figures_audit_20260824.md`。
- 2026-08-24: 为 Abacus 精读笔记核对正式 PVLDB 版全部 Figure 1–8。新增目录中的 8 个 PNG 与论文一致，但正文错误引用不存在的 `assets/fig*.png`；现已统一修复为 `research/精读文献笔记/abacus_pvldb2026/figures/`，并在动机、系统流程、Cascades、三个 benchmark 查询计划、prior、约束响应和消融对应段落补充来源与证据边界。图号、页码、SHA256、视觉一致性和低分辨率边界见 `audit/abacus_deep_reading_figures_audit_20260824.md`。
- 2026-08-24: 为 Palimpzest 精读笔记从本地 2024 arXiv v2 PDF 选择并裁剪全部 Figure 1–7，输出 7 个裁剪件到 `research/精读文献笔记/palimpzest_cidr2025/figures/`；配图覆盖系统流程、三个 SAPP 工作负载、声明式程序、关系代数、多模态依赖、实测 Pareto frontier 和 Policy 选择结果。附录 Figure 8–9 与已转写的 workload/程序信息重复，不加入正文；选图、CIDR 版本边界、页码、SHA256 与视觉 QA 见 `audit/palimpzest_deep_reading_figures_audit_20260824.md`。
- 2026-08-24: 新增实现状态候选图 `architecture/opening_target_architecture_status.{png,svg}` 及语义命名副本。该资产现只保留作历史 / 内部候选，当前报告不引用；生成脚本为 `scripts/generate_opening_target_architecture_status.py`，视觉与主张审计见 `audit/opening_target_architecture_status_audit_20260824.md`。
- 2026-08-24: 完成开题报告图 1、图 6、图 9、图 13、图 14 的可读性专项。图 1 改为“AI 语义算子的外部物理执行”；图 6 将运行、排队轴名和底部说明自然中文化；图 9 将容量记录、队列选择、模型执行、补位、释放和运行状态框中文化；代价估计原合成图保留为备用并新增图 13a、13b 两张 A4 大字号拆分图，原合成图与图 13b 均删除散点区域内的统计数字 / 摘要框，以图外六项图例解释点型、颜色和参考线；图 14 保留 `baseline` 并自然化未解释简写。完整源映射、尺寸、哈希和视觉检查见 `audit/opening_report_figure_readability_audit_20260824.md`。
- 2026-08-24: 为 Galois 精读笔记从本地 SIGMOD 2025 论文 PDF 选择并裁剪 Figure 1–4、7–11，输出 9 个裁剪件到 `research/精读文献笔记/galois_sigmod2025/figures/`；配图覆盖 DB-first 动机、predicate pushdown、Table-Scan/Key-Scan、logical-plan 枚举、logprob 过滤、query complexity 质量/成本、`τ` 校准和 oracle-optimal gap。Figure 5–6 的 prompt syntax 与 Table/Algorithm 已由正文转写，不重复截图；选择理由、页码、SHA256 与视觉 QA 见 `audit/galois_deep_reading_figures_audit_20260824.md`。
- 2026-08-23: 修正开题报告图 2、图 3、图 5 和图 6，并同步权威源、开题专用图集与报告副本。图 2 用具体动作说明三项跨层能力；图 3 将 WorkDescriptor 基础字段与可选代价估计结果分开；图 5 将代价估计连接数据库优化器 / 多 SQL 调度，将实际运行状态返回提交与路由模块；图 6 只调整公开组名和说明文字，七个实验数值保持不变。完整路径、SHA256 和视觉检查见 `audit/opening_report_minimal_figure_corrections_audit_20260823.md`。
- 2026-08-22: 为 Relational LLM Queries 精读笔记选择论文全部 Figure 1–6，并输出 6 个裁剪件到 `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/`；配图覆盖 fixed/per-row field ordering 动机、GGR 递归拆分、Filter/Projection/RAG 主结果、Multi-LLM/Aggregation、Llama-3-70B 趋势与 accuracy correctness。Algorithm 1 与 Table 1–7 已由正文转写，不重复截图；版本、选择理由、页码、SHA256 与视觉 QA 见 `audit/relational_llm_queries_deep_reading_figures_audit_20260822.md`。
- 2026-08-22: 为 VTC 精读笔记选择 Figure 1、2、3、4、6、8、9、10、12、15、16、19，并输出 12 个裁剪件到 `research/精读文献笔记/vtc_osdi2024/figures/`；配图覆盖 VTC 调度位置、动态 cost/capacity、公平与 work-conservation、backlog 语义、异构 input/output cost、isolation、Counter Lift、真实 trace、bound sensitivity、weighted fairness 和 length prediction。其余重复图、已转录 Table/Algorithm 与 profiled-cost 扩展不重复截图；版本、选择理由、页码、SHA256 与视觉 QA 见 `audit/vtc_deep_reading_figures_audit_20260822.md`。
- 2026-08-22: 为 BlendServe 精读笔记选择 Figure 1、2、3、4、5、6、7、9、10、11，并输出 10 个裁剪件到 `research/精读文献笔记/blendserve_asplos2026/figures/`；配图覆盖 batching 动机、trace 分布、资源失衡、compute density、完整设计、dual scanner、端到端结果、prefix locality、resource balance 与 simulated sensitivity。Figure 8/12–15、Table 与 Algorithm 已由正文转写或不承担新的独立机制，未重复截图；选择、页码、SHA256 与视觉 QA 见 `audit/blendserve_deep_reading_figures_audit_20260822.md`。
- 2026-08-22: 为 Ray OSDI 2018 精读笔记选择 Figure 4、5、6、7、8、10、11、12、14，并输出 10 个裁剪件到 `research/精读文献笔记/ray_osdi2018/figures/`；Figure 10a/10b 因对应 GCS reconfiguration 与 flushing 两个独立小节而分开。其余流程图、代码、已转录 microbenchmark 与 building-block 结果不重复截图；选择、页码、SHA256 与视觉 QA 见 `audit/ray_osdi2018_deep_reading_figures_audit_20260822.md`。
- 2026-08-22: 为 Cortex AISQL 精读笔记选择并裁剪 Figure 1、7、9、10、11、12，为 Ray Data Streaming Batch 精读笔记选择并裁剪 Figure 2、4、5、6、7、9；两组分别输出到对应论文目录的 `figures/`。其余背景图、已转录表格/算法和信息重复图不截图；版本、选择理由、页码、SHA256 与视觉 QA 见 `audit/cortex_aisql_deep_reading_figures_audit_20260822.md` 和 `audit/ray_data_streaming_batch_deep_reading_figures_audit_20260822.md`。
- 2026-08-22: 为 AYO 精读笔记从用户提供的 ASPLOS 正式版 PDF 选择并裁剪 Figure 1、3、4、5、6、7、8、9、10、11、12，输出位于 `research/精读文献笔记/ayo_asplos2025/figures/`。Figure 2 与 Algorithm/Table 继续使用正文转写；选择、页码、SHA256 与视觉 QA 见 `audit/ayo_deep_reading_figures_audit_20260822.md`。
- 2026-08-21: 为 LOTUS 精读笔记从正式 PVLDB PDF 选择并裁剪 Figure 1、4、6、7，输出位于 `research/精读文献笔记/lotus_pvldb2025/figures/`。这些是论文原图的 Markdown 显示副本，不进入项目实验图排名；选择、页码、SHA256 与视觉 QA 见 `audit/lotus_deep_reading_figures_audit_20260821.md`。
- 2026-08-11: Rebuilt the Shared Credit region in `architecture/editable/04_state_aware_scheduling` to remove the `Request Credit` overflow, replaced ambiguous coin/gauge icons with editable request-slot/work-budget SVGs, and re-audited adjacent arrows and borders.
- 2026-08-11: Corrected the two short orange inter-panel arrows and the green refill arrow in figure 04: compact heads now leave visible shafts, and the refill bend is vertically centered in the available gap.
- 2026-08-11: Rebuilt figure 02's editable Sink SVG so the PostgreSQL cylinder has a complete body and lower closure; the validation badge now sits outside the silhouette instead of visually cutting it away.
- 2026-08-11: Reframed editable figure 05 from an internal causal/evidence-gate workflow into an opening-defense research plan: two completed foundations, five deliberately broad future-work stages, unified experiment principles and expected evaluation dimensions. Removed visible contract/Trace/gate terminology and the S7 feedback connector.
