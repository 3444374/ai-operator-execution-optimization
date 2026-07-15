# 开题材料 project log

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

- 新增 `experiments/plans/strategy_design_implementation_reference.md`，作为后续实验设计和系统实现参考，汇总 Ray、vLLM/Ray Serve/Triton、GPU 数据放置和 DB AI 算子文献机制如何支持本课题三层策略。
- 明确三层策略口径：计划层数据组织、运行层入口调度、服务端 dynamic / continuous micro-batching；该口径后续应同步到开题报告、PPT 和答辩讲解中。
- 进一步补充“系统优化蓝图”和“机制到实现任务优先级”，把文献机制拆成 Workload Profiler、Plan-time Data Organizer、Ray Admission Controller、Endpoint Router、Service-side Micro-batcher 和 E2E Guardrail 六个可实现模块。
- 已同步 `experiments/plans/README.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`。

## 2026-07-15 GPU 调度与数据放置补充调研

- 新增 `opening/literature/gpu_scheduler_data_placement_supplement_20260715.md`，用于说明策略控制器设计参考了 GPU / LLM 推理服务调度、Ray/Ray Data 异构数据管线、GPU 数据库算子与数据库 AI 算子等文献线索。
- 该文件当前作为“设计依据与后续精读清单”，不替代逐篇精读笔记；未下载或未逐篇核验的条目仍按待核验处理。
- 已同步 `opening/README.md`、`opening/literature/reading_list.md` 和 `PROJECT_INDEX.md` 入口。

## 2026-07-15 策略设计重新评判与三层收窄

- 将策略机制图从“全运行时控制器”收窄为 three-layer upstream execution strategy：计划层在执行前选择 `batch_size` / `partition_count` / `object_merge`，运行层调整 `K_max`、routing、backpressure，服务端用 dynamic / continuous batching 形成推理 `micro-batch`。
- 当前不采用“运行时重切数据库侧已物化 RecordBatch”作为主方案；动态 batch 借鉴 vLLM / Ray Serve / Triton 思路，放在模型服务侧尚未执行的请求队列中。
- 补充 Ray OSDI 2018 调度思想映射：task/actor、resource-aware scheduling、local/global scheduler、object store locality 和 actor pool 可迁移为 task 粒度、actor 池、资源约束、placement/locality、`K_max` 与 routing 等实验变量。
- 已重新生成 `figures/architecture/runtime_strategy_control_loop.png` / `.svg`，并同步图表审计与策略设计文档。

## 2026-07-13 项目级图资产目录迁移

- 根据后续 learning、中期汇报和毕业论文都会复用图表的要求，将正式图资产从 `opening/assets/` 和 `learning/figures/` 迁移到根目录 `figures/`。
- 新增 `figures/AGENTS.md`、`figures/README.md`、`figures/audit/figure_plan.md`、`figures/audit/experiment_charts_audit.md` 和 `figures/scripts/README.md`，明确项目级图资产库、正文主图、备份图、审计记录和绘图脚本的职责。
- 当前正式主图位于 `figures/architecture/` 和 `figures/data/report_main/`；补充说明图位于 `figures/data/backup/`；学习讲解专用图位于 `figures/learning/`。
- 同步更新 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md`、`learning/experiment_walkthrough.md`、`learning/README.md`、`opening/navigation.md`、`opening/assets/README.md`、`PROJECT_INDEX.md` 和根目录 `README.md` 中的图路径或入口说明。
- 删除旧的 `opening/assets/charts/`、`opening/assets/figures/`、`learning/figures/` 以及旧 ECharts 生成脚本和重复系统架构图副本。后续如需重生成实验图，应使用 `figures/scripts/` 中的 Python 脚本和原始 CSV。

## 2026-07-13 开题报告本地调整与旧 PPT 作废

- 根据项目当前主线，将 `opening/report/opening_report.md` 中研究内容一从“数据组织与批处理执行调度”收束为“数据组织与批处理构造”，减少与研究内容二“GPU 推理服务状态感知的 Ray 并行调度与反压控制”的重叠。
- 同步更新 `PROJECT_OUTLINE.md`、根目录 `README.md`、`PROJECT_INDEX.md`、`overview/current_direction_and_plan.md`、`opening/outline.md` 和图表选择说明中的研究内容名称。
- 根据用户要求，当前 `opening/slides/opening_ppt.md` 和 `opening/slides/opening_defense_20260712.pptx` 的内容和表现形式先作废；保留学校模板中的标题区、正文安全区、图表区和页脚等页面布局经验。
- 新增 `opening/slides/README.md` 记录 PPT 当前状态。`opening/ppt_rules.md` 保留版式规则，并明确旧版 PPT 不再作为正式汇报内容依据。
- 本轮只调整本地开题报告和相关入口文件；飞书正文与线上文档等用户过目本地报告后再同步。

## 2026-07-13 GPU-backed 动机实验图生成

- 使用 `D:/Tools/echarts` 中的 ECharts + sharp 生成第一批开题实验图，脚本为 `opening/assets/generate_echarts_experiment_charts.js`。
- 输出目录为 `opening/assets/charts/`，每张图同时生成 SVG 与 PNG，便于报告和 PPT 分别引用。
- 当前生成三张图：fine vs coalesced 端到端耗时对比、16K 行阶段拆分、双 endpoint 下 Python / Ray task / Ray actor 对比。
- 数据均来自 `motivation/results/gpu/` 的真实 GPU-backed CSV，排除 warm-up，仅使用 formal repeats 平均；PG18.4 连接验证、dry-run 和 smoke 结果不单独画图，后续保留为表格或文字说明。
- 运行时 `sharp` 报告 fontconfig cache 不可写，但 SVG 和 PNG 均已生成；暂不修改系统级字体缓存配置。

## 2026-07-13 系统架构图版式修订

- 根据用户反馈修订 `opening/assets/figures/system_architecture_ai_data_execution.svg` 与 PNG 预览。
- 主要调整：主链路箭头改为水平通道连接，取消斜向研究箭头；各阶段框内改用等宽小标签；标题居中；底部研究卡片增高，避免文字贴边或越界。
- 根据二次反馈继续修订：将黄框“观测与策略层”的标签改为七个等宽等距标签，避免不明确的三段式间距；增大底部研究卡片编号与标题的间距，避免编号圆点与标题文字重叠。
- 根据三次反馈修复主链路阶段编号位置：编号改为框内左上角编号槽，避免越出边框或遮挡 `GPU model service` 等较长标题；同时将黄色“观测与策略层”与下方四个执行阶段左右边界对齐，并调整下指箭头到对应阶段中心线。
- 同步更新生成脚本 `opening/assets/generate_system_architecture_figure.py` 和质检记录 `opening/assets/figures/system_architecture_ai_data_execution_audit.md`。
- 当前图稿用于人工确认系统架构图方向，尚未自动替换到开题报告正文或重新生成 PPTX。

## 2026-07-13 系统架构图初稿

- 使用 `figure-designer` 重新设计开题系统架构图，图类型定位为 Solution Overview / System Architecture。
- 新增可复现生成脚本 `opening/assets/generate_system_architecture_figure.py`。
- 生成 SVG 与 PNG：
  - `opening/assets/figures/system_architecture_ai_data_execution.svg`
  - `opening/assets/figures/system_architecture_ai_data_execution.png`
- 新增质检记录 `opening/assets/figures/system_architecture_ai_data_execution_audit.md`，检查向量格式、字体、颜色、图注和无装饰性图表元素。
- 当前版本为初稿，尚未替换进开题报告正文或重新生成 PPTX，先供人工查看和确认结构。

## 2026-07-13 开题报告主线调整

- 根据用户确认，将开题报告题目调整为“面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究”。
- 重写 `opening/report/opening_report.md` 的背景、研究目标、研究内容、总体框架和预期创新点：数据库 AI 算子降为 workload 入口和验证场景，Daft/Arrow、Ray、GPU 模型服务、Lance / pgvector / PostgreSQL sink 成为数据执行与存储协同的研究主体。
- 同步更新 `opening/feishu/opening_report_wiki.md`、`opening/slides/opening_ppt.md`、`opening/outline.md`、`opening/qa_bank.md`、`opening/README.md` 和 `opening/AGENTS.md`。
- 同步检查并修改项目级规划文档：`README.md`、`PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`AGENTS.md`、`overview/current_direction_and_plan.md` 和 `motivation/plans/integration.md`。
- 尝试使用 user 身份覆盖写入开题飞书 wiki 时，`lark-cli` 因用户目录刷新锁文件权限返回 `Access is denied`；提升权限重试被自动审批拒绝。本地源稿已准备好，线上飞书 wiki 需要后续有权限后再同步。

## 2026-07-12

- 创建 `opening/` 开题工作区。
- 建立开题总体规则、PPT 制作规则、统一骨架、文献清单和答辩问答库。
- 当前汇报主线确定为：数据库 AI 算子的新执行链路问题 -> 阶段画像与初步瓶颈 -> 模型服务感知的 batch、调度、反压和写回优化。
## 2026-07-12 飞书同步目标

- 登记两个飞书同步目标：
  - 开题报告与开题汇报：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`
  - 动机测试与可行性测试：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`
- 新增 `opening/feishu/README.md`，规定本地 Markdown 是源稿、飞书是发布面。
- 后续实际写入飞书时使用 `lark-doc`；如遇嵌入 Base 或飞书幻灯片，再分别使用 `lark-base` 和 `lark-slides`。

## 2026-07-12 默认参考 skill

- 在 `opening/AGENTS.md` 中登记开题工作默认参考 skill：`karpathy-guidelines`、`humanizer`、`academic-research-suite`、`deep-research`、`nature-academic-search`、`vibe-research-workflow`、`ppt-master`、`nature-paper2ppt`、`lark-doc`、`lark-base`、`lark-slides`。
- 在 `opening/literature/reading_list.md` 中明确：文献检索优先使用 `nature-academic-search`，系统综述和研究缺口判断按需使用 `deep-research` 和 `academic-research-suite`。
- 补充说明：skill 是方法参考，不是固定流程；后续执行要结合本项目真实阶段、已有实验、学校模板和导师要求灵活使用。

## 2026-07-12 ECharts 图表规则

- 新增 `opening/assets/echarts_rules.md`，记录 ECharts SSR 生成 SVG、sharp 转 PNG、PPT 用 PNG、Word / 报告用 SVG 的流程。
- 在 `opening/ppt_rules.md` 和 `opening/assets/README.md` 中登记 ECharts 图表生成与嵌入规则。
- 在 `opening/AGENTS.md` 中补充 `figure-designer` 和 `nature-figure` 作为图表设计和论文级图表审查的可参考 skill。

## 2026-07-12 开题导航规则

- 新增 `opening/navigation.md`，说明开题材料需要项目内容、实验结果、文献、PPT 素材和飞书同步信息时分别从哪里找。
- 在 `opening/README.md` 和 `opening/AGENTS.md` 中登记 `navigation.md` 为开题目录阅读入口。
- 明确报告、PPT、飞书版之间的关系：报告负责完整论证，PPT 负责现场讲解，飞书负责发布同步，三者必须保持同一口径。

## 2026-07-12 开题报告与 PPT 初稿

- 补全 `opening/report/opening_report.md`，形成开题报告初稿，覆盖研究背景、国内外现状、研究目标、关键问题、技术路线、初步实验、预期创新点、进度安排和预期成果。
- 补全 `opening/slides/opening_ppt.md`，形成 16 页 PPT 内容源稿，并为每页加入 `汇报讲稿` 和 `答辩备注`。
- 更新 `opening/feishu/progress_update.md`，同步当前已完成内容、真实 GPU-backed 实验事实、问题、下周计划和待确认事项。
- 更新 `opening/literature/reading_list.md`，补充 Snowflake AISQL、Ray、Daft、Arrow、Lance、pgai、pgvector、PostgresML、Spark SQL tuning 等候选资料分类和精读顺序。
- 扩展 `opening/qa_bank.md`，补充 `AI_EMBED` 优先级、PG18.4/PG18.3 边界、pgvector 写回边界、模型推理淹没外部链路、传统查询优化区别等答辩问题。
- 更新 `opening/README.md`，将报告、PPT、飞书、文献和问答状态从占位阶段同步为初稿阶段。

## 2026-07-12 去除个人规划口径

- 根据用户反馈，调整开题报告、PPT、文献清单和答辩问答中关于 `AI infra / inference infra` 的表述。
- 开题材料改为只从项目和论文角度说明研究价值：数据库 AI 算子中的模型服务调用、批处理推理、外部调度和结果写回问题。
- 避免把研究方向表述为个人未来规划或职业取向。

## 2026-07-12 调整开题题目口径

- 根据用户反馈，将开题题目从“面向数据库 AI 算子的外部执行链路优化研究”调整为“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”。
- 调整 `opening/report/opening_report.md`、`opening/slides/opening_ppt.md`、`opening/outline.md`、`opening/README.md`、`opening/feishu/progress_update.md` 和 `opening/qa_bank.md`。
- 保留“外部链路 / 可控执行路径”作为系统边界解释，不再把它作为正式题目中的研究对象。

## 2026-07-12 正式材料术语降频

- 根据用户反馈，将正式报告和 PPT 源稿中的“外部链路 / 链路”进一步替换为“执行路径”“执行过程”“阶段画像”“跨系统执行过程”等更适合开题报告的表述。
- 保留少量“外部 worker”“外部执行”等技术事实表达，用于说明系统边界。

## 2026-07-12 开题报告正文去元话语

- 根据用户反馈，清理 `opening/report/opening_report.md` 中“当前最适合写入开题”“本文件用于承接学校模板”“草稿”等工作流提示。
- 将第 6 节改为正式报告口吻：直接陈述已完成实验、实验结果和适用边界。

## 2026-07-12 记录开题材料生成顺序

- 根据用户反馈，明确当前阶段不生成 DOCX，优先维护本地 Markdown 和飞书文档。
- 记录开题材料顺序：本地 Markdown -> 飞书文档补全 -> PPT -> PPT 同步飞书 -> 最终 DOCX 生成。
- 在 `opening/templates/README.md` 中记录：最终 DOCX 必须使用学校 Word 模板生成，继承章节样式、字体、行间距、图表标注和参考文献格式。

## 2026-07-12 继续完善报告与飞书源稿

- 补充 `opening/report/opening_report.md` 的进度安排细节、预期关键技术指标和主要参考文献初稿。
- 新增 `opening/feishu/opening_report_wiki.md`，作为“开题报告与开题汇报”飞书 wiki 的本地源稿。
- 更新 `opening/feishu/README.md`、`opening/README.md` 和 `opening/navigation.md`，登记飞书 wiki 源稿入口。
- 更新 `opening/feishu/progress_update.md`，同步当前“已按模板重排报告”和“已新增飞书 wiki 源稿”的进展。

## 2026-07-12 研究内容写法规则

- 根据用户提供的既往开题反馈经验，明确“研究内容”不能写成工程任务清单，应写成“问题、技术难点、拟采用方法、评估方式”的闭环。
- 重写 `opening/report/opening_report.md` 第 3.2 节，将原先的执行路径画像、batch 划分、endpoint routing、写回优化和多 workload 验证，改为可检验的研究问题。
- 同步更新 `opening/feishu/opening_report_wiki.md` 的研究内容口径，便于后续补全飞书文档。
- 更新 `opening/work_rules.md`，记录研究内容写作规则：工程步骤放在技术路线或进度安排里，研究内容必须说明为什么难、怎么解决、如何证明有效，并包含对比对象、评价指标、消融实验和适用边界。

## 2026-07-12 研究内容范围收敛

- 根据用户反馈，进一步区分“研究手段”和“研究内容”：阶段划分、执行画像和瓶颈归因保留为动机测试、方案设计和评价基础，不再作为独立研究内容或预期创新点。
- 将 `opening/report/opening_report.md` 第 3.2 节收敛为三个方法问题：模型服务感知的批处理执行调度、写回压力感知的结果汇聚与持久化协同、多类数据库 AI 算子的策略选择与适用边界。
- 使用 `humanizer` 检查并改写相关段落，减少机械模板句式和元话语，保留正式开题报告语气。
- 同步更新 `opening/feishu/opening_report_wiki.md` 和 `opening/report/opening_report.md` 的预期创新点。

## 2026-07-12 可行性分析表格化

- 根据用户反馈，重写 `opening/report/opening_report.md` 第 4.2 节，用表格说明已有实验链路、GPU-backed `AI_EMBED` 分阶段结果、双 endpoint Ray 动机测试和 fake/CPU 历史预研。
- 可行性分析明确区分正式 GPU-backed 实验事实、PG18.4 本地预演事实、fake/CPU 预研和不能声称的边界。
- 同步更新 `opening/feishu/opening_report_wiki.md` 的初步实验依据部分，补充关键结果表和预研结果使用方式。

## 2026-07-12 正文去除写作元话语

- 根据用户反馈，清理 `opening/report/opening_report.md` 研究内容中的“该部分不把目标写成”“不能写成”等写作提醒式表述。
- 将相关句子改为正式研究表述，直接说明研究对象、变量、评价方法和适用边界。
- 同步清理 `opening/feishu/opening_report_wiki.md` 中同类表达。

## 2026-07-12 开题与项目方向同步

- 根据用户反馈，明确开题材料不是独立展示稿，开题报告中收敛出的题目、研究内容和实验边界会反向影响项目方向与后续实验规划。
- 更新项目级 `README.md`、`PROJECT_INDEX.md` 和 `overview/current_direction_and_plan.md`，登记当前开题题目“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”及三项研究内容。
- 更新 `opening/README.md` 和 `opening/work_rules.md`，记录修改开题题目、研究内容或实验边界时必须同步检查项目入口文档，避免 opening 与项目主线割裂。

## 2026-07-12 飞书开题 wiki 同步

- 使用 user 身份将 `opening/feishu/opening_report_wiki.md` 覆盖写入开题飞书 wiki：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`。
- 飞书返回 `result=success`，文档 revision 更新为 `4`，5 个 Mermaid 图块成功解析为 whiteboard，无写入警告。
- 本次同步仍以本地 Markdown 为源稿；后续修改报告、PPT 或实验边界时，需要先改本地文件，再同步飞书。

## 2026-07-12 开题核心图规划

- 根据用户反馈，明确项目框架结构、总体流程和方向把控类大图必须保留，不能只依赖表格和文字说明。
- 新增 `opening/assets/figure_plan.md`，记录开题必须优先完成的三类图：课题总体研究框架图、端到端执行路径与阶段画像图、可行性实验关键结果图组。
- 在 `opening/report/opening_report.md` 第 3 节新增“总体研究框架”，用 Mermaid 描述三类 AI 算子场景、可观测批处理执行过程、三项研究内容和评价证据之间的关系。
- 同步更新 `opening/feishu/opening_report_wiki.md` 的研究内容部分，加入总体研究框架图源稿。
- 已重新同步飞书开题 wiki，飞书返回 `result=success`，文档 revision 更新为 `8`，6 个 Mermaid 图块成功解析为 whiteboard，无写入警告。

## 2026-07-12 开题与项目规划双向同步规则

- 根据用户反馈，进一步明确开题报告和项目规划不是单向关系：开题报告要基于当前项目进展与后续规划撰写；后续开题报告内容和方向调整时，项目整体规划、实验优先级和侧重点也要同步调整。
- 更新项目级 `README.md` 和 `PROJECT_INDEX.md`，明确开题报告、overview、motivation 计划和项目入口之间必须保持同一方向口径。
- 更新 `overview/current_direction_and_plan.md`，说明该文件与开题报告保持双向同步，不能长期描述两个不同方向。
- 更新 `opening/README.md` 和 `opening/work_rules.md`，将开题调整分为语言格式调整、方向内容调整、实验结论调整三类，并记录对应的同步检查范围。

## 2026-07-12 调整实验主线入口

- 根据用户反馈，降级 `feasibility/guide.md` 在项目索引中的地位：该文件只作为早期组件可行性验证指南，不再承担当前实验大纲职责。
- 重写 `PROJECT_INDEX.md` 第 3 节为“实验主线与证据入口在哪里”，将主入口调整为 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md`。
- 更新 `motivation/README.md`，移除 GPU-backed 结果“待补”的过时表述。
- 更新 `feasibility/README.md` 和 `feasibility/guide.md`，明确 feasibility 只负责组件、环境和脚本可用性，不承载开题主线或 GPU-backed 性能结论。

## 2026-07-12 根目录项目总纲与日志

- 根据用户反馈，新增根目录 `PROJECT_OUTLINE.md`，汇总当前项目大纲、实验主线、关键证据、近期优先级和开题/项目双向同步规则，方便直接从根目录阅读和调整。
- 新增根目录 `PROJECT_LOG.md`，作为项目级简要操作日志，用于记录跨目录、影响项目方向或入口结构的调整。
- 更新根目录 `README.md` 和 `PROJECT_INDEX.md`，登记 `PROJECT_OUTLINE.md` 和 `PROJECT_LOG.md` 作为快速入口。

## 2026-07-12 开题报告与飞书内容复核

- 根据当前项目大纲、GPU-backed 动机结果和实验主线入口，复核 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md`。
- 确认开题报告整体方向合适：研究内容、技术路线和可行性分析均以 `motivation/results/gpu/` 的真实 GPU-backed 结果作为主证据，并区分 PG18.4、fake/CPU 和 PostgreSQL 18.3 内部平台边界。
- 清理 `opening/feishu/opening_report_wiki.md` 开头的本地源稿说明，避免飞书发布面出现工作流元话语。
- 在飞书后续计划中补充 PostgreSQL 18.3 内部平台复测安排。
- 修正 `motivation/results/README.md` 中 GPU-backed 结果入口的过时措辞。

## 2026-07-12 实验结论写作标准

- 根据用户反馈，将 `learning/AGENTS.md` 的实验讲解标准作为后续实验结论、数据分析、开题可行性分析和飞书实验摘要的写作参照。
- 更新 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md` 和 `opening/work_rules.md`，要求实验分析说明实验目的、链路流程、参数含义、数据来源、结果读法、不能证明内容、结论类型和下一步验证。
- 明确正式报告可以更凝练，但结论边界和分析精细程度不能低于学习材料中的标准。

## 2026-07-12 开题飞书按学校模板重排

- 根据用户要求，严格参照 `opening/templates/硕士生开题报告模板0604.docx` 的大标题与章节结构调整开题飞书 wiki。
- 将 `opening/feishu/opening_report_wiki.md` 从汇报式结构改为模板式报告结构，保留封面占位、`1. 课题背景、目的和意义` 到 `7. 主要参考文献` 的章节顺序，并移除“当前主线”“一句话说明”“初步实验依据”“后续计划”“需要确认”等模板外标题。
- 使用 user 身份覆盖写入开题飞书 wiki：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`。飞书返回 `result=success`，文档 revision 更新为 `18`，6 个 Mermaid 图块解析为 whiteboard，无写入警告。
- 覆盖后重新拉取飞书目录，确认目录只包含模板章节和对应二级结构；关键词检查未发现旧的汇报型标题残留。

## 2026-07-12 动机测试飞书页补全

- 新增 `opening/feishu/motivation_feasibility_wiki.md`，作为“动机测试与可行性测试”飞书 wiki 的本地源稿。
- 源稿按 `learning/AGENTS.md` 的实验讲解标准组织：本页定位、当前结论摘要、真实 GPU-backed `AI_EMBED` 结果、多 endpoint Ray 动机测试、fake/CPU 预研使用边界、可行性验证边界、对开题方向的含义和下一步实验。
- 使用 user 身份覆盖写入飞书 wiki：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`。飞书返回 `result=success`，文档 revision 更新为 `10`，5 个 Mermaid 图块解析为 whiteboard，无写入警告。

## 2026-07-12 开题汇报 PPTX 生成

- 使用 `opening/templates/opening_ppt_template_version_v6_long_notes_source_checked.pptx` 作为学校 PPT 模板，基于 `opening/report/opening_report.md`、`opening/slides/opening_ppt.md` 和 GPU-backed 动机实验生成 16 页开题汇报 PPTX。
- 生成目录：`projects/opening_defense_20260712/`；最终交付文件：`opening/slides/opening_defense_20260712.pptx`。
- 质量检查：模板填充容量检查 `ok=294 warn=0 error=0`；PPTX 读回验证 `ok=181 warn=0 error=0`；`nature-paper2ppt` 审查结果为 `high=0, medium=0, low=26`，剩余 low 均为模板近似对齐提示。
- 已使用 user 身份将 PPTX 导入为飞书在线幻灯片：`https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg`，导入任务 `7661615330808482775` 返回 `job_status_label=success`。

## 2026-07-13 开题报告飞书图文同步

- 将 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 中的总体研究框架图和三张 GPU-backed 实验结果图，从 Mermaid 图替换为本地 PNG 图片引用。
- 使用 user 身份覆盖写入开题飞书 wiki：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`，正文同步后 revision 更新到 `32`；由于飞书 Markdown 不直接导入本地图片路径，返回 `partial_success` 和本地图片资源警告。
- 随后使用 `docs +media-insert` 将 4 张 PNG 上传并插入对应图注前：`system_architecture_ai_data_execution.png`、`gpu_embed_fine_vs_coalesced_e2e_20260712.png`、`gpu_embed_16k_stage_breakdown_20260712.png`、`gpu_embed_multi_endpoint_operator_wall_20260712.png`。
- 回读飞书文档确认目录完整，图 3-1、图 4-2、图 4-3、图 4-4 均已成为真实图片块，文档 revision 更新到 `41`；关键词检查未发现本地 `assets` 路径残留。
- 更新 `opening/feishu/README.md`，记录本地 PNG 不能仅依赖 Markdown 覆盖导入，后续同步需配合 `docs +media-insert`。

## 2026-07-13 链路阶段时延图修订

- 根据用户反馈，将 `gpu_embed_stage_overview_20260712.svg` / `.png` 从按阶段分组的横向柱状图，改为“场景为纵轴、阶段为柱内颜色”的横向堆叠柱状图。
- 新图以 4K / 16K、single endpoint / dual endpoint 四个场景为纵坐标，柱内堆叠 DB fetch、Arrow batch build、Ray submit / scheduling residual、GPU model request wall、fan-in 和 sink writeback，并在柱尾标注端到端总时延。
- 调整纵坐标标签左对齐和绘图区起点，避免图形过度居中；保留小阶段色块但不强制标注数值，突出 GPU 请求墙钟时间和 PostgreSQL JSON text writeback 两个主阶段。
- 同步更新 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md` 和 `opening/assets/charts/experiment_charts_audit.md` 中对图 4-5 的说明。
- 重新覆盖写入开题飞书 wiki 并逐张上传图片块；回读确认图 4-5 已替换为新版横向堆叠柱状图，正文、图注和图中编码一致，文档 revision 更新到 `71`，未发现本地 `assets` 路径残留。

## 2026-07-13 Python 实验图表生成

- 根据用户要求，将真实 GPU-backed 实验数据图重新用 Python 生成，脚本放在 `opening/assets/charts/scripts/generate_gpu_experiment_charts.py`，输出目录为 `opening/assets/charts/python/`。
- 新增五张候选正式图：调用粒度对比、single / dual endpoint 执行方式双面板对比、Ray actor 单 / 双 endpoint 扩展对比、链路阶段绝对时延、链路阶段占比。
- 图表只使用 `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv` 和 `motivation/results/gpu/ai_embed_multi_endpoint_20260712.csv` 的 formal repeats 平均，排除 warm-up，不混入 fake/CPU 或连接验证结果。
- 已目检 PNG 输出，确认中文、坐标、图例和标签可读；其中执行方式对比图将 single endpoint 和 dual endpoint 放在同一图中，避免单独强调 Ray 更快而忽略适用条件。
- 更新 `opening/assets/README.md`、`opening/assets/charts/scripts/README.md`、`opening/assets/charts/experiment_charts_audit.md` 和 `learning/README.md`。本批 Python 图暂未替换报告、飞书或 PPT 正式引用，后续替换时需同步图注、正文解释和讲稿备注。

## 2026-07-13 全量有意义实验对比图

- 根据用户要求，新增 `opening/assets/charts/scripts/generate_all_meaningful_experiment_charts.py`，把项目中对研究有解释价值的实验数据都生成候选图。
- 输出目录为 `opening/assets/charts/all_meaningful/`，共生成 14 张 PNG / SVG 图，覆盖 CPU/GPU endpoint、PG18.4 fake-model、fake/CPU 历史预研和 feasibility 组件 benchmark。
- 处理规则：带 `phase` 的实验只取 formal；无 `phase` 的历史实验排除 `repeat=0`；summary、smoke、dry-run 和连接验证数据不画正式性能图。
- 抽查 CPU/GPU、writeback、backpressure 和 Ray small task 图，确认中文、坐标、图例和关键数值可读；Ray small task 图已改为 y 轴从 0 开始，避免夸大差异。
- 更新 `opening/assets/README.md`、`opening/assets/charts/scripts/README.md` 和 `opening/assets/charts/experiment_charts_audit.md`，明确这些图是候选图集，报告正文仍应优先使用 GPU-backed 主证据。
- 根据用户反馈，修正 `all_cpu_vs_gpu_endpoint_e2e_20260712` 右上角图例与最高柱标签的拥挤问题：将图例移到绘图区上方，并增加 y 轴顶部留白；重新生成全量图集并目检该图。
## 2026-07-13 开题动机图表筛选

- 根据用户要求，从已有系统架构图、真实 GPU-backed 实验图和全量候选实验图中筛选开题主线图组。
- 新增 `opening/assets/charts/selected_motivation_figures.md`，记录主线图、备用图、不建议进入主线的图和推荐讲解顺序。
- 当前主线建议为：系统架构图、真实 GPU-backed 链路阶段耗时、调用粒度对比、执行方式与模型端点数量对比、actor endpoint scaling / 写回约束。
- 明确 fake/CPU、PG18.4 fake 和 feasibility 组件 benchmark 只作为附录、答辩备用或研究设计来源说明，不能替代真实 GPU-backed 主证据。
- 根据用户补充要求，在 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md` 和 `opening/assets/charts/selected_motivation_figures.md` 中补充“三类 workload 为什么选”和“为什么调 batch / partition / task / actor / routing / backpressure / writeback”的依据说明，明确依据来自外部系统资料和项目实验信号，而不是主观选择。
- 进一步将图表使用策略调整为 A/B/C 三层：A 层为报告和 PPT 正文主线图，B 层为支撑场景选择和变量选择的数据图，C 层为表格或文字即可的数据；明确 workload matrix、granularity attribution、backpressure、writeback batching 和 Ray / Arrow fan-in 属于值得关注的支撑性图。
- 为方便查找，新增集中目录 `opening/assets/charts/selected/`，其中 `report_main/` 存放报告和 PPT 正文建议图，`ppt_backup/` 存放 PPT 备份和飞书补充图；同步将开题报告本地正文和飞书源稿的图片引用切换到 `selected/report_main/` 下的短文件名版本。
- 根据用户要求，记录后续图表资产清理规则：最终只保留 `selected/`、生成脚本、审计记录、图表选择说明和系统架构图；`python/`、`all_meaningful/` 和旧 ECharts 根目录图在报告、PPT、飞书均完成路径切换后可以删除。
# 2026-07-14 PG18.4 pgai-integrated GPU rerun figures and report update

- Generated report-main figures from `motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv` with `figures/scripts/generate_pgai_integrated_gpu_rerun_charts.py`.
- Added `06_gpu_pgai_rerun_granularity_20260714`, `07_gpu_pgai_rerun_stage_writeback_20260714`, and `08_gpu_pgai_rerun_endpoint_comparison_20260714` under `figures/data/report_main/`.
- Updated `opening/report/opening_report.md` to cite the PG18.4 local rehearsal + CUDA endpoint rerun, replacing older 2026-07-12 GPU figures in the current report body.
- Revised the endpoint-comparison figure wording so the dual-endpoint result is presented as absolute E2E time (`3.62s -> 2.86s`) and stage movement, not as a visually emphasized percentage claim.
- Synchronized figure index/audit files. Boundary remains: local PostgreSQL 18.4 rehearsal, JSON text writeback, two local endpoint replicas on one RTX 5070, not PostgreSQL 18.3 or multi-GPU.
# 2026-07-14 pgvector(384) writeback comparison

- Completed the same-chain GPU-backed Ray actor writeback comparison for no writeback, JSON text, and pgvector `vector(384)`.
- Output CSV and report:
  `motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv` and
  `motivation/results/gpu/pgvector_writeback_20260714.md`.
- Generated figure:
  `figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png`.
- Updated `opening/report/opening_report.md` with the new figure, table, and boundary note. The result remains PG18.4 local rehearsal, not PostgreSQL 18.3 internal-platform performance.
- Feishu/wiki and PPT were not synchronized in this pass.

# 2026-07-15 research plan figure for opening report

- Refined `figures/architecture/cross_layer_method_framework.png` / `.svg` into a research-plan figure for Section 4.1.
- Inserted the figure into `opening/report/opening_report.md` and local Feishu source `opening/feishu/opening_report_wiki.md` as Figure 4-1; renumbered subsequent Section 4 figures.
- Updated the figure asset index and added an audit record. Online Feishu/wiki and PPT were not synchronized in this pass.

# 2026-07-15 research-plan figure drawing rules

- Synchronized the research-plan figure drawing cautions into `figures/AGENTS.md` and `opening/ppt_rules.md`.
- The rules now require concrete workload cards, explicit upstream-tuning labels, full border/overflow checks, and no visible `RC/BL`, unexplained `vs`, or vague `边界确认` labels in formal figures.

# 2026-07-15 opening report mainline adjusted to upstream tuning plus end-to-end evaluation

- Adjusted the opening-report mainline from “independent best vs joint optimal” to “upstream execution-path tuning plus end-to-end evaluation”.
- Updated the local report and Feishu source so the main route is now staged profiling, upstream execution-path tuning, writeback-inclusive full-chain validation, and multi-workload validation.
- Kept independent-best vs end-to-end configuration as an optional enhanced contrast when later experiments show clear cross-stage coupling.

# 2026-07-15 opening report architecture figures aligned with three-layer strategy

- Regenerated the opening-report architecture figures:
  `figures/architecture/system_architecture_ai_data_execution.*`,
  `figures/architecture/cross_layer_method_framework.*`, and
  `figures/architecture/runtime_strategy_control_loop.*`.
- Updated `opening/report/opening_report.md` and `opening/feishu/opening_report_wiki.md`: Figure 4-1 now states the three-layer upstream execution strategy, and Figure 4-2 now uses the runtime control-loop figure instead of the previous Mermaid chain sketch.
- The updated figures clarify that database-side batch/partition are primarily plan-time choices, while runtime optimization focuses on `K_max`, `routing policy`, backpressure, and service-side `micro-batch`; writeback remains an end-to-end guardrail and bottleneck test.

# 2026-07-15 opening report figure color semantics corrected

- Revised Figure 4-1 so the three-layer upstream strategy is drawn as three separate neutral cards: plan-time data organization, runtime admission/routing, and service-side batching.
- Revised Figure 3-1 bottom research-content cards to use neutral borders, avoiding a misleading one-to-one color mapping with the system pipeline stages above.
- Adjusted research content 2 wording to `运行层调度与服务端批处理`, matching the current plan that combines Ray-side admission/routing with model-service-side `micro-batch`.

# 2026-07-15 opening report figure wording conservatism pass

- Revised the research-gap figure so the bottom positioning matches the current plan: data organization, runtime scheduling plus service-side batching, and writeback bottleneck determination.
- Revised the strategy rule table title to `信号触发的候选策略规则表`, making clear that table entries are candidate triggers for later experiments rather than proven rules.

# 2026-07-15 opening report text aligned with revised strategy

- Updated `opening/report/opening_report.md` and `opening/feishu/opening_report_wiki.md` so the prose matches the current figures.
- The report now describes the method as plan-time data organization, runtime admission/routing, and service-side batching, with writeback used for bottleneck determination and end-to-end benefit checks.
- Removed stale mainline wording around GPU-only scheduling, writeback as an independent contribution, and mandatory independent-best vs joint-optimal comparison.
