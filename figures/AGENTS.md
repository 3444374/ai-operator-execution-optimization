# figures/AGENTS.md

本目录是项目级图资产库，服务 learning、开题报告、开题 PPT、中期汇报和毕业论文。进入本目录前先读根目录 `AGENTS.md` 和 `PROJECT_INDEX.md`，再读本文件和 `figures/README.md`。

## 1. 目录职责

- `architecture/`：系统架构图、流程图、方法框架图。用于说明研究对象、系统边界、组件关系和技术路线。
- `data/report_main/`：报告、PPT、论文正文优先使用的数据图。只保留能支撑主线论证的图。
- `data/backup/`：答辩备份、飞书补充、learning 讲解用支撑图。用于解释场景选择、变量选择和实验设计来源。
- `learning/`：学习材料专用图，重点服务术语解释和实验过程讲解。
- `audit/`：图表质检记录、图表选择说明、设计审计记录。
- `scripts/`：可复现绘图脚本。正式数据图必须能从项目内 CSV / 报告 / 脚本追溯。

正式材料优先引用本目录，不再从 `opening/assets/charts/`、`opening/assets/figures/` 或 `learning/figures/` 复制分散图。

## 2. 做图前先判断图的角色

每次新增或修改图，先判断它属于哪一类：

1. Motivated Example / 动机图  
   用来回答“为什么要做这个课题”。本项目中通常是三类 AI workload、调用粒度、模型服务队列、writeback 等动机信号。

2. Solution Overview / 系统架构图  
   用来回答“研究对象是什么、系统边界在哪里、方法作用在哪些环节”。本项目中的核心图是数据库驱动 AI workload 进入 Daft/Arrow、Ray、GPU model service、sink 的执行与存储协同架构。

3. Experimental Results / 实验结果图  
   用来回答“数据证明了什么”。必须来自真实 CSV、结果报告或明确统计过程，不能凭印象画。

如果一张图不能说清楚它属于哪一类、核心结论是什么、放在哪个材料里，就不要先画。

## 3. Skill 使用规则

- 设计或审查论文级核心图时，使用 `figure-designer`。适用场景包括系统架构图、Figure 1 动机图、实验结果图选择、用户反馈“图不好看/不清晰/不专业”。
- 准备毕业论文、投稿级或需要高质量图注/颜色/版式审查时，使用 `nature-figure` 或 `figure-designer` 做二次审计。
- 将报告内容转成 PPT 时，使用 `nature-paper2ppt` 或 `ppt-master`，但图资产仍从 `figures/` 引用，不在 PPT 目录重新维护副本。
- 画实验数据图时，优先使用 Python + Matplotlib / Seaborn；不要再优先使用一次性 ECharts 脚本，除非用户明确要求 ECharts 风格或已有 ECharts 资产必须延续。
- 写图注、图说明和报告正文时，遵循 `humanizer` 的自然写作要求，但不能牺牲实验边界和证据分层。
- 做任何数据结论图前，遵循 `karpathy-guidelines`：先明确可验证目标、数据来源、不能声称的结论，再写脚本或改图。

## 4. 系统架构图经验

今天反复修改系统架构图的主要原因是：元素对齐、箭头遮挡、编号越界、框内文字不规整、观测层和执行层割裂。这些问题以后必须提前规避。

架构图必须遵守：

- 先定义画布网格，再放组件。主链路组件、观测层、指标条和研究内容卡片要共享同一套左右边界和中心线。
- 主数据流尽量水平连接相邻组件，不要让长箭头穿过下方研究内容框。
- 从观测层指向执行层的箭头要落在对应模块中心线，且箭头终点与模块顶部留出可读间距。
- 编号徽标必须在框内，不能越出边框，不能遮挡标题，尤其注意 `GPU model service` 这类长标题。
- 每个大框内的子内容用等宽小标签或整齐列表，不要用大量斜杠堆在一行。
- 标题居中或左对齐必须全图一致；同一层级的标题字号、标签宽度、内边距要一致。
- 黄色观测层这类跨模块框必须与被观测模块对齐，不能视觉上悬空或割裂。
- 图中只放关键词和必要指标；解释性长句放到正文、图注或 PPT 讲稿。
- 不使用装饰性渐变、阴影、3D、无意义背景纹理。颜色要服务分组，不服务装饰。
- 架构图中的术语要正式：优先使用“执行过程”“执行路径”“阶段画像”“模型服务感知批处理执行”“写回协同”，少用口语化的“外部链路”。

系统架构图修改后必须更新：

- `figures/architecture/` 下的 PNG / SVG；
- `figures/audit/system_architecture_ai_data_execution_audit.md`；
- 引用该图的报告、PPT 源稿、飞书源稿和 learning 文档。

## 5. 实验数据图规则

实验图优先用 Python 生成，原因是可复现、便于批量统一样式、便于从 CSV 直接追溯。

适合用 Python 画图的情况：

- 来自 CSV / parquet / markdown 表格的实验数据；
- 需要多张图共享统一配色、字体、尺寸、图例和边界说明；
- 后续还会用于报告、PPT、中期和论文；
- 需要同时输出 PNG 和 SVG；
- 需要保留脚本以便新实验结果到来后重画。

不适合画成图的情况：

- 只有连接验证、dry-run、smoke test；
- 数据点太少，表格比图更清楚；
- 数据来源不稳定或边界不清；
- fake/CPU 预研结果容易被误读成真实 GPU-backed 性能结论。

图表类型选择：

- 同一指标、多场景对比：分组柱状图。
- 同一场景的阶段耗时构成：堆叠柱状图，场景较多时优先横向堆叠柱。
- 阶段占比：100% 堆叠柱，但必须同时保留绝对耗时图，避免只看占比误判。
- 参数随规模变化：折线图或带点折线图。
- 两个因素共同影响结果：小 multiples / 分面图，不要把所有信息塞进一张拥挤图。
- 只有少量关键数值：表格或正文数字即可。

实验图必须标注：

- 数据来源文件；
- 是否排除 warm-up；
- 使用 formal repeat 还是历史结果；
- PostgreSQL 18.4 本地预演、PostgreSQL 18.3 内部平台、GPU-backed、PG18.4 fake、fake/CPU、feasibility benchmark 等证据层级；
- 不能声称的结论，例如不能把 CPU/fake 图写成真实 GPU-backed 瓶颈归因。

## 6. 图文一致性规则

图、正文、PPT 和飞书必须讲同一个故事：

- 正文说“数据库到 GPU 再到写回的阶段时延”，图中就必须按 DB fetch、Arrow build、Ray residual、GPU request wall、fan-in、sink writeback 等阶段组织。
- 正文说“为什么选三类 workload”，图或备份图要能支撑三类 workload 的差异与共性，不能只凭感觉解释。
- 正文说“为什么调 batch / partition / task / actor / routing / backpressure / writeback”，图或结果表要给出对应实验信号、文献/官方资料依据或明确的后续验证计划。
- 一张图的图题、坐标轴、图例、正文解释和 PPT 讲稿必须使用同一套术语。
- 如果图更新了，必须同步检查 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md`、`opening/slides/`、`learning/` 和后续 PPT 源稿。

## 7. 质量审计清单

每张正式图进入 `report_main/` 前必须检查：

- 是否有一句核心结论；
- 是否能追溯到 CSV、结果报告或明确设计稿；
- 是否同时有 PNG 和 SVG，除非该图只适合单一格式；
- 字体在 PPT 缩放后仍可读；
- 坐标轴从 0 开始，除非有明确理由；
- 图例不遮挡数据，不挤在标题或右上角；
- 颜色可区分，不能只靠颜色表达含义；
- 标签、编号、箭头不重叠、不越界；
- 图注说明证据层级和边界；
- 没有装饰性图表元素。

审计结果写到 `figures/audit/`，不要只在对话里说“看起来可以”。

## 8. 保留与删除规则

- 长期保留：正式图、备份图、学习图、绘图脚本、审计记录。
- 可以删除：中间生成目录、低清截图、旧路径重复副本、未被任何材料引用且可由脚本重生成的候选图。
- 删除旧图前，先用 `rg` 检查报告、飞书源稿、PPT 源稿、learning、中期和论文草稿是否仍引用旧路径。
- 在 Windows 上递归删除前，必须解析绝对路径并确认目标位于当前工作区内。

## 9. 更新检查

修改本目录后，同步检查：

- `figures/README.md`
- `figures/data/selected_motivation_figures.md`
- `figures/audit/*`
- `opening/report/opening_report.md`
- `opening/feishu/opening_report_wiki.md`
- `learning/README.md`
- `learning/experiment_walkthrough.md`
- `PROJECT_INDEX.md`
- 根目录 `README.md`

不是每次都必须修改这些文件；但如果图的路径、用途、证据层级或主线结论变了，必须同步更新。
