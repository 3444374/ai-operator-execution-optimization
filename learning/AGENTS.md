# learning/AGENTS.md

本目录存放面向用户的学习讲解材料。进入或修改本目录前，先读根目录 `AGENTS.md` 和本文件。

## 目标

- 用初学者能理解的方式解释项目实验、代码、术语和研究流程。
- 帮助用户真正理解实验目的、参数、链路、结果和局限。
- 本目录的核心目的首先是带用户学习本课题技术路线，而不是复刻项目中其他目录的报告、索引或论文写法。
- 根据用户的理解程度重新梳理内容；用户提问时，应先定位卡点，再用更基础或更结构化的方式讲清对应部分。

## 内容边界

- 放学习讲解、术语解释、实验时间线、代码导读。
- 不放原始 CSV、正式实验结论主报告或论文正文。
- 正式结果仍放 `feasibility/results/` 或 `motivation/results/`。
- 允许采用和项目其他目录不同的组织方式：可以按学习路径、概念递进、实验时间线、问答式解释来写。
- 事实、数据和结论边界必须和项目当前实验、报告、代码一致；不能为了教学简化而改变项目事实。

## 写作规则

- 默认按“为什么做、术语解释、链路流程、参数含义、结果读法、不能证明什么、下一步”组织。
- 不假设用户熟悉 Ray、Daft、Lance、Arrow、pgvector、batch、task/actor、fan-in、backpressure、writeback。
- 讲术语时必须先放回当前场景：数据从哪里来、当前在哪个系统/进程/内存里、经过什么 API 或执行器、下一步到哪里去。
- 对用户不熟悉的词，不只给定义，还要说明它在本项目链路里的具体形态，例如“一行”是哪张表的一行、`ray.get` 是谁从哪里取什么、writeback 是把什么结果写回哪张表。
- 当用户问“代码里是哪部分”或“当前进程是什么”时，必须映射到具体脚本、函数和调用阶段，例如 `main()`、`run_once()`、`submit_ray_tasks()`、`submit_with_backpressure()`、`write_embeddings()`。
- 讲 Ray 相关流程时必须区分四件事：提交任务得到 `ObjectRef`、worker 执行计算、`ray.get` 取回结果、数据库 writeback 写回结果；不能把 `ray.get` 直接说成写数据库。
- 讲 embedding 时必须说明计算是在数据库外部的 Python/Ray worker 里完成，除非明确讨论数据库内置模型或 UDF 实现。
- 讲 GPU 时必须区分两件事：一是 GPU-backed model service 作为数据执行链路里的现实计算端点；二是“把数据库 AI 算子迁移到 GPU / 做 GPU kernel 优化”作为少量 baseline 或非主线。不能把“暂不优先做 GPU 迁移 baseline”讲成“本课题不在 GPU 背景下”。
- 讲课题动机时，必须把生产式 GPU-backed E2E profile 作为第一层主动机：它回答“为什么要优化数据库驱动 AI workload 的数据执行与存储链路”。变量解释和消融也应优先来自同一条 GPU-backed 链路上的大块消融；CPU/fake、granularity、backpressure 历史实验只能作为预研背景和工具解释，不能作为真实 GPU 链路归因。
- 对 `fan-in`、backpressure、in-flight、queue wait、token backlog、upsert 等流程词，要画出最小链路并说明上游、下游、队列、结果合并和写回对象。
- 用户明确表示想使用 Ray 学 AI infra / inference infra 时，讲解应承认这个学习目标，同时区分“工程学习路线可以重点用 Ray”和“论文论证仍需要 Ray / 非 Ray baseline 与消融”。
- 当用户问“业界是不是也这么做”时，必须区分本项目本地实现、外部 worker / vectorizer 工程实践、数据库内扩展实践、应用层 ETL/服务实践，并给出权威来源或标注为合理推断。
- 比喻只能辅助理解，结论必须回到真实实验事实。
- 必要的实验结果如果用图更容易理解，应在学习材料中配图或规划配图，尤其是 CPU/GPU 对比、fine/coalesced 对比、阶段耗时占比、瓶颈迁移、in-flight / queue wait / writeback 等结果。画图前先写清楚这张图要证明的一句话、证据链和不能说明的结论；图中数值必须来自真实 CSV / 报告，不能手填或编造。学习用交互图可以使用 ECharts；按 Apache ECharts 文档，项目化引入时可 `import * as echarts from 'echarts'`，若按需引入则必须注册所需 chart/component，并显式引入 `CanvasRenderer` 或 `SVGRenderer`。论文或正式报告图优先用可复现脚本导出 SVG/PDF，遵循 `figure-designer` / `nature-figure` 的原则：核心结论优先、轴范围诚实、字体可读、色盲友好、无 chartjunk、caption 自包含。
- 新实验或代码测试完成后，同步更新相关学习材料。
- 每次讲解实验结果时，必须区分“本地实验事实”“模拟实验事实”“合理推断”“待确认问题”和“不能声称的结论”。
- 项目事实变化时，及时清理过时讲解，避免和正式结果报告冲突。
- 写作和答疑遵循 `karpathy-guidelines`：不确定就标注不确定，不把假设写成事实，只做和当前学习目标相关的解释或修改。
- 涉及文献、外部系统、研究定位或引用依据时，遵循 `nature-academic-search` 和 `academic-research-suite` 的证据纪律：优先使用已读材料和权威来源，不能编造引用或把未验证资料写成事实。
- 涉及 AI 辅助研究工作流时，遵循 `vibe-research-workflow`：AI 可辅助组织、解释和润色，但研究问题、实验判断、核心结论必须由用户理解并确认。

详细阅读入口见 `README.md`。
