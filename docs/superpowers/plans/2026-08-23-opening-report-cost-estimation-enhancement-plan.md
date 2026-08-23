# Opening Report Cost Estimation Enhancement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修改开题报告，使两项研究内容各自具有清楚、可引用且不依赖代价估计的研究动机和基础方法，同时准确说明 AI 算子代价估计对数据库优化以及两项研究内容的可选增强作用。

**Architecture:** 第二章用论文和官方文档说明现有系统分别怎样处理数据批次、模型服务内部调度和集群资源管理；第三章分别写清两项研究内容的常见做法、现有不足、本课题研究对象和基础方法；第四章把研究计划与当前离线实验分开，并通过对照实验评价代价信息是否带来额外收益。已有实验数据、图片和两项研究内容的总体结构保持不变。

**Tech Stack:** Markdown、PowerShell 文本检查、项目现有 46 篇参考文献、Ray Data 与 vLLM 官方文档。

## Global Constraints

- 报告保持学校模板的七部分结构和现有 11 张图片，图片路径和实验数据不变。
- 参考文献不少于 45 篇；新增两条官方资料后总数为 48 篇，正文必须实际引用新增条目。
- 研究内容一和研究内容二在不使用代价估计时仍能独立回答研究对象、研究理由、基础方法和评价方式。
- 第 2.2、2.3 节承担已有系统和文献机制的具体介绍；第 2.4 节只归纳尚未解决的问题，第 3.2 节只简要承接，不重复展开同一批文献。
- 代价估计主要支持数据库执行计划比较和多 SQL 编排，只在前两项研究内容中作为可选信息输入。
- 第 4.1 节使用研究计划口径，第 4.2 节只陈述已经完成的实验；429 条记录仍是离线估计实验，不能写成数据库优化或调度收益。
- 正式正文不使用项目管理用语，不新增未经验证的实验结论，不改桌面 PPT、Word 文档和报告图片。
- 保留工作区内与本任务无关的现有修改，提交时只暂存本计划和本轮报告文件。

## Terminology Ledger

| 统一术语 | 首次说明 |
|---|---|
| 工作描述（WorkDescriptor） | 保存记录身份、作业身份、分阶段工作特征和数据局部性等运行前已知信息 |
| 基础数据组织方法 | 直接使用词元数、输出上限、图像或帧规模、共享前缀等已知特征形成批次 |
| 基础调度方法 | 使用预先测得的可承载请求数和工作量上限、完成通知及实际运行状态安排提交与路由 |
| 代价信息增强 | 在基础方法不变时，额外使用预计执行时间、预计资源需求或预计剩余工作量 |
| 模型服务容量 | 当前机器、模型和服务配置下，每个服务实例可同时接收而尚未完成的请求数及其预计工作量 |

## Claim-Evidence Map

| 正文判断 | 依据 | 使用方式 |
|---|---|---|
| 数据框架通常以分区、数据块和批次组织执行 | Ray Data Streaming Batch [18]、Daft 官方文档 [19]、Ray Data `map_batches` 官方文档 [47] | 说明现有接口解决数据切分、并行和内存管理，不声称其按 AI 计算量自动组批 |
| 模型服务在请求到达后进行迭代级或连续批处理 | Orca [11]、vLLM [12] | 说明服务端调度与数据库上游组织的分工 |
| vLLM 提供每轮最大序列数、最大批处理词元数和先来先服务或优先级策略 | vLLM 调度配置官方文档 [48] | 作为固定运行参数和服务端策略的工业系统实例，不外推为所有系统的统一做法 |
| 多作业或多客户端调度已有累计服务量、公平性和集群调度方法 | VTC [16]、Llumnix [17]、Themis/Tiresias/Pollux [35-37]、FairServe/DLPM/Autellix [43-45] | 说明这些工作位于模型服务、应用运行时或 GPU 集群层，并准确描述其研究对象 |
| 平均预测误差不能替代数据库执行决策评价 | Learned cost model 研究 [26-28]、CONCERTO [46] | 支撑计划排序、实际查询时间和资源结果的评价设计 |

---

### Task 1: 补足常见做法及其文献与工业依据

**Files:**
- Modify: `opening/report/opening_report.md:116-188`

**Interfaces:**
- Consumes: 当前第 2.4 节的问题归纳和第 3.2 节两项研究内容。
- Produces: 带有 [18]、[19]、[47]、[48] 等准确引用的常见做法说明，以及可独立成立的研究内容一和研究内容二。

- [x] **Step 1: 记录修改前的结构和引用状态**

Run:

```powershell
Select-String -Path opening/report/opening_report.md -Pattern '^## |^### |^#### '
Select-String -Path opening/report/opening_report.md -Pattern '\[47\]|\[48\]'
```

Expected: 七个一级部分仍存在；正文尚未引用 [47] 和 [48]。

- [x] **Step 2: 修改第 2.4 节**

先在第 2.2 节现有 Ray Data、Daft 段落中补充官方接口依据，在第 2.3 节现有 vLLM 段落中补充服务端参数依据。第 2.4 节不重新介绍系统，只用一至两句话归纳：数据框架按分区、数据块或目标行数批次完成数据切分与并行执行[18-19,47]，模型服务负责已经到达请求的批处理[11-12]，这些机制没有直接回答数据库记录在到达模型服务前怎样按词元、图像准备或其他 AI 处理需求组成请求。

调度段只承接第 2.3 节的分类结论：服务内部调度、多客户端公平和 GPU 集群作业调度分别处理已经到达的请求、客户端服务量或集群资源[16,35-37,43-45,48]，但没有直接把数据库尚未处理的记录、完整作业进度和模型服务实际状态共同用于上游提交。

- [x] **Step 3: 修改第 3.2.1 节**

用一句话承接第 2.2 节的分区和批处理做法，不重新解释 Ray Data、Daft、Orca 或 vLLM。随后按“现有接口尚未表达的 AI 工作量、本课题研究、基础方法、可选增强、评价方式”组织段落。基础方法直接使用运行前已知的词元数、输出上限、图像或帧规模、共享前缀和批次工作量上限，不要求代价估计器先给出时间预测。预计批次执行时间或资源需求只在独立验证后作为额外输入，并与不使用预测的基础方法在相同调度条件下比较。

- [x] **Step 4: 修改第 3.2.2 节**

用一句话承接第 2.3 节的固定服务参数、服务端调度和集群调度，不再次列举各篇论文。随后按“本课题关注的上游问题、基础方法、可选增强、评价方式”组织段落。基础方法只使用预先测得的请求数和工作量上限、请求完成通知、排队和运行状态；预计服务时间和预计剩余工作量只作为后续实验中的额外信息。

- [x] **Step 5: 检查两项研究内容是否能脱离代价估计独立阅读**

Run:

```powershell
$text = Get-Content -Raw -Encoding utf8 opening/report/opening_report.md
$text -match '基础数据组织'
$text -match '基础调度'
```

Expected: 两个表达均为 `True`，且 3.2.1 和 3.2.2 各自先写基础方法，后写可选增强。

### Task 2: 校准代价估计用途和实验表述

**Files:**
- Modify: `opening/report/opening_report.md:184-226`
- Modify: `opening/report/opening_report.md:314-368`

**Interfaces:**
- Consumes: Task 1 中定义的基础数据组织方法、基础调度方法和代价信息增强。
- Produces: 数据库优化用途、可选增强用途、研究计划和现有离线结果之间一致的表述。

- [x] **Step 1: 修改第 3.2.3 节**

保留数据库内核优化器用途：估计关系算子与 AI 语义算子的可比较成本，支持结果含义相同的 SQL 执行计划比较，并为多条 SQL 的执行顺序和并发方式提供信息。增加一段简短说明：预计批次执行时间可增强数据组织，预计服务时间和剩余工作量可增强多作业调度，但两项基础方法不依赖这些预测。

- [x] **Step 2: 修改第 4.1 节**

把“工作量估计与数据组织模块”改为“工作描述与数据组织模块”。在代价估计实验设计后增加两组增量对照：同一数据组织方法加入或不加入预计批次执行时间；同一调度方法加入或不加入预计服务时间和剩余工作量。明确预测误差较低不代表系统一定更快，最终报告实际吞吐、作业完成时间、尾延迟和 SQL 计划结果。

- [x] **Step 3: 收紧第 4.2.5 节**

保留双 RTX 4090、两个 Qwen2.5-7B 服务实例、20 个执行情境、四个在途词元工作量上限、六种估计方法和 429 条运行记录。补充说明当前实验没有把预测结果接入数据组织、多作业调度、关系算子计划比较或完整数据库优化器，因而只支持当前文本环境中的离线代价区分能力。

- [x] **Step 4: 修改进度安排与预期成果**

2026 年 9 月和 10 月先完成不依赖代价预测的两项基础方法评价；2026 年 11 月在数据库 SQL 计划实验之外，分别加入代价信息增强对照。预期成果先写数据组织方法和调度组件，再写轻量 AI 算子代价估计组件对数据库优化及前两项方法的可选支持。

- [x] **Step 5: 检查研究计划和当前结果没有混写**

Run:

```powershell
Select-String -Path opening/report/opening_report.md -Pattern '已经|现有|计划|拟|后续'
```

Expected: 第 4.1 节以“计划、将、拟”描述待研究内容；第 4.2.5 节以“现有实验、当前结果”描述已经完成的离线实验。

### Task 3: 增加官方资料、完成 QA 和变更记录

**Files:**
- Modify: `opening/report/opening_report.md:372-464`
- Modify: `opening/report/opening_report_20260820_qa.md`
- Modify: `opening/README.md`
- Modify: `opening/logs/project_log.md`
- Modify: `PROJECT_LOG.md`
- Modify: `PROJECT_INDEX.md`

**Interfaces:**
- Consumes: Task 1 和 Task 2 的最终正文。
- Produces: 48 条参考文献、引用覆盖检查、正式语言检查和可追溯的项目记录。

- [x] **Step 1: 增加两条官方参考资料**

在参考文献末尾增加：

```text
[47] Ray. ray.data.Dataset.map_batches Documentation[EB/OL]. [2026-08-23]. https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html

[48] vLLM. Scheduler Configuration Documentation[EB/OL]. [2026-08-23]. https://docs.vllm.ai/en/stable/api/vllm/config/scheduler/
```

- [x] **Step 2: 更新 QA、开题状态和项目日志**

记录本轮新增的依据、两项基础方法与代价信息增强的关系、4.2.5 的离线实验限制，以及“未修改图片、PPT 和 Word”的事实。`PROJECT_INDEX.md` 只增加本实施计划的索引行，不覆盖当前工作区的其他改动。

- [x] **Step 3: 运行结构、引文和语言检查**

Run:

```powershell
$p = 'opening/report/opening_report.md'
$text = Get-Content -Raw -Encoding utf8 $p
($text -split "`n" | Where-Object { $_ -match '^## [1-7]\.' }).Count
([regex]::Matches($text, '^!\[图 ', 'Multiline')).Count
([regex]::Matches($text, '^\[[0-9]+\] ', 'Multiline')).Count
Select-String -Path $p -Pattern '冻结|门禁|闭环|边界|约束|合同|产品轨|框架轨|正式点|晋级|失效|—|–'
```

Expected: 一级部分 `7`，正文图片 `11`，参考文献 `48`，高风险正式材料用语和长破折号无匹配。

- [x] **Step 4: 检查新增参考文献均被引用且图片存在**

Run:

```powershell
Select-String -Path opening/report/opening_report.md -Pattern '\[47\]|\[48\]'
$missing = Select-String -Path opening/report/opening_report.md -Pattern '^!\[[^\]]+\]\(([^)]+)\)' | ForEach-Object { Join-Path 'opening/report' $_.Matches[0].Groups[1].Value } | Where-Object { -not (Test-Path $_) }
$missing
```

Expected: [47] 和 [48] 均至少出现在一处正文中；`$missing` 无输出。

- [x] **Step 5: 运行差异与隐私检查**

Run:

```powershell
git diff --check
D:\Anaconda\python.exe code/scripts/environment/scan_git_secrets.py
git diff -- opening/report/opening_report.md opening/report/opening_report_20260820_qa.md opening/README.md opening/logs/project_log.md PROJECT_LOG.md PROJECT_INDEX.md docs/superpowers/plans/2026-08-23-opening-report-cost-estimation-enhancement-plan.md
```

Expected: 无空白错误、无隐私信息，差异只包含本轮报告定位调整及其记录。

- [x] **Step 6: 提交本轮修改**

```powershell
git add -- opening/report/opening_report.md opening/report/opening_report_20260820_qa.md opening/README.md opening/logs/project_log.md docs/superpowers/plans/2026-08-23-opening-report-cost-estimation-enhancement-plan.md
git add -p -- PROJECT_INDEX.md PROJECT_LOG.md
git commit -m "docs(opening): clarify independent research methods"
```

Expected: 提交只包含本轮开题报告、QA、状态记录、实施计划和对应索引日志，不包含其他文献或图片改动。
