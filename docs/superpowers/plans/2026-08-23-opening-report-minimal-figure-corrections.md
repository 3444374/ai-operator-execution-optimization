# Opening Report Minimal Figure Corrections Implementation Plan

> 状态：已于 2026-08-24 完成。本文仅保留实施过程；当前图资产入口以
> `../../../figures/README.md` 为准。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the conceptual relationships and public-facing labels in report Figures 2, 3, 5, and 6 without redesigning the other figures or changing any experimental values.

**Architecture:** Figures 2, 3, and 5 remain Draw.io-authored system diagrams. Their authoritative Draw.io and SVG sources are edited together, then SVG is rendered to PNG and synchronized to the opening figure set and report folder. Figure 6 remains a reproducible Matplotlib result figure generated from existing project data; only literal labels change in the existing script.

**Tech Stack:** Draw.io XML, SVG, Node.js with `sharp` for SVG-to-PNG rendering, Python with Matplotlib for Figure 6, PowerShell validation, Markdown documentation.

## Global Constraints

- Modify only Figures 2, 3, 5, and 6 from the report figure set.
- Keep the existing canvas sizes, four-colour visual system, main card positions, and reading directions.
- Do not change Figure 6 data, bar lengths, numeric labels, axis limits, means, or standard deviations.
- Do not modify `C:\Users\Administrator\Desktop\0819.pptx` and do not generate a Word document.
- Do not edit PNG files directly. Update Draw.io/SVG or the plotting script, then regenerate PNG copies.
- Preserve unrelated working-tree changes. Stage only files belonging to the current task.

---

### Task 1: Record the current four-figure inputs and output contracts

**Files:**
- Read: `figures/audit/opening_report_minimal_figure_corrections_design_20260823.md`
- Read: `opening/report/figures/README.md`
- Read: `figures/opening_figure_set/README.md`

**Interfaces:**
- Consumes: the approved design document and existing source-to-copy mappings.
- Produces: a baseline list of source files, output files, image dimensions, and Figure 6 data labels used by later verification.

- [ ] **Step 1: Confirm the four report image paths and dimensions**

Run:

```powershell
$paths = @(
  'opening/report/figures/fig02_ai_data_execution_gap.png',
  'opening/report/figures/fig03_work_unit_organization.png',
  'opening/report/figures/fig05_system_architecture.png',
  'opening/report/figures/fig06_text_baseline_boundaries.png'
)
Add-Type -AssemblyName System.Drawing
foreach ($path in $paths) {
  $image = [System.Drawing.Image]::FromFile((Resolve-Path $path))
  "$path`t$($image.Width)x$($image.Height)"
  $image.Dispose()
}
```

Expected: Figures 2, 3, and 5 are `1600x900`; Figure 6 is `3874x1522` or the current script's exact regenerated dimensions.

- [ ] **Step 2: Confirm authoritative sources and report copies exist**

Run `Test-Path` for:

```text
figures/architecture/editable/01_research_gap.drawio
figures/architecture/editable/01_research_gap.svg
figures/architecture/editable/03_work_unit.drawio
figures/architecture/editable/03_work_unit.svg
figures/architecture/editable/02_system_architecture.drawio
figures/architecture/editable/02_system_architecture.svg
figures/data/report_main/opening_text_baseline_evidence_map.svg
figures/scripts/generate_opening_story_figures_20260808.py
```

Expected: all paths return `True`.

- [ ] **Step 3: Record Figure 6 visible numeric labels before modification**

Use the existing report image and plotting input to retain these visible labels:

```text
Left panel: 136.6 ± 0.6, 136.7 ± 0.1, 137.8 ± 2.4
Right panel: 17.8 ± 0.0, 17.3 ± 0.1, 16.7 ± 0.1, 3.6 ± 0.0
```

Expected: later regenerated Figure 6 contains the same seven values.

---

### Task 2: Separate cost information and runtime observation in Figure 2

**Files:**
- Modify: `figures/architecture/editable/01_research_gap.drawio`
- Modify: `figures/architecture/editable/01_research_gap.svg`
- Modify: `figures/architecture/editable/01_research_gap.png`
- Modify: `figures/opening_figure_set/editable_drawio/P05_研究空白_AI数据执行层.drawio`
- Modify: `figures/opening_figure_set/main_svg/P05_研究空白_AI数据执行层.svg`
- Modify: `figures/opening_figure_set/main_png/P05_研究空白_AI数据执行层.png`
- Modify: `opening/report/figures/fig02_ai_data_execution_gap.png`

**Interfaces:**
- Consumes: the current three-column research-gap layout.
- Produces: two distinct auxiliary information blocks and two distinct return paths.

- [ ] **Step 1: Update the Draw.io source labels and geometry**

Keep the three main columns. Replace the combined bottom card with two adjacent cards:

```text
AI 算子代价信息
预计时间 · 资源需求
返回数据库优化器与多 SQL 调度

运行状态观测
排队 · 缓存 · 完成速度 · 作业进度
返回上游提交与路由
```

The cost-information arrow ends at the database-side planning card. The runtime-observation arrow ends at the submission/routing portion of the AI data execution layer. No arrow connects cost information directly to the model-service status card.

- [ ] **Step 2: Apply the same content and geometry to the authoritative SVG**

Use the existing orange styling for planned execution-layer functions. Use a blue-accented cost-information card because its output returns to the database side, and a grey or teal-accented runtime-observation card because it is measured during execution.

- [ ] **Step 3: Synchronize the Draw.io copy and SVG copy**

Copy the authoritative Draw.io XML content to:

```text
figures/opening_figure_set/editable_drawio/P05_研究空白_AI数据执行层.drawio
```

Copy the authoritative SVG content to:

```text
figures/opening_figure_set/main_svg/P05_研究空白_AI数据执行层.svg
```

- [ ] **Step 4: Render SVG to PNG and synchronize report copies**

Use bundled Node.js and `sharp` to render the authoritative SVG at `1600x900`, then copy the result to the opening figure set and report folder.

Expected PNG destinations:

```text
figures/architecture/editable/01_research_gap.png
figures/opening_figure_set/main_png/P05_研究空白_AI数据执行层.png
opening/report/figures/fig02_ai_data_execution_gap.png
```

- [ ] **Step 5: Validate visible text and arrows**

Search Draw.io and SVG for the removed combined phrase:

```powershell
Select-String -Path @(
  'figures/architecture/editable/01_research_gap.drawio',
  'figures/architecture/editable/01_research_gap.svg',
  'figures/opening_figure_set/editable_drawio/P05_研究空白_AI数据执行层.drawio',
  'figures/opening_figure_set/main_svg/P05_研究空白_AI数据执行层.svg'
) -Pattern '共同使能：代价估计|代价估计 \+ 状态观测' -Encoding UTF8
```

Expected: no matches. Preview the report PNG and confirm that both return arrows have explicit sources and targets.

---

### Task 3: Separate WorkDescriptor fields from optional estimates in Figure 3

**Files:**
- Modify: `figures/architecture/editable/03_work_unit.drawio`
- Modify: `figures/architecture/editable/03_work_unit.svg`
- Modify: `figures/architecture/editable/03_work_unit.png`
- Modify: `figures/opening_figure_set/editable_drawio/P12_研究内容一_WorkUnit与数据组织.drawio`
- Modify: `figures/opening_figure_set/main_svg/P12_研究内容一_WorkUnit与数据组织.svg`
- Modify: `figures/opening_figure_set/main_png/P12_研究内容一_WorkUnit与数据组织.png`
- Modify: `opening/report/figures/fig03_work_unit_organization.png`

**Interfaces:**
- Consumes: staged work features from Source/Input, Prepare, and Model/Result.
- Produces: a WorkDescriptor with known fields and a separate optional estimate output.

- [ ] **Step 1: Correct the WorkDescriptor contents**

Change the stage title:

```text
1) 分阶段工作描述
```

Replace the WorkDescriptor body with:

```text
• 身份：record · query · operator · job
• 阶段工作：source · prepare · model
• 局部性：prefix · data locality
• 服务信息：priority · deadline / SLO
```

- [ ] **Step 2: Add a separate optional estimate label**

Place a small adjacent card outside the WorkDescriptor boundary:

```text
可选代价估计结果
预计时间 · 误差范围 · 校准状态
```

Use an orange outline to distinguish an estimated value from the blue known-field descriptor. Connect it with a short dashed arrow from the WorkDescriptor, not as a nested bullet.

- [ ] **Step 3: Apply the same changes to the SVG and synchronize copies**

Update the authoritative SVG, then synchronize Draw.io, SVG, and regenerated `1600x900` PNG copies to the opening figure set and report folder.

- [ ] **Step 4: Validate descriptor semantics**

Search the Draw.io and SVG files.

Expected:

```text
`Confidence — uncertainty · calibration` has zero matches.
`可选代价估计结果` appears once in each source.
`分阶段工作描述` appears once in each source.
```

Preview the PNG and confirm that the estimate card is visibly outside the WorkDescriptor boundary.

---

### Task 4: Correct the cost-estimator role and public labels in Figure 5

**Files:**
- Modify: `figures/architecture/editable/02_system_architecture.drawio`
- Modify: `figures/architecture/editable/02_system_architecture.svg`
- Modify: `figures/architecture/editable/02_system_architecture.png`
- Modify: `figures/opening_figure_set/editable_drawio/P11_系统架构_数据组织与状态调度闭环.drawio`
- Modify: `figures/opening_figure_set/main_svg/P11_系统架构_数据组织与状态调度闭环.svg`
- Modify: `figures/opening_figure_set/main_png/P11_系统架构_数据组织与状态调度闭环.png`
- Modify: `opening/report/figures/fig05_system_architecture.png`

**Interfaces:**
- Consumes: database records, WorkDescriptor fields, historical measurements, and runtime observations.
- Produces: a corrected main execution path plus a separate database-planning output from the cost estimator.

- [ ] **Step 1: Update title and main-path labels**

Use this title:

```text
数据库记录的组织、提交、模型执行与结果返回
```

Use this subtitle:

```text
工作描述连接数据组织与外部执行，运行状态返回提交与路由模块
```

Keep `WorkDescriptor` as its own card. Its body contains only staged work, locality, job identity, and service information.

- [ ] **Step 2: Add the separate cost-estimation path**

Add two compact cards above the main path:

```text
AI 算子代价估计
预计时间 · 资源需求

数据库优化器 / 多 SQL 调度
计划比较 · 查询运行安排
```

Inputs to the estimator are WorkDescriptor and historical measurements. Its output arrow ends at the database-planning card. Do not route this arrow through runtime-state observation.

- [ ] **Step 3: Replace internal labels with concrete Chinese labels**

Apply these exact replacements:

```text
统一 source contract -> 统一输入字段
公共状态契约 -> 统一状态记录
统一结果契约 -> 结果完整性检查
safe capacity -> 运行前测得的工作量上限
同上限 frozen-static A/B · 动态策略不预设优胜 -> 同一工作量上限下比较固定参数与候选方法
```

Replace the bottom reuse sentence with:

```text
文本与图像共用：工作描述 · 数据组织 · 工作量上限 · 作业队列 · 路由 · 运行记录
```

- [ ] **Step 4: Synchronize Draw.io, SVG, and PNG outputs**

Update the authoritative source, synchronize the opening figure-set copies, render at `1600x900`, and replace the report PNG.

- [ ] **Step 5: Validate prohibited labels and arrow destinations**

Search Draw.io and SVG for:

```text
contract
frozen-static
WorkDescriptor + 代价估计
状态感知调度闭环
```

Expected: zero visible-text matches. Preview the PNG and confirm the cost estimator points to the database-planning card, while runtime state points to submission/routing.

---

### Task 5: Regenerate Figure 6 with formal group names and unchanged data

**Files:**
- Modify: `figures/scripts/generate_opening_story_figures_20260808.py`
- Modify: `figures/data/report_main/opening_text_baseline_evidence_map.png`
- Modify: `figures/data/report_main/opening_text_baseline_evidence_map.svg`
- Modify: `figures/opening_figure_set/main_png/P06_文本基线_执行路径与可比边界.png`
- Modify: `figures/opening_figure_set/main_svg/P06_文本基线_执行路径与可比边界.svg`
- Modify: `opening/report/figures/fig06_text_baseline_boundaries.png`

**Interfaces:**
- Consumes: the existing SQuAD and ShareGPT summary inputs already loaded by `figure_text_baseline_evidence_map()`.
- Produces: the same two-panel bars and numeric values with public-facing Chinese labels.

- [ ] **Step 1: Write a failing literal-label check**

Run:

```powershell
$script = Get-Content -Encoding UTF8 -Raw 'figures/scripts/generate_opening_story_figures_20260808.py'
@('产品 / database-E2E 轨','官方 Chat graph 轨','3 次 formal','vendor scheduler ownership','正式点') |
  ForEach-Object { if ($script.Contains($_)) { "OLD_LABEL: $_" } }
```

Expected before editing: at least the five listed labels are reported.

- [ ] **Step 2: Replace only the Figure 6 label literals**

Use these exact strings:

```text
两组文本执行路径需要分别比较
完整数据库执行路径：SQuAD 结果可直接比较
框架原生执行路径：ShareGPT 模型服务吞吐
统一 PostgreSQL 数据源与结果收集；3 次统计运行
同一 ShareGPT 输入清单；3 次统计运行；调度由被测框架负责
项目方法尚无相同输入与计时范围的对应结果，因此不加入本组排名
左右两图的输入、输出要求和指标不同，只能分别解释。条末数字为均值 ± 标准差。
```

Do not modify the data-loading, aggregation, sorting, bar-plotting, axis-limit, or numeric-formatting code.

- [ ] **Step 3: Regenerate only the text baseline figure**

Run the existing single-figure entry point with the bundled Python runtime:

```powershell
& 'C:\Users\Administrator\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' `
  'figures/scripts/generate_opening_story_figures_20260808.py' --figures T
```

Confirm the output files are:

```text
figures/data/report_main/opening_text_baseline_evidence_map.png
figures/data/report_main/opening_text_baseline_evidence_map.svg
```

- [ ] **Step 4: Verify numeric labels are unchanged**

Search the regenerated SVG for all seven baseline labels:

```text
136.6 ± 0.6
136.7 ± 0.1
137.8 ± 2.4
17.8 ± 0.0
17.3 ± 0.1
16.7 ± 0.1
3.6 ± 0.0
```

Expected: all seven appear once.

- [ ] **Step 5: Synchronize the opening figure-set and report copies**

Copy the regenerated SVG and PNG to P06, and copy the PNG to `opening/report/figures/fig06_text_baseline_boundaries.png`.

- [ ] **Step 6: Run the literal-label check again**

Expected: none of the old labels appear in the Figure 6 function or regenerated SVG.

---

### Task 6: Synchronize report wording, manifests, and visual audit

**Files:**
- Modify: `opening/report/opening_report.md`
- Modify: `opening/report/opening_report_20260820_qa.md`
- Modify: `opening/report/figures/README.md`
- Modify: `figures/opening_figure_set/README.md`
- Modify: `figures/audit/opening_figure_set_manifest_20260811.md`
- Modify: `figures/audit/opening_report_minimal_figure_corrections_design_20260823.md`
- Modify: `figures/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `opening/README.md`
- Modify: `opening/logs/project_log.md`

**Interfaces:**
- Consumes: the four final PNG/SVG outputs and the approved design.
- Produces: one consistent description of the figures, their sources, and the remaining limitations.

- [ ] **Step 1: Update Figure 2, Figure 3, and Figure 5 captions**

Remove explanations that existed only to correct the old picture, including statements that the combined box should be interpreted as separate concepts. Replace them with direct descriptions of the corrected visible structure.

- [ ] **Step 2: Update Figure 6 caption to match visible labels**

Use `完整数据库执行路径` and `框架原生执行路径` consistently. Remove explanations of the deleted `产品轨` and `vendor scheduler ownership` labels.

- [ ] **Step 3: Update source mappings and audit records**

Record that Figures 2, 3, and 5 received concept-only label and relationship corrections and Figure 6 received label-only regeneration. State explicitly that no experimental values changed.

- [ ] **Step 4: Run structural and terminology checks**

Run:

```powershell
$report = Get-Content -Encoding UTF8 'opening/report/opening_report.md'
"sections=" + (($report | Where-Object { $_ -match '^## [1-7]\.' }).Count)
"figures=" + (($report | Where-Object { $_ -match '^!\[图 ' }).Count)
"references=" + (($report | Where-Object { $_ -match '^\[[0-9]+\] ' }).Count)
Select-String -Path 'opening/report/opening_report.md' -Pattern '产品轨|框架轨|frozen-static|contract|WorkDescriptor \+ 代价估计' -Encoding UTF8
```

Expected:

```text
sections=7
figures=11
references=46
no prohibited-term matches
```

- [ ] **Step 5: Preview the four final report PNG files**

Confirm:

- no clipped text;
- no overlapping labels;
- every new arrow has a concrete source and target;
- the Figure 3 estimate card is outside WorkDescriptor;
- Figure 6 contains the original seven numeric labels.

- [ ] **Step 6: Run repository checks and commit only this task's files**

Run:

```powershell
$taskFiles = @(
  'figures/architecture/editable/01_research_gap.drawio',
  'figures/architecture/editable/01_research_gap.svg',
  'figures/architecture/editable/01_research_gap.png',
  'figures/architecture/editable/03_work_unit.drawio',
  'figures/architecture/editable/03_work_unit.svg',
  'figures/architecture/editable/03_work_unit.png',
  'figures/architecture/editable/02_system_architecture.drawio',
  'figures/architecture/editable/02_system_architecture.svg',
  'figures/architecture/editable/02_system_architecture.png',
  'figures/scripts/generate_opening_story_figures_20260808.py',
  'figures/data/report_main/opening_text_baseline_evidence_map.png',
  'figures/data/report_main/opening_text_baseline_evidence_map.svg',
  'figures/opening_figure_set',
  'opening/report/figures',
  'opening/report/opening_report.md',
  'opening/report/opening_report_20260820_qa.md',
  'opening/report/figures/README.md',
  'figures/README.md',
  'PROJECT_INDEX.md',
  'opening/README.md',
  'opening/logs/project_log.md'
)
git diff --check -- $taskFiles
python code/scripts/environment/scan_git_secrets.py
```

Expected: no whitespace errors and no secret-scan violations. Review the staged file list before committing so unrelated working-tree changes remain unstaged.

## Plan self-review

- Every requirement in the approved design appears in Tasks 2 through 6.
- The plan contains no TBD or TODO placeholders.
- Figure 6 data integrity has a before-and-after check.
- Concept diagrams preserve the existing layout and only add the minimum boxes and arrows needed to correct information ownership.
- Documentation and project indexes are included because the source figures and audit record change.
