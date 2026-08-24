# 开题 PPT 状态说明

## 2026-08-12 v9（当前 26 页对外答辩版）

当前 PPTX：`opening_defense_20260812_v9.pptx`；可复现构建脚本：
`build_opening_defense_v9_artifact_tool.mjs`。v9 以学校模板提供的页眉、配色和基本视觉识别为
基础，在 v5 的演示节奏上重组内容；当前版本删除重复页面并把结尾恢复为模板式的“后续工作与
预期成果 → 致谢”。核心依据是对外答辩叙事：

```text
数据库成为 AI 任务入口
  → 传统算子假设难以描述外部 AI 执行
  → 数据库 AI、数据执行、模型服务、代价决策四层研究现状
  → 数据库与模型服务之间仍缺跨层执行闭环
  → 两条总动机分别导出工作描述与数据组织、提交路由与多作业调度
  → 动机二再按配置层、模型服务层和 Job 层组织证据
  → 代价估计作为共同使能，图像作为跨模态验证
  → 两项研究内容独立验证后拼接，并检验联合优化是否必要
```

全稿面向外部评审清理了项目管理术语。关键数据结构和机制保留专业英文名，第一次出现时增加
中文解释，例如 `WorkDescriptor（分阶段工作描述）` 与 `Runtime State Snapshot（运行状态快照）`。
三张逐篇论文截图页合并为一张相关工作分层图，验证标准并入验证方案，研究计划与预期贡献合并。
数据页统一写清实验现象、系统含义与设计对应；每张主图下均有解释文字和本页结论。26/26 页已
完成渲染、画布溢出、空占位符、speaker notes 和逐页视觉检查；详见
`opening_defense_20260812_v9_qa.md`。

## 2026-08-12 v8（历史 20 页中文主讲版）

当前 PPTX：`opening_defense_20260812_v8.pptx`。v8 在 v7 学校模板与冻结叙事上增量替换第
2–4 页：数据库 AI 外部执行链路、传统/外部 AI 执行假设对照、相关工作分层三张图已经进入
正式内容区；其余 17 页和 20/20 speaker notes 保持不变。构建脚本为
`update_opening_defense_v8_artifact_tool.mjs`。

20/20 页渲染、overflow、模板保真、notes 和逐页目视均通过；详见
`opening_defense_20260812_v8_qa.md` 与 `opening_defense_20260812_v8_figure_content_audit.md`。

## 2026-08-12 v7（历史完整基线）

PPTX：`opening_defense_20260812_v7.pptx`。该版本继承 v6 学校模板的母版、页眉页脚和
安全内容区，按 `../opening_defense_outline_20260808.md` 重组为 20 页中文主讲结构；生成使用
`build_opening_defense_v7_artifact_tool.mjs`，没有运行旧 `build_ppt.py`。

该版本已完成 20/20 页渲染、图文一致性审计、空 placeholder、speaker notes 和模板保真
检查。正式图的比较合同与 `../claim_matrix.md` 一致；第 2–4 页增强图的历史提示词记录在
`opening_defense_20260812_v7_figure_content_audit.md`，其正式实现已经进入 v8。
完整记录见 `opening_defense_20260812_v7_qa.md`。

## 2026-08-08 内容大纲冻结（历史阶段）

该阶段按用户要求暂停 PPT 排版和生成，只维护
`../opening_defense_outline_20260808.md` 中的内容大纲、必要实验矩阵和图表数据合同；
SQuAD/ShareGPT replacement matrix 通过 feeding、correctness 与 stability 门禁后，只更新
实验报告和证据图，不生成或同步新的 PPTX。算子代价估计在大纲中作为数据组织与状态感知
调度共同使用的使能部件，不作为第三项研究内容。

## 2026-08-07 v6（已降级为待替换底稿）

当前 PPTX：`opening_defense_20260807_v6.pptx`（28 页）。它从 v5 学校模板映射复制，
使用 `build_opening_defense_v6_artifact_tool.mjs` 定点改写继承文本和图片帧；没有重跑旧
`build_ppt.py`。一页统一文本三臂边界、四张核心证据图、两项研究内容和计划/风险已经与
`opening/report/opening_report.md`、`opening/claim_matrix.md` 对齐。

程序化 QA：28/28 speaker notes 含“汇报讲稿/答辩备注/[Sources]”；空 placeholder 为 0；
`slides_test.py` 画布溢出检查通过；逐页 PNG montage 已人工检查并修正 v5 正文残留、图片替换
与底图透出。2026-08-08 已在 Microsoft PowerPoint 中真实打开并识别为 28 页，首页和缩略图
正常，无文件修复提示。PPT 本地内容与兼容性门禁已通过；飞书正文和四图也已同步回读，
其历史 QA 只证明文件可打开和旧内容自洽，不代表 2026-08-08 新叙事已经冻结。用户已明确
不需要 Wiki 同步。
完整记录见 `opening_defense_20260807_v6_qa.md`。

## 2026-07-29 v6 设计（历史计划）

下一版以 v5 的章节和页面为骨架，保留学校模板与人工调整。正文重点讲动机
测试、总体架构、两项策略设计和实验设计；已完成的大量正式实验进入答辩
备份或讲稿。official baseline 完成后，只有通过等价性、计量口径、规模校准
和正式重复门禁，才模块化插入 1–2 页结果。

设计说明：

```text
opening/slides/opening_defense_v6_design.md
```

该计划已由 2026-08-07 v6 实现取代。当前 Codex workflow 按 presentations skill 使用
artifact-tool 在 v5 复制映射上定点编辑；仍遵守“不运行旧 `build_ppt.py` 覆盖人工模板”的约束。

早期 `opening_defense_20260712.pptx`（已删除）和 `opening_ppt.md` 的内容与表现形式先作废，不作为下一版开题汇报内容依据；当前版本见文末 2026-07-20 v5 段落。

可以保留和复用的部分：

- 学校模板中的页面布局；
- 标题区、正文安全区、图表区和页脚位置；
- 每页一个主结论的页面组织原则；
- `汇报讲稿` 和 `答辩备注` 两类备注结构；
- 报告、PPT、飞书版需要保持同一口径的同步规则。

下一版 PPT 应重新基于：

```text
opening/report/opening_report.md
figures/README.md
figures/data/selected_motivation_figures.md
opening/ppt_rules.md
```

当前开题主讲版优先使用 `figures/opening_figure_set/` 的专用图集；权威实验源、可编辑源和复现脚本仍保留在原目录。PPT 生成后仍需用 WPS/PowerPoint 实际打开检查页面布局。
## 2026-07-20 v5

Current PPTX:

```text
opening/slides/opening_defense_20260720_v5.pptx
```

This version was copied from `opening_defense_20260720_v4.pptx` and edited
incrementally with `python-pptx`; `build_ppt.py` was not rerun. Main incremental
changes include the three data-organization mechanism slides, refined cover /
TOC / bottom-note layout, conservative prefix-aware wording, and one added
follow-up slide on multimodal generalization using the Daft stage-breakdown
figure.
