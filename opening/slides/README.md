# 开题 PPT 状态说明

## 2026-08-07 v6 内容冻结版

当前 PPTX：`opening_defense_20260807_v6.pptx`（28 页）。它从 v5 学校模板映射复制，
使用 `build_opening_defense_v6_artifact_tool.mjs` 定点改写继承文本和图片帧；没有重跑旧
`build_ppt.py`。一页统一文本三臂边界、四张核心证据图、两项研究内容和计划/风险已经与
`opening/report/opening_report.md`、`opening/claim_matrix.md` 对齐。

程序化 QA：28/28 speaker notes 含“汇报讲稿/答辩备注/[Sources]”；空 placeholder 为 0；
`slides_test.py` 画布溢出检查通过；逐页 PNG montage 已人工检查并修正 v5 正文残留、图片替换
与底图透出。2026-08-08 已在 Microsoft PowerPoint 中真实打开并识别为 28 页，首页和缩略图
正常，无文件修复提示。PPT 本地内容与兼容性门禁已通过；整个材料包仍待外部同步后发布冻结。
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

优先使用 `figures/architecture/` 和 `figures/data/report_main/` 中的正式图。PPT 生成后仍需用 WPS/PowerPoint 实际打开检查页面布局。
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
