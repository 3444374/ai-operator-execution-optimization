# 开题 PPT 状态说明

## 2026-07-29 v6 设计

下一版以 v5 的章节和页面为骨架，保留学校模板与人工调整。正文重点讲动机
测试、总体架构、两项策略设计和实验设计；已完成的大量正式实验进入答辩
备份或讲稿。official baseline 完成后，只有通过等价性、计量口径、规模校准
和正式重复门禁，才模块化插入 1–2 页结果。

设计说明：

```text
opening/slides/opening_defense_v6_design.md
```

设计已按当前反馈修订，等待最终复审，尚未生成 v6 PPTX。实施时必须从 v5 复制后使用
`python-pptx` 增量编辑，不得重新运行 `build_ppt.py` 覆盖现有人工调整。

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
