# 开题可编辑概念图批次清单（2026-08-11）

## 选择结论

本批次不逐张照搬六张参考图，而是按当前 20 页答辩主线保留五个不可替代的页面任务：

| ID | 输出图 | 参考图 | 页面任务 | 处理结论 |
|---|---|---|---|---|
| G1 | `opening_research_gap_overview_editable` | 01 研究空白 | 说明 Database 与 Model Service 之间的研究空白、两项研究内容与共同使能 | 重构；保留三栏与蓝/橙/紫图标语言，删除过长清单及面向内部的研究边界锁卡 |
| G2 | `opening_system_closed_loop_editable` | 02 系统架构 | 说明 source → work organization → state-aware scheduling → executor → sink 及反馈闭环 | 重构；以当前 `WorkDescriptor` / `RuntimeStateSnapshot` 口径替换旧模块 |
| G3 | `opening_work_unit_organization_editable` | 03 Work-unit | 展开研究内容一：估计、typed descriptor、packing 与 BatchRequest | 重构；组织策略标为候选/互斥决策维度，不暗示全局最优 |
| G4 | `opening_state_aware_scheduling_editable` | 04 Credit / Routing | 展开研究内容二：安全容量、shared credit、fair queue、routing、completion release | 重构；明确先标定强静态上限，动态动作待同上限 A/B 验证 |
| G5 | `opening_causal_validation_route_editable` | 06 Evidence Gate | 说明 descriptor→observe-only→fallback→admission→routing/fairness 的因果验证路线 | 重构；不把既有初步证据画成“策略已经胜出” |

参考图 05“多模态共用统一调度抽象”不单独重绘。其核心信息并入 G2：文本/图像只替换 modality adapter 与执行后端，`WorkDescriptor`、状态快照、credit、routing 和 trace 复用。单独成图会与总体架构重复，也会挤占开题主讲页。

## 共用样式合同

- 画布：16:9，1600 × 900；白底，无装饰性渐变和阴影。
- 主色：数据/输入蓝 `#165DCC`；研究控制橙 `#E85D04`；执行后端紫 `#5B2AA6`；完成/验证绿 `#15803D`；观测灰 `#6B7280`。
- 图标：每个图标作为独立 SVG/Draw.io 对象，可移动、缩放、替换和改色；文字、卡片、连接器全部使用 Draw.io 原生元素。
- 字号（2026-08-11 大字版修订）：主标题 38–40 px；分区标题 26–30 px；卡片标题 22–24 px；
  正文与图例原则上 18–22 px。不要靠缩小到 14–16 px 塞字段；放不下时删除解释性细节，移到
  图注、报告正文或 PPT 讲稿。按 16:9 全屏和 A4 正文宽度两种缩放同时检查。
- 信息密度：每个主卡片最多 2–3 行，每行只保留机制关键词；完整字段、trace 和实验合同放审计/
  图注。字体放大必须同步调整卡片、间距与换行，不允许文字碰边或临时缩小局部字号。
- 编码：颜色与位置/线型双重编码；反馈流统一灰色虚线，完成/释放统一绿色实线。
- 术语：正式材料不使用 RC/BL/Phase/P0 等内部代号；动态机制统一写为“候选/待同上限验证”。

## 完成门槛

每张图必须同时提供 `.drawio`、PNG 预览和独立审计文件；通过 XML/越界检查，并在全尺寸预览中检查文字、箭头、图标与卡片边界。SVG 图标不作为不可编辑的整图截图嵌入。
