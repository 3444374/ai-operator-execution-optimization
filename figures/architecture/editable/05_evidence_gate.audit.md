# 05 研究基础与后续工作计划 — 可编辑图审计

参考版式：`figures/audit/reference_opening_editable_20260811/05_evidence_gate.png`
当前用途：开题汇报中的“已有基础 + 后续研究计划”，不作为内部实验门禁或算法承诺清单。

## 内容边界

- 前两阶段只陈述当前已经完成的基础：数据库 AI 执行链路与正确性检查、强静态基线与容量标定。
- 后五阶段统一标为“后续工作”，仅说明研究任务与评价方向；不预设最终算法、实现顺序或动态策略一定胜出。
- 图面不再使用“合同 / contract、Trace、Evidence Gate、晋级门槛、Admission-only、Observe-only、Formal Repeats、因果验证路线”等内部工程术语。
- 后续任务概括为：数据组织、状态观测、提交控制、多作业调度、综合验证与多模态泛化。

## 可见元素 inventory

| ID | 区域 | 内容 | 介质 | 状态 |
|---|---|---|---|---|
| T1 | 顶部 | 页码 05、标题“研究基础与后续工作计划”、一句话副标题 | native text | accepted |
| S1 | 32,120,196,400 | 已完成：基础链路与正确性 | native + shield SVG | accepted |
| S2 | 252,120,196,400 | 已完成：强静态基线与容量标定 | native + gauge SVG | accepted |
| S3 | 472,120,196,400 | 后续工作：数据组织策略 | native + document SVG | accepted |
| S4 | 692,120,196,400 | 后续工作：状态观测与反馈 | native + state SVG | accepted |
| S5 | 912,120,196,400 | 后续工作：提交控制策略 | native + bounded-flow SVG | accepted |
| S6 | 1132,120,196,400 | 后续工作：多作业调度 | native + routing SVG | accepted |
| S7 | 1352,120,196,400 | 后续工作：综合验证与多模态泛化 | native + repeat SVG | accepted |
| A1–A6 | 七阶段之间 | 蓝色研究推进箭头 | native connectors | accepted |
| P1 | 左下 | 统一实验原则：输入与任务、资源与配置、重复与记录、验证场景 | native | accepted |
| P2 | 右下 | 预期评价维度：效率、服务质量、多作业表现、结果可靠性 | native + check SVG | accepted |
| L1 | 底部 | 研究推进 / 已完成 / 后续工作 / 评价与调整图例及不预设方案声明 | native | accepted |

## 箭头 inventory

| ID | 源 → 目标 | 样式与语义 | 状态 |
|---|---|---|---|
| A1–A6 | 相邻阶段 | 4 px 蓝色水平实线，表示工作计划的时间推进，不表达因果晋级 | accepted |
| F1 | 原 S7 → Evidence Gate | 已从 Draw.io 与 SVG 中删除；计划图不需要证据门反馈线 | accepted |

## 版式与文字检查

- 1600×900、16:9；主标题 40 px、副标题 21 px、阶段标题 22 px、分区标题 27 px、正文与图例最小 18 px。
- 每个阶段只保留两张信息卡：前两阶段为“已完成 / 已有基础”，后五阶段为“主要内容 / 评价重点”。
- “后续工作”刻意保持方向级表述，不锁定具体算法或实现细节。
- 七列正文均位于 172 px 内卡片中，实际渲染无裁切、越界、贴边或跨列。
- 底部两块面板标题、三张实验原则卡、四条评价维度和橙色说明均无越界。
- 蓝色推进箭头位于 24 px 列间距内；无灰色反馈线、悬空箭头或重复箭头头部。
- 图标为 8 个独立 SVG / Draw.io image 对象；无整图截图、遮罩、补丁层或隐藏残片。

## 技术验证

- `check_drawio.py`：通过，96 cells、88 vertices、6 edges、8 个独立 SVG image cells、60 text cells。
- Draw.io XML 与 SVG：`xmllint --noout` 通过。
- Draw.io CLI：本机未安装，`export_drawio.py` 返回 `draw.io CLI not found`。
- PNG：由同坐标 SVG 使用本机 headless Chrome 渲染，1600×900；已在原始尺寸完成视觉检查。
- 可见文字扫描：无“合同 / Evidence Gate / 因果验证 / 待同上限验证 / Trace / Admission-only / Observe-only / Formal Repeats”。
- 未解决项：无。
