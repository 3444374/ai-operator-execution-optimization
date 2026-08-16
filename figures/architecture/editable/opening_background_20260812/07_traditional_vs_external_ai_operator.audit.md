# 07 传统数据库算子与外部 AI 算子：可编辑重构审计

## 角色与口径

- 图类型：背景概念图 / 通用执行假设对比。
- 核心结论：传统数据库算子的典型成本可由 rows、cardinality、CPU 与 I/O 概括；外部 AI 算子跨越输入、预处理与传输、服务排队与动态 batching、模型推理、结果整理与写回，成本不再由行数单独决定。
- 叙事边界：本页只介绍大众化的外部 AI 算子执行链路，不出现本课题的 Work Unit、WorkDescriptor、credit、state-aware admission/routing、typed Ray actor 或 SLO 设计。
- 参考：`figures/audit/reference_opening_background_20260812/07_traditional_vs_external_ai_operator.png` 仅提供左右对照版式，不作为内容事实源。

## 可见元素 inventory

| ID | 区域 | 内容与视觉描述 | 介质 | 状态 |
|---|---|---|---|---|
| C0 | 0,0,1600,900 | 16:9 白底画布 | native | accepted |
| T1 | 顶部 | `传统数据库算子与外部 AI 算子的执行假设` | native text | accepted |
| G1 | 标题下方 | 执行主流程、执行阶段、影响因素图例 | native | accepted |
| P1 | 左侧 | rows/selectivity → CPU/I/O operator → result | native + SVG | accepted |
| P2 | 右侧 | 输入记录 → tokenize/decode/resize → 请求构造与传输 → 服务队列与动态 batching → 模型推理 → 结果整理与写回 | native + SVG | accepted |
| C1 | 左下 | rows、cardinality、CPU、I/O | native | accepted |
| C2 | 右下 | 输入规模、预处理与传输、服务队列与 batch、模型计算、输出规模 | native | accepted |
| K1 | 底部 | `AI 算子跨越多个执行阶段，成本不再由行数单独决定` | native | accepted |
| I1-I9 | 两侧流程卡 | 9 个不同的独立线性 SVG 图标 | independent SVG | accepted |

## 箭头 inventory

| ID | 源 → 目标 | 路径 / 样式 | 语义 | 状态 |
|---|---|---|---|---|
| A1-A2 | 左侧三卡相邻连接 | 竖直蓝色实线，完整线身与小型向下箭头 | 传统流水线 | accepted |
| A3-A7 | 右侧六卡相邻连接 | 竖直蓝色实线，完整线身与小型向下箭头 | 外部 AI 执行主流程 | accepted |
| L1 | 图例 | 蓝色短线与箭头 | 解释主流程线型 | accepted |

本版没有状态反馈回路、虚线总线、分支或汇合箭头；影响因素只用橙色虚线分组框表示，避免把本课题控制设计提前画入背景页。

## 技术验证与视觉审计

- [x] `check_drawio.py`：53 cells / 43 vertices / 8 edges / 9 SVG-image cells / 27 text cells，无 warning。
- [x] Draw.io、主 SVG、图集 SVG 与独立 icon 均通过 XML 检查。
- [x] PNG 为 1600×900；已按原尺寸与 900×506 缩放目视检查，无文字越界、卡片重叠、重复边框、残留图层或断线箭头。
- [x] 左侧 `result` 与右侧六个阶段标题均按卡片几何中心对齐；图标独立左置，未挤压正文。
- [x] 右侧步骤 2 使用预处理图标、步骤 3 使用组织/传输图标，图标语义与顺序一致。
- [x] 所有主流程箭头同时具有可见线身和箭头头；短间距使用一致的小箭头尺寸。
- [x] 图中无整页 raster、遮罩、白块覆盖、隐藏旧节点或重复路径。

**最终状态：accepted；未解决项：无。**
