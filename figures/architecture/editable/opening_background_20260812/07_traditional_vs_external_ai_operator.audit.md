# 07 传统数据库算子与外部 AI 算子：可编辑重构审计

## 角色与口径

- 图类型：Motivated Example / 执行假设对比。
- 核心结论：行数与静态上限不能同时表示 AI work 与模型服务运行状态。
- 项目口径：外部链路负责请求组织、预处理、准入与路由；文本后端为 vLLM，图像后端为 typed Ray GPU actor；不修改模型服务内部 batching。
- 参考：`figures/audit/reference_opening_background_20260812/07_traditional_vs_external_ai_operator.png`。

## 可见元素 inventory

| ID | 区域 / bbox | 内容与视觉描述 | 介质 | 样式与字号 | 状态 |
|---|---|---|---|---|---|
| C0 | 0,0,1600,900 | 16:9 白底画布 | native | 无渐变、无阴影 | accepted |
| T1 | 120,18,1360,58 | 主标题 | native | 42 px 黑色粗体 | accepted |
| P1 | 35,112,580,552 | 传统数据库算子面板 | native | 淡蓝背景、浅蓝外框 | accepted |
| P2 | 655,112,910,552 | 外部 AI 算子面板 | native | 淡蓝背景、浅蓝外框 | accepted |
| L1-L3 | 左面板 | rows/selectivity → CPU/I/O → result | native + SVG | 28 px，三张独立卡 | accepted |
| R1-R6 | 右面板 | records → organization → tokenize/resize → admission/routing → vLLM/Ray actor → gather | native + SVG | 25–27 px，六张独立卡 | accepted |
| N1 | 模型服务卡 | 模型服务内部 batching 保持不变 | native | 22 px 灰字 | accepted |
| F1 | 右面板外侧 | 运行状态反馈标签 | native | 青色胶囊，22 px，右边距 65 px | accepted |
| G1 | 标题下方 | 线型与边框图例：执行主流程、状态反馈、执行步骤、字段分组 | native | 18 px；蓝色实线、青色虚线、实线框、虚线框 | accepted |
| C1 | 45,678,580,142 | 传统成本字段 | native | 标题 28 px，卡片 24 px | accepted |
| W1 | 655,670,910,88 | AI work 字段 | native | 标题 22 px；四卡一行 24 px | accepted |
| S1 | 655,766,910,54 | 模型服务 state 字段 | native | 22 px；四卡等距 | accepted |
| K1 | 45,834,1520,44 | 核心结论条 | native | 28 px 橙色粗体 | accepted |
| I1-I9 | 两条流程卡内 | 表、CPU/I/O、结果、记录、组织、预处理、准入、服务、汇聚 | 独立 SVG | 同一蓝色线性图标族 | accepted |

## 箭头 inventory

2026-08-12 修正：三条青绿色反馈支线在 SVG 中拆为三个独立 path，每条分别带左向箭头；避免复合 path 仅在最后一段显示 marker。Draw.io 源仍对应三个独立原生连接器 `fb1`–`fb3`，线型与箭头尺寸一致。

同轮将右侧六张步骤卡压缩为 58–66 px 高，并把相邻间距统一扩大到 18 px；五段蓝色主流程连接均保留清晰线身与向下箭头。卡内正文使用卡片几何中心 x=1025，不再通过 `spacingLeft` 偏移。

| ID | 源 → 目标 | 路径 / 样式 | 语义 | 状态 |
|---|---|---|---|---|
| A1-A2 | 左侧三卡相邻连接 | 竖直蓝色实线，完整线身与块状箭头 | 传统流水线 | accepted |
| A3-A7 | 右侧六卡相邻连接 | 竖直蓝色实线，完整线身与块状箭头 | 外部 AI 执行流水线 | accepted |
| F2 | 模型服务 → 请求组织 | 右侧青色虚线总线，水平回流 | 运行状态参与请求组织 | accepted |
| F3 | 模型服务 → 预处理 | 右侧青色虚线总线，水平回流 | 状态辅助准备节奏 | accepted |
| F4 | 模型服务 → 准入/路由 | 右侧青色虚线总线，水平回流 | 状态参与准入与路由 | accepted |

## 技术验证与视觉审计

- [x] `xmllint --noout`：Draw.io、主 SVG 与 9 个独立 icon SVG 全部通过。
- [x] `check_drawio.py`：通过；新增图例后重新检查，无 warning；精确 cell/edge 数见批次审计。
- [x] Draw.io CLI 在当前环境不可用；PNG 由与 Draw.io 同坐标、同文字、同图标的 SVG 经本机 headless Chrome 渲染，结果为 1600×900 RGB。
- [x] 已用 `view_image(detail=original)` 全尺寸检查。标题、两侧流程、反馈虚线、字段卡和结论条均完整；无文字越界、相互遮挡、边框覆盖或残留旧图层。
- [x] work 字段经过重新分配：最终四卡只保留“输入 work / 输出 work / 局部性键 / 剩余 work / SLO”，使用 24 px 单行大字；卡宽 205/205/205/219 px，卡间距 12 px，无长英文串、压叠或缩字。
- [x] 反馈标签在右上独立胶囊中，距画布右边 65 px；虚线总线位于 x=1520，三个箭头落在目标卡右边界，不穿标签或正文。
- [x] 结论条位于 y=834–878，距画布底部 22 px；文字基线 y=865，未贴边或裁切。
- [x] 主流程箭头与反馈箭头均有线身和箭头头，方向清晰；反馈只回到请求组织、预处理和准入/路由，不进入模型内部。
- [x] 标题下方图例明确说明蓝色实线、青色虚线、实线框和虚线框含义；右侧六卡正文均以卡片几何中心对齐。
- [x] 左侧 `result` 取消旧 `spacingLeft`，文字中心与卡片几何中心 x=325 对齐；图标保持独立左置。
- [x] 9 个图标均为不同的独立 SVG，可移动、缩放和替换；没有整图截图、遮罩、白块覆盖或重复边框。

### 900 px 宽 PPT 缩放审计

- 1600→900 的缩放系数为 0.5625；42 px 主标题约等效 23.6 px，32 px 面板标题约 18 px，25–28 px 流程文字约 14.1–15.8 px。
- 该图按约 900×506 px 等比显示时，流程文字、字段卡和结论均保持可辨；同类字段只保留短标签，未依赖小字号长段落。
- 若落版区严格限制为 900×460，建议等比缩至 818×460 后居中；关键正文仍约等效 11.2–15.8 px，适合作为整页主图，不建议再叠加旁注。

## 全尺寸对照结论

- 保留参考图的左右对照、传统成本字段、AI work/state 字段与右侧反馈结构。
- 按项目实际补入文本与图像两条后端、typed Ray GPU actor、queue age、KV/GPU、completion/service rate，并明确不修改模型服务内部 batching。
- 当前版本所有 inventory 均为 accepted，未解决项：无。
