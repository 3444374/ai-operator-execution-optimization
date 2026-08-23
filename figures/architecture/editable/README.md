# 开题可编辑概念图

本目录保存 2026-08-11 按当前开题答辩主线重构的 Draw.io 概念图。参考图只作为版式和图形语言来源；可见文字、机制层次和证据口径以 `opening/claim_matrix.md` 与 `opening/opening_defense_outline_20260808.md` 为准。面向听众的图面只保留解释研究问题和方案所需的信息，内部边界清单留在项目文档中。

## 图集

| 图 | 答辩任务 | 建议页面 |
|---|---|---|
| `01_research_gap` | 说明数据库与模型服务之间需要补充的三项跨层能力，不提前展示具体候选算法 | 第 5 页 |
| `02_system_architecture` | 数据组织、提交、模型执行和结果返回，并分开代价估计与运行状态路径 | 第 11 页 |
| `03_work_unit` | 研究内容一：分阶段工作描述、可选代价估计结果、候选批次组织与同一上限评价 | 第 12–13 页 |
| `04_state_aware_scheduling` | 研究内容二：安全容量、shared credit、fair queue、routing 与 completion release | 第 14–15 页 |
| `05_evidence_gate` | 已有研究基础与后续工作计划 | 第 19 页 |
| `opening_background_20260812/06_ai_native_execution_architecture` | 数据库 AI 算子外部执行工作流 | 第 2 页 |
| `opening_background_20260812/07_traditional_vs_external_ai_operator` | 传统成本字段与通用外部 AI 多阶段执行链路对照 | 第 3 页 |
| `opening_background_20260812/08_related_work_landscape` | 数据库 AI、数据执行、推理服务、代价决策的相关工作分层与衔接缺口 | 第 4 页 |

每张图提供：

- `.drawio`：主编辑源，文字、卡片、连接器和 icon 均为独立对象；
- `.svg`：Word、报告和 PPT 的矢量插图；
- `.png`：1600 × 900 预览；
- `.audit.md`：元素 inventory、箭头 inventory、证据边界和视觉检查；
- `assets/<stem>/`：图中 icon 的独立资产；机制图标优先为 SVG，产品标识可保留官方提供的 PNG/SVG。

## 图标与授权

本批次不直接裁剪参考 PNG 中的 icon。机制 icon 依据参考图的线性视觉语言，以简单 SVG 路径重绘并作为独立对象嵌入；没有把参考图或整张位图嵌入 `.drawio`。产品 Logo 优先使用该项目官方仓库或品牌资源页提供的原始资产，保持官方比例与颜色，并在对应审计中登记来源、哈希和商标边界。通用第三方图标优先使用许可证清楚的 Lucide（ISC）或 Material Symbols（Apache-2.0）。

## 编辑建议

1. 在 Draw.io 打开 `.drawio` 修改文字、卡片和连线；不要在 PNG 上覆盖文字。
2. icon 可在画布中独立移动、缩放或替换；如需改路径，编辑 `assets/<stem>/*.svg` 后重新嵌入。
3. PPT 只需要缩放时导入 SVG；如果需要逐节点修改，回到 Draw.io 编辑后再导出。
4. 动态策略始终保留“候选 / 待同上限 A/B 验证”边界，不把绿色门禁或完成标记解释为策略已经胜出。
5. 本图集以 PPT 全屏和 A4 正文宽度共同可读为门槛：正文/图例原则上不小于 18–22 px；如果
   增加内容导致字号下降，应先删减字段和解释，而不是缩小字体。
6. 禁止用整页位图、白色遮罩或新卡片覆盖旧错误对象。修改时应删除错误节点并重建单一干净结构；导出前检查重复边框、共线重复路径和隐藏残片。
7. 箭头除语义正确外还要服从统一视觉层级：同类主流程使用一致的线宽、头部尺寸、中心线和端点留白；短间距缩小头部；反馈流用更细的灰色虚线和更小头部走外围；汇合后只保留一个箭头头部。图例必须同步这些样式。

`opening_background_20260812/` 是第 2–4 页的补充批次。背景页与方案页严格分开：第 3 页只画
大众化外部 AI 算子流程，不提前使用本项目的 Work Unit、credit 或 state-aware 控制设计；第 4 页
只作相关工作分层和审慎问题归纳。每张图均提供 Draw.io、SVG、1600×900 PNG 和独立审计；批次清单见同目录
`drawio_batch_manifest.json`。

批次选择合同见 `../../audit/opening_editable_diagrams_manifest_20260811.md`；原始参考图仅保存在 `../../audit/reference_opening_editable_20260811/` 供视觉审计，不作为正式材料插图。
