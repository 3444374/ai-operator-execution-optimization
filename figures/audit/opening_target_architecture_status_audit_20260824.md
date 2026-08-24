# 开题报告目标架构与实现状态图审计

日期：2026-08-24

## 1. 图件与来源

- 权威矢量图：`figures/architecture/opening_target_architecture_status.svg`
- 权威位图：`figures/architecture/opening_target_architecture_status.png`
- 报告副本：`opening/report/figures/target_architecture_status.png`
- 生成脚本：`figures/scripts/generate_opening_target_architecture_status.py`
- 画布：1600 × 900，PNG 为 RGB、300 DPI；SVG 已通过 XML 解析检查。
- 权威 PNG 与报告副本 SHA256：`b86a1fe43d2db2e55ac58af5706c2495ee291fa7de0bd3ffee51f9d7f646ed7f`。
- SVG SHA256：`132a9c3f06040c7589cc66d999360a8ab90c7697744e6b37d464f00b9c313265`。

该图为概念架构与实现状态图，不包含新增实验数据。内容来源是项目总纲、`opening/claim_matrix.md`、当前报告方法章节和现有实现记录。

## 2. 主张审计

图中有意分为两条路径：

1. 上层是目标数据库内算子路径，包括 PostgreSQL planner-visible AI 算子、关系 child plan、snapshot、`RowEnvelope`、LOTUS 1.2.4 `sem_map`、可替换外部物理后端和模型执行。
2. 下层是当前可运行路径，包括 PostgreSQL 外部读取、Daft/Arrow、`WorkDescriptor`、静态或共享提交、vLLM 或图像 Ray GPU actor，以及结果收集和写回。

上层使用橙色虚线并逐项标为“待实现”“迁移中”或“候选组合”；下层使用绿色实线并标为“已运行”或“有证据”。图中明确写出：两条路径共享外部物理执行思想，但现有外部链不能证明数据库内算子已经完成。

图下方只列两项研究内容。算子代价估计单列为共同使能组件，不写成第三项研究内容。图中没有声称修改 PostgreSQL core、vLLM 内部调度器、Ray scheduler 或模型实现。

## 3. 视觉审计

- 结构：两条水平执行链和三张底部研究卡片，阅读顺序从左到右；连接线不穿过文字。
- 状态编码：颜色与线型同时编码，绿色实线和橙色虚线不依赖颜色单独传达状态。
- 文字：中文采用系统黑体，主体字号不低于 16 px；关键模块标题为 19 px，主标题为 34 px。
- 图例：图底部解释实线、虚线和浅色模块的含义。
- 视觉检查：已按原始 1600 × 900 尺寸打开复核，未发现裁切、缺字、文字重叠或箭头遮挡。
- 可编辑性：SVG 保留文本和几何元素，后续调整应修改生成脚本后重新导出，不直接编辑 PNG。

## 4. 使用限制

该图只用于说明系统位置、目标路径和当前实现状态，不用于证明任何策略性能。实现状态变化后，必须同时更新脚本、SVG、PNG、报告副本、报告状态表和本审计文件。
