# 开题第 2–4 页背景图批次审计

日期：2026-08-12

## 输出与用途

| 页码 | 权威源 | 图集副本 | 页面任务 |
|---:|---|---|---|
| 02 | `architecture/editable/opening_background_20260812/06_ai_native_execution_architecture` | `P02_背景_数据库AI算子外部执行链路` | 说明数据库 AI 算子已经形成数据库—外部执行—模型服务—写回链路 |
| 03 | `architecture/editable/opening_background_20260812/07_traditional_vs_external_ai_operator` | `P03_背景_传统算子与外部AI执行假设` | 对照传统成本字段与 AI 语义算子的通用外部物理执行链路 |
| 04 | `architecture/editable/opening_background_20260812/08_related_work_landscape` | `P04_相关工作_跨层执行闭环` | 分层归纳代表工作，并指出数据库任务语义与服务运行信息衔接不足 |

## 内容边界

- 06 按项目实际链路绘制 PostgreSQL、数据库 AI 算子、AI Data Execution Layer、文本 vLLM、
  图像 typed CLIP Ray GPU actor 与 PostgreSQL + pgvector 写回；不把 AI_FILTER 或模型服务内部
  batching 写成课题研究内容。
- 07 只比较传统数据库算子与 AI 语义算子的通用外部物理执行阶段，不声称性能高低；外部物理路径使用
  输入、预处理/传输、服务队列/动态 batching、模型推理、结果整理/写回的通用流程，不出现本项目的
  Work Unit、credit、state-aware admission/routing、typed Ray actor 或 SLO 设计。
- 08 的系统归类和短标签以本地精读材料为证据；底部只指出四层工作各自演进以及任务语义—运行信息
  衔接不足，不使用本项目方案词汇，也不作文献穷尽性声明。

## 结构与视觉门禁

- 三张图均有 `.drawio`、`.svg`、1600×900 `.png` 和逐图 `.audit.md`。
- `check_drawio.py`：06 保持原版；07 为 53 cells / 43 vertices / 8 edges / 9 SVG images；08 为
  37 / 35 / 0 / 0。三图全部通过。
- Draw.io 和 SVG XML 均通过 `xmllint --noout`；批量验证 3/3 通过。
- 逐图以 1600×900 原尺寸和 900×506 PPT 缩放检查：无文字越界、裁切、重复边框、隐藏残层、
  断线箭头或箭头穿字。
- 图集 SVG 的相对 icon 已转为内嵌 data URI，可单文件拖入 PowerPoint/Word；Draw.io 仍保留
  独立可编辑文字、卡片、连接器和 icon。
- 2026-08-12 用户放大复核后再次修正 06：把 `文本 / Prompt`、`Shared credit`、
  `queue age`、`locality` 分别改为更紧凑的 `文本 / 提示词`、`共享额度`、`队龄`、`局部性`；
  主流程短箭头改为固定 12 px 头部并保留可见线身。权威源、图集副本与 v8 PPT 已同步重导出。
- 同轮增加颜色/线型/边框图例，并把两张上方机制卡的内容组左移 8 px；随后按工作流页面定位
  删除“本课题研究对象”，改为“外部 AI 数据执行层 / Work · Control · State”，避免与后续
  研究空白页重复。

未解决项：无。

## 2026-08-16 P03 背景叙事修订

- 删除此前提前出现的项目方案：Work Unit/WorkDescriptor、状态反馈、准入/路由、typed Ray GPU actor、
  work/state/SLO 字段。
- 右侧改为六阶段通用链路，五段蓝色连接均显示完整线身与一致的小型向下箭头。
- 图例只保留“执行主流程 / 执行阶段 / 影响因素”；影响因素用橙色虚线分组框表达，不与流程线混淆。
- P02 按用户要求保持不变；本轮未生成或修改 PPT。
