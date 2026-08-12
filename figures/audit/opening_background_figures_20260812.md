# 开题第 2–4 页背景图批次审计

日期：2026-08-12

## 输出与用途

| 页码 | 权威源 | 图集副本 | 页面任务 |
|---:|---|---|---|
| 02 | `architecture/editable/opening_background_20260812/06_ai_native_execution_architecture` | `P02_背景_数据库AI算子外部执行链路` | 说明数据库 AI 算子已经形成数据库—外部执行—模型服务—写回链路 |
| 03 | `architecture/editable/opening_background_20260812/07_traditional_vs_external_ai_operator` | `P03_背景_传统算子与外部AI执行假设` | 对照传统 rows/cardinality 与 AI 多阶段 work/state 假设 |
| 04 | `architecture/editable/opening_background_20260812/08_related_work_landscape` | `P04_相关工作_跨层执行闭环` | 分层归纳相关工作，并把跨层闭环表述为待系统验证问题 |

## 内容边界

- 06 按项目实际链路绘制 PostgreSQL、数据库 AI 算子、AI Data Execution Layer、文本 vLLM、
  图像 typed CLIP Ray GPU actor 与 PostgreSQL + pgvector 写回；不把 AI_FILTER 或模型服务内部
  batching 写成课题研究内容。
- 07 只比较执行假设与需要表达的字段，不声称传统算子或 AI 算子性能高低；运行状态反馈只回到
  request organization、prepare 和 admission/routing。
- 08 的系统归类和短标签以本地精读材料为证据；底部结论使用“仍需系统验证”，不是文献穷尽性或
  已证实研究空白声明。

## 结构与视觉门禁

- 三张图均有 `.drawio`、`.svg`、1600×900 `.png` 和逐图 `.audit.md`。
- `check_drawio.py`：06 为 93 cells / 84 vertices / 7 edges / 14 SVG images；07 为
  57 / 44 / 11 / 9；08 为 37 / 35 / 0 / 0。三图全部通过。
- Draw.io 和 SVG XML 均通过 `xmllint --noout`；批量验证 3/3 通过。
- 逐图以 1600×900 原尺寸和 900×506 PPT 缩放检查：无文字越界、裁切、重复边框、隐藏残层、
  断线箭头或箭头穿字。
- 图集 SVG 的相对 icon 已转为内嵌 data URI，可单文件拖入 PowerPoint/Word；Draw.io 仍保留
  独立可编辑文字、卡片、连接器和 icon。

未解决项：无。
