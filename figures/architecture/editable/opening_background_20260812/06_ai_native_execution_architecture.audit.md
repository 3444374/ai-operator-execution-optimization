# 06｜数据库 AI 算子的外部执行链路：重构审计

## 角色与结论

- 角色：开题 PPT 第 2 页背景架构图；解释“数据库 AI 算子为什么产生一条独立的外部执行链路”。
- 核心结论：PostgreSQL 中的表、文本和图像经 AI 算子进入外部 `work → control → state` 闭环，文本由 vLLM 执行，图像由 typed CLIP Ray GPU actor 执行，结果回写 PostgreSQL + pgvector；本课题不改模型服务内部 batching。
- 证据边界：这是一张研究对象与系统位置图，不表达动态策略已胜出；没有使用 `AI_FILTER` 作为主场景，也没有写“研究边界”或内部合同用语。

## 参考图可见元素 inventory

| ID | 区域 / bbox | 内容与视觉 | 介质 | 状态 |
|---|---|---|---|---|
| R01 | 0,0,1600,900 | 16:9 白底、无阴影/渐变 | native | accepted |
| R02 | 48,28–690,82 | 蓝色章节徽标“01”与主标题 | native | accepted |
| R03 | 120,104–1450,130 | SQL Analytics → Database AI Operators → External AI Execution 演进带 | native | accepted |
| R04 | 42,160–340,705 | 数据与 SQL 入口；PostgreSQL、SQL/AI SQL、表/文本/图像 | native + 独立 SVG | accepted |
| R05 | 365,160–605,705 | 数据库 AI 算子；AI_COMPLETE、AI_EMBED、AI_CLASSIFY | native + 独立 SVG | accepted |
| R06 | 640,160–1160,712 | 橙色虚线“本课题研究对象”；四张机制卡 | native + 独立 SVG | accepted |
| R07 | 670,254–888,422 | Work Unit / Descriptor；work 估计、locality、Packing | native + descriptor.svg | accepted |
| R08 | 912,254–1130,422 | 容量感知准入与路由；安全容量、SLO、endpoint | native + admission.svg | accepted |
| R09 | 670,446–888,614 | 多 Job Shared credit；idle borrowing、fair queue | native + routing.svg | accepted |
| R10 | 912,446–1130,614 | 运行状态快照；RuntimeState、队列/KV/完成率/queue age | native | accepted |
| R11 | 670,637–1130,681 | 组织 → 准入/路由 → 状态反馈总结条 | native | accepted |
| R12 | 1188,160–1558,705 | 模型服务与 GPU；vLLM、typed CLIP Ray GPU actor、不改内部 batching | native + 独立 SVG | accepted |
| R13 | 52,760–1548,836 | PostgreSQL + pgvector 写回、结果类型与产品实例 | native + sink.svg | accepted |
| R14 | 全图 | 蓝/橙/青/紫/绿五类语义色；2 px 主边框、13–18 px 圆角 | native | accepted |
| R15 | 全图 | 主标题 40 px、列标题 28 px、卡片正文 22–23 px；无低于 22 px 的可见正文 | native | accepted |

## 非文字视觉的介质判断

- 所有图标均为简单线性符号，采用独立 SVG，保持同一 3 px 圆角笔画体系；没有裁剪参考图，也没有商业 Logo。
- 卡片、容器、标题、标签、虚线边界和全部箭头为原生 Draw.io 可编辑对象。
- `.drawio` 未嵌入整页位图；没有白色遮罩、覆盖补丁、隐藏残片或重复共线边框。

## 箭头 inventory

| ID | 起点 → 终点 | 方向 / 几何 | 样式与语义 | 状态 |
|---|---|---|---|---|
| A01 | SQL Analytics → Database AI Operators | 水平，300,116 → 585,116 | 橙色 3 px，演进关系 | accepted |
| A02 | Database AI Operators → External AI Execution | 水平，900,116 → 1185,116 | 橙色 3 px，演进关系 | accepted |
| A03 | 数据入口 → 数据库 AI 算子 | 水平，340,455 → 365,455 | 蓝色 5 px，数据/请求流；头部落在目标框边缘 | accepted |
| A04 | 数据库 AI 算子 → AI Data Execution Layer | 水平，605,455 → 640,455 | 蓝色 5 px，执行请求流；不穿字 | accepted |
| A05 | AI Data Execution Layer → 模型服务 | 水平，1160,455 → 1188,455 | 紫色 5 px，服务提交；头部落在④左边缘 | accepted |
| A06 | 模型服务状态 → RuntimeState | 水平向左，1188,544 → 1130,544 | 青色 2.5 px 虚线，外围反馈；头部落在状态卡右边缘 | accepted |
| A07 | 模型服务 / GPU → PostgreSQL + pgvector sink | 垂直，1373,705 → 1373,758 | 绿色 3 px，结果/写回流；头部明确落到 sink 上边缘 | accepted |

## 技术与视觉验证

- `python3 .../check_drawio.py 06_ai_native_execution_architecture.drawio`：通过；93 cells、84 vertices、7 edges、14 个独立 SVG image cell、50 个 text cell。
- XML：`.drawio` 与 `.svg` 均通过 `lxml.etree.parse`。
- 输出：PNG 为 1600 × 900 RGBA；SVG 为 1600 × 900 纯矢量出版视图。
- 词语残留扫描：无“研究边界”、`AI_FILTER`、`RC1/RC2`、`BL1/BL2`、“合同/contract”。
- 全尺寸目视：逐项检查标题、四个主容器、机制卡、底部 sink、全部图标和 7 条箭头；无文字越界、卡片重叠、边框被覆盖、箭头只有头无线身、箭头穿字或裁切。
- PPT 缩放目视：按 900 × 506 预览检查；标题、四列标题、卡片主标签和主流程仍清晰，次级说明可用于讲解页；没有出现新的换行或裁切。
- 内容审计：明确区分文本 vLLM 与图像 typed CLIP Ray GPU actor；写回为工程 sink，不作为独立研究内容；动态机制只列研究对象，不宣称已经全面优于静态策略。

## 最终结论

所有 inventory 项与箭头项均为 `accepted`，无未解决问题。
