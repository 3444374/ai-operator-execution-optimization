# 02_system_architecture 可编辑重构审计

## 1. 图的角色与结论

- 角色：开题答辩第 11 页的 Solution Overview / 总体技术路线图。
- 参考：`figures/audit/reference_opening_editable_20260811/02_system_architecture.png`。
- 当前口径来源：`opening/claim_matrix.md`、`opening/opening_defense_outline_20260808.md` 第 11 页及多模态段落、`figures/audit/opening_editable_diagrams_manifest_20260811.md`。
- 一句话结论：数据库记录先被转成可估计、可组织的 work，再由安全容量和新鲜运行状态约束准入、路由与多 Job 分配；文本与图像仅替换 adapter/backend，公共 work/state/credit/routing/trace 契约复用。
- 研究边界：不修改 vLLM continuous batching、Ray 调度器、模型结构或 GPU kernel；动态动作标为候选，必须与同上限 frozen-static 做因果 A/B。

## 2. 样式与介质合同

- 画布：1600 × 900，16:9，白底，无阴影与装饰性渐变。
- 字体：Draw.io 原生文本使用 `PingFang SC`；SVG 文本保留为 `<text>/<tspan>`，并设置 Microsoft YaHei / Arial 回退。大字版使用 40 px 主标题、26–28 px 分区标题、22–24 px 卡片标题和不低于 18 px 的正文/图例。
- 颜色：输入与 work 描述蓝 `#165DCC`；研究控制橙 `#E85D04`；模态 adapter/backend 紫 `#5B2AA6`；sink/结果绿 `#15803D`；状态与反馈灰 `#6B7280`。
- 结构：卡片、分区、标签、标题、说明和连接器均为 Draw.io 原生对象。
- 图标：12 个自绘线性 SVG 与 1 个 Daft 官方 PNG 标识，分别作为独立 Draw.io image 对象嵌入，同时保存在私有资产目录；没有把整张参考图作为截图嵌入。Daft 标识保持官方黑/洋红配色和纵横比，不改路径、不改色。
- 反馈流：灰色虚线；前向数据/work 流：蓝色；调度决策流：橙色；完成/写回：绿色。

## 3. Visible-element inventory

| ID | 区域 / 近似 bbox | 内容与视觉说明 | 介质 | 样式要点 | 状态 |
|---|---|---|---|---|---|
| I01 | 全画布 0,0,1600,900 | 白色 16:9 页面 | native | 无纹理、无阴影 | accepted |
| I02 | 40,28,72,56 | 蓝色页码徽标 `02` | native | 圆角、白字 | accepted |
| I03 | 132,24,1390,97 | 主标题与统一 work/state 契约副标题 | native | 黑色粗体标题、灰色副标题 | accepted |
| I04 | 40,170,250,430 | 数据源与数据引擎面板 | native | 蓝色标题条、白底蓝框 | accepted |
| I05 | 80,250,190,94 | PostgreSQL source：数据库图标、prompt/image、job/SLO | SVG + native text | 蓝色线性图标 | accepted |
| I06 | 65,365,200,1 | source 内部分隔线 | native | 浅灰 1 px | accepted |
| I07 | 80,395,190,96 | Daft DataFrame：扫描/投影/分区/批次 | official PNG + native text | Daft 官方黑/洋红标识 | accepted |
| I08 | 65,525,200,48 | 统一 source contract 标签 | native | 蓝色浅底 pill | accepted |
| I09 | 335,155,760,455 | AI Data Execution Layer 研究层 | native | 橙色虚线圆角框 | accepted |
| I10 | 365,165,700,33 | execution-layer 标题、动态候选标签 | native | 橙色标题与浅橙 pill | accepted |
| I11 | 370,207,690,38 | 两项研究内容分区标题 | native | 蓝/橙双编码 | accepted |
| I12 | 370,255,320,120 | WorkDescriptor + 代价估计 | native + 2 SVG | staged work、locality、SLO、uncertainty；详细字段移到图注 | accepted |
| I13 | 730,255,330,120 | Work Organizer | native + SVG | work budget、balance、locality、BatchRequest | accepted |
| I14 | 370,410,320,140 | 安全容量 + State-aware Admission | native + SVG | bounded active work、fresh/stale fallback | accepted |
| I15 | 730,410,330,140 | Routing + Multi-job | native + SVG | credit、idle borrowing、fair queue、SLO guard | accepted |
| I16 | 370,564,690,28 | 同上限 frozen-static 因果 A/B 约束 | native | 浅橙提示条；明确不预设优胜 | accepted |
| I17 | 1120,170,225,430 | 模态分支面板 | native | 紫色标题条与边框；只保留模态差异 | accepted |
| I18 | 1140,248,185,103 | Text Adapter / Prompt → vLLM | native + SVG | 两行短文本、紫色文档线性图标 | accepted |
| I19 | 1140,370,185,125 | Image Adapter / Tensor → CLIP / ViT | native + SVG | 两行短文本、紫色图像线性图标 | accepted |
| I20 | 1150,516,175,62 | 公共 GPU/KV/queue/stage 受控状态 | native + SVG | 紫色 GPU 线性图标 | accepted |
| I21 | 1365,170,195,430 | 统一 Sink 面板 | native | 单一绿色标题条与边框；与紫色面板保留 20 px 净距 | accepted |
| I22 | 1377,265,171,228 | PostgreSQL；text/label、pgvector embedding、exactly-once | native + SVG | 绿色数据库-check 图标；正文严格三行 | accepted |
| I23 | 1380,525,160,48 | 统一结果契约 | native | 绿色浅底 pill | accepted |
| I24 | 335,645,1035,145 | RuntimeStateSnapshot + Trace 公共状态契约 | native + 2 SVG | 灰色虚线框 | accepted |
| I25 | 365,738,980,36 | 四组观测字段 chips | native | stage queue、running/waiting/KV、GPU/MFU/service rate、per-job progress/completion | accepted |
| I26 | 335,806,1225,52 | 跨模态复用结论条 | native | 蓝色浅底；紫色 adapter/backend pill | accepted |
| A01 | source → estimate | 前向 source/work 流 | native connector | 蓝色实线 block arrow | accepted |
| A02 | estimate → organizer | work 描述进入组织器 | native connector | 蓝色实线 | accepted |
| A03 | organizer → admission | BatchRequest 进入容量/准入 | native connector | 橙色折线 | accepted |
| A04 | admission → routing | bounded active work 进入路由与共享 | native connector | 橙色实线 | accepted |
| A05 | routing → backend | 提交到模态执行后端 | native connector | 蓝色折线 | accepted |
| A06 | backend → sink | 完成与统一写回 | native connector | 绿色实线 | accepted |
| A07 | backend → snapshot | 执行阶段状态反馈 | native connector | 灰色虚线 | accepted |
| A08 | sink → snapshot | completion / result feedback | native connector | 灰色虚线 | accepted |
| A09 | snapshot → admission | 新鲜状态约束准入 | native connector | 灰色虚线 | accepted |
| A10 | snapshot → routing | 新鲜状态约束路由/多 Job | native connector | 灰色虚线 | accepted |

## 4. 图标资产 inventory

`assets/02_system_architecture/` 包含 12 个独立 SVG：`database.svg`、`estimate.svg`、`descriptor.svg`、`organizer.svg`、`admission.svg`、`routing.svg`、`text.svg`、`image.svg`、`gpu.svg`、`sink.svg`、`snapshot.svg`、`trace.svg`，以及 Daft 官方标识 `daft-official.png`。官方资产取自 Daft 官方仓库 `docs/img/favicon.png`，本地仅等比缩至 256 × 256，SHA-256 为 `e9715fc2499a742ebb90732048e642be99d5196da7d6e8532cdb215a0c70368d`。该标识只用于指明图中的 Daft DataFrame 产品，不表示官方背书；仓库代码许可证与商标/Logo 使用权分开判断。

## 5. 技术验证

- `check_drawio.py`：通过。
- 统计：93 cells；81 vertices；10 editable edges；13 image cells（12 个 SVG + 1 个官方 PNG）；40 text cells。
- Draw.io CLI：本机不可用，未安装额外依赖。
- SVG：已从源文件重建为与 Draw.io 同坐标的 1600 × 900 页面；文字、卡片和箭头均为原生矢量对象，12 个机制图标为独立 SVG，Daft 官方标识为唯一的局部 PNG 资产。
- 源图层清理：删除了旧版 SVG 中作为整页底图的 `data:image/png`，也删除了后加的紫色/绿色覆盖层。独立复核再次确认：当前紫色模态面板和绿色 Sink 都只存在一套原生边框与内容，不依赖遮盖错误对象。
- PNG：使用本机 headless Chrome 从当前干净 SVG 直接渲染，保留实际中文字体度量和外部图标；未使用 Quick Look padding、裁切或白色遮罩。
- PNG 尺寸：1600 × 900。
- 禁用术语扫描：未使用 RC/BL/Phase/P0 等内部代号；没有把动态策略写成已胜出或通用最优。
- 字号检查：除图标本身外，正文、chips、候选标签、边界说明和底部复用条均不低于 18 px；没有为解决局部拥挤而缩小字体。

## 6. 全尺寸视觉审计

- 标题、副标题、页码、所有面板和图标齐全；未发现裁切、越界、背景接缝、重叠旧边框或残留底图。
- 主链路方向可从左到右读取；source→WorkDescriptor、WorkDescriptor→Organizer、Organizer→Admission、Admission→Routing、Routing→模态分支、模态分支→Sink 六段全部可见。Organizer→Admission 的折线路径只经过上下卡片之间的 35 px 空白，不穿文字。
- 四条灰色虚线反馈均走面板外围：backend/sink 指向 snapshot，snapshot 分别从研究边界左右空隙进入 admission/routing；均避开底部 A/B 约束条及正文。
- 蓝/橙/紫/绿与灰虚线同时由颜色、位置和线型编码，灰度下仍可根据分区与线型区分。
- `RuntimeStateSnapshot + Trace` 与被观测执行后端、sink、admission、routing 共享中心线，不悬空。

## Sink icon completeness repair (2026-08-11)

- The former sink SVG intentionally shortened the database body and bottom contour around the check badge, which made the cylinder look clipped or partially missing after scaling.
- The icon was redrawn as a complete three-level database cylinder with closed side and bottom geometry. The validation badge is smaller and sits at the outer lower-right edge, preserving both the cylinder silhouette and the check mark.
- The fix was applied to the independent `assets/02_system_architecture/sink.svg` source and re-embedded in the Draw.io image cell; no overlay, mask or raster patch was added.
- The regenerated 1600×900 PNG was inspected at original size. The database top, two dividers, side walls, lower closure and validation badge are all visible. Unresolved items: none.
- 文本与图像分支明确写出各自 adapter/backend；底部复用条重复一次公共契约，跨模态结论无需读图注即可识别。
- 文字在 1600 × 900 全尺寸预览中可读；紫色模态卡与绿色 Sink 的每一行均完全位于卡内，和边框保持安全间距，没有跨框或互相覆盖。
- PPT 可读性：按 16:9 全屏检查，主链、四张研究卡、模态差异和反馈层无需放大即可识别；每张卡只保留 1 个标题和 1–2 行关键词。
- A4 文档可读性：按页面宽度缩放后，18 px 最小字号、四色分区、实线/虚线和粗体层级仍可区分；实现字段清单与 typed Ray actor 等细节保留在本审计/正文图注，不在图面局部缩字。
- 未解决项：无。

## 7. 大字版压缩说明

- 从卡片中移除 `source/prepare/model/result work`、`predicted drain`、`per-job floor/cap`、`Completion`、`typed Ray actor` 等实现或字段堆叠，只保留答辩时必须读出的决策接口。
- 被移除细节不改变系统口径：图像实现后端仍是 typed Ray GPU actor，文本后端仍是 vLLM；两者只替换 adapter/backend，公共 WorkDescriptor、Organizer、safe capacity、credit、routing、multi-job 和 trace 复用。
- 保留“同上限 frozen-static A/B · 动态策略不预设优胜”，避免大字压缩削弱证据边界。

## 8. 独立边界级复核与修复（2026-08-11）

### 发现的问题

1. SVG 把 10 条连接器放在带填充的研究边界和卡片之前，导致 layer 内四条主连接器被背景覆盖；PNG 只显示 source、routing→backend 等暴露在面板外的小段，不能表达完整流程。
2. Draw.io 的 `flow_source_estimate` 指向不存在的 `layer_panel`，属于断开的错误目标节点。
3. 原 snapshot→admission/routing 虚线路由从卡片底部进入，会穿过 `同上限 frozen-static A/B` 约束条。
4. 紫色模态面板到绿色 Sink 只有 10 px 间距，而 SVG marker 默认随 4 px 线宽缩放，短绿色箭头头部过重且不清晰。
5. `Text Adapter` / `Image Adapter` 在真实 PingFang 字体下右侧安全边距不足。

### 精确修复

- 删除 SVG 顶部那组会被面板遮住的旧连接器节点，在所有结构对象之后重建唯一一组 10 条 connector；没有增加 mask、overlay 或白色遮罩。
- 将 Draw.io `flow_source_estimate` 的 target 从不存在的 `layer_panel` 改为 `estimate_card`，并把 entry 对齐到卡片左侧中心线。
- source→WorkDescriptor 延长到真实目标边界；四条 layer 内部前向箭头使用水平/垂直中心线和一致的圆角正交路径。
- snapshot→admission 改为经研究边界左侧 15 px 空隙进入卡片左侧；snapshot→routing 经研究边界右侧空隙进入卡片右侧。两条路径不再穿过 A/B 约束条。
- 模态→Sink 间距从 10 px 增至 20 px；Sink 保持右边界不变，宽度由 205 px 调整为 195 px，并重新居中图标、标题和正文。
- SVG 前向箭头改为固定 user-space marker：蓝/橙 12 px，短绿色 10 px；灰色反馈 9 px。Draw.io 对应使用 `endSize=10/8/7`。短绿色箭头线宽降为 3 px，避免在 20 px 间距内形成视觉楔块。
- `Text Adapter` 与 `Image Adapter` 最终统一为 18 px，并通过左移图标、扩展标题文本框恢复右侧安全留白；没有低于全图 18 px 下限。

### 复核结果

- `check_drawio.py`：通过，93 cells / 81 vertices / 10 edges / 13 image cells（12 SVG + 1 official PNG）/ 40 text cells。
- Draw.io edge reference 审计：10 条边的 source/target 全部存在；0 个重复 cell id。
- `xmllint --noout`：Draw.io 与 SVG 均通过。
- 字号扫描：Draw.io 与 SVG 可见文字最小 18 px；无小于 18 px 的补丁字号。
- 图层扫描：Draw.io 仅 `source_daft_icon` 含 1 个局部 `data:image/png`（Daft 官方标识）；0 个整页 raster、0 个 mask/clip/filter、0 个透明隐藏节点、0 个 overlay。其余 12 个 SVG 图标角色互异。
- PNG：使用 headless Chrome 从修复后的 SVG 重新渲染为 1600 × 900，并以 `view_image(detail=original)` 全尺寸复核。
- 箭头审计：所有前向箭头起止点明确；同类 stroke/head size 一致；短绿色箭头不过重；四条灰虚线弱化并走外围；无箭头穿字、被面板遮住或停在空白处。
- 未解决项：无。

## 9. Daft 官方标识与溢出复核（2026-08-11）

- 旧版蓝色折线 Daft 图标是本图集按参考风格重绘的近似符号，不是 Daft 官方 Logo；现已从 Draw.io、SVG 与资产目录中真实删除，没有用官方图覆盖旧对象。
- 现使用 Daft 官方仓库 `docs/img/favicon.png` 的黑/洋红标识。Draw.io 嵌入 PNG 以保证单文件可编辑与可移动；SVG 引用同一独立资产；两处均保持纵横比。
- Admission 卡不再把 241 px 左右的 `State-aware Admission` 塞入 205 px 文本框：两行标题改为使用卡片 286 px 内容宽度并居中，图标和两行说明下移到独立内容行。标题、正文仍分别为 22 px、18 px。
- `Text Adapter` / `Image Adapter` 标题统一为 18 px，图标缩至 28 px 并左移，标题文本框扩至 138 px；两条 backend 说明使用 175 px 内容宽度。两张紫色卡的右侧均保留安全边距，未改变卡片外框，也未增加遮罩或覆盖层。
- 从修正后的 SVG 重新渲染 1600 × 900 PNG，并以原始尺寸检查 Admission、两张 Adapter 卡、紫绿相邻边框及两侧箭头；未发现文字越界、边框重叠或箭头遮字。
