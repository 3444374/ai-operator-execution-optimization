# 04 State-aware Scheduling — reconstruction audit

Reference: `figures/audit/reference_opening_editable_20260811/04_state_aware_scheduling.png`
Target canvas: 1600 × 900, 16:9, white background
Role: opening-defense mechanism figure for pages 14–15, with page-19 validation boundary.

## Content contract

- Main chain: 作业入口 → 提交前容量检查 → 共享容量记录 → 多作业队列选择 → 状态感知的服务实例路由 → 文本 / 图像模型执行。
- 完成反馈通过“完成与释放”和“请求位置补充”闭合容量循环。
- “运行状态记录”通过灰色虚线向容量检查、容量记录和路由提供反馈。
- Dynamic actions are explicitly labeled as a candidate and “待同上限 A/B 验证”; no visual or wording implies that dynamic control already beats frozen strong static.
- Static capacity is calibrated first and tied to machine/model/protocol/workload signature.

## Visible-element inventory

| ID | Region / approximate bbox | Content / visual description | Medium | Style notes | Status |
|---|---|---|---|---|---|
| I01 | 0,0,1600,900 | White 16:9 canvas | native | no gradient, texture or shadow | accepted |
| I02 | 30,18,78,54 | Blue page-number badge “04” | native | #165DCC, white 34 pt bold | accepted |
| I03 | 130,15,1430,60 | Main title | native | black 36 pt bold, left aligned | accepted |
| I04 | 360,82,880,84 | Candidate/validation callout | native | orange dashed border, two-line hierarchy | accepted |
| I05 | 30,215,205,350 | 多作业输入框 | native + native queue cells | blue outline; 作业 A/B/N rows | accepted |
| I06 | 265,215,205,350 | Admission panel | native + SVG icon | orange; safe capacity calibration and frozen-static fallback | accepted |
| I07 | 490,215,245,350 | 共享容量记录框 | native + two SVG icons | orange; 请求位置数量上限与工作量空间计算量上限 | accepted |
| I08 | 755,215,220,350 | 多作业队列选择框 | native + native queue cells | orange; 作业保底量 / 上限、按工作量记录差额、空闲容量借用 | accepted |
| I09 | 1005,215,235,350 | 状态感知的服务实例路由框 | native + SVG icon | orange; 就绪 / 排队、处理速率、KV / GPU / SLO | accepted |
| I10 | 1290,175,280,170 | 文本模型执行框 | native + SVG icon | purple; vLLM AI_COMPLETE path | accepted |
| I11 | 1290,395,280,170 | 图像模型执行框 | native + SVG icon | purple; Ray GPU 有状态执行单元，图像嵌入 / 分类 | accepted |
| I12 | 430,625,300,120 | 请求位置补充卡片 | native + SVG icon | green; 请求完成后立即补位 | accepted |
| I13 | 790,625,420,120 | 完成与释放卡片 | native + SVG icon | green; 释放请求位置与工作量空间并更新剩余量 | accepted |
| I14 | 1235,610,335,145 | 运行状态记录框 | native + SVG icon | gray; 就绪 / 运行中 / 排队中、处理速率、等待时长、KV / GPU | accepted |
| I15 | main row | Blue request/data connectors | native | solid 5 px, block arrowheads | accepted |
| I16 | right execution branch | Purple execution connectors | native | solid 4 px, branched to text/image backends | accepted |
| I17 | bottom loop | Green completion/release/refill connectors | native | solid 4 px, completion → release → refill → credit pool | accepted |
| I18 | runtime feedback | Gray dashed feedback paths | native | 3 px dashed, explicit source/targets, arrowheads | accepted |
| I19 | 35,790,1530,84 | Four-color flow legend | native | blue/orange/purple/green + gray dashed | accepted |
| I20 | icons | clipboard, request slots, work-budget bars, router, text, image GPU, completion, refill, snapshot | separate SVG files + separate Draw.io image objects | clean 64×64 linear family, matching stroke proportions | accepted |

## Arrow inventory

| ID | Source → target | Type / role | Planned geometry | Status |
|---|---|---|---|---|
| A01 | 作业入口 → 容量检查 | solid blue request flow | horizontal centerline | accepted |
| A02 | 容量检查 → 共享容量记录 | solid orange control flow | horizontal centerline | accepted |
| A03 | 共享容量记录 → 多作业队列选择 | solid orange control flow | horizontal centerline | accepted |
| A04 | 多作业队列选择 → 服务实例路由 | solid orange control flow | horizontal centerline | accepted |
| A05 | 路由 → 文本 / 图像模型执行 | solid purple execution flow | short trunk + two branches | accepted |
| A06 | 模型执行完成 → 完成与释放 | solid green completion flow | converge on release card without crossing backend text | accepted |
| A07 | 完成与释放 → 请求位置补充 → 共享容量记录 | solid green loop | leftward then upward | accepted |
| A08 | 运行状态记录 → 路由 | gray dashed feedback | upward into router bottom | accepted |
| A09 | 运行状态记录 → 候选控制方法 | gray dashed feedback | right-edge return path, outside backend cards | accepted |
| A10 | Candidate controller → Admission/Credit | orange control | downward branch to credit-control region | accepted |

## Typography / layout tokens

- Title: Microsoft YaHei / STHeiti fallback, 39 px bold.
- Section/step titles: 24–25 px bold; snapshot label 24 px.
- Body and legend: 18 px minimum; no text cell below 18 px.
- Colors: blue `#165DCC`, orange `#E85D04`, purple `#5B2AA6`, green `#15803D`, observation gray `#6B7280`.
- Cards: white fill, 2 px colored stroke, 10–14 px corner radius, no shadow.
- Connectors: orthogonal, rounded where needed; feedback only is dashed.

## Verification log

- Reference inventory: completed before reconstruction; all 20 visible-element items accepted.
- `check_drawio.py`: passed after the large-type and route-cleanup revisions — 116 cells, 95 vertices, 19 native edges, 9 independent SVG image cells, 40 text cells; no containment, duplicate-payload or SVG-encoding failures.
- `export_drawio.py`: executed and returned `draw.io CLI not found` (exit 1). No Draw.io executable is installed on this machine.
- PNG renderer: passed. The 1600 × 900 PNG was rendered from the same-coordinate SVG with local headless Chrome, so actual Chinese font metrics and all external icon assets were checked together.
- SVG validity: `xmllint --noout` passed. The SVG contains native text, paths and independent icon references; it does not embed a full-slide raster or use a repair overlay.
- Draw.io XML validity: `xmllint --noout` passed.
- Full-size visual audit: completed against the 1672 × 941 reference at original size. Titles, card containment, all icon roles, line routes, arrow endpoints, flow colors, feedback dashes and footer legend were reviewed.
- PPT readability audit: downscaled the shipped PNG from 1600 × 900 to 1280 × 720 and reviewed at original size. Main title, step titles, three-line control cards, two-line executor cards, completion/refill cards, snapshot and legend remain readable; no connector crosses text.
- A4 document readability audit: downscaled to a conservative 1000 × 562 document-column preview. The 18 px minimum body/legend contract remains legible; details were deleted rather than reduced below the font floor.
- Font audit: parsed every Draw.io `fontSize`; minimum 18 px, maximum 39 px, and no values below 18 px.
- Claim-boundary scan: no RC/BL/Phase/P-level internal labels, historical 45.7%/6.4× claims, “global optimum”, or dynamic-win wording remains.

## Deliberate deviations from the old reference

- Split the single GPU box into “文本模型执行”和“图像模型执行”两张卡片，使跨模态执行边界清晰；图像框明确写为“Ray GPU 有状态执行单元”。
- Replaced the old generic state-aware credit banner with the current contract: calibrate/freeze the strong static cap first; dynamic admission/routing is a candidate awaiting matched-cap A/B.
- Expanded the snapshot to the required signals（就绪 / 运行中 / 排队中、处理速率、等待时长、KV / GPU），并将反馈路径布置在执行框外侧。
- Added 作业保底量 / 上限、按工作量记录差额和空闲容量借用，不暗示已经确定最终算法。

## Large-type compression revision

- Admission is compressed to three lines: capacity calibration, frozen strong-static ceiling and static fallback.
- “共享容量记录”只保留请求位置、工作量空间以及同上限 / 完成释放语义。
- “多作业队列选择”只保留作业保底量 / 上限、按工作量记录差额和空闲容量借用。
- Router retains three short state lines; Text and Image executors each retain two body lines.
- 运行状态记录、完成与释放、请求位置补充均使用两至三行短说明。
- Router and snapshot wording was shortened a second time to preserve at least 12 px right-side padding without reducing the 18 px body font.
- Text and image completion branches now meet at one explicit merge point and enter `Completion & Release` through one green arrow. The former coincident green segments and duplicate arrowheads were deleted; the gray feedback path was moved to a separate lane.

## Remaining risks

- Draw.io-native CLI export could not be run locally because the application/CLI is absent. The `.drawio` passes structural checks, and the browser-rendered PNG plus direct SVG geometry were visually audited; a final one-click export in diagrams.net/Draw.io is still recommended before inserting into the final PPT.

## Boundary-level re-audit

- All visible text was rechecked at 1600 × 900 and at 1000 × 562 document scale: no title, card body, executor label, snapshot field or legend item crosses its assigned card or touches a border.
- Every structural arrow has one readable head at the target boundary. Blue request, orange control, purple execution, green completion and gray dashed observation paths remain distinguishable by both color and line style.
- Main flows now share a 4 px visual weight and a restrained 20 × 16 px head; feedback is reduced to 2.5 px with a smaller 16 × 14 px head. Draw.io `endSize` values and the footer legend use the same hierarchy.
- The two executor completion branches converge before entering `Completion & Release`; no coincident segment or duplicate arrowhead remains.
- The gray snapshot feedback lane is separated from the green completion lane, and the outer feedback return stays outside executor text and borders.
- No full-slide raster, repair overlay, mask, exact duplicate native object or border-covering patch is present.

## Shared Credit containment and icon revision (2026-08-11)

- The overflow was verified geometrically: the rendered `Request Credit` label is about 130 px wide, while its former Draw.io text cell was only 108 px wide.
- The Shared Credit panel was rebuilt from `x=500, w=225` to `x=490, w=245`, preserving the original center at `x=612.5`. Both inner cards were widened from 185 px to 215 px; the two text cells are now 142 px wide, so the longest label retains visible horizontal padding at the actual 18 px font.
- The adjacent Admission → Credit and Credit → Fair Queue arrows were shortened symmetrically to 20 px. Their centerline, stroke width, arrowhead size and target-boundary spacing remain matched.
- The former coin-stack icon was deleted and replaced by two request-slot rows under one explicit upper-limit bracket. The former speedometer icon was deleted and replaced by work bars under a single upper-limit line. The new icons express quota semantics without implying money or execution speed.
- Both replacements remain independent 64×64 SVG assets and independent Draw.io image cells. No old icon object, white cover, mask or patch layer remains.
- Final 1600×900 Chrome render was inspected at original size: panel title, both credit labels, bottom release note, icon strokes, four panel borders and all connecting arrows are contained and unobstructed. Unresolved items: none.

## Short-arrow proportion repair (2026-08-11)

- The two 20 px orange inter-panel connectors formerly used a 20×16 px arrowhead, leaving no readable shaft. They now use a dedicated 12×12 px compact head while retaining the 4 px control-flow stroke and exact horizontal centerline.
- The green Refill → Shared Credit route now bends at the vertical midpoint of the 60 px gap (`y=595`) and uses the matching compact green head. Its final vertical segment is 30 px, leaving a clearly visible shaft before the head.
- The long control, completion and legend arrows retain their original larger heads; compact heads are used only where the available connector run is short. This preserves the intended flow hierarchy without making the whole figure visually weak.

## 对外文字中文化复核（2026-08-24）

- 可见标题已改为“共享容量记录”“多作业队列选择”“文本模型执行”“图像模型执行”“请求位置补充”“完成与释放”和“运行状态记录”。
- `Request Credit` / `Work Credit` 分别改为“请求位置 / 数量上限”和“工作量空间 / 计算量上限”；`typed Ray actor` 改为“Ray GPU 有状态执行单元”。
- 作业名称统一为“作业 A / B / N”；路由与运行状态字段统一使用“就绪、运行中、排队中、处理速率、排队 / 等待时长”。
- 公平队列说明改为“作业保底量 / 上限、按工作量记录差额、空闲容量可借用”，完成反馈说明改为“释放请求位置与工作量空间、按实际工作量更新剩余量”。
- Draw.io 和 SVG 均通过 XML 解析；SVG 未命中上述旧英文可见标签。1600×900 PNG 已从同坐标 SVG 重新导出并按原尺寸检查，模块文字、连线和图例无裁切或遮挡。
