# 04 State-aware Scheduling — reconstruction audit

Reference: `figures/audit/reference_opening_editable_20260811/04_state_aware_scheduling.png`
Target canvas: 1600 × 900, 16:9, white background
Role: opening-defense mechanism figure for pages 14–15, with page-19 validation boundary.

## Content contract

- Main chain: Job input → Admission / safe-capacity calibration → shared request/work credit → fair queue → state-aware endpoint routing → text/image execution backends.
- Completion closes the loop through credit release and request-level refill.
- `RuntimeStateSnapshot` feeds admission, credit and routing through gray dashed feedback paths.
- Dynamic actions are explicitly labeled as a candidate and “待同上限 A/B 验证”; no visual or wording implies that dynamic control already beats frozen strong static.
- Static capacity is calibrated first and tied to machine/model/protocol/workload signature.

## Visible-element inventory

| ID | Region / approximate bbox | Content / visual description | Medium | Style notes | Status |
|---|---|---|---|---|---|
| I01 | 0,0,1600,900 | White 16:9 canvas | native | no gradient, texture or shadow | accepted |
| I02 | 30,18,78,54 | Blue page-number badge “04” | native | #165DCC, white 34 pt bold | accepted |
| I03 | 130,15,1430,60 | Main title | native | black 36 pt bold, left aligned | accepted |
| I04 | 360,82,880,84 | Candidate/validation callout | native | orange dashed border, two-line hierarchy | accepted |
| I05 | 30,215,205,350 | Multi-Job input panel | native + native queue cells | blue outline; Job A/B/N rows | accepted |
| I06 | 265,215,205,350 | Admission panel | native + SVG icon | orange; safe capacity calibration and frozen-static fallback | accepted |
| I07 | 490,215,245,350 | Shared request/work credit panel | native + two SVG icons | orange; request-slot ceiling and work-budget ceiling icons | accepted |
| I08 | 755,215,220,350 | Fair Queue panel | native + native queue cells | orange; per-job floor/cap, work-fair deficit, idle borrowing | accepted |
| I09 | 1005,215,235,350 | State-aware endpoint routing panel | native + SVG icon | orange; ready/active/queued, service/KV/queue signals | accepted |
| I10 | 1290,175,280,170 | Text vLLM backend | native + SVG icon | purple; text completion path | accepted |
| I11 | 1290,395,280,170 | Image typed GPU actor backend | native + SVG icon | purple; image embedding/classification path | accepted |
| I12 | 430,625,300,120 | Request-level refill card | native + SVG icon | green; refill after completion | accepted |
| I13 | 790,625,420,120 | Completion & release card | native + SVG icon | green; release request/work credit and update completed/remaining work | accepted |
| I14 | 1235,610,335,145 | RuntimeStateSnapshot panel | native + SVG icon | gray; ready/active/queued, service rate, queue age, KV/GPU, freshness/signature | accepted |
| I15 | main row | Blue request/data connectors | native | solid 5 px, block arrowheads | accepted |
| I16 | right execution branch | Purple execution connectors | native | solid 4 px, branched to text/image backends | accepted |
| I17 | bottom loop | Green completion/release/refill connectors | native | solid 4 px, completion → release → refill → credit pool | accepted |
| I18 | runtime feedback | Gray dashed feedback paths | native | 3 px dashed, explicit source/targets, arrowheads | accepted |
| I19 | 35,790,1530,84 | Four-color flow legend | native | blue/orange/purple/green + gray dashed | accepted |
| I20 | icons | clipboard, request slots, work-budget bars, router, text, image GPU, completion, refill, snapshot | separate SVG files + separate Draw.io image objects | clean 64×64 linear family, matching stroke proportions | accepted |

## Arrow inventory

| ID | Source → target | Type / role | Planned geometry | Status |
|---|---|---|---|---|
| A01 | Job input → Admission | solid blue request flow | horizontal centerline | accepted |
| A02 | Admission → Shared Credit | solid orange control flow | horizontal centerline | accepted |
| A03 | Shared Credit → Fair Queue | solid orange control flow | horizontal centerline | accepted |
| A04 | Fair Queue → Router | solid orange control flow | horizontal centerline | accepted |
| A05 | Router → Text/Image backends | solid purple execution flow | short trunk + two branches | accepted |
| A06 | Text/Image completion → Completion & Release | solid green completion flow | converge on release card without crossing backend text | accepted |
| A07 | Release → Refill → Shared Credit | solid green loop | leftward then upward | accepted |
| A08 | RuntimeStateSnapshot → Router | gray dashed feedback | upward into router bottom | accepted |
| A09 | RuntimeStateSnapshot → Candidate controller | gray dashed feedback | right-edge return path, outside backend cards | accepted |
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

- Split the single GPU box into Text vLLM and Image typed Ray GPU actor cards so the cross-modal execution boundary is explicit.
- Replaced the old generic state-aware credit banner with the current contract: calibrate/freeze the strong static cap first; dynamic admission/routing is a candidate awaiting matched-cap A/B.
- Expanded the snapshot to the required signals (`ready/active/queued`, service rate, queue age, KV/GPU, freshness/signature) and routed feedback outside the backend cards.
- Added `per-job floor/cap`, work-fair deficit and idle borrowing as isolation/work-conservation mechanisms without implying a final algorithm.

## Large-type compression revision

- Admission is compressed to three lines: capacity calibration, frozen strong-static ceiling and static fallback.
- Shared Credit retains only Request Credit, Work Credit and same-cap/completion-release semantics.
- Fair Queue retains only `per-job floor/cap`, `work-fair deficit` and `idle borrowing`.
- Router retains three short state lines; Text and Image executors each retain two body lines.
- Runtime Snapshot, Completion/Release and Request Refill each use two body lines.
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
