# 01 Research Gap — Draw.io reconstruction audit

## Role and content contract

- **Slide role:** audience-facing research-gap figure for opening-defense page 5.
- **Core question:** database-side task semantics and model-service runtime information are not naturally connected, and multi-job external execution lacks common efficiency, isolation and fairness constraints.
- **Narrative boundary:** this page states the gap only. It does not preview the project's Work-unit/Packing, Admission/Routing, Shared Credit or other proposed mechanisms, and it does not contain the internal “research boundary” lock band.
- **Authority checked:** `opening/claim_matrix.md` and the project-level scope in `AGENTS.md`.

## Reference and style tokens

- Reference: `figures/audit/reference_opening_editable_20260811/01_research_gap.png` only for three-column composition and visual grammar.
- Output canvas: 1600 × 900, 16:9, white background.
- Palette: database blue `#165DCC`, gap orange `#E85D04`, service purple `#5B2AA6`, conclusion green `#15803D`.
- Typography after the 2026-08-17 readability revision: title 42 px, section headings 30 px, card titles 25 px, body/labels 20 px, conclusion 26 px. No visible text is below 20 px.

## Visible-element inventory

| ID | Region | Content / visual | Medium | Status |
|---|---|---|---|---|
| T01 | title band | `01` badge, main title, page-role note | native shape + text | accepted |
| P01 | left column | Database-side capability and blind-spot panel | native cards + 3 independent SVG icons | accepted |
| P02 | center column | three unresolved cross-layer gaps | native cards + 3 independent SVG icons | accepted |
| P03 | right column | text generation, image representation/classification, service-visible state | native cards + 3 independent SVG icons | accepted |
| A01 | left gap | blue request arrow with standalone `请求` label | native connector + text | accepted |
| A02 | right gap | purple submission arrow with standalone `提交` label | native connector + text | accepted |
| A03 | right gap | gray dashed runtime-state feedback cue | native connector + text | accepted |
| G01 | bottom | one-line research-question conclusion | native green card/text | accepted |

The center cards are: `工作量表达不一致`, `运行状态难以对应作业`, and `多作业缺少共同约束`. They describe the problem without committing to a later implementation.

## Icon inventory

`assets/01_research_gap/` contains nine independent SVGs: `db_sql.svg`, `query_job.svg`, `sink_semantics.svg`, `work_unit.svg`, `estimate_observe.svg`, `scheduler.svg`, `text_service.svg`, `image_service.svg`, and `gpu_state.svg`. Icons are semantic aids only; all explanatory wording remains native editable text.

## Technical verification

- `check_drawio.py`: passed — 58 cells, 53 vertices, 3 edges, 9 SVG-image cells, 31 text cells.
- Draw.io, main SVG, curated SVG and private icon SVGs pass XML validation.
- Draw.io CLI is unavailable on this machine; PNG is 1600 × 900 RGBA, rendered from the asset-embedded SVG with the same geometry. It was not patched with masks.
- Source scan: unique cell IDs; no hidden cell, duplicate border, full-slide raster, overlay/mask layer, stale boundary-lock object or visible font below 20 px.
- Arrow audit: request and submit use fixed-size 14 × 12 px heads (`markerUnits=userSpaceOnUse`), matched 4 px strokes and 60 px paths from 5 px outside the source panel to 5 px before the target panel. Their visible line bodies remain about 47 px instead of being consumed by stroke-scaled markers. The feedback cue uses a fixed 11 × 10 px head, 2.5 px dashed stroke and the same 60 px gap path, so it remains visually subordinate but clearly directional.
- Terminology audit: `图像表征 / 分类` replaces ambiguous `嵌入 / 分类`; proposed mechanism terms are absent from the gap cards and conclusion.

## Full-size visual audit

- Inspected at native 1600 × 900 and at 960 × 540 document-width proxy.
- All text remains inside its card with visible padding; no line crosses text or a card border.
- The 42/30/25/20/26 px hierarchy remains readable after PPT/document scaling; enlarged body text does not wrap unexpectedly or touch icons and borders.
- Both colored inter-panel arrows show a long, continuous line plus a compact head; the dashed feedback arrow also has a visible line body and unambiguous leftward direction.
- Blue, orange, purple, gray and green borders are continuous and singly rendered.
- The internal boundary paragraph and lock cards are absent from the source, not hidden or covered.

**Final status: accepted; no unresolved visual defect.**

## 2026-08-23 报告用语修正

- 权威图继续采用数据库侧、三项跨层能力、模型服务侧三列结构；报告中此前使用的是较旧副本，本轮已重新同步。
- 三项能力改为“按 AI 处理需求描述工作量”“把运行状态与作业进度联系起来”“共同安排多个数据库作业”，并在卡片内写出各自动作和评价对象。
- 数据库侧和模型服务侧的英文简写已改为中文说明；KV 首次直接写成 KV 缓存，作业和服务目标不再只写 Job / SLO。
- 1600×900 PNG 以原始尺寸检查，左右两列的长句已经分行，没有裁切或越出卡片。
