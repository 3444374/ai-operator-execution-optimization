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
- Every visible label is at least 18 px.

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
- PNG: 1600 × 900 RGBA, rendered from the clean vector geometry rather than patched with masks.
- Source scan: unique cell IDs; no hidden cell, duplicate border, full-slide raster, overlay/mask layer, stale boundary-lock object or visible font below 18 px.
- Arrow audit: request and submit arrows have matched centerlines, line widths, head sizes and panel clearances; the feedback cue is thinner, dashed and visually subordinate.
- Terminology audit: `图像表征 / 分类` replaces ambiguous `嵌入 / 分类`; proposed mechanism terms are absent from the gap cards and conclusion.

## Full-size visual audit

- Inspected at native 1600 × 900 and at 960 × 540 document-width proxy.
- All text remains inside its card with visible padding; no line crosses text or a card border.
- Blue, orange, purple, gray and green borders are continuous and singly rendered.
- The internal boundary paragraph and lock cards are absent from the source, not hidden or covered.

**Final status: accepted; no unresolved visual defect.**
