# 01 Research Gap — Draw.io reconstruction audit

## Role and content contract

- **Slide role:** audience-facing research-gap and solution-overview figure for opening-defense page 5.
- **Core conclusion:** the research object is the upstream AI Data Execution Layer between Database and Model Service.
- **Content rule:** the center contains research content 1, research content 2, and their shared cost-estimation/state-observation foundation; it does not add a third research content.
- **Audience revision:** the internal research-boundary paragraph and three lock cards were deleted from the source rather than hidden or covered. This audience-facing page now explains only the gap and proposed solution; the sole lower-band object is the green research-solution conclusion.
- **Authority checked:** `opening/claim_matrix.md` §1–§2; `opening/opening_defense_outline_20260808.md` page 5 and page 10; `figures/audit/opening_editable_diagrams_manifest_20260811.md` G1.

## Reference and style tokens

- Reference: `figures/audit/reference_opening_editable_20260811/01_research_gap.png` (1672 × 941).
- Output canvas: 1600 × 900, 16:9, white background.
- Visual grammar: three columns; blue/orange/purple grouping; green conclusion; numbered orange circles; line icons; gray dashed feedback.
- Palette: blue `#165DCC`, orange `#E85D04`, purple `#5B2AA6`, green `#15803D`, gray `#6B7280`.
- Typography: Microsoft YaHei in Draw.io; PingFang/Heiti fallback in SVG/PNG. Every visible label is at least 18 px.

## Visible-element inventory

| ID | Region | Content / visual | Medium | Status |
|---|---|---|---|---|
| T01 | title band | `01` badge, main title, page-role note | native shape + text | accepted |
| P01 | left column | Database-side capability and blind-spot panel | native cards + 3 independent SVG icons | accepted |
| P02 | center column | two research contents plus shared enabler | native cards + 3 independent SVG icons | accepted |
| P03 | right column | text generation, image representation/classification, service-visible state | native cards + 3 independent SVG icons | accepted |
| A01 | left gap | clean blue request arrow with standalone `请求` label | native connector + text | accepted |
| A02 | right gap | clean purple submission arrow with standalone `提交` label | native connector + text | accepted |
| A03 | right gap | gray dashed runtime-state feedback cue | native connector + text | accepted |
| G01 | bottom | one-line solution conclusion | native green card/text | accepted |

## Icon inventory

`assets/01_research_gap/` contains nine independent SVGs: `db_sql.svg`, `query_job.svg`, `sink_semantics.svg`, `work_unit.svg`, `scheduler.svg`, `estimate_observe.svg`, `text_service.svg`, `image_service.svg`, and `gpu_state.svg`. `boundary_lock.svg` was removed with the audience-only boundary band. The SQL asset uses a 22 px source label because the icon is displayed at 66/80 scale, yielding an approximately 18.2 px visible label in the main figure.

## Technical verification

- `check_drawio.py`: passed — 58 cells, 53 vertices, 3 edges, 9 SVG-image cells, 31 text cells.
- `xmllint --noout`: passed for `.drawio`, `.svg`, and private icon SVGs.
- PNG: 1600 × 900 RGB, re-rendered once from a blank canvas using the same geometry and local PingFang/Heiti fallback; it was not patched with cleanup masks.
- Source scan: 58 unique Draw.io cell IDs; no duplicate IDs, hidden cells, invisible objects, embedded full-slide raster, overlay/mask layer, stale label, boundary-lock reference, or visible font below 18 px.
- True-font width audit under the local Chinese fallback: the tightest title/body margins are 16 px (`研究内容二` title), 18 px (right-state first line), 24 px (left body), and 25 px (shared-credit body).
- Arrow audit: request `408→458` and submit `1142→1192` share the same `y=397` centerline, 5 px stroke, compact 16 × 20 px block head, and mirrored 8/12 px panel clearances. Feedback `1188→1142` uses a weaker 2 px dashed stroke and smaller 13 × 16 px left head on the lower outside gap (`y=618`). All labels are standalone above their line; no turn, merge, or branch is present, so no unmatched corner radius is introduced.
- Terminology scan: `图像表征 / 分类` replaces the ambiguous `嵌入 / 分类`; no RC/BL/Phase/P-level internal code names or dynamic-win claim.

## Full-size visual audit

- Inspected at native 1600 × 900 and at a 960 × 540 conservative A4/document-width proxy.
- All text stays inside its card with visible inner padding; no real-font line touches a border or icon.
- Request, submit, and feedback arrows stay in their gaps; labels do not intersect the lines, and arrowheads remain fully visible before the target panels.
- Blue, orange, purple, gray, and green borders are continuous and singly rendered. No line crosses text or a card.
- The internal boundary paragraph, three lock cards, and lock asset are absent from the source. The green research-solution strip is the only lower-band object; there are no hidden alternatives, white masks, stale glyphs, or duplicate borders.
- The source is directly editable in Draw.io; the SVG is vector text/shapes/icons, and the PNG is a clean render from empty canvas rather than a screenshot with corrections painted on top.

**Final status: accepted; no unresolved visual defect.**
