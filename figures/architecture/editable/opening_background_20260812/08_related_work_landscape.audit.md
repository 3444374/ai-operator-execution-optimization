# 08 Related Work Landscape — reconstruction audit

## Scope and evidence

- Reference: `figures/audit/reference_opening_background_20260812/08_related_work_landscape.png` (1672 × 941).
- Output canvas: 1600 × 900, white background, 16:9.
- Local evidence checked before wording changes:
  - `research/ai_operator_literature_inventory.md`
  - `research/inference_pipeline_interaction_literature.md`
  - `opening/literature/top15_reading_notes/` notes for LOTUS, Galois, GaussML, Orca, vLLM, Sarathi-Serve, VTC, GRACEFUL, COSTREAM and Abacus
  - `research/reading_notes/cortex_aisql_sigmod2026.md`
- No web evidence and no commercial logos are used.
- Intentional content correction: VTC is described as service-counter fairness; GRACEFUL as UDF execution-time estimation; COSTREAM as streaming operator-placement cost estimation; Abacus as multi-objective Pareto plan search. These replace unsupported admission/routing-like labels in the reference.

## Visible-element inventory

| id | bbox / region | content / visual | medium | style notes | status |
|---|---|---|---|---|---|
| canvas | 0,0,1600,900 | white 16:9 canvas | native | no grid, shadow, texture or hidden background image | accepted |
| title | 160,18,1280,64 | related-work layered framing | native text | 42 px bold, centered, dark gray | accepted |
| db-panel | 30,100,370,655 | database-AI column | native | blue 2 px border, very light blue fill, 16 px radius | accepted |
| data-panel | 420,100,370,655 | data-execution column | native | matches db-panel | accepted |
| service-panel | 810,100,370,655 | inference-service column | native | matches db-panel | accepted |
| cost-panel | 1200,100,370,655 | cost-and-decision column | native | matches db-panel | accepted |
| headers | y=112..158 | four column headings | native text | 29 px bold blue | accepted |
| db-cards | x=48, four cards | LOTUS, Galois, GaussML, Cortex AISQL + two labels each | native card + text | names 27 px; labels 22 px; no small pills | accepted |
| data-cards | x=438, three cards | Ray Data, Daft, NeuStream + two labels each | native card + text | names 27 px; labels 22 px; taller cards | accepted |
| service-cards | x=828, four cards | Orca, vLLM, Sarathi-Serve, VTC + two labels each | native card + text | names 27 px; labels 22 px | accepted |
| cost-cards | x=1218, four cards | Learned Cost Models, GRACEFUL, COSTREAM, Abacus + two labels each | native card + text | names 27 px; labels 22 px | accepted |
| focus-lines | y=702 | four dividers | native line | 1.5 px blue | accepted |
| focus-text | y=710..744 | four layer-focus summaries | native text | 21 px bold blue; visible at 900 px-wide preview | accepted |
| conclusion | 30,783,1540,82 | cautious two-line separation statement | native card + text | orange 2.5 px border, 29 px bold; 30 px left/right and 35 px bottom canvas clearance | accepted |
| icons/logos | none | no icons or commercial logos in this classification figure | none | omission is deliberate; reduces unsupported visual branding and preserves PPT readability | accepted |

## Arrow inventory

None. This is a categorical landscape, not a causal or process flow. No arrows, connectors, loops, arrowheads or dashed feedback paths are present in the reference reconstruction. The four columns are peers rather than a left-to-right sequence.

## Technical checks

- `check_drawio.py`: pass — 37 cells, 35 vertices, 0 edges, 0 image/SVG cells.
- XML: parsed by `check_drawio.py`; native Draw.io text/cards/lines only.
- Export: SVG rendered through local headless Chrome at exactly 1600 × 900 to `08_related_work_landscape.png`.
- Full-size visual inspection: accepted. No text clips, no overlapping cards, no crossed or duplicate borders, no masked corrections, and no hidden stale layer.
- PPT-scale inspection: resized to 900 × 506 and visually checked. System names and both label lines remain distinguishable; four columns remain separated; the conclusion is readable. No text touches a card edge.
- Draw.io source and standalone SVG carry the same wording, geometry, colors, and font hierarchy.

## Resolved defects from the first render

- Removed all side-by-side label capsules after full-size rendering showed long English and Chinese labels pressing into adjacent capsules.
- Replaced them with two centered 22 px text lines per system card; no font was reduced below the requested floor.
- Preserved at least 16 px horizontal text clearance inside every card; system cards remain within their parent columns.
- Reworded the title and bottom statement to keep the page at the related-work layer: representative systems optimize database semantics, data pipelines, inference services or cost decisions separately; the connection between database task semantics and model-service runtime information remains insufficient. The figure does not introduce the project's solution vocabulary.

## Final status

All visible-inventory items are **accepted**. There are no unresolved visual, semantic, boundary, overlap, layering, arrow or export defects.
