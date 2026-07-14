# pgvector(384) Writeback Chart Audit, 2026-07-14

## Data Source

```text
motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv
motivation/results/gpu/pgvector_writeback_20260714.md
```

Only `phase=formal` rows are plotted. Warm-up rows are excluded.

## Generated Figures

```text
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png
figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.svg
```

## Design Role

Experimental-results figure.

The figure compares no writeback, JSON text writeback, and pgvector
`vector(384)` writeback in the same GPU-backed Ray actor chain. It intentionally
shows model request wall time and operator wall time beside writeback so the
reader does not interpret the result as a model-service speedup.

## Audit

| Check | Result | Note |
|---|---|---|
| Traceable to CSV | Pass | Generated directly from `ai_embed_pgvector_writeback_20260714.csv`. |
| Warm-up excluded | Pass | Script filters `phase == formal`. |
| Vector output | Pass | PNG and SVG are generated. |
| Axis honesty | Pass | Y-axis starts at 0. |
| Caption/boundary | Pass | Footer states PG18.4 local rehearsal and not PG18.3. |
| No chartjunk | Pass | Plain grouped bars, no 3D effects or decorative elements. |
| No overclaim | Pass | Title says sink stage changes, not GPU operator. |

## Boundary

This figure supports a local experiment fact: in this PG18.4 rehearsal, JSON
text writeback took more time than pgvector `vector(384)` writeback for 4096
rows on the same GPU-backed Ray actor chain. It does not prove PostgreSQL 18.3
performance, multi-node behavior, or a final writeback optimization.
