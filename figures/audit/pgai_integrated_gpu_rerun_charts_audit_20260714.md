# pgai-integrated GPU rerun chart audit, 2026-07-14

## Scope

Charts generated from:

```text
motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv
motivation/results/gpu/pgai_integrated_key_rerun_20260714.md
```

Script:

```text
figures/scripts/generate_pgai_integrated_gpu_rerun_charts.py
```

Outputs:

```text
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.png
figures/data/report_main/06_gpu_pgai_rerun_granularity_20260714.svg
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.png
figures/data/report_main/07_gpu_pgai_rerun_stage_writeback_20260714.svg
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.png
figures/data/report_main/08_gpu_pgai_rerun_endpoint_comparison_20260714.svg
```

## Figure roles

| Figure | Type | Main claim |
|---|---|---|
| `06_gpu_pgai_rerun_granularity_20260714` | Experimental result | Fine-grained per-row model calls are much slower than coalesced calls for the same 1024 rows. |
| `07_gpu_pgai_rerun_stage_writeback_20260714` | Experimental result | After GPU model calls are fast, JSON writeback becomes a visible part of end-to-end time. |
| `08_gpu_pgai_rerun_endpoint_comparison_20260714` | Experimental result | Two local endpoint replicas reduce model/operator wall time, while writeback remains similar. |

## Boundary

- Results are PostgreSQL 18.4 local rehearsal facts, not PostgreSQL 18.3 internal platform facts.
- Results are GPU-backed job-table profile facts, not pgai SQL performance facts.
- `8000` and `8001` are two local service replicas on the same RTX 5070, not two GPUs.
- Current writeback is PostgreSQL JSON text for 384-dim embeddings, not pgvector vector(384).
- Each setting has one formal run in this rerun. Use as motivation/profile evidence, not final optimization proof.

## Design audit

- Vector output: pass. SVG files are generated for each chart.
- PNG output: pass. PNG files are generated for report/PPT preview.
- Font size: pass by visual inspection at report scale.
- Color use: pass. Distinct blue/orange/green/purple encodings with legends and labels.
- Axis range: pass. Bar charts start at zero except the granularity chart, which uses a labeled log scale because values differ by 37.5x.
- Chart type: pass. Bar charts match discrete scenario comparisons; stacked bars match stage composition.
- Caption readiness: pass. Each figure has source and boundary note in the plot footer; report text still needs a self-contained caption.
- Chartjunk: pass. No 3D effects, background decoration, or non-data ornament.

## Use in opening report

Use these as the latest opening-report motivation figures before older
2026-07-12 GPU charts. The old figures remain useful historical references, but
the 2026-07-14 rerun reflects the pgai-integrated local environment and should
be cited first.
