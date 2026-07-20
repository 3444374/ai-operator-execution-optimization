# Data Organization Strategy Mechanism Figure Audit

Generated: 2026-07-20

Revision note, 2026-07-20:

- Redesigned `data_organization_token_budget_mechanism.*` after visual review.
  The original token-budget panel placed labels near bar ends and used notes
  inside tight card boundaries, which caused text collisions around `row3`,
  total-token notes, and the over-budget note. The revised panel separates row
  labels from token bars on the fixed-row side and uses two explicit submission
  cards on the token-budget side.
- Revised `data_organization_length_align_mechanism.*` section labels after
  PPT review. The left/right panel headers previously mixed bold Chinese text
  with inline English terms such as `batch`, which looked uneven after PNG
  scaling in PowerPoint. The revised labels use compact Chinese mechanism
  wording while keeping the technical meaning unchanged.

Figure files:

```text
figures/architecture/data_organization_token_budget_mechanism.png
figures/architecture/data_organization_token_budget_mechanism.svg
figures/architecture/data_organization_length_align_mechanism.png
figures/architecture/data_organization_length_align_mechanism.svg
figures/architecture/data_organization_prefix_aware_mechanism.png
figures/architecture/data_organization_prefix_aware_mechanism.svg
figures/scripts/generate_data_organization_strategy_mechanism.py
```

## Role

Type: Solution Overview / mechanism mini-figures.

These three figures explain the candidate data-organization policies in
research content one. They should be used as a small mechanism series under the
method section, not as experimental result figures. Their claim is deliberately
bounded: each figure explains how an upstream request shape is constructed; it
does not claim the policy has already improved throughput, tail latency, or
prefix-cache hit rate.

## Figure Contract

Core conclusion:

> Fixed row count is an imprecise proxy for AI_COMPLETE request cost; upstream
> data organization should expose token budget, token-length similarity, and
> prefix locality as controllable request-shaping policies.

Evidence chain:

| Figure | Unique role | Required validation later |
|---|---|---|
| `data_organization_token_budget_mechanism.*` | Shows how fixed rows are converted into token-budget submissions | Token P95, queue wait, tokens/s, E2E latency |
| `data_organization_length_align_mechanism.*` | Shows how sorting/grouping reduces within-batch token variance | Service P95/P99, straggler gap, tokens/s |
| `data_organization_prefix_aware_mechanism.*` | Shows how shared system prompts are grouped to create prefix locality | vLLM APC/prefix cache metrics and E2E effect |

Archetype: schematic-led composite, split into three single-purpose panels.

Export contract: PNG for preview/PPT, SVG for editable/vector report use.

## Design Decisions

- Replaced `rc1_*` visible naming with full formal names:
  `data_organization_*_mechanism`.
- Used Chinese visible labels to match the opening report and future thesis
  prose; retained only necessary technical tokens such as `token`, `batch_size`,
  `prefix`, and `system prompt`.
- Replaced strong causal wording such as "reuse KV-cache" with bounded wording:
  "create prefix locality" and "cache-hit condition".
- Kept each figure to one mechanism rather than combining all policies into a
  dense rule table.
- Used a restrained blue/orange/green/red palette consistent with the existing
  architecture figures.

## Quality Audit

| Check | Result | Notes |
|---|---|---|
| Vector format | Pass | SVG files generated alongside PNG previews |
| Font size | Pass | Labels remain legible in PNG preview |
| Self-contained caption | Pass | Each figure includes a one-sentence suggested caption and validation boundary |
| Colour-blind safety | Pass with caveat | Colour is paired with position and labels; not colour-only |
| No chartjunk | Pass | No 3D, shadows, gradients, or decorative backgrounds |
| Forbidden visible tokens | Pass | New SVGs do not contain `RC1`, `RC2`, `RC3`, `BL1`, `BL2`, `边界确认`, `Workload 入口`, or `Ours-v0` |
| Claim boundary | Pass | Figures state candidate mechanisms, not proven gains |

## Use Guidance

Use the three new `data_organization_*_mechanism.*` figures when the report or
PPT needs to explain the internal design of research content one. Keep
`runtime_strategy_control_loop.*` as the higher-level strategy-loop figure.

The older `rc1_*` files are internal drafts and should not be cited in formal
materials unless they are explicitly archived or renamed later.
