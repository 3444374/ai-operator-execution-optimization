# Superseded Output-aware BFD Gate

This 64-row gate was executed before sequential token-budget packing adopted
the same `ray_batch_rows` hard cap as BFD. Its lifecycle and resource traces
remain valid infrastructure evidence, but its cross-algorithm batch membership
is not controlled and it is excluded from performance comparisons.

Use `../output_aware_bfd_gate_v2_20260726/` for the corrected gate.
