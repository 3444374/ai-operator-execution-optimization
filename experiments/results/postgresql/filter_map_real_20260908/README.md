# Filter→Map real-model smoke

## Purpose and setup

This verifies the PostgreSQL execution foundation for subsequent data organization and scheduling work,
using the [composition design](../../../plans/postgresql_call_binding_design.md#12-2026-09-08真实组合小规模验证).
Source `e84d308d` contains the previously verified Filter→Map implementation; its PG library SHA-256
was checked against the earlier strict-build artifact. No production PG code changed in this run.

A fresh PostgreSQL 18.3 cluster and one localhost vLLM service ran on an otherwise idle RTX 4090.
Nine model/config/tokenizer files were rehashed against the prior verified Qwen/Qwen2.5-7B-Instruct
revision `a09a35458c702b33eeacc393d103063234e8bc28`. Runtime versions: vLLM 0.25.1, torch 2.11.0,
transformers 5.14.1. The model used BF16, eager execution, length 4096, maximum four sequences,
4096 batched tokens, FCFS, one GPU at memory utilization 0.8 and prefix caching disabled. These are
smoke settings, not a calibrated performance configuration. Core/text runtime preflight passed;
existing assets and separate driver/model environments were reused. No download or installation occurred.

## Method and result

The existing choice_gateway_observer and AttemptLedger enforce a fresh limit of 12 POST attempts.
Four synthetic rows exercise positive/negative Filter decisions, a NULL Map input and a NULL Filter
input. Filter temperature is 0 with maximum 8 output tokens; Map temperature is 0 with maximum 128.
Each phase issues three Filter requests and one Map request. LIMIT zero, a NULL-only Filter input and
EXPLAIN issue none. An independent database connection audits committed INSERT output.

| Phase | Observed result |
|---|---|
| Filter wire v3 → Map SELECT | 4 requests, output/NULL and response identity/usage checks pass |
| Filter wire v4 → Map SELECT | 4 requests; structured choices appear only on Filter requests |
| Filter wire v4 → Map INSERT | 4 requests; two plan nodes report 3 and 1 model calls; independent audit matches |
| Cleanup | PG and gateway exit; model service exits, port closes, both GPUs return to 1 MiB |

All three phases pass: [run summary](raw/run-summary.json), [HTTP evidence](raw/run-http-events.jsonl),
[session evidence](raw/run-sessions.jsonl), [durable budget](raw/prepare-budget.jsonl),
[model identity](raw/prepare-model-verification.json), [controller](raw/prepare-controller-summary.json).
The fixture target is pipeline correctness: original columns remain intact and Map bytes equal the raw
model response. This is not a model quality evaluation, performance experiment, formal resource run,
or a test of the new incremental session backend. The later five-request incremental test uses a
separate ledger and leaves this 12-request ledger unchanged.

The private controller runs `launch_vllm_with_identity.py`, then the bounded driver through
`isolated_pg18_cluster` and the public observer CLI. Controller/driver hashes and source identity are
recorded in [source provenance](raw/source-identity.json); machine settings remain outside Git.
Every request is counted, there was no retry, and original logs remain in private artifacts. Public
logs are redacted/whitespace-normalized with [hashes](raw/artifact-index.json).
