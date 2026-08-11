# Two-Job phase-change state-aware experiment runbook

This is a **project-derived VTC-shape workload**, not an official VTC
reproduction and not a Daft/Ray-native baseline. Its sole question is whether
the project controller can move between two offline-calibrated capacity arms
when a second, long Job turns on and off.

The hard stop order is:

```text
environment + immutable workload
  -> A-only lower/upper calibration
  -> A+B pressure/relief calibration
  -> adaptive up/down/up/down action gate
  -> frozen-lower / frozen-upper / adaptive formal
```

Never start a later step after an audit exits non-zero. Keep every stopped or
failed directory; do not resume it into a different rate or capacity contract.

## 1. Fixed identity and runtime preparation

Use an isolated clean worktree at the exact pushed commit. The server was
recently restarted, so first verify PostgreSQL, both vLLM endpoints, GPU
ownership and Ray. There must be no other runner.

```bash
cd /root/autodl-tmp/ai-operator
set -a
source /root/autodl-tmp/ai-operator-runtime.env
set +a

export PROJECT_ROOT=/root/autodl-tmp/ai-operator
export ARTIFACT_ROOT=/root/autodl-tmp/experiment-artifacts
export MODEL_ROOT=/root/autodl-tmp/models
export DATA_ROOT=/root/autodl-tmp/data
export VENV_ROOT=/root/autodl-tmp/venvs

PYTHONPATH=code /root/miniconda3/bin/python \
  code/scripts/environment/manage_environment.py \
  --env-file /root/autodl-tmp/ai-operator-runtime.env \
  check --groups core,text,analysis \
  --json-out "$ARTIFACT_ROOT/phase_change_<commit>/preflight.json"
```

The preflight must report the automatically selected 2x4090 profile and all
required checks `ok`. In addition, require:

- endpoint 8000/8001 health returns 200 and both service configs are identical;
- Ray has one node, 32 CPU / 2 GPU total, no pending demands;
- `nvidia-smi` shows only the two intended vLLM processes;
- PostgreSQL is reachable and both prompt pools exist;
- no `run_phase_change.py`, shared-vLLM runner or profiler process exists;
- after a failed Ray status following a restart, stop Ray first and remove only
  `/tmp/ray/ray_current_cluster` before recreating the 32-CPU/2-GPU head.

Record exact git commit, driver/vLLM Python paths, vLLM version, model, model
path, `max_num_batched_tokens`, `max_num_seqs`, endpoint command lines, PG and
pgvector versions in the artifact root. Do not install into the vLLM serving
environment.

The prompt-pool check on the current server found 6,479 SQuAD rows within
256+/-64 tokens and 663 ShareGPT rows within 1024+/-256 tokens. Recheck those
counts; they justify only the preregistered finite rate sets below.

## 2. Workload preparation contract

The old `phase_change_probe_20260811` database rows and probe directory are an
invalid diagnostic: its manifests used non-canonical fields. Never reuse,
overwrite, delete or cite it as evidence.

Every new point uses a new target workload, output directory and collision-free
doc-id range. Preparation without `--apply` writes files only; the experiment
runner refuses to run until `--apply` has produced a matching import receipt.
The builder fixes two 60-second OFF-first cycles, real database prompts,
arrival scale 1, two endpoints and a global 512-token `ignore_eos` output cap.

```bash
/root/miniconda3/bin/python \
  code/scripts/data/prepare_phase_change_workload.py \
  --database-url "$DATABASE_URL" \
  --short-source squad_v11_dev_short_answer \
  --long-source sharegpt_concentrated \
  --target-workload <new-immutable-workload> \
  --doc-id-base <verified-unused-base> \
  --rate-a <A-rate> --rate-b <B-rate> \
  --short-target 256 --short-max-dist 64 \
  --long-target 1024 --long-max-dist 256 \
  --output-cap 512 --duration-s 240 --period-s 60 \
  --endpoint-count 2 --seed 20260811 \
  --output-dir <new-contract-dir> --apply
```

Immediately load the contract with
`src.experiments.phase_change.load_contract`; this round-trips canonical
`ChatRequest` manifests, validates SHA/count/output/endpoint/phase fields and
matches the PostgreSQL import receipt. A builder failure due to prompt-pool or
doc-id limits is a hard stop for that point; never loosen distance or repeat
source rows.

## 3. Frozen probe arms and environment

The following are probe candidates, not portable final settings:

```bash
export PHASE_CHANGE_LOWER_K=128
export PHASE_CHANGE_LOWER_W=131072
export PHASE_CHANGE_UPPER_K=160
export PHASE_CHANGE_UPPER_W=163840
```

Also export `DATABASE_URL`, `VLLM_VERSION`,
`VLLM_MAX_NUM_BATCHED_TOKENS`, `VLLM_MAX_NUM_SEQS`, `COMPLETION_MODEL`,
`MODEL_PATH` and the explicit Ray address. The workload wrapper supplies
`PHASE_CHANGE_WORKLOAD`, row counts, manifest paths and output cap from the
validated contract; never set those by hand.

Every run uses:

```bash
/root/miniconda3/bin/python code/scripts/experiments/run_phase_change.py \
  --contract-dir <contract-dir> \
  --config <config> \
  --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir <new-output-dir> \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls http://127.0.0.1:8000/metrics,http://127.0.0.1:8001/metrics \
  --ray-address <explicit-address>
```

## 4. A-only rate gate

Try A rates in order **16, 20, 24 req/s**, with B fixed at 2.5 req/s in the
prepared contract (the A-only config does not submit Job B). Run
`phase_change_a_only_calibration.example.json`, then:

```bash
/root/miniconda3/bin/python code/scripts/analysis/audit_phase_change.py \
  --mode a-only --run-dir <a-only-output> --contract-dir <contract-dir> \
  --lower-k "$PHASE_CHANGE_LOWER_K" \
  --upper-k "$PHASE_CHANGE_UPPER_K" \
  --output <a-only-output>/a_only_audit.json
```

Stop at the first passed point and freeze its A rate. A valid point requires,
on both endpoints: the lower arm is at least 80% occupied in at least 50% of
state samples; replayed-arrival-to-submit P95 is at least 1 second in the
request trace; there is no vLLM waiting and KV remains below 0.85 on both arms;
and the upper arm improves median service rate by at least 5%. The request lag
is the source/admission backlog signal. Do not use `organizer_queued_work` for
this A-only gate: that field is shared-credit waiting work, and a single job's
equal job-local cap stops the scheduler before that queue under frozen-static
calibration. If no preregistered rate passes, stop the experiment.

For `state_aware_adaptive`, the runner deliberately sets job-local request/work
ceilings to the largest calibrated candidate while the shared coordinator starts
at the lower arm. The coordinator is therefore the only owner of K128/K160
actuation, and its waiting-work signal remains observable. Before the action
gate, inspect the resolved job command and require local
`--max-inflight/--max-active-work-per-endpoint` to equal the upper arm while
`--shared-credit-request-limit/--shared-credit-work-limit` initially equal the
lower arm. A mismatch is a hard stop: an action counter alone is not proof that
capacity changed.

## 5. A+B pressure/relief gate

Keep the selected A rate. Try B rates in order **2.5, 3.5, 4.5 req/s** using a
new immutable contract for each point. Run
`phase_change_pressure_calibration.example.json`, then:

```bash
/root/miniconda3/bin/python code/scripts/analysis/audit_phase_change.py \
  --mode pressure --run-dir <pressure-output> --contract-dir <contract-dir> \
  --a-only-audit <passed-a-only-output>/a_only_audit.json \
  --lower-k "$PHASE_CHANGE_LOWER_K" --lower-w "$PHASE_CHANGE_LOWER_W" \
  --upper-k "$PHASE_CHANGE_UPPER_K" --upper-w "$PHASE_CHANGE_UPPER_W" \
  --output <pressure-output>/pressure_audit.json \
  --calibration-output <pressure-output>/calibration_selection.json
```

Stop at the first passed point. Both OFF phases must be safe. In both ON
phases and on both endpoints, frozen-upper must show real service pressure
(`waiting>0` or KV>=0.85) while frozen-lower has safe P95 pressure and visibly
relieves waiting or KV. The script emits the only calibration selection file
accepted by the later configs. If all B rates fail, stop without an action or
formal run.

After the pass, export exactly the selection values and path:

```bash
export PHASE_CHANGE_CALIBRATION_CONTRACT=<pressure-output>/calibration_selection.json
export PHASE_CHANGE_TARGET_SERVICE_RATE=<selection value>
```

Do not round, tune or hand-edit the target service rate.

## 6. Bidirectional action gate

Use the exact passed A+B contract and calibration selection. Run
`phase_change_action_gate.example.json`, then:

```bash
/root/miniconda3/bin/python code/scripts/analysis/audit_phase_change.py \
  --mode action --run-dir <action-output> --contract-dir <contract-dir> \
  --lower-k "$PHASE_CHANGE_LOWER_K" \
  --upper-k "$PHASE_CHANGE_UPPER_K" \
  --output <action-output>/action_audit.json
```

The audit must observe, on **each endpoint**, ordered increase/decrease/
increase/decrease actions in phases 0/1/2/3, correct reasons, exact applied
arms, zero fallback, post-upshift active-request P50 above lower K during the
2--20 second observation window, and measurable risk relief after each
downshift. A mere change in a counter, GPU utilization, or whole-run throughput is not enough.
Any failure ends the experiment; do not enter formal.

## 7. Formal comparison and claim gate

Only after all prior JSON audits say `passed`, run
`phase_change_formal.example.json`. It contains 1 warm-up + 3 formal repeats
for frozen-lower, frozen-upper and adaptive, deterministically interleaved by
the shared runner. Expected wall time is about 50--65 minutes including drain
and idle gates.

```bash
/root/miniconda3/bin/python code/scripts/analysis/audit_phase_change.py \
  --mode formal --run-dir <formal-output> --contract-dir <contract-dir> \
  --lower-k "$PHASE_CHANGE_LOWER_K" \
  --upper-k "$PHASE_CHANGE_UPPER_K" \
  --output <formal-output>/formal_audit.json
```

The claim gate requires exactly three clean formal repeats per arm, throughput
CV<=10%, valid bidirectional adaptive actions in every repeat, adaptive total
tokens/s at least 5% over frozen-lower and at least 95% of frozen-upper,
adaptive OFF-phase service rate at least 95% of frozen-upper, and adaptive
ON-phase waiting/KV no worse than frozen-lower (KV tolerance 0.02). A failed
claim gate is still useful negative evidence, but cannot be described as a
dynamic-scheduling improvement.

Archive the contract, resolved configs, records, all per-job and state/resource/
credit traces, four audit JSON files, calibration selection, environment
preflight and service identity. Report using the project's seven-step structure
and distinguish facts, inference, failed gates and claims that cannot be made.
