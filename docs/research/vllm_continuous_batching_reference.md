# vLLM Continuous Batching Reference

> **Compiled:** 2026-07-16  
> **Purpose:** Technical reference for designing an upstream dynamic batching system that feeds into vLLM.  
> **Sources:** vLLM official documentation (docs.vllm.ai), GitHub repository (vllm-project/vllm), SOSP 2023 paper, community forums, technical blogs.  
> **Source type:** Mixed (papers, official docs, source code analysis, community discussions).

---

## Table of Contents

1. [vLLM Continuous Batching Core Mechanism](#1-vllm-continuous-batching-core-mechanism)
2. [Automatic Prefix Caching (APC)](#2-automatic-prefix-caching-apc)
3. [Metrics and Observability](#3-metrics-and-observability)
4. [Scheduler Internals and Chunked Prefill](#4-scheduler-internals-and-chunked-prefill)
5. [Academic Papers and System Comparisons](#5-academic-papers-and-system-comparisons)
6. [Practical Integration](#6-practical-integration)

---

## 1. vLLM Continuous Batching Core Mechanism

### 1.1 What is Continuous Batching?

Continuous batching (also called dynamic batching, in-flight batching, or iteration-level scheduling) means that requests **join and leave the batch mid-flight** — after every decode step, the scheduler can drop finished requests, add new ones from the queue, and recompute KV cache memory budgets. This contrasts with static batching where a batch must fully complete before a new batch begins.

The unit of scheduling is **one forward pass of the model**, not one request. Each scheduling step corresponds to a single model `forward()` call.

### 1.2 Iteration-Level Scheduling Algorithm

At each iteration, the scheduler produces a dictionary `{req_id: num_tokens}` specifying how many tokens to process per request:

- **Prefill (new requests):** full prompt length (or a chunk in chunked prefill mode)
- **Decode (existing requests):** 1 token per request (auto-regressive generation)
- **Chunked prefill:** N tokens where N fits within the remaining token budget

The scheduling loop runs in three phases per iteration:

#### Phase 1: Schedule
1. **Prioritize decode requests** — existing running requests get slots first.
2. **Process prefill requests** from the waiting queue:
   - Check prefix cache hits (if APC is enabled).
   - Allocate KV cache slots (`allocate_slots`).
   - Move requests from `waiting` to `running` with status `RUNNING`.
   - Update token budget.
3. **KV cache allocation:** Calculate how many new blocks are needed (16 tokens per block by default). If the `free_block_queue` is exhausted, the scheduler may **preempt** low-priority requests.

#### Phase 2: Forward Pass
- Prune completed requests from the input batch.
- Copy CPU buffers to GPU; compute positions and `slot_mapping`.
- Build attention metadata.
- Run model forward pass with PagedAttention kernel.
- Extract hidden states from final position of each sequence; compute logits; sample tokens.

#### Phase 3: Post-Process
- Append sampled token IDs to each `Request` object.
- Detokenize.
- Check stop conditions: max length, EOS token, `stop_token_ids`, stop strings.
- If completed: return KV cache blocks to `free_block_queue`, clean up request.

### 1.3 The Three-Queue State Machine

| Queue | Description |
|---|---|
| **Waiting** | New unscheduled requests. Requests remain here until the scheduler admits them into a running batch. |
| **Running** | Active sequences currently in the model execution batch. At each decode step, 1 token is generated per sequence. |
| **Swapped** | Preempted sequences whose KV cache was evicted to CPU RAM (v0 engine). In v1 engine, this queue is deprecated in favor of recomputation. |

State transitions:
```
WAITING --> RUNNING  (scheduler admits request, KV cache blocks allocated)
RUNNING --> DONE     (EOS emitted, max_tokens reached, or stop condition met)
RUNNING --> SWAPPED  (preemption: KV cache evicted to CPU to free GPU memory)
SWAPPED --> RUNNING  (preemption recovery: KV cache restored from CPU to GPU, or recomputed)
```

### 1.4 Key Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `max_num_seqs` | 128 | Maximum number of sequences (requests) that can be processed concurrently in one iteration. This is the hard cap on the running batch size. |
| `max_num_batched_tokens` | 2048 | Maximum total tokens (across all sequences) processed in a single iteration. Token budget per scheduler step. |
| `max_model_len` | model-dependent | Maximum sequence length (prompt + generation). Requests exceeding this are rejected. |
| `max_num_partial_prefills` | 1 | Maximum number of sequences that can be partially prefilled concurrently (requires chunked prefill). |
| `max_long_partial_prefills` | 1 | Maximum number of long prompts (above `long_prefill_token_threshold`) allowed to prefill concurrently. |
| `long_prefill_token_threshold` | 0 (defaults to `max_model_len * 0.04`) | Token threshold above which a request is considered "long". Shorter prompts can jump ahead of long ones. |
| `enable_chunked_prefill` | True (v0.14+) | Whether to split long prefill requests into chunks interleaved with decode steps. |
| `policy` | `"fcfs"` | Scheduling policy: `"fcfs"` or `"priority"`. |
| `async_scheduling` | False | Perform scheduling asynchronously to avoid GPU idle gaps (v1 only; not compatible with speculative decoding and pipeline parallelism yet). |

### 1.5 How vLLM Decides Which Requests to Include in Each Forward Pass

The scheduler must satisfy two constraints simultaneously:

1. **Token budget constraint:** Total tokens across all scheduled sequences must not exceed `max_num_batched_tokens`.
2. **Sequence count constraint:** Total number of scheduled sequences must not exceed `max_num_seqs`.

The scheduler fills the batch in priority order:
1. **Decode requests first** (1 token each) — these are latency-sensitive.
2. **In-progress chunked prefills** — requests that have started but not finished prefilling.
3. **Swapped requests** (if any) — restore from CPU.
4. **New prefills from the waiting queue** — admit as many as the remaining budget allows.

### 1.6 Prefill vs Decode in the Same Batch

- **v0 engine (legacy):** Could only handle one phase type per batch. Batching was either entirely prefill OR entirely decode. A long prefill could stall all decode requests ("generation stalls").
- **v1 engine (current):** Supports **mixed prefill+decode in the same forward pass** via chunked prefill. Long prompts are split; prefill chunks and decode steps are interleaved. This eliminates generation stalls and produces a single fused step mixing decode tokens with prefill chunk tokens.

### 1.7 How PagedAttention Enables Continuous Batching

PagedAttention is the KV cache memory management system that makes continuous batching practical:

- KV cache is divided into **fixed-size blocks** (default 16 tokens per block, configurable up to 32 on CUDA).
- Blocks are stored in **non-contiguous physical GPU memory**, mapped via a **block table** (analogous to OS page tables).
- Memory is allocated **on demand** — new blocks assigned only when previous blocks fill up.
- Waste is bounded to < 1 partial block per sequence (~8 tokens average).
- **Reference counting** enables prefix sharing (multiple requests share the same physical blocks for shared prefixes).
- **Copy-on-write (CoW)** for parallel sampling and beam search.

Without PagedAttention, naive batching would require pre-allocating max-length contiguous KV cache per request slot, wasting most VRAM (real utilization was only 20.4-38.2% in prior systems like FasterTransformer and Orca).

---

## 2. Automatic Prefix Caching (APC)

### 2.1 How APC Works Internally

APC is a **content-addressed** caching system operating at KV cache block granularity. It automatically detects when a new request shares a common prefix with previously processed requests and reuses the cached KV blocks instead of recomputing them.

### 2.2 Hash / Key Matching Mechanics

Each KV cache block is identified by a composite hash:

```
hash(parent_block_hash, tokens_in_block, extra_metadata)
```

Key properties:
- **Parent-chaining:** Each block's hash depends on the hash of its preceding block. Validating one block implicitly guarantees the entire prefix up to that point is identical.
- **Hash algorithm:** SHA-256 (as of v0.11).
- **Extra metadata:** Can include LoRA adapter changes (different adapters produce different hashes for the same tokens), `cache_salt` for tenant isolation.

### 2.3 Cache Hit vs Miss Determination

Matching proceeds **sequentially from block 0 forward**:

```
New request arrives
    Compute hash for block 0 -> Match against cache?
        YES -> Reuse block, move to block 1
            Compute hash for block 1 -> Match?
                YES -> Continue...
                NO  -> Stop. Recompute block 1 and all subsequent blocks.
        NO  -> Recompute entire sequence from scratch.
```

Critical rules:
- **Only full blocks** can be cached. Partial blocks at the end of a sequence are never cached.
- **The first hash miss terminates the search immediately.** There is no "skip and match later" behavior.
- **Any token change within a block invalidates that block's hash AND all subsequent blocks** (parent-chain dependency).
- Block size is fixed (default 16 tokens, configurable up to 32 on CUDA).

Example: A 50-token prompt with `block_size=16` produces 3 full blocks (48 tokens cacheable) + 1 partial block of 2 tokens (not cached).

### 2.4 Cache Eviction

- **LRU (Least Recently Used)** eviction when GPU memory fills.
- **Tail blocks** (end of sequences) are preferentially evicted before prefix blocks.
- **Reference counting** ensures blocks with active in-flight requests are never evicted.
- Multiple concurrent requests sharing the same prefix **share the same physical KV cache blocks** via reference counting.

### 2.5 V1 Engine Improvements

In v1 (2025+), prefix caching became **zero-overhead and enabled by default**:
- Constant-time hash eviction.
- Pre-allocated block pools.
- Append-only block tables.
- No configuration needed.

### 2.6 Maximizing APC Hit Rate from the Request Side

Strategies for upstream systems to maximize APC effectiveness:

1. **Group requests with shared system prompts** — submit them close together in time so the shared prefix blocks remain in cache (not LRU-evicted).
2. **Use consistent system prompts** — even minor differences in the prompt prefix break the cache chain.
3. **Submit shared-prefix requests concurrently** — multiple concurrent requests with the same prefix share physical blocks via reference counting (100 requests = 1x compute cost for the shared prefix).
4. **Avoid unnecessary prefix variations** — each unique prefix occupies separate cache blocks.
5. **Be aware of block boundaries** — tokens should align to 16-token block boundaries where possible (though exact alignment is not required for correctness).
6. **Order requests by prefix** — if you control submission order, submit all requests sharing prefix A, then all sharing prefix B, to avoid eviction of A's blocks by B's blocks.

### 2.7 Limitations

- A **single token difference** in an early block invalidates the entire subsequent cache chain.
- Short prompts (< 16 tokens) produce no cacheable blocks (partial block never cached).
- LoRA adapter changes produce different hashes even for identical token sequences.
- Cache capacity is bounded by `--gpu-memory-utilization` allocation.

---

## 3. Metrics and Observability

### 3.1 Metrics Endpoint

vLLM exposes Prometheus-compatible metrics at:
```
GET http://<host>:<port>/metrics
```

All metrics carry a `model_name` label. Request-level metrics include `finished_reason` label (`stop`, `length`, `abort`).

### 3.2 v1 Engine Metrics (Current, v0.11.1+)

#### Gauge Metrics (Instantaneous State)

| Metric | Description | Use for Upstream Control |
|---|---|---|
| `vllm:num_requests_running` | Number of requests currently in model execution batches | **Active batch size** — primary signal for admission control |
| `vllm:num_requests_waiting` | Number of requests waiting to be processed | **Queue depth** — primary signal for backpressure |
| `vllm:kv_cache_usage_perc` | Fraction of used KV cache blocks (0-1). Renamed from `vllm:gpu_cache_usage_perc` in v0. | **Memory pressure** — >0.9 indicates near capacity |
| `vllm:cache_config_info` | Cache configuration information | Static config reference |

#### Counter Metrics (Cumulative)

| Metric | Description |
|---|---|
| `vllm:request_success_total` | Finished requests by `finished_reason` |
| `vllm:num_preemptions_total` | Total number of preemption events |
| `vllm:prompt_tokens_total` | Total prompt tokens processed |
| `vllm:generation_tokens_total` | Total generated tokens |
| `vllm:prefix_cache_queries` | Number of prefix cache queries |
| `vllm:prefix_cache_hits` | Number of prefix cache hits |

#### Histogram Metrics (Latency Distributions)

| Metric | Description |
|---|---|
| `vllm:time_to_first_token_seconds` | TTFT — time from request arrival to first token |
| `vllm:inter_token_latency_seconds` | ITL (replaces `vllm:time_per_output_token_seconds` in v1) — time between consecutive output tokens |
| `vllm:e2e_request_latency_seconds` | End-to-end request latency |
| `vllm:request_queue_time_seconds` | Time spent in the waiting queue |
| `vllm:request_inference_time_seconds` | Time spent in model execution |
| `vllm:request_prefill_time_seconds` | Time spent in prefill phase |
| `vllm:request_decode_time_seconds` | Time spent in decode phase |
| `vllm:iteration_tokens_total` | Tokens processed per engine step |
| `vllm:request_prompt_tokens` | Input prompt token count distribution |
| `vllm:request_generation_tokens` | Generation token count distribution |
| `vllm:request_params_max_tokens` | `max_tokens` request parameter distribution |

### 3.3 v0 Engine Metrics (Legacy, Additional)

The v0 engine additionally exposes:
- `vllm:num_requests_swapped` — requests swapped to CPU (deprecated in v1)
- `vllm:cpu_cache_usage_perc` — CPU cache usage
- `vllm:gpu_prefix_cache_hit_rate` / `vllm:cpu_prefix_cache_hit_rate` — cache hit rates (replaced by query/hit counters in v1)
- `vllm:spec_decode_draft_acceptance_rate` / `vllm:spec_decode_efficiency` — speculative decoding metrics
- `vllm:model_forward_time_milliseconds` / `vllm:model_execute_time_milliseconds` — model timing

### 3.4 Key Observability Patterns for Upstream Control

| Situation | Metrics Pattern | Upstream Action |
|---|---|---|
| **Healthy** | `num_requests_waiting` ≈ 0, `num_requests_running` stable, `kv_cache_usage_perc` < 0.9 | Normal submission rate |
| **System saturation** | `num_requests_waiting` growing, `num_requests_running` plateaued | Apply backpressure, reduce submission rate |
| **Memory pressure** | `kv_cache_usage_perc` > 0.9, `num_preemptions_total` increasing | Reduce `max_num_seqs` or increase GPU memory, throttle long-context requests |
| **Overload** | All three metrics elevated simultaneously | Aggressive backpressure, possibly drop/redirect requests |
| **Starvation risk** | `num_requests_running` < `max_num_seqs` but `num_requests_waiting` > 0 | Check if long requests are blocking (preemption not working), consider chunked prefill |

### 3.5 Querying Scheduler State via API (Non-Prometheus)

vLLM does **not** expose a dedicated REST API endpoint for scheduler state. Options:

1. **`/metrics` endpoint** — Parse Prometheus text format for gauge values. This is the recommended approach.
   ```python
   import requests
   resp = requests.get("http://localhost:8000/metrics")
   for line in resp.text.split("\n"):
       if "num_requests_running" in line or "num_requests_waiting" in line:
           print(line)
   ```

2. **Internal Python API** (when embedding vLLM as a library):
   - `Scheduler.get_request_counts()` returns `(num_running_reqs, num_waiting_reqs)`
   - Available via `AsyncLLMEngine` or `LLMEngine` when not using the standalone server.

3. **Server load endpoint** (community-contributed, not in core vLLM):
   - Some deployments add a custom `/health` or `/load` endpoint to the FastAPI server.

### 3.6 Relationship Between `kv_cache_usage_perc` and `gpu-memory-utilization`

`kv_cache_usage_perc` measures usage **relative to the KV cache portion** of reserved GPU memory, NOT total GPU memory. The reserved amount is controlled by `--gpu-memory-utilization`:

- `--gpu-memory-utilization 0.85` means vLLM reserves 85% of total GPU memory.
- Within that 85%, memory is split between model weights and KV cache.
- `kv_cache_usage_perc = 1.0` means the KV cache portion is fully occupied (not the entire GPU memory).

A value consistently above **0.9** signals the system is near its KV cache capacity limit.

---

## 4. Scheduler Internals and Chunked Prefill

### 4.1 Scheduler Source Code Locations

| Component | Path | Purpose |
|---|---|---|
| Scheduler Config | `vllm/config/scheduler.py` | Defines `SchedulerConfig` dataclass with all parameters |
| Core Scheduler (v0) | `vllm/core/scheduler.py` | Legacy scheduling logic (`_schedule_default`, `_schedule_chunked_prefill`) |
| Scheduler Interface (v1) | `vllm/v1/core/sched/interface.py` | V1 scheduler interface (`schedule()`, `get_request_counts()`, `pause_state()`) |
| Attention Backend Utils (v1) | `vllm/v1/attention/backends/utils.py` | Chunked prefill batch reordering (`split_prefill_chunks()`) |
| Metrics | `vllm/engine/metrics.py` | Metrics collection and Prometheus formatting |

### 4.2 How the Scheduler Selects Requests from the Waiting Queue

**Default policy (`fcfs`):** First-come, first-served. Requests are admitted in arrival order, subject to:
- KV cache block availability (must fit in remaining GPU memory).
- Token budget remaining in the current iteration.
- `max_num_seqs` limit.

**Priority policy (`priority`):** Requests carry a `priority` field (lower value = higher priority). The scheduler processes lowest-priority-value requests first, enabling mixed interactive/batch workloads:
```python
# Priority assignment pattern for mixed workloads
interactive_request: priority = 0   # highest priority
batch_request: priority = 10         # lower priority
```

**Selection logic per iteration:**
1. Count available slots: `max_num_seqs - len(running)`.
2. Compute remaining token budget: `max_num_batched_tokens - tokens_used_by_running`.
3. Iterate through waiting queue in priority order (FCFS within same priority).
4. For each candidate: check `can_allocate()` (KV cache space), check token budget.
5. Admit as many as constraints allow.

### 4.3 How `max_num_batched_tokens` Interacts with Varying Request Sizes

The token budget is consumed differently depending on request phase:

- **Decode requests:** 1 token each (fixed cost).
- **Short prefill:** Full prompt length (e.g., 50 tokens = 50 from budget).
- **Long prefill without chunked prefill:** Full prompt length — if it exceeds the remaining budget, must wait for next iteration.
- **Long prefill with chunked prefill:** Takes `min(remaining_budget, remaining_prompt_tokens)` tokens, then returns to waiting queue.

Interaction example with `max_num_batched_tokens=2048` and `max_num_seqs=128`:
- Maximum decode-only batch: 128 sequences consuming 128 tokens.
- With one short prefill (500 tokens): 500 + (up to 128 decode) = up to 628 tokens.
- With one long prefill (4000 tokens) and chunked prefill: 2048 tokens processed this step (remaining 1952 tokens wait).
- With one long prefill (4000 tokens) and NO chunked prefill: request rejected if `max_num_batched_tokens < max_model_len`.

### 4.4 Chunked Prefill: Detailed Mechanism

#### What It Solves
Long prefill requests are compute-intensive (GEMM operations). Without chunking, a single long prefill can monopolize the GPU for seconds, causing "generation stalls" for decode requests that need 1 token each.

#### How It Works
1. **Token budget allocation:** At each iteration, a `max_num_batched_tokens` budget is available.
2. **Decode-first filling:** Decode requests consume 1 token each from the budget first.
3. **Prefill chunking:** The remaining budget is allocated to prefill requests. If a prompt cannot fully fit, it is **chunked** — only the portion that fits is processed.
4. **State tracking:** The scheduler tracks `PartialPrefillMetadata` containing:
   - `schedulable_prefills` — minimum prefills to schedule this iteration.
   - `long_prefills` — count of active long prefill requests.
   - `can_schedule(seq_group)` — rejects long requests if `max_long_partial_prefills` is reached.
5. **Batch reordering:** After scheduling, the batch is reordered into four regions for the attention kernel:
   - `decode` — requests done prefilling.
   - `short_extend` — short requests still chunked-prefilling.
   - `long_extend` — long requests still chunked-prefilling.
   - `prefill` — first chunks with `num_computed == 0`.
6. **Hybrid attention:** Each chunk after the first is neither pure prefill nor pure decode — it's an "extend" operation that attends to both the current chunk's tokens AND the cached KV from previous chunks.

#### V1 FCFS Interleaving Example
```
token_budget = 10

Time 1: Req 1 arrives (14 tokens)
    -> Schedule tokens 1-10 of Req 1

Time 2: Req 2 arrives (10 tokens)
    -> Schedule tokens 11-14 of Req 1 + tokens 1-6 of Req 2

Time 3:
    -> Schedule 1 decode token of Req 1 + tokens 7-10 of Req 2
```

This shows a single batch containing **both decode and chunked prefill** work.

#### Constraints
- Only `max_num_partial_prefills` sequences can be prefilled concurrently in chunked mode.
- Only `max_long_partial_prefills` long prompts (above `long_prefill_token_threshold`) can prefill concurrently.
- Encoder-decoder models disable chunked prefill forcibly.
- Multimodal inputs may disable partial scheduling via `disable_chunked_mm_input`.

### 4.5 Validation Rules from SchedulerConfig

```
1. max_num_batched_tokens < max_model_len AND chunked prefill disabled -> ValueError
2. max_num_batched_tokens < max_num_seqs -> ValueError
3. max_num_batched_tokens > max_num_seqs * max_model_len -> Warning
4. max_num_partial_prefills > 1 without enable_chunked_prefill -> ValueError
5. Encoder-decoder model -> chunked prefill forcibly disabled
```

### 4.6 Scheduling Overhead (2024-2025 Findings)

Research (MLSys @ WukLab, 2025, analyzing vLLM v0.5.4) found:

- **Scheduling overhead can exceed 50% of total inference time**, especially with smaller models on fast GPUs.
- The overhead comes primarily from **tensor pre/post-processing**, not the scheduling algorithm itself: building input tensors, per-request metadata preparation (Python object creation), and detokenization.
- Overhead grows with **request count** — workloads with longer outputs and more decode requests incur higher overhead (a batch packs more decode requests per token budget than prefill requests).
- **Chunked prefill increases scheduling overhead** (allows more requests in a batch, reducing model forward time, making scheduling time relatively larger).
- Suggested mitigations: avoid unnecessary detokenization, async detokenization, PyTorch vectorized ops, native C++ implementations.

---

## 5. Academic Papers and System Comparisons

### 5.1 vLLM (SOSP 2023)

**Paper:** "Efficient Memory Management for Large Language Model Serving with PagedAttention"  
**Authors:** Woosuk Kwon, Zhuohan Li et al. (UC Berkeley)  
**Venue:** SOSP 2023  
**arXiv:** https://arxiv.org/abs/2309.06180  
**Code:** https://github.com/vllm-project/vllm

**Core contributions:**
1. **PagedAttention:** KV cache divided into fixed-size blocks; non-contiguous physical memory with block table indirection; reference-counted copy-on-write sharing.
2. **Continuous batching:** Iteration-level scheduling where requests join/leave mid-flight.
3. **Memory utilization:** 96.3% vs. 20.4% for Orca (Max) — near-zero KV cache waste.

**Performance vs. Orca (Oracle):** 1.7-2.7x higher request rate.  
**Performance vs. FasterTransformer:** up to 22x higher request rate.  
**Chatbot workloads:** 2x higher request rate.  
**Few-shot shared prefix:** 3.58x throughput vs. Orca (Oracle).

**Overhead:** PagedAttention kernel is 20-26% slower than FasterTransformer's contiguous kernel, but end-to-end throughput gain from superior memory utilization far outweighs this.

### 5.2 Orca (OSDI 2022)

**Paper:** "Orca: A Distributed Serving System for Transformer-Based Generative Models"  
**Venue:** OSDI 2022

**Key differences from vLLM:**
- **First to introduce iteration-level scheduling** — the theoretical foundation for continuous batching.
- **Hybrid batching:** Prefill and decode can coexist in the same batch (unlike vanilla vLLM).
- **Block-based KV cache** — direct inspiration for PagedAttention, but with fragmentation issues.
- **Prefill-prioritizing:** No prefill chunking, so long prefills can block decodes.
- **Memory utilization:** ~40% vs. vLLM's 90%+.

### 5.3 Sarathi-Serve (OSDI 2024)

**Paper:** "Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve"  
**arXiv:** https://arxiv.org/abs/2403.02310

**Key innovations:**
- **Chunked prefill** — splits long prompts into ~512-token chunks interleaved with decode steps.
- **Stall-free scheduling** — eliminates generation stalls (the key limitation of vanilla vLLM).
- **Decode-prioritized** — fills batches with decode tokens first, then uses remaining budget for prefill chunks.
- **Throughput-optimal** (proven work-conserving).

**Performance:** 2.6x higher capacity vs vLLM (Mistral-7B, A100), up to 5.6x for Falcon-180B. P99 TTFT 450ms vs vLLM's 600ms; P99 TBT 32ms vs 45ms.

**Legacy:** vLLM later adopted Sarathi-Serve's chunked prefill approach through community PRs, making it work-conserving and throughput-optimal.

### 5.4 TGI (Text Generation Inference)

**Developer:** Hugging Face

**Key differences:**
- **Dynamic batching** (not full iteration-level) — collects requests within a configurable timeout window, then processes the batch.
- **Contiguous KV cache allocation** — pre-allocates for max sequence length (19-27% more memory than vLLM).
- **Latency-first** design: 1.3-2x lower TTFT at low concurrency, more predictable per-request latency, but 2-24x lower throughput.
- Extensive quantization support (GPTQ, AWQ, EETQ), HF Hub integration.

### 5.5 Comparison Matrix

| Dimension | Orca (OSDI'22) | vLLM (SOSP'23) | Sarathi-Serve (OSDI'24) | TGI |
|---|---|---|---|---|
| **Scheduling Granularity** | Iteration-level | Iteration-level | Iteration-level | Batch-level (dynamic) |
| **KV Cache** | Block-based (fragmentation) | PagedAttention (90%+ util) | PagedAttention | Contiguous pre-allocation |
| **Prefill/Decode Mixing** | Hybrid | Segregated (vanilla); Hybrid (v1+chunked) | Always hybrid via chunked prefill | Hybrid supported |
| **Long Prefill Impact** | Blocks decodes | Blocks decodes (vanilla); No stall (v1+chunked) | No stalls | Mitigated via timeout windows |
| **Throughput** | Moderate | High | High + stable latency | Lower (2-24x less) |
| **GPU Utilization** | ~85% | 85-92% | 85-92% | 68-74% |
| **Memory Efficiency** | Moderate | Best | Best | Lower (19-27% more) |
| **Latency Profile** | Moderate | High throughput, variable latency | Best balance | Best TTFT at low concurrency |
| **Work-Conserving** | Yes | Not guaranteed (vanilla); Yes (v1+chunked) | Yes | N/A |

### 5.6 Scheduling Overhead Paper (2025)

**Paper/Report:** "Can Scheduling Overhead Dominate LLM Inference Performance?"  
**Source:** MLSys @ WukLab (2025)  
**URL:** https://mlsys.wuklab.io/posts/scheduling_overhead/

Key findings summarized in Section 4.6 above.

---

## 6. Practical Integration

### 6.1 How to Submit Requests for Optimal Continuous Batching

**Recommended approach: Submit all requests concurrently. Do NOT batch manually.**

vLLM's continuous batching is most efficient when it has a queue of waiting requests to pull from. Manual batching (e.g., send 15, wait for all to complete, send 15 more) creates GPU idle gaps between batches.

```python
# RECOMMENDED: Concurrent submission with concurrency cap
import asyncio
from openai import AsyncOpenAI

client = AsyncOpenAI(base_url="http://localhost:8000/v1", api_key="not-needed")
semaphore = asyncio.Semaphore(64)  # cap concurrent in-flight requests

async def process(request):
    async with semaphore:
        return await client.chat.completions.create(
            model="your-model",
            messages=request["messages"],
            max_tokens=request.get("max_tokens", 256),
        )

results = await asyncio.gather(*[process(r) for r in all_requests])
```

### 6.2 Standard OpenAI-Compatible Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/v1/completions` | POST | Legacy completions API (text in, text out) |
| `/v1/chat/completions` | POST | Chat completions API (messages format) |
| `/v1/embeddings` | POST | Embeddings API (text in, vector out) |
| `/v1/models` | GET | List available models |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

### 6.3 Batch Chat Completions Endpoint (`/v1/chat/completions/batch`)

vLLM-specific endpoint that accepts multiple conversations in one HTTP request:

**Advantages:**
- Reduced HTTP overhead for multiple independent prompts.
- Simplified result handling (all outputs in one response).
- Supports structured outputs (JSON schema, regex constraints) per conversation.

**Limitations:**
- **No streaming support.**
- **No tool use support.**
- **No beam search support.**
- Non-standard (outside OpenAI API spec).

### 6.4 Rate Limiting and Backpressure

**vLLM does NOT provide built-in rate limiting or per-user quotas.** It processes whatever requests it receives.

**Internal backpressure via scheduler:**
- `--max-num-seqs` provides natural concurrency-based backpressure. When the running batch is full, new requests queue internally.
- When KV cache is exhausted, requests are **preempted** (swapped to CPU, then recomputed later).
- No explicit HTTP 429 or 503 responses from vLLM itself.

**Recommended production architecture:**
```
Client -> Nginx/Envoy (rate limiting) -> Application Proxy (auth, quotas) -> vLLM
```

Layers to add:
1. **Connection-level rate limiting** (Nginx `limit_req_zone`).
2. **Token-aware rate limiting** (track tokens/minute and tokens/day per user, not just requests).
3. **Queue with max depth** — return 503 if queue is full, with `Retry-After` header.
4. **Timeout** — cancel requests that exceed configurable queue timeout (e.g., 30 seconds).

### 6.5 Key Tuning Recommendations for Different Workloads

#### Throughput-Oriented (Batch Processing)
```bash
vllm serve model-path \
  --max-num-seqs 128 \
  --max-num-batched-tokens 8192 \
  --gpu-memory-utilization 0.90 \
  --enable-chunked-prefill
```

#### Latency-Sensitive (Interactive Chat)
```bash
vllm serve model-path \
  --max-num-seqs 16 \
  --max-num-batched-tokens 2048 \
  --gpu-memory-utilization 0.85 \
  --enable-chunked-prefill
```

#### Mixed Interactive + Batch (Priority Scheduling)
```bash
vllm serve model-path \
  --scheduling-policy priority \
  --max-num-seqs 64 \
  --max-num-batched-tokens 4096
```
Assign `priority=0` to interactive requests, `priority=10` to batch jobs.

### 6.6 Upstream Dynamic Batching Design Implications

For designing an upstream system that feeds vLLM:

**Things the upstream should do:**
1. **Group requests by shared prefix** and submit them close together in time to maximize APC hit rate.
2. **Monitor `vllm:num_requests_waiting` and `vllm:kv_cache_usage_perc`** for admission control and backpressure decisions.
3. **Apply token-aware admission control** — estimate token load (prompt + expected generation) before submitting; throttle when vLLM signals saturation.
4. **Use `max_tokens` to bound generation length** — prevents single long-generation requests from blocking the queue.
5. **Set request priorities** if using `--scheduling-policy priority` (lower value = higher priority).
6. **Cap concurrent in-flight requests** using `asyncio.Semaphore` or similar.

**Things the upstream should NOT do:**
1. **Do NOT manually batch** — vLLM's scheduler is more efficient with a queue of individual requests.
2. **Do NOT wait for batch completion before sending more** — this creates GPU idle time between batches.
3. **Do NOT strip or modify system prompts to "save tokens"** — this breaks APC prefix chains.
4. **Do NOT submit all requests with unbounded `max_tokens`** — use the minimum needed for each request type.

### 6.7 Upstream Parameter Mapping (vLLM Config -> Upstream Control Knobs)

| vLLM Parameter | What It Means for Upstream | Upstream Control Knob |
|---|---|---|
| `max_num_seqs` | Hard cap on concurrent sequences | `K_max` (max in-flight requests). Monitor `num_requests_running` to stay within limit. |
| `max_num_batched_tokens` | Token budget per iteration | Estimate token consumption of submitted requests (prompt tokens + expected decode tokens). Batch-size decisions in upstream. |
| `kv_cache_usage_perc` | Memory pressure signal | Backpressure trigger. If > 0.9, reduce submission rate. |
| `num_requests_waiting` | Queue depth signal | Backpressure trigger. If growing, reduce submission rate. |
| `num_preemptions_total` | Preemption count (KV cache exhaustion) | Reduce concurrent requests or reduce max_tokens. |
| `time_to_first_token_seconds` | Latency signal | If p50/p99 TTFT rises, reduce concurrent load. |
| `inter_token_latency_seconds` | Decode latency signal | If p50/p99 ITL rises, reduce batch size (lower `max_num_seqs`). |
| `prefix_cache_hits / prefix_cache_queries` | APC effectiveness | If hit rate drops, check if requests are being submitted with varying prefixes. |

---

## References

1. Kwon, W. et al. "Efficient Memory Management for Large Language Model Serving with PagedAttention." SOSP 2023. https://arxiv.org/abs/2309.06180
2. vLLM GitHub Repository. https://github.com/vllm-project/vllm
3. vLLM Official Documentation - Metrics. https://docs.vllm.ai/en/latest/design/metrics/
4. vLLM Official Documentation - Automatic Prefix Caching. https://docs.vllm.ai/en/latest/features/automatic_prefix_caching/
5. Agrawal, A. et al. "Taming Throughput-Latency Tradeoff in LLM Inference with Sarathi-Serve." OSDI 2024. https://arxiv.org/abs/2403.02310
6. Yu, G. et al. "Orca: A Distributed Serving System for Transformer-Based Generative Models." OSDI 2022.
7. "Can Scheduling Overhead Dominate LLM Inference Performance?" MLSys @ WukLab, 2025. https://mlsys.wuklab.io/posts/scheduling_overhead/
8. "Inside vLLM: Anatomy of a High-Throughput LLM Inference System." vLLM Blog, 2025. https://blog.vllm.com.cn/2025/09/05/anatomy-of-vllm.html
9. vLLM Community Forums. https://discuss.vllm.ai/
10. "Continuous Batching: vLLM's In-Flight Scheduling Trick." https://zeroentropy.dev/concepts/continuous-batching/
