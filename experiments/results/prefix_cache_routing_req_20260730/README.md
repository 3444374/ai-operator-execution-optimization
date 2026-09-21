# Prefix-affinity routing 实验报告

日期：2026-07-31（运行于 2026-07-30 23:47–2026-07-31 00:09）
分支：`claude/prefix-cache-experiments`（commit `5f000bd`）
平台：AutoDL 2×4090, Qwen2.5-7B-Instruct, vLLM 0.25.1, PG 18.4 + pgvector 0.8.5
配套 batching 报告：`../prefix_cache_data_org_20260730/README.md` §2

## 1. 研究问题与设计

**问题**：vLLM prefix cache 开启后，上游 routing 是否还有空间？具体测 cache 碎片化假设——naive `least_queued` 把同 prefix 请求散到两个 endpoint，可能使每个 endpoint 的 per-endpoint cache 命中率碎片化；`prefix_affinity`（rendezvous hash by prefix_key）把同 prefix 钉到同一 endpoint，应反碎片化、提高命中率与吞吐。

**三臂**（固定 batching=token_budget，只变 routing；第三臂叠加二级排序）：

| 场景 | routing | batching-policy |
|---|---|---|
| `route_least_queued_tb` | least_queued | token_budget |
| `route_affinity_tb` | prefix_affinity | token_budget |
| `route_affinity_pala_tb` | prefix_affinity | prefix_aware_length_align_token_budget |

1 warmup + 3 formal repeats，seed=20260729，场景交错（formal 由 runner 随机洗牌）。

## 2. 实验设置

### 关键参数
- Completions 协议, httpx_async, return-token-ids。
- K256 (max-inflight), W65536 (active-work credit)。
- 1×256 actor pool, ray-batch-rows 256, token_budget=8192。
- **prefix cache ON**（vLLM 两 endpoint 进程均带 `--enable-prefix-caching`，启动于 07-30 18:25，整个实验期间未重启）。
- 数据：`sharegpt_multiturn`（2,048 行，766 session，89% 行共享 prefix_key），请求 manifest `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。

### submission 粒度 = **request**（不是 batch）
`manifest_guard.py:82-93` 强制：Completions 在 **batch** 粒度只允许 `least_queued`；`prefix_affinity` 只在 **request** 粒度被允许（commit `5f000bd` "Allow prefix_affinity + least_queued routing for request-granularity Completions"）。这符合语义——prefix_affinity 是 per-request rendezvous hash，只在 request 级路由才有意义。

**方法学注记**：本 routing 实验因此用 request 粒度，其 `route_least_queued_tb` baseline 与 batching 实验（batch 粒度）的 least_queued run **不直接可比**（request 粒度因逐请求提交有更高开销，吞吐整体低 ~3.7%：~16,100 vs batching 的 ~16,700）。但 routing 三臂之间在相同 request 粒度下是干净的 A/B，这正是测 routing 假设所需要的。

### 配置与数据路径
- 配置：`/root/autodl-tmp/gates/prefix_cache_routing_req_20260730.json`（由 batching 模板派生；`experiment_id=prefix_cache_routing_req`，`submission_granularity=request`，routing 移入每场景显式指定）。
- 结果目录（远端）：`experiments/results/prefix_cache_routing_req_20260730/`（runs.csv + manifest.json + 12 套 trace）。
- manifest：status=completed, 12/12 ok, 0 incident。

### 环境漂移控制
batching 实验后 runtime env 被改（`BEST_TOKEN_BUDGET` 8192→32768、`SOURCE_WORKLOAD_NAME` multiturn→burstgpt）。若用 `${}` 占位符会 silent 跑成不可比配置。本配置对 `token-budget=8192`、`source-workload-name=sharegpt_multiturn`、`request-manifest`、`active-work-per-endpoint=65536` **硬编码**回 batching 的值，其余占位符（DATABASE_URL/endpoints/model/SLO 等）从 runtime env 展开。

## 3. 实验数据（model-request tok/s 中位数，3 formal reps）

| 场景 | routing | tp 中位数 | raw | CV | SLO 违约 | P95 | P99 |
|---|---|---|---|---|---|---|---|
| route_least_queued_tb | least_queued | 16,093 | [16107, 16094, 16020] | 0.3% | 0.8% | 104.9 | 107.4 |
| route_affinity_tb | prefix_affinity | 16,078 | [16049, 16208, 16079] | 0.5% | 0.7% | 104.3 | 106.8 |
| route_affinity_pala_tb | prefix_affinity + length-align | 16,382 | [16456, 16382, 16352] | 0.3% | 0.8% | 102.9 | 105.2 |

## 4. 结果解释

### 事实
- **纯路由效应**（prefix_affinity vs least_queued，都 token_budget）：16,078 vs 16,093 = **−0.1%**。完全中性。
- **length-align 效应**（pala vs affinity，都 prefix_affinity）：16,382 vs 16,078 = **+1.9%**。三 repeat 不重叠（[16456,16382,16352] vs [16049,16208,16079]），像是真有但很小的效应，**低于 5% 晋升门禁**。
- 三臂整体 spread ≈1.9%，CV ≤0.5%，SLO 违约均 ≤0.8%。

### 推断
- **cache 碎片化假设不被支持**：prefix_affinity routing 相对 least_queued 在吞吐/SLO/P95/P99 上无差异。vLLM APC（radix-tree automatic prefix cache）在多轮 ShareGPT 上自动复用 prefix，不依赖上游把同 prefix 钉到同一 endpoint——即使 least_queued 把同 session 请求散到两 endpoint，每个 endpoint 仍能看到足够多的重复 prefix 使 APC 命中。
- 与 batching 实验一致：vLLM 内部机制（APC + continuous batching）在 cache ON 时覆盖了上游能做的 prefix 优化（batching 顺序 + routing）。
- pala 的 +1.9% 来自 length-align batching 策略（组内按长度排序降低 HOL blocking），而非 routing；与 batching 实验（batch 粒度下 length-align +0.7%）方向一致、量级相近，均未过门禁。

### 不能声称
- 不能声称"prefix_affinity routing 在所有 workload/拓扑下无效"——只测了 ShareGPT 多轮 + 2 endpoint + K256 + W65536 + request 粒度。更高 endpoint 数（>2）或更低 prefix 重复率的 workload 下碎片化是否出现，未测。
- 不能声称"length-align 有效"——+1.9% 低于 5% 门禁；虽 repeat 不重叠提示非纯噪声，但不晋级为策略结论。
- **per-arm prefix cache 命中率未单独记录**：`resources.csv` 只采样了 `vllm_kv_cache_usage_perc`（KV 用量），命中率仅在 vLLM 文本日志按 engine 连续打印，formal 场景交错使事后无法干净归因到单臂。吞吐中性间接说明 cache 行为在臂间等价（若 affinity 显著拉高命中率，吞吐会反映）。

## 5. 对课题含义

1. **prefix 方向收口**：batching（§2）+ routing（本报告）均中性 → vLLM 内部 prefix cache 在多轮/模板化 workload 上已覆盖上游能做的 prefix 组织与路由优化。上游调度层的 prefix-aware 策略无额外空间（<5% 门禁）。
2. 这不否定 prefix cache 本身的价值（batching §2 的 ~+28% vs cache-OFF BurstGPT 是 workload 差异的初步信号，需 controlled cache-off ShareGPT 对照才能确认），只说明**开 cache 后上游怎么组织/路由 prefix 不再重要**。
3. 该结论与上游调度主线一致：在 vLLM continuous batching + APC 饱和后，routing/quantum/flush/batching 策略均趋于中性；上游有意义的杠杆在 active-work / K_max / 数据按计算量组织的更上游环节。

## 6. 下一步

1. 如需补全机制证据：在 runner 的 resources 采样中增加 vLLM `prefix_cache_hit_rate`（Prometheus 或 engine log 解析），使未来 cache 相关实验能直接看命中率。
2. 可选扩展（仅当 reviewer 追问跨拓扑泛化）：>2 endpoint 或低 prefix 重复率 workload 下重测 prefix_affinity。
3. prefix 方向判定完成，按既定优先级转 **Phase 2：OceanBase baseline**（需先在远端部署 OceanBase：当前 `obd`/`observer` 均未安装，端口 2881/2882 无监听）。

## 7. 附：失败记录与已知 bug

- **首次 launch（batch 粒度）失败**：`prefix_cache_routing_20260730/` 目录保留作事故证据。route_affinity warmup 在 `manifest_guard.py:90` 报 `batch-granularity Completions manifest requires least_queued routing`，exit 1。修复：改用 request 粒度（本报告）。
- **manifest 元数据 bug**：`service_metadata.prefix_caching` 在 batching 与本实验的 manifest 中均声明为 `false`，但 live vLLM 实际为 ON（进程参数 `--enable-prefix-caching`，日志 `Prefix cache hit rate: ~71%`）。runner 从 runtime env 默认值填该字段，未探测 live vLLM。不影响实验有效性（cache 确实开着），但元数据不准，应修：runner 启动时从 vLLM `/metrics` 或进程参数探测实际 prefix-cache 开关。
