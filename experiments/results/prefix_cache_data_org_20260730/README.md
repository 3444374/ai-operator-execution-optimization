# Prefix Cache 实验报告（batching + routing）

日期：2026-07-30
分支：`claude/prefix-cache-experiments`
平台：AutoDL 2×4090, Qwen2.5-7B-Instruct, vLLM 0.25.1, PG 18.4 + pgvector 0.8.5

## 1. 实验设置

### 数据
- **ShareGPT 多轮对话**（通过新增的 `--multi-turn` importer 导入）。
- DB workload `sharegpt_multiturn`：2,048 行，766 个 session，2.67 行/session。
- 89% 的行共享 prefix_key（同一对话的后续轮包含前面轮的上下文）。
- prompt_tokens avg 572, range 3–1,486（后续轮 prompt 更长 = prefix 增长）。
- 对比：BurstGPT（旧 workload）90% prefix 唯一，无 prefix 重复结构。

### vLLM 配置
- `--enable-prefix-caching`（prefix cache ON，本轮新开）。
- 其余不变：max-num-batched-tokens 8192, max-num-seqs 256, no-eager, chunked-prefill, mfu-metrics。

### 代码变更
- `organizers.py`：新增 `prefix_aware_length_align_{token_budget,fixed_rows}` 二级排序策略。
  - 一级：按 prefix_key 分组（cache 局部性）。
  - 二级：组内按 prompt_tokens 排序（降低 HOL blocking）。
- `profiling/cli.py`：新增对应 argparse choices。
- `import_ai_complete_workload.py`（main 分支已合并）：新增 `--multi-turn` 导入模式。

### 固定参数
- Completions 协议, httpx_async, return-token-ids。
- K256 (max-inflight), W65536 (active-work credit)。
- 1×256 actor pool, ray-batch-rows 512, token_budget=8192。
- 1 warmup + 3 formal repeats, seed=20260730（场景交错）。

## 2. 实验 1：prefix-aware 数据组织（batching）

### 设计
固定 routing = least_queued，只变 batching-policy：
- `baseline_tb`：token_budget（不按 prefix 分组）。
- `prefix_aware_tb`：prefix_aware_token_budget（同 prefix 进同批）。
- `prefix_length_align_tb`：prefix_aware_length_align_token_budget（同 prefix + 组内按长度排序）。

### 数据（model-request 中位数, 3 formal repeats）

| 场景 | mr_median tok/s | CV | vs baseline | P95 s | SLO 违约 | MFU |
|---|---|---|---|---|---|---|
| baseline_tb | 16,661 | 0.2% | — | 100.8 | 75.5% | 0.466 |
| prefix_aware_tb | 16,581 | 0.5% | -0.5% | 101.3 | 75.5% | 0.467 |
| prefix_length_align_tb | 16,780 | 0.0% | +0.7% | 100.7 | 77.1% | 0.466 |

12/12 runs ok, 0 incident, CV ≤0.5%。

### 结果解释
- **三臂 within 1.2%（中性）**：prefix-aware batching 不改善吞吐/SLO/MFU。
- **原因**：vLLM APC（automatic prefix cache, radix tree）自动检测所有到达请求的 prefix 重叠，不依赖上游 batching 顺序。上游怎么排序，vLLM 看到的请求集合一样 → cache 命中一样。
- **正面发现**：cache ON 在多轮 ShareGPT 上吞吐 ~16,700 tok/s，对比 cache OFF（BurstGPT ~13,000）**~+28%**。这是 prefix cache 本身的真实价值，不依赖上游策略。

### 不能声称
- 不能声称"prefix-aware batching 在所有 workload 上无效"——只测了 ShareGPT 多轮 + K256 + W65536。
- 不能声称"cache 在 BurstGPT 上也有 +28%"——BurstGPT 几乎无 prefix 重复，cache 收益会小很多（未测 cache-OFF 的 ShareGPT 多轮直接对比）。

## 3. 实验 2：prefix-affinity routing（运行中）

### 设计
固定 batching = token_budget，只变 routing：
- `route_least_queued_tb`：least_queued（同 prefix 散到两个 endpoint → cache 可能碎片化）。
- `route_affinity_tb`：prefix_affinity（rendezvous hash → 同 prefix 到同一 endpoint → 反碎片化）。
- `route_affinity_pala_tb`：prefix_affinity + prefix_aware_length_align 二级排序。

### 假设
如果 vLLM cache 是 per-endpoint 的（两个 endpoint 各自独立缓存），naive routing 会让同 prefix 请求散到两边 → 每边只看到一半 → 命中率碎片化。Affinity routing 集中到一边 → 命中率更高 → 吞吐更高。

### 数据
（运行完成后填写）

## 4. 对课题含义

1. **vLLM prefix cache 有真实价值**（+28% 吞吐 on 多轮 ShareGPT）——支持"在多轮/模板化 workload 上开 cache"的建议。
2. **上游数据组织（batching）在 cache ON 时不增加价值**——vLLM APC 自动处理，上游策略中性。与研究内容一的其他发现一致（routing/quantum/flush 也在 vLLM 饱和后中性）。
3. **prefix-affinity routing 是唯一还没排除的上游 prefix 策略**——如果它也不赢，说明 vLLM 内部机制完全覆盖了上游能做的所有 prefix 优化；如果它赢了，说明跨 endpoint 的 cache 碎片化是个真实问题，上游 routing 能解决。

## 5. 下一步

1. 等 routing 实验完成 → 分析 → 填写 §3 数据。
2. 如果 routing 也中性 → prefix 方向判定"vLLM 内部已覆盖，上游无额外空间"，转 OceanBase。
3. 如果 routing 有收益 → 扩展测试（不同 endpoint 数、不同 prefix 重复率）。
