# 4-endpoint prefix-affinity routing 消融实验报告

日期：2026-07-31（运行于 2026-07-31 10:06–10:11，重跑成功）
分支：`main`（manifest guard 放宽在 commit `a26c1e2`）
平台：AutoDL 2×4090，**4× Qwen2.5-1.5B-Instruct**（2 endpoint/卡），vLLM 0.25.1，PG 18.4 + pgvector
前置 2-endpoint/7B 报告：`../prefix_cache_routing_req_20260730/README.md`

## 1. 研究问题与设计

**问题**：2-endpoint/7B routing 实验中 `prefix_affinity` 相对 `least_queued` 完全中性（−0.1%，见前置报告 §4）。该报告 §6.2 把「>2 endpoint 或低 prefix 重复率 workload 下重测」列为可选扩展，§「不能声称」也明确「更高 endpoint 数（>2）下碎片化是否出现，未测」。本实验回答：**在 >2 endpoint 且 per-endpoint KV cache 更小（更高淘汰压力）的 regime 下，prefix_affinity 是否能反碎片化、产生可测收益？**

**两臂**（固定 batching=token_budget，只变 routing；相对前置报告去掉第三臂 pala，聚焦纯路由 A/B）：

| 场景 | routing | batching-policy |
|---|---|---|
| `route_least_queued_tb` | least_queued | token_budget |
| `route_affinity_tb` | prefix_affinity | token_budget |

1 warmup + 3 formal repeats，seed=20260729，formal 由 runner 交错洗牌。

## 2. 为了 4 endpoint 做的调整

### 2.1 代码调整：放宽 manifest 比较契约（commit `a26c1e2`，**本实验的真正使能点**）

**阻塞点**：`code/src/profiling/manifest_guard.py` 的 `validate_profile_manifest_contract` 原本硬编码 `if endpoint_count != 2: raise ValueError("request manifest comparison requires two endpoints")`。这是契约里唯一的 endpoint 数硬编码（协议、粒度、routing、审计其余部分都与 endpoint 数无关，且 `ProfileManifestGuard` 已校验 `endpoint_index < len(endpoint_ids)`）。4-endpoint routing 配置在 warmup 阶段就被它挡掉（exit 1）。

**改动**（`manifest_guard.py:52-62`）：

```python
-   if endpoint_count != 2:
-       raise ValueError("request manifest comparison requires two endpoints")
+   if endpoint_count < 2:
+       raise ValueError(
+           "request manifest comparison requires at least two endpoints"
+       )
```

**语义边界（写在代码注释里）**：两个 endpoint 仍是标准双卡比较；多于两个允许用于**动态分发请求的 routing 消融**（prefix_affinity / least_queued）。「每 endpoint 等量工作」的比较在 manifest 分片数上精确、超出分片数后变松——对 routing 消融可接受，**不能用于正式 pinned-comparison 排名**。

**测试**：`code/tests/observability/test_profile_manifest_guard.py` 新增 `test_profile_manifest_contract_accepts_more_than_two_endpoints`（endpoint_count=4 + prefix_affinity 通过）。既有「拒绝 1 endpoint」用例同步改为匹配 `"two endpoints"` 子串（新消息 `"at least two endpoints"` 仍命中）。

### 2.2 配置调整：`/root/autodl-tmp/gates/prefix_cache_routing_4ep_1.5b.json`

由 2-endpoint/7B 的 `prefix_cache_routing_req_20260730.json` 机械派生（`build_4ep_config.py`）。相对源配置的 delta：

| 字段 | 源值（2-ep/7B） | 本配置（4-ep/1.5B） |
|---|---|---|
| `experiment_id` | `prefix_cache_routing_req` | `prefix_cache_routing_4ep_1.5b` |
| `service_metadata.prefix_caching` | （声明失真，见前置报告 §7） | 显式 `true`（并已用 live probe 校验，见下） |
| `--completion-endpoint-urls` | 2 个（8000,8001） | 4 个（8000,8001,8002,8003） |
| `--endpoint-gpu-ids` | `0,1` | `0,0,1,1`（每卡 2 endpoint） |
| `--completion-model` / `--cost-model-id` / `--cost-tokenizer-id` | `qwen2.5-7b` | `qwen2.5-1.5b` |
| `--model-metrics-urls` | 2 个 | 4 个 |
| `scenarios` | 3 臂（含 pala） | 2 臂（去掉 `route_affinity_pala_tb`） |

其余参数（K256、active-work 65536、token_budget 8192、request 粒度、sharegpt_multiturn 2048、SLO 30000ms 等）与源配置一致，保证本实验两臂是干净的 A/B。

### 2.3 vLLM 部署调整：4× Qwen2.5-1.5B-Instruct（2 endpoint/卡）

runtime env `/root/autodl-tmp/4ep-1.5b.env`：`MODEL_PATH=Qwen2.5-1.5B-Instruct`，`GPU_IDS=0,0,1,1`，`PORTS=8000,8001,8002,8003`，`VLLM_GPU_MEMORY_UTILIZATION=0.43`（×2 endpoint/卡 ≈ 0.86，留 headroom），`--enable-prefix-caching`。

**为什么换 1.5B（实验杠杆）**：1.5B 模型更小 → 每 endpoint 的 KV cache 更小 → 2048 行 ShareGPT 的 working set 不再被 per-endpoint APC 完全覆盖 → 产生**真实淘汰压力**。这是本实验相对 7B/2-ep 的关键不同：7B/2-ep 下 APC 已覆盖 working set → 路由中性；1.5B/4-ep 下 APC 不够覆盖 → 路由效应应该显现。代价：1.5B≠7B，model size 与 endpoint 数混在一起（见 §7 不能声称）。

**live prefix-caching 校验**：启动后 `vllm_probe` live 探测返回 `True`，4 个 endpoint 进程均带 `--enable-prefix-caching`，与配置声明值一致（前置报告 §7 的 manifest 失真问题已在 commit `37431b6`/`b00dc10` 修复，本次不再失真）。

### 2.4 环境调整：清理 stale Ray pointer（主机重启后）

主机重启后首次 launch 失败：`/tmp/ray/ray_current_cluster` 残留重启前的死地址 `172.17.0.8:6380`（重启后容器 IP 实际变为 `172.17.0.3`），`ray.init()`（无 `--ray-address`、无 `RAY_ADDRESS`）读取该 stale 指针后反复连接死 GCS 共 ~14 分钟，最终 `ConnectionError`。修复：删除该指针文件（无活跃 Ray 进程需要 stop）。详见 §4 与 `deploy/autodl/README.md` 开机恢复流程。

## 3. 实验设置

- Completions 协议, httpx_async, return-token-ids。K256 (max-inflight), W65536 (active-work credit)。1×256 actor pool，ray-batch-rows 256，token_budget=8192，**request 粒度**（`prefix_affinity` 的 per-request rendezvous hash 只在 request 级路由才有意义，`manifest_guard.py:82-93` 强制）。
- **prefix cache ON**（4 endpoint 均带 `--enable-prefix-caching`，live probe = True）。
- 数据：`sharegpt_multiturn`（2,048 行，766 session，89% 行共享 prefix_key），请求 manifest `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。
- 配置：`/root/autodl-tmp/gates/prefix_cache_routing_4ep_1.5b.json`。结果目录（远端）：`experiments/results/prefix_cache_routing_4ep_1.5b_20260731/`。manifest：`status=completed`，8/8 ok，**0 incident**。

## 4. 失败与重跑（事故记录）

- **首次 launch 失败**（2026-07-31 09:42–09:56）：runner 在第一个 warmup 的 `ray.init()` 卡死 ~14 分钟后 `ConnectionError`，0 请求发出。根因：stale `/tmp/ray/ray_current_cluster`（见 §2.4）。诊断依据：`000_warmup_1_route_least_queued_tb.stderr.log` 的 GCS connect-timeout 堆栈 + Ray 自身提示「previous Ray instance that has since crashed」。
- **修复**：删除 stale 指针（无活跃 Ray 进程）。重跑 warmup stderr 出现 `INFO worker.py:2015 -- Started a local Ray instance.`，2 个 warmup 均成功，0 incident。
- **证据保留**：失败首跑目录原位保留为 `experiments/results/prefix_cache_routing_4ep_1.5b_20260731_failed_raystale/`（不删——其 stderr 记录了 14 分钟挂起的根因）。
- **回归防范**：在 `deploy/autodl/README.md` 开机恢复流程增加「清理 stale ray pointer」步骤。

## 5. 实验数据（model-request tok/s 中位数，3 formal reps）

| 场景 | routing | tp 中位数 | raw | CV | SLO 违约 | P95 |
|---|---|---|---|---|---|---|
| route_least_queued_tb | least_queued | 44,317 | [44425, 44317, 43882] | 0.6% | 31.4% | 39.31s |
| route_affinity_tb | prefix_affinity | **46,943** | [47604, 46861, 46943] | 0.9% | **25.1%** | **36.16s** |

## 6. 结果解释

### 事实
- **纯路由效应**（affinity vs least_queued）：46,943 vs 44,317 = **+5.9%**。3 repeat 完全不重叠（affinity 最小 46,861 > least_queued 最大 44,425），CV ≤0.9%。**跨过 5% 晋升门禁**。
- **SLO**：25.1% vs 31.4% = **−6.3pp**；**P95**：36.16s vs 39.31s = **−3.15s**。affinity 在吞吐和 tail latency 上同时更优。

### 推断
- **cache 碎片化假设在本 regime 下被支持**：1.5B/4-endpoint 的 per-endpoint KV cache 不足以覆盖 working set，`least_queued` 把同 prefix 散到 4 个 endpoint 使每端命中率碎片化；`prefix_affinity` 把同 prefix 钉到同一 endpoint，反碎片化、提高 per-endpoint APC 命中率、减少重算 → 吞吐升、tail 降。
- **相对 2-ep/7B 的中性结论部分翻盘**：前置报告的「vLLM APC 已覆盖上游 prefix 优化」在 APC 够大时成立，在 APC 不够覆盖 working set 时不再成立——此时上游 routing 重新有空间。

### 不能声称
- **不能干净归因于「endpoint 数」单一变量**：本实验同时改了 model（1.5B vs 7B）、endpoint 数（4 vs 2）、per-endpoint KV 大小。+5.9% 是「高淘汰压力 regime」的效应，不是「4 endpoint」本身。要单独隔离 endpoint 数，需补 4-endpoint/7B 或 2-endpoint/1.5B。
- **不能声称「affinity 普遍有效」**：只测了 ShareGPT 多轮（89% 共享 prefix）+ 1.5B/4-ep/K256/W65536/request 粒度。低 prefix 重复率 workload 未测。
- **两臂都处于过饱和 regime**：SLO 违约 25–31% 表明 1.5B 在该负载下 cache 抖动严重。**相对比较成立，绝对数字是 thrashing 区间**——跨过 5% 门禁应审慎，需隔离消融确认后才正式晋级。
- **per-arm APC 命中率仍未单独记录**（与前置报告同一缺口）：`resources.csv` 只采样 KV 用量，命中率按 engine 连续打印、formal 交错使事后无法干净归因到单臂。吞吐 + tail 的方向性差异间接说明 cache 行为臂间不等价。

## 7. 对课题含义

1. **prefix 路由方向有条件地重新打开**：2-endpoint/7B 的中性结论在该 regime 成立，但 4-endpoint/1.5B（高淘汰压力）下 `prefix_affinity` 重新显现 >5% 收益。这是首个 >2 endpoint 数据点，直接回应前置报告 §「不能声称」的缺口与 §6.2 的扩展项。
2. **强相关地指向 Mooncake/共享 KV cache 方向**：25–31% SLO 违约 + affinity 的收益共同表明该 regime 的瓶颈是 KV cache 淘汰/重算。affinity 在 routing 层抓住了一部分价值；共享 CPU KV 池（淘汰后按需 reload GPU，即用户讨论的 Mooncake 思路）会在 cache 层抓住这部分价值——这正是该方向值得投入的实证信号。
3. 不改变上游调度主线，但为「跨引擎 KV 管理」作为潜在第二贡献提供了首个动机数据点（需与导师确认是否纳入）。

## 8. 下一步

1. **隔离消融**：4-endpoint/7B 或 2-endpoint/1.5B，把 endpoint 数与 model size 解耦，确认 +5.9% 中有多少来自「更多 endpoint」、多少来自「更小 KV」。
2. **补 per-arm APC 命中率指标**：在 runner resources 采样中增加 vLLM `prefix_cache_hit_rate`（Prometheus 或 engine log 解析），使 cache 相关实验能直接归因。
3. **连接受 Mooncake/共享 KV cache 方向**：本实验的高淘汰 regime 是该方向的价值验证场景（待用户决定是否启动 Phase 0 设计）。
4. 同步 `experiments/plans/experiment_status_and_gaps.md` 与 `PROJECT_OUTLINE.md`：prefix 方向状态从「收口/中性」改为「2-ep/7B 中性、4-ep/1.5B 有条件 +5.9%，待隔离消融」。
