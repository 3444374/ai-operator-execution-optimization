# SAOR 当前签名 feeding ceiling（2026-08-14）

## 1. 实验目的

在不重跑六臂 rehearsal、不修改 K/W、$0.125W_e$ 或 95% 门槛的前提下，只补一个当前
model/tokenizer/template/service signature 的 `direct_no_job` bounded ceiling，判断 Project SAOR
路径是否达到预注册的 feeding-saturation 要求：

$$
\rho_{feed}=\frac{T_{SAOR}}{T_{direct}}\ge 0.95.
$$

direct cell 不使用 bounded-ready、shared credit 或 SAOR；HTTP client 按每 endpoint K128 有界提交，
随后由 vLLM FCFS + continuous batching 调度。它是服务容量 ceiling，不是具有公平/SLO 语义的
算法 baseline。

## 2. 实验设置

| 项目 | 冻结值 |
|---|---|
| ceiling 代码 | `c988622a643699925faeeb3cecc4c351913b728b` |
| Project reference | 封存 `63d17300` 六臂 root 中的 SAOR cell |
| 硬件/服务 | 2×RTX 4090；vLLM 0.25.1；Qwen2.5-7B；2 endpoint；FCFS |
| 服务特性 | chunked prefill、prefix cache、`max_num_batched_tokens=8192`、`max_num_seqs=256`、GPU memory utilization=0.9 |
| workload | long bulk 512@0s + short foreground 512@5s；相同 immutable manifest SHA |
| 协议/work | chat-completions；raw prompt；template overhead=29；fixed output cap=256 |
| client bound | K128/endpoint；direct 不应用 Project W/credit admission |
| 数据路径 | immutable manifest → bounded HTTP direct client → vLLM；无 PostgreSQL/Daft/Ray Job scheduler |
| 重复 | 单个 current-signature ceiling cell；用于 feeding gate，不作策略稳定性排名 |

环境 preflight 为 `status=ok`；PostgreSQL、两个 endpoint 与 Ray 均健康/空闲。运行前 wrapper 逐字段
比较 reference/ceiling 的 endpoints、service metadata、common args、typed work cost、K/W、
calibration、manifest、rows 与 arrival，并重新核验 model/tokenizer/template identity。

服务器从仓库外 runtime/formal env 加载连接和资产路径后执行：

```bash
PYTHONPATH=code "$DRIVER_PYTHON" \
  code/scripts/experiments/run_saor_feeding_ceiling.py \
  --rehearsal \
  --evaluation-contract deploy/autodl/saor_project_mechanism_formal_contract.json \
  --reference-config deploy/autodl/saor_project_mechanism_formal.example.json \
  --config deploy/autodl/saor_project_feeding_ceiling.example.json \
  --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
  --python-executable "$DRIVER_PYTHON" \
  --output-dir "$ARTIFACT_ROOT/saor_project_feeding_ceiling_<unique-id>" \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --ray-address "$RAY_ADDRESS"
```

## 3. 严谨性自检

- manifest `completed`，1/1 cell，0 incident；repository commit 为 `c988622a...`；
- 两 Job expected/completed 均为 512，`job_exactly_once=[true,true]`，endpoint successes=1,024；
- prompt token delta=636,378，与封存 SAOR cell 完全一致；generation delta=234,010；
- metrics/resources/MFU 均为 `ok`；两个 endpoint 都收到请求；
- 独立 summarizer 比较相同 manifest、arrival、row count、success 与 prompt work 后，给出
  `evidence_valid=true`；
- 95% 是六臂运行前已存在的项目规则，本次没有依据结果调整。

运行历史中有两个不计入结果的失败 root：首个 foreground SSH 进程被会话挂断，留下
`running/0-completed`；第二个全新 root 暴露 direct adapter 未显式返回
`expected_count/completed_count/exactly_once`，以 `KeyError` fail closed。`c988622a` 在
`validate_results()` 成功后结构化写出这三个字段，119 tests + 7 subtests 通过；最终结果来自第三个
全新 root `...c988622a...retry2`，没有 resume 或拼接失败产物。

## 4. 实验数据

### 4.1 Feeding 判决

| 路径 | tokens/s | 相对 direct | feeding gate |
|---|---:|---:|---|
| current direct bounded ceiling | 13,684.90 | 100% | reference |
| sealed SAOR rehearsal | 12,713.03 | **92.898%** | **failed (<95%)** |

绝对吞吐差为 971.87 tok/s，SAOR 相对 ceiling 低 7.10%。这一结果几乎复现 2026-08-12 高度
匹配 direct ceiling 得到的 92.96%，因此历史信号现已被当前完整 artifact identity 证实。

### 4.2 Ceiling 服务与资源形状

| 指标 | 当前 direct ceiling | 含义 |
|---|---:|---|
| duration | 63.602s | group 服务窗口 |
| vLLM request latency mean | 12.821s | 全请求模型端平均延迟 |
| queue/prefill/decode mean | 2.204 / 0.805 / 9.573s | vLLM counter 分解 |
| TTFT P95/P99 | 13.173 / 18.635s | 全请求混合分布 |
| ITL P95/P99 | 0.100 / 0.685s | 全请求混合分布 |
| running mean/P95/max | 161.38 / 256 / 256 | 两 endpoint 总和 |
| waiting mean/P95/max | 37.99 / 88 / 138 | 两 endpoint 总和 |
| KV mean/P95/max | 0.687 / 0.999 / 1.000 | 分数，1=100% |
| GPU util mean/P95 | 98.83% / 100% | 双卡平均忙碌采样 |
| MFU | 55.39% | 当前项目 FLOPs 口径 |
| GPU power mean/P95 | 855.71 / 899.57W | 双卡聚合采样 |
| GPU energy | 54.28kJ | cell 边界内梯形积分 |
| energy / 1K observed tokens | 62.36J | 不含 CPU/整机基础功耗 |

direct 的 bulk/foreground JCT 为 50.873/58.594s，P99 为 44.212/53.378s，30s SLO miss 为
53.52%/98.63%。这些数说明 ceiling 通过把 vLLM 推到 waiting/KV 极限获得吞吐，不表示它在 SLO、
隔离或公平上优于 SAOR。feeding gate 只问 Project 路径是否充分利用同协议容量。

## 5. 结果解释

### 事实

1. 当前签名 ceiling evidence 有效，但 SAOR feeding ratio 只有 92.898%，未过预注册 95%。
2. direct MFU 55.39%，SAOR 47.91%；而两者 GPU utilization 都接近 100%，再次证明只看 GPU
   utilization 会掩盖约 7%的服务吞吐差。
3. SAOR 的 foreground P99/SLO 显著好于 direct ceiling，但这不修复 feeding failure；两者回答的
   是不同问题。

### 推断

当前 K128/W65,536 Project execution path 或其 admission/actor plumbing 仍留下约 7%的 raw service
capacity gap。仅凭本 cell 不能区分差距来自 W envelope、actor/direct transport 或 Project 路径固定
开销；继续定位需要新的诊断命题，不能在已经冻结的 formal 合同内调参追正。

### 不能声称

- 不能声称 SAOR 已获得正式性能晋级或胜过 FIFO/DRR/VTC；feeding 前置门失败。
- 不能把 direct 的高 waiting/KV 与差 SLO解释成 SAOR 算法胜出；direct 只是 capacity ceiling。
- 不能降低 95% 门、提高 K/W 或重跑六臂直到通过。
- 不能外推到 4 Job、其他硬件、原生 Daft/Ray Data 或图像 workload。

## 6. 对课题的含义

`63d17300` 仍是有效的机制 rehearsal：projected-debt recovery、service lag 与 work conservation
证据不被推翻。但当前 Project execution path 没有达到正式性能归因要求，所以本轮 SAOR
`1 warm-up + 3 formal` 应停止。论文可报告“机制闭环 + valid feeding-negative”，不能报告稳定的
constrained-Pareto 胜出。

## 7. 下一步

1. 将 formal contract 保持 `formal_authorized=false`，状态改为 `locked_failed_feeding`；
2. 不运行当前 1+3，不改本合同的 workload、K/W、cap 或门槛；
3. 若继续研究，另立 diagnostic：在不改变 selector 的前提下分解 W envelope、actor transport 与
   direct HTTP 的 7.10% gap；只有形成新的、预注册且重新 rehearsal 的候选合同，才可开启另一轮
   formal，而不是复活本合同追正；
4. 原生 Daft/Ray Data 系统层比较与图像异构执行模型保持独立，不用本 feeding-negative 替代。

## 证据与完整性

- [feeding_validation.json](raw/feeding_validation.json)：SHA256
  `6c656f25b8128fe102a06b65093c8be7e593f182029febc68201af265cdba3d5`
- [group_runs.csv](raw/group_runs.csv)：SHA256
  `869e44fde23e36c4f8f161edd643b5264c07ca8a50aba453c3eeb81fa78fa489`
- [manifest.json](raw/manifest.json)：SHA256
  `cc708717c2b1a1debe417365ae8b17b527e063d9bcfb445e7613b9a524914e34`
- [feeding_ceiling_contract.json](raw/feeding_ceiling_contract.json)：SHA256
  `e4608a84f0710d756fa7e2812472f8bd0284893247da913723efc3cbf9de8e92`
- [environment_preflight.json](raw/environment_preflight.json)：SHA256
  `9a82a3e1673be68b27d885b67ddd1acf8a6295e1dbc676f66b34c2a93cc4e59b`
- [完整 12-file archive](raw/saor_project_feeding_ceiling_c988622a_20260814_retry2.tar.gz)：SHA256
  `ebf5c35a699ff034891855d14c3332dbe42dabef3ede1f0641d3ac18a4079fb2`
