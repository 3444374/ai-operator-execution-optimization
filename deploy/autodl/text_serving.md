# 文本模态（vLLM 生成式 LLM）推理服务引擎——部署与使用

> 本项目按**数据模态**分部署文档：**文本（本篇）**、图像（`image_serving.md`），后续 video/audio 各起一篇。各模态共享同一套"调度策略模态无关"框架（见 `deploy/autodl/README.md` 总览），本篇只写文本独有部分。
> **共享平台 setup**（实例/venv/network_turbo/代码同步/模型下载方法/PG）在 `deploy/autodl/README.md` §1–§7，本篇不重复。
> 文本实验结果在 `experiments/results/`（RC1 数据组织、K_max、active-work、routing 等）；本篇只讲"引擎是什么 + 在服务器上怎么部署/跑"。

## 1. 这个"模态"是什么

- **数据模态 = 文本**。workload 每一行是一段 **prompt 文本**（如 ShareGPT 多轮对话的 prompt）。
- **AI 算子 = `AI_COMPLETE`**：把 prompt 送给 LLM，**生成**一段补全文本（token 序列），不是定长向量。
- **模型 = Qwen2.5-Instruct**（1.5B 主用 / 7B 大模型对照）。生成式 decoder-only LLM。

### vLLM 是什么（核心，务必理解）

**vLLM = 一个开源的大模型 serving 引擎**（不是模型本身）。它把一个生成式 LLM 包装成高吞吐的 HTTP 推理服务，核心机制：
- **Continuous batching**：动态把多个在途请求合成一个 batch，请求随到随合（不是固定 batch 等满），GPU 持续满载。
- **PagedAttention / KV cache**：把每请求已算 token 的 key/value 缓存按页存在 GPU 显存（"KV 池"），复用避免重算；显存预算满了就淘汰旧页。
- **APC（Automatic Prefix Caching）**：自动缓存重复 prompt 前缀的 KV，后续同前缀请求跳过 prefill 重算（本项目 `--enable-prefix-caching` 开启）。

**本项目对 vLLM 的定位**：**部署平台，不修改其内部**。我们研究的是 vLLM **上游**怎么组织/调度请求（数据组织策略 + 提交控制策略），不是改 vLLM 的 batching/attention。所有文本实验把 vLLM 当"现实的 GPU 计算端点"，在上游 Ray 层做调度优化、观测 vLLM 的服务状态（队列、KV、命中率）来反馈调度。

> 文本 payload 每行 ~1KB，CPU→GPU 搬运很轻——所以文本 regime 的 binding 瓶颈在 **vLLM serving 侧**（KV 压力、prefill、batching），不在数据搬运。RC1 实测 `db_fetch` 1.4–2.4s vs `model_wall` 27–37s 证实。这也是为什么"找数据搬运瓶颈"要切到图像（见 `image_serving.md`）。

## 2. vLLM 引擎 vs CLIP 引擎（关键差异，与 `image_serving.md` §2 对称）

| 维度 | vLLM（本篇，文本） | CLIP（`image_serving.md`，图像） |
|---|---|---|
| 模型类型 | 生成式 LLM（Qwen2.5） | embedding 模型（CLIP image encoder） |
| 输出 | token 序列（可变长） | 定长向量（512d） |
| 服务机制 | continuous batching + APC（prefix cache）+ KV cache（PagedAttention） | 批量 embedding（**无 KV / 无 prefix / 无生成**） |
| 关键观测 | `prefix_cache_hit_rate` / `kv_cache_usage_perc` / `running·waiting` / TTFT / TBT-ITL | CPU decode+resize / CPU→GPU transfer / GPU embed 计时 / endpoint 队列深度 |
| 部署 | `start_endpoints.sh`（本篇 §3 + README §8） | `image_serving.md`（FastAPI endpoint + Ray CPU decode） |

**适用策略**：vLLM 文本侧验证了 prefix-aware routing/grouping（RC1 regime-dependent）、active-work/K_max/flush/queue-adaptive（这些**模态无关**，图像侧也适用）。

## 3. 环境配置（在 AutoDL 服务器上怎么配）

### 3.1 前提（共享平台，不重复）
先按 `deploy/autodl/README.md` §1–§7 完成：AutoDL 2×4090、`venvs/vllm-4090`、`network_turbo`、代码 git 同步、PostgreSQL+pgvector。

### 3.2 Qwen 模型下载
Qwen2.5 模型用 `deploy/autodl/download_model.sh`（封装了 turbo + 禁 Xet + 校验），或直接 Python `snapshot_download`：
```bash
source /etc/network_turbo 2>/dev/null
export HF_HUB_DISABLE_XET=1 HF_HUB_ENABLE_HF_TRANSFER=0
bash /root/autodl-tmp/ai-operator/deploy/autodl/download_model.sh   # 读 runtime env 的 MODEL_PATH
# 或: /root/autodl-tmp/venvs/vllm-4090/bin/python -c "
# from huggingface_hub import snapshot_download as s
# s('Qwen/Qwen2.5-1.5B-Instruct', local_dir='/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct')"
```
模型放 `/root/autodl-tmp/models/Qwen2.5-*-Instruct`，由 runtime env 的 `MODEL_PATH` 指向。**注意**：和 CLIP 一样，hf_hub 1.x 的 `huggingface-cli` wrapper 有问题，优先 Python API 或 `download_model.sh`。

### 3.3 启动 vLLM endpoint（`start_endpoints.sh`）
vLLM 用项目脚本起，**env 驱动**（读 runtime env 或命令行 export）：
```bash
set -a; source /root/autodl-tmp/ai-operator-runtime.env; set +a
export GPU_IDS=0,1 PORTS=8000,8001 VLLM_GPU_MEMORY_UTILIZATION=0.9 VLLM_MAX_MODEL_LEN=2048
export VLLM_EXTRA_ARGS="--enable-prefix-caching --max-num-seqs 256 --max-num-batched-tokens 8192"
bash /root/autodl-tmp/ai-operator/deploy/autodl/start_endpoints.sh
# 脚本自轮询 /health；首次 JIT 编译 ~2–5 分钟（后续秒级）；打印每端口 models + GPU 进程
```
- **拓扑**：`2-ep/0.9`（1 endpoint/GPU，util 0.9，大 KV 池、低淘汰）或 `4-ep/0.43`（2 endpoint/GPU，util 0.43，小池、KV 饱和——consolidation 压力 regime）。
- `--enable-prefix-caching` 必须开（prefix 实验的前提；runner 会 fail-closed 校验 service_metadata.prefix_caching 与 live vLLM 一致）。
- 详细的"全新/开机恢复/正式启动"步骤见 `deploy/autodl/README.md` §8 + 开机恢复流程。

## 4. 数据集（哪来 / 怎么配）

### 4.1 ShareGPT 多轮对话（文本主 workload）
- **`sharegpt_multiturn`**（2048 行）：prompt_tokens 3–1486，target_output 1–256。manifest 在 `/root/autodl-tmp/gates/sharegpt_multiturn_2048.jsonl`。
- **来源 + 生成**：从 ShareGPT 多轮对话数据切分 + 生成 request manifest（每行 = 一个独立完整的 vLLM 请求：prompt + 预期 output token 上限）。生成脚本与历史在 `data/`（见 `data/README.md`）。
- **怎么进 pipeline**：PostgreSQL `documents` 表（`workload_name='sharegpt_multiturn'`）→ Daft 读 `df["prompt"]` 列 → Ray actor 组织成 batch 请求 → vLLM `/v1/completions` →（`--writeback-mode none` 时不写回 / 或写回 pgvector）。

### 4.2 其他文本 workload（对照）
`sharegpt_concentrated`（prefix 集中度更高，prefix routing 泛化对照）、`sharegpt_burstgpt`（早期，**已废弃**——`SOURCE_WORKLOAD_NAME` env 仍残留 burstgpt 是 stale，实际用 multiturn）。详见 `data/README.md`。

## 5. 怎么操作（跑文本实验）

### 5.1 runner（`run_ai_operator_scenarios.py`）
文本实验统一用项目 runner，**config 驱动**（scenario JSON，`deploy/autodl/*.example.json` 是模板）。完整命令（**所有参数必填**，漏 `--metrics-urls` 等会在 argparse 阶段退出）：
```bash
cd /root/autodl-tmp/ai-operator
set -a; source /root/autodl-tmp/ai-operator-runtime.env; set +a
nohup /root/miniconda3/bin/python code/scripts/experiments/run_ai_operator_scenarios.py \
  --config /root/autodl-tmp/gates/<scenario>.json \
  --profiler code/scripts/profiling/postgres_ai_operator_profile.py \
  --python-executable /root/miniconda3/bin/python \
  --output-dir <OUTPUT_DIR> \
  --health-url http://127.0.0.1:8000/health \
  --metrics-urls "$MODEL_METRICS_URLS" \
  --idle-timeout-s 120 \
  </dev/null ><LOG> 2>&1 &
```
- runner 把 config 里的 `${VAR}` 从 env 展开（所以 runtime env 要 source）。
- 合同：K256 inflight / W65536 active-work / token_budget / request 粒度 / fixed-50ms flush / least_queued routing / 1 warmup + 3 formal（formal 交错）/ 喂饱门禁（E2E ≥95% of 同协议 bounded）。详见 `AGENTS.md` §7.5。

### 5.2 feeding-saturation 门禁（bounded baseline）
文本侧的"喂饱 vLLM"参照 = 同协议 bounded HTTP client（`run_official_baseline_gate.py`，batched cells b16/b32）。2-ep/0.9 bounded 真上限 ~79,488 tok/s。注意：bounded gate 原硬限 2 endpoint，现已放宽 ≥2（`gate_runner.py` `!= 2` → `< 2`）。详见 `AGENTS.md` §7.5.C + `experiments/results/rc1_data_organization/`。

### 5.3 完整流程
全新/开机恢复/正式启动/gate 的**逐步命令**在 `deploy/autodl/README.md`（"全新实例从零准备" / "开机后完整恢复流程" / "实验 gate 与正式启动"）——本篇只到引擎层。

## 6. 注意事项（坑汇总）

| 坑 | 表现 | 解法 |
|---|---|---|
| stale Ray 指针 | `/tmp/ray/ray_current_cluster` 主机重启后留着死地址，`ray.init()` 卡 ~14min | 重启后先 `rm -f /tmp/ray/ray_current_cluster`（README 开机恢复 §1.5） |
| `--metrics-urls` 漏 | runner argparse 直接退出，不创建 manifest | 必填（见 §5.1 命令，不能凭记忆缩写） |
| service_metadata 与 live vLLM 不符 | runner fail-closed（`prefix_caching`/`gpu_memory_utilization`/`max_num_batched_tokens` 等要对上） | 起的 vLLM 参数要和 scenario config 的 service_metadata 一致 |
| 首次 JIT 慢 | 第一个请求触发 kernel JIT，~2–5 分钟（误以为卡死） | 正常，`start_endpoints.sh` 会轮询到 ready |
| `vllm_kv_cache_usage_perc` 是分数非 % | 0.06 = 6%（不是 0.06%），曾误读成"指标坏" | 按分数 ×100 读；指标本身正常（见 `kv_budget_sweep` 纠正） |
| prefix-caching 没开 | prefix 实验结论无意义 | `--enable-prefix-caching` 必开 + runner 校验 |
| bounded gate 多 endpoint | 原 `run_official_baseline_gate` 硬限 2 endpoint | 已放宽 ≥2（`gate_runner.py`），4-ep bounded 可跑 |

## 7. 关联文档
- 共享平台 + 总览：`deploy/autodl/README.md`
- 图像模态（对称篇）：`deploy/autodl/image_serving.md`
- 文本实验结果：`experiments/results/`（RC1 数据组织、routing、active-work、K_max 等）
- 实验合同 + 喂饱门禁：`AGENTS.md` §7.5
- 数据生成 + manifest：`data/README.md`
- 文本实验配置模板：`deploy/autodl/*.example.json`
