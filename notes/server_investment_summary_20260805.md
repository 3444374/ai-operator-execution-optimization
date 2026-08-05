# 双 4090 云服务器租赁：必要性与产出总结（2026-08-05）

本文面向导师与出资方，说明本课题研究对计算环境有什么需求、为何必须租赁一台双 GPU 云服务器
（AutoDL 2×RTX 4090）、为什么是双卡而非单卡，以及租赁至今在服务器上完成的全部工作与产出。
所有结论均可在仓库结果目录中追溯，数字来自对应报告 README。

## 1. 研究需求（决定服务器配置的源头）

本课题四块研究内容，每一块都对计算环境有明确要求，叠加起来正好对应"双卡 + 大显存 + 独占长跑 +
多组件共存"：

| 研究内容 | 对环境的要求 |
|---|---|
| 研究内容一：数据组织策略（按 token/帧计算量组织请求、按计算量相似度分组） | 要在"容量无压力"和"KV 缓存满载"两种运行条件下对比策略 → 需要能把 KV 缓存撑饱和的大显存（2×24GB） |
| 研究内容二：调度与提交控制（多 endpoint 路由 / 公平队列 / credit / 提交节奏） | 必须 ≥2 个模型服务 endpoint 并存 → 至少 2 张 GPU（或一张卡塞多 endpoint） |
| 多模态泛化验证（图像 AI_EMBED / AI_CLASSIFY，CLIP） | GPU actor 做 embedding + CPU 做 decode/resize/normalize → 需要 GPU + 足够 CPU/内存 |
| 算子代价估计（共同使能组件，喂给调度决策） | 要扫（workload × 行数 × 输出上限 × active-work）大量组合的 profile → 独占 GPU 长时间跑（320-run 约 3.5–4h） |

外加两条项目规则强加的硬约束：

- **厂商原生 baseline 对比**（formal baseline 必须由被测系统自有调度，不许项目替它调度）→
  同机要同时承载 vLLM 服务 + PostgreSQL + Ray + Daft + DuckDB 等多组件。
- **可复现 + 正式实验不在线调参** → 需要独占、不被他人干扰的机器，加校准签名冻结配置。

这些需求叠加，单卡个人机（单 endpoint、12GB、不能长跑、环境漂移）无法满足；双 4090 云服务器
是满足全部研究需求的最小可行配置。

## 2. 为什么必须租赁服务器（本地单 5070 不够）

本课题研究方向是**数据库 AI 负载的外部执行链路优化与调度**，研究对象是"数据如何组织成请求、
以什么节奏提交给模型服务、如何根据模型服务状态调节并发"。这些问题的最小可验证单位是
**多个模型服务 endpoint 之间的调度**，而不是单卡推理本身。因此：

| 需求 | 本地单 5070 | 租赁双 4090 |
|---|---|---|
| 多 endpoint 调度（研究内容二的核心） | ❌ 单卡只能 1 endpoint，无法研究多 endpoint 路由/公平/credit | ✅ 2 卡可做 2-endpoint 干净基线 + 4-endpoint 合并两种运行条件 |
| 显存（7B 服务 + CLIP + KV 饱和实验） | ⚠️ 12GB 紧张，7B+prefix-cache 难以饱和 | ✅ 2×24GB，可构造 KV 缓存满载运行条件 |
| 持续多小时正式实验 | ❌ 320-run 代价估计 ~3.5–4h、图像 60K×2 数十分钟，不能占用个人机 | ✅ 独占、可长跑 |
| 隔离可复现环境 | ⚠️ 个人机环境漂移 | ✅ codex/Claude 双环境 + 共享 Ray + host-scope lease + 校准签名 |

本地 5070 仅用于 smoke 与单 endpoint 预演；课题主线证据（不同容量压力下的调度分化）
在单卡上**根本无法产生**。

## 3. 为什么是双 GPU（不是 1 张、也不是更多）

双卡是支撑课题两组核心运行条件的**最小配置**，多了浪费、少了缺其中一组：

- **条件一：2 个 endpoint、容量无压力**——每个 endpoint 独占一张 4090（gpu-memory-utilization 0.9），
  KV 缓存池很空，是干净对照。
- **条件二：4 个 endpoint、KV 缓存满载、容量互相抢**——每张 4090 放 2 个 endpoint（各 0.43 util），
  KV 缓存池饱和（max 98–100%），模拟真实部署的容量竞争。

课题当前**最强的主线证据**正是在这两组运行条件之间出现的**质变**：

> 数据组织策略在"2 endpoint、容量无压力"时近似中性（5 策略 50–56k），但在"4 endpoint、
> KV 缓存满载"时分化达 27%、且**排名反转**（sequential > fixed >> row_cap ≈ best_fit
> > length_align），机制是重排序类 organizer 打散 prefix 组导致命中率从 0.60–0.76 塌到
> 0.06–0.07。

这个"效果随容量压力而变"的结论**只能在多 endpoint 多卡环境观察到**，是单卡实验永远无法发现的，
也正是课题"上游调度价值在什么条件下显现"的核心回答。同理，prefix-affinity routing 的
+5.9%（4-ep/1.5B，跨门禁）也只能在多 endpoint 上测出。

## 4. 投入产出：服务器上完成的工作（均可追溯）

### 4.1 GPU-backed 主动机证据（图像 AI_EMBED）

回答"为什么要优化数据库 AI 负载的执行链路"——这是开题的立题依据。

- 5K COCO + CLIP 端到端画像（`motivation/results/gpu/` 多个画像）：**GPU 严重没喂饱**
  （Daft 自带 / Ray Data 原生 baseline 的 GPU 利用率都只有 1–4%），真正的瓶颈是 **CPU 上的图像
  decode/resize/normalize**（单张 CPU 准备 4.4–4.8 毫秒，是 GPU 算一张的 13–31 倍），不是 GPU 算力、
  不是数据搬运、也不是数据库读取。
- 杠杆验证：加 CPU 预处理进程是唯一显著有效的手段（1→16 个 CPU 进程：冷启动端到端 34.9 秒→11.5 秒，
  143→437 张/秒）。
- 注意区分：开题报告里的 fine vs coalesced **37.5×（推理阶段）/ 13.4×（端到端）是早期文本 AI_EMBED
  预研（2026-07-12，文本向量）的数字，不是图像 CLIP 的**；图像的对应动机是上面的"GPU 没喂饱 + CPU
  预处理瓶颈"。

### 4.2 图像 AI_EMBED 正式实验（schema-v12，派生指标全部 image-safe）

- 256-row 3-arm 门禁、3-arm 12K 一致性、2-arm 60K matched-resource（project vs Ray Data，
  cpu8/cpu16 因果隔离）。
- **每图指标**：joules/image、GPU-s/image、CPU-core-s/image、streaming-onset——
  图像轨不再机械套用文本的 token 指标（`experiments/results/image_ai_embed_operator_formal_20260803/`）。
- 项目自写代价模型（CE0–CE6 六档层级 + context-LOO 评价）的 profile 数据采集。

### 4.3 强 baseline 实现（项目规则：formal baseline 必须由被测系统自有调度）

- **Daft built-in embed**（5K 校准 + 60K 长跑）与 **Ray Data native graph**（5K + 60K×2）——
  均为厂商原生执行路径，项目只做数据源/sink/指标采集，不注入项目调度。
- **DuckDB `ai` 社区扩展** baseline：已接入项目 baseline 框架为原生 adapter（
  `code/src/baselines/text/products/duckdb_ai.py`），set-oriented `ai_complete`，扩展自有调度。
- OceanBase CE `AI_COMPLETE` 能力 gate、vLLM CLIP pooling capability gate、ImageNet/ResNet18
  vendor-code parity 准备（代码 SHA + Daft 0.6.2 venv + S3 接入确认）。

### 4.4 文本轨多 endpoint 调度证据（随容量压力而变）

- RC1 数据组织 5 策略对照（2-ep 中性 vs 4-ep 27% 分化 + 排名反转，机制 `prefix_group_ratio`）。
- prefix-affinity routing（2-ep/7B 中性 −0.1%、4-ep/1.5B **+5.9%** 跨门禁）。
- KV-budget × prefix-affinity sweep、active-work 八档曲线、shared request/work credit 多 job
  公平性、complete-row service quantum、SLO-aware EWMA flush、4-endpoint prefix_cache hit rate 归因。
- 320-run 算子代价估计 formal：首次运行因双 runner 并发 + 空 `--ray-address` 各起 local Ray 被
  codex 审计判无效；codex 修正（host-scope lease + 空 flag fail-closed + live prefix-caching 探测）
  后，由 cache-on gate 验证通过（0 local-Ray、共享 Ray、exactly-once、cache counter 一致），
  待干净单 runner 重跑。

### 4.5 跨机器可复现的部署与运行时合同

- `manage_environment.py` preflight（自动硬件识别 + machine profile 选择）。
- 仓库外 `runtime.env`、driver/vLLM venv 隔离、host-scope lease 防并发 runner。
- 校准签名合同（机器+模型+协议+workload 签名一致才可复用冻结配置）。

## 5. 量化产出

- **~50 个结果目录**：`experiments/results/` 40+、`motivation/results/gpu/` 9、`feasibility/results/` 3，
  每个含 README + 原始 CSV/manifest，可在仓库中追溯。
- **多张可投稿图**（per-image 资源效率、容量压力对比、prefix 命中率归因等）。
- **可复现的 runner + gate + 观测 pipeline**：matrix runner（single-writer lease + fail-closed gate）、
  baseline gate runner（双 endpoint 公平性 + exactly-once + service-counter 一致性）、
  观测采样器（vLLM Prometheus + nvidia-smi + MFU）。

## 6. 诚实边界

- 320-run 首次运行已判无效，干净重跑待 codex 认可 DuckDB-ai baseline 方法论后进行。
- vLLM CLIP pooling 在当前 0.25.1 环境两次 1-image offline gate 均 600s timeout（FlashInfer
  autotune 卡住），暂 blocked；ImageNet/ResNet18 parity 阻塞于 Daft 公共 S3 在 AutoDL 的下载速度。
- 早期部分文本轨实验（cache-off）为历史参考，当前主线为 cache-on。

## 7. 一句话结论

本地单卡只能做"单 endpoint 能跑通"的 smoke；课题的**核心证据**——上游调度策略在多 endpoint、
KV 满载运行条件下的显著分化与排名反转——**只在双卡多 endpoint 环境才会出现**。租赁双 4090
是把课题从"能跑"推进到"能产出可发表证据"的必要投入，产出已覆盖主动机、正式实验、厂商原生
baseline 与可复现部署合同四个层面。
