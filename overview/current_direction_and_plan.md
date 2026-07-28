# 当前方向与计划

生成日期：2026-07-17（最后更新：2026-07-26）

> 本文档是项目方向的**快速参考卡片**。完整定义、依据和细节见 `PROJECT_OUTLINE.md`（项目总纲）、`AGENTS.md`（规则边界）、`research/knowledge_hub.md`（知识库）。本文档不替代上述文件，仅提供 TL;DR。

---

## 1. 课题定位

优化数据库 AI 算子外部执行链路的上游调度——数据如何组织为请求、以什么节奏发送、如何根据模型服务状态调节并发。

**一句话**：数据库触发 AI workload 后，上游如何组织请求、以什么节奏发送，显著影响下游 continuous batching 的效率，且这种策略抽象不依赖数据模态。

---

## 2. 技术栈

```text
PostgreSQL 18.3 → Daft DataFrame（数据引擎）→ Ray actor（策略执行）
  → vLLM Continuous Batching（部署平台，不修改）→ PostgreSQL + pgvector（写回）
```

| 组件 | 角色 |
|---|---|
| Daft | 数据引擎（Rust + Arrow + @daft.cls GPU UDF），文本阶段直接接入 |
| Ray | 架构设计空间（异构 actor pool + 去中心化协调） |
| vLLM | 部署平台 + baseline（Continuous Batching + PagedAttention），不修改内部 |
| PostgreSQL + pgvector | 数据源 + 写回 sink |

---

## 3. 研究内容

1. **研究内容一：数据组织策略** — token-budget batching、length-aligned/prefix-aware grouping + Daft 引擎级参数
2. **研究内容二：调度与提交控制策略** — queue-adaptive flush、K_max 动态控制、actor pool 分池路由 + Daft 引擎级参数
3. **多模态泛化验证**（正文 §5.3）— 图像 workload 上同一套策略代码，验证模态无关性
4. **算子代价估计**（§6.1 补充讨论）— 基于已有 profile 数据，不新增实验

写回使用 PostgreSQL + pgvector（COPY + deferred index），不作为独立研究内容。

---

## 4. 主场景

| 场景 | 模态 | 状态 |
|---|---|---|
| AI_COMPLETE（生成式 LLM） | 文本 | 主场景 |
| AI_EMBED / AI_CLASSIFY | 图像 | 多模态泛化验证（正文实验） |

模型：Qwen2.5-1.5B（文本）、CLIP-ViT-B/32（图像 embedding）、Qwen2.5-VL-3B（图像分类，optional）。硬件：单 RTX 5070 12GB VRAM。

---

## 5. 当前优先级

**已完成**：
- ✅ vLLM + Qwen2.5-1.5B baseline 建立（07-18）
- ✅ Daft 文本阶段直接接入，链路跑通
- ✅ Token-tail revision + Token-budget vs Fixed Row 对照
- ✅ Shared-vLLM K_max 干扰实验（K_max 在共享 vLLM 下必要）
- ✅ Shared-vLLM 128/512 typed AIMD 正式复验：AIMD 0 次 decrease、窗口
  均值 15.953，未优于 static K16；static K8 继续作为前台保护 guardrail
- ✅ Queue-adaptive flush 首次实现与测试
- ✅ Output-aware cost、deterministic BFD 与 GPU/功耗/能耗/MFU 观测链路；
  512 行正向候选但 1024 行负向，已确认经典 BFD 不是无条件最优
- ✅ Queue-adaptive 自然 EOS 三组随机化复验：fixed-50 与 adaptive 均显著
  优于 fixed-25，二者不可分辨；当前采用更简单的 fixed-50
- ✅ Shared-vLLM adaptive flush 补充：约 89.4% 决策选择 50ms，AIMD 下
  与 fixed-50 四项差异均小于 0.3%，无稳定增量
- ✅ Batching × submission 18 单元筛选与候选重复：独立拼接和联合候选
  不可分辨，当前采用分层优化
- ✅ 跨 arrival-rate 与 2048 自然 EOS held-out：fixed-50 保持当前默认，
  adaptive 未出现排序反转
- ✅ Prefix 受控 0/30/70/100% cache-off 实验：prefix-only 无稳定收益，
  修复 organizer 的重排和策略耦合问题
- ✅ 算子代价估计初版：283 条 profile、70 个配置组，五切分平均
  MAE 11.68s、MAPE 50.60%、R² 0.776
- ✅ vLLM CUDA Graph 部署基线：同一 512-request workload、每侧 3 次
  formal，相对 eager 的 E2E -71.76%、observed tokens/s +254.05%、
  MFU 4.02% → 14.51%；后续本地稳态调度实验采用 graph 服务
- ✅ 双 4090 request-level replenishment 重复：等名义 offered work 的
  request K48 与 batch K16 吞吐持平；K64 最高但 work 增加约 33% 且 P99
  更差，尚未证明补位机制的独立增量
- ✅ 双 4090 active-work 容量曲线：16K→65K 吞吐仍上升但边际递减；
  49K 是当前 knee candidate，65K 是最高已测吞吐边界而非容量最优

**当前缺口**（详见 `experiments/plans/experiment_status_and_gaps.md`）：
1. **P1**：active-work 五档双卡曲线已完成；以 49K 拐点候选为主、
   65K 高负载点为敏感性对照，固定 work 扫 token budget 和 membership
2. **P1**：在上述固定 active work、组织边界与服务容量下，复验 Ray 上游
   request-level continuous replenishment；当前 K 计数实验已跑通，但
   work-matched 对照无吞吐增量
3. **P1**：SLO-aware EWMA flush；当前 25/50ms two-level 只作为 baseline
4. **P1**：Prefix cache 开启后的机制实验与 length-align 显式联合消融
5. **P2**（文本门禁已完成）：多模态泛化验证
6. 在当前 2×4090 上完成多 endpoint / 多 GPU 重复 formal 验证
7. 代价模型增加独立时间段/新 workload 校准和预测区间
8. **P1**：Shared-vLLM 扩展 1/2/4 job；共享 endpoint 的 request/work
   credit 与 work-conserving 公平队列代码已完成，尚待远端门禁和正式数据

继续从文献提取机制时，统一按
`experiments/plans/literature_driven_pipeline_optimization_guide.md` 的机制卡、
假设迁移、fatal-flaw audit 和最小隔离实验流程执行。

**Scope 缩减触发条件**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion（✅ 已建立，未触发）
- 研究内容一+二的消融实验未完成前，不启动 Daft 多模态 pipeline
- VLM 生成实验始终标记为 optional
- Adaptive 控制器 3 轮改进后不能超过 static K_max=8 → 研究内容二降级

---

## 6. 关键证据

**AI_COMPLETE（主场景，07-18/19 本地 vLLM baseline）**：
| 证据 | 能说明什么 |
|---|---|
| Token-tail revision：固定行 batch=8 时 token 跨度 13.9×，batch=128 时 token P95=26678 | 固定行数是计算量的弱代理 |
| Token-budget vs Fixed Row：token_budget=6144/8192 约束 token P95 至 ~6141/8171 | token-budget 能有效约束 token tail |
| Shared-vLLM K_max 干扰：bulk unbounded 时 foreground E2E 恶化 2.3× | K_max 在共享 vLLM 下必要 |
| Shared-vLLM 128/512 重复：AIMD 0 decrease、均值 K=15.953；相对 K16 前台 E2E +1.22%、后台 tokens/s -1.45% | 当前 AIMD 盯 vLLM waiting 但信号不反映 Ray 侧积压，static K8 + fixed-50 保持默认 |
| 自然 EOS 三组随机化复验：fixed-50 与 adaptive 相对 fixed-25 tokens/s 分别 +32.23% 与 +32.09%；adaptive vs fixed-50 -0.10% ± 4.13% | 收益来自更长 coalescing window；当前采用更简单的 fixed-50 |
| Output-aware BFD：512 行相对同成本 sequential +12.019%，1024 行反转为 -5.156% | 数据组织收益依赖规模与 row cap；经典 BFD 只能作候选，需联合搜索 |
| 联合候选相对独立拼接 tokens/s -0.26% ± 2.07% | 当前单 GPU 下分层独立优化已足够，没有联合在线控制器的证据 |
| vLLM CUDA Graph 相对 eager：E2E -71.76%、tokens/s +254.05%、request P99 -77.82% | 部署执行模式是一阶 baseline 变量；属于环境调优，不是上游调度贡献 |

**AI_EMBED（预研，已完成）**：
| 证据 | 来源 | 能说明什么 |
|---|---|---|
| fine vs coalesced（operator 阶段 37.5× / 端到端 13.4×） | GPU-backed 预研 CSV（2026-07-12/14） | batch 粒度是一阶变量 |
| pgvector writeback 0.897s vs JSON 1.567s | GPU-backed 预研 CSV | pgvector 写回可行 |
| 研究空白双重确认 | 多源检索（2026-07-16） | 无 CCF-A 论文研究上游 pipeline batching × downstream continuous batching 交互 |

**尚未建立**：prefix cache-on 机制收益、多模态泛化验证、多 endpoint /
多 GPU 实测、PG18.3 内部平台复测。

---

## 7. 完整文档入口

| 想知道... | 读这个 |
|---|---|
| 完整研究内容定义、实验路线、近期优先级 | `PROJECT_OUTLINE.md` |
| 项目规则、边界、不能写成什么 | `AGENTS.md` |
| vLLM 机制 + Ray 架构 + 66 篇文献 + 策略设计 + 知识缺口 | `research/knowledge_hub.md` |
| Daft 技术细节、多模态管线、具身智能连接 | `research/daft_ray_multimodal_reference.md` |
| 实验状态与缺口分析 | `experiments/plans/experiment_status_and_gaps.md` |
| 实验计划与实现参考 | `experiments/plans/strategy_design_implementation_reference.md` |
| 开题报告正文 | `opening/report/opening_report.md` |

---

## 8. 不能写成什么

- 改造 vLLM continuous batching、改造 Ray scheduler
- Daft/Ray 单纯集成、传统 GPU 查询算子、模型 kernel 优化
- 把引擎级参数调优写成策略贡献（需明确区分"引擎提供的"和"我们提出的"）
- 把多模态泛化论证写成"已解决具身智能问题"
- 把 PG18.4 本地预演写成 PG18.3 内部平台结论

---

## 2026-07-26 状态更新

- Row-cap-first 已完成无 prefix cache 的 512 行重复和 1024 行 held-out。
- 1024 行 tokens/s 约提高 0.82%，但 10 秒 SLO violation 从 50.39%
  升到 88.67%，因此 sequential token-budget 继续作为默认。
- queue-adaptive 随机化变长输出复验与 batching × submission 联合消融均已
  完成。前者优于 fixed-25 但未优于 fixed-50；后者未显示联合搜索相对独立
  拼接的可分辨增量。
- 跨 arrival-rate、2048 held-out、受控 prefix cache-off 与代价估计初版均
  已完成；当前最优先工作转为 prefix cache-on 门禁和多模态泛化验证。
- UCB 端到端和多 GPU 实测尚未完成，且不会在缺少正确 reward 归因或硬件时
  伪接入。
- Infra 代码与证据边界见 `code/INFRA_STATUS.md`。
