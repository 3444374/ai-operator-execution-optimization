# 当前方向与计划

生成日期：2026-07-17（最后更新：2026-07-20）

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
- ✅ Queue-adaptive flush 首次实现与测试

**当前缺口**（详见 `experiments/plans/experiment_status_and_gaps.md`）：
1. **P0**：改进 queue-adaptive 控制器，在 shared-vLLM 下超越静态 K_max=8
2. **P0**：两项策略联合消融——独立拼接 vs 联合 grid search
3. **P1**：Prefix 受控 workload + scale 到 2048 行
4. **P2**（触发：P0+P1 完成）：多模态泛化验证
5. 算子代价估计（§6.1 讨论，最低优先级，基于已有数据）

**Scope 缩减触发条件**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion（✅ 已建立，未触发）
- 文本 RC1+RC2 消融未完成前，不启动 Daft 多模态 pipeline
- VLM 生成实验始终标记为 optional
- Adaptive 控制器 3 轮改进后不能超过 static K_max=8 → RC2 降级

---

## 6. 关键证据

**AI_COMPLETE（主场景，07-18/19 本地 vLLM baseline）**：
| 证据 | 能说明什么 |
|---|---|
| Token-tail revision：固定行 batch=8 时 token 跨度 13.9×，batch=128 时 token P95=26678 | 固定行数是计算量的弱代理 |
| Token-budget vs Fixed Row：token_budget=6144/8192 约束 token P95 至 ~6141/8171 | token-budget 能有效约束 token tail |
| Shared-vLLM K_max 干扰：bulk unbounded 时 foreground E2E 恶化 2.3× | K_max 在共享 vLLM 下必要 |
| Queue-adaptive flush 已实现但未超越静态 K_max=8 | RC2 当前最高风险 gap |

**AI_EMBED（预研，已完成）**：
| 证据 | 来源 | 能说明什么 |
|---|---|---|
| fine vs coalesced = 37.5× | GPU-backed 预研 CSV（2026-07-14） | batch 粒度是一阶变量 |
| pgvector writeback 0.897s vs JSON 1.567s | GPU-backed 预研 CSV | pgvector 写回可行 |
| 研究空白双重确认 | 多源检索（2026-07-16） | 无 CCF-A 论文研究上游 pipeline batching × downstream continuous batching 交互 |

**尚未建立**：联合消融（独立拼接 vs 联合 grid search）、多模态泛化验证、PG18.3 内部平台复测。

---

## 7. 完整文档入口

| 想知道... | 读这个 |
|---|---|
| 完整研究内容定义、实验路线、近期优先级 | `PROJECT_OUTLINE.md` |
| 项目规则、边界、不能写成什么 | `AGENTS.md` |
| vLLM 机制 + Ray 架构 + 57 篇文献 + 策略设计 + 知识缺口 | `research/knowledge_hub.md` |
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
