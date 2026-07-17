# 当前方向与计划

生成日期：2026-07-17（最后更新：2026-07-17）

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

1. **P0（当前）**：建立 vLLM + Qwen2.5-1.5B baseline，替代手动 HTTP endpoint
2. **P1**：文本 RC1 消融 + RC2 消融 + Daft 引擎级参数 sweep
3. **P2**：耦合验证（独立拼接 vs 联合 grid search）+ 多模态泛化验证
4. **P3**：算子代价估计（基于已有数据，不新增实验）

**Scope 缩减触发条件**：Month 1 无 vLLM baseline → 多模态降 Discussion；文本 RC1+RC2 未完成不启动多模态 pipeline。

---

## 6. 关键证据

| 证据 | 来源 | 能说明什么 |
|---|---|---|
| AI_EMBED fine vs coalesced = 37.5× | GPU-backed 预研 CSV（2026-07-14） | batch 粒度是一阶变量 |
| pgvector writeback 0.897s vs JSON 1.567s | GPU-backed 预研 CSV | pgvector 写回可行 |
| 研究空白双重确认 | 多源检索（2026-07-16） | 无 CCF-A 论文研究上游 pipeline batching × downstream continuous batching 交互 |

**尚未建立**：vLLM baseline、Daft pipeline、多模态实验。所有策略论证在当前阶段为机制论证，待 vLLM 实验验证。

---

## 7. 完整文档入口

| 想知道... | 读这个 |
|---|---|
| 完整研究内容定义、实验路线、近期优先级 | `PROJECT_OUTLINE.md` |
| 项目规则、边界、不能写成什么 | `AGENTS.md` |
| vLLM 机制 + Ray 架构 + 57 篇文献 + 策略设计 + 知识缺口 | `research/knowledge_hub.md` |
| Daft 技术细节、多模态管线、具身智能连接 | `research/daft_ray_multimodal_reference.md` |
| 实验计划与实现参考 | `experiments/plans/strategy_design_implementation_reference.md` |
| 开题报告正文 | `opening/report/opening_report.md` |

---

## 8. 不能写成什么

- 改造 vLLM continuous batching、改造 Ray scheduler
- Daft/Ray 单纯集成、传统 GPU 查询算子、模型 kernel 优化
- 把引擎级参数调优写成策略贡献（需明确区分"引擎提供的"和"我们提出的"）
- 把多模态泛化论证写成"已解决具身智能问题"
- 把 PG18.4 本地预演写成 PG18.3 内部平台结论
