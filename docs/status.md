# 当前状态（摘要）

本页只回答两个问题：主要能力目前做到哪里；质量、资源、性能等维度分别有什么验证。
详细清单以 [`../code/INFRA_STATUS.md`](../code/INFRA_STATUS.md)（实现状态）、
[`results/EXPERIMENT_EVIDENCE_REGISTRY.md`](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)（证据强度）与
[`plans/experiment_status_and_gaps.md`](plans/experiment_status_and_gaps.md)（缺口）为准，
本页不维护第二份明细。

## 功能实现程度（2026-09-21）

- **数据库载体**：`REL_18_3` extension 的 recording `SemMap/SemFilter`、三参 exact `SemFilter`、
  生成型 `SemMap`（wire v5/多在途 v6）、图像 `ai_semantic.embed`（schema 5/wire 7）已实现并通过
  PG18.3 回归与 TAP（2,159 项）；planner 拥有版本化 plan spec，公共 compatibility suite 通过。
- **外部执行核心**：增量 session、有界提交、组织窗口、查询级 Job 归属、多 Job 共享执行与
  MethodDriver 已实现并接入 PG；Daft/Ray/vLLM/CLIP 为可替换 backend。
- **资源控制**：PG 单算子总量留存预算、Core 输入/结果预算、工作量上限已实现并有真实检查。

## 各维度验证程度

- **质量**：图像 embed 三路径数值一致（151 次真实 CLIP 前向）；文本 Map/Filter 的小规模真实
  请求通过，但 SemFilter 语义资格未通过、生成型 Map 存在质量负结果（0/55 逐字复述），
  真实校准仍暂停。
- **资源**：多轮真实运行完成账本核对与责任归零；生成型 Map 的正式资源压力资格仍未通过。
- **性能**：M1 吞吐平台尚未选点（PG/direct 均持续供给不足）；动态调度策略相对同上限静态
  配置尚无稳定胜出；正向证据为数据组织效果随容量压力变化、多 Job 的效率/隔离/公平权衡与
  图像静态 matched-resource 的 JCT 改善（各自适用条件见结果报告）。
- **代价估计**：context-LOO 下配置选择为边际通过（max regret 14.72%），尚未在线驱动决策。

## 明确缺项

SemFilter 第二 physical path 与真实 matched calibration、M1 平台选点与后续调参/评价、
GPU 计算中故障与匹配性能验证、五臂 formal、跨 workload/时间段/硬件的代价模型校准。
