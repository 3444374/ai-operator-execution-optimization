# 开题答辩 PPT v6 设计说明

## 1. 目标与状态

本文件定义 `opening_defense_20260720_v5.pptx` 的后续增量版本设计。
当前状态为：**设计已确认，尚未修改 PPTX**。

v6 的目标不是单纯美化 v5，而是把 2026-07-29 已形成的实验事实、负结果、
当前默认配置和后续验证任务组织成一套完整、可按现场时间跳页的开题答辩材料。

设计原则：

- 保留学校模板、母版、页眉页脚和用户已有的人工版式调整；
- 不重新运行 `opening/slides/build_ppt.py` 全量覆盖 v5；
- 从 v5 复制生成新文件后，使用 `python-pptx` 做增量编辑；
- 正文保留完整证据链，通过讲稿标签区分“优先讲”“可跳过”“答辩备份”；
- 架构图对标 SIGMOD/VLDB 系统论文的 Solution Overview 与机制放大图；
- 所有结论以项目权威总纲、正式结果报告和 CSV 为依据。

## 2. 权威来源与证据边界

制作 v6 时按以下优先级确定内容：

1. `PROJECT_OUTLINE.md`
2. `overview/current_direction_and_plan.md`
3. `experiments/results/` 下对应正式结果报告与 CSV
4. `motivation/results/` 下 GPU-backed 动机实验结果
5. `opening/report/opening_report.md`
6. v5 PPT 与旧图仅作为版式和历史口径参考

必须采用的当前结论：

- 数据组织当前默认采用 sequential token-budget；
- shared-vLLM 单 endpoint 当前默认采用 static `K=8` 与 fixed `50 ms`；
- queue-adaptive flush 与 SLO-aware EWMA 相对最佳静态窗口没有达到 5% 晋升门槛；
- 独立最优配置拼接与联合搜索未出现显著差距，当前证据不要求联合调优；
- 双 4090 active-work 扩展按预注册规则选择每 endpoint `65,536`；
- 固定资源 Actor Pool 保留 `1×256`，多 actor 未达到 5% 晋升门槛；
- complete-row fixed service quantum 未提高稳态吞吐，保留 request-level completion/credit 语义；
- endpoint-shared request/work credit 和 1/2/4-job 正式实验已完成；
- 4-job 收益属于高竞争条件下的条件性结果，仍需 held-out、错峰、加权公平性与异构 workload 验证；
- prefix-only 在 cache-off 下无稳定收益，下一步必须做 cache-on 独立机制验证；
- 多模态泛化、路由增量与故障迁移、代价模型跨时间段或新 workload 校准仍属于后续任务；
- 写回使用 PostgreSQL + pgvector、COPY + deferred index，仅作为工程 baseline 和端到端 guardrail。

必须修正的旧口径：

- `37.5×` 是 operator/推理执行阶段差异，端到端差异约为 `13.4×`；
- 不把 writeback bottleneck determination 写成第三项独立研究内容或创新点；
- 不把 queue-adaptive、AIMD 或 SLO-EWMA 写成已经优于静态策略；
- 不把 vLLM `num_requests_waiting` 画成当前默认控制器的一阶有效反馈；
- 不把双 endpoint、双 GPU、单 endpoint 结果混为同一资源条件；
- 不在正式可见文字中使用 `RC1/RC2/RC3`、`BL1/BL2`、`Phase`、`P0/P1/P2` 等内部代号。

## 3. 演示规模与跳页策略

目标规模：

- 正文约 31 页；
- 答辩备份约 8 页；
- 其中约 22 页标为优先讲，其余正文页标为可跳过。

标签只写入 speaker notes，不在页面上显示：

```text
页面角色：优先讲
页面角色：可跳过
页面角色：答辩备份
```

每页备注继续保留：

```text
汇报讲稿：
答辩备注：
```

讲稿必须说明该页与前后页的因果关系，不照抄页面文字。答辩备注必须记录结论
边界、资源条件和不能声称的内容。

## 4. 正文页面结构

| 页码 | 页面题目 | 角色 | 核心信息 |
|---|---|---|---|
| 1 | 封面 | 优先讲 | 正式题目、报告人、指导老师 |
| 2 | 汇报路线与一句话定位 | 可跳过 | 数据库 AI 算子外部执行链路的上游调度 |
| 3 | 数据库成为 AI workload 入口 | 优先讲 | 工业场景与 AI SQL 算子 |
| 4 | AI 算子执行链路为何不同 | 优先讲 | 模型推理引入数据库优化器未覆盖的决策地带 |
| 5 | 现有研究与连接处缺口 | 优先讲 | DB4AI、模型服务、数据系统各自边界 |
| 6 | 研究问题与明确边界 | 优先讲 | 数据组织、提交控制、多模态泛化、补充性代价估计使能 |
| 7 | 可观测的端到端执行路径 | 优先讲 | 阶段划分、计时边界和统一评价指标 |
| 8 | GPU-backed AI_EMBED 动机结果 | 优先讲 | operator 约 37.5×、端到端约 13.4× |
| 9 | vLLM + AI_COMPLETE 基线设置 | 可跳过 | workload、模型、资源和无写回边界 |
| 10 | 固定行数不是固定计算量 | 优先讲 | token tail 与请求计算量失配 |
| 11 | sequential token-budget 的当前证据 | 优先讲 | 约束 token tail，保留接近的吞吐 |
| 12 | 共享模型服务需要在途保护 | 优先讲 | static K8 对前台作业的保护作用 |
| 13 | fixed 50 ms 与 adaptive flush | 优先讲 | adaptive 无额外增量，选择更简单静态窗口 |
| 14 | 双 4090 active-work 饱和点 | 优先讲 | 每 endpoint 65,536 的预注册选择 |
| 15 | 固定资源 Actor Pool 与 service quantum | 可跳过 | 多 actor 与 fixed quantum 未晋升 |
| 16 | 多作业 shared-credit 正式结果 | 优先讲 | 容量安全、公平性与条件性 4-job 收益 |
| 17 | 当前选定的两类执行配置 | 优先讲 | 单 endpoint：token-budget + K8 + 50 ms；双 endpoint：request-level credit + 65,536 + 1×256 |
| 18 | 总体执行架构 | 优先讲 | 系统边界、两项研究内容、黑盒 vLLM 和写回 baseline |
| 19 | 研究内容一：数据组织设计空间 | 优先讲 | Cost Adapter、Organizer 与 BatchRequest |
| 20 | Request Shaping 机制放大图 | 优先讲 | token/frame cost、budget、length、prefix metadata |
| 21 | length-align 与 output-aware 的证据边界 | 可跳过 | 单点正向不等于跨规模稳定收益 |
| 22 | prefix-aware 的受控验证要求 | 可跳过 | cache-off 负结果与 cache-on 下一步 |
| 23 | 研究内容二：提交控制设计空间 | 优先讲 | per-job queue、shared credit、router、endpoint |
| 24 | Runtime Credit Lifecycle 机制放大图 | 优先讲 | acquire、submit、complete、release |
| 25 | 独立拼接与联合搜索 | 优先讲 | 未出现显著 coupling gap |
| 26 | 多模态策略复用 | 优先讲 | prompt token cost 切换为 image/frame cost |
| 27 | 算子代价估计补充 | 可跳过 | 共同使能组件，不独立构成研究内容；评价 ranking、regret 与预测区间 |
| 28 | 证据状态与剩余验证矩阵 | 优先讲 | 已选定、未晋升、待验证三类状态 |
| 29 | 后续工作、进度与风险控制 | 优先讲 | prefix cache-on、多模态、校准和公平性扩展 |
| 30 | 总结 | 优先讲 | 场景、方法、证据、边界 |
| 31 | 致谢 | 优先讲 | 结束页 |

所有实验页标题必须改为结论式标题，禁止继续连续使用
`vLLM + AI_COMPLETE Baseline` 作为四页相同标题。

## 5. 答辩备份页

| 页码 | 页面题目 | 用途 |
|---|---|---|
| 32 | 完整实验环境与可复现参数 | 回答硬件、模型、版本与 workload 问题 |
| 33 | baseline 等价性与证据层级 | 区分本地预演、双 GPU 正式结果和不同 runner |
| 34 | active-work 八档扩展曲线 | 解释 65,536 的选择规则 |
| 35 | Actor Pool 与 service quantum 完整结果 | 支撑未晋升判断 |
| 36 | SLO-EWMA 与 fixed-50 完整对照 | 支撑负结果判断 |
| 37 | 1/2/4-job 公平性与 shared credit | 展开 fairness、P99、JCT 与条件性收益 |
| 38 | PostgreSQL + pgvector 写回 baseline | 解释写回为何不是独立贡献 |
| 39 | 参考文献与研究边界问答 | 给出核心文献和常见质疑口径 |

## 6. 总体执行架构图

### 6.1 图类型与范式

- 类型：Solution Overview / System Architecture；
- 范式：三层高密度系统架构；
- 主方向：左到右的数据路径；
- 上层：规划与配置；
- 中层：数据执行主路径；
- 下层：运行状态、完成事件、观测指标和 guardrail。

### 6.2 一级模块

主路径最多保留六个一级模块：

1. PostgreSQL Workload Source
2. Daft Data Engine
3. Request Organizer
4. Ray Admission / Shared Credit / Router
5. vLLM Endpoint Pool
6. Fan-in + PostgreSQL/pgvector Sink

研究内容一只覆盖 Cost Adapter、Request Organizer 和 BatchRequest 成形；
研究内容二只覆盖 per-job queue、shared request/work credit、router 和 completion
release。Daft 标为数据引擎，Ray 标为调度载体，vLLM 标为不修改内部的部署平台。

### 6.3 数据与控制语义

线型必须有图例：

- 深灰实线：数据或请求流；
- 橙色虚线：admission、routing 和 credit 控制；
- 灰色点线：telemetry/observability；
- 绿色回路线：completion event 与 credit release。

禁止使用含义不明的双箭头。每条线标注具体对象，例如：

- `Arrow RecordBatch`
- `BatchRequest`
- `HTTP request`
- `completion event`
- `request/work credit release`
- `COPY + deferred index`

vLLM 指标进入 Observability/Evaluation，不直接进入当前默认静态控制路径。
候选自适应策略可作为虚线 plug-in 出现，但必须标注为“候选/待验证”。

### 6.4 高信息密度规则

高密度通过层次和复用实现，不通过缩小字体实现：

- 每个一级模块最多两层信息；
- 一级模块标题使用正式组件名；
- 框内只放状态、接口和关键策略名；
- 动作放在连线上；
- 指标统一收拢到观测带；
- 选定默认、候选策略、平台组件使用不同边框和徽标编码；
- 论文缩放后的最小字体不低于 8 pt；
- PPT 图内最小字体不低于 16 pt，一级模块标题不低于 20 pt。

### 6.5 配色

- 数据组织与 Daft：蓝色 `#2F6FEB`
- 调度与提交控制：橙色 `#F97316`
- vLLM 模型服务：紫色 `#7C3AED`
- 已选定配置与通过门禁：绿色 `#16A34A`
- 平台、辅助组件和未强调基线：中性灰

颜色必须同时配合空间位置、标题和线型，不允许只靠颜色表达语义。

## 7. Request Shaping 机制放大图

该图展示从数据库行到 `BatchRequest` 的成形过程：

```text
row
  -> Cost Adapter
  -> estimated prompt/output/frame work
  -> token/frame budget
  -> optional length/prefix metadata
  -> BatchRequest
```

图中明确区分：

- sequential token-budget：当前默认；
- length-align/output-aware：需要跨规模正式重复；
- prefix-aware：需要 prefix cache 开启后的独立验证；
- oversize row：单独提交，不能静默丢弃或截断。

图中不声称 length-align 必然降低尾延迟，也不声称 prefix-aware 必然提高 APC
命中率。图注第一句必须说明该图展示的是“候选请求成形机制与当前证据状态”。

## 8. Runtime Credit Lifecycle 机制放大图

该图按请求生命周期组织：

```text
Per-job Queue
  -> acquire endpoint request/work credit
  -> endpoint routing
  -> submit to vLLM
  -> request completion
  -> release request/work credit
  -> fan-in / writeback
```

必须准确表达：

- credit 是 endpoint-shared，而不是每个 job 各自独占；
- request credit 与 work credit 是两个不同约束；
- credit 在 completion 时释放，不在 submit 时提前释放；
- active work 与 request count 不得混用；
- routing/failover 是可插拔模块，未完成的机制使用虚线边框；
- single-endpoint `K=8 + 50 ms` 与 dual-endpoint active-work/shared-credit
  属于不同资源配置，不绘制成同一数值控制器。

## 9. 多模态复用图

该图只突出一个抽象：

```text
TextCostAdapter(prompt tokens)
ImageCostAdapter(frames / pixels / patches)
                   ↓
        same Organizer / Scheduler / Tracing
```

不把多模态画成另一条独立系统。主张范围限定为策略接口和配置逻辑复用，
最终性能收益仍由图像 workload 实验验证。

## 10. 图表和页面视觉规范

- 学校模板继续作为版式约束，不重新绘制校徽、页眉和页脚；
- 页面标题使用结论式短句；
- 正文可见字号原则上不低于 18 pt；
- 每页只保留一个主结论；
- 数据图优先从 CSV 通过项目脚本重画；
- 正式数据图同时输出 SVG 与 PNG；
- 架构图以 SVG 为权威源，输出 EMF 供 Office 2013/PPT 使用，并保留高分辨率 PNG；
- 不使用普通文生图生成架构图；
- 不使用渐变、阴影、3D、装饰性图标和无意义背景纹理；
- 实验图必须有轴名、单位、图例、误差或重复说明以及证据层级；
- 页面底部结论条只写该页结论，不写“图注建议”“后续补充”等制作提示。

## 11. 增量编辑与文件保护

实施时：

1. 保持 `opening_defense_20260720_v5.pptx` 不变；
2. 复制为日期化的 v6 文件；
3. 使用 `python-pptx` 定位和修改现有 shape；
4. 不重新运行 `build_ppt.py`；
5. 不覆盖用户已有的手动图片位置、字号和模板元素，除非该页进入明确重排范围；
6. 新增架构图和数据图统一从 `figures/` 引用；
7. 不在 `opening/slides/` 保存重复图资产副本。

## 12. 同步范围

v6 完成时至少同步：

- `opening/slides/README.md`
- `PROJECT_INDEX.md`
- `PROJECT_LOG.md`
- `opening/logs/project_log.md`
- 新增或修改图对应的 `figures/README.md`
- 新增或修改图对应的 `figures/audit/`

若 v6 修正了开题报告中的旧结论，还需同步本地：

- `opening/report/opening_report.md`
- `opening/feishu/opening_report_wiki.md`

未经用户明确授权，不执行线上飞书发布或覆盖。

## 13. 质量验收

内容验收：

- 所有数字能追溯到结果报告或 CSV；
- `37.5×` 与 `13.4×` 阶段口径正确；
- 当前默认、未晋升策略和待验证任务明确区分；
- writeback、vLLM、Ray、Daft 的研究边界准确；
- 页面、讲稿和答辩备注使用相同口径；
- 正式可见内容无内部代号和制作提示。

视觉验收：

- PowerPoint 渲染后无文本溢出、裁切和元素重叠；
- 所有正文和图内标签在 100% 投影视图下可读；
- 核心架构图在缩放到单页安全区后仍可追踪主路径；
- 所有图例、线型和颜色语义一致；
- 不存在低于目标分辨率的核心 PNG 图；
- 至少完成一次“渲染—发现问题—修复—重新渲染”闭环；
- 最终 PPTX 使用 PowerPoint 或 WPS 实际打开检查。

交付物：

- v6 PPTX；
- 审阅用 PDF；
- 全部页面预览图；
- 三张核心架构图的 SVG、EMF 和 PNG；
- 对应图表审计记录；
- 更新后的本地文档索引与变更日志。

## 14. 非目标

- 不修改 vLLM 内部调度器；
- 不把 Ray 或 Daft 产品集成包装成独立创新；
- 不重新设计学校模板；
- 不为填满页面增加没有证据的新机制；
- 不把负结果隐藏或改写为正向收益；
- 不在本轮直接发布线上飞书材料。
