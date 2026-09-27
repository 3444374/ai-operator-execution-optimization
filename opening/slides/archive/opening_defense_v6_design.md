# 历史开题答辩 PPT v6 设计（已被取代）

冻结日期：2026-08-07

> **状态护栏（2026-08-09）**：本文件只保留旧版页面映射与版式经验，**不得继续作为
> 当前内容合同、数据源或PPT生成输入**。其中首轮database-E2E、数据组织“近似中性”及
> 四图结构均已被后续replacement、原生单/多Job和第一性原理复审取代。当前权威内容入口为
> `opening/opening_defense_outline_20260808.md`、`opening/claim_matrix.md`、
> `opening/report/opening_report.md`和
> `figures/audit/opening_story_figures_contract_20260808.md`。用户已暂停PPT成品，本文件不更新
> 为新版本，也不得据此覆盖云端材料。

本文件定义从 `opening_defense_20260720_v5.pptx` 增量生成 v6 的内容合同。学校模板、母版、页眉页脚和人工版式调整继续保留；旧稿只提供版式，不再提供研究结论。最终可见文字不得出现内部实验代号。

## 1. 一句话叙事

数据库已经成为 AI workload 的入口，但模型服务只接收请求，不理解数据库行、作业和写回语义；本课题研究两者之间的 AI 数据执行层，解决 work-unit 如何构造，以及请求如何在容量约束下提交、路由和多作业协调。

统一系统边界：

```text
Database
  -> AI Data Execution Layer
       -> work-unit construction
       -> cost estimation
       -> admission and routing
       -> resource-aware scheduling
       -> multi-job coordination
  -> Model Service / GPU Executor
  -> Database / Vector Sink
```

研究内容一是 workload-aware work-unit 构造；研究内容二是 runtime-state-aware 提交、路由和多作业调度。算子代价估计是共同使能组件，不单列为第三项研究内容。Daft、Ray、vLLM、PostgreSQL 和 CLIP 是实现与验证平台，不是贡献名称。

## 2. 证据与表达边界

正文只保留四张核心证据图，每张图只回答一个问题：

1. `opening_serving_capacity_frontier`：固定资源下存在最小饱和 active work；无限增压只会恶化尾部。
2. `opening_work_organization_regime`：数据组织价值依赖 serving regime；work balance 与 prefix locality 可能冲突。
3. `opening_image_matched_resource`：相同资源和输出合同下，图像静态执行结构获得可重复的约 13%–15% operator-JCT 改善。
4. `opening_cost_model_decision_quality`：轻量代价估计已表现出配置选择价值，但最坏 regret 仍是边界风险。

统一文本 database-E2E 三臂实验只放一页表格，必须同时报告正确吞吐、外部 E2E、语义失败和 feeding-saturation。任何未过门禁的臂均以醒目标记呈现，不进入“项目胜出”叙事。DuckDB AI 的固定输出上限语义不兼容必须解释为产品语义边界，不能伪装成基础设施失败或单纯性能差距。

以下表述禁止进入正文：

- “动态策略已经优于强静态点”；
- “sequential 或 prefix-aware 普遍最优”；
- “项目路径已经在统一三臂中胜出”；
- “图像实验提升 45.7%”；
- “代价模型已经解决”；
- “修改了 vLLM continuous batching、Ray 调度器或 GPU kernel”。

## 3. 28 页正文映射

| 页 | 页面题目 | 页面角色 | 唯一主结论 | 复用 v5 页 |
|---:|---|---|---|---:|
| 1 | 数据库 AI 负载的执行优化与调度研究 | 优先讲 | 题目与研究对象 | 1 |
| 2 | 汇报结构 | 可跳过 | 问题—证据—方法—计划 | 2 |
| 3 | 数据库正在成为 AI workload 入口 | 优先讲 | AI SQL 使数据库成为模型调用与结果管理入口 | 3 |
| 4 | 模型服务前后出现新的数据执行层 | 优先讲 | 数据库行不能直接等同于同成本请求 | 4 |
| 5 | 现有系统分别优化两端，连接层仍缺方法 | 优先讲 | DB 内优化与 serving 内优化均未覆盖完整上游链路 | 5 |
| 6 | 前期证据与研究边界 | 可跳过 | 进入证据章节 | 6 |
| 7 | 统一文本三臂揭示静态路径边界 | 优先讲 | 同 source/sink 后不预设项目胜出；门禁和语义失败同表展示 | 7 |
| 8 | 先标定最小饱和点，再比较策略 | 优先讲 | 65K 达已测最大吞吐 97.8%，继续增压收益小且尾部变差 | 8 |
| 9 | 数据组织收益取决于 serving regime | 优先讲 | 低压力近似中性，KV 饱和时出现排名反转 | 9 |
| 10 | 相同资源下，图像执行结构获得可重复收益 | 优先讲 | 主报告冻结约 13%–15%，不使用旧极值 | 10 |
| 11 | 代价估计已能辅助选择，但仍有最坏情形 | 优先讲 | pooled regret 低，max regret 仍需继续校准 | 11 |
| 12 | 证据支持问题存在，不代表方法已经完成 | 优先讲 | 已证明、条件性、待验证、不能声称四级边界 | 12 |
| 13 | 研究目标与方法 | 可跳过 | 进入方法章节 | 13 |
| 14 | 总体架构：AI 数据执行层连接数据库与模型服务 | 优先讲 | 两项研究内容位于同一层，模型服务保持黑盒 | 14 |
| 15 | 三个研究问题限定方法设计 | 优先讲 | 最小饱和、相同 work 的组织、多作业共享 | 15 |
| 16 | 研究内容一：按工作量构造 work-unit | 优先讲 | 从固定行数转向 token/frame budget | 16 |
| 17 | 数据组织同时处理 balance 与 locality | 优先讲 | length/prefix 是候选信号，不是预设最优答案 | 17 |
| 18 | 数据组织策略用强静态点和机制指标证伪 | 优先讲 | 先独立消融，再检验跨 regime 排名稳定性 | 18 |
| 19 | 研究内容二：按容量提交、路由与协调 | 优先讲 | 控制 active work，而不是无限压入请求 | 19 |
| 20 | request/work credit 在完成时精确释放 | 优先讲 | shared credit、idle borrowing 与公平队列形成闭环 | 20 |
| 21 | 状态感知策略必须超过同上限静态点 | 优先讲 | 服务信号只驱动上游决策，不修改 vLLM 内部 | 21 |
| 22 | 代价估计共同服务两项研究内容 | 可跳过 | 预测 work/service/slack，并以 ranking regret 评价 | 22 |
| 23 | 验证计划与风险控制 | 可跳过 | 进入计划章节 | 23 |
| 24 | 实验矩阵只回答可证伪问题 | 优先讲 | baseline、消融、独立拼接/联合搜索和多作业公平性 | 24 |
| 25 | 同一抽象跨文本与图像复用 | 优先讲 | token cost 换成 frame/patch cost，Organizer/Scheduler 不变 | 25 |
| 26 | 进度安排与停止规则 | 优先讲 | 不扩第二数据库和无关矩阵；按证据门禁推进 | 26 |
| 27 | 预期创新、风险与降级路径 | 优先讲 | 负结果转化为适用边界，不改问题追结果 | 27 |
| 28 | 谢谢各位老师 | 优先讲 | 结束并进入问答 | 28 |

## 4. 页面 7 的统一文本三臂合同

页面可见表格最多六列：workload、路径、correct rows/s、database-E2E、语义/基础设施失败、feeding gate。表下只保留三句话：

- SQuAD 是均匀控制组，不预设项目路径更快；
- ShareGPT controlled-skew 检查异质 work 是否放大路径差异；
- correct rows/s 将 cap 语义失败保留在分母中。

正式值必须来自 `experiments/results/opening_database_e2e_text_20260807/raw/formal_summary.csv`。若某一臂 service tokens/s 低于 direct 的 95%，表中标记“未过 feeding 门”，讲稿明确说明该结果不能支持策略性能 claim。

## 5. 四图页面合同

四张图分别占页面 8–11 的主视觉区，图题不得重复正文标题。每页只允许一个结论条：

- 页面 8：`65K 是当前合同下的最小饱和点，不是跨机器通用常数。`
- 页面 9：`组织策略必须在明确的 endpoint/KV regime 下评价。`
- 页面 10：`图像证据支持执行结构可行性，尚不支持状态感知增量。`
- 页面 11：`代价估计进入决策闭环，但最坏 regret 仍需压缩。`

图内字号、颜色和误差表达以 `figures/audit/opening_core_evidence_figures_contract_20260807.md` 为准。不得在 PPT 内重绘另一套数字。

## 6. 备注合同

每页 speaker notes 必须包含：

```text
页面角色：优先讲 / 可跳过

汇报讲稿：
说明该页如何承接上一页、要让评委记住什么、如何转入下一页。

答辩备注：
记录资源条件、证据等级、不能声称的内容和被追问时的最短回答。

[Sources]
- 本地结果报告或论文/官方文档来源
```

页面 7–12 的答辩备注还要写明对应 Claim Matrix 等级。备注不照抄页面正文。

## 7. 模板增量编辑合同

1. v5 保持不变，最终输出为日期化 v6 文件。
2. 先完成 28 页 source-slide inventory 和 `template-frame-map.json`，再复制为 starter deck。
3. 只编辑映射中声明的原有文本框和图片框；母版、校徽、页眉页脚、章节色条默认保留。
4. 图片使用 `figures/data/report_main/` 的权威 PNG/SVG，不在 `opening/slides/` 保存副本。
5. 每页渲染为 PNG，并检查裁切、溢出、空 placeholder、字体替换和 source note。
6. 最终 PPTX 用 PowerPoint/WPS 实际打开验证；若本机无法完成该人工检查，状态不得写成“已冻结”。

当前 Codex 桌面工作流使用 artifact-tool 导入、检查和增量导出 PPTX；这一实现选择不改变“保留 v5 人工版式、不重跑旧 build 脚本”的项目约束。

## 8. 验收条件

- 正文恰好 28 页，页面与上表一一映射；
- 只有四张 headline evidence 图；
- 统一文本三臂表包含门禁与语义失败，不只给吞吐排名；
- 题目、两项研究内容、代价估计定位和多模态边界与 Claim Matrix 一致；
- 所有正式值可追溯到 CSV/JSON，页面没有内部代号、制作提示或过时数字；
- 28 页均有 `汇报讲稿`、`答辩备注` 和 `[Sources]`；
- 最终 XML 中不存在未处理的空结构 placeholder；
- 渲染与实际 PowerPoint/WPS 打开检查均通过后，才将 v6 标记为冻结。
