# 开题实验、方法与叙事的第一性原理复审

日期：2026-08-08

> 本文件保留 2026-08-08 的第一性原理复审和当时的材料状态。当前 PPT 为 26 页 v9，报告为
> 16 图/53 条参考文献，二者的最新差异仍待审查；当前状态与可直接使用的对外措辞以
> `README.md`、`claim_matrix.md` 和 `opening_defense_outline_20260808.md` 为准。

## 1. 结论先行

开题不需要在答辩前完成整套动态方法实验，但必须形成一条闭合、诚实的论证链：

```text
数据库记录不是等成本请求
  -> 需要可比较的 work-unit 描述
固定服务容量存在欠供给、近饱和和过载区间
  -> 需要先标定强静态容量边界
workload 与流水线状态会随时间和作业组合变化
  -> 需要状态观测与受约束的动态提交/路由
数据组织改变 work balance、locality 和后续队列形态
  -> 数据组织不是独立预处理，而是调度的输入
文本与图像的主瓶颈阶段不同
  -> 公共抽象应描述分阶段 work，不应只把 token 改名为 frame
```

因此，开题前的完成标准是：问题存在、研究对象明确、已有证据证明方法接口可实现、强静态基线合格，并以两作业错峰说明共享 credit 的可测量性。该最小因果点及1-short+3-long四Job扩展均已于 2026-08-09 完成：它们验证了前台/long干扰和效率—隔离—公平权衡，但没有证明动态全面胜出。开题不要求 proposed 已经全面胜出；同上限动态 surface、weighted/held-out 多 job、图像新策略增量与跨硬件外部有效性属于开题后的论文主实验。

## 2. 从目标函数反推方法

数据库 AI 外部执行层的目标不是单独最大化 GPU utilization，而是在 correctness 和资源约束下优化：

```text
correct throughput / SLO goodput / job completion time / tail latency / fairness / cost
```

GPU/MFU、running/waiting、队列深度、KV 使用率和阶段 busy ratio 是解释原因和触发控制的信号，不是最终目标。任何动态动作都必须在同一资源、同一最大 active-work/K、同一 source 和同一输出语义下与强静态点比较。sink 只在 database-E2E 护栏实验中统一，不是调度消融的必选阶段。

由此得到五项必要设计，其中算子代价估计是两项研究内容的共同使能部件，而不是第三项研究内容：

1. **离线容量包络**：绑定机器、模型、协议、workload 分布和执行拓扑，标定最小近饱和点与过载边界。
2. **共同代价估计**：解析输入特征并结合 profile 校准与 residual correction，给出 stage/service/remaining work、SLO slack 和不确定区间；组织器与调度器消费同一校准口径。
3. **work 描述**：每条记录或请求携带可比较的预计工作量、局部性、到达时间、作业与 SLO 元数据。
4. **在线状态快照**：同时观测上游等待、各阶段 active/ready work、完成速率、队龄和服务压力；过期或缺失信号回退到冻结静态点。
5. **有界动作**：只在离线验证过的安全动作集合内调整组织预算、work credit、路由或 job 份额，并使用 deadband、最短驻留时间和回退规则防振荡。

## 3. work-unit 应如何调整

现有 `BatchRequest.work_units + work_unit` 能完成标量 admission，但不足以表达图像的 CPU/GPU 两阶段差异，也没有估计置信度和校准签名。下一版应保持兼容，同时引入分阶段 work 描述：

| 字段 | 含义 | 文本示例 | 图像示例 |
|---|---|---|---|
| `row_count` | 数据库语义与 exactly-once 单位 | 行数 | 图像数 |
| `source_work` | 读取/解码前输入规模 | prompt bytes | encoded bytes |
| `prepare_work` | CPU 准备阶段需求 | tokenize cost | decode/resize/normalize cost |
| `model_work` | 模型执行需求 | prompt + predicted output tokens | pixels/patches 或校准后的 CLIP service work |
| `result_work` | fan-in/sink 压力 | generated tokens/bytes | embedding bytes |
| `locality_key` | 可复用局部性 | prefix/session | source shard/shape bucket |
| `deadline_s` | job/SLO 的绝对或归一化时限 | answer deadline | embedding freshness/SLO |
| `uncertainty` | 预测不确定性 | output-token interval | preprocess/service interval |
| `calibration_signature` | 防止跨环境误用 | machine/model/protocol/workload | machine/model/processor/topology |

调度不能机械地把上述各维相加。当前瓶颈在模型服务时，以 `model_work` 作为 endpoint credit；图像 CPU prepare 为瓶颈时，以 `prepare_work` 约束 CPU stage，并用 ready-tensor work 维持 GPU feed。`row_count` 继续承担数据库 correctness 与公平统计，不能作为计算量代理。

## 4. 数据组织为什么能帮助后续调度

Organizer 的输出不应只是 payload batch，而应同时输出 `WorkDescriptor`。这使 scheduler 能够：

- 用 work credit 而非请求数控制 active work；
- 用预计 drain time 比较 endpoint，而不是只看 running 数；
- 在多 job 中按 work 做 deficit round robin，避免长请求被按“一条请求”低估；
- 在 locality 与 balance 冲突时显式保留 `locality_key`，而不是重排后再猜缓存损失；
- 用 remaining work 和 SLO slack 做 job 级选择；
- 在图像管线分别控制 CPU-ready queue、GPU-ready queue 和内存中的 tensor bytes。

答辩内容大纲与后续图示必须把这条关系画出来：

```text
Database rows
  -> Organizer forms work units
Cost Estimator -> stage/service/remaining work + SLO slack + uncertainty
               -> WorkDescriptor -> Organizer and Scheduler
Fresh State    -> Scheduler consumes work/locality/deadline/state
  -> admission + routing + fair sharing
```

不能再把“数据组织”和“调度提交”画成两张互不相干的策略清单。

## 5. 动机证据必须怎样对应方法

动机不是证明动态方法已经胜出，而是证明设计需求不是凭空产生。建议用两页、三条一一对应的证据：

| 动机现象 | 当前可用证据 | 导出的挑战 | 后文方法 |
|---|---|---|---|
| 同样行数包含的 token work 差异很大 | 固定 16 行时 batch token min/max=474/6,793（14.3×）；固定 128 行时 token P95 达 26,677 | 行数不是成本代理 | work descriptor + budget organizer |
| 同一静态上限在 high 与 arrival-limited 条件下观测到完全不同的 running/MFU | 固定 65K active-work 下，high 约 169–172 running、MFU 约 35%；arrival-limited 约 19 running、MFU 约 7% | 运行状态会变，单次静态标定不能解释所有时段 | fresh state snapshot + safe fallback |
| active work 过小欠供给，超过近饱和点后吞吐边际收益递减而 P99 继续上升 | 65K/endpoint 达已测峰值 97.8%，继续加压主要增加尾部 | 控制目标是围绕标定的最小近饱和点权衡吞吐与 tail，不是无限提交 | bounded dynamic work credit |
| 图像 CPU prepare 为 GPU actor 的 13.8–31.2 倍 | CLIP exact-path 画像，质量一致 | 跨模态瓶颈阶段不同 | stage-aware work 与 queue control |

这四条足以支撑“为什么要研究 work-unit、感知和动态提交”。它们不支持“当前动态控制器已经有效”；已有 SLO-EWMA、AIMD 等负结果应作为强静态基线和信号选择教训。

## 6. 开题前、开题后与可停止实验

### 6.1 开题前必须完成

1. 完成 SQuAD 与 ShareGPT 两套统一三臂 replacement；项目静态臂必须通过 feeding-saturation、correctness 与稳定性门。该矩阵保留为一次 database-E2E 护栏，不要求后续方法实验重复 sink。
2. short/long 两作业最小实验已完成 online/eager 两套到达合同及 full/half matched control；1-short+3-long四Job也完成full/quarter/static/shared与三条原生轨内single→four-job。两Job证明arrival-regime dependence，四Job进一步分离quota/竞争并暴露shared的Jain/long稳定性缺口；3:1 weighted与held-out留论文阶段。
3. 把现有 token-work 差异、high/arrival-limited 状态差异、active-work frontier、数据组织 regime、图像阶段失衡与 matched-resource 结果、cost decision quality 重组为动机和可行性证据。
4. 报告、答辩内容大纲、Claim Matrix 和图使用同一数值与边界；state-aware、图像动态和 cost held-out 只作为可证伪研究计划，不用完成时表述。此条在 2026-08-08 表示当时不制作 PPT；后续 26 页 v9 已生成并完成独立 QA。

### 6.2 开题后的论文主实验

1. **强静态基线**：同机、当前 commit、统一语义下完成 Daft built-in、Ray Data native、project frozen-static 和必要 direct ceiling；正式 baseline 由框架拥有调度。
2. **稳态控制**：动态策略在 steady workload 下不应显著劣于冻结静态点，用于确认控制开销与回退正确。
3. **状态变化**：阶段突变、突发到达、长短 work mix；比较相同最大 K/active-work 下的 static 与 state-aware。
4. **多 job 扩展**：在开题前两/四作业结果上扩展3:1 weighted fairness、Long→Short、held-out job mix/offset与故障迁移；报告 JCT、P99、SLO goodput、Jain fairness、isolation 与 work conservation。
5. **组织—调度耦合**：组织独立最优 + 调度独立最优的拼接，与小规模联合搜索比较；不同时扩大两个维度后归因。
6. **跨模态复用**：同一 `WorkDescriptor`、credit、state snapshot 和策略接口；只替换 modality adapter。
7. **外部有效性**：至少一个 held-out 时间段或 workload；资源允许时再增加第二硬件签名。

### 6.3 可选或停止

- K512/endpoint 不属于当前喂饱校准；K256/endpoint × 2 已是全局 512 request credit。只有研究过载/退化时才加入 K512/endpoint，并明确服务和 actor 合同改变。
- VLM 生成、第二数据库、文本 Daft/Ray Data 全矩阵和故障迁移不是开题 blocker。
- 动态策略若在吞吐、tail、SLO、JCT、公平性均未超过同上限静态点，则记录失效边界，不换 workload 追正结果。

## 7. 图像部分是否足够

当前图像证据已经足够开题：它支持“存在 CPU/GPU 阶段失衡，分阶段 actor 结构可行且有 matched-resource 初步收益”。它不足以支持“图像上的 proposed 已完成”，下列内容全部属于论文阶段缺口，不是开题 blocker：

1. Daft built-in、Ray Data native 和项目路径在当前 commit、同机、同模型、同 normalization、同 source/gather 边界下的统一 operator-E2E 正式排名；
2. 图像任务质量闭环，AI_EMBED 至少报告 Recall@K/nDCG 或经过冻结真值的等价检索指标；
3. 按图像实际 prepare/model work 的组织对照，而不是只按固定图片数；
4. CPU-ready/GPU-ready 队列和 tensor bytes 的状态观测；
5. steady 与 phase-change/burst/mixed-cost 下的同上限 static vs dynamic；
6. 多 job 图像任务的 work fairness 与 tail；
7. 若主张跨模态复用，代码级 capability/trace 证据必须证明 organizer/scheduler 没有 image-specific 分支。

## 8. 图表重构

现有图的主要问题不是配色，而是信息职责重叠：同一结果在柱图和相对改善图中重复；散点形状代表方法但缺少直觉；多个原始点挤在主图里却没有告诉读者哪些是重复、哪些是场景。

建议冻结为四张正文图：

1. **动机图：两条总动机，多个子层证据**。动机一说明记录数不能代表分阶段 AI 工作量；动机二说明单一静态量或局部指标不能代表当前可调度状态。联合图左侧承担动机一，右侧分别承担动机二的配置层与模型服务层；Job 层由多 Job 图单独补充。每个标记直接写含义，不使用未解释散点；在途工作必须注明是峰值还是时间平均。
2. **方法总览图：组织产生调度可消费的工作描述**。上方数据流，下方反馈流；只保留 Cost Adapter、Work Organizer、State Observer、Admission/Router/Fair Queue 与 Model/CPU-GPU stages。
3. **初步机制图：组织策略依赖 serving regime**。用 small multiples 表达相同双卡硬件下 2-endpoint 低 KV 压力约 12% 的策略范围与 4-endpoint consolidation 高 KV 压力下约 27% 的分化；单独用一条简洁机制注释说明 locality hit collapse，不把运行压力误写成硬件池大小，也不画十个形状散点。
4. **图像与代价可行性图**。若一页空间紧张，图像作为正文 hero，代价估计移备份；图像图只画 matched-resource JCT 或 throughput 其中一个，并用小 inset 标 CPU prepare/GPU service 比，不重复画相对改善。

三臂 database-E2E 用紧凑表格或归一化单 panel 展示，等纠正重跑通过门禁后再生成。代价估计的六模型全量图适合备份页；正文只显示 Hybrid 的 median/macro/max regret 和门槛，避免双 panel 重复。

## 9. 答辩内容大纲

当时 28 页版本中“问题范围、证据章节、研究问题、两项方法和验证计划”多次重复，内容主线因此
收敛为 19 个 take-away，附录保留完整表和诊断图。这里记录的是 2026-08-08 的内容收敛动作；
后续 26 页 v9 已生成，当前内容基线见 `opening/opening_defense_outline_20260808.md`。

| 页 | take-away 标题 | 作用 |
|---:|---|---|
| 1 | 数据库 AI 负载的执行优化与调度研究 | 题目 |
| 2 | 数据库已经成为批量 AI 任务的入口 | 场景 |
| 3 | 记录数不能代表分阶段 AI 工作量 | 动机一，文本与图像是两个证据层次 |
| 4 | 单一指标不能代表当前可调度状态 | 动机二的配置层与模型服务层 |
| 5 | 全局服务状态不能代表各 Job 的当前进度 | 动机二的 Job 层 |
| 6 | 两条总动机分别导出两项研究内容 | 问题汇总与角色分层 |
| 7 | 现有数据库与模型服务分别优化两端 | 研究空白 |
| 8 | 本课题只研究两端之间的 AI 数据执行层 | 边界与问题 |
| 9 | 数据组织先生成可被调度消费的 work descriptor | 总体方法 |
| 10 | work-unit 同时保留工作量、局部性与期限 | 研究内容一 |
| 11 | 组织后的 work balance 直接决定 active work 与路由 | 内容一到二的桥 |
| 12 | 状态感知控制只在离线安全包络内动作 | 研究内容二 |
| 13 | 多作业按 work 共享 credit，而不是按请求数平均 | 公平与调度 |
| 14 | 文本和图像复用策略接口，但使用不同阶段 work | 泛化设计 |
| 15 | 先建立饱和强静态，再比较同上限动态 | 因果实验设计 |
| 16 | 纠正后的统一三臂给出开题静态基线 | 前期证据 |
| 17 | 组织 regime、图像结构和代价估计给出初步信号 | 前期证据 |
| 18 | 论文实验按 steady→变化→多 job→跨模态推进 | 计划与停止规则 |
| 19 | 预期贡献是 work 表征与状态感知上游执行方法 | 总结 |

附录放完整三臂表、容量曲线、负结果、MFU/能耗口径、Daft/Ray Data provenance、代价模型六 estimator、风险与答辩问答。

## 10. 验收门槛

- 动机中的每个现象都能指向一个研究挑战和一个后续实验；
- 每张图只有一个结论，图中每个点、线、颜色和阴影均有自然语义；
- work-unit 从“模态名称替换”升级为分阶段 work 合同；
- 动态方法只与同上限、同资源的冻结强静态点比较；
- 图像 baseline、质量、operator-E2E 边界、资源、调度 owner 和当前 commit 合同统一；sink 单列为工程闭环，不混入调度性能归因；
- 开题材料不把计划写成完成，不把 preliminary evidence 写成最终贡献。
