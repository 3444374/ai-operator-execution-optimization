# 语义算子前缀复用：研究候选、因果分析与实现取舍

日期：2026-09-03。状态：设计审查完成；候选机制、质量与性能均待验证。

本文是内部研究设计审查，回答“瓶颈是什么、为什么某种动作可能有效、相对已有工作还需证明什么”。
研究对象仍为 **PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。
本文不是新的架构主计划、运行合同或已完成贡献；工程次序只由
[主计划](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#research-mechanism-slices)维护，
实验对照由 [baseline reference](../experiments/plans/baseline_reference.md#semantic-prefix-causal-controls)维护。
源码核对基线是 main c7b1e9e6；本次没有运行 PG、tokenizer、模型、缓存或性能实验。

## 1. 结论：有条件采纳，不按五个创新点开工

保留“真实 token 前缀是物理条件”和“布局 × 提交时机的因子实验”。撤回以下预设：

- 一处字符串顺序调整就能保持旧语义并获得跨算子复用；
- 普通 credit 保留能够保证远端 KV 驻留；
- 版本化 prompt、质量检查、流水或多 Job 的组合本身就是创新；
- 所有后续工程必须等待一个新布局或前缀实验成功。

第二份评论比第一份克制，但“二维前缀优化”仍只是候选问题的简称，不是新颖性结论。
真正值得检查的是：**数据库只增量提供有限任务、模型服务不暴露可靠逐前缀状态时，何时应保留
同组工作，何时应推进已就绪的后继，何时应放弃亲和去填充可用容量？额外数据库信息是否真的改变
了已有算法的决定，并在相同质量、资源和可见输入条件下减少完整查询时间？**

这仍归入数据组织与提交/路由两项研究内容。多表示是条件性语义策略，代价估计是共同支撑，
多 Job 是第二项内容的独立扩展；不增加第三至第五项研究内容。

## 2. 最近邻文献：哪些区别不能成立

下表是论文正文核验，不是作者代码复现。“未在所查模型中研究”不等于全球文献没有。

| 一手来源与版本 | 已有工作及必须修正的说法 | 本项目仍可检验什么 |
|---|---|---|
| [Optimizing LLM Queries in Relational Data Analytics Workloads](https://proceedings.mlsys.org/paper_files/paper/2025/file/b5dc49f44db2fadc5c4d717c57f4a424-Paper-Conference.pdf)，MLSys 2025，§3–4、§6.1.2、§6.4、Appendix A | GGR 可为不同记录选不同字段序；实验含 Filter→Projection，也检查字段序对准确率的影响。不能概括成“仅单算子、全局单字段序、不管质量” | 完整输入上的重排假设如何适应有限、增量可见的任务；不能仅以新增 Map 或质量检查区分 |
| [Kalypso: Relational LLM Serving](https://arxiv.org/html/2607.23815v2)，arXiv v2，2026-08-14，§4–7 | 已有依赖流水、后继完成前的内存计账，以及默认使用的 best-effort virtual pinning；explicit pinning 另需引擎支持。还有超 token bound 重试，不能移植成自有 stop-only/no-retry 行为 | 在 PG 保有关系结果与取消、外部仅见有限 sealed tasks 的条件下，信息与控制差异是否产生可测损失；“不改 vLLM”本身不足以区分其 virtual 模式 |
| [Making Prompts First-Class Citizens for Adaptive LLM Pipelines](https://www.vldb.org/cidrdb/papers/2026/p26-cetintemel.pdf)，CIDR 2026，SPEAR §2–4 | 有版本化 prompt views 与 refinement；R3 评估 evidence-first 的结构调整和质量/成本。§3 的缓存、batching/fusion 多为设计机会，不能说都已实现 | 对有限候选表示、质量适用性、精确 token 共享与执行成本建立可检验选择规则；不能把 prompt 版本化叫无人区 |
| [BlendServe: Optimizing Offline Inference with Resource-Aware Batching](https://arxiv.org/html/2411.16102v2)，所查 v2 正文标 ASPLOS 2026 | 已联合 prefix tree 与 compute/memory balance；论文实现依赖其执行栈，不能把 GPU 侧数字移作自有 PG 端到端收益 | 固定 prompt 之外的合法表示选择、增量输入与 PG 结果要求；须保留资源均衡强对照 |
| [Locality-aware Fair Scheduling in LLM Serving](https://arxiv.org/html/2501.14312v1)，DLPM/D²LPM，arXiv v1 | 已有公平、本地性、分布式派发及 uncached prompt work 计账；§9 把跨 client 共享分摊和程序依赖列作后续 | 依赖下的外部完成事件记账可以研究，但不能借用引擎内公平定理或只改 counter 名称 |
| [KVFlow: Efficient Prefix Caching for Accelerating LLM-Based Multi-Agent Workflows](https://arxiv.org/html/2507.07400v1)，所查 arXiv v1 | 已将工作流 future-use 与共享 prefix-tree node 结合，修改缓存/预取行为 | 黑盒服务下的可观察信息与可执行动作不同；“横向边＋纵向边”不能单独作为新增方法 |

SPEAR 是本次有界补充检索发现的直接相关工作，不并入 Top 15 或冒充全文精读完成。
另检索到 [Prompt Cache](https://proceedings.mlsys.org/paper_files/paper/2024/hash/a66caa1703fe34705a4368c3014c1966-Abstract-Conference.html)
与 [BatchLLM](https://proceedings.mlsys.org/paper_files/paper/2026/hash/5b7ae1758452854dee4e962207d38304-Abstract-Conference.html)；
本次只核对官方摘要，属于正式锁定论文贡献前需继续阅读的线索，不能声称已经排除重叠。

## 3. 源码事实不等于瓶颈证据

### 3.1 当前代码确实表达了什么

| 已核对位置 | 当前事实 | 设计含义 |
|---|---|---|
| [sem_operator_machine.c](../code/postgres/semloom_pg/src/sem_operator_machine.c)、[sem_filter_machine.c](../code/postgres/semloom_pg/src/sem_filter_machine.c)、[sem_message_writer.c](../code/postgres/semloom_pg/src/sem_message_writer.c) | 公共 machine 已做分发；Filter 自己构造 system directive/instruction，writer 编码 system/user | 附件旧行号和“三行常量改造”已过期；公共编码器不拥有布局策略 |
| [semantic_map.py](../code/src/execution_provider/semantic_map.py) | Map 是 verbatim instruction→system、text input→user；Python 值和 v5 已有，生成 Map 的 PG 接线尚未完成 | 与 Filter 的 system 内容一般不同，但未运行实际 tokenizer，不能给出真实共同 token 数 |
| [sem_pump.c](../code/postgres/semloom_pg/src/sem_pump.c)、[ai_provider_port.h](../code/postgres/semloom_pg/src/ai_provider_port.h) | child slot/借用值只需存活到同步 drive 完成；下一项可复用 slot/tuple context | 不是泄漏或坏设计；多在途需要新的所有权与有界结果存储，不是换函数名 |
| [server.py](../code/src/execution_provider/server.py)、[semantic_session.py](../code/src/execution_provider/adapters/semantic_session.py) | 单固定 Adapter，按完整 session 串行服务；一个 session 对应一份 spec | 是多节点接入前的进展限制，不是已经测出的模型性能瓶颈；不能偷偷把两个算子塞进一个 spec |
| [wire/v5.py](../code/src/execution_provider/wire/v5.py) | protocol 5 / plan schema 4 / max inflight 1 已用于同步生成 Map | 新布局或异步不占用这些身份，不能只给旧帧增加字段 |
| [models.py](../code/src/scheduling/core/models.py)、[policies.py](../code/src/scheduling/endpoint_routing/policies.py) | kv_usage 可为空，是 endpoint 总量；prefix_key 是不透明分组；HRW 目标不可用时可以转到其他 endpoint | 有字段不等于已测逐前缀驻留；相同 key 不保证同 endpoint，更不保证命中 |
| [fixed HTTP Adapter](../code/src/execution_provider/adapters/openai_compatible_fixed.py) | 当前只返回 raw/model/finish/两类 usage，没有转发 cached_tokens | 不能写“加一行即可有可靠逐任务命中证据”；先核对服务版本和指标语义 |
| [sem_filter_cost.h](../code/postgres/semloom_pg/src/sem_filter_cost.h)、[work.py](../code/src/planning/work.py) | 已有代价与校准表示、分阶段 work；没有布局质量证明 | 质量、成本、可执行能力分别管理；不把新证书塞进 Filter cost struct 伪造通用对象 |

本次 9 项无网络源码/符号检查复核了上述消息、版本、摘要分组、telemetry 和 HRW 行为，并检查了下文
两个数学反例；它们不是实际 tokenizer、缓存命中或 PG 性能证据。

### 3.2 必须先区分的四种损失

| 候选损失 | 必须观察的信号 | 不能直接得出的结论 |
|---|---|---|
| 上游供给不足 | 已有合法 ready work 且有容量，却长时间未提交；同时记录 source/编码/等待和服务 busy/idle | 看到同步 drive 不能推出所有 workload 都 GPU 饥饿；无 ready work 也不一定是调度错误 |
| 可避免的 prefill 重算 | 相同实际 token blocks、本可在同 cache domain 复用，却重复处理；cache-on/off 与时序证据一致 | 相同 tuple、短公共 system 或高 kv_usage 不证明存在这一损失 |
| 亲和引发排队/容量闲置 | 相同资源上限下，等待缓存目标增加的时间大于可节省的工作；其他目标可接受任务 | prefix hit 更高不必然使查询更快 |
| 数据准备/桥接/重排成本 | A/B/C/D 及同路径消融中的 CPU、bytes、queue、reorder 和 sink 时间 | RSS/FD 不增长不代表开销可忽略；吞吐差不能直接拆成相加的阶段耗时 |

历史[数据组织结果](../experiments/results/rc1_data_organization/README.md)说明策略效果依赖服务与局部性条件，
但其外部 workload 身份不能重标为当前 PG 多算子结果。既有负结果仍有效，不能因为换了候选名称就重置。

## 4. 第一性原理：优化的是可兑现的工作，不是命中率数字

### 4.1 从机会到收益还差哪些条件

定义 p_v 为一次请求经**实际模型 tokenizer/chat template**形成的 token 序列，b 为该部署的实际
cache block 大小。两条请求的最长公共前缀（LCP）只给出潜在复用量：

    potential_blocks(u,v) = floor(LCP(p_u,p_v) / b)

这是同模型/cache 兼容条件下的机会估计，不是实际 cached_tokens。实际命中还取决于块是否算完、
仍驻留、cache namespace/权限、引擎布局及最后 token 等实现条件；混合 attention/multimodal 需另查。
[vLLM 官方设计](https://docs.vllm.ai/en/stable/design/prefix_caching/)说明块哈希关联前序 token，
额外模型输入和 cache salt 也影响复用；参数值须核对实际版本，不能固定写成所有服务都 16 tokens。

如果不可避免的其他工作已占绝大多数，再高的复用率也未必改变端到端时间。先估计可避免 prefill
在当前关键路径中的占比。仅在其他阶段不变、串行近似适用时，Amdahl 型上界 1/(1-f) 才能帮助
判断“完全消除占比 f 的工作”是否值得做；并发流水用实际事件/关键路径分析，不能机械套这个上界。
节省的算时还须覆盖 tokenizer、分组、额外复制、等待、重排及预取后未消费的 work。

### 4.2 横向、纵向不是两份可以相加的缓存

横向表示不同 tuple 之间共享，纵向表示依赖算子对同一上下文共享。这只是复用来源，不天然是
互相冲突的资源。符号反例（不是实际 tokenizer）：

~~~text
Filter A = common | category X | document A | filter task
Map A    = common | category X | document A | map task
Filter B = common | category X | document B | filter task
Map B    = common | category X | document B | map task
~~~

按 A 的 Filter→Map，再 B 的 Filter→Map 执行，仍保留 category X 前缀。缓存足够时，先做所有 Filter
也可复用相同块。因此要先展示**有限容量、候选表示、就绪时机或不同分组使最优动作确实改变**的
反例，才把“二维冲突”作为问题；不能仅画两类边就宣布新优化问题。

对同一请求只统计实际兼容前缀的最长连续命中，不叠加包含关系里的两份 saved work。若横向给出
3 个符号 token、纵向给出包含它的 5 个，最多是 5，不是 8。对随机驻留状态，正确概念是
E[saved_work(longest_resident_prefix)]，不是把所有 pairwise gain 求和，也不是只对最长历史请求
乘一个命中概率；较短前缀可能仍然驻留。初版可只用经过验证的保守前缀档位，不必先建复杂概率模型。

### 4.3 标识、容量、目标函数各管一件事

- **逻辑调用标识**关联一次查询/节点/行的结果；内容相同的两个调用仍是两项。
- **内容亲和标识**来自兼容域与实际共享 token 前缀。不能用 hash(semantic_spec_digest + row_id)：
  Filter/Map 的 spec 不同会错误拆组，相同行号也不证明内容相同。全量 payload digest 同样不表示前缀。
- **位置观测**另外绑定 endpoint/cache incarnation 和观测时间；模型兼容不表示两个 GPU 已共享 KV。
  provider 实现身份、部署证明与亲和 key 不能混成一个 digest。摘要也不自动匿名化低熵敏感输入。
- **活动任务容量**用 request/work cap；host 输入与完成缓冲用 bytes cap；GPU KV bytes 是另一个量。
  不把 token-work credit 当 KV 内存，不因预计命中就未经校准地下调安全容量扣费。

单查询主目标是查询完成时间 T_q = 最后所需结果完成时刻 − 查询开始时刻，并同时限制首结果延迟、
内存和未消费 work；多 Job 再预先规定加权 JCT 或服务目标。sum(queue + prefill + decode) 不是
并行查询 JCT：两项各 10 秒的工作并行完成可用 10 秒，求和却是 20 秒。
prefill/decode 在 continuous batching 中还会相互影响，saved tokens 不是固定速率的秒数。

数据库信息只有在提供额外可用信息时才有意义。例如一个 tuple 的纵向机会可写成：

    expected_reuse_work = P(后继实际需要执行) × E[可省 prefill work | 后继需要执行]

第一项包含 Filter 生存、下游 LIMIT/消费需求等；第二项包含消息兼容、同 cache domain 与复用时机。
这个条件期望写法不假设选择率与驻留独立。两项都不能从行号或总 kv_usage 直接得到；预测来自合法
统计/独立样本，真实后继仍须等条件满足才生成。低生存率会压低纵向收益上界，长 decode 可能掩盖
prefill 节省；关系分组也可能已经让普通 prefix-first 足够好。这些是要测的条件，不是预设结论。
如果已就绪 prompt 本身已包含所有有用信息，数据库 hint 只是重复编码，就不能主张信息层面的创新。

## 5. 两种候选分别实现，先固定最小动作

### 5.1 不改语义：有限可见任务中的组织与提交

输入是已编译 messages 不变的 sealed tasks、各自 estimated stage work，以及合法、已就绪的有限
集合 R(t)。上限分别为 ready requests/work/bytes、active requests/work 和未交付结果 bytes。
不能为凑组全量扫描，不提前生成尚未满足 Filter 条件的 Map，也不窥视将来输出或测试集结果。

先在同一执行核心保留固定顺序、固定 work budget、普通最短队列/最少 work、HRW 亲和对照。
最小候选按以下流程设计；这是待验证工程/算法候选，不预称新算法：

1. 先排除依赖未满足、数据外发不允许、模型/程序不兼容及容量不够的动作；公平/最长等待要求也在
   此处限制候选。安全要求不是可以由高 cache score 抵消的权重。
2. 在同一 R(t) 中比较“立即提交普通可用目标”与“提交可复用目标/保持同组顺序”。保持 request
   独立，不合并 prompt。先只重排已就绪项，不为等待未知 future tuple 建新的聚合定时器。
3. 仅当可节省工作折算的**保守收益**超过新增 queue/组织/重排成本及预测不确定性时，采用亲和动作；
   区间重叠、观测过期或不支持前缀信息时，使用相同上限的普通动作。首版不学习任意多参数加权分数。
4. 若随后证据显示等待更多输入有价值，再单独增加有限等待动作；计入 source demand、idle time、
   首结果与取消浪费。不能以维持软亲和为由占住已完成任务的活动容量。
5. 独立采集预测与实际结果；预测只能使用决定之前可见的信息。任务完整终态用于资源计账，
   本地断连但远端结果未知的 work 另记，不能当作已完成或立即释放远端容量。

假设的收益来源是减少不必要重算或错误等待，而非更换模型、减少调用、扩大 K/W 或接管引擎批处理。
same-message 表示程序/请求不变，不承诺真实模型逐字确定；并发改变可能带来数值或服务非确定性，
仍需按该任务的质量要求检验，不能以 temperature=0 豁免。
同一可见集中的普通 longest-prefix/最少 work 策略若已足够，新状态机只算工程实现。
只有真实依赖消费者出现后再传最小 lineage；多 Job 可独立研究，不依赖这个前缀候选成功。

### 5.2 改变表示：数据库授权的候选 prompt 程序

当前输入是一个 text，并没有可任意重排的关系字段。未来如研究字段顺序，先有显式字段 schema、
名称/类型/NULL 与序列化规则、合法 permutation 和 PG 列依赖；不能在 gateway 中猜测并拆分不透明文本。
PG 统计只用于其能解释的字段分布，统计不足时标明未知，不能把 text 的 OID/列号当字段语义。

新的 common-system + DATA/TASK 或多消息表示都是**新 prompt 程序**，会改变指令角色和模型行为。
不把不可信 tuple 提升到 system；在 user 中加标签/转义也不证明防注入，必须测试分隔符冲突、
伪造指令、Unicode、长文本、NULL/空串和真实模板的角色处理。旧 Map/Filter messages 原样保留。

首个表示实验只比较 canonical 与一个明确候选，不预建三种布局、自动 prompt 搜索或逐 tuple 切换。
若它们需要共同 system 才可跨算子复用，选择对象是**整个已支持链上的程序组合**，不能各算子
独立最小化局部 cost 后假定全链兼容。第一版每次 query 固定选定组合，运行期不换表示。

质量证据是指定任务分布下的统计结果，不称为数据库证明了语义等价：

- 明确 reference-relative agreement 与独立标签/任务质量的区别。不合格 reference 的高 agreement
  可以作差异诊断，不能为新表示取得生产质量资格；Filter 旧失败不因此改判。
- 固定任务/model revision、prompt/parser/generation、数据划分、质量指标、允许退化量、置信方法与
  样本量依据；重复同一例不算新的独立样本。选择布局的样本与最后验证样本分开；比较多个候选时处理选择偏差。
- 质量 artifact 与 cost artifact 独立；前者决定候选是否允许，后者比较允许候选。记录实际样本、
  版本和适用范围，不把 JSON digest 当统计证明，不把 prompt 的估计 hit rate 混入 quality struct。
- PG 保存显式授权、选定程序/算法与 artifact 引用，保持计划复制、prepared recheck 和 EXPLAIN；
  不能把会变结果的布局未经授权放进普通等价 Path 集合，仅由较低 cost 获胜。
- 资格不存在、过期或适用性不符时，在选用候选前保留 canonical 或拒绝；已经执行后的质量回退需要
  另定重算、预算和结果处理，不能假设 gateway 运行时知道答案对错。

即使所有表示实验失败，5.1 的 same-message 组织/提交与 Map 接入仍可继续。表示优化不是自有系统
可运行的前提，也不是绕过既有 Filter 质量条件的方式。

### 5.3 实现时保留的职责

纯语义 Module 构造选定程序；PG planner/carrier 保存合法选择与列/节点关系；PG runtime 管 owned
tuple、结果关联、取消和释放；gateway 转换版本化任务；SemLoom 组织/准入/路由；模型 Adapter
解释供应商指标；引擎拥有真正 KV。公共 JSON writer 不计算策略，Filter cost 不成为通用认证中心。
具体文件、切片及完成条件见[主计划](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#research-mechanism-slices)。

缓存 telemetry 与 SQL 结果有效性分离。有效 SQL completion 不因 cached_tokens 缺失而失败，
但没有可信命中证据的 run 不能宣称命中归因。可选 telemetry 仍要验证范围、来源、时间与 task/
deployment 关联；“可选”不表示任意值可信。它可以进入独立版本化审计记录，但不暗改旧 completion
字段或摘要；若要协议绑定，也须独立设计。普通 HTTP 的总 usage 不自动暴露逐前缀驻留。

## 6. 需要什么证据才能写进论文

详细的 baseline、2×2、质量/任务量混淆、cache-on/off、同窗口与选择 regret 要求见
[因果对照说明](../experiments/plans/baseline_reference.md#semantic-prefix-causal-controls)。
本节只定义每个研究判断需要的证据，不授权运行：

| 待验证命题 | 支持它需要什么 | 否定后怎样收敛 |
|---|---|---|
| 有足够、可兑现的前缀机会 | 实际 tokens/完整 blocks、相同兼容域、冷/暖及服务信号核对，且 prefill 在可改善路径中占比足够 | 没有共同完整块或上界小于新增成本时，停止该表示/场景，不修改 wire 来追求不存在的机会 |
| 同量 work 的组织/提交优于简单方法 | 相同 prompt、调用、容量、可见窗口下超过最强固定/简单控制，报告开销、首结果、尾部和负条件 | 只赢串行或小窗口对照时，结论限于供给/窗口工程改进 |
| 表示与时机需要共同选择 | 2×2 交互与实际命中变化一致；进一步在不同 workload 上发生候选排序改变 | 两者可独立选择就保留分层，不为“联合”而联合；单个优势布局直接固定 |
| 数据库信息带来独立价值 | 同一实现中移除/打乱合法 relation/dependency hint，与相同 prompt-only prefix 策略比较；候选窗口生成规则一致 | 没有额外作用则不主张 database-aware 算法增量；SQL/生命周期仅作为载体价值 |
| cost 支持更好的决定 | 独立 held-out 上比较简单 token/静态模型与候选，报告选择错误和有约束 oracle regret | 只降低回归误差却不改善选择时，不增加模型复杂度 |

论文可能成立的三层叙述是：测出一个明确损失；提出一个利用特定信息且动作有限的方法；在真实
PG18.3 接入及强对照下验证收益、代价和不成立条件。当前只有设计与部分底座，不能把这三层写成
完成的贡献。Sema/Cortex/LOTUS-like 算子优化仍独立保留，不被本前缀候选替代。

## 7. 方案评估与可行性

### 7.1 First impression

Paper type：New Setting + 待验证的 Novel Method，不是已证明 Novel Problem。
一句话候选：在合法表示与有限增量输入下，用可校验的工作收益和等待代价决定何时利用局部性，
并检验数据库信息是否比 prompt-only 方法多带来收益。

### 7.2 Fatal-flaws audit

| 风险 | 程度 | 防守动作 |
|---|---|---|
| 与最近邻重复，只把既有流水/多表示/亲和换成 PG 名称 | MAJOR，尚可收窄；未发现不可修复的 CRITICAL 条件 | 本文 §2 的逐项比较＋§6 的信息消融与同条件强对照；没有增量就不锁定该论文主张 |
| 五项同时实施，观测不到独立原因 | 设计层可修正 | 拆为 same-message 执行和新表示资格；工程进展不绑定候选成功 |

### 7.3 Lifecycle and capability match

属于 Frontier Exploration 与系统方法研发。已有 C/Python 值、PG18.3 carrier 和外部调度代码可复用；
Map 的后续单 GPU/Qwen2.5-7B/vLLM 0.25.1 配置只属于既定 capability 计划，本次未检查实时资源。
周有效工时、论文期限与新实验预算未提供，时间匹配为 Yellow/待确认，不承诺“半天四臂”或数日完工。
初步 token/值检查可无模型；性能从获准的单 endpoint 起步，需要路由问题时才增加独立 endpoint。

### 7.4 Five-dimension radar

分数仅表示当前方案论证的主观强度，不是实测增益、概率或论文接收预测。

| 维度 | 1–10 | 依据及需要补什么 |
|---|---|---|
| Higher：质量提高 | 2 | 目标是保持质量，不是新预测方法；新布局甚至可能降低质量 |
| Faster：执行效率 | 7 | 有明确重算/等待动作和可证伪对照；尚无本路径实测倍率，是主要验证方向 |
| Stronger：稳健性 | 5 | 有不确定性退回普通策略和版本/资源条件；新 workload 泛化仍需验证 |
| Cheaper：总体成本 | 4 | 可能省 GPU work，但标注、tokenizer 和额外组织有成本；HTTP 计费也未必随 cached work 同比例下降 |
| Broader：可迁移性 | 6 | 同一中立核心可接 PG 与公开 producer；这是可验证工程结构，移植本身不证明算法新颖 |

### 7.5 Paradigm-shift probe

First Principles：Partial，已有工作已经质疑 request-centric 执行；本候选收窄信息限制。
Elephant in the Room：No，未证明存在无人处理的公认重大问题。
Technology Cycle：Yes，原生语义算子与自动前缀缓存使问题有现实意义，但不是本项目独占条件。
Hamming's Rule：Partial，可能改善昂贵查询，尚不足以声称改变领域优先级。
定位为有条件的系统方法增量，不作范式变革承诺。

### 7.6 Feasibility

| 风险 | 当前程度 | 最小控制 |
|---|---|---|
| 计算与运行预算 | 未批准新性能实验 | 单 endpoint、有限候选，独立预算；不借用 Map 的 32 次能力验证额度 |
| 标签/数据 | 新表示质量依据不足 | 公共/获准数据，独立标签与开发/验证划分；不用私有公司样例或旧失败 reference 自证 |
| 工程 | 多在途与组合尚缺真实接入 | 继续主计划小切片；前缀不成功也不阻塞共享执行和生命周期工作 |
| 可观测性 | 当前无逐任务缓存证据 | 先核对版本与观测等级，不可用就保留无亲和基线或缩小归因范围 |
| 工期 | 未知 | 以证据完成条件推进，另确认资源与论文期限，不虚构个人能力评分 |

### 7.7 Integrity gate

证据、来源、风险、评分与判定一致；新颖性与有效性均明确为未证实。能力/工期信息不完整，
不输出完整进度承诺；文献更新速度需持续核对，正式投稿前重新检索。没有新增模型或 PG 测试证据。

### 7.8 Verdict

**Accept with Revisions：接受收窄后的验证路线，不接受原五个创新点作为实施或论文结论。**

## 8. 当前交付与下一步

本次交付是经过源码和一手文献修正的候选设计、对照要求与工程切片映射。
Map 按已有四 D 合同继续；SemLoom 增量核心按既有 Interface 表征推进。候选先写一个具体 workload
上的机会与反例记录，再决定是否编写 token 分析工具、同量任务实验或新的表示合同。
当前不新增生产 layout registry、复合 pump、prefix lease、协议字段、模型调用或正式实验。
