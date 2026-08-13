# 开题答辩攻击面与问答库

冻结基线：2026-08-09。本文档用于内部演练，回答必须服从 `opening/claim_matrix.md` 的证据等级。原则是先承认证据边界，再说明研究问题为何仍成立；不把计划写成成果，也不靠更换 workload 掩盖负结果。

## 一句话版本

### 你的课题究竟研究什么？

> 我研究数据库触发 AI 算子后、数据进入模型服务之前的 AI 数据执行层：如何把表中记录组织成计算量可控的 work unit，并依据服务容量与运行状态控制提交、路由和多作业共享。数据库是任务入口和结果 sink，vLLM 是模型服务，Ray/Daft 是实现与实验平台；我不修改数据库内核、vLLM continuous batching、模型结构或 GPU kernel。

### 两项研究内容是什么？

> 第一项是 workload-aware 的 work-unit 构造，解决固定行数不能代表 token/frame 计算量，以及 work balance 与 prefix locality 冲突的问题。第二项是 runtime-state-aware 的提交、路由与多作业调度，目标是在冻结资源和上限下维持最小饱和工作量，并控制尾延迟、公平性与过载。轻量代价估计为两项内容提供 work、JCT 和配置选择信号，不单列为第三项研究内容。

## 题目、边界与创新性

### 题目会不会太大？

> 题目中的“数据库 AI 负载”定义 workload 和外部执行场景，不代表我要同时改数据库、模型服务和存储内核。方法只作用在 Database → AI Data Execution Layer → Model Service 这条路径的 work-unit、admission、routing 和 multi-job coordination。调度主实验以完整结果 gather 为边界；统一 PostgreSQL/pgvector sink 只做独立正确性与 database-E2E 护栏，不是每组实验的必选阶段，也不是独立贡献。

风险提示：如果正文继续罗列大量数据库内核、向量数据库或模型 kernel 工作，却没有回到上述边界，委员有理由认为范围失控。报告与 PPT 必须用一张系统抽象图反复固定边界。

### 这是不是“把 Daft 和 Ray 接起来”？

> 不是。集成只是搭建可观测实验对象。研究变量是 work unit 如何构造、在途 work 如何约束、请求何时提交、向哪个 endpoint 路由以及多作业如何共享 credit。已有实验已经显示，换成更复杂的 actor pool、AIMD 或 EWMA 并不会自动更快，因此论文贡献不能写成框架集成或“用了动态策略”，必须来自与同上限强静态点的因果对照。

### vLLM 已经有 continuous batching，为什么还需要你的工作？

> vLLM 处理已经到达服务端的请求，决定 token 级调度与 KV 管理；它不决定数据库行怎样形成请求，也不知道一个数据库 job 的剩余 work、SLO 或公平权重。我的控制面位于服务端之前：先把数据库记录组织成 work unit，再依据容量和状态决定 admitted work、routing 和 job 间分配。两层会相互作用，所以实验要冻结 vLLM 配置并显式记录 KV、running/waiting、TTFT 与 ITL。

### 为什么这仍然是数据库课题？

> 任务由 SQL/数据库表触发，输入具有行、列、谓词和 job 边界，输出必须 exactly-once 地回到统一数据库 sink，并以数据库端到端 JCT 和任务质量评价。普通在线 serving 只看到独立请求；数据库场景还需要批量扫描、work-unit 构造、跨行 locality、多 job 查询语义和结果写回。这些约束决定了优化对象不是一个通用 HTTP 客户端。

### 为什么不做第二种数据库？

> 开题阶段的关键问题是控制变量和证据闭环，而不是产品数量。第二数据库会引入不同 AI 函数语义、source 和计时边界，反而削弱因果比较。当前已有文本三臂用统一 PostgreSQL source/sink 提供一次 database-E2E 护栏；方法消融统一到完整结果 gather。若论文阶段需要外部有效性，再在方法稳定后增加数据库，而不是在开题前铺大矩阵。

## 现有证据能证明什么

### 四个等权部件各自最强的动机证据是什么？

> 第一，Work Unit：固定16行的batch token最小/最大为474/6,793，相差14.3倍；图像的CPU prepare/GPU actor又相差13.8–31.2倍，因此需要staged work表达。第二，状态感知：相同W65K上限下，high和arrival-limited的MFU约为35%/7%，原生单Job又呈现overqueue/underfeed两种形态，证明静态配置不是当前状态。第三，动态与多作业调度：5s guaranteed-overlap证明后到long会伤害short；online中shared提高总吞吐却损害short/Jain，eager中shared相对static同时改善short、long、吞吐和Jain，方向反转证明必须感知arrival regime并显式约束效率、隔离和公平。第四，算子代价估计：20个context中候选配置选错代价为12.0%–86.5%，简单proxy的selection regret很高，CE5才勉强跨过预注册决策门。四条都是“为什么这样设计”的证据，不是“最终方法已完成”的证据。

### 为什么不是“动态策略已经有效”？

> 因为现有多项动态候选没有超过强静态点：AIMD、PID、adaptive flush、service quantum 和多 actor 多数未过约5%晋级门槛。5s short/long实验补齐了两层证据：原生路径观察多应用竞争；项目则用full/half single与static/shared分离quota和竞争。在线replay中shared提高总吞吐但伤害short/Jain；eager中half quota本身使short JCT+59.00%，shared通过idle borrowing相对static使short JCT−48.94%、总吞吐+31.85%、Jain 0.894→0.972，但相对full single仍受long影响+28.90%。方向随arrival regime反转，所以需要状态感知和SLO/fairness guard，不能说动态已经普遍胜出。

### 如果两个 Job 没有执行时间重叠，还能证明多 Job 干扰吗？

> 不能证明“后到 Job 干扰已经运行的前台 Job”。只有当两个 Job 已经同时到达、但框架主动把其中一个排队串行化时，零执行重叠才能证明 admission/HOL 阻塞；如果 short 自然完成后 long 才到达，则没有制造竞争。旧15s Daft Native属于后者。当前5s offset下各原生路径都真实overlap；项目又分别在在线与eager合同下使用full/half single控制。eager结果显示quota-only并非总可忽略，因此归因必须使用同quota baseline。

### 当前 5 s 实验到底回答哪个到达方向？为什么不换成 Long→Short？

> 当前冻结方向是 `Short@0s → Long@5s`，回答“后到 long 会不会伤害已经运行的 short”，与本轮动机问题完全一致。`Long→Short` 回答的是“long 已占用服务时，新到 short 的排队/SLO”，同样有价值但属于不同反事实；它不能替代当前结果，也不是证明当前前台干扰所必需。论文阶段若研究优先级/SLO guard，再用 standalone-normalized offset 单独补该方向，不能把两个方向混成一次实验。

### 各系统都用 5 s offset，干扰强度能直接比吗？

> 不能直接做系统间的“抗干扰排名”。5 s 只对齐 Job 级 `Short@0→Long@5` 启动轨迹：项目在每个 Job 内按 `arrival_time_scale=0.001` 逐请求 replay，原生 Daft/Ray Data graph 则在 Job 启动后获得完整 manifest，没有相同的逐请求到达轨迹；各系统的 standalone JCT 和 long 到达时所处生命周期比例也不同。因此只报各轨内部的 single→overlap 变化，不混排绝对 JCT。若论文阶段需要比较干扰敏感度，必须再统一 request-arrival 合同，并按 standalone 生命周期比例做机制实验。

### 为什么项目 single short 要 71.24 s，而 Daft Native 只要 11.06 s？

> 首先不能把它读成“项目慢 6.4 倍”。逐请求 raw 已精确分解：项目71.2416s=66.875s arrival span+4.3666s最后 drain，93.87%是冻结的在线 replay。all-at-t0 Project 1+3 已完成：T0 profiler E2E14.957s，严格对齐到T3“最早模型提交→最晚响应完成”为11.354s；Daft Native 同边界11.059s，Project仅+2.67%，service tokens/s与MFU也只差−2.48%/−2.52%。所以没有Project模型请求路径慢6.4×的证据。Daft 的manifest/provider/DataFrame/expression准备在现有timer之前，缺匹配T0，完整E2E仍不能排名；在线replay与eager诊断也不能互相替换。

### 为什么不把 short 调到喂满 GPU？能否只重测项目？

> 保持在线语义时不能提交尚未到达的请求；66.875s arrival span是offered load，不是K/W不足。Project all-at-t0将service throughput/MFU提高到14,361tok/s/42.93%，与Daft短Job只差约2.5%，无需扫K256/K512或把short扩到60s。公平long影响重测已完成：同一Project runner下full single、half single、static+long、shared+long各1+3，arrival span 66.76µs、12/12 formal通过；Daft/Ray Data原始eager数据复用。该诊断与在线反事实分轨保留，不升级为绝对框架排名。

### 一个 short 加一个 long 足以说明多 Job 管理的必要性吗？为什么不直接做四个 Job？

> 两个 Job 足以回答最小因果问题，因为 arrival、quota 和竞争最容易分离；但现在也已补完受控 `short@0s→3 long@5s` 四 Job 扩展。四个 Job 都是512行，Project有full/quarter single、static/shared配对，三条原生路径各自运行single→four-job三重复。按三次formal均值，shared相对static总吞吐+8.68%，四个Job JCT全部改善，是效率/JCT子向量的baseline-relative empirical Pareto；但这不是完整多目标Pareto。raw-work Jain 0.960→0.923只说明收益更不均，不能单独判定正式公平性质。shared/quarter-solo为0.45/1.29/1.14/0.68，long1/2未达到经验性保留份额非劣；三条原生路径的short和long也全部退化。因此两Job负责干净因果，四Job负责验证多long干扰与三种反事实。现有compact数据不能还原event-level service lag/starvation，也不足以证明weighted fairness、SLO guard、DRF/Themis/VTC性质、Long→Short或新workload稳定性，这些留论文阶段。

### 当前 short/long 多 Job 是文本还是图像？

> 是文本 ShareGPT `AI_COMPLETE`，共享的后端是两个 vLLM/Qwen2.5-7B endpoint。它证明了多作业运行状态与共享权衡在文本场景中存在，不能直接当作图像多 Job 结果。图像当前只已证明 prepare/model 阶段失衡与 matched-resource 静态结构信号；图像 phase-change/two-job dynamic 属于论文阶段。

### 当前四个设计部件是否已经全部接入项目并验证？

> 没有。共享 work credit、completion release、neutral work admission 和 least-work routing 已进入调度器，其中两/四作业 shared/static 已做正式 A/B；staged WorkDescriptor、fresh runtime snapshot 和有界 stage controller 已有类型/单测，但 production descriptor builder 和正式 runner 接入尚未完成；CE5 仍是离线配置选择器。开题可以证明设计动机、接口可执行性和部分机制权衡，不能把完整 state-aware + cost-driven 方法写成已完成贡献。

### Daft Ray 是项目自己的调度路径吗？

> 不是。Daft Native 和 Daft Ray 都调用 Daft `functions.prompt` 的官方/内置执行路径；Daft Ray 只是由 Daft 使用 Ray runner 执行 graph，scheduler owner 仍是 Daft/Ray 原生路径。项目仅负责 manifest、endpoint 和证据采集适配，没有向它注入 shared credit、least-work router 或项目 actor pool。项目方法臂必须另行标记为 project frozen-static/shared-work。

### 负结果是不是说明课题做不下去？

> 负结果排除了错误问题表述：仅增加控制器复杂度并不能带来收益，服务端会吸收一部分上游差异。现在研究问题更窄也更可证伪——只有在 workload 或服务状态发生变化、且冻结上限相同的条件下，state-aware 策略才可能有增量。如果仍不超过静态点，论文结论就收敛为 work-normalized admission、regime 诊断和动态策略失效边界，而不是继续换 workload 找正结果。

### 65,536 是不是最佳参数？

> 不是。它是当前机器、模型、协议和 workload 签名下的“最小近饱和点”：达到最大已测均值的 97.80%，下一档只增 0.92%。它不是 vLLM 内部容量上限，也不能跨机器复用。任何签名变化都必须重新做 scale ramp 和校准。

### K256 已经测了，为什么不再测 K512？

> 当前 vLLM 服务合同的 `max_num_seqs=256`。ShareGPT bounded C32/C64/C128/C256 扫描已覆盖欠供给、最小饱和和过量排队：C128 达 C256 已测峰值的 98.22%，C256 只多 1.82% 吞吐，却将 TTFT mean 从 0.829 s 推到 6.181 s。K512/endpoint 会超出服务同时 resident sequence 上限，主要增加 client/service queue，且改变当前冻结合同。它只对专门研究过载退化有价值，而开题动机已由 C256 闭合，因此不是开题 blocker。两/四 Job 正式 A/B 都在每 endpoint 总 K128/W65,536 的同一上限内比较 static/shared，不把扩大 K 冒充调度收益。

### 数据组织图能否证明 sequential 普遍最好？

> 不能。相同双卡硬件下，2 endpoint 低 KV 压力时五种策略在 50–56k tok/s 范围；4 endpoint consolidation 使 KV 压力达到 98%–100% 时分化到 39–50k，并出现 sequential 优于重排序类。这个结果证明的是“策略排序随 serving regime 改变”以及 work balance 与 locality 冲突，不是硬件池大小变化，也不是某个 organizer 的全局最优性。

### 图像实验 GPU 利用率很低，结果还有意义吗？

> 有，但结论必须缩窄。GPU busy 约 6–10%，说明当前图像链路受 CPU decode/resize/normalize 喂入限制。matched-resource 对照仍能证明显式 CPU/GPU 分级与提交结构在同一瓶颈下减少 JCT，且两次实验、两档 CPU 都同向；它不能证明 GPU 饱和，也不能证明状态感知动态策略已经胜出。

### 为什么旧的 45.7% 不能用？

> 旧比较把项目 cpu16 与 Ray Data cpu8 放在一起，Ray Data 在 60K 规模被低估配。匹配资源和各自最佳静态点后，正式口径是约 13–15%；独立复测给出 10.0%–18.5% 同向范围。保守 headline 用 13–15%，不能把复测端点包装成更窄置信区间。

### 代价估计器已经解决了吗？

> 只能称可行性证据。Hybrid 在 429 个 formal 观测、20 context × 4 candidate 的 context-LOO 中得到 pooled regret 1.67%、macro 2.90%、candidate pairwise 0.808，max regret 14.72% 刚好低于 15% 门槛。但只剩 0.28 个百分点裕量，换 split、更多 context 或硬件可能翻转，所以它是 marginal pass，不是稳健通过。

## Baseline、公平性与实验合同

### 为什么有 direct、DuckDB AI 和项目三条路径？

> direct static-sharded 给出没有数据库产品调度层的强上游参照；DuckDB AI static-sharded 让被测产品拥有自己的执行与调度；project frozen-static 是后续状态感知方法必须超过的冻结项目点。三者共享 PostgreSQL source、immutable manifest、两个 endpoint、模型与 prefix-cache 配置、统一 PostgreSQL sink、外部 database-E2E 计时和任务质量，避免只比较内部 operator wall。

### 为什么 database-E2E 只跑 SQuAD 和 ShareGPT 两组？

> SQuAD short-answer 是均匀控制组，用于检验服务层能否吸收上游差异；ShareGPT controlled-skew 是异质实验组，用于增加 prompt/output work 方差。它们只承担 database-E2E/correctness 护栏。随后冻结的 ShareGPT Chat 原生单 job 与两 job 错峰矩阵承担框架执行和多作业动机，不再增加第三种 workload、第二数据库或更多产品追求更好看的结果。

### SQuAD 上为什么三条静态路径几乎没有差异？

> K128 replacement 中，project、direct、DuckDB AI 的 correct rows/s 为 137.77、136.63、136.68，service tokens/s 为 41,277.95、40,920.72、40,955.99；project/direct service ratio 为 100.87%。SQuAD 是均匀短输出控制组，服务层可以吸收上游静态结构差异，因此近似中性是有效结论。它说明后续动态方法不能靠弱 baseline 获胜，必须在同上限和明确状态变化或资源竞争中证明增量。

### DuckDB AI 的 cap semantic failure 是否意味着实验失败？

> runner 把 transport、worker、timeout 等基础设施失败设为 fail-closed；同时把固定输出 cap 下的产品语义差异单独记录为 cap-semantic failure，并把错误行从 correct rows/s 分子中扣除。这不是静默忽略，也不是把产品错误包装成成功。报告会同时给出 raw rows/s、correct rows/s、failure 类型和 finish reason，读者可以看到性能与语义代价。

ShareGPT replacement 的具体结果是 4,921/6,144 行 cap 语义失败，而基础设施失败为 0。DuckDB AI 的 raw rows/s 为 11.35、service tokens/s 为 9,421.31，几乎等于 direct；correct rows/s 只有 2.26。因此最准确的说法是“模型服务容量没有明显掉速，但当前产品 fixed-cap 语义与该 workload 不兼容”，不能笼统说 DuckDB AI 更慢。

### ShareGPT 异质组是否证明动态方法有效？

> 没有。replacement 中 direct、DuckDB AI、project frozen-static 的 service tokens/s 为 9,425.25、9,421.31、14,568.91；但后续同 manifest 饱和扫描证明 C32 direct 只有已测峰值的 52.07%。所以 154.57% 比值首先是并发/执行结构不匹配，不能作为项目方法收益。有效结论是 database-E2E correctness、DuckDB cap 语义边界，以及必须先按 workload 标定最小饱和点。

### 为什么用 correct rows/s 而不是只用 tokens/s？

> 三条路径可能在输出截断、空结果或任务质量上不同。只报 tokens/s 会奖励产生更多无效 token 或返回错误的路径。SQuAD 用 EM/F1 定义正确行，ShareGPT 用成功、非空和 cap 语义审计；correct rows/s 把质量纳入吞吐分子，同时仍单独报告 raw rows/s 和 service tokens/s。

### 如何证明数据读取、写回和 exactly-once 一致？

> 每个 cell 都从同一 immutable manifest 校验 PostgreSQL source，固定两 endpoint 的行数和 work 分配；完成后写入统一 sink，再以行数和 digest 回读。report.json 同时记录 source manifest SHA、rows written、readback digest、exactly-once、失败数和数据库版本。任一基础设施失败或 sink mismatch 都会使矩阵 fail-closed。

### 为什么两/四 Job 干扰实验没有 sink？文本侧是否必须每次写回？

> 不必须。两/四 Job 实验要隔离的是 shared-vLLM serving 竞争，因此以完整结果 gather、manifest 完成和 exactly-once request 证据为边界，使用 `writeback-mode=none`。若强制 sink，数据库写回会把 serving 干扰与 I/O 竞争混在一起。SQuAD/ShareGPT 三臂已单独用统一 PostgreSQL source/sink 闭合 database-E2E 和 correctness 护栏。因此文本侧的方法主实验可以不做 sink，但不能把它说成 database-E2E 结果。

### PostgreSQL 18.4 与项目要求的 18.3 冲突吗？

> 当前双 4090 AutoDL 运行环境实测是 PostgreSQL 18.4 + pgvector 0.8.5，因此只能标为 AutoDL rehearsal，不能写成内部 PostgreSQL 18.3 平台结论。版本写进每个 cell 的 identity 字段。它不影响三臂在同一环境内的因果比较，但限制外部平台外推；后续正式迁移必须按 runtime preflight 重跑。

### feeding-saturation 门怎么判断？

> 优先使用同 workload 的 bounded concurrency frontier，并联合 service tokens/s、running/waiting、KV、MFU、TTFT 与 time-series GPU utilization。ShareGPT C32 的 GPU mean 约 98% 但吞吐只有峰值 52.07%，直接证明高 GPU utilization 或 waiting=0 都不能单独说明已经喂饱。当前冻结 C128，因为它是达到 C256 已测峰值 97% 的最小点。

### Daft Native、Daft Ray 和 Ray Data 的正式同环境结果说明了什么？

> 同一 2,048-row ShareGPT manifest 的 1+3 formal 中，bounded C128、Daft Native、Daft Ray、Ray Data 的 service tok/s 为 17,800、17,286、16,747、3,551，CV 都低于 0.6%。更重要的是状态不同：Daft 两臂 waiting mean 约 783/742、KV max≈1，属于过量提前提交；Ray Data running mean 17.3、MFU 0.112，属于当前 graph 供给不足。它说明现有原生路径会把同一服务推入不同压力区，需要联合状态感知与有界提交；不能据此说项目已经胜出，也不能把外部现象归因成框架内部算法缺陷。

## 方法设计与可证伪性

### 状态感知策略具体看哪些信号？

> 输入分三类：workload 侧的 predicted work、prefix/frame locality 与 job remaining work；执行侧的 admitted active work、inflight、完成事件和每 endpoint 队列；服务侧的 running/waiting、KV pressure、TTFT/ITL 和吞吐。第一版只用能稳定采集、能在离线回放中解释的少量信号；动态策略始终与同上限 frozen-static 比较。

### 你如何避免“两个研究内容其实是一件事”？

> 数据组织决定“一个 work unit 里放哪些行”，调度提交决定“这些 work unit 何时、向哪里、以多少在途 work 发送”。两者先独立搜索并分别与静态点消融，再把独立最优拼接，与小规模联合 grid 对比。联合显著更好说明需要协同；两者接近则说明可以分层优化。无论结果如何，两项决策变量和因果对照仍可分离。

### 跨模态如何做到不是“再跑一个图像 demo”？

> 公共调度代码只消费 estimated work units、credit、queue 和 completion 事件；文本把 token work 映射为 work units，图像把 frame/pixel/preprocess work 映射为同一合同。不适用所有模态的 prefix locality 通过 capability 显式声明，不能缺列时静默退化。图像实验要保持同一策略代码和配置逻辑，只替换模态 adapter 与任务质量指标。

### 如果最终动态方法仍然不超过静态点，论文还有什么？

> 论文必须预注册三层结果。最好情况是 state-aware 在状态变化或多 job 竞争下超过同上限静态点；中间情况是只改善 tail/SLO/fairness 而吞吐相当；最坏情况是稳定不增益。最坏情况下仍可形成：work-normalized admission 方法、serving-regime 诊断、数据组织与 locality 冲突、强静态点的校准规则，以及复杂动态控制器失效的边界。但不能把这些边界包装成正向性能贡献。

## 攻击面审计表

| 攻击面 | 当前强度 | 最诚实回答 | 材料动作 |
|---|---|---|---|
| 题目范围过大 | 中 | 方法只位于 AI Data Execution Layer；数据库/服务/sink 是边界 | 首页和方法页复用同一系统抽象 |
| 与 vLLM 调度重复 | 中 | vLLM 管已到达请求，本课题管 DB work-unit 与上游 admission/routing | 明确“不修改 vLLM” |
| 只是 Ray/Daft 集成 | 高 | 框架是载体；贡献需来自 work/credit/state 的可证伪策略 | 删除“集成创新”措辞 |
| 动态策略尚未胜出 | 高 | 明确列为待验证，现有负结果用于冻结强静态基线 | 任何页不得写完成时 |
| 接口实现被误写成端到端方法 | 高 | descriptor/snapshot/controller/cost 的生产接入与正式 A/B 尚未齐全 | 方法页分列“当前实现”和“论文待验证” |
| Daft Ray 被误当成项目方法 | 高 | Daft Native/Ray 都是 vendor-owned graph；项目 static/shared 另行标记 | 每张表保留 scheduler owner/provenance |
| 5 s offset 被误用作框架间抗干扰排名 | 高 | 只比各系统内 single→overlap；不比框架间绝对 JCT | 图中标 observational/causal 和 actual overlap |
| 项目 71.24 s 被误读为比 Daft Native 慢 6.4× | 高 | 项目逐请求 replay，原生只对齐 Job 启动且完整 manifest eager 可用；绝对值不 rankable | 正文、图注和问答显式标 request-arrival contract mismatch |
| 两/四 Job 被外推为多租户完整结论 | 高 | 两Job闭合最小因果，四Job只扩展一个offset/equal-weight workload；weighted/SLO/held-out待论文验证 | 主文与图注显式写外推边界 |
| 数据组织 feeding 门有边界 | 中 | 只声称 regime dependency，不声称全局排名 | 图注保留 KV/feeding 条件 |
| 图像 GPU 未饱和 | 中 | 证明 matched-resource 执行结构收益，不证明 GPU-serving 优化 | 报 GPU busy 6–10% |
| 文本多 Job 被外推为图像动态结论 | 中 | 当前 short/long 是 ShareGPT/vLLM；图像只有 staged-work 与静态结构证据 | 图像 phase-change/two-job 留作论文阶段 |
| 代价模型贴线 | 中 | 14.72% 为 marginal pass | 图中画 15% 门槛和 0.28 pp 裕量 |
| database-E2E correctness 与 ShareGPT 产品语义不兼容 | 中 | replacement 24/24 cells；DuckDB ShareGPT 4,921/6,144 cap 失败；旧 ShareGPT C32 对照欠供给 | SQuAD 作静态地基；ShareGPT 标 not rankable；原生矩阵只作状态指纹 |
| 单数据库/单机器外推 | 中 | 开题先闭合因果合同，外部有效性留论文阶段 | 标 AutoDL PG18.4 rehearsal |
| 写回贡献不清 | 低 | sink 只做统一边界、正确性与收益吞噬检查 | 不单列研究内容 |

## 最终答辩红线

- 不说“动态策略已经优于静态策略”。
- 不说“项目路径普遍优于 DuckDB AI、Ray Data 或 direct”。
- 不把 Daft Ray 当成项目调度方法。
- 不说“65K 是 vLLM 最优并发或容量上限”。
- 不把 5 s offset 下各框架的绝对 JCT 做抗干扰排名。
- 不把项目 arrival-replay 71.24 s 与 Daft Native eager-manifest 11.06 s 写成系统性能倍数。
- 不用原 15 s Daft Native 无 overlap 数据证明运行中干扰。
- 不把两/四 Job 文本结果外推到更多 Job 数、weighted/SLO、Long→Short 或图像动态。
- 不说“sequential 是普遍最优 organizer”。
- 不说“图像路径提升 45.7%”。
- 不说“Hybrid 稳健通过”。
- 不把 PostgreSQL 18.4 AutoDL rehearsal 写成 PostgreSQL 18.3 内部平台结论。
- 不把 Ray、Daft、vLLM、CLIP 或 pgvector 的使用本身列为创新点。
