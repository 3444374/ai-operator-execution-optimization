# 开题答辩攻击面与问答库

冻结基线：2026-08-07。本文档用于内部演练，回答必须服从 `opening/claim_matrix.md` 的证据等级。原则是先承认证据边界，再说明研究问题为何仍成立；不把计划写成成果，也不靠更换 workload 掩盖负结果。

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

### 目前最强的四项事实是什么？

> 第一，固定行数不是可靠 work 代理；同一行数下 token 分布跨度可达 13.9 倍。第二，当前双 4090/Qwen 配置存在最小近饱和 active work：65,536/endpoint 已达最大已测吞吐均值的 97.80%，继续增压只带来很小吞吐增量而 P99 上升。第三，数据组织效果取决于 serving regime：大 KV 池下策略近似中性，小 KV 池饱和时排名反转，破坏 prefix locality 会让命中率塌到 0.06–0.07。第四，在图像 workload 的 matched-resource 对照中，项目静态分级 actor 路径把 operator JCT 稳定降低约 13–15%。

### 为什么不是“动态策略已经有效”？

> 因为现有多项动态候选没有超过强静态点：AIMD、PID、adaptive flush、service quantum 和多 actor 多数未过约 5% 晋级门槛。它们证明“动态”不是贡献本身，也帮助冻结了更强的静态基线。开题冻结前用 short/long 两作业错峰补两层证据：Daft/Ray Data 原生路径只观察多应用竞争与可观测性，项目 static-partition vs shared-work-credit 才检验 idle borrowing 的因果增量。同上限 phase-change 与 weighted 仍是论文阶段实验；开题不提前声称动态已经胜出。

### 负结果是不是说明课题做不下去？

> 负结果排除了错误问题表述：仅增加控制器复杂度并不能带来收益，服务端会吸收一部分上游差异。现在研究问题更窄也更可证伪——只有在 workload 或服务状态发生变化、且冻结上限相同的条件下，state-aware 策略才可能有增量。如果仍不超过静态点，论文结论就收敛为 work-normalized admission、regime 诊断和动态策略失效边界，而不是继续换 workload 找正结果。

### 65,536 是不是最佳参数？

> 不是。它是当前机器、模型、协议和 workload 签名下的“最小近饱和点”：达到最大已测均值的 97.80%，下一档只增 0.92%。它不是 vLLM 内部容量上限，也不能跨机器复用。任何签名变化都必须重新做 scale ramp 和校准。

### 数据组织图能否证明 sequential 普遍最好？

> 不能。2 endpoint 大 KV 池下五种策略只在 50–56k tok/s 范围内接近；4 endpoint 小 KV 池饱和时才分化到 39–50k，并出现 sequential 优于重排序类。这个结果证明的是“策略排序随 serving regime 改变”以及 work balance 与 locality 冲突，不是某个 organizer 的全局最优性。

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

> 喂饱后的 K128 replacement 中，project、direct、DuckDB AI 的 correct rows/s 为 137.77、136.63、136.68，service tokens/s 为 41,277.95、40,920.72、40,955.99；project feeding 已达 direct 的 100.87%。SQuAD 是均匀短输出控制组，服务层可以吸收上游静态结构差异，因此近似中性是有效结论。它说明后续动态方法不能靠弱 baseline 获胜，必须在同上限和明确状态变化或资源竞争中证明增量。

### DuckDB AI 的 cap semantic failure 是否意味着实验失败？

> runner 把 transport、worker、timeout 等基础设施失败设为 fail-closed；同时把固定输出 cap 下的产品语义差异单独记录为 cap-semantic failure，并把错误行从 correct rows/s 分子中扣除。这不是静默忽略，也不是把产品错误包装成成功。报告会同时给出 raw rows/s、correct rows/s、failure 类型和 finish reason，读者可以看到性能与语义代价。

ShareGPT replacement 的具体结果是 4,921/6,144 行 cap 语义失败，而基础设施失败为 0。DuckDB AI 的 raw rows/s 为 11.35、service tokens/s 为 9,421.31，几乎等于 direct；correct rows/s 只有 2.26。因此最准确的说法是“模型服务容量没有明显掉速，但当前产品 fixed-cap 语义与该 workload 不兼容”，不能笼统说 DuckDB AI 更慢。

### ShareGPT 异质组是否证明动态方法有效？

> 没有。replacement 中 direct、DuckDB AI、project frozen-static 的 correct rows/s 为 11.36、2.26、17.55；project service tokens/s 为 direct 的 154.57%，DB-E2E 为 116.70 s 对 180.33 s。它证明喂饱后的冻结静态执行结构在该异质 workload 下有明显差异，但项目臂与 direct 的请求成形和执行结构整体不同，不能隔离出状态感知、动态调度或 WorkDescriptor 的因果收益。后续仍需同 source、同上限和明确状态变化的 static/shared A/B；不要求重复 sink。

### 为什么用 correct rows/s 而不是只用 tokens/s？

> 三条路径可能在输出截断、空结果或任务质量上不同。只报 tokens/s 会奖励产生更多无效 token 或返回错误的路径。SQuAD 用 EM/F1 定义正确行，ShareGPT 用成功、非空和 cap 语义审计；correct rows/s 把质量纳入吞吐分子，同时仍单独报告 raw rows/s 和 service tokens/s。

### 如何证明数据读取、写回和 exactly-once 一致？

> 每个 cell 都从同一 immutable manifest 校验 PostgreSQL source，固定两 endpoint 的行数和 work 分配；完成后写入统一 sink，再以行数和 digest 回读。report.json 同时记录 source manifest SHA、rows written、readback digest、exactly-once、失败数和数据库版本。任一基础设施失败或 sink mismatch 都会使矩阵 fail-closed。

### PostgreSQL 18.4 与项目要求的 18.3 冲突吗？

> 当前双 4090 AutoDL 运行环境实测是 PostgreSQL 18.4 + pgvector 0.8.5，因此只能标为 AutoDL rehearsal，不能写成内部 PostgreSQL 18.3 平台结论。版本写进每个 cell 的 identity 字段。它不影响三臂在同一环境内的因果比较，但限制外部平台外推；后续正式迁移必须按 runtime preflight 重跑。

### feeding-saturation 门怎么判断？

> 优先使用运行期 time-series：GPU utilization mean/p50/p95/max、vLLM running/waiting、KV usage，并以同 workload 的 direct bounded client formal mean service tokens/s 为分母。单点 `gpu_utilization_pct` 不能作为结论。replacement 中项目臂 SQuAD/ShareGPT feeding 为 100.87%/154.57%，GPU mean 为 88.39%/97.19%，均已过门；首轮 89.93%/91.38% 只作历史诊断。waiting=0 也不能单独证明已经喂饱。

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
| 数据组织 feeding 门有边界 | 中 | 只声称 regime dependency，不声称全局排名 | 图注保留 KV/feeding 条件 |
| 图像 GPU 未饱和 | 中 | 证明 matched-resource 执行结构收益，不证明 GPU-serving 优化 | 报 GPU busy 6–10% |
| 代价模型贴线 | 中 | 14.72% 为 marginal pass | 图中画 15% 门槛和 0.28 pp 裕量 |
| database-E2E 静态差异随 workload 改变，且 ShareGPT 产品语义不兼容 | 中 | replacement project feeding 为 100.87%/154.57%；DuckDB ShareGPT 4,921/6,144 cap 失败 | 不把静态结构差异归因成动态收益，不新增 workload 追正 |
| 单数据库/单机器外推 | 中 | 开题先闭合因果合同，外部有效性留论文阶段 | 标 AutoDL PG18.4 rehearsal |
| 写回贡献不清 | 低 | sink 只做统一边界、正确性与收益吞噬检查 | 不单列研究内容 |

## 最终答辩红线

- 不说“动态策略已经优于静态策略”。
- 不说“项目路径普遍优于 DuckDB AI、Ray Data 或 direct”。
- 不说“65K 是 vLLM 最优并发或容量上限”。
- 不说“sequential 是普遍最优 organizer”。
- 不说“图像路径提升 45.7%”。
- 不说“Hybrid 稳健通过”。
- 不把 PostgreSQL 18.4 AutoDL rehearsal 写成 PostgreSQL 18.3 内部平台结论。
- 不把 Ray、Daft、vLLM、CLIP 或 pgvector 的使用本身列为创新点。
