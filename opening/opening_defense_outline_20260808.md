# 开题答辩内容大纲与证据合同

日期：2026-08-08（2026-08-10 完成 baseline、两/四作业证据与图集审计）
状态：内容大纲；暂不制作 PPT 成品

## 1. 一句话主线

数据库把数据行交给外部 AI 服务时，记录数不能准确表示计算工作量，固定提交上限也不能适应运行状态与多阶段瓶颈的变化。因此，本课题研究数据库 AI 算子外部执行链路中的两项问题：一是把数据组织成携带分阶段工作量、局部性与期限的 work unit；二是在离线安全容量包络内，依据新鲜运行状态进行准入、路由与多作业共享。轻量算子代价估计同时为两项研究内容提供 stage/service/remaining work、SLO slack 和不确定区间，是共同使能部件，不单列为第三项研究内容。

## 2. 四条同等严格的证据链

```text
Work Unit：同行数的文本 token work 可差 14.3×，图像 prepare/model 成本也不同
  -> 不能用 rows/images 定义可比 work，需要 staged descriptor 和局部性字段
状态感知：相同静态上限在 high/arrival-limited 下对应完全不同的 running/MFU
  -> 需要 fresh stage/service/job snapshot，过期或签名不匹配时回退强静态点
动态调度：多 job 错峰到达时，预分配份额可能空闲，独立 job 并发又可能叠加成全局过载
  -> 需要有界准入、路由、shared work credit 和 idle borrowing，但必须败过同上限强静态点
算子代价估计：不同 context 的四个 active-work 候选 E2E 差 12.0%–86.5%，简单均值/解析/lookup 选择失败
  -> 使用解析结构 + profile 校准 + residual correction，以 ranking/regret 而非只看 MAE 验收
```

四条证据链权重相同：每条都必须说清“为什么做、为什么这样设计、证据支持到哪、尚未证明什么”。代价估计仍是两项研究内容的共同使能部件，不单列为第三项研究内容。

PPT 与报告不得把 baseline、动机和研究内容拆成三个互不相干的章节。每一组材料固定使用
同一条四步句法：`baseline/动机现象 → 暴露的缺口 → 对应研究内容与设计 → 验证实验`。
文本 baseline 分产品 database-E2E 与官方 Chat graph 两轨，导出 work 表达、正确性和状态
感知问题；图像动机先用 prepare/model、transfer 形态和 active-window 现象导出 staged work、
CPU/GPU 队列感知和跨阶段提交，再由独立 baseline 图分开能力门禁、12K 结构诊断与
120K matched-resource 排名。不可排名的合同边界必须显式保留，不能为了版面完整
合并成总排行榜。

## 3. 主讲大纲

| 序号 | take-away 标题 | 必须讲清的内容 | 所需证据或图 | 可声称边界 |
|---:|---|---|---|---|
| 1 | 数据库 AI 负载的执行优化与调度研究 | 题目、对象和边界 | 无 | 不修改数据库内核、vLLM、Ray 调度器或模型 kernel |
| 2 | 数据库正在成为批量 AI 任务的数据入口 | PostgreSQL 行经过数据引擎、外部 AI 执行层和模型服务再写回 | 简洁链路图 | 研究对象是数据库 AI 算子的外部执行链路 |
| 3 | 同样的数据库行数并不代表同样的 AI work | 固定 16 行的 token work 最大/最小相差 14.3 倍 | 动机图左侧 | 行数不是可靠成本代理 |
| 4 | 固定提交压力无法同时避免欠供给与过载 | high 与 arrival-limited 在相同上限下 running/MFU 不同；容量曲线存在近饱和区和 tail 代价 | 动机图右侧 | 动态必要性来自状态变化；尚未证明动态收益 |
| 5 | 图像阶段失衡导出 staged work 与状态观测 | CLIP CPU prepare/GPU service 为 13.8–31.2 倍；R0/R1/R2 transfer 形态差异；active64 吞吐回退且等待增加 | 图像 staged-work 动机图 | microprofile/screening 只证明机制，不作系统排名或动态胜出结论 |
| 6 | 图像 baseline 必须分开能力、诊断与排名 | Direct control；Daft Built-in 12K/20K 扩展边界；Ray Data 原生；vLLM pooling blocked；120K 仅 Ray Data/Project 可排名 | 图像 baseline 分层图 | 12K 三臂不作稳态排名；blocked 路径不生成性能值 |
| 7 | 三类现象导出表征、感知和控制三个挑战 | 现象到挑战再到设计的逐项映射 | 三列表或因果箭头 | 动机测试只证明设计必要性 |
| 8 | 文本 baseline 暴露产品语义与服务供给边界 | SQuAD 产品轨近似中性；DuckDB ShareGPT cap 失败；Daft/Ray Data Chat graph 呈现 overqueue/underfeed | 文本 baseline 分轨图 | 对应 neutral WorkDescriptor、correctness-aware evidence 与状态感知提交；不跨轨排名 |
| 9 | 本课题研究两端之间的 AI Data Execution Layer | 两项研究内容、共同代价估计、多模态验证 | `opening_ai_data_execution_boundary` | 只有两项研究内容；代价估计是共同使能部件 |
| 10 | 代价估计把原始记录变为可决策的分阶段 work | 解析模型、profile 校准、residual correction、预测区间与校准签名 | 代价估计到 WorkDescriptor/调度器的数据流图 | 现有证据只支持初步配置选择价值 |
| 11 | 数据组织输出调度可消费的 WorkDescriptor | row/source/prepare/model/result work、locality、deadline、uncertainty | WorkDescriptor 字段图 | 组织的输出不是只有 payload batch |
| 12 | 数据组织同时平衡 work、局部性和下游队列形态 | token/frame/stage budget；balance 与 prefix/locality 的冲突 | organization regime 图 | 已有结果是 serving-regime-dependent，不宣称普遍胜出 |
| 13 | 状态感知控制只在离线安全包络内动作 | offline safe envelope、fresh snapshot、候选 credit、deadband、回退 | `opening_work_to_schedule_overview` | stale/missing signal 回退强静态点 |
| 14 | 多作业按 work 共享而不是按请求数平均 | shared work credit、deficit/fair queue、idle borrowing、remaining work/SLO slack | 原生四Job归一化图证明Short与全部Long均受影响；Project四Job图拆分quota/competition/shared，shared相对static总吞吐+8.68%、short JCT−72.23%，但Jain下降 | 已证明idle borrowing与效率—公平权衡；weighted/held-out、SLO guard仍需论文阶段验证 |
| 15 | 文本和图像复用接口，但主导 stage 不同 | text token work；image prepare/model/tensor work；同一 descriptor/state/controller 接口 | 跨模态映射表 | 泛化是接口复用，不是假设两种负载成本相同 |
| 16 | 因果评估必须先冻结饱和强静态点 | 同资源、同最大 K/work、同 source、同完整结果语义；dynamic 仅改变策略 | 实验合同图 | 未通过 feeding/correctness/stability 的数据不得排名；sink 仅用于 database-E2E 护栏 |
| 17 | 原生 graph 将同一服务推入不同压力区 | bounded C128 最小饱和；Daft Native/Ray overqueue；Ray Data 当前路径 underfeed | `opening_native_single_job_state_fingerprint`（报告/答辩备份） | 只讲外部现象；database-E2E 护栏与 DuckDB raw/correct 放 appendix 表 |
| 18 | 前期证据分别覆盖组织、图像结构和代价选择质量 | organization regime、image matched-resource、cost decision regret | 三个不重复的小图或一页表 | 都标为 preliminary/conditional evidence |
| 19 | 论文主实验按稳态、变化、多作业、跨模态推进 | steady no-regression；phase shift/burst/mixed-cost；multi-job；image | 实验路线与停止规则 | K512、VLM、故障迁移不是开题 blocker |
| 20 | 贡献是统一 work 表征与状态感知上游执行方法 | 两项研究内容、共同使能代价估计、多模态验证和严格实验合同 | 一页总结 | 不把工程集成或弱 baseline 写成贡献 |

## 4. 已落地的数据与可排名边界

### 4.1 两组 replacement database-E2E

| workload | 作用 | 三臂 | 当前动作 | 必过门禁 |
|---|---|---|---|---|
| SQuAD 均匀控制组 | 验证统一 source/sink 和质量口径 | bounded direct static-sharded、DuckDB AI static-sharded、project frozen-static | correctness/稳定性通过；project/direct service=1.0087，近似中性 | 只作该 workload 静态地基，不外推到 ShareGPT |
| ShareGPT 受控异质组 | 验证长短 work 异质下的容量与语义边界 | 同上 | correctness 护栏通过；后续 C32–C256 扫描冻结 bounded C128；旧 project/C32-direct=1.5457 不排名 | C128 达 C256 已测峰值 98.22%；正式原生矩阵用 C128 |

每臂至少汇总：correct rows/s、database-E2E wall time、service/operator tokens/s、request P50/P95/P99、GPU util time-series、MFU、显存、功耗/能耗、J/1k token、running/waiting、KV usage、prefix hit、各 pipeline 阶段时间、质量、failure 和成本假设。图中只放支持主结论的 3–5 个指标，其余进入结果报告表。

### 4.2 文本原生系统同环境对照

| 轨道 | arms | 作用 | 最小合同 |
|---|---|---|---|
| Chat原生框架轨（已完成） | bounded Chat control、Daft `prompt()` Native/Ray、Ray Data HTTP Processor | 测量现有框架在异质work下的JCT、service throughput、feeding、资源与可观测性边界 | 同一ShareGPT controlled-skew manifest；各臂独立冻结运行点；1+3交错formal；不与Project不同T0/arrival合同作绝对排名 |
| Project eager诊断（已完成） | project frozen-static all-at-t0及full/half/static/shared多Job匹配控制 | 排除71.24s来源并分离quota与competition | 只作Project内因果和与Daft Native对齐T3诊断；不升级为完整框架容量排名 |
| DuckDB有界输出产品轨（已完成） | DuckDB AI vs同manifest direct/project | 检验数据库AI产品入口的database-E2E、质量、错误和可观测性 | SQuAD/cap=64可作静态地基；ShareGPT fixed-cap语义不兼容，只作产品护栏 |

两轨分开是语义门禁，不是为了选择性报告。同环境原生框架对照用来发现问题，不预设项目一定赢；只有计时、语义和 scheduler-owner 边界一致的指标才同表排名。

### 4.3 多 Job 原生观察与项目机制对照

| 层次 | arms | 回答的问题 | 边界 |
|---|---|---|---|
| 原生系统观察 | Daft Native、Daft Ray、Ray Data 各自启动 short/long 两个错峰 job | 独立数据作业共享同一模型服务时，是否出现全局压力叠加、干扰、资源超卖或可观测性缺口 | 框架自己拥有 batching/backpressure；Job 启动后完整 manifest 可用，不重放项目逐请求 arrival；不注入项目 credit/router；不把 barrier 冒充 request P99 |
| 项目因果 A/B | `project_static_partition` vs `project_shared_work` | 感知 job 活跃/完成状态并借用空闲 work credit，能否在相同 endpoint 总 K/work 下改善 JCT/tail/fairness | 只改共享与 idle-borrowing 策略；1+3 后无论正负均停止，不扫 offset/weight 追正 |

2026-08-09 已完成统一5s offset：原生三轨 short JCT 相对各自single增加
82.42%/104.84%/32.76%，均有实际overlap。项目在线replay下quota-only≈0，shared提高
总吞吐但short/Jain回退；统一eager后quota-only已使short JCT+59.00%，matched
static+long又+58.77%，matched shared+long+28.90%。eager shared相对static使short JCT
−48.94%、总吞吐+31.85%、long JCT−25.75%、Jain 0.894→0.972。因此开题结论是
“多Job管理必须感知arrival regime、支持idle borrowing并显式约束SLO/fairness”，不是
“shared/dynamic全面优于static”。

在线5s矩阵只统一Job级启动；新增Project eager矩阵又把DB arrival span压到66.76µs，
但Daft仍缺准备前T0。因此不作跨轨绝对JCT比较；只展示项目matched-cap因果A/B和各原生轨
内部single→overlap变化。

主指标是 per-job/group JCT、goodput、Jain fairness、isolation、global running/waiting/KV/GPU/MFU 时序。`borrowed_work_seconds` 只在项目 A/B 中由请求/credit trace 计算；原生框架无 job-level active-work 标注时必须明确标记不可观测。

### 4.4 设计—实现—证据边界

| 等权部件 | 动机证据 | 当前实现 | 开题可声称 | 尚不能声称 |
|---|---|---|---|---|
| Work Unit | 同行数 token work 14.3×；图像 prepare/model 阶段失衡 | staged descriptor 类型、neutral work consumer 和图像携带接口已存在；正式 runner 尚未构造 production descriptor | 字段设计由现象导出，接口可执行 | staged organization 已端到端胜出 |
| 状态感知 | 同 W 下 high/arrival-limited 状态不同；原生路径出现 underfeed/overqueue | endpoint/resource trace 已正式采集；fresh stage snapshot 与 fallback controller 仅通过单测、未接正式 runner | 必须联合观测 work rate、queue、KV/MFU/tail，并校验 freshness/signature | runtime snapshot 已带来性能收益 |
| 动态调度 | 5s 两 job 显示真实前台干扰和效率—隔离—公平权衡 | completion release、least-work、shared DRR credit 已进入调度器并完成 A/B；stage-aware controller 未接正式主实验 | bounded shared work 是可执行机制，目标必须同时包含 efficiency/SLO/fairness | shared 或动态全面优于静态 |
| 算子代价估计 | 候选选错代价 12.0%–86.5%；简单 estimator 决策失败 | CE1–CE5 离线分析器与 context-LOO 已完成；尚未在线驱动调度 | 文本配置选择有 marginal feasibility | 已预测跨模态 remaining work/SLO 并改善在线决策 |

工程下一步按 descriptor builder → observe-only snapshot → no-op/fallback gate → 单动作消融推进；
不把四个部件同时接入后再做无法归因的总对比。

### 4.5 可直接复用的正式/初步证据

| 证据组 | 目的 | 当前结论 | 还需动作 |
|---|---|---|---|
| token-work 异质性 | 证明 fixed rows 不是成本代理 | 固定 16 行 batch token 最大/最小 14.3× | 核对 CSV 溯源并保留直接标注 |
| active-work frontier 与状态差异 | 证明低供给、最小近饱和点、边际收益递减及状态变化 | 65K/endpoint 约达已测峰值 97.8%；继续加压主要抬高 P99 | 图中分开画容量结果与运行状态，不用未定义区间着色 |
| organization regime | 证明组织策略受 serving/KV/locality 状态影响 | 相同双卡硬件下，2 endpoint 低 KV 压力时策略范围约 12%；4 endpoint consolidation 高 KV 压力下分化约 27% 且重排破坏 prefix group | 保留一张机制图；严格 feeding-saturation 边界可见，不等于动态方法收益 |
| image exact-path profile | 证明跨模态存在分阶段瓶颈 | CPU prepare/GPU service 13.8–31.2× | 统一单位与质量合同，暂不做 proposed 胜出 claim |
| cost decision quality | 证明代价估计有资格作为共同使能候选 | pooled regret 1.67%、macro 2.90%、max 14.72%，pairwise 0.808 | 主图只保留决策质量；完整 estimator 对比放附录 |

## 5. 开题后必须补齐的论文实验

### 5.1 强 baseline 与 provenance

- 文本和图像都按相同环境、当前 commit、统一 source 与输出语义比较；调度主实验统一到完整结果 gather，database-E2E 护栏才统一 sink；
- Daft built-in 和 Ray Data native API graph 必须由框架自身拥有调度；
- project typed actor frozen-static 是 proposed 的强静态对照；
- vLLM pooling 只有在模型与任务语义等价时进入图像 baseline；
- 记录 upstream URL/commit、实现来源、scheduler owner 和适配 diff。

### 5.2 数据组织独立实验

1. fixed rows/images；
2. scalar token/frame budget；
3. staged work budget；
4. balance-aware；
5. locality-aware；
6. balance + locality 组合。

先固定调度与资源，只改变 organization。报告 batch-work CV、packing、oversize、stage/endpoint skew、locality preservation、queue age、throughput、tail、quality 和 energy。

### 5.3 同上限 static–dynamic 因果实验

按顺序运行 steady underload、steady near-saturation、overload guardrail、low→high/high→low、burst arrival、short/long 或 easy/hard mix。每个场景在相同最大 K、active-work、buffer bytes、CPU/GPU 和 actor 数下比较 frozen-static、observe-only、admission-only、routing-only 和最小联合候选。

动态策略只有在吞吐、SLO goodput、P99/JCT 或资源效率至少一项改善约 5%，且 correctness、failure、其他关键指标无不可接受退化时才晋级。steady 场景的目标是 no-regression，不要求制造正收益。

### 5.4 多 job

覆盖 1/2/4 job、staggered overlap、3:1 weighted、异构 work mix 和 arrival offset。报告 per-job JCT/P99/goodput、Jain fairness、isolation、work conservation 和 idle borrowing。按请求数公平与按预计 work 公平必须同时出现，说明为什么 WorkDescriptor 会改变结论。

### 5.5 图像完整验证

统一 CLIP 模型/processor/dtype/normalization、PostgreSQL BYTEA source、到 gather 完成的 operator-E2E 边界、CPU/GPU reservation 和冻结 ground truth，比较 bounded direct、Daft built-in、Ray Data native、project frozen-static，最后才加入 project dynamic。workload 至少包含 uniform、decode-cost skew、phase shift、burst 和 two-job mix；报告阶段队列、tensor bytes、correct embeddings/s、JCT、energy、embedding finite/norm/digest。Recall@K/nDCG 与 pgvector exactly-once sink 作为小规模质量/工程闭环单列，不进入调度性能主排名。

### 5.6 算子代价估计作为共同使能的独立门禁

代价估计需要分别回答“预测准不准”和“决策是否因此更好”：

- 文本：input、output、service、remaining work 与 SLO slack；
- 图像：prepare work、model work、tensor/buffer pressure；
- 指标：MAPE/区间覆盖仅作预测质量，ranking、pairwise、configuration regret 和 online decision regret 才是主指标；
- 消融：无估计、简单解析、profile 校准、residual correction、带不确定区间；
- 外部有效性：独立时间段或 held-out workload，必要时第二硬件 calibration signature；
- 若估计器不能稳定排序候选，它只能作为 tracing 字段，不能驱动 organization 或 scheduler。

## 6. 需要绘制的图与数据合同

| 图 | 唯一问题 | 画法 | 数据来源 | 完成条件 |
|---|---|---|---|---|
| A 动机：work 与状态 | 为什么 rows 和固定上限不足 | 左：固定行数的 work 范围；右：低供给—最小近饱和点—边际收益递减及 high/arrival-limited 状态 | 正式 CSV 聚合 | 每个点/线直接标义；不出现无数据定义的区间色带 |
| B 研究边界与主线 | 两项研究和共同使能如何连接 | 数据流 + 反馈流；cost estimator 同时连 organizer 与 scheduler | 方法合同 | 不把 cost 画成第三项研究内容 |
| C organization regime | 组织收益为何依赖 serving regime | 低压力/高压力 small multiples 或 dumbbell；附 locality 机制注释 | cache-on 正式结果 | 一张图只讲 regime dependence |
| D 图像 stage-aware | 为什么跨模态需要 staged work 和阶段状态 | CPU prepare/GPU service 比 + transfer 形态 + active-window screening | image exact-path profile 与 screening | 不把 microprofile/screening 当系统排名 |
| I 图像 baseline | 五条图像路径在哪些合同下可比较 | 左：角色/门禁矩阵；中：12K 结构诊断；右：120K matched-resource 正式排名 | image operator formal + vLLM capability gate | 只有 120K Ray Data/Project panel 可排名 |
| E cost decision quality | 代价估计是否能帮助选择 | median/macro/max regret、pairwise 与门槛；不堆所有预测散点 | cost-profile formal | 明确共同使能和 conditional 结论 |
| F 原生单 Job 状态指纹 | 现有原生 graph 如何落入不同服务压力区 | 左：JCT/tok/s；右：running、waiting、KV、MFU 原单位 small multiples；标 underfeed/minimum-saturation/overqueue | `opening_text_native_single_job_formal_20260808` 12 formal | 只解释外部现象；database-E2E 三臂降为 appendix correctness/语义表 |
| G static–dynamic | 状态变化下动态是否超过同上限静态 | 当前只保留 workload phase 与同上限 A/B 实验合同 | 无结果不画图；论文正式运行后再决定图型 | 最大 K/work/resources 完全匹配 |
| H multi-job | shared credit 如何改变效率、前台/long隔离与公平 | 两Job给最小因果；四Job画full→quarter→static→shared、long spread、Jain/MFU | 开题两/四作业 formal；论文阶段扩展 weighted/held-out | 四Job只覆盖一个offset/equal-weight workload；shared改善效率但Jain/long稳定性回退 |

2026-08-10 已完成 A/T/N/C/H/D/I/E 八张正文数据图与 F 状态备份图。G 无结果且不画，
database-E2E 只保留附录表。N 使用三条原生轨各自的 four-job/isolated-single 归一化
影响，H 使用 `Short@0s → 3 Long@5s` 的 Project quota/competition/shared 与效率—公平
权衡；两Job arrival-regime 放附录。所有误差线表示三次 formal 的离散，warm-up 不进入
统计，三次原始点在关键图中直接显示。

## 7. 停止规则

- replacement 三臂、文本 Chat 原生单 job、两 job 因果点和四 job 扩展均已完成；开题前不再换模型、数据库、workload、offset、weight、Job 数或扩大并发扫描追正结果；
- DuckDB 仅保留在语义成立的有界输出产品轨；Daft/Ray Data 多 job 只做原生系统观察，不给它们注入项目调度器；
- K256 已覆盖当前每 endpoint 校准上界；K512/endpoint 只用于独立过载退化研究；
- 动态未超过同上限强静态点时记录失效边界，不换弱 baseline 或挑 workload；
- 代价估计现有 429-run 仅声称文本配置选择初步可行；图像 held-out 仅在无法用已有 profile 数据构造决策对照时才新跑，不扩为 TPC-H 或复杂模型搜索；
- 图像 official baseline 未满足同语义和 scheduler-owner 合同时不进入主排名；
- 未通过 feeding、correctness、quality 或稳定性门禁的数据不能进入结论图；
- 当前只冻结本大纲、实验数据和图，不制作或同步新的 PPT 成品，也不同步 Wiki。
