# 研究内容一：动态数据组织与批处理构造策略实验计划

本文只保留**当前未解决的问题、下一步判断、依赖和完成条件**，以及现行研究框架。
已经执行过的输入、配置、请求额度、停止条件和预定方法随对应测试记录保存（见下表），
不再在本计划中复制；“用户今天要求继续”“本轮不合并”“随后推送”等完成后的操作话术不再作为
计划内容。旧计划中的未用额度不构成今天继续运行的授权。

## 当前问题与下一步

**当前问题**：已有检查确认了控制机制生效，但还没有形成支持方法收益的比较结果。
在单查询、不可变全扫描、固定模型与输出目标下，维持接近已测最高有效吞吐需要多少供给；
在该区域增加在途工作，是否仍有完成收益，还是主要增加排队与留存。

**已知情况**：较紧工作量限制曾降低 HTTP 尾延迟，同时增加完整查询时间（见下方记录）；
容量修复后的完整复查仍未得到平台候选——PG 与 direct 均持续供给不足；原偶发故障根因待确定。
M2 的五种 PG 源信息方式完成固定 C4 诊断，尚无稳定收益。

**下一步需要解决的判断**：当前结果主要受供给、执行开销还是控制策略影响；什么条件下可以开展
后续比较。先把 PG 路径与 direct 的供给差距分解到具体阶段（取数/构造/序列化/传输/调度/HTTP），
再决定容量筛查与调参是否可执行。

**完成条件**：能够用一致的计时、输入与资源记录回答上述判断；不以“跑完更多配置”代替问题解决。
具体执行清单另见对应验证记录中的预定方法，不在本计划中复制；每轮新实验仍按根规则先给
具体输入、预算与授权。

## 已移出的执行前声明与实施规格（原文随记录保存）

| 记录 | 运行前声明/实施规格 |
|---|---|
| [M1 完整容量复查](../../results/postgresql/m1_full_recheck_20260920/README.md) | [plan.md](../../results/postgresql/m1_full_recheck_20260920/plan.md) |
| [C64 异常诊断](../../results/postgresql/m1_c64_errors_20260920/README.md) | [plan.md](../../results/postgresql/m1_c64_errors_20260920/plan.md) |
| [M1 供给复查](../../results/postgresql/m1_supply_followup_20260920/README.md) | [plan.md](../../results/postgresql/m1_supply_followup_20260920/plan.md) |
| [M1/M2/F 有限真实验证](../../results/postgresql/m1_m2_f_real_20260920/README.md) | [plan.md](../../results/postgresql/m1_m2_f_real_20260920/plan.md) |
| [等待位置小实验](../../results/postgresql/waiting_positions_pilot_20260914/README.md) | [plan.md](../../results/postgresql/waiting_positions_pilot_20260914/plan.md) |
| [常驻服务对照](../../results/postgresql/waiting_positions_persistent_20260914/README.md) | [plan.md](../../results/postgresql/waiting_positions_persistent_20260914/plan.md) |
| [真实输入准备](../../results/postgresql/waiting_positions_real_preparation_20260914/README.md) | [plan.md](../../results/postgresql/waiting_positions_real_preparation_20260914/plan.md) |
| [查询共享 E](../../results/postgresql/query_sharing_e_20260914/README.md) | [plan.md](../../results/postgresql/query_sharing_e_20260914/plan.md) |
| [异步期限与输入/交付诊断](../../results/postgresql/async_deadline_flow_20260914/README.md) | [plan.md](../../results/postgresql/async_deadline_flow_20260914/plan.md) |
| [容量等待与零任务诊断](../../results/scheduling/capacity_wait_empty_map_20260912/README.md) | [plan.md](../../results/scheduling/capacity_wait_empty_map_20260912/plan.md) |
| [C/D 审计与真实预算](../../results/postgresql/cd_validation_20260911/README.md) | [plan.md](../../results/postgresql/cd_validation_20260911/plan.md) |
| [A/B 验证补充](../../results/postgresql/ab_validation_20260911/README.md) | [plan.md](../../results/postgresql/ab_validation_20260911/plan.md) |
| [执行修复工作包 A](../../results/postgresql/execution_repairs_20260910/README.md) | [plan.md](../../results/postgresql/execution_repairs_20260910/plan.md) |
| [图像工作包 F](../../results/postgresql/image_stages_f_20260920/README.md) | [plan.md](../../results/postgresql/image_stages_f_20260920/plan.md) |
| [数据库源与公共查询 B](../../results/postgresql/database_queries_20260910/README.md) | [plan.md](../../results/postgresql/database_queries_20260910/plan.md) |
| [C/D 留存预算与组织规格](../../results/postgresql/pg_window_budget_20260910/README.md) | [plan.md](../../results/postgresql/pg_window_budget_20260910/plan.md)（D 组织部分同适用） |
| [单卡容量画像](../../results/postgresql/map_capacity_20260910/README.md) | [plan.md](../../results/postgresql/map_capacity_20260910/plan.md)（含历史 PG 单 Map 切片全部章节） |

<a id="m1-throughput-platform"></a>
## 当前 M1：有效吞吐平台附近的在途工作与资源代价（2026-09-20）

本实验只回答两个问题：在单查询、不可变全扫描、固定模型与输出目标下，维持接近已测最高有效吞吐需要多少供给；
在该区域增加在途工作，是否仍有完成收益，还是主要增加排队与留存。目标不是最小提交量或服务队列恒为零：
需要保留覆盖取数、构造、通信及补充波动的供给余量。不得先减少有用计算供给，再用 HTTP P99 下降宣称查询优化。

**旧 M1 已结束其测量任务**：[常驻对照](../../results/postgresql/waiting_positions_persistent_20260914/README.md)
中，W512 对 work 最小值 187 的任务最多容纳两项，不能充分使用四个受控服务槽。
HTTP P99 下降 42.23%，完整查询时间却从 0.395086 增至 1.315864 秒。这是指标反例，
不证明 token 控制优于调优请求数，也不证明有限候选优于全局轻量信息。不继续重复这一欠供给比较。
原始输入、代码快照、时间线及全部失败保留。

### 先检查可达性，再消耗模型额度

`m1_campaign.py` 改为显式 v2 配置，每次只执行一个阶段。`--preflight` 只读本地身份与预算，
不连接 PG、不加载模型或 tokenizer、不初始化请求账本。各扫描点共用同一份存储设置、CPU/服务配置和输入身份。
逐点输出请求上限、Core 留存项数、结果预留可支持项数、输入字节可支持项数、PG 行窗口/留存/暂存与客户端配置。
服务 `max-num-seqs` 是内部执行配置，不直接等同于客户端在途数；真实 running/waiting 需另测。

源码每项 Map 最大响应预留为 1 MiB，因此旧 Core 结果 32 MiB 使实际接纳数不超过 32，C64 扫描点不可达。
32 MiB 是上游配置，不是 GPU 能力。若计划留存 128 项，仅 Core 结果预留就需至少 128 MiB；输入、PG 等还须逐项通过。
新的静态估量采用 manifest 字节上限、最坏 JSON 转义、现有 PG 行分配公式及保守元数据余量；
它只证明声明资源允许目标供给，运行中仍要核对实际计划窗口、字节使用和持续在途曲线，不能用瞬时峰值代替。
资源不够时返回具体原因，不在线缩减响应预留、切换 CPU、降低模型 context 或改输入来凑出有效点。
若研究固定存储下的限制，另立身份，不混入服务能力扫描。

### 分阶段选点与独立评价

1. **容量筛查 `screening`**：只用调参输入；匹配请求数 FIFO（按输入先后提交），两条路径使用相同的、至少三个可达 C。
   PG 复用 `PersistentMapGateway`；匹配 direct 复用 `pg-source-direct` 的有界客户端，模型消息与生成设置逐项核对。
   该 direct 仍含 PG 源读取和每查询客户端准备，属于匹配执行参照；两条路径时间不能相减解释为纯 PG 开销。
   若两者都停止增长，只能先报告各自路径的平台候选；要把它解释成服务能力，须另以已有 `DirectMap` 的准备输入路径
   排除共同取数/客户端供给限制，并核对服务观测，不新建调度器或把参照冒充原生系统。
2. **工作量调参 `strategy-tuning`**：仅在供给有解释力的区域，固定 FIFO 顺序，以已调优请求数为参照，
   在相同有限较高 C 下检查少数预先列出的 W。C 作为保护上限，W 由调参结果决定，不预置“紧 W4096 必须有效”。
   各策略获得相同资源与调优机会；记录尝试过的全部点与额度。W 未触发、吞吐下降或与简单控制相当均保留，
   不调整 context 迫使 W 起作用，也不换评价数据追求优势。
3. **独立评价 `evaluation`**：先保存调参决定及其 SHA，记录选定组、服务签名、输入身份、资源和判定规则；
   评价配置须逐项匹配该决定，使用独立评价输入。主要组见下表；不在看到评价结果后重选 C/W。
4. **观测开销 `observation`**：仅选少量代表组做完整/摘要记录配对；不将日志模式扩成所有 C×W 的矩阵。
   每次显式列出全部轮次顺序；第零轮为预热，其余是同服务生命周期内的配对重复，不冒充独立服务启动样本。

| 比较组 | 选取依据 | 能回答什么 |
|---|---|---|
| 有限较高并发 request FIFO | 已测平台附近或更高、资源允许的 C；不使用故意 OOM 的无限提交 | 更多提前提交的完成收益及资源代价 |
| 调优 request FIFO | 调参中维持近似吞吐的较小 C，独立评价复核 | 简单请求数控制能做到什么 |
| 调优 token FIFO | C 为相同的较高保护上限，W 独立调参；实际有 work-only 阻塞 | 工作量信息相对调优请求数的增量 |
| 少量 token 表征消融 | 与较高 request FIFO 同 C，W 对所有该 C 项集合均不生效 | 分词、描述与记录成本；也给同 C、只改 W 的归因提供参照 |

完整策略比较允许 C/W 数值不同；归因 W 时保持同 C、同顺序、同观测和其余设置。

### 平台与不确定性的判定

主量为 `X(C)=完整且合法完成的任务数 / 调用入口至客户端 EOF 的完整查询时间`。
合法但语义判断错误的输出保留在吞吐计数和质量分母中；非法、缺失、重复输出以及关联/资源/请求计数错误停止该阶段，
失败证据继续保存，不能删掉失败重复再选点。实际输入/输出 token、输出变化与标签质量同时报告。

只在声明的可达集合内计算 `Xmax`。`epsilon` 在运行前确定，候选较小 C 满足 `X(C) >= (1-epsilon) Xmax`。
3% 可以作为待审阅选择，程序不再内置 97% 规则；同时必须声明最少重复数、可接受重复范围以及中段供给判定参数。
`select_capacity()` 要求完整配对记录、实际 HTTP 持续供给与重复原值，并分别返回：

- `platform_candidate`：已测集合内的平台候选，仍须独立评价；不宣布饱和或统计等效。
- `no_platform_observed`：最高两个档位仍有超过声明差异的持续改善。
- `inconclusive_supply`：至少一个点缺少持续供给；优先检查实际读取、通信、留存和客户端。
- `inconclusive_repeat_variation`：重复波动或配对差异跨过判定值，当前不明确。
- `lower_supply_range_unresolved`：最小档位已是观测最好点，尚未辨明更低供给范围。

程序使用配对吞吐比的全部原值和观测最小/最大范围进行保守筛查，该范围不是置信区间。
至少三次测量只是输入完整性要求，不保证充分重复；最初小规模筛查若波动太大，先报告不明确，
再单独设计新的重复数与额度，不自动追加。评价同样报告配对差异，不能挑最快重复。
查询全程的启动和排空保留在主指标；固定中段仅用于解释是否持续供给，不删除不利时段制造平台。

### 指标、数据与解释规则

主表保留完整 JCT（调用至消费完毕的时间）、有效完成吞吐、合法性、任务质量、输出差异、失败和实际调用量。
机制表按数据所在位置分别记录：尚未物化、已准备未提交、HTTP 在途/服务等待、模型执行、已完成未消费。
不能把尚未物化任务数叫内存字节，也不能因服务排队减少就声称整个系统少留存了数据。

现有记录器继续提供逐行等待、PG 留存峰值、Core 逻辑占用、实际 HTTP 曲线及进程 RSS/CPU 采样。
新增离线汇总给出每个逻辑域的占用时间积分、每个已采样进程的 RSS 字节秒与 CPU 时间增量，
从观测区间计算；缺少区间不填零，Core 结果预留不当成实际响应大小，PG 完整字节时间线缺失则写 unavailable。
所有常驻 gateway 与 driver 在两条路径运行期间统一采样，活跃 PG reader 另由各查询采样。
逐进程峰值不能相加称同时峰值；RSS 可能重复计共享页，逻辑预算下降不等于物理内存同比下降。

vLLM 0.25.1 的 [Production Metrics](https://docs.vllm.ai/en/v0.25.1/usage/metrics/)
列出 running/waiting、排队、KV 与抢占等字段；未来执行前按部署实例实际输出核对，不把文档字段当本轮已测值。
当前编排会明确记录服务指标尚未附加；未来由已有服务采样器提供独立文件。GPU 利用率与 MFU（模型算力利用率）仅用于解释，
MFU 缺少可信运算量及对应精度峰值时不报告。[Optimization and Tuning](https://docs.vllm.ai/en/v0.25.1/configuration/optimization/)
讨论了抢占/重计算的代价，但本项目旧 M1 没有测出该机制，不能借文献补写因果。

| 观察 | 解释 |
|---|---|
| 吞吐相近，实际留存或服务排队减少 | 存在减少多余提前工作的空间；先排除队列转移 |
| HTTP 尾延迟下降，完整 JCT 变差，上游等待增加 | 等待移动或供给不足；明确吞吐代价 |
| 吞吐改善且测到额外工作减少 | 有执行收益信号，机制仍须对应证据 |
| token 与调优请求数相当或更差 | 当前负载保留简单请求数参照，不强求复杂控制 |
| 合理点持续增长或供给始终不足 | 尚不能评价服务平台附近的限制 |

已准备的短评论继续用于基础容量、表征成本和近同质控制，不单独支持异质 work 优势。
若请求工作量相同为 w，则 W 等价于请求数上限 `floor(W/w)`；当前 work 174–249、均值约 199.833 的评价输入
尤其需要面对调优请求数参照。后续只有明确的工作量失配假设才增加一种真实异质性；
不通过调大最大输出冒充长输出，应报告真实输出长度。

全局轻量信息、有限物化和候选范围仍归 M2；多查询服务分配与长查询代价归 M4，可按其自身共享资源和输入资格独立推进，
不要求所有单查询先达到所谓完美极限。E 已有共享服务，token 组织仍有单 Job 接入要求，两者不混称。
M1 不新增长度排序、全局信息策略、公平算法或 vLLM 调度修改。

**当前交付**：研究设计、v2 可达性预检、分阶段运行表、选点及资源/输出汇总和无外部服务测试。
新机器和真实 PG/模型验证尚未执行，已准备数据保留；旧 44,544 次/30 分钟组合退出当前方案。
新的调用量由每阶段实际输入行数×组数×显式轮次（含预热）计算；先审阅少量筛查点，再确定后续点、重复数、
机器/模型/服务签名、具体请求与时间额度以及停止/清理条件。阶段不自动接续、不重试、不退款或复用旧额度。
[代码与验证记录](../../results/postgresql/m1_platform_revision_20260920/README.md)。

<a id="design-hypotheses"></a>
## 当前研究：设计假设、证据与强对照（2026-09-14）

受众：内部研究计划。本节接管**下一阶段研究顺序**；下方A—F继续表示工程工作包和原始验证合同，
不表示六项贡献，也不要求先完成E/F全部功能才能研究。研究对象仍为
**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**，两项研究内容不变。

当前问题是：提前准备与提交增加并行和局部性机会，也增加数据留存、已提交不可重新分配的工作及查询间干扰。
需要检验何时这些机会抵得过代价，再决定读取、组织和推进方式。有限窗口、token配额与长度排序都是可比较的控制量，
不预设它们必然优于强静态或全局信息。理论与论文迁移条件见本节及[知识库条件卡](../../docs/research/knowledge_hub.md#execution-transfer-cards)。

**执行状态**：`3f1f5540` 已包含本轮期限修订及完整验证记录。96次受控请求与57次真实生成请求均已登记，
其中1条真实结果用于预期超时检查，不计成功查询。M1已完成顶部两轮受控观测/机制小实验，其余设计假设尚未运行；本文不新增模型额度。
E已按执行/资源范围完成，M1 旧测量反例已完成；真实输入划分已有记录，当前改为吞吐平台与资源代价研究，独立模型校准尚未执行。
旧图像、SAOR、原生框架结果保留外部执行身份，不追认为当前PG语义节点结果。

### 1. 设计选择与可证伪的证据链

| 设计选择 / 研究问题 | 已有证据、版本与含义 | 缺少的合理对照 | 什么结果会否定当前偏好 | 最近邻与区别待证 |
|---|---|---|---|---|
| 行数与字节留存分开 | [C原验证](../../results/postgresql/pg_window_budget_20260910/README.md)证明平均份额会拒绝某些合法宽行，total按实际占用缩小留存后完成；[C同配置](../../results/postgresql/cd_validation_20260911/README.md) equal/total为3.751273/3.810127s，仅一次短查询 | 相同合法输入、行上限、总字节及转换/接收暂存；计入准备、释放和驻留面积 | 在目标输入上平均份额已足够，total没有额外可用性或成本优势时，仅保留为资源能力；不能强求加速 | Ray Data已有内存感知执行；需说明PG Datum/结果序/消费释放提供了什么额外信息 |
| 请求数与token work控制分开 | [D真实预算诊断](../../results/postgresql/cd_validation_20260911/README.md)：C32下W4096峰值HTTP20–21，W16384为32；rows/work/length紧预算JCT8.721490/5.744630/4.133930s，宽松4.327613/3.856271/3.213935s | 独立调优的请求数FIFO；token表征但不限制的FIFO；同排序且生效的work控制 | 若只降低提交后延迟，却增加上游等待且JCT不改善，token控制不作为性能方法 | VTC的服务量公平与黑盒端点的提交限制不是同一动作；work描述不等于真实服务时长 |
| 有限候选范围 | 已有组织代码和有界输入，并没有“有限候选优于全局信息”的直接实验 | 流式FIFO、同预算局部组织、全局轻量元数据规划后有界执行；必要时加入合理落盘方案 | 全局扫描/统计便宜且其净收益更大时，采用全局或混合方案 | *Optimizing LLM Queries in Relational Data Analytics Workloads* 是必须面对的全局关系信息参照 |
| 准备深度与交付次序 | [新时序诊断](../../results/postgresql/async_deadline_flow_20260914/README.md)，源码0274b121：fixture就绪行可交付后等待82.093/82.692ms，ready-first为0.038/0.026ms；但客户端首行未改善；四次真实对照均未遇到就绪队首 | 同观测、同存储/计算预算下的准备深度与交付次序；分开节点和客户端终点 | 若只移动等待位置或延迟下一轮供给，不认定更早节点交付是用户收益 | IMLane/Ray Data已有异步流水；需测数据库消费进度能否带来增量，不能据测试分支名称判断 |
| 查询级机会分配 | QueryRegistry/Engine已有归属和共享能力；[历史容量实验](../../results/scheduling/saor_capacity_development_20260811/README.md)中更高吞吐伴随某Job更差P99，属于旧外部执行线索 | 同到达、同信息、同共享容量，调优FIFO与已有公平控制；两条matched solo分别定义slowdown | 共享强静态已足够或不可抢占工作不是后到查询的主要代价时，不新增复杂控制器 | Agentix关注程序JCT，VTC/DLPM关注服务机会；需说明SQL消费/已提交工作提供的新决策价值 |

历史图像[active-batch筛查](../../results/motivation/gpu/image_host_path_screening_20260802/README.md#43-active-window-有最小饱和点继续排队会伤害延迟)
提供供给深度的动机线索：活跃批次数8/16/32/64，查询阶段吞吐786/995/1015/945 images/s，
批次完成时间中位数0.47/0.79/1.35/1.81s。这里是P50而非P99，是 `c1484f2`、PG18.4、双4090、Daft/Ray/CLIP的单次筛查。
本次核对历史报告，不重算其CSV、不据此为当前文本选并发；16→32收益递减和64回落说明“准备越多越好”需要检验。
历史source-thread收益很小，不能预设数据库读取或PCIe必然是瓶颈。

同样，C/D已有一次模型输出差异，新8行样本反复使用且有2个情感误判；这些都属于开发/功能证据。
不把反复使用的8/16/128行重新称为独立质量评价集，不将两次短重复或某次正差异升级为方法贡献。

### 2. 最小模型：信息、责任与用户目标

对查询j保留逻辑输入、依赖和实际SQL语义；对合法工作i保留行出现ID、所属查询/阶段和完整请求身份。
模型允许轻量全局元数据与有界payload同时存在；不能因当前API尚不暴露某信息，就在对照中禁止获取它。

| 量 / 状态 | 定义与现有观测 | 不能混用的事实 |
|---|---|---|
| 查询释放r_j、终点F_j | 主目标JCT_j=F_j-r_j。先声明终点为客户端观测EOF，另报结果持久化时间；若比较写回，则F_j改为已核验写回提交，两种合同不混排 | 现有q0计时从SQL/direct release开始，**不含**此前gateway/tokenizer启动；新实验须另报查询专属准备和冷启动，不能重解释旧JCT |
| 工作资格a_i | 与被比较策略独立的合法可执行时刻：固定全扫描可用共同release/离线输入条件定义，依赖场景由上游语义结果决定 | child被拉取、token化结束或Core接纳会受策略影响，不能直接当共同a_i；不能观察时标unknown，仅报告release至完成及可测子段 |
| 提交s_i、完成观察f_i^obs | 分别用Core实际提交和权威终态被Core处理的时刻；direct用自身出站/完成事件，路径定义分别登记 | Core终态是外部观察，不是真实GPU kernel结束；HTTP开始/结束不是Core计算责任释放，内部queue/model耗时未知时不拆造 |
| PG就绪g_i、节点交付h_i、客户端收到d_i | 诊断构建的result_ready、node_return与实际客户端received；用独立before-offer/accepted绑定连接行ID | 不将d_i-f_i^obs直接叫PG节点留存；同机时钟已核验后才能相减，多机时钟须另处理 |
| 输入观察范围、候选集、存储与计算责任 | 分开已知元数据集合、已物化payload、合法候选、已提交未结算、已完成未消费结果 | L是行数，B_r是特定内存域字节，C是请求数，W是校准单位work；相同数字不代表相同资源 |

在这些时点均可比较时，逐项保留恒等式：

$$
d_i-a_i=(s_i-a_i)+(f_i^{obs}-s_i)+(g_i-f_i^{obs})+(h_i-g_i)+(d_i-h_i).
$$

资格到提交和完成到消费的等待不能遗漏。若只能观察child返回后的时间，须明确这是部分区间；
不能靠把资格时间推迟到接纳时来降低“端到端”延迟。
全扫描主要比较完整JCT、资源和质量；多查询增加逐查询JCT/slowdown与最长无服务时间；
LIMIT另比较需求满足时间及多做的工作，并先满足原有严格按需语义，不用全扫描重排权限替代LIMIT资格。

查询专属全局扫描、metadata排序、额外随机读取、token化和组织均进入新比较的外层计时；
可复用metadata必须另报建造/刷新/储存成本和按什么复用次数摊销，不将预先算出的评价集信息免费提供给某一臂。

### 3. 可以证明什么，以及目前不能证明什么

以下是明确模型下的数学推导和证明任务，**不是新增算法贡献，也不是完整实现已被形式化验证**。

**资源安全。** 对内存域r，令L_r(t)为仍由该域负责的数据，m_ir(t)为它实际计费的表示或足以覆盖分配的预留，
M_fixed为固定开销。要求：

$$
M_r(t)=M_{r,fixed}(t)+\sum_{i\in L_r(t)}m_{ir}(t)\le B_r.
$$

若初始状态满足预算，每次转换都在分配前检查“旧占用+新表示+必要暂存”的峰值，责任交接不重复漏计，
只在相应责任真正结束后扣账，则对转换次数归纳可得不变量。C的证明对象仅为实际计费的行上下文/元数据及各自有界暂存；
要将结论连到代码，还需列出全部接纳、转换、结果预留、交付、异常与未知终态转换并检查。它不约束整个PG/gateway/Ray RSS，
也不证明吞吐或最终一定完成。现有测试与事件重放是该证明任务的支持证据，不替代穷尽转换的证明。

**积压面积。** 对包含所有a_i和d_i的完整有限观测区间，N(t)=Σ_i 1{a_i≤t<d_i}，有

$$
\sum_i(d_i-a_i)=\int N(t)\,dt,\qquad
\sum_j\omega_j(F_j-r_j)=\int\sum_j\omega_j1\{r_j\le t<F_j\}\,dt.
$$

证明是对每个指示函数积分，再有限求和；第二式要求权重预先固定。内存驻留成本另记为∫M_r(t)dt，单位B·s。
观测不完整时只能积分截断区间，并报告未完成/取消项，不能把其当作完整sojourn或只剩成功样本。
离散RSS采样面积只是采样近似；完整逻辑状态转移的阶梯积分可以精确重算。
这些有限样本恒等式不需要假设M/M/1；[Little的平均关系](https://pubsonline.informs.org/doi/10.1287/opre.9.3.383)
本身不能推出P99、最优窗口或当前有限工作集的系统稳定性。

**按序消费的反例。** 单个单位速率串行执行器、全部任务在0时刻合法、确定耗时p_i>0、无局部性/切换收益，
结果按输入序即时交付，且没有额外传输、消费阻塞或并发效果。任意顺序完成输入前缀i至少处理Σ_{k≤i}p_k的工作，故

$$
d_i\ge\sum_{k=1}^{i}p_k.
$$

无空闲FIFO对每个i达到该下界。输入耗时[10,1]时，FIFO的有序交付为[10,11]，短项优先为[11,11]。
因此不能用短任务优先的无序完成优势证明本项目按序结果一定更快。方法必须指出利用了哪条不成立的假设，
例如并行资源互补、真实prefix复用或阶段重叠，并用对照单独识别它。

**有限动作的决策误差。** 给定同一状态s、同一有限时域/终端代价和有限合法动作集A(s)，
令J为真实代价、Jhat为预测，假定对所有候选同时成立
|Jhat(a|s)-J(a|s)|≤ε，选择误差至多δ，即Jhat(ahat|s)≤min_a Jhat(a|s)+δ。令a*最小化真实代价，则

$$
J(\hat a\mid s)\le\hat J(\hat a\mid s)+\varepsilon
\le\hat J(a^*\mid s)+\delta+\varepsilon
\le J(a^*\mid s)+2\varepsilon+\delta.
$$

若采用逐动作界ε_a，只有Jhat(a)+ε_a<Jhat(a_0)-ε_0才可在这些界成立时判断候选优于强静态a_0；
实际下发动作还须满足对应不等式，不能忽略近似求解误差。RMSE、单个动作的覆盖率或被选动作上的误差均不自动满足同时误差界。
若使用统计界，须声明覆盖概率、候选比较/自适应选择的处理；有限时域结论不推出整查询全局最优，未获保证时保留静态动作。

**进展与公平仍是独立证明任务。** 有界队列可把积压转移到入口；平均服务差也不能推出查询P99有界。
证明候选不会永久饿死某项，至少需要有限或受约束到达、每项可容纳、消费最终继续、资源释放被观察、后端最终给出权威终态、
选择器具有公平机会等前提；未知终态可能合法保持计费，此时应有限地失败/隔离而不是编造完成。
尚未证明当前端到端执行在任意黑盒服务下均可推进。Neely有限缓冲研究中的丢包条件与SQL不能丢弃必要行冲突，
有限重排缓冲的颜色切换成本也不等于KV生命周期；见[理论迁移条件](../../docs/research/knowledge_hub.md#execution-transfer-cards)。

### 4. 四项设计假设实验（M1 已调整研究问题）

M1—M4只是内部实验标识。正式报告用下列中文问题名；每项都保留负结果，不能通过反复改输入或阈值强求支持。

| 实验 | 对照与固定条件 | 必需观察与主要判定 | 最小实现缺口 / 下一步 |
|---|---|---|---|
| M1：有效吞吐平台附近的在途工作与资源代价 | 可达性预检；匹配 direct/PG 容量筛查；有限较高 FIFO、调优请求数 FIFO、调优工作量 FIFO；少量同 C 表征消融 | 完整 JCT、有效吞吐、持续供给、各处留存与质量；无平台、吞吐代价和无 work 增量均保留 | 旧测量反例结束；当前按[分阶段设计](#m1-throughput-platform)，本轮真实清单已执行，结果见本文顶部 |
| M2：局部候选是否比全局信息更划算 | 流式FIFO、同预算有限候选组织、全局ID/work元数据规划+有界payload执行；全局memory不足时合理外排，不能用故意OOM的无限提交作强参照 | 共同信息条件先比较selector，再比较付费获取全局信息的完整方案；计入扫描/统计/token化/排序/额外读取、metadata峰值和持久存储；固定快照与结果语义 | PG源实验层五组真实对照完成，尚无稳定优势；SemMap全局预扫尚未接入。首次构建和元数据复用分列，不免费借用完整评价信息 |
| M3：行数、字节还是阶段工作解释留存 | 文本先复用C/D；固定各域空间与CPU/GPU配额，改变真实输入长度、宽度及表示膨胀；比较行数控制、总字节反压、简单阶段感知控制 | 每阶段表示大小、准备/模型/消费工作及驻留面积；不把token当成解码图像字节。若瓶颈由表示膨胀决定，就让对应字节域控制它 | 只有文本结果需要跨阶段验证时才接F的最小图像reference和阶段链；Ray Data须保留合理原生策略及原生调度所有者 |
| M4：过早提交是否损害后到查询 | 固定两查询到达与资源；长查询先开始、短查询按预定延迟到达；改变已承诺工作量，同信息比较共享FIFO和已有公平控制；matched solo另跑 | 每查询JCT、相对同服务资源/保留份额solo的slowdown、最长无服务时间、不可抢占已提交work及长查询代价；不能只报短查询改善 | E共享服务接入已具备；按自身共享资源、输入和额度资格开展先到/后到对照，不等待所有单查询达到性能极限。既有shared-credit/公平算法先核对适配与默认装配，不重新实现 |

共同实验条件与验收：

- 先区分安全全扫描、依赖产生和提前结束场景；新SQL形状分别满足snapshot/权限、NULL/error/order及取消要求。
  全局预扫不能越过本来尚不允许求值的关系/语义步骤。暂时无法保持相同语义的路线不进入同表排名。
- 调优与评价按原始行出现ID和内容近重复检查划分，所有既用8/16/128行及其他调优样本列为开发数据；
  新评价manifest、划分SHA与模型/硬件签名尚未生成，生成并审计后才确定请求数量。按真实长度/可复用前缀分布分层，不人工选“最能胜出”子集。
- 静态搜索范围在运行前固定、各臂有同等调优机会；同时包含低于、接近、超过已观测平台的合理点。
  不从本轮C4八行或旧C32候选直接继承“最优”。W若小于支持的单项context、或大于该数据的最大并发work却声称生效，预检直接拒绝。
- 每个比较以独立完整查询/查询组作为重复单位，交错或配对运行；先用开发运行估计方差并选出需要区分的效应，
  再固定重复数、warm-up、停止条件与分析。一次运行内数千条请求不是数千个独立系统重复，失败不删除、不自动追加重复。
- 主表报告每次值、配对差与不确定性、完整JCT、质量/结果差异、任务/请求量和各资源域；计时终点不一致则分表。
  token/s只有具备匹配服务counter时报告；实际表示/驻留不能由总量相除猜测。缺少观测写unknown，不能据最终0反推全过程0。
- 完整诊断观测与低扰动运行另做匹配对照；tokenizer启动、常驻RSS、组织/序列化、trace写入、执行及结果持久化分别记录。
  不把观测开销移到计时外后仍称“完整方法成本”。没有完整归因时不开发通用缓存、复杂selector或新调度平台。
- 出站请求、失败和全部预留有独立账本；每个GPU campaign重新给出确切单元表、最大请求数/时间、模型身份和清理条件并取得授权。
  **本节没有新增GPU运行授权；历史57次与E的63次请求均已结束，不沿用其额度。**

### 5. 候选方法与既有实现落点

候选为**消费进度感知的有界组织**：只对合法、可观测、尚未提交的工作，比较继续准备、现有FIFO/长度次序、
促进结果前缀消费及少量准备深度动作。它是等待M1/M2及最近邻审查的候选，不是已实现/已证明的新算法。
在物化预算内保留多少未承诺工作，需要真实查询进展和驻留代价支持；不添加十几个无依据的评分权重。

| 数学对象 / 能力 | 已有落点 | 本轮决定 |
|---|---|---|
| 行身份、合法输入、消费序和取消 | PG child plan、语义节点、独立结果绑定 | 沿用数据库语义；新动作不能扩大合法求值范围 |
| 行/暂存计费与消费释放 | `sem_pump.c` total预算；资源事件 | 沿用C；增加域或证明时逐一列实际责任，不能映射成进程RSS |
| 完整消息work及校准身份 | 模态adapter、`WorkDescriptor`、`MapOrganizationConfig` | 复用D；与真实token usage核对；不把单项max-new上界解释为运行时长 |
| 候选和组内次序 | `WorkWindowOrganizer` / 已有organization回调 | 现有FIFO/有限长度控制作对照；消费进度信息尚未接入该策略，不宣称已支持 |
| 计算容量、提交、终态 | `SessionCapacity`、`SessionEngine` | 不复制账本；后端权威终态前保留未知责任 |
| 查询机会与阶段能力 | 既有Job/flow选择、MethodDriver和可替换backend | E/F按实验需要补最小适配；PG方法桥接、动态份额和图像阶段不因接口存在而算已验证 |
| 全局元数据、反事实与误差 | 实验/离线分析层 | 先构成强对照，未知未来完成时长仅可作明确的离线oracle；不泄露给在线候选 |

每个动作都要对应实际变化：读取/准备次数、被提交任务、提交时刻、传输或资源使用至少一项改变。
只改batch ID而物理执行不变的分组不能成为独立优化；不允许拼接多个prompt或强加整组完成屏障来制造收益。
对于未来候选，先在小状态空间枚举/受控反例核对合法性、安全和有限模型下的反事实，再安排真实M1/M2；
模拟只能证明所声明模型，不替代真实PG/模型对照。

### 6. 近期交付与停止开发的条件

1. **已完成文档交付**：本节五类设计选择与来源/反例表、明确作用域的最小模型与推导、M1—M4对照规格；
   知识库维护论文可见信息/动作/目标/保证/迁移条件，证据仍归各结果目录。
2. **下一步定位 M1 供给问题**：本轮真实容量筛查未得候选，先解释gateway CPU和观测成本，再制定新校准。M2实验层全局元数据已运行，实际SemMap接入与方法效果另验。
   这些测量/对照缺口尚未实现；先使比较可执行、可审核，再给出有限GPU预算，不能先实现复杂proposed再找支持数据。
3. **按反事实选择方法**：全局元数据净收益更好就采用全局/混合；调优请求数已足够就将token额度定位为资源控制；
   只有消费序/驻留代价解释了真实改善，才实现消费进度策略。M3/M4按其结果选择，不串行建设完整E/F。

<a id="historical-design"></a>
## 历史设计与后续条件性扩展

以下保留 2026-07 的研究假设与矩阵；其中“当前”“下一步”和运行参数均按原日期解释，
不能覆盖上方当前切片或自动恢复旧实验。
## 0. 前置依赖（先读这个）

**本计划中所有实验必须在 vLLM + 小 LLM baseline 建立后才能产生论文可用的最终数据：**

```
前置：vLLM + Qwen2.5-1.5B 级 LLM baseline 建立（替代手动 HTTP endpoint）
前置：COPY + deferred index 写回 baseline 建立
前置：模型 batch scaling 曲线（§4 前置实验）

当前状态：vLLM baseline、Daft 文本链路和 COPY/pgvector 工程 baseline 已建立；双 GPU
cache-on 数据组织已在 2-endpoint/4-endpoint 条件下完成复验。下方前置条件与候选矩阵是
2026-07-16 的设计快照，当前结论以文件顶部状态和 `rc1_data_organization/` 结果为准。
```

**为什么**：在 suboptimal GPU/写回 baseline 上搜出来的"最优 batch_size"会因为 GPU 端或写回端的瓶颈位置不同而偏移。论文必须用 S 级 GPU + A 级写回上的 参数组合穷举 结果。

**过渡期**：可以用当前 baseline 跑一遍 研究内容一 来验证脚本、确认趋势、调试阶段拆解——但最终数据必须来自 P0 完成后的重跑。

---

## 1. 研究问题

在"数据库触发 → 外部执行"链路中，行数据如何组织为 Arrow RecordBatch、partition 和 Ray object，才能匹配下游 AI 算子的执行特征？什么情况下需要感知 workload 类型（EMBED/FILTER/COMPLETE）来选择数据组织策略？

---

## 2. 假设（Hypotheses）

每个实验段在跑之前必须先写清楚要推翻什么。**不是盲目扫参。**

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H1.1 | 固定 batch=64 在所有 workload 和规模下已经接近最优 | 能否被推翻？| §6.1 参数组合穷举 |
| H1.2 | batch_size 的最优值与 partition_count 独立（无交互效应）| 能否被推翻？| §6.1 参数组合穷举 |
| H1.3 | 不同 workload 类型（EMBED/FILTER/COMPLETE）的最优 batch_size 相同 | 能否被推翻？| §6.2 workload 对比 |
| H1.4 | selectivity 不应影响 batch 构造策略 | 能否被推翻？| §6.3 selectivity-aware |
| H1.5 | 模型自身的 batch scaling 在 batch=64 时已达到吞吐平台期 | 能否被推翻？| §4 前置实验 |

**最可能被推翻的假设决定 研究内容一 的核心贡献**：如果 H1.3（不同 workload 的最优 batch 相同）被推翻 → 研究内容一 有独立贡献；如果 H1.3 成立但 H1.5 被推翻（模型在 >64 时继续 scaling）→ 研究内容一 的贡献移到"模型 scaling 行为驱动 batch 选择"而非"workload 感知"。

---

## 2.5 分组策略设计空间：按相似度分还是按均衡分

### 2.5.1 问题定义

Token-budget 策略确定"每个 batch 放多少 token 总量"（batch 边界），但**不决定"哪些行放入同一个 batch"**（分组策略）。分组策略的选择直接影响：
- 每个 batch 内的 prefill 时间同质性
- vLLM chunked prefill 的 prefill-decode 交错效率
- 异构 actor pool 的路由可行性
- 与 prefix-aware grouping 的兼容性

### 2.5.2 两种分组策略

| 策略 | 机制 | 示例（token budget = 4096） |
|---|---|---|
| **A: Length-Align** | 按 token 长度相似度分组，短的和短的在一起，长的和长的在一起 | Batch 1: [50, 60, 45, 55, …] × 80 行 ≈ 4000 tok；Batch 2: [3500, 4000, 3800] ≈ 11300 tok（可能超过 budget，需单独处理） |
| **B: Bin-Packing** | 混合不同长度，使每个 batch 的总 token 量尽量接近 budget | Batch 1: [50, 3500, 500] ≈ 4050 tok；Batch 2: [4000, 60] ≈ 4060 tok |

**关键区分**（来源：2026-07-20 chunked prefill 交叉分析）：
- **A 操作的是"batch 内的同质性"**——batch 之间差异大，batch 内部差异小
- **B 操作的是"batch 间的均衡性"**——batch 之间差异小，batch 内部差异大

### 2.5.3 两种策略在 vLLM Chunked Prefill 下的行为差异

vLLM chunked prefill 的调度器采用 **decode-priority** 策略（事实，来源：vLLM 官方文档 v0.4.2+）：每轮迭代优先调度 decode 请求，剩余 token 预算分配给 prefill chunk。

**方案 A（Length-Align）的行为**（推断）：
- 短 batch：所有请求 prefill 快速完成 → 全部进入 decode → decode 阶段有多请求并发
- 长 batch：所有请求 prefill 都很大 → prefill 被 chunked 分步执行 → **没有短 decode 请求可交错** → chunked prefill 的 "prefill-decode 混合" 优势减弱
- 如果短 batch 和长 batch 到达 vLLM 的时间错开，内部队列只有同类请求 → 失去混合调度的多样性

**方案 B（Bin-Packing）的行为**（推断）：
- 每个 batch 天然混合长短请求 → 提交后，短请求第一个 chunk 就完成 prefill 进入 decode，长请求继续跨 chunk prefill
- vLLM 调度器在后续 iteration 中：decode（来自短请求）+ prefill chunk（来自长请求）在同一 forward pass 中混跑
- **这正好是 chunked prefill 设计的最优场景**：compute-bound prefill 与 memory-bound decode 交错

### 2.5.4 两种策略的 Fatal Flaw

| 策略 | Fatal Flaw | 触发条件 | 验证方式 |
|---|---|---|---|
| A: Length-Align | 数据单峰分布 → 退化为随机分组 | 数据集中 > 80% 行集中在同一 token 长度区间 | 实验前画 token 长度分布直方图，确认多峰或长尾 |
| B: Bin-Packing | 极端 outlier 稀释优势 | 存在单行 token 量 > budget → 独占整个 batch，其他行与 length-align 无异 | 检查 P99/P50 token 比；如果 max > 2× budget，bin-packing 退化为 "outlier 独占 + 其余正常打包" |

### 2.5.5 与异构 Actor Pool 和 Prefix-Aware Grouping 的交互

| 交互对 | 兼容性 | 说明 |
|---|---|---|
| Length-Align × 异构 Actor Pool | ✅ **天然兼容** | 短 batch → 普通 actor，长 batch → 高容量 actor，路由清晰 |
| Bin-Packing × 异构 Actor Pool | ❌ 冲突 | 所有 batch 特征相同，无法按特征分池——分池路由失去意义 |
| Length-Align × Prefix-Aware | ✅ **可叠加** | 先按前缀分组（最大化 APC 命中率），再按长度子分组 |
| Bin-Packing × Prefix-Aware | ⚠️ 冲突 | 为均衡 token 量可能拆散同前缀的行 → 降低 APC 命中率 |

### 2.5.6 实验策略建议

**主推方案 B（Bin-Packing）作为 RC1 主策略**（推断，待实验验证）：
- 与 vLLM chunked prefill 的 prefill-decode 交错机制天然协同
- 优化目标清晰："每个 batch 的 GPU 计算量均衡"
- 文献依据：Orca 的 selective batching、vLLM 的 continuous batching 本质上都在混合不同长度的请求

**方案 A（Length-Align）保留为消融对比**：
- 在异构 actor pool 场景下（§5.3 或后续实验）：length-align + 分池路由 vs bin-packing + 统一路由
- 在 prefix-aware 联合实验中：length-align + prefix-aware 两级分组 vs bin-packing-only

**新增假设**：

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H1.6 | Bin-packing 分组在统一 actor pool + vLLM chunked prefill 下的端到端吞吐优于 length-align 分组 | 能否被推翻？| §6.1 扩展 |

### 2.5.7 语义安全边界：行内 prompt 不可拆分

**红线**（事实，来源：2026-07-20 vLLM chunked prefill deep-research 验证）：
> 将一份逻辑完整的 prompt 手动拆分成多条独立 vLLM 请求 → KV cache 隔离、上下文断裂、输出语义错误。vLLM 的 `--enable-chunked-prefill` 是引擎内部 token 级分片（数学等价），与手动 request 级拆分是完全不同的机制。

**对本实验的约束**（推断）：
- **每行数据 = 一个独立完整的 vLLM 请求**。Token-budget 策略决定的是 "多少行合并为一个 batch"，不是 "如何切割一行内的 prompt 文本"
- 如果某单行的 prompt token 量超过模型 context window（如 Qwen2.5-1.5B 的 32K），**禁止在上游 Daft/Ray 层自动切分该行的 prompt 内容为多条请求**
- 超长单行的处理方式：① 在数据准备阶段截断（truncate）到 context window 内；② 或将超长行标记为单独处理（独占一个 batch，不做拆分）；③ 或从数据集中排除

**实验前检查清单**（添加到 §12）：
- [ ] 确认数据集中每行的 prompt 是自包含的（self-contained），行间无语义依赖
- [ ] 确认所有单行的 token 量 < 模型 context window（32K for Qwen2.5-1.5B）
- [ ] 如果存在超长行：明确处理策略（truncate / 独占 batch / 排除），并记录在实验报告中

### 2.5.8 每行 token 来源与元数据记录规范

`prompt_tokens` 是每行 prompt 在目标服务模型 tokenizer 下的输入 token 数。它不是字符数、词数，也不是数据集 trace 中原始 request token 字段的无条件复用值，而是为了让上游调度策略感知模型侧计算量而附加到行上的执行元数据。

获取方式：

```python
tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, local_files_only=True)
prompt_tokens = len(tokenizer.encode(prompt, add_special_tokens=False))
```

要求：

- 使用与 vLLM 服务端模型一致的 tokenizer，例如本地 `models/Qwen2.5-1.5B-Instruct`。如果 tokenizer 与服务模型不一致，该实验不能用于 token-aware 策略结论。
- 在 workload 导入或执行前预先计算 `prompt_tokens`，写入 PostgreSQL `documents.prompt_tokens`，并随 Daft/Arrow table 一起进入 `DataOrganizer`。
- `token_budget` 组批使用的单行估计代价为 `prompt_tokens + completion_max_tokens`。其中 `completion_max_tokens` 是本次实验请求的生成上限；如果后续改用历史 P95 输出长度，需要在实验报告和 CSV 字段中显式记录。
- vLLM 返回或 Prometheus 暴露的 prompt token 指标只作为运行后校验信号，不作为执行前分组的唯一来源，因为分组决策必须在请求提交前完成。
- 如果数据集自带 request token 字段，只能作为 trace 元数据或 fallback；正式策略实验优先使用目标模型 tokenizer 重新计算后的 `prompt_tokens`。

必须记录到实验材料的字段：

| 字段 | 说明 |
|---|---|
| `tokenizer_path` / `tokenizer_name` | 计算 `prompt_tokens` 使用的 tokenizer |
| `tokenizer_add_special_tokens` | 是否在计数时加入 special tokens；当前默认 `false` |
| `prompt_tokens_min/p50/p95/p99/max` | 输入 token 分布，用于证明 fixed rows 是否是弱代理 |
| `completion_max_tokens` | 估计单行总代价时加入的输出 token 上限 |
| `max_model_len` | 过滤或标注超长行的上下文窗口约束 |
| `token_count_source` | `model_tokenizer` / `trace_metadata` / `char_proxy`，正式实验应为 `model_tokenizer` |
| `batch_tokens_p50/p95/p99/max` | 分组后每个 Ray/vLLM 请求的 token 形状 |

文档落点规则：

- token 获取、字段定义、分组公式、超长行处理写在本文件。
- 历史代码模块、抽象接口与 CSV 字段映射见
  `reference/strategy_design_implementation_reference.md`；当前接口以 `../../code/README.md` 为准。
- 具体实验命令、CSV 路径、结果解释写在对应 `results/.../README.md`。

---

## 3. 变量

| 变量 | 含义 | 取值范围 |
|---|---|---|
| `batch_size` | 每次提交到 GPU 的行数 | {8, 16, 32, 64, 128, 256, 512} |
| `partition_count` | Ray task/actor 数 | {1, 2, 4, 8} |
| `object_merge` | Arrow RecordBatch 的合并策略 | {none, coalesce_input, coalesce_output} |
| `workload_type` | AI 算子类型 | {EMBED (真实), FILTER (模拟), COMPLETE (模拟)} |
| `selectivity` (仅 FILTER) | 语义过滤的选择率 | {0.1, 0.3, 0.5, 0.8} |
| `text_length` (仅 COMPLETE) | 平均 token 数 | {short <128, medium 128-512, long >512} |
| `grouping_strategy` | 如何选择哪些行放入同一 batch | {random (baseline), length_align, bin_packing} |
| `token_budget` (仅 grouping ≠ random) | 每个 batch 的目标 token 总量上限 | {1024, 2048, 4096, 8192}（根据模型 context window 调整）|

**关于 FILTER/COMPLETE 的诚实标注**（参照 Orca 合成权重的做法）：

| Workload | 当前状态 | 论文中标注 |
|---|---|---|
| EMBED | ✅ 真实 GPU embedding（all-MiniLM-L6-v2, 384d）| 真实 workload |
| FILTER | ⚠️ 模拟——用 embedding 相似度 + 阈值模拟布尔输出，selectivity 人工控制 | "simulated AI_FILTER with known selectivity" |
| COMPLETE | ⚠️ 模拟——用随机长度处理延迟模拟 token generation | "simulated AI_COMPLETE with controlled token length distribution" |

---

## 4. 前置实验：模型 batch scaling 曲线（P0c，必须在 研究内容一 所有实验之前跑）

### 4.0 研究问题

在讨论"batch_size 如何影响端到端延迟"之前，必须先搞清楚：**GPU 模型自身的吞吐是怎么随 batch_size 变化的？** 如果模型在 batch=32 就饱和了，那讨论 batch=256 毫无意义。

### 4.0 假设

H1.5：模型自身的 batch scaling 在 batch=64 时已达到吞吐平台期。

### 4.0 方法

```
脱离数据库/Ray 链路，直接用模型推理：
  model = SentenceTransformer("all-MiniLM-L6-v2")
  texts = [random text of length ~200 chars] × N

  batch_size ∈ {1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
  N = max(batch_size) × 10（确保有足够多 batch）
  
  每 batch_size: 跑 20 个 batch，忽略前 5 个（warm-up），取后 15 个的中位数
  指标: T_per_batch, T_per_row, rows/s

预计耗时: ~30 分钟（不需要数据库、不需要 Ray）
```

### 4.0 输出

- 一条 `batch_size → rows/s` 曲线（X=batch_size, Y=吞吐）
- 标注吞吐平台期的起始 batch_size
- 如果平台期在 batch=32：研究内容一 讨论区间应聚焦 8-128，256/512 只有验证价值
- 如果平台期在 batch=256：研究内容一 有更大 tuning space，batch 选择对 GPU 利用率影响更大

**这条曲线是 研究内容一 所有讨论的前提。画好了才能解释后续所有实验里 batch_size 的影响。**

---

## 5. Baseline 对照

| 编号 | 描述 | 级别 | 来源 |
|---|---|---|---|
| **A1.1** | 固定策略 Baseline（coalesced vs fine 互相对照）| 合理默认 | 已有 |
| **D1** | Fixed Partition + Fixed Batch（Daft/Spark 默认，不做 workload 感知）| B 级 | Daft 文档 + Spark SQL Tuning |

---

## 6. 实验矩阵

### 6.0 7B 双 GPU 复验隔离规则（2026-07-28）

旧双卡配置同时启用了 accelerated arrival replay、50ms flush 和 token-budget。
现场 1024 行 gate 的 packing budget utilization 仅约 13.5%，平均每批约 3 行：
大多数 batch 在达到 token budget 前已被 timeout 关闭。该配置只能研究在线
arrival/flush，不足以判断 token-budget 或 length-align 本身是否有效。

复验前先以 request-level submission 标定 per-endpoint active work 饱和区。
随后使用 `source_order=doc_id`、关闭 arrival replay，令完整 organizer 输入
可见，固定 active work、较高的 static per-endpoint K 和 endpoint routing，
仅改变 token budget：

```text
sequential_token_budget ∈ {8192, 16384, 24576, 32768, 49152, 65536}
```

这条曲线要验证的不是“更大的 budget 能装更多行”这一恒真命题，而是吞吐是否
存在甜点。budget 太小时，HTTP/Ray 调用数和固定开销增加，单次提交给 vLLM 的
可选请求不足；budget 太大时，兼容 HTTP 的列表响应形成更粗的完成屏障，
短请求要等同 submission 中最长请求返回，补位变慢，P99、completion span 和
job 间干扰可能上升。因此预期 `tokens/s` 随 budget 先升后平台或下降，而不是
单调上升；如果一直单调上升到 65536，说明搜索上界还没覆盖平台，不能声称
任何较小预算最优。

容量曲线必须先检查：

- `packing_budget_utilization_mean`、`organization_batch_rows_mean`、
  `organization_row_cap_hit_ratio`、submission batch 数和 HTTP 调用数；
- observed tokens/s、rows/s、request/service P95/P99；
- submission completion span、补位间隔和 credit idle ratio；
- 每 endpoint running/waiting、GPU/MFU 与端点流量分布。

固定 work 的曲线确定 `BEST_TESTED_TOKEN_BUDGET` 后，第二轮才在同一预算和
active work 上比较
`fixed_rows_8`、sequential、row-cap-aware 和 length-align，避免把预算大小与
membership 算法混成一个因素。如果利用率低于 50%，不进入策略胜负解释，先
诊断 256 row cap、oversized rows 或 organizer visibility。只有离线组织阶段出现
可辨认的 batch-shape 差异后，才把候选带回 arrival replay 验证在线泛化；
不能同时调 flush timeout 来“帮助”某个组织策略。

### 6.0.1 动态 token budget 的晋级实验

动态 budget 不是直接把 vLLM `running` 或 GPU utilization 映射成一个更大的
数。上游组织预算、上游 active-work admission 和 vLLM 内部
`max_num_batched_tokens` 是三个不同控制量。第一版动态组织只允许使用：

```text
pending predicted work
arrival-rate EWMA
endpoint completion/service-rate EWMA
oldest-request slack
```

控制动作是在已经由静态容量曲线标定的 `{B_min, B_mid, B_max}` 中选择下一批
的目标预算，并受 hard max-wait 约束。正式挑战 workload 分三段
`short-heavy → long-heavy → mixed/burst`。比较：

1. 每段分别使用其静态最优预算（oracle 上界，不可在线实现）；
2. 全程使用训练 workload 的最佳单一静态预算（强 baseline）；
3. 动态预算；
4. 动态预算去掉 service-rate feedback 的消融。

动态策略只有在 held-out 顺序或到达率下接近 oracle、显著优于最佳单一静态值，
并且 P99/饥饿 guardrail 不退化时才晋级。稳态单一 workload 下收敛到静态值是
合理结果，不应为制造收益而持续振荡。

### 6.1 参数组合穷举：建立静态最优 baseline

**假设**：H1.1（固定 batch=64 已最优）、H1.2（batch_size 和 partition_count 独立）。

```
batch_size      ∈ {8, 16, 32, 64, 128, 256, 512}
partition_count ∈ {1, 2, 4, 8}
object_merge    ∈ {coalesce_output}  # 当前已知最优
──────────────────────────────────────────
总组合: 7 × 4 = 28
每组合: 3 次重复（Ray 重启、warm-up 1 次不计入）
总运行: 84 次

固定条件（P0 完成后）:
  - GPU: vLLM / Ray Serve（S 级 baseline）
  - 写回: COPY + unlogged staging + deferred HNSW index（A 级 baseline）
  - 数据规模: 16384 行
  - Workload: AI_EMBED（真实）
```

**输出**：联合最优的 `(batch_size*, partition_count*)` = 研究内容一 的 A 级 baseline。同时检验 H1.2（是否存在交互效应——某些 batch_size 在特定 partition_count 下表现异常）。

### 6.2 Workload 对比

**假设**：H1.3（不同 workload 的最优 batch_size 相同）。

| Workload | batch_size | partition_count | 数据规模 | 标注 |
|---|---|---|---|---|
| EMBED | 参数组合穷举 最优 × 3 | 参数组合穷举 最优 × 3 | 1024, 4096, 16384 | ✅ 真实 |
| FILTER | selectivity ∈ {0.1, 0.5} × 参数组合穷举 | 参数组合穷举 最优 | 4096, 16384 | ⚠️ 模拟 |
| COMPLETE | text_length ∈ {short, long} × 参数组合穷举 | 参数组合穷举 最优 | 1024, 4096 | ⚠️ 模拟 |

每种组合 3 次重复，Ray 重启，warm-up 1 次不计入。

**如果 H1.3 被推翻**（不同 workload 的最优 batch 不同）→ 研究内容一 核心发现成立。
**如果 H1.3 成立**（所有 workload 下 batch=64 都最优）→ 研究内容一 的贡献变为"验证了固定策略的鲁棒性"，workload-aware 的增量价值需重新评估。

### 6.3 Selectivity-Aware 策略（当 FILTER 场景可用时）

**假设**：H1.4（selectivity 不应影响 batch 构造策略）。

| selectivity | 假设最优策略 | 为什么 |
|---|---|---|
| < 0.2 | 小 batch (32)、多 partition | 大部分行被过滤，小 batch 减少 GPU 浪费 |
| > 0.5 | 大 batch (128)、单 partition | 大部分行都过，大 batch 省 invocation 开销 |

**对照**：同 selectivity 下，固定 batch=64 作为基线。

---

## 7. 指标

| 指标 | 测量方法 | 论文参照 |
|---|---|---|
| **端到端延迟** | `time.perf_counter()` 从 DB fetch 开始到 writeback 结束 | vLLM/Orca 的端到端 serving latency |
| **阶段拆解** | DB fetch → Arrow build → GPU request wall → fan-in → writeback | TurboVecDB 的 HNSW 层级拆解思路 |
| **吞吐 (rows/s)** | `total_rows / T_e2e` | vLLM 的 requests/second |
| **Ray object 数** | `ray.objects()` 计数 | 诊断指标 |
| **GPU 利用率** (如有) | vLLM 可采集；手动 HTTP endpoint 无此指标 | vLLM 的 GPU utilization |

**关键**：不报"coalesced 比 fine 快 13.4×"这样的单点数字，而是画 `batch_size → T_e2e` 全景曲线，让 reviewer 看到全工作点。

---

## 8. 消融设计

对 A1.2（Workload-Aware Partition）的消融：

| 消融项 | 做法 | 要检验什么 |
|---|---|---|
| 规则表 vs 固定策略 | A1.2 规则表 vs A1.1 参数组合穷举 最优固定值 | 规则表在 workload 变化时是否优于固定策略？ |
| 规则表 vs 随机 | A1.2 规则表 vs 随机选配置（5 次取中位数）| 排除"随便选也能中"——规则表必须好于随机 |

---

## 9. 结果展示图

| 图号 | 内容 | 类型 | 论文参照 |
|---|---|---|---|
| Fig_RC1_0 | 模型 batch scaling 曲线：batch_size → rows/s | 折线图（前置实验）| — |
| Fig_RC1_1 | batch_size → T_e2e 曲线（不同 partition_count 各一条线）| 折线图 | vLLM 的吞吐-延迟曲线 |
| Fig_RC1_2 | 三 workload 的阶段拆解并排柱状图 | 堆叠柱状 | TurboVecDB 的层级拆解 |
| Fig_RC1_3 | selectivity → T_e2e（固定策略 vs workload-aware）| 折线图 | Orca 的多模型尺度图 |

---

## 10. 统计规范（参照 vLLM/Orca 标准）

| 要求 | 做法 |
|---|---|
| **重复次数** | 每组配置 3 次（参数组合穷举）。核心发现（被推翻的假设）额外补到 5 次 |
| **集中趋势** | 取**中位数**（不取平均值——系统实验的临时 outlier 会拉偏平均值）|
| **离散度** | 报告 IQR（四分位距），5 次以上报告标准差 |
| **Ray 状态重置** | 每次重复之间 `ray stop` → `ray start`，避免内存缓存/对象复用 |
| **数据库状态** | 每次重复之间 TRUNCATE 目标表，确保写入量一致 |
| **Warm-up** | 每组配置先跑 1 次 warm-up（不计入结果），后面 N 次计入 |
| **随机种子** | 数据生成固定 seed（`random.seed(42)`），确保不同配置跑同一批数据 |

---

## 11. "When does it NOT help?" 边界验证

每个边界条件必须对应一个**可跑的实验点**，不是空洞的自省。

| 边界条件 | 验证实验 | 期望结果 |
|---|---|---|
| workload 特征在运行前已知且不变 | 固定 1 种 workload，比较 "规则表选择" vs "固定 batch=64" | 差异 < 5% → 边界成立 |
| 数据量 < 500 行 | 256 行规模下，比较 batch_size ∈ {8, 32, 64, 256} | 各配置 T_e2e 差异 < 10% → 边界成立 |
| GPU 模型对所有 batch_size 吞吐几乎恒定 | 看 §4 前置实验的 batch scaling 曲线 | 如果平台期从 batch=8 开始 → batch_size 选择不重要 |
| 数据集中存在超过 context window 的单行 | 检查 max(token_count) 是否 > 模型 context window（32K for Qwen2.5-1.5B）| 如有 → 预处理截断或排除；**禁止在 Ray 层自动拆分单行内容为多条请求**（会导致语义断裂，参见 §2.5.7） |
| 分组策略与 chunked prefill 的交互 | length_align vs bin_packing 在 `--enable-chunked-prefill` on/off 下的对比（仅 V0；V1 强制开启）| bin_packing 在 chunked prefill on 时优势更大（prefill-decode 天然混合） |

---

## 12. 运行检查清单

- [ ] P0c: 模型 batch scaling 曲线（§4）完成
- [ ] P0a: vLLM/Ray Serve 接入完成
- [ ] P0b: COPY + deferred index 写回 baseline 确认
- [ ] P1: 参数组合穷举（batch_size × partition_count）在 P0 完成后重跑，确立 `(batch_size*, partition_count*)`
- [ ] P1: 三 workload（EMBED + FILTER/sim + COMPLETE/sim）完成
- [ ] P1: 阶段拆解数据可以画 Fig_RC1_2
- [ ] P2: selectivity-aware 策略对照（当 FILTER workload 可用时）
- [ ] §11 的边界验证实验点完成
- [ ] 所有结果 CSV 保存在 `results/rc1/`
- [ ] 每个图标注：数据来源、排除 warm-up、硬件/模型/数据库版本、重复次数、取中位数还是平均值
- [ ] **语义安全检查**（来自 §2.5.7）：每行 prompt 自包含、行间无语义依赖
- [ ] **语义安全检查**：max(token_count) < 模型 context window，超长行的处理策略已明确
- [ ] **分组策略检查**：数据集的 token 长度分布直方图已画出，确认分布特征（多峰/单峰/长尾）→ 据此决定 length_align 是否有区分度
- [ ] **Chunked Prefill 状态**：确认实验使用的 vLLM 版本及 chunked prefill 是否开启，记录在 CSV 的 `server_version` 字段中
