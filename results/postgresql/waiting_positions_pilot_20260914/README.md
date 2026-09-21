# 提交控制与等待位置：首轮受控小实验

2026-09-20 状态说明：本轮作为**已完成的测量反例**保留。后续研究按
[有效吞吐平台附近的在途工作与资源代价](../../../docs/plans/data_organization_batching.md#m1-throughput-platform)，
不继续把欠供给下的 HTTP 延迟下降作为优化目标。以下运行设计、结果和当时建议保留历史原文。


后续[常驻服务与C4/C8重复检查](../waiting_positions_persistent_20260914/README.md)已完成；本文保留前两轮每查询新gateway的条件与原始结果。

状态：2026-09-14。M1已开始并完成第一轮观测/机制检查，**尚未取得真实模型性能结论**。受众：内部研究记录。
[运行合同](../../../docs/plans/data_organization_batching.md#m1-pilot)。研究对象为
**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。

## 问题与实现

检验提交限制是否改善查询，还是把等待从提交后转到提交前/消费端。复用现有请求FIFO、Map token描述与work限制，
不修改生产调度策略。`query_runner`在调用入口记录查询专属准备起点，保留旧SQL/stream release；
`waiting_positions.py`复用PG独立行绑定和flow诊断，补Core权威终态与HTTP事件，逐行核对时间守恒。

对已准备且不再修改的普通表、单Map全扫描，声明所有行从调用时刻a具有逻辑执行资格。
这不是从Core接纳或child拉取时刻反推资格，也不适用于NULL、依赖、LIMIT或可变输入。
每行记录并核对：

`客户端收到-a = (提交-a) + (终态-提交) + (PG就绪-终态) + (节点交付-PG就绪) + (客户端收到-节点交付)`。

driver和受控服务在查询间常驻；每个查询启动自己的gateway，token臂另外加载tokenizer。
共享数据构造/导入在查询之外；调用至EOF包含manifest/连接/配置、gateway/tokenizer准备及实际SQL执行，结果持久化/评价另报。
因此该总时包含**当前runner的每查询新gateway代价**，不能说常驻生产服务的每个查询也必然承担这笔代价。
Core终态是责任结算观察，不是GPU完成；HTTP包含连接、排队、响应读取。

## 配置、资格与预定对照

- 首轮实现`b7c46443`，运输层跟进`9088eb55`。Linux/Python3.12.3/PG18.3，使用隔离测试构建`SEMLOOM_FLOW_DIAGNOSTIC=1`、`-O2 -Werror`。
  沿用现有诊断字段，不改C代码；ready-first关闭、不注入输入暂停，所有臂相同追踪。
- 真实PG连接生产网关；HTTP是本机受控分类fixture，服务槽4。没有调用真实模型或改动GPU服务。
- 物化窗口8，Core输入/结果各8MiB，PG total 8MiB、单行暂存4MiB；结果维持输入序。
- 输入按24/80/144/224个词重复长度，16行调优、32行比较，ID互不重叠。全部为合成机制数据，不是自然语言质量评价集。
  既有Movie-derived Map语义与版本化消息保持一致；fixture返回其预设分类，不沿用E的Echo失败目标。
- 每个完整请求work分别为187/243/307/387 token（prompt+128最大输出），都能放入context512。
  C8最高8项work之和3096，宽W=3096保证不约束任一8项集合；紧W512会生效。
  由于3×最小work187=561>512，紧组最多2项在途；这不是把估计token当真实服务时长。
- 首轮C2/4/8各2次16行查询，第二次反转顺序；按调用至EOF中位数选C8。C4/C8仅差约0.00055秒，
  这只是预定选择规则的结果，不是稳健最优或真实服务强静态资格。
- 三臂固定C8：request FIFO、同序token FIFO且W3096、同序token FIFO且W512。三次Latin次序预先规定。
  所有token臂实际提交顺序为0…31，宽组work-only拒绝0，紧组明确出现work-only拒绝。

## 两轮执行与运输层排查

首轮15个查询、384次HTTP全部完成。宽token组两次第8个请求约1.06秒，造成P99与SQL耗时波动。
服务器实际Python3.12的ThreadingHTTPServer默认listen backlog=5，小于客户端并发8；首轮未采集TCP握手/drop，不能事后宣称已证明根因。

跟进保留首轮selection、输入、C8、W3096/512及顺序，明确设置fixture backlog32，补handler收到请求、获得4槽服务资格、结束三个时刻。
三臂各3次32行，共288次，没有重新调优。backlog与观测同时变化，两轮差异不是严格单变量因果证明。
本轮未再出现约1秒的首批异常；其服务并发峰值request/宽token均4，紧token为2。
跟进中的请求正文哈希与handler记录逐查询完全匹配，HTTP服务队列的平均等待在宽组约14–19ms、紧组约0.021–0.022ms。

两个原始轮次均保留，不把后者替换前者。共24个查询、672次受控HTTP，实际模型0次；所有查询输出/资源/独立关联检查通过。
本地与服务器12项观测/旧绑定合同检查通过，逐行时间段之和与实际总时一致；840份非Markdown代码/配置一致。
PG、网关、HTTP、任务进程、端口与临时ACL均已清理。

## 跟进轮结果

以下为每臂3个完整查询的中位数，单位秒。HTTP P99在单个32请求查询内线性插值，再取3次中位数；不能据此作稳定尾延迟估计。

| 对照 | 调用至EOF | 查询前准备 | SQL至EOF | HTTP P99 | SQL release至提交的逐行平均等待 |
|---|---:|---:|---:|---:|---:|
| 请求数FIFO | 0.945036 | 0.487975 | 0.457138 | 0.101076 | 0.209197 |
| token FIFO，W3096 | 5.654536 | 5.161990 | 0.481275 | 0.098061 | 0.226533 |
| token FIFO，W512 | 6.715896 | 5.368059 | 1.353774 | 0.059790 | 0.693792 |

紧W相对宽W的**中位数之比**：HTTP P99减少39.03%，SQL至EOF增加181.29%（2.81倍），调用至EOF增加18.77%。
提交前的逐行平均等待（已剔除查询前准备）从0.227秒增加到0.694秒。服务端排队显著缩短，同时在途服务峰值由4降为2。
这组受控证据不支持“单请求HTTP尾延迟降低代表查询收益”，且说明不能漏掉提交前与消费等待。

另一个独立观察是当前token两臂的查询前准备约5秒；其逐行token准备合计只有约20–30毫秒。
不能把约5秒全部称为逐行token化成本，它包含新gateway、模块/模型相关库与tokenizer加载等准备。
具体启动组成未进一步profile；在常驻gateway/tokenizer下是否还能得到同样的总时比例，仍未验证。

## 全部单次值

第一轮保留调优与异常；第二轮固定第一轮选择，仅做运输层跟进。单位秒。

| 轮次 | 单元 | 调用至EOF | 准备 | SQL至EOF | HTTP P99 |
|---|---|---:|---:|---:|---:|
| controlled | waiting-tuning-request-2-0 | 1.001441255 | 0.487464379 | 0.513976876 | 0.056634428 |
| controlled | waiting-tuning-request-4-0 | 0.859483521 | 0.539279362 | 0.320204159 | 0.062344076 |
| controlled | waiting-tuning-request-8-0 | 0.786969079 | 0.478034560 | 0.308934519 | 0.101419481 |
| controlled | waiting-tuning-request-8-1 | 0.799222208 | 0.478020247 | 0.321201961 | 0.104619860 |
| controlled | waiting-tuning-request-4-1 | 0.727803727 | 0.398213749 | 0.329589978 | 0.057552558 |
| controlled | waiting-tuning-request-2-1 | 0.902729051 | 0.399635624 | 0.503093427 | 0.054979014 |
| controlled | waiting-evaluation-request-8-0 | 0.849879593 | 0.397936953 | 0.451942640 | 0.096716892 |
| controlled | waiting-evaluation-token-wide-8-0 | 6.404867588 | 5.002358590 | 1.402508998 | 0.760442334 |
| controlled | waiting-evaluation-token-tight-8-0 | 6.411643434 | 5.060793918 | 1.350849516 | 0.057495632 |
| controlled | waiting-evaluation-token-tight-8-1 | 6.950524777 | 5.641950936 | 1.308573841 | 0.057366967 |
| controlled | waiting-evaluation-request-8-1 | 0.855828030 | 0.397727585 | 0.458100445 | 0.098799186 |
| controlled | waiting-evaluation-token-wide-8-1 | 5.797451295 | 5.340053055 | 0.457398240 | 0.097233152 |
| controlled | waiting-evaluation-token-wide-8-2 | 6.543420961 | 5.120161055 | 1.423259906 | 0.764503490 |
| controlled | waiting-evaluation-token-tight-8-2 | 6.558997835 | 5.200001012 | 1.358996823 | 0.059371695 |
| controlled | waiting-evaluation-request-8-2 | 0.849641050 | 0.399135980 | 0.450505070 | 0.101111587 |
| transport | waiting-evaluation-request-8-0 | 0.945035941 | 0.487974953 | 0.457060988 | 0.097113911 |
| transport | waiting-evaluation-token-wide-8-0 | 5.654536256 | 5.161989598 | 0.492546658 | 0.098061029 |
| transport | waiting-evaluation-token-tight-8-0 | 6.452640049 | 5.098866154 | 1.353773895 | 0.057874405 |
| transport | waiting-evaluation-token-tight-8-1 | 6.715896394 | 5.368059320 | 1.347837074 | 0.059790286 |
| transport | waiting-evaluation-request-8-1 | 0.963847477 | 0.498162366 | 0.465685111 | 0.101437127 |
| transport | waiting-evaluation-token-wide-8-1 | 5.723602011 | 5.257876940 | 0.465725071 | 0.101362233 |
| transport | waiting-evaluation-token-wide-8-2 | 5.639086917 | 5.157811626 | 0.481275291 | 0.094440363 |
| transport | waiting-evaluation-token-tight-8-2 | 7.306630174 | 5.924030744 | 1.382599430 | 0.059962404 |
| transport | waiting-evaluation-request-8-2 | 0.937932636 | 0.480794367 | 0.457138269 | 0.101076305 |

## 证据与能说明的范围

[原始投影清单](raw/manifest.json)、[环境](raw/preflight.json.gz)、[构建](raw/build.json.gz)、
[首轮](raw/controlled__m1__waiting-comparison.json.gz)、[跟进](raw/transport__m1__waiting-comparison.json.gz)、
[handler时间](raw/transport__m1__fixture-http-timing.json.gz)、[独立读回复核](raw/readback.json.gz)、
[首轮清理](raw/controlled.json.gz)、[跟进清理](raw/transport.json.gz)。

原始请求/输出/SQLite账本保留在仓库外；公开raw为脱敏、内容哈希投影，并分别记录原件、投影与压缩文件SHA。
等待面积是任务数×秒，资格未兑现为物化行时也计入；它不是PG留存字节、进程RSS或GPU显存。
未直接测量模型kernel、真实GPU服务量、低扰动版本或多查询收益。没有官方baseline排名、置信区间或显著性结论。

下一步仍留在M1：先用常驻gateway/tokenizer做同配置对照，单列一次性准备与逐查询成本，并对近乎打平的C4/C8做稳定性检查；
之后再建立新的真实输入划分与独立模型预算。该轮没有触发M2、修改生产策略或复用E的模型额度。
