# Records 执行模块整理与容量责任

本记录对应 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化中的执行工程整理。
基准为 PR #90 的 `bfb9100e2f3fa090fdf2304359979b853236e06a`，tree 为
`c8a86b0bcff8bf0161dcbbd005bc2660060d228e`。该 PR 当时为 Draft，未合并。
本地候选只做 CPU 检查，尚待独审与统一 Git 整合；不提供模型性能结论。
机器摘要见 [checks.json](raw/checks.json)，完整输出通过运行标识
`semloom-runtime-refactor-20261104` 和其中的 SHA-256 交接。

## 容量实际归属

以下为源码核对结果。R 是旧 `RecordsResources.max_active_batches`，C 是查询接纳窗口；
IPC 表示模型进程间通信。已有独立资源对象继续负责记账，没有新增控制器。

| 资源 | 实际记账与归还 | 旧默认换算 | 显式独立声明 |
|---|---|---|---|
| Core 驻留任务 | `SessionCapacity.records`；结果租约归还后删除记录 | `held_tasks=R` | `budget.held_tasks` |
| Core 输入／结果字节 | 真实输入长度与每任务结果预留；随记录保留 | 各为 R × 单项额度 | `budget.input_bytes/result_bytes` |
| Core 模型请求／工作量 | 记录上的 compute 状态；确认终态后归还 | 请求和普通请求工作量为 R | `budget.active_requests`；既有可选 token 工作量仍单独解释 |
| Method 续算及最终结果 | `MethodBudgetPool`；消费者归还最终行后释放 | R 个续算；R × 完整逐行预留 | `budget.method_runs/method_bytes` |
| IPC 请求／结果／命令／控制槽 | `AsyncEngineLimits` 与原进程 owner；沿原 ACK、消费和清理路径归还 | 请求、结果、命令槽从 R 换算；控制预留保持原式 | 完整有限 `async_limits` |
| IPC 输入／结果字节 | 原进程 owner 持有；unknown 不能当已完成 | 输入总额从 owner R 换算；结果总额从结果槽换算 | 显式总额和单项额度分别保留 |
| 引擎序列／每轮 token／模型上下文 | vLLM 解析与实际 ready 回执 | 异步省略值已交给引擎 | 显式值保留；实际模型长度仍检查 |

旧默认的 R 仍会同时影响多种容量。本批将该换算集中表达，保留历史行为；
不能据此称普通默认已经完成 R 解耦。显式独立预算已能分别生效，但要求完整字节与槽位声明，
目前不自动探测硬件，也没有面向普通调用方的少量参数配置方案。
完整预期关系、调用审计及 Python 对象开销不都包含在上述逻辑额度中，不能把它们称为进程内存上限。

## 可达执行链与验收

维护配置经 `records_configuration`、`create_records_core` 进入普通 Core 或
`PreparedRecordsCore`。后者复用 `SemLoomRecordChainCore`：方法产生合法 ready 请求，
`PreparedTokenOrganizer` 组织传输批，原 transport、async actor 与私有模型进程负责交接，
Core 处理逐成员终态和结果，Method 推进合法后继，消费者归还最终行。
这条 records 路径保持外部 workload／fixture 身份；PG gateway 使用共享 Core 不能代替其自身接入检查。

| 验收对象 | 本批保持的行为与已有检查 | 尚需处理 |
|---|---|---|
| 合法依赖与原数据 | Filter→Map 原数据、逐记录补位、Extract 完整关系要求；运行既有多阶段 CPU 用例 | 新推进策略须另验完整查询与输出 |
| 准备、传输、模型、消费 | 主推进方法与 actor 方法定义保持；慢结果不改批成员 | `_tick` 先填充再推进的交错候选另审 |
| 容量及 unknown 责任 | Core、Method、IPC 沿原对象保留额度；结果未消费和未知终态不提前归还 | 普通默认仍耦合 R；完整关系与审计占用另核 |
| 引擎参数 | 不改已接入的实际上下文、原生序列／token 默认委托 | dtype 与显存比例默认另行核对 |
| 查询时钟与质量 | 不移动 start/evaluate/消费时钟，不改 prompt/parser、输出或计量字段 | 真模型、真实 Ray、多卡与完整查询比较尚未验证 |

## 首批：容量与执行配置分离

新增 [records_capacity.py](../../../../code/src/execution_provider/adapters/records_capacity.py)，
集中原 `RecordsCapacityBudget`、`CoreRecordsCapacity` 及 IPC 解析；各资源仍消费
`SessionLimits`、`MethodCapacity`、`AsyncEngineLimits`。
新增 [records_profile.py](../../../../code/src/execution_provider/adapters/records_profile.py)，
集中 sync/async 选择、具名历史选项和 token 组批选项的验证。
旧 `lotus_records_core` 导入保留同一对象，容量类型的序列化路径保持。
异步 actor 与链直接引用容量模块；生命周期、模型分配、关闭与 ACK 代码不搬动。

六份提取定义的语法树与 bfb 完全相同；其余 Core、链和异步 actor 的类／函数定义保持。
402 组声明回放的结果、错误类型与消息相同，包含普通／独立预算、C、R、IPC、历史选项与非法值。
原套件 145 项为 144 成功、1 既有失败；候选相同 145 项加 4 项新检查为 148 成功、相同 1 项失败，
没有错误或跳过。新检查覆盖旧导入、序列化、各预算只改变所属资源，以及独立预算不随旧 R 改变。
既有失败为 `CoreFeedbackTests.test_actual_new_method_prompts_Q1_Q3_Q2_Q3_reuse_one_model`，
本批没有改写它。两个套件运行期间源码均未变化。

随后扩大检查发现历史策略的测试读取器只在旧文件查找配置函数，产生 4 项新失败。
已让当前源码读取跟随显式 `records_profile` 导入，历史 oracle 仍只从 manifest 核对的原件读取，
重复定义立即报错；该组 18 项修订后全部通过。初次 4 项失败的原输出保留。
另在独立 bfb 原源码工作树复现 2 项 token 生命周期失败和 6 个帧计量方法错误
（共 68 次错误事件，含子用例），这些不是本批引入，不能改报成功；具体标识见机器摘要。

五份历史投影的 3010 条绑定保持原 10 处不匹配，本批新增 0；原 manifest、摘要、投影身份和副本不变。
更广套件此前的六项失败及资产／SDK 缺项仍是原记录，本批没有重跑全部用例，不能称整体回归全绿。

## 后续拆分与并行职责

| 对象 | 当前核对与下一步 | 分工 |
|---|---|---|
| R 与默认配置 | 先完成本批结构提取，再以配对 CPU 用例验证少量声明如何解析为已有独立额度；未定默认不能写成最佳值 | 本会话统一安排 |
| 纯观测解析与报告 | `lotus_chain_token_batches.py` 可提取无状态资源解读、快照头及报告拼装；保留原锁、时钟、样本消费和控制决定 | 本会话下一独立批次 |
| 准备／派发交错 | `_RecordLocalRows._tick` 仍先填充再推进；变更会影响调度与完整耗时，不能作为单纯移动代码处理 | 已有准备交错会话负责行为候选 |
| 压力样本消费 | 待 RPC 时清除未消费样本的候选已有独立证据；提取不改 `_consume_snapshot` | 已有压力会话负责修正 |
| 取消与审计交付 | bfb 保留被接收方暂拒的审计回执；计算退休、审计交付和 ACK 分别核对 | 已有生命周期会话继续负责 |
| `adaptive.py` | Aimd、EwmaAimd、HolAgeAimd 在 PG profiler 可达；ResourceFeedbackController 在普通 records 可达 | 不按文件大小删除 |
| `request_scan.py` | AscendingRequestController 在 request feedback 可达，并复用原几何搜索／成对证据 | 先列现行和历史调用方，再决定是否归并 |
| 帧与候选计量 | 原 `_header_sizes` 缓存及已有重复计量修正保持 | 不重新实现同类缓存 |
| Git 与实机 | 每批 CPU 证据交独审，准确候选交唯一 Git 整合；GPU 会话独立使用服务与测试计划 | 本会话不推送或合并、不启动模型／Ray／GPU |

普通默认 R 解耦、完整关系与审计内存、真实库及模型资格均为后续工作，
不能用本次代码提取或 CPU 通过替代。多卡路由仍属待验证设计。
