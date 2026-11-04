# Movie 有限矩阵维护入口（CPU 候选）

普通运行与 Session 的配置解释由 [`movie_records_config.py`](movie_records_config.py) 共用，
其容量／执行声明直接读取 `records_capacity`／`records_profile`，预检不导入查询或模型 owner。
包括 Core profile、任务与 IPC 容量、应用批选项及 admission cache 身份。
它只调用既有类型与 resolver，不创建运行时、模型或控制器；原运行器中的五个私有导入名保留兼容。
配置到执行的职责及本轮差分检查见[重构记录](../../../experiments/results/diagnostics/records_config_refactor_20261103/README.md)。

单一入口为 [`run_movie_records_matrix.py`](../../scripts/experiments/run_movie_records_matrix.py)，
实现为 [`MovieMatrix`](movie_records_matrix.py)，启动与收尾 helper 为
[`movie_matrix_support.py`](movie_matrix_support.py)。本地候选已写入维护代码，已按内容寻址冻结稀疏源码；未部署或运行模型；
不能继承 r3 或旧 Native/Core/IMLane 的实机资格。

这次迁入的是 r3 的 owner/worker 编排，不是新的执行后端。`run_movie_session`、原 supervisor、
预算 ledger、各后端批处理、并发、背压、模型生命周期和完整 LOTUS 解释评价保持原实现。
沿用同一有限矩阵 schema；新建运行的查询数、调用额度和时间窗口来自 sealed matrix、scope 和输入计划。
原 e269 的 87-query/44280-call/10800s 模板保留在父包。本组合准备的是公共校准工程资格包：
2 个完整 Q3 目标查询（显式预热/测量各120行），最多6个扫描 owner、14个查询、1680调用上限，
execution_enabled=false、分配0。该显式上下文封包的缓存命中只执行目标240调用；潜在扫描预留不伪装实际模型调用。
新目录和新 budget ID 必须由新封包显式绑定，不能运行已花费的旧 packet 取得新额度。

正式封包检查还要求新封包源码树的`deploy/autodl/sembench_records/`提供
`source.lock.json`、`tasks.json`、`comparison.json`；本review候选不携带这些运行绑定文件。
普通records/Core工厂不依赖它们，formal/sealed检查缺配置会在模型前失败。
这些文件必须由运行owner在新的仓库外sealed源码副本中生成并锁定当前源码/输入/环境，不能复制已花费旧packet或据`--help`通过推断可执行。

## 命令与输入

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B code/scripts/experiments/run_movie_records_matrix.py check \
  --packet "$CURRENT_SEALED_PACKET" --packet-sha "$PACKET_SHA256" --output-dir "$NEW_PRIVATE_OUTPUT"
```

`run` 不带 `--execute-model` 同样只检查，零真实 claim。实际运行另加该 flag，仍由既有运行 owner
提供经过核验的解释器、CPU affinity、GPU 分配、离线环境和有限计划。本候选不连接 SSH、
寻找旧连接或自动恢复实验。调用者可通过正常外层传输执行同一 argv，不再运行嵌入 Python 的历史 SSH launcher。
`worker` 与 `audit` 是同一入口的内部动作；worker 的必填 index/context/output 参数在读 packet 前检查。
worker 使用 driver Python，CPU token audit 使用 model Python 和空 CUDA mask。owner 超时由现有
startup/query timeout 和查询数推导，服从总墙钟与 cleanup reserve；supervisor 的清理余量保留。

输入是显式、不可变、当前自足的 sealed packet，不是历史目录名。仍需 packet 中的
`q3_packet.py`、`q3_host.py`、`host_helpers.py`、`execution_policy.py`、`active_failure_evidence.py`、
`token_audit.py` 及其受锁传递依赖、sitecustomize、输入和参照。旧文件名保留作兼容；
维护入口只从给定 packet 导入，不搜索旧实验目录，也不导入整套旧 `q3_controller`。
封包缺 helper 或摘要不匹配会在导入/模型前失败。固定数据、模型、旧账本及原件的外部路径仍是
显式资产/证据绑定，不能把消除代码目录依赖理解成可删除这些数据。

运行时入口与 driver 必须属于 packet 绑定的同一源码目录，随后执行原完整 source.lock/资产检查；
不能让旧 source 的资格掩盖树外新代码。新运行文件需要新源码锁和 packet/review 摘要，旧锁没有更新或被冒用。

## 已统一的行为

- 控制端和 worker 共用同一个 context 构造及完整性校验，包含 r3 的 `lease_path`、摘要、IPC 私有目录、
  budget/runtime/资源必填字段；IMLane 专有项只对 IMLane 检查。可选 tokenizer fallback 保持。
- 控制器完整检查计划一次，worker 保留本组计划、原 sealed packet/资产/输入/lease 与实际逐行 token 检查。
- `clock.json`、性能采样关闭和进度 JSONL 的普通异常只警告，不覆盖业务结果；必要资源清理及 IPC 环境恢复先执行。
  权威结果、独立物理观察、before-release/campaign 证据、GPU 冲突和未知身份仍是必要门禁。
- 物理 finalizer 从实际 r3 archive 核同，只注入 host/packet 依赖；没有把物理退出等同 API close。
- 新建运行保留原独立预算；显式 continuation 保留原 snapshot、截止时间、skip 和已花调用，不重新 reserve，
  不重试旧 unknown。接续成功的状态仍为原 `completed_remaining83_original_matrix_incomplete`，不会宣称旧矩阵完整。
- per-query、session 和 group 的原结果/计时字段保留。新入口不生成旧 SSH relay 的 outer 指标；
  调用者另记传输及入口墙钟，不能把它与旧 SSH outer 时间混为同一口径。

## CPU 验证及边界

[`test_movie_records_matrix.py`](../../tests/experiments/test_movie_records_matrix.py) 对实际维护类和 helper
执行 36 个 CPU 用例，包含 30 个逐字段缺失子用例、五后端字段合同、连续 resident 的 engine identity、
接续不预留/不重试、故障记录与清理分离、源码绑定、可迁移封包及默认零执行。
两作者补丁在 r3 快照组合的另外 10 项配置回归通过，含原 sitecustomize 的无模型 CPU 子进程检查；
总计 46 个不同测试，复跑不相加。这是入口迁入初版的验证记录；其后 Core 查询方法复用和下面的配置增量
分别记录，不能再把整个当前候选称为与 `068f5248` 字节相同。新源码模型和物理资源资格须重新绑定。

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B -m unittest discover \
  -s code/tests/experiments -p test_movie_records_matrix.py -v
```

初次 fixture 缺失 backend、旧 mock 未接内部 keyword、macOS 临时路径符号链接比较失败均保留在本地交接证据，
修复没有放宽生产断言。没有运行 GPU/SSH、操作真实账本、改写旧失败或删除原件。

## 清理接续

正式入口经下一次完整封包检查并确认没有外部调用者后，旧 `execute_movie_q1/q2`、`execute_resident`、
`session_driver`、r1/r2/r3 的 `execute_matrix`、`stage_*`、`prepare_*` 可退出活动入口索引并归档原件；
不在本次直接删除。旧 `q3_controller` 在新入口中不再被导入，但要保留其历史回放副本。
原 `observe_existing_master` 不再是本入口依赖；其他运行/取回工具仍可能使用，不能据此直接删除。

当前 packet 的 host/policy/sampler/token/evidence/bootstrap helper 及旧数据/账本不在清理名单。
原日志/归档/总结/收据/失败和其恢复所需脚本继续保留，正在汇总的 r3 原件完全未改。


<a id="record-token-batch-entry"></a>
## 记录级异步 token 组批入口

正常 `run_lotus_records_movie.py check/run` 的
`semloom.movie_records_run.v1` 配置可选择原 `semloom-local-model-core-async`
backend，并增加以下字段。省略新字段仍走原工厂和默认路径；示例数值不代表校准结果：

```json
{
  "core_profile": {"mode": "async", "max_inflight": 128},
  "resources": {"prepare_batch_rows": 1, "model_batch_rows": 1, "max_active_batches": 128},
  "prepared_batching": {"policy": "token_budget", "max_rows": null, "token_budget": 17506},
  "batch_concurrency": 4
}
```

这是现有完整配置的片段；engine、generation、query_id、task及冻结输入仍须显式绑定。
B 是每批完整模板 prompt token 加各请求阶段输出上限；C_batch 是仍有未结算计算成员的应用传输批数；
R 是原工厂解析后的请求容量，同时仍限制resident行。没有每批行数帽，native `max_num_seqs`／
iteration token容量单独记录。默认B/C_batch固定，W关闭；不把原逐请求扫描C改名为批并发。
默认批模式不支持逐请求admission；admission cache、stage_timing、非rolling resident或受行帽限制的组合仍在预检拒绝。显式rolling resident见下文。

### SemLoom records 的 Core／IPC 容量


异步上下文的请求值与模型实际解析值分别记录；`max_model_len`省略／null在异步路径委托vLLM，
实际ready回执进入长度检查与运行中的缓存身份。离线校准缺实际回执时拒绝，不以未知值代替无限。
离线省略上下文时，先执行首个已预留完整样本，从其实际owner取得解析值；缓存未命中时复用该样本，
不重复执行。缓存命中仍保留这次确认的调用、未知回执与时间，不称为零模型调用的查表。
每个扫描／目标owner在生成前再核对自身回执；不同上下文或模型不共用身份，失败不回退4096。
`model_context_confirmation`记录确认成本，`frontend_total_wall_s`包含它一次；公开selector自身墙钟从确认后开始，
不能忽略确认成本或与外层时间重复相加。显式上下文仍沿用原零扫描命中流程。
接口、同步参照兼容及剩余项见[上下文说明](../execution_provider/adapters/README.md#records-model-context)。

普通 Movie 配置和 `semloom-core` Session plan 可在根部声明 `core_capacity`，
异步模式另可声明部分 `async_limits`；其他 baseline 不接收这两个字段。例如：

```json
{
  "core_capacity": {"item_input_bytes": 4194304, "item_result_bytes": 2097152},
  "async_limits": {"feedback_interval_s": 0.1}
}
```

`core_capacity`复用`CoreRecordsCapacity`，上述两个字段是单任务完整输入／结果的字节额度。
普通 async Filter 与 rolling token 工厂在未给完整 `budget` 时，按已有 IPC 总字节声明和 Core 单项
形成独立存储额度，静态 C 与硬请求 R 分开。IPC 省略项使用 `AsyncEngineLimits` 的有限缓冲声明，
请求／命令许可仍来自 R，结果槽和输入／结果字节各用自己的声明；显式字段原样保留。
Core 总量至少容纳自己的一个单项，不为此扩大较小的显式 IPC 总量或单项。
Method 驻留按单阶段完整 continuation 与输入／结果的整项字节容量推导；任务／行数保护位于字节可容纳数之上。
这不代表实测 RSS、空闲内存或模型容量。R 默认仍是调用方声明值，没有自动硬件额度探测。
IPC 的最小帧、最终编码和未知责任保持；源、提示、生成规则和 B 不变。

普通 Movie、session、工厂和离线校准复用 `resolve_records_runtime`；
`check` 给出有效 Core budget 和 `resolved_async_limits_before_feedback`，统计采样间隔仍由反馈接线配置。
新默认校准使用 `records_capacity_policy_v2` 绑定真实解析结果，C 扫描不重算存储或 IPC；
显式计数可独立小于 R，不再因为不等于 C 而拒绝。引擎每轮序列参数与外层请求候选分开。
不同有效资源值隔离缓存，等效数值表示归一；新身份不复用旧 R 派生结果。
显式历史策略及具名参数沿用旧分配／缓存身份；同步、非 rolling 整批和低层直接构造仍待分批迁移。
当前只有 CPU 接线检查，实际内存、模型和完整查询性能未验证。
[CPU配置与边界检查](../../tests/experiments/test_records_capacity_config.py)覆盖部分配置、独立字节限额、
3MiB输入／结果、unknown预留、普通／常驻入口和查询账本重置；未新增真实Ray、模型或GPU运行。
缓存等价／隔离和扫描约束见[容量身份测试](../../tests/experiments/test_core_capacity_identity.py)。

<a id="显式独立资源预算默认关闭本地cpu候选"></a>
#### 独立资源预算与显式覆盖（本地 CPU 候选）

`core_capacity` 可提供完整 `budget` 显式覆盖；普通工厂的省略／null 默认已按上方规则生成独立额度。
预算须来自部署明确声明的有限额度，不能由本次输入N、历史最佳并发或任意大数代替。
当前不自动发现可用内存；显式 budget 仍须给全六项。以下是字段和单位，表中没有通用推荐值：

| `core_capacity.budget`字段 | 约束对象 |
|---|---|
| `held_tasks` | Core排队、运行及交付租约的任务槽；保留有限对象数量保护 |
| `input_bytes` | Core已接纳的完整编码输入字节总量 |
| `result_bytes` | Core在执行前预留、交付后至消费释放的结果上界总量 |
| `active_requests` | owner硬请求槽；与性能窗口C分开 |
| `method_runs` | 方法驱动器驻留行槽；与C及Core任务槽分开 |
| `method_bytes` | 原`MethodBudgetPool`的序列化输入／状态／结果保守预留总量 |

六项均须为正整数；输入／结果总量须容纳其Core单项额度，方法字节须容纳完整continuation上界。
已有完整独立`budget`时，`async_limits`可以省略或只覆盖部分字段：请求／命令槽取`active_requests`，
结果槽取`held_tasks`，输入／结果总字节取对应预算；控制槽继续原协议预留公式，以`active_requests`为参数。
所有数值仍须有限；显式字段优先，完整显式对象保持原值，可以少于Core硬请求槽。
IPC单项继承Core单项或显式覆盖，原最小帧及编码检查保留，不会为匹配C而扩大额度。
Core与IPC仍分别核对和持有自己的表示，两层使用相同默认数值不合并记账，也不表示实测进程内存。
上述 IPC 缺省推导属于调用方给出完整 budget 的路径；普通省略预算使用前述独立默认规则。

普通Movie的`core_profile.max_inflight`／Session的`profile.max_inflight`只控制新请求准入；
固定模式须显式声明C，rolling资源反馈仍从原冷起点1出发。C变化不重算存储、状态或IPC预算，
缩容只停新准入，不取消已借用请求。旧`resources.max_active_batches`保留在声明元数据中，
在此显式路径由`budget.active_requests`提供owner硬额度，不再借旧R决定全部容量。
token B继续只组织提交；驻留已满／字节不足保留单个pending输入并沿原advance／wake等待，消费释放后重试。

当前范围为普通async Filter及rolling token请求，可沿正常Movie `check/run`、isolated／resident Session进入。
sync、fused和非rolling的批槽组合不接此预算候选。合法后继与Extract关系屏障保持，完整查询审计／关系仍可能materialize。
直接构造`SemLoomRecordsCore`时，新预算必须配已有async actor owner，禁止自动或显式采用同步owner；
完整C／IPC配置资格仍由上述工厂和入口校验。普通owner跨查询保留硬容量，首查询C不锁定后续查询窗口。
Core输入与IPC编码体积可实计；结果及方法状态是保守预留，不代表Python／Ray对象、socket缓冲或RSS实测。
unknown、失联、取消请求及未消费结果不能按空闲释放；只有原终态／消费ACK／生产者退出证明结清相应责任。
新预算及token组批／rolling／切包消融路径进入现有校准身份；C扫描不改变该身份，
预算或组织方式变化不得复用旧选择。token配置先按已有`PreparedBatching`校验和补齐默认字段，
省略`max_rows`与显式`null`等有效配置共用同一身份。
[配置／容量边界CPU检查](../../tests/experiments/test_records_resource_budget.py)和
[CPU子进程IPC检查](../../tests/execution_provider/test_lotus_resource_budget_runtime.py)不授予真实模型、内存容量或性能资格。

### 固定 rolling 的 B 切包消融（仅实验）

固定 rolling 配置在普通 Movie `check/run` 或 Session plan 根部可显式加入
`"experimental_skip_token_packing": true`。仍提供原来合法的正值 B 和
`max_rows:null`，同时 `rolling_refill:true`；此模式不允许
`core_profile.admission`、batch_feedback 或批数并发。旧预算（`core_capacity.budget`省略／null）
固定 C_request=R；[显式独立资源预算](#显式独立资源预算默认关闭本地cpu候选)允许固定 C_request<R，
硬请求额度 R 由 `budget.active_requests` 声明，IPC 另按其有限额度背压。
只旁路 token 切包，仍按当前合法 ready、兼容性、C/R 空位及 RPC/IPC 字节硬限提交，
ready 只有一条就发一条，不等待凑满或上一包全部结束。成员/驻留、输入/结果、单请求 context
及引擎容量保护不变。R 与 native max_num_seqs 独立，后者不是应用批大小。

默认省略/false 不变，fused 和其他 token 策略保持原状。B 与旁路标记均进入配置/实际报告；
不装动态 B 或动态 C 控制器。该选项会改变包成员，须独立于成员等价的降开销补丁评审。
当前仅支持[CPU 机制回放](../execution_provider/adapters/README.md#fixed-rolling-token-packing-ablation-cpu-experiment-only)，
不是新 SemBench 成绩；真实模型的完整查询时间、gold、输出变化和引擎供给/排队仍待验证。

### 请求级动态 rolling 补位（本地CPU候选）

在已声明B及硬容量R的完整配置中，移除`batch_concurrency`和`batch_feedback`，加入：

```json
{
  "rolling_refill": true,
  "core_profile": {"mode": "async", "admission": {"kind": "resource_feedback"}}
}
```

该入口使用 `ResourceRequestConfig` 与已审请求策略，从一开始，不支持跨查询 hint 或磁盘 admission cache。
实际请求许可来自当前 Core／IPC／结果预留，不把 R 当作实测显存容量；原生样本绑定同一实例及 Core session。
采样间隔、窗口、样本年龄与三窗比较分别保留，旧 `search_policy`、KV／busy 百分比选项在当前入口拒绝。
新鲜原生反馈和可选的 actor 分配设备／CPU 读数只支持可撤回试探，不构成性能收益或硬件容量证明。
缺失的独立内存保护或原生拒绝计数不填零；要求尚未接入的独立内存来源时配置拒绝。
B 固定，C_batch 为 null；已接纳请求、合法后继、未消费结果、unknown 与 ACK 继续由原组件结算。
缩小 C 只停未来选择，不取消已借用成员。初始 C、B、硬 R 与统计回执分别记录。

显式 `legacy_resource_feedback` 或直接传旧 typed 选项才保留历史 ascending／reference 与条件续扫；
其原数值及成功查询后确认 hint 的规则保持。同步／native baseline 和非 rolling 批槽策略不借用新请求策略。
[当前配置与旧策略说明](../scheduling/runtime/calibration.README.md#resource-request-public-current)分别说明有效选项；
本批没有真实 Ray／模型／GPU 或动态 B、准备交错组合的资格。

另一种与rolling互斥的模式，在原批配置中显式加入以下片段，选择固定B、从一批起步的供给反馈候选：

```json
{
  "batch_concurrency": 1,
  "batch_feedback": {"max_batches": 4}
}
```

`max_batches`须显式给出且不大于R，是运行容量护栏，不是已测最优值；上例只演示字段。
省略`batch_feedback`或设null保持固定对照，不安装采样器。可选`options`复用
`SupplyFeedbackConfig`／`ResourceFeedbackConfig`字段；部分覆盖继承供给策略默认的computed工作量、
local_pairs比较和geometric探索。`check`输出解析后配置且不构造owner。
供给策略默认`exploration_s=null`、`max_changes=null`、`max_candidate_attempts=null`、`min_remaining=0`，
不继承旧“两次候选”或10秒／64待办的截断；有限预算只在显式配置旧预算规则时启用。容量上限仍须保留。
策略读取实际Core就绪需求、确认完成工作及同owner/session原生running/waiting/KV／抢占，
只改变C_batch，B与R／IPC字节责任保持独立。缺失／陈旧信息不表示空闲，配置碰顶不表示性能平台。
反馈间断使本轮比较失效并回退，只恢复中断候选的重试资格，原尝试计数／探索成本与显式预算保留；不读gold或全表token reference选参。
CPU/GPU利用率、物理可用内存、原生容量readback及动态B仍未接入。

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B code/scripts/experiments/run_lotus_records_movie.py check \
  --source-manifest "$SOURCE_MANIFEST" --sembench-checkout "$SEMBENCH_CHECKOUT" --config "$QUERY_CONFIG"
PYTHONPATH=code "$DRIVER_PYTHON" -B code/scripts/experiments/run_lotus_records_movie.py run \
  --source-manifest "$SOURCE_MANIFEST" --sembench-checkout "$SEMBENCH_CHECKOUT" --config "$QUERY_CONFIG" \
  --output-dir "$NEW_PRIVATE_OUTPUT" --execute-model
```

`check`不构造owner或请求模型。`run`沿用原监督worker；正常会话／sealed matrix计划也可增加相同组批及可选反馈字段，
其`profile`使用上例`core_profile`的值，且`backend="semloom-core"`、`lifecycle="isolated"`。
`rolling_refill=true`的固定RF／动态RD还可显式选择`lifecycle="resident"`：沿用原Session schema，在查询计时前等待模型ready，健康EOF只结算本查询，最终Session close才关闭模型。
RF保持配置的固定C（旧预算模式为R）；默认ascending RD在兼容owner内续用已显式确认的健康档位／搜索进度，重新验证当前条件后保持或局部续探；
没有合格提示才从1起步，显式reference仍逐查询重置。提示须在完整业务结果解释／投影成功后确认，不是通用跨任务学习。
批槽模式仍不支持resident。条件失效、供给范围扩展和native间断的转换见上述唯一策略正文。
仍由原owner提供lease、调用额度、完整source.lock／packet和物理退出检查；新入口须重新绑定这些资产，
不能把旧packet或旧树模型证据直接套用到新源码。isolated中实际发生的启动／关闭计入原查询；resident另保留真实ready、首次／后续query及最终关闭时钟，不能由冷查询减估计启动成本冒充。
兼容模型／生成／资源固定，逐查询输入、账本、结果和额度独立；失败、取消或排空未确认禁止继续复用。接口及本地CPU资格见[rolling resident说明](../execution_provider/adapters/README.md#resident-rolling-token-owner)，此新组合尚无实机性能资格。

MovieTask／source manifest继续拥有Q1/Q2/Q3提示、普通谓词、完整Filter覆盖、head5／COUNT及原评价器。
执行工厂把普通Filter声明接到已实现的记录级链，保留链调用ID与普通ledger ID的显式一对一映射。
正常关闭回执含`record_batching`、`async_token_batches`及`transport_batches[].receipt_members`，记录真实成员、
逐成员确认／状态／native结束时刻，成功时核对B/C_batch/R、组内索引和RPC批数。
资源责任仍逐请求结算；unknown不因关闭而改成成功或零用量。普通账本适配在已有完整链遍历后交付，
不会以该账本交付时间冒充逐请求延迟；完整输入及审计关系的内存保留也不宣称受R约束。

[CPU入口测试](../../tests/experiments/test_movie_async_token_entry.py)执行正常CLI的Q1/Q2/Q3、原isolated driver、
零模型检查、空输入和unknown失败会计，使用真实LOTUS方法与CPU子进程IPC，Ray和上游评价为显式fixture。
准确`41ce4c24`已完成真实Ray/模型功能验收及Movie Q1/Q2/Q3矩阵独审；默认仍关闭，未证明完整冷查询加速。
Q1严格原生输出等价未通过，Q3共同COUNT=7、gold=14。测试条件、完成范围与未覆盖项见
[实现状态](../../INFRA_STATUS.md#async-record-token-batches)和[PR84公开摘要](https://github.com/3444374/ai-operator-execution-optimization/pull/84)，不继承为后续修复或其它组合的资格。
新增可选反馈配置透传／默认关闭同样由 CPU 入口覆盖；本组合的反馈与观测增量待准确新树实机验收，
不继承上述 `41ce4c24` 资格。

## Core 查询方法与模型复用（本地 CPU 增量）

Core 使用原 create_records_core 和模型 owner；evaluate(source, query_id=..., method=None)
现在可以逐查询传入同一模型名的 FilterMethod。省略 method 保留构造时默认方法。
prepare actor 接收该查询冻结的方法，在其原串行方法槽内使用轻量 formatter；默认 formatter 与查询覆盖值分开，
所以 Q1→Q3→Q2→Q3 不会把 Q3 指令残留到下一次默认调用。只缓存一个最近覆盖 formatter，不缓存答案或模型结果。
模型 actor / AsyncLLM、采样参数、batch1、显式 async4、背压、结果/租约/取消责任沿用原实现。

同一 Core 会话允许不同指令；模型、tokenizer、模板及构造时 generation 配置仍由同一 owner 固定，
换模型名拒绝。新查询须等待前一查询完成并结清，执行协程捕获完整 session ID 与查询方法，拒绝迟到旧 session 提交。
Native/Ray/Daft/IMLane 的原生生命周期和调度未修改。

维护封包仅将 Core 原有按指令拆分的 resident owner 合并，查询顺序、87 条查询、44280 调用上限和
全部其他后端计划不变；新的加载/关闭次数与时间必须在新版本实测，不能改写旧结果。
当前验证是原 LOTUS formatter / SessionEngine / 有界异步桥及真实 CPU 子进程，加明确 Ray/Daft 调度替身。
7a/独立私有版本的跨指令实测记录单独保留。本公共校准组合须重新绑定实际 token/template/decode、
配置和物理关闭资格，没有继承该实测，也未测得新增性能收益。

## 运行配置贯通（独立 CPU 增量）

- task / input profile 仍由 MovieTask 和 source manifest 选择。Q1/Q2/Q3 的原提示、普通谓词、
  head5/COUNT 和评价是任务语义；120 是当前输入筛选结果，不是所有 Q2/Q3 的运行常数。
- Core `profile.mode` / `max_inflight` 和 `resources.cpu_workers` 沿用现有 factory/actor 通路。
  `max_inflight="auto"` 只取声明的 Core 请求容量，与原生引擎序列配置分开，不是硬件自动调优。
  prepare 仍每 actor 1 CPU；模型 actor 1 CPU / 1 GPU，Core 每次准备/模型调用仍为 1 条。
  当前协议明确只支持 `gpu_workers=1` / TP1，0/2 会拒绝，未添加多卡能力。
- 新建 campaign 的额度、查询数、launch/cleanup 时间由现有 matrix/scope 一致绑定；每 query 的
  预留量来自 source preflight 的 `expected_calls`。旧 r3 continuation 的固定数值是原已花费账本
  的回放合同，仍单独校验，不用于新运行，也不退款或重试 unknown。
- context 使用现有 cfg 的 budget、GPU idle 阈值、IPC prefix，以及 comparison 的 CPU affinity /
  RSS / SHM 上限。scratch 写入本轮输出的 runtime 子目录，不写不可变输入封包。
- EngineSpec 的模型/解释器/tokenizer/template 和 generation 直接传入已有 owner。
  token 审计按同一 owner 的确切 query 序列选择计划，使用该 generation，校验报告配置一致；
  tokenizer 路径为空时回退模型路径，模板文件或内联模板沿用引擎语义。

换模型必须重新绑定本地权重、tokenizer、模板、版本、expected token reference 和源码/封包，
不能只修改路径后继承旧资格。同一 resident owner 中更换模型/generation 不在此增量范围。
CPU 非默认配置与旧路径测试见 [`test_movie_runtime_configuration.py`](../../tests/experiments/test_movie_runtime_configuration.py)。
这些配置推导未改变 baseline 的原生调度、批次或背压，未测得新的 GPU 性能收益。

## 公共运行校准与唯一维护入口

扫描、选择、签名和缓存属于
[`src.scheduling.runtime.calibration`](../scheduling/runtime/calibration.py)，只依赖标准库。
普通非 Movie 调用与本入口共用 `calibrate` / `run_calibrated`；未匹配的缓存通过真实 workload
callback 有界测量、选择并执行，命中跳过测量。identity 加候选集合、参数顺序、重复数和比例共同决定缓存键。
旧 Core 工厂的显式固定 profile 保留；不在 import/普通构造时暗自加载模型，也不把声明容量 auto 当成实测选择。

普通调用可直接使用公共 API，不需要 experiments、Movie 或 gold：

```python
from src.scheduling.runtime.calibration import Measurement, ScanBudget, run_calibrated

run_calibrated(identity, candidates, execute,
    cache_dir=cache_dir, measure=measure,
    budget=ScanBudget(timeout_s=scan_seconds, max_work=scan_work_limit),
    work_per_sample=complete_work, repetitions=3)
```

调用方的 `measure(configuration, repeat_index=..., deadline_monotonic=..., work_limit=...)`
执行相同完整工作负载，并在必要资源结束后返回 Measurement；`execute(configuration)` 实际应用配置。
不确认结束须抛错。候选只是容量参数，模型/生成/语义放入固定 identity，不能按 gold 或其他系统成绩选参。

维护入口通过 owner-group 可选 `calibration` 字段启用同一能力：

```json
{"cache_dir":"/private/artifacts/runtime-calibration-cache",
 "candidates":[{"max_inflight":1,"cpu_workers":1},{"max_inflight":4,"cpu_workers":2}],
 "repetitions":3,"near_peak_ratio":0.97,"timeout_s":1500}
```

没有该字段就是原固定配置。Core 扫描目前只接 async、batch1；固定 sync 在途4仍只提供准备/排队流水，
同步模型 actor 并发1，不是同步模型 batch4。模型重入锁、原生调度、批处理和背压均未修改。

薄 Movie adapter只负责输入/计划和完整查询回执转换。扫描和目标均走原 run_group/session/计费/supervisor；
唯一 controller 调用公共 run_calibrated，不再实现选择/缓存/候选测量循环。
所有扫描身份、调用数和资源 slot 预先从有限配置展开并计入新账本；sample ID 使用账本合法字符和查询位置，
不放宽旧 CellBudget 单元命名协议。缓存命中零扫描 claim，既有已花/unknown 账本不退款或重发。

所选 C/P 先物化最终 plan，再计算摘要和创建 owner。context 同时绑定原声明、最终 plan 与不可变选择收据副本；
worker 在 claim 前复核，token 审计读取同一有效计划，支持扫描前缀 ID。
只有实际 Core mode/C 与已测选择一致才标 performance_tuned；固定/容量 auto 保持 False。
新查询先清旧报告，后置来源检查错误仍保留本查询已知 usage。

`resolution_wall_s`、当前 scan_cost、原 calibration_cost、target_wall_s、公共 total_wall_s 分别保存；
前端观测/计划准备与总 group 时间、完整 campaign 墙钟另报，不隐藏首次扫描成本或重复累加 session 时间。
目标查询仍拆启动、完整查询、eval、关闭。公共选择只是有限已测有效候选中的近峰值，不是全局最优承诺。

身份复用现有机器/配额观测、目标设备、完整模型/Tokenizer/模板记录、最终源码和依赖版本。
新增 model-preflight 收据记录 tokenizer 资产内容摘要；旧收据不能冒充新源码资格。
新 source/packet 同级，IPC base 保留完整八位十六进制语义，执行必须同时使用 commands 的 env 和 argv。
本组合没有叠加私有11f补丁、H1/H4或语义策略。


## 递增搜索到平台或明确限制（独立 CPU 候选）

可选 `calibration.search` 由同一公共运行时执行，无新实验选择器。
设置一个起点及具名容量/资源边界后，按倍数递增并以重复观测确认局部平台；
命中容量、资源或预算上限分别记录，边界最佳不等于平台。
规则、失败行为、缓存隔离和 Core 单 owner 查询额度见
[`calibration.README.md`](../scheduling/runtime/calibration.README.md)。
维护 CLI 现有隔离 sample 生命周期保持不变；普通公共回调可用查询额度复用一个模型 owner。
模型 `max_num_seqs` 的变化仍需新 owner、身份和缓存。该增量仅 CPU 验证，
未部署、未改全局默认，未继承旧候选的 GPU 资格或性能结果。


### 静态与 Core 容量预检

static session 的 CPU 检查现要求已有有限请求额度；显式 `R=None` 不能通过该旧静态路径。
`_MovieDriver.start` 也在创建私有目录、读取 Ray 状态和初始化 runtime 前验证 static 额度，
并提前调用同一个纯 Core 配置解析器。这样即使绕过 `check_plan`，未解析或非法容量也不会
先启动 Ray。没有新增计数上限或硬件探测，普通 async／rolling 的 None 加完整独立预算仍可用，
Core、Method 和显式 IPC 值保持，见[预检检查](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/static_capacity_preflight_checks.json)。

旧 geometric 的完整资格仍未通过。公共观测组件把 regular poll 的空返回当不可用，并保留
已见序号；独审已有确定性回放，新增实际 CPU 跟踪也观察到 pending RPC 的空返回。
不能把它解释成无害抖动，也不能由不同控制器推断所有新路径均不受影响。
[交接调查](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/geometric_source_handoff_checks.json)
记录实际调用点、语义核对和未证明的原失败唯一原因；本次没有改 provider 或控制器阈值。


### 直接 session 启动的组批校验

`_MovieDriver.start()` 也在私有目录与 Ray 初始化前调用现有 `resolve_record_batching`，
并复用同一次选项解析供原工厂使用。非法 `batch_concurrency`（含布尔值）、rolling 与批并发
冲突、缺少 `prepared_batching` 的批选项会提前拒绝；合法固定批仍是原来的延后模型启动，
rolling 完整预算与显式容量的行为保持。
[CPU 摘要](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/session_batching_preflight_checks.json)
分别保存正常预检、直接启动及 inert Ray 调用证据，不表示所有未验证输入都已提前检查。


<a id="record-preparation-mode"></a>
### Movie 准备模式的显式选择

普通 `run_lotus_records_movie.py check/run` 配置和 `semloom-core` session plan 可增加一个字段：

```json
{"preparation_mode": "event_driven"}
```

这是已有完整配置的补充，不是一份可独立执行的计划。取值为 serial、interleaved 或 event_driven；
省略等同 serial，保持现有配置和校准身份。interleaved 使用既有准备／派发交错；event_driven
还使用既有完成与 RPC 可用通知。它们映射到 SDK 的两个既有选项，不创建第二执行器或控制器。

这两个显式模式目前只支持固定 rolling token 请求：保留正数 B、token_budget、无单批行帽、
async 与 rolling_refill=true；不与逐请求／批反馈、其他 backend 或 stage_timing 混用。
原生 baseline 参数和调度仍由各 baseline 负责，公共入口没有因此支持任意链或 DAG。
同一字段适用于隔离和 resident 查询；resident 仍先取得同一模型 ready，再逐查询完成并在 session 结束时关闭。
预检和实际 record_batching 回执记录有效选项，模式变化会改变校准身份；None 加完整独立预算仍使用有限额度。

准备模式不会承诺错误查询零提交：原始 source 预检不预先构造所有未来提示。后面的提示超长或
结果处理报错时，前面合法请求可能已经完成并产生用量；失败查询保留这些结果和用量，不能标为完成。
不同准备顺序下的合法前缀可能不同，不能声称严格错误副作用等价。已接受但结果不确定的请求仍由原管理对象持有。

本批只有明确替身的 CPU CLI worker、连续查询及回归检查，模型、Ray RPC 和外部资格读取使用原 fixture；
不提供真实模型、GPU 或查询加速结论。原 geometric 不稳定与负计时记录继续保留，
详细来源见[接线检查](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/preparation_cli_checks.json)。


<a id="record-service-payload"></a>
### 普通 async 的服务载荷声明

普通 Movie Core 配置和 semloom-core session plan 可以显式给 service_payload_bytes 正整数字节。
这是完整配置上的可选字段；数值由调用方服务实际配置提供，不从硬件或历史实验猜测。
resources 可为空对象，prepare_batch_rows／model_batch_rows 此时为 1、max_active_batches 为 None；
core_capacity 只描述单项字节及原有工作设置，不另提供 budget。预检复用工厂的纯解析，
check 不领取字节或创建 owner，实际计划核对及领取发生在 Ray 初始化前。

此路径只支持普通 async Filter；prepared_batching、rolling_refill、批反馈、准备交错、显式 IPC、
admission_cache、legacy profile 和其他 backend 会在 CPU 预检拒绝。它不改变未配置服务字节的旧入口。
R／结果槽按服务已给字节和原 IPC 完整预留计算；未给 admission 时沿用静态许可，
显式选择现代逐请求反馈时软窗口仍从一开始。不能把该计数称为实测模型并发。
当前服务声明不要求手填 R 或结果槽，但服务总字节仍须明确，不提供自动 RAM 预留或 GPU 探测。

普通 CLI worker 使用一个私有池。session 的 resident／isolated 查询共享同一进程池；
调用方 context 若已给 _records_payload_pool，须为同一个真实池且总额一致，session 不替调用方关闭它。
若外来池已占用而无法领取完整预检计划，执行在目录创建及 Ray 初始化前拒绝，不能静默缩小并发。
返回报告记录真实分配计划、关闭及池状态。session 的最终池记录存于独立私有文件，返回对象提供其摘要，
先前 session.json 时间线不被倒写。最终记录报错时异常仍带完整 session_report 和原池／driver 句柄。

晚到回执先合并到原查询用量，再将完整保留结果及账目存入内容命名私有文件并同步文件及父目录，
最后接受同一回执快照。存储、接受或清理不确定时继续持有字节；成功、失败和 unknown 不互相改写。
同进程 retry_cleanup 仅操作原 owner，使用过 Ray 后须核对原 module／job／node；原 runtime 结束或被换时拒绝远端清理。
入口在初始化后立即绑定同一已验证身份，Core 在 actor 启动及清理前再次核对；启动清理报错只附注到原异常。
failed-ready 实例只有保留启动前已验证身份时才能在原连接重试清理，不能从后来出现的连接补认。
未决计算／观测 RPC 使原 Ray 连接继续保留，关闭回执为 unknown；原 RPC 返回后可在同一 driver 上再次关闭。
字节已归还而 Ray 关闭报错时也保留该 driver，避免把两种责任重新绑在同一计数上。
普通 worker 的异常另带 records_runtime 和 records_finish_runtime()，供同进程在原 owner 清理完成后结束原连接。
保留连接时不发 runtime_closed 事件；CLI 私有 worker 退出后这些 Python 句柄不可恢复。
只剩本地结果转交时可在 Ray shutdown 后重试保存。socketpair、Future 或进程 PID 不提供跨进程重连，
不支持从快照重建空池或重启后自动回收。真实模型及 GPU 检查仍由专用执行会话完成。
详见[实际 CPU 记录](../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-entry-recovery)。
