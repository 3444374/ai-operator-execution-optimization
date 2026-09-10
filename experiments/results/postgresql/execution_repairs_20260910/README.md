# 查询计时、错误保留与执行推进修复

日期：2026-09-10。角色：内部工程验收。对应[当前工作包 A](../../../plans/data_organization_batching.md#当前实施执行修复与数据库查询套件2026-09-10)。
研究对象为 **PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。

当前状态：工作包 A 的代码、受控验证和匹配真实模型诊断已完成。第一次授权的运行因gateway启动
失败而以0模型POST停止，原失败保留；用户另行授权的新一轮完成18单元、8,448模型POST，423.47秒
内正常关闭模型和两套诊断PG，没有自动重试。后续数据库查询、PG总预算、组织、共享查询和图像
接入分别继续，不能从本报告推断其性能。

## 目的与设置

修复执行错误被清理覆盖、失败 JCT 包含清理等待、固定预留反压下的重复探测，以及全局动作预算
耗尽后仍有可派发任务却进入等待的问题。实验对照为受控旧 C pump 与修复路径；不是原生系统排名。

- PostgreSQL 18.3 独立软件副本、数据目录与 Unix socket；production gateway、v6、真实 tuple pump。
- 反压用 32 行已知 fixture、PG 窗口 16、核心结果预留 2 MiB、最多两项活跃请求；后端返回受控结果。
- Python 同时覆盖实际 socket worker、已有 Core/方法驱动回归、记录器与 observer；HTTP 集成只访问本地 fixture。
- 编译沿用先前资格的 `PG_CFLAGS='-O2 -Werror'`，不改变 PostgreSQL 18.3 自带警告集合。
- [运行源码指纹](raw/source-a.json)记录基线及逐文件 SHA；受控 GPU 输出、CPU 单元测试和真实模型结果不混用身份。

## 修复与接口

| 部分 | 行为 |
|---|---|
| 记录器 | 源错误在上下文退出前记录；保留首个异常与 SQLSTATE，独立记录 query/cleanup/recording error；同步、异步、取消与期限共用时点规则 |
| 时间字段 | 旧 execution schema 与 elapsed 保留；`semloom.query_timing.v3` 的 query JCT 终止于观测到的 EOF/错误，取消触发、清理结束及持久化分别记录 |
| 单元收尾 | 准备、执行、评价和持久化失败均保留现有证据；坏的单个摘要不阻止读取其他产物；资源没有权威记录时写 unknown |
| 连接 | 容量 runner 要求专用、空闲、autocommit 连接；通用 recorder 不提交或回滚调用者事务 |
| 观测 | 内容完整度与同步／缓冲写入独立配置；完整内容仅写私有文件；legacy direct qualification 明确是紧凑事件同步写入 |
| 接纳 | 单项不可能满足 Job 存储时永久拒绝，v6 返回错误；暂时反压保留类别；零接纳不自发制造 wake |
| PG pump | 一次反压后暂停本窗口的后续探测；仅在本流 receive 对应的结果责任释放后恢复，不消耗被拒任务序号 |
| 全局推进 | `CleanupReport` 兼容追加立即工作、最近期限和阻塞原因；有可派发／待清理工作继续有界推进，每轮仍处理控制与其他 Job；受阻时等待 |
| 时间线 | 保留 HTTP 开始／结束、Core terminal、release、submitted、offer 与 PG producer/消费时点；HTTP 结束本身不证明未知远端计算已停止 |

`has_immediate_work` 不由队列非空推导；READY 结果需要消费者领取，不算全局 tick 自己能完成的工作。
后端明确未接纳或策略暂时不能派发仍保留重试期限，避免忙循环。

## 受控证据与验收

[反压计数](raw/controlled-backpressure.json)来自实际 PostgreSQL 和 production gateway：

| 路径 | offer 次数 | 接纳任务 | 模型后端提交 | 相关责任未释放时重复探测 |
|---|---:|---:|---:|---:|
| 旧 C pump | 660 | 32 | 32 | 598 |
| 修复后 | 62 | 32 | 32 | 0 |

两条路径均返回相同的 32 行且最终资源归零。这里的后端提交是受控 coroutine，不是真实模型调用。
探测次数减少证明机制修复，不等于真实服务上的加速比例。

- 本地：实验工具 432 项、PG 静态 116 项通过；provider 51 项检查通过，其中两项 Linux 专用检查跳过。
- Linux：调度 369 项、provider 51 项、PG 静态 116 项通过；受影响实验工具 75 项通过，另有一个测试模块
  因独立调用缺少测试目录 import path 未加载，补齐路径后该模块 3 项通过，共覆盖 78 项。
- PostgreSQL：严格编译、回归 **1/1**、全部 15 个 TAP 文件 **2,043/2,043** 通过；包含旧 Filter/Map、
  query Job、权限、快照、取消、乱序、绑定和新增反压测试。
- 新增 opt-in 集成测试使用真实 PG 和本地 HTTP，覆盖 full＋buffered / compact＋synchronous 两种
  配置的 PG/direct 两臂、重复输入关联、实际请求期限、原始 SQL 错误及 PG 取消 SQLSTATE。
  最终3项通过；32个完整fixture请求加2个已开始后被客户端期限中止的请求，共34次本地HTTP。
  三次集成尝试分别为16、32、34次，全部保留于[尝试计数](raw/fixture-attempts.json)，真实模型调用均为0。
- 受控时钟验证第 2 秒执行失败、第 12 秒清理结束，并覆盖清理／写盘再次失败、准备／评价失败、
  零行、部分结果和消费中止；消费者失败不伪造完整查询 JCT。

日志见 [Linux 调度](raw/linux-scheduling.log.gz)、[provider](raw/linux-execution_provider.log.gz)、
[受影响工具](raw/linux-affected-experiments.log.gz)、[PG 静态](raw/linux-postgres.log.gz)、
[完整 TAP](raw/tap-all-a2.log.gz)、[回归](raw/regression.log.gz)。
[独立资源 runner](raw/linux-resource-runner.log.gz)补足前述测试目录 import path；
[最终PG/HTTP集成](raw/pg-recorder-integration-started-request.log.gz)明确断言期限触发前已有实际请求到达fixture。
不同测试组可能引用相同 helper，不把组内数字相加为独立实验样本数。

复跑入口：对 caller-owned PG 18.3 设置 `SEMLOOM_TEST_PG_DSN`、`SEMLOOM_TEST_PG_LOG` 和一个
全新私有 `SEMLOOM_TEST_ARTIFACT_ROOT`，执行
`PYTHONPATH=code python -m unittest tests.experiments.test_pg_query_execution_integration`。
PG 与 gateway 应由同一测试用户运行，以保持私有目录权限；调用者负责 cluster 生命周期。

## 历史解释与失败保留

[历史归档计数](raw/historical-offer-counts.json)逐流重新统计旧 producer 日志，只输出次数和配置单元名。
`bytes-result` 为 24,708 次 offer / 512 次接纳，即 48.2578125 倍。未公开原始行 ID、输入或预测。
旧低结果预算点还改变了核心输入与 PG 字节预算；旧 direct qualification 使用紧凑同步事件。
[原容量报告](../map_capacity_20260910/README.md)已补充解释，原成功数字和失败时间没有回写。

| 本次未通过的尝试 | 解释与处理 |
|---|---|
| 额外启用 `-Wextra` 的第一次构建 | 暴露旧代码 signedness/longjmp 警告；保留日志，随后使用先前资格的同一编译参数，未声称额外警告集合通过 |
| 旧 pump 新反压测试 | 两项断言失败，保留修复前反例和完整计数 |
| 首次复跑沿用数据目录 | TAP 拒绝覆盖旧数据；改用独立 `TESTDATADIR`，原目录保留 |
| 全套 TAP 沿用日志目录 | 绑定测试读入旧 trace；保留失败，独立 `TESTLOGDIR` 后完整通过 |
| Linux 全实验目录发现 10 项错误 | 部署副本只包含 code/deploy，旧工具所依赖的 Git 元数据和历史结果未复制；本地全目录通过，Linux 的本次受影响工具另行验证，不把该轮标为全通过 |
| 第一轮 PG runner fixture | root 创建的私有目录阻止 postgres 访问 socket；保留失败，以 postgres 身份在新私有目录执行，不扩大目录权限 |

这些失败及修复前测试日志均在 `raw/`；不能删除失败记录后把最后一次执行冒充唯一尝试。
HTTP fixture 和各 gateway 已关闭；独立 PG 暂留作紧接的数据库查询／预算受控验证，未启动模型服务。
整个任务结束时再停止保留的 PG 并恢复临时目录 ACL，单独登记清理结果。

## 对研究与后续工作的含义

这是测量与基础执行修复，不是新的数据组织或公平调度算法。下方真实模型匹配对照支持特定反压配置的修复效果；静态容量平台、
基础组织收益及多 Job 性能仍待分别验证；已有预算不会自动续用。
每个后续工作包独立准备代码、受控验证和运行清单，不以完成全部功能为首次真实验证的前提。

## 真实诊断的零请求启动失败与修复

[运行摘要](raw/real-attempt-stopped.json)保存额度、0模型POST、未领取预留、204.31秒总时长、
模型退出码0、两套PG停止及端口重新绑定检查。GPU恢复空闲，未使用强制终止，也未退还或续用旧预留。
模型文件、服务身份和共同测量工具的旧/新执行检查均已完成，但真实单元启动时的干净环境暴露了遗漏：
runner自身向`sys.path`添加源码目录，并不会自动传给在产物目录启动的gateway子进程。

修复在runner构建子进程环境时显式加入自己的绝对源码目录，并保留其他Python搜索路径。
这不是要求用户设置某个隐藏shell变量。新单元测试先复现缺少PYTHONPATH；实际PG/HTTP测试主动删除
调用者的该变量后，两臂/日志模式/期限/错误记录及相关测试37项通过。
证据：[首个单元失败](raw/real-warm-old.log.gz)、[gateway启动原因](raw/real-gateway-startup.log.gz)、
[修复后的受控检查](raw/launch-fix-controlled.log.gz)、[修复文件SHA](raw/launch-fix-source.json)。


## 同配置的真实模型修复前后诊断

来源为真实PostgreSQL/vLLM执行及私有完整原始记录。用户单独授权的新一轮编号为A2；
8,448次模型POST包括两组各128行预热和16组各512行测量，全部预留与实际出站一致，没有重试。
总耗时423.466秒，未达到20分钟上限。两套PG与模型退出码均0；模型未被强制终止，相关端口可重绑，
两张GPU均恢复1MiB占用，无计算进程。另一套受控PG仍供后续工作包使用，整个任务的ACL清理未结束。

模型为Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28`；vLLM0.25.1，
BF16、TP1、4096上下文、128服务序列、8192批token、利用率配置0.8、FCFS、chunked prefill、prefix cache。
每单元先确认一次prefix cache reset。任务固定原自然分布tuning前512行，temperature0、max_tokens64。
固定C32、核心输入128MiB、PG窗口64MiB、producer trace开启、compact/buffered、flush_rows64。
第一轮配置顺序为(32,32)、(64,32)、(32,64)、(64,64)，先旧后新；第二轮配置逆序，先新后旧。

旧执行为 `1dd6d060`，新执行为 `e42b43b8`；两者共用 `e42b43b8` 的测量目录。
因此旧臂称为“旧执行＋共同测量补丁”，不称未修改的原版整体复现。共同observer不等于事件数量相同：
新Core额外记录offer/released事件，开销保留在查询时间内。本次同时包含推进与反压修复，不能把耗时
差异全部归因于单一函数或扣掉日志耗时后制造净加速。

| L / 核心结果预留MiB | 旧执行JCT两次/s | 新执行JCT两次/s | 旧offer两次 | 新offer两次 |
|---|---|---|---|---|
| 32 / 32 | 10.877930 / 11.416208 | 11.196000 / 11.172879 | 512 / 512 | 512 / 512 |
| 64 / 32 | 17.051289 / 17.323184 | 10.072762 / 10.015239 | 23836 / 24042 | 862 / 858 |
| 32 / 64 | 11.341089 / 11.187800 | 11.305409 / 11.333312 | 512 / 512 | 512 / 512 |
| 64 / 64 | 10.129688 / 10.385281 | 11.659800 / 10.587566 | 512 / 512 | 512 / 512 |

每个测量单元均接纳/提交/实际POST512次，HTTP峰值32，输入/结果责任最终归零；producer绑定、
请求多重集合和结果关联独立复核。新执行的全部单元中，反压后未发生相关release即再次offer的次数为0。
旧执行未记录release/offer事件，因此该细项写“无法从旧时间线直接计算”，不能用新字段回填旧证据。
L64/结果32MiB时，新执行有效held峰值仍为32；L增加没有突破结果预留所能容纳的32项。

可支持的观察是：L64/结果32MiB下，过量探测从23,836/24,042降为862/858，两次JCT从约17秒降至约10秒。
其余配置均为512次offer，没有一致加速；L64/结果64MiB的新执行两次都较慢。
两次重复不足以给出稳定总体排名，也没有达到60秒稳态条件，不能登记强D0或数据组织算法收益。

512行测量的EM为81.0547%–81.6406%；temperature0下输出仍有少量差异，未筛除答错行，未更改阈值，
不声称质量逐行完全相同。保留逐单元EM/F1和结果哈希；这里只有短查询的功能、责任和机制比较。

机器可读的全部重复、资源峰值、时点、源文件指纹、原始归档哈希及独立复核见
[真实匹配诊断](raw/real-matched-diagnostic.json)。
[清理记录](raw/real-matched-cleanup.json)仅对应本轮服务；
[旧执行受控链路](raw/real-old-controlled.log.gz)、[新执行受控链路](raw/real-new-controlled.log.gz)
各3项通过，每臂34次fixture HTTP、0真实模型请求。
完整源输入、预测和原始事件保留在仓库外私有归档，Git只保存计数、指纹和脱敏记录。
