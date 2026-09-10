# 查询计时、错误保留与执行推进修复

日期：2026-09-10。角色：内部工程验收。对应[当前工作包 A](../../../plans/data_organization_batching.md#当前实施执行修复与数据库查询套件2026-09-10)。
研究对象为 **PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。

当前状态：代码及受控验证完成；真实模型验证待单独确定新额度。此处没有新增真实模型请求，
没有重用旧容量实验预算，也没有登记新的性能结论。后续数据库查询、PG 总预算、组织控制、
共享查询和图像接入分别按工作包继续。

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

这是测量与基础执行修复，不是新的数据组织或公平调度算法。真实模型匹配对照、静态容量平台、
基础组织收益及多 Job 性能仍待分别验证；已有预算不会自动续用。
每个后续工作包独立准备代码、受控验证和运行清单，不以完成全部功能为首次真实验证的前提。
