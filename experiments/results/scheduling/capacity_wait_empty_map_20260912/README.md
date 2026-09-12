# 容量等待与零任务 Map 复审修订

状态：本地修订与回归完成，服务器验证 **pending**。基于 `3a097d36`，保存于
`codex/capacity-wait-empty-map`；本次未合并 main。用户明确服务器尚未开机、暂不进行服务器测试。
没有连接服务器、启动数据库或模型，也没有新增模型 POST；不继承此前 1,152 次调用额度。

## 目的、范围与依据

本次处理 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化中的基础推进和验证问题。
A/B 共同完整性、原生行关联以及 D 活跃责任与原始提交顺序的旧缺口保持已修复。
本次没有重做这些组件，也不改变 C 的总字节留存、FIFO/已选组顺序、模型语义或资源归还规则。
实施决定见[当前计划](../../../plans/data_organization_batching.md)，
[代码身份](raw/source-identity.json)列出本次非文档文件的 SHA-256。

## Core 推进：复现、原因和修订

确定性时钟、真实 `SessionEngine`/`SchedulingSession`、受控 backend：C=2、W=10，
已运行 6-work，队列为 5-work、4-work。普通选择与组内选择、legacy 与 registered 四种组合，
在 1 ms 终态后都被原来 10 ms 重试期限挡住；该反例不是服务器耗时测量。
[原始失败](raw/red-capacity-corrected.log.gz)包含 4 个及时推进失败和 2 个重复容量探测失败。

[修复前后实际 Core 时间线](raw/capacity-timeline.json.gz)使用归档源码与当前源码分别运行：
旧版在1 ms时active work已为0，但retry仍为10 ms；新版在1 ms已派发后续5+4-work。
根因是同一个 `_retry_at` 同时表示容量不足和 backend/endpoint/credit 暂不接纳。
修订仅为容量拒绝保存一个有界的 TaskKey，使用现有 `can_dispatch` 重新判断该成员是否能提交；
容量未恢复不重跑任务选择/派发，恢复后立即具备推进资格。没有增加第二套计算责任账本。
后端拒绝、端点策略或任务选择暂缓仍使用重试期限，无关 wake 和终态不清除该期限。
未知接纳、取消与权威终态的资源处理保持原实现。

回归还覆盖部分释放仍不足、另一 Job 释放共享容量、一个动作只完成 poll 后仍正确报告立即工作，
以及容量恢复后选择器暂缓时建立自己的重试期限。测试中的 9 ms 差值不能乘以旧拒绝次数，
也不能据此解释旧 rows/work/length 的真实 JCT（查询总耗时）差异。

## 零任务审计

`verify_organization`/`replay_active_work` 接受 `expected_task_count`；查询评价器从经过
manifest 校验的原始输入和相同选择条件取得该数量，不从结果条数、描述事件或提交日志反推。
独立预期为零、实际无任务/请求/责任且没有开流事件时，允许没有 `job_drained`。
如果已观测开流，仍要求 drain；非空查询继续要求完整提交、权威终态和结算。
日志为空但没有独立零任务证明、预期非零或伪造活跃快照均拒绝。

本地测试贯穿 `evaluate_recording` → 查询评价器 → 组织审计；相同空结果在零行选择时通过，
改为非零行选择时拒绝。[非空历史重放](raw/nonempty-replay.json.gz)核对源文件 SHA，
21 份轨迹共 1,380 个任务的审核结果与归档值完全相同。它是离线重放，不是新 PG/模型实验。
三种组织模式的真实 PG 空选择测试已添加，服务器未运行，不能据本地 fixture 声称 lazy-open 集成已验证。

## 首行延迟：源码依据和待运行诊断

REL_18_3 的发送缓冲默认 8,192 字节，`internal_putbytes` 在缓冲满时刷出或直接发送足够大的数据。
因此小结果在连接缓冲中等待是具体候选解释；该源码检查没有定位此前 SemMap 时间线。
[固定版本源码](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/libpq/pqcomm.c#L1197-L1237)。
libpq 单行模式控制客户端收到结果后的交付，不保证服务端逐行刷新。
[PostgreSQL 18 文档](https://www.postgresql.org/docs/18/libpq-single-row-mode.html)。

[普通 SQL 诊断脚本](../../../../code/scripts/experiments/pg_result_buffer_probe.py)已准备：
128 行，每行 scalar function 睡眠 5 ms 后记录服务端时间；有效载荷按 8/128/128/8 字节顺序运行。
只改变返回字节量，使用同一流式客户端，保存每行标记/接收时间、实际 DataRow 字节估算、计划和全部值。
每条查询 statement timeout 为 15 秒，四次最多 60 秒查询时间；失败不重试，临时对象随独占连接关闭。
服务端只报告自身时间跨度，客户端只报告自身单调时钟，不相减不同机器绝对时钟。

已执行 `--describe` 和 Python 编译检查；**数据库执行未运行**。
普通 SQL 对照只观察标量表达式完成至客户端接收，仍不等于 Core 完成、PG 收到完成、SemMap 节点交付的分层测量。
本次不增加逐行 flush、不修改 PG 协议或顺序恢复，也不重新解释旧积压峰值 128 为节点留存行数。

## 本地验证与失败保留

| 检查 | 本轮实际结果 | 证据 |
|---|---|---|
| 定向推进/容量观察/组织审计/共同评价 | 27 项通过 | [日志](raw/focused-final.log.gz) |
| 调度目录 | 372 项发现，371 通过，1 个模块导入失败：本机缺 `pyarrow` | [原始输出](raw/scheduling-final.log.gz) |
| 实验工具目录 | 497 项，482 通过、15 项环境跳过 | [日志](raw/experiments-final.log.gz) |
| provider 目录 | 55 项，53 通过、2 项平台跳过 | [日志](raw/provider.log.gz) |
| PG Python 协议/合同 | 116 项通过；不是 PG 服务端/TAP | [日志](raw/pg-contract.log.gz) |
| 非空旧轨迹 | 21 份、1,380 任务，结果不变 | [重放结果](raw/nonempty-replay.json.gz) |
| PG 空查询/发送缓冲/模型 | 未运行；模型 POST 0 | 用户要求暂缓服务器测试 |

可复现的本地入口：

```bash
PYTHONPATH=code python3 -m unittest tests.scheduling.test_capacity_wait tests.scheduling.test_capacity_observation tests.experiments.test_organization_evaluation tests.experiments.test_query_validation -v
PYTHONPATH=code python3 -m unittest discover -s code/tests/scheduling -v
PYTHONPATH=code python3 -m unittest discover -s code/tests/experiments -v
PYTHONPATH=code python3 -m unittest discover -s code/tests/execution_provider -v
PYTHONPATH=code python3 -m unittest discover -s code/tests/postgres -v
PYTHONPATH=code python3 code/scripts/experiments/pg_result_buffer_probe.py --describe
```

测试发现阶段的旧 `pyarrow` 缺失没有通过安装或排除用例掩盖。[首次调度输出](raw/scheduling.log.gz)
和[首次实验输出](raw/experiments.log.gz)也保留。开发中曾误用 `immediate_work` 字段，
[首次输出](raw/red-capacity.log.gz)保留；改正为 `has_immediate_work` 后六个行为反例仍失败，随后由生产修复通过。
空审计新增参数前的失败见[日志](raw/red-empty.log.gz)；共同评价测试曾尝试重用已写评价的目录而被正确拒绝，
已改用两份独立 fixture 目录。重放脚本首次误写归档文件名，修正后才完成 21 份核对；这些均不是模型失败。

## 结论与下一步

源码与本地受控证据支持容量等待修订和零任务审计规则；当前不声称 Linux/PG 集成或性能已重新验证。
服务器恢复后先按 runtime 规则 preflight，再运行缺失的调度集成、PG 空选择与普通 SQL 缓冲诊断。
所有未来数据库调用仍由单独的显式执行启动；本次没有创建自动任务。

C 的价值仍是固定字节下的合法留存和推进；D 已证实 token 代理工作量可以节流，尚未证明紧预算提高性能。
正式比较前仍需：相同资源的交错重复、请求数 FIFO 对 token FIFO 的表征成本对照、宽松/生效预算下的
重排效果，以及完整诊断记录与低扰动观察的成本比较。不同实验臂拒绝事件数量不同，记录开销可能不同，
不从总耗时简单扣日志时间。原生 Ray/LOTUS 的真实质量和性能仍独立待验证；本次不推进 E/F。

[全部文件与摘要](raw/manifest.json)。本报告为内部工程验证记录，原始 C/D 预测和耗时未修改。
