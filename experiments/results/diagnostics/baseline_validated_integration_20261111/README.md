# 原生 baseline 隔离组合的 CPU 验证

## 来源和范围

运行标识为 `baseline-validated-integration-20261111`，分支为
`integration/baseline-validated-20261111`，尚未合入 main 或推送。
以 `2e2616a54b976ffc82e05bba0fa203c4bc9a17f2` 合并
`6a31cc38f3476873287bd07985eaa1919d2537b6`，合并提交为
`8da079780d4941e0c942c06067d7a7d598bc85d9`；双方共同祖先为 `73ee255e`。
`909f842c`、`1c38e14c` 和 `73ee255e` 已在祖先链内，没有重复挑选。
只有实现状态文档被双方修改，Git 自动组合，无生产源码冲突。

最终 CPU 测试源码为 `12124551e0e6e5a1839d8f9952742e30b9a7acfb`，
code tree 为 `9ab487e37efcd0c57ee7a6ee5240927bbd7fe5a8`。
该提交相对合并树只修复五个测试/夹具文件；之后的状态同步只改 Markdown。
全部生产执行文件分别保持两个冻结父提交中的原字节，不增加调度或模型改动。
原 worktree、包、成功/失败记录及 main 既有脏文档不迁移、不删除。

## 实际 CPU 结果

预先固定 34 个受影响模块，去重后实际 **340 项：326 通过、14 跳过、0 失败、0 错误**。
保留已有 driver venv、Daft SDK、原 SemBench、IMLane 源及 tokenizer，离线运行，不安装依赖。
14 个跳过项均因本地缺少准确 SemBench 的 Animals Q1 SQL Git blob，禁止 lazy fetch；
不把它们计作通过。原 MMQA sf25 和完整 Medical 数据已绑定，相应检查实际执行。

首轮失败原件保留，修复没有改变生产合同：

- Q9 接线增加明确任务分派后，旧 Kalypso 生命周期/时钟夹具仍给空任务或 `fixture`。
  测试改用支持的 `movie-q2`，保留原异常、并发会话与时钟断言；空任务和未知任务继续拒绝。
- Python DuckDB 先导入时，C++ 存储替身的同名 `duckdb` 符号被真实库接管，
  VARCHAR fixture 因类型不匹配失败。仅将替身和原样 converter include 放入匿名命名空间，
  增加 DuckDB-first 子进程回归。真实 converter、工程 patch、DSO 和调度未变。

五组限定回放全部通过：

| 路径 | 输入与验证 |
|---|---|
| IMLane Q1 | 25 条保存答复，完整消息/token、原 Extract/sink、25/26 预算 |
| IMLane Q6a | 25 条保存答复，原 Airlines sink、发生项关联、ACK、25/26 预算 |
| IMLane Medical Q4 | 历史冷轮 1200 条保存答复，重复消息逐发生项、完整 join/filter/mean、1200/1201 预算与后置失败 |
| LOTUS Q5 | 原序 8×8，64 对显式软件答复，原 SDK/Unix IPC、完整 Join、sink 时钟与65次拒绝 |
| Kalypso Q9 Map | 完整冷暖各256条保存输出，原排序/评价，加错误索引、模型身份与重复 request ID 三类失败边界 |

这些均为软件引擎/保存答复检查，真实模型调用为0。Q5 答复明确为合成 fixture，
不能把它或其他历史答复称为新的模型质量或性能。没有 SSH、GPU、服务启动或部署。

## 独立组件的目标状态

以下只记录协调器提供的目标结果，不是本组合树的实测，也不累加到上述 CPU 数字。
本轮未重新执行目标任务；目标归档由协调器核验后交接，完整资格、资源与性能仍以各冻结来源的原件为准。

| 冻结来源 | 协调器交接的范围 |
|---|---|
| `73ee255e` | IMLane Q1 的25次真实执行与完整 sink 已完成 |
| `d176cbb2` | MMQA Q3b–g、Q6a–c 九道独立25次已执行；Q3e 模型/sink通过但外层 guard失败，不能记整轮成功 |
| `6a31cc38` | Medical Q4 冷轮1200次及完整 sink 已通过 |
| `2e2616a5` | Q9 Map 冷暖共512次、0重试、逐条完整token/sink审计已通过 |
| `2e2616a5` | Movie Q1–Q4 新 sink 冷暖全部完成，共4720次：Q1为2000×2，Q2/Q3/Q4各120×2。392份原件经协调器逐份SHA核验；原 Filter 未记录完整 runtime token 数组，仅完整消息hash与CPU核对通过，不能称逐token实测一致 |
| `51eed806` | 已执行 Q5 小样本64对目标任务；不授予 Q6/Q7 或完整256×256资格 |
| `909f842c` | Ray Q7、Kalypso MMQA Q3b–g/Q10及 LOTUS MMQA Q5 按该冻结来源单列，不转记后续组合版本 |

Q9 原件运行标识为 `kalypso-movie-q9-2e2616-target-20261111-r1`，
协调器指向其中 `TARGET-AUDIT.json` 及136份原件，报告完成时间为2026-11-11 19:10 UTC。
Q1、Medical 和 Join 的原件分别按
`imlane-mmqa-q1-73ee-target-qualification-20261111-r1`、
`imlane-medical-q4-6a31-target-qualification-20261111-r1`、
`lotus-movie-join-51eed-target-cpu-20261111-r1` 定位。
Q1–Q4 原件标识为 `kalypso-filter-q1234-memory2e-originals-20261111-r1`，
各目标 `TARGET-AUDIT.json` 分别保留。四题原任务指令精确不同，不能用同一 prompt 复用或推定另一题已通过。
正在准备的 Kalypso MMQA Q6 使用旧 `909` 加专用 `4922774c` 查询和Q6 hooks，
属于另一专用迁移，既不归 `2e2616a5`，也不归本组合树。

外部 host guard 的新 helper `7c1d` 仅由协调器报告16项目标 CPU 检查通过；
它不是本仓源码，本次不纳入包或合并，也不改动旧 `340bb` 身份。

## 证据保存和退出条件

仓库外同名运行目录保存 `CPU-PLAN.json`、`CPU-PLAN-r2.json`、两轮完整 CPU 结果/日志、
五组回放及原始输入引用、符号冲突复现、逐文件组合来源、原 worktree 保护摘要和 Git bundle。
这些原件由本次工程交接获取；公开仓库不复制真实路径、配置或大批原始回复。

旧 `execute_records`、Daft `execute_q3`、IMLane records owner、Q7 按摘要载入的
host/socket helper 和未迁移 Kalypso 私有调用方仍在使用。没有新增可直接删除的生产模块。
只有现行调用方迁移、原功能与退出验证、唯一证据恢复路径全部明确后，才退出对应旧启动引用。
本次不清理任何源码、工作树或旧包。main 仍需另行处理既有文档修改后决定接纳；
组合树没有新的 GPU/性能资格，跳过的 Animals 检查也仍未完成。
