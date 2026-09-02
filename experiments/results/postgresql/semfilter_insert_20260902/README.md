# Filter INSERT 的 PostgreSQL 18.3 carrier 修复

内部工程验证，2026-09-02。来源为自有源码、官方 PG18.3 源码及实际回归输出。
对应[专项计划的 INSERT 切片](../../../plans/completed/postgresql_choice_profile_engineering.md#filter-insert-的独立修复切片已完成)。
生产修复为 `8e50addf`，最终测试源码为 `39007150d5d0f84904fcd0c36b7bab87de7c07c1`。
本切片支持数据库语义算子的正确执行，不是数据组织、调度或模型质量实验。

## 目的、原因与改动

修复已由[旧/新二进制对照](../choice_pg_wire_20260902/raw/insert-comparison.json)确认的既有问题：
普通 Filter `INSERT ... SELECT` 没有生成 CustomScan，执行报 `55000`。

官方 `REL_18_3` 源码身份为 `62d6c7d3df6287f1bd83199c1a746e50d31571a0`。
`prepjointree.c:pull_up_simple_subquery` 会把简单源查询的 jointree 放入父查询；WHERE 条件非空时
保留 FromExpr。Map 的目标列表含 volatile marker，不能按相同方式上拉。此前 Filter carrier 只看
当前 SELECT 层的顶层 quals，因此遗漏 INSERT 源查询上拉后的形状。
参见[官方 planner](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/optimizer/plan/planner.c)
与[源查询预处理](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/optimizer/prep/prepjointree.c)。

生产改动仅在 `sem_filter_path.c`：识别单个上拉的源 FromExpr，并在源关系上创建原有 Filter path；
INSERT 限制检查同时处理已上拉的外层 INSERT 和未上拉的子 SELECT。没有改写 Query 树、阻止 PG
优化、增加 hook、修改 core 或新建执行层。结果仍是 `ModifyTable → SemFilter → ordinary child`。
runtime、provider、wire、严格 parser、三值/NULL 规则、plan identity 和 cost 公式均未修改。

公司 demo 只作工程参照，不决定自有能力范围。只读核对与保留决定在专项计划；未复制公司源码、
测试输入或材料，也未把来源说明放入生产代码、测试或注释。未来移植仍包括算子处理/优化与
SemLoom 执行调度，不限于一个公司 Adapter。

## 设置、设计与结果

使用既有仓库外 runtime env，`core` preflight 通过；单独复制 PG18.3 安装并创建 socket-only
临时测试集群，不使用已有数据库。无依赖安装、模型下载、真实模型启动、held-out 访问或性能拟合。
对照为修复前同一 SQL 的失败，以及修复后的 recording、exact v3、choice v4 合成行为；所有模型
结果来自 golden/HTTP fixtures。只改变 planner 接管，不进行模型或算法消融。

| 验证 | 实际结果 |
|---|---|
| 最小红测试，`efc24bb1` | 3/3 失败：缺少 Filter plan，执行 `55000`，目标表为空 |
| 最小修复，`8e50addf` | 同一 3/3 通过 |
| INSERT 专项最终测试，`39007150` | 171/171 通过 |
| PG18.3 完整 TAP | 919/919：原 440 + 90 + 218，新 INSERT 171 |
| PGXS regression | 1/1；actual/expected 逐字节一致 |
| 构建与运行 | PostgreSQL 18.3；干净 `-O2 -Werror`，无 warning |
| Python，本地/服务器各自运行 | 83/83：PG 协议/静态 68、gateway migration 5、calibration 10 |
| 中立 header、profile encoder、三份 machine | C11 / Wall / Wextra / Werror / pedantic 通过 |
| 真实模型尝试、held-out | 0 次；未使用 |

171 项检查覆盖三个 Filter 配置的直接写入、普通谓词/投影、重复值、三值/NULL、计划身份、实际
任务/输出计数、ORDER BY/LIMIT 未上拉路径、commit/rollback、prepared/invalidation、savepoint 的
部分写入回滚及同 backend 恢复、目标 CHECK 错误、零任务零连接。另验证 choice INSERT 的源表
RLS、目标写权限、取消与新会话恢复。RETURNING、ON CONFLICT、OVERRIDING、join/aggregate 的
拒绝保持。已保存的结果由 SQL 查询核对，不以正常退出代替行集验证。

## 失败与证据审计

- 第一次同步修复时，postgres 用户访问 root 所有的 Git worktree 元数据被拒绝；随后运行仍是
  `efc24bb1`，再次 3/3 失败。`history-fix-*` 不证明修复失败或成功。用限定到本 worktree 的
  safe.directory 和 root 执行 Git、确认完整新 commit 后，`history-green-*` 才是修复验证。
- 最初的行为测试为 131/131。审阅发现旧配置并不公开 semantic digest，比较两个缺失值没有证明力；
  最终改测实际公开字段，并对 choice 固定独立 digest 向量，再增加计数和拒绝形状，得到 171/171。
- 完整验收首次传入短 commit，严格身份检查在构建前拒绝；该输出保存在仓库外
  `qualification-identity-attempt.log`。改为完整身份后完成上述完整验收，未放宽比较条件。
- 本地第一次运行受沙箱 socket bind 限制产生 12 个 PermissionError；获准本地监听后 83/83 通过。
  [原失败输出](local/postgres-sandbox.log)保留，不作为产品失败或成功证据。

[服务器 qualification](raw/qualification.json)保存命令、退出码、版本、源码和二进制 SHA；
[raw/SHA256SUMS](raw/SHA256SUMS)覆盖完整 TAP/server、regression、历史失败和[采集脚本](raw/qualify.py)。
公开脚本副本只规范化文件尾空行；仓库外执行原件保留，主清单分别绑定原件和公开清单。
[本地 qualification](local/qualification.json)核对同一 commit 的源码与独立复跑结果；
[local/SHA256SUMS](local/SHA256SUMS)覆盖本地输出。旧 748 项证据保留，不用新结果改写历史。
原始 preflight、bundles、红/绿/行为/覆盖测试目录、最终测试目录和扩展二进制保存在仓库外
`filter_insert_20260902` 证据包，主/公开 manifest 均校验通过。

## 清理、结论与下一步

本轮临时集群已停止，原始证据归档后仅移除本轮服务器 worktree；可用 bundle 和归档恢复。
其他历史 worktree、服务器主工作树和本地主工作区用户文档未修改。研发分支保留，未合并或推送。

当前受限单表 Filter INSERT 已有真正的规划、执行和事务验证；不能外推到任意 SQL 形状、真实模型
质量或资源不增长。下一步仍是 C.5 预先定义的 RSS/FD/线程与取消恢复检查，再在累计最多 100 次
尝试内核验真实服务 choice。四 C 未全部完成，校准继续暂停，不启动第二 physical path。
