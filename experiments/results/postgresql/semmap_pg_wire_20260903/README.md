# 生成型 Map：C v5 与 PostgreSQL golden 执行验证

## 目的与设置

本切片验证 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化所需的同步生成入口，
对应[四 D 合同](../../../plans/postgresql_semmap_generation_contract.md)。它不是模型质量或性能实验。
从已通过复核的 `035b0ccf` 继续；最终验收源码为 **`5031bb5072cd42f08357d6d7aa604009e03bc9c9`**，
生产修复在 `114a411a`，最后一提交仅给隔离测试角色设置 fixture 地址的权限。
分支 `codex/semmap-pg-plan` 尚未合入或推送 main。

环境使用官方 REL_18_3 源码 `62d6c7d3df6287f1bd83199c1a746e50d31571a0`、独立安装前缀和临时
socket-only 测试集群。未覆盖主安装、原集群或服务器主工作树，没有下载或启动模型。
输入、输出、usage 均为公开合成 fixture；SQL 的 Model Calls/usage 字段验证的是此类完成值，
不能当成真实模型消耗。真实模型预算仍为 **0/32**；没有使用 calibration held-out。

## 改动与检查方法

- plan-owned stop 是否存在、输入/输出限额进入中立 open spec；正向识别 recording/Filter/Map，
  v5 使用独立 execution identity，不回退旧版本。共有 reference 名称整理后摘要保持不变。
- 保留来源与原生 EXECUTE ACL/hook；移除临时 plan-only 拒绝，复用 machine、pump、runtime、UDS、
  framing 和 Python session。Map 完成值检查仍调用纯值模块，PG 只映射错误并管理生命周期。
- 先运行独立 ASCII golden 向量，随后测试 NULL/空输入/空输出、Unicode/控制字符、任意文本、
  163840-byte 输入与 65536-byte 输出及越界，列关联、重复输入、LIMIT/OFFSET、prepared plan、
  INSERT/rollback/savepoint、child 错误和取消后恢复。no-task 与撤权使用真实监听 sentinel 验证零连接。
- 故障 peer 复用正式 session，仅修改出站帧；覆盖字段、版本、十进制字符串计数、identity/evidence、
  model/usage、finish reason、UTF-8/NUL 和错误帧。Map 不 trim、不截断、不重试、不把错误转 NULL。
- 旧路径完整运行一次最终 regression/TAP；没有按新测试数量推导异步、多会话、资源或模型资格。

运行命令、preflight、源码/二进制哈希和脚本分别在各运行的 `raw/<commit>/` 中。
最终入口是 [qualification.json](raw/5031bb50/qualification.json)、[完整步骤](raw/5031bb50/steps.json)
和 [TAP 输出](raw/5031bb50/tap.log)。脚本接收仓库外 runtime env 与独立前缀，不在 Git 记录机器路径。

## 实际结果与失败记录

| 运行 | 实际结果 | 解释 |
|---|---|---|
| `76156526` | 首条 golden SQL 失败 | 原实现明确拒绝执行，保留 red 记录 |
| `6941b91e` | PG18.3 `-Werror`、ASCII SQL 3/3 | 首次 C v5 → golden 返回 `1\|hello`，尚非全面资格 |
| `f7765d6c` | 全量 TAP 失败 | 常量输出回退原值；Unicode fixture 转义错误；非法 UTF-8 分类未先完成 |
| `69139c03` | 定点反例失败 | 常量与等值列别名问题；正确 evidence＋非法 UTF-8＋超长文本误报 54000 |
| `114a411a` | 新 Map 执行测试通过，完整 TAP 中止 | 修复生效；006 的测试角色无权设置 fixture 地址，未完成全量资格 |
| `5031bb50` | 完整资格通过 | 保持同一查询角色/缓存计划，只增加隔离测试的参数设置权限 |

输出位置问题来自原 carrier 将 Map 与输入写成相同的 scan 表达式。最小复现分别得到
`hello`、`hello|hello`、`generated|generated`，而预期是 `generated`、`hello|generated`、
`generated|hello`。修复只在 child 降低输入，保留 scan 的逻辑 marker 身份供 PG setrefs 生成
独立 slot 引用；不是让 marker 在 executor 中执行。recording 和生成型 Map 都有反例覆盖。

UTF-8 修复仅作用于 v5：有界帧在 JSON/evidence/输出长度前检查表示是否合法。正确摘要不能使
非法 UTF-8 成为合法输出。Unicode fixture 改用明确字符拼接，并先核验 SQL 存储的独立 hex；
未改写旧失败记录或合同向量。`114a411a` 的中止是测试配置错误，不是生产权限放宽的理由。

最终结果（一次完整 PG 运行，本地与服务器各一次合同运行）：

| 组件 | 结果 |
|---|---|
| PG build/runtime | PostgreSQL 18.3；`-O2 -Werror`，无 warning |
| PG regression | **1/1**；actual 与 expected 字节相同 |
| PG TAP | **1741/1741**，7 个文件；Map plan 268、Map execution 451 |
| Python/真实 C 调用合同 | 本地/服务器各 **137/137**（110＋6＋10＋11） |
| C11 | 本地/服务器各 **8/8**（7 个纯 C 模块＋中立 header） |
| 真实模型 / 资源压力 | **未运行**；不能借用旧 Filter/recording RSS/FD 数字 |

## 结论、资源与下一步

源码和本轮 PG 结果支持：三参生成型 Map 已能读取 ordinary child、执行同步 v5 任务、把完整文本
返回正确列，并在当前受限 SQL 形状下保留 NULL、权限、事务、错误和取消行为。旧路径继续通过。
Standards/Spec 两路只读复核指出的问题已修复并再次核对；审查本身不代替上述运行结果。

本切片六个测试目录的 PG/gateway 进程已结束，测试集群已停止，服务器主工作树保持干净；
测试工作树和仓库外原始产物保留以便复核，不声称服务器全局没有其他工作负载。
各阶段证据和失败均单独保存；总 SHA 清单与提交绑定源码检查用于回查，不重新绑定历史结果。

尚未完成四 D：接下来按原合同完成生成型 Map 的受控 RSS/FD/线程压力与固定模型验证；
运行前登记具体模型 revision、样例、timeout 和剩余请求预算。当前不进入组合、多会话、异步、
第二 physical path 或 core patch；Filter 质量与校准仍保持此前结论。
