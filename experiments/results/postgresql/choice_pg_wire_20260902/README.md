# 四 C：PG choice SELECT 的 open spec 与 wire v4 接线

内部工程验证记录，2026-09-02。来源为自有生产源码、固定向量、实际 PostgreSQL 18.3 测试和
旧/新扩展二进制对照。对应[choice 专项计划](../../../plans/completed/postgresql_choice_profile_engineering.md)，
不作为模型质量、成本或 GPU 性能实验。最终源码为 `80bb7fc508d42a44f2ca485944d800131fa59df3`；
保存在独立研发分支，未合并或推送，主工作区用户文档未覆盖。

## 目的和实现

让已保存的 schema-3 profile 真正进入 PostgreSQL 的执行生命周期，并保持旧 v2/v3 行为。
本切片只验证受支持的 SELECT 形状，不把四 C 的资源和真实模型验证写成已完成。

- `AiOpenSpec` 显式保存 profile 是否存在及完整固定宽度值。runtime 将 bytes 复制到 query context，
  UDS open 再复制为 session-owned bytes，不从运行时 GUC 或注册表重建生成语义。
- C `wire_v3/v4` 共享 `wire_semantic.c` 的固定 exact codec 和既有 framing/JSON primitives；v4 使用
  独立字段、版本及摘要，open 传完整 profile，opened/task/completion 核验 profile digest。
  v3 入口拒绝带 profile 的请求；v4 拒绝重复字段、错版、缺失/额外字段和身份/evidence 篡改。
- pump 的 `pending_plan` 分支已移除。新 SELECT 回到既有 runtime/machine，严格 parser、NULL、
  tuple keep/drop、sequence、输入 preflight、query cleanup 和中立错误映射继续复用。
- HTTP/供应商字段留在 gateway。实际出站旧/新请求逐字段比较，仅多出显式 choice；请求被拒绝时
  不无约束重试，UNKNOWN 与非法标签都保留原始值，由 PG 分别 drop 或报错。

参考来源与采用决定只在工程计划 C.0/C.1，不放进源码、测试或注释。没有复制公司源码或材料，
没有增加 core patch、异步、多在途、调度或通用框架。

## 环境、方法与实际结果

环境预检使用仓库外 runtime env 和 `core` 能力组；已有 PG18.3 工具链复制为本轮独立安装，
临时集群仅监听本地 Unix socket。旧 v2/v3 为兼容对照；模型侧仅使用 golden/localhost HTTP fixture。
没有安装依赖、下载模型、启动 vLLM、访问 calibration held-out 或修改已有数据库。

| 验证 | 最终观测 |
|---|---|
| PostgreSQL 构建与运行 | 18.3；干净 `-O2 -Werror`，无警告 |
| PGXS regression | 1/1；actual/expected 逐字节一致 |
| 完整 TAP | 748/748：主套件 440、更新后的 choice plan/no-task 90、choice execution 218 |
| 本地/服务器 Python，各自运行 | 83/83：PostgreSQL 协议/静态 68、gateway migration 5、calibration 10 |
| 中立 header、profile encoder、三份 machine | C11 / Wall / Wextra / Werror / pedantic 通过 |
| 真实模型请求 | 0；本轮模型调用均为 fixture |
| 新资源 smoke、真实解码能力、模型质量、成本精度 | 未验证，不沿用历史数字代替 |

218 项执行检查包含三值、NULL、空串/Unicode、重复值、LIMIT、EXPLAIN counters、prepared/invalidation、
同 backend savepoint 恢复、普通写入回滚、完整错误帧、旧端拒绝、raw/escaped NUL、query cancel/recovery、
输入上限和实际 HTTP 参数对照。90 项计划测试保留完整 profile 复制、严格解码、身份和零任务零连接；
已删除临时 `0A000` 拒绝预期，因此不与历史 97 项直接相加。旧校准隔离仍由主套件验证。

## 单独发现：Filter INSERT 尚未接管

[二进制对照](raw/insert-comparison.json)使用已归档的 `7d72d9ad` 和当前扩展，在同一 PG18.3 临时
数据库分别检查 recording Filter、exact v3、choice v4。两边 SELECT 计划都有 CustomScan；六个
INSERT 计划均无 CustomScan，执行均返回 `55000: ai_semantic.filter marker was not lowered to a semantic plan`。
两份 `.so` SHA 与各自 qualification 完全一致，诊断数据库已停止。

这是既有 Filter carrier 缺口，不是本轮 wire 回归；本轮未修改 planner 来修复它。此前将 Map 的
direct INSERT 验证泛化为“两个算子都支持”不准确，现役文档已收窄。普通 INSERT 回滚不证明
Filter INSERT 可用；该问题须独立修复并验证，不能以本报告宣布 Filter 写入路径已通过。

## 失败和分步验证

1. `d99cd168` 的真实 PG 红测试 2/3 失败，原因是原有 plan-only 执行拒绝。
2. 先保留拒绝，`8e7cd92d` 接好 codec/open spec 并通过旧 TAP 537/537；之后 `8674269d` 才移除
   保护，最初 PG→golden 三项通过。没有在仅完成半条路径时允许 v3 回退。
3. `e10ba862` 的 savepoint 测试尝试 Filter INSERT，但此语句未 lowering；gateway 等待不存在的
   第二个 session。保留诊断日志后仅终止该测试 gateway，输出虽为 150/150，因人工干预及断言漏掉
   第二个 SQL 错误，不作为有效验收。最终增加错误次数、成功 SELECT 结果、等待时限和进程退出检查，
   并将 INSERT 问题独立核对。见 [排除的尝试](raw/excluded-savepoint-attempt.log)。
4. 首次完整 748 项通过，但采集器在导出 HTTP 请求时停止：PG 默认清理删除了测试数据目录。
   原始输出保存于仓库外 `qualification-attempt1.tgz`。最终使用 `PG_TEST_NOCLEAN=1` 完整重跑，
   仍为 748/748，并实际保留请求文件。这是证据采集修正，不是放宽产品测试。

## 证据、清理与剩余工作

[qualification.json](raw/qualification.json)记录源码/构建/运行身份、命令、退出码、源码 SHA 与二进制摘要；
[raw/SHA256SUMS](raw/SHA256SUMS)覆盖服务器日志、失败尝试、[运行脚本](raw/qualify.py)、
[INSERT 诊断脚本](raw/diagnose_insert.py)与实际
[旧请求](raw/http-request-legacy.jsonl)、[choice 请求](raw/http-request-choice.jsonl)、
[被拒请求](raw/http-request-rejected.jsonl)。本地复跑见 [local/SHA256SUMS](local/SHA256SUMS)。
原始 preflight、提交 bundles、所有失败/中间尝试、测试目录和二进制保存在仓库外
`choice_pg_wire_20260902` 证据包，主/公开清单已校验。

本轮临时集群已停止、测试端口无监听；归档后只移除本轮服务器 worktree，源码与产物可从 bundle
和压缩包恢复。其他工作负载和历史 worktree 未清理；本地分支保留供审核。

剩余工作：单独修复 Filter INSERT carrier；按专项计划预先定义采样再做新 profile 的 RSS/FD/线程检查；
在总预算内验证真实服务的 choice 能力。模型质量、整轮校准、第二 physical path 均未推进。
