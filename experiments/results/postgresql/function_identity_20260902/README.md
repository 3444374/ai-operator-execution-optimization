# PostgreSQL 函数对象身份检查（2026-09-02）

内部工程验证；对应[主计划工作包六](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md#carrier-audit-work-package)。
这是 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化的 carrier 正确性检查，不是模型、
调度或性能实验。原独立分支 `codex/pg-function-identity`，起点 `c494e1b2`；实现与证据已随
`390f666a` 合入 main，服务器验收仍绑定下方测试源码，不重新归属于文档提交。

## 结果与范围

已复现并修复同名普通函数被 planner 误接管的问题。扩展未安装时，普通 `ai_semantic.map(text)`
本应返回 `ordinary:false`，原实现却返回 `recorded:false`，EXPLAIN 显示 SemLoom CustomScan。
新实现只有在固定 schema/名称/参数解析出的函数确属 `semloom_pg` 时才接管。

另有一个尚未解决的管理 DDL 限制：仅 `ALTER EXTENSION … ADD/DROP FUNCTION`、函数定义未变时，
已有 generic plan 不自动重建。最初 Map 诊断发现此行为，后续四种调用形式都用两个物理连接记录了
ADD、DROP 的刷新前行为，并验证读会话 `DISCARD PLANS` 后计划正确，准备语句仍保留。
函数定义替换与删除重建的自动失效也保留独立测试，没有一起延期。

按用户确认，临时操作流程只针对上述仅成员变更：暂停相关查询，结束旧事务及游标；成员 DDL 提交后，
在每个相关物理 backend 执行 `DISCARD PLANS` 或重新连接，再恢复使用。连接池必须覆盖实际数据库
连接，不能只刷新 DDL 会话；无法保证全部相关连接刷新的环境不适用该方案。
[PG 官方说明](https://www.postgresql.org/docs/18/sql-discard.html)规定 `DISCARD PLANS` 使该会话准备语句下次使用时重新规划。
仅成员变更的跨会话自动失效仍为 pending；不支持在线无感变更，不把成员移除当作即时权限撤销。

## 对照与实现

| 阶段 | 身份 | 实际结果 |
|---|---|---|
| 原实现反例 | 测试 `aaa52967`；已验收 PG18.3 二进制来自 `39007150`，所测 `extension.c` 与 `c494e1b2` 相同 | 2/2 断言失败；实际行值与计划均证明误接管 |
| 最小修复 | `792a0408` | 生产只改 `extension.c`，原反例 2/2 通过 |
| 扩展测试 | `5b281226` | 24 项通过后夹具因未保留原 input 参数名而中止，不计为通过 |
| 中间验收 | `5f144cecd73491f72f288efc09a7f040ee3cae19` | 保留参数名后身份 TAP 71/71；完整 990/990，原始记录保留 |
| 最终测试源码 | `934f4f614c7fcef980c0a6af81dfd62d3979cc5c` | 加入双会话仅成员 ADD/DROP 刷新；身份 103/103，完整验收见下表 |

工程参照的来源、采用理由与不采用项只记录在[主计划的函数身份切片](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md#function-identity-slice)。
采用 PG catalog 成员关系检查原则，没有复制公司代码、修改公司工作副本或上传公司内容。
官方 `REL_18_3` 源码身份核对为 `62d6c7d3df6287f1bd83199c1a746e50d31571a0`；
`getExtensionOfObject` 查询扩展依赖，准备计划依赖收集由 PG 管理。源码定位：
[pg_depend.c](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/catalog/pg_depend.c)、
[plancache.c](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/utils/cache/plancache.c)。
源码阅读不代替下面的运行证据。

变量只有对象身份校验，数据均为公开合成值；没有模型、参数矩阵、随机样本或性能消融。
测试经真实 SQL → planner → executor 观察行值与 EXPLAIN，而不是检查 helper 内部实现。
保留固定 schema、原函数属性、三种 plan schema、wire v2/v3/v4、parser 和公共生命周期。

## 验收证据

| 检查 | 本次结果 | 原始记录 |
|---|---|---|
| 独立 PG18.3 `-O2 -Werror` 构建、安装 | 通过，无 warning | [build.log](raw/refresh/server/build.log)、[qualification.json](raw/refresh/server/qualification.json) |
| PGXS regression | 1/1；服务器原始 actual/expected 字节一致，公开副本另做空白规范化 | [regression.log](raw/refresh/server/regression.log)、[公开 actual](raw/refresh/server/regression-actual.out) |
| 完整 TAP | 5 文件，1022/1022，含新增身份 103 项 | [tap.log](raw/refresh/server/tap.log)、[身份 SQL 日志](raw/refresh/server/tap-005_function_identity_function_identity.log) |
| 本地与服务器 Python | 各 94/94：PG/protocol 68、gateway 5、calibration 10、choice 工具 11 | [本地](raw/refresh/local/)、[服务器](raw/refresh/server/tests-postgres.log)、[分类数量](raw/refresh/server/qualification.json) |
| 中立 C11 | profile、operator/filter/map machine、provider header 均通过 | [构建步骤与退出码](raw/refresh/server/qualification.json) |
| 初始额外缓存诊断 | 3 个检查通过；观察到自动成员变更失效缺口，不纳入完整 TAP 分母 | [observations.json](raw/membership-cache/observations.json)、[完整日志](raw/membership-cache/regress_log_membership-cache) |
| 哈希 | 两轮各 48 个服务器公开文件及各自 82 个源码文件匹配；总清单覆盖本地和诊断记录 | [最终 verification](raw/refresh/verification.json)、[SHA256SUMS](raw/SHA256SUMS) |

**原始文件与公开副本的哈希分开解释。** `qualification.json` 的 `regression_actual_sha256` 和
`regression_expected_sha256` 指服务器脱敏前文件；运行器先断言字节一致，再由 `scrub` 生成公开副本。
`scrub` 同时去除行尾空白，因此仓库的 `regression-actual.out` 不与未处理的 expected 直接按字节比较。
两轮原始文件记录的 SHA-256 均为 `8b261aa9247bee846a24e76f2f4e1b4afe5b060444dc11771296eea004ab5b5c`；
公开副本为 `98dc30b50a45970f3c7d1c819b3fdd325e58dd97e7cc7858e902bbd2b519a2b3`，由各公开 SHA 清单核验。
主线合并前另对相应提交的 expected 做相同空白规范化，确认与公开 actual 完全一致；未改写任何 raw 文件。

最终扩展 SHA-256：`f641c23ee67c33b7c2fcaae2088b9a251a95566deac2d3b30a1b777b733c50ef`。
中间构建为 `6e49fb281922b2053ebac0e50bf25a1594d16a49b031225e7823685bea2f6c23`，两轮使用不同独立前缀，
不以相同 C 逻辑推定二进制字节相同；各自源码和二进制身份分别保存。
原对照安装仍为 `534b1a5245999a85da941a4c2932e06582961f946732b490db98a1c5c9b0fcc6`，未覆盖。
103 项覆盖扩展缺失/删除、同签名非成员、其他 schema/重载、其他扩展成员、真正成员继续 lowering、
Map/recording Filter/exact v3/choice v4 的同 OID 替换、新 OID 重建及准备计划重新识别。
其中新增 32 项用 reader/DDL 两个不同 PID 的连接，仅改变成员关系并显式提交，再由 reader 刷新。
ADD、DROP 都核对函数 OID/定义未变、刷新后计划正确、准备语句仍存在；刷新前计划记录在
[TAP 明细](raw/refresh/server/tap-regress_log_005_function_identity)。这不是连接池集成测试；保证刷新
所有相关物理连接仍是使用者责任。只在 DDL backend 刷新不能满足本次条件。

本地首次命令漏设 `PYTHONPATH=code`，导致 3 个模块无法导入，工具输出未另存原始文件；不计为测试通过。
修正命令后，沙箱禁止 localhost bind，12 项报权限错误，保留[失败日志](raw/local/function-identity-local-postgres.log)。
获准监听后 68/68 通过，其余 26 项也通过。两轮独立的服务器完整验收各一次通过；早期反例与夹具失败日志全部保留，
不以最终通过覆盖失败。无新增依赖、模型下载、真实模型请求或 held-out 使用；累计 choice 预算没有变更。

## 复现与运行隔离

在上述干净源码提交准备独立 PG18.3 prefix、artifact root、仓库外 runtime env，先确认数据盘空间。
不使用系统默认 PG18.4，不覆盖历史工具链/数据库；`postgres` 用户运行测试节点，仅本地 Unix socket。
最终完整运行器为 [qualify.py](raw/refresh/server/qualify.py)：

```bash
python <qualify.py> --repo <clean-checkout> --root <fresh-artifact-root> \
  --prefix <independent-pg18.3-prefix> --env-file <runtime-env> \
  --history-root <earlier-artifact-root> \
  --commit 934f4f614c7fcef980c0a6af81dfd62d3979cc5c
```

该运行器先执行只读 core preflight，再构建、测试、核验、脱敏归档；为保留本轮失败历史，最后的归档
步骤还读取本切片已有的 red/green/coverage 日志。独立复跑可单独运行其 build/TAP/regression 命令，
不要伪造缺失的历史日志。模型相关测试均使用 fixture。

全部测试节点已停止，服务器测试工作树干净，原始产物和源码工作树保留。只对本切片资源作清理判断；
未重跑 RSS/FD 规模测试，旧资源与真实服务证据仍绑定原提交，不重新归属于本次构建。

下一步按主计划定义四 D 最小生成型 Map 语义；上述管理 DDL 要求已确认，自动刷新仍单列待实现。
本次没有实现真实 Map、planner 组合、多会话或增量 SemLoom session；Filter 质量与真实校准仍暂停。
