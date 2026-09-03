# 生成型 Map：C v5 与 PostgreSQL golden 执行验证

## 目的与设置

本切片验证 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化所需的同步生成入口，
对应[四 D 合同](../../../plans/postgresql_semmap_generation_contract.md)。它不是模型质量或性能实验。
从已通过复核的 `035b0ccf` 继续；最终验收源码为 **`5031bb5072cd42f08357d6d7aa604009e03bc9c9`**，
生产修复在 `114a411a`，最后一提交仅给隔离测试角色设置 fixture 地址的权限。
该阶段在 `codex/semmap-pg-plan` 独立验收；后续修复及 main 集成见下方[合并复核](#merge-review)，
原始 1741 项验收不重新绑定到后续提交。

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

<a id="merge-review"></a>

## 合并前协议与执行栈复核

在 `98f60993` 上复核发现两项协议规则不一致：v5 握手会错误接受仅属于 task 的
`OUTPUT_TOO_LARGE`；C 还额外要求两项 usage 之和不越过 uint64，而四 D 合同只要求各字段合法，
并由 runtime 分别检查累计溢出。本轮先在既有 Python wire Interface 与 PG SQL 错误/结果 Interface
补反例，再作最小修复；历史 `5031bb50` 的 1741 项资格不重新绑定为修复后的验证。

这是自有协议的工程修正，不引入新的公司实现来源。继续采用主计划 §8.7.2 已记录的请求/结果分离
与共享执行原则，保留现有公开合成 fixture、UDS/framing、纯值校验与 PG 生命周期设施。
编码与解码应共享本版本的错误合法性规则；wire 只检查表示、协议阶段和身份，Map 纯值模块负责
完成值规则，runtime 负责跨任务计数及资源。执行栈复核另检查这些职责是否真正落实，不能只增分支。
不改变旧协议，不添加通用 registry 或新的执行层，不启动真实模型或资源压力实验。

### 规则归属与修复

- `6ea34477`：Python `build_error_message` 负责本版本错误码与阶段的合法值，`validate_error`
  保留字段、版本、规范 sequence 检查并复用前者；消除两份可漂移的错误规则。
- `110ad445`：在真实 PG SQL Interface 增加握手反例，fixture 同时检查没有执行任何模型 task。
  `65b8382d` 将 C 的 code/version/是否存在 task 集中到既有错误校验函数，runtime 不识别 wire 错误码。
- `0baaae0d`：将合法单项 usage、篡改 evidence 和跨任务计数溢出分开测试。
  `f46fe936` 只删除 C 端合同外的两项 usage 求和限制；字段范围、output budget、evidence 和
  runtime 分别累计的溢出保护全部保留。

| 独立预期 | PostgreSQL 行为 |
|---|---|
| open 收到 OUTPUT_TOO_LARGE，sequence 为 null | 非法错误帧，08P01；零模型 task |
| 当前 task 收到 OUTPUT_TOO_LARGE，sequence 正确 | 输出超限，54000 |
| task 错误 sequence/字段/版本不合法 | 非法错误帧，08P01 |
| 单次 prompt_tokens=18446744073709551615、output_tokens=1，evidence 正确 | 接受并分别显示两项 usage |
| 上述完成值篡改 usage 而不重算 evidence | 关联校验失败，08P01 |
| 第二项合法完成值令累计 prompt_tokens 超过 uint64 | runtime 数值越界，22003；关闭 session |

### 实际运行

最终源码为 `f46fe936cdaceae8b5e3571321e28dfae6ac724a`，
[服务器资格](raw/protocol_review/server/qualification.json)、[本地检查](raw/protocol_review/local/verification.json)
与[整体核对](raw/protocol_review/verification.json)分别保存身份和结果。

| 运行 | 结果 |
|---|---|
| Python 握手反例，修复前/后 | 修复前编码、解码两项子断言均失败；修复后通过 |
| PG `110ad445` 握手红测 | 457 项中仅 1 项失败，实际误报 54000 而非预期 08P01 |
| PG `0baaae0d` usage 红测 | 468 项中 4 项新 usage 断言失败；握手反例已通过 |
| `f46fe936` 首次完整运行 | TAP 1758/1758、合同 139/139、C11 8/8 通过；regression 启动因测试 socket 路径超过 107 字节而中止 |
| 同一源码、短路径独立重跑 | PG18.3 `-O2 -Werror` 无 warning；regression 1/1；TAP 1758/1758；合同 139/139；C11 8/8 |
| 本地同一源码 | 合同 139/139；C11 8/8 |

最终 `.so` SHA-256：`e827fd3a1f68c92d26208c934f1df9c60f69dda93b5b6bb895e630721daa20af`。
准备时误选 analysis 环境的依赖探测失败，随后复用已有 driver 并通过 core preflight，没有安装依赖。
服务器 Git 自动维护提示一个既有历史 worktree HEAD 问题；本轮 fetch/隔离 checkout 成功，后续命令仅
关闭本次自动 GC，没有修改全局配置、历史 worktree 或服务器 main。该提示不是生产源码缺陷。

### 执行栈判断与后续

源码复核确认 Map 未复制 Filter 执行链：machine 分别编译消息和解释结果；PG pump/runtime 仍统一
取数、绑定、provider 生命周期和清理；UDS/wire 与 Python session/CompletionAdapter 共用。
machine 的 Strategy、provider 的 Adapter 和 query-fixed factory 均已有真实消费者，不是只增加命名。
runtime 调用 Map 纯值 validator 后做 PG 错误映射，符合专项合同；不把文本策略重写为另一套 PG 逻辑。
两项发现反映局部规则归属缺口，已在拥有规则的 Module 内收拢；未发现本轮还需拆层的结构性阻断。
两路只读审查最终 Standards 无未解决项，Spec 的 2 项均修复且无未解决项。

本次集成保留 main 的研究设计、SPEAR 笔记与分支全部历史证据。当前结构可以继续承接下一步，
不等于未来无需修改：组合须补 planner 独立调用/依赖绑定与 gateway 有界多会话；多在途须另外定义
接受后输入、完成缓冲和重排的所有权，不能直接延长当前 borrowed slice 的寿命或只删除单算子检查。
具体设计继续由主计划负责，不另建执行框架。

[收尾审计](raw/protocol_review/postflight.json)显示本轮四个测试目录无 PG PID 文件和匹配存活进程，
源码工作树干净，服务器 main 未改；原安装二进制未覆盖。测试工作树与仓库外原始产物保留，不作全机清理声明。
所有通过和失败日志保留并校验 SHA。**生成型 Map 真实模型仍 0/32，RSS/FD/线程压力未运行，四 D 尚未完成。**
