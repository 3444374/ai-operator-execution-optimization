# 生成型 SemMap：PG 计划与权限接入验证

日期：2026-09-03。类型：源码、真实 PostgreSQL 功能测试与确定性 fixture；不是模型或性能实验。

最新追加：[SQL wrapper 来源修复](#sql-wrapper-source-check)绑定 `676615fa`，PG18.3 完整 TAP
1283/1283。下文首次 1260 项及原始产物仍绑定 `2205ccbb`，不重新归属。

## 目的与范围

按[四 D 合同 §3、§8](../../../plans/postgresql_semmap_generation_contract.md)完成 SQL 注册、schema 4
计划保存、严格解码、PG 权限和初始化检查，为后续 C v5 接线提供已验证入口。保留现有 scan、pump、
runtime/provider 分工；本次没有实现生成型 Map 的逐行执行，也没有修改 core、gateway、wire 或调度。

研发分支 `codex/semmap-pg-plan` 从 `340356e8` 开始，最终验收源码为
`2205ccbbb764e273ee80cede0a2dd1170136f785`。本记录不表示该分支已经合入或推送 main。
期间主线新增 `c7b1e9e6`、`0bf56253` 文档，已只读核对其不改变四 D 合同和运行范围，没有覆盖那些修改。

## 设置与合规检查

- 显式使用 PostgreSQL **18.3**，官方源码提交 `62d6c7d3df6287f1bd83199c1a746e50d31571a0`；
  不使用系统默认工具链。先运行 runtime `core` 只读 preflight，再创建独立 Git worktree、安装前缀和测试集群。
- 每轮固定干净源码，`-O2 -Werror` 构建并核对安装二进制哈希；原工具链中的 extension 二进制不变。
  服务器主工作树仍为 `5d4ecf96` 且干净；没有切换、覆盖或推送 main。
- 只使用公开合成 SQL 数据、golden/fake HTTP 和本地 UDS fixture；未复制公司代码、私有数据或环境文件。
  服务器路径、账号、命令及异常在公开产物落盘前由公共脱敏器处理，真实环境报告留在仓库外并记录哈希。
- 真实 Map 模型请求 **0**，累计预算仍 **0/32**；未启动模型、使用 held-out 或运行 RSS/FD 压力验收。
  既有 Filter 质量/校准失败结论及历史资源证据没有改变。

## 已实现与测试设计

| 对象 | 本次可核对行为 |
|---|---|
| SQL 0.2.0 | 新 `map(text,text,jsonb)` 为 C/VOLATILE/PARALLEL UNSAFE/SECURITY INVOKER、非 STRICT、非 LEAKPROOF；0.1.0 升级保留旧 OID、属性与自定义授权；新装与升级定义一致 |
| 固定参数 | planner hook 在 PG 参数代入/折叠前拒绝 instruction/options 的 Param、Var、SubLink、STABLE/VOLATILE；普通 input/谓词参数保留；INSERT source 同样检查 |
| 实际参数值 | NULL、字段集合、类型、UTF-8 字节上限及数学整数范围分别验证；128/128.0/等值指数写法和 JSONB 键顺序得到相同身份，不改变旧 Filter 规则 |
| 计划 | 29 个命名字段、独立列号/函数 OID binding、完整 S/PH；copyObject 后删除原/复制 context，值仍独立；逐字段篡改、重复/缺失/未知字段及非法 binding 被拒绝 |
| 原生权限 | 每次节点初始化检查新函数 EXECUTE，并调用原生 execution hook 一次，先于 child/provider；直接、LIMIT 0、空输入、NULL、EXPLAIN 和跨会话缓存计划撤权均验证 |
| 函数身份与重规划 | 非成员和其他扩展成员不接管；同 OID 函数替换、删除重建触发 generic plan 重规划；恢复成员后重新接管 |
| 受限 SQL 位置 | 顶层 SELECT 与直接 INSERT SELECT 可生成新 Map 计划；CASE、WHERE、混用 Filter、子查询、排序及禁止的 INSERT 修饰被明确拒绝 |
| 暂时执行状态 | 普通 EXPLAIN 显示 `Execution Support: plan-only`、`Provider: not-connected`；实际执行以 `0A000 / generative SemMap execution is not connected` 结束，不能误走旧协议；零 provider 连接 |

测试入口为 `code/postgres/semloom_pg/t/006_map_plan.pl`。测试专用动态库调用生产计划 codec，
并通过真实 PG planner/object-access hook 观察回调；没有在生产代码加入测试 GUC 或计数器。
权限使用 PG 的 `object_aclcheck`、`aclcheck_error`、`InvokeFunctionExecuteHook`；lowering 删除 marker
表达式后显式登记函数依赖。此处仅针对新重载，不扩大为旧算子所有 ACL 行为已重新实现的声明。

仅修改成员关系仍不自动刷新所有 backend。本次双会话记录显示：DROP 后未刷新缓存的 Map 计划返回
`XX000 / invalid generative SemMap function binding`；ADD 后未刷新仍可见普通 Seq Scan。
每个相关物理连接执行 `DISCARD PLANS` 后，两方向均正确重新规划，函数定义不变。
继续遵守[现有受控刷新要求](../../../../code/postgres/semloom_pg/README.md#function-identity-and-administrative-membership-changes)，
不将成员移除当作即时权限撤销或在线无感变更。

## 运行与原始结果

[服务器执行器](raw/server/qualify.py)接收仓库外路径及完整提交参数；实际命令、退出码和输出见
[步骤](raw/server/steps.json)、[完整 TAP](raw/server/tap.log)、[regression](raw/server/regression.log)。
可复核的调用形式为：

```sh
python <qualification-script> --repo <isolated-worktree> --root <new-artifact-directory> \
  --base-prefix <explicit-pg18.3-prefix> --env-file <runtime-env-file> \
  --pg-source <official-pg18.3-source> --commit 2205ccbbb764e273ee80cede0a2dd1170136f785
```

| 最终检查 | 实际结果 | 原始证据 |
|---|---|---|
| PG18.3 构建/安装 | 无 warning，`-O2 -Werror`，安装 SHA 与构建一致 | [build](raw/server/build.log)、[资格清单](raw/server/qualification.json) |
| regression | 1/1，actual 与 expected 原始字节 SHA 相同 | [actual](raw/server/regression-actual.out)、资格清单 |
| TAP | **1260/1260**，6 个文件；其中新 Map plan **238** 项 | [总输出](raw/server/tap.log)、[新用例](raw/server/tap-regress_log_006_map_plan) |
| Python/static/protocol | 本地、服务器各 **136/136**：109 + 6 + 10 + 11 | [本地](raw/local/verification.json)、服务器资格清单 |
| C11 | 两端七个 PG-independent module 与 neutral header 通过 | [本地输出](raw/local/checks.log)、服务器 `c11-*.log` |
| 本切片退出核对 | 14 个测试目录均无 postmaster.pid，无匹配这些测试源码/前缀的存活进程 | [退出核对](raw/postflight.json) |

这些是单次工程验证，不是重复实验或性能统计；没有性能 baseline/消融，不能计算模型质量或吞吐收益。
所有服务器 worktree、隔离二进制和原始目录保留在仓库外持久 artifact 目录，没有清理历史唯一证据。
公开副本及哈希核验入口为 [raw/verification.json](raw/verification.json) 与 [SHA256SUMS](raw/SHA256SUMS)。
本地汇总日志仅删除聚合器添加的一行末尾空行以满足 Git 格式检查；原外部文件保留，
[规范化记录](raw/local/normalization.json)保存变更前后哈希，测试内容与结果未改。

### 测试先行过程与失败记录

`raw/stages/<commit>/` 保存每轮构建、TAP、源码哈希及实际退出码；红灯是反例，不算通过数量。

- `d6705574` 缺升级路径 → `56c0d913` SQL 注册通过；`3512cc38` 新 Map 未接管 → `2bb9196e` plan-only 通过。
- `a1638e4c` 复现 custom 参数/函数内联绕过固定参数来源限制 → `97f35227` 在 planner hook 先检查。
- `afdaf94d` 复现新函数撤权未检查 → `58ba199b` 原生 ACL/hook 顺序通过。
- `5c598ada` 复现 CASE/WHERE/混用 Filter 被放宽 → `848f9ad3` 先验证调用位置；`a347def7` 144 项通过。
- `b746e28a` 的新增 Perl fixture 使用了错误引号，**没有运行任何子测试**；修正后的 `52e0fcd4`
  实际执行 238 项，其中 4 项断言证明输入/谓词子查询仍被接受。`2205ccbb` 仅对新 Map 补 `hasSubLinks`
  检查，随后完整 1260 项通过。不能把 fixture 语法错误当作生产反例。
- 首次本地合同运行有 13 项 localhost bind 受沙箱限制，另有旧 default-version 静态断言；允许本地 fixture
  监听并将断言更新为本次明确新增的 0.2.0 后，固定提交完整通过。未修改测试语义来绕过模型或 PG 错误。
- 一次服务器 Git 自动维护与工作树创建重叠，出现 `bad object worktrees/source8/HEAD` 警告；
  只读核对相关提交/工作树引用有效、main 干净，后续 fetch 仅按命令禁用自动维护。
  未删除 gc 日志、运行 prune/repack 或改动其他工作树；警告原因属于并发推断，不作全仓健康结论。

## 结论与下一步

源码和实际 PG 测试支持：新 Map 的 SQL/plan/权限初始化已接入现有 PG 载体，旧执行路径回归保持。
不支持的声明包括：新 Map 已生成数据库结果、PG→v5→模型已通、逐行 NULL/顺序/大文本/取消已经完成
新路径验收、资源不增长、模型质量合格或四 D 全部完成。原 1022 TAP 仍只证明各历史提交。

下一步按专项合同连接 `AiOpenSpec → C v5 → gateway`，复用现有 machine/pump/runtime，移除本次临时
plan-only 分支；先用 PG＋golden/fake HTTP 验证真实取数、完成值、INSERT、取消及资源生命周期，
再满足运行前登记要求后使用已批准的真实模型预算。无需增加 core patch、异步或通用框架。

<a id="sql-wrapper-source-check"></a>

## SQL wrapper 来源反例与修复

对照基线 `b58479f7`；反例源码 `ae6589a906dc47daf2c29a573e7aab0e7bb73e6f`，
修复与最终验收源码 `676615fafd602d609aa7216f45f3534145bcb194`。本次仅修新 Map 的 planner 识别，
未修改 plan schema/digest、权限初始化、runtime/provider/wire、模型或资源预算；Filter 命名的共有常量
整理继续留到实际 C v5 接线，不在此另建公共层。

**实际反例。** SQL 函数包装整个 Map，调用方将准备语句 `$1` 作为 instruction。
PG18.3 custom plan 接受并生成 schema 4 CustomScan；generic plan 拒绝。
两种模式各自的连接哨兵均为零，见[反例 TAP](raw/inline_20260903/red/tap.log)和
[逐项输出](raw/inline_20260903/red/tap-regress_log_006_map_plan)。首次测试运行到第 206 项后，
因为 SQL-standard 函数体对 Map 的依赖阻止后续身份测试 DROP 而中止；这是 fixture 清理问题，
不改变先前已经实际复现的 3 个 custom 失败断言。修复测试在 wrapper 检查后显式删除自己创建的函数，
然后继续完整旧测试，不删除生产对象或放宽预期。

**修复方式。** 现有入口只支持原始 SELECT 顶层或直接 INSERT 来源中的显式 Map。
前置检查记录本次规划已验证的唯一来源层级，后续路径构造须匹配；内联后才暴露的 Map 返回
`0A000 / generative SemMap must be a direct query output`。因此整个 Map 的 wrapper（包括固定字面量参数）
不是本版新增入口。普通 SQL 函数内联、直接 Map 的普通 input wrapper 与合法常量仍保留。
临时状态仅覆盖本次 planner 调用，通过 `PG_FINALLY` 在成功、嵌套规划和 ERROR 后恢复；
不依赖源码字符位置推断来源，不使用跨查询 registry，不修改 PostgreSQL 内联开关或 core。

这是依据现有受限入口的工程选择，而不是尝试在这一切片支持任意包装函数。官方
[REL_18_3 clauses.c](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/optimizer/util/clauses.c)
的 `inline_function` 使用已经简化的实参并递归简化展开结果，与上述实测风险链一致；本轮还核对了
服务器官方源码，不把源码推断代替反例。参考来源只记录于此及专项合同，不进入生产/测试注释。

| 追加检查 | 实际结果 |
|---|---|
| 新增行为 | 两种 SQL 函数体写法 × custom/generic 均拒绝，零 provider 连接；普通函数仍内联，直接 Map 的输入函数仍允许；嵌套规划成功/捕获 ERROR 后外层计划身份正确 |
| 完整 PG18.3 | 无 warning 的 `-O2 -Werror`，regression 1/1、TAP **1283/1283**（Map plan 261 项），原生权限/旧路径同时重跑 |
| 本地与服务器 | 各 **136/136** Python/static/protocol；七个独立 C module 与 neutral header 共 **8/8** C11 检查 |
| 模型、资源 | 真实模型请求 0，累计 **0/32**；没有运行新 Map RSS/FD 压力、质量或校准 |

证据入口：[服务器资格](raw/inline_20260903/server/qualification.json)、
[TAP](raw/inline_20260903/server/tap.log)、[本地](raw/inline_20260903/local/verification.json)、
[独立 SHA 清单](raw/inline_20260903/SHA256SUMS)。原 205 项归档及首次构建/测试清单没有重写。
本次只增加两个隔离测试工作树/前缀，原安装二进制和服务器 main 未覆盖，测试节点停止后保留证据。
[退出核对](raw/inline_20260903/postflight.json)确认本次目录无 postmaster.pid 或匹配测试进程，
不对服务器其他工作负载作全局清理声明。[两路只读复核](raw/inline_20260903/review.json)分别为
Standards 0 项、Spec 0 项；复核者未重跑服务器或模型，运行证据由上述原始日志支持。
新 Map 仍处于 plan-only，C v5、PG＋golden 与真实模型尚未接通；本次修复不能改写为整个四 D 完成。
