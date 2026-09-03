# SemMap 规范消息编译验证（2026-09-03）

内部工程验证，对应[四 D 合同 §8.0](../../../plans/postgresql_semmap_generation_contract.md#80-本次源码复核与首个子切片2026-09-03)。
目的只是在新增 Map 消息编译时保留现有算子行为，不是模型质量、成本或调度性能实验。
验收时使用研发分支 `codex/semmap-message-contract`，起点 `63d86c0e`；本记录的原始数字保持下方提交身份。
后续包含本切片的 `b0400944` 已合入本地 main，见[合并检查](../semmap_values_20260903/README.md#main-integration)。

## 改动与实际结果

测试提交 `ef26a06c` 先固定旧 Filter 消息，再用缺失 Map 编译函数的失败测试明确新增行为。
源码 `6903cf46ad8a1673ff44783a8a29d8481552c5aa` 增加 C/Python 两消息编译：system 为原样 instruction，
user 为原样 input。C 不分配内存、不做 I/O，接受带长度且独立标记 NULL 的借用值，写入前检查
UTF-8/NUL、4096/163840 字节上限和目标容量，失败不修改目标。Python 以静态错误拒绝非法文本。

自有 JSON escaping 移入共用 writer；Filter 的 directive/分隔符回到 Filter machine。
Python Filter 只委托共用 JSON 编码，旧验证、消息、摘要和错误行为保持。未修改 scan/pump/runtime/port，
没有新增 SQL、schema 4 或 wire v5；recording Map 没有被提前切换为生成执行。
来源和采用理由只保留在上述专项与[主工程对照](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md#company-engineering-reference)，
未复制、修改或上传公司 demo。

## 验证设计与数据

输入都是合成文本，无训练集、held-out、模型调用或性能消融。13 项新增公开接口测试调用实际 C
机器/Map 消息函数和 Python 函数，不读取内部 writer 状态。独立文字向量覆盖 ASCII、空串和 Unicode
的 81/76/103 字节；另覆盖全部 JSON 转义种类、UTF-8 标量端点/畸形序列、NUL、字节上限、NULL、
空 instruction、容量不足/多余、前后保护字节和失败不写入。旧 recording 无消息、Filter schema 2/3
字节保持也逐项核对。最大转义后的**规范消息**为 1,007,677 字节；这不是完整 v5 frame 的验收。

| 检查 | 本轮实际结果 | 原始记录 |
|---|---|---|
| 本地/服务器合同 | 各 107/107：PG/protocol/static/message 81、gateway 5、calibration 10、choice 工具 11 | [本地](raw/local/)、[服务器](raw/server/)、[数量及源码核验](raw/verification.json) |
| PG18.3 构建 | 独立 prefix，`-O2 -Werror`，无 warning | [build.log](raw/server/build.log)、[qualification.json](raw/server/qualification.json) |
| PG18.3 完整 TAP | 实际重跑 5 文件、1022/1022；本子切片未新增 TAP 项 | [tap.log](raw/server/tap.log) |
| PGXS regression | 1/1；actual/expected 在脱敏前逐字节相同 | [regression.log](raw/server/regression.log)、[公开 actual](raw/server/regression-actual.out) |
| 中立 C11 | 五个 C module 与 provider header 通过；消息测试另以严格 C11 链接并执行 | [命令/退出码](raw/server/qualification.json) |
| 证据与源码 | 38 个服务器公开文件、88 个源码 SHA 与测试提交一致；主清单含 81 个文件 | [服务器 SHA](raw/server/SHA256SUMS)、[总清单](raw/SHA256SUMS) |

本次确实重新构建并运行 PG18.3；同为 1022 项不表示挪用旧身份切片证据。完整测试覆盖的是现有
recording Map/Filter 与 exact/choice Filter 的 SQL、INSERT、NULL、LIMIT、权限/计划失效、取消和错误恢复。
不能将它们当成尚未接入的生成型 Map 的 PG 行为验证。

## 失败与恢复

- [首个 red](raw/local/red.log)：旧 Filter 通过，Map API 尚不存在。后续 UTF-8、长度、Python
  消息及 Python 验证按先失败后通过保留阶段日志。
- [Python 新函数 red](raw/local/red-python.log)同时发现一处测试加法笔误：最大非转义文本长度应为
  `4096 + 163840 + 61 = 167997`，不是 168997；更正预期，不修改语义或截断数据。
- 中间阶段标记 `dirty=true`，当时未跟踪文件未纳入其源码 SHA；这些记录只保留过程，不能作为完整
  可复现验收。最终 `final-*.json` 与服务器验收均绑定干净提交 `6903cf46`。
- 本地首次完整套件有 12 项、choice 工具另有 2 项被沙箱禁止 localhost bind；获准后全过，
  [失败记录](raw/local/postgres-sandbox.log)和[工具失败](raw/local/choice-tools-sandbox.log)保留。
- Git bundle 首次仅以短 SHA 作导出 ref 被 Git 拒绝，未生成文件；改用具名分支后核验成功。
  服务器同步首次被自动审批拦截，用户明确授权 Git 同步后才上传；未绕过审批。
  origin 的只读 SSH 查询未成功，因此本轮不声称重新验证了远端 main。

## 隔离、复现与结论范围

服务器原主工作树未移动；Git bundle 导入独立 detached worktree，明确核对官方 `REL_18_3`
commit `62d6c7d3df6287f1bd83199c1a746e50d31571a0`，先保存仓库外 runtime env 的 core preflight。
从已有 18.3 工具链复制到新的独立 prefix，构建/install 只写新前缀；原 `.so` 哈希保持。
测试由 `postgres` 用户运行，仅使用临时 fixture 和 socket；结束后测试进程已退出、regression PID
文件不存在。源码工作树干净，数据盘上的源码、独立二进制、机器报告及日志保留；不是服务器全局清理承诺。

复现：[服务器运行器](raw/server/qualify.py)接受 `--repo --root --base-prefix --env-file --pg-source --commit`；
在同一干净源码、新 artifact root 和精确 PG18.3 工具链下运行。本地用
`PYTHONPATH=code python -m unittest discover -s code/tests/postgres -p 'test*.py' -v`，
其余三个 discovery 命令见 `raw/local/final-*.json`；[本地记录器](raw/local/run_check.py)在落盘前脱敏。
公开 regression 副本去掉行尾空白；其哈希不与脱敏前 actual/expected 混用。

本步证明消息编码与现有路径兼容。完整纯值 spec/摘要、完成元数据与输出 policy、0.2.0/三参 Map、
schema 4、执行权限与参数前置检查、v5、PG＋golden、真实模型及资源验收仍未完成。
用户已同意继续自主研发；真实运行之前仍需确认具体模型/服务、样例、请求预算、停止条件、gateway
RSS 和清理时限。没有恢复 Filter 质量实验或成本校准。
