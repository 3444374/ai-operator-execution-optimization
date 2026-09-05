# SemMap 资源测量工具修复与验证（2026-09-06）

本文件是内部工程验收记录，研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
当前实施入口仍是 [Map 合同 §8.4.3](../../../plans/postgresql_semmap_generation_contract.md)，本文件记录实际执行证据。

历史验证/审计驱动已从工作树退役，源码链接指向已推送的 `d93e3f9b`；[退役源码索引](../retired_sources.json)
保存原文件 SHA-256 和 Git blob。原始审计、失败记录和历史测试数量保留，`PUBLIC_SHA256SUMS.json`
校验当前保留文件。下文旧驱动命令需在固定 Git 版本执行；当前实验使用 `code/` 公共入口。

**结论：受限重写和已授权的小规模验证完成。** 实际 PG18.3 1×100 fixture-only 诊断的四组场景、九个必需阶段均有效且通过；正式 3×2000 未运行、未授权，四 D 整体仍未完成。模型请求为 0；本轮结果不证明模型质量、异步调度能力或性能收益。

## 1. 实现判断与提交身份

原实现的主要问题在测量和编排：目录创建晚于编译、客户端先退出再测清理、未完成阶段与观测缺口混用、恢复覆盖先前失败，以及 socket 归因跨窗口配对。这会同时产生假通过和合法运行永远无法通过的问题。

从资源所有权出发，每个阶段必须知道“哪个存活进程、哪个资源、何时观察、何时关闭”；从证据出发，操作失败和采样失败必须分别保存，再由全部必需阶段生成结论。因此保留已经验证的 PostgreSQL 语义载体与 wire，重写工具的生命周期和聚合，定点修正采集与归因。无需回退整个 SemMap 实现。

| 身份 | 内容 |
|---|---|
| 审查起点 `e5f4dd12` | 用户最新 `semmap-resource-v2`；没有 reset、覆盖旧分支或改写旧证据 |
| `0e1c6c1c` | 进程观测、重试历史、稳定基线与 session 归因修复 |
| `836448ab` | 独占目录、阶段生命周期、统一聚合及受控验证；首次目标诊断 |
| `0d98d2d7` | 独立 fault fixture 等待同批两端观测，再放行握手 |
| `72c665a0` | 修正测试自身对采样线程交错的依赖；第二次诊断 |
| `77a123de` | 区分首次连接缺失路径和已建立连接断开的 SQLSTATE；第三次诊断与最终运行时代码 |

工作分支为 `codex/semmap-resource-repair`。其后只补充 listener 基线回归测试与结果文档；诊断身份始终绑定 `77a123de21af2f19eacad207a310109393d0894c`，不倒填为后续文档提交。生产 PG planner/executor、provider、Map 语义、wire v5、模型配置和调度层未修改。

源码主要落点：[runner](../../../../code/src/experiments/postgresql/semmap_resource_runner.py)、[阶段采集](../../../../code/src/experiments/postgresql/resource_phase.py)、[纯结果聚合](../../../../code/src/experiments/postgresql/resource_lifecycle.py)、[归因](../../../../code/src/experiments/postgresql/provider_session_attribution.py)、[采集/记录器](../../../../code/src/observability/process_resources/recorder.py)。CLI 删除了原来被忽略的必填 `--client`，统一编译源码管理的参数化 C client。

## 2. 真实测试结果与受控证据

| 测试类别 | 本地 macOS | Linux |
|---|---:|---:|
| 生命周期、runner、observer、判定 | 54 通过 | 54 通过 |
| session 归因 | 17 通过 | 17 通过 |
| procfs/记录器/跨进程 integration | 33 通过、2 跳过 | 35 通过 |
| 采样异常和中断 | 8 通过 | 8 通过 |
| 旧 v1 重放 | 5 通过 | 5 通过 |
| 受影响的 choice 兼容 | 未在本地执行 | 11 通过 |
| 合计 | **119 项：117 通过、2 跳过** | **130 项通过** |

其中完整组的 118/129 项绑定 `77a123de`；随后新增 1 项 listener 基线回归，在两平台分别执行，包含缺失、身份持续变化、accepted 未清理三个子情况。统计不重复计算旧测试；[附加测试身份](raw/verification-final/additional-baseline-regression.json)记录测试文件 SHA。
两项跳过的理由都是 `Linux procfs/SO_PEERCRED required`。Linux integration 由独立采集进程观察两个子进程，验证 listener/client/accepted、无关普通 FD 和同一存活身份的清理。

[本地完整组](raw/verification-final/local-tests.json)、[Linux 完整组](raw/verification-final/server-tests.json)、[验证脚本](https://github.com/3444374/ai-operator-execution-optimization/blob/d93e3f9b58b4ecfedd46b32754d69c82b4ed3dc6/experiments/results/postgresql/semmap_resource_lifecycle_20260906/verify_tests.py)记录模块、版本、数量和日志哈希。C 客户端还实际通过[乘法溢出拒绝检查](raw/verification-final/client-overflow.json)，在连接之前退出。

初始已有测试是 process 33 项（2 跳过）、resource 46 项（本地缺 psycopg，3 个导入错误）、attribution 12 项；随后五个行为反例产生 3 failures / 2 errors，暴露不稳定基线、首末采样异常和异常正文持久化。修复后的第一组 [115 项历史检查](raw/local-tests.json)保留原记录，不能与最终数量混写。现在 psycopg 延迟到实际 PG adapter 导入，受控测试无需安装该依赖。

[受控执行摘要](raw/controlled-evidence.json)与三套完整文件分别保存：

- [成功路径](raw/controlled_success/summary.json)：真实创建目录和 fake executable/log，实际执行 phase 与生产聚合，退出 0。
- [操作失败](raw/controlled_failure/phase_report.json)：保留 operation raw，结果 failed。
- [采样不完整](raw/controlled_incomplete/phase_report.json)：保留已有采样，结果 not_evaluated。
- 旧目录拒绝返回 3；sentinel/summary 的前后哈希一致，preflight/compiler 调用均为 0。

这些目录的 producer 是 `synthetic_process_observations`。成功目录的 formal 配置只测试汇总/退出码，未实际运行 PG 或 3×2000；不能据此宣称正式资格。旧 v1 重放仍复现 observed 3 / limit 2 的失败。

保留的中间失败包括：PG 头文件在额外 `-Wextra` 下的警告、清理中断测试对线程交错的错误假设，以及最终测试驱动遗漏工作目录导致 CLI 找不到脚本。[错误工作目录的测试计数](raw/verification-final/server-tests-wrong-cwd.json)仍在；修正测试驱动后完整组通过。没有用重试删除失败历史。

## 3. 隔离 PG18.3 诊断

已有外部 runtime env 的 `core` 只读预检通过；复用现有 driver，未安装或下载依赖。复制已有 PostgreSQL 18.3 toolchain 到本轮私有目录，扩展以 `-O2 -Werror` 构建并仅安装到该副本。独立 cluster、socket 和源代码检出均由本轮持有；已有服务没有改动。toolchain、扩展、C binary/source 与预检 SHA 见 [诊断审计](raw/verification-final/diagnostic-audit.json)和[构建审计](raw/verification-final/history-and-build-audit.json)。

三次诊断使用不同目录，都是 **实际 1 轮 × 100 行**，每行输入 100000 bytes、输出 65536 bytes。客户端报告 warmup、round_complete rows=100、all_complete rounds=1/rows=100；压力阶段另有 100 个 task 与 100 个 task_complete，warmup 明确不属于压力窗口。

| 运行 | 源码 / 测量修订 | 结果与处置 |
|---|---|---|
| d1 | `836448ab` / phase-lifecycle-1 | 压力、取消/恢复通过；瞬时 disconnect 无采样覆盖，后续跳过，整体未评价 |
| d2 | `72c665a0` / phase-lifecycle-2 | 九个阶段测量均有效、资源通过；absent 期待 08006、实际 XX000，整体 valid/failed |
| d3 | `77a123de` / phase-lifecycle-3 | 同一完整运行内四场景九阶段 valid/passed；diagnostic_status=passed |

每次 CLI 退出码都是 2；diagnostic 的 phase/case/run 顶层 qualification_status 都是 not_evaluated，实际评价保存在 `assessment` 与 `diagnostic_status` 中。d3 的通过没有拼接前两次结果。

d1 后先登记了 `observe-before-handshake-v1`：独立故障夹具在 fault/recovery 连接握手前等待同批观察两端，最多 5 秒。超时取消并 join 查询；被动 observer 只记录，压力阶段不加等待。该方法支持关联和错误清理，不评价自然连接时延。

d2 的 SQLSTATE 差异单独核对生产源码与既有 [PG TAP](../../../../code/postgres/semloom_pg/t/001_semloom_pg.pl)：missing UDS provider 明确期待 XX000（1911 行），已建立连接后的 disconnect 期待 08006（1988 行）。`77a123de` 只修正对应场景的测试期待，没有扩大允许集合或更改生产错误映射；d2 仍是失败。

| d3 必需阶段 | 评价 | 同批 socket 合计峰值 | 结束状态 |
|---|---|---:|---|
| stress | valid/passed | 2 | 原 backend + gateway 清理通过 |
| client_exit | valid/passed | 不适用 | cleanup 后才发 finish，客户端退出 0 |
| cancel | valid/passed，57014 | 2 | 原 backend + gateway 清理通过 |
| cancel recovery | valid/passed，完整输出 | 2 | 同一对进程清理通过 |
| disconnect | valid/passed，08006 | 2 | 旧 gateway 仍存活时清理通过 |
| disconnect recovery | valid/passed，完整输出 | 2 | 原 backend + 新 gateway 清理通过 |
| gateway alive | valid/passed，完整输出 | 2 | 旧 gateway 仍存活时清理通过 |
| gateway absent | valid/passed，XX000 | 0 | 仅原 backend 清理通过 |
| gateway recovery | valid/passed，完整输出 | 2 | 原 backend + 新 gateway 清理通过 |

各 cleanup 的所有 tick 均保持本阶段 baseline 的 PID/start-time；最终 FD identity 集合与原基线一致，total FD/thread 增量均为 0，provider socket 残留为 0。压力 backend 的 PID/start-time 为 `(4972,469702221)`，gateway 为 `(4970,469702209)`；故障序列 backend 为 `(4969,469702195)`，替换 gateway 的新身份分别登记。运行结束后，对三次运行保存的进程身份逐项只读复查，无本轮进程仍存活，postmaster PID 文件均已移除。

压力采样为 26 个顺序观察批次；backend/gateway sampled RSS peak 增量为 290816/503808 bytes，end 为 24576/0 bytes。压力窗口保留16次带错误的先前FD读取尝试；重试后的最终观察均一致有效，没有未解决的partial/invalid。此前失败尝试仍在原始attempt记录中，不充作连续覆盖。每个窗口完整状态计数、retry历史错误数、全部阶段资源结束值都在机器审计中，不能把“采样有效”解释为连续微秒观测。

压力期间另见 backend 新 eventpoll：fd=18、inode=15484，5 次采样可见，时间范围 4697022477480284–4697022994712073 ns；cleanup 时消失。该数字身份属于此运行，不是可移植常量。其存在解释“两个 session socket”和“进程总 FD 合计多三个”可同时成立；实际创建来源仍是待验证假设，未取得调用栈/UNIX_DIAG_PEER 证据。socket 专用 2/0 检查不表示全部 UDS 配套 FD 只有两个。

## 4. R1–R7 与 T01–T20 对应关系

测试文件缩写：[L：生命周期](../../../../code/tests/experiments/test_resource_lifecycle.py)、[R：runner](../../../../code/tests/experiments/test_semmap_resource_runner.py)、[S：采样](../../../../code/tests/observability/test_sampling_lifecycle.py)、[A：归因](../../../../code/tests/experiments/test_provider_session_attribution.py)、[P：procfs](../../../../code/tests/observability/test_proc_collection_validity.py)、[Q：判定](../../../../code/tests/experiments/test_resource_qualification.py)、[B：基线](../../../../code/tests/observability/test_process_recorder.py)。以下均为行为测试；涉及组合的条目同时验证纯聚合与实际 phase 文件，不以源码字符串代替执行。

| 修复项 | 实现落点 | 验证 |
|---|---|---|
| R1 目录所有权 | runner.run | T01–T03、旧目录真实 CLI 拒绝 |
| R2 先清理同一 backend 再退出 | runner.stress_case / client_exit | T07、d3 stress 与 client_exit |
| R3 进度与终态质量分开 | resource_lifecycle.assess_phases | T04–T06、T10、T19 |
| R4 所有故障与恢复参与汇总 | resource_phase.execute_phase / required phases | T11–T12、d2 保留 absent 失败、d3 九阶段 |
| R5 采样错误/中断先保存 | recorder.record_operation / checkpoint | T08、首采无效、operation/cleanup 中断 |
| R6 cleanup 需连续有效窗口 | resource_phase.cleanup_settle | T09、d3 全 cleanup tick 与原身份 |
| R7 session/进程/资源/时间绑定 | provider_session_attribution | T13–T18、Linux 跨进程 integration |

| 测试项 | 对应可执行测试或组合 |
|---|---|
| T01 新目录与真实编译文件副作用 | L `test_new_root_compiles_real_files_and_runs_all_phases_to_zero` |
| T02 旧目录所有字节不变 | L `test_existing_root_is_byte_identical_and_no_actions_run`；CLI 独立子进程测试 |
| T03 preflight/编译失败落盘 | L `test_preflight_nonzero_and_build_failure_write_owned_summary` |
| T04 合法完整输入可通过 | L `test_new_root_compiles_real_files_and_runs_all_phases_to_zero` |
| T05 cleanup pending 可成为通过 | L `test_pending_cleanup_becomes_pass_only_when_completed` |
| T06 缺口不可由后续好样本覆盖 | L `test_real_phase_positive_failure_and_gap`、`test_valid_overlimit_sample_retains_failure_beside_partial_tick` |
| T07 finish 前同一存活 backend | R `test_cleanup_samples_same_live_backend_before_finish` |
| T08 首/中/末非 OSError 与中断 | S 对应 `first_sample`、`middle_non_oserror`、`final_sample`、`operation_interrupt` 测试 |
| T09 cleanup 首个坏 tick 不被丢弃 | L `test_cleanup_bad_first_tick_is_retained_and_cannot_end_poll` |
| T10 空集/缺项/缺观察/缺角色 | L `test_missing_and_empty_required_sets_never_pass`；Q empty/invalid/missing-role 测试；S 首采空角色 |
| T11 recovery 失败与 absent 泄漏 | L 实际 failure phase + `test_invalid_original_survives_failed_recovery` + `test_absent_backend_leak_fails_without_gateway_requirement`；d2 实际 absent 失败保留 |
| T12 原 invalid 与 failed recovery 同时保留 | L `test_invalid_original_survives_failed_recovery` |
| T13 一段可见不能代表另一 tickless session | A `test_one_observed_session_does_not_qualify_another_tickless_session` |
| T14 跨 tick 端点不可配对 | A `test_endpoints_in_different_ticks_cannot_be_paired` |
| T15 FD 复用/错误重分类/结束后残留 | A `test_reused_baseline_fd_with_new_inode_is_new_candidate`、`test_unknown_reuse_and_other_session_are_not_reclassified`、`test_post_session_residual_remains_in_cleanup_evidence` |
| T16 thread/listener/accepted 不稳定基线 | S thread/active-socket；B `test_fd_identity_replacement_is_not_stable`；L 新增三种 listener/accepted 反例 |
| T17 diagnostic 不污染 formal 配置 | L `test_diagnostic_does_not_mutate_subsequent_formal_spec`；真实 CLI 解析测试 |
| T18 finally/孤立/重复/错误 task terminal | A `test_duplicate_orphan_and_unclosed_events_are_rejected`；L `test_mismatched_completion_id_cannot_qualify`；observer 实际异常方法测试 |
| T19 phase/case/run/mode/退出码一致 | L `test_diagnostic_phase_case_and_run_agree`、正常退出0、旧目录退出3；d1–d3 全部诊断退出2 |
| T20 重试历史保留且最终观察不累积污染 | P `test_race_then_agreement_retains_attempt_history_without_poisoning_final_read`、persistent-churn 测试 |

双视角复核中，规范审查发现的 client-exit 报告不一致、无效首采仍运行、中断丢 baseline/cleanup 三项已修复并复核关闭；需求审查确认握手放行不替代严格归因，所有必需阶段仍参与最终评价。审查本身不是目标环境证据。

## 5. 原始证据、公开摘要与复现

三次原始诊断继续保留在服务器的独立 d1/d2/d3 根目录。未批量导出原始日志、payload 或私有 FD 路径。公开的 [diagnostic-audit.json](raw/verification-final/diagnostic-audit.json)由 [audit_diagnostics.py](https://github.com/3444374/ai-operator-execution-optimization/blob/d93e3f9b58b4ecfedd46b32754d69c82b4ed3dc6/experiments/results/postgresql/semmap_resource_lifecycle_20260906/audit_diagnostics.py)在服务器只读复算，只包含允许字段；这是派生摘要，与原始 raw 不是同字节文件。

机器审计列出原始 selected-file SHA 和每个原始 SHA256SUMS 清单自身的 SHA；d1/d2/d3 分别校验 1093/1190/1190 个清单条目（含嵌套重复条目），不匹配数均为 0。其余原始日志保留在原处，通过哈希引用。公开文件自身的 SHA 单独见 [PUBLIC_SHA256SUMS.json](PUBLIC_SHA256SUMS.json)；[本地日志转换记录](raw/verification-final/local-log-provenance.json)区分原文件与公开副本 SHA。

旧 v2 diagnostic raw 也已只读核对：[历史审计](raw/verification-final/history-and-build-audit.json)。旧 summary 的 expected100/observed6000、tasks101/6001 差异与记录一致；stress 原始过程含 valid2534/partial266，cleanup 含 invalid2/valid2；该 artifact 根下没有找到声称的 checksum/source/environment 清单。旧 README 的数字与 `a4119e73` 身份均保持历史身份，不为新实现背书，旧 v1 failed 不改。

复现受控测试（无需模型；Linux choice 组需要现有 driver 依赖）：

```bash
PYTHONDONTWRITEBYTECODE=1 python <result-dir>/verify_tests.py <repo> <new-test-output>
```

复算私有诊断摘要（只读；输出到另一个新文件）：

```bash
PYTHONPATH=<repo>/code python <result-dir>/audit_diagnostics.py <d1> <d2> <d3> \
  --toolchain <private-pg-prefix> --preflight <private-preflight.json> > <new-audit.json>
```

实际 diagnostic 的调用形状如下，凭据和外部路径由获授权操作者在仓库外填写；root 必须不存在：

```bash
PYTHONPATH=<repo>/code <driver-python> <repo>/code/scripts/experiments/run_semmap_resource_checks.py \
  --repo <clean-source> --root <new-short-root> --prefix <private-pg18.3-prefix> \
  --commit 77a123de21af2f19eacad207a310109393d0894c --diagnostic
```

## 6. 当前未完成项

R1–R7 的实现问题已关闭，小规模目标环境验证已完成。正式 3×2000 仍须用户单独授权，并使用固定源码/测量修订、同一完整运行的全部必需场景；本轮不执行。eventpoll 创建调用链仍未证实，归因方法仍限同步单 session；这些限制已明确记录，没有被扩展成通用精确 FD 拓扑或性能结论。

以后变更 PG/provider 多在途、重排或调度接入时，应沿各自真实路径重新验证，而不能借用这次 fixture-only 资源诊断。完整 SemMap/四 D、质量/成本及性能状态继续由主计划和证据台账维护。

<a id="reuse-refactor"></a>

## 7. 后续共享观测与配置重构

来源为 `66d23963` 后的维护性重构，实施依据为 Map 合同 §8.4.5。复核确认已有 PG runtime、
消息编码、provider 和 HTTP adapter 由 Filter/Map 共用。删除的是实验入口的重复观测、故障包装
以及 gateway 全局替换；不删除各算子 placement、prompt/parser 或版本化 wire 校验。
预算 ID/总上限、隔离 PG 用户/端口改为调用配置，C 客户端读取实际连接参数。

[本地测试清单](raw/refactor-local-tests.json)共212项：210通过，2项Linux专属跳过；包含
v3/v4/v5 × golden/合成HTTP六条接线、跨重启/并发/身份拒绝预算、观测异常关闭与不同连接参数。
记录每组模块及日志SHA；HTTP只返回合成响应，模型请求0。目标PG18.3头文件/libpq下
fixture客户端 `-O2 -Wall -Werror` 编译通过。该测试源码在提交 `b7eeea53` 中固定，
同一组 [Linux测试](raw/refactor-linux-tests.json)212项全部通过。

[隔离PG审计](raw/refactor-diagnostic-audit.json)绑定 `b7eeea53` 的干净源码、PG18.3、
schema v2.1 / phase-lifecycle-3，实际使用新端口55499。1×100、100000/65536-byte输入/输出的
四场景九阶段全部valid/passed，diagnostic退出2且正式qualification保持not_evaluated。
原始哈希1190项（含嵌套重复）全部匹配，8个已观测backend/gateway进程均退出，PG pidfile已消失。
新预算实现只读验证旧32/32账本成功，文件SHA前后一致，没有新预留或模型请求。
服务器数据盘空间与core预检通过；使用既有隔离PG工具链和driver，无安装/下载或服务配置变化。
原始新run保留在服务器独立reuse1目录；公开版仅收录允许字段的派生审计及哈希。

复现时使用上述CLI，源码改为 `b7eeea536c1f9c57faadb01ccdc8b4ae6658e50d`，增加
`--pg-port 55499` 并选择新目录。操作系统用户可用 `--pg-user` 指定；不同用户名的参数传播
有受控测试，本次实际目标用户仍为既有测试用户，不宣称完成另一真实OS用户的运行。

需求复核发现的 fixture 入口可绕过账本，以及规范复核发现的运行配置未入 manifest，均已修正
并用行为测试验证。旧 choice 的默认账本格式、CLI、各版本消息与错误行为保留；fixture CLI
拒绝 fixed-model 是唯一明确收紧，真实配置须走预算入口。Linux 专有 peer/socket 信息在其他
平台记录不可用，不能据此声称资源归因通过。历史真实脚本字节保持原状，后续执行不再依赖它们。

该重构不改变上面 d1/d2/d3 或真实模型运行的结论，也没有增加真实请求或正式资源资格。

<a id="retirement"></a>

## 7. 合并前退役代码清理（2026-09-06）

依据 [Map 合同 §8.4.7](../../../plans/postgresql_semmap_generation_contract.md)，从已推送的
`d93e3f9b` 清理 19 个退役文件：5 个兼容入口、1 个仅重放旧错误的测试、13 个历史实验驱动。
删除文件原有 2208 行；另外删除 golden session 转发与 adapter 旧身份属性 fallback。
四个 TAP 直接启动公共 gateway CLI；Python 调用方使用公共协议、session 和明确预算的通用账本。
SQL recording/reference、wire v2/v3/v4/v5、当前 Filter 校准/质量代码和资源判定反例保留。

[退役源码索引](../retired_sources.json)逐文件登记固定 Git blob、SHA-256 和源码 URL，已与
`git show d93e3f9b:<path>` 的原始字节核验。历史数据、失败记录和当时哈希不改写；历史 raw 中
的旧 import 只在其报告绑定的完整 Git 版本执行，不作为现役依赖。本次没有合并 main。

运行时代码提交为 `fcd1237398f10f136c4f43d82935bf3cef612e04`。本地受影响组 233 项中 231 通过、
2 项因 Linux procfs/SO_PEERCRED 跳过；Linux 同组 233/233 通过。core 预检通过，PG18.3
`-O2 -Werror` 构建成功，安装件与构建件 SHA 相同；四个迁移后的 TAP 文件实际执行 1297/1297
通过，测试进程残留 0。当前公共入口覆盖 recording、exact/choice Filter、Filter INSERT 与 Map；
本次没有执行其余 TAP 文件或 PGXS SQL regression，也没有新增模型请求。

[验证摘要](raw/retirement-verification.json)保存每组模块、数量、日志 SHA、TAP 输出和编译件 SHA；
完整日志在仓库外，公开摘要不含机器连接信息。对应组可按摘要的 `modules` 列表使用
`PYTHONPATH=code:code/tests/experiments:code/tests/observability:code/tests/postgres:code/tests/execution_provider`
运行 `python -m unittest <modules> -v`。TAP 使用 README 的 PG18.3 安装环境，以
`make PG_CONFIG=<pg18.3>/bin/pg_config REGRESS= PROVE_TESTS='t/001_semloom_pg.pl t/003_choice_execution.pl t/004_filter_insert.pl t/007_map_execution.pl' installcheck`
只选择四个受影响文件，作为非 root 用户运行。历史清单的 71 个本地条目与 4 个 Git 源码条目均
匹配原 SHA。初次局部测试暴露了测试自身在 bind/listen 之间抢连的时序问题，已改为有时限的
实际连接等待，随后局部组 73/73 与完整组通过；不把删除测试导致的计数变化当作能力增加。
既有真实模型结果仍绑定 `b7eeea53`，本次结果不替代正式资源、质量或性能实验。
