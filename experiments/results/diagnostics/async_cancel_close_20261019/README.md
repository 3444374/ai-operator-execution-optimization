# Async r2 取消／关闭交接诊断与 CPU 修补

源码固定 `82e685b3233e6524732934bbf220fb5563e515f8`，修补提交
`ec0138354ccec436ab6319ba0308fa5b697ced4b`，职责分支 `fix/async-cancel-close-r2`。
本单元只读既有本机 r2 证据并运行 CPU 验证，没有访问服务器、启动模型或改动原资格包／账本。

**已复现并修补取消交接缺陷；不能确定真实 r2 的实际竞争顺序，原 r2 取消失败与 unknown 保持。**
模型下一步只建议一次新 cap1 定向观测，尚未分配或执行。

## 原 r2 已知事实与仍缺的观测

[r2_evidence_binding.json](r2_evidence_binding.json)记录只读来源及 12 件原件摘要。
其保存的 411 项生产源码逐件与 Git `82e685b3` 及源清单核同；没有用后续集成源码替换历史来源。
原始私有材料由 GPU owner 持有，公开文件不包含原提示文本、环境文件或服务器路径。

| 层次 | 原件实际支持的判断 |
|---|---|
| 自然迟到供给 | late 三次 succeeded；第三次输入／原生接纳发生于第一请求结束前。保留既有成功，不重跑 |
| 取消请求 | 一次原生接纳，约 10ms 后 driver 记录取消意图；query primary 为 `InterruptedError` |
| 调用结果与用量 | `unknown/AbortNotConfirmed`；2402 输入＋1 输出 token；Core held=1、retained ref=1，不能因关闭或物理退出释放其历史记录 |
| 观察器 | 只有 9 条事件，没有 `native_abort_requested`；该事件实际只在 `WitnessEngine.abort` 入口写出，不是原生 `AsyncLLM.abort` 的入口确认 |
| 关闭与退出 | 嵌套模型 API shutdown 为 true，actor engine close 为 false；原独立收据记录 73 个已观察出生身份退出及 GPU／SHM／RPC 清理通过。这里未重新检查服务器资源 |
| 资格与预算 | 原取消资格正确保留失败；四次永久额度已领取／使用完，不能退款、补成零或自动重试 |

原观察器包装了原生 add_request／generate，但没有包装原生 abort／shutdown。
因此，“没有 adapter abort 入口事件”与“native finish_reason=abort”不能确定谁触发了停止。
最少还缺一条以完整 Core TaskKey 关联 request_id 的时序：Core cancel callback／Ray RPC 发出、
actor abort 进入／返回与 close 进入、IPC 入队／完整发送／child 接收、abort worker 开始、
原生 abort 进入／返回及 shutdown 进入。空 observation-errors 也不证明这些路径已被覆盖。

## 确定性反馈环与最小改动

依据[模型引擎实施路线](../../../plans/数据组织.md#model-engine-roadmap)、
[模型 owner 合同](../../../plans/系统架构.md#model-engine-execution)和
[增量执行](../../../plans/增量执行.md)，复用既有 Core、actor、三通道 IPC 和结果账本。
没有修改作者 baseline、共享 Core／base owner、公开 selector、原生方法或模型参数。

| 可复现缺陷 | 原源码／反例 | 修补及边界 |
|---|---|---|
| 已接纳 abort 被 close 超越 | 父进程 abort 返回 true、队列已有一项；IO 仍优先发送 close，child 不再读取后续 abort。原源码 3/3 RED | 已排队 controls 前缀先发送；close 冻结后拒绝新 abort，不再误报接纳 |
| 取消 RPC 迟到时仅执行 shutdown | 新增 actor 反例在 FIFO 修补后仍 RED；生产 Core 组合也复现 query 已取消、晚 RPC 被关闭拒绝 | actor close 显式接管仍持有的 pending 调用，先交接 abort，再用同一期限剩余时间关闭原 engine |
| 交接容量暂满、异常或期限耗尽 | 专属用例覆盖控制队列满、abort 抛错及超时 | 重试已保留意图，不重发 accepted 模型调用；异常仍尝试关闭同一 child；未结清责任、句柄和用量保持 |
| child 串行 abort worker 被前一原生调用阻塞 | 两请求用例确认后一个已入队但尚未调用 engine.abort；shutdown 仍能运行 | 明确保留未确认边界，未重写 worker 或调度器，未将 shutdown 算作该请求已原生 abort |

生产改动仅在[async_engine_process.py](../../../../code/src/serving/backends/async_engine_process.py)和
[lotus_records_async_actor.py](../../../../code/src/execution_provider/adapters/lotus_records_async_actor.py)。
新增的 `abort_handoff` 只报告 `parent_control_queue_only`，列出关闭接管、已排队及未排队的完整 keys，
`engine_terminal_confirmed=false`；关闭接管不是原取消 RPC 已被接纳的证明。
原 RPC 迟到返回 false 仍为 false。abort 输出继续为 unknown，GPU／后代字段不扩张。

新增[IPC／actor 顺序测试](../../../../code/tests/serving/test_async_cancel_close_order.py)、
[CPU child witness](../../../../code/tests/serving/async_cancel_order_fixture.py)和
[生产 Core 组合测试](../../../../code/tests/execution_provider/test_async_cancel_core_close.py)，共 8 项。
CPU witness 的事件均带 fixture 前缀；真实模型路径未加入等待、hold 或采样改动。

## 固定源码验证与保留的失败

使用已有 Python 3.12.6、Ray 2.56.1、Arrow 24.0.0、Daft 0.7.21、LOTUS 1.2.4 和原 SemBench
`c814e380` checkout，无安装。各冻结运行的被测源码前后摘要一致；原输出落盘前脱敏后无损 gzip。
pytest 时间是测试耗时，不是查询性能或方法比较。

| 运行 | 结果与原始证据入口 |
|---|---|
| 当前测试＋精确基线方法回放 | [baseline_replay_red_20261019T124625Z.json](baseline_replay_red_20261019T124625Z.json)：4 个明确症状全部 RED；只在内存载入 Git 82e 的 `_io`、`abort`、actor `close`，不改磁盘 |
| 修后常规回归 | [final_cpu_20261019T124625Z.json](final_cpu_20261019T124625Z.json)：163 passed、65 subtests、7 个显式实库 skip；包含新增 8 项及同步 owner／Core／static、期限与公开 Async 入口回归 |
| 实际 Ray actor／CPU child | [real_ray_cpu_20261019T124844Z.json](real_ray_cpu_20261019T124844Z.json)：1 passed，零逻辑 GPU；不代替模型资格 |
| 实际 Core／Ray／Daft／LOTUS／原 evaluator | [real_library_cases.json](real_library_cases.json)：7 个既有方法均通过 CPU 固定响应，具体运行分别保存；其中 6 项有独立外层 supervisor 收据 |

实库首轮错误地把须 fresh driver 的 7 项合跑：首项通过后，Daft 全局 runner 使后 6 项在创建 runtime
前被守卫拒绝。该[失败原件](real_core_libraries_20261019T124843Z.json)保留。
没有放宽守卫或改生产代码；仅将后 6 项改为各自新的受监督 driver，全部通过，remaining／cleanup errors
均为空。首项未重跑，其原进程没有这 6 份外层 supervisor 的出生身份观测范围。
private supervisor 原件按 run ID、成员、字节数、SHA 记录于每次 JSON，由本 CPU 执行 owner 提供；
它们只确认已观察自有 OS 身份，不认证 GPU 或未观察后代。

初期三次 FIFO RED、独立 actor RED、晚 abort 误接纳 RED 的原日志和运行身份同目录保留；
早期新测试文件后来补充了用例，旧文件摘要不冒绑最终文件。最终基线回放使用已提交的当前测试，
提供可重复的判红入口。[辅助脚本准备错误](fresh_driver_helper_setup.json)发生于子进程启动前，0 次测试／模型执行。

从仓库根使用已有 CPU 环境复现：

```sh
# 原缺陷回放，预期四项失败；不修改文件。
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code python experiments/results/diagnostics/async_cancel_close_20261019/replay_baseline.py
# 修后8项专属回归。
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=code python -m pytest -q -p no:cacheprovider code/tests/serving/test_async_cancel_close_order.py code/tests/execution_provider/test_async_cancel_core_close.py
```

完整常规命令见运行 JSON。实库必须按每个 node-id 新建受监督 Python 进程，使用
`RUN_LOTUS_RECORDS_ASYNC_CPU_LIBRARIES=1` 与已核验的 `SEMBENCH_CHECKOUT`，不能整类合跑。
本轮常规组合中的原生模型边界及 Ray／Daft 组合 fixture 与后续实际库资格分开解释。

## 独审、定向复测与交接

[reviews.json](reviews.json)保存两名独立审查者的固定范围、报告及测试范围。
Standards 对新增与受影响旧路径共 48 项／9 子测试 PASS；Spec 对新增与旧 actor/Core 共 25 项 PASS。
二者均确认 unknown、用量、完整身份、期限和物理退出范围保持，并明确实际 r2 交错仍未定。

[targeted_retest_plan.json](targeted_retest_plan.json)建议唯一 GPU owner 新建一次 cap1 取消单元，
保留原 cancel 消息、模型／模板／generation／窗口；不重跑 late 三次，不重置旧预算。
先独审新的 cancel-only helper／观测器，并绑定实际原生 abort／shutdown 方法来源及分层事件。
原控制器假定 late3＋cancel1，不能原样重启。取消窗错过、观测缺失、首次异常或期限到即停止，
保留已耗额度，不自动再试。关闭发起的 abort 与直接取消 RPC 各自记录；即使原生 abort 入口可见，
也不把 abort return、shutdown 或独立物理退出当成请求终态确认，不回写原 r2 失败。

集成者选择本补丁后串行登记 adapters／backends／diagnostics README、`INFRA_STATUS.md`、证据台账和
`PROJECT_LOG.md`。真实模型定向复测、原生方法进入时序及 GPU／后代资格仍待执行；
本包没有完整查询性能、质量或 GPU 执行重叠收益结论。
