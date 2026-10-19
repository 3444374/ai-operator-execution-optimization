# 本地异步模型组件（可选）

本组件是 [本地模型实施路线](../../../../experiments/plans/数据组织.md#model-engine-roadmap) 的
CPU 合同切片。显式 Async actor／Core／CLI 已接入；同步 `VllmEngine` 和 baseline 默认保持。
当前交付不表示 records 或有限链已经跨阶段持续供给，也没有 GPU 或性能资格。
条款映射、固定官方源码与验证记录见[组件报告](../../../../experiments/results/diagnostics/local_async_engine_20261019/README.md)。

## 最小接口

- `VllmAsyncEngine(EngineSpec, GenerationSpec, limits=AsyncEngineLimits(...))` 必须在其所属
  asyncio event loop 和已核验的模型专用解释器中构造。`generate_prepared(row, request_id=...,
  generation=..., deadline_monotonic=...)` 每次消费一条已有文本 prepared row，返回共享结果 schema。
  直接调用者须在该实例寿命内使用唯一 request ID，并有界持有/消费结果。
- `AsyncEngineProcess(EngineSpec, GenerationSpec, limits=..., startup_timeout_s=...)` 是隔离解释器
  的进程桥；`submit(row, generation=..., deadline_monotonic=...)` 返回 `queued`、`backpressure`
  或 `rejected`。非法合同/单项超限抛异常，零交接。`queued` 仅确认父端已接管输入。
- `snapshot()` 返回持有请求、输入逻辑 bytes、结果预留/已缓冲 bytes、child accepted 和未知数；
  `poll(max_events)` 按到达次序转交有限的 `TerminalEvent`，包含完整调用身份、结果及
  `engine_terminal_confirmed`。未消费的已知结果继续占结果槽；消费已知结果后才恢复结果容量。
- `abort(request_id)` 仅返回控制命令是否已在父端排入；child 的 `accepted` 和 `abort_requested`
  均不是实际生成完成证明。已接纳的控制按前缀先于 close 发送，关闭冻结后新 abort 返回 false。
  `drain(timeout)` 等待本组件已知的活动工作，不消费结果，也不把超时变成完成。
- `close(timeout)` 有界等待 API shutdown、直接 child 退出与 I/O 线程关闭，返回 scoped 报告。
  `gpu_exit_confirmed`、`descendants_exit_confirmed` 当前为 `None`；外层模型 owner 必须继续核验和
  清理模型后代与 GPU，不能只凭 scoped `status=closed` 报告整个查询/模型资源已结清。

所有 deadline 使用同主机 `time.monotonic()` 的绝对时刻。owner 应传递覆盖准备、供给和收尾的查询期限；
启动/close timeout 只界定局部等待。这里没有重试、batch 选择、路由、调度算法或共享模型池。
一进程桥同时只持有一个 query；前一 query 的输入、结果或未知责任未结清时拒绝另一 query。
内部 ID 使用每 owner UUID 与单调序号；旧 abort 不作用于下一 query。

## 有界性与未知工作

`AsyncEngineLimits` 分别限制活动请求数、输入总/单项 bytes、未消费结果槽及总/单项预留 bytes、
data/control 队列数和 control frame bytes。每个已接受请求提前预留一个最大结果包；输出过大时发送
保留身份、usage、finish reason 和确认状态的小失败回执。身份过大、连小回执也放不下时在接受前拒绝。
这些是序列化逻辑字节与有限队列约束，不能当成 Python RSS、tokenizer 临时对象或 GPU 显存硬限。

data、control、terminal 使用独立 socket。父 I/O 持续读取并优先给 control 一次有限读写机会，
不等待调用者 `poll()`；每个通道最多另有一个正在发送的 frame。child 的 reader、writer 与生成
task 并行；原生 abort 使用一个独立 worker 加有限队列，不能卡住 close/drain 的读取。
结果队列最多 `result_slots`，abort 队列最多 `max_inflight`，各有一个 worker 当前项；
内核 socket 缓冲由操作系统限制，frame 以长度前缀核对上限。
串行 abort worker 的前一个原生调用若挂起，后一个控制可能尚未进入原生 abort；shutdown 可继续，
但不能据此确认该请求已取消。[确定性 CPU 反例与修补边界](../../../../experiments/results/diagnostics/async_cancel_close_20261019/README.md)
分别保存队列接管、原生调用和关闭的证据，不指定历史 r2 的实际竞争层。

固定 vLLM 0.25.1 在发送底层 ABORT 前会合成 `finish_reason=abort` 的最终输出。这类结果和生成进入后
的异常/期限保持 `unknown`，保留已有 usage、输入与结果预留并使 owner 不再接新输入。只有匹配的
正常实际终态才归还输入责任；取消竞态中迟到的正常终态仍保留。child 死亡保留已收到的已知回执，
只将未确认项标成未知；调用者消费未知通知不会清除未知工作责任。
期限通知不等待原生取消清理：每个已保留请求最多持有一个生成 task；到期后取消该 task、立即交付
unknown，并保留 task/输入责任，防止原生 generate 的 abort 等待吞掉期限。入口与渲染后均检查
实际截止时间。close 报告仍未退出的 `pending_request_tasks`，不会把挂起的 task 当成已清理。

## 集成 owner 接点

共享 `vllm_engine.py` 已由唯一 owner 在 `a4f47eab` 提取 `result_record`、`result_from_output`；
本实现直接复用它们及 `EngineSpec`、`GenerationSpec`、身份检查、sampling/logprobs 转换，不复制 parser。
显式 Async owner 已接入 actor 的生命周期和 Core backend：将派发交给 `submit`，将有限 `poll` 事件
关联到原任务，只在可信终态释放原责任；未知项进入既有失败/清理流程。baseline 的原生调度不共享 Core。

冻结82e的真实迟到供给 late3 成功与 r2 取消失败按原来源解释；新取消修补尚无模型资格，原 unknown、
2402＋1 tokens、held/ref=1 保留。原生 abort／shutdown 时序、新源 token／模板／日志及后代／GPU范围
由独立资格计划核验；不重启旧 late3＋cancel1 控制器。cascade 全局校准与 Extract 关系屏障保持原方法合同。
