# Git 同步的记录查询资源观测

采集器和检查入口随仓库提交同步；模型、数据、机器绑定和全部回执留在 Git 外。后续更新不需要
重新上传 Python 包、手工填写七份代码摘要或复制 CPU 资格材料。入口只做配置、观察与证据校验，
不增加生产依赖或调度策略。来源为已审采集器 `3b7991c1`，逐文件来源见 [SOURCE.json](SOURCE.json)。

工作流沿用既有记录查询检查的 Git 身份、CPU 资格、受监督执行方式。运行仍复用
`control_resource_C2.py`、原 budget/lease/supervisor；旧文件名仅为兼容，C 从私有配置读取。
采集器从工具 checkout 加载，被测代码从独立的 `source_root` 加载，两者分别绑定准确提交，
不要求为了重现旧代码把测试工具覆盖进旧 checkout。controller 也在被测解释器的新进程中运行，
不复用工具进程缓存的 `src` 模块。

## 同步、资格和最小 on/off 配置

在独立、干净的工具 checkout 同步已经审查的提交；命令不会自行联网、安装或连接服务器：

```sh
git fetch origin "$VERIFIED_TOOL_COMMIT"
git switch --detach "$VERIFIED_TOOL_COMMIT"
TOOL=code/scripts/profiling/record_observation/workflow.py
"$DRIVER_PYTHON" -B "$TOOL" qualify --tool-commit "$VERIFIED_TOOL_COMMIT" \
  --config "$PRIVATE_BASE_CONFIG" --output-root "$NEW_CPU_DIRECTORY"
```

`qualify` 跑本仓 CPU fixture、单请求及 token 批路径的 off/on 公共基类探针，不启动 Ray RPC、真实模型或 GPU。
旧版被测源码没有批 API 时，该探针明确报告不支持，`CPU-READY.handoff_CPU_paths.token_batch=false`，
不能将单请求资格解释成该版本的批路径资格。
失败输出保留，未通过不产生 `CPU-READY.json`。工具提交、被测源码、基准配置和七份 runtime
代码摘要都绑定在该新回执中，后续变化须重新资格检查。Movie 路径在被测源码的新进程中调用原
`check_plan` 零模型预检，冻结每个 manifest／输入的摘要和完整调用数；不只冻结文件路径。

`PRIVATE_BASE_CONFIG` 使用原有成功运行的仓外配置。仍由现场一次性保留并绑定的内容包括：

- `source_root`、`expected_source.head/tree`：独立、干净的被测 Git checkout。
- `host_helpers`、`host_config` 的路径／SHA：既有站点适配及配置，含设备与监督绑定；不进仓库，
  不随每次测试重传，也不把它们改造成新的主机控制框架。
- `engine`／`generation`／`resources`／`ray_options`／`runtime_binding`／`environment`：原部署、
  输入和并发配置。Movie 使用原 `movie_plan`；有限链使用原 CSV／SHA／列绑定及完整方法。
- `workload.expected_calls`、`units`、`limits`：完整调用上限和原操作时限；native call cap 必须
  相符。准备过程不改模型、C、请求／生成参数或业务工作量。
- `artifact_root`、`run_id`、`cell_id`：基准配置已有身份；准备新格时改为显式的新身份和仓外目录。

仅新增的最小观察片段见 [handoff.example.json](handoff.example.json)：`profile=handoff`，
off/on 只改变 `mode`。事件数量与单事件字节是操作保护，不是性能调参建议。
`full` 保留原 Nsight、CPU/GPU 和 socket 观察能力，所需 native 源码 hash、版本、采样与存储边界
继续由实际私有配置给出；不能省略或猜测。

用同一个基准配置和资格回执准备两个新格；只改变观察模式和产物身份：

```sh
"$DRIVER_PYTHON" -B "$TOOL" prepare --config "$PRIVATE_BASE_CONFIG" \
  --ready "$NEW_CPU_DIRECTORY/CPU-READY.json" --mode off --profile handoff \
  --cell-id "$OFF_CELL_ID" --output-root "$NEW_OFF_DIRECTORY"
"$DRIVER_PYTHON" -B "$TOOL" prepare --config "$PRIVATE_BASE_CONFIG" \
  --ready "$NEW_CPU_DIRECTORY/CPU-READY.json" --mode on --profile handoff \
  --cell-id "$ON_CELL_ID" --output-root "$NEW_ON_DIRECTORY"
"$DRIVER_PYTHON" -B "$TOOL" check --config "$NEW_ON_DIRECTORY/PREPARED-CONFIG.json"
```

预算身份使用原 API 校验，冒号、路径字符及基准旧身份会被拒绝。已有输出目录、账本、资格和原件均不覆盖。
被测代码、输入或站点绑定改变后会拒绝旧回执；只在代码目录放未跟踪 overlay 也会被拒绝。
`prepare` 保留原并发配置，不沿用旧矩阵生成器去修改 C、native 容量或调用数。

## 由唯一执行者运行

运行前遵守[现有环境与物理清理流程](../../../../deploy/runtime/README.md)，确认上一任务完成、
GPU／进程空闲及 lease 已释放。下列入口继续领取原 lease 和额度，没有额外调度器、隐式重试、
模型 warmup 或并行执行：

```sh
"$DRIVER_PYTHON" -B "$TOOL" run \
  --config "$NEW_ON_DIRECTORY/PREPARED-CONFIG.json" --execute-model
```

`--execute-model` 是显式执行参数，不是再次询问用户的审批流程。`control_resource_C2.py` 和
`run_resource_C2.py` 是此入口的内部监督／worker，不以直接运行 worker 绕过 Git 与资格核查。
当前 GPU 队列和私有材料不会由本工具删除或接管。

## 原件回收和观察资格

进程停止写入后，对整个结果目录制作独占创建的逐文件字节清单；失败尝试也会被保留。
回收后使用同一份清单校验，不要求重新运行模型：

```sh
"$DRIVER_PYTHON" -B "$TOOL" manifest --root "$FINISHED_CELL" \
  --output "$FINISHED_CELL/ORIGINALS.json"
"$DRIVER_PYTHON" -B "$TOOL" verify --root "$RECOVERED_CELL" \
  --manifest "$RECOVERED_CELL/ORIGINALS.json"
```

清单复用既有流式 `file_identity`、私有文件权限和禁止覆盖的写入原语；拒绝逃逸／重复路径、
符号链接、缺失、增加或变更的文件及损坏清单。清单与原始日志／输出都留在 Git 外。
文件字节校验通过不表示观察完整、模型质量通过或物理退出确认；`capture_complete` 保留未知。

handoff 模式只要求实际 driver 和 Ray model actor 两个角色的最终 sink。有限 MMF 没有 prepare
actor，model/GPU worker 也不在轻量模式发事件。缺角色、缺 status／flush、丢事件、写错误、
无法唯一关联请求或未知 close，均不能拿“已收到的 sinks”子集冒充完整。物理退出和 lease
由原 supervisor 单独核对。

full 与 handoff 的 actor sink 都在原生生产者静止、reader 归零且最后一项批 ACK 消费后收尾，
不依赖 Ray actor 进程的 atexit。snapshot 返回前的观察与收尾共用原 actor condition；当前
回调先完成入缓冲，再排空、fsync 和写 footer。关闭观察入口后到达的 snapshot／poll 仍走
原生方法，返回或异常不变，但不再写这份 sink。该边界不证明 Ray 结果已送达，也不代替
driver 采样器／Future 回调结束。driver 仍按原 worker finally 停止采样、关闭 telemetry 和
自身 sink；full 的原生模型／GPU 事件与 trace export 另随原监督退出流程完成，须分别核对。

普通 records 的 full 路径另有准备 actor。它在原 owner 的终止接点前，通过同一单并发
actor 队列领取 `close_observation` 回执，复用 `close_events` 排空、fsync 和 footer；
当前准备调用结束后才关闭，后到的准备调用保留原生返回／异常并处于该 sink 观察范围之外。
回执写入 `model_owner.prepare_observation`，分别记录 RPC 终态、引用是否保留及实际 sink 状态。
超时、缺 footer 或刷盘失败仍继续原有 kill，观测保持未知，不改模型请求结果或把 owner 的
API 关闭当作刷盘成功。重复关闭不重复派发，未取到的 RPC 引用由原 owner 保留。
准备回执直接使用本次 `close_events` 的事务返回值，核对 producer PID、`footer_written`
及最终错误／缓冲计数，不重读可读旧 footer 来证明成功。权限收尾出错也保留失败；
中断仍在 `finally` 请求原 kill，并按原 owner 的异常规则传递和记录，两项失败都不抹去。
等待沿用原关闭 deadline；off 和没有准备 actor 的 token 路径不增加 RPC。旧源码若缺少该终止
接点，工具不安装这一收尾，缺失回执仍是未知，不能借用本候选的资格。

标记分别表示 actor 原 reader guard 内的返回前边界、额外 Future 完成通知和 driver decode 后
审核入口。通知与原 await 恢复不保证全序；既有 `Ray_result_to_Core` 是 transfer **入口**，
不是 sink 已完成。跨进程仅使用同机 monotonic 边界，同线程才解释 thread CPU，重叠区间取并集。
light 不能给出 GPU 忙／闲、硬件瓶颈或 Ray/Core 因果结论；on/off 需保留额外观察开销和所有重复。

token 批路径沿用原 `submit_prepared_batch`／`poll_prepared_batch` 和逐成员 ACK。`actor_result_return`
在原 poll 锁内、Ray 返回前按成员首次暴露记一次，附原 Core `key`、`batch_key` 和请求身份；
复用原 `poll_exposed`／关闭后待 ACK 状态，不另建完成台账，重读分页和重复 ACK 不增加成员完成数。
这只是首次尝试返回，不能证明该 RPC 已送达。`actor_batch_poll_return.members` 仅列首次暴露子集，
不冒充完整页成员；`driver_batch_future_done` 每次 poll RPC 通知只带批身份，不读取 Future 结果，
不能按该事件数统计成员或唯一配对重读页。批模式的 `driver_future_done` 则是原批 driver 解码后
本地成员 Future 就绪，不当作 Ray RPC 完成。actor 关闭后仍有批 ACK 待处理时 sink 不宣称收尾；
缺失后续 ACK、sink 或有效时钟仍是未知，不能据此宣称完整捕获。

## 设备绑定与 native 反馈审计

full 模式的 NVML 读取器仍只接受物理 GPU UUID。入口通过现有 GPU allocation 模块解析
`runtime_binding.cuda_visible_devices`：UUID 列表保留顺序并核对继承分配；数字 `0` 仅在 NVML 确认恰有一个非 MIG 设备时
解析为该设备的 UUID。多设备数字序号、空配置、无法读取或身份漂移均记录 unknown；不猜测
CUDA ordinal 与 NVML index 的对应，不回退到全机设备，不把缺失填零。每次指标读取仍验证 UUID。

CPU/RSS/NVML 的 `sample_interval_s` 是轮询间隔；报告保存实际间隔、采集起止与缺失原因。
NVML GPU 活动率使用设备内部的采样周期，不能将每次轮询当作独立硬件样本；它不是 MFU、
SM 占用或 HBM 带宽瓶颈证明。原 `nvidia-smi` 时间序列仍是另一份有起止括号的设备观察，
不能与 NVML 或 CUDA trace 重复计数。handoff 模式不启动这些物理资源采样。

观察开启时，native 反馈元数据复用原事件 sink，先写有界内存缓冲，`close_events` 再排空和 fsync；
反馈/决策线程不写文件，也不等待 sink 的写锁。缓冲条数与单包字节分别受既有
`max_events_per_process`／`max_event_bytes` 限制；达到边界丢弃新观察并在 footer 记数，不覆盖旧证据。
footer 的 `feedback_buffer` 保存 attempted/enqueued/drained/dropped/errors、峰值、剩余与边界；
`drain_complete` 和 sink 的 flush/error/drop 需分别核对，均不代替原生产者退出和未决 RPC 证明。
`observation_ns` 是入缓冲时钟，普通事件时钟是随后写 sink 的时间；来源时钟、接收时间分别保存。

`native_feedback_frame` 的 `IPC_cache` 只表示原客户端接受/拒绝缓存帧；`actor_snapshot` 带实际
Core session，表示 Ray 返回前的观察，尚无消费结论。`native_feedback_consumption` 分别记录
`RPC_sample` 的 UID/session/查询起点校验、`sample` 的序号/源时钟校验、`consumer` 的新鲜度/重复
消费判断。`accepted=true` 不是策略已接纳更高并发，也不证明所有工作可比。UID、session、
源序号/源时钟缺失仍未知；仅保留规范 native UID、数值和理由码，不记录 prompt、输出或环境。

供给 sampler 的可选分段观测继续使用查询关闭回执和这些消费事件，不新增日志通道。
`control_request_index` 关联控制 RPC、provider 采纳和消费；它不是模型请求数，重复源帧不会
刷新被采纳帧的 RPC 身份或年龄。源 `NativeStats.clock`、接收进程 monotonic 和 provider clock
分别标注；`observation_ns`／sink 写入时钟不能替代它们。原 Future 的本地 ready 通知及
轮询／取走时间见关闭回执，通知可能晚于实际就绪，注册时已完成或通知缺失时不补猜时间。
同进程“通知→取走”可观察部分就绪后等待；跨进程差值仍需时钟锚点，不称精确传输时延。
该补充只覆盖提供这些可选字段的 sampler；旧 request 路径字段缺失仍未知。观察 off 不注册
这项 Future 回调，on/off 不改 poll/RPC 节奏、新鲜度阈值或业务输入生成参数，实机开销待测。

该消费审计需要被测 runtime 的可选 provider observer 接口；旧 runtime 明确发出
`native_feedback_audit_unavailable`，不能仅凭工具版本宣称完整。main 的 request-feedback sampler
提供完整来源元数据；独立供给 sampler 需同样传入 `ServiceMetricsSnapshot` 的可选
`source_uid/source_session_id/received_at_s`，并在 RPC 校验处调用 `observe_native`。观察器不复制执行器，
不为了审计开启反馈策略，观察 off 保持原路径。

启用既有 native stats 的异步引擎可从原 ready 回执读取 `native_capacity`：值来自构造完成的
`AsyncLLM.vllm_config`，不以 EngineSpec 声明补值。有效 seq/token/context 配置、KV池块数/块大小
分别保留未知；这不是空闲 KV/显存读数，也不改变控制策略。上述新增接点仅有本地 CPU 验证，
准确新树的 NVML/Ray/vLLM 接入与 on/off 开销仍待实机检查。

## 本地维护和清理边界

可直接运行 `qualify` 中的 unittest 模块及两个 `probe_record_resource*_handoff.py`，不用仓库外
Python 消费器。旧 `prepare_cells`、八格冻结配方和只依赖私有分析目录的联动用例未带入此入口；
它们的原件保留在原审核／运行目录。新 `prepare` 取代旧矩阵配置拼装，但不删除 full 模式或
任何有效优化。当前在用的私有包、历史冻结源码、运行回执、失败记录及原始数据均不清理。
