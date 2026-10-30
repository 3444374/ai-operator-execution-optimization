# Git 同步的记录查询资源观测

采集器和检查入口随仓库提交同步；模型、数据、机器绑定和全部回执留在 Git 外。后续更新不需要
重新上传 Python 包、手工填写七份代码摘要或复制 CPU 资格材料。入口只做配置、观察与证据校验，
不增加生产依赖或调度策略。来源为已审采集器 `3b7991c1`，逐文件来源见 [SOURCE.json](SOURCE.json)。

工作流沿用既有记录查询检查的 Git 身份、CPU 资格、受监督执行方式。运行仍复用
`control_resource_C2.py`、原 budget/lease/supervisor；旧文件名仅为兼容，C 从私有配置读取。
采集器从工具 checkout 加载，被测代码从独立的 `source_root` 加载，两者分别绑定准确提交，
不要求为了重现旧代码把测试工具覆盖进旧 checkout。

## 同步、资格和最小 on/off 配置

在独立、干净的工具 checkout 同步已经审查的提交；命令不会自行联网、安装或连接服务器：

```sh
git fetch origin "$VERIFIED_TOOL_COMMIT"
git switch --detach "$VERIFIED_TOOL_COMMIT"
TOOL=code/scripts/profiling/record_observation/workflow.py
"$DRIVER_PYTHON" -B "$TOOL" qualify --tool-commit "$VERIFIED_TOOL_COMMIT" \
  --config "$PRIVATE_BASE_CONFIG" --output-root "$NEW_CPU_DIRECTORY"
```

`qualify` 跑本仓 CPU fixture 和 off/on 公共基类探针，不启动 Ray RPC、真实模型或 GPU。
失败输出保留，未通过不产生 `CPU-READY.json`。工具提交、被测源码、基准配置和七份 runtime
代码摘要都绑定在该新回执中，后续变化须重新资格检查。

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

预算身份使用原 API 校验，冒号和路径字符会被拒绝。已有输出目录、账本、资格和原件均不覆盖。
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

标记分别表示 actor 原 reader guard 内的返回前边界、额外 Future 完成通知和 driver decode 后
审核入口。通知与原 await 恢复不保证全序；既有 `Ray_result_to_Core` 是 transfer **入口**，
不是 sink 已完成。跨进程仅使用同机 monotonic 边界，同线程才解释 thread CPU，重叠区间取并集。
light 不能给出 GPU 忙／闲、硬件瓶颈或 Ray/Core 因果结论；on/off 需保留额外观察开销和所有重复。

## 本地维护和清理边界

可直接运行 `qualify` 中的三个 unittest 模块及 `probe_record_resource_handoff.py`，不用仓库外
Python 消费器。旧 `prepare_cells`、八格冻结配方和只依赖私有分析目录的联动用例未带入此入口；
它们的原件保留在原审核／运行目录。新 `prepare` 取代旧矩阵配置拼装，但不删除 full 模式或
任何有效优化。当前在用的私有包、历史冻结源码、运行回执、失败记录及原始数据均不清理。
