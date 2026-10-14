# HSE static 本地正确性与 broker 历史开销审查

本轮审查已有异构分阶段执行（HSE）的 static runner 与共享 broker，以
`integration/baselines` 的 `3e7bd54bbc0e3416dfca991fcaaa2ffc41270a71` 为源码基点；
远端 stage 分支核对为 `5a7851426b9d4412a3d20192a84a72cd1acf1c6c`。
保留多图 batch、常驻 CPU/GPU actor、ObjectRef 依赖、FIFO 与 direct-dependency 强静态对照。
只验证本地生产 Core/broker 与受控 actor/source 替身，不要求 PostgreSQL 接入。

## 已复现并修复

| 对象 | 修前反例 | 修后行为 |
|---|---|---|
| broker lease | prepare/model 传入 NaN、负数或无穷值后抛错，但块先被移出队列，`is_drained=True` | 先验证时间，再修改队列；错误保持原状态、容量与可推进性 |
| 永久超大块 | 单块 ready bytes/work 超过完整额度，仍可入队并永久阻挡后续块 | 入队前拒绝，未占容量；正常后续块仍可推进 |
| 活动峰值 | 上限4的 fixture 实际持有4块，CPU因 ready 额度不足未派发，结果只报告3 | 每次入队立即采样；该反例报告4 |
| 引用寿命 | 单块48字节额度结清后，旧 payload ref 仍由局部变量持有；下一次准备时两个 ref 同时存在 | model call 持有 payload 到完成确认；随后清除，下一次准备前旧 ref 已释放 |
| 错误退出 | submit/get 错误立即归还 lease；调用方没有异常 finally | HSE停止继续供给，CPU `ready` / GPU `synchronize` 确认后才结清；未确认状态、引用与额度保留 |
| 派发顺序 | 已有 ready 块且 GPU 空闲时，先读取下一批 source | 先派发已有 ready model work，再补充 source；FIFO、batch 与额度保持 |

错误处理先登记 actor/lease，再提交远端调用；所有确认请求一起发出，等待共用30秒上限。
部分确认只归还对应 actor 的额度。再次中断或清理自身报错保留原始异常；已完成 broker 转换但
尚未移除的调用记录按其实际状态处理。未知调用保存在异常的 `hse_unconfirmed_work`，同一
borrowed pool 的后续 HSE 调用会被拒绝；正常热路径不增加 CPU/device 确认 RPC。
该完成证明使用既有同步、串行、同一提交方、无自动重试的 actor 设定；
[Ray 官方执行顺序说明](https://docs.ray.io/en/latest/ray-core/actors/task-orders.html)支持这项设计前提，
本轮尚未在固定 Ray 版本执行实库故障测试。

## 已确认的开销与未验证的收益

修前 `snapshot()` 与每次不变量检查遍历全部终态历史，固定活动窗口仍有累计 O(B²) 扫描。
现在缓存完成/失败计数，活动检查只遍历活动块；保留默认历史、重复行检测与显式
`release_terminal` 语义，没有通过删除历史换取速度。

[原始三次测量](broker_cost.json)与[复算入口](probe_broker.py)只运行单活动块的本地 CPU broker
完整生命周期，每轮都检查恰好一次完成及全部历史仍保留；未执行 Ray、模型、GPU或数据库。
基点与修改版同进程按顺序交替运行，无单独预热。下表为各三次 elapsed 的中位数，单位秒：

| 累计块数 | 基点 | 修改版 |
|---:|---:|---:|
| 1,000 | 0.071473 | 0.030907 |
| 2,000 | 0.228983 | 0.061171 |
| 4,000 | 0.793945 | 0.123499 |

这些数字只支持 broker 历史扫描成本下降。优先派发的真实 GPU 收益、ObjectRef/Arrow/Ray
物理副本与 RSS 变化、完整吞吐和延迟均未验证；不据此替代 direct-dependency 或原生 Ray Data
的匹配资源性能对照。ready 时间核查已排除误报：model lease 读取的是 broker 保存的 driver 时间。

暂时的 FIFO head-of-line 等待保留。即使后方小块能放入，当前 static 仍先等队首所需 bytes/work；
新增用例同时覆盖字节与工作量不足。fit-aware 选择需要另行明确扫描成本、公平与饥饿处理，
本轮没有收益证据，因此未改策略。

## 验证与剩余工作

[修前反例日志](before_fix.log)记录21项检查的13次子项失败与2次错误；后续新增中断检查另计。
共享 scheduling 完整发现首先因缺 `pyarrow` 产生1个 import error，
[失败日志](discovery_missing_arrow.log)保留。排除该无法导入模块后运行现有 scheduling 全集、
HSE/descriptor、incremental image 和 Map preparation；最终范围、计数、跳过项与源码摘要见
[机器记录](local_checks.json)和[输出](local_checks.log)。没有混装依赖或启动服务器。

HSE 本路径使用 Ray Core actor；broker管理自己的 encoded/preparing/ready/modeling payload
账本，Ray管理实际 actor/task 排程，Daft source 与其缓冲仍由数据源执行方管理。原生 Ray Data
观察臂未接入这些额度。本轮没有重复控制 Ray Data 自己的 pipeline/backpressure。
这些逻辑 byte/work 额度不包含 source/Arrow缓冲、序列化副本、Ray pinned对象、GPU actor 的
writable host copy、device tensor、模型缓存或全部 RSS；这几类物理成本需独立采集。

`run_image_clip_e2e.py` 由另一任务独占，本分支未修改。它目前仅在正常返回后停止worker与
`ray.shutdown()`；调用方应在异常后终止/丢弃本次池，不能改用 direct-dependency 重用未知池，
因为那条路径不读取 HSE 的保护记录。停止池的外层 finally 与 sampler 清理应由 CLI owner处理。

后续最小实库需求交主会话排期：4 CPU、约512 MiB object store、300秒，采用串行 CPU/模拟模型
actor验证真实多返回 ObjectRef、部分完成、错误确认及未知池拒绝，模型/GPU/POST均0。
真实性能验证需要单4090与最多16 CPU，复用既有本地模型和同一 source/输出语义；从已有batch64
强静态配置开始，direct-dependency/HSE先做正确性，再做预热及至少三次交错重复，独立采集
source、prepare、ready、model、fan-in与实际内存。准确运行配置和停止条件由主会话排期前确定。
