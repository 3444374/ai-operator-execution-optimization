# 本地异步模型组件：CPU 合同与有界进程桥

状态：新增组件的 CPU 验证，尚未接入 actor/Core/CLI，尚无真实 vLLM/GPU 或性能资格。
root 后续选择性整合及关闭异常修复见末节；原始候选证据按其固定字节保留。
原始任务基线 `integration/data-execution@d2915206`；唯一共享引擎 owner 后续交付
`a4f47eab19ce4ff1b33f82b2cd1def2c47106e67`，本职责分支以 fast-forward 消费该依赖。
本工作包只新增组件、专属测试及本报告；`a4f47eab` 以前的集成差异不属于本包实现。

## 条款、已有实现、缺口与验收

核对来源为 [根规则](../../../../AGENTS.md)、[代码规则](../../../../code/AGENTS.md)、
[架构 §3.2 模型执行](../../../plans/系统架构.md#model-engine-execution)、
[数据组织实施路线](../../../plans/数据组织.md#model-engine-roadmap) 和
[增量生命周期](../../../plans/增量执行.md#session)。当前条款之间未发现本切片的方向冲突。
计划内异步供给是后续资格步骤；本地同步、HTTP、static 与生产 Core 的已有能力分别解释，不能互相代替。

| 文档条款 | 固定基线现状 | 本切片缺口与落点 | CPU 验收 |
|---|---|---|---|
| 架构 §3.2：常驻模型属于数据执行；不强制 HTTP；单卡 TP1 与环境隔离 | `VllmEngine.run_prepared` 持锁同步 `LLM.chat`；私有 `EngineProcess._exchange` 与 child 命令串行；增加 Core 在途不能解除该排空边界 | 新 `vllm_async_engine.py`、`async_engine_process.py`，不修改同步默认或基线策略 | hold 首请求仍接纳并完成第二；真实 CPU 子进程/三 socket 验证 |
| 数据组织：一次模板，复用原参数/结果/usage/logprobs，逐 occurrence 身份 | `EngineSpec`、`GenerationSpec`、`_prepared` 已有；owner 的 `a4f47eab` 共享结果转换 | renderer 产生 token EngineInput 后传入 `AsyncLLM.generate`；复用共享 converter，核 request ID 和返回 prompt IDs | 一次 render、FINAL_ONLY、逆序结果、JSON/logprobs、逐调用参数互不污染 |
| 架构/增量：输入保留至终态、结果归消费者、有限资源 | 现有 Core 账本不应在模型桥另造调度策略 | 有界请求/input/result/command/control，结果先预留，消费者 poll 后归还结果容量 | 满容量恢复、bytes 背压、慢消费者、超限结果保留 usage 的失败回执 |
| 数据组织：abort 后终态、失败前缀与下一查询隔离 | cancel 本地 future 不证明模型终态 | accepted/abort_requested/terminal 分离；unknown 保留责任，关闭由 outer owner 核物理资源 | 早取消、合成 abort、迟到正常完成、deadline、child 死亡、两 query 隔离 |
| 架构：owner 核模型后代/GPU；清理失败不能冒称关闭 | 真实模型资格与进程桥 CPU 资格分开 | 本组件只确认 engine API、直接 child 与 I/O；后代/GPU 返回 unknown，由 owner 串行接线 | CPU child 退出、FD/线程清理、shutdown 超时保留同一尝试；真实后代/GPU pending |
| 根/代码：保留旧 backend 与同方法屏障；不可把 fixture 当收益 | 同步引擎、原 parser、全局 cascade 与 Extract 关系边界继续存在 | 新组件可选；不改公共 records、actor/Core、CLI 或其他 backend | 旧同步和共享转换回归；未测跨阶段流水、完整查询或性能 |

## 固定官方源码核对

下列均为公开 vLLM `v0.25.1` 源码，只读核对，没有执行下载内容；单文件 SHA-256 进入验证记录。

- [offline_utils.py](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/offline_utils.py)、
  [renderer/base.py](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/renderers/base.py)、
  [renderer/params.py](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/renderers/params.py)：
  采用原 chat renderer/ChatParams 一次渲染，固定 `add_generation_prompt`、`continue_final_message`、tools 与 tokenizer 条件；
  核 token 输入和最终 token IDs。不能把 messages 直接当 `generate` prompt。
- [async_llm.py](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/engine/async_llm.py)：
  每请求 async generator 与 FINAL_ONLY，abort 接口和同步 shutdown；原生 shutdown 放独立线程并保留同一 future。
- [output_processor.py](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/engine/output_processor.py)、
  [core_client.py](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/engine/core_client.py)：
  abort 合成最终输出后发送 ABORT，不是远端 GPU 停止确认。采用保守 unknown，不归还未知容量。

有界队列和三通道 IPC 是本项目工程决定，没有新增批形成/公平/路由策略。
公司工程与 pgml 的采用范围沿架构 §8.7–8.8 已有记录，本切片不读取或复制公司材料。

## 验证设置与运行

本地 Python 3.12.6 既有独立非模型 venv；无新安装、Ray runtime、模型权重、GPU、HTTP 或外部服务调用。
native wrapper 测试使用明确替换的 vLLM/renderer 对象和真实共享转换；进程桥使用实际 CPU child、socket 与 I/O 线程，
生成部分为固定 fixture。没有 warm-up；JCT、质量、RSS、显存、吞吐和尾延迟未采集（本合同验证不测这些指标）。
限额在专属测试中为 2 在途/2 结果、每结果 4096 bytes、总结果预留 8192 bytes；另有 4096 input bytes 用例。
出现失败保留本次身份与原因，修复后使用新运行；不会将测试耗时解释成模型性能。

已完成的运行：

| 运行 | 结果及解释 |
|---|---|
| 首次 IPC | 共享 `result_record` 依赖尚未消费，失败 1；本地工具输出 chunk `da776a`，未保存全量原日志，不能作为通过证据 |
| 依赖消费后 IPC | 10 passed，2.65s；chunk `f8556d`，仅组件验证 |
| 首次 native wrapper | 8 passed/1 failed；shutdown 超时被错误地永久标记失败，chunk `916675`；已修复为保留同一 future，实际异常仍不作成功 |
| 首次组合回归 | 88 passed/144 subtests，9.44s；chunk `716642`；覆盖两个新组件与同步/共享转换旧路径 |
| abort 读取独立、FD 失败清理补充 | 新专属集 21 passed，4.57s；chunk `172f3c` |
| 扩大旧进程回归（sandbox） | 104 passed/177 subtests、13 failed；旧同步 psutil 枚举被 macOS sysctl sandbox 拒绝，见[失败日志](raw/cpu_20261019T065230Z.txt.gz)与[来源](verification_20261019T065230Z.json) |
| 同源码在允许只读进程枚举的 CPU 环境重跑 | 114 passed/188 subtests，21.99s；见[日志](raw/cpu_20261019T065326Z.txt)与[来源](verification_20261019T065326Z.json)，不覆盖随后审阅修复 |
| 独审修复后专属集 | 29 passed/2 subtests，7.60s；chunk `5c8cf8` |
| 冻结修复 `3499916f` 的组合回归 | **121 passed/190 subtests，21.86s**；[日志](raw/cpu_20261019T070124Z.txt)与[来源/逐文件摘要](verification_20261019T070124Z.json)；运行前后源码摘要一致 |

早期运行仅工具记录可定位，全量 pytest 输出未采集；不补写或冒充原日志。最终源文件摘要、命令、退出状态及脱敏日志在本目录保存。

## 独立审阅与修复

Standards 与 Spec 两位独立 reviewer 固定审查 `a4f47eab..777a0bdb`，读取规则、计划和固定官方源码，
只运行 CPU fixture。原始发现保留，不以首轮测试通过代替独审：

- 两者复现空闲 child EOF 后继续 queued 的缺陷。提交时检查 child/I/O 可用性，EOF 标记 owner unavailable；
  不覆盖仍待读取的已知回执。新增空闲死亡/最后已知终态后死亡双用例。
- Spec 复现合法大身份使小回执预检低估，生成 task 异常未被主循环监督。预留覆盖共享回执的最大时间戳、
  256-byte 引擎 ID、usage 与确认字段；身份不能放入回执时接受前拒绝，序号有固定上界；
  生成 task 失败由单个有限 fault future 传给 worker 生命周期，不能默默 pending。
- Spec 复现原生取消分支等待 abort 使 deadline 挂起；生成使用已预留请求内的独立 task，deadline 通知
  与清理解耦，未知 task 保留归属。Standards 另复现已过期/渲染后过期仍调用模型；入口与生成前明确检查期限。
  新用例覆盖原生取消清理挂起、过期零模型额度、边界身份、大结果、意外 task 错误和已知最终 usage 后流清理异常。

两位原 reviewer 对固定 `3499916f99827bf2a0bb74e493389311396ca980` 的修复和最终上下文均复核通过。
各自独立重跑 29 项专属测试、2 个子测试（Standards 6.85s，Spec 6.58s），还分别复现边界身份拒绝、
取消清理挂起和 close 有界未知报告。已保存[审阅结论](reviews.json)，不冒称原始完整调试日志。

## 集成接口与剩余项

最小 API、容量和确认语义见[组件接口](../../../../code/src/serving/backends/async_engine.README.md)。
唯一集成 owner 串行处理公共 `vllm_engine.py`、records/actor/Core/CLI 与共享导航/状态/证据台账；
本分支没有直接编辑这些共享文件。需要 owner 将本报告登记到已有 README/台账，保持当前唯一状态入口。

pending：真实 vLLM0.25.1 版本/依赖资格、单 GPU render/token IDs 与 logprobs、迟到供给、真实 abort 后状态、模型后代/GPU退出；
actor/Core 生命周期接线；相同方法和生命周期下的完整查询对照。GPU 由 owner 统一排期。
组件通过不等于跨阶段流水，cascade 全局校准或 Extract 关系屏障未移除；不声明性能收益。

失败 pytest 日志带有原生尾空格，公开副本采用无损 gzip 保留原字节；来源记录同时给出压缩/解压摘要与字节数。
可用 `gzip -dc raw/cpu_20261019T065230Z.txt.gz` 恢复阅读。原未压缩副本仍可从提交 `0f79bbce` 追溯。

## root 集成与关闭异常修复

原四个职责提交选择性整合为 `6792213d`、`7df2eaf2`、`ee598ac0`、`cd977302`。
root Spec 独审发现 kill 后第二次 wait 超时会跳过本地 I/O 收尾及 unknown 报告；
`bc92f7ce` 接纳职责修复 `f8cceff9`，没有改同步、方法、Core、actor 或 CLI。

构造器在保护区内登记每对 FD，部分 socketpair／setblocking 失败仍显式收尾。
close 的 wait／TERM／KILL／poll 失败会保留诊断并在 finally 尝试关闭 FD／有界 join；
未确认 child 退出仍 unknown，保留同一 owner 句柄和未知预留。
构造失败重新抛原启动异常，附 `async_close_report` 与 `async_engine_process`，不以清理次错覆盖主错。
GPU／后代字段仍 None，重试仅可用于该同一 owner 的清理，不重新提交模型调用。

[派生集成摘要](integration_verification.json)绑定修复两份源码和私有原件摘要。
7 项新增故障、原 16 项实际 CPU IPC 和 13 项 Async 内核替身共 36 项通过、0 skip；
生产 supervisor 确认 17 个已观察 owned 身份退出，remaining／cleanup 为空。
首轮新 fixture 漏 limits 的失败保留，修正仅在新测试；没有以此扩大生产范围。
原候选 121／190 与修复后 36 的范围不同，不相加为独立样本数；本轮未重跑原完整 121 套件。
两位独立 reviewer 复核关闭原 P2，Standards／Spec 均无剩余阻断。
actor／Core 持续供给接线、真实 vLLM／GPU、跨阶段流水与性能继续 pending。
