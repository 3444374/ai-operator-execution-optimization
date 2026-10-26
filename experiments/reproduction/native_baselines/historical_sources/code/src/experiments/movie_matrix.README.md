# Movie 有限矩阵维护入口（CPU 候选）

单一入口为 [`run_movie_records_matrix.py`](../../scripts/experiments/run_movie_records_matrix.py)，
实现为 [`MovieMatrix`](movie_records_matrix.py)，启动与收尾 helper 为
[`movie_matrix_support.py`](movie_matrix_support.py)。本地候选已写入维护代码，已按内容寻址冻结稀疏源码；未部署或运行模型；
不能继承 r3 或旧 Native/Core/IMLane 的实机资格。

这次迁入的是 r3 的 owner/worker 编排，不是新的执行后端。`run_movie_session`、原 supervisor、
预算 ledger、各后端批处理、并发、背压、模型生命周期和完整 LOTUS 解释评价保持原实现。
沿用同一有限矩阵 schema；新建运行的查询数、调用额度和时间窗口来自 sealed matrix、scope 和输入计划。
原 e269 的 87-query/44280-call/10800s 模板保留在父包。本组合准备的是公共校准工程资格包：
2 个完整 Q3 目标查询（显式预热/测量各120行），最多6个扫描 owner、14个查询、1680调用上限，
execution_enabled=false、分配0。缓存命中只执行目标240调用；潜在扫描预留不伪装实际模型调用。
新目录和新 budget ID 必须由新封包显式绑定，不能运行已花费的旧 packet 取得新额度。

## 命令与输入

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B code/scripts/experiments/run_movie_records_matrix.py check \
  --packet "$CURRENT_SEALED_PACKET" --packet-sha "$PACKET_SHA256" --output-dir "$NEW_PRIVATE_OUTPUT"
```

`run` 不带 `--execute-model` 同样只检查，零真实 claim。实际运行另加该 flag，仍由既有运行 owner
提供经过核验的解释器、CPU affinity、GPU 分配、离线环境和有限计划。本候选不连接 SSH、
寻找旧连接或自动恢复实验。调用者可通过正常外层传输执行同一 argv，不再运行嵌入 Python 的历史 SSH launcher。
`worker` 与 `audit` 是同一入口的内部动作；worker 的必填 index/context/output 参数在读 packet 前检查。
worker 使用 driver Python，CPU token audit 使用 model Python 和空 CUDA mask。owner 超时由现有
startup/query timeout 和查询数推导，服从总墙钟与 cleanup reserve；supervisor 的清理余量保留。

输入是显式、不可变、当前自足的 sealed packet，不是历史目录名。仍需 packet 中的
`q3_packet.py`、`q3_host.py`、`host_helpers.py`、`execution_policy.py`、`active_failure_evidence.py`、
`token_audit.py` 及其受锁传递依赖、sitecustomize、输入和参照。旧文件名保留作兼容；
维护入口只从给定 packet 导入，不搜索旧实验目录，也不导入整套旧 `q3_controller`。
封包缺 helper 或摘要不匹配会在导入/模型前失败。固定数据、模型、旧账本及原件的外部路径仍是
显式资产/证据绑定，不能把消除代码目录依赖理解成可删除这些数据。

运行时入口与 driver 必须属于 packet 绑定的同一源码目录，随后执行原完整 source.lock/资产检查；
不能让旧 source 的资格掩盖树外新代码。新运行文件需要新源码锁和 packet/review 摘要，旧锁没有更新或被冒用。

## 已统一的行为

- 控制端和 worker 共用同一个 context 构造及完整性校验，包含 r3 的 `lease_path`、摘要、IPC 私有目录、
  budget/runtime/资源必填字段；IMLane 专有项只对 IMLane 检查。可选 tokenizer fallback 保持。
- 控制器完整检查计划一次，worker 保留本组计划、原 sealed packet/资产/输入/lease 与实际逐行 token 检查。
- `clock.json`、性能采样关闭和进度 JSONL 的普通异常只警告，不覆盖业务结果；必要资源清理及 IPC 环境恢复先执行。
  权威结果、独立物理观察、before-release/campaign 证据、GPU 冲突和未知身份仍是必要门禁。
- 物理 finalizer 从实际 r3 archive 核同，只注入 host/packet 依赖；没有把物理退出等同 API close。
- 新建运行保留原独立预算；显式 continuation 保留原 snapshot、截止时间、skip 和已花调用，不重新 reserve，
  不重试旧 unknown。接续成功的状态仍为原 `completed_remaining83_original_matrix_incomplete`，不会宣称旧矩阵完整。
- per-query、session 和 group 的原结果/计时字段保留。新入口不生成旧 SSH relay 的 outer 指标；
  调用者另记传输及入口墙钟，不能把它与旧 SSH outer 时间混为同一口径。

## CPU 验证及边界

[`test_movie_records_matrix.py`](../../tests/experiments/test_movie_records_matrix.py) 对实际维护类和 helper
执行 36 个 CPU 用例，包含 30 个逐字段缺失子用例、五后端字段合同、连续 resident 的 engine identity、
接续不预留/不重试、故障记录与清理分离、源码绑定、可迁移封包及默认零执行。
两作者补丁在 r3 快照组合的另外 10 项配置回归通过，含原 sitecustomize 的无模型 CPU 子进程检查；
总计 46 个不同测试，复跑不相加。这是入口迁入初版的验证记录；其后 Core 查询方法复用和下面的配置增量
分别记录，不能再把整个当前候选称为与 `068f5248` 字节相同。新源码模型和物理资源资格须重新绑定。

```sh
PYTHONPATH=code "$DRIVER_PYTHON" -B -m unittest discover \
  -s code/tests/experiments -p test_movie_records_matrix.py -v
```

初次 fixture 缺失 backend、旧 mock 未接内部 keyword、macOS 临时路径符号链接比较失败均保留在本地交接证据，
修复没有放宽生产断言。没有运行 GPU/SSH、操作真实账本、改写旧失败或删除原件。

## 清理接续

正式入口经下一次完整封包检查并确认没有外部调用者后，旧 `execute_movie_q1/q2`、`execute_resident`、
`session_driver`、r1/r2/r3 的 `execute_matrix`、`stage_*`、`prepare_*` 可退出活动入口索引并归档原件；
不在本次直接删除。旧 `q3_controller` 在新入口中不再被导入，但要保留其历史回放副本。
原 `observe_existing_master` 不再是本入口依赖；其他运行/取回工具仍可能使用，不能据此直接删除。

当前 packet 的 host/policy/sampler/token/evidence/bootstrap helper 及旧数据/账本不在清理名单。
原日志/归档/总结/收据/失败和其恢复所需脚本继续保留，正在汇总的 r3 原件完全未改。


## Core 查询方法与模型复用（本地 CPU 增量）

Core 使用原 create_records_core 和模型 owner；evaluate(source, query_id=..., method=None)
现在可以逐查询传入同一模型名的 FilterMethod。省略 method 保留构造时默认方法。
prepare actor 接收该查询冻结的方法，在其原串行方法槽内使用轻量 formatter；默认 formatter 与查询覆盖值分开，
所以 Q1→Q3→Q2→Q3 不会把 Q3 指令残留到下一次默认调用。只缓存一个最近覆盖 formatter，不缓存答案或模型结果。
模型 actor / AsyncLLM、采样参数、batch1、显式 async4、背压、结果/租约/取消责任沿用原实现。

同一 Core 会话允许不同指令；模型、tokenizer、模板及构造时 generation 配置仍由同一 owner 固定，
换模型名拒绝。新查询须等待前一查询完成并结清，执行协程捕获完整 session ID 与查询方法，拒绝迟到旧 session 提交。
Native/Ray/Daft/IMLane 的原生生命周期和调度未修改。

维护封包仅将 Core 原有按指令拆分的 resident owner 合并，查询顺序、87 条查询、44280 调用上限和
全部其他后端计划不变；新的加载/关闭次数与时间必须在新版本实测，不能改写旧结果。
当前验证是原 LOTUS formatter / SessionEngine / 有界异步桥及真实 CPU 子进程，加明确 Ray/Daft 调度替身。
7a/独立私有版本的跨指令实测记录单独保留。本公共校准组合须重新绑定实际 token/template/decode、
配置和物理关闭资格，没有继承该实测，也未测得新增性能收益。

## 运行配置贯通（独立 CPU 增量）

- task / input profile 仍由 MovieTask 和 source manifest 选择。Q1/Q2/Q3 的原提示、普通谓词、
  head5/COUNT 和评价是任务语义；120 是当前输入筛选结果，不是所有 Q2/Q3 的运行常数。
- Core `profile.mode` / `max_inflight` 和 `resources.cpu_workers` 沿用现有 factory/actor 通路。
  `max_inflight="auto"` 仅取声明的 Core 容量和 `engine.max_num_seqs` 的较小值，不是硬件自动调优。
  prepare 仍每 actor 1 CPU；模型 actor 1 CPU / 1 GPU，Core 每次准备/模型调用仍为 1 条。
  当前协议明确只支持 `gpu_workers=1` / TP1，0/2 会拒绝，未添加多卡能力。
- 新建 campaign 的额度、查询数、launch/cleanup 时间由现有 matrix/scope 一致绑定；每 query 的
  预留量来自 source preflight 的 `expected_calls`。旧 r3 continuation 的固定数值是原已花费账本
  的回放合同，仍单独校验，不用于新运行，也不退款或重试 unknown。
- context 使用现有 cfg 的 budget、GPU idle 阈值、IPC prefix，以及 comparison 的 CPU affinity /
  RSS / SHM 上限。scratch 写入本轮输出的 runtime 子目录，不写不可变输入封包。
- EngineSpec 的模型/解释器/tokenizer/template 和 generation 直接传入已有 owner。
  token 审计按同一 owner 的确切 query 序列选择计划，使用该 generation，校验报告配置一致；
  tokenizer 路径为空时回退模型路径，模板文件或内联模板沿用引擎语义。

换模型必须重新绑定本地权重、tokenizer、模板、版本、expected token reference 和源码/封包，
不能只修改路径后继承旧资格。同一 resident owner 中更换模型/generation 不在此增量范围。
CPU 非默认配置与旧路径测试见 [`test_movie_runtime_configuration.py`](../../tests/experiments/test_movie_runtime_configuration.py)。
这些配置推导未改变 baseline 的原生调度、批次或背压，未测得新的 GPU 性能收益。

## 公共运行校准与唯一维护入口

扫描、选择、签名和缓存属于
[`src.scheduling.runtime.calibration`](../scheduling/runtime/calibration.py)，只依赖标准库。
普通非 Movie 调用与本入口共用 `calibrate` / `run_calibrated`；未匹配的缓存通过真实 workload
callback 有界测量、选择并执行，命中跳过测量。identity 加候选集合、参数顺序、重复数和比例共同决定缓存键。
旧 Core 工厂的显式固定 profile 保留；不在 import/普通构造时暗自加载模型，也不把声明容量 auto 当成实测选择。

普通调用可直接使用公共 API，不需要 experiments、Movie 或 gold：

```python
from src.scheduling.runtime.calibration import Measurement, ScanBudget, run_calibrated

run_calibrated(identity, candidates, execute,
    cache_dir=cache_dir, measure=measure,
    budget=ScanBudget(timeout_s=scan_seconds, max_work=scan_work_limit),
    work_per_sample=complete_work, repetitions=3)
```

调用方的 `measure(configuration, repeat_index=..., deadline_monotonic=..., work_limit=...)`
执行相同完整工作负载，并在必要资源结束后返回 Measurement；`execute(configuration)` 实际应用配置。
不确认结束须抛错。候选只是容量参数，模型/生成/语义放入固定 identity，不能按 gold 或其他系统成绩选参。

维护入口通过 owner-group 可选 `calibration` 字段启用同一能力：

```json
{"cache_dir":"/private/artifacts/runtime-calibration-cache",
 "candidates":[{"max_inflight":1,"cpu_workers":1},{"max_inflight":4,"cpu_workers":2}],
 "repetitions":3,"near_peak_ratio":0.97,"timeout_s":1500}
```

没有该字段就是原固定配置。Core 扫描目前只接 async、batch1；固定 sync 在途4仍只提供准备/排队流水，
同步模型 actor 并发1，不是同步模型 batch4。模型重入锁、原生调度、批处理和背压均未修改。

薄 Movie adapter只负责输入/计划和完整查询回执转换。扫描和目标均走原 run_group/session/计费/supervisor；
唯一 controller 调用公共 run_calibrated，不再实现选择/缓存/候选测量循环。
所有扫描身份、调用数和资源 slot 预先从有限配置展开并计入新账本；sample ID 使用账本合法字符和查询位置，
不放宽旧 CellBudget 单元命名协议。缓存命中零扫描 claim，既有已花/unknown 账本不退款或重发。

所选 C/P 先物化最终 plan，再计算摘要和创建 owner。context 同时绑定原声明、最终 plan 与不可变选择收据副本；
worker 在 claim 前复核，token 审计读取同一有效计划，支持扫描前缀 ID。
只有实际 Core mode/C 与已测选择一致才标 performance_tuned；固定/容量 auto 保持 False。
新查询先清旧报告，后置来源检查错误仍保留本查询已知 usage。

`resolution_wall_s`、当前 scan_cost、原 calibration_cost、target_wall_s、公共 total_wall_s 分别保存；
前端观测/计划准备与总 group 时间、完整 campaign 墙钟另报，不隐藏首次扫描成本或重复累加 session 时间。
目标查询仍拆启动、完整查询、eval、关闭。公共选择只是有限已测有效候选中的近峰值，不是全局最优承诺。

身份复用现有机器/配额观测、目标设备、完整模型/Tokenizer/模板记录、最终源码和依赖版本。
新增 model-preflight 收据记录 tokenizer 资产内容摘要；旧收据不能冒充新源码资格。
新 source/packet 同级，IPC base 保留完整八位十六进制语义，执行必须同时使用 commands 的 env 和 argv。
本组合没有叠加私有11f补丁、H1/H4或语义策略。


## 递增搜索到平台或明确限制（独立 CPU 候选）

可选 `calibration.search` 由同一公共运行时执行，无新实验选择器。
设置一个起点及具名容量/资源边界后，按倍数递增并以重复观测确认局部平台；
命中容量、资源或预算上限分别记录，边界最佳不等于平台。
规则、失败行为、缓存隔离和 Core 单 owner 查询额度见
[`calibration.README.md`](../scheduling/runtime/calibration.README.md)。
维护 CLI 现有隔离 sample 生命周期保持不变；普通公共回调可用查询额度复用一个模型 owner。
模型 `max_num_seqs` 的变化仍需新 owner、身份和缓存。该增量仅 CPU 验证，
未部署、未改全局默认，未继承旧候选的 GPU 资格或性能结果。
