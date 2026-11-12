# 原生 LOTUS 同 owner 资格与正式 resident 入口

本报告区分已冻结复现的真实结果和最新 main 上的维护性迁移。
执行仍为原 LOTUS accessor/LM/LiteLLM；项目只管理本地模型 owner、源/完整 sink、预算和观测，
不使用 SemLoom Core 调度。LM64 和 resident 生命周期是显式实验 profile，不能称作者默认。
共同条件采用协调版本 `acae5b2a` 的[原生基线校准](../../../plans/原生基线校准.md)，不另建比较协议。

## 已完成的冻结真实资格

运行 `lotus-mmqa-q3a-engine-qualification-20261110-r1` 使用入口
`ee625c877976b209ef8480edad8ec15c92d22ac6`、运行时 `fd06f6f04b537dea1c005ee5e69fe93b8ad77799`，
原 SemBench 为 `c814e3807e72d4cf876b852b17e77f3cc94575c2`。
冷25＋暖25的50个唯一确认终态、0未知，同一实际 engine/model API PID及出生身份，
50个完整GPU prompt token数组均与冻结参考一致，两轮原F1均为0.9166666667。
冷暖原始答复在源位置7不同，完整输出关系和质量相同；原始差异保留。

| 已观测区间 | 秒 |
|---|---:|
| 最早外层入口→冷 sink | 55.426604 |
| 内层 controller→冷 sink | 55.000597 |
| 新鲜 source→冷 sink | 46.179451 |
| 新鲜 source→暖 sink | 1.587665 |

GPU空闲、owned进程、新SHM和私有socket检查通过，全局及内层租约释放，46原件散列核验通过。
计数依据为原生唯一终态、共享SQLite额度和scheduler finished50；深层generate独立日志、
每请求原生SamplingParams未观测，不能扩大为独立generate/kernel计数或逐请求参数实证。
原始 `HANDOFF.json` SHA-256 为 `b994aefb44935d19e6fc36585a69ef632d094bd1f3ecd0161c798e8335b8ec51`；
可按运行标识向项目证据保管者取得，归档SHA为
`0082d802f45b7d80159944b200109c60a9efba9bd9acd8384d4874c0e53b8ecb`。
内层supervise的600秒含30秒退出预留，之后的finally cleanup_owned另有最多30秒；
实际外层supervise为810秒含30秒预留，物理清理总截止为900秒。旧数值未追改为新边界。

## 当前维护性迁移

正式入口是现有 [SemBench text CLI](../../../../code/src/baselines/README.md#lotus-native-resident-sembench)：
`lifecycle=resident` 显式启用，原 isolated 默认不变；原 stage 借用外层owner，
每轮新鲜source/prepare至原完整sink，既有 `native_query_lifecycle` 和监督器统一负责关闭/进程退出。
声明式配置验证所选当前源码、输入发生项、模型/tokenizer、参数和资源，不再硬编码fd06或私有helper地址。
source、实际model-ready、原生submit/terminal、sink、后置snapshot/审计/评价/持久化和最终close分列。

候选 `1af1a75278238f04ee9744a1811052316f2b1acf` 的代码树为
`8b290e813550308d15b1a9855a82881494de277b`；完整回执见 [INTEGRATION.json](INTEGRATION.json)。
初始123项CPU回归通过，其中3项在指定已有SemBench checkout后补跑通过。
完整回放先发现发生项摘要误含每轮执行ID，在首次请求前停止；修正为源位置、源索引和字段值的摘要，
保留每轮独立请求身份，并新增回归、重跑受影响8项通过。合计124个不同检查，0失败、0未补跳过。
覆盖借用owner成功/异常归属、参数不匹配、默认isolated、原生关联/预算、跨后端生命周期和时钟边界。

正式入口Q3a冷25＋暖25、Q4冷24＋暖24的完整CPU回放均通过：使用真实LOTUS 1.2.4 accessor、
LM、LiteLLM及IPC子进程，保存答复按完整消息绑定，异步完成次序变化不改变源行对应。
逐发生项消息、完整token、答复、生成参数、完整原关系、sink与原评价均相等；
Q3a两轮参考F1为0.9166666667，Q4为0.4864864865，均只是保存答复的评价回放。
每批一个实际CPU模型替身PID及出生身份、一次构造/关闭，50/48个唯一软件请求终态，超额拒绝，
新鲜source→sink→snapshot/审计→评价→持久化顺序通过。两项正式CLI `check`亦通过。
该回放未执行host监督器或真实GPU，不提供新的性能、物理GPU清理或模型输出结论。
本地新模型/GPU调用为0，冻结真实结果仍属于ee625/fd06，不改称当前候选实测。

## 退出与待验证范围

本整合树未引入 `experiments/reproduction/lotus_mmqa_resident` 或旧Q4控制器副本。
它们在原工作树/冻结packet中仍是已验证复现证据，当前不删除或修改。
新日常用法只指向正式CLI；CPU和目标导入/绑定已通过，待活动调用方迁移及必要退出职责核验后，
由协调者迁移活动launch/config并退役旧部署副本，保留精确commit、散列和原始成功/失败证据。
本轮不改Ray Q7及其旧Q1 semaphore cleanup依赖；公共host/socket helper尚有活动调用，不能删除。

| 旧复现职责 | 当前正式替代 |
|---|---|
| `lotus_mmqa_resident/resident_stage.py` | `code/src/baselines/text/frameworks/lotus_native_stage.py` 借用owner |
| `lotus_mmqa_resident/resident_worker.py`、`mmqa_binding.py` | `code/src/experiments/sembench_text_resident.py`及既有原查询/原sink |
| `lotus_mmqa_resident/timing.py` | `code/src/observability/native_query_timing.py`与既有生命周期事件 |
| `lotus_mmqa_resident/source_contract.py`、`CONFIG.template.json` | `sembench_lotus_native.example.json`声明当前源码/输入/资产身份 |
| `lotus_mmqa_resident/run_cpu.py`、`cpu_response_child.py`、`cpu_guard.py`、`check_budget.py` | `code/tests/baselines/text/replay_lotus_resident.py`、既有IPC fixture及预算回归 |
| `lotus_mmqa_resident/server_controller.py` | 正式CLI＋既有supervisor；host租约/资源/物理清理仍由唯一外层协调者提供 |

表中旧路径相对 `experiments/reproduction/`，均只在受保护旧树/冻结包中存在，未整包合入本候选。
当前源码及依赖清理保留main已删除的`lotus/sdk.py`和`SDK_FILES`，未恢复旧包装层。
共同协议仅移入 `acae5b2a` 的正文与导航两文件差分，保留各自原生版本及统一时钟合同；不硬合旧分支或新建协议。

## e097目标环境的零模型迁移验收

[TARGET-CPU.json](TARGET-CPU.json)对应 `baseline-main-e097-target-cpu-20261110-r1`：
真实目标依赖中的正式Q3a/Q4 `check` 各执行一次，源码、原数据、有序发生项及声明的完整四分片、索引、
模型配置和tokenizer文件摘要均通过。权重只作为文件核验，未构造模型；不是仅核tokenizer或模型名。
既有host监督器、资源监视和租约实际监督CPU子进程，进程/SHM/租约退出通过，59原件完整验收。
首批Q3a已通过，但后置审计误读尚未惰性初始化的shared_requests表；保留失败原件，修正审计后只续Q4，未重跑Q3a。
模型、generate、查询执行及预算消费均为0；本次未发现导入/绑定迁移需要重复旧GPU的差异。
原始HANDOFF SHA为 `825ec8f7dcf950a1af7e5a28fe88b167041b0a4dc464e859a92eac3a68fe4e53`。

此结果不授予e097或后续文本PR新的真实GPU/模型退出资格。完整旧模型结果仍绑定ee625/fd06，
新Daft审计边界修正另见其报告。正式调用方迁移仍需保留同硬件资源、真实外层计时及物理清理责任；
只有实际语义或原生兼容性发生变化时才决定必要的最小目标验证，不自动重复50/48调用。
