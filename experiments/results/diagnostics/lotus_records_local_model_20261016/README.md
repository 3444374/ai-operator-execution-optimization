# Movie Q3 records 到本地模型接点与分层资格

本切片在已审阅 main `52391e375b6d9699e5bd76bb6d37bdbd939ef23e` 上独立实现，
集成时同步已审纯文档 main `d73c44463e9fdb98e2ac1397295114568afabb1c`，由 `integration/data-execution` 集成。
后续再同步已审纯文档main `6e3828b4a05d16962efb1166bb872a7fd97ae627` 的Daft布局／消费与有限算子链合同；下述实机仍按各自固定源码解释。
2026-10-19 同步纯文档 main `9b74f00db35ffa532ca74ecf9fae65ae38015ef1` 的原生查询与计时边界；不改变历史模型资格来源。
最终 Daft／Ray Data／IMLane CPython 与作者 Ray 对照均保留为计划要求；首阶段支持普通 Movie Q3 的 Filter＋COUNT。
后续 Core CPU、有限链和共享引擎增量在下方分别记录，尚无这些新来源的模型或性能资格。旧 request 接入、活动 baseline 和旧测试树没有修改。
2026-10-19 的 Daft 关闭修正单元已在冻结 `f526fbf1` 完成两查询／四次真实调用，见[独立结果](daft_model_close_correction.json)；此前失败、未知计数和永久预留保留。
当前普通 Filter 的 Ray Data／Daft／静态参照／Core 已消费中性扫描与 caller 绑定方法，换列换指令的资格为 CPU；不继承冻结 Daft 模型来源。

## 实现与来源

| 层次 | 本轮文件与采用行为 |
|---|---|
| 输入与逐调用记录 | [movie_records.py](../../../../code/src/baselines/text/movie_records.py)、[records.py](../../../../code/src/semantic_methods/lotus/records.py)：原 CSV、任务、谓词、schema、标签分离投影及摘要绑定；按 occurrence 保留 query／role／stage／call；失败与 unknown 的账本独立于最终 COUNT |
| 原方法 | [engine.py](../../../../code/src/semantic_methods/lotus/engine.py)：固定 LOTUS 1.2.4 的 dtype-aware 准备、formatter、parser、原 LM physical／virtual usage 与有限额度；不修改上游 accessor |
| 模型内核 | [vllm_engine.py](../../../../code/src/serving/backends/vllm_engine.py)：不同 Python prefix 的 driver／模型环境；worker 内常驻同步 `LLM.chat`；一次 chat template、真实 token IDs／usage／finish reason，TP1／PP1／uni executor |
| 原生 Ray Data | [ray_data_lotus_records.py](../../../../code/src/baselines/text/frameworks/ray_data_lotus_records.py)：原生 CSV→filter→projection→worker 准备→独立单 GPU 模型 stage→紧凑结果；原生图拥有物理分批与 actor 池 |
| 原生 Daft | [daft_lotus_records.py](../../../../code/src/baselines/text/frameworks/daft_lotus_records.py)、[私有引擎进程](../../../../code/src/baselines/text/frameworks/daft_engine_process.py)：Daft Native process UDF 原准备／私有模型进程／完整终态 JSON，原生 runtime 拥有分批及调度；共享 selector 不初始化 Ray |
| SemLoom 静态参照 | [lotus_records_static.py](../../../../code/src/execution_provider/adapters/lotus_records_static.py)：Daft 惰性读取与普通关系操作，Ray Core 准备／模型 actor 直接交接 ObjectRef；明确 `production_core_used=false`，属于 `diagnostic_reference`，尚无新策略结论 |
| 查询、评价与退出 | [lotus_records_movie.py](../../../../code/src/experiments/lotus_records_movie.py)、[supervisor](../../../../code/src/experiments/lotus_records_supervisor.py)：原 SemBench evaluator；成功才形成完整查询结果；关闭失败不覆盖前缀 usage／原错误；有限总期限与已观测 owned 进程回收 |

源码依据为已有受信 LOTUS/SemBench 适配及以下固定公开实现：

- LOTUS `b1a85fd7a66fabed8a1585d44d7597d592b4433f` 的 `sem_filter.py`、task instructions、postprocessor、LM stats／limits 和 cache/settings；实际安装版本与逐文件 SHA 均核对。
- SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2` 的 Movie Q3 与原 `AccuracyEvaluator`；只读干净 checkout。
- [Ray 2.56.1 Dataset](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/dataset.py) 的 map／map_batches／ActorPoolStrategy，以及 `runtime_env.py_executable`；不是官方 vLLM Processor 的资格。
- [vLLM 0.25.1 LLM](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/llm.py) 的同步 chat／sampling／shutdown 接口；首阶段只作源码与替身合同；后续真实模型资格另列固定源码。
- [架构 §8.7–8.8](../../../plans/系统架构.md#company-engineering-reference) 已有工程参照：保留方法与执行职责、昂贵实例复用和查询独立状态；本轮没有读取、复制或移植公司源码，也没有安装 pgml。

附件均经本执行环境 Library 流程材料化，5 件可读；ZIP SHA 为
`42251c0e02b422dc3ee4e4a3d821d5b447d49ff23bea9d1783f14845cd395a46`。
29 条 ZIP 路径安全、28 个清单摘要一致、16 个 Python 文件仅作 AST 静态读取。
附件代码、安装脚本、测试未执行；没有整包导入或生产 v1／v2 副本。仅吸收环境隔离、准备／模型 kernel 与静态参照的方向，
重新实现来源绑定、账本、LM 职责、评价与完整进程监督；附件简化 cascade 不采用，旧 PZ 包未扩展。

## 实际验证

[verification.json](verification.json) 记录版本、执行范围和保留原件的 SHA；原始日志在仓库外保留，未删除失败运行。

| 运行身份 | 实际范围与结果 |
|---|---|
| `initial-tests` | 首次 82 项发现 3 个 `LMStats.model_dump` 兼容错误；改用原 dataclass 后，8 项受影响的真实 LOTUS／SemBench 测试通过 |
| `supervisor-tests` | 首次真实进程检查发现 leader 消失与 children 枚举竞争；仅处理 `NoSuchProcess` 并继续 owned PGID 检查后，16 项通过。leader-exit 额外 20 次通过只保留工具输出（chunk `fe8d21`），没有独立日志／文件 SHA |
| `ray-cpu` | 首次 Ray 启动因 macOS 临时路径超过 Unix socket 长度失败；无模型调用。保留原失败，下一运行使用新的短 `/tmp` 目录 |
| `ray-cpu-shortpath` | 真实 Ray 2.56.1 的 CSV／filter／projection／worker LOTUS formatter／CPU 固定响应／原 parser／原 evaluator 通过；COUNT 1、TP 1、TN 2。监督器确认 18 个已观测 owned 进程退出 |
| `final-tests` | Python 3.12.6，106 项和 211 个 subtests 全部通过，无跳过；含真实 LOTUS 1.2.4／LiteLLM 1.95.0／OpenAI 2.50.0、Ray 2.56.1、Daft 0.7.21。18.29 秒为测试墙钟，非查询性能；外层监督 19.67 秒、已观测 owned 进程剩余 0 |
| `supervisor_redaction_regression_20261016` | Standards 独审指出原 worker.log 未经脱敏落盘；改为非阻塞 PIPE、落盘前逐行脱敏、16 KiB 长行整行省略及计数。21 项实际进程／替身回归通过，不等待未知 writer |
| `post-review-tests` | 修复两轴 P2 并补 driver 绑定和静态模型 CPU 预留后，114 项／213 subtests 全通过、0 跳过，18.15 秒；外层监督 19.43 秒、已观测 owned 进程剩余 0、输出完整且已脱敏 |

Spec 独审指出空源原生参照丢失 DataFrame schema，现显式保留列；空源和无谓词命中均通过原 Q3 差分。
首次补测误以为原 Q3 跳过 LM 包装层，2 个 subtests 失败（工具输出 chunk `2fce28`，无独立日志），
按固定源码改为保留一次空 batch 的包装调用／有效 kwargs，实际模型 call 和 usage 仍为 0。
公开 graph／CLI 的 driver 解释器现与配置 prefix／executable 目录／base prefix 绑定，检查先于模型分配；静态模型 actor 显式预留 1 CPU／1 GPU。

两位新独立 reviewer 分别完成 Standards／Spec 审查，并对修订提交
`b3f6cfaf0c4a176711dc30fbd06a6608509eb159` 定向复核：各发现 1 项 P2，均已关闭，未发现残留。
二者均核对了修后代码／日志／监督报告摘要；审查只读，未重跑测试或加载模型。
同步 `d73c4446` 仅涉及已审文档；最终代码字节与修后资格记录保持一致。

真实库检查明确区分两部分：

- 原 LOTUS Q3 固定响应差分逐项核对有效 messages、parser 决策、COUNT、原 physical／virtual stats 和有限额度；成功后调用原 SemBench evaluator。重复 reviewId、稀疏输入、空串和非法输出有独立用例。
- 实际 Ray CPU 图验证了 driver 解释器 worker 的模块可见性；模型 stage 是明确的 CPU 固定响应替身。Daft typed CSV 确认空串、`NULL/null/None/NaN` 与换行文本保持；该首阶段的生产 static actor 图只有合同替身测试，后续真实 CPU 图与模型资格另列。

此外覆盖源摘要篡改、重签投影、调用身份错误、未知 usage、失败前缀、quota、取消／期限、close 失败与重试、leader 先退出及 TERM→KILL。
首阶段 CPU 环境在仓库外单独创建，按 runtime 规则保存安装前后 check；其 driver CPU 依赖检查为 OK，未安装 vLLM、下载权重或构造模型。

复现使用固定原 SemBench checkout、上述版本的独立 Python 3.12 环境，设置 `PYTHONPATH=code`、
`SEMBENCH_CHECKOUT` 和离线环境。九个新增测试文件从本报告对应源码旁的 `code/tests/` 运行；
真实 CPU 库检查另设置 `RUN_LOTUS_RECORDS_CPU_LIBRARIES=1`，入口为
[test_lotus_records_libraries.py](../../../../code/tests/experiments/test_lotus_records_libraries.py)。
完整测试以 `lotus_records_supervisor.supervise()` 包裹，180 秒总期限／10 秒清理保留。
实际 CLI 与外置配置见[脚本说明](../../../../code/scripts/README.md#movie-q3-records-本地模型接点)。

## Daft 独立接入与集成 CPU 资格

职责 `fc5380a9` 的[模块合同](../../../../code/src/baselines/text/frameworks/daft_lotus_records.README.md)和[独立证据](daft_verification.json)分别固定实现与真实 Daft0.7.21 process 图资格：65 项CPU／原LOTUS／原SemBench evaluator及单独新Python的旧vLLM替身30项，均零skip、零模型／GPU。失败运行与两轴审查修复保留，不将作者固定响应当作实际推理。
集成后新增共享 `daft-native-local-model` selector，原惰性执行工厂和外层supervisor保持；Daft 不导入／初始化 Ray。实际批检查先于首个模型进程创建，模型关闭失败不覆盖已完成结果或usage。
`daft-integration-tests-r1` 的26项／2subtests、0skip通过，测试62.56秒，外层63.76秒；已观测owned进程全部退出。命令、实际源码摘要和原件 SHA 见 `verification.json.daft_integration_cpu`。本次仅固定响应，真实vLLM／GPU、fused、级联、性能仍pending。

## API 交接与剩余资格

共享方法使用 `FilterMethod`／`GenerationSpec`，records 必须携带 `identity()` 的 query、model role、stage、row occurrence、source position、call index／ID。
worker 本地 `LotusFilterMethod.prepare_rows()` 产出未套模型模板的 `messages_json`；模型 owner 返回紧凑 terminal rows，
`run_movie_query(..., execute, query_id=...)` 在分配前检查、按 call 记账，最后由原 LOTUS 解释并评价。

source 是共同离线投影的无标签 `records.csv`；driver 的 `preflight()`／`expected()` 在 `query_seconds` 起点前读取并核验 records、references 与原 CSV，后端仍会再次核验和读取。
同步 `LLM.chat` 的批次返回与共同端收齐整个 Filter 后才原 parser／COUNT 是两个屏障；结果迭代不等于逐行推进后继。成功调用的异常格式由原 parser 解释，执行失败／unknown 不作为语义答案。
`query_seconds` 在完整 COUNT 后、evaluator 前截取；结果交付前的主动模型关闭与 EOF 收尾可能在内，最终 owner 清理按实际时点及 `elapsed_seconds` 报告，不将全部清理一概移出或移入主计时。
IMLane 新桥可复用这些值合同和方法核对，必须由作者 DBEnd／session 拥有调度；本报告的 LOTUS 模型切片未覆盖它，[独立无模型echo与集成检查](../imlane_records_echo_20261016/README.md)另列。

当前仍为 pending：首发早于末准备及峰值留存观察、原全局 helper-LM 级联的模型资格、AsyncLLM 持续供给、图像、
Daft 原生 process UDF 的完整模型／GPU资格、IMLane 模型方法与作者 Ray，以及新 Core 与有限链的模型资格。
单次同步 `LLM.chat` 跨批排空，不能称为跨批持续供给。下方单卡资格只覆盖实际执行的普通 Q3 小切片。

source 目前限制 4,096 行、CSV 16 MiB、单 review UTF-8 1 MiB；这些是有限输入资格，不是 RSS／GPU 显存硬上限。
LM result cache 关闭，非默认 serialization、rate／TPM、logprobs、repetition override 等未支持配置明确拒绝。
后端进程监督只确认已观测且身份匹配的 owned 进程；未观测 detached 后代保持 unknown。后端 API 的 GPU 状态与外层独立实测分列，
`ray.kill`、API shutdown 或 iterator close 均不作为 GPU 退出证明。本轮按父线程协调使用既有服务器与资源，未新增租用或接触学校服务器。

## 真实资格准备与独立 CPU 记录

在已发布 `957e91eb` 后准备既有服务器的单 4090／Qwen2.5／vLLM0.25.1 资格，应用模型请求总上限32、零自动重试，不下载权重或修改共享环境。
源码 `06f4b687` 加入可选 `max_model_calls` 单引擎上限及真实 prompt token IDs／规范 messages SHA；隔离 Python3.12.6 下52 项受影响 CPU 测试和74 subtests通过、零skip。
对应命令与环境记录在 `verification.json` 的 `qualification_cap_cpu.run_3`，原件为私有 `nonmodel-evidence/model-qualification-cpu-r3.{json,log}`，字节数及 SHA256 见 `raw_evidence`。
先前系统 Python 的37通过／1依赖skip及另次52通过的简略日志分别登记为 run_1/run_2；run_2 的pytest通过后，外层zsh因使用只读变量 `status` 退出1，现由完整记录的 run_3 补齐调用来源。
原 114 项资格仍绑定其记录源码，不转授这些后续改动。真实运行的精确 source、模型／模板、unit、版本和结果另按实际记录，本准备段不构成真实模型结果；实测来源见下一节。

## 普通 Q3 的单卡真实模型资格

使用既有单张 RTX4090（24,564 MiB）、隔离的两个 Python3.12.3 环境：driver 固定 LOTUS1.2.4／Ray2.56.1／Daft0.7.21，模型环境 vLLM0.25.1 且无 LOTUS。
[模型身份](model_identity.json)逐一核对已有 Qwen2.5-7B-Instruct 固定 revision `a09a35458c702b33eeacc393d103063234e8bc28` 的四个权重 SHA256 与七个配置／tokenizer Git blob；仅获取官方元数据，没有下载权重。
原 CSV 前15行按原谓词取 source positions 1、14，均为负例；选择前缀先于标签评价。生成保持8 tokens、temperature0、top_p1、seed0、stop换行，BF16／TP1／PP1；准备批2、模型批1、一个在途批。

| 独立单元 | 固定生产来源 | 实际结果 | 应用调用账目 |
|---|---|---|---|
| engine-native | `06f4b687` 的387文件闭包 | 直接模型接点和原 SemBench Q3 accessor 各一次；原提示／一次模板／真实 token IDs／parser／LM统计／COUNT／原评价一致 | 已确认4；一个引擎 lifetime |
| ray-data | 同一 `06f4b687` 闭包 | 原生 CSV→谓词／投影→LOTUS准备→本地引擎→原解析／评价，与参照逐调用一致 | 已确认2；一个引擎 lifetime |
| static 首次 | 同一 `06f4b687` 闭包 | Ray先初始化后，Daft推断Ray runner，Native guard在模型创建前拒绝；原失败及close TypeError保留 | 原计数为null，两个未确认usage的账本项保留；未观测引擎UID |
| static-corrected | `e4c07188`，同387路径仅 static文件改变 | 显式Native初始化；两次新 query复用同一引擎、call IDs隔离，与原参照全值一致 | 已确认4；一个引擎 lifetime |

[首次原样派生记录](model_first_run.json)保留两成功与一失败；[独立静态补充记录](model_static_correction.json)单列新身份和原始证据摘要。
原生 accessor 只在 scoped、uncached 本地模型 seam接入，未修改原 accessor；这是方法诊断参照，不作为性能 baseline。
每次完成的 Q3 查询均 COUNT0、原 evaluator误差0、TN2；两个负例不能证明全库质量或性能排名。
原生单元另核真实 SamplingParams、HF直接模板／render后encode／vLLM实际prompt IDs三路一致。
Ray和static将引擎实际 messages SHA及真实prompt／output IDs与该参照核对；首次报告里Ray的旧scope文字已在派生记录明确标记为过期描述，原件不改。

首次static的Native guard失败通过21项合同／8项新回归及真实Ray／Daft CPU双查询图定位并修复，见 `verification.json.static_native_runner_fix_cpu`。
新静态单元复用同一持久SQLite：前三项各8次永久预留均已claim，新增8次后为32/32，不退款、不重建或延长原期限，不重跑两个成功单元。
补充harness两独审关闭期限缺口后，实际监督和child hard／work deadline均夹紧原截止，保留15秒清理；8项CPU检查另有原件。
全部成功单元共已确认10条实际请求；首次static的未知计数仍为null，不强行给全轮实际总数。没有额外应用预热或自动重试，vLLM内部启动profiling／编译单列。

首次controller与补充controller分别观察204／37个owned身份，均确认退出；最终独立核验两个controller自身也已退出。
各单元后及最终GPU compute inventory为空、显存回到1 MiB，无foreign工作，normal shared lease已按owner释放。
API的GPU unknown与这些外层独立结果分别保存；原始报告、日志、冻结配置和前后账本副本均在仓库外归档并逐文件SHA核验。
实机来源是固定Git归档的文件字节验证，没有将服务器目录冒称为已核对的Git checkout。

该资格覆盖普通Filter的真实小切片、同方法差分与观测到的生命周期。SemLoom仍明确为未接production Core的静态参照；
Daft独立路径、IMLane、全局级联、图像和AsyncLLM持续供给沿各自后续资格推进。vLLM日志的内部asynchronous scheduling不代表本适配已实现跨批AsyncLLM供给。

<a id="daft-双查询模型资格准备尚未运行"></a>
## Daft 双查询资格的历史准备

基于固定 `7d12d63c` 的389文件字节闭包，已准备独立 Daft 入口、来源证明与有限预算，
见[准备记录](daft_model_preparation.json)。新账本最大16次应用调用，只计划为首个双查询单元永久预留4次，
每查询私有引擎 cap2、单Infer并发1、零重试、每图只消费一次；准备记录形成时尚未创建账本或执行该预留。
旧32/32永久预留及失败未知项不退款、不复用。单元外层监督1200秒并留15秒清理，不设工程累计截止时间。

Python3.12.6 的控制器8项、child 7项检查全部通过；实际 Daft0.7.21 的安装源码摘要和CPU物理计划
检查通过，GPU1计划门槛明确拒绝CPU0计划。`explain` 只给出预测计划，实际消费会重新优化；
调用上限证明还依赖受审单线图、完整execution／planning配置摘要保持一致，以及本隔离进程无配置写入。
摘要观察不是框架的原子配置锁，CPU检查也不是模型或GPU资格。

原SSH复用socket已结束，首次严格主机校验、公钥只读连接在认证前关闭，资源／lease现场预检未执行。
2026-10-19 重查本机未发现可复用通道；严格公钥只读探测退出255、标准输出／错误均为空，无法据此确定失败阶段。
用户随后明确服务器尚未开机；新探测原件与摘要另行保留。恢复后须先重新核对资源、模型／输入身份与正常lease。
上述准备阶段未上传新源码或发起模型调用；恢复后的首轮失败记录见下一节，原准备 JSON 不改写。
IMLane本轮仅形成[方法与usage交接](../../../../code/integrations/imlane_records/README.md#普通-filter-方法接入交接)，没有增加第二个GPU单元。

## Daft 首轮真实模型：关闭超时，查询失败

2026-10-19 恢复原服务器通道后，重新只读核对输入、隔离 Python 环境、11 个模型文件与空闲 GPU。
固定 `7d12d63c` 的389文件字节闭包及原方法、生成配置、输入与原生参照均保持；
重启后的配置修订只重绑三个平台空闲进程的 PID／出生身份。初次环境检查因缺少 `DATABASE_URL` 停止，
随后仅在仓库外加入无账号／密码的 localhost 占位值，环境复核通过；records 路径不连接数据库，未安装依赖。

[首轮记录](daft_model_first_run.json)保留失败：第一 query 返回两条 `Answer: False` 成功终态，
已知 prompt253／completion8／total261 tokens、一个引擎 lifetime；EOF 时私有引擎关闭超时，
故原 parser、COUNT 和 evaluator 均未完成，第二 query 未启动。最终引擎计数仍为 `null`，不能用两个已知终态代填。
定位为父进程默认5秒关闭中只给 RPC 2.5秒，而 child 调用引擎 API 的期限为10秒；原件保持原样。

同一个新16次账本永久预留并 claim 首单元4次，不退款、不重跑；旧32/32账本前后 SHA 相同且只读。
外层确认33个已观察 owned 身份退出，controller 自身退出、正常 lease 仅由 owner 释放；
独立 GPU 检查为空 compute／1 MiB。后端 `engine_api=null`／`gpu_exit_confirmed=null` 与这些外层观察分开记录。
38个原始文件已在 Mac 独立归档、逐成员大小和 SHA 核验，归档 SHA 为
`e706d80ad331f2d9bb71232ae927d0adf2b8b1eb53850d0f55d4f15c13db1262`。
修复后的新单元需另绑源码、另占同账本额度并独立审查；本失败不提供完整 Daft 查询或性能资格。

关闭修复 `11dda33b`／`cc96c8d` 已通过两轴独审：默认锁后15秒，RPC11秒／清理4秒，
已启动进程须取得字面值 True 的 API 确认才可 closed；API已确认后的 OS 清理重试不再重复 RPC。
旧行为和四项确认反例先复现，新增5项／原17项及后续16项定向CPU检查按各自来源保留，不能相加作唯一测试数。
GPU保持unknown，硬墙钟仍由外层supervisor负责；`verification.json.daft_close_repair_cpu_20261019` 登记新源。
用于修正资格的 `f526fbf1` 从原7d12只改变私有引擎这一个生产文件，389路径闭包其余字节不变；后续运行单列如下。

## Daft 关闭修正的真实模型资格

新单元 `daft-close-corrected-double-query` 执行两个不同 query ID，各处理原15行前缀中的两条命中记录。
两次独立引擎寿命各有2次成功调用、253／8 tokens，总计4次调用、506／16／522 tokens；COUNT均0，原SemBench误差均0、补充审计各TN2。
实际消息、prompt／output token IDs、文本、usage、finish reason、原parser均与固定原生LOTUS控制核同；两个负例不代表完整质量或性能。
API每寿命关闭一次，所属进程退出与独立GPU1MiB／无compute分别核查；外层共59个已观察owned身份退出，正常lease释放。
41个归档成员943,843字节已下载、逐成员大小与SHA核同，归档SHA为 `6868bd9be1b72d2085e94e2fea8438c4362e6187a26c9939494141adb30e5ee9`。
同一16次账本保留原4次永久预留，再新增不同单元4次，现8／16；旧32／32 SHA不变，无退款、自动重试或额外暖身。
原失败的最终实际计数仍null，不能与本单元4次相加推算全轮调用数。
精确源码、模型／配置／原件SHA和范围见[结果JSON](daft_model_close_correction.json)。本资格不覆盖新通用方法、Core、有限链或级联。

## 共享引擎与 actor owner：CPU 增量

为有限链提供[逐调用生成配置](../../../../code/src/serving/backends/README.md)：职责 `64044682` 在
`run_prepared(..., generation=None)` 保持构造默认；显式配置整份覆盖本次调用，每次新建采样参数。
JSON 仅为 `json_object`，不增加严格 schema 或改写 stop；普通 Filter 方法明确拒绝该模式。
本段原资格形成时 logprobs 尚未开放；后续有限日志接口另列如下，普通 Filter 方法仍拒绝 helper 配置。

职责 `f1d5f780` 提取[准备／模型 actor owner](../../../../code/src/execution_provider/adapters/lotus_records_actor_owner.py)，
static 的 `_StaticRows`／`evaluate` 调度保持原样；owner 不调用生产 Core，也不拥有查询或逐调用账本。
后续 `8d5d9418` 只增加受信 actor 类型覆写点；独审发现模型构造部分失败后可能误报 empty closed，
`cabba5b5` 单独修复为保留 unknown／句柄、禁止重构与模型 actor kill。未尝试构造的空关闭保持兼容。

合成来源的内核51项、原LOTUS方法及 owner／static／Ray合同44项在隔离 Python3.12.6 下通过，零skip、零模型／GPU；
后者包括真实安装 LOTUS 的6项方法检查，Ray 仍为合同替身。
提取提交 `f1d5f780` 另有真实Ray2.56.1／Daft0.7.21双查询、原LOTUS parser与原SemBench evaluator的CPU图；
6条固定响应、COUNT1／TP1／TN2，外层确认14个已观察进程退出。其首次沙箱进程检查 unknown 和后续核验保留；
factory／启动失败修订只重跑CPU合同，未将这次图测冒称为修订后重跑。
两轴只读独审均已通过 generation／Filter guard 和 owner 修订，原件与源码SHA见
`verification.json.shared_records_cpu_20261019`。新增代码没有真实模型或性能资格，旧模型结果仍只绑定原归档来源。

<a id="records-core-cpu-20261019"></a>
## Core 接线、逐调用身份与日志：组合 CPU 资格

新增 [Core records adapter](../../../../code/src/execution_provider/adapters/lotus_records_core.py) 与显式
`semloom-local-model-core` selector，复用原 `SessionEngine`／`BoundedAsyncBackend` 和共享 actor owner。
首版要求 prepare／model batch rows 均为1；raw records bytes 由 Core 接纳，准备 ObjectRef 直接交给模型 actor，
结果消费后 release，unknown 保留 Core 额度及引用。工作单位为 model calls，不是 token 成本。
`production_core_used` 从实际 SessionEngine owner／iterator 回填；静态参照仍为 false，模型与 GPU 关闭状态独立。

共享 runner 合并 close 期间已知终态后再标 missing unknown，只允许升级自身创建的缺失占位，
不覆盖 backend 的真实 unknown，不重跑语义解释。`identity()` 允许声明 role／stage，旧普通 Filter ID 字节不变。
共享引擎 `logprobs=0..20` 返回与 output token IDs 对齐的 OpenAI content 数组；
缺日志或非法概率失败时保留真实 usage，不填造概率。逐调用生成配置保持独立，原全局级联的控制流由方法负责。

原职责 Core fixture104项、集成 wiring 与 selector108项及隔离内核51项按各自来源分列；
新 logprobs62项包括真实 LOTUS LM／LiteLLM 合同，role／stage59项和迟到 receipt34项另列，均零skip、零模型。
最终组合在实际 Ray2.56.1／Daft0.7.21、LOTUS1.2.4 与原 SemBench evaluator 下运行：
static／Core 各两 query、共12条固定 CPU 响应，逐调用消息／解析／usage／结果一致，均 COUNT1／TP1／TN2。
Core 标记为 true、静态为 false，两个 Core session 独立、每臂复用同引擎；Core 额度归零，transport 关闭，20个已观察进程退出。
模型 kernel、GPU 分配及解释器预检为显式替身，token IDs／每查询12 tokens 为合成值，不能外推真实模型。
首个补充启动器漏建报告子目录的失败及退出报告保留，修启动器后在新目录通过；生产源码未因该失败改动。
来源、原件 SHA 与逐项范围见 `verification.json.core_logprobs_integration_cpu_20261019`；新 Core／引擎 GPU 资格仍待验证。
安全节点同步 main `b8b5f3d1` 的纯工具职责后，records 查询直接从中立 `private_artifacts` 导入 `json_metric`；
旧 PG 导入仍重导出同一函数对象。30项工具／查询 CPU 回归通过，未改变 evaluator、序列化值或历史模型来源。

## 有限链独立接纳

选择接纳职责 `62001fb9`／`52037286` 的新方法、Ray fused 图、私有传输及原 CPU 证据，
见[有限链报告](../lotus_record_chain_20261019/README.md)。已有共用阶段循环覆盖 Filter→Map→Extract 与 Map→Filter，
生成列进入后继 Filter，重复消费与末阶段后关闭均有 CPU 检查；没有为 Map→Filter 新建执行器。
整合有限 logprobs 与关闭修复后，18项原LOTUS方法差分加24项原生／传输检查重新通过（含5项实际Ray CPU图），
零skip、零模型／GPU，67个已观察进程退出。首次启动器使用短测试模块名导致4项worker导入失败，
改用可导入的完整模块名后在新目录通过；生产源码未改变，失败原件仍在。
本次组合源码与原件SHA见 `verification.json.finite_chain_integration_cpu_20261019`。
本段形成时单Filter公共后端尚在迁移旧Q3绑定；后续四后端的独立验收见下节，不向其他路径转授资格。
新链真实模型、Daft／IMLane／Core 有限链、跨阶段 AsyncLLM 仍待验证。

### 共享批内核及 Daft／Core 有限链 CPU 增量

后续选择接纳 `6c584426` 的 [FusedChainBatch](../../../../code/src/baselines/text/frameworks/lotus_chain_batch.py)
与 [ChainBatchResults](../../../../code/src/baselines/text/frameworks/lotus_chain_results.py)。前者只调用唯一的
`LotusRecordChain.run_batch()`，后者只验证结果、身份、来源及账本，不拥有执行或关闭调度。
Ray 原入口和错误类型保留，六个消费／关闭方法 AST 不变。22项共享合同、2项真实私有 CPU 传输及5项实际 Ray 图通过；
首次同 driver 重复初始化 Ray 的失败保留，改为各图独立 driver 后，72个已观察 owned 身份均退出。
该实库运行与提交的批模块仅末尾一处空行不同，原 SHA 可由提交字节加一个 LF 复原，完整 AST 相同。
后续只读 `cleanup_errors` 快照另有独立审查，不改接纳或关闭状态。

| 新路径 | 实际执行与边界 | 本轮资格 |
|---|---|---|
| [Daft 有限链](../../../../code/src/baselines/text/frameworks/daft_lotus_chain.py)，职责 `73a34b71` | 原生 process class 拥有分批／缓冲；每批由共享内核运行，首行携完整 envelope、其余 null 由原生 filter 去除；不使用 Ray。模型输入批上限生效，prepare_batch_rows／max_active_batches 不作为该布局的调度参数 | 13项零skip，包括7个实库用例（含预分配拒绝）；FME／MF／MMF／FFM 分别7／6／9／7次合成评价，各为两个原生批、一个引擎、两条最终发生项；原提示、生成、解析、统计及关系核同，29个已观察 owned 退出 |
| [Core 有限链](../../../../code/src/execution_provider/adapters/lotus_record_chain_core.py)，职责 `515179f8` | 原 SessionEngine 接纳有界 raw batch bytes，BoundedAsyncBackend 单槽交本机共享 worker，再经私有模型进程 pipe；此 profile 不使用 Ray／Daft，与上方普通 Filter 的 actor profile 分开 | 23项零skip，真实 Core／异步 backend／LOTUS；其中私有进程协议及 OS 退出真实，模型和解释器探针为替身。单 owner 单 query，cpu_workers／max_active_batches 均为1，work 是 rows×stages 上界 |

两个新路径均覆盖生成列传递、业务 row_id／call_id 与内部身份分离、重复业务索引、空源／全false、失败已知usage、取消、unknown与清理。
根线程在当前组合上另跑70项协议／Core fixture／原LOTUS方法检查，零skip通过；该次明确不运行原生runtime或模型子进程，实库与OS证据仍按上表来源分列。
Core unknown 保留额度及输入；一次保留的清理 future 只等待同一 owner，不重建模型。调用方仍须提供独立硬墙钟监督。
逻辑输入／结果 bytes 不包括 worker 副本、审计文件与消费端关系／账本，不能作为 RSS 或 GPU 上限。
Extract 仍限末端，跨 query 复用、跨 batch 持续供给、链 CLI／完整 SemBench 任务及全部新链模型资格仍pending。
精确来源、保留失败及原件摘要见 `verification.json.finite_chain_backends_cpu_20261019`，不把分列回归累加成独立样本数。

## 原全局级联的独立 CPU 接纳

选择接纳职责 `8ac06508`／`259c4173` 的原控制流 records seam、Ray阶段及查询函数，
见[方法合同](../../../../code/src/semantic_methods/lotus/cascade.README.md)。原 LOTUS accessor／阈值学习代码对象在独立globals执行，
全体 helper评分、全局分位数、原重要性采样、main样本、阈值和回退合并均保留；不按物理batch重学。
集成有限token logprobs及严格关闭后，21项原方法＋2项原SemBench评价＋7项原生Ray／进程CPU检查通过，
零skip、零模型／GPU，25个已观察owned身份退出。新查询直接使用中立json_metric，未复制原函数。
当前仅Q3文本helper profile；各角色阶段按序创建／关闭，main可能加载两次，完整初始化／切换成本保留。
共享CLI接线、真实helper模型／GPU、Daft／IMLane／Core级联、其他任务及性能仍待验证，详见 `verification.json.global_cascade_integration_cpu_20261019`。

## 普通 Filter 的任务边界与四后端复用

`FilterMethod` 是唯一普通文本方法，旧 `TextFilterMethod` 为同对象别名。原 `parse_cols` 的 caller 列序在分配前冻结，worker验证实际列集合并保留该序，避免不同Python hash seed改变原提示。
任务拥有 `RecordScan` 的typed schema、payload列与可选等值筛选；Movie source／runner继续严格校验原Q3指令、列、谓词、COUNT与原评价。
Ray Data、Daft Native、SemLoom静态参照、production Core四个普通路径均已去掉Q3业务字段假设，包括Core `_fill()` 的输入编码。
185项职责合同零skip；实库四后端各两query、28条明确合成响应，换列换指令、有／无谓词、不同worker列序均与原LOTUS消息／判定／COUNT／LM统计一致；旧Q3图另有回归。
集成固定 `5c6d7a3b` 的79项方法／绑定／链／Movie／级联组合检查零skip通过，40个已观察进程退出。
首次根线程启动受沙箱进程枚举限制、未产生测试输出，child退出已独立核对；原件保留。三项旧Core未绑定错误在最终完整Movie27中关闭。
来源、各次失败和精确SHA见 `verification.json.generic_filter_integration_cpu_20261019`。所有新推理响应均为CPU替身，IMLane与helper级联不包含在本通用迁移声明中。
有限链共享原stage循环；Extract仍只允许末端，重复业务index及业务列名与调用身份分开。逻辑bytes额度不表示物理RSS或显存上限。

后续模型准备还修正owner的关闭范围：字面True API与成功kill请求只确认请求／API范围，OS与GPU仍待独立观察。
部分分配失败无model句柄仍unknown，静态未确认工作继续保留；最终91项CPU零skip通过，不将旧模型结果重新绑定新owner源码。
