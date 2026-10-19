# Movie Q3 records 到本地模型接点与分层资格

本切片在已审阅 main `52391e375b6d9699e5bd76bb6d37bdbd939ef23e` 上独立实现，
集成时同步已审纯文档 main `d73c44463e9fdb98e2ac1397295114568afabb1c`，由 `integration/data-execution` 集成。
后续再同步已审纯文档main `6e3828b4a05d16962efb1166bb872a7fd97ae627` 的Daft布局／消费与有限算子链合同；下述实机仍按各自固定源码解释。
2026-10-19 同步纯文档 main `9b74f00db35ffa532ca74ecf9fae65ae38015ef1` 的原生查询与计时边界；不改变历史模型资格来源。
最终 Daft／Ray Data／IMLane CPython 与作者 Ray 对照均保留为计划要求；首阶段支持普通 Movie Q3 的 Filter＋COUNT。
后续 Core CPU、有限链和共享引擎增量在下方分别记录；冻结 `ebe3035f` 的普通Core已独立完成四次真实调用，后续源码和新profile不继承该模型资格。旧 request 接入、活动 baseline 和旧测试树没有修改。
2026-10-19 的 Daft 关闭修正单元已在冻结 `f526fbf1` 完成两查询／四次真实调用，见[独立结果](daft_model_close_correction.json)；此前失败、未知计数和永久预留保留。
当前普通 Filter 的 Ray Data／Daft／静态参照／Core 已消费中性扫描与 caller 绑定方法，换列换指令的资格为 CPU；不继承冻结 Daft 模型来源。

<a id="finite-mfe-model-20261019"></a>
## 非 Q3 的 Map→Filter→Extract 模型小轮与失败诊断

2026-10-19 在 `8be539cf5cf66f0050732f1804bfccaaa90f11f7` 的 394 项文件指纹上，按 Core→Ray→Daft 各执行一次单 GPU 模型小轮；输入、提示和标签没有为结果改写。每条独立永久上限 6 次，共 18 次，无 warmup 或模型重跑。此处是非 Q3 有限链，不能替代上文普通 Filter 的结果。

| 后端 | Map / Filter / Extract 实际调用 | 输入 / 输出 tokens | 完整单元耗时 | 资格 |
|---|---:|---:|---:|---|
| Core 私有 pipe | 2 / 2 / 0 | 360 / 18 | 81.9454 s | failed |
| Ray Data 2.56.1 | 2 / 2 / 0 | 360 / 18 | 81.6806 s | failed |
| Daft Native 0.7.21 | 2 / 2 / 0 | 360 / 18 | 67.4457 s | failed |

Map 的质量检查均通过，但 Filter 对正、负两行均判为 false，因此 Extract 没有实际执行。原 LOTUS 的零模型重放复现相同决策；12 份提示摘要、同一 tokenizer/chat template 重建的完整 prompt token IDs、实际输出解码均匹配。Filter 只生成 4 tokens，低于上限 8，且以 EOS 正常停止。现有证据支持该固定提示下的模型语义误判，未发现 Filter 接线或生成截断问题。

另有独立的结果物化缺陷：`record_chain.py:records_to_frame` 从空记录重建 object index，原 LOTUS 空结果保留 int64 index。三路径重放均在最终 `assert_frame_equal` 报此差异，去除 index 后列、dtype 和数据相同。应把源 index 元数据传到空结果物化，补 `test_lotus_record_chain.py` 的空整数／字符串 index 差分，不能全局强设 int64。原失败不改写；完整 MFE 与 Extract 模型资格均未通过。

实际共 12 次调用、1134 tokens，单元各一个引擎，身份／父链／终态、token 与最终 counter 已核同。Core 结束后仅继续原账本中未 claim 的 Ray／Daft；没有预算重建或退款。最终已观察进程均退出，独立 GPU 1 MiB／空 compute、空共享内存及正常 lease 释放分列，旧账本 SHA 不变。完整耗时包含加载和关闭，不支持性能排名；美元成本未指定。配置、计数和诊断结论见 [mfe_model_qualification.json](mfe_model_qualification.json)，不含原始输入、提示或逐调用输出。63 项私有原件只保留归档 SHA、成员数和获取方式，未整包进入 Git。

<a id="map-extract-coverage-20261019"></a>
## 独立一行 Map→Extract 的模型覆盖补验

原 MFE 失败后另建 `one-row-map-extract-coverage-v1`，仍使用 8be 源码。一行带前缀的文本先由 Map 去掉前缀，再由末端 Extract 读取唯一 `rewrite` 列提取 `item/color`；不含语义 Filter，因此每个后端都必须实际执行这两个方法。两项 CPU 合成输出分别语义正确和故意不同，覆盖检查均通过而质量判定不同；模型检查继续分别核对身份／父链、列隔离、完整终态、原 LOTUS 差分、最终 counter 和退出。独立账本总 6 次、每后端 2 次，三单元各 claim 一次，无模型重跑，不复用或退款原 18 次账本。

| 后端 | 实际 Map / Extract | 输入 / 输出 tokens | 完整单元耗时 | 独立审计结论 |
|---|---:|---:|---:|---|
| Core 私有 pipe | 1 / 1 | 197 / 19 | 54.1263 s | passed |
| Ray Data 2.56.1 | 1 / 1 | 197 / 19 | 94.0191 s | passed |
| Daft Native 0.7.21 | 1 / 1 | 197 / 19 | 72.2567 s | lifecycle_failed |

三条 Map 与 Extract 的语义质量检查均通过，原 LOTUS messages／parser／usage／最终 frame 均匹配；实际共 6 次调用、648 tokens，各一个引擎、最终 counter 2。Daft 私有引擎 API 与进程已正常关闭，但外层 supervisor 在 driver 正常退出的瞬间发现仍存活的已观察子进程并发送 SIGTERM，所以生命周期资格未通过。该 supervisor 当场检查存活并立即发信号，没有先等待自然退出；此证据不能单独证明持续泄漏，也没有记录具体被发信号者，后续应从 `daft_lotus_chain.py:_DaftChainRows.close` 和 `lotus_records_supervisor.py:supervise` 做 CPU 定点复现。

本次私有控制器误漏 supervisor 失败判定，原报告的 `completed` 保留。独立汇总读取实际 supervisor 失败、SIGTERM 和错误后更正为上表，未把 Daft 升为通过；控制器判定已在本地用三条真实回执完成先失败后通过的回归，无新增模型。最终全部已观察身份退出，GPU 1 MiB／空 compute、空共享内存与正常 lease 释放已另验，原四账本加原 MFE 账本 SHA 均不变。用例配置、聚合计数和更正来源见 [map_extract_model_coverage.json](map_extract_model_coverage.json)，不含原始输入、提示或逐调用输出；46 项原件留私有归档。该结果只支持各自实际方法与退出状态，原 MFE、Extract 后继、Async 和性能结论均不转授。

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
IMLane模型方法与作者Ray、有限链，以及后续共享源码的模型资格。Daft关闭修正和普通Core的固定小样资格在后文独立登记。
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

## IMLane 通用 Filter与共享有限链的本地接纳

选择接纳 `52213fa5` 的原职责文件及历史报告，再接 `7cd08b3a` 的兼容／有限链和 `174ee853` 的丢账修复；
原作者 DBEnd／Coordinator／Lane／CPython executor 继续拥有调度与传输，C++桥字节保留。
[Filter](../../../../code/integrations/imlane_records/filter_host.py) 在 caller 冻结中性scan／原列序，wire 使用分离的 identity_json／payload_json；
raw Filter拒绝身份名文本列。[有限链](../../../../code/integrations/imlane_records/chain_host.py)使用一条transport occurrence承载有限原始批，
只调用共同 FusedChainBatch 与结果核验，业务同名列保留于嵌套values，不复制阶段循环。
FME／MF／MMF／FFM 共用同一路径；Extract限末端、单部署，query间owner／engine独立。

关闭独立保留20秒，不被过期query期限截短；共享API reply11秒／进程清理4秒不变，GPU另核。
作者启动前核方法Python3.12／Arrow16 ABI与固定源，模型解释器保持独立。
新模型配置显式声明CPU8／owned家族RSS32GiB／SHM2GiB及不超过8核的root child affinity；这些仍待实机绑定，旧echo默认和共享cgroup未改。

| 固定来源 | 已完成与保留范围 |
|---|---|
| `0190e8cd`／`9f1a817f` | [作者CPython普通Filter无模型报告](../imlane_filter_author_cpu_20261019/README.md)：7echo、两Q3 owner及四行mixed；两次依赖失败原件保留。该旧源结果不资格当前wire、关闭或有限链 |
| `7cd08b3a` | 68项CPU检查零skip，真实LOTUS／固定作者及模型替身／自写CPU child；旧C++和四份历史报告字节保持 |
| `174ee853` | 两项P1反例与63项NoPopen子集通过，Popen／child／作者／模型均0；仅测试内模拟NumPy SVE探针不支持和失败wire用例的Git元数据前检。三次guard碰到只读探针的失败原件保留 |

当前worker／host收到的唯一完整关联终态保持已知usage；矛盾身份仅留raw，缺项unknown，host停止补块并排空已接纳块后抛原错。
close receipts归原query，已有unknown不升级，不重发。固定174仍保留上游共享EngineProcess异常时整批unknown的边界；本节形成时不能据此声称完整链路故障资格。
完整72项最初因自动审批服务容量错误明确未执行；恢复后固定174完成72／72、零skip，15个已观察owned退出，源字节前后不变。原阻断记录不覆盖；后续common／Daft修复与最新组合在下节单列。
新wire作者CPU、真实模型／GPU、作者Ray、共享链CLI及性能仍pending。精确源码、失败与排除用例见 `verification.json.imlane_generic_chain_cpu_20261019`。

后续模型准备还修正owner的关闭范围：字面True API与成功kill请求只确认请求／API范围，OS与GPU仍待独立观察。
部分分配失败无model句柄仍unknown，静态未确认工作继续保留；最终91项CPU零skip通过，不将旧模型结果重新绑定新owner源码。

<a id="engine-result-receipts-cpu-20261019"></a>
## 共享私有引擎的故障回执与 CPU 复验

共享私有进程原先在条数／顺序／关联异常时把整批改为unknown，导致独立调用的已知usage丢失。
职责 `270a6fde`／`826ad217` 现保留唯一、完整关联的实际终态及部分usage；缺项、重复或矛盾项仍unknown。
`last_result_failure`及关闭报告保留实际IPC行的raw JSON、已核回执和未关联下标，写盘／清理失败不替换这些回执。
每项携保留字段 `engine_protocol_error`；其字段存在即失败，空值也不能继续提交或解析，自然顺序重复的非空原生request ID同样触发故障。
健康回复对象、同步批接口、15秒关闭预算与原生调度保持；故障后的API未确认、OS退出和GPU未知分别报告。

语义提交 `0b81ba67` 让普通Filter、共用链方法和级联先对可表示的已知成功响应更新原LOTUS统计，随后在parser／后继方法／校准前失败。
Daft职责 `cce7e0e3` 的close只回收同query且尚未交付的有效回执，复用现有账本校验；已收到的unknown不升级，已收到的prepare／infer元数据不被引擎原始行覆盖。
若iterator或EOF清理先失败，runner尚未进入解释，独立账本仍保留known usage，LMstats保持实际值；没有补跑parser或制造统计。

| 固定来源／运行 | CPU结果与实际范围 |
|---|---|
| `174ee853` 完整复验 | 72项零skip；原LOTUS／SemBench、明确作者／模型替身及实际私有CPU子进程；15个已观察owned退出。仅资格该固定baseline |
| `0b81ba67` 方法回归 | 87项零skip，含7个新增故障用例；真实固定LOTUS方法／统计，推理为合成响应，未启动Ray／Daft runtime |
| Daft最终 `cce7e0e3`＋共享修订 | 17项零skip：7纯合同、7实际Daft0.7.21／私有CPU传输和3旧回归；55个已观察owned退出。完整runner保留consumer的PermissionError，账本succeeded2／unknown1、已知10／4 tokens、总量null、query LMstats0；独立事后原interpret诊断累计10／4且parser0，不计入该query，也不发请求 |
| root组合 `84c71605` | 116项零skip，含common14、旧close9／IPC7、Daft7纯合同、IMLane完整72、语义7；生产supervisor确认35个已观察owned退出，remaining／cleanup errors／signals均空，输出完整且落盘前脱敏 |

这些运行分列，不相加为独立样本；模型／GPU请求及作者runtime启动均为0，GPU退出未实测。
root组合固定1024项code文件摘要，测试前后字节不变；精确源码和原件SHA见 `verification.json.engine_result_receipts_cpu_20261019`。
语义首次RED、两轴独审指出的两项边界、Daft首次把batch_size上界误当填满保证的fixture失败，以及中间通过原件全部保留。
Daft原四行关系筛选可合法形成2＋1批；修后仅测试使用三个eligible输入，观测一个三行批，生产没有强制攒批或更改buffering。
此前101项工具执行与最终12项pure按各自source边界记录，不替代上表最终监督资格。

main PR48的 `8dd884ca` 已正常同步，仅更新数据组织文档。新Core四调用只绑定冻结 `ebe3035f`／394文件，
与此次common修复分开；用户恢复strict SSH mux后，先完成只读资源／旧账本核验，再执行一次上传和一次启动，结果见下节。
已成功Daft单元未重放，旧32／16账本SHA不变；新wire作者、后续源码模型、级联helper与性能仍按各自资格推进。

<a id="core-model-qualification-20261019"></a>
## 冻结普通 Core 的双查询模型资格

[公开派生结果](core_model_qualification.json)绑定 `ebe3035fa1d105bdc14ceaf745aafbc613c1e6c6` 的394个常规文件，
通过Git archive逐字核同；服务器未建立或验证Git checkout。该闭包先于上节common修复及下节纯结果提取，不能认证当前分支全部源码。
既有单4090、Qwen2.5-7B-Instruct固定revision和vLLM0.25.1不变；LOTUS1.2.4／Ray2.56.1／Daft0.7.21的driver与模型Python prefix隔离。
准备／模型批及active上限均为1，generation为max8／temperature0／top_p1／seed0／换行stop，无logprobs或JSON模式；同步引擎批不是AsyncLLM持续供给。

| 核验项 | 实际结果 |
|---|---|
| 查询与实例 | 同一owner／生产Core／Ray模型actor／engine，两个独立session和query，各2条发生项；共4次实际调用、0额外应用warmup／自动重试／unknown |
| 原方法对照 | Movie Q3原15行前缀中的两个eligible负例，原source position为1、14；有效messages摘要、完整prompt／output token IDs、原文本／parser判定／finish／usage、COUNT、原SemBench metrics与LMstats核同；原生参照未新增模型调用 |
| 结果与usage | 两query各COUNT0、TN2、原评价误差0，各253 prompt／8 completion tokens；合计506／16／522。重复相同两负例不算四个独立质量样本 |
| Core生命周期 | 每query后held／input／result／requests／work及保留输入引用归零，第二query重新分配session与call IDs；逻辑额度不是RSS或显存硬上限 |
| API与独立退出 | owner仅确认Core transport／engine API及actor kill请求，owner的OS／GPU字段仍null；外层观察的40个owned出生身份及控制器，在归档前后均absent。独立GPU为1MiB、无compute，正常owned lease已释放；未观察到的后代不在认证范围 |
| 永久预算 | 新 `records-core-model-20261019` 4／4永久预留、单unit claim一次，实际4；旧32和Daft16文件SHA核同，Daft旧actual null未补写，成功单元未重跑，无退款／重置 |

配置文件原字节SHA为 `a45efda16f0a916b4fae9f641c34e74a040b1440894e3750c67957a5e60044ed`，
输入canonical为 `c0b456b211f83abe5c08ab0fe7b4186b0dd7d1348165e34c84ebbc2814571561`；
控制器加入输入身份后的runtime canonical为 `dc9718966e1ca58da27c88a62c358b89a6b859c8331d3b5a08891e03880b68c0`，三者分别保存。
比较的是上述有效值；新schema增加默认null字段，原native accessor报告也没有新增classification audit，因此不声称整份JSON字节相等。

私有归档969770字节／37个常规成员（36件索引原件及索引本身），SHA为
`470ca99cbbdb2214d308c8fbdb72e457c900a66ad77cc34e798695559e2eacbe`；全部成员与内层394源码摘要核同，未装入环境内容或原SQLite账本。
原件独审87项交叉检查、公开派生独审63项检查通过，均为来源核验项而非模型调用或测试样本数。
原派生脚本首次把lease字符串当对象的本地错误已保留，修后仅重新派生报告，没有重跑模型。
75.28秒是单次资格监督墙钟，不能作性能比较。有限链、helper级联、IMLane、图像及后续Async profile仍分别待验。

<a id="shared-result-conversion-cpu-20261019"></a>
## 同步与后续 Async 共用的纯结果转换

作者 `37e3b4fa` 在root以 `a4f47eab` 接纳，仅修改[vllm_engine.py](../../../../code/src/serving/backends/vllm_engine.py)
及新增[专属测试](../../../../code/tests/serving/test_vllm_result_conversion.py)。
`result_record`建立原终态信封，`result_from_output`转换已关联的单RequestOutput，保留校验错误前的token IDs／usage；同步 `run_prepared`共用，`_result`原签名薄转发。
abort的真实finish／已知tokens仍保留并按原规则失败；未来Async外层负责unknown及取消容量，不把本地abort结果当GPU完成确认。
采样、预算、batch关联、初始化、deadline和close保持，未新增重试、调度或Async生命周期。

7项新pure golden及原内核／原LOTUS独立解释器组合共69项CPU零skip；64项固定旧 `84c71605` 源码逐字段差分通过。
生产supervisor确认4个已观察owned退出、remaining／cleanup errors为空，模型／GPU／SSH动作0。
Standards／Spec只读独审均无发现，源码和6份原件SHA核同；精确摘要见 `verification.json.shared_result_conversion_cpu_20261019`。
此提取不继承前节冻结Core模型资格，独立Async组件及其新进程桥还须验证多在途、abort、迟到结果、容量和关闭。

<a id="records-four-layer-status-20261019"></a>
## 基本接入、实际库、真实模型和扩展按来源分层

| 执行 owner | 基本 records／本地模型接点 | 实际库验证 | 真实模型资格 | 扩展覆盖 |
|---|---|---|---|---|
| Ray Data | 原生批／actor／本地私有引擎→原 LOTUS parser／SemBench；独立于旧请求执行树 | Ray 2.56.1 实际 CPU 图 | 固定06f4的小范围普通Q3，两负例；本报告原件为准 | 有限链及显式 Extract 分段关系 CPU；新链模型与持续供给待验 |
| Daft Native | 原生 process 图／本地私有引擎→同方法解析评价 | Daft 0.7.21 实际 CPU 图 | 固定f526修正：两query／4调用；首轮失败actual null保持 | 共享有限链及显式 Extract 分段关系 CPU；新链模型待验 |
| SemLoom production Core | 真实 SessionEngine／BoundedAsyncBackend／lease-release；static参照身份独立 | 普通Filter实际Ray actor组合；新有限链为单worker／pipe，分别核验 | 固定ebe3035f：同engine两query／4调用；不转授后续源码 | 显式Core Extract屏障 CPU通过；原同步批不等于Async持续供给 |
| IMLane | 原作者CPython carrier／v2记录桥／独立方法和模型解释器 | 0190／9f1固定CPU作者资格保留；新wire不整体继承 | 冻结8be首次初始化ZMQError，第二query未启动；新4永久claim，actual null，[原unknown与独立结清](#imlane-model-initialization-attempt-20261019)分列，尚未取得模型资格 | 通用Filter及有限链fixture分列；作者Ray和完整模型链待验 |

固定来源的普通 Filter 资格与新算子覆盖是不同验收项。当前支持的 Filter／Map／Extract
继续复用原 LOTUS 提示、生成配置、parser 和 LM 统计；COUNT／SemBench解释仍由任务拥有。
这张表不声明 Join／Agg／全局cascade／图像或整套SQL已完成，也不将 static 参照称为新策略。
e4c 修正资格属于独立 static 参照，不归入 Ray Data 或 production Core 的模型来源。

[显式 Core Extract 屏障](../lotus_extract_relation_20261019/README.md#后续共享合同与显式-core-接线)
完成共享父调用／nullable／schema和同模型寿命的CPU接线；
后续[Ray／Daft 显式分段布局](../lotus_extract_relation_20261019/README.md#raydaft-显式原生关系布局)
已取得新 CPU 图资格，每非空段独立引擎，成本与每实例上限单列；默认非末端 guard 保持。
[独立 Async 组件](../local_async_engine_20261019/README.md#root-集成与关闭异常修复)
已完成组件CPU与关闭故障修复，但 actor／Core／CLI 尚未接入，因此当前完整查询没有持续供给资格。

IMLane冻结8be的两owner资格使用126个Git同字节文件、原C++ binary与Arrow16方法环境，
与此前CPU来源分开。第一次实际运行在首query模型初始化返回ZMQError，外层观察22个owned身份退出，
作者namespace/SHM与unit GPU报告已清理；API handle和最终counter不可得，故actual保持null、
4次预算不退、原controller报告保留host lease，不能据eligible行数或ready错误补造0。
其后独立物理结清见下节；原自动报告不改写，不把当前单元称作模型通过。

<a id="imlane-model-initialization-attempt-20261019"></a>
## IMLane 首模型尝试与独立物理结清

IMLane CPython 的本次普通 Movie Q3 Filter 实际模型尝试保留为 `unknown`。冻结来源为 `8be539cf` 的126文件源包；计划两个独立查询 owner、每个两条 eligible 记录，实际只尝试首个查询。首查询一条 failed、一条 unknown，均未确认提交；第二查询未启动。实际模型调用数、最终 API counter 和总 usage 均未知。新永久预算一次 claim 4/4，无模型重试、暖身、退款；本轮未完成原 LOTUS parser/COUNT/SemBench evaluator 与原 native control 的结果差分。

归档前后及单独人工结清前的独立检查确认本次23个、前驱135个已观测 PID 出生身份均为 literal absent，指定 namespace SHM 为0，GPU使用1MiB且 compute 列表为空，foreign 工作门通过。归档时保留原 owned lease；随后单独的 owned-only 人工结清已确认释放。该物理结清不改写原 controller/unit 的 unknown，不确认模型 API shutdown，不证明未观测后代退出，也不退款或补造调用计数。

原初始化诊断仅记录 `ZMQError` 类型，没有原错误 message/errno。独立 CPU 路径试验得到116字节路径失败（errno36）、81字节路径 bind 成功，支持路径长度解释；不能据此将 errno36 回填为历史模型错误。本轮只适用于上述冻结来源，作者 Ray、实际模型完成及后续 Extract/Async profile 仍分别待验，无性能结论。

[公开摘要](imlane_model_initialization_attempt.json) 保留三种配置摘要、归档/索引/结清收据 SHA 与原件/脱敏导出双 SHA；公开摘要不包含连接、具体远端目录、环境内容、凭据、原始数据库或权重。
