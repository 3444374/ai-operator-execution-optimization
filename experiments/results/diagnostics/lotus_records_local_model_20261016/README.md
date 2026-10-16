# Movie Q3 records 到本地模型接点：非模型资格

本切片在已审阅 main `52391e375b6d9699e5bd76bb6d37bdbd939ef23e` 上独立实现，
集成时同步已审纯文档 main `d73c44463e9fdb98e2ac1397295114568afabb1c`，由 `integration/data-execution` 集成。
最终 Daft／Ray Data／IMLane CPython 与作者 Ray 对照均保留为计划要求；本切片只支持普通 Movie Q3 的 Filter＋COUNT，
不是级联、图像、生产 Core 或性能结果。旧 request 接入、活动 baseline 和旧测试树没有修改。

## 实现与来源

| 层次 | 本轮文件与采用行为 |
|---|---|
| 输入与逐调用记录 | [movie_records.py](../../../../code/src/baselines/text/movie_records.py)、[records.py](../../../../code/src/semantic_methods/lotus/records.py)：原 CSV、任务、谓词、schema、标签分离投影及摘要绑定；按 occurrence 保留 query／role／stage／call；失败与 unknown 的账本独立于最终 COUNT |
| 原方法 | [engine.py](../../../../code/src/semantic_methods/lotus/engine.py)：固定 LOTUS 1.2.4 的 dtype-aware 准备、formatter、parser、原 LM physical／virtual usage 与有限额度；不修改上游 accessor |
| 模型内核 | [vllm_engine.py](../../../../code/src/serving/backends/vllm_engine.py)：不同 Python prefix 的 driver／模型环境；worker 内常驻同步 `LLM.chat`；一次 chat template、真实 token IDs／usage／finish reason，TP1／PP1／uni executor |
| 原生 Ray Data | [ray_data_lotus_records.py](../../../../code/src/baselines/text/frameworks/ray_data_lotus_records.py)：原生 CSV→filter→projection→worker 准备→独立单 GPU 模型 stage→紧凑结果；原生图拥有物理分批与 actor 池 |
| SemLoom 静态参照 | [lotus_records_static.py](../../../../code/src/execution_provider/adapters/lotus_records_static.py)：Daft 惰性读取与普通关系操作，Ray Core 准备／模型 actor 直接交接 ObjectRef；明确 `production_core_used=false`，属于 `diagnostic_reference`，尚无新策略结论 |
| 查询、评价与退出 | [lotus_records_movie.py](../../../../code/src/experiments/lotus_records_movie.py)、[supervisor](../../../../code/src/experiments/lotus_records_supervisor.py)：原 SemBench evaluator；成功才形成完整查询结果；关闭失败不覆盖前缀 usage／原错误；有限总期限与已观测 owned 进程回收 |

源码依据为已有受信 LOTUS/SemBench 适配及以下固定公开实现：

- LOTUS `b1a85fd7a66fabed8a1585d44d7597d592b4433f` 的 `sem_filter.py`、task instructions、postprocessor、LM stats／limits 和 cache/settings；实际安装版本与逐文件 SHA 均核对。
- SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2` 的 Movie Q3 与原 `AccuracyEvaluator`；只读干净 checkout。
- [Ray 2.56.1 Dataset](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/dataset.py) 的 map／map_batches／ActorPoolStrategy，以及 `runtime_env.py_executable`；不是官方 vLLM Processor 的资格。
- [vLLM 0.25.1 LLM](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/llm.py) 的同步 chat／sampling／shutdown 接口；本轮仅源码与替身合同，未运行实际引擎。
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
- 实际 Ray CPU 图验证了 driver 解释器 worker 的模块可见性；模型 stage 是明确的 CPU 固定响应替身。Daft typed CSV 确认空串、`NULL/null/None/NaN` 与换行文本保持；生产 static actor 图仍仅有合同替身测试。

此外覆盖源摘要篡改、重签投影、调用身份错误、未知 usage、失败前缀、quota、取消／期限、close 失败与重试、leader 先退出及 TERM→KILL。
环境在仓库外单独创建，按 runtime 规则保存安装前后 check；driver CPU 依赖检查为 OK，未安装 vLLM、下载权重或构造模型。

复现使用固定原 SemBench checkout、上述版本的独立 Python 3.12 环境，设置 `PYTHONPATH=code`、
`SEMBENCH_CHECKOUT` 和离线环境。九个新增测试文件从本报告对应源码旁的 `code/tests/` 运行；
真实 CPU 库检查另设置 `RUN_LOTUS_RECORDS_CPU_LIBRARIES=1`，入口为
[test_lotus_records_libraries.py](../../../../code/tests/experiments/test_lotus_records_libraries.py)。
完整测试以 `lotus_records_supervisor.supervise()` 包裹，180 秒总期限／10 秒清理保留。
实际 CLI 与外置配置见[脚本说明](../../../../code/scripts/README.md#movie-q3-records-本地模型接点)。

## API 交接与剩余资格

共享方法使用 `FilterMethod`／`GenerationSpec`，records 必须携带 `identity()` 的 query、model role、stage、row occurrence、source position、call index／ID。
worker 本地 `LotusFilterMethod.prepare_rows()` 产出未套模型模板的 `messages_json`；模型 owner 返回紧凑 terminal rows，
`run_movie_query(..., execute, query_id=...)` 在分配前检查、按 call 记账，最后由原 LOTUS 解释并评价。
IMLane 新桥可复用这些值合同和方法核对，必须由作者 DBEnd／session 拥有调度；没有将其纳入本轮资格。

以下仍为 pending：实际 vLLM 0.25.1／模型解释器的 Ray 模块与 Arrow 可见性、模型／tokenizer／template 内容身份、
真实 token／输出与原 LOTUS 的差分、单 GPU 资源和物理退出、static 的真实 actor 图与连续查询隔离、
首发早于末准备及峰值留存观察、原全局 helper-LM 级联、AsyncLLM 持续供给、图像、Daft 原生模型 UDF、
IMLane 两种原生宿主和生产 Core 接入。单次同步 `LLM.chat` 跨批排空，不能称为跨批持续供给。

source 目前限制 4,096 行、CSV 16 MiB、单 review UTF-8 1 MiB；这些是有限输入资格，不是 RSS／GPU 显存硬上限。
LM result cache 关闭，非默认 serialization、rate／TPM、logprobs、repetition override 等未支持配置明确拒绝。
进程监督仅确认已观测且身份匹配的 owned 进程；未观测 detached 后代和 GPU 物理退出始终保留 unknown，
`ray.kill`、API shutdown 或 iterator close 均不作为 GPU 退出证明。真实运行须由父线程协调既有服务器及资源 owner，未新增租用或接触学校服务器。

## 真实资格准备

在已发布 `957e91eb` 后准备既有服务器的单 4090／Qwen2.5／vLLM0.25.1 资格，应用模型请求总上限32、零自动重试，不下载权重或修改共享环境。
源码 `06f4b687` 加入可选 `max_model_calls` 单引擎上限及真实 prompt token IDs／规范 messages SHA；隔离 Python3.12.6 下52 项受影响 CPU 测试和74 subtests通过、零skip。
对应命令与环境记录在 `verification.json` 的 `qualification_cap_cpu.run_3`，原件为私有 `nonmodel-evidence/model-qualification-cpu-r3.{json,log}`，字节数及 SHA256 见 `raw_evidence`。
先前系统 Python 的37通过／1依赖skip及另次52通过的简略日志分别登记为 run_1/run_2；run_2 的pytest通过后，外层zsh因使用只读变量 `status` 退出1，现由完整记录的 run_3 补齐调用来源。
原 114 项资格仍绑定其记录源码，不转授这些后续改动。真实运行的精确 source、模型／模板、unit、版本和结果另按实际记录，本段不表示模型已运行。
