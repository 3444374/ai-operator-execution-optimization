# MMQA Q2b 最小接线与 CPU 核验

本项在独立分支补充官方 Q2b 的输入、原生查询、评价和阶段观察，不修改 SemLoom 控制器。
用途是为不同于 Animals Q7 的 Join→Extract 依赖链准备一次有限观察；没有方法性能结论。
输入、真实模型及正式观察的实际状态以本页证据为准。

<a id="join-only-diagnostic"></a>
## 固定 24 次的 Join 提示诊断入口

[独立 CLI](../../../../code/scripts/experiments/run_mmqa_join_diagnostic.py)从现有父数据中选择两张原图，
覆盖全部六种不同 Track，两种提示各 12 次，共用新的总 24 次预算；不是原 Q2b 子集成绩。
它对每个 pair 调用固定 SDK 的原生单 pair `sem_join`，保留 formatter、parser 和原默认布尔行为，
分别记录旧字符串提示与明确的 CoT 枚举提示。两臂参数及 user 内容相同，先后顺序在相邻 pair 间交替。
完整 Q2b 的策略参数解释由[适配模块](../../../../code/src/baselines/image/sembench_mmqa_compat.py)拥有，
诊断不调用 `_execute_q2b`、不放行 Extract，也不把父数据的完整额度改成 24。

选择文件绑定父 manifest、原源码、图片、全部六种 Track 及其来源行；执行校验准确源码与新预算。
错误、未知、集合外请求及记录错误都停止后续调用，已扣计不退还，预热与重试为 0。
HTTP 失败响应副本用既有 redactor 清洗后落盘，原摘要分列；网关退出或清理失败时整体记录为失败。
真实模型和服务身份仍由实际运行会话先核对，worker 监督与输出、预算及租约按原有组件处理。

[用法与全部参数](../../../../code/scripts/README.md#mmqa-join-diagnostic)及
[CPU 用例](../../../../code/tests/experiments/test_mmqa_join_diagnostic.py)随代码发布；
新增 CPU 日志、请求及监督原件保留仓外，维护者可按 `semloom.mmqa_join_diagnostic.cpu.20261104` 提供摘要匹配的记录。
这不发布新的证据归档，不作质量或性能判断；新策略的真实功能观察仍待协调。

SemBench 固定为 `c814e3807e72d4cf876b852b17e77f3cc94575c2`，
调用[官方 `_execute_q2b`](https://github.com/SemBench/SemBench/blob/c814e3807e72d4cf876b852b17e77f3cc94575c2/src/scenario/mmqa/runner/lotus_runner/lotus_runner.py)。
上游声明 LOTUS 1.1.3，本入口选择已有可核源码的 LOTUS 1.2.4、LiteLLM 1.95.0 与 OpenAI 2.50.0，
不称完整上游环境复现。工程依据沿用[架构方案的公开接入参照](../../../plans/系统架构.md#pgml-engineering-reference)：
复用已有调用、预算和清理能力，各算子保留自己的提示、解析与关系处理；未读取或迁移公司材料。

## 保留的任务与资源职责

- 原始表 `ap_warrior.csv` 全部列和行进入原方法，图片使用原 `ImageArray`，保留目录枚举顺序。
  原生成器保留八张固定图片，再用原目录列表和 seed 42 选择额外图片；入口支持原 scale factor 25～64。
- 明确使用 exact policy。原 `_configure_lotus_for_join_type("image")` 在 exact 下不设置检索模型，
  不借用其他查询的 RM、索引或近似算法，也不调用带隐含准备工作的 runner 构造器。
- 原 Join 以 `zs-cot` 评价完整图片×表行候选；完整 Join 结果形成后，原 `sem_extract` 才接收匹配行。
  Extract 的 `response_format={"type":"json_object"}`、原提示、解析和最终 `ID/image_id/color` 三列保持。
- 对 I 张图片、T 个表行，Join 必需 I×T 次调用；Extract 最多 I×T 次，完整保守额度为 2×I×T。
  同一图片匹配多个表行时仍分别 Extract，不按图片去重，不因实测匹配少而缩减预留量。
- 调度归原生 LOTUS LM 与模型服务。既有共享 SQLite 预算在每次 HTTP 发送前扣计，失败不退还、重试为零，
  原生 LM 决定并发。进程监督负责总时长、取消时关闭预算及本地子进程回收，远端计算结束仍需独立核对。

入口为 [run_sembench_mmqa.py](../../../../code/scripts/experiments/run_sembench_mmqa.py)，
数据与原评价适配在[sembench_mmqa.py](../../../../code/src/baselines/image/sembench_mmqa.py)，
阶段观察在[mmqa_q2b_observation.py](../../../../code/src/experiments/mmqa_q2b_observation.py)。
原评价器读取未修改的 `q2b.json` 五个 gold 三元组，颜色属于质量评分，标签不进入模型输入。

## 验证与证据

[CPU 用例](../../../../code/tests/experiments/test_sembench_mmqa.py)使用真实 LOTUS、原查询和原评价器，
模型响应由 localhost fixture 提供，GPU 不可见，模型服务与 Ray 均未启动。作者目标 driver 的原运行记录为 51 项通过，其中 15 项是 Q2b 用例，另 36 项覆盖复用的预算与监督组件；这不是其他环境的通过声明。
选择性 fixture 为 3 图×2 行：6 次 Join、3 次 Extract；全匹配为 6＋6 次，达到完整保守额度。
检查完整颜色评价、相同内容的多个发生项、HTTP 错误不重试、输入与枚举变化拒绝、观察器恢复、字节上限、
必需记录器报错和监督超时后关闭预算。上述 CPU 核验的真实模型请求为 0；首次 GPU 失败及辅助观测器修正见后文。

原实现遇到空 Join 时，Extract 没有新增 `color` 列，最后投影抛 `KeyError`。
本入口保存该失败并停止，没有伪造空结果或修改上游任务。模型服务返回内容错误或 JSON 解析失败时亦保留原行为。
进程内 CPU 检查观察到 SDK socket 的 `ResourceWarning`；实际执行入口每条查询使用独立 worker，
进程退出证据与客户端复用的长期资源性质分别判断。

机器报告、每轮失败与成功日志、源码摘要及输入获取记录保留原件；紧凑核验记录见
[validation.json](validation.json)，原件与公开副本的身份及获取方式见下文。早期失败包括 Extract 类名、namespace 包来源检查和关闭后读取 proxy 状态，
修复后重跑对应项；一次组合命令选错测试模块名的失败同样保留。

精确提交 `d02b4fe0a154dc54f87a4be240250b1f9f1708bb` 另通过完整 CLI 核验：
真实官方数据的 `--preflight` 为零调用，固定响应执行预留 12 次、实际 9 次 HTTP，成功 9、失败或未知 0；
本地 worker 退出码为 0、残留子进程为空、预算已关闭、lease 已归还。该检查没有真实模型或 GPU 调用。
其中每条原生查询使用独立 worker，完整 worker 启动到退出由 supervisor 的起止时刻覆盖；
worker 内应用时间与原查询时间另列，不把准备或评价从完整过程隐去，也不把 fixture 时间当成模型性能。

## 独审修正与环境分列

独审确认原 summary 已脱敏，但原 CLI 把同一异常再次交给 Python 默认 traceback，导致它进入 `worker.log`。
提交 `f4f723fc01f5ea6f1477e507bcfc3728d49a6c48` 在新增 CLI 最终异常输出处复用既有 redactor，
保留异常类型、原因链、notes、非零退出以及整数退出码；KeyboardInterrupt 仍为 130。
未修改原查询、gold、预算或共享 supervisor。合成探针实际经新 CLI 与原 supervisor 落盘，
保留 `ValueError`、退出码 1 和 `password=***`，合成敏感值没有进入日志，残留子进程为空。

测试结果按环境分别解释：

| 环境与来源 | 完成情况 | 未完成部分 |
|---|---|---|
| 作者原目标 driver，原记录 | 51 项通过，原件引用摘要已在本轮取回核对 | 不替代独审环境运行 |
| 独审本地环境，原报告 | 44 项字面通过，其中 43 项完整触及预期行为 | 8 项预期行为未完成；缺 overrides/cdlib，另有 localhost/进程权限造成的早期错误 |
| 本次作者目标 driver | 已声明依赖检查通过；55 项通过，包含新增 4 项 CLI 检查；真实模型请求 0 | 修正后的独立复审和 GPU 资格仍 pending |
| 本次作者本地解释器 | 3 项无 HTTP 的 CLI 子进程检查通过 | 未在该解释器运行完整 native 查询套件 |

目标 driver 已有 overrides 7.7.0、cdlib 0.4.1、Pillow 12.2.0；独审本地 Pillow 为 13.0.0，
且缺前两项依赖，不能称同环境。没有为独审环境混装包，也不把两轮数量相加或把导入异常算作 HTTP 行为通过。
原 observer 恢复用例现要求实际发出 HTTP，并核对失败尝试计数，避免缺依赖时仅因抛异常而字面通过。

修正提交另完成完整 CLI 固定响应复验：12 次预留、9 次实际 HTTP、9 次成功、失败或未知 0，
worker 退出码 0、预算关闭、lease 归还、残留子进程为空。官方输入 preflight 仍为零调用。
这只是作者目标环境的 CPU 功能证据；未替代独立复审，也没有真实模型或 GPU 运行。

<a id="evidence-retrieval"></a>
## 原件、副本与获取

稳定 artifact 身份为 `semloom.mmqa_q2b.cpu_evidence.20261104`。
[映射清单](evidence-map.json)逐项记录运行名、原件相对路径、原摘要、公开归档成员及脱敏后摘要；
[公开归档](raw/cpu-evidence-20261104.tar.gz)保存 63 份可直接取得的脱敏副本，包含旧失败、CPU 日志、
完整 CLI summary/footer/supervisor、新脱敏探针及目标环境版本。它不是原件逐字节副本。
原件 66 份均已取回本地并核对；事务数据库与含机器路径的可运行输入 manifest 继续只保留原件。

从本提交取 Git 文件后，不需要服务器账号即可核验公开副本和 validation 的 16 个来源引用：

```sh
python3 experiments/results/diagnostics/mmqa_q2b_wiring_20261104/verify_evidence.py
```

如需对照旧 validation 的原摘要，向项目维护者提供上述 artifact 身份及 `original_relative_path`。
维护者通过仓外交接清单的存储登记取得对应原件或原归档，交付前核对 `original_identity`；
接收方按清单相对路径放置后运行：

```sh
python3 experiments/results/diagnostics/mmqa_q2b_wiring_20261104/verify_evidence.py \
  --original-root "$ORIGINAL_EVIDENCE_ROOT"
```

本轮该检查核对了 63 份公开副本、66 份原件和 16 个来源引用。
副本使用既有 redactor，并以占位符替换机器路径；原摘要与副本摘要分列，不能互换。
独审环境的记录以明确投影单列，原审查报告的摘要与私有获取映射同样保留；没有据此声称该环境已完成复验。

## 原数据核对

已从官方来源取得并保留原 ZIP，字节、摘要与实际图片后缀见[数据说明](../../../../data/README.md#mmqa-source)。
原压缩包包含 1000 张图片，其中原程序可选的小写 PNG/JPG 为 925 张；完整表为 13 行。
服务器按原生成器产生 scale factor 25，25 张图片及原表逐字节／像素核对通过，manifest 摘要为
`b9600fef2b884b7729292bac91dba3c79d4312d92b6d693587831e2e97475511`。
本次完整调用上限为 325 次 Join＋最多 325 次 Extract＝650；数据准备和 preflight 的模型调用均为 0。
该 manifest 同时绑定原源码、目录枚举、图片内容和表摘要；移动路径或重生成后须重新核对，不能复制旧摘要作为新输入身份。

<a id="service-observer-capacity"></a>
## 服务辅助观测器容量修正

首次真实模型观察使用 `130aa647642dddc367dee356a328db1d43face78`。
查询预留 650 次、实际扣计 325 次：252 次 HTTP 200，72 次 HTTP 500，1 次没有完整响应。
Join 未完成，Extract 为 0，没有完整查询结果或质量分数；这次运行不支持完整链功能或性能结论。
只读复核原件清单的 37 项全部摘要匹配；旧 summary、轨迹、响应和预算数据库均保留。

原因来自服务启动时借用的辅助 `processor_receipt`，原源码摘要为
`3b6468274bebe2ce5b8e9605c9ed3fd0d14a6c764eda3082a53e1c81148174e3`。
记录器共 256 个不同配置名额，实际为 3 条 processor 身份、1 条已加载模型配置和 252 条逐请求 UUID。
后续 72 个完整 HTTP 错误的消息均为 `runtime observer distinct-profile cap reached`。
另一条 gateway 记录为 `ServerDisconnectedError`，没有完整上游响应，继续按未知请求计费；
不能凭同批其他错误推定它没有执行。全部 325 次尝试和 73 个失败或未知数不退还、不重写，
旧预算已关闭，未用的 325 次不能自动用于重试。
失败阶段的 `model_calls=0` 来自只在阶段成功结束时写入的计数；它不能替代查询 summary、
共享账本和 HTTP 轨迹中的 325 次实际尝试。324 份完整响应与 1 条未知分别保存。

独立修正新增[版本化模块](../../../../code/scripts/profiling/record_observation/processor_receipt.py)，
只保留 processor 工厂与已加载模型配置观察，不再导入或改写原生 UUID 处理函数；原有容量数值不变。
身份字段、去重、写入格式及首次 POST 前核对实际回执的要求保持。
原 Q2b 查询、模型参数、请求／响应轨迹、阶段记录和生产调度源码没有改动。
服务启动改用[版本化模块路径](../../../../code/scripts/profiling/record_observation/README.md#service-processor-model)，
不能仅把新文件放入 checkout 后仍加载旧临时路径。

准确源码提交为 `0c6759ab6a84ae8815e6d77291989ad0dc136a43`，原三个身份记录函数的抽象语法树
与保留的旧源码一致；只有安装的观察项减少。作者目标 driver 的预检通过，依赖版本、
七个测试模块及完整 50 个用例清单见[本轮核验](observer-validation.json)。
50 项通过、0 失败、0 跳过；它与前文旧 55 项使用不同的选择清单，不能相加或声称全仓库覆盖。

| CPU 检查 | 结果 |
|---|---|
| 精确旧模块，651 个不同 UUID | 252 次返回后，399 次因记录容量抛错；记录达到 256 条 |
| 修正模块，相同 651 次调用 | 全部返回原对象，固定身份保持 4 条；原生异常仍原样抛出 |
| 原生 Q2b，25 图×13 行合成输入，全匹配 | 325 次 Join＋325 次 JSON Extract；完整请求、响应、HTTP 轨迹各 650 条，原评价器实际调用 |
| 325 次 HTTP 故障注入 | 252 成功、72 错误、1 次断连；全部扣计，无重试，关闭后不能再发送，fixture socket 回收 |
| 相同准确提交，独立 worker 的完整 CLI | 预留及实际均 650；4 条身份记录，失败或未知 0，worker 退出 0、残留子进程为空、预算关闭、lease 归还 |

首次 6 项检查中的故障用例没有收集预期断连，5 项通过、1 项报错；其中完整 650 次原生查询已经通过。
修正测试客户端的收集和 `finally` 收尾后，重跑上述 50 项通过。首次 CLI fixture 因错误序列化
模型配置的字段，在 worker 和 HTTP 前被拒绝；修正私有 fixture 配置后另建运行目录完成核验。
两次失败日志均保存。进程内 SDK 的 socket `ResourceWarning` 与既有记录一致；
独立 CLI 的子进程退出和回收另有证据，不据此声称 SDK 长期复用的资源性质已经验证。

本轮 artifact 身份为 `semloom.mmqa_q2b.observer_cpu.20261104`，
[公开归档](raw/observer-cpu-evidence-20261104.tar.gz)含 32 份脱敏副本，包括保留的精确旧模块、
首次失败、50 项回归、完整 CLI 轨迹及原 GPU 失败的只读统计投影；没有下载或公开旧 GPU 的请求正文与图片。
[逐项映射](observer-evidence-map.json)登记 33 份原件及各副本摘要，事务数据库仅保留原件。
私有原件已取回逐项核验；获取方式仍为向维护者提供 artifact 身份和 `original_relative_path`。
默认核验命令继续检查此前 63 份副本，新一轮使用：

```sh
python3 experiments/results/diagnostics/mmqa_q2b_wiring_20261104/verify_evidence.py \
  --map observer-evidence-map.json --validation observer-validation.json
```

取得私有原件后可追加 `--original-root "$OBSERVER_ORIGINAL_ROOT"`，核对全部 33 份原件。
如需重现旧容量问题，将公开归档解到仓库外目录，设置
`SEMLOOM_TEST_PROCESSOR_RECEIPT_BEFORE` 指向其中 `processor_receipt_before.py`；测试会先核对旧摘要。
`SEMLOOM_TEST_SEMBENCH_ROOT` 指向前文固定的上游 checkout，在已预检 driver 中运行
`PYTHONPATH=code python -B -m unittest tests.experiments.test_processor_receipt tests.experiments.test_mmqa_service_observer -v`。
新修正尚待独立复审，没有启动 vLLM、模型或 GPU；真实 processor 身份、JSON Extract 和 GPU 完整链复验仍 pending。

## 完整 Q2b 的后续观察要求

下述条款适用于需要完整 Q2b 的运行，不替代上面的固定 24 次提示诊断。

由 GPU 会话重新核对服务器空闲、独立源码版本、原始数据 manifest、模型与 processor revision、tokenizer 摘要、
服务参数和资源上限后运行。下次仍只做一个 native Q2b，不附加优化臂，不隐藏 warm-up，不自动重试或调整参数。
本入口要求显式并发、输出 token、单请求字节、每阶段准备消息字节、结果字节与总时长；实际 GPU 配置由该次计划记录。

观察包括原生图片准备、Join 候选与实际消息、HTTP 提交及完整响应、Join 结果形成时刻、Extract 消费和完整查询时间。
完整查询、准备、评价及应用收尾分别记录。HTTP 与阶段时间使用同一 worker 的 monotonic clock；
媒体准备是调用墙钟时间，不能当作 CPU 核占用。逻辑结果返回不证明物理内存释放；GPU、HBM、队列和实际传输另采或记 unavailable。
必须检查事件、请求、响应、summary 与 footer 的摘要，并保留未知尝试和失败。
先复审辅助观测器修正和完整预算的 CPU 证据，再由 GPU 会话协调新运行；本次工程修正不直接重跑。
只有两个原语义算子实际执行并通过原评价与资源核对后，才报告该路径的真实模型功能观察；不能外推完整系统或优化收益。
