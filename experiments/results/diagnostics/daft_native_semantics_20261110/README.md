# Daft 原生 AI 正式接线与完整 sink 时钟

本轮修复两个实际缺口：维护树的原生 AI worker 原来只暴露 Movie Q3；历史 MMQA driver
虽然使用原生 Daft，却先等待模型 worker 关闭，再运行原查询后处理，关闭等待因而进入查询时间。
现复用历史 Medical Q10/MMQA 的 prepared prompt 接点、模型 owner 和现有监督器，
把完整单 F/M/E 查询接线放回[正式入口](../../../../code/src/baselines/README.md#daft-native-prepared-sembench)。
原 Movie Q3、class/batch UDF 对照及原研究证据保持各自身份，未改 SemLoom 生产调度或作者算法。

## 实现与计时

- 模型 worker 继续调用 Daft `prompt(vllm-prefix-caching)` 和原生 `LocalVLLMExecutor`，
  driver 只准备完整请求并使用原 LOTUS parser、原 SemBench 查询和 evaluator。
- `call_id` 标识源发生项，返回乱序时按身份还原；重复、缺失或无关 ID 拒绝。
  JSON null 与原 Filter True-before-False/default=True 行为不修订。
- raw-native-ready 触发 driver 完整 sink 回调，监督器的关闭等待随后继续。
  完整 sink timestamp 先于序列化、质量评价和持久化；实际关闭可与 driver 后处理重叠，另列事件。
- `source_start_ns → sink_ready_ns` 从此 driver 函数入口计时，包括 fresh source、prepare、
  模型启动、完整原生算子和原 sink，以及真实轮询/IPC。严格 host controller 导入/预检/租约入口
  未观测，保持未知；不追认历史 cold 值，也不将首末 API 时间叫 GPU kernel 时间。
- 每次 native generate 前复用已认领 SQLite 预算，失败不退还；超额输入在启动前拒绝。
  没有额外预热或整轮重试，单冷执行不声称同 owner warm。

原生依赖版本保持，按共同硬件/计时口径资格比较；共同协议仍以
[原生基线校准](../../../plans/原生基线校准.md)及协调分支 `acae5b2a` 为准。
本报告是该实现的真实回执，不另立比较协议。

## CPU 实际结果

[机器可读回执](CPU.json)记录最终源码摘要和原件摘要。
Mac arm64 的真实 Daft0.7.21、LOTUS1.2.4、transformers4.57.6、pandas2.3.3：

| 检查 | 结果与边界 |
|---|---|
| 原入口/新接线/预算/关联/期限 | 22项通过，0失败、0跳过 |
| 真实软件子进程的 owner 关闭与失败清理 | 5项通过；不代表 GPU/SHM 清理 |
| MMQA Q3a | 25条保存答复，完整消息/token、源关联、原 accessor 全关系、sink/evaluator 全等 |
| MMQA Q4 | 24条保存答复，索引19缺席，完整消息/token、Extract 全关系、genres原分组/sink/evaluator 全等 |
| 额度 | 各完整 CPU 回放恰好25/24次软件 native generate，第26/25次预算拒绝；直接 generate 委托前拦截另测 |
| 时钟顺序 | 完整原 sink 在模型 fixture close 之前，先于持久化/评价；严格 controller/GPU 时钟未知 |

最初新增 Extract 测试错误地期待 JSON null 得到 Python `None`；实际原 LOTUS 输出字符串
`"None"`。保留这个失败结论，修正测试预期，未改变 parser。Q3a/Q4历史冻结参考与旧交付包均未修改。

全查询 CPU 回放使用真实 SDK 原生执行器和 CPU collect，模型答复为按源身份绑定的软件替身。
其中监督回调在同一进程触发；实际 OS owner 关闭由上述5项独立软件进程测试验证。
新真实模型调用为0，不将保存答复的 F1 当本次 Daft 模型质量。

```bash
PYTHONPATH=code "$CPU_PYTHON" -m unittest \
  tests.baselines.text.test_daft_sembench_native \
  tests.baselines.text.test_daft_prepared_native \
  tests.baselines.text.test_sembench_prepared \
  tests.experiments.test_native_daft_query \
  tests.baselines.text.test_daft_native_shutdown -v

PYTHONHASHSEED=3 LITELLM_LOCAL_MODEL_COST_MAP=True HF_HUB_OFFLINE=1 \
  TRANSFORMERS_OFFLINE=1 PYTHONPATH=code "$CPU_PYTHON" \
  -m tests.baselines.text.replay_daft_sembench \
  --config "$CPU_CONFIG" --reference "$FROZEN_REFERENCE" --output-dir "$NEW_PRIVATE_OUTPUT"
```

CPU Python 需要官方 Daft SDK 和 driver 依赖，不安装或调用 vLLM/模型；此回放的 `model`
配置只指向已冻结 tokenizer。`PYTHONHASHSEED=3` 固定原 LOTUS `parse_cols(set(...))` 的列顺序，
用于已冻结的 MMQA 提示身份，不改作者实现。

## 目标真实资格与最新 main 整合

原始运行 `daft-mmqa-ffbc-native-qualification-20261110-r1` 使用
`ffbc352ac5c31a2d5b89b2bdf8429d5003a329f5`，code tree 为
`41bfb1f0a2df2833934f412261d7e757e2a69fb2`：Q3a25与Q4 24分别冷启动，共49终态、0未知、0重试，
两题完整 sink 均先于模型 close 完成、评价和持久化，资源、socket及租约完成清理，72原件散列复核通过。
原评价 F1 分别为0.916667、0.486486。Q4相对旧参考的一处语义差异和一处空白差异保留；
实际 GPU 完整 token 数组未独立记录，已核实的是消息/native prompt hash、CPU完整token及原生token数量。
原始 `HANDOFF.json` 的 SHA-256 为
`137903ba7d9c000dab2d803500d7983f1a9c025717029eb82265220a1640497a`，可按运行标识向项目证据保管者取得。
这只授予两题的原生正确性和退出资格，不是正式性能对比或同 owner 暖轮。

正式整合以 main `6b0f4f6efcfe61225ffc0c7ad5a3f4af261dc1bf` 为基底，只迁入 ffbc 的 Daft 自有差分。
六个执行/语义源码文件与 ffbc 逐字相同，保留 main 删除 `lotus/sdk.py`、`SDK_FILES` 和后续文档清理。
[整合回执](INTEGRATION.json)记录22项旧 Q3/新 prepared/关联/预算/期限回归全部通过，
以及Q3a25、Q4 24按消息身份重放的完整消息/token、原 accessor 关系、sink和评价全等。
新模型调用为0，未重复49次真实模型或目标 SDK 资格；既有5项软件进程关闭测试作为未变职责的原证据引用。
旧真实记录仍绑定 ffbc，不能改称本次 main 候选的 GPU 结果。

后续独立CPU子项 `baseline-main-integration/daft-q1-cpu` 固定 `9256287f8` 的源码快照，
真实Daft0.7.21 SDK/executor、LOTUS1.2.4和离线tokenizer完成Q1的25条保存答复重放：完整消息/token、
原left join/Role/director关系及sink/evaluator一致，第26次预算拒绝，sink先于close/审计。
模型/provider和监督器为fixture，新GPU调用0，不授予新入口的Q1真实性能或物理清理资格。
该子项HANDOFF的SHA-256为 `dc8028176a4528ee257006c60e8ce921db5da01308e5511772730035d58b6478`；本任务仅核验并引用，没有重跑。

## 完整 sink 与后置审计的计时修正

独审发现 `ffbc352ac`、`9256287f8` 和 `e097bd6c9` 的 `finish()` 在返回前已构造逐行消息摘要、
并将算子关系转换为JSON可保存结构；旧driver在函数返回后才记录sink。因此旧49条真实结果及
旧CPU回执的质量/条数仍保留，但其source→sink混入了这些审计成本，不能当严格共同sink口径。
没有足够原始事件倒推出旧纯sink时刻，本修正不追改或重算旧值。

后续修正在既有 `finish(..., on_result_ready=...)` 接口中，将必要的源身份验证、原parser、关系重建和
原完整查询保留在执行路径；原查询返回后立即通知driver记录sink，再生成消息摘要及完整关系审计产物。
回调默认省略时仍返回相同结果与审计结构，已有调用者兼容。审计异常向上抛出，driver保留失败状态、
已观测sink和原生输出，不把结果已出误报成整次通过。审计起止及结果持久化时间单独记录。

[修正回执](SINK-CLOCK.json)记录19项受影响CPU检查，以及Q1 25、Q3a25、Q4 24的完整保存答复回放。
可控时钟分别为消息摘要和关系序列化注入耗时，验证这些耗时均位于sink之后、未进入source→sink；
失败用例验证后置审计失败仍保留原生答复及失败driver回执。完整消息/token、原关系、sink及evaluator保持一致。
这些74条是软件回放，真实模型调用0；旧原生49条不重跑，新计时实现仍没有真实模型性能数据。
当前资格按明确源码和实际时钟分别记录，不能将旧回执中的 `sink_before_close_and_audit` 标记当成本次验证。

## 尚未完成与清理边界

参数化入口有19题，完整源数据CPU回放覆盖Q1/Q3a/Q4三题，其余16题不能自动标记已合格。
真实模型资格仍只覆盖ffbc的Q3a/Q4；其他17题及共同host总时钟/跨系统条件仍须按协议核对。
这个入口没有实现新的同 owner 暖会话；不把“模型已构造后的尾段”算暖查询。

恢复来源是 `daft-native-medical-q10-cold-20261108-r1` 的准确私有 source，以及
`daft-native-mmqa-q1-cold-20261110-r1` 的原 driver；其历史 GPU 成绩不转授本次新源码。
不再复制 per-query driver，原始 packet/失败记录保留可追溯恢复。正式候选已准备，合 main 由协调者执行；
只有新入口在目标环境覆盖旧职责、引用迁移及恢复核验通过后，才删除对应旧活动接线。
LOTUS Q3a/Q4、Kalypso Q1 冻结包和其他活动工作树不在此次清理范围。
