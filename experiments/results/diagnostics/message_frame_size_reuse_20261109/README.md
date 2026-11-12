# 同请求消息长度复用的 CPU 检查

来源：main `f44f174a27e564734774c015fa0847334698a321`；研究分支独立实现。
[计划](../../../plans/数据组织.md#message-frame-size-reuse)与[研究说明](../../../../docs/research/优化方法依据.md#message-frame-size-reuse)
先限定比较和停止条件。后续准确 `586fe4c` 的实际 Q9 输入 CPU 检查已触发停止条件：长度复用未胜过直接 JSON。
本报告区分早期 fixture、实际 CPU 回执与 2026-11-12 模板身份修复；未安排该候选的 GPU 实验。

## 发现与实现

源码中的 `PreparedTokenOrganizer._estimate` 对同一 `messages_json` 分别测算模型提交帧、数据 RPC 帧；
每请求的估计原本已缓存，重复发生在首次测算内。候选只求一次 JSON 转义后的 UTF-8 长度，
再求两种空消息信封的长度，最后得到原帧的精确字节数。真正发送仍完整编码。
生产改动仅提取这对测算到一个静态方法；原组织器仍使用原 `_frame`。研究子类覆盖测算，
普通入口不导入研究模块，也不新增配置开关。

另一个简单内部对照直接使用 `json.dumps` 测量两份完整帧，不复用消息长度。
它先物化完整 JSON 再核对大小，原编码器按片段检查；本次没有测量二者的完整进程主存差异，
不因此建议替换实际发送编码器。三臂均由原组织器选择批成员，均不是第三方原生 baseline。

未采用的候选：token 工作量估计与异步 token 组批在当前入口互斥，不能凭两处计数代码声称同路径重复；
现有 FIFO／后继优先已经可用。没有取得实际查询的完整 token 前缀语料，本次不实现前缀重排。

## 检查范围

证据包内的 `checks-r2.log`（17 项）通过：新增 6 项、原组批 8 项、常驻反馈 3 项。
新增检查覆盖 103 种空值／转义／Unicode 消息，精确容量与少一字节拒绝、上下文超限、
批成员与 token 读取次数，以及模型参数、token ID、父关联和重复项不变。
实际有限链的 Filter→Map、Map→Filter、Filter→Map→Extract、Extract→Map 使用脚本化 formatter／parser／输出，
核对逐调用完整消息、生成参数、依赖、Extract 完整关系和最终顺序。
原反馈检查另包含真实 CPU 子进程／socket，同 owner 两轮 Map／Extract；Ray、SDK 与模型均是替身。
固定输出回放不能证明真实 GPU 输出逐位相同，也不提供语义质量结论。

全部运行无模型、无服务器访问、无依赖安装。实际测量源码的 SHA-256 见证据包内 `source-r2.json`。
[无损证据包](raw/evidence.tar.gz)保存原 11 个文件的全部字节，包括两臂预演、三臂五次重复、
原检查记录和 Ruff 缺失记录；不混合两次采样。[存储清单](raw/storage-manifest.jsonl)与
[恢复验证](raw/storage-verification.json)核对 31,790 字节原件，压缩包为 3,634 字节。
原有恢复器逐字节核对后才移除展开副本，没有删选重复值或慢样本。
`bcc95e423` 时另有[19 项检查](raw/followup-checks.log)，只增加真实输入采集器的两项 CPU 替身检查，
不因此声称原生 SDK 已执行。当时检查的两个本地 Python 环境均没有 Ruff／LOTUS／transformers，
静态 lint 和原生 CPU 成本保持 pending；未安装依赖。

## CPU 结果

Python 3.12.6、Darwin arm64；64 条已就绪记录，窗口 8，4 MiB 逻辑帧容量。
每臂一次预热、五次交错测量，所有重复值保留在下列 JSON。输入为脚本化文本和 codepoint token ID，
不冒充 SemBench 数据或实际模型 tokenizer。
计时包含任务编码、记录 token 读取、原组批、字节预测、完整帧编码和固定结果装配；源构造另计，
不是完整语义查询时间。

| 正文字符数 | 原测算（ms） | 直接 JSON（ms） | 长度复用（ms） | 复用相对原测算 | 证据包成员 |
|---|---:|---:|---:|---:|---|
| 256 | 6.700 | 5.181 | 6.689 | −0.17% | `cpu-256-r2.json` |
| 4096 | 18.535 | 16.943 | 18.048 | −2.63% | `cpu-4096-r2.json` |
| 65536 | 215.022 | 219.001 | 204.699 | −4.80% | `cpu-65536-r2.json` |

表中为中位数，百分比为 `(复用/原测算−1)×100`。短、中消息分别比直接 JSON 慢 29.09%、6.52%；
长消息比直接 JSON 快 6.53%，该场景直接 JSON 有一次 291.603 ms 的慢样本，未删除。
这是有限 CPU 观察信号；较小场景的绝对节省不足以支持 GPU 供给改善结论。
GPU 空转、KV 命中、prefill、显存与完整进程主存未测，JSON 中为 null 并说明原因。

## 复现与实际输入

无需额外依赖；从仓库根目录运行：

```bash
python3 code/scripts/experiments/replay_message_frame_sizes.py \
  --rows 64 --body-chars 4096 --window 8 --repeats 5 \
  --item-input-bytes 4194304 --item-result-bytes 8192 --input-bytes 4194304 \
  --output /tmp/message-frame-replay.json
cd code
python3 -m unittest tests.research.data_organization.test_message_frame_sizes \
  tests.execution_provider.test_prepared_request_organization \
  tests.execution_provider.test_chain_resident_ready_feedback -v
```

对实际保存的已就绪集合，可用 `--input /path/to/frontier.json` 取代生成输入。
文件是单一模型／模板下的 JSON 数组，每项为 `row`（原 prepared row，含完整身份及 `messages_json`）、
`generation`、`model`、`token_ids`、`parent_call_ids` 和 `result`（含同一 `call_id` 的固定终态）。
数组只表示已经合法就绪的请求，不从全链日志猜测就绪时刻；保留输入原件和完整来源，
输出只保存校验摘要。脚本不调用 tokenizer、模型或服务器，也不生成新的 DAG。

## 最小真实输入 CPU 验证

[上游 CSV 只读画像](raw/movie-source-shape.json)固定 SemBench `c814e380` 的公开 `Reviews.csv`，
blob 为 `710028a863dbf04853e45a3e618113dff7521b4d`：2000 条、1864 种正文，正文 UTF-8 字节
中位数 148、95% 分位 250、最大 301。Q9 的 quantumania 子集为 256 条／128 种正文，最大同为 301。
这些是实际源字段长度，不是完整 prompt、token 数或 CPU 成本；65536 字符 fixture 不代表该数据。
Movie Q9-map 只有一个语义 Map，前置为普通谓词，不能据此证明多算子链优化。

已提供[原生采集入口](../../../../code/src/research/data_organization/native_frame_profile.py)，
只复用既有 `SemBenchTextQuery` 的原选择、`LotusRecordMethod.prepare_rows` 和本地
`AutoTokenizer`，不创建服务 owner、Ray、模型或新 DAG。选原 Q9-map 的前 128 个已就绪 occurrence；
初始调用无语义父节点，重复项不删。也可改用既有 Medical Q10 配置检查 Extract 输入准备，
不能由该初始集合推断模型结果之后的就绪状态。

以下为当时交给主会话的执行说明；实际完成状态见文末准确来源回执。变量均取现有实际路径和运行声明，
尤其字节额度不能套用 fixture 值。先按 runtime README 使用已有 `AI_OPERATOR_ENV_FILE`
完成 `core,text,semantic-benchmarks` 的只读 preflight；源码、数据和 tokenizer 不下载。

`bcc95e423` 的[实际配置入口检查](https://github.com/3444374/ai-operator-execution-optimization/pull/93#issuecomment-6776049824)
在采集前被阻塞：回放固定 8192 结果字节，并将单项输入与总输入混用；实际 Q9 的单项输入／结果均为 2 MiB。
当时没有执行采集或回放、没有模型／GPU 调用。本次只修实验接口，三项字节参数都必须显式提供，
各臂构建相同声明的 `AsyncEngineLimits`，构造开销仍计入原回放区间；不修改生产默认，也不据参数修复判断收益。

| 参数 | 透传位置 |
|---|---|
| `--item-input-bytes` | `AsyncEngineLimits.item_input_bytes` 与每项提交帧的编码上限 |
| `--item-result-bytes` | 任务 `max_result_bytes`、`AsyncEngineLimits.item_result_bytes` 和 RPC 的 `maximum` |
| `--input-bytes` | `AsyncEngineLimits.input_bytes`，原组织器的总输入预算及该版本的批 RPC 负载上限 |

最后一项取既有 owner 的 IPC 声明，不用单项字节数或 Core 总池替代。RPC 没有另设独立容量字段；
原 owner 的批 RPC `_encode` 同样使用 `limits.input_bytes`，回放遵循该映射。
九项受影响 CPU 检查通过，覆盖非 8192 结果值、不同单项／总输入值传入 CLI、原尺寸核验和编码，
并保持三臂成员、完整请求、token 与固定装配相同；采集替身另覆盖 2 MiB 单项声明。
新增本地输出追加到[原检查日志](raw/followup-checks.log)，不覆盖先前结果；该阶段的真实输入结果当时为 pending，随后在准确 `586fe4c` 执行。

```bash
CUDA_VISIBLE_DEVICES='' HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 PYTHONPATH=code \
  "$DRIVER_PYTHON" -m src.research.data_organization.native_frame_profile \
  --config "$EXISTING_Q9_CONFIG" --sembench-checkout "$SEMBENCH_CHECKOUT" \
  --data-dir "$EXISTING_MOVIE_DATA_DIR" --tokenizer "$EXISTING_TOKENIZER_DIR" \
  --rows 128 --repeats 5 --item-input-bytes "$ACTUAL_ITEM_INPUT_BYTES" \
  --input-bytes "$ACTUAL_INPUT_BYTES" --item-result-bytes "$ACTUAL_ITEM_RESULT_BYTES" \
  --output-dir "$CPU_PROFILE_ROOT/capture"

"$DRIVER_PYTHON" code/scripts/experiments/replay_message_frame_sizes.py \
  --input "$CPU_PROFILE_ROOT/capture/frontier.json" --repeats 5 \
  --window "$EXISTING_REQUEST_WINDOW" --input-bytes "$ACTUAL_INPUT_BYTES" \
  --item-input-bytes "$ACTUAL_ITEM_INPUT_BYTES" --item-result-bytes "$ACTUAL_ITEM_RESULT_BYTES" \
  --output "$CPU_PROFILE_ROOT/replay.json"
```

`capture/frontier.json` 在 Git 外保留完整消息、生成参数、模型声明、原生 token ID 和 occurrence；
结果明确为未执行占位，不冒充原模型答案。`profile.json` 保存源文件／SDK 来源、模板摘要、
消息字节／token／两种帧大小的最小值、中位数、95% 分位和最大值，以及完整重复分布。
每次直接计时 formatter、原 prepared 校验、原生 token 化、信封构建及三种测算；
`original_sizing_fraction_in_slice` 的分母只含这些串行 CPU 段。
Core payload 编解码、源读取、LM／tokenizer 构造、发送与落盘未混入分母，不能称完整查询占比。
准备总耗时若另从现有原始 CPU 区间取得，仍须对齐实际路径；本切片给出忽略额外准备成本的宽松上界。
首次懒初始化样本单列保留，后四次观察复用态，不从含启动的时间里减去猜测值。

请求／token 或任意尺寸不同立即停止；字节超额保留原拒绝。若长度复用不能胜过直接 JSON，
或即使消除全部目标测算也小于准备时间的重复波动，就停止该候选，不安排 GPU。
该候选只评估 CPU 工程开销，不是 DAG／CPU–GPU 跨层方法；后续实际 CPU 回执判定停止，生产接点不扩大，PR 保持 draft。

无损原件可用已有命令恢复到一个新目录：

```bash
PYTHONPATH=code python3 code/scripts/analysis/restore_result_evidence.py \
  experiments/results/diagnostics/message_frame_size_reuse_20261109/raw/storage-manifest.jsonl \
  --output /tmp/new-message-frame-evidence
```

<a id="actual-cpu-stop"></a>
## 准确来源的实际 CPU 结果与停止决定

2026-11-12 回查[原执行回执](https://github.com/3444374/ai-operator-execution-optimization/pull/93#issuecomment-6776968035)，
将准确 `586fe4c44eb21ddd4670fb9f1b3099b775219caa` 的结果补入本报告；
[回执副本](raw/actual-cpu-result-comment.json)保留原文与来源。本次没有重新运行实际采集，也未重新取得其私有原始包；
以下数字归该回执，不能据本轮 fixture 修复将它们转授新源码。

实际环境为 Linux x86_64、Python 3.12.3、LOTUS 1.2.4、Transformers 4.57.6，CPU 0–7。
原 Q9 初始 128 个 occurrence 包含 99 种消息，29 种出现两次、70 种一次。
五次原生 formatter/tokenizer 采集；各臂一次预热、五次计时回放。
单项输入和结果各 2 MiB，总输入 272 MiB、窗口 136；没有模型调用、Ray runtime 或 GPU 工作。

| 实际 Q9 输入回放 | 中位时间（ms） |
|---|---:|
| 原测算 | 47.897 |
| 直接 JSON | 36.468 |
| 长度复用 | 47.249 |

长度复用比直接 JSON 慢 29.56%，已满足预先声明的停止条件，不安排此候选的 GPU 实验或默认接入。
原测算占已捕获串行 CPU 切片约 12.81%；它不是完整查询占比。
原回执保留首次采集包装器的导入失败及修正，源码和依赖未改；进程、SHM 与 lease 收尾通过。

<a id="template-identity-review"></a>
## 2026-11-12：模板身份缺陷修复

两项独立审查在相同 `586fe4c` 上发现同一个问题：CLI 接受 `engine.chat_template`，
但采集使用 tokenizer 默认模板，随后把带覆盖值的 engine 声明写入 frontier。
不同模板会产生不同 token，三臂共享错误采集结果的内部一致性检查无法发现这种来源错配。

现与已有请求采集入口一致，在采集前拒绝显式模板覆盖；没有增加模板加载器或改变默认模板行为。
新增 CLI 反例先复现未拒绝，修后通过；连同消息帧测算、请求组织共 18 项本地 CPU 检查通过。
SDK、tokenizer 和查询对象均为临时 fixture，无模型或外部服务访问。
[修复证据](raw/template-identity-checks.json.gz)保留红／绿日志、两次测试启动／记录器错误及源码摘要；
这些失败未被计入 18 项通过数，也不与历史检查累加为整树资格。
准确 `586fe4c` 的实际运行使用默认模板，原停止判断保留；新版本不声称重新取得实际 CPU 或 GPU 资格。
