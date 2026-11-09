# 同请求消息长度复用的 CPU 检查

来源：main `f44f174a27e564734774c015fa0847334698a321`；研究分支独立实现。
[计划](../../../plans/数据组织.md#message-frame-size-reuse)与[研究说明](../../../../docs/research/优化方法依据.md#message-frame-size-reuse)
先限定比较和停止条件。本报告支持 CPU 组件及固定结果回放，并补充上游 CSV 只读画像；实际输入的原生准备成本与 GPU 性能仍未验证。

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
本轮另有[19 项检查](raw/followup-checks.log)，只增加真实输入采集器的两项 CPU 替身检查，
不因此声称原生 SDK 已执行。两个已存在的本地 Python 环境均没有 Ruff／LOTUS／transformers，
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

由主会话在已有合法 CPU 环境执行；本任务没有登录服务器。变量均取现有实际路径和运行声明，
尤其字节额度不能套用 fixture 值。先按 runtime README 使用已有 `AI_OPERATOR_ENV_FILE`
完成 `core,text,semantic-benchmarks` 的只读 preflight；源码、数据和 tokenizer 不下载。

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
目前保留为 CPU 工程微优化；没有完成用户期待的 DAG／CPU–GPU 跨层创新成果，
也没有实际输入的原生成本结论。本轮不扩大生产接点，PR 保持 draft。

无损原件可用已有命令恢复到一个新目录：

```bash
PYTHONPATH=code python3 code/scripts/analysis/restore_result_evidence.py \
  experiments/results/diagnostics/message_frame_size_reuse_20261109/raw/storage-manifest.jsonl \
  --output /tmp/new-message-frame-evidence
```
