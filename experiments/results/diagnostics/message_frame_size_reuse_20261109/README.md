# 同请求消息长度复用的 CPU 检查

来源：main `f44f174a27e564734774c015fa0847334698a321`；研究分支独立实现。
[计划](../../../plans/数据组织.md#message-frame-size-reuse)与[研究说明](../../../../docs/research/优化方法依据.md#message-frame-size-reuse)
先限定比较和停止条件。本报告只支持 CPU 组件及固定结果回放，尚无真实数据、原生 tokenizer 或 GPU 性能证据。

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

[17 项结果](raw/checks-r2.log)通过：新增 6 项、原组批 8 项、常驻反馈 3 项。
新增检查覆盖 103 种空值／转义／Unicode 消息，精确容量与少一字节拒绝、上下文超限、
批成员与 token 读取次数，以及模型参数、token ID、父关联和重复项不变。
实际有限链的 Filter→Map、Map→Filter、Filter→Map→Extract、Extract→Map 使用脚本化 formatter／parser／输出，
核对逐调用完整消息、生成参数、依赖、Extract 完整关系和最终顺序。
原反馈检查另包含真实 CPU 子进程／socket，同 owner 两轮 Map／Extract；Ray、SDK 与模型均是替身。
固定输出回放不能证明真实 GPU 输出逐位相同，也不提供语义质量结论。

全部运行无模型、无服务器访问、无依赖安装。实际测量源码的 SHA-256 见[源码清单](raw/source-r2.json)。
首次 14 项结果与两臂预演保留在 `raw/checks.log`、`raw/cpu-*.json` 和 `raw/source.json`，
后续三臂使用 `-r2` 文件；不混合两次采样。原始文件均已脱敏。Ruff 在本机未安装，静态 lint 为 pending；没有为此混装依赖。

## CPU 结果

Python 3.12.6、Darwin arm64；64 条已就绪记录，窗口 8，4 MiB 逻辑帧容量。
每臂一次预热、五次交错测量，所有重复值保留在下列 JSON。输入为脚本化文本和 codepoint token ID，
不冒充 SemBench 数据或实际模型 tokenizer。
计时包含任务编码、记录 token 读取、原组批、字节预测、完整帧编码和固定结果装配；源构造另计，
不是完整语义查询时间。

| 正文字符数 | 原测算（ms） | 直接 JSON（ms） | 长度复用（ms） | 复用相对原测算 | 原始重复值 |
|---|---:|---:|---:|---:|---|
| 256 | 6.700 | 5.181 | 6.689 | −0.17% | [JSON](raw/cpu-256-r2.json) |
| 4096 | 18.535 | 16.943 | 18.048 | −2.63% | [JSON](raw/cpu-4096-r2.json) |
| 65536 | 215.022 | 219.001 | 204.699 | −4.80% | [JSON](raw/cpu-65536-r2.json) |

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

## 后续范围与停止决定

当前停在独立研究原型；未改生产默认，未合并，未占用 baseline GPU。
先由主会话对真实已就绪输入运行上述 CPU 命令，记录完整查询中的测算占比。
若占比不足，停止 GPU 验证；若足够，再按计划排期最多 1088 次真实调用，并先完成 runtime preflight、
固定实际机器／模型／服务／工作负载签名及持久次数记录。当前未执行这一步。

既有 rolling owner 的有限实机接点是：`evaluate` 返回、首次消费前，仅将该 owner 的
`_token_organizer._request_frame_sizes` 绑定到研究函数 `reused_message_frame_sizes`；
默认臂保持原静态方法。两臂完整记录请求与结果，计时从原查询开始到结果可用。
这是提供给主会话现有监督程序的接点，不新增模型启动器、租约或预算实现；
不在 baseline 进程中修改全局类，不复用本机 fixture 的容量值作为实机参数。
