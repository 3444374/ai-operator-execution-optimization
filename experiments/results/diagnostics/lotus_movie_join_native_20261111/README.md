# LOTUS Movie 原生 Join 的有限 CPU 接线

问题是原 SemBench Q5/Q6/Q7 exact Join 已实现，但正式有限文本入口只接 F/M/E 与 TopK。
本候选增加一条共享 Join 描述和原生绑定，复用既有 resident、LM/LiteLLM、本地 IPC、永久预算和监督。
原作者 SemBench 固定 `c814e3807e72d4cf876b852b17e77f3cc94575c2`，LOTUS SDK 为 1.2.4；
SemLoom Core 不执行或调度 Join。代码和用法见[原生基线入口](../../../../code/src/baselines/README.md#lotus-movie-exact-join)。

## 数据、原语义和请求上限

原 `Reviews.csv` 为 2,000 行，SHA-256 为 `ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf`。
本轮只按原 CSV 顺序选择 Ant-Man 的前 8 个 occurrence，原位置为 `8,10,17,25,29,39,40,42`，
保留所有原字段；生产 loader 将 gold 字段排除在模型输入之外。模型只见原 Join 的两份 reviewText。

LOTUS exact Join 对左右有序笛卡尔积逐项比较，包含相同 ID、相同文本与自身对。
原 Q5 使用同情感指令，Q6/Q7 使用反情感指令；无效 Boolean 保留原 `default=True`，不另发请求。
模型返回后作者才排除同 reviewId，Q5/Q6 再 `head(10)`，Q7 保留全部原匹配；评价继续调用原 evaluator。
样本每题上限为 8×8＝64，原模型 owner 显式禁重试、结果缓存关闭、每次 IPC 交接前扣永久预算。
完整输入有 256 occurrence、128 个不同 reviewId，每题 65,536 对，512 个同 ID 对只在模型后排除；
完整三题 196,608 次上限只是源码推导，本任务未释放该额度。

## 已完成的软件检查

运行身份为 `lotus-join/q5-r1`、`q6-r1`、`q7-r1`；原件在项目仓库外同名交接目录，通过 HANDOFF 清单恢复。
每题通过真实 LOTUS accessor/LM/LiteLLM 和 Unix IPC，模型子进程是显式软件 fixture：

- 64 个不同 pair 身份、64 次软件请求及终态；完整原消息和离线真实 tokenizer token 数组一致。
- 合成 Boolean 答复经原 Join、完整原 sink 和 evaluator 重放一致；没有用 Map 或其他模型结果替代。
- 第 65 次永久预算扣除被拒；每题一个软件 model owner 构造并关闭。
- 结果就绪先于 receipt 汇总、snapshot、后置语义重放、评价、持久化和最终关闭。

9 项定向单测包括重复文本/ID 不去重、原 parser 回退、非法 Join 选项、非 resident 配置和 F/M/E 路径拒绝、
不足预算在 owner 前拒绝、失败证据及原异常保留。受控时钟额外给 receipt 汇总 11 秒、snapshot 7 秒、
评价 13 秒，source→完整 sink 仍为 3 秒；结果后审计失败保留已观测 sink 并以失败关闭，不发布成功。
第一轮 3 个测试错误来自本候选曾借一元 FilterMethod 构造 LM，导致 `:left/:right` 被错误解析；
已改直接原 LM 构造，原失败摘要保留，不归因为作者 Join 缺陷。

首轮冷查询的既有 source 时钟包括惰性模型启动；实际 model-ready 事件与 startup 分项另记，
ready 后尾段不能改称 fresh warm 查询。严格外层控制器起点、engine 首接受和 kernel 时间未由 CPU fixture 证明。

## 目标边界

未运行真实模型/GPU、目标 vLLM renderer、物理资源监督或完整 256×256 查询。
这些软件答复的 evaluator 数值不是模型质量，CPU 时间不作性能结果。
先按既有唯一服务器协调流程绑定样本文件、source/code tree、实际模型/tokenizer/版本与有限预算，
必要目标零模型门检后再决定各题独立 64 次资格；不重跑其他已完成模型任务。
不支持图像/音频的系统格子保持原边界，本变更不授予 Ray/Daft 的 Join 资格。
