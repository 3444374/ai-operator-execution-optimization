# Palimpzest Movie 原生接线与固定计划边界

本切片先建立原 SemBench Movie Q3 完整过滤／count，再保留 Q2 LIMIT5，
使用 PZ 原并行策略和 Generator，未改上游源码。接口与运行方法见
[PZ 模块](../../../../code/src/semantic_methods/palimpzest/README.md)；
[本节计划](../../../plans/语义系统对照.md#palimpzest-native-and-fixed-plan)与[公开原值](qualification.json)分开保存。

固定 SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2`，
PZ 专属 tag `0.8.2.sem_agg` 对应 `6efc19841c49d727ce42f5c2dd55e7433601d2d9`。
使用显式 `QueryProcessorConfig`，避免旧 `Dataset.run` 丢弃 kwargs；
只提取原查询方法并捕获其终端 run，不使用 generic runner 的 Gemini 回退。
旧 vLLM enum 与精确本地服务模型的绑定、原模型卡与实际费用分别记录。

## 软件资格

本机 Python3.12 独立环境中运行真正固定 PZ／LiteLLM／OpenAI 库，
使用 8 行合成数据和 localhost HTTP fixture，无模型推理、无 GPU 工作。
PZ0.8.2、LiteLLM1.95.0、OpenAI2.50.0、pandas2.3.3、NumPy2.0.2、Arrow20.0.0；
Pydantic2.14.1、HTTPX0.28.1 实际版本另记。依赖解析、安装、freeze 与 preflight 原件仓外保留。

| 查询 | 实际 fixture POST／完成 | 原生实际配置 | 关系结果 |
|---|---:|---|---|
| Q3 | 6／6 | parallel，workers4，join4，batch_size=null | COUNT=5 |
| Q2 | 6／6 | parallel，workers4，join4，factory 将 batch_size 夹至5 | 5行，保留重复 reviewId |

Q2 的结果上限不是请求上限。两条查询的桥均恢复、活动 Generator 归零，
错误及解析默认均为0；这不单独证明远端 GPU 已停止。
Q3 刻意让一次假阳性和一次假阴性相互抵消，COUNT 误差为0，逐行审计仍分别记下 FP1、FN1。
失败／解析默认与真实 negative 分开，缺 usage 为 unavailable，原成本卡默认零不解释为免费。

固定计划 CPU 检查另用原库完成：原 prompt/parser/usage、completion／parse 默认、missing-usage 原异常、
parallel 的完整 count、禁止重优化和计划变化拒绝。它们使用内存 fixture completion，不产生 HTTP 或模型请求。
首轮 sandbox socket 拒绝与缺 cdlib 的失败保留；首次测试临时目录已清理，不能声称恢复其原始响应。
其后受影响项在补依赖后使用新的输出目录重跑；unittest 总耗时不作为查询性能。

独审发现并修正两项接入合同问题：响应模型身份失配现为致命合同错误，并阻止之后的新提交；
另将 metadata GET 纳入新目录的失败记录。新增真库反例中，两行错模型仅发1次 POST，
普通503两行仍发2次并保留上游默认；新增3次 fixture POST、模型0，未重跑上项12次或改写其来源。
metadata 错误／失联保留配置与失败摘要，busy lease 不创建输出。

## 当前边界

`freeze_best_plan` 只覆盖无 validator 的最终计划；完整系统采样和 validation 不得被省略。
只替换 completion 接点时，PZ 继续拥有 DAG。源数据、配置、请求摘要和服务／缓存状态须与计划指纹共同核对。
原生完整 2000 行 Movie 模型资格、待执行工作交接，以及 PZ＋SemLoom／IMLane／Daft／Ray Data 的逐臂资格仍待完成。
本记录不支持五组排名、真实模型质量、性能收益或可靠尾分位。

附件审查针对 PZ `807ed301`／1.5.3，本切片继续保留 SemBench 所需的旧 tag。
后续正式后端接入按 A（选定计划后、原线程池前）和 B（最终 SDK 请求准备后）两处边界实施；
现有仅 completion 的桥保留为原生／诊断证据，不授予完整后端替换资格。
首版适配仅接受已核单模型 LLMFilter、单字段 ONE_TO_ONE LLMConvertBonded 及普通扫描／谓词／投影／结构 count；
LIMIT、join、distinct、语义聚合或未核多调用形状明确 unsupported。计划中的六臂另加 native-adapted 控制，
默认 work=1 只表示请求计数；其实现、序列化、取消与模型资格仍需分别验证。
