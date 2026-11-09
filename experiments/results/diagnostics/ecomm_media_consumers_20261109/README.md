# Ecomm Q11 表示与消费次序 CPU 可行性

实验身份 `ecomm_media_consumers_20261109`，基于远端 main
`f44f174a27e564734774c015fa0847334698a321`，隔离分支 `research/semloom-ecomm-media-consumers-20261109`。
本报告只记录原型与本地标准库检查；原生 SDK／真实图片尚未运行，GPU 与方法收益为 pending。
PR #93 的请求序列化微优化是另一项工程诊断，不代替本研究目标。

[方案](../../../plans/数据组织.md#ecomm-media-cpu)负责资源和停止条件；
[机制、源码比较与命令](../../../../code/src/research/data_organization/ecomm_media.md)负责接口说明。
新代码只在研究目录及其测试，生产调度、语义方法、vLLM 与默认均没有改动。

## 已运行

本地 Python 3.12.6，七项标准库检查通过；另一个原生 Q11 检查因缺少 LOTUS、pandas 和已选 SemBench checkout 被跳过。
覆盖资产分组保留重复项、重排请求／响应逐项相同、模型／参数／媒体／依赖改变时拒绝、重复／漏项／额外请求拒绝，
以及块成功／异常时的表示清理和原生缓存保留。CLI help 与编译通过，Ruff 因已有环境无该工具为 pending。
这些 fixture 只说明实现行为，不说明真实图片准备成本、GPU 请求顺序、显存、缓存命中或质量。
完整本地输出及最初测试命令错误保留在 [checks.log](raw/checks.log)。

## 交给同一服务器协调会话

先执行原生软件 fixture；再使用已有最小 Ecomm 样本，按模块命令跑 `all_true` 和 `mixed` 两种固定输出。
完整消息仅保存在私有目录，原生图片变换仍由 LOTUS `fetch_image` 处理；不启动模型／GPU，不安装或下载。
等待协调会话确认 CSV／图片／SDK 的真实路径与环境报告后，登记实际输入、全部重复、失败与源码哈希。
若 native 已覆盖重复或剩余成本不值得处理，记录负结果并停止该候选，不把距离或逻辑字节当 GPU 指标。
无 CPU 实测收益或足够服务缓存证据前，不安排模型运行；后续 GPU 范围由主会话统一排期。
