# SAOR 历史研究与离线证据

当前保留 SAOR 调度算法、研究方案、合同、离线分析与历史证据。
通用共享额度与 SAOR 策略一起位于[独立研究组件](../../research/multitask/README.md)，
本目录只编排、校验与分析，不向普通 records 注入研究策略。
14 份原实验声明位于[独立材料](../../../../experiments/saor/README.md)，
其原路径与逐文件摘要见[资产清单](../../../../experiments/reproduction/legacy_recipes_20261105/MANIFEST.json)。

`native_system_contract.py`、validator、artifacts、completion、observation、publisher 与 summary
保留原五臂身份、完成证据、失败关闭校验和结果读取职责；feeding-gap、feeding-ceiling、project-mechanism
模块继续支持历史研究分析。离线 summary 直接使用所属证据模块，不加载旧矩阵执行器。
旧 matched 配置、执行编排和专属 preflight 已退役，完整来源及恢复方式见
[历史执行接线](../../../../experiments/reproduction/native_baselines/README.md#retired-native-system-matrix)。
这些模块不为当前 SemBench 装配另一套执行器。

依赖旧 PG／HTTP 的 bindings、readiness、execution 和启动脚本已退出维护目录；
旧运行命令只在[精确冻结树](../../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)解释。
原 native-system 三份 JSON 随该批归档，不在新目录补建兼容入口。

未接入的 `saor_pipeline.py` 和 CPU control microbenchmark 原件仍位于
[历史复现来源](../../../../experiments/reproduction/native_baselines/README.md)。
未执行的 S-LoRA FCFS／VTC 与跨层原型继续从
[固定来源](https://github.com/3444374/ai-operator-execution-optimization/tree/928a339334a4075fb2cf96a2dddc4727dec984fe)恢复。
这些材料、旧模板与 CPU 检查不构成模型比较、SemBench 查询或性能资格。
