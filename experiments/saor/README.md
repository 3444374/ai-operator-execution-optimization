# SAOR 独立研究材料

本目录保存从 `deploy/autodl/` 移出的 14 份现存 SAOR 声明：实验 JSON、两份锁定合同与 env 示例。
内容逐字节保留；原路径、新位置、字节数与 SHA-256 见[资产清单](../reproduction/legacy_recipes_20261105/MANIFEST.json)。
旧 raw 元数据和历史命令中的原路径仍按其原来源解释。

当前主线为 SemBench。这里的声明供研究核对和历史恢复，不作为新的运行默认；
模板、原许可字段与历史预算均不授予新实验资格。依赖旧 PG／HTTP 的启动器及 native-system 三份 JSON
已先行退出维护目录，整套接线从[准确冻结树](../reproduction/paused_pg_and_retired_http_20261105/README.md)恢复。
需要旧相对路径布局时使用清单中的精确 Git 来源，不能只把新位置的单文件接到当前执行器。

保留的研究内容从以下入口定位：

- [多任务共享服务与 SAOR 实现](../../code/src/research/multitask/README.md)，与普通 records 单查询分开维护。
- [SAOR 证据模块](../../code/src/experiments/saor/README.md)及[离线脚本](../../code/scripts/README.md#独立研究与离线分析)。
- [历史研究方案](../plans/archive/README.md)和[现行方案](../plans/README.md)。
- [历史结果](../results/README.md)与[证据台账](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)。

调度算法、原始结果、失败、成本和 unknown 保持；路径整理不改变研究结论或恢复旧矩阵的运行资格。
