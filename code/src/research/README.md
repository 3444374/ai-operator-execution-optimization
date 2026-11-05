# 独立研究组件

本目录保存与普通 SemLoom records 单查询执行分开维护的研究实现。

- [多任务共享服务与 SAOR](multitask/README.md)：通用共享额度、策略选择、有序释放和研究适配。

实验编排、声明和历史结果分别留在 `src/experiments/`、仓库 `experiments/` 和原证据目录。
普通执行不导入本目录；研究组件按需消费现有中性数据类型和执行协议。
