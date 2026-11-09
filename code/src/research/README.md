# 独立研究组件

本目录保存与普通 SemLoom records 单查询执行分开维护的研究实现。

- [多任务共享服务与 SAOR](multitask/README.md)：通用共享额度、策略选择、有序释放和研究适配。
- [数据组织](data_organization/README.md)：已有文本组织原型与独立 Ecomm Q11 图片表示／消费次序 CPU 候选。

实验编排、声明和历史结果分别留在 `src/experiments/`、仓库 `experiments/` 和原证据目录。
普通执行不导入本目录。多任务算法使用本包只读输入接口；显式 SemLoom 适配消费执行协议，复用原执行器。
