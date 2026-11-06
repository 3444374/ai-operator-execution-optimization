# 多任务共享服务与 SAOR

本包研究多个 Job 争用同一模型服务时的请求／工作额度分配。通用额度协调和
Stage-Aware Ordered Release（SAOR，按阶段状态排序释放工作）一起维护；SAOR 是可选策略，
不是当前 records 单查询的请求反馈、批反馈或服务内存预算。

普通 Movie records 创建 Session 时不传入共享协调器；默认配置、执行导入和核心测试均不依赖本包。
本次没有修改 Session、adaptive 控制器、records 查询循环或模型传输的实现。

## 职责与依赖

| 模块 | 负责什么 | 仓库内依赖 |
|---|---|---|
| [shared_credit.py](shared_credit.py) | 端点／Job 额度、租约、等待队列、容量和公共事件 | 本包的策略与 SAOR 数据类型 |
| [credit_policies.py](credit_policies.py) | FIFO、DRR、VTC、严格优先级及 SAOR 的选择和完成记账 | 本包的 SAOR 策略 |
| [saor.py](saor.py) | 纯排序、份额／债务计算和 drift-plus-penalty 决策 | `CapacityArm` 请求／工作容量值 |
| [ordered_release.py](ordered_release.py) | Job 队首、按序发布和完成回收 | `CapacityArm`、`PayloadEnvelope` |
| [saor_capacity.py](saor_capacity.py) | SAOR 容量消融的观测换算和容量选择 | `CapacityArm`、`RuntimeStateSnapshot` |
| [shared_credit_ray.py](shared_credit_ray.py) | 命名 Ray actor 与协调器客户端 | 本包协调器；调用方传入的 Ray API |

```text
shared-vLLM / image-multijob / SAOR replay / 显式共享调度画像
  -> research.multitask（额度与策略）
  -> scheduling.core 的中性容量／请求数据、planning.work 的状态数据

records 单查询 -> Session / 本地准入与路由 -> 现有模型适配
```

`scheduling` 不反向导入研究包，也不再导出这些策略。旧六个模块路径已删除，无转发壳；
调用方直接从本包所属模块导入。同步调度器与增量 Session 继续消费既有结构化 credit 协议，
可选的本地协调器接线不增加它们对某一策略实现的依赖。
画像仅在显式提供 `shared_credit_config` 时加载 Ray 研究接入，空配置不加载研究包。

本包暂不是独立发行项目。将来拆仓时需迁移上述三个中性数据类型，或在新项目的适配层转换它们；
Job 注册、方法语义、模型所有者、资源反馈和普通查询执行继续由 SemLoom 维护。
无需为尚未启动的研究增加另一套 Session 或模型 owner。

## 测试与研究材料

研究测试集中在 [tests/research/multitask](../../../tests/research/multitask)，
含原协调器／SAOR／Ray 替身测试，以及从核心测试迁出的七项具体协调器接线用例。
另两项旧配方检查移到研究测试并读取现存归档；原断言保持，不恢复旧部署路径。
核心测试只检查协议和执行行为；研究接线测试单向复用核心 fixture。

```bash
PYTHONPATH=code python -m unittest discover -s code/tests/research -t code
PYTHONPATH=code python -m unittest tests.scheduling.test_scheduler tests.scheduling.test_incremental_session tests.architecture.test_architecture_boundaries
```

实验声明仍在 [experiments/saor](../../../../experiments/saor/README.md)，
历史读取与离线分析仍在 [src/experiments/saor](../../experiments/saor/README.md)，
共享服务的研究数据类型与离线指标仍在 [src/experiments/shared_vllm](../../experiments/shared_vllm/README.md)。
声明、结果、失败和 unknown 均保留原身份；迁移不恢复已退役的 HTTP／PG 启动命令，
也不表示完成真实 Ray、模型或性能验证。源码保持核对与 CPU 范围见
[整理记录](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#saor-multitask-isolation)。
