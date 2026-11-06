# 多任务共享服务与 SAOR

本包保存既有共享额度研究与 Stage-Aware Ordered Release（SAOR，按阶段状态排序释放工作）。
SemLoom 主线保留简单多查询运行，不需要导入本包：固定份额、普通轮转、查询注册、算子流（flow）、
统一资源记账与真实终态仍由执行核心处理。本次恢复基底已有逻辑，未新增研究算法或控制器。

## 现有职责

| 所属部分 | 文件 | 职责 |
|---|---|---|
| 主线基础多查询 | [session_jobs.py](../../scheduling/core/session_jobs.py)、[Session](../../scheduling/core/session.py) | `equal_share_job_budget` 按声明的最大 Job 数分配固定存储与执行额度；`round_robin_flow` 先轮转查询组，再轮转组内 flow |
| 可迁移的研究输入 | [interfaces.py](interfaces.py) | 调用方容量、请求、工作封装和阶段观测的只读字段接口（Protocol） |
| 可迁移的既有研究算法 | [shared_credit.py](shared_credit.py)、[credit_policies.py](credit_policies.py)、[saor.py](saor.py)、[ordered_release.py](ordered_release.py)、[saor_capacity.py](saor_capacity.py)、[shared_credit_ray.py](shared_credit_ray.py) | 共享额度、权重、优先级、债务、服务目标、按序释放和容量消融；不导入 SemLoom 值类，Ray 运行时由调用方传入 |
| 显式 SemLoom 研究适配 | [semloom/session.py](semloom/session.py)、[semloom/gateway.py](semloom/gateway.py) | 原共享计算上限选项与研究 CLI；使用主线工厂，不另建模型 owner 或普通执行循环 |
| 显式同步共享接入 | [semloom/scheduler.py](semloom/scheduler.py)、[配置](semloom/scheduler_types.py)、[候选窗口](semloom/ready_window.py) | 共享等待、候选注册与未提交候选清理；通过真实完成交接记账 |

固定预算的分母是已声明的 `max_jobs`，并非当前活跃数量；空闲份额不自动借出。
每个 Job 的存储、单查询多个 flow 的存储划分、服务总请求／工作上限都由原资源表校验，
不能向每个 Job 重复授予整份存储。取消或关闭查询不抹去未确认的远端工作。
单查询的主机驻留额度 R、当前请求窗口 C、反馈与准备仍按相应入口维护。

## 实际入口与适用范围

| 入口 | 当前范围 |
|---|---|
| `SessionEngine`／注册的查询组 | 默认固定预算与普通轮转可推进多个 Job；CPU fixture 覆盖独立完成、取消、晚到结果与 unknown 占用 |
| 固定模型增量 Map gateway | 主线 `--max-active-jobs` 大于一不再要求研究工厂；文本 socket fixture 使用默认工厂验证多个查询 |
| typed image gateway | 主线也有基础多 Job 默认；当前只有本地替身与方法／socket 检查，真实图像服务多查询待验证 |
| records resident | 原 owner 只接受一个 active query；模型常驻后的顺序查询复用不表示并发完整 LOTUS 查询 |
| 提前准备与 organization | `preparation_factory` 加多 Job 仍明确拒绝，`--organization-config` 仍限单 Job；没有删除这些保护 |
| 原生 Ray payload transport | 真实多查询组合未在本次执行；不能由 gateway 的 fixture 结果转授资格 |

`--job-compute-policy` 只在研究入口 `python -m src.research.multitask.semloom.gateway` 中解析。
普通主线 gateway 使用基础固定份额；研究入口可显式选择既有共享计算选项。

## 后续 SAOR 接入的位置

- `allocate_job` 返回 `JobBudget`，由执行核心校验并持有；当前基本授予在注册时确定，不是动态份额协议。
- `choose_flow` 消费只读的就绪 Job／flow 和选择记录，返回 `FlowChoice`；执行核心再次检查其成员身份。
- `credit` 跟随真实提交与完成交接。当前增量接入只接受 `incremental_safe` 的本地策略；
  现有协调器仅本地 FIFO 满足此项，不能直接把 SAOR 协调器传入就声称完成接入。
- `SubmissionLifecycle` 供同步研究适配在提交前和已验证完成后处理额度，不重复实现普通循环。

SAOR 的阶段观测与决策需在显式适配中转换成上述输入和动作，并分别验证资源、关联、取消及结果行为。
本次没有实现这项研究接入，也不新增动态跨查询控制器或多租户系统。
算法层八份 Python 文件可迁移；`semloom/` 仍依赖执行项目，不是独立运行的算法层。

## 测试与资料

基础多查询用例留在核心 [Session 测试](../../../tests/scheduling/test_multisession.py)
和 [provider 测试](../../../tests/execution_provider/test_multisession_gateway.py)。
研究测试在 [tests/research/multitask](../../../tests/research/multitask)，只为可选研究接线依赖本包。
独立进程禁止研究包与模型运行时导入后，12 项基础功能探针通过。
准确源码、全部本地检查、首次误选与服务器待测项见
[当前记录](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#basic-multquery-defaults)。

研究声明、离线分析和历史来源继续分别在
[experiments/saor](../../../../experiments/saor/README.md)、
[src/experiments/saor](../../experiments/saor/README.md)和
[src/experiments/shared_vllm](../../experiments/shared_vllm/README.md)维护。
原失败、负结果及暂停的 PG／HTTP 身份保留；没有恢复旧启动命令。
