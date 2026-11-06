# 提交控制

本目录维护提交窗口、供给反馈及其公共数据。请求数、批数和存储字节各由实际 owner 负责；
策略消费已有声明和观察，不接管 vLLM 内部 token／KV 调度。

| 文件 | 职责 |
|---|---|
| [feedback_options.py](feedback_options.py) | 请求／批反馈的配置类型；不加载控制器 |
| [feedback_work.py](feedback_work.py) | 共用供给状态及成对工作量计量；不分配资源 |
| [request_scan.py](request_scan.py) | 现代逐请求试探、测量和压力响应 |
| [stage_work.py](stage_work.py) | 显式批反馈的需求接合与批配置 |
| [adaptive.py](adaptive.py) | 现用批测量策略、几何候选和加性增加、乘性降低（AIMD）基类 |
| [resource_probe.py](resource_probe.py)、[resource_step.py](resource_step.py) | 解释来源、压力与试探许可；观察不等于物理内存分配 |

现代请求路径直接依赖公共配置与工作数据，不加载批策略。批反馈仍是独立显式选项。
旧指数平滑（EWMA）／队首等待（HOL）两种控制器及三个旧导出已退役，当前不提供旧位置转发；其他平滑统计和队首等待观察字段继续维护。
旧机制从[准确历史来源](../../../../experiments/reproduction/native_baselines/README.md#retired-admission-variants)恢复，
不将旧源码加入当前运行搜索路径。配置解析和测量说明见[配置总览](../../../../docs/执行配置与优化开关.md)。

The standalone PID admission prototype no longer has a maintained runner or configuration selector.
Its module and package exports are retired; the current request policy and explicit batch feedback remain.
[PID recovery](../../../../experiments/reproduction/native_baselines/README.md#retired-pid) preserves the exact implementation and original tests.
