# CLIP 实验职责

本目录装配已有图像来源、执行图和观测组件。数据从 PostgreSQL 读取后由所选外部框架执行；
数据库内 AI 语义算子的接入另按项目计划验证。

| 文件 | 职责 |
|---|---|
| [setup.py](setup.py) | 实验选项检查、源数据身份、预热输入及 CPU／阶段资源声明 |
| [execution.py](execution.py) | 框架初始化、工作进程、预热／查询切换、开始时钟与资源采样 |
| [reporting.py](reporting.py) | CSV 字段、派生指标、embedding 副本及 manifest 输出 |
| [命令入口](../../../scripts/experiments/run_image_clip_e2e.py) | 解析参数并按顺序调用上述实现 |

执行和报告使用同一份准备结果。报告中的类型引用只供类型工具读取，运行时没有反向导入执行模块。
`CODE_ROOT` 用于原 Git 身份及 Ray worker 的 Python 路径，不加载框架或模型。
原参数、对照角色、计时、字段和执行顺序保持，旧私有函数没有增加转发副本。

此次整理未改变异常收尾流程；相关问题与后续验证单独记录。
准确源码、前后输出比较及现有测试见[图像职责迁移记录](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#image-clip-module-owners)。
