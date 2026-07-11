# notes/AGENTS.md

本目录维护和导师、企业侧沟通相关材料。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 记录待确认问题、沟通话术和会议结论。
- 把外部反馈及时转化为项目规则、实验计划或方向边界。

## 沟通规则

对外优先表述为：

> 数据库内置 AI 算子的外部分布式数据处理执行链路优化。

不要直接说“我要做 Daft/Ray/Lance”，避免显得脱离数据库落地。

当前更稳妥的解释：

> 我们先把数据库触发 AI 算子后的外部批处理执行链路跑通并画像，再判断 batch、partition、task/actor、object、模型服务队列和写回哪个是主要瓶颈；Ray/Daft/Lance 类系统是候选承载链路和调优对象，不是预设结论。

## 必须持续确认的问题

- PostgreSQL 18.3 内部验证平台能力：安装、扩展、外部 worker、网络和权限。
- AI 算子接口形态：SQL UDF、表函数、外部执行器，还是批处理服务。
- 数据流转格式：Arrow、Parquet、Lance、IPC 或其他。
- 是否存在 join/groupby/repartition/embedding preprocessing。
- 达梦是否会使用 Ray/Daft/Lance 或类似外部执行系统。
- 为什么需要 Ray/Daft/Lance 类系统，而不是数据库内部线程池或普通服务。
- GPU / 模型服务资源何时可用。

## 更新规则

如果导师或企业侧改变课题边界，同步更新：

- 根目录 `AGENTS.md`
- `PROJECT_INDEX.md`
- `overview/current_direction_and_plan.md`
- 相关 `motivation/results/` 或 `validation/results/` 说明文件
