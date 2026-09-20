# 工作包 F：PostgreSQL 图像类型与阶段执行

状态：**工程代码与受控验证完成，真实 CLIP 验证尚未执行**。研究对象为 PostgreSQL 内置 AI 语义算子的
外部分布式物理执行与调度优化。本轮新增真实模型调用 **0 次**，Ray 测试实例声明 **0 个 GPU 资源**。
计划见[工作包 F](../../../plans/data_organization_batching.md#work-package-f)。

下文用 PG 指 PostgreSQL。CPU 负责图像准备，GPU 是模型计划使用的加速设备；
MethodDriver 是方法行驱动，stage broker 是阶段资源管理器。

## 实现与职责

扩展 `0.3.0` 增加 `ai_semantic.embed(bytea,jsonb)→real[]`，提供从 `0.2.0` 升级的 SQL。
默认安装仍为 `0.2.0`，图像须显式选择 `image-reference` 或 `image-staged`。
图像是 Map 的关系行为，复用既有 CustomScan、ordinary child、窗口、权限、快照和事务处理。
没有新建调度器，也没有复制文本 gateway 的连接循环。

| 对象 | 所有者与完成条件 |
|---|---|
| SQL、输入与结果类型 | PG schema 5 保存模型/processor ID 与 revision、dtype、输入尺寸、输出维数、projection/L2、NULL、异常和输入顺序 |
| 连接与查询 | 既有共享 gateway 和独立 Job（查询作业）；图像首版一条连接对应一个图像算子，尚未接入 query-job 多算子流 |
| 每行方法与最终向量 | `MethodDriver`、`MethodBudgetPool` 保留行身份与固定数据预留，最终向量发送后才释放 |
| 任务与后端结果 | 既有 `SessionEngine` 负责接纳、Job 轮转、资源登记与终态；Core 结果交给方法后释放自身结果槽 |
| 编码图像与准备后张量 | `BoundedStageBroker` 在 CPU 准备前预留张量空间；准备完成后只将小型描述符交回 driver，张量保留为 Ray ObjectRef（对象引用） |
| CPU 与模型计算 | 一次语义图像请求包含 CPU prepare 和 model 两段物理执行；共享 actor 由服务拥有，任务无重试，actor 无重启 |
| 取消与未知状态 | 取消请求不释放计算；Ray 异常后等待同一 actor 的后续确认，GPU worker 还执行 CUDA synchronize，等待设备工作结束。无法确认则保留占用 |

wire v7 对图像单独编码，校验语义、执行、逐行输入和完成摘要；不改变文本 v2–v6 的消息定义。
同步 PG drive 与增量 offer/receive 共用图像语义。独立 `SynchronousImageReference` 直接运行相同
prepare/model 内核，供数值核对；它本身不调用 Ray 或调度器。服务装配仍使用统一的查询资源管理。

支持单个可见图像调用、单表 SELECT、普通谓词与投影、受限 INSERT ... SELECT、正向执行及 prepared plan。
NULL 返回 NULL 且不创建任务；LIMIT 0 零任务，可能预读额外错误的形状保持窗口 1。
图像接在语义 Filter 之后、join、重扫、任意 PG 方法程序和图像分类尚未实现。

## 输入与资源设置

输入为单帧 JPEG/PNG，最多 262,144 编码字节、16,777,216 解码像素；尺寸在像素解码前检查。
输出为一维、下标从 1 开始、无 NULL 元素的有限 float4 数组，维数为 1–4096。不会静默截断图像。
真实模型与 processor 只从指定 revision 的本地缓存加载；不自动下载，也不把一个本地目录冒充带 revision 的仓库。

本轮 fixture 为 2×2 RGB PNG，模型明确命名 `fixture/rgb-projection`，processor 为 `fixture/pillow-rgb`。
它用三个通道均值构造并归一化三维向量，不是 CLIP。reference 与 staged 对红/绿/蓝及重复输入逐行一致。

| 资源域 | 本轮设置 |
|---|---|
| 共享查询与方法行 | 最多 2 个 Job、全局 4 行；每行方法预留 525,082 字节，共 2,100,328 字节 |
| Core 输入/结果 | 全局输入 1,048,576 字节，结果 48 字节；staged 最多 2 个图像请求，reference 最多 1 个 |
| 阶段空间 | 编码 524,288 字节，准备后张量 96 字节、工作量 8 pixels |
| 阶段计算 | CPU prepare 最多 1 项，model 最多 1 项；实际均使用 Ray CPU actor |
| PG 留存 | staged 启用既有 total 计费，复用 8 MiB 留存与独立暂存、结果预留及顺序恢复；reference 逐项返回 |
| 验证环境 | PostgreSQL 18.3、Ray 2.56.1、Pillow 12.2.0、NumPy 1.26.4；独立 PG 前缀、数据目录与 Ray 实例 |

Core 的 active request 包含图像准备与等待，不能当成纯 GPU 在途计数。方法数据、Core 输入/结果和
阶段张量属于不同责任域。解码暂存还受像素上限与 CPU actor 数量控制；这些逻辑字节不等于 RSS（进程常驻内存），
本轮没有测量真实 CLIP 内存峰值或吞吐。

## 验证结果

| 检查 | 结果 |
|---|---|
| PG18.3 编译 | `-O2 -Werror` 通过 |
| PG 回归 | 1/1 通过 |
| PG TAP | 19 文件、2,159 项通过；其中新增 21 项检查图像安装/升级、类型、摘要、prepared plan 和零任务行为 |
| PG＋实际 Ray＋真实解码 | 最终 9 组通过，耗时 30.645 秒；模型为 CPU fixture |
| Python provider | 本地/服务器各 76 项；分别跳过 11/9 项，服务器跳过的 9 项即单独执行的 PG/Ray 集成 |
| Python 图像 | 本地/服务器各 81 项通过 |
| Python PG | 本地/服务器各 116 项通过 |
| 方法与 stage broker | 本地/服务器各 18 项通过 |

九组集成检查覆盖：

- 同步/阶段输出、C/Python 摘要、一维数组、NULL、重复输入与输入顺序。
- LIMIT 0、LIMIT 1 后的非法图像不被提前求值，以及超大编码输入与非法 options 在提交前失败。
- INSERT 正常结果、ROLLBACK、解码失败后的事务回滚及旧 recording Map。
- 列/函数权限、RLS 不可见图像不出站、repeatable-read 快照与 prepared plan。
- 6 次累计查询超过 2 个活跃 Job 名额，查询与方法预留正常复用。
- PG 取消正在运行的 Ray 模型 fixture 后，计算与张量仍保持占用；允许远端结束后释放，共享 worker 继续服务下一查询。
- 7 类完成帧篡改：行号、语义摘要、完成摘要、维数、非有限 float4、额外字段及重复字段均被拒绝。

另有实际 socket 受控检查证明：发送被阻塞时，Core 已释放该结果槽，方法行仍完整计费，其他 Job 可完成。
本轮最后 9 个 gateway 共登记 17 个 Job；8 个有任务的 gateway 均观察到方法使用量和分配量归零，
另 1 个是零 Job 检查，没有创建 MethodDriver。全部 Core 最终占用归零。
[汇总](raw/validation-summary.json)、[原始集成日志](raw/integration-05.log)与[PG 回归日志](raw/pg-regression-02.log)保留具体结果。

## 失败、修订与停止

本轮仅使用临时 fixture，按现有授权修复并重跑受影响检查；没有使用真实模型额度。

- 首次 Ray 启动因 Unix socket 路径过长失败，0 项测试；改为短的数据盘路径，保留原日志。
- 随后的 5 组集成有 1 项测试预期错误：旧 recording Map 返回 `recorded:old`，而非原文 `old`；纠正预期。
- MethodDriver 接入后的本地收尾检查发现服务停止与正常 EOF 竞争；仅对仍可推进的会话执行正常结束。
- 服务器验证发现方法已经关闭会话后，网关再次调用 `fail` 会中断共享服务。终态不再重复标记失败；
  新增“一个图像方法失败、其他查询仍可执行”的实际 socket 检查，最终九组集成通过。
- 本地早期检查还纠正了 fixture 的准备时间戳，以及新增 wire 文件所属的传输模块清单。
- 离线汇总最初错误地要求零 Job 服务也有方法事件；单列“未创建方法”，没有再次运行查询。

公开日志已脱敏，并清理构建日志的行尾空格；私有原始日志保持原样。
原失败日志与早期源码快照继续保留。中间版本曾使用临时 SQL 名称，最终统一为原计划的 `ai_semantic.embed`；
最终 C 实现、升级检查与集成都使用该入口。

PG、Ray、任务进程已停止，临时根目录 ACL 已恢复；没有本任务或 Ray 活跃进程，两个 GPU 均为
1 MiB/0%。[清理记录](raw/cleanup.json)和[源文件摘要](raw/source-files.json)可复核，原始私有运行目录保留。

## 来源与尚未验证项

工程参照沿用[主架构 §8.7–8.8](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md#pgml-engineering-reference)
已核对的类型入口、共享模型准备和查询独立清理职责，没有访问或复制公司源码。Ray 的
[取消说明](https://docs.ray.io/en/latest/ray-core/api/doc/ray.cancel.html)与
[actor 故障说明](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)用于核对请求与终态的区别；
实际行为另在上述 Ray 2.56.1 环境验证。这是工程实现与受控证据，不作为新算法贡献。

真实 CLIP 数值误差、真实 GPU 正常/故障清理、吞吐、模型算力利用率（MFU）、跨机器及原生系统比较均未验证。
下一步真实运行须先核对缓存模型/processor、软件与服务配置，按 dtype 写明数值容差，并另行确定
运行资源、调用额度与停止条件。M1 的真实模型暂停要求继续有效。
