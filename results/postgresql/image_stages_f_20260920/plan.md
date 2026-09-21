> 本文件是该次验证的运行前声明/实施规格原文，2026-09-21 从当时的主题计划（`docs/plans/data_organization_batching.md`）按“执行说明与测试材料放在一起”的原则移入。其中请求额度、停止条件和“本轮/下一步”均为当时记录；未使用的旧额度不构成今天继续运行的授权。

<a id="work-package-f"></a>
### 工作包 F 的工程实施记录（2026-09-20）

该次工程工作包只完成工程与受控验证，当时真实模型暂停；后续151次真实前向见本文顶部结果。
不继承文本实验请求额度，不启动旧图像性能矩阵。源码起点为 `2554f94b`。

**代码与受控验证已完成**：[完整记录](../../results/postgresql/image_stages_f_20260920/README.md)。
扩展 `0.3.0` 显式提供 `ai_semantic.embed(bytea,jsonb)→real[]`，默认安装仍为 `0.2.0`。
schema 5 / wire 7 分别保存图像语义和传输；`MethodDriver` 负责行与最终向量，Ray backend 负责两段物理执行。
PG 回归 1/1、19 个 TAP 文件共 2,159 项及 9 组 PG/Ray/真实解码检查通过；真实模型 0 次。
本轮模型由明确的 CPU fixture 代替，不能据此登记真实 CLIP 数值、GPU 故障恢复或性能通过。

- 数据库负责 encoded `bytea` 输入、版本化模型与处理器身份、输出固定维数 `real[]`、NULL、
  异常、顺序和查询结束。SQL marker 未被计划接管时仍报错；先建立逐项同步执行，再扩大已验证路径。
- 图像 adapter 复用 `ImageEmbeddingBatch/Result`、CLIP prepare/model 组件与
  `BoundedStageBroker`。CPU/GPU worker 由服务拥有；查询只拥有任务、数据和结果责任。
  编码字节、准备后张量、计算名额与未消费结果分别记账；broker 不无限保存已交付任务历史。
- Ray 保持零重试、零 actor 重启；不因一个查询取消而终止共享 actor。发送取消请求后继续保留
  名额与数据责任，只有远端方法完成证据才能释放；通信错误保持未确定状态。ObjectRef 放弃与
  远端终态分别记录，不把 Python future 结束当成 GPU 工作结束。
- 同步与阶段路径保持同一模型、处理器、projection、dtype、normalize 和输出维数。受控模型
  使用明确 fixture 身份；真实 CLIP 的数值容差、模型文件和运行额度留待单独核验。
- 验证覆盖重复输入、NULL 零任务、非法图像、错误维数/非有限 float4、容量拒绝、慢消费者、
  取消、迟到结果、其他查询继续执行、累计行数超过窗口，以及受影响的旧文本路径。

工程来源沿用主架构 §8.7–8.8 已核对的 PG 类型入口、共享调用与查询独立清理职责；本次没有
读取或复制公司材料。Ray 的[取消说明](https://docs.ray.io/en/latest/ray-core/api/doc/ray.cancel.html)
与[actor 故障说明](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html)用于核对取消请求
不等于计算结束，实际集成还须记录所用 Ray 版本。以上属于工程决定，不作为新调度算法结论。
