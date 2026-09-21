> 本文件是该次验证的运行前声明/实施规格原文，2026-09-21 从当时的主题计划（`docs/plans/data_organization_batching.md`）按“执行说明与测试材料放在一起”的原则移入。其中请求额度、停止条件和“本轮/下一步”均为当时记录；未使用的旧额度不构成今天继续运行的授权。

<a id="work-package-e"></a>
## 已完成工作包E：依赖与共享服务的执行验证（2026-09-14）

用户明确选择先完成E，再做M1/M2。本节接管实施优先级，下方设计假设与历史合同保留其研究作用。
范围为既有Filter→Map依赖、查询归属和共享Engine上的完整多查询执行：计算机会轮转与空闲计算容量共享，
存储按Job独立保留；验证2/4查询、同时/错峰到达、暂停恢复、慢消费、取消/失败隔离，以及累计查询数超过并发上限。
这不扩大到F、任意SQL语义组合、PG方法桥接或新的预测/公平算法；M1/M2在E完成后执行。

所有权：PG拥有输入合法性、关系依赖、结果序、query cancel和生命周期；QueryRegistry拥有可信Job/flow归属；
Job预算策略只声明存储保留和计算上限；SessionCapacity/Engine拥有唯一责任表、轮转选择、提交及权威终态回收。
取消通知不等于计算释放；已完成未消费结果仍占存储，不占已结算的计算容量。

源码依据：起点db81867e的`equal_share_job_budget`固定划分计算额度，已有`round_robin_flow`可在Job/flow间轮转；
`open_query_job`按flow数划分Job存储；PG `query-job`当前Map固定窗口1。既有依赖和关闭逻辑保留。
架构§8.7–8.8的语义所有权、共享外部执行与资源复用原则适用；复用公开自有接口，不访问公司材料。
知识库与baseline reference已有共享容量/公平机制及强对照；本轮是既有能力接入和工程验证，不宣称新颖性。

复用核对（源码）：`scheduling/core/session_capacity.py`的records已同时负责全局、Job和session占用；
`session_jobs.py`已有注册、静态存储分配和Job/flow轮转；`SessionEngine`已有未知终态保留与退休回收。
本轮仅装配新的计算上限策略，实际接纳、轮转、终态和回收继续走这些实现。
`submission_control/shared_credit.py`的`FairEndpointCreditCoordinator`与`runtime/shared_credit_ray.py`
已提供独立执行器间的端点共享额度；其增量接入只允许不记录历史事件的FIFO，并有退休清理测试。
当前一个gateway只有一个Engine，不叠加第二份端点账本；旧DRR/VTC/SAOR不自动获得PG增量路径资格。

实施与验收：

- 增加显式共享计算预算策略，保留现有equal-share默认参照；各Job计算上限可到服务全额，实际总量仍只由Core统一限制。
  共享模式启动前确认每Job存储能承载所声明并发；不动态借用他人存储、不创建第二份credit账本。
- 为可信query-job Map提供显式可选窗口配置，默认仍窗口1；沿用现有安全预读判断与总量留存。
  Filter→Map的关系依赖仍由PG执行，不能将不安全child推进扩大成投机求值；两条路径分别检查。
- 可复用的共享查询运行/审计记录query release、开始/结束/拒绝、独立PG→flow→Job映射、实际出站、结果关联、
  每Job与全局责任、选择机会、慢消费者和收尾。累计Job不限为并发上限，满额时按现有策略明确拒绝。
- 先用受控后端和确定性时序验证借用/恢复/轮转/未消费结果与未知终态；再用真实PG、受控HTTP覆盖依赖及多查询。
  2/4查询与暂停/失败后恢复均保留全部行、请求和清理证据；不以单个绿结果或只最终归零替代全过程检查。
- 回归覆盖原equal-share、单查询incremental-map、query-job Filter/Map、NULL/LIMIT/权限/取消与正常PG TAP。
  新真实模型验证须在实施和受控检查通过后给出独立有限单元清单、请求/时间上限；此前57次额度已结束。

当前状态：共享策略、PG窗口与受控集成已完成；Linux调度385/provider57/PG合同116、PG回归1与TAP2138通过。
共享查询163次受控HTTP验证25个Job结清；真实模型准备脚本另用63次受控请求通过。
以下真实模型范围已完成：[完整记录](../../../results/postgresql/query_sharing_e_20260914/README.md)。首次准备实际0次失败，获继续确认后63次真实请求/19个Job关联与回收通过；
Map逐字复述0/55是质量负结果。PG/模型/进程/GPU/ACL已清理；不将工程完成解释为质量或性能通过。

### E真实模型验证（2026-09-14，独立新额度）

用户明确允许调用真实模型。最多63次生成POST、单GPU，从模型文件核对到清理最多20分钟；
使用既有Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`、vLLM0.25.1、
BF16/TP1/context4096、FCFS、max-num-seqs128/max-num-batched-tokens8192、显存比例0.8，prefix cache和chunked prefill开启。
本轮无预热比较或cache reset，不作性能结论；63次是包括准备后全部实际生成请求的总上限。

- 输入为4个独立文本ID、各自内容`TRUE`，不使用公司数据。Map指令`Echo.`，Filter指令`The input is TRUE.`，
  指令从验证配置传入通用核验函数；模型/路径/容量由仓库外配置提供。
- 2个Map查询8次、4个Map查询16次、2个Filter→Map查询最多16次、3个暂停游标与1个活跃Map查询16次、
  7次顺序LIMIT 1共7次。预计19个Job；Filter若返回合法drop，则相应Map不调用，实际总数可能小于63。
- 服务C4/Job4/held32，输入和结果各32MiB；PG窗口4、total留存8MiB、单行暂存4MiB。
  单查询30秒，整个查询worker180秒；总20分钟内预留90秒清理。每个请求先记入既有持久账本，不退款。
- 复用PG逐行记录器、Filter/Map独立生产者关联、socket peer归属、Core提交/权威终态重放；
  记录全部原始输出和输入对应关系。模型答错记入质量结果，不能作为错配或调度性能结论。
- 首次非预期失败或上限到达立即停止，保留所有已完成/失败输出，不重试、不追加额度、不改变运行配置。
  收尾核对实际HTTP/服务日志/账本、Job与计算责任、PG/模型/网关进程、端口、GPU和临时ACL。
