> 本文件是该次验证的运行前声明/实施规格原文，2026-09-21 从当时的主题计划（`docs/plans/data_organization_batching.md`）按“执行说明与测试材料放在一起”的原则移入。其中请求额度、停止条件和“本轮/下一步”均为当时记录；未使用的旧额度不构成今天继续运行的授权。

### 工作包 C、D 的实施规格（2026-09-10继续）

当前授权为C的PG单算子留存预算和D的基础组织控制，完成代码、受控验证、文档及通过后的main集成。
模型运行继续按独立具体额度执行；不复用A或更早的余额，不推进E/F。

- C来源核对：基线`d9a27ac4`的`sem_pump.c`先物化/复制，再以B/L拒绝；provider另有按L分配的关联数组。
  PG18.3公开`execTuples.c:tts_virtual_materialize`按实际by-reference表示分配连续内存；
  `aset.c:AllocSetAlloc`有块/粒度开销。保留PG原生slot、TOAST与MemoryContext，不新增core allocator。
  主架构§8.7–8.8的PG生命周期、typed adapter、单项语义与复用原则继续适用；未访问公司私有副本。
- C采用显式可选的total模式；旧等份模式保留为参照。按实际保留行上下文计费，完整结果缓冲在接纳前
  预分配，完成时只填充，不再因待接纳行占用预算而无法接收结果。行数配置是上限，空间不足时减少实际留存数。
- 最多一行处于有界暂存：TOAST原始大小在解压前检查，slot物化/输入复制/消息和结果缓冲先计算保守分配上限，
  再分配并核对实际MemoryContext字节。暂存及转换的配置、峰值、固定关联元数据与协议收发暂存分别报告。
  B不是整个PG查询或普通child plan的work_mem；多算子由各节点分别给出配置与峰值，报告其总和。
- C反例覆盖同B、小L成功而大L旧模式拒绝；新模式异质行、乱序完成和已满窗口下一个待接纳行仍推进。
  超单项/暂存/单行总预算不兼容须在出站前明确失败；NULL、LIMIT0/1、易抛错表达式、取消和下一查询恢复保留。
- D优先复用`WorkWindowOrganizer`、typed `WorkDescriptor`、既有组织窗口和固定活跃work控制。
  三种控制为固定行数输入序、固定工作预算输入序、同有限窗口长度分组；不修改消息或拼接多个请求。
  同一比较固定源、候选揭示条件、ready/active/输入/结果容量，记录实际组织批次与出站顺序。
  token描述须由实际规范消息和固定tokenizer生成，单位及校准身份入配置，不能把request-count改名叫token。
- D验证先用受控异质work、实际PG与HTTP；确认输入/输出一一对应、窗口有限、未饥饿、取消和所有责任归零。
  原生Ray/LOTUS继续拥有自己的调度，不注入SemLoom组织器。真实组织比较在对应路径/参照有效且获新额度后执行。

C最小真实诊断（已单独授权并完成，见[报告](../../results/postgresql/pg_window_budget_20260910/README.md)）：最多288模型POST、单张4090，从模型准备起15分钟；无重试。
沿用已固定Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`与vLLM0.25.1，
BF16/TP1/context4096/max_num_seqs128/max_num_batched_tokens8192/GPU利用率配置0.8，FCFS、prefix cache与chunked prefill。
Movie-derived Map使用已导入原始源的前128个行出现；不按标签筛选，实际tokenizer检查最大输入131tokens，生成上限128。
两个模式各预热前16行，再各执行128行，共4单元；顺序为equal预热、total预热、equal验证、total验证。
每单元重置prefix cache并核对实际请求/关联/PG责任；C8、核心输入128MiB、结果64MiB、PG B1MiB、暂存4MiB，
equal为L2，total为L32。L与内存策略一起变化，只有功能/资源诊断身份，不比较算法性能。
任务输出格式无效、身份/关联/资源失败或达到请求/时间上限即停止；分类准确率和FP/FN如实记录，不据小样本登记质量或吞吐结论。
实际288/288 POST完成，包含模型SHA准备及清理共202.55秒；四单元有效，模型正常退出。PG及临时ACL暂留D受控验证。

D已完成代码和受控验证：复用组织器并增加有限候选前缀/纯行数组配置；完整规范消息经本地固定tokenizer
计算`prompt_tokens + max_tokens`，不改变出站输入。校准身份包含模型/服务版本、tokenizer SHA和context上限。
PG三组36行受控查询共108 HTTP通过，行数组/work输入序分别9/27组但出站顺序相同；长度分组改变顺序，
全部36行完成，最高活跃work484≤512。最终审计另核对候选确实来自当时已接纳且未提交的队列前缀。
Linux调度369、provider55、PG合同116、审计/文本6项及PG完整TAP2138项通过；原失败与取消恢复证据保留。

D独立真实诊断已单独授权：最多408 POST/单4090/模型准备起15分钟，首个有效性失败停止、不重试。
Movie原始前128行与C相同，每种控制先8行预热再128行验证，共6单元。C8/L32、PG B/S各4MiB、
核心输入/结果128/64MiB、候选32、组行数8、组work1024、活跃work4096三组相同，仅变`rows/work/length`。
固定Qwen2.5-7B-Instruct、vLLM0.25.1及C的服务参数，每单元清前缀缓存。完整消息47–131输入tokens，
生成上限128；逐行检查实际POST/token usage/关联、分组与出站、字节/活跃work及清理。
只登记真实功能/资源与组织行为，不将短查询差异解释成稳态性能；726份代码与模型准备副本一致。
实际408/408 POST全部完成、244.96秒；三组128行预测一致，长度分组改变实际HTTP顺序，输入序控制只改变组数。
模型/PG/任务进程已停止、临时ACL恢复；[完整组织报告与证据](../../results/postgresql/map_organization_20260910/README.md)。

零模型验证依次覆盖 Python、PG18.3 严格构建/回归/TAP、真实 PG+受控 HTTP、共享 gateway 和
Ray CPU 阶段。真实实验入口先准备完全匹配的 L32/64×结果预算32/64MiB、可达 C32/64/128
及查询/CLIP 最小清单；不自动调用。未找到平台不能阻止独立查询接入，也不能写成已经找到强 D0。

条件性研究项：完整响应预留须有可执行的最大响应依据；批量 IPC 须先观测协议开销；Core 索引须
先 profile。方法×执行 2×2 在一个公共 reference 任务质量可评价、所需方法接口具备后选择一种
已有方法，不等待全部算子。上述条件不授权恢复旧 SAOR/HSE formal 或修改 vLLM/Ray scheduler。
