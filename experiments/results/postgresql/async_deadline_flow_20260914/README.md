# 异步期限修订与 PG 输入、交付节奏诊断

状态：2026-09-14。实现 `0274b121` 已通过本地、Linux和正常PG构建回归，受控节奏诊断完成。
首轮真实模型在24次生成请求后因配置准备错误停止；用户随后批准剩余33次，修正配置后全部完成。
累计57次真实生成请求：56条正常结果及1条预期超时的临时结果。原失败和账本保留，未退款或重跑已完成单元。

## 目的与设置

本轮服务于 **PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**，对应
[当前执行计划](../../../plans/data_organization_batching.md#异步期限与输入交付节奏诊断2026-09-14)。
处理共同异步记录器的同步工作跨期限后仍可能报告成功的问题，并测量输入准备、实际提交、节点交付和客户端接收。
既有容量等待、零任务Map、普通PG发送缓冲验证不重复实施；不据短诊断选择默认调度策略。

- 源码从 `2b7824b7` 修订；源码与测试提交为 `0274b121`，服务器使用同一干净checkout。
  [最终身份](raw/local-final-source-files.json.gz)中的833份非Markdown代码/配置与本地逐份SHA一致。
- Linux/Python3.12.3、PG18.3；复用已安装driver，未安装依赖、下载模型或数据。
  [环境检查](raw/preflight.json.gz)使用 `core,text,semantic-benchmarks`。
- 正常扩展与 `SEMLOOM_FLOW_DIAGNOSTIC=1` 测试扩展安装到两个隔离prefix，均以 `-O2 -Werror` 编译。
  正常构建不含新增的4个GUC、字段、事件与ready-first分支；诊断构建包含它们。
  [构建身份](raw/build-profiles.json.gz)、[正常构建](raw/build-normal.log.gz)、[诊断构建](raw/build-probe.log.gz)。
- 真实模型使用已核验的 Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28`；
  vLLM0.25.1、BF16、TP1、context4096、max-num-seqs128、max-num-batched-tokens8192、显存比例0.8、FCFS、prefix cache与chunked prefill。
  [模型/源码核验](raw/real-run__identity-verified.json.gz)、[实际服务命令](raw/real-run__service-command.json.gz)、
  [启动身份](raw/real-run__vllm-identity.json.gz)、[参数读回](raw/real-run__service-ready.json.gz)。
- 模型请求原上限64、计划57次，单GPU，从身份核验到清理上限20分钟；首个非预期失败即停止，不重试或退款。
  每个单元前清空一次prefix cache并从服务日志确认，管理POST与生成POST分别计数。
  原始Movie输入按既有原始输入次序取前8行，不按标签筛选，标签不进入SQL输入或prompt。

## 期限修订与验证

`asyncio.timeout` 的取消回调需要event loop获得运行机会；同步输入准备、热迭代器或最后一次同步写入可能持续占用线程。
依据为受影响源码与 [Python 3.12 asyncio.Timeout](https://docs.python.org/3.12/library/asyncio-task.html#asyncio.Timeout)。
修订在进入行流后、记录行前后和EOF前检查同一个timeout对象的绝对期限。
发现过期后保留TimeoutError、已写结果和原有清理路径，不新增watchdog，不回填一个并未发生的准时取消。

execution摘要新增声明的event-loop期限、实际发现时点和发现方式。正常查询、外部取消、慢清理与首错处理维持原合同。
这保证已执行到检查点的过期工作不会被报为完成；它不能抢占尚未返回的同步阻塞代码，也不把慢清理计作准时取消。

| 范围 | 结果 | 证据 |
|---|---|---|
| 修复前期限复现 | 5项失败，缺失预期TimeoutError | [失败日志](raw/local-deadline-red.log.gz) |
| 最终本地定向 | 37项通过：期限、既有记录与流时序解析 | [日志](raw/local-local-focused.log.gz) |
| 本地实验工具 | 507项，491通过、16环境集成跳过；随后新增3项解析测试另行通过 | [日志](raw/local-local-experiments.log.gz) |
| Linux provider / PG Python合同 | 55 / 116项通过 | [provider](raw/execution_provider.log.gz)、[PG](raw/postgres.log.gz) |
| Linux实验工具 | 510项，494通过、16需显式环境的集成跳过 | [日志](raw/experiments.log.gz) |
| 正常PG扩展 | PGXS回归1/1、18个TAP文件共2138项通过 | [日志](raw/normal-installcheck-v2.log.gz) |
| 正常PG/direct旧入口 | 3项通过，34次fixture HTTP；保留SQLSTATE、失败与部分结果 | [日志](raw/normal-legacy-v2.log.gz) |
| 诊断PG输入/交付对照 | 12个单元、96次fixture HTTP完成 | [日志](raw/flow-comparison-v2.log.gz)、[读回审计](raw/controlled-readback.json.gz) |

定向用例覆盖上下文进入、阻塞源、热迭代、EOF、末次阻塞写入、真实 `DirectMap.rows` 迭代控制流、协作式取消及慢清理。
这些定向反例均为受控无模型测试；另经下文一次真实模型结果加同步慢写入验证过期期限，仍不声称可以抢占任意阻塞代码。
表中套件有交叠，不相加作为独立用例总量；34和96只统计两项指定PG集成，完整TAP自行创建的fixture请求没有汇总进这两个数。

## 受控流时序：全部重复值

同一诊断构建比较现行补输入优先和测试用ready-result-first，C4/L4、PG留存8MiB、暂存4MiB；
组织候选4行、组最多4行、active work2048。fixture context512，第0个HTTP请求延迟120ms，其余2ms。
输入pull在无暂停、index3暂停80ms、index4暂停80ms三种条件下交错各跑两次。每次8行，查询期限10秒。
暂停位于child调用前，是可控慢源注入；不能解释为真实磁盘I/O。

PG事件区分child返回、task准备、offer确认、结果就绪、node return与行释放；Core记录提交与权威终态，客户端记录实际收到。
使用同机 `CLOCK_MONOTONIC`，按独立PG before-offer/accepted绑定连接行ID与sequence，不从完成结果推断输入关联。
新增离线读回在原始Core终态上补充各段耗时，未重跑查询或改变记录。

下表单位均为 **ms**；首行与JCT从应用release计算。输入→提交为首次child返回到首次Core提交。
“行1可交付→节点”是 `node_return[1] - max(result_ready[1], node_return[0])`，区分输入序的队首等待与之后额外等待。
完整逐行时间、终态→PG就绪和节点→客户端时间见[读回文件](raw/controlled-readback.json.gz)；
基础公式见[分析实现](../../../../code/src/experiments/postgresql/flow_timing.py)和[离线脚本](raw/readback-audit-v2.py.gz)。

| 顺序 | 暂停index | 次序 | 输入→提交 | 节点首行 | 客户端首行 | JCT | 行1可交付→节点 |
|---|---:|---|---:|---:|---:|---:|---:|
| 1 | 无 | 现行 | 25.355 | 265.926 | 294.845 | 295.032 | 2.497 |
| 2 | 无 | ready-first | 18.510 | 248.076 | 275.034 | 275.202 | 0.057 |
| 3 | 3 | 现行 | 101.333 | 308.365 | 335.468 | 335.599 | 1.861 |
| 4 | 3 | ready-first | 104.296 | 331.222 | 361.111 | 361.472 | 0.062 |
| 5 | 4 | 现行 | 20.707 | 226.458 | 332.709 | 332.825 | 82.093 |
| 6 | 4 | ready-first | 20.702 | 231.837 | 342.540 | 342.673 | 0.038 |
| 7 | 无 | ready-first | 28.743 | 243.599 | 268.368 | 268.529 | 0.057 |
| 8 | 无 | 现行 | 19.027 | 251.378 | 280.240 | 280.377 | 2.683 |
| 9 | 3 | ready-first | 100.539 | 308.085 | 332.940 | 333.127 | 0.029 |
| 10 | 3 | 现行 | 100.296 | 310.550 | 337.605 | 337.886 | 1.918 |
| 11 | 4 | ready-first | 18.560 | 242.608 | 354.743 | 354.901 | 0.026 |
| 12 | 4 | 现行 | 23.812 | 230.995 | 337.838 | 338.182 | 82.692 |

事实与解释：

- 12次首次候选均为4行，首次PG receive/poll到Core首次提交为0.648–0.943ms。
  index3的80ms等待发生在第一次poll之前，输入→提交约100–104ms；无暂停约19–29ms。
  这直接表明当前首次派发依赖poll，准备候选期间模型尚未开始。
- index4两次现行策略在暂停开始时均有就绪队首（行1）；其可交付后仍等82.093/82.692ms才返回节点。
  ready-first两次先返回前四行，再执行index4暂停，行1额外等待降到0.038/0.026ms。
  这是已被注入反例区分出的控制流现象，不是正确性、丢行或整组完成屏障错误。
- 相同index4条件下，ready-first的客户端首行反而较晚：342.540/354.743ms，现行为332.709/337.838ms。
  提前的节点结果没有提前被客户端看到，节点→客户端等待仍存在。没有抓包，不能把全部差额精确归给某一个缓冲层。
- 单元结果、独立绑定、HTTP占用、完整组织成员和实际组内提交顺序均通过重放；Core计算责任与PG留存最终归零。
  测试构建增加字段和事件开销，两个策略臂使用同一构建；不能将其耗时与正常无追踪构建拼成性能排名。

## 真实模型首轮：已完成部分与停止原因

[完整运行结果](raw/real-run__campaign-result.json.gz)、[逐单元读回](raw/real-readback.json.gz)。
计划8个单元，完成前三个；第4个在创建组织配置对象时被拒绝，尚未启动gateway或发出模型请求。

| 单元 | 构建 / 路径 | 生成POST | 查询JCT | 质量与关联 |
|---|---|---:|---:|---|
| warm | 正常PG Map | 8 | 0.328637 s | 6/8分类正确，2个假阴性，无无效输出，绑定通过 |
| pg | 正常PG Map | 8 | 0.284169 s | 同上 |
| direct | PG-source direct | 8 | 0.275728 s | 同上，逐行关联通过 |
| flow0-current | 诊断PG，未执行查询 | 0 | 不可用 | 配置校验失败 |
| 其余3个flow与deadline | 未启动 | 0 | 不可用 | pending |

三个已完成单元的8条输出逐条相同，包括相同的两条误判；8条样本仅反映本次功能检查，不能推出整体质量水平或PG/direct性能差异。
PG两次峰值留存4行、计费403,360B，最终留存与暂存责任检查通过；HTTP峰值4、最终0，Core逻辑责任归零。
直接路径HTTP同样峰值4、最终0；其资源表示与PG不同，不对两个路径RSS作同口径优劣比较。

失败原因是本轮一次性准备脚本把fixture的 `active_work=2048` 复制到了 `context_tokens=4096` 的真实模型配置。
`MapOrganizationConfig` 明确要求active work至少能够接纳一个支持的完整请求，因此正确地拒绝该配置。
[原始配置](raw/real-proposal__organization.json.gz)、[失败日志](raw/real-run__flow0-current.log.gz)、
[失败摘要](raw/real-run__flow0-current__summary.json.gz)。这是准备遗漏，不修改生产检查来绕过。

原账本预留32次、实际24次，4个已预留单元全部关闭，其中失败单元8次预留没有退款。
模型服务独立访问日志同为24次生成POST；另外4次prefix-reset管理POST均已确认。
原计划中的末次真实写入跨5秒期限检查未运行，不能据此声称真实过期路径通过。

## 获批补跑：33次请求与真实期限检查

用户明确要求先完成剩余33次后，使用已准备并通过配置构造器/tokenizer检查的独立33次账本继续。
[新增授权](raw/continuation__continuation-proposal__authorization.json.gz)、
[环境复检](raw/continuation__continuation-preflight.json.gz)、
[补跑结果](raw/continuation__real-continuation__campaign-result.json.gz)、
[独立读回](raw/continuation__continuation-readback.json.gz)。
四个8行对照采用active work4096、context4096，其余条件和交错次序不变，未重跑首轮已完成的24次。

| 顺序 | 次序 | 生成POST | 节点首行 | 客户端首行 | JCT | 暂停开始时队首是否已就绪 |
|---|---|---:|---:|---:|---:|---|
| 1 | 现行 | 8 | 223.700 ms | 419.588 ms | 420.159 ms | 否 |
| 2 | ready-first | 8 | 189.742 ms | 383.579 ms | 383.771 ms | 否 |
| 3 | ready-first | 8 | 211.253 ms | 405.562 ms | 405.726 ms | 否 |
| 4 | 现行 | 8 | 216.627 ms | 406.127 ms | 406.358 ms | 否 |

四次8行输出与首轮正常PG逐条一致，同为6/8分类正确、2个假阴性、0个无效输出；每行请求、结果绑定和组织审计通过。
实际peak active work812，低于4096；Core work/request最终归零，HTTP峰值4、最终0。
PG峰值留存4行、计费403,424B，最终留存/待接纳行及字节归零。

**真实输入没有复现fixture中的就绪队首阻挡。** 四次index4暂停开始时队首都是未就绪的行1，
因此这组真实诊断没有区分ready-first专门针对的分支，不能将两次较短JCT归因为该策略收益。
它确认了两个测试次序在真实模型下的功能、关联与资源行为；不提供稳定排名，也不修改正常构建默认次序。

最后1次使用真实PG-source direct请求，结果在应用release后约135.130ms收到，再令最后一次同步写入持续跨过5秒绝对期限。
记录器以 `absolute_check` 在期限后 **51.928ms** 发现过期，实际取消触发时点不回填；
execution仍为 `failed`，1条69B结果与SHA完整保留、标为临时结果，评分器拒绝执行。
独立审计重新核对该行原始输入、出站请求与完成关联，HTTP1起1终、最终0；没有对失败结果评分。
该单元观测到查询终止5.051682s，记录全过程5.054527s，慢写入占4.916414s。
[期限证据](raw/continuation__real-continuation__deadline__q0__execution.json.gz)、
[预期错误验收](raw/continuation__real-continuation__deadline__expected-timeout.json.gz)。
这是真实响应加受控写入阻塞的功能检查，不能描述成模型推理耗时5秒或正常成功查询。

补跑账本与模型访问日志独立确认 **33次生成POST**，另有5次已确认prefix-reset管理POST；5个预留单元全部关闭。
两段累计57次生成、9次管理请求，累计预留65次但实际57次：首轮失败的8次预留仍保留，未退款。
补跑从身份核验至清理228.211秒，低于新批准的20分钟。
[最终清理及833份源码一致性](raw/continuation__continuation-final-check.json.gz)确认模型/PG/任务/GPU无残留、端口关闭；ACL恢复。

## 保留的失败与清理

1. 首轮正常构建TAP从孤立扩展副本启动，部分测试通过 `FindBin` 寻找仓库scripts/PYTHONPATH时失败。
   保存[首轮日志](raw/normal-installcheck.log.gz)和[失败及清理记录](raw/controlled.json.gz)，没有改动生产语义或断言。
   随后复制同版本完整code目录，在新PGDATA/输出目录重新执行正常构建测试，2138项全部通过；原失败文件保留。
2. 初版离线读回脚本误写fixture模型名为 `model`，请求身份检查拒绝。第二版从已保存model配置读取真实fixture身份后，
   12个单元重新审计通过；这只是离线重放，无HTTP、无重跑模型。[原脚本](raw/readback-audit.py.gz)、[修订脚本](raw/readback-audit-v2.py.gz)。
3. 真实模型配置失败后立即停止，未用剩余额度运行其他单元。全段含准备和清理207.535秒，低于20分钟。
   模型监督进程没有执行SIGKILL；vLLM内部退出日志报告清理一个剩余进程及一个semaphore警告，这些原文保留，不能表述为所有内部退出均无警告。

[受控阶段清理](raw/controlled-v2.json.gz)、[模型阶段清理](raw/real-run__campaign-result.json.gz)、
[最终进程检查](raw/local-final-process-check.json.gz)：PG pid文件、任务进程和GPU计算进程均无残留，
本轮端口关闭，临时ACL已逐字恢复。原数据、模型输出、失败与PGDATA保留在私有数据盘目录。

## 对课题的含义与后续研究

期限正确性修订和受控流时序证据已具备；现行补窗口优先可能延后就绪结果，ready-first又可能改变后续供给。
本次不修改正常PG的默认次序，也不提出新的调度层。后续应以相同资源、完整与低扰动观测、充足重复比较供给与交付取舍，
再研究请求计数FIFO、token表征FIFO和简单重排，强静态容量与原生系统真实质量仍独立待完成。

配置修正已在单独[补跑方案](raw/continuation-proposal__schedule.json.gz)中准备，原结果与账本保持原值。
active work改为4096，context仍4096；本批输入完整token work为193/219/195/179/210/212/206/184，
四个最大请求合计847，因此4096在该输入上不限制提交。模型、输入、C4/L4、80ms暂停和两策略交错次序保持不变。
[离线预检](raw/continuation-proposal__pre-model-validation.json.gz)已调用实际配置构造器、核验tokenizer并检查完整消息。

原补跑方案范围：仅剩余4个8行flow单元和1个deadline单元，**33次生成POST、单GPU、准备至清理20分钟**，另有5次cache-reset管理POST。
使用独立33次账本，保留原账本32次预留/24次实际；两段累计预定57次实际生成请求，不重跑已完成的24次、不退款、不放宽生产校验。
首个非预期失败仍停止。此前[未批准的方案快照](raw/continuation-proposal__authorization.json.gz)继续保留；
用户随后明确批准，新增执行/授权/结果作为独立文件保存，完成情况见上节。
本轮执行修订验证已完成；后续设计假设与强对照由[当前研究计划](../../../plans/data_organization_batching.md)维护，不能据补跑自动开启新实验。

## 证据投影说明

[清单](raw/manifest.json)登记成功、失败、配置、运行/分析脚本、逐行时间、资源与质量摘要的原始和公开SHA。
公开文件已经脱敏，评论、参考标签、prompt和模型原文不公开，以摘要或hash替代；结果文件的原始SHA对应私有原件，
不对应转换后的公开投影。公开时序可复算，完整语义重放仍需保留的私有原始输入与事件。
一次性脚本的绝对路径转换为明确占位符，表示实际执行材料的脱敏副本，不声称无需配置即可执行。
