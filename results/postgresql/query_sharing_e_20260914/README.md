# 查询依赖与共享计算验证

状态：2026-09-14。工作包E实现、受控与真实模型执行/关联/资源检查完成。首次真实准备失败实际0次，获确认后63次全部完成。
真实Map逐字复述质量为0/55，保持为质量负结果，不纳入质量通过结论。
受众：内部实现与验证记录。[实施合同](../../../docs/plans/data_organization_batching.md#work-package-e)。

## 目的与设置

服务于 **PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。
完成现有Filter→Map依赖、查询归属与共享Engine的接入：就绪Job共享计算机会，存储份额独立保留。
不扩大SQL组合、方法桥接、图像路径或多Job token组织，不将工程接入称作新的公平算法。

源码起点`db81867e`；运行实现`90d08843`，扩展检查`6b740bdb`，通用Filter核验迁移`c1e8e92f`。
Linux服务器使用干净checkout；正常PG18.3扩展以`-O2 -Werror`构建，不启用flow诊断编译。
复用已安装Python3.12.3 driver与PG18.3软件，没有安装依赖或下载数据。

## 复用与变更

| 对象 | 原有能力与本次落点 |
|---|---|
| 全局计算与存储责任 | 继续由SessionCapacity的同一records表持有，不建立第二份credit账本 |
| Job/flow选择与回收 | 复用JobRegistry、round_robin_flow、SessionEngine终态和退休回收 |
| 共享计算策略 | 新增shared_compute_job_budget，仅调整计算上限；存储仍按最大并发Job数划分，启动前检查每Job能承载完整请求容量 |
| 跨执行器端点额度 | FairEndpointCreditCoordinator及Ray桥保留；当前一个gateway只有一个Engine，不额外装配重复账本。旧DRR/VTC/SAOR没有因此获得PG增量路径资格 |
| PG查询窗口 | query-job Map默认窗口1；管理员显式开启enable_query_job_window后使用provider_window_tasks，仍执行既有child安全预读判断 |
| Filter输入核验 | 从Movie查询模块迁到同目录filter_bindings.py，与map_bindings.py并列；指令由调用方传入，Movie入口保留原默认调用兼容 |

CLI通过`--job-compute-policy shared`选择共享计算，默认仍为`equal-share`。
共享模式的Job计算上限可到服务全额；每次实际提交仍检查全局/Job/session三层容量。
就绪Job按请求机会轮转，不保证token、时间、GPU服务量或查询完成时间公平。

## 受控设计与结果

真实PG与生产socket网关对接受控本机HTTP。服务C4/Job4/held32，输入和结果各32MiB，连接16，frame期限500ms。
PG query-job窗口4，total留存8MiB、单行暂存4MiB。8行合成文本交替KEEP/DROP；Filter按标记返回TRUE/FALSE，Map原样返回。
生成请求上限256、专项120秒；没有真实模型调用，也没有对照性能排名。

| 检查 | 实际结果 |
|---|---|
| 本地核心与网关新增行为 | 9项核心方法、2项socket/CLI方法通过；覆盖2/4登记Job、慢消费、未知终态、累计退休与W先于C限制 |
| Linux回归 | 调度385、网关57、PG Python合同116全部通过 |
| 正常PG回归 | regression 1/1；18份TAP文件共2138项通过 |
| 2/4同时Map、2错峰Map、4同时Filter→Map | 112次受控HTTP；逐查询结果与依赖输入匹配 |
| 3暂停游标、1活跃查询、满额拒绝及恢复 | 32次；3个Job暂停时活跃Job可占4个槽，第5个查询拒绝；暂停超过frame期限后恢复 |
| 取消与其他查询继续 | 12次；取消查询的4次已提交工作保留至权威终态，另一查询完成 |
| 累计Job数超过并发上限 | 7次LIMIT 1均完成；全专项25个Job登记并结清 |
| 请求与结果 | 163次提交/163次终态，127条Map与32条Filter完成；取消项4次HTTP结算但不交付Map结果 |
| 全局峰值 | held16/上限32；输入3056B/32MiB；结果预留16MiB/32MiB；请求4/4，request-count work4/4 |
| PG留存 | 15个完整记录查询（14完成、1取消）中，11个Map峰值4行、4个组合Map峰值1行；所记录节点结束后行数/计费字节归零 |

请求原始字段按预定场景和现有versioned wire消息重新构造，与全部163次HTTP正文逐项计数核对。
使用PG before-offer与accepted记录确定行ID/序列，socket peer PID确定查询，flow登记确定Job；
Filter决定独立核验后再检查Map输入集合。14个完成查询共96条Map结果通过完整生产者关联；
其余暂停/顺序查询31条Map结果在各自执行处逐行核对。Core提交/终态集合与每个事件的计算占用一致。
独立复核确认838份非Markdown代码/配置SHA与本地一致。296份原始事件/配置/日志的脱敏投影及转换说明登记在[raw清单](raw/manifest.json)。

## 失败与修正记录

本地fixture的单项字节限额、socket结果释放时点和连接预留期望曾配置错误；修正测试设置后通过。
本地完整调度发现缺少pyarrow，未混装依赖；服务器已有依赖，385项全部执行通过。
首次PG共享专项在LIMIT 0前被现有文本ID追踪要求拒绝，0次HTTP；修正fixture ID类型后通过，未放宽生产检查。
首次专项同名控制器日志被第二轮覆盖；失败目录/摘要保留，首次错误保留为控制台转录，不能视为原文件。
离线复核脚本两次准备错误（Filter/Map消息形状混用、方法未调用）均在离线修正，未重复执行查询。

## 真实模型验证

用户随后批准新增真实模型检查：同一份可配置worker已先通过63次受控HTTP，真实额度独立为最多63次、单GPU、20分钟。
覆盖2/4查询、2个Filter→Map查询、3暂停查询与活跃邻居、7次顺序LIMIT 1；输入4行TRUE。
首次准备脚本将字符串传给要求Path的load_env_file，16.355907秒停止；模型未启动，实际POST为0。
原账本63次预留保持关闭，未退款；PG/进程/GPU/端口/ACL已清理。修正类型并增加启动前检查后通过，
用户明确确认继续后另建63次执行账本，旧账本仍保持关闭；两份账本合计126次预留、实际63次，未重置或退款。

修正后的执行用时195.917315秒，63次生成POST与模型服务日志、持久账本完全一致，19个Job登记/结清。
模型为Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`，vLLM0.25.1/BF16/TP1，
实际启动参数与上述实施合同相符；838份代码/配置和PG扩展哈希核对通过。

| 实际真实模型观察 | 结果 |
|---|---|
| Filter | 8/8返回TRUE，8行均进入对应Map；输入和PG决定逐项匹配 |
| Map | 55条完成结果全部与独立生产者序列及PG行ID匹配，返回结果SHA与落盘行数/字节数一致 |
| 逐字复述质量 | 0/55等于输入TRUE；54条为同一段中文解释，1条为另一段解释；全部finish_reason为stop，不做归一化或重跑 |
| 全局资源峰值 | held16/32、输入3024B/32MiB、结果预留16MiB/32MiB、请求4/4、request-count work4/4 |
| 每Job请求峰值 | 5个Job到4、5个到2、9个到1；请求机会轮转不等于服务时间公平 |
| PG计费留存 | 10个节点峰值4行、9个峰值1行；全部记录的行/字节责任结束后归零 |
| 收尾 | 19个Job结清；Core、transport、PG、模型、网关、端口、GPU与临时ACL均完成清理，无强制模型终止 |

独立读回复核再次从原始PG绑定、socket peer、请求消息、模型完成和逐行记录核对全部55条Map与8条Filter。
质量失败表明Echo.在本次模型/输入上没有得到逐字复述；没有单独实验定位原因，不能归因于共享调度。
本轮验证的是外部生成结果被正确执行、关联、交付和回收，不能说生成内容满足了该复述目标。

## 证据与合规自检

- [环境检查](raw/model-preflight.json.gz)、[PG构建身份](raw/build.json.gz)、[838文件身份](raw/local-source.json.gz)。
- [调度回归](raw/python-scheduling.log.gz)、[网关回归](raw/python-execution_provider.log.gz)、[PG合同](raw/python-postgres.log.gz)、[PG回归/TAP](raw/pg-installcheck.log.gz)。
- [共享专项](raw/shared-v2__shared__audit.json.gz)、[独立受控复核](raw/independent-audit-v3.json.gz)、[专项清理](raw/shared-v2.json.gz)。
- [真实运行与账本](raw/real-run-v2__campaign-result.json.gz)、[真实独立读回](raw/real-run-v2__readback-audit.json.gz)、[服务参数](raw/real-run-v2__service-command.json.gz)、[模型身份](raw/real-run-v2__identity.json.gz)。
- [第一次真实准备失败](raw/real-run__campaign-result.json.gz)、[控制台失败转录说明](raw/failure-captures.json.gz)。

本轮是SemLoom工程资格检查，无官方系统排名、统计重复或服务吞吐结论。
控制器计时包含模型核验、启动、请求与清理，195.917315秒不是查询JCT。
Core work采用request-count；PG计费行上下文与结果预留不等于进程RSS，峰值相加不代表同时驻留总量。
原始输入/请求/输出与SQLite账本保留在仓库外；公开raw为脱敏投影，正文/输出字段以哈希替代。
清单同时记录原件、公开投影与压缩文件SHA；不能用投影的字节校验值代替原件SHA。

## 结论与下一步

受控证据支持所测SQL路径的依赖、归属、计算容量共享、独立存储与生命周期回收。
本轮不提供稳态吞吐、延迟改善、token公平、全局最优或完整语义质量结论。
真实检查与清理已完成；E可按执行/资源范围合并。后续先做等待位置与全局元数据对照；本轮没有实施M1/M2/F。
本轮合成任务的质量负结果保留，不能充当后续方法质量实验的合格任务。
