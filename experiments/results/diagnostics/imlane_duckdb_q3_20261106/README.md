# DuckDB＋作者IMLane原生Q3准备

本报告记录实际构建与CPU接线资格，不是模型质量或性能比较。
[接线与构建入口](../../../../code/integrations/imlane_duckdb/README.md)、
[机器摘要](preparation.json)和[准确恢复来源](source_recovery.json)分别维护接口、实测状态和来源。
完整私有构建记录按`imlane-duckdb-q3-prep-20261106-r1`至`r5`标识及摘要交接；旧失败不覆盖。

## 作者源码及构建

作者为`IM-DM4AI/IMLane0@072f8257db80569e42c9f5727d839587df529652`，现有patched DBEnd-C/C++前缀、
Python3.12.3／Arrow16.1.0保持。所有构建写在新数据盘目录，canonical源码／库均不改。
先使用现有core环境preflight，再以GCC11.4／CMake3.22.1／Ninja1.10、C++17编译shared DuckDB。
不安装软件、不加载模型；作者DuckDB无可核对release tag时版本如实为unknown，不填写论文0.10.1。

实际发现并保存了三个构建接线问题：作者CMake包直接放在`prefix/lib/cmake`，需显式`IMLane_DIR`；
作者根`build*`误忽略必要源；导出target没有提供Boost等SDK的传递头目录。
另有一次控制器误传`supervise(env=...)`失败，发生在CMake启动前，已修控制器并保留原件。

缺源恢复严格限于官方
[`duckdb/duckdb@5ebd8ca697099859239ba85e0de07dac7e1b7c04`](https://github.com/duckdb/duckdb/tree/5ebd8ca697099859239ba85e0de07dac7e1b7c04)
的optimizer cpp/header及mbedtls `build_info.h`三件。157个现存optimizer文件逐字匹配该提交，缺的恰为上述两件；
third_party的889件同源文件相同，仅缺该mbedtls头。各Git blob／SHA256及未恢复项由来源manifest给出。
整体src的共同相同2531、不同56、作者新增14、上游有而作者缺2；不能将整个fork声称为纯上游树，
也不能说56项差异全是prediction改动。这些已有差异全部保留，未启用的TPCH/TPCDS/Python/Actions漏项不借机补入。

恢复只写隔离副本。CMake仅将硬编码fmt前缀改为可传入的CACHE PATH；SDK头路径作为明确构建环境传入，
不改prediction算法、普通优化器调用、作者batch／scheduler／执行器实现。
`r5`增量编译返回0，已观察进程退出、资源回执通过、SHM新增残留为0；共享库44,524,536字节，
SHA256 `52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf`。
随后准确`798e13771263319234df574fa04fd3c483896c39`的薄driver已链接通过，实际SQL／DBEnd结果见下节。

## 当前接线与CPU边界

薄C++ driver使用作者四VARCHAR注册API，保留PROCESS／THREAD两种实际路径，拒绝空实现SCHEDULE；
普通源COUNT及EXPLAIN不执行AI，AI COUNT只执行一次。若意外落入本地C++回调便明确失败，不能偷偷替代DBEnd。
完整gold-free CSV保留2000来源发生项，原普通筛选留在DuckDB；UDF回执再核对实际只有120 eligible输入。

Arrow边界是`c0..c3`四列、同序、无NULL的单chunk int8返回；SQL比较`=1`，避开位图BOOL的已知转换风险。
Python UDF对每个作者batch只调用一次provider，不注入逐行串行、并发门槛或重试。
每批私密回执保留实际消息摘要、原答复、唯一发生项及调用状态；unknown不判成功，物理model attempts不推定。
driver结果在native析构前原子发布，进程退出和namespace资源仍需外层核对。

本地22个不同CPU方法最终通过：14个Arrow／provider合同和8个packet／离线审计／worker错误行为。
初轮两项macOS路径别名fixture断言失败，归一测试根路径后只补跑这两项，不叠加通过数。
本地Arrow24检查不替代作者Arrow16.1和真正DuckDB／DBEnd；实际服务器资格另列如下。
798e的原worker只允许明确的软件fixture，不能把软件COUNT14当作模型结果；后续模型接线另列如下。

## 实际作者CPU链路与资源关闭

`imlane-duckdb-q3-native-798e-20261106-r3`在host租约及资源supervisor内，各运行一次新的PROCESS与THREAD
coordinator／executor／namespace。[CPU机器证据](native_cpu.json)保存准确源码、完整EXPLAIN、评价及原件摘要。
两者均为完整Movie2000→120、一次AI COUNT、一次120行batch-provider、软件COUNT14；原评价零误差，
逐行TP14／TN106／FP0／FN0。注册alias真实完成，Arrow16.1的四VARCHAR→无NULL int8往返已验证。
这是金标软件回复的接线检查，不能替代真实模型质量或原Daft／Ray七个FN。

保持作者默认4 executors、默认DuckDB16线程、standard vector2048；CPU亲和性仍为显式分配的8核，
未扩宽亲和性、不注入OMP／OpenBLAS单线程。所选Filter实际用作者prediction size2048，
不消费注册API的batch参数，不能将该注册参数当成有效调优旋钮。
PROCESS／THREAD均正常worker exit0，外层未发TERM／KILL，已观察自有进程退出、SHM无新增残留；
最终host SHM增量0，资源回执通过。driver析构前发布的`process_exit_verified=false`保持，
退出证据来自外层supervisor，未将API结果重写成物理退出保证。

此前`native-r2`在启动阶段触发旧512MiB SHM预算，已停止并清理，原失败保留。
作者默认分配是4×128MiB Lane和8个Boost控制队列；实测每队列逻辑8592B、实际分配12288B，
总12个命名对象、536,969,216B＝512MiB＋96KiB。新CPU运行按此分配图声明预算，没有暗降executor数，
20GiB sampled-RSS单独计量，不将SHM容量当作RSS或模型内存。另有一次旧不完整Git对象集的共享clone失败，
发生在启动前；改为新仓准确SHA fetch后通过，失败日志保留。

完整cohort及120行尾批已实际通过。特殊Unicode／空字符串、重复业务ID、空选择和provider异常由本地合同覆盖；
这些额外边界尚未逐项经过真正作者二进制，不扩大本次资格范围。

## 独立模型provider与worker接线

[模型provider／服务owner／worker](../../../../code/integrations/imlane_duckdb/README.md#真实模型worker与独立http对照)
已实现，数据库／executor保留作者ArrowLane，只有UDF至一个外部模型服务使用loopback HTTP。
SemLoom主线不加入HTTP模型路径。这是作者原生API的项目工作负载绑定，不宣称作者Q7未公开的客户端实现。
HTTPX按同batch异步发出原消息，不加固定并发门槛／线程池、重定向或重试；每batch显式关闭client，错误后drain同批，
started缺terminal仍unknown。length原文本保留给原parser并标记截断；错误JSON／身份错／abort等保留后失败。

服务明确generation-config=vllm，避免repo生成默认改变共同SamplingParams；这是项目选择。
普通服务GET模型／实际上下文，未启用dev mode时其他resolved容量unknown，不靠调大8／4096来凑接通。
实际现有driver与Arrow16.1方法环境均具备HTTPX0.28.1，不安装或混装环境。
审计修正了两个真实边界：不再把完整自有大回执套用配置JSON默认8MiB限制；
UDF调用与HTTP批次用PID／调用时间唯一配对后逐position核对，重复消息交换回复不能躲在同COUNT／多重集中。

新薄driver在析构前给出CLOCK_MONOTONIC READY；模型worker原查询期限包含输入物化及启动，
结果后只有一个关闭窗口。模型owner期限随READY交接，sampler不抢先用观测延迟判wall超时。
作者close既有分段等待未改，硬截止须由外层supervised_run按READY绑定；非零服务退出、强杀或未知身份不当作正常关闭。
软件模式必须提供显式测试factory，普通CLI拒绝它；不因写了software配置而意外启动真实模型。

相关88个不同本地CPU方法全部通过，[源码／日志摘要](model_components.json)关联完整范围；
22个provider、22个owner、22个模型worker／审计、原8个fixture worker及14个Arrow内核方法不重复累计。
真实作者＋软件HTTP CPU闭环待本批下一步，当前没有加载模型或启动GPU。完整消息、8192生成条件、
cold／ready／关闭与下一次120逻辑请求的范围见同一校准计划，不将本地组件通过当作GPU资格。

## 真实模型仍需验证

本批不安排GPU。CPU闭环通过后，单列真实模型provider、共同Qwen7B消息／生成条件、作者配置依据、
逻辑120及实际attempt预算、cold／模型常驻边界和停止条件，再由现有服务器任务执行；不复制相同已测查询。
