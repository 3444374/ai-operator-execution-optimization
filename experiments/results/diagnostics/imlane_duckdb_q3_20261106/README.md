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
这只证明shared library构建，尚不证明薄driver、实际SQL、alias、Arrow及完整资源生命周期已通过。

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
本地Arrow24检查不替代作者Arrow16.1和真正DuckDB／DBEnd；真实C++编译及CPU运行在下一步单列。
真实模型provider仍未绑定，当前worker只允许明确的软件fixture，不能把软件COUNT14当作模型结果。

## 后续CPU与服务器测试

先用准确候选Git源码在现有shared library上编译driver，再在持有host租约、CPU资源supervisor和空闲7086端口时，
分别以新的coordinator／executor／namespace验证PROCESS与THREAD。
保持作者默认4 executors／原DuckDB线程数，只有显式选择的诊断才覆盖；不注入OMP/OpenBLAS单线程。
全量Movie2000→120、固定软件回复COUNT14及原评价、尾块／Unicode／重复业务ID、空结果和错误均需核对。
还须确认alias与实际DBEnd握手、同一份回执的完整性、CPP/CPython退出、SHM与外部物理清理。

本批不安排GPU。CPU闭环通过后，单列真实模型provider、共同Qwen7B消息／生成条件、作者配置依据、
逻辑120及实际attempt预算、cold／模型常驻边界和停止条件，再由现有服务器任务执行；不复制相同已测查询。
