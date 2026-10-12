# 三种原方法的原生执行接入

本轮在独立分支`codex/native-execution-adapters`整合原LOTUS、DuckDB AI与Sema的方法接点。
旧主及三个原子任务工作树保持只读，原始成功／失败与私有环境继续保留。
执行目标与范围见[方案](../../../plans/语义系统对照.md#native-executor-adapters)，
紧凑事实见[verification.json](verification.json)。本轮软件调用只访问localhost响应fixture；
单GPU短验证另列，不把软件fixture当模型质量或性能结果。

## 方法与执行身份

| 方法 | Daft | Ray Data | IMLane CPython／作者Ray |
|---|---|---|---|
| LOTUS | 原LM未缓存完整批次→Daft Native→原SDK／parser | 同方法，Ray Data HTTP Processor拥有执行图 | 同方法，DBEnd／作者Coordinator拥有执行，两个模式分别登记 |
| DuckDB AI | 当前SQL向量→Daft→原C++ FinishCompletion | 同一完整批次ABI与原解析 | `native3`明确登记`imlane`；两个作者模式使用同一ABI |
| Sema | 原SQL／请求池之后的有限HTTP快照 | 同一后置服务与原作者解析 | 保持原SQL／请求池，两个作者模式显式核验PID与二进制 |

各方法保留自己的提示、生成参数、解析、调用出现身份和完整响应。
Sema的HTTP序号只表示抵达服务的请求，不代表数据库源行；原池前就绪仍不可观测。
Ray Core静态actor拓扑继续作为`diagnostic_reference`单列；IMLane作者Ray使用作者执行器，名称与Ray Data分开。
配置与完整调用说明见[IMLane接口](../../../../code/integrations/sema/imlane_calls.md)，
共同单查询／常驻入口见[CLI说明](../../../../code/scripts/README.md#native-adapter-query)。
IMLane有效128行／32MiB输入，超额整体拒绝；原C++桥未提供聚合32MiB驻留输出保证。
作者IPC的Job ID仍为空，跨查询公平、独立取消、任意UDF异常及全部传输异常类型一致性尚未证明。

## 软件与构建事实

- 原LOTUS／网关交付`f9921475`：本机和服务器各45项检查通过，完整字节、重复头、压缩、状态、缓存及SDK回送按对应来源保留。
- 原DuckDB交付`f4de379a`／`5e95852d`：50项旧路径、19项转换与ABI检查，三个后端真实SQL代表用例；失败轮及首次输入错误修正保留。
- 作者IMLane构建交付`fdd6a227`：两个模式630行／132次Arrow调用，来源与未检查项见[构建记录](../../diagnostics/imlane_build_20261012/README.md)。
- 共同四条LOTUS路径、错误观察和混合owner检查先完成38次软件POST。
- Daft同driver反例可复现`Cannot set runner more than once`。中立初始化入口修复后，实际Daft物化及原LOTUS／Daft／Ray Data／SemLoom同driver组合通过，整组54次软件POST，模型0。
- 新公共IMLane联调两模式各完成LOTUS／DuckDB／Sema四行完整消费，并各完成两次LOTUS常驻查询。成功轮48次软件POST，43.808秒，模型／GPU／重试0。

IMLane两个模式均使用CPU72–79、声明2个作者执行器；空stdin探得settings执行器4、任务线程6，配置并发额度8。
实际各触及2个worker，Ray记录2个非空actor ID；Coordinator及独立Ray正常退出码0、未强制，已观测自有存活进程及共享内存0。
DuckDB只增加`imlane`名称选择，匹配二进制为`0.4.14-native3`，摘要见紧凑事实；固定1.5.4头文件下构建约34.851秒。
原作者构建、桥、kernel和旧DuckDB构建保持原件。

失败轮完整保留：首轮21次POST，Sema错误使快照结束未确认，后续复用正确被拒绝；
第二／三轮11／14次POST因检查程序重复使用保护文件名而停止，改为独立记录后成功轮48次。
累计94次软件POST，小于本次128上限。归档解包首次拒绝Ray绝对符号链接，保留原包并只恢复普通文件／目录，没有重跑fixture。
首次DuckDB构建前版本检查失败，核对旧build的固定头文件路径后在新build目录完成，原失败保留。
本机初轮127项只有一个旧错误文字期待失败，24项未启用；修正后受影响5项通过。修后完整127项通过，24项跳过。
固定稳定性`9bb6c415`与原生工作源码在独立副本组合，247项通过、29项跳过；
首轮仅测试辅助导入路径错误，原输出保留，修运行路径后完成全组。
同一组合源码再完成三方法／常驻的双mode32次真实软件POST，37.327秒，两个Coordinator／Ray正常退出且自有存活／共享内存0。
组合保留稳定性全部实现摘要，原生新增文件逐项匹配；本次相关软件累计126次，小于128上限。

## 单GPU短验证与剩余模型工作

固定较早源码下启动同一Qwen2.5-7B服务，原LOTUS、Daft、Ray Data各完成一条8行warmup，共24次真实POST。
SemLoom在HTTP之前的Daft重复初始化失败，测量查询0；该反例随后由软件修复核对，修后模型没有重跑。
已占用32个模型名额，实际POST24；启动期限到达后服务退出，无自有存活或GPU计算进程。
IMLane真实模型与六路径测量、多GPU／服务饱和仍待新的运行时间安排，不从warmup顺序与软件耗时给性能排名。

## 结束责任与原件恢复

正常复用要求完整响应、EOF和正常DBEnd退出。错误或提前停止消费后的未确认记录按Coordinator PID/start ticks、实例名称和work_root保留，
新对象不得跳过该记录继续提交；调用者核清并结束自己的Coordinator／Ray后才使用新身份。
通用HTTP端点的`remote_http_stopped`保持unavailable，consumer退出不能当作远端请求停止。
执行稳定性与多副本另在`codex/semloom-stability-and-replicas`，原生分支不重复实现这些修补；
固定两分支组合已完成上述回归，原生单独分支不重复携带稳定性修补。完整方法接通不自动证明PG载体或调度收益。

仓库外恢复根分别为`native-executor-20261012`、`native-executor-baselines-20261012`及接续任务的`evidence/`。
先按紧凑事实核source archive和binary摘要，再读取`source-identity.json`、query summaries、原始HTTP／worker trace、SQLite记账及所有失败轮。
IMLane新交付原件另保存在`imlane-call-evidence/`，接续整合原件在`evidence/imlane-entry01/`；配置、进程身份及大日志不进入Git。
