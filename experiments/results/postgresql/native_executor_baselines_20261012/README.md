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

## 较早源码的单GPU失败轮

固定较早源码下启动同一Qwen2.5-7B服务，原LOTUS、Daft、Ray Data各完成一条8行warmup，共24次真实POST。
SemLoom在HTTP之前的Daft重复初始化失败，测量查询0；该反例随后由软件修复核对，该轮修后未重跑模型。
已占用32个模型名额，实际POST24；启动期限到达后服务退出，无自有存活或GPU计算进程。
该轮24次实际POST／32个占用名额独立保留，不归入下述新一轮，也不退还失败查询的名额。

<a id="lotus-six-model"></a>
## 六条LOTUS路径的单GPU真实模型检查

新独立授权轮`lotus-six-model01/attempt02`全部通过：原LOTUS、Daft、Ray Data、SemLoom、
IMLane CPython及IMLane作者Ray各执行一条8行预热和两条8行测量，合计18条查询／144次真实POST。
网关、SQLite请求记账和vLLM访问日志均为144，已结束记账单元18个；错误、超时及重试均为0。
本轮最多192名额，占用144，剩48未使用；独立软件检查额度64没有消耗。
此前24实际POST／32占用名额保留，因此两轮累计实际168、占用176，没有合并统计样本。
测试driver耗时76.203秒，supervisor覆盖driver启动及退出的区间79.361秒，第二次模型就绪48.099秒。

执行源码是本地组合提交`9c2fdefa`，父提交为原生`fd8c00bc`与稳定性`9bb6c415`；
生产文件与已回归组合逐项一致。使用同一物理RTX4090 GPU0、单服务、张量并行1、流水线并行1，
缓存Qwen2.5-7B-Instruct的全部权重／tokenizer文件逐项SHA256固定。
vLLM0.25.1、BF16、context4096、max-num-seqs16、batched-token2048、显存比例0.8，
eager执行，前缀缓存关闭。生成参数为16输出token上限、temperature0、top_p1；完整请求、原SDK与原parser相同。
六条路径的driver均限CPU72–79，模型服务限CPU80–87；执行图配置并发额度8、线程8，实际并行与首次图成本分别保留。

输入是[完整记录](model_source.json)中的8条人工构造文本，包含重复出现、换行、中文与标点。
它不是SQuAD或SemBench测试集；要求返回`ok`只检查完整调用、原解析、行关联与消费。
全部18查询的请求值多重集合一致，每次消费8行且原解析结果为8个`ok`；重复文本仍保持两个独立行ID。
这支持本次真实模型接线，不支持任务质量、服务饱和或总体性能排名。

下表查询时间从每次driver入口到原parser、结果消费、记录EOF及该查询清理完成；可复用group／作者进程启动另列。
每条路径测量查询样本数2，请求样本数16；预热单列，不进入测量统计。单位秒。

| 路径 | 预热查询 | 测量查询1 | 测量查询2 | 查询P50 | 查询P95／P99／max |
|---|---:|---:|---:|---:|---:|
| 原LOTUS | 0.685973 | 0.254204 | 0.213361 | 0.213361 | 0.254204 |
| Daft | 3.425198 | 0.223028 | 0.231553 | 0.223028 | 0.231553 |
| Ray Data | 15.186813 | 5.439133 | 1.438763 | 1.438763 | 5.439133 |
| SemLoom | 0.291085 | 0.181207 | 0.177223 | 0.177223 | 0.181207 |
| IMLane CPython | 0.526106 | 0.331219 | 0.384607 | 0.331219 | 0.384607 |
| IMLane作者Ray | 1.375768 | 0.372150 | 0.382981 | 0.372150 | 0.382981 |

请求时间独立采自同一网关进程的单调时钟。网关区间为handler进入至完整响应写出结束；
模型HTTP区间为实际上游headers发送至完整响应体读取，含网络与服务排队，不能当作GPU纯计算。
下表每项n=16、单位秒；最近秩P95与P99在本次样本中都等于最大值。

| 路径 | 网关P50 | 网关P95／P99／max | 模型HTTP P50 | 模型HTTP P95／P99／max |
|---|---:|---:|---:|---:|
| 原LOTUS | 0.112805 | 0.132864 | 0.107354 | 0.117802 |
| Daft | 0.110623 | 0.118906 | 0.102197 | 0.109197 |
| Ray Data | 0.067228 | 0.078621 | 0.064258 | 0.077028 |
| SemLoom | 0.103110 | 0.113720 | 0.097331 | 0.102508 |
| IMLane CPython | 0.063437 | 0.074942 | 0.061678 | 0.071538 |
| IMLane作者Ray | 0.060198 | 0.068122 | 0.057590 | 0.065907 |

原LM在批次完成时共同发布各调用结果，其ready至整批返回区间另存，不能将8个相同区间当作独立HTTP返回时间。
六个group按固定顺序分块执行；Ray Data两条测量查询差异较大，首次图成本、进程启动及服务历史均可能影响小样本。
最近秩P99在n=2／16下只是样本最大值，不是稳定总体尾延迟。
全部原值在[144条请求](model_requests.csv)与[18条查询](model_queries.csv)，
全部分段／吞吐／n／错误数在[机器结果](model_results.json)。
[离线复算](recompute_model_metrics.py)仅读取CSV，不启动任何服务：

```bash
python experiments/results/postgresql/native_executor_baselines_20261012/recompute_model_metrics.py --check
```

IMLane两模式各实际24次调用、2个worker；作者Ray另有2个非空actor ID，均使用同一DBEnd桥与薄UDF。
空输入探得settings执行器4／线程6，声明执行器2、图额度8；settings4不作为实际执行器数。
Coordinator及独立Ray退出码0、未强制，自有存活进程、共享内存、未确认标记均0。
模型API服务退出码0；按记录的自有PID／start ticks核对无存活进程，GPU计算进程归零。
完整资源身份和原件摘要见[来源记录](model_source.json)。

本轮首次启动在模型就绪后、本地连接断开时因supervisor写stdout发生`BrokenPipeError`，driver未启动，POST／分配均0；
API服务退出0、GPU进程归零。改为自有脱离连接的supervisor和持久日志后，在原15分钟启动期限内完成第二次启动，
没有重置期限或重试查询；首次失败与两次服务日志同包保存。
原始归档`final-evidence.tar.gz`的SHA256为`b5b92075e09bfee3d68ff4e95af1a9c47c2f6a2dde8ebf97f08716f2ff60a623`，
保存在接续任务仓库外`evidence/lotus-six-model01/`，成员以`lotus-six-model01/attempt02/`定位。
归档包含SQLite记账、完整protocols、HTTP／worker观测与失败原件；公开CSV不筛除慢样本。

<a id="lotus-two-gpu-next"></a>
## 同机双GPU的后续方案

双GPU尚未执行。具体条件与停止要求进入[同一方案的双副本章节](../../../plans/语义系统对照.md#lotus-two-gpu)。
拟在新独立额度下成对执行1×TP1与2×TP1，六条路径共用透明HTTP轮询路由；
不能把现有PG Map多副本CLI直接放进原生baseline。
当前48未用名额不转入下一轮；模型副本身份、公共路由软件资格与完整查询指标先核对，再由父协调批准执行安排。

## 结束责任与原件恢复

正常复用要求完整响应、EOF和正常DBEnd退出。错误或提前停止消费后的未确认记录按Coordinator PID/start ticks、实例名称和work_root保留，
新对象不得跳过该记录继续提交；调用者核清并结束自己的Coordinator／Ray后才使用新身份。
通用HTTP端点的`remote_http_stopped`保持unavailable，consumer退出不能当作远端请求停止。
执行稳定性与多副本另在`codex/semloom-stability-and-replicas`，原生分支不重复实现这些修补；
固定两分支组合已完成上述回归，原生单独分支不重复携带稳定性修补。完整方法接通不自动证明PG载体或调度收益。

仓库外恢复根分别为`native-executor-20261012`、`native-executor-baselines-20261012`及接续任务的`evidence/`。
先按紧凑事实核source archive和binary摘要，再读取`source-identity.json`、query summaries、原始HTTP／worker trace、SQLite记账及所有失败轮。
IMLane新交付原件另保存在`imlane-call-evidence/`，接续整合原件在`evidence/imlane-entry01/`；配置、进程身份及大日志不进入Git。
