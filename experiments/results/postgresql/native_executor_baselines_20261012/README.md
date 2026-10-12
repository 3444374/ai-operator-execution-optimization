# 三种原方法的原生执行接入

<a id="imlane-2000-capacity"></a>
## IMLane原Q1完整2000行软件检查

备份补丁SHA256为`01be5a5627b3165f4f486e17c249e4186c22d2c5bcd21647e9625a54023632ad`，七文件原样提交
`2cca502df43e1401284b33ac14862ba945c7c55d`；在已发布`1e96935610069d17430c6fb75fe37a75659aa84e`
上只cherry-pick这七文件得到`7bb10980032e984f8f60ccaed4732c39bc168a1c`，逐文件摘要相同。
现有作者源码仍为`072f8257db80569e42c9f5727d839587df529652`；仅链接既有patched库，不重建作者系统。
桥C++源码SHA256为`1e27ee6e7d7a370c0235582564c3941bdbeb15a14259a4e608a03c8b7f1e7984`，
二进制SHA256为`b201be8891493a2885ff9faae6d755bfe1eb7c20e10bdb2cda3f5f25c56cf5c6`，编译10.927/120秒。

公共IMLane、Sema DBEnd及C++共用`IMLaneBatchLimits`：本次全批2000行、payload32MiB、
wire48MiB（UTF-8 JSON lines含换行）、单项payload8MiB、待交付128项、累计响应32MiB；
Q1单项完整HTTP响应额度1MiB。每查询一个DBEnd上下文，在交付后补入下一调用并释放该输入/输出引用。
作者Lane、Coordinator、CPython与作者Ray继续拥有执行；没有每128行重建进程或整组完成后才继续供给。
这些编码字节额度不保证聚合RSS。接口与旧128来源见[IMLane说明](../../../../code/integrations/sema/imlane_calls.md)。

原SemBench Q1全量2000调用的payload为1,500,488字节，原stdin为2,649,072字节，最大payload902字节、
最大原wire行1527字节。实际fixture端口使stdin为2,657,072字节；复原原endpoint后wire摘要
`d85a25ca894b6f89ffbd471f7fca8edcbb992a7b4d0123a7ac748994a45f4125`完全匹配，payload摘要
`ec6948a91625073ac3d85e901dd51f823b4d0b164415e68a66de8737f8448fd6`也完全匹配。
CPython和作者Ray各只有一份2000行DBEnd输入，2000个独立出现身份均收到完整响应；
原LOTUS1.2.4 `sem_filter`完成全部消费后，原Q1才返回5行。没有截断原评论或提前选5条。

两模式通过128/129行、相同完整请求的独立身份、乱序、慢消费、2001行及单项/累计payload/wire超限拒绝。
两项待交付额度的实际观察证明第3条已到达HTTP时，慢第1条仍未完成；作者线程/META保持。
256字节响应额度实际触发作者UDF ValueError并恢复原诊断；已发送取消保留未知远端状态和uncertain记录，
同实例的新对象拒绝提交。核清并结束自有Coordinator/Ray后，用新实例验证下一正常查询。
`1e969356`的首错/notes/cookie修复未与七文件重叠；组合源码在两模式另以12次POST复验错误、取消及下一查询。

| 软件运行 | 本次服务器POST | 累计POST | 从首次启动至该次结束（秒） | 结果 |
|---|---:|---:|---:|---|
| 首次 | 2259 | 2259 | 46.793 | CPython完整Q1通过；持续供给观察的脚本断言失败 |
| 第二次 | 9 | 2268 | 214.875 | CPython持续供给通过；32字节诊断预期断言失败 |
| 第三次 | 2279 | 4547 | 475.878 | 两模式全部目标检查通过，已完成的CPython Q1未重跑 |
| 组合源码补测 | 12 | 4559 | 783.993 | 两模式错误/取消/下一查询通过，完整Q1未重跑 |

全部服务器操作使用CPU16–23。首次启动11:01:41.629 UTC，原900秒截止11:16:41.629 UTC始终保持；
最后一次已于首次启动后783.993秒结束。本机原容量/协议/分类13项及组合24项通过、无跳过；
组合keepalive产生4次POST（本机控制台最初的0注记已更正）。服务器4559＋本机4＝4563/5000，模型/GPU0。
最终独立核查173组PID/start ticks及14个实例，自有存活进程和共享内存为0；三个既有作者二进制摘要均未变，SSH master保持。

保留编译监督报告缺失：监督脚本将自身timeout父进程纳入清理，编译本身退出0；独立资源复核通过，
脚本在作者测试前修正，未重跑编译。首轮HTTP到达时刻受作者队列影响，不能代替桥补入时刻；
两项额度用例另外实际检查持续供给。32字节用例也限制错误诊断，C++交付检查正确返回ValueError，
脚本却要求UDF错误字段；保留该失败，再以能承接诊断的256字节额度核验原作者错误类别和内容。
生产代码和原Q1未为通过这些检查改写。

机器事实见[imlane_capacity_verification.json](imlane_capacity_verification.json)，84成员脱敏观测见
[imlane_capacity_observations.tar.gz](imlane_capacity_observations.tar.gz)，可用[离线复算](recompute_imlane_capacity.py)核对全部POST、
4000个Q1身份、持续供给、失败来源与清理。原件和旧前置拒绝保持原保存位置。
MovieQuery的独立128预检仍在`sembench_queries.py:156`，由发布者结合新桥配置接续；本任务不推送native或main。
未运行真实模型/GPU、质量或性能比较、DuckDB/Sema2000行、全部4096行与共享Coordinator查询隔离。
完整Q1实库检查属于2cca502d；组合七文件相同，组合异常路径已核，不称完整Q1在组合源码重跑。

<a id="native-release-composition"></a>
## 发布组合与本机回归

本节保存截至1e969356的发布组合事实；后续扩容资格见上述独立记录，原Q1前置拒绝仍保留。

2026-10-12恢复本机执行后，以已发布`8fc28d97`在独立检出继续现有native分支。
独立数据集配置测试`e51f4206`已整合为`6faa05ea`，12项本机回归通过、0跳过并已推送核验。
前四块`c47304d1/c8484a2b/47adb0f1/c4f7f157`先与SemBench组合，14个Python源／测试文件逐项匹配固定候选；
首轮本机60项中49通过、11因缺LOTUS依赖跳过，已独立发布并核验远端`9c1e1f22`。
前四块规范与需求独立复审各0项必修；组合保留准备／响应回调、重复行身份、首错和附加清理错误。
随后整合`5e0390ec/97af50c5`两块，规范复审发现1项事实同步，需求复审发现独立HTTP关闭可能覆盖首错和请求内cookie重定向行为改变两项必修。
已保留原异常及脱敏的附加清理诊断，并让每条完整请求独享默认cookie jar；连接客户端仍复用，同一请求重定向保留cookie，并行请求不互相继承。
数据集测试辅助参数随Ray请求数量核查调整为有效声明，并使用已进入的processor session替身，原429／transport／行身份检查保持。
修前16项复现为2失败／4错误，原输出保留；修后100项中86通过、14因缺LOTUS／OpenAI／LiteLLM依赖跳过，0失败／错误。
14项跳过未取得本机验证资格；实际厂商图、keepalive HTTP fixture及作者runtime未在本次运行。
修后规范与需求分别复核，剩余必修各0；新增源码和最终回归摘要逐项一致，隐私／新增链接／文字／diff检查通过。
已保存实际SDK验证仍属`c4f7f157`：20通过、0跳过、12localhost POST、模型/GPU0，源包摘要核验通过。
本次本机新服务器、HTTP POST、模型及GPU运行均0；不占用服务器CPU16–23。
保存的`97af50c5`桥编译退出0，源包与该发布组合C++逐字匹配，源码SHA256及已记录的二进制SHA256进入机器事实。
作者系统未重新构建，新桥二进制未用于作者runtime或2000行检查；这些资格仍pending。
原Q1 2000行的两条IMLane适配器前置拒绝及历史记录保留，当前128行是适配器接受范围。
机器事实见[integration_verification.json](integration_verification.json)，首四块输出见[integration_first4.txt](integration_first4.txt)，修前复现见[integration_late_before.txt](integration_late_before.txt)，最终输出见[integration_final.txt](integration_final.txt)。

<a id="execution-repairs"></a>
## 响应与生命周期修补

2026-10-12从`7829bb45`在独立worktree修补原生消费线程的提前关闭诊断。
线程停止/join后记录执行及关闭错误，附加关闭异常对象保存在`last_cleanup_errors`；
查询第一异常保留，显式close没有第一异常时传播关闭失败。
5项本机真实消费循环／图替身回归通过，HTTP／模型0；原图、远端不确定状态和旧模型原值保持。
共享SDK解释已接入五条替换批次路径，原LOTUS保持；11项Core/准备/取消控制测试与2项共享入口检查通过。
固定SDK响应差分2项及既有SDK清理11项因本机缺依赖跳过；没有安装或下载。
IMLane容量/身份和作者UDF异常另以执行错误结构化返回，正常传输错误保持；5项Arrow/kernel替身分类回归通过。
其中ValueError不会进入SDK连接错误解释，原类别/首因诊断保留；尚未运行真实作者过程或重新构建。
实际Daft／Ray异常路径、固定SDK资格、客户端复用及共同容量仍待分别核查，不称完整公平等价。
共享解释继续改为每批次惰性创建一个SDK客户端，五条替换批次复用同一机制；
不再逐响应重建客户端，仍调用固定SDK转换，重放没有实际HTTP请求。
11项准备/清理控制检查及3项入口/关闭检查通过，3项实际SDK检查本机缺依赖跳过；
新增实际SDK用例覆盖错误后继续解释、不同输出及重复值、客户端身份复用和释放。
指定服务器CPU16–23第一次只读采样最高忙10%，因此该次没有启动验证；后续先重新检查空闲状态。
用户批准具体服务器源码上传后，固定`c4f7f15742d3473274b75c42556c2d75be423060`
通过20项真实SDK/清理检查、无跳过，12次localhost POST、模型/GPU0，15.739秒；
fixture服务器关闭、自有进程观察为0。来源摘要见[修补核验](repair_verification.json)。
实际Daft0.7.21零HTTP探针4行证明查询EOF后异步loop仍运行，支持检查query/loop持有连接。
原生HTTP kernel现按查询及其Daft loop持有客户端；Ray Data遵循原HTTP stage的session_factory批次寿命。
连接器不增加请求准入额度；每次请求保留自己的期限，cookie只在同一请求及其重定向中沿用，不跨独立调用。
6项生命周期检查通过，其中4次真实localhost调用使用同一TCP连接，含不同输出和重复输入；
5项提前关闭回归及2项Ray参数替身检查通过。实际新池的厂商图完整消费/取消资格仍待有限复验。
Ray默认batch_rows=1时每HTTP stage批次只有一个请求，不声称跨批次或跨查询池复用。
报告同时保存声明C与actors/async_batches/batch_rows/source_blocks；C不等于实际请求峰值。
Ray配置上限按actors×async_batches×batch_rows核查，实际执行和供给继续由原native图拥有。
共同`LotusCallLimits`明确请求/响应字节、批次行数/累计字节和request/batch期限；
native LOTUS完整响应默认从8MiB改为1MiB，与普通Engine接收范围对应。
6项本机检查通过，包含IMLane式2000行目标准备前失败、小Engine显式profile失败、最终SDK解释超过期限；
保留SemLoom有限准备及各native图的物理准备，配置相同不表示所有内部时序或实际并发相同。
IMLane128来源核到项目`5ee1b90c3`的C++快照检查及Python检查，不是作者系统自然容量。
桥源码完整stdout输出后释放完成调用`calls[i]`持有的输入/输出；保存的新桥编译成功且源码摘要匹配，作者runtime尚未运行。
32MiB只限定当前声明的输入/已交付结果编码量，不保证C++待交付输出、Arrow/IPC复制及聚合RSS。
该历史128行桥的512/2000目标未支持；新增完整2000行软件资格见上述独立记录，作者并发保持。

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
IMLane默认128行；显式2000行新桥已取得上述软件资格，超额拒绝；聚合RSS仍没有32MiB保证。
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
eager执行，前缀缓存关闭。生成参数为16输出token上限、temperature0、top_p1；完整请求、最终LOTUS parser相同，正常返回／消费通过，中间响应解释仍有差异。
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

用户已授权继续双GPU工程检查；软件资格与随后真实模型轮均通过，原安排见[当前方案](../../../plans/语义系统对照.md#lotus-two-gpu)，实测进入下一节。
上一版单双卡重测、到达轮询及55分钟安排已由双卡一轮、完整请求值固定分配、15分钟启动／10分钟测试替代。
[固定请求路由](../../../../code/src/experiments/fixed_request_routes.py)不读服务负载，不改变正文、原parser或并发额度；
所有六条路径共用一个端点，源行0–3映射GPU0，4–7映射GPU1，重复行1／2同副本，[固定清单](dual_route_manifest.json)保留各行关联。
路由不从HTTP序号制造源行身份，模型前先用既有单卡完整请求生成不可变摘要清单。

本机旧网关与新路由30项通过／39软件POST，服务器新路由6项通过／11POST；
实际六条LOTUS路径再各消费两行软件响应，共12POST，两副本各6，原SDK／parser及退出检查通过。
累计62次软件POST、小于64上限，模型／GPU／隐式重试均0。
完整gzip字节、重复头、远端HTTP错误、超时及caller取消都保留一次发送；
caller关闭时不宣称远端停止，受控fixture由自己的owner结束后才记清理完成。
真实双卡计划144POST、独立上限288，不为花完额度扩大样本，旧轮剩48保持未用。

<a id="lotus-dual-model"></a>
## 六条LOTUS路径的双GPU真实模型检查

唯一双卡轮`lotus-dual01/models`全部通过：同机2×TP1／PP1、两个独立Qwen2.5-7B服务，
六条路径共18查询／144真实模型POST，错误、超时、隐式重试均0。
每次8行完整消费、原parser返回8个`ok`，完整请求值多重集合与既有单卡轮逐项一致，
两个副本每查询4／4，各累计72POST／144输出token；测量请求各48，预热各24。
内层网关、公共固定路由及SQLite记账均为144，两份vLLM访问日志各72，已结束记账单元18。
新独立上限288占用144、剩144未用；旧单卡剩48未用，不转入本轮。
三轮累计实际模型312POST／占用320名额，较早失败及两次单卡启动记录保持原件。

固定组合源码`c0f420f8`保留稳定性`9bb6c415`并合入原生`7829bb45`，仅项目日志发生合并冲突、两份记录均保留。
模型源码归档SHA256为`c45fa20abd747d475fe715e9c5b9801e8d1a32b9fe1d6b0f25977be248dbadf1`；
路由实现与服务器软件资格逐字一致，原方法／公共native_calls／供应商入口没有为本轮新增调度策略。
权重、tokenizer、生成、原方法及C8／线程8与单卡保持；每卡vLLM配置也相同，CUDA服务隔离并实际核对GPU UUID。

API PID189412／189413、实际engine PID189623／189612分别在GPU0／GPU1；
其实际CPU亲和为80–87／88–95。两个服务各就绪约46.128秒，含环境／模型文件预检的共同启动区间77.708秒，
保持一次15分钟期限；模型supervisor测试78.467秒、driver区间75.199秒，小于10分钟。
查询driver／公共路由使用72–79，CPU8–15未占用。两个模型API退出码0，已观测自有存活／GPU计算进程0。
作者IMLane两模式各实际24调用、2worker，Ray另有2个actor ID；Coordinator／Ray退出0、未强制，共享内存和未确认标记0。
各PID／start ticks、实际设备和服务参数由[机器结果](dual_model_results.json)定位。

每条路径仍为8预热＋16测量，n=2完整查询／16请求。下表查询单位秒，双卡预热单列，
单卡原值取同一8行输入的已测轮；吞吐比为`双卡16/两次查询耗时之和 ÷ 单卡16/两次查询耗时之和`。
双卡多了公共透明路由一跳，服务启动历史与固定路径次序也不同；比值只作有限诊断，不能报可靠加速。

| 路径 | 双卡预热查询 | 单卡两次测量 | 双卡两次测量 | 消费吞吐比（双／单） |
|---|---:|---:|---:|---:|
| 原LOTUS | 0.748612 | 0.254204／0.213361 | 0.180697／0.482898 | 0.705 |
| Daft | 2.795650 | 0.223028／0.231553 | 0.192738／0.186022 | 1.200 |
| Ray Data | 14.414726 | 5.439133／1.438763 | 5.629190／1.837113 | 0.921 |
| SemLoom | 0.279135 | 0.181207／0.177223 | 0.164347／0.186899 | 1.020 |
| IMLane CPython | 0.512712 | 0.331219／0.384607 | 0.329087／0.371936 | 1.021 |
| IMLane作者Ray | 1.284567 | 0.372150／0.382981 | 0.367844／0.393930 | 0.991 |

请求时间采用独立单调时钟区间。内层网关包含公共路由与模型HTTP往返；模型HTTP采用公共路由的实际上游headers发送至完整响应体读取。
下表n均16，单位秒；最近秩P95／P99均等于本次最大值，不是稳定总体尾延迟。

| 路径 | 内层网关P50 | 内层P95／P99／max | 实际模型HTTP P50 | 模型P95／P99／max |
|---|---:|---:|---:|---:|
| 原LOTUS | 0.094460 | 0.418599 | 0.076766 | 0.082842 |
| Daft | 0.081307 | 0.097283 | 0.066549 | 0.082304 |
| Ray Data | 0.067435 | 0.075549 | 0.061952 | 0.070680 |
| SemLoom | 0.082056 | 0.096746 | 0.061780 | 0.078596 |
| IMLane CPython | 0.061598 | 0.073379 | 0.055746 | 0.069972 |
| IMLane作者Ray | 0.054760 | 0.072843 | 0.049669 | 0.066457 |

首次可复用group启动单独记录：原LOTUS8.164秒、Daft0.428、Ray Data10.699、SemLoom11.997、
IMLane CPython0.275、作者Ray0.289；IMLane作者服务另需0.558／5.563秒。
这些不进入两次查询测量；每次查询包括原parser／消费／记录EOF及该查询清理，
全部公共路由响应写出终点早于记录的查询终点，18／18逐项核对。

GPU利用采样请求间隔0.25秒，加上每次nvidia-smi命令耗时。模型测试区间每GPU各241样本：
GPU0样本平均3.759%、最大100%、非零14个；GPU1平均3.942%、最大94%、非零16个。
该区间含group／graph启动和请求间等待，并非连续GPU计算或饱和测量；短任务可能落在采样之间。
各查询样本数保留，n=0表示没采到样本，不能写成利用率0。
采样窗口采用同一host的CLOCK_MONOTONIC，不跨worker时钟相减制造模型计算时间。

全部原值包括[公共路由144请求](dual_requests.csv)、[内层144请求](dual_inner_requests.csv)、
[18查询](dual_queries.csv)及[482条设备采样](dual_gpu_samples.csv)。
[机器结果](dual_model_results.json)保存每卡请求分布、n／分位、group启动、全部单→双比值及退出。
复算不访问任何服务：

```bash
python experiments/results/postgresql/native_executor_baselines_20261012/recompute_model_metrics.py --dual --check
```

原件包`final-evidence.tar.gz`保存在接续任务仓库外`evidence/lotus-dual01/`，
SHA256为`190d41994ef9dfc70fb7ea268e4bfbb14088121816958de73ddab3ddaa2cac80`。
按成员`lotus-dual01/models/`恢复两服务日志／身份、SQLite记账、完整协议、HTTP／worker及GPU采样，
`lotus-dual01/software/`恢复软件资格；同包保留两份source archive和精确执行程序。
所有新慢样本保留，本轮没有模型失败重跑；旧轮失败独立保存。

<a id="lotus-known-audit-limits"></a>
### 已确认的成本与错误资格差异

独立只读审计核对`e571d4be`及`7829bb45`的相关核心；本轮`c0f420f8`未改这些文件。
完整正常请求／最终LOTUS parser／关联／消费证据继续有效，中间响应解释不能表述为“原SDK全部相同”：
[原生响应记录](../../../../code/src/semantic_methods/lotus/native.py)存在逐响应HTTPX／OpenAI客户端和LiteLLM本地重放成本，
[替换路径转换](../../../../code/src/semantic_methods/lotus/sdk.py)采用直接转换；非2xx与畸形JSON的类型／字段尚未通过同SDK完整对拍。
正常usage／logprobs已有测试，不能扩为全部错误路径等价，也不能把较短查询时间全部归为执行调度收益。

[native_calls](../../../../code/src/baselines/text/frameworks/native_calls.py)每call创建／关闭aiohttp session，
IMLane逐call事件循环与SemLoom客户端复用不同；原生完整响应默认8MiB、SemLoom默认1MiB，未在本轮统一。
当前输入8行、正常响应远低于共同1MiB范围，仍然是有效接线证据；扩大规模前需显式共同容量及期限。
IMLane128行／32MiB输入、C++聚合驻留输出无32MiB保证继续保留，不能以新双卡成功宣称512行能力或内存峰值资格。
提前close的消费线程附加异常可能未进入最终关闭报告，这是已复现的诊断遗漏；不能据此声明真实厂商泄漏或远端已停止。
这些修补已由独立owner按用户批准在另一工作树推进；本轮固定旧源码未带入，当前任务没有修改共享核心或重花模型预算。
新公共路由的超时／caller取消检查只覆盖该HTTP入口，不代替上述六路径所有异常生命周期资格。

### 后续正式SemBench测量条件

由独立SemBench owner完成原查询、真实输入、提示／parser、NULL／重复行及答案质量核对，并完成上述响应解释／连接复用／共同容量／期限和关闭诊断的软件修补及对拍后，才能开展公平性能对照；
本轮人工文本与固定`ok`不能替代该资格。固定源码、原Q／输入、模型逐文件身份和服务参数，
在1×TP1与2×TP1都经过同一公共固定路由下重测，保留各原生调度、相同总额度和明确的每卡容量。
预热、冷group／服务启动分别登记；使用预先安排的交错重复及足够样本，并通过真实请求形状的容量／饱和检查，
报告请求与完整查询原值、吞吐、质量、每卡真实调用／利用、错误／超时／未知责任。
正式清单、服务期限、POST额度和停止条件须单独授权；当前余量不自动继承，不增加动态路由、SemLoom专属负载信息或独立Q实现。

## 结束责任与原件恢复

正常复用要求完整响应、EOF和正常DBEnd退出。错误或提前停止消费后的未确认记录按Coordinator PID/start ticks、实例名称和work_root保留，
新对象不得跳过该记录继续提交；调用者核清并结束自己的Coordinator／Ray后才使用新身份。
通用HTTP端点的`remote_http_stopped`保持unavailable，consumer退出不能当作远端请求停止。
执行稳定性与多副本另在`codex/semloom-stability-and-replicas`，原生分支不重复实现这些修补；
固定两分支组合已完成上述回归，原生单独分支不重复携带稳定性修补。完整方法接通不自动证明PG载体或调度收益。

仓库外恢复根分别为`native-executor-20261012`、`native-executor-baselines-20261012`及接续任务的`evidence/`。
先按紧凑事实核source archive和binary摘要，再读取`source-identity.json`、query summaries、原始HTTP／worker trace、SQLite记账及所有失败轮。
IMLane新交付原件另保存在`imlane-call-evidence/`，接续整合原件在`evidence/imlane-entry01/`；配置、进程身份及大日志不进入Git。
