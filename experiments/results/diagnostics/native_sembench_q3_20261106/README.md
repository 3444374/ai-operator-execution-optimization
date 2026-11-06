# 原生 Movie Q3 的进程收尾与环境资格

这是首测失败的诊断和修复记录，不是完整查询性能结果。首测运行标识为
`native-q3-e0ec-1106-r1`，受测源码为 `e0ec300c1c927b6882e1a934dd8efb64ab099996`。
[紧凑证据](evidence.json)保存版本、输入、生成参数、质量、时钟与原件摘要；完整私有原件保持原位，
通过运行标识、文件角色和 SHA256 经既有 artifact 交接取得。没有删除、覆盖或重判首测失败。
下一步条件见[校准计划](../../../plans/原生基线校准.md)。

## 首测实际完成范围

Daft0.7.21／vLLM0.25.1 使用完整 Movie2000 输入，原生筛选后产生120条预测；
Qwen2.5-7B-Instruct、BF16、temperature0、top_p1、max_tokens8192、无 stop，
engine_kwargs／provider_options 均为空，不覆盖原生线程或引擎容量。
COUNT 为7，金标14；逐行 TP7／FN7／TN106／FP0、未解析0，原评价相对误差0.5、绝对误差7、MAPE50%。
这是实际质量缺失，关闭修复不改变这七个漏判，也不构成质量达标声明。

查询 API 在66.027224515秒返回完整结果，但 worker 与 EngineCore 没有正常退出。
外层在结果文件出现约242秒后才发送 TERM，监督总时长311.836秒。
因此66.027秒仅是结果可用时间，311.836秒是失败监督时间，均不能填成正常完整 JCT。
没有独立 model-ready 时钟，不能减去日志中的初始化阶段来构造常驻时间。
最终观察到的125个持有进程均已退出，GPU空闲、SHM清理完成；这是失败后的回收证据。

本次冷查询指新进程／新模型，文件系统和JIT缓存并非全冷；首测JIT写入位置不符合当前数据盘布局，
后续按runtime runbook迁移缓存时必须记录差异。输入14566 token合计／单条最大191来自本地tokenizer，
不是引擎返回的实际token ID或完整计费。Daft已消费的120个逻辑请求及原失败保留；Ray的120额度未claim、实际请求0。
Ray当时因未找到0.22环境而未运行，这不证明0.25.1不可能兼容。

## 可复现的关闭缺口与修复

实际安装源码表明，Daft `LocalVLLMExecutor` 创建异步引擎和 daemon事件循环，却没有关闭接口；
事件循环持续运行并持有引擎。原生表达式也没有将模型owner交给调用方。
源码：[Daft执行器](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/daft/execution/vllm.py#L111)。
安装源码摘要保存在紧凑证据，未修改第三方库。

使用真实Daft执行器、真实CPU RecordBatch和持有普通非daemon子进程的模型替身，得到以下独立执行：

| 处理 | 结果产生 | 3秒内正常退出 | 退出等待位置 |
|---|---|---|---|
| 原路径 | 1条软件回复 | 否 | `multiprocessing._exit_function → process.join` |
| 删除引用并GC | 1条软件回复 | 否 | 同上 |
| 显式公共shutdown | 1条软件回复 | 是 | 无该等待 |
| 本次owner作用域 | 1条软件回复 | 是 | 无该等待 |

这复现了源码中的资源寿命缺口，并解释了服务器现象；不是对真实GPU收尾调用栈的完整测量。
回归测试在修复前因“native query left its model process alive”失败，修复后通过。
CPU替身没有模型推理，不把其3秒窗口解释为真实模型关闭耗时。

[owner作用域](../../../../code/src/baselines/text/frameworks/daft_native_owner.py)仅用于独立单查询进程，
临时观察公共 `AsyncLLMEngine.from_engine_args`，保留原参数／对象，先恢复工厂再关闭实际创建的引擎。
未改Daft分桶、异步提交、生成、原COUNT和解析。构造已开始但未返回对象时记unknown，不能报not_started。
关闭异常保留为cause，查询主异常和已有预测保留。
[vLLM公共shutdown](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/engine/async_llm.py#L231)
只将timeout传给EngineCore；renderer和线程取消仍可能阻塞，API返回也不是独立物理退出证据。
当前daemon循环交由独立进程退出，不据此宣称支持模型常驻复用。

[新worker／监督入口](../../../../code/src/experiments/native_daft_query.py)在关闭前原子发布
`result-ready.json`，完整预测只有这一份；若COUNT或READY发布已失败，已有预测另存
`failed-result.json`，不触发READY时钟。终态单独写`worker.json`，附实际关闭回执。
写附加artifact出错时保留原异常及supervisor清理报告，并附写入失败说明。
查询窗口1200秒；READY实际时刻起120秒收尾，其中112秒允许正常关闭、8秒用于TERM/KILL；
没有READY时按查询期限停止，总预算至多1320秒。监督晚看到READY也不延后收尾期限。
`completed`只表示worker执行完成；物理GPU退出字段保持未知，须由外层持有者观察确认。

## Ray现有环境能证明什么

[守卫CPU API审计](ray_cpu_api.json)的实际环境为Ray2.56.1、vLLM0.25.1、Torch2.11.0、Transformers5.14.1。
SDK导入、公共签名绑定、采样与输入结构、构造的SDK输出转换、本地Qwen模板／分词／解码均通过，
本次导入路径没有缺失模块。允许的只读探测为urllib3的IPv6 loopback能力检查、`uname -p`和当前Python的`file`检查，
各记录一次事件，未发现其他禁止尝试。v1–v3因拦截这些元数据探测未通过，原记录保留，不算SDK不兼容。

脚本没有调用模型构造、推理或Ray runtime；其零模型字段基于脚本行为、入口守卫和初始化状态，
不是独立OS／GPU计数器。Python hook不是系统沙箱，部分入口patch在导入之后安装。
输出对象与消息为合成CPU输入，没有覆盖真实120行Dataset／actor、Arrow传输、引擎配置、CUDA ABI、质量或关闭。
审计支持把现有环境列为有限项目资格候选，不能称为已完成原生Ray运行。

[Ray2.56.1发布extra](https://github.com/ray-project/ray/blob/ray-2.56.1/python/setup.py#L349)
固定 `vllm[audio]==0.22.0`，另声明jsonref及NIXL等依赖；现有环境的发布契约仍为fail。
jsonref／NIXL缺失没有阻断本次普通文本导入路径，不能直接推成Q3必装项。
本轮没有安装或改动服务器依赖，也没有为补齐未使用的audio／KV能力升级共享环境。

## 首次修复的CPU验证与复测安排（历史）

相关CPU回归覆盖真实Daft执行器＋模型替身、真实普通关系、原Q3语义、Ray API合同和执行层隔离；
另覆盖READY实际时钟、关闭失败保留预测、部分构造未知和附加写入失败不覆盖主异常。
最终运行方法数、源码及输出摘要见紧凑证据中的`validation`。
这些检查不替代修复后的GPU退出、实际引擎容量或原生Ray actor资格。

下一批先运行一次修复后的Daft完整120行，追加120逻辑请求；Ray可在单列0.25.1项目profile完成预检后，
使用原来尚未claim的120额度进行一次有限冷查询。两者不做额外模型warmup或整查询自动重跑，保留原生内部重试。
任何重试、质量差异、ABI错误、资源或关闭异常均保留并停止性能结论；物理attempt／token数采不到仍写unknown。
如必须使用官方发布extra，则在新隔离环境解析 `ray[llm]==2.56.1` 的完整依赖、核对容量和下载预算后安装，
不在现有模型环境就地降级vLLM。该修复提交只做CPU验证，随后实测另记如下。

<a id="retest-and-ray-ipc"></a>
## 67a4复测与Ray命名信号量残留

服务器随后执行`native-q3-67a4-1106-r1`，准确源码为`67a4fec3d58627e1864a065b855609960f2dac2f`，
各一次、无额外模型预热或整查询重跑。[复测证据](retest.json)关联原件摘要；本次诊断只读原日志、构建身份和库文件，
没有再运行模型、Ray集群、作者程序或安装软件。

| 路径 | 结果可用／worker结束／监督退出（秒） | 资源收尾 |
|---|---|---|
| Daft | 77.615118694／81.237611056／83.291295642 | 公共shutdown3.619秒；已观察进程、GPU和SHM检查通过 |
| Ray Data LLM候选 | 69.828121957／71.269628204／73.264323887 | worker退出0、runtime关闭、已观察进程／GPU通过，但原SHM检查失败 |

两组各120个唯一发生项，当前原始答复与解析逐条相同；均COUNT7、gold14、七个FN，未改善质量。
Ray返回14566输入token、579输出token、120个不同request_id；仍没有独立物理attempt总数。
新运行独立claim240个逻辑请求，原e0ec120次及失败保持，不重置或混算旧未claim预留。
这是一次固定顺序的Daft→Ray，使用共同数据盘JIT缓存、文件系统缓存未清空；不能排序为稳定性能，
也不能将相对旧失败的时间变化归因于关闭修复。未观察的后代退出与model-ready时间保持unknown。

Ray遗留的是一个`sem.mp-`前缀的POSIX具名信号量：文件逻辑大小32字节、占用4096字节。
它在本轮worker期间创建、运行前不存在；回收时没有活动映射持有者，已观察自有进程全部退出后才单独`sem_unlink`。
人工回收到结果可用约359.9秒，未满足自动120秒清理。原`resource.passed=false`、原FINAL及旧失败不改写。
临时控制器曾只凭worker／进程／GPU判`completed`，遗漏`ResourceMonitor.finish().passed`；
后续控制器必须先取得资源终态，再将其纳入完整成功条件，`failures=[]`不能替代`passed`。
这是已确认的汇总错误；信号量本身为何没有unlink是另一个待隔离的问题。

原模型actor的完整stdout记录`MPClient`启动关闭、engine manager stopped、background resources清理和complete。
原core-worker日志另外记录：actor因全部引用被移除而正常退出，随即成功杀死两个direct children，其中一个由vLLM日志确认是EngineCore。
所以不能写成“未调用引擎shutdown”，也不能仅凭完整API回执声称子进程按Python终结流程退出。
对应日志的三个独立SHA保存在复测证据，不以driver不完整转发日志代替actor原件。

固定[Ray退出实现](https://github.com/ray-project/ray/blob/ray-2.56.1/src/ray/core_worker/core_worker_shutdown_executor.cc#L161)
先执行child cleanup，再执行graceful callback；child cleanup使用强制终止。
这与本轮原日志相符，提供了“Python资源终结机会不足”的机制依据。
另一个被杀子进程是否为`resource_tracker`、具体信号量由谁创建，本轮没有命令行／创建栈证据，仍未定案。
实际tqdm4.67.3的首次进度条可能创建单个multiprocessing RLock，时间与权重加载接近，但这只是待检验假设。
不能据此直接删除全局tqdm锁、全局关闭Ray child cleanup，或在生产中按前缀盲目unlink。

版本判断也保持两层：Ray2.56.1＋vLLM0.25.1已完成这一次真实Q3，却仍不满足官方extra的0.22.0约束。
[vLLM0.22.0 CUDA要求](https://github.com/vllm-project/vllm/blob/v0.22.0/requirements/cuda.txt)同样固定Torch2.11.0，
不能将现有Torch2.11.0误判为版本冲突。0.22／0.25都存在引擎manager与资源关闭链，
尚无证据表明仅降级vLLM就能消除残留；如检查官方组合，须独立环境、独立身份，不覆盖本次候选结果。

当前最小下一步是[零模型CPU复现](../../../plans/原生基线校准.md#ray-semaphore-cpu)，
记录实际SemLock名称／创建栈、tracker进程和原生actor退出顺序后，再选择最窄修复；本轮没有安装或打补丁到Ray／vLLM。

<a id="imlane-author-environment"></a>
## IMLane作者环境的实际准备范围

只读现有配置指向的作者根目录确认：源码HEAD为`072f8257db80569e42c9f5727d839587df529652`，
已存在patched构建，Python3.12.3／Arrow16.1.0；records方法环境也为3.12.3／16.1.0。
论文的Python3.11／DuckDB0.10.1不能写成这台机器已经验证的二进制版本。
该prefix内有`imlane`、Python executor、DBEnd-C和C++共享库；C++库声明Arrow1601 ABI。
构建身份、两库摘要、NEEDED项与修改过的五份源文件名保存在复测证据；这是库存／来源检查，没有加载库验证ABI。

作者`database/duckdb`源码存在，但未有build目录或CMakeCache；所核对的release／prefix位置没有DuckDB二进制或库。
仓内也没有DuckDB原生Q3 driver；`semantic-benchmarks`检查普通pip DuckDB不证明作者修改版已构建。
因此“DBEnd-C库缺失”不准确，实际缺项是作者DuckDB的构建、链接、SQL函数／coordinator alias和Arrow结果资格。
现有records客户端只链接C++ runtime，不能替代这一条数据库路径。

最小准备沿固定作者源码：先核对已有五处patch是否只是所需兼容修正，再在独立目录构建作者DuckDB并验证DBEnd-C链接；
加薄C++ SQL driver和整表Arrow UDF，先做真实`PROCESS_PREDICTION`无模型fixture，再独立核验THREAD路径。
`SCHEDULE_PREDICTION`的Filter空实现不能用来凑接通；同名函数alias与作者默认coordinator连接必须真实生效。
先验证非空整数0／1＋SQL`=1`，避免未经资格的Arrow位图分支；覆盖2000→120发生项、尾批、Unicode、重复业务ID、
软件回复COUNT14和错误／进程／SHM清理。以上完成前不安排IMLane原生Q3模型查询。
