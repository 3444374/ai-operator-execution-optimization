# IMLane桥生命周期检查

本次在固定基线`8862c8130bfb53aaa4873ac83354b8afa4711d73`上实现可选常驻DBEnd桥，
默认逐批桥保持。阶段定义、固定作者源码依据与采用决定集中于
[接口说明](../../../../code/integrations/sema/imlane_calls.md#bridge-lifecycle)，不重复解释为研究贡献或性能收益。

2026-10-14本机去重检查98项：97通过、0失败／错误、1项因缺真实Arrow Python模块跳过。
去重采用测试来源文件、类名和方法名，避免多个模块导入同一fixture后重复计数。
[逐项日志](tests.txt)保留具体用例；[机器摘要](summary.json)记录实际源码SHA-256与结果。
新Python会话17项覆盖多批复用、完整请求／响应、重复源行、逆序返回、429、transport错误、
消费／parser中止、取消、期限、崩溃、错批／重复／迟到结果、记录核对与首错保留。
桥为明确子进程fixture，HTTP只访问localhost，公共工厂和同Coordinator未知记录使用实际适配器。
独立审查复现消费者悬停后直接close会延迟写入unknown标记；修后close返回前拒绝新同Coordinator拥有者，
新增两项关闭／context退出回归，并保留首个未知记录与parser原错。
同一审查又复现transport错误结束时提前清除pending的并发关闭问题；修后发送登记和关闭共用owner锁，
只有健康结束才清除pending，新增结束记录与close交错回归。独立复核17项通过，最终代码无审查阻塞。

三项C++原语检查使用已有Arrow16.1.0头／库和本机clang++，从实际桥源码抽取converter、
有界读取与固定路径函数后编译运行；作者只用模板声明替身。
连续三次复制后覆盖原Arrow缓冲，保留的结果仍正确；同时验证超量／NULL／身份／负行号拒绝、
逐行读取上限及目录／文件符号链接、FIFO、文件体积拒绝。这些是实际Arrow复制与文件行为，
不是完整JSON／session桥或作者IPC执行证明。

定向复现入口如下，Arrow路径必须指向已有一致版本，不安装或下载依赖：

```sh
PYTHONPATH=code python3 -m unittest discover -s code/tests/baselines -p 'test_imlane*.py'
PYTHONPATH=code python3 code/tests/execution_provider/test_sema_imlane.py
PYTHONPATH=code python3 code/tests/execution_provider/test_imlane_session.py
IMLANE_CPP_TEST_ARROW_INCLUDE=<existing-arrow-include> \
IMLANE_CPP_TEST_ARROW_LIB=<existing-arrow-library-directory> \
PYTHONPATH=code python3 code/tests/baselines/test_imlane_cpp_primitives.py
```

上述分组可能重复加载旧fixture，合并报告按来源去重。首次unittest文件参数触发stdlib `code`模块重名，
随后localhost fixture受沙箱bind限制；只访问localhost的执行经正常权限流程后通过。
完整桥语法检查在`nlohmann/json.hpp`缺失处停止，未安装／下载依赖。
失败种类与实际范围记录于摘要，不把未执行写成运行失败或通过。

本轮作者CPython／作者Ray运行0、模型请求0、GPU运行0，没有占用原单卡服务器。
截至本机阶段，全桥链接／实际常驻多批、两作者模式的IPC与Lane复用、公开查询／原SDK消费及适配开销占比均待主协调安排；接续实库范围见[作者软件检查](#author-session)。
HTTP client仍逐调用；阶段累计会重叠，未比较完整JCT、RSS排名或代表性性能。
旧同步／async的真实作者软件证据继续保留在[既有报告](../../postgresql/native_executor_baselines_20261012/README.md)。

<a id="local-verifier"></a>

## 薄资格验证器本机阶段

在已推送桥实现`9766e784ea9d1f0c16a56a0b81a369847081bbce`上补齐
[独立验证器及SDK客户端](../../../../code/integrations/sema/README.md#imlane-session-verifier)，
没有再次修改生产适配器、C++桥、作者调度或共享`native_calls.py`／`prepared_map.py`。
本轮定向45项全部通过，0失败／错误／跳过：既有helper23项、新验证器14项、SDK客户端8项。
[逐项日志](verifier-tests.txt)及[机器摘要](verifier-summary.json)保存用例、实际源码摘要和失败历史；
不与上方98项相加，受影响helper包含重叠用例。

验证器使用真实Python适配器配合有限子进程／localhost HTTP替身，覆盖同一执行器三批、完整字节与重复头、
重复源行、批ID、乱序、429、输出持有，以及transport／timeout／consumer／cancel／crash后的持久隔离。
CPython／Ray四格在本地只是协议标签，没有启动作者运行时；SDK客户端测试也使用显式SDK／query替身。
额外故障注入检查lease变化、RSS／SHM／期限、只清理自有身份、首错保留及强制清理不冒充健康退出。
初次协议测试因错误fixture端口出现14项错误，修后通过；随后macOS路径别名导致一项mock断言失败，修正为规范化路径。
故意取消／崩溃产生的localhost连接reset stderr保留在仓库外，本轮最终用例均通过。

独立子代理审核修正了只读Git checkout被误拒、SDK依赖缺锁延后发现、消费清理覆盖首错的问题；
同时保留venv解释器路径，避免符号链接选择基础Python环境。审核只读核对固定LOTUS源码与原Q1/Q2/Q3语义，
独立复跑SDK及验证器的无HTTP子集通过，最终无阻塞发现。

本轮服务器上传／负载、作者CPython／Ray、实际LOTUS SDK、模型／GPU均为0。
完整新桥编译、作者两运行时×默认／复用及原SDK完整消费仍pending，须在主协调分配的独立窗口验证；
实际host额度16核，拟分配8槽与Ray／Daft重负载串行。薄runner的实现与本机替身不能提升常驻候选的实库资格。

<a id="author-session"></a>

## 作者与原SDK的有限软件资格

固定生产适配提交`9766e784ea9d1f0c16a56a0b81a369847081bbce`，验证器来自
`f788a094fd5270607294b2303e309a09450e399e`；原178文件包上传摘要核验通过。
按最新指令只在原173源码成员范围更新两个验证文件，执行源锁绑定
`6916ebe5bda5fc1d6826df09db0950bfeff1231f`，`--wall-limit-s 0`取消健康总时长截断；
内存、lease、POST、单批／请求及显式故障保护保持。8项定点本地回归和独立补丁审查通过。
原包、旧源码、控制指令截断及只读汇总字段错误记录留在仓外；确认初次控制指令未启动进程后才重发，未重跑作者矩阵。

复用既有作者`072f8257`、Arrow16.1.0和LOTUS1.2.4环境；两个独立解释器预检均通过。
完整桥在单编译器下构建成功，源码／二进制摘要见[机器摘要](author-summary.json)。
四格按默认→复用、CPython→作者Ray顺序执行，全部软件检查通过：

| 作者模式 | 桥路径 | root POST | 原SDK健康／parser POST | 总POST |
|---|---|---:|---:|---:|
| CPython | 默认逐批 | 24 | 28／4 | 56 |
| CPython | 显式复用 | 24 | 28／4 | 56 |
| Ray | 默认逐批 | 23 | 28／4 | 55 |
| Ray | 显式复用 | 23 | 28／4 | 55 |

每格同一个root执行器连跑三批×4行，重复源行、完整字节／重复头、乱序、429和结束记录通过；
context与注册计数分别为默认`[1,1,1]`、复用`[1,0,0]`，复用同桥PID与独立batch ID。
原SDK四批为Map4／Q1 12／Q2 6／Q3 6，保留原Filter、顺序、LIMIT5／COUNT6及重复reviewId；
两种作者模式均核对实际两个worker，Ray进一步核对两个actor身份。gateway元数据中的`native_executors=4`
不代表四个物理执行器；实际Coordinator声明及worker数均为2，实际task threads为6。
首批四个完整响应在后续批次后仍正确，输出深复制继续保留。
独立重算128条SDK请求的正文摘要／长度和原headers，以及116条已消费完整响应的34,244字节、
状态／HTTP version／重复头；parser探针每格只消费首1／4条即以原SDK异常停止，不能称完整消费该故障批。

transport／timeout／consumer／cancel／crash和parser各使用新Coordinator／namespace；
unknown持久记录、同身份新拥有者拒绝、首错及首条marker保留均通过。杀桥仍不表示远端HTTP取消。
28个实例中CPython14项原始clean；Ray8项原始clean，另外6项为默认／复用各自的
timeout、consumer、cancel故障，退出后需要清理各自namespace的6个SHM成员。
这6项保留原始`passed=False`，在PID、线程与SHM出口核对后另记
`expected_fault_owner_cleanup_verified=True`；没有把强制SHM清理当成健康退出，没有强制进程兜底样本。
最终305组PID／start ticks复核无存活，lease已释放，作者资产及173成员摘要未变；RSS／SHM／lease保护无违规。

[批次与清理观测](author-observations.json)只保存必要统计，不复制正文、环境或完整日志。
仓外原件的文件大小／摘要登记在机器摘要，以运行标识`cpython01`／`ray01`获取；
[复算入口](summarize_author.py)从原件生成这两份公开记录，不连接服务器或重跑测试。
独立审核核对源锁、完整请求／响应和6项非clean故障边界，未发现阻塞问题。
本轮模型／GPU／重试均0，只支持同步HTTP内核的有限作者及原SDK软件资格；
原2000行完整CLI、PG新session接入、async与reuse组合、模型质量及同口径性能仍待验证。
默认`reuse_bridge=False`保持，阶段累计重叠，软件检查时长不作模型性能或调度创新结论。

<a id="integration-close-snapshot"></a>

## 整合时的关闭报告修正

`integration/baselines`在固定`d1f5ef74`上普通合入以上四个来源提交，
保留原生HTTP并发证据及非IMLane生产文件。独立整合审查另发现默认逐批路径的关闭竞态：
关闭锁外先读取空进程，随后批次创建桥，停止该桥失败时可能误报`bridge_stopped=True`。
现由同一锁阻止新启动并取得pending／process快照；退出字段仍以该子进程的实际poll为准，
unknown记录及同身份新拥有者拒绝保持，未声称远端HTTP已停止。

有限本地用例在修前确实失败，修后通过，不启动作者进程或HTTP。
整合版受影响回归按源文件／类／方法去重共210项：194通过、16明确跳过、0失败。
跳过包括本机缺固定Ray／Arrow、未启用实际图验证，以及缺显式Arrow头文件／库的C++原语检查；
没有把这些跳过补写成实库通过。以上关闭快照修正只有本地资格，
作者四格222 POST继续绑定生产`9766e784`和执行`6916ebe5`，不改其原件、六项nonclean或退出事实。

本次非IMLane生产差分为空，共用`native_calls`／`prepared_map`／LOTUS SDK与batch逐字节保持。
IMLane默认逐批分支仍关闭复用，但新增的阶段计时、有限读入和复制核查会进入执行路径；
下一轮匹配IMLane比较须固定同一新版和明确复用设置，不能给旧`d1f5ef74`测量换标签。
未改源码的其他臂能否复用旧结果仍取决于输入、参数、运行环境、缓存和计时口径一致。
本次没有服务器、模型或GPU执行；当前模型owner继续使用其已固定版本。
