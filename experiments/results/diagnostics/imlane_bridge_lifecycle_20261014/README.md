# IMLane桥生命周期本机检查

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
全桥链接／实际常驻多批、两作者模式的IPC与Lane复用、公开查询／原SDK消费及适配开销占比均待主协调安排。
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
