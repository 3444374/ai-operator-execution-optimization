# HSE static 正确性与 broker 历史开销审查

本轮审查已有异构分阶段执行（HSE）的 static runner 与共享 broker，以
`integration/baselines` 的 `3e7bd54bbc0e3416dfca991fcaaa2ffc41270a71` 为源码基点；
远端 stage 分支核对为 `5a7851426b9d4412a3d20192a84a72cd1acf1c6c`。
保留多图 batch、常驻 CPU/GPU actor、ObjectRef 依赖、FIFO 与 direct-dependency 强静态对照。
先验证本地生产 Core/broker 与受控 actor/source 替身，随后完成固定包的实际 Ray CPU actor 检查；
不要求 PostgreSQL 接入。

## 已复现并修复

| 对象 | 修前反例 | 修后行为 |
|---|---|---|
| broker lease | prepare/model 传入 NaN、负数或无穷值后抛错，但块先被移出队列，`is_drained=True` | 先验证时间，再修改队列；错误保持原状态、容量与可推进性 |
| 永久超大块 | 单块 ready bytes/work 超过完整额度，仍可入队并永久阻挡后续块 | 入队前拒绝，未占容量；正常后续块仍可推进 |
| 活动峰值 | 上限4的 fixture 实际持有4块，CPU因 ready 额度不足未派发，结果只报告3 | 每次入队立即采样；该反例报告4 |
| 引用寿命 | 单块48字节额度结清后，旧 payload ref 仍由局部变量持有；下一次准备时两个 ref 同时存在 | model call 持有 payload 到完成确认；随后清除，下一次准备前旧 ref 已释放 |
| 错误退出 | submit/get 错误立即归还 lease；调用方没有异常 finally | HSE停止继续供给，CPU `ready` / GPU `synchronize` 确认后才结清；未确认状态、引用与额度保留 |
| 派发顺序 | 已有 ready 块且 GPU 空闲时，先读取下一批 source | 先派发已有 ready model work，再补充 source；FIFO、batch 与额度保持 |

错误处理先登记 actor/lease，再提交远端调用；所有确认请求一起发出，等待共用30秒上限。
部分确认只归还对应 actor 的额度。再次中断或清理自身报错保留原始异常；已完成 broker 转换但
尚未移除的调用记录按其实际状态处理。未知调用保存在异常的 `hse_unconfirmed_work`，同一
borrowed pool 的后续 HSE 调用会被拒绝；正常热路径不增加 CPU/device 确认 RPC。
该完成证明使用既有同步、串行、同一提交方、无自动重试的 actor 设定；
[Ray 官方执行顺序说明](https://docs.ray.io/en/latest/ray-core/actors/task-orders.html)支持这项设计前提，
固定 Ray 2.56.1 的 CPU actor 故障检查已通过，见下方实库记录；真实 GPU 完成语义仍未验证。

## 已确认的开销与未验证的收益

修前 `snapshot()` 与每次不变量检查遍历全部终态历史，固定活动窗口仍有累计 O(B²) 扫描。
现在缓存完成/失败计数，活动检查只遍历活动块；保留默认历史、重复行检测与显式
`release_terminal` 语义，没有通过删除历史换取速度。

[原始三次测量](broker_cost.json)与[复算入口](probe_broker.py)只运行单活动块的本地 CPU broker
完整生命周期，每轮都检查恰好一次完成及全部历史仍保留；未执行 Ray、模型、GPU或数据库。
基点与修改版同进程按顺序交替运行，无单独预热。下表为各三次 elapsed 的中位数，单位秒：

| 累计块数 | 基点 | 修改版 |
|---:|---:|---:|
| 1,000 | 0.071473 | 0.030907 |
| 2,000 | 0.228983 | 0.061171 |
| 4,000 | 0.793945 | 0.123499 |

这些数字只支持 broker 历史扫描成本下降。优先派发的真实 GPU 收益、ObjectRef/Arrow/Ray
物理副本与 RSS 变化、完整吞吐和延迟均未验证；不据此替代 direct-dependency 或原生 Ray Data
的匹配资源性能对照。ready 时间核查已排除误报：model lease 读取的是 broker 保存的 driver 时间。

暂时的 FIFO head-of-line 等待保留。即使后方小块能放入，当前 static 仍先等队首所需 bytes/work；
新增用例同时覆盖字节与工作量不足。fit-aware 选择需要另行明确扫描成本、公平与饥饿处理，
本轮没有收益证据，因此未改策略。

## 验证与剩余工作

[修前反例日志](before_fix.log)记录21项检查的13次子项失败与2次错误；后续新增中断检查另计。
共享 scheduling 完整发现首先因缺 `pyarrow` 产生1个 import error，
[失败日志](discovery_missing_arrow.log)保留。排除该无法导入模块后运行现有 scheduling 全集、
HSE/descriptor、incremental image 和 Map preparation；最终范围、计数、跳过项与源码摘要见
[机器记录](local_checks.json)和[输出](local_checks.log)。该本地轮没有混装依赖或启动服务器。

HSE 本路径使用 Ray Core actor；broker管理自己的 encoded/preparing/ready/modeling payload
账本，Ray管理实际 actor/task 排程，Daft source 与其缓冲仍由数据源执行方管理。原生 Ray Data
观察臂未接入这些额度。本轮没有重复控制 Ray Data 自己的 pipeline/backpressure。
这些逻辑 byte/work 额度不包含 source/Arrow缓冲、序列化副本、Ray pinned对象、GPU actor 的
writable host copy、device tensor、模型缓存或全部 RSS；这几类物理成本需独立采集。

`run_image_clip_e2e.py` 由另一任务独占，本分支未修改。它目前仅在正常返回后停止worker与
`ray.shutdown()`；调用方应在异常后终止/丢弃本次池，不能改用 direct-dependency 重用未知池，
因为那条路径不读取 HSE 的保护记录。停止池的外层 finally 与 sampler 清理应由 CLI owner处理。

固定包实库检查已按主会话排期完成：4 CPU、512 MiB object store，采用串行 CPU/模拟模型
actor验证真实多返回 ObjectRef、部分完成、错误确认及未知池拒绝，模型/GPU/POST均0。
健康运行没有300秒或其他总墙钟截断；单操作无完成等待60秒、故障确认30秒、自有actor退出核对30秒。
真实性能验证需要单4090与最多16 CPU，复用既有本地模型和同一 source/输出语义；从已有batch64
强静态配置开始，direct-dependency/HSE先做正确性，再做预热及至少三次交错重复，独立采集
source、prepare、ready、model、fan-in与实际内存。准确运行配置和停止条件由主会话排期前确定。

<a id="integration-handoff"></a>

## 固定47f7800c的逐文件集成交接

CLI默认仍为`direct_dependency`，HSE需显式选择。HSE的ready优先、引用持有及异常确认自动生效；
正常热路径没有新增完成确认RPC。共享broker的合法调用、FIFO、batch、额度、输出schema、默认历史
及重复检测保持；永久超大ready需求改为入队前抛`BufferError`。两项性能行为只支持本地机制结论。

| 固定提交的文件 | 正确性／证据作用 | 性能或默认行为影响 |
|---|---|---|
| `code/src/modalities/image/staged_execution.py` | 提交前登记owner；payload持有到完成确认；异常保留未确认lease/ref并拒绝同池HSE；入队峰值补记 | 已有ready工作先于source.next派发；及时删除局部ref及完成的admission时间；仅HSE选择生效 |
| `code/src/scheduling/runtime/stage_broker.py` | lease时间校验先于队列修改；永久超大ready bytes/work拒绝 | 终态计数缓存、活动检查不遍历终态历史；对所有broker调用方生效 |
| `code/tests/modalities/image/test_hse_static_lifecycle.py` | 11项owner、异常、中断、ref、峰值回归 | 检查ready先派发，无生产默认变化 |
| `code/tests/scheduling/test_stage_broker.py` | 补时间、超大块、暂时FIFO等待、retry/cancel计数 | 检查不重扫终态历史，无生产默认变化 |
| `PROJECT_LOG.md` | 记录该实现切片 | 无运行影响 |
| `code/INFRA_STATUS.md` | 实现状态及尚未验证内容 | 无运行影响 |
| `code/README.md` | 报告与owner责任入口 | 无运行影响 |
| `experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md` | 登记CPU／替身证据身份 | 无运行影响 |
| `experiments/results/diagnostics/README.md` | 报告入口 | 无运行影响 |
| 本目录`README.md` | 审查结果与适用范围 | 不改变运行配置 |
| 本目录`before_fix.log` | 修前失败原件的脱敏文本 | 无运行影响 |
| 本目录`broker_cost.json` | 18个CPU broker原值 | 不提供GPU资格 |
| 本目录`probe_broker.py` | 同源码基点的离线复算 | 仅显式调用时运行CPU broker |
| 本目录`discovery_missing_arrow.log` | 缺Arrow导入失败 | 无运行影响 |
| 本目录`local_checks.json` | 481项范围、状态与4个源码摘要 | 无运行影响 |
| 本目录`local_checks.log` | 本地检查输出 | 无运行影响 |

CLI交接行号属于固定47f源码，后续file source增量可能改变位置；由函数名和调用点定位：

| 触发路径 | 所需owner清理 |
|---|---|
| `main:1506` → `_ImageExecution.__init__:788` → `_initialize_arm:868–870`初始化失败 | 外层Ray退出；生产worker构建的部分handle另见下段 |
| `_run_observed_image_execution:1091`的warmup异常／`:1092`停止异常 | warmup pool逐actor处理；保留原错误，保证剩余退出清理继续 |
| `:1093` → `_wait_for_formal_start`文件／JSON／等待／中断异常 | warmup已停止；仍需Ray退出 |
| `:1099–1100`任一sampler启动失败 | 停止已经启动的sampler；Ray退出 |
| `:1101` → `start_formal_workers:880`构建失败 | sampler停止、部分新handle处理、Ray退出；旧pool变量不能代表部分新actor |
| `:1103` → `_ImageExecution.execute:918、960`source或正式HSE异常 | sampler停止，丢弃本次pool；HSE未知记录不能通过切换direct-dependency绕过 |
| `:1114` → `_write_embedding_capture`校验／文件／`SystemExit`异常 | sampler、pool与Ray继续退出清理 |
| `:1124–1126`sampler stop或pool stop自身异常 | 继续其余独立清理，保留原始执行异常 |
| `main:1521、1530–1538`结果行／CSV／manifest／print异常 | 此时正常pool/sampler已停止，仍需Ray退出 |

`main:1540`目前只在成功路径`ray.shutdown()`。另一个实际源码缺口在
`code/src/modalities/image/execution.py::build_project_ray_worker_pool`：`:288–307`创建actor，
`:308–309`readiness及`:319–323`线程校验任何一步抛错时，已创建handle尚未组成返回pool。
因此只给CLI添加`stop_project_workers()`不足；builder应自行保留创建列表并异常处理，外层仍应负责
Ray与sampler生命周期。这些函数已由独立审查者做CPU异常注入核对，本分支不修改CLI或builder。

<a id="ray-validation-package"></a>

## 最小无模型实际Ray验证包

[包清单与逐文件SHA-256](ray_package_manifest.json)包含实际加载所需的项目source／test模块，
入口为[专属opt-in测试](../../../../code/tests/modalities/image/test_hse_static_ray.py)。
生产source逐一匹配47f7800c；新增测试入口取包含本清单的提交。
没有runtime.env、连接目标、凭据、数据集、模型、原始服务日志或Git metadata；仅生成a–e五个虚拟行。
复制或固定GitHub提交取得这些成员后，先按清单核对摘要；不混入原生IMLane作者文件或GPU依赖。
依赖只需NumPy、Arrow、Ray、psutil，可复用已有`imlane-runtime`四模块版本检查，不加载作者库。

| 实际Ray用例 | 核对内容 |
|---|---|
| 多返回ObjectRef及队尾 | 5行、batch2、两prepare actor／一零GPU模拟model actor；driver只get descriptor/result |
| prepare真实异常 | serial CPU ready成功之后才归还prepare额度 |
| 模拟model真实异常 | serial synchronize成功之后才归还ready额度；不宣称CUDA完成证明 |
| 部分完成／确认报错 | 未确认块48字节及payload ref继续持有；同pool再次HSE拒绝 |
| 真实提交后丢driver确认 | 一次返回丢失注入；真实actor队列里的ready晚于prepare完成；确认后同HSE可恢复 |

默认opt-in关闭；显式启用后缺依赖报错，不能以skip认领实库通过。每次只用3个自有串行actor，
资源上限4 CPU／0 GPU／512 MiB object store。测试结束只kill本测试创建的handle，按同节点PID＋birth
核对进程退出；driver连接结束，短`/tmp/hs*`下的原始Ray日志留给owner私下保存。若退出无法确认，
保留故障状态并停止后续资格判断。逻辑额度和该object-store配置均不代表总RSS上限。

共同lease由主会话协调，内容审核后按既有上传及服务器授权在空闲窗口执行；没有逐包重新授权。
固定包已完成下方五项检查，没有启动GPU／模型。公开复现入口如下，不包总墙钟timeout：

```bash
SEMLOOM_RUN_HSE_RAY=1 CUDA_VISIBLE_DEVICES= PYTHONPATH=code \
  <driver-python> -m unittest tests.modalities.image.test_hse_static_ray -v
```

仅在获窗口后以`address=local`启动自有零GPU Ray，避免环境变量使测试误连其他人的runtime。
单操作无完成60秒、生产故障确认30秒、自有actor退出30秒分别计时；
健康运行总时长不截断。本地28项回归通过，5项实库用例默认跳过；另以离线异常注入核对逐actor清理、
退出证明超时／再次中断和部分初始化失败。随后固定包显式启用，实际结果为`passed_5_of_5`。
原始失败、控制、日志及PID/birth证明保存在私有artifact目录；公开记录移除连接目标、主机、地址与路径。

<a id="actual-ray-checks"></a>

## 固定ed209238的实际Ray检查

[机器记录](ray_checks.json)与[脱敏测试输出](ray_checks.log)对应
`ed20923876d668d6b11b0e4005cca2c7caa89c12`的26成员固定包，生产source仍是47f。
2026-10-14 06:07:36–06:08:12 UTC运行，5通过／0失败／0错误／0跳过。
unittest记录35.576秒，只是软件检查耗时，没有预热或性能比较。
Ray日志保留两条预期的`fixture prepare failed`异步通知，没有通过屏蔽错误取得成功。

| 实际版本／配置 | 值 |
|---|---|
| Python／NumPy | 3.12.3／1.26.4 |
| Arrow／Ray／psutil | 16.1.0／2.56.1／7.0.0 |
| Ray CPU／GPU／object store | 4／0／536,870,912 bytes |
| 同时actor上限／自动重启／task重试 | 3／0／0 |

只读机器预检和四模块检查后，在Ray初始化前核对Arrow／Ray／psutil精确版本及NumPy的`>=1.25,<2`范围。
NumPy、Arrow、psutil来源均在新私有
driver venv；Ray复用base driver。运行后base的2.4.6／24.0.0／7.2.2／2.56.1保持原样。
依赖安装与Ray原件均在数据盘，没有访问模型或数据库。后续私有依赖准备补禁pip配置和
安装目标／额外索引覆盖；已经执行的控制与原件保留原样。

五项结果确认正常5行／batch2的三个块完成且driver不get prepared tensor；CPU及模拟model异常
先确认、后归还；部分确认失败保留48字节／4 work与payload引用并拒绝同池HSE；真实submit后
丢driver确认由串行actor fence收尾，确认后同HSE可恢复。模拟model的`synchronize`没有执行CUDA。
各项快照、driver get类型及确认／归还事件顺序保存在机器记录。

五项共创建12个actor，每项PID/birth退出核对通过。包含依赖准备、预检、driver与Ray运行时的
35个自有PID/birth均退出；释放lease前Ray head为0，随后独立复查确认controller及本任务lease也已退出。
正常运行无总墙钟截断；逻辑额度和512 MiB object store不等于全部RSS／pinned／device内存上限。

此前原件分别保留：首次旧能力清单缺组，Ray前停止并释放lease；第二次因其他owner持锁，
controller尝试准入但未取得lease，未启动依赖准备／预检／测试／Ray子进程；第三次base版本不符，
0个用例执行、Ray前停止并释放lease。
第二次旧观察器把外部Ray head误记为`cleanup_unconfirmed`，第三次setup错误的旧计数算成-1通过；
公开记录同时保留原值与正确分类，后续私有控制修正显示，未改生产source或掩盖失败。
最初SSH旧配置失败及自动审批拒绝均未执行上传／测试；后续可信授权解除原限制后，相同拒绝调用
只重试一次获准。原始归档的SHA与每次状态在机器记录中可对应。

本次只支持固定47f组合代码的实际Ray Core软件／故障语义，不验证GPU吞吐、完整延迟、物理副本、
CLI／pool factory、Daft source或PG接入。A+B+C生产代码与47f相同；A单独HEAD没有实际Ray运行，
不把本次成功移作A单独资格。

<a id="integration-slices"></a>

## 独立集成提交与挑选顺序

原47f及本分支历史不改写；三片在`fix/hse-static-integration-slices`从3e重新构造，
[该分支的交接与逐片记录](https://github.com/3444374/ai-operator-execution-optimization/blob/d1db2ea9117e97db68bfa5be2c685b13d4d815d4/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md)
区分当前树与原组合证据。共同候选可先挑A，B／C分别另行决定。

| 片／提交 | 生产与测试的精确分配 | 依赖／验证 |
|---|---|---|
| A `d307bb9265a32e516052f3bea7d8206f8906c288` | staged_execution的owner／ref／异常确认／未知池、局部寿命及admission峰值；broker的时间原子校验／永久超大块拒绝；10 lifecycle＋broker时间／超大块／临时FIFO三项 | 直接从3e；保留source→CPU→model→wait和全历史扫描；29定向通过，共享478 run／477pass／1skip |
| B `7c47e2cc1c8e0e0e5afdf71f7f388019e3f574f3` | 仅broker活动集合／终态计数及转换、snapshot／release／invariant；不重扫历史＋retry/cancel两项；CPU成本原件／复算入口 | 在A上加入，不依赖C；31定向通过，共享480 run／479pass／1skip；仅CPU机制收益 |
| C `d1db2ea9117e97db68bfa5be2c685b13d4d815d4` | 仅提取A的model提交循环并在source前调用；1项派发顺序测试及局部fixture | 依赖A，不依赖B；32定向通过，共享481 run／480pass／1skip；A+C内存组合另有15／15通过 |

各提交同时同步事实入口，不改source／CLI／pool factory或主路线图。A+B+C四个生产／测试文件
与47f逐字节相同，均完成独立只读最终diff审查、隐私扫描和远端核对；未合并main。
正常路径完成屏障与强static对照保持，B／C实GPU及完整性能结论仍未验证。
已审核实际Ray包取`ed20923876d668d6b11b0e4005cca2c7caa89c12`；五项实际Ray检查与退出证明见上节。
共同候选仍分别决定A／B／C，本轮没有合并main或启动GPU性能验证。
