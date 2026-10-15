# HSE static集成切片

当前 `bdae61bb` 的身份与 B/C 恢复见[本轮基础修正](#foundation-restoration-20261015)。以下初始交接及原 A-only 资格保持各自固定来源，不改称本轮实测。

本分支从`3e7bd54b`构造可顺序挑选的A／B／C；已发布`47f7800c`与原修复分支保持。
A仅修正确性，B历史扫描优化、C ready优先分别待加入；生产行为和测试依赖如下。
[原完整审查与失败证据](https://github.com/3444374/ai-operator-execution-optimization/blob/47f7800ce5fbe280c5075165453b2c6dec5437f4/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md)属于47f的完整组合，不能当作当前A含B／C的证据。

| 片 | 精确代码／测试 | 依赖与默认影响 |
|---|---|---|
| A正确性 | staged_execution的提交前owner登记、payload至完成证明持有、异常确认／未知池保护、局部引用清理和admission峰值；stage_broker的lease时间原子校验、永久超大ready拒绝；10项lifecycle＋broker时间／超大块／临时FIFO三项 | 从3e直接应用；保留source→CPU提交→model提交→wait、FIFO和全终态历史扫描；CLI默认direct_dependency保持；正常HSE无新增确认RPC |
| B历史扫描 | 仅stage_broker的活动集合／终态计数及其转换、snapshot和不变量；broker不重扫历史及retry/cancel计数两项 | 可在A上独立加入；不改HSE顺序；保留默认历史与duplicate检测；原 A-only 交付未包含 |
| C ready优先 | 将A已修正的model提交循环提取为helper；source.next前及原model派发位置调用；1项顺序测试的局部source／GPU fixture | 依赖A的owner/ref登记与公共fixture；不依赖B；原 A-only 交付未包含 |

A不引入新worker、模型、source、batch、输出schema或资源选点。逻辑byte/work额度不代表全部
RSS、Arrow／Ray副本、pinned对象或device内存上限。临时FIFO等待仍保留，不增加fit-aware策略。
A的异常归还仍等待串行CPU ready／GPU synchronize完成证明；确认失败保留lease与ObjectRef，
后续同池HSE拒绝。共享Core和broker本地检查见[逐片机器记录](integration_checks.json)。

[CLI／pool factory固定源码交接](https://github.com/3444374/ai-operator-execution-optimization/blob/ed20923876d668d6b11b0e4005cca2c7caa89c12/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#integration-handoff)列出47f上的外层异常清理调用点。
这些文件由image owner独占，部分构建清理及连接容量说明已在`2f116d4a`完成，并以
[图像集成`1b90c1ea`的固定SDK记录](../image_file_sdk_20261014/README.md)交付；本切片不重复修改。
[零模型实际Ray包](https://github.com/3444374/ai-operator-execution-optimization/blob/ed20923876d668d6b11b0e4005cca2c7caa89c12/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#ray-validation-package)在ed209238独立发布，26成员、4 CPU／0 GPU／512 MiB
object store、5项真实Ray用例。其生产模块匹配完整47f；不能借此认领A集成HEAD的实库验证。
原A交接时尚未取得实际窗口；当前集成的独立A-only资格见[下方记录](#integration-a-qualification)，不借组合结果。
健康运行没有总墙钟截断，单操作60秒、故障确认30秒、自有actor退出30秒分别计时。


<a id="integration-a-qualification"></a>

## 图像集成上的独立 A-only 资格

固定测量源码 `45da0a5451bfcf785d98b9ab5336054bdbaf199f`，图像基点 `1b90c1ea39e12c9fff2e2265aae003e25aa9d618`。
A 的两份生产文件和两份回归与 `d307bb92` 逐字节一致；实际 Ray 测试入口逐字取 `ed209238`。
B 历史扫描优化与 C ready 优先均未引入，保留 source→CPU→model→wait、FIFO、完整历史与
CLI 的 direct_dependency 默认。其余生产源码以及 native N/S、IMLane、图像 reader／CLI 保持图像基点。

[独立机器记录](integration_a_verification.json)绑定本轮源码、版本、逐成员摘要和退出证明；
[19成员公开原件](raw/integration-a-sdk.tar.gz)保存实际用例、预检、两个driver、观察器和本地回归。
179 个 Git 输入全部与固定源码逐字节核验。原
[组合五项Ray资格](https://github.com/3444374/ai-operator-execution-optimization/blob/f735fa6a7f223ff55addd69c69907ffaabcf722e/experiments/results/diagnostics/hse_static_lifecycle_20261014/README.md#actual-ray-checks)
仍只属于47f／A+B+C，没有更名或复用为本轮。

| 本轮检查 | 结果与范围 |
|---|---|
| 本地受影响组合 | 63 run、62通过、1个真实Daft/Arrow适配因缺库跳过、0失败；原A的29定向检查另列 |
| 独立A-only实际Ray | 5通过／0失败／0错误／0跳过：多返回与尾批、prepare／模拟model确认后归还、未确认引用和额度保留及同池拒绝、submit确认丢失后的串行fence |
| 共享准备数据适配 | 1通过／0跳过，分别使用真实Daft和Arrow表核对合并buffer、行身份及释放；Ray transport为替身，没有HTTP／模型调用 |

Ray driver 为 Python 3.12.3、NumPy 1.26.4、Arrow 16.1.0、Ray 2.56.1、psutil 7.0.0；
共享数据driver为 NumPy 1.26.4、Arrow 24.0.0、Daft 0.7.21、Ray 2.56.1、psutil 7.2.2。
均复用现有环境，没有安装依赖。Ray请求4 CPU／0 GPU、512 MiB object store、同时至多3个
CPU fixture actor；这些声明不构成OS CPU配额、总RSS／pinned／device内存上界。
正常检查没有总墙钟截断；单操作无完成60秒、异常确认30秒、自有actor退出30秒分别计时。
无benchmark warm-up，不作性能比较。

本轮 `2026-10-14T07:26:44.382326+00:00` 开始、`2026-10-14T07:27:31.694777+00:00` 完成，controller执行
47.312秒；Ray unittest 36.762秒、数据适配 unittest
3.078秒分别保留，不能相加为方法JCT。两条预期的prepare错误通知原样保留于脱敏日志。
12个fixture actor的PID／birth退出核对通过；27个自有子进程均退出，没有强制清理。
`2026-10-14T07:32:32.055281+00:00` 独立复查27个出生身份及controller均未存活、自有lease已释放；
没有移除他人的lease。模型／GPU／HTTP调用均0。

fence仅适用于独占同一pool、同提交者的同步串行actor、restart／task retry均0。
CPU模拟model的synchronize不证明CUDA完成；弱引用记录被回收也不证明actor退出。
真实CLIP质量／性能、device完成／退出、完整CLI／pool factory／sampler进程树停止、
全物理内存与副本上界、PG／Daft source接入仍未由这些检查覆盖。B／C没有默认启用或新增收益证据。

完整私有原件保留：320成员、599251字节，SHA-256
`5e0d58c93808108aba6871ca10e806684a1eb97b44795d2c0506577e8a92ea52`；公开19成员副本逐项映射原始与脱敏摘要。
最初只读探测包装器返回1、未保留stderr，原因未确认，记录该限制；当时没有上传或执行SDK。
随后只读确认外部owner持lease时未启动；其释放后由本controller原子取得共同lease，未抢占窗口。

<a id="literal-gpu-handle-guard"></a>

## 借用池的重复 GPU handle 入口校验

在已发布 `58462a0d` 之后单独补 HSE 入口：同一 Python handle 对象重复出现在
`gpu_actors` 中时，在 source 读取及 actor RPC 前抛出 `ValueError`。不静默去重或改变额度，
broker、标准 pool builder、默认 direct_dependency 和本节之前的固定测量源码保持。
[两项专属回归](../../../../code/tests/modalities/image/test_hse_pool_resource_identity.py)修前一项因读取
source 而失败，另一项正常；修后连同旧 HSE 生命周期、部分确认及 broker 回归共31项通过、0跳过。
两个不同替身 actor 的四行输出仍恰好一次。这里只核对重复 Python 对象；反序列化或包装后仍指向
同一 Ray actor 的不同 handle、实际设备身份与共享物理 GPU 未识别。新增服务器／模型／GPU／HTTP
执行均0，新 guard没有借用上方 `45da0a54` 的实际 SDK 资格。

<a id="source-iterator-closure"></a>

## HSE 上游迭代器关闭核查

独立固定 `da379b2577118d58b890cff3fca6400af7bda8f2`，
`staged_execution.py` 原 blob 为 `926ba2c4a0fe1a6fa2c4e424f9e133c4c36a9e91`。
后续远端 `015a3855e79fe853f49cff40d00e375d6ca3abb3` 未修改该文件。
完整函数创建的 `batches` 没有统一关闭；CLI 只拥有 DataFrame 和借用池，没有取得该迭代器的关闭责任。
只读核对 [Daft v0.7.21 的 to_arrow_iter](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/daft/dataframe/dataframe.py#L556)
及 [API 装饰器](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/daft/api_annotations.py#L23)：
没有针对消费方行物化异常的自动关闭处理。此项是源码核对，不是实际 SDK 运行。

[新增行为测试](../../../../code/tests/modalities/image/test_hse_source_closure.py)调用完整 HSE 函数、真实 broker 和 audit，
Arrow scalar／iterator 与 Ray actor／wait／get 使用确定性替身。
原始附件的缩减控制流仅作线索；本轮完整函数中，行物化失败后保留同一异常及 traceback，再执行
`gc.collect()`，修前 generator 仍为 `GEN_SUSPENDED`，显式 close 才执行 finalizer；修后已为 `GEN_CLOSED`。
单个 `finally` 在正常 EOF、部分迭代失败和取消后尝试一次源 close；首执行错误优先，关闭再报错只附加异常类型。
关闭失败后仍运行既有串行 actor fence、queued cleanup 和未知 lease／引用保留；正常运行不增加 actor fence。

本机 Python 3.12.6／NumPy 2.5.0：新增8个测试方法、16组行为条件在修前全部失败，
修后连同既有 HSE／可选派发、actor identity、image audit／CLI owner、descriptor 和 broker／dispatch
共10模块102项通过，0失败／0跳过。正常／空源、保留 traceback、部分读取、next 错误、
prepare／model取消、close再次抛普通异常／取消，以及已有pending model的失败确认与同池拒绝均覆盖。
[机器记录](source_closure_checks.json)绑定源码blob、版本与逐成员摘要；
独立规范和需求两轴审阅另列于[脱敏原件归档](raw/source-closure-local.tar.gz)，与主回归不相加。

修补只改 HSE 编排与新增测试；source先行、FIFO、全终态历史扫描、CLI direct_dependency默认和
可选设备派发 `None` 保持。原生baseline、文本native_calls和LOTUS图像backend未修改。
本轮服务器／真实Daft与Ray／模型／GPU／HTTP运行均0。源 close 返回、generator关闭和替身
fence不证明设备完成或actor物理退出；真实源内部资源停止、CUDA完成、完整CLI退出及性能仍待其独立验证。

### 源关闭修补的集成核查

固定 `91803fe8f880b6804ebbb2c74f569c7dcc95624a` 普通接纳在 `4db1aec2` 后；
唯一生产文件 `staged_execution.py` 与原交付 `0532aa8f` blob 保持，没有覆盖 LOTUS／Daft
接入、计时和原数值材料。元数据冲突保留双方记录，[本次核查](source_closure_integration.json)
为11模块124项全部通过、0跳过：原10模块102项加22项LOTUS接入生命周期；
与前次核查存在重叠，不能相加。原8成员6283字节归档逐SHA核对，修前失败完整保留。

上游 iterator 的 EOF／异常／取消均尝试关闭；执行首错、既有 actor fences、queued cleanup
及未知 lease／ObjectRef 责任保持。source先行、FIFO、dispatch默认None未变。
这一步只有本地确定性替身，没有真实SDK／服务器／HTTP／模型／GPU执行；
源close或替身fence不证明真实源内部停止、设备完成或actor物理退出。

两轴独立终审在冻结候选 `01b0e34f` 上各0必修，原 SDK／物理完成边界保持。


<a id="foundation-restoration-20261015"></a>
## 真实身份、既有小优化与结果观测修正

实现固定 `bdae61bb1f067224ff400ef526f6dba70443851d`，基点 `671a22dd4285319209396ece3572bb89ff5ab74e`。
B 来自 `7c47e2cc1c8e0e0e5afdf71f7f388019e3f574f3`，C 来自 `d1db2ea9117e97db68bfa5be2c685b13d4d815d4`，
只按 hunk 适配，保留当前 model_dispatch／unknown 责任；历史保全不等于此前已集成，也未恢复旧 worktree。

原 LOTUS HSE 的 `SlowClipPrepare → ClipImagePreprocessor → decode_rgb_image` 实际使用 PIL，
descriptor 却借用默认 torchvision transform 身份。此为已复现的身份标注问题，没有据此判定向量已错。
真实预处理器现在发布 decoder／版本、实际 processor revision／类／包版本／配置摘要、NCHW／FP32／尺寸及执行变体；
ready→pool→descriptor→worker 完成一致核验，v2 缺字段或不匹配在 source／解码前拒绝。
Fast 的 torchvision 身份独立保留；原像素运算 AST 不变，真实 slow CPU 张量也逐字核对。

显式 transform 使用 v2；所有新 encoded descriptor 的 work-v2 含 decoder、执行变体和完整 transform。
合法旧自定义池／旧 transform helper 仍保留原 transform-v1 默认；直接 work helper 未给新身份时保留原 work-v1。
旧校准与旧证据属于旧来源，不能混为本轮 v2。模型签名与向量数学不变，不重新量化或改变 slow rescale 顺序。

broker 以活动集合及终态计数检查现有不变量，保留历史、重复检测及 release_terminal 的调用方身份不复用约定；
ready 派发前移到 source.next 前，不增加 source 线程、future、控制器或缓存。
OrderedMatrix 使用一次 ID→位置映射、预分配矩阵及 seen，保留非法矩阵优先于 ID 错误、错误前合法前缀、空输入错误与 finish 自有复制；
RM 仅在左侧 scatter，避免完整 RHS 高级索引临时矩阵。workflow 所有退出在 settings 退出后保留一次 owned 调用快照；
CLI 对 JSON／NPZ 分别确认成功后才释放对应记录，未知、未导出、写失败及写出期间新增后缀保留，编号和累计行数不重置。
这些释放不覆盖 request_gateway／persistent owner，不构成全任务 RSS 有界保证。

| 检查 | 实际范围与结果 |
|---|---|
| Linux CPU 定向 | 同一925成员源码包，302 passed／4条件skip／107通过子测试；真实 processor 与 Ray opt-in 后续单列，不把skip算通过 |
| 独立 A/B 审查 | 身份早拒绝问题已修；97项含1实际Ray条件skip、额外2项dispatcher源边界；与原broker共9600次状态转换逐步一致 |
| 独立 C 审查 | workflow后置回调／settings退出漏快照P2已修；52项全通过、8606次收集动作、88组RM差分与12组写出取消核对 |
| broker组件 | [全部三重复](https://github.com/3444374/ai-operator-execution-optimization/blob/3f8748fd273f5a0d5c99e31fa6663dc500a68af9/experiments/results/diagnostics/hse_static_lifecycle_20261014/restoration_broker_cpu.json)：1000／2000／4000历史、活动窗口1／8／32；4000窗口1中位0.795873→0.126429秒，仍保留4000历史 |
| NumPy组件 | [全部七次交错原值](https://github.com/3444374/ai-operator-execution-optimization/blob/3f8748fd273f5a0d5c99e31fa6663dc500a68af9/experiments/results/diagnostics/hse_static_lifecycle_20261014/restoration_numpy_cpu.json)：128／1024／4000／5000行；5000收集70.438→7.405毫秒，反序RM12.103→11.364毫秒；10类错误type/message一致，全部输出字节一致 |
| 分配与所有权 | 5000行collector持有embedding ndarray从5000个到1个；finish完整复制保留。反序RM tracemalloc峰31,564,994→21,192,214B，非进程RSS |
| 真实 CPU／GPU 与物理退出 | 18项真实CPU SDK全过；单4090旧／新四owner ABBA，264图／16真实forward、8完整矩阵逐字同；4个GPU actor及全部自有子进程退出，正常lease释放。仅embedding API资格，非新完整LOTUS／PG／稳态性能结论 |

NumPy最终测量的整份 image_backend 文件还包含当时 A 补丁状态；被测 OrderedMatrix AST 与固定实现一致，
image_rm／checked_matrix 与最终被测源码一致。原版逐字取基点；中间 Python 逐行 scatter 的退化轮、
初始并发CPU轮、缺pytest导入、hash fixture失败、独审反例和preflight命令位置错误均保存在仓外原件，未择优覆盖。
设备原件另经28项独立核验全过、0必修；运行覆盖 Transformers 4.57.6，未覆盖5.14.1。后续只读观察确认外层controller已退出，精确退出时点仍未知。
两类组件不合成端到端收益，少量设备轮不提供稳态／尾分位／方法创新结论，也不替代完整 LOTUS、PG 或多卡资格。

逻辑 encoded／ready 额度不含 RM 完整输入、原图 decoder scratch、序列化／输出副本和诊断；prepare 输出像素 work 不代表原图解码代价。
本轮不实现表示选择、GPU cache、uint8 延后展开或复杂学习器。后续有限候选及云端负例边界见[数据组织计划](../../../plans/数据组织.md#single-gpu-image-critical-path)。
服务器源码、机器报告、设备原值／服务器命令失败及退出证据保存在仓外运行标识 `hse-correctness-20261015`，
对应归档校验映射见[设备与退出摘要](https://github.com/3444374/ai-operator-execution-optimization/blob/3f8748fd273f5a0d5c99e31fa6663dc500a68af9/experiments/results/diagnostics/hse_static_lifecycle_20261014/restoration_qualification.json)。Mac组件各轮原值、历史原件、独审及中间失败另保存在仓外组件／审查目录，分别归档，不混作同一服务器包；两包均已核对独立Mac／服务器副本，按运行标识与校验值向维护者索取。


同任务 GPU 原值（秒；每 owner 一次预热、一次测量，源码与顺序固定，不作排名）：

| 顺序 | 源码 | 模型启动 | 预热完整调用 | 测量完整调用 |
|---|---|---:|---:|---:|
| A0 | `671a22dd` | 9.873292 | 12.051645 | 0.438164 |
| B0 | `bdae61bb` | 11.225503 | 10.147269 | 0.471367 |
| B1 | `bdae61bb` | 10.370828 | 12.437455 | 0.443743 |
| A1 | `671a22dd` | 10.199323 | 10.864620 | 0.453913 |

旧版两个测量均值0.446038秒，新版0.457555秒，本小轮新版慢2.582%；重复不足以判定稳定差异。
修正版的两个测量值未显示可主张的端到端收益；本轮仅取得同数学、真实设备与退出资格，不将CPU组件改善外推为GPU加速。
