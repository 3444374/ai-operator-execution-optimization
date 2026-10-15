# 原生 LOTUS CLIP 图像检索接入

当前融合源码 `2afcaf47` 的[十格实际资格](#final-integrated-qualification)已完成，绑定六路径64图组合工作流和四格已有索引查询。
下方较早的六路径12／64／256图资格绑定 `06332d4f`，[PNG候选观测](#png-candidate)绑定 `eab9f11a`；
各轮保留自己的输入、配置与来源，见[RM／CLI说明](../../../../code/src/semantic_methods/lotus/README.md#原生-clip-图像检索)。

本轮接通固定 LOTUS1.2.4 的图片建索引及图文检索。图库、原生 Faiss 索引、在线图像／文本编码、
完整 top10 输出均经过 LOTUS；Direct、SemLoom static、SemLoom HSE static、Ray Data 和 Daft Native
接到公开 RM 执行接口。作者算子、索引、检索和框架源码保持。六条路径的 12／64／256 图及已有索引
新进程查询资格已通过。当前结果用于语义与数值资格，不作性能排名。

## 实际语义及模型身份

[官方多模态测试](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/.github/tests/multimodality_tests.py)
使用 `SentenceTransformersRM('clip-ViT-B-32')`、`FaissVS` 和 `sem_index`→`sem_search('bird')`。
该版本 `sem_index` 将列转成 list；`ImageArray` 的路径字符串因而不会保留图片 dtype。
原生 RM 不对这个 list 解码，SentenceTransformers 会把 str 当文本。本轮实际 SDK 负例证明这条分支；
不能仅运行路径示例就认定图片已编码。自有入口显式解码为 RGB PIL，放入一维 object 列，保留原始
RangeIndex 并另存稳定业务 ID。图片查询传 PIL，`bird` 查询传文本，原生算法继续拥有排序和输出组装。

模型与预处理资格来自实际文件、398 个加载参数、有效输入张量和完整向量，见
[模型身份](embedding_identity.json)。固定 ST revision 为 `327ab6726d33c0e22f920c83f2ff9e4bd38ca37f`，
cached OpenAI revision 为 `3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268`。两个权重文件 SHA 不同，
加载参数内容摘要相同：`968f8926f4522526f88da56b4d5f47aa41d7cc28220136e3ab830666a70c56e6`。
同 slow PIL 预处理的有效像素差记录为 0，12×512 stock／Direct 保存向量独立重算差为 0。
三个短文本探针的 token IDs 一致；这不代表所有 tokenizer 配置都相同。改名但同像素结果相同，
改为 magenta 内容后向量差记录为 0.1600715071。参数、像素及内容控制的原始张量未纳入紧凑 NPZ，
相应结论来自已执行源码和运行摘要；不称为对这些控制张量的离线重算。

使用单卡 RTX4090、FP32、512 维 projected／L2-normalized 完整输出，Torch 线程池 1／1。
LOTUS1.2.4、SentenceTransformers3.4.1、Transformers4.57.6、Torch2.12.1+cu130、NumPy1.26.4、
Pillow12.2.0、Faiss1.15.0；其他包按逐次原始观测记录。图库来自 COCO val2017 JPEG，先 eager RGB PIL
解码，再无损 PNG 传入批次执行器。输入文件摘要见[公开源清单](https://github.com/3444374/ai-operator-execution-optimization/blob/08e02fd00336164f940546e2fbb73e3f4773de4c/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/source_manifest.json)，不保存图片或权重。
stock 的外层 max_batch_size=64，原 ST encode 默认 batch_size=32；替换臂模型 batch_size=32。

## 公共接点与执行职责

[RM 协议](../../../../code/src/semantic_methods/lotus/image_rm.py)提供
`embed_batch(doc_ids: tuple[str,...], images: Sequence[bytes]) -> ImageEmbeddingResult`、`semantics` 和 `close()`。
bytes 为同一 RGB 像素的 PNG；RM 接收完整 float32 `[N,512]`、按 ID 恢复输入顺序，并检查 finite／单位范数。
完整图库矩阵是原生索引的要求，本入口显式限制至 5000 行，不是正式流式图像 runner 或 PG 接入。

| 接点 | 原行为及自有采用 |
|---|---|
| LOTUS RM／ST wrapper | stock 委托原 `_embed`；替换臂仅提供公开 `_embed`，原 query conversion 保持 |
| LOTUS `sem_index`／`sem_search`／FaissVS | 原生 Flat inner-product、持久化与完整输出保持；有序像素／模型身份及 index／vecs 文件哈希用 sidecar 核对 |
| Direct | 复用已有 slow `ClipImagePreprocessor` 和 `ClipTensorActor` |
| SemLoom static／HSE static | 复用已有 image driver、typed pool 与 HSE driver，不复制内部调度 |
| Ray Data | 原生 `map_batches` 解码／CLIP UDF 和 actor pool；原生图拥有执行与调度，自写图记录为诊断用途 |
| Daft | 惰性 `daft_native` 接纳独立 `adf8ea10` 后端；原生 RGB decode、stateful batch UDF、GPU 资源声明、完整消费和 close 保持 |

这些采用为工程决策；架构参照沿用[现有接入计划](../../../plans/系统架构.md)。本入口串行拥有本进程 LOTUS
settings，原生 cache 关闭。未资格跨查询并发、filtered refill 或 `sem_sim_join` 的子集及缺邻居行为。
本任务是 CLIP 检索；VLM `sem_filter/map`、相关性质量、PG、跨 Job 调度与新的优化算法均未验证。

## 验证状态与计时

`lotus-retrieval-20261014-d`：五臂各 12 图，一次组合工作流、三个在线图片查询和一条 `bird`。
保存的五份图库及在线图片向量相对 stock 全部差为 0；文本 CPU／CUDA 最大差为 5.96e-8、分数差为
2.98e-8。20 组完整 top10 的顺序、位置、ID 与分数通过独立复算；三个图片查询各自 top1 找回源 ID。
没有 caption 或相关性标签，因此不据 `bird` 排名声称任务质量。
26 项实际 SDK／确定性 fixture 检查通过、0 跳过；八次受监督调用全部成功，257.747 秒含 16.806 秒清理，
逐次自有物理退出确认、GPU compute 空、共享 lease 释放。
[独立审核重算](small_d_independent_review.json)记录原件与源码校验。

`lotus-retrieval-20261014-e`：64／256 图五臂各一次组合工作流，以及五臂共享 stock256 索引的新进程 search，
15 个模型 CLI 加一次预检全部成功。完整图库及在线图查询与同形状 stock 向量差为 0，
完整排序、位置、ID 和分数通过；已有索引路径只编码在线查询，未再次编码图库。
累计 763.510／1200 秒含 33.394 秒逐次清理，16 次物理退出确认、GPU compute 空、共享 lease 释放。
两轮共 20 个模型 CLI、60 图片／20 文本完整查询；全部原始值留存，不将重复源图片算作独立性能样本。
[规模与公开恢复独立审核](scale_e_independent_review.json)核对 49／124／9 原件成员、97 个完整矩阵及
全部 80 条查询。图片向量差为 0，文本向量最大差 5.96e-8、分数最大差 4.47e-8。

`lotus-retrieval-20261014-f2`：在整合源码 `8959175d` 上，Daft 原生图片执行通过 LOTUS 的
12／64／256 图组合工作流及共享 stock256 索引的新进程 search，四次 CLI 全部成功。
完整图库及在线图片向量相对同形状 stock 差为 0；原生 top10 的位置、ID、排序与分数通过。
四次自有物理退出、GPU compute 空及 lease 释放已确认，136.426 秒含 8.315 秒清理。
原 600 秒窗口包含首次 CPU 判定失败和修订间隔，总用时 581.682 秒；没有重新计时增加预算。
合并 d／e／f2 为 24 个模型 CLI、72 图片／24 文本完整查询。

首次 f 的预检和 SDK 都退出 0、物理退出及 lease 释放确认，但监督器错误要求 JUnit suite.tests=39，
实际为 39 个测试项及 21 个通过的 unittest 子测试，suite.tests=60。其 ValueError 失败原件保留，
当次尚未进入模型。f2 校验并复用这两份 CPU 证据，未重跑 CPU 检查或覆盖原失败。
独立后端的 9 图 GPU 与 13 项 SDK 证据见[Daft 原记录](../daft_clip_native_20261014/README.md)，
不计入上述 24 次 LOTUS CLI。当前 Daft 配置虽请求 `use_process=True`，实际原生模型在 driver PID
复用，未产生 owned child；请求字段与实际观察分别保留。

每格一次语义资格、无预热；前期准备方案中的性能重复在规模 run 前明确延后，不改变数值容差或失败判定。
每次最多 180 秒，e 累计最多 1200 秒、f／f2 共用最多 600 秒。GPU 使用严格共享 lease，`recover_stale=False`；不回收 foreign lease、
不停止 foreign 进程。清理未确认时监督 owner 保持存活，不启动新工作，避免死 owner 被其他旧 runner 自动回收。

CLI 分开记录 `index_s`、已有索引 `index_load_s`／每条完整 `sem_search`、`native_workflow_s`、
source 内容核查／解码至完整查询输出的 `application_e2e_s`、启动及 evidence 写出。
原 `sem_search` 实际加载索引的时间仍计入查询；`index_load_s` 只记录 `load_sem_index` 注册行为。
默认使用低开销单调时钟，可选细计时借用已有模块，不新增计时平台。源码中Ray Data每次图片RM新建图，
Daft使用stateful wrapper，其他内置图片路径在workflow对象内持有模型；实际加载次数未独立采集。非stock文本RM位于CPU，stock文本位于CUDA。
`model_startup_s` 包含身份核查、stock构造和runtime启动，但不保证覆盖Ray／Daft消费图时延迟发生的模型构造。
原 `application_scope` 中的“model startup separate”不能据此解释为所有模型加载均排除在应用区间外，按[字段口径](../../../plans/reference/对照规范.md#measurement-field-scopes)及实际构造位置解释；原计时数值和字段保留。
替换 RM 包含完整图片 PNG 编码、执行器内解码与传输；
stock 直接使用 PIL，这些额外成本保持在实际计时内，不归因于调度。这些配置不可直接做性能排名。
样本少，P99／P99.99 为 unavailable；失败、超时和实际输出在相应运行身份下保留。

下表为 source-d 单次 256 图观察，单位秒；组合应用含源核查／解码、建索引及四条查询，已有索引应用
含源核查／解码及四条查询。CLI准备区间另报；查询内发生的模型构造仍在原计时内，原始每条 `sem_search` 时间见压缩观测。

| 路径 | 建索引 | 组合应用 | 已有索引应用 |
|---|---:|---:|---:|
| stock | 2.132 | 3.692 | 1.745 |
| Direct | 30.752 | 32.517 | 2.276 |
| SemLoom static | 35.271 | 37.431 | 4.009 |
| SemLoom HSE static | 33.703 | 35.829 | 5.855 |
| Ray Data | 42.183 | 73.228 | 29.483 |

Daft 后续 f2 的单次 256 图建索引为 33.247 秒、组合应用 35.124 秒、已有索引应用 4.571 秒。
这四次使用新增后端后的 source-f，原始逐阶段观测单独保留，不用于跨时段性能排名。

[可选计时接点本机核查](timing_hook_local.json)使用 `bbbd8a1b` 的原模块，两个实际 span 的成功／失败四事件
保留，首个业务异常保持，默认未创建 writer。该核查只有本机 API／事件写出，无 LOTUS 模型或 GPU；
可选模块需要先由整合分支接纳，本轮 source-d／source-f 的实际模型资格始终关闭细计时。

同图 stock 图库 batch12 与在线 batch1 的向量最大差为 9.4e-5～2.4e-4，各臂匹配实际 batch 形状仍为 0 差。
因此资格只要求同样 batch 形状的跨执行器数学等价，不宣称 CLIP 跨 batch 形状恒等。

## 原件、失败与复现

[精简原始观测](observations.json.gz)保留完整逐次时间、RM 计数、top10 输出与监督状态。
[向量 NPZ](vectors.npz)按实际内容 SHA 无损去重，[逐运行映射](vector_map.json)可以恢复所有保存的完整矩阵；
没有挑选有利元素或只比较截断输出。运行原件、命令、日志、原生 index／vecs、未公开的控制材料在独立
artifact 目录保留；[原件映射](https://github.com/3444374/ai-operator-execution-optimization/blob/08e02fd00336164f940546e2fbb73e3f4773de4c/experiments/results/diagnostics/lotus_native_image_retrieval_20261014/originals.json)按运行 ID、成员名、字节数与 SHA 定位，不上传私有大包。

首次 `lotus-retrieval-20261014-a` 在资产预检失败，源 tar 中的 macOS AppleDouble JSON sidecar 导致
UnicodeDecodeError，未进入模型／GPU；原失败保留。新 source-b 排除该元数据，预检及实际 SDK 通过，
下载固定公开 ST snapshot 后释放自己的 lease。固定 source-d 的 11 个运行代码／测试摘要及
source-f 的 14 个摘要见[源码身份](source_identity.json)。source-f 为 `8959175d` 的代码归档，
其中 Daft 三份源码与已审核／实测的 `adf8ea10` 对应 blob 逐字节相同；source-d 的 11 份适配文件保持。
基线 SHA 不代替当次适配代码的实际身份。

公开证据可在安装 NumPy 的环境重算：

```bash
python experiments/results/diagnostics/lotus_native_image_retrieval_20261014/verify_evidence.py
```

[任务 CLI](../../../../code/scripts/experiments/run_lotus_image_retrieval.py)支持 `index/search/combined`；
模型及运行路径放 Git 外，[模型资格 CLI](../../../../code/scripts/experiments/qualify_lotus_clip.py)
先生成实际摘要匹配的 embedding identity。完整调用说明见[LOTUS 目录](../../../../code/src/semantic_methods/lotus/README.md)。

## 集成接纳与关闭修补

固定 `06332d4f01876efa10b7dc23acbeeeaf44ece7b4` 普通合入 `da379b25` 后，原 Daft 交付
`adf8ea10` 的三份代码／测试 blob 在接纳时与 `8959175d` 逐字一致。集成另修四份运行文件：
两个 CLI、图片 backend 与 Daft backend；原 source-d／source-f 摘要及其六路径实际模型资格保持，
这些测量不改挂到关闭修补后的源码。

部分启动立即请求逐 actor 终止并保留 cleanup-only 责任；SemLoom 关闭先封口，失败后不复用。
Ray Data 完整 EOF、失败或取消都尝试 iterator close 和原生 executor shutdown，未知完成状态拒绝复用；
Daft iterator-open 失败先保留未知工作。实际执行／取消首错保留，真实关闭错误优先于可选计时诊断，
两份 CLI 的向量、JSON 和写出收据失败分别保留，继续尝试独立输出。终止请求不代替物理退出证明。
图库后在线查询继续使用默认 HSE static，未接通可选派发策略；关闭互斥不改变原生 batching／排序。

[集成核查](integration_checks.json)：61 项本地接点／故障用例为48通过、13条件跳过，其中22项新生命周期回归；
另六个已有图像／计时模块58项为57通过、1条件跳过。两组路径无重叠，共105通过、14跳过；
既有计时契约仅24次本机固定HTTP回复，模型／GPU／新服务器负载0。初次沙箱 bind 拒绝及缺 pandas
的两个本地用例失败原日志保留；私有隔离测试依赖和本机fixture权限补齐后按同源码通过，未改服务器环境。
公开原件离线复算96条完整查询、12个去重矩阵可恢复116份逐调用完整矩阵，图库最大差0；
两次模型前失败仍保留。当前融合版实际 LOTUS／Daft／Ray 与 GPU 关闭异常资格尚未重跑。

两轴独立终审在冻结候选 `b3183c32` 上各0必修；发布接纳范围与原实测来源边界保持。

<a id="png-candidate"></a>

## PNG 等级1与固定 HSE 容量候选

实现 `eab9f11a0338ef632f208dd178edcf87cd0a9799` 保持省略参数时的原 Pillow 调用；
`--png-compress-level 1` 只在替换 RM 的无损编码处传入等级。stock 继续使用原生 PIL。
所有替换后端共用该接点，本次真实模型资格只覆盖 Direct／HSE。HSE 容量及块上限显式固定为
58,110,400／29,055,200 bytes；不按新 PNG 大小推高，也不改变 batch、窗口或输入。
超大块沿用原驱动 `BufferError`，实际通过／拒绝由软件回归覆盖。

本地94项回归与独立源码审核通过；匹配服务器实际99项 SDK／fixture 测试通过，零跳过、零失败。
两个等级1的12图资格通过后，依序运行 Direct 默认→HSE 等级1→HSE 默认→Direct 等级1四个256图格。
单卡、FP32、batch32、HSE窗口2／模型worker1、Torch线程1／1、图像／模型／slow processor身份与旧资格相同。
每格一次、无显式预热。8个监督调用（含两个CPU检查）全部正常退出，逐次物理清理确认，lease释放；
总墙钟344.989秒／1200秒，包含16.712秒监督清理。每次模型调用限180秒，未超时。

[候选原始观测与恢复映射](png_candidate.json)保留每格结果、全部阶段时间、配置、原件成员摘要和实际源码身份。
30个完整矩阵逐字节恢复自原[向量库](vectors.npz)，不新增重复数组。六个图库及18个图片查询相对stock差为0；
6个文本查询相对stock CUDA最大差5.96046448e-8。24条完整原生top10排名、位置和业务ID相同，分数在原1e-5容差内。
同臂256图默认／等级1的五个完整矩阵逐字节相同。原生图库matrix也在私有备份中，已与RM完整输出核对。
[独立证据审核](png_candidate_independent_review.json)核对76个原件成员、1054个Git源码blob及上述全部输出，无必修问题。

下表为当次256图观测，单位秒；预处理是各批 decode／resize／normalize 墙钟之和，不是独立纯解码或应用关键路径。
组合应用包含源核查／解码、建索引和三图片＋一文本完整查询；启动及证据写出另计。

| 路径 | PNG | 图库编码墙钟／CPU | 预处理累计 | 建索引 | 组合应用 | 启动 |
|---|---|---:|---:|---:|---:|---:|
| Direct | 默认 | 25.994／26.009 | 4.615 | 31.017 | 32.729 | 16.824 |
| Direct | 等级1 | 12.517／12.529 | 5.056 | 17.962 | 19.516 | 21.589 |
| HSE | 默认 | 26.830／26.895 | 4.949 | 34.252 | 36.358 | 39.286 |
| HSE | 等级1 | 11.981／12.018 | 5.298 | 19.734 | 21.667 | 33.361 |

编码CPU记录使用进程累计时钟，包含该进程并发线程的CPU用时，不是独立主线程计时。
图库 PNG 从121,203,712增至127,821,765 bytes（+5.4603%）；HSE默认／等级1的实际encoded峰值为
32,801,033／34,348,574 bytes，均在固定容量内。RM仍先保存完整PNG列表，broker额度不涵盖该列表或序列化复制。
Direct／HSE继续使用已有 host copy、actor service telemetry，HSE另记录submit；纯解码、跨进程 transport、
H2D／kernel／D2H未分拆，详细CUDA计时关闭。原始四条查询与完整CLI时间均在候选JSON中，不能只拿短查询代表建库工作。
这些单次值提供输入编码成本的观测依据，不作后端性能排名、可靠P99／P99.99或吞吐结论，也不从旧JCT扣除CPU差值。

候选前的CPU诊断仅完成5个格（默认一次、等级1与0各两次）。默认图库编码26.061秒，等级1为11.593／11.790秒；
等级0为4.685／4.700秒但总字节增74.9207%，未做等级0模型资格。1280次像素相等检查和4个默认／显式6字节探针
来自执行源码及原摘要，PNG／RGB控制张量未保存，不称这些控制的离线逐字节重算。
首次错误将E清单与D文件摘要比较，模型／编码前失败；修订共用原300秒窗口，末个默认格超时，未完成两整轮。
两份失败原件、全部完成格和剩余额度算式在候选JSON中映射；后续GPU是另行授权的1200秒窗口。

私有备份含76个成员、完整NPZ、原生index／vecs、命令及日志；Git只保留紧凑观测和复算入口。
备份后的即时lease检查误用文件名，原字段不用于结论；后续正确文件名的只读快照及监督器正常释放记录均保留。
独立交付时尚未整合；本次参数接纳见下方记录。原六路径资格、厂商源码及其他正在测试的工作树保持原来源。

### 可选 PNG 参数的集成接纳

在`e50dd2b9`后普通合入`0faf41eb3c0ea39585ce34b5eae6ffb412f95388`，实现来自`eab9f11a`。
默认仍不传Pillow压缩参数，stock原生PIL路径保持；Direct、HSE和其他替换路径共用同一RM编码选项。
这降低的是共享输入适配开销，不作为SemLoom独有调度或系统创新。

本次仅运行本地确定性回归：7模块103项全通过，SDK／服务器／HTTP／模型／GPU执行0。
首轮85通过／4失败来自两侧fixture未初始化融合字段或未返回流水线观测，原日志保留；
先修测试替身并补默认容量与拒绝后未知工作拒复用；原生产生命周期未回退。
独立审查又发现新增观测getter会吞取消：修前6个首错条件失败，成功关闭时3类取消还会错误返回0；
接入既有首错保留链并独立尝试全部清理。另3个纯timing先错反例确认真实取消须优先，普通getter诊断继续保留成功。
修后13个新增条件与旧90项合为上述103项，不与前轮90或原候选94／99相加。
SemLoom互斥入口／close、Ray Data实际iterator及graph关闭、HSE源关闭与文本Ray清理保持`e50dd2b9`。
[集成核验](integration_checks.json)新增独立PNG小节；原六路径的复算结果与旧失败继续保留。

只读复核76个私有原件成员、1054个固定实现Git源码blob、30份完整RM矩阵与6份原生vecs；
24条候选完整查询及原96条查询分别复算，未重新启动SDK或模型。
原模型测量仍绑定`eab9f11a`，只覆盖Direct／HSE等级1；容量58,110,400 bytes、块上限29,055,200 bytes、
batch32、每格一次，既有默认观察及CPU失败均未覆盖或删除。
按`100 × (默认application_e2e_s − 等级1application_e2e_s) / 默认application_e2e_s`，
当次Direct与HSE应用区间分别降低40.3704%与40.4065%；model_startup_s另列，不纳入该式。
这不是含启动的完整总时收益、六路径新参数性能资格、可靠尾分位或稳态结论。
融合后的模型／完整SDK资格未新增；文本Ray清理实库补验由原owner接续，本次没有调用或干扰。

Standards／Spec两轴对`3e4af0bc`终审均0必修；终审后只填写核验收据，生产、测试与原观测保持冻结内容。

<a id="final-integrated-qualification"></a>

## 最终融合源码的实际资格

`lotus-final-image-2afcaf-20261014-j` 使用固定源码
`2afcaf475b8a22a7467f11f4d2303c9c124bd32f`，不把旧模型结果转记到新提交。
实际 LOTUS／Daft SDK 与确定性 fixture 共 151 个测试项、37 个通过的子测试，零跳过、失败或错误；
JUnit 的 `tests` 汇总为188（含37个子测试），实际 `testcase` 节点151个。源码归档含1078个文件，
17份图像运行文件与先前检查版本逐字节一致。

本次十个模型 CLI 为六路径各一次 64 图组合工作流，以及 SemLoom static／HSE static、Ray Data、Daft
各一次新进程已有索引查询；后四格共用本轮 stock64 原生索引，前后文件摘要不变。
每格三个在线图片查询（ID 139／2587／6723）和一个 `bird` 文本查询，完整经过原生 LOTUS
`sem_index`／`sem_search`／FaissVS，仅公开 RM 执行接口接入替换后端。

独立离线复核全部 46 份完整矩阵和 40 条完整 top10：六份 `[64,512]` 图库及 30 份图片查询
相对旧、本轮同形状 stock 向量差为 0；10 份文本查询最大差 `5.96046448e-8`。
排序、位置、业务 ID 与分数均通过原容差，三个图片查询各自 top1 找回源 ID。
复核原生 index／vecs 与 RM 全量矩阵一致，没有相关性标签或召回率评估。

[精简结果](final_image_qualification.json)保留每格启动、源核查／解码、建索引或注册、四条完整查询、
组合应用、CLI、监督调用及结果写出耗时，另有逐 RM PNG 墙钟／进程 CPU 与已有流水线观测。
替换臂 PNG 等级1、stock 原生 RGB PIL，FP32／batch32、Torch 线程1／1；HSE 容量及块上限为
58,110,400／29,055,200 bytes。Ray Data 图片 RM 每次声明新原生图，Daft 声明跨图 stateful wrapper；
本轮没有独立模型载入／销毁计数，不从查询顺序推定复用次数。stock 文本在 CUDA，替换臂文本在 CPU。
每格仅一次、无预热，P99／P99.99 为空，不据此作性能排名；纯解码、transport、H2D／kernel／D2H 未分拆。

三个 CPU 监督调用及十个模型 CLI 全部退出0，逐次物理清理、两个控制器的进程出生身份退出、
最终 GPU compute 为空及共享 lease 释放已确认。CPU 阶段37.468秒、模型阶段411.811秒，合计449.278秒；
健康工作不设累计或单格墙钟中止时限，30秒清理窗口只用于退出、故障或取消后。

命令、日志、完整 NPZ、原生索引及计时原件留在 Git 外；结果记录三个归档的成员数、字节数与 SHA。
CPU 归档23个成员、模型原归档93个成员，另补10份既有写出计时记录；补取未重跑模型或改写原归档。
独立复核摘要已绑定到结果，旧失败及旧测量保持各自运行身份。当前结论只覆盖上述固定源码、输入与配置。
仓库外原件可按本轮运行标识向项目维护者获取，并以精简结果中三个归档的SHA-256核验。
