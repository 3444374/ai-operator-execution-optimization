# 原始 SemBench Animals Q1 图像资格

本轮核对固定 SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2` 的 Animals Q1：
原 `ImageArray(ImagePath)`、`sem_filter("The image {Image} contains a zebra.")` 和 `count(*)`，
质量直接调用原 Species SQL 与原 Animals 聚合评价器。选用 LOTUS 1.2.4；上游 requirements 为 1.1.3，
因此是原查询／评价器在当前版本上的适配，不是完整同版本环境复现。CLIP／Faiss 检索与 Ecomm 不属本轮。

## 输入及来源

官方 [wildlife.zip](https://drive.google.com/file/d/1HG6tvXIA0BtpqbZqCR46oNSeY2yZ4Lko/view)
为 5,355,131,661 字节。先取完整中央目录，含 8,718 个原图及 650 个音频；注释表含 8,524 行。
在完整目录上执行固定上游生成函数与 seed 42、SF64，原选择含 10 个 monkey 样本及其余随机样本，
实际64图、SQL斑马真值3。未根据已下载文件抽样；只在选择固定后按 Range 取对应图像，未下载整包或音频内容。
生成时通过临时文件目录表达完整 archive catalog 的存在性；原筛选、抽样和位置函数不修改。
现有环境缺 openpyxl，私有准备脚本只用标准 OOXML 读取器取 Filename／Device／Species 字符串列，
交给原 pandas 抽样函数；不安装或更换依赖。原注释表、完整目录、抽样次序与准备脚本保留。

注释表及选中64图的 SHA-256 均与[作者 Mendeley v6](https://data.mendeley.com/datasets/6mhrhn7rxc/6)
公开文件记录精确匹配，所选文件依其 CC BY 4.0 授权并保留作者署名：Lorna Mugambi、Gabriel Kiarie、
Jason Kabi、Ciira Maina。不据此声称完整 Kaggle v2 与 Mendeley v6 相同；Kaggle 元信息许可 Unknown 单独保留。
固定运行 manifest digest：`04816fecdbad5c7f39b3483cb3a0fdac87c3bc840d22bb006944f8b21a589758`。
原图、源许可/目录响应、逐文件hash、生成记录与manifest在仓外输入／来源包，Git只放精简来源及结果。

## 检查设计与执行

问题是当前完整请求执行器能否保持原图像 Filter 的提示、像素、解析和关系结果，不提出新方法或调度收益。
CPU检查先于真实模型：初轮23项通过、0跳过，包含真实固定LOTUS／SDK、三执行者的gzip响应、原SQL评价、
缺元数据时的原失败留证及既有批次预检／响应重放。另有16项原有纯文本LOTUS／Daft／Ray真实CPU图回归通过、0跳过；不把多次复测相加。
首次统计字段、匿名key及原始压缩响应观察失败均保留；已修正，未重写失败运行。
规范与需求独审指出的压缩响应和失败总结问题已修，两轴已复核当前修复及入口文档，无剩余阻断；审查不代替实际测试。

模型固定为公开 Apache-2.0 的 `Qwen/Qwen2-VL-2B-Instruct`，revision
`895c3a49bc3fa70a340399125c650a463535e71c`，12个必要文件共4,429,603,990字节，
两片权重hash与官方LFS相符。现有vLLM0.25.1支持该架构，复用原driver／serving环境；首次CDN TLS失败及同目录续传保留。

使用单RTX4090、BF16、同一个服务、相同64输入及生成参数，依次执行原生LOTUS、Daft、Ray Data。
每臂仅一条首次完整查询；固定顺序、同进程及同服务有导入与服务热态影响，不是三个独立冷服务。
显式 `observation_mode=performance`，保留实际请求／完整响应体、用量、决策和HTTP时间线；
该模式仍有共同proxy、哈希与证据写入成本，不称零观测。模型API预热为0：不调用GenericLotusRunner构造器；
vLLM自身初始化／profile成本另列，不解释为没有内部模型启动工作。请求超时有限，健康完整查询没有人为总时限；
GPU服务从启动到物理退出全程持正常共享lease，`recover_stale=false`，不处置其他owner。

原生PNG请求可超过1MiB；当前SemLoom Core单项上限不足，原样全64图臂保持pending，不缩像素或拆请求绕过。
原生Daft的batch-UDF并行值及Ray的actors×async值分别记录；模型服务拥有内部批处理。
初轮声明HTTP额度4，但离线实测LOTUS／Daft／Ray峰值为4／5／2；Daft超限，不能视为匹配资源的资格。
后续修复去掉Daft多加的一个槽位，并在查询后按gateway同一单调时钟核对实际峰值；超限保留原质量和结果并失败。
修复后25项CPU检查通过、0跳过，新增16图延迟Daft容量检查及超限失败留证；不与原23项重复累加。
新增fixture首次因Daft进程全局线程数重复设置失败，改为各自独立测试进程后通过，失败日志保留。
准备、完整查询、查询后评价／持久化与服务退出分列；失败、取消或不完整覆盖停止该臂资格并保留原件。
原相对误差外还记录逐行TP/FP/TN/FN，防止COUNT中的错误相互抵消。少量query不报告可靠P99/P99.99或性能排名。

## 真实 VLM 结果

初轮源码固定 `45dd1524738e9ed40f54a39c222f2c75c5fe2986`，运行标识 `model-02`，
三条完整查询共192次真实模型POST。三者原SQL真值和预测均为3，原相对误差、绝对误差和MAPE均为0；
逐图补充审计均为TP3／TN61／FP0／FN0。此处“正确”指与本次原Species标签一致。

容量修复源码 `ce7d328514abc33969e7c08139c4af72c2b01236` 的 `model-03` 仅补Daft64图，
同输入／模型／processor／生成及服务配置，实际峰值4，质量仍全部一致；累计4查询／256模型调用。
未重跑未受影响的LOTUS／Ray，初轮Daft保留为语义有效、资源超限的诊断，不进入匹配额度的资格选择。
补轮是独立服务，与旧两臂的源码和启动身份分列；不将跨次单值当作性能对照。

| 后端 | 运行 | 完整查询秒 | 应用秒 | 实测HTTP峰值 | 资源资格 |
|---|---|---:|---:|---:|---|
| 原生 LOTUS | model-02 | 37.823664327 | 40.231047972 | 4 | 通过 |
| Daft native（初轮） | model-02 | 42.232154645 | 44.025995127 | 5 | 超限，原件保留 |
| Ray Data native | model-02 | 62.005999545 | 75.126611353 | 2 | 通过 |
| Daft native（补轮） | model-03 | 43.997942451 | 46.514915157 | 4 | 通过 |

“完整查询”从调用原Q1前至全部结果返回，包含该次源码核验／读取、ImageArray构造、PNG序列化、
后端执行图、请求及结果汇合；LM配置、Daft runner／Ray runtime初始化在查询前。
“应用”从框架／LM准备前至评价、证据持久化和本臂context清理后，包含这些额外成本；
调用前manifest预检及一次数据校验不在此计时内，因此它也不是全CLI墙钟。
服务启动初轮70.989094977秒、补轮60.447400036秒单列；服务物理退出未分摊给单次查询。
不能将HTTP往返解释为GPU kernel时间，不能把同一查询中的64个请求当作64次查询重复。
各臂实际请求体均为380,284／1,598,858／2,380,568字节（min／median／max），
总108,068,304字节；64条中63条超过Core的1MiB单项上限。实际每臂用量为81,307 prompt tokens与128 completion tokens。
这些是网关完整请求和服务响应的观测，HTTP额度是允许上限，并不保证每臂用满。

首次 `model-01` 因当前vLLM不接受旧启动参数 `--disable-log-requests` 退出，模型POST为0；
只改仓外controller为当前支持的 `--no-enable-log-requests`，以新运行标识执行，生产源码未改。
成功轮服务退出码0，自有存活PID为空，GPU回到1MiB，共享lease已正常释放且未恢复stale owner。
这只证明所列输入、模型和三个后端的原任务资格；SemLoom Core原样64图仍待容量支持，Ecomm另行安排。
少量查询不支持可靠P99／P99.99、稳态、性能排名、调度收益或数据库端到端结论。

## 证据与复算

[来源及配置](source.json)固定数据／模型／processor／资源身份；[输入次序与标签](input-selection.csv)
保留64个原始archive成员及编码／RGB哈希。[复算入口](recompute.py)读取仓外输入、原始运行及补轮各自的源码包，
核对64原图、全部实际PNG像素、消息多重集、各运行完整请求／响应／ledger／trace及逐行决策，
生成[机器结果](results.json)及[逐请求观测](requests.csv)。请求分布、用量与原始成员校验值由该单一入口计算。
响应留证包括完整body和status；响应headers没有另行归档，运行时透传与SDK解释仍由既有合同及gzip CPU资格检查。
LM的本地价格未配置，统计中的cost数值0不能解释为测得的零费用；能耗及完整GPU利用率未采集。

这些包在Git外按 `source.json` 的运行标识／字节数／SHA-256向项目artifact owner获取；
运行原件含首次失败、各次CPU日志、服务命令／日志／就绪与退出、实际请求／响应、进程资源观测和源码逐文件身份。
服务器原件与本地独立主机备份均保留；Git不嵌入图像或base64载荷。既有源码包绑定1094个执行文件。
独立来源包另含三个原准备helper、未改动的上游函数、原XLSX／中央目录、作者v6许可页面及文件响应。
其中 `reproduce_selection.py --input-archive <input.tar.gz>` 已离线重现原64行完整成员／Species／City／StationID次序，
模型及网络调用均为0；它与固定输入到结果的复算分开，不把整个数据版本假定为相同。

```sh
python recompute.py --raw-archive "$RAW_ARCHIVE" --input-archive "$INPUT_ARCHIVE" \
  --source-archive "$SOURCE_ARCHIVE" --retry-archive "$RETRY_ARCHIVE" \
  --retry-source-archive "$RETRY_SOURCE_ARCHIVE" --output "$OUTPUT_DIRECTORY"
```

该命令只需现有Pillow，不访问模型或网络。原始官方入口是
`src/run.py --systems lotus --use-cases animals --queries 1 --model <image-LM> --scale-factor 64`；
本次使用[有限适配入口](../../../../code/scripts/experiments/run_sembench_animals.py)调用其未改动Q1方法及评价器，
以固定manifest替换CSV定位，并单独配置LM，避免上游构造器预热。没有声称整套官方CLI／依赖环境原样执行。
