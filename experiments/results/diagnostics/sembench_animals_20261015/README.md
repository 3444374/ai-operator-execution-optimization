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
原图、源许可/目录响应、逐文件hash、生成记录与manifest在仓外原始输入包，Git只放精简来源及结果。

## 检查设计与当前状态

问题是当前完整请求执行器能否保持原图像 Filter 的提示、像素、解析和关系结果，不提出新方法或调度收益。
CPU检查先于真实模型：23项通过、0跳过，包含真实固定LOTUS／SDK、三执行者的gzip响应、原SQL评价、
缺元数据时的原失败留证及既有批次预检／响应重放。另有16项原有纯文本LOTUS／Daft／Ray真实CPU图回归通过、0跳过；不把多次复测相加。
首次统计字段、匿名key及原始压缩响应观察失败均保留；已修正，未重写失败运行。
规范与需求独审指出的压缩响应和失败总结问题已修，两轴已复核当前修复及入口文档，无剩余阻断；审查不代替实际测试。

模型固定为公开 Apache-2.0 的 `Qwen/Qwen2-VL-2B-Instruct`，revision
`895c3a49bc3fa70a340399125c650a463535e71c`，12个必要文件共4,429,603,990字节，
两片权重hash与官方LFS相符。现有vLLM0.25.1支持该架构，复用原driver／serving环境；首次CDN TLS失败及同目录续传保留。

计划使用单RTX4090、BF16、同一个服务、相同64输入及生成参数，先原生LOTUS，再逐臂Daft／Ray Data。
每臂仅一条完整冷查询，显式 `observation_mode=performance`，保留实际请求／完整响应体、用量、决策和HTTP时间线；
该模式仍有共同proxy、哈希与证据写入成本，不称零观测。模型API预热为0：不调用GenericLotusRunner构造器；
vLLM自身初始化／profile成本另列，不解释为没有内部模型启动工作。请求超时有限，健康完整查询没有人为总时限；
GPU服务从启动到物理退出全程持正常共享lease，`recover_stale=false`，不处置其他owner。

原生PNG请求可超过1MiB；当前SemLoom Core单项上限不足，原样全64图臂保持pending，不缩像素或拆请求绕过。
原生Daft的batch-UDF并行值及Ray的actors×async值分别记录，使有效HTTP上界同为4；模型服务拥有内部批处理。
准备、完整查询、查询后评价／持久化与服务退出分列；失败、取消或不完整覆盖停止该臂资格并保留原件。
原相对误差外还记录逐行TP/FP/TN/FN，防止COUNT中的错误相互抵消。少量query不报告可靠P99/P99.99或性能排名。

真实模型尚未执行；后续真实结果在本报告同一入口追加，不借用CPU fixture或既有CLIP资格。
