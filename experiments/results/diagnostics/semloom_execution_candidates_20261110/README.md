# 提示布局、图片表示、准备位置与执行候选

本轮继续尝试[研究方案](https://github.com/3444374/ai-operator-execution-optimization/blob/eb5e9406c5d3f6c21735af9a06d22bcade543cac/experiments/plans/%E6%95%B0%E6%8D%AE%E7%BB%84%E7%BB%87.md#gpu-implementation-starts)中的独立方向。
当前材料包括真实文本的无模型准备、真实照片上的 CPU 操作、明确构造的消费轨迹，以及可选执行接点。
这些结果尚不证明完整查询或 GPU 加速。源码基于 `b1d965fea`；各次实际代码摘要和上游来源见
[provenance.json.gz](provenance.json.gz)，公开副本、原件身份与恢复说明见 [MANIFEST.json](MANIFEST.json)。
完整请求、图片、机器环境与逐次原件保留在仓库外；公开压缩文件为 UTF-8 JSON，可用 Python `gzip.open` 复算。

## 实际完成范围

| 方向 | 本轮实现与运行 | 尚缺的证据或机制 |
|---|---|---|
| 提示布局与请求分组 | 原 LOTUS formatter 的独立布局候选；Q9、Q8 完整初始算子的原字段／发生项和分词统计 | 布局候选的语义质量、实际缓存和 GPU 批费用 |
| 多层表示留存 | 原 URI owner 的竞争性消费试验；vLLM 原 EncoderCacheManager 的 CPU 状态机探针 | encoder 张量和 KV 的真实留存、物理回收及跨进程未来使用信息 |
| 准备位置与传输 | 两个真实 CPU worker，对比编码图片／RGB、流式／整段准备，16 臂 | H2D、模型 processor、跨 GPU 和完整查询 |
| 提前物理准备 | 有界 CPU 单 worker；固定、概率和期望成本三种选择，32 臂 | 实际语义分支概率、足够可重叠计算与共享 CPU 竞争 |
| 合法就绪工作选择 | MethodDriver→FIFO/Core 接线；短工作、长剩余路径和路径／工作比例，可显式 FIFO 回退与老化 | 实测 GPU 成本来源、跨阶段性能和完整查询公平性 |
| 多实例位置选择 | 当前可用 endpoint 内比较排队、传输与重建估计 | 真实数据位置采样、多 GPU／多实例实验、迁移机制 |
| 模型阶段协同 | 原生前缀缓存、chunked prefill、cascade、CUDA Graph 参数；prompt／非 prompt 与 encoder 表示事件分析 | 分阶段部署、共同资源配置和端到端比较 |

没有实现一个统一自适应控制器，也没有进行投机模型计算。新参数默认省略；就绪与路由策略需显式传入。
准备工作和策略接点保持各自 owner，语义方法仍决定哪些后继合法。

## 提示布局：机会量与实际成本分开

来源为 SemBench `c814e380` 的 Movie 原查询、LOTUS 1.2.4 和 Qwen2.5-1.5B tokenizer。
同一行完整字段与生成参数保留。原 Q9 捕获摘要与前一准备实验完全相同。
每 64 条请求一个窗口，以 16 token 完整前缀块构建前缀树；“重复块 token”计数为所有前缀块出现数减树中不同节点数，再乘 16。
它包含窗口内子组，不等于整个组共同前缀、缓存命中或 attention 节省。

| 输入 | 布局 | 请求数 | 完整 prompt token | 重复前缀块 token |
|---|---|---:|---:|---:|
| Movie Q9 | 原布局 | 256 | 57812 | 12480 |
| Movie Q9 | instruction 提前 | 256 | 57812 | 41712 |
| Movie Q8 | 原布局 | 120 | 14206 | 3840 |
| Movie Q8 | instruction 提前 | 120 | 14326 | 9456 |

Q8 每条增加一个 token，说明内容重排可能改变分词成本。布局改变须单列语义 profile，不能从字段相同推定质量相同。
本次两个输入都只有一个模型字段，没有触发 common-field 顺序候选；未触发不代表该方向无用。
全部窗口、准备时间与来源在 [layouts.json.gz](layouts.json.gz)。

## 图片传输与准备位置

六张公开 COCO val2017 JPEG，共 902879 字节；相同最终 RGB（三通道像素）为 4545600 字节。
两个 spawned CPU worker 加 driver，worker 先初始化 Pillow 后回报 READY；不绑定为两个 CPU core。
startup 与实际传输／准备／消费时间分开，没有模型、主机到 GPU 传输（H2D）或人工等待。每种方式四次交错运行，全部最终像素摘要一致。

| 传输表示／准备方式 | 实际进程间通信（IPC）帧字节 | 完整 CPU pipeline 毫秒，按重复序号 |
|---|---:|---|
| 编码图片／流式 | 903242 | 8.316, 8.101, 7.283, 7.542 |
| 编码图片／整段 | 903242 | 7.216, 7.818, 7.341, 7.014 |
| RGB／流式 | 4546037 | 13.767, 13.102, 12.916, 13.661 |
| RGB／整段 | 4546037 | 12.282, 12.739, 13.760, 12.893 |

精确纳秒、worker 身份、实际字节及采样 RSS 见 [media-transport-r2.json.gz](media-transport-r2.json.gz)。
[首次运行](media-transport-r1.json.gz)包含 worker 首次懒加载 Pillow 的费用，单独保留，不与上述就绪口径合并。
这些小规模观察支持继续检查编码表示的准备位置；不证明任意图片大小或 GPU pipeline 都采用相同选择。

## 消费顺序、竞争留存与提前准备

图片文本载体（URI）试验使用原 LOTUS 图片准备函数和真实照片，源字节预先读入。消费边、丢弃结果及四种轨迹是明确构造的，
不是实际语义输出。容量为 1196817／1795226 字节，四次重复，共 128 臂；输出一致，借用和消费责任归零。
原件与每项驱逐事件在 [media-consumers-r5.json.gz](media-consumers-r5.json.gz)。

| 消费轨迹 | 关闭留存 | 简单留存 | 最近最少使用（LRU） | 消费者数×准备成本 |
|---|---:|---:|---:|---:|
| 按阶段轮转 | 18 | 14／12 | 17／16 | 14／12 |
| 一条记录完成后再继续 | 18 | 6／6 | 6／6 | 6／6 |
| 不均匀消费者 | 27 | 10／9 | 12／11 | 13／15 |
| 后续揭示丢弃 | 15 | 12／10 | 14／13 | 13／11–12 |

表中是编码次数，斜线分隔两种容量；除最后一项大容量为 `[11,11,12,11]` 外各重复一致。
逐记录轨迹没有驱逐；其他轨迹确实形成了竞争。简单“未来次数×重建成本”在不均匀轨迹上增加重建，
没有表达下次使用距离；后一个解释是依据轨迹和算法作出的推断，尚不是实际查询因果证据。
这提示同时考察合法推进顺序和表示留存，不能只调整驱逐分数。

提前准备使用真实解码与当前 RGB checksum 重叠；dense／sparse mask 和概率明确为人工输入。
全部输出像素一致。dense 固定提前准备 11 次、丢弃 2 次，约 14.6–15.8 ms；按需约 13.2–13.8 ms。
sparse 固定提前准备 11 次、丢弃 8 次，约 10.0–10.35 ms；按需约 7.4–7.6 ms。
期望成本选择在 dense 只提前准备 2 次且不丢弃；sparse 不提前准备，保留全部重复见
[preparation-ahead-r2.json.gz](preparation-ahead-r2.json.gz)。这组 checksum 提供的重叠空间较小，
固定提前准备不划算；概率来源、CPU 机会成本和实际下游计算仍须实测。

## 原生接点与检查记录

EncoderCacheManager 探针从官方 vLLM 0.25.1 文件用 AST 取出原类，原方法体不改；CPU request stub 调用实际
`can_allocate/allocate/free`。提示仅重排可回收队列，实际驱逐由原类执行：正常选择 A，提示选择 B，活跃 A 仍受保护。
三臂见 [provenance.json.gz](provenance.json.gz)；这不是实际 encoder tensor、设备显存回收或 GPU 性能证据。
实现参照 [vLLM EncoderCacheManager](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/core/encoder_cache_manager.py)、
[编译配置](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/config/compilation.py)与
[分离 encoder 说明](https://github.com/vllm-project/vllm/blob/v0.25.1/docs/features/disagg_encoder.md)。未启动分离部署示例。

就绪策略由调用方提供估计；没有成本时记录 FIFO 理由。Core 接线用原关系 reference 核对最终输出、发生项、stage profile 和资源收尾。
路由只在原可接纳 endpoint 中选择；缺成本显式使用 least-queued，不声称搬运 KV。
native stage 分析将实际调度的 prompt 与其他位置分开；其他位置包括投机位置，不能当作最终生成 token。
已计算 prompt 也不自动算作前缀缓存命中。encoder 仅观察 shape、dtype、element size 和对象身份，不读取设备内容；逻辑字节不是唯一分配或释放证明。

检查原始输出、失败和分项结果见 [checks-and-failures.json.gz](checks-and-failures.json.gz)。
53 项首次观测检查通过；48 项组合有一项既有 late-receipt close 失败，在未修改基线 `b1d965fea` 也复现。
另一个新增用例曾错误预期 Filter 全保留，改用相同回答的原 reference 后相关 6 项、后续 5 项分别通过；不把各轮计数相加。
消费者首次两次缺完整 compatibility profile，第三次传入可变边列表，修正后重跑；所有失败保留。
完整请求回放与原工作流合计 29 项检查通过，覆盖乱序、分词不一致、失败停止、原生成参数和原单／批 off/on 组合。首次命令错误指定不存在的测试模块，其失败也保留。

## 后续实机协议

使用原租约、监督器、Git／CPU 资格和私有配置运行 `request_capture`；调用原 AsyncEngineProcess，不替换原生调度器。
首批选 Q8 的完整 120 请求，原布局／instruction 提前各自独立进程；原 8192 输出上限、temperature 0、无 stop 保留。
前缀缓存开启，原生批容量和输入组匹配，off/on 先验证一致与观测成本，再作交错重复。
单 RTX 4090，最多一份模型 owner；全部阶段累计上限 960 请求，运行超时／未知终态／像素或 token 身份不符即停止当前臂，记录失败。
保留全部生成结果供原评价与布局一致性检查；没有原评价前只报告请求层变化。模型启动、请求执行、收尾与 GPU 事件分别计时。
实机执行状态和回执在取得后补入本报告；当前本段是已准备协议，不是已运行结果。

## 实机接点修正：实际使用 v2 runner

源码 `5dee1c196` 的首个 Q8 off/on 各完成 120 请求，token 身份全部匹配、结果逐条相同，物理清理和租约释放确认。
原布局两臂均报告 3872 个缓存命中 token；请求时间分别为 1.019339263／2.692026648 s，
启动分别为 120.553319066／71.512895254 s。on 含 Nsight 和详细观察，这一配对不足以估计稳定开销或查询加速。
原 Q8 评价对 off 输出给出 absolute_error=2、relative_error=0.04043126684636118；这是原查询聚合指标，不是逐行分类准确率。

on 的事件完整收尾，但 28 轮批决策均未知。实际 runner 为 `vllm.v1.worker.gpu.model_runner`，
原新增决策接点只装在 v1；这是观察覆盖缺项，不是零 GPU 工作。安装文件 SHA-256 与官方 v0.25.1 完全相同。
新接点在 v2 `prepare_inputs` 原返回处读取实际成员、CPU prompt／computed 数组和实际 graph descriptor，
保持原返回／异常并在 execute 后还原。该 v2 实现没有 cascade dispatch，报告单列不支持，不将它算作有机会但未选择。
最小回归先因缺 graph 事件失败，修正后相关 52 项通过（1 项按原条件跳过）；
[复现和来源](v2-observer-fix.json.gz)保留原失败。新接点需要新的实机检查；原始 on 记录仍保持未知。
原生初始化的内部 warmup 发生项须与实际用户请求分开汇总，不能计入 120 条业务请求。
