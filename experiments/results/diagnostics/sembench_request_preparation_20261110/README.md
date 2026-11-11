# SemBench 原请求画像与 Core 接入准备

本报告对应[首批观察计划](../../../plans/数据组织.md#dag-first-observation)和
[实施设计 PR #96](https://github.com/3444374/ai-operator-execution-optimization/pull/96)。
下述2026-11-10准备阶段完成原请求采集／离线分析，以及补充文本的 Core 接线。来源为真实作者输入、原方法源码与 CPU 软件检查；
没有模型推理、权重加载、真实 Ray 执行或 GPU 性能结果。

## 实现与检查范围

- [请求采集](../../../../code/src/experiments/sembench_request_profile.py)使用原 LOTUS formatter、本地 tokenizer 和完整 chat template，
  保留逐发生项、消息、生成配置与 token；重复采集核对内容，准备时刻不进入摘要。
- [前缀分析](../../../../code/src/research/data_organization/prefix_opportunity.py)以父前缀和实际 token 块建立树，
  比较固定源窗口内的原序、长度序和前缀序，不模拟缓存或改变实际执行。
- [Core 适配](../../../../code/src/experiments/sembench_core_operator.py)由原 `TextOperation`构造单阶段有限链，
  复用融合／逐记录 owner、原结果恢复和评价。生产调度代码及默认值未修改，前缀排序未接入 Core。

检查分为两组：28项工具／Core／文本入口检查通过；随后33项原查询／原生接线与依赖检查中31项通过、2项跳过。
合计59项实际通过；未取得完整 Medical CSV 和 MMQA sf25 资产的两项真实数据检查保留未运行身份。
保留原生报告说明文字后，另对8项受影响的入口与 Q9 检查复验通过，不与前述59项重复相加。
其中包括原 Q9 的 prompt、评分解析、最终关系及评价差分；F/M/E 的重复标签与额外字段；空输入不分配 owner；
真实 CPU 子进程／三路 socket 的乱序完成、结果源序恢复、actor／模型子进程关闭和 Core 额度归零。
Ray 调用使用线程替身，模型响应由显式 CPU fixture 提供。它们不证明新接线的真实 Ray 或 GPU 退出。

原生 LOTUS／Ray Data／Daft／IMLane 的旧路径仍通过不导入 Core 调度的检查。
新配置只有显式 `backend=semloom-core`才加载 Core 实验适配；该路径要求执行模式和有限请求数。
真实 run 继续使用原持久预算与外部资源监督，`fixture`本身仍是零推理的软件方法检查。

## 真实 Q9 输入的 CPU 观察

运行标识为 `semloom-request-preparation-20261110`，采集为 `q9-original-profile-r1`。
上游为 [SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2`](https://github.com/SemBench/SemBench/tree/c814e3807e72d4cf876b852b17e77f3cc94575c2)，
使用原 `files/movie/data/sf_2000/Reviews.csv` 的2000行与原 Q9-map 选择，最终256个初始发生项全部采集。
tokenizer 为 [Qwen2.5-1.5B-Instruct](https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct/tree/989aa7980e4cf806f80c7fef2b1adb7bc71aa306)
的 `989aa7980e4cf806f80c7fef2b1adb7bc71aa306`，仅下载配置／tokenizer文件，没有权重。
LOTUS 1.2.4、LiteLLM 1.95.0、OpenAI 2.50.0、pandas 2.3.3、Transformers 4.57.6、Python 3.12.6；
独立 macOS CPU 环境的27项能力检查通过，不借用服务器的软件或 GPU 身份。

原数据含128条不同评论，每条出现两次；采集保留256个不同 call_id、128组相同完整消息和 token 序列。
总输入57812 token，最短185、中位数224.5、最近秩95分位261、最长275。
所有请求的完整共同前缀为39 token；原 LOTUS `user_message_formatter`把上下文放在指令前，
因此 Q9 较长的固定评分指令并未成为所有输入的长前缀。这是该输入、格式和 tokenizer 下的观察。

下表使用16 token 块，只表示所选次序中相邻请求的完整公共块 token 总和。
窗口是可在其内改变次序的源记录数量，不是模型批；原生 APC（自动前缀缓存）可能已经保留更早的重复前缀。

| 窗口行数 | 原序 | 长度序 | 完整 token 前缀序 |
|---|---:|---:|---:|
| 1 | 8160 | 8160 | 8160 |
| 16 | 8160 | 9136 | 9344 |
| 64 | 8160 | 11040 | 12576 |
| 256 | 8160 | 18160 | 32128 |

16 token 块的前缀树包含3494次完整块出现、1486个不同前缀块；按重复内容计数得到32128 token。
这一数字不是实际命中或可避免的 GPU 工作，最后一个块的处理、容量、驱逐、并发和实际调度均未模拟。
32 token 块及全部原始派生值见[机器可读观察](observation.json)。

三次原 formatter／tokenizer 采集均保留，未删除首次初始化成本：

| 次数 | formatter 秒 | 请求检查秒 | tokenizer 秒 | 代表帧／记录尺寸测算秒 |
|---|---:|---:|---:|---:|
| 1 | 0.002881041 | 0.003140963 | 0.096623660 | 0.010898007 |
| 2 | 0.002723792 | 0.002881544 | 0.084309642 | 0.009382489 |
| 3 | 0.002557250 | 0.002807704 | 0.082149671 | 0.009255006 |

这些是本地 CPU 分段时间，不是完整查询或模型性能重复。原读取、依赖初始化、内容摘要、词表身份与写出各有额外成本；
私有记录保留完整工具时间。代表 submit 帧采用固定请求标识和空期限，不能视为真实网络传输。

## 对后续实验的作用

可以分别检验保持完整请求的次序改变、提示布局变化，以及原生 GPU 批的选择。
前者保留256次发生项，不能把相同评论合并成128次推理；后者改变 token 次序，需要原任务真实质量评价。
还需观察原生缓存是否已经覆盖重复、外层次序是否改变 GPU 批、组等待是否抵消节省。
本次没有缓存命中、实际 prefill／decode、kernel、显存带宽、质量或完整查询加速证据。

Q9 Core 的实际 Ray／模型检查、Ray 官方 Processor／Daft 原生 AI 的 Q9 绑定、vLLM 逐轮观察仍为 pending。
多阶段、图像、表示位置和多实例设计继续独立推进，不由本题的重复输入决定整条研究路线。

## 来源、失败与恢复

实现基底为 main `70ee0970260a710efee73e5cc3db841abed75ead`；运行时新增源码的逐文件摘要由
`source-snapshot-before-profile.json`保存，不能把基底提交当作已含新实现。
该次十份改动源码另保存在`source-at-profile.tar.gz`，逐文件摘要与采集前记录一致；依赖清单单独保存。
[原件清单](evidence_manifest.json)记录配置、源码摘要、tokenizer文件、请求、分析、全部重复及检查日志的字节数与 SHA-256。
私有原件从仓库外 artifact 按运行标识及相对路径取得；Git 只保存本报告、紧凑派生观察和原件清单。

初始预检缺依赖，首次安装因本机 Python 的证书链失败；保留原日志，改用系统 CA 验证后完成独立环境安装。
旧环境元数据读取等待后中断，未修改旧环境；本次没有覆盖或删除历史原件。
Medical／MMQA 两条跳过项不计入通过。真实模型次数为零，GPU 实验未开始。

离线窗口对照可从同一私有请求文件重新计算；相邻公共块计数为确定性派生量，CPU 组织耗时随运行重新测量：

```python
from src.research.data_organization.prefix_opportunity import analyze

for window in (1, 16, 64, 256):
    result = analyze(capture["requests"], block_sizes=[16, 32], window_rows=window)
    counts = {name: value["adjacent_full_block_content_tokens"]
              for name, value in result["orders"].items()}
    print(window, counts)
```


## 完整内存结果计时补充（2026-11-11，目标验证前）

在本 PR 精确 `1cbb2c73eea535d0330a326f0af708800b5bccf4` 上补充独立时钟，保留原
`query_seconds` 及原查询、调度、formatter/parser、输入和生成配置。`memory_sink_timing`
从实际 CSV 重读前开始，到原完整关系结果返回后结束；包括冷启动，排除最终 owner/Ray 关闭、
调用审计、质量评价与写盘。不存在从历史时长扣除估算关闭成本的换算。

成功 EOF 可以显式推迟最终 owner 关闭，由查询作用域保证退出时关闭；默认 iterator 和独立
operator 调用仍 EOF 关闭。源与 session 收尾、Core 与 transport 清零先于成功 EOF。
原关系失败没有 sink 终点；sink 之后关闭失败仍判查询失败，并保留已经观测到的时钟。

当前 5 项新增计时／异常／生命周期检查和 6 项既有 Core 入口检查共 11 项通过。
更广的 115 项回归为 112 通过、1 失败、2 项缺完整资产跳过，不能写为全绿。
失败为已有 record-local 20ms close 之后迟到回执的 shutdown 状态不确定；clean 的精确旧
`1cbb` 同样复现，新旧各有通过和失败。该测试未进入此次 EOF 分支。原始日志全部保留，
本补充不声称解决超短期限取消／关闭问题，也不据此取得该路径的失败恢复资格。
独立只读审查未发现新增阻断；目标真实 Ray/GPU 完整查询仍待执行。


### 首次目标验证与 fused 关闭接点修正

`c7e78a73ac2e81209e3b684a528113f2baea55b1` 在固定 RTX 4090、Qwen2.5-7B-Instruct、
vLLM 0.25.1 上完成原 Q9 Map 全部256发生项的两次独立冷查询，fused 与 record-local 各256调用。
目标零模型门核验1020源码文件、11项计时测试、原消息及完整 tokenizer 数组。全部512实际请求的
完整输入 token 与原方法一致，均终态成功；两路径输出仅253/256一致，位置69、177、238不同。
无预热、重试或输入去重。不是原生 baseline 或稳定性能比较。

| 首次目标路径 | 观测完整 sink 秒 | 旧关闭包含时钟秒 | Spearman | Kendall |
|---|---:|---:|---:|---:|
| fused | 73.534884014 | 73.705151117 | 0.675370889 | 0.590104802 |
| record-local | 62.604972856 | 67.162185942 | 0.689541343 | 0.602602967 |

这两个首次 sink 时钟**尚不能作为同一最终关闭边界比较**。真实回执终审核出 shared fused worker
默认 `close_on_source_complete=True`，在最后一批返回前已关闭模型；因此 fused sink 包含该内部关闭，
不能由历史时间扣去 `close_seconds` 修造对齐值。record-local 关闭在已观测 sink 后。
独立 stage 的物理退出与资源检查都通过；外层复制 Ray 已退出 socket 时归档失败，失败保留。
随后只恢复1278个普通文件并验证 SHA-256，socket 与 symlink 单独记元数据，未重跑模型。

修正只把 Core 已有 `close_on_eof` 选择透传到非 relation fused worker 的现有关闭开关。
默认 True、原生 baseline、relation-barrier 已有 False 和失败时立即关闭均保持。
新增测试运行真实 shared worker 生命周期，仅模型 Engine 为 CPU 替身；修正前准确复现提前关闭。
修正后的27项定向检查全部通过，含底层关闭失败仍保留已观测 sink 并使整查询失败。
只计划为修正 fused 补一次独立单冷256目标资格，不重复已通过的 record-local，也不转授旧 fused 时钟。

目标原件摘要与完整输入／输出差分计数见[机器可读验证](target_validation_20261111.json)。
修正源码 `edd287ff1b3fac141769e19f0e36faec4816dde7` 的服务器上传未获执行许可，
因此补充 fused 256 目标轮尚未启动；当前仅有上述27项修正后本机 CPU 证据。
