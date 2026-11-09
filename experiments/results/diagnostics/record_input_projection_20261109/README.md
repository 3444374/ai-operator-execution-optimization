# Map／Extract 临时输入列投影原型

本切片以 `main@f44f174a27e564734774c015fa0847334698a321` 为基底，独立 draft，不合并 main。
只探索模型请求准备前的 CPU 表示选择，不涉及 SAOR、多租户、Q11 或 JSON 尺寸计数。

## 问题与实施前计划

源码依据：[record_method.py](../../../../code/src/semantic_methods/lotus/record_method.py) 的 Map／Extract
先用完整 `record.values` 构造 pandas 临时表，再将声明输入列交给 LOTUS。
Filter 已直接构造所用列，因此不把同一改动重复加入 Filter。
Movie Q9 是单 Map；本次也未在已有 Medical Q1／Q4／Q10 文本查询中识别出可用的模型算子链。
多阶段检查只使用明确标记的软件夹具，不能据此声称 SemBench 或 PG 已接入新链。

候选只将临时表的输入改为声明列向量，完整 occurrence、源字段与关系结果仍由原对象持有。
`prepare_rows(..., project_input_columns=True)` 显式选择；默认 False，普通 runner、服务配置、
组批、并发、模型参数、提示构造和解析均沿原路径。没有新增 CLI 生产开关。

验证先比较逐请求完整消息、身份、调用集合及原有关系装配，再测量 Python 输入容器的构造成本。
本地范围：软件夹具、无模型／SDK／数据下载，1／64／512 行和 1／8／64／128 个源列，
每个配置两臂交错 7 次、每次 32 次构造、计时前各预热 2 次；源构造不计入计时。
任何请求、occurrence、重复、NULL、父关联或屏障不同即停止收益判断；本次不开启服务器实验。
所测不包含 pandas、LOTUS formatter、模型或完整查询；这些缺项不能写成零成本。

## 已完成与待验证

[CPU 检查](../../../../code/tests/semantic_methods/test_lotus_input_projection.py)覆盖 Map／Extract
输入列序、完整消息与 occurrence、不改完整源记录、空输入、缺列／非文本拒绝及显式 NULL。
共享 ChainRun 的 Map→Extract→Filter→Map 软件夹具核对 11 次完整调用及其全部生成参数、
乱序返回、重复标签按源序赋值、动态字段、NULL、Extract 屏障、父调用和最终完整记录。
固定响应与替身 formatter 只证明此软件执行的等价，不能证明真实 SDK 或 GPU 输出一致。

真实 LOTUS／pandas 的无模型 formatter 差分另有可执行检查，当前环境未取得该项证据。
本机现有 pandas 经系统 Python 读取分别在 15 秒和 45 秒超时，未安装或下载依赖。
首次夹具运行有两项用例错误：Extract 缺 `json_object`，以及把原有屏障的空列表误期望为异常；
已按原接口修正测试，原记录见 [validation.json](raw/validation.json)。

CPU 成本与全部重复保存到 [cpu.json](raw/cpu.json)。本页的数值只解释实际测量对象，
不将声明矩形单元数当作 RSS、内存节省或 pandas 实际分配；完整源记录仍全部驻留。

本地 Python 3.12.6、arm64 共执行 31 项检查，30 项通过，原 SDK 检查 1 项因依赖不可用跳过；
包括既有记录结果、身份与 Filter 配置回归。源文件摘要和全部检查输出见 [validation.json](raw/validation.json)。
容器构造共 168 个观测，逐配置各臂 7 次组件重复；它们不是独立完整查询。

| 夹具行数／源列数 | 原行列表，中位数 μs | 投影向量，中位数 μs | 此步峰值 Python 分配，原／投影 bytes |
|---|---:|---:|---:|
| 1／128 | 0.163 | 0.286 | 80／128 |
| 64／8 | 0.509 | 1.013 | 560／608 |
| 512／1 | 5.014 | 11.635 | 4208／4256 |
| 512／128 | 4.638 | 15.763 | 4208／4256 |

中位数由对应七个 `input_build_ns / 1000` 计算；表列出的峰值分配在各自七次中相同。
该独立步骤观察到额外 CPU／容器开销，没有测得净收益。尤其单列输入没有可省的额外列；
宽表是否能由 pandas 少构造列抵消约 11 μs 的新增构造成本，仍需真实 SDK／完整准备测量。
亚微秒样本波动及所有原值均保留，不以微小差值作为采用依据。

## 已有能力与增量

| 来源 | 已有能力与本次采用决定 |
|---|---|
| [LOTUS formatter 源码](https://github.com/lotus-data/lotus/blob/main/lotus/templates/task_instructions.py) | `df2multimodal_info` 已由声明列选择输入。保留原 formatter，候选只把本项目临时表的投影提前；滚动源码不是本机 1.2.4 的执行证据。 |
| [vLLM](https://github.com/vllm-project/vllm)、[自动前缀缓存](https://docs.vllm.ai/en/latest/features/automatic_prefix_caching/) | continuous batching 和 KV 前缀复用已有。候选不修改请求或 KV，也不声称补齐这些能力；可能收益只来自客户端 CPU 准备。 |
| [Kalypso: Relational LLM Serving](https://arxiv.org/html/2607.23815v2) §5–6 | 已有语义流水、选择性影响、KV 生命周期与内存感知调度。此原型没有这些机制，不作为 Kalypso 复现或优越性证据。 |
| [Optimizing LLM Queries in Relational Data Analytics Workloads](https://proceedings.mlsys.org/paper_files/paper/2025/hash/b5dc49f44db2fadc5c4d717c57f4a424-Abstract-Conference.html) | 已有行／字段重排与前缀复用。候选维持声明输入列序和完整消息，不提供重排算法或新颖性结论。 |
| [Ray 2.56.1 fusion](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/_internal/logical/rules/operator_fusion.py)、[原生流水](https://docs.ray.io/en/latest/data/internals.html) | 原生执行已有融合和 CPU／GPU 流水。公共方法的投影若有净收益，也应提供给同方法的原生后端，不能称 SemLoom 独占机制。 |
| [现有工程参照](../../../plans/系统架构.md#own-to-company-transfer) | 继续复用方法 formatter／parser、provider 和关系装配；本次未访问、复用或发布公司材料，没有增加 PG 接入。 |

这是传统列投影的一个明确工程落点，尚无完整查询净收益或研究贡献证据。
CPU 输入构造会新增列向量遍历；即使宽表减少临时列，窄表或单行也可能净退化。
实际 SDK 下一步范围为本文件链接的单项 CPU 原 formatter 检查，无模型调用；由主会话排期。
真实数据 CPU、完整请求准备、RSS、GPU／查询耗时与质量仍为 pending，未自行使用服务器。

## 复算

```sh
PYTHONPATH=code python3 -B -m unittest tests.semantic_methods.test_lotus_input_projection -v
PYTHONPATH=code python3 -B code/scripts/experiments/check_record_input_projection_cpu.py --output /tmp/record-input-projection-cpu.json
```

第二条只测 Python 输入容器。不能用其耗时差推出 GPU 供给改善或端到端加速。
