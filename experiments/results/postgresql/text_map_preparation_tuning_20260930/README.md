# 文本Map准备成本诊断与worker复用

2026-09-30。对应[调优计划](../../../plans/text_map_preparation_tuning.md)。研究对象为
PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。

本轮定位了重复查询中Ray新worker启动的主要准备成本，并实现可选的服务拥有worker。
1024行无模型内部对照中，三次中位数准备4.951→2.888秒，完整查询16.263→13.963秒。
真实模型请求为0；不能据此更新[旧四路径真实排名](../text_map_four_path_comparison_20260930/README.md)
或声称生成质量、容量平台与稳定GPU收益。

## 环境与计时

沿用原机器和独立源码目录，起点`07a8cfd9`；执行代码的逐文件SHA在原始控制器摘要中，
本次唯一执行层改动为`ray_map_transport.py`的可选worker服务及借用处理。
原比较源码与结果未覆盖。目标PG18.3、driver Python3.12.3、Ray2.56.1、Daft0.7.21、
Arrow24.0.0、psycopg3.3.4、httpx0.28.1。128个系统CPU、两张RTX4090；诊断仅声明8个Ray逻辑CPU、
256MiB对象存储与0GPU。[只读环境检查](raw/preflight.json.gz)通过，driver和模型环境保持隔离。

全部输入为新的合成Movie-shaped材料，含中文、英文、重复原始ID与唯一行编号；
确定性HTTP fixture按标记返回POSITIVE/NEGATIVE。PG、Ray和fixture在查询前启动；
每查询创建新driver/gateway，C64、PG窗口256、1个Ray actor、批次16、2MiB传输窗口与4MiB对象额度。
保留数据库输入、语义请求、消费结果和请求计数检查，不使用独立真实评价集。

完整查询按`(t_query_terminal_ns-query_preparation_started_ns)/1e9`计算；
准备按`(t_release_ns-query_preparation_started_ns)/1e9`计算。消费至EOF之后的清理和共享服务启动另记，
不混入此公式。gateway/Ray分段使用所属进程的持续时间，嵌套项不能相加；首次RPC不代表HTTP开始。

## 首轮分段

最终24条查询、12,416次fixture请求通过，186.437秒；每规模两路径各1次预热、3次交错测量。
下表为三次测量中位数，单位秒；全部重复见[分析原值](raw/analysis.json)与
[控制器完整摘要](raw/d02-controller-summary.json.gz)。direct只作诊断参照。

| 行数 | SemLoom完整查询 | SemLoom准备 | direct完整查询 | direct准备 |
|---|---:|---:|---:|---:|
| 16 | 7.385 | 3.196 | 0.362 | 0.196 |
| 512 | 10.205 | 3.123 | 1.795 | 0.181 |
| 1024 | 16.149 | 5.305 | 3.569 | 0.194 |

源码与原始记录：gateway自身库加载约0.13秒、观测设置约0.11秒；Ray库加载约1.8–2.1秒，
driver连接约0.05秒。12次SemLoom查询中，前8次actor就绪0.25–0.31秒，后4次2.41–2.53秒；
Raylet启动日志逐项对应后4次的新worker。首批Daft/Arrow准备0.82–3.17秒，位于查询释放后。

已核对的[Ray2.56.1源码](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/_private/services.py)
按逻辑CPU数量预启动Python worker。现有查询完成后结束actor，重复查询消耗初始worker后需启动替代worker。
源码、日志和分段共同支持这一工程解释；旧真实运行没有这些分段，不能把其全部5.3秒准备归为单一原因。

## 可选服务与内部对照

采用[该版本命名actor](https://github.com/ray-project/ray/blob/ray-2.56.1/doc/source/ray-core/actors/named-actors.rst)
和[结束行为](https://github.com/ray-project/ray/blob/ray-2.56.1/doc/source/ray-core/actors/terminating-actors.rst)：
调用方在自己的Ray连接中创建worker服务，保持原创建者所有权，无detached actor或自动重试。
`RayMapConfig.worker_pool`选择已存在服务；默认仍由查询创建worker。
模型/HTTP配置摘要与容量必须相同，同一worker只接受一个query identity。正常完成后归还；
未确认执行保留借用与对象计费，拒绝后续查询借用，服务拥有者结束worker并收到清理错误。
未发送取消行不发HTTP，已发送取消继续等待真实结果。

对照先各做16行correctness，再由旧实现执行8条16行查询消耗预启动worker；
1024行两种生命周期各1次预热、3次交错测量，共18条查询，8,352次fixture请求，227.180秒。
fixture监听队列512。原始观测HTTP活动峰值：旧方式5/6/7，复用方式2/2/4；
这些即时fixture没有形成C64饱和服务，不能作为容量比较。

| 指标（秒） | 查询拥有worker：三次原值 | 服务拥有worker：三次原值 | 中位数变化 |
|---|---|---|---|
| 准备 | 4.951 / 4.708 / 5.413 | 2.658 / 2.978 / 2.888 | 4.951→2.888，减少41.7% |
| 完整查询 | 16.263 / 15.796 / 17.129 | 14.116 / 12.685 / 13.963 | 16.263→13.963，减少14.1% |
| 释放至全部消费 | 11.312 / 11.087 / 11.716 | 11.459 / 9.707 / 11.075 | 11.312→11.075 |

旧方式就绪2.174/2.031/2.469秒，复用方式借用0.324/0.318/0.330秒；
两臂Ray库加载仍约2秒，连接仍约0.05秒。准备下降最清楚；首批处理仍有波动，不能把流式部分变化全部归为worker复用。
新增worker服务一次启动0.355秒，共用Ray启动10.208秒另计。worker服务单独结束时间为
`unavailable`，控制器未设置该时钟；结束确认与整体227.180秒已保存。
全部查询与服务启动原值见[完整控制器摘要](raw/p02-controller-summary.json.gz)。

## 失败与检查

- 首轮控制器把仅适用于PG的参数传给direct，配置检查停止；此前1条16行查询成功，共16次fixture请求。
  [原始失败](raw/d01-controller-summary.json.gz)保留，修订控制器后使用独立目录与账本执行24条清单。
- 首次worker对照在旧方式1024行预热出现29条HTTP读取连接重置，停止并清理。
  服务实际收到295次POST，账本分配1184次；[完整失败摘要](raw/p01-controller-summary.json.gz)及29条错误保留。
  默认fixture监听队列5；改为512并记录监听计数后，新18条对照通过。
  新轮次ListenOverflows/ListenDrops增量0、handler异常0。这支持检查诊断服务配置，不证明首次重置或旧真实模型故障的根因。
- 本地相关21项中16通过、5项缺Daft/Arrow跳过；增加借用取消检查后，worker模块11项中9通过、2项缺依赖跳过。
  目标环境完整相关87项通过，最终worker模块11项全部通过，覆盖89个不同用例。
  旧HTTP、配置/编号校验、行关联/尾批、取消、额度、错误和部分启动回收均保留。
- 实际Ray核对错误模型/容量、已借用服务拒绝、顺序借用、未确认执行保留及服务结束，共5项，不发HTTP。
  PG对照逐查询行数、标签、请求计数与资源回收通过；测试标签一致不作为真实语义质量结论。
- 四轮控制器累计21,079次fixture请求，真实模型0次；失败账本不退款、不合并进成功测量。
  [最终清理](raw/cleanup.json)核对201个采样PID及创建时间，活动残留0；PG/Ray/HTTP停止，
  两处父目录ACL恢复原值，GPU计算进程为空。一次清理脚本语法错误及修订后的核对记录同样保留。

## 存储、复核与后续

[分析原值](raw/analysis.json)、[逐文件存储清单](raw/storage-manifest.jsonl)和
[压缩核验](raw/storage-verification.json)直接可读。批量文本脱敏后进入[归档](raw/evidence.tar.gz)，
控制器摘要另有gzip副本；清单同时保留原始和公开摘要，地址/路径脱敏导致摘要变化时明确区分。
原始完整导出保存在仓库外备份，原始archive摘要写在清单首行。
可重建的PG数据目录、runtime二进制账本和临时链接不提交，选择记录见
[未公开的可重建材料清单](raw/omitted-runtime-files.json.gz)；这些原件仍在仓库外原始归档中。
脱敏后归档逐项解压比对公开SHA，所有选择文本在压缩前执行隐私扫描；旧失败没有删除。
[交付检查](raw/validation.json)记录措辞、本地链接、源码身份、请求计数与隐私核对。

可用`tar -xzf raw/evidence.tar.gz -C <新目录>`恢复公开文本。恢复后的路径以`raw/`开头；
原摘要指向原始未脱敏字节，公开摘要用于核对恢复的文本。
控制器、查询配置、清单、请求/输出/资源轨迹、Raylet日志、测试和环境报告均在归档中。

当前采用可选worker服务，保留默认旧实现。下一项先补serialization/put/RPC与首批处理观测，
再列真实模型同输入、同容量、完整生命周期和新额度的复测清单；不复用旧41,024次额度。
本轮只证明有限无模型工程改善，真实模型收益仍为pending，四路径原质量差异与排名保持原值。
