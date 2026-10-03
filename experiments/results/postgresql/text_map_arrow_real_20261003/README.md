# SemLoom Daft／Arrow 分批的真实模型对照

2026-10-03。**10 条查询、8,224 次真实模型请求全部完成，独立行关联、顺序、资源与清理核对通过。**
完整运行 299.914 秒；1024 行三次完整查询中位数为 Daft 分批 10.060 秒、直接 Arrow 分批 9.779 秒，少 2.793%。
三对都更快，但属于复用输入、固定配置下的小幅工程观察；默认 Daft 保留，Arrow 继续作为显式选项。
这是 SemLoom 两种物化方式的内部对照，不能读作原生 Daft／Ray 排名或多模态收益。

## 问题与来源

只替换已经由公共执行核心选好的有限请求的分批方式，检查直接 Arrow 是否减少完整 PostgreSQL SemMap 查询成本。
当前实现见[分批源码](../../../../code/src/data/materializers/payloads.py)，原 fixture 结果见[无模型对照](../text_map_arrow_batches_20261002/README.md)。
Daft 路径为 Arrow 表 → Daft 图与分批 → Arrow 批次；直接 Arrow 构造独立批次。
数据库语义、Core 选行、完整模型请求、同步持久记账、Ray 提交及结果处理保持一致。

执行源码来自 GitHub main `1be050a0a6258794a63acb67bace0693b921e647`，830 份来源摘要在运行前后核对。
准备检查发现旧测试 fixture 未提供任务键，也仍假设只有请求事件；补上 `TaskKey`，分别检查请求数和成功／失败计时事件。
只修改这项测试，生产实现没有改动；源码补丁与精确文件摘要一并保留。
首次服务器检查 81 项中 80 项通过、1 项旧 fixture 报错；修订后所在 3 项在本地和服务器都通过，覆盖 81 个不同检查。
实际 PostgreSQL 18.3／扩展、8 CPU／0 GPU 的 Ray 启停、102 字节最长 socket 路径、输入摘要及 9 份模型文件核对通过，准备阶段模型请求为 0。

## 已授权清单与环境

原清单先在仓库外保存，再由用户授权新的账本：最多 8,224 次请求、20 分钟，单查询 120 秒，预留 120 秒清理。
两臂各 16 行检查、1024 行一次预热与三次交错测量；总量为 `2 × (16 + 4 × 1024) = 8224`，检查及预热全部计入。
首次请求、查询、身份、关联、顺序、计费、容量或超时错误即停；没有重放、重置额度或自动重启。
来源为[当前清单](../../../plans/查询调优.md#arrow-real-20261003)与[实际计划](raw/run-plan.json)。

- 机器：128 CPU、双 RTX 4090，实际模型只使用空闲 GPU 0；每卡 24 GiB。Linux 5.15.0-97-generic，驱动 595.71.05。
- PostgreSQL 18.3，扩展 0.2.0；复用已核对的软件前缀，在数据盘启动独立临时数据库，不安装或改写扩展。
- Qwen2.5-7B-Instruct，修订 `a09a35458c702b33eeacc393d103063234e8bc28`；vLLM 0.25.1、BF16、单卡张量并行。
  固定模型长度 4096、显存比例 0.8、最大序列 128、单次调度 token 上限 8192，FCFS、关闭 prefix cache、启用 chunked prefill。
  生成温度 0、最大输出 128 token；两臂共享同一模型进程。
- driver：Python 3.12.3、Ray 2.56.1、Daft 0.7.21、PyArrow 24.0.0、psycopg 3.3.4；driver 与 vLLM 环境分开。
- 两臂各一个共享 gateway 和调用方拥有的 HTTP worker；Ray 8 个逻辑 CPU、0 GPU、256 MiB 对象存储，两 worker 常驻后初始可用 6 CPU。
  请求名额 64、PG 窗口 256 行、批次最多 16 行、传输窗口 2 MiB、在用 Arrow 对象 4 MiB、Daft 8 线程。
  PG 输入／结果各 128 MiB，总留存 256 MiB、暂存 4 MiB。没有物理 CPU 隔离。
- 输入复用此前 Movie 检查与评价集合，16／1024 行 manifest 摘要分别为 `77ca615b525f9704d249f9de64e3b6724c78c8b10704741e4728f7346af65cb8`／`4e4d7872d239000d4cc631be3c91f711f5a7febbdb5c13c3801d5543fdb27250`。
  本轮复制到独立私有目录并逐字节核对；不是新独立评价集，也没有搜索最佳容量。

## 完整查询与质量

JCT（完整查询时间）为 `(t_query_terminal_ns − query_preparation_started_ns) / 1e9`，包含每查询准备、SQL 执行与消费。
共享模型、Ray、worker、gateway 的启动另列；终态后的清理不混入 JCT。

| 1024 行测量 | SemLoom Daft 分批（秒） | SemLoom Arrow 分批（秒） | Daft 准确率 | Arrow 准确率 | 配对输出差异（行） |
|---|---:|---:|---:|---:|---:|
| 1 | 10.076469 | 9.734589 | 84.9609% | 85.0586% | 1 |
| 2 | 10.060052 | 9.779081 | 84.9609% | 85.1563% | 2 |
| 3 | 9.932774 | 9.886773 | 85.0586% | 85.1563% | 3 |
| 中位数 | 10.060052 | 9.779081 | — | — | — |

减少比例为 `100 × (1 − 9.779081436 / 10.060052440) = 2.792938%`。
配对分别少 0.341879、0.280971、0.046000 秒；第三对仅少约 0.46%。没有统计优越或质量等价证明。
预热值 Daft／Arrow 为 13.215006／9.669719 秒，未进入测量中位数。
测量每查询准备为 11.888–16.398 毫秒；两个共享 gateway 启动为 2.416514／2.377078 秒，Ray 启动 9.524983 秒。
准确率从全部输出与私有参考标签重新计算，范围 84.9609%–85.1563%；请求集合一致不表示生成结果完全相同。
全部重复与计算来源见[复算输出](raw/real-analysis.json)。

## 分段与解释

以下为每查询各事件的累计时间，存在重叠，不能相加为 JCT。

| 累计项 | Daft 三次 | Arrow 三次 |
|---|---|---|
| 分批 `next` 实际执行（秒） | 1.855550 / 2.443023 / 1.781201 | 0.281797 / 0.277515 / 0.330407 |
| `next` 完成后恢复等待（秒） | 5.398986 / 5.153902 / 5.213224 | 6.066833 / 6.371367 / 6.322757 |
| 发送前持久记账（秒） | 4.934250 / 4.703322 / 4.751698 | 4.926954 / 5.221323 / 5.130364 |
| 准备窗口数 | 80 / 142 / 85 | 114 / 88 / 120 |
| Ray 对象数 | 117 / 169 / 119 | 145 / 123 / 148 |

直接 Arrow 的分批执行累计少约 1.45–2.17 秒，但完整查询只少约 0.05–0.34 秒，恢复等待也更长。
窗口数量随返回与同步记账时序变化，不能将减少建图时间直接等同于模型推理加速，或据此认定单一根因。
本次实验观察层仍同步持久记账，其约 5 秒的累计时间不能归为 SemLoom 调度算法本身。
对象 put、RPC、worker 前后时刻及服务时间序列已保留；尚未采集独立的 vLLM GPU kernel 时间或完整 PG 节点阶段时间。

多模态后续可复用 Daft 的读取与解码，Arrow 继续承担合适的 CPU 数据表示；本次只测不透明文本请求的分批。
通用媒体引用、视频／episode 窗口、设备内张量的完整 PG 执行路径尚未实现，不能由这次文本结果推导其性能。
执行层分工与准备粒度的候选继续由[方法评估](../../../../docs/research/优化方法依据.md#engine-ownership-assessment)维护。

## 独立核对与证据

从原始输入与全部结果重算 8,224 行：PG 发送前绑定、请求多重集、结果关联与顺序、逐行 guard、服务成功增量、
逻辑容量峰值／终态、Arrow 对象寿命及 PG 留存／暂存记录均通过。三个总数——计费、成功增量和访问日志——均为 8,224。
[独立清理](raw/cleanup-independent.json)确认本次 PG／Ray／模型退出、相关端口关闭、父目录 ACL 恢复及扩展摘要保持；两 GPU 各 1 MiB。
vLLM 退出曾报告一个 semaphore 待清理；最终独立检查该类文件为 0，模型正常退出，无强制杀进程。

准备时系统 Python 缺少 psutil，发生在 owner 启动之前，模型请求为 0；改用已检查的 driver 后启动本轮。
旧 fixture 失败、修订与这项准备错误分别保留，不算作查询成功或重放。正式查询没有失败。

完整私有原件的独立本地备份标识为 `GPU-arrow-real-20261003/ar0k303njj`：239 份逐文件字节／SHA 核对通过，
包括 SQLite、原始评论、完整请求、配置与日志。编译缓存、临时数据库数据和 Git checkout 不进入证据归档。
公开保留 120 份必要记录，以 105 个归档对象及 4 份直接文件保存，归档 4,942,450 字节。
公开材料使用既有 compact 事件投影、命令／异常脱敏，并保留公开与私有原件各自摘要；原始评论、SQLite 和真实环境文件不进入 Git。
[来源与恢复清单](raw/storage-manifest.jsonl)、[恢复核验](raw/storage-validation.json)定位必要材料；复算不启动模型：

```sh
PYTHONPATH=code python3 code/scripts/analysis/restore_result_evidence.py experiments/results/postgresql/text_map_arrow_real_20261003/raw/storage-manifest.jsonl --output /path/to/new-private-arrow-evidence
PYTHONPATH=code python3 experiments/results/postgresql/text_map_arrow_real_20261003/raw/analyze_real.py --evidence /path/to/new-private-arrow-evidence --inputs /path/to/verified-real-inputs --repository /path/to/verified-execution-source --output /path/to/arrow-analysis.json
```

复算输入从上述私有备份的 `real-inputs/` 获取，源码使用 main 来源提交加保留的测试 fixture 补丁。
公开恢复与私有原件得到相同查询数、计费、准确率、分段、配对输出差异及中位数。
