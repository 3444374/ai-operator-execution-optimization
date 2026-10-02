# 文本Map原生候选与baseline补充

2026-09-30。接续[41,024次真实运行](../text_map_matched_20260930/README.md)之后的供给修订。
本次只使用合成HTTP服务，**新增真实模型请求0次**；该阶段未执行提交、推送或合并。
研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。

2026-09-30存储修订：原55份raw文件按原字节保存在[完整归档](raw/evidence.tar.gz)，
报告直引的大文件改为gzip链接；[原路径与摘要](raw/storage-manifest.jsonl)、
[独立恢复核验](raw/storage-verification.json)全部通过，包含全部未采用方案与失败。
恢复方法及本地/远端状态见[分支审查](../text_map_four_path_comparison_20260930/README.md#branch-review)。

## 当前补齐了什么

主表使用 `profile=main`：PG＋SemLoom Daft/Ray、PG-source direct、原生Ray Data、原生Daft Native。
SemLoom本地执行（此前叫PG＋SemLoom HTTP）另用 `local-ablation` 作内部消融。
旧四路径的配置、时间和角色保持原值，工具默认 `legacy-four-paths` 以兼容历史记录。

这补充的是固定语义单Map的执行层对照。LOTUS当前原始Movie查询入口不等于匹配Map已测；
Sema作者artifact的环境和语义适配也未验证。内置Daft prompt、数据库AI函数、跨模态和多Job另列，
详见[baseline覆盖审计](../../../plans/reference/对照规范.md#current-map-coverage)。不能称整篇研究的baseline已全部完成。

## 修订后的候选

| HTTP允许上限 | Ray actor数 | 每actor异步批次 | Ray批行数 | Ray SQL块/读取并发 | Daft原生异步参数/批行数 |
|---:|---:|---:|---:|---|---|
| 16 | 1 | 16 | 1 | 4 / 2 | 15 / 1 |
| 64 | 2 | 32 | 1 | 4 / 2 | 63 / 1 |
| 128 | 4 | 32 | 1 | 4 / 2 | 127 / 1 |

Ray固定2.56.1、8个逻辑CPU、256MiB对象存储、零Ray GPU名额；Daft固定0.7.21，
通过官方 `set_runner_native(num_threads=8)` 设置计算线程，SQL读取4分区。均不声称物理CPU隔离。
Ray参数按actor数×异步批次核对；原来的每批32行不再被解释为32路HTTP。
主表每条路径各有16、64、128三个候选，由
[候选生成器](../../../../code/src/experiments/postgresql/text_map_candidates.py)统一生成12组配置，不启动服务或模型。
主表preflight拒绝旧Ray欠供给配置、原生CPU声明不同和各路径调参容量集合不同。

## 新增Daft路径的身份

使用官方 `read_sql` 和 `func.batch`，读取、批次并行和背压均由Daft拥有。
工作函数只做输入转换、同语义HTTP、响应检查与独立关联；没有项目任务池、信号量、调度器或重试。
该路径是 **Daft Native SQL→HTTP工作函数执行图**，不是内置 `prompt()` 的测量。
每次单行批次拥有一个HTTP客户端，与修订后Ray的单行批次形式一致；其创建与清理都在查询时间内。

源码依据：固定0.7.21的逐行异步调用会对输入批次中的行执行 `asyncio.gather`；配置不能直接解释为整个查询的HTTP峰值。
`AsyncUdfSink`先提交任务，再在在途数大于参数时回收。因此显式单行批次和C−1参数用于声明HTTP上限C，
并由真实运行时上的合成服务验证。未修改Daft源码，也未给baseline加入SemLoom容量调度。
官方接口见[SQL读取](https://docs.daft.ai/en/stable/connectors/sql/)、[UDF](https://docs.daft.ai/en/stable/custom-code/func/)；
固定版本源码位置与摘要见[上游核对](raw/native-upstream-audit.json)。

Daft SQL连接工厂要求SQLAlchemy Connection；psycopg通过NullPool的creator接入，保持只读和语句期限。
runtime新增`sql-readers`能力组。预检查发现缺SQLAlchemy，安装预览只含该项，随后只在独立driver环境安装2.1.1；
SQLGlot30.14.0、ConnectorX0.4.5和模型环境未因此更换。安装前后报告和日志均保留。

## 受控设计与失败记录

512条合成评论包含中文、重复文本与重复原始review ID，执行行ID保持唯一。
每候选一个查询，HTTP服务暂时保留响应，直到活跃请求达到目标峰值后才释放；25秒未达到则失败。
每次发送都先消费持久额度，核对请求/响应/最终行关联、token用量、512行完整消费、资源释放及服务收到的实际次数。
这是结构性供给检查，不能用这些延迟或吞吐推断模型性能、饱和或质量。

| 尝试 | 结果及处理 |
|---|---|
| ns01 | Ray三组均到达16/64/128，各512次；Daft在发送前因缺少取消回调停止。改为关闭出站额度，并由既有监督器限制进程存活时间 |
| ns02 | Daft在发送前发现SQLAlchemy缺失；保留失败，按runtime流程补齐driver依赖，并使用该版本要求的SQLAlchemy连接 |
| ns03 | Daft逐行UDF完成512次，但声明16时服务峰值283，资源检查拒绝；没有把结果当成通过 |
| ns04 | 显式设置8计算线程和进程选项后，仍在512次中观察到峰值124；保留该未采用方案 |
| ns05 | 改为原生单行异步batch后，Daft三组实际峰值16/64/128，各512次；完整检查通过 |
| run07 | 旧路径、含Daft的新主表完整重复/选点、各路径错误恢复及三种路径超时恢复共6项通过；PG与Ray正常清理 |
| ns06 | 最终统一版本六组全部通过，Ray/Daft各自峰值16/64/128，每组512次，共3072次合成POST |

失败运行与预扣但未发送的额度均保留，不将分配额度当实际HTTP次数，也不将失败混入性能排名。
Daft超时先关闭进一步POST分配；未完成请求由外层监督器和客户端期限收尾，不声称即时取消远端模型计算。

本地96项相关检查与8项环境工具检查通过；测试范围有复用，不把历次计数相加当独立覆盖率。
最终[六组候选](raw/ns06/result.json)全部通过；[完整流程回归](raw/run07/controller.txt)共6项通过，
391次合成POST（352正常、22注入错误、8延时、9挂起）。失败为明确注入，并非真实模型错误。
[96项本地检查](raw/candidates-final-tests.txt.gz)、[8项环境检查](raw/candidates-environment-tests.txt)保留日志。
761份执行源码与服务器一致，见[源码核对](raw/source-check.json)；归档伴随文件另列，不计入源码数。
[独立清理](raw/cleanup.json)确认各专用PG停止、自有进程为空、GPU计算进程为空、会话初始ACL一致。
[材料清单](raw/artifact-index.json.gz)记录导出文本与脱敏副本摘要；单位汇总为派生材料，不冒充原始逐字节日志。
此前失败的控制器、worker日志和单位摘要也在清单中。这些材料在该阶段完成时尚未提交；后续发布状态以Git历史为准。

## 后续真实比较

下一轮主表需重新核对模型/环境身份、不可变输入、独立评价集、预热、轮次顺序及完整预算。
按16行正确性、512行三候选调参、1024行独立评价及1次预热＋3次测量，主表拟需68查询、41,024次新请求；
可选本地执行消融另列。旧额度用尽，本次没有启动该模型清单。
已有1024行评价数据保留已见身份；如采用新的独立评价，应排除历史使用数据并保存电影/评论/文本重叠检查。

新增文字、本地链接、隐私与差异空白检查通过，见[交付检查](raw/final-checks.json)。

后续用户已授权并执行[新版真实比较](../text_map_four_path_comparison_20260930/README.md)，该轮因SemLoom远程异常停止；本报告的合成验证身份保持原值。
