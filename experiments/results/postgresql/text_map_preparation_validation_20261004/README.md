# SemLoom 准备分段的 PG 生命周期、模型与物理内存验证

2026-10-04。main `380824f2` 的两条接法累计完成 **16 项 PG 生命周期检查、26 次本机 HTTP**。
随后真实模型清单在两个 16 行检查后停止，已消费 **32 次模型请求**；尚未执行 1024 行预热或测量。
发现准备路径的对象观测遗漏正在 put 的缓冲区，原失败保留，不能据本轮判断完整查询收益或推荐启用。

## 来源与实际设置

问题来自[现行验证计划](../../../plans/查询调优.md#map-preparation-pg-real)：
验证显式准备分段的 PG 接入、真实模型完整查询时间及进程物理内存。
该选项复用[准备原型](../../diagnostics/map_input_preparation_20261004/README.md)，默认即时执行保持。

服务器从 GitHub 取得 main `380824f2951f6843bb7f326c3f65c95b7cd4352a` 的独立源码，
861 份代码与 runtime 文件摘要在模型运行前后核对。driver 的 `core,text` 环境检查通过；
当前 main 在 Linux 完成 230 项相关检查，全部通过。
实际版本为 PostgreSQL 18.3、Ray 2.56.1、Daft 0.7.21、PyArrow 24.0.0、psycopg 3.3.4。
PG 软件与扩展沿用[此前已核对的安装](../text_map_arrow_real_20261003/README.md)：
从 `1be050a0` 到被测 main 的 C/SQL 源码没有变化，未声称重新编译。

真实模型为已保存的 Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28`，
9 份模型文件逐字节摘要通过；vLLM 0.25.1、单张 RTX 4090/GPU0、bfloat16、单份张量并行。
服务长度 4096、显存比例 0.8、先到先服务、最多 128 序列/8192 批次 token，关闭 prefix caching、开启 chunked prefill。
输入复用原 Movie 16/1024 行集合及完整 prompt，temperature 0、输出上限 128；不是新的独立评价集。

两臂均为 SemLoom Daft/Ray 内部对照：请求/work 64、PG 窗口 256、batch 16、传输 2 MiB、对象 4 MiB；
Ray 8 CPU/0 GPU/256 MiB 对象存储，调用方拥有两个各一 actor 的服务及共享 gateway。
候选另启一个准备线程，编码 2 MiB、ready 4 MiB、ready work/共享块额度 256。
同步持久记账及日志设置相同；没有运行原生 Daft/Ray、新的多 Job 或多模态比较。

## PG 生命周期与准备失败

用户授权原清单最多 1024 次本机 HTTP、10 分钟，首次非预期错误即停；每次失败后分别授权新的有限继续。
前面已通过的检查不重放，各次失败与新台账分别保留。

| 运行 | 状态 | 新完成检查 | 实际 HTTP | 说明 |
|---|---|---:|---:|---|
| `pm4l` | failed | 0 | 0 | 辅助模块目录权限错误，PG 已退出 |
| `pm4b` | failed | 2 | 3 | 进度记录重复创建已有文件，停止并清理 |
| `pm4c` | failed | 3 | 4 | 探针使用无数据表的 Map，当前实现要求从一张表取输入 |
| `pm4d` | passed | 11 | 19 | 改为表列输入及 PG 规范错误消息，跳过已通过五项 |
| 合计 | — | 16 个不同检查 | 26 | 真实模型请求 0 |

每种接法各验证 NULL/LIMIT0、乱序完成后的关联与行序、空文本、重复读快照、行级权限、函数权限，
以及预期 HTTP400 后恢复、发送前取消、HTTP 已开始后的取消；取消保留 SQLSTATE 57014。
模型拒绝按当前数据库映射核对 SQLSTATE 38000/规范消息，而非把 wire 错误码当 PG 消息。
PG、Ray、gateway 和本机 HTTP 均正常退出；最终对象、输入、结果、请求/work 记录归还。
两臂的 PSS 采集可用。完整检查与失败原因见[独立复算](raw/initial-verification.json)。

## 真实模型停止与对象观测修订

用户授权的独立清单为最多 8224 次真实请求、20 分钟，两臂各 16 行检查、1024 行一次预热和三次交错测量。
实际运行 211.532 秒；原路径 16 行通过，准备路径的 16 行均返回后在资源核对处失败。
台账、访问日志与服务成功时间序列均为 32；第一次非预期错误即停，没有退款、重置或自动续跑。

离线从私有输入、PG 发送前绑定、完整请求和结果重新核对 32 行，关联、行序、完整请求多重集、
逐行 guard 和 Core 请求/work 记录通过。整体运行仍保持 failed，1024 行测量为 0。
首个错误事件为 `core_ray_block_reserved`：已生成 1939 字节，但 `object_bytes` 为 0；
后续 put 完成才记入 1939。旧检查器要求已观测对象的字节总量与留存记录逐项相同，因此按实际数据正确停止。

准备接口的修订在表生成后立即记入 `amount`，计数持续到最后归还；
对象状态和对应事件在同一把可重入锁内推进，put 发布先于可能的完成/取消释放，最后一行取消也记录释放。
put 失败仍归还本地缓冲区并保留失败，不把它记成成功 put。检查器保持严格，模型提交上限与默认路径没有改变。

新增三个本地回归覆盖正在 put 时前一块返回、就绪块最后一行取消和 put 失败。
前两个在原实现分别失败/报错；修订后相关 236 项中 225 项通过、11 项缺库或平台跳过。
修订后的 Linux 实际库、PG 与完整模型清单尚待检查，原 main 的样本不替代新修订的验证。

## 物理内存口径

实验辅助采样器每 200 毫秒读取 RSS（驻留页字节）和 PSS（共享页按进程分摊后的字节），
按 PID 与创建时间识别进程，同一时刻去重后求和；角色峰值不相加。
查询采样包括 PG、gateway、driver 与 Ray 进程树；模型服务在就绪后另采样，不覆盖模型加载峰值。
缺项保留原因，不填零；短于采样间隔的峰值可能遗漏。该采样器只用于实验，没有修改生产内存管理。

| 16 行检查 | 采样数 | 执行进程同时刻合计 PSS 峰值 | PSS 缺项 |
|---|---:|---:|---|
| 原即时路径 | 14 | 1638842368 字节 | 无 |
| 准备路径 | 15 | 1658802176 字节 | 无 |

这是短检查的实际观测，包含共享运行时及常驻 worker，不能解读为准备数据大小、硬内存上限或 1024 行峰值结论。
模型成功返回不替代完整性能对照；目前继续保留原默认。

## 保存与独立清理

服务器原件与独立本地备份标识为 `GPU-map-preparation-real-20261004`；80 份 PG 原件、83 份真实运行原件的
字节数与 SHA-256 已核对，SQLite、原始评论、完整请求及真实环境留在 Git 外。
公开材料仅保存必要的配置、摘要、全部事件/采样、失败、源脚本与检查日志；请求正文作 compact 投影，命令与异常脱敏。
[恢复清单](raw/storage-manifest.jsonl)与[恢复核验](raw/storage-validation.json)定位 156 份记录，
142 个独立归档对象共 282656 字节。原样本的错误字段保留，未伪造或修正历史事件。
公开恢复记录配合私有输入，从[离线脚本](raw/analyze_initial.py)得到相同16项PG检查、26次本机HTTP及32行模型关联；脚本不访问模型。

独立检查确认本轮 PG/Ray/模型服务退出、端口关闭、临时父目录 ACL 恢复，扩展摘要不变，两张 GPU 各 1 MiB。
模型退出日志有 semaphore 警告；清理后的独立检查该类文件为 0。完整原件保持，后续运行使用新标识及明确授权。

```sh
PYTHONPATH=code python3 code/scripts/analysis/restore_result_evidence.py experiments/results/postgresql/text_map_preparation_validation_20261004/raw/storage-manifest.jsonl --output /path/to/new-private-preparation-evidence
python3 experiments/results/postgresql/text_map_preparation_validation_20261004/raw/analyze_initial.py --evidence /path/to/new-private-preparation-evidence --inputs /path/to/verified-real-inputs --output /path/to/initial-verification.json
```
