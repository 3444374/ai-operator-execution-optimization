# 容量等待与零任务 Map：服务器验证

状态：2026-09-14 服务器验证完成。原修订 `cc0f54f6` 通过 Linux/PG 检查；随后修复诊断脚本的
写入问题，`8d4786c9` 通过定向回归和普通 SQL 实测。生产 Core、组织审计、PG C/wire 在服务器验证期间没有修改。
用户本轮授权服务器测试，沿用此前“测试无误后提交、推送、合并 main”的指令。

## 目的与设置

本轮为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化补齐基础执行验证，
对应[当前计划](../../../docs/plans/data_organization_batching.md)和[本地修订记录](../capacity_wait_empty_map_20260912/README.md)。
验证容量恢复后的推进、组织开启时的零任务查询、受影响旧入口，以及客户端首行延迟的发送缓冲假设。
不运行性能排名，不调整工作量上限，不调用真实模型。

- 服务器 Linux、Python 3.12.3；PG18.3，psycopg 3.3.4、Arrow 24.0.0、Ray 2.56.1。
- 新建数据盘任务目录和隔离 PG 集群，复用已有 driver 和 PG 安装；没有安装依赖、下载模型或数据。
- [初次 preflight](raw/preflight.json.gz)和[完整 checkout preflight](raw/preflight-checkout.json.gz)均通过。
  原归档与完整 checkout 内容核对，PG 109 份非 Markdown 文件与之前已验证安装的源码一致；
  [安装二进制/扩展摘要](raw/source-identity.json.gz)、[checkout 身份](raw/checkout-identity.json.gz)
  和[最终 830 份代码/配置摘要](raw/final-source-files.json.gz)分别保存。
- PG 集成只使用测试类在 localhost 启动的 synthetic HTTP；上限 2,500 次 fixture 请求，实际 262 次。
  真实模型 POST 为 **0**，未启动模型服务；此前 1,152 次模型预算与本轮无关。

## 验证结果与完整性

| 范围 | 结果 | 原始记录 |
|---|---|---|
| Linux 调度，包括本机缺 Arrow 的 Daft/Ray 路径 | 376 项通过 | [日志](raw/scheduling.log.gz) |
| Linux provider | 55 项通过 | [日志](raw/execution_provider.log.gz) |
| PG Python 协议/合同 | 116 项通过；不是服务端 TAP | [日志](raw/postgres.log.gz) |
| 完整 checkout 的实验工具 | 497 项：482 通过，15 项需显式环境的集成测试跳过 | [日志](raw/experiments-complete-checkout.log.gz) |
| PG 组织非空、组织空选择、总量留存 | 3 项通过：3种非空模式、3种空选择及1个总量查询，228 fixture POST | [日志](raw/pg-query-integration.log.gz) |
| 旧 PG/direct 记录、超时和 SQLSTATE | 3 项通过，34 fixture POST | [日志](raw/pg-legacy-integration.log.gz) |
| 诊断脚本修复 | Linux 3 项通过；本地合并定向检查30项通过 | [Linux](raw/probe-writer-fixed.log.gz)、[本地](raw/local-focused-final.log.gz) |
| 修复后普通 SQL 诊断 | 4 次查询完成，每次128行，模型请求0 | [结果](raw/integration__buffer-probe-fixed__summary.json.gz) |

所有7个PG查询的结果文件已重新核对完整状态、行数、字节数与SHA；6个组织查询用原始输入选择得到
预期数量，再重放事件，审计结果与运行报告一致。3个空选择都无任务、POST、Job open/drain或provider session事件。
共享账本228个实际请求、7个单元全部关闭。空选择也预留了单元额度，但预留数量不等于请求数量。
[读回审计](raw/readback-audit.json.gz)、[组织/总量fixture](raw/integration__fixture__fixture-summary.json.gz)、
[旧入口fixture](raw/integration__legacy__fixture-summary.json.gz)。

这次确认了原先本地无法验证的 PG lazy-open 空选择路径。容量推进代码仍使用现有全局/Job/session额度，
后端退避与未知责任保留；本轮未发现需要继续修改生产 Core 的问题。
未重跑完整 PGXS/TAP：本轮没有 PG C、注册或协议实现变更，109份对应源码已核对一致。

## 发送缓冲的诊断结果

只运行普通 SQL：`generate_series` 提供128个整数，逐行 scalar 函数睡眠5 ms后记录服务端时间，
返回行号、有效载荷和该标记。计划为 Function Scan；未调用 SemMap 或模型。
采用同一客户端 `cursor.stream(size=1)`，有效载荷按8/128/128/8字节顺序运行，每次查询15秒超时。
计时包含客户端执行至读完；服务端标记只计算其自身跨度，不相减不同机器绝对时钟。

| 顺序 | 每行有效载荷 | DataRow 合计 | 服务端标记跨度 | 客户端首行 | 客户端接收跨度 | JCT |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 8 B | 5,906 B | 0.645607 s | 0.651467 s | 0.000509 s | 0.651998 s |
| 2 | 128 B | 21,266 B | 0.645618 s | 0.249694 s | 0.401993 s | 0.651732 s |
| 3 | 128 B | 21,266 B | 0.645580 s | 0.249875 s | 0.401767 s | 0.651668 s |
| 4 | 8 B | 5,906 B | 0.647183 s | 0.653068 s | 0.001133 s | 0.654235 s |

DataRow字节从实际三列文本重算：每行 `7 + Σ(4 + UTF-8列字节数)`，不含RowDescription等其他消息。
逐行原始标记和客户端接收时间均已保留于raw；源数据持续约0.65秒产生，但小结果只在结尾集中被客户端看到。
增大结果字节后，首行提前到约0.25秒，随后接收持续约0.40秒。

**观察支持发送缓冲解释。** REL_18_3 的默认发送缓冲为8,192字节，缓冲满时刷出；本轮小结果低于该量级，
大结果超过它。[固定源码](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/libpq/pqcomm.c#L1197-L1237)。
libpq单行模式负责收到结果后的交付，不要求服务端逐行刷新。
[PostgreSQL 18文档](https://www.postgresql.org/docs/18/libpq-single-row-mode.html)。

**不能声称**已定位旧SemMap查询中Core完成、PG收到完成、节点向上层交付、连接发送和应用接收各自的耗时。
本轮没有抓包或为SemMap节点新增时点；也没有证明旧积压128行就是PG节点留存128行。
没有修改PG输出刷新、协议或输入序。两次短重复是诊断证据，不是模型/系统吞吐排名。

## 失败与修订经过

1. 初次code/deploy快照执行完整实验工具套件，497项中10项因缺少仓库内fixture或Git元数据出错，15项跳过。
   [失败日志](raw/experiments.log.gz)保留。改用相同 `cc0f54f6` 的完整干净checkout，核对代码一致并重做preflight，
   再执行该套件通过；没有修改测试或放宽条件来消除失败。初次环境准备还误猜了扩展库目录，随后使用
   `pg_config --pkglibdir` 解析实际安装位置，PG启动前完成身份核验。
2. 首次PG集成6项都通过，随后新诊断脚本在第二次查询之后写summary时触发 `FileExistsError`。
   原因是 `write_private_json` 按约定只创建新文件，而脚本每次迭代都使用同名summary。
   [整体失败/清理记录](raw/pg-validation.json.gz)、[错误日志](raw/pg-buffer-probe.log.gz)、
   [首轮残留summary](raw/integration__buffer-probe__summary.json.gz)和两份逐行原始输出均保留。
   首轮不是一个完成的四查询实验，没有将其部分值混入上表。
3. 本地测试稳定复现2个writer错误；修复为每次查询写独立checkpoint，最终只写一次summary。
   原始写入API保持不变，并覆盖后续查询出错时保留首个checkpoint和原始错误、流中断时保留已收到的行。
   [修复前](raw/local-probe-writer-red.log.gz)、[修复后](raw/local-probe-writer-green.log.gz)。
   新提交 `8d4786c9` 仅改变这个诊断脚本和它的测试；服务器定向3项通过后，新建第二个PG集群和输出目录，
   仅执行修复后的四次普通SQL查询。没有重跑已经通过的PG/HTTP案例，也没有消耗额外fixture或模型请求。

## 清理、对课题的含义与下一步

第一段PG/fixture执行与清理57.114秒，诊断重跑与清理3.783秒；两次隔离PG均已停止，pid/socket消失，
任务进程无残留，原ACL逐字核对恢复。GPU计算进程为空。
[首次清理](raw/pg-validation.json.gz)、[修复后清理](raw/pg-probe-fixed-validation.json.gz)、
[最终进程检查](raw/final-process-check.json.gz)。原PGDATA、失败和合成输入保留在私有数据盘目录，未删除唯一证据。

本轮关闭容量等待/空查询的服务器验证缺项；普通SQL诊断说明应用首行时间需要区分服务器产出和连接发送。
C的留存可用性、D工作量控制的生效仍成立；当前tight预算的性能收益、rows/work/length稳定排名、
表征与观测成本、原生Ray/LOTUS真实质量、E/F均不由本轮证明。
这些研究比较继续按当前计划另行确定数据、配置、重复与模型预算，不自动启动。

全部导出文件经脱敏、SHA校验和读回检查；[raw清单](raw/manifest.json)含执行脚本、日志、配置与成功/失败证据。
本报告为内部验证记录；旧C/D模型预测和耗时保持原值。
