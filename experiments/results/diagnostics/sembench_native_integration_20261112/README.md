# SemBench 原生 baseline 接入的分路径验收

本报告维护 `integration/baseline-general-20261112` 后续接入的验收身份，保留原 SemBench
`c814e3807e72d4cf876b852b17e77f3cc94575c2` 的查询与评价。它不是全套 baseline 完成报告，
也不把不同原生算法、冷暖条件或后端的单次样本排成性能榜。
当前实现范围见[能力表](../../../../code/INFRA_STATUS.md#current-refactor-status)，
执行与计时规则见[原生接线说明](../../../../code/src/baselines/README.md#sembench-native-query-timing)。

## 完整 Movie source 与 Kalypso Join

源码 `4f12b556e3911497bb15fd21047aa70f71265a5d`、code tree
`b6ba58486d675d7457c42e4494fc168201ae6472` 移除共享 Movie 读取的三列投影，保留原 CSV 全关系；
只有原算子引用的列进入模型消息。11列样本恢复22列 Join 关系。131项受影响软件检查为128通过、3跳过；
跳过项不能当作通过。Daft／Ray 各64条历史实际答复的完整源回放保持消息、冻结输入 token、
完整关系和最终 sink／评分，新增模型调用0；旧三列来源的模型耗时不回填。

固定 Kalypso 作者 `05024f027f932c0e398852e38302be30b9015481`，运行
`baseline-4f12-movie-fullcsv-cpu-20261113-r1` 及其 GPU 子批次，先通过完整关系门，
再执行 Movie Q5 原序8×8样本。原作者 CartesianProduct→SemFilter、8-token 输出上限及原生调度保留。
实际64次请求全部成功，未知0、重试0；得到20×22中间关系、10行原 sink，原评价为
precision=1、recall=0.8、F1=0.888888888888889。
模型就绪后完整 source→sink 为2.350992505秒，模型启动40.220577725秒、额外预检8.198883225秒另计。
本次为单样本、单冷轮；原生 prefix cache 开启、结果缓存关闭。完整消息及CPU token参考已核，
GPU实际输入／输出 token 数组、usage、金额和kernel计数未观测。物理清理、租约释放通过。
不转授 full256×256、其他后端或后续组合树。

## Kalypso 原作者完整排名

源码 `11eff9cd7b8b4f24138be2ea8095225abc13e752`、code tree
`4ae252a7fc0fa358905696d4ffd648b425dd3b03` 使用同一原作者来源的 `SemTopK(k=N)`：
N<8用插入，否则递归归并；5-token A/B比较、并发20和作者默认引擎保留。
它不同于 LOTUS quick TopK。原 Q9 TopK 分支继续拥有完整排名到评分值的变换及评价。

`baseline-11eff-kalypso-topk-cpu-20261113-r1` 的5项软件检查和原作者0／1／8行门通过；
8行检查覆盖56个有向比较的完整消息、token及上下文，模型调用0。
随后 `baseline-11eff-kalypso-topk-gpu8-20261113-r1` 在同一8行11列原 CSV 上执行一次真实模型资格。
永久预留上界28次，实际16次成功比较，未知／重试／额外预热均为0；未消费额度不退款。
完整排名为 `[6,1,5,3,4,7,2,0]`，原 rank-to-score 与评价经保存答复重放通过，
Spearman=0.60616080621109、Kendall=0.49135381491199537。
模型就绪后完整查询1.041000821秒，模型启动41.215229964秒另计。

16次实际 generate API 输入 token 数组与 CPU 参考一致；API逻辑用量为prompt3954、completion32，
total3986。它们不是缓存未命中量或GPU kernel工作量；输出token数组、金额、纯kernel计数未知。
原生prefix cache开启、结果缓存关闭，物理清理与租约释放通过。此资格只覆盖8行功能样本；
full256及其他后端 TopK 仍须单独接线／验证。

<a id="imlane-boolean-join-sql"></a>
## LOTUS exact Join over IMLane prediction 的真实 SQL

源码 `8667d526c83f26679ec7efb3dfea8d52584e68cd`、code tree
`986a62fa65f5ff98ea7c7c1ccaadeff9f16759b0` 扩展既有零模型 SQL 资格入口的
`--saved-reference`，复用同一 Boolean 任务、真实 DSO／驱动／UDF／Arrow／Unix IPC／完整 sink ACK。
只有模型进程由显式软件服务替代。保存参考绑定每个 occurrence 的消息、token、答复、完整关系、
最终 sink／评价；同消息不同答复不合并。没有新增 native SQL 或逐题执行器。

运行 `baseline-8667-imlane-join-sql-cpu-20261113-r1`，Movie Q5 原序8×8，源11列，使用64条
历史实际答复作为软件 fixture。参考 SHA-256为
`1d4ef86443cf6e30e8408ef9db5544d1a9604a8193cbfb552d13aefe5a215fff`；
原答复重放不是本次模型质量，最终空表也不能代替完整中间关系检查。

| 门 | 实际 native AI SQL | 软件答复 | 验收 |
|---|---:|---:|---|
| complete | 1 | 64 | 完整8×22中间关系、原 sink／评价、ACK后退出 |
| missing-row | 0 | 0 | 原准备完成后删一输入行，在provider前明确拒绝 |
| duplicate-id | 0 | 0 | 原准备完成后制造重复occurrence，在provider前明确拒绝 |

三门全部通过，模型调用0，物理资源／SHM／租约清理通过。复用既有 DSO
`52bf5763aab6f35b76746a9fc15b75e27bac1534315d1a20926eb4a78c2badaf`，
其原构建身份为 `author_build_mode=patched`；未重编DSO、重链接driver或重跑旧链接探针。
逻辑 Join 仍属于 LOTUS exact，IMLane拥有 prediction与原生传输；不能称为复现原作者 Join 算法。
真实模型质量、性能与其他输入范围仍未由这三门验证。

## 旧 Ray Q3 专项图退出维护

当前生产入口已调用共享 `execute_prepared`，旧 `execute_q3` 只剩测试调用；本次删除该专项
Filter／COUNT图及callbacks，保留原题任务层。Movie Q1–Q4 使用真实官方函数、LOTUS SDK及同一个
软件 Ray processor 做消息／完整 sink 差分，重复业务ID继续按 occurrence 关联。
现用 prepared 函数、默认参数、原生图、预算及关闭实现逐字不变；HTTP profile 保留其不同服务拓扑。
旧 API 的默认重试／caller runtime 不能冒充新图的隔离零重试，恢复方式及差异见
[历史入口](../../../reproduction/native_baselines/README.md#retired-ray-q3-native-binding)。

81项不同受影响软件检查为78通过、3跳过，模型调用0。首轮新增四题差分误把参考helper的
`(messages, options)`批列表当作平铺消息，断言失败；修正夹具展开方式后四题全部通过，
首轮失败及修复日志保留。3项跳过为独立Daft SDK和未绑定的两个可选真实数据环境变量，
不据此判断资产缺失。此清理不新增任何模型／性能资格，也不承担项目整体历史清理。

## 原件恢复与独立核验

下列私有归档由服务器协调会话《统一安排服务器 GPU 测试》按运行标识提供，完整清单、配置、
source binding、日志和观测保留在归档内；公开仓库不复制真实机器路径、模型回复或runtime配置。
本接入会话重新核对每个payload及清单的字节数／SHA，未重复执行模型。

| 归档身份 | 字节数 | payload数 | SHA-256 |
|---|---:|---:|---|
| 4f12 CPU | 见原清单 | 91 | `0c5a7feb696868ba9ba3eb802f5a3229e77fce9f3e9dbed353034c1098b1717e` |
| 4f12 GPU | 见原清单 | 111 | `b7ae2399c9b0278a549069a60cbe2a96e0b27d95078b421b7e77f01a32b49695` |
| 11eff CPU | 123286 | 102 | `9234bcc5e0185aeea54684b21ded6ac54768c7a16c0b993fcf1d056c6002fc36` |
| 11eff GPU8 | 652933 | 220 | `d310d0585d086d823c923c7dcbb32888bc1b614f20a0084555f8ffe9a89e21de` |
| 8667 SQL CPU | 342404 | 177 | `ac98ad437e07f71486442606720d0622134c0d833e0ff1c1304f1130b1487b73` |

payload数不包括每包另存的一份清单。已完成的资格不重复请求；缺数据、SDK接线限制和未运行模型
分别记录在[当前状态](../../../../code/INFRA_STATUS.md)，不由本报告扩展支持范围。
