# MMQA Q2b 最小接线与 CPU 核验

本项在独立分支补充官方 Q2b 的输入、原生查询、评价和阶段观察，不修改 SemLoom 控制器。
用途是为不同于 Animals Q7 的 Join→Extract 依赖链准备一次有限观察；没有方法性能结论。
输入、真实模型及正式观察的实际状态以本页证据为准。

SemBench 固定为 `c814e3807e72d4cf876b852b17e77f3cc94575c2`，
调用[官方 `_execute_q2b`](https://github.com/SemBench/SemBench/blob/c814e3807e72d4cf876b852b17e77f3cc94575c2/src/scenario/mmqa/runner/lotus_runner/lotus_runner.py)。
上游声明 LOTUS 1.1.3，本入口选择已有可核源码的 LOTUS 1.2.4、LiteLLM 1.95.0 与 OpenAI 2.50.0，
不称完整上游环境复现。工程依据沿用[架构方案的公开接入参照](../../../plans/系统架构.md#pgml-engineering-reference)：
复用已有调用、预算和清理能力，各算子保留自己的提示、解析与关系处理；未读取或迁移公司材料。

## 保留的任务与资源职责

- 原始表 `ap_warrior.csv` 全部列和行进入原方法，图片使用原 `ImageArray`，保留目录枚举顺序。
  原生成器保留八张固定图片，再用原目录列表和 seed 42 选择额外图片；入口支持原 scale factor 25～64。
- 明确使用 exact policy。原 `_configure_lotus_for_join_type("image")` 在 exact 下不设置检索模型，
  不借用其他查询的 RM、索引或近似算法，也不调用带隐含准备工作的 runner 构造器。
- 原 Join 以 `zs-cot` 评价完整图片×表行候选；完整 Join 结果形成后，原 `sem_extract` 才接收匹配行。
  Extract 的 `response_format={"type":"json_object"}`、原提示、解析和最终 `ID/image_id/color` 三列保持。
- 对 I 张图片、T 个表行，Join 必需 I×T 次调用；Extract 最多 I×T 次，完整保守额度为 2×I×T。
  同一图片匹配多个表行时仍分别 Extract，不按图片去重，不因实测匹配少而缩减预留量。
- 调度归原生 LOTUS LM 与模型服务。既有共享 SQLite 预算在每次 HTTP 发送前扣计，失败不退还、重试为零，
  原生 LM 决定并发。进程监督负责总时长、取消时关闭预算及本地子进程回收，远端计算结束仍需独立核对。

入口为 [run_sembench_mmqa.py](../../../../code/scripts/experiments/run_sembench_mmqa.py)，
数据与原评价适配在[sembench_mmqa.py](../../../../code/src/baselines/image/sembench_mmqa.py)，
阶段观察在[mmqa_q2b_observation.py](../../../../code/src/experiments/mmqa_q2b_observation.py)。
原评价器读取未修改的 `q2b.json` 五个 gold 三元组，颜色属于质量评分，标签不进入模型输入。

## 验证与证据

[CPU 用例](../../../../code/tests/experiments/test_sembench_mmqa.py)使用真实 LOTUS、原查询和原评价器，
模型响应由 localhost fixture 提供，GPU 不可见，模型服务与 Ray 均未启动。最终 51 项检查全部通过，其中 15 项是本次 Q2b 用例，另 36 项覆盖复用的预算与监督组件。
选择性 fixture 为 3 图×2 行：6 次 Join、3 次 Extract；全匹配为 6＋6 次，达到完整保守额度。
检查完整颜色评价、相同内容的多个发生项、HTTP 错误不重试、输入与枚举变化拒绝、观察器恢复、字节上限、
必需记录器报错和监督超时后关闭预算。本项全部真实模型请求为 0。

原实现遇到空 Join 时，Extract 没有新增 `color` 列，最后投影抛 `KeyError`。
本入口保存该失败并停止，没有伪造空结果或修改上游任务。模型服务返回内容错误或 JSON 解析失败时亦保留原行为。
进程内 CPU 检查观察到 SDK socket 的 `ResourceWarning`；实际执行入口每条查询使用独立 worker，
进程退出证据与客户端复用的长期资源性质分别判断。

机器报告、每轮失败与成功日志、源码摘要及输入获取记录留在仓库外运行目录；紧凑核验记录见
[validation.json](validation.json)。早期失败包括 Extract 类名、namespace 包来源检查和关闭后读取 proxy 状态，
修复后重跑对应项；一次组合命令选错测试模块名的失败同样保留。

精确提交 `d02b4fe0a154dc54f87a4be240250b1f9f1708bb` 另通过完整 CLI 核验：
真实官方数据的 `--preflight` 为零调用，固定响应执行预留 12 次、实际 9 次 HTTP，成功 9、失败或未知 0；
本地 worker 退出码为 0、残留子进程为空、预算已关闭、lease 已归还。该检查没有真实模型或 GPU 调用。
其中每条原生查询使用独立 worker，完整 worker 启动到退出由 supervisor 的起止时刻覆盖；
worker 内应用时间与原查询时间另列，不把准备或评价从完整过程隐去，也不把 fixture 时间当成模型性能。

## 原数据核对

已从官方来源取得并保留原 ZIP，字节、摘要与实际图片后缀见[数据说明](../../../../data/README.md#mmqa-source)。
原压缩包包含 1000 张图片，其中原程序可选的小写 PNG/JPG 为 925 张；完整表为 13 行。
服务器按原生成器产生 scale factor 25，25 张图片及原表逐字节／像素核对通过，manifest 摘要为
`b9600fef2b884b7729292bac91dba3c79d4312d92b6d693587831e2e97475511`。
本次完整调用上限为 325 次 Join＋最多 325 次 Extract＝650；数据准备和 preflight 的模型调用均为 0。
该 manifest 同时绑定原源码、目录枚举、图片内容和表摘要；移动路径或重生成后须重新核对，不能复制旧摘要作为新输入身份。

## 下一次 GPU 观察

由 GPU 会话重新核对服务器空闲、独立源码版本、原始数据 manifest、模型与 processor revision、tokenizer 摘要、
服务参数和资源上限后运行。首轮只做一个 native Q2b，不附加优化臂，不隐藏 warm-up，不自动重试或调整参数。
本入口要求显式并发、输出 token、单请求字节、每阶段准备消息字节、结果字节与总时长；实际 GPU 配置由该次计划记录。

观察包括原生图片准备、Join 候选与实际消息、HTTP 提交及完整响应、Join 结果形成时刻、Extract 消费和完整查询时间。
完整查询、准备、评价及应用收尾分别记录。HTTP 与阶段时间使用同一 worker 的 monotonic clock；
媒体准备是调用墙钟时间，不能当作 CPU 核占用。逻辑结果返回不证明物理内存释放；GPU、HBM、队列和实际传输另采或记 unavailable。
必须检查事件、请求、响应、summary 与 footer 的摘要，并保留未知尝试和失败。
只有两个原语义算子实际执行并通过原评价与资源核对后，才报告该路径的真实模型功能观察；不能外推完整系统或优化收益。
