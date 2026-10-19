# IMLane 原 LOTUS 全局级联 CPU 接线

职责分支 `feat/imlane-cascade-records` 从通用级联 `6e8b1382` 建立；
[方法合同](../../../../code/src/semantic_methods/lotus/cascade.README.md)和
[IMLane 接点](../../../../code/integrations/imlane_records/cascade.README.md)分别拥有算法与载体。
新增三个 Python 模块及显式 CPU fixture，不修改公共方法、共享引擎、作者调度／队列／批推进、
Lane 生命周期、C++ 桥或旧 HTTP 配置。此处是工程资格，不提供性能、模型质量或 GPU 结论。

## 本机验证

`imlane-cascade-cpu-20261019-r4`：新增 11 项级联与 3 项普通 Filter 回归通过；
另 3 项原 LOTUS 差分因未开启 `RUN_LOTUS_CASCADE_CPU` 跳过，随后按原名启用补跑，3 项均通过。
共 17 个不同测试通过，不将定向复跑重复相加。进程监督确认全部已观察 owned 出生身份退出，
无信号、剩余 PID 或 cleanup error；该观察不证明未观察后代或 GPU 退出。

非 Q3 的 main `title+content`、helper `synopsis` 使用不同指令，并与原 SDK 的消息、采样重复项、
阈值／统计、用量和最终决策逐项差分。原 Q3 指令另作回归。角色、阶段、查询与 call ID 显式传值；
重复采样来源不合并；已知失败项保留 prompt 75／completion 5，剩余 5 项用量保持未知。
模型 API 关闭不确认、作者异常退出及晚到 stderr 写入失败均阻断后续阶段；重复 close 不恢复健康。

失败原件保持：r1 因 macOS 受限进程枚举失败，后续已确认该旧出生身份消失；r2 有一项预期异常类型
与通用控制器实际缺项错误不符，修正断言后 r3 五项定向回归通过。r4 的三项 skip 和启用补跑分列。
没有安装依赖、真实模型请求、GPU 请求或 Ray runtime 初始化。

## 作者 CPU 窗口

冻结公开源码包后，通过唯一执行会话安排两条合成十行查询，最多 50 次 CPU fixture 评价，
总限 300 秒含 30 秒清理，8 CPU／RSS 8 GiB／SHM 2 GiB 观察上限。每个非空逻辑阶段一个
作者 CPython scope／解释器，原 DBEnd 异步提交及收集；物理 block 不构成算法查询屏障。
复用已核的 Arrow 16.1 方法环境与 v2 二进制；原作者 checkout 的 HEAD、tracked status／diff、
12 项源码和 3 项二进制前后必须相同，不增加作者补丁。

原 r2 包的首轮作者运行在 98.92 秒失败：私有 helper 向真实 `write_private_json` 传入不支持的
`replace=True`，generic 的三个 scope 已退出，Q3 未启动且最终报告未生成。失败目录及归档原件保持。
修补仅将保存改为独占的逐查询快照和单独最终文件，三个实际保存 API 回归通过，未改共享 writer
或生产源码。协议／资源两项独立审阅接受 r3 包；其 1060 个公开源码与 r2 逐字节相同。

`imlane-cascade-author-cpu-20261019-r3-output-r2` 单次补验通过，监督耗时 171.03 秒，未自动重试。
两条十行查询各执行 helper_score 10／threshold_sample 5／fallback 7，共 44 次 CPU fixture 评价；
原 SDK 决策、stats、LM usage、逐调用 messages SHA、角色／查询／Arrow 16.1 关联均一致。
抽样索引均为 `[4, 9, 4, 9, 6]`，保留重复来源；每查询三个独立 engine 正常关闭。
两个查询快照及最终报告成功保存。

独立审计确认 77 个已观察 PID／出生时间实例和 28 个作者 PID／start_ticks 身份退出，六份作者
scope 与模型子进程关闭回执通过。正常 lease 已释放；无强制清理、cleanup error 或残留 SHM，
GPU 为 1 MiB 且无计算进程。作者 HEAD／status／diff、12 项源码、3 项二进制及八份旧账本未变。
两次活跃采样最大观察 RSS 1,812,480,000 字节、SHM 134,234,912 字节，采样进程使用 CPU 0–7；
这些数值不代表完整运行峰值或 cgroup 硬限制。证据包 64 个常规文件已逐成员核验。

作者 CPU 资格只绑定该 r3 来源；真实 helper、阶段间真实模型／GPU 退出、作者 Ray 和性能继续
pending。普通 Filter 的成功短 IPC 模型单元另有来源，不转授本级联或后续集成提交。

来源摘要、私有原件定位和实际运行边界见 [qualification.json](qualification.json)。

## 通用级联公开入口

独立 `record-cascade-check/run` 显式选择 `imlane-author-local-model-cascade`，
[精确schema与调用合同](../../../../code/integrations/imlane_records/cascade.README.md#公开通用记录入口)
要求caller提供instruction、两个角色部署／生成配置、有限源JSON及正常owner上下文。
不约束Q3或调用Movie evaluator。原 `run_cascade`、阈值学习／抽样／校准代码对象、
IMLane backend／作者C++／v2桥和模型引擎均未修改；旧Movie级联命令保持原身份。

新source只将main/helper原列序的并集及发生项身份同值写入有限CSV；作者原生扫描、准备和
执行，driver仍由原全局方法决定逻辑阶段。完整决策按原发生项关联返回幸存关系，保留Index、
未投影业务列、精确float64／uint64及最终空prototype。来源字段不替换身份，重复抽样独立记call。

正常外部lease、StopBudget、永久unit与最终回执编排从普通链worker提取为单一共享函数，
两条入口只传语义预检／执行回调，不传假链或另设scheduler。原方法给出Q=2N+S，
每query有限上界和每stage实例cap分列；已预留unit一次claim覆盖max(1,Q)，少用／失败不退款。
在原生命周期检查处复用GPU门，前阶段API／作者资源／GPU未结清则阻断后继；unknown不升级。
无execute仅预检，不落CSV或建backend／作者／模型，不claim预算。

最终64项新CPU合同固定当前源码：27入口／67子用例、21 owner和16 source，零fail／error／skip。
实际LOTUS1.2.4与原SDK全局差分、公共prepare／parser／全局方法、真实临时CellBudgetLedger、
私有JSON／CSV及哈希是真实实现；作者执行、模型响应、ABI／GPU观察均为显式fixture。
全局NoPopen／NoSocket及离线限制阻断真实子进程与网络；本轮没有服务器或模型／GPU动作。

另70项旧普通链／owner回归及20项旧Movie级联CPU通过。早先91项组合绑定新入口较早源码，
不把它改绑为后续关闭修补；最终64项重验已覆盖当前新入口／owner。旧普通链方法和Movie原14函数
及共享owner提取的AST兼容另有独立检查，不用模型矩阵重复资格。
暂存检查仅移除共享owner末尾一个多余换行，AST相同；旧70保留原字节来源，最终64在清理后
重验，合计92个子用例通过，重复运行不累加测试数。

独审普通RED保留：backend.close返回含unknown的阶段列表，校验先抛错；此后metadata getter
也抛错时，已知API／物理回执漏出report。修补只提前保存完整owner_close快照再验证，
新交叉回归保持原primary／calls同对象／knownusage和unknown，后继仍被阻断。测试首次误用
ordinary账本counts字段、cascade profile fixture缺字段和早期WIP运行亦保留，不记为生产失败。

最终源／测试SHA、分组回执、原RED、完整脱敏日志恢复映射及独立审阅见
[public_selector_verification.json](public_selector_verification.json)。CSV／DTO／字段及额度限额
是逻辑入口边界，不证明完整查询RSS硬限额或吞吐。旧r3作者44fixture、当前入口CPU和未来真实
helper／模型／GPU／性能保持各自来源；服务器正在使用的8d35普通链源码包不变、不重打包。
