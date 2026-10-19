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
