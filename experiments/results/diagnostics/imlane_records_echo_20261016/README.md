# IMLane 多行记录与资源责任检查

本轮完成独立无模型记录桥：实际作者 DBEnd → Coordinator → Lane → 一个 CPython executor →
多行 Arrow 输出。研究对象为 AI 语义算子的数据执行；这里是独立 DBEnd 记录桥，
为新增数据执行主链提供接入依据，不提供模型、SQL、质量、吞吐或 GPU 资格。

实现和接入顺序见[记录桥](../../../../code/integrations/imlane_records/README.md)，
紧凑原始结果及准确来源见[qualification.json](qualification.json)。
职责分支以 `e9a54e3e43ef9ce9e3b6973530715655028b1da5` 为基点，交由数据执行集成负责人选择性整合。
原职责交付未修改公共方法、source、ledger、engine、Ray Data、SemLoom 静态参考或原 HTTP 桥。
主线仅选择其新桥与本报告目录，未恢复旧 HTTP 执行树。

## 实际来源与设置

作者来源为公开 [IM-DM4AI/IMLane0](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)
`072f8257db80569e42c9f5727d839587df529652`，使用已核身份的既有 fixture 配置／事件循环／退出补丁构建。
新桥只链接现有 prefix，源码与原二进制摘要匹配；没有重建作者组件或修改共享环境。
作者 Python3.12.3、Arrow16.1.0、NumPy1.26.4、Ray2.56.1、psutil7.0.0 预检通过，Ray 未启动。
本机 Python3.12.6／Arrow24 只执行独立 Python 检查，没有用于重建作者库。

实际使用 CPU16–23、每scope一个 CPython executor、GPU可见范围为空、模型／HTTP请求均为0。
宿主使用项目既有互斥lease和 `AuthorScope` 的 PID/start ticks（进程出生标识）、namespace、退出与清理核验。
有限资源／停止条件在实现 README 中预先记录：首轮7个scope，独审补强后增加两个受影响scope，另补同内容健康1个，累计10个。
没有新增调度器、线程池、模型服务或机器。

原服务器main的首次preflight因旧清单缺少 `imlane-runtime` 组而未生成报告；改用本轮当前项目清单后通过。
Library五份附件已通过支持流程材料化到本机，核对字节数、681／179／122／226／9行及文件标识后仅作为参考，
没有直接执行附件。初始Python下载缺CA，使用系统现有CA完成TLS验证，没有修改系统安全设置。

## 实际结果

| 检查 | 首轮 `echo-r1` | 补轮 `echo-r2` |
|---|---|---|
| scope数／预期结果核对 | 7／7通过 | 2／2通过 |
| 健康路径 | 2查询、4物理块、7行、实际1个worker | 相同规模、实际1个worker |
| 收集前pending块数 | 2 | 2 |
| 输入拒绝与隔离 | 跨块callID重复、错query、空表、NULL、负row、错类型／stage等10项 | 相同检查通过 |
| 复制原语 | C++可变Lane缓冲区改写后旧结果不变 | 最终头文件同项通过，含nullable=False |
| 实际Lane后的旧C++Table | 未独立记录；旧字段只检查Python值 | 后续collect触发3次逐值重读，均一致 |
| 输出故障 | 乱序、缺行、互斥字段错误、明确worker错误、过大缓冲区各触发目标检查 | 未受影响的5项不重复执行 |
| 裸Python异常 | 原生future不就绪，3秒期限后监督清理 | 作者日志核对到目标异常，同期限及终态检查通过 |
| RSS峰值 | 161,484,800 bytes | 152,424,448 bytes |
| 共享内存文件总大小峰值 | 134,234,912 bytes | 134,234,912 bytes |

首两轮健康检查都保留重复 `row_id=7`、不同 `call_id`，并核对中文、换行、NUL字符串及worker模块状态跨块／查询复用。
查询二可以重新使用相同callID，而旧查询记录被发送前拒绝。每个块只在查询仍活动时提交，未添加逐块查询结束等待。

同内容补轮 `echo-r3` 使用同一最终桥，只将fixture文本统一为完全相同内容；仍以不同callID保留每次发生。
该轮一个健康scope通过两查询、4块、7行、2块在途、3次旧C++Table重读和正常清理，
RSS峰值152,150,016 bytes，共享内存134,234,912 bytes。首两轮文本含发生项后缀，不作为完全相同内容的证据。

最终 C++ 原语检查有9类负输出、真实缓冲区改写、未就绪／重复收集和future抛错后的单次消费检查；
本机和作者环境分别9项Python行为测试通过。两份独立只读审阅发现的期限、采样错误记录、故障原因匹配、
nullable和收集后终态问题均修复；两审接受的最终源码摘要与实测构建一致。

## 异常与资源证据的读法

固定作者 `include/dbend/dbend_context.hpp` 的摘要为
`f524d36e40dcfa6540a1105df65e875d6b34cf04fce3fdf631631b5d1c3fca9b`。
`HandleExecuteFunc` 先调用转换器，再删除SHM对象并发送 `RestoreLane`；转换器抛错会跳过后两步。
本桥在转换器内保留异常，并在作者回调返回后通过future的单次消费重新抛出。
这保留全部输入／输出检查，不需要新的作者资源清理补丁。作者任务异常或worker错误使owner终态，未当成健康实例继续使用。

8个scope的作者正常退出核对通过；另外两次裸异常的正常清理字段均为false，分别由既有监督器终止一个仍等待的桥进程。
两次Coordinator都正常退出0；全部10个scope最后自有存活进程为空、SHM为空、线程停止，未使用强制SHM删除。
三次运行的宿主lease均正常释放并另行只读核对不存在。
因此裸异常只取得“期限触发、终态拒绝、监督清理完成”的资格，不能称为作者已能返回任意Python异常。
future完成也不构成逐次Lane归还确认；这里分别保留同Lane后续成功调用与owner最终资源核验。

工程参照沿用[架构计划§8.7–8.8](../../../plans/系统架构.md#company-engineering-reference)中
结果关联、查询资源独立、昂贵实例与查询结果分开的原则。本次没有读取、复制或上传公司材料；
公开作者符号、采用决定和当前自有落点如上，未扩展PG载体或模型架构。

## 原件与后续接入

132项完整配置、构建身份、日志与清理原件保存在仓库外归档 `records-echo-evidence-final.tgz`，
SHA-256 为 `a2c702c0f408ade3a51efc0ee18e5a761e39a4fc80ba105a11fd3232c31d89e5`；
本机私有证据目录和获准机器的数据盘均保留，按该摘要向本任务执行环境获取。
初轮与最终源码、二进制身份分别保存，首轮结果未重新绑定到最终新二进制。
公共JSON只剔除动态PID／运行目录等定位字段，保留全部scope、精确错误、强制清理计数和关键原值。

后续集成只需接入五列轻量记录、固定worker UDF、有限块提交与收集，以及现有进程owner的停止回调；
上层算法继续拥有方法、准备／解析规则、查询期限及结果交付。
本轮不实现本地模型worker。作者Ray的GPU资源声明、环境传播、按解释器固定角色、模型关闭重试和后代退出核验仍pending，
不能借用此次echo资格启动真实模型或声称完整data-to-local-model已完成。

## 主线选择性集成

`78f05783` 的原 7 项源码摘要及 10 个作者 scope 保持原身份。由于当前主线已移除旧验证树，
集成只将需要的 `AuthorScope`、停止预算、start-ticks、namespace／SHM 核验提取到
`code/integrations/imlane_records/owner.py`；验证器改为独立导入它，并使用现有 runner lease
的显式 `recover_stale=False`，已有 host lease 原件不被自动接管。

集成第一轮 Python3.12.6／Arrow24 的 15 项本地检查及两份原始记录保留。独审随后发现 host lease
可能在资源 unknown 时自动释放、身份读失败可能被当作退出，以及 bridge stderr 直接落盘。
修复后 22 项本地检查通过，包括原 9 项客户端／echo 检查与 13 项 owner 检查；另检查 Python AST
与 `verify.py --help`。本轮确认资源 unknown 时保留 SHM／本次 lease、最终结清后显式释放，
不删除 foreign lease，首次 root 身份读取失败仍登记为 unknown，stderr 脱敏后写入并核验 writer 退出。
这些检查使用本地控制 fixture、
临时文件和身份替身，不访问作者环境、不编译 C++、不启动 Coordinator、executor、Ray 或模型。
新增 patch 身份和本轮复测在 `qualification.json.integration_patch` 单列；原 `reviewed_sources_sha256`
仍对应交付提交，原两审接受与作者实测不自动适用于新helper。新helper修订另经Spec／Standards独立复核，三项生命周期／脱敏问题均已关闭；这不替代作者实机重跑。

此次集成尚未重跑作者 scope。当前 `collect_ready` 遇 worker 错误会拒绝整块，未来接模型时
还须保留块内已完成调用的 usage，并显式映射主链的模型角色、阶段与发生项身份；echo 资格
不证明这些方法和账目接线已经完成。
