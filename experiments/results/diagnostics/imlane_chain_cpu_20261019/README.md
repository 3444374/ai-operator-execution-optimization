# IMLane 普通 Map／Extract 及关系链 CPU 验证

职责分支 `feat/imlane-records-chain` 固定基点 `aebbbcd8`，选择性接纳共享
`empty_prototype` 提交 `f977c979`。新增
[IMLane 通用入口](../../../../code/integrations/imlane_records/chain.README.md)复用原方法、
有限模型进程、结果账本及 Extract 关系控制；不修改作者调度、Lane、C++、旧 HTTP，
不增加线程池或 SemLoom 调度。Map／Extract 是公共链的单阶段值。

## 实际本地结果

新增八项和原有限链九项，共 17 个不同测试通过；定向复跑不相加。
普通 Map、普通 Extract、ME、EF、MEFM、EEF 六种组合分别与 LOTUS 1.2.4 accessor 的实际
messages 和最终 DataFrame 严格差分。输入为非 Q3 `title/body`，Index `17,17,29` 保留重复标签，
业务 `row_id/call_id`、额外 Extract 键和缺失键保持原语义；后继 grouped parents 不去重来源。
两块 pending 乱序就绪仍保留调用及发生项关联。

ME／EF 另用真实 CPU 子进程和三个独立 engine 身份检查装配及关闭；它们只返回声明响应，
没有模型推理。已知失败保留两项终态及 prompt 10／completion 4，阻断 Filter 后继。
作者异常退出即使物理资源已结清仍保持 unknown；已确认模型 API 证据保留。
模型关闭 unknown 保留 owner，取消前拒绝分配且单 query backend 不复用。

首次 r1 整轮失败原件保留：原 SDK 测试响应缺少 `engine_request_id`，失败工厂函数受 Python
descriptor 绑定影响，另复现公共空 Map 新列为 float64、旧组装为 object 的真实差异。
前两项只修测试 scaffold，r2 六项通过；r3 四项针对乱序 ready、准确失败身份和关闭证据补验通过。
原九项在 r1 通过且相关源码未改，没有重复执行。

空 dtype 不在 IMLane 猜测。公共 owner 的 `f977c979` 增加最终零行 DataFrame 原型，r4 两项
空源及 Filter→Map→Extract 空结果通过，严格保留 dtype／Index／列序断言，未增加模型调用。
本地监督分别观察 r1／r2／r3 的四个出生身份和 r4 的一个出生身份全部退出，无信号、残留或
cleanup error；这些观察不证明未观察后代或 GPU 退出。

私有作者 qualifier 的三个实际独占保存 API 回归及一个实际终态 SDK replay 回归另行通过，
不增加上述生产测试计数。保存采用逐查询快照和单独最终报告，不覆盖失败证据。

## 作者与模型待验范围

作者 CPU 材料固定 ME／EF 各三行，最多 12 次 CPU fixture 评价，300 秒含 30 秒清理，
8 CPU／RSS 8 GiB／SHM 2 GiB 观察限额，复用既有 Arrow 16.1 方法环境和 v2 二进制。
source、作者 HEAD／status／diff／源码／二进制、现有账本和独立进程／SHM／GPU清点均需核同。
新作者 CPU 尚未执行，不能用历史 echo、Filter 或级联资格填补。

真实模型配置保持禁用，拟用 ME／EF 各两行、新永久额度最多八次、每段新实例、无暖身和自动重试。
只重放实际终态给原 SDK，不产生第二套模型调用；每段独立 GPU 结清后才允许后继。
机器身份／当前 runtime、tokenizer／template 内容、永久账本与当前 slot 配置由唯一服务器 owner
绑定后才可启用，当前没有新模型／GPU请求。作者 Ray、多 executor、公共 selector、质量和性能另验。

精确源码与各次运行 SHA、失败原件定位见 [qualification.json](qualification.json)。

## 集成后的关闭异常复核

本次选择职责提交 `7a4524b7` 的十个所属文件，共享 `f977c979` 不重复接纳。
原17项含真实CPU child的检查继续绑定原职责来源；本轮另以当前依赖做16项无child组合核验，
不把两组数字相加。所有原作者scope／真实模型／GPU待验项保持。

独审发现公共 `NativeExtractRelationRows.close` 在阶段关闭抛错时，只保留异常名，
遗漏阶段已保存的模型API和作者物理状态。共享修补 `200330ce`、`eaa1c1d7` 在失败时
收集该报告副本、维持unknown和原错误；API的True／False字面值、已知usage和资源证据分列。
重复close不重计调用，后继仍被阻断。读取报告失败、坏元数据及None／非字典返回不跳过其余阶段清理。

5项新回归与11项旧原生合同共16项NoPopen通过；IMLane组合16项及独立author失败／model unknown
公开字段反例通过。最初公开报告缺失RED及中间坏报告反例保留，修补未认证作者环境或GPU。
精确源／测试和独审摘要见 [close_report_verification.json](close_report_verification.json)。
冻结的作者CPU包需由唯一执行owner重新绑定最终共享源码，不能沿用旧包哈希认证本修补。
