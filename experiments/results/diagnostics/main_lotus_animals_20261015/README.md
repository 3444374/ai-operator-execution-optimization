# main Animals 图文请求接入资格

本批在main LOTUS文本四臂上加入原Animals Q1的必要图文与容量闭包。软件检查与模型测试分开记录；
固定最终发布main后的真实Movie／Animals完整查询另行登记，不借旧源码的模型资格。

## 固定来源与依赖

底稿`84203e0d3e3ca5170866b081ffb6b63b6e0f76ec`；原图文与容量实现来自
`a0d1f6acdc50ebe190dbe605732be770a6b55b28`，原交付`f0b4cd9e5a99a0ff9e3a72a0fff1e1d70f784172`，
由baseline`7a4c1846fa8c973225d2fe894ad4f0ab11deded1`接纳。
代码候选`0cbb6aa4162dd8a48b278235ab84ace1eb4cab7c`含9个生产文件、3个测试文件；
后续首错修复固定`cf58d4426de3e4c0d60b99828fb9761dd330aa2e`。实际检查见[机器摘要](checks.json)。

| 依赖闭包 | 必要行为 |
|---|---|
| LOTUS sdk／batch／native | 固定`lotus-image-text-v1`源码、PNG data URI与消息结构；图文强制整批预检 |
| native_tasks／incremental_execution／ray_map_transport | 私有assembly共同接受显式native请求上限与总input，先检查Ray窗口／对象／准备容量再创建transport |
| Animals source／query／CLI | 固定原数据、发生项／像素、原ImageArray→sem_filter→COUNT及evaluator；原标签仅在查询后评价 |
| 已有四臂执行器／共享Daft runner／网关与预算 | vendor保留调度；原SDK保留prompt/parser；完整请求／响应、真实HTTP峰值和失败证据持久保存 |

三工厂仅移植`0da59fea→a0d1f6ac`的容量hunk，保留当前main的完整HTTP和身份保护。
图文SDK与入口适配当前共有Daft初始化和native iterator生命周期；未建立新调度封装。
不采用整个baseline历史、PZ Stage2／IMLane、新平台、CLIP／HSE／RM／index、临时脚本或debug副本。
[架构参照](../../../plans/系统架构.md#company-engineering-reference)沿用上一LOTUS切片：方法拥有有效请求与解析，执行层只替换物理owner；没有访问或复制公司材料。

## 容量、错误和查询边界

native和PG默认仍为1MiB，PG wire与公开PG工厂不开放本批上限。native显式逐请求可选至8MiB，非默认值必须同时给出总input且不小于单请求。
原64图选择4MiB请求、8MiB总input、4MiB+24 Ray窗口、8MiB对象额度，不启用准备broker；
它们不包含SDK／proxy副本、未接纳producer后缀或完整进程RSS。原图不resize、拆请求或借用CLIP预处理；服务processor另行固定。

非法图片后缀在Core flow／native graph创建之前失败。accepted-prefix、逆序完成、借用释放、取消和unknown工作责任保留；
unknown不因本地关闭就归零。资源门禁失败仍保存COUNT、逐行判定、质量和关闭报告。
独审发现账本收尾读取错误会覆盖先前查询异常并阻止摘要，`cf58d442`修复后以null/unavailable表示未确认计数，保留首错并附收尾说明；不退款或重建账本。

## 实际检查与未验证范围

Linux CPU使用LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0、Daft0.7.21、Ray2.56.1和Arrow24。
环境`core,text,image,semantic-benchmarks`通过，CUDA屏蔽且模型调用0。
初始provider 202项中188通过／14条件跳过；SDK／输入组合81项中77通过／4条件跳过。
实际原Animals库／evaluator四臂13项全过，真实CPU Ray大输入10项全过；原始图像像素fixture以HTTP传递，未调用模型。
账本修复后Animals 14项全过；Movie按3+1分轮，两轮各2项全过，包含四臂×原Q1/Q2/Q3共12个完整软件查询、80次查询HTTP。两轮还各自检查原SDK错误解析；计数不重复加总。日志校验值在机器摘要中。

初始Movie控制脚本将4臂传入最多3臂的旧fixture而失败，未修改生产代码来放宽用例；改为3+1的新运行身份后复验。
初始lint发现两个新测试未使用import，移除；相关失败原件保留。独立Standards／Spec在修复源码上均P1/P2为0。
PG条件跳过不计作真实运行中数据库或图像集成资格。Mac、CPU／软件HTTP和原作者a0模型结果都不代表新main模型质量、GPU收益或稳定尾延迟。

原来源的历史模型与恢复仍从[原报告](https://github.com/3444374/ai-operator-execution-optimization/blob/f0b4cd9e5a99a0ff9e3a72a0fff1e1d70f784172/experiments/results/diagnostics/sembench_animals_20261015/README.md)定位；
本批仅在当前main重新做必要资格，未重跑全部旧模型。最终真实查询采用正常共享lease、有限请求／故障／清理监督，健康累计wall不作截止。
原始CPU运行用`main-animals-cpu-20261015`及机器摘要的归档SHA定位，由维护者提供；公开仓库保留紧凑来源与结果，完整原件单份封存。

<a id="observation-body-capture"></a>
## Animals 观测模式补充修复（2026-10-16）

底稿为main `f1820755564532fff39bafc1fd0f708e0f8c7739`；[本批机器摘要](observation_checks.json)以生产／测试文件SHA固定实际检查源码，与上文和旧连续模型运行分别解释。
`performance`关闭完整请求JSON／响应base64副本，保留序号、模型、消息／body摘要、字节数、HTTP状态，时钟和usage仍由原HTTP trace拥有；`qualification`继续保存完整body。必要的JSON解析与消息摘要校验保留，摘要模式不能独立恢复原body。
两种模式仍使用同一原SDK、HTTP透传、预算和原查询；未增加队列、日志服务、runtime manager或GC策略。

Mac CPU实际固定LOTUS1.2.4／LiteLLM1.95.0／OpenAI2.50.0／Daft0.7.21通过16项Animals、25项网关及2项Daft组合回归，0跳过。新增用例在同一进程执行performance／qualification两个完整Daft查询，每次4个loopback POST，均COUNT2，质量／usage／身份／关闭一致；兼容配置复用、不兼容runner／线程数拒绝。1MiB故障请求在performance中的请求记录不足1KiB，两模式保留429状态、模型／消息校验及查询首错。
本批只验证记录体积与行为，不测内存回落、JCT收益、模型质量或其他执行臂的新资格；模型／GPU运行0，服务器与现有lease未使用。原始失败与成功日志按摘要中的运行标识／SHA由维护者提供；可复现fixture在代码测试中，旧PR29和raw不改。
