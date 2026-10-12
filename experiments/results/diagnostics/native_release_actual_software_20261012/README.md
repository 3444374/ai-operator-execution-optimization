# 固定发布版的真实 SDK 与原生图软件核查

本记录保留已发布4566f1fc在既有服务器、CPU8–15上的有限软件验证。没有安装依赖、重建作者库或运行模型／GPU；IMLane及2000行矩阵未重复。
自动机器模板的CPU项仍为8<10，相关既有依赖项通过；不宣称完整机器模板或模型环境资格。

## 第一窗口：部分通过，整轮未通过

原窗口上限600秒、256次localhost POST。监督从首次测试进程启动计时，实际结束363.192秒，累计154次POST，未超额度。
首个脚本因common模块重名在用例执行前退出，POST0；失败保存，纠正后沿用原始开始时刻与计数器，没有重置窗口。
共执行17项：14通过、1项失败（两个backend子用例失败）、2项用例错误，另有1项类初始化错误；0跳过。

| 检查 | 实际结果与范围 |
|---|---|
| 共享SDK | 三项固定OpenAI／LiteLLM解释差分通过；输入是保存响应及参考MockTransport，真实SDK运行，未发送HTTP。正常usage／logprobs／可选字段、错误状态／坏JSON／缺内容／timeout、一次客户端连续解释均核对。 |
| LOTUS＋Daft／Ray Data | 四项通过，64次真实fixture POST；原方法正常、cache／统计、异常类型、完整响应、两阶段等待与提示保持，非法参数／分派前取消为零发送。 |
| 原生图 | 重复身份／完整HTTP错误、字节超额及取消／期限通过。提前关闭的到达计数断言分别4→5和23→24失败；缺少发送与关闭的同域时间戳，不能据此认定新请求是在关闭后发出，更不能认定历史泄漏。 |
| 真实客户端 | keepalive四次请求复用同一连接通过；真实关闭后注入OSError，首个字节错误及清理诊断保持。cookie脚本漏开follow_redirects，解析空307响应失败，尚未取得该项资格。 |
| 观察模式 | 发送前预算拒绝通过；模式差分因concurrency2不能承接两actor×两async batch在分派前拒绝，新增POST0，尚未取得差分资格。 |
| Ray Core参照 | 原方法／cache／SDK正常通过，12POST；后续类由两种导入名重复初始化Daft而失败，actor CPU／终止检查未运行，非六条主路径资格。 |

监督及独立只读复核均确认登记的82个进程身份无存活残留、无CPU8–15以外的已登记进程，GPU compute列表为空。
这些数据只支持对应组件及软件fixture范围；没有实际模型质量或性能结论，performance仍保留响应转换及日志开销。

机器事实和分项结果见[verification.json](verification.json)，首轮失败、日志、收据与验证脚本见[observations.tar.gz](observations.tar.gz)。

## 第二窗口：失败与未覆盖项的有限复核

父任务另行划定600秒／256次POST的新窗口，继续CPU8–15、模型／GPU0；首轮原件和计数保持独立。
先明确cookie重定向选项、有效Ray参数，并用发送／关闭时间戳区分在途请求与关闭后新开始。
原生无法同步确认的责任继续保存为未知，提前关闭不得改写为clean，不更改厂商调度。
观察模式使用原Movie Q2/Q3与官方评价、八行明确software_fixture，三条原生路径分别连续两查询；不将fixture标签核对写成真实模型质量。

公开CLI零POST预检暴露只读SemBench Git checkout被私有产物检查拒绝。修补仅为checkout来源路径关闭该私有路径检查，
其固定commit／tracked内容核验仍由verify_checkout执行；模型配置、清单、CSV、输出与运行配置继续按原私有路径规则检查。
本地30项受影响回归全部通过、0跳过；第二窗口结果待本次实库执行后补入。

task-10独立async交付02a75cc4的作者生命周期在153POST后因307缺cookie停止，作者Ray及Q1未启动；该候选仍未合格且未默认启用。
本服务器aiohttp3.14.3与其3.14.5的ClientSession都经`update_cookies_from_headers`处理原始header，代理jar原继承DummyCookieJar的空实现。
现只增加向本次ContextVar默认jar的同名委派，保留旧mapping入口、IP来源cookie拒绝、同请求重定向及并发／跨请求隔离。
依据来自固定安装源码及[aiohttp3.14.5客户端](https://github.com/aio-libs/aiohttp/blob/v3.14.5/aiohttp/client.py#L931)和[cookie接口](https://github.com/aio-libs/aiohttp/blob/v3.14.5/aiohttp/cookiejar.py#L992)。
41项本机受影响回归通过、0跳过；实际3.14.5的307／302与两loop回归将在新窗口先运行，未重复作者矩阵。
