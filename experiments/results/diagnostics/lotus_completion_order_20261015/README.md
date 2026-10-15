# LOTUS TwoMap 完成推进顺序：CPU 核验

仅保留在 `exp/semloom-completion-order`，默认关闭，不建议启用，不进入主测试。当前 localhost fixture 未建立稳定净收益；这不是新调度算法或生产／GPU 资格。

固定实现源为 `integration/baselines@bf41757e8aa55326f6ee83c8f31f65946ef75a98`，未从仅维护文档的 main 取实现，也未改其他活动源码。现有 query-sharing、stage-execution、replica-adaptation 研究源未发现同机制。云端数字只用于提出核验问题，没有作为本轮实测。

## 机制与责任

原 row-ready 入口先由 Engine 派发旧队列，再由 MethodDriver 解析已完成响应，合法后继因此可能错过本 tick。

- `SessionEngine.advance(N, *, dispatch=True)` 保持原默认；`dispatch=False` 仍处理 timeout、取消、终态和 prepare，仅暂缓选择与派发。单独 reap 不具有相同 timeout／deadline 语义。
- `iter_two_map_rows(..., completion_first=False)` 保持原默认；开启后先 `advance(N, dispatch=False)`，再运行原 MethodDriver 完成真实解析、释放 Core lease 和生成后继，最后以 `N - first.events` 推进并在 yield 前派发。没有剩余额度时不再推进。

两份 CleanupReport 累加事件，采用最后占用、最早 deadline、首个错误及 immediate 的并集。MethodBudget 仍在 consumer release_result／close 时归还。没有新增 scheduler、source 预读假设或阻塞反馈；生产默认 selector 仍为 FIFO。实验中的 SJF 与固定末阶段优先仅通过已有 `SessionPolicies.choose_task` 设置。

开启是显式研究合同：解析／方法错误可能让更少的后继发出；yield 前可能已经派发消费者稍后关闭也不能撤回的工作。原始响应、原方法 usage／解析与首错责任保留，未知远端工作继续由 owner 排空。它不能透明替换普通 LOTUS 的整阶段 Map 屏障。

## 验证范围

本机 macOS arm64、Python 3.12.6，LOTUS 1.2.4（b1a85fd7）、LiteLLM 1.95.0、OpenAI 2.50.0、HTTPX 0.28.1；`sdk.validate_source()` 核对版本及固定源文件哈希。只读复用既有 SDK 环境，没有安装或启动模型／GPU。

受影响 Core、MethodDriver、LOTUS 及完整响应定向回归共 **185 项通过、0 跳过**。新增检查覆盖 N=1/2/3、小预算与多 Job＋prepare、分数 deadline、spurious wake、consumer throw／close／取消、HTTP／JSON／缺 content 错误、实际 LOTUS usage-limit 异常及最终 owner 排空。

仓库外原实现差分共 26 场景×4 路径＝104 次运行；原入口、当前默认和显式 false 的完整结果 dict（含事件、等待、错误及占用）相同，四路径最终全部归还。该差分使用确定性 Core fixture；SDK／HTTP 资格由下一组实际运行单独提供。

原生 `staged_two_map` 的完整请求 JSON 字段、行／raw output 与 usage／计费均对拍。原生 SDK 和准备入口的 HTTP 字段序列化顺序不同，只能称完整 JSON 字段等价；各自原字节仍保留。六个 row-ready 内部控制之间则核对原字节 payload、最终行及完整 raw response 一致，每份 raw body 都与服务返回逐字节相同。

## 冻结的 localhost 对照

使用当前真实 owner loop、BoundedAsyncBackend 和完整 HTTP transport，服务为两个不可抢占的 localhost slot。每次四行、每行两阶段；request=2、active_work=16、step_actions=8，Method 行窗口为3（wide为4），各臂相同。每个单位服务时间为25ms，实际 sleep／HTTP／主机调度时间全部计入墙钟。

已知 fixture 阶段 work 同时提供给全部内部控制，SJF 可读取同一声明成本；这不是对真实模型输出成本的免费预测。各行两阶段成本依次为：

- 计划正例／wide：`(1,1), (8,1), (4,1), (1,8)`。
- 反例：`(1,8), (8,1), (4,1), (1,1)`。

六臂是原入口／新桥分别配 FIFO、SJF、固定末阶段优先，均属 SemLoom 内部控制，不是原生系统性能 baseline。每场景同一 LM／backend 先运行两次完整预热，随后六臂按轮次旋转顺序各测五次：合计6预热＋90测量＝96查询／768个 fixture POST；每次4行、8个唯一阶段调用，服务并发不超过2、开启路径每 tick 总事件不超过8，完整消费到 EOF 后 Core／Job 占用归零。

计时从组件与 selector 就绪后开始，包含方法构造、source、SDK 准备／解析、完整结果消费与 lease／Job 关闭。owner CPU 使用 `thread_time_ns`，不含后台 HTTP 与服务线程；Engine、MethodDriver CPU 分项分别记录。source 事件表示实际拉取 pending 行，不表示已被 MethodBudget 接纳。

完整消费墙钟中位数，毫秒：

| 场景 | 原 FIFO | 桥 FIFO | 原 SJF | 桥 SJF | 原固定末阶段 | 桥固定末阶段 |
|---|---:|---:|---:|---:|---:|---:|
| 计划正例 | 457.701 | 455.951 | 458.827 | 459.855 | 460.982 | 454.920 |
| 反例 | 359.679 | 355.978 | 357.262 | 394.712 | 356.822 | 393.156 |
| 扩大窗口 | 426.023 | 427.278 | 423.680 | 454.448 | 447.691 | 450.140 |

计划正例中桥固定末阶段仅比最佳原 FIFO 快0.607%，五组配对差值跨越正负，未建立稳定收益；反例五组均慢，中位数比最佳原固定末阶段慢10.183%。wide 比最佳原 SJF 慢6.245%。没有继续调换 fixture 或丢弃慢样本追求正结果。

隔离桥本身成本的匹配 FIFO 控制（中位数；CPU为毫秒）：

| 场景 | 原 owner CPU | 桥 owner CPU | 原 Engine CPU | 桥 Engine CPU | 原／桥 Engine 调用数 |
|---|---:|---:|---:|---:|---:|
| 计划正例 | 16.624 | 18.855 | 3.456 | 4.854 | 53／110 |
| 反例 | 16.868 | 19.627 | 3.167 | 3.986 | 45／90 |
| 扩大窗口 | 14.634 | 16.510 | 3.286 | 4.118 | 51／100 |

这些配对中 Engine 调用约翻倍、owner CPU 增加；不能外推为所有 selector／场景 CPU 都变差。候选与不同静态控制的 CPU 及全部五次原值均在原件中。没有真实应用任务、PG、Ray／Daft执行资格、模型质量／吞吐或 GPU 性能结论；本次小 fixture 也不证明该机制普遍无收益。

云端抽象时间没有计入本机的两次全局推进、SDK解析、HTTP及主机唤醒成本；本次这些成本由实际owner和服务时间线计入。合法后继提前出现也不等于后续source行已经可接纳：MethodBudget必须等消费者归还，yield前的第二次派发仍可能先占用剩余槽。上述是源码责任与实际时间线可核对的原因；它们不能逐项解释云端收益幅度，因为任务输入没有附云端原fixture源码，本次冻结的四行成本也不是同一workload的配对复跑。当前证据只支持“抽象收益尚未转化为本机稳定净收益”，不追调启发式、不弱化静态对照，Core新参数不合入baseline／main。

## 复核与原件

两轮独立只读审核未发现默认关闭研究功能的代码阻止项；最终审核重算全部18组摘要，并逐查询核对768请求、原字节、raw body、usage和动作上限，建议只保留独立研究分支、继续默认关闭。最终共享回归日志随后确认185项全过。

定向回归入口（固定 SDK 环境，localhost 权限；代码只产生 fixture 请求）：

```sh
PYTHONDONTWRITEBYTECODE=1 LITELLM_LOCAL_MODEL_COST_MAP=True PYTHONPATH=code \
  <fixed-sdk-python> -m unittest \
  tests.semantic_methods.test_lotus_completion_order \
  tests.semantic_methods.test_lotus_completion_sdk
```

对应测试保留最小服务 fixture 和 OwnerCase，未新增生产 runner 或 CI 默认开关。计时计划、测量／差分脚本、所有预热和测量值、完整请求／响应、早期测试失败及独立审核原件保存在仓库外交接索引，运行标识 `completion-order-localhost-v1`，由 C 保留并按文件名／SHA-256 获取；不随源码进入 Git：

| 原件 | SHA-256 |
|---|---|
| `localhost-plan-v1.json` | `daa199c25ce23b6cd0d04eaeaa949d01a09f78b6924d7906df03a56e0bc45d14` |
| `localhost-raw-v1.json` | `7db22992e962eb732102bb84914d2b190ff21ebf1b9ec1bdd5e2c277a56de2fa` |
| `localhost-summary-v1.json` | `10416af70e457129dfcc1f65f80f6f6872bd92a03d16bc877a6f2a785dc8c414` |
| `default-parity.json` | `e5514183f188e4e2eb681f41f45dd16ded19d6b892cc8781347ec7553b2aba87` |

这份报告的结论是“本次未建立稳定净收益，暂不启用”；保留默认兼容性和负例，不将云端模型时间写成实际设备收益。
