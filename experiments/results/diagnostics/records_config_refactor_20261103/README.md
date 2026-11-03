# Records 配置传递的渐进整理

本记录属于源码审计与 CPU fixture 验证。底稿为 PR #90 已发布
`b018d88a5615433b1efb708fc04e57be85719b01`，tree 为 `dfdaa15a4b694d934d6523e8ba3014917a31708c`；
本轮核查时 PR 仍为 Draft、未合并。候选尚待独审与统一整合。

## 实际调用与职责

| 部分 | 可达路径与职责 |
|---|---|
| 普通配置与运行 | `run_lotus_records_movie.py` → `lotus_records_movie.read_config`／`_model_worker` |
| 连续查询 | `movie_records_session.check_plan`／`_MovieDriver.start` → 同一个 `create_records_core` 工厂 |
| 配置解释 | [`movie_records_config.py`](../../../../code/src/experiments/movie_records_config.py) → 原 Core profile／批组织／IPC resolver |
| 方法与准备 | 原 LOTUS 方法、输入物化、逐记录就绪；Filter→Map 原关系、Extract 完整关系等待保持 |
| 接纳与派发 | 原 Core → async backend → Ray model actor → 私有 `AsyncEngineProcess` |
| 模型与结果 | vLLM实例拥有内部序列／token／KV调度；原逐成员结果、接收确认、取消及未知结果记录保持 |

五个原配置函数仅改变所属模块。普通运行器保留兼容导入；Session 的配置调用不再依赖运行器，
查询解释、评价与收尾继续使用原函数。没有新控制器、缓存、并发值或执行路径。

## 本轮检查

先在原底稿运行，再在提取后的相同方法集运行。55组配置覆盖缺省、显式值、部分容量、
独立输入／结果总量、具名历史profile、批选项、缓存身份、非法类型及其他baseline拒绝。
返回、首个异常、身份及输入是否修改的JSON逐字节一致，SHA-256为
`1115b109e5326bf1e59ee138e3ca3cd3e098fc209aa15b324e20c43a1e0fef5f`。

b018改前与改后的同一187个方法选择，unittest均运行183项，181个方法成功、2个方法跳过，另1个测试类跳过。
初轮826记录的179摘要误扣了类级跳过，正确为运行182项、180个方法成功、2个方法跳过，另1个测试类跳过；原件保持。
原底稿与提取后的沙箱运行均有3个方法因socket创建被拒而报错，其中一个方法有两个报错子用例；
它们没有被改成skip或通过。验证脚本首轮嵌套dataclass未序列化的错误也保留，修正只涉及验证脚本。
全部运行日志和配置回放按 `records-config-refactor-20261103` 交接，机器可读摘要见
[`raw/checks.json`](raw/checks.json)。

五份历史投影已经为这两个入口配置准确来源映射，十条映射的字节摘要匹配，故未追加重复来源。
既有5项失败、30项跳过和历史10处来源不匹配属于此前各自记录，本轮选定方法集没有重新认证它们。
原始失败、负结果、baseline和历史实验文件均保留。

## 上下文配置的后续独立变化

源码审计确认普通异步records缺省仍显式传 `max_model_len=4096`、
`gpu_memory_utilization=0.8` 与 `dtype=bfloat16`。本次模块提取不改变这些值。
用户另要求未显式给上下文时由vLLM解析；该项需在ready后取得实际`ModelConfig.max_model_len`，
再向上游校验、可选token工作量及身份传递。缺失实际值不能解释成无限，也不能留下4096影子限制。
显式上下文、同步参照、历史参数、单GPU及张量／流水线并行各为1的支持范围与有限资源保护分别核对。
每轮token由另一个独立候选处理；显存比例与dtype的默认不在该上下文切片内改变。

本轮真实Ray、模型及GPU运行均为0。完整查询耗时、质量与实际配置读回仍须由专用测试会话
在最终已审准确源码上比较，不能从配置提取或CPU耗时推断加速。
