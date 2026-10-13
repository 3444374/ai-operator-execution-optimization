# 已封口小Job共享机会：无模型机制与反例

本报告只解释真实SessionEngine加两个串行替身服务，模型、GPU、HTTP和PG查询均为0。
研究问题和真实前端预演只维护在[同题M4方案](../../../plans/数据组织.md#query-sharing-burst)。
LOTUS/Sema/DuckDB原生多查询及真实模型性能均pending；FCFS诊断不代表这些系统。

## 来源与证据

- 固定base：`9bb6c4154a3fa321c49a84f45eaeb6c7e12b129c`，隔离分支`exp/semloom-query-sharing`，未merge native/main或推送。
- [原始证据](raw/probe.json.gz)：126次七场景/六臂/三次确定性重复、两次静态校准、42次满资源/保留资源solo，
  共170次运行/8308次合成请求；工作定义和四份运行源码SHA在原件中，另保留288个探索cell。
- [summary.json](summary.json)由[独立分析器](../../../../code/src/experiments/query_sharing_analysis.py)复算，含全部重复、每查询三种JCT反事实、
  最差短查询、长查询与吞吐比；源码入口为[probe](../../../../code/src/experiments/query_sharing_probe.py)。
- [checks.json](raw/checks.json)、[checks.log](raw/checks.log)记录无模型回归、早期失败和不可用依赖。

新版观测、封口及输入/结果消费在所有内部臂相同；候选与强静态adapter/K/router相同。
旧单查询与旧适配器测量未用于改善计算。stability与本地主仓库互不包含，指定native对象本地尚未取得，
共同集成及服务器匹配实验需由主协调，两个研究分支不是共同基线。

## 固定条件

每行一个完整独立任务；长短分别注册Job并按各自release到达，有界offer/lease释放。
全局在途请求最多4；每Job16个持有任务、1024输入字节、128结果字节。
每endpoint一个不可抢占串行worker，相同LeastQueued router；真实服务时间只属于后端/离线审计，不传给在线选择器。
长Job独立校准K=2/4都是32模拟秒，强静态选最小饱和admission=2；候选主臂同为2，全局上限仍为4。
最少在途估计的工作守恒、共享RR和FCFS用4；候选4另列已承诺工作消融。该校准不为真实模型选择K。
候选只优先已封口且剩余估计≤4的Job；被连续跳过的有资格Job累计工作量达到4后先获得选择机会。
未知/混合工作单位回到RR；取消通知不代表停止证明，不提前释放远端名额。

JCT终点为同步合成consumer释放最后一个正确结果；真实前端要计客户端EOF、读源和专属准备。
最长无提交时段仅在仍有未提交必要工作时计算；最长无完成服务另报，不能混称墙钟饥饿保证。

## 观察与反例

下表三次结果一致，单位是模拟秒。确定性一致不代表真实性能统计显著。

| 场景 | 工作守恒短JCT | 强静态短JCT | 候选短JCT | 强静态→候选长JCT | 强静态→候选整组JCT |
|---|---:|---:|---:|---:|---:|
| 64行长Job，4行短Job在0.5s到达 | 4.5 | 4.5 | 2.5 | 34→34 | 34→34 |
| 没有竞争 | 2 | 2 | 2 | 32→32 | 42→42 |
| 两Job都16行 | 15.5 | 15.5 | 15.5 | 15→13 | 16→16 |
| 两个20s不可抢占任务占用 | 20.5 | 20.5 | 20.5 | 20→20 | 21→21 |
| 短Job三行均实际5s、估计1 | 12.5 | 11.5 | 10.5 | 10→11 | 12→11 |
| 短Job实际1、估计20 | 8.5 | 4.5 | 4.5 | 34→34 | 34→34 |
| 8个短Job连续到达，最差JCT | 16.5 | 25.5 | 9.5 | 80→80 | 80→80 |

主场景短Job较两个合理控制均少44.444%，长与吞吐不变，是本模型里的采用信号。
连续短到达也满足本次三项要求；其他五个场景未同时满足短Job改善10%、长Job退化≤5%、吞吐降低≤2%。
估计偏小时长Job较强静态慢10%、较工作守恒慢22.222%，即使整组更快也拒绝。
288-cell探索找到四个长Job退化超5%的cell，全部cell保留，候选阈值未根据枚举调优。

候选K=4主场景短JCT为3.5，慢于同策略K=2的2.5；既有承诺工作削弱优先级效果。
估计偏差场景该消融整组13秒、强静态12秒，吞吐少7.692%；多短Job场景该消融也伤害部分短Job。
这些反例支持静态回退，不能预设SemLoom普遍赢。

独立审计从提交到服务终态重建名额，检查到达、恰好一次、endpoint无重叠、实际时长、请求/work峰值、
每Job存储和最后归零，不只相信runner汇总。模型输出质量、SQL权限/快照、真实CPU控制成本和客户端尾延迟均pending。

## 检查与后续

本机432项scheduling、6项probe/审计、19项native task、6项真实本地UDS gateway，共463项通过。
gateway初次sandbox未创建socket；只读诊断确认`listener.bind`被拒，原6项随后在获准本地socket范围通过。
早期新测试用错`seal`和`CleanupReport.events`名称，修正后通过，失败身份保留在checks。
`test_scheduling_daft_ray_contract`因本机缺少`pyarrow`未导入，不计通过；未混装依赖。
当前本机解释器无LOTUS/LiteLLM/OpenAI/DuckDB，未冒充真实前端资格。
用户授权后只读复用已核验SSH master检查一次GPU状态：两张4090未列出模型进程，未启动模型/GPU任务；
这不是新校准或窗口分配。后续先共同源码与前端桥接、策略关闭的新强静态，再由主协调有限模型预演。

从仓库根生成新输出，不覆盖原件：

```sh
python code/scripts/benchmarks/query_sharing_probe.py --output /path/to/private/fresh-probe.json.gz
PYTHONPATH=code python -m src.experiments.query_sharing_analysis \
  --input /path/to/private/fresh-probe.json.gz --output /path/to/private/fresh-summary.json
```

不把host elapsed当模拟服务时间；完整资源、决策及重复事件均在gzip，summary只是一份可再生聚合。
