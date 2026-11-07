# 当前入口、共享层与原生 LOTUS 本地 IPC：CPU 清理检查

此轮收敛维护入口：退出旧 Movie HTTP、PG／HTTP 实验与相连专用测试／配置；PG 后续仍可恢复。
共享文件／脱敏、LOTUS 方法和 CLIP workers 归职责层，生产不再导入 baseline 实现。
新原生 LOTUS Movie 仍使用原算子、LM 和 LiteLLM batch_completion，只把 provider 接到现有本地模型 IPC；
普通 Movie session 可选冷／常驻 owner。没有使用 Core 调度冒充原生对照。

最终受影响集合 **685 项：679 通过，6 跳过，无失败／错误**，不是全仓全量回归。
各轮是实际执行记录，方法重复时不能相加成独立测试数量；最终集合按方法 ID 去重。
原生 SDK、公共 IPC、budget、来源、结果／时钟、关闭及 late accounting 使用 CPU fixture。
4 个维护 CLI 的 `--help` 通过；静态检查无退役模块导入、无生产→baseline 实现依赖，
344 条归档来源 SHA 正确，5 份历史投影的 3,010 个原始绑定和 ID 保持。

6 项未运行分别是3个官方完整 Movie 查询／评价场景、2个实际 Daft runtime 场景和1个实际 Daft/Arrow 批场景。
本地没有补装依赖；官方源还缺 `overrides/cdlib/duckdb`。保留的真实 Daft→Ray 适配测试也未执行。
未启动真实模型、Ray 或 GPU，没有查询性能结论。Q2b 视觉对照的非 HTTP 替换不在本次实现中。

## 反例和边界

组合首轮 422 项中有5个错误：工作负载摘要迁移漏接、已退役别名测试断言、
两处脚本搜索路径夹具，以及 Kalypso 空配置夹具。后三项在准确基底646216fa独立复现；
修正仅给测试合法原生配置和正确脚本搜索路径，原业务／回执／退出断言保留。
工作负载摘要归文本工作负载模块，没有塞入通用文件工具。原失败身份和日志摘要在机器数据中保留。

聚焦复审又发现并修正新原生接线的两个失败边界：resident最终close才可见的late用量、
以及把提交前拒绝误计为模型API调用。最终close原生审计单独移交，原 source unknown 不改成成功；
关闭失败可重试，成功后释放原输入／结果引用。session发布失败保留移交对象并使用既有原子发布接口重试，
没有新增执行器、无限历史缓存或放宽容量。提交前拒绝和提交未知仍保留诊断，不能进入已确认模型调用计数或耗时样本。

## 可核验来源

- [压缩CPU回执](cpu_validation.json.gz)含每个方法状态、版本、测试源码SHA、各轮失败／跳过和原件映射；
  [SOURCE.json](SOURCE.json)绑定压缩及解码SHA／字节数。只发布脱敏结构，不公开含本机路径的原始日志。
- 原始日志以运行名、SHA和字节数映射到本地组 `runtime-entry-cleanup-20261105`，
  可向仓库所有者申请获准的本地核验；没有把同一数据副本重复当独立运行。
- [归档清单](../../../reproduction/paused_pg_and_retired_http_20261105/README.md)给出完整准确Git恢复树。
- [当前入口和执行归属](../../../../code/src/baselines/README.md)区分原生LOTUS、Daft、Ray Data、IMLane和Core。

用Python标准库读取回执即可按 `final_run` 的 `test_cases` 重算各状态计数；
`source_python_sha256` 只绑定实际测试的当前Python源，不要求现代实现与历史AST形状相同。
本次未修改任何历史实验成绩、质量标签、失败、unknown或模型调用成本，也不据CPU检查批准合并。

## 与最新已发布代码的组合

最终组合保留公开6754eebe的普通IPC容量、shared active-credit、Ray调度观测与类型拆分。
冲突中的文件／摘要／查询验证只留职责层的一份实现；已有公开gzip历史源优先，不增第二份同字节来源。
旧画像大文件中仍用于当前Ray scheduler的6个CPU方法按原行为迁出；旧PG／HTTP启动器不恢复。
`lotus-native`现指新本地IPC对照，旧HTTP profile仍在启动前拒绝，不把其历史时钟混入新API样本。

组合首轮685项发现一项旧断言“result_slots必须等于独立类型默认8”，
已在未改动的公开6754eebe独立复现。普通factory按显式请求额度3推导为3，
修正仅将测试对齐该合同，保留max_inflight和实际active_requests断言，没有改变生产容量或删除保护。
最终回执包含此RED与后续GREEN，之前各轮不回填成通过。
