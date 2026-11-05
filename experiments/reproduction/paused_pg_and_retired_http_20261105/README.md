# 暂停 PG 与退役 HTTP 实验的准确来源

当前主线为 LOTUS 方法与 SemLoom 数据执行、SemBench 实验。PG 后续仍可恢复，**暂停并非永久放弃**。
旧 Movie HTTP 与旧固定矩阵另见[恢复记录](../native_baselines/README.md#retired-movie-http)。

[MANIFEST.json](MANIFEST.json)逐项记录退出当前入口的源码／测试／部署配置路径、字节数和 SHA256。
源树为 [`7bf1d92baf2611925c608cfe76b4a766855bd77e`](https://github.com/3444374/ai-operator-execution-optimization/tree/7bf1d92baf2611925c608cfe76b4a766855bd77e)，
包含退役前的 PG C／SQL、build、测试、Python 接线、旧 HTTP 对照、相连启动脚本及部署源。
在新的隔离 checkout 中选择该精确提交即可恢复整套依赖；不要把存档目录加入当前 PYTHONPATH，
不要只取单文件并与新 owner 或协议混用。恢复源码不授权启动数据库、服务、模型或恢复旧预算。

后续清理也移除残留的 PG C 编译／静态检查及其夹具；原件与上述完整来源树一致，
恢复 PG 时使用该树中的配套测试。仍有用途的 Python plan、generation 和 wire 合约测试继续维护。

| 处理 | 内容 |
|---|---|
| 退出当前执行入口 | 旧 Movie HTTP、固定五后端矩阵、prepared-call HTTP 图、PG／HTTP 查询／画像及旧部署配置 |
| 暂停 PG | 扩展、SQL、部署源、实验接线、旧测试均以精确 Git 树保存，不触碰数据库数据／容器／卷 |
| 保留当前共享能力 | 私有文件／脱敏、LOTUS 方法、CLIP workers、Core／IPC 和结果责任 |
| 保留研究 | SAOR 调度算法、离线分析、方案及历史结果；依赖旧 HTTP／PG 命令的启动胶水在冻结树恢复 |
| 恢复原生 LOTUS 对照 | 新普通 Movie session 使用原 LOTUS LM + 本地模型 IPC，真实模型仍未验证 |

五份具名历史投影继续使用原 ID、原 expected SHA 和原来源，只为受影响文件增加精确旧字节 override。
历史成绩、失败、unknown、成本和私有原件不改写。此清理是工程职责和入口收敛，不是查询提速或质量修复。
