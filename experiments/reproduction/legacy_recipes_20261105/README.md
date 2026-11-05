# 旧实验配方与部署指南的准确来源

当前部署入口只维护可复用环境工具与 SemBench 指引。本目录保存旧双卡、ShareGPT、开题、
phase-change 和已退役 SemBench 四臂配方，供研究核对与恢复，不加入当前执行路径。

| 处理 | 内容 |
|---|---|
| 56 份 JSON 逐字节移入 `autodl/` | 旧双卡校准／正式矩阵、ShareGPT、图像与开题多 Job、VTC、phase-change、旧 SemBench 四臂模板 |
| 5 份原件无损压缩 | 原平台／文本／图像 runbook、phase-change runbook、完整旧 `autodl.env.example` |
| 14 份 SAOR 声明单独保留 | [SAOR 目录](../../saor/README.md)；同一清单记录移动关系 |
| 保留共享部署工具 | 当前 [AutoDL](../../../deploy/autodl/README.md)的网络／磁盘规则、模型下载、显式 HTTP launcher 和仍由 COCO importer 使用的 SQL |

[MANIFEST.json](MANIFEST.json)记录全部 75 项的原路径、保存路径、字节数、SHA-256；
gzip 项另外记录压缩文件 SHA。原字节来自公开提交
[`b1429923caa886c4b8cd91996fc88372c2304c60`](https://github.com/3444374/ai-operator-execution-optimization/tree/b1429923caa886c4b8cd91996fc88372c2304c60)。
压缩原件可用标准 `gzip -dc` 解码后按清单核对；内容与来源提交一致。

配置中的相对路径、原脚本与旧参数只在原隔离 Git 树解释，归档资产目录不是可执行 checkout。
需要准确布局时恢复上述提交，再按[PG／HTTP 来源](../paused_pg_and_retired_http_20261105/README.md)
找到已退役接线。历史源码、配置和许可字段不授权重放旧预算或启动服务。

研究方案、正式结果、必要失败证据与 SAOR 算法保留。本次不运行模型、Ray、GPU 或服务器，
不操作数据库、容器、数据目录和持久卷，也不把路径整理写成新的性能证据。
