# AutoDL 环境与实验入口

本目录维护网络、持久存储和可复用部署工具。当前实验以 SemBench 为主；模型、数据、
设备、并发与生命周期从实际任务配置读取，实现与验证范围见[代码状态](../../code/INFRA_STATUS.md)。

## 环境与源码

按[runtime](../runtime/README.md)选择仓库外环境文件、能力组与模型／数据资产。
普通 Movie CSV／records 使用 `core,text,semantic-benchmarks`；按实际任务再选择图像或数据库能力。
driver 与 vLLM 保持独立 Python 环境。[环境模板](autodl.env.example)只保存平台路径和可选服务参数，
历史 ShareGPT、SAOR、双卡与固定 K/W 配方不作为新任务默认。

联网按[网络规则](AGENTS.md#网络)选择 turbo；Git 使用 HTTPS remote。
若目标域名需要 turbo 且 `/etc/network_turbo` 存在，在同一 shell 中加载后再 fetch。
先核对工作区与在途实验，再在独立 checkout 选择本次已审提交，不覆盖未提交修改或切换活跃实验源码。

模型、数据、虚拟环境、缓存和实验产物遵守[数据盘规则](AGENTS.md#磁盘分工)。
运行前检查当前挂载点与空间，同盘副本不作为独立备份；容量不能从旧机器结果继承。

## 维护入口

| 用途 | 入口 |
|---|---|
| 单次 Movie、session、矩阵与原生 LOTUS 对照 | [SemBench 脚本](../../code/scripts/README.md#current-entry-roles)、[Movie 配置](../../code/src/experiments/movie_matrix.README.md) |
| 完整查询观察 | [record observation](../../code/scripts/profiling/record_observation/README.md) |
| 文本模型与 owner | [文本部署](text_serving.md) |
| 原生 CLIP 查询与模型检查 | [图像部署](image_serving.md) |
| 显式需要 HTTP 服务的任务 | [start_endpoints.sh](start_endpoints.sh)，使用前核对模型、引擎、设备与端口；本地 IPC owner 不要求另起 endpoint |
| 模型下载 | [download_model.sh](download_model.sh)，先核对来源、持久目标和完整性 |
| COCO 导入所需 schema | [image_documents_workload_key.sql](image_documents_workload_key.sql)，由仍保留的 importer 使用 |

每次实验明确问题、源码、输入、模型、对照、资源上限和停止条件，并完成对应环境检查。
结果可用、API 返回、启动、预热、探索与关闭分开计时；保留请求账本、质量、失败、unknown 和最终资源回执。
CPU 检查不代表完整查询、真实 Ray 或 GPU 性能已验证。

## 历史材料

SAOR 的 14 份现存声明已逐字节移至[独立研究材料](../../experiments/saor/README.md)。
旧双卡、ShareGPT、开题、phase-change 和旧 SemBench 四臂模板共 56 份 JSON，以及原 runbook／环境模板，
保存在[旧配方归档](../../experiments/reproduction/legacy_recipes_20261105/README.md)。
原配置路径与参数只在精确来源树解释，不把旧命令接到新执行器。

PG／pgai 接入仍可按[准确来源](../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)恢复。
这些源码整理不操作数据库、容器、数据目录或持久卷；研究方案、原始结果和负结果继续保留。
