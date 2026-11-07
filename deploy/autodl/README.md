# AutoDL 环境与实验入口

本目录维护网络、持久存储和可复用部署工具。当前实验以 SemBench 为主；模型、数据、
设备、并发与生命周期从实际任务配置读取，实现与验证范围见[代码状态](../../code/INFRA_STATUS.md)。

## 环境与源码

按[runtime](../runtime/README.md)选择仓库外环境文件、能力组与模型／数据资产。
普通 Movie CSV／records 使用 `core,text,semantic-benchmarks`；按实际任务再选择图像或数据库能力。
driver 与 vLLM 保持独立 Python 环境。[环境模板](autodl.env.example)只保存平台路径和可选服务参数，
历史 ShareGPT、SAOR、双卡与固定 K/W 配方不作为新任务默认。

先核对工作区与在途实验，再在独立 checkout 选择本次已审提交，不覆盖未提交修改或切换活跃实验源码。

模型、数据、虚拟环境、缓存和实验产物遵守[数据盘规则](AGENTS.md#磁盘分工)。
运行前检查当前挂载点与空间，同盘副本不作为独立备份；容量不能从旧机器结果继承。

```sh
findmnt -T /root/autodl-tmp
df -hT / /root/autodl-tmp
```

## 网络

按最终访问域名选择连接方式，遵守[网络规则](AGENTS.md#网络)。原 runbook 的 turbo 目标包括
`github.com`、`githubusercontent.com`、`githubassets.com` 和 `huggingface.co`；
运行前核对[平台当前支持范围](https://www.autodl.com/docs/network_turbo/)。
目标使用 turbo 且 `/etc/network_turbo` 存在时，在执行命令的同一 shell 中加载：

```sh
source /etc/network_turbo
```

Git remote 使用 HTTPS，先用 `git remote get-url origin` 核对，再 fetch 本次实际分支。
PyPI／Conda 索引使用[软件源](https://www.autodl.com/docs/source/)或可用直连；进入这些域名前取消临时代理：

```sh
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY
```

一个安装命令若混合访问 GitHub／Hugging Face 与 PyPI，拆开下载和安装步骤，
不按 `pip` 等客户端名称统一开关代理，也不硬编码 `/etc/network_turbo` 内部地址。

## 维护入口

| 用途 | 入口 |
|---|---|
| 单次 Movie、session、矩阵与原生 LOTUS 对照 | [SemBench 脚本](../../code/scripts/README.md#current-entry-roles)、[Movie 配置](../../code/src/experiments/movie_matrix.README.md) |
| 完整查询观察 | [record observation](../../code/scripts/profiling/record_observation/README.md) |
| 文本模型与 owner | [文本部署](text_serving.md) |
| 原生 CLIP 查询与模型检查 | [图像部署](image_serving.md) |
| 显式需要 HTTP 服务的任务 | [start_endpoints.sh](start_endpoints.sh)，使用前核对模型、引擎、设备与端口；本地 IPC owner 不要求另起 endpoint |
| Kalypso 外置 owner 的退役导入 | [迁移补丁](kalypso_owner_imports.patch)，只用于新私有 packet；范围和检查见[适配说明](../../code/src/baselines/text/frameworks/kalypso_native/README.md#外置-owner-辅助的退役引用迁移) |
| 模型下载 | [download_model.sh](download_model.sh)，先核对来源、持久目标和完整性 |
| 历史 COCO 数据库导入 | [独立 PG 原件](../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md#pg-image-data-tools)，当前文件图像不要求导入数据库 |

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
