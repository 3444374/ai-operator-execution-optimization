# 固定 JPEG 文件源与 CPU 工厂退出的实际 SDK 检查

固定集成候选 `964e70eff7f61d5cef4af07fc28eba76a61a8da0`，完整树
`3139825591e356b6da807b66d8a94e2cc1df2a8b`，图像来源
`2f116d4a43049b0b8979c973c7ddb84a426557ef`。本次三项实际 SDK 检查全部通过，
0 失败、0 跳过；生产源码和测试未在测量后修改。逐项结果与来源映射见
[verification.json](verification.json)，成功、首次失败和验证入口保存在
[16 成员脱敏原始归档](raw/sdk-observations.tar.gz)。

| 检查 | 实际范围 | unittest 秒 | 验证器墙钟秒 |
|---|---|---:|---:|
| Daft 文件 reader | offset 1 选 2 张 JPEG、重复 2 次，共 4 行；ID 顺序及原始编码字节一致 | 3.843 | 4.841954 |
| Ray binary reader | 相同 4 行的 ID 顺序及原始编码字节一致 | 15.269 | 17.846179 |
| Ray 工厂部分启动失败 | 1 个 preprocess 与 1 个 model_cpu_fixture 的 PID／出生身份在客户端 shutdown 前消失 | 16.149 | 18.689611 |

环境为 Linux、Python 3.12.3、Daft 0.7.21、Ray 2.56.1、Arrow 24.0.0；
NumPy 2.4.6、Pillow 12.2.0、Torch 2.12.1+cu130。Ray 请求 CPU2／GPU0、
object store 128 MiB，CPU slots 不代表 OS 配额。未加载模型、未执行 CUDA／GPU 或 HSE。
验证器墙钟包含子进程启动、退出、日志写入和归纳；这些单次组件时间不作性能比较或查询 JCT。
本轮没有正式 benchmark warm-up，reader 的重复读取也不构成冷 page cache 保证。

工厂原始退出观测在 `2026-10-14T06:06:29.942664Z` 保存：PID 42259／start ticks
310650999、PID 42260／310651000 均已不存在。终止请求与物理退出观测分别保存，
失败池的通用状态仍保守为未确认。这个证明只覆盖本次两个 CPU 替身 actor；
不推导 Ray reader、CLI 或所有 Ray 子进程已退出，也不推导 GPU 工作已停止。
SDK receipt 确认共同 lease 已释放、服务器已有仓库 SHA 前后一致；
`06:18:20.631597Z` 清理记录的本次自有 cwd 进程列表为空。

第一次接收在 `receiver cannot transfer a different lease` 处失败，尚未进入任何 SDK 检查，
SDK tests run 为 0；失败 preflight、receiver 状态及完整异常另存于归档的
`first-handoff-failure/`。此前上传的自动审批拒绝与授权重试记录保留在仓库外，
不将它或 lease 交接失败计作 SDK 测试失败。

固定输入 tar 为 122606 字节，SHA-256
`074675cb8de2f77befdd37199843d5f59c022bbfc4ade60642abec5b578a0516`：
95 个成员中 68 个普通文件、27 个目录；67 个摘要项为 66 个 Git 依赖文件和 1 个外部 wrapper，
另有 manifest 本身。66 个依赖文件与测量候选逐字节一致。公开归档逐成员记录原始与脱敏字节数／SHA，
原始输入包、完整原件和未合入研究成果均保留，公开脱敏副本不替代原件。

本地先前窄回归为 79 通过／4 固定依赖跳过，与本次三项实际 SDK 检查分别记账，
不相加成一次运行。真实 CLIP 质量／性能、GPU／device 完成及退出、HSE A／B／C、
完整 CLI 采样器／进程树退出、厂商总 I/O 并发仍待验证。Daft 的
`download(max_connections)` 仅按固定版本的 per-thread 语义记录，未测总 I/O 上界。
