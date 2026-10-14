# 单GPU原生CLIP文件算子：静态、HSE与Ray Data

2026-10-14，运行 `clip-45da0a54-22bde476` 完成15次原CLI执行。问题是当前文件输入下三条已有流水的冷算子耗时与供给信号；实际入口没有PostgreSQL查询或写回。全部计时值、失败与数值限制保留，性能数据和待执行的逐行诊断分开。

## 来源、设置与计时

实际源码为 `45da0a5451bfcf785d98b9ab5336054bdbaf199f`；导出的387个源码／测试／runtime文件与已发布 `58462a0d19dbf6e2a7d010edde27225d98471ac2` 逐文件同字节。原CLI在archive源码中的 `git_commit=unknown` 保持原值，外部完整SHA与[逐文件清单](raw/source-files.json)绑定来源。发布版提供[原生Ray Data图](https://github.com/3444374/ai-operator-execution-optimization/blob/58462a0d19dbf6e2a7d010edde27225d98471ac2/code/src/baselines/image/frameworks/ray_data.py)与[共同审计](https://github.com/3444374/ai-operator-execution-optimization/blob/58462a0d19dbf6e2a7d010edde27225d98471ac2/code/src/modalities/image/execution.py)的可读副本；测量身份仍是45。结果分支的当前HEAD不追加运行资格。

同一COCO val2017文件清单，5000张／814,705,164编码字节；资格取前257张，预热各64张。CLIP ViT-B/32模型与processor固定revision `3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268`，实际权重／配置／processor文件SHA见[provenance](provenance.json)。同一FastClip预处理产生224×224 FP32张量，FP16 forward，投影512维FP32输出在actor内L2归一化，完整流式Arrow消费者执行共同ID／shape／finite／exactly-once检查。

一张RTX4090，CPU cgroup额度16核。三臂均声明7 CPU槽：项目路径为4准备＋1模型actor CPU＋2 Daft source线程，原生Ray Data为2 source＋4准备＋1模型actor CPU。batch64、GPU actor1、Torch intra/inter各1；声明槽是准入条件，不是OS线程硬配额。项目window4；HSE encoded额度356,622,848字节、ready额度154,140,672字节／12,845,056像素、prepare在途4／model在途1。Daft下载连接上限2按线程生效，两线程潜在4连接。全部实际参数、版本、reader与资源观测在[15条原始row](raw/measurements.jsonl)，公共schema字段未改名。

Direct是已有高效静态流水，HSE是已有分阶段有界流水；原生Ray Data拥有read_binary_files、CPU/GPU map_batches与调度，使用同一预处理／typed CLIP kernel，没有注入项目额度策略。

`operator_e2e_s = result.total_s + explicit_project_worker_setup_s`，从冷worker边界，经source open/read、prepare、模型及完整消费到末尾文件metadata复核。排除Ray集群启动、共享manifest准备、环境检查和写回；每个CLI预热后项目模型worker重建，正式worker设置计入主时间。未启用capture或逐阶段CUDA同步。共享manifest内容哈希与预热可能预热OS page cache；page cache未受控，也没有新增adapter payload cache。[原始合同](raw/contracts.json)保留完整指标定义。

## 全部计时值

单位秒。资格与scale各一次，独立保留；主重复次序为ABC、BCA、CAB（A=Direct，B=HSE，C=Ray Data）。重复中位数来自[summary.json](summary.json)，由[summarize.py](summarize.py)对保留row复算；sample CV使用n−1标准差／样本均值，不作显著性或等价性判断。

| 阶段／行数 | Direct | HSE | Ray Data |
|---|---:|---:|---:|
| 资格／257 | 13.305736432783306 | 12.245688616298139 | 12.072395382914692 |
| scale／5000 | 21.147315533831716 | 21.73653863510117 | 38.79796502087265 |
| repeat1／5000 | 21.227541075088084 | 21.400042964611202 | 55.55321142775938 |
| repeat2／5000 | 20.623728388454765 | 20.360143897123635 | 38.375772891100496 |
| repeat3／5000 | 21.118473371025175 | 20.575613962486386 | 39.64305949723348 |
| repeat中位数 | 21.118473371025175 | 20.575613962486386 | 39.64305949723348 |
| repeat sample CV | 0.0153302681 | 0.0264147991 | 0.2149976668 |

Direct/HSE接近，逐轮差值有正有负；不能据此证明HSE稳定收益或非劣。Ray Data在这一固定冷工作点耗时更长，55.553秒慢重复保持原值。每臂只测一个配置；现有steady-duration字段均为false，未证明稳态平台、饱和点或原生最优配置。三臂逐行数值等价尚未证实，所以此表保存实际耗时观察，不据此作同结果质量的正式优越结论。

15次共60,771测量图＋960预热图＝61,731次模型图；原两项可选逐阶段诊断未运行。共享准备和15次模型CLI共18个子进程退出0，每次后都核查自有CPU身份与GPU物理退出，再开始下一次。[生命周期](raw/lifecycle.json)保存每次原argv、UTC、退出码与核查；08:51:19 UTC完成、共享lease释放，08:57:57的独立只读postcheck确认自有controller退出与lease不存在。这是该时点的证明，不表示服务器以后持续空闲，也不扩为GPU cancel/error/unknown-device completion资格。

## 数值审核：已知与未证实

全部15次共同ID／shape／finite／exactly-once审计通过，同尺度三臂source ID SHA一致。5000图的Direct与HSE每次聚合及digest一致；Ray Data自身四次5000图（含scale）稳定，但与它们不同：

| 5000图 | Direct／HSE | Ray Data |
|---|---|---|
| 第一维之和 | −36.44171080313299 | −36.4367401782024 |
| 全分量之和 | −1004.8990153283116 | −1005.0556894620051 |
| rounded5 XOR digest | `2e357c1ee89a68479f8f65eba49e85a9` | `b5cfe99ca3cbf03e88aae787ba31d925` |
| 最大单位norm偏差 | 1.1920928955078125e−7 | 1.7881393432617188e−7 |

digest定义是：每行转FP32，round到五位小数，再编码小端FP32；对 `UTF8(doc_id)+NUL+row_bytes` 做128位BLAKE2b，然后逐行整数XOR。XOR不依赖到达顺序或consumer分组；三臂共享此函数和dtype／归一化合同。[独立源码／原件审核](raw/digest-independent-review.json)与纯离线源行为检查验证顺序、分组、无损float64容器与大小端表示不变性，也验证正负零能在值相等时改变digest。检查和复算入口为[digest-offline-checks.json](raw/digest-offline-checks.json)与[audit_digest.py](audit_digest.py)。

因此摘要不同不能直接解释为“排序不同”。正负零、round边界、模型批次中的数值差异或ID与向量关联差异等原因尚未区分，不能指定根因。原输出没有逐行向量，现有原CLI仅project_ray／Daft built-in支持capture；Direct/HSE摘要匹配也不能代替逐ID、逐分量等价证明。三臂数值误差幅度与容差内等价仍待验证。

[最小逐行计划](numerical-validation-plan.json)已准备：同一前257图／offset0，三臂各一次原生诊断＋各64预热，共963模型图，状态为未执行。Direct/HSE复用原CLI capture；[Ray Data capture助手](capture_ray_data.py)只为formal driver audit加入有界向量保存，保留原生图、GPU batching、模型、dtype与归一化。助手要求外层coordinator持有原canonical共享lease，核对parent PID／birth、来源和owner token；coordinator仍须串行、保存自有进程身份与物理退出证明，不能仅凭助手返回释放lease。资源窗口由主会话协调，当前没有启动这些模型调用或服务器动作。

[compare_vectors.py](compare_vectors.py)按ID对齐原FP32输出，输出逐行误差、cosine、norm、bitwise与显式容差内计数；容差必须由调用方明确给出，仅作描述，不设新的整体通过门槛，不重新归一化输出来消除差异。小样本即使接近，也不自动证明5000图逐行等价。capture诊断的耗时不加入上表，不为通过修改baseline算法，不重跑性能矩阵。

## 供给观察与证据保存

Direct/HSE重复的batch预处理P50为0.304–0.345秒，整个模型actor服务P50为0.017–0.037秒；正式worker设置9.55–10.57秒已计入约21秒冷算子时间。这些重叠区间不能相加为精确E2E分解，但提示先检查CPU准备与冷设置。已有source-next、driver materialization、submit、HSE prepare queue／ready residence／额度峰值与原生engine stats均保留；它们不能据此归因原生Ray Data的额外耗时。完整consumer独立墙钟未采集，包含在E2E内；H2D／forward／D2H主性能字段的0来自未启用详细同步计时，原生项目专属字段的空值／0也表示未暴露，均不能读成无成本。低频GPU utilization是供给诊断，MFU未测。

两次模型启动前失败独立保留：[失败证据](raw/failures.json)记录公开Git的45 ref不可取，以及首次source guard发现8个test文件漏部署。补齐原SHA文件后使用相同已审核controller开始第二次部署；此前模型调用为0，不重置模型预算。实际模型CLI未观察到运行失败。历史[host screening](../../motivation/gpu/image_host_path_screening_20260802/README.md)、[formal operator](../../image_ai_embed_operator_formal_20260803/README.md)、[transfer ceiling](../../motivation/gpu/image_clip_transfer_ceiling_20260803/README.md)及消费者缓存负结果保持原报告身份，本结果不覆盖或删除它们。

公共材料压缩为15条完整row、一份各臂合同、18次退出收据、预检、部署失败、来源和离线复算；仅将私有绝对路径替换为具名占位符，指标值未变。[provenance.json](provenance.json)记录原件与公开record的独立SHA映射，[storage-manifest.json](raw/storage-manifest.json)校验公开副本。私有原包的85个清单成员已逐SHA核验，清单本身另存（共86个文件）；原包仍按run ID保留在owner控制的本地artifact及已授权服务器，SHA256为 `e2fe329b739ba46ee1702325b132beae0cd3fb83a252e12d6f4ad7c4c84670ff`，不提交Git；需要原包时由owner按此标识取回，公开不含凭据／服务器路径。原JPEG清单、全部日志及进程时间序列仍在私有原包，公开核验无法替代重取这些原件。

当前可支持的结论限于固定文件算子的健康执行、全部冷时间与已观测准备信号；仍缺逐行数值诊断、原生分阶段因果测量和真实GPU异常责任资格。PG E2E、常驻缓存策略、多GPU拓扑／跨卡传输或训练均未由这一轮验证。
