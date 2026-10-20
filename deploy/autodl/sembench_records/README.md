# 普通 Movie Q3 records / local-model 首轮配置

本目录是**准备材料**；本轮没有模型运行或正式额度分配。
`prepare.py` 生成有限逐查询身份和配置；`verify_cpu.py` 检查身份、预算及人工固定响应差分。
新增正式控制器`code/scripts/experiments/run_sembench_records_formal.py`默认只check；真正执行必须另有绑定
全部文件/源码/runtime/4200次与30150秒的正式authorization及显式`--execute-model`。
原LOTUS和IMLane adapter已在独立组合区接入公共worker，CPU替身通过单次claim、原Q3及observer契约；
当前source的Linux进程资源、真实模型/template/token与GPU退出资格仍待工程owner补测。
Movie计时observer与supervisor监测callback均为独立最小补丁，交集成者串行接纳；原生调度和模型引擎不改。

## 当前可用结论

首轮只选原始 **Movie Q3：taken_3 普通 Filter → COUNT**。固定执行输入2,000行，原谓词选中120条，
每次完整查询上界为120次main/filter调用；没有helper、抽样、后继算子或模型预热。
五目标臂各给两个有限配置点，再做五个交错测量block：

| 阶段 | 完整查询数 | 每查询上界 | 全阶段上界 |
|---|---:|---:|---:|
| 两点校准 × 五臂 | 10 | 120 | 1,200 |
| 五个测量block × 五臂 | 25 | 120 | 3,000 |
| 合计 | 35 | 120 | **4,200** |

这是**新提案，已分配0次**。失败/unknown不退额度，无自动重试或修复reserve；若缺臂不能称完整排名。
工程补测164次/130分钟属于唯一GPU owner的另一账本。旧四臂×三查询35840模板不参与此计算。
计划估时120–240分钟来自每个冷查询3–6分钟、控制15–30分钟的**未校准假设**；并非模型测量。
提议硬墙钟为35×810＋1800＝**30150秒（502.5分钟）**，需正式外层控制器执行；当前生成器没有运行该控制器。

## 来源、输入与质量

准备基线提交为`79dd14514b34dac9e25a965dcd27381b0ce7bb9d`；集成者只读确认原生产代码与
`a50351051de0d960736c46914f4c56bde2fa5111`一致。新组合实现的精确checkout SHA记录在生成包
`package-identity.json.checked_out_commit`，`source.lock.json`逐文件锁定新增controller/adapter及既有运行代码。
`--check-source`另外要求基线祖先关系和clean checkout；任何运行代码变化都要求重新固定/审查和按范围补测。
SemBench固定`c814e3807e72d4cf876b852b17e77f3cc94575c2`，使用未修改的Movie Q3方法、gold SQL和原评价器；
LOTUS执行profile为1.2.4，区别于SemBench原依赖1.1.3。

执行CSV的SHA256为`ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf`，654765字节。
upstream sf_2000 CSV的SHA256为`df231cfb189c9e169aa6afe415e4fa24533b04499179bbd61416c4c6af28ca21`，654791字节。
两者有360行内容不同，原始生成manifest未找到；不能称全表是同一官方数据。**Q3选中的120条完整行及source_position逐项相等**，gold正例数14。
准备时再次核对两份输入SHA及该关系等价；不认证其他查询。选择全2,000行先于质量评价，执行数据不含标签，独立references只供审计/评价。

模型、tokenizer和权重身份沿[既有完整哈希证据](../../../experiments/results/diagnostics/lotus_records_local_model_20261016/model_identity.json)：
Qwen2.5-7B-Instruct revision`a09a35458c702b33eeacc393d103063234e8bc28`，BF16/TP1/PP1，模板哈希
`cd8e9439f0570856fd70470bf8889ebd8b5d1107207f67a5efb46e342330527f`。
8 output tokens、temperature0、top_p1、seed0、换行stop，result cache关闭，原True/False parser/default保留。
`comparison.json`保留参数、版本、资源、计时和质量门槛；既有身份报告不能代替当前部署重新核验。

每臂须保留真实逐行决策、TP/FP/TN/FN、invalid/default解析、usage、COUNT和原评价。
所有臂与原生参照同错仍须报告原质量不足；COUNT误差抵消不能隐藏行错误。
人工全False响应只用于CPU方法差分，绝不称模型质量或后继算子通过。

## 五臂能力与尚待接点

| 目标臂 | 已有路径 | 当前门槛 |
|---|---|---|
| 原LOTUS | 原SemBench accessor、LM/LiteLLM原生调度、新本地HTTP adapter | CPU完整120行p1/p2差分；实机服务/模板/tokens/质量及退出待补测 |
| SemLoom Core | `semloom-local-model-core`，生产SessionEngine、固定静态策略 | batch1是自身限制；当前source/新profile与完整计时须核验；无Core的static-reference另列 |
| Ray Data | `ray-data-local-model`，原生CSV/Filter/actor batches | 保留native actor pool、batch4/8和两块在途；现有CLI仍是qualification |
| Daft | `daft-native-local-model`，原生process UDF、私有模型IPC | 不初始化Ray；`max_active_batches`不生效；保留原生缓冲及加载/关闭成本 |
| IMLane | 作者DBEnd/Lane/CPython、新Q3 COUNT/evaluator adapter | 公共worker与作者carrier CPU通过；当前作者ABI、模型及物理退出待补测 |

精确profile及有效/无效字段见`backends.json`。Native LOTUS保留自己的`max_batch_size`与LiteLLM调度，
不得将Core、公共credit或自制持久模型池注入native臂。原生配置各获两个有限机会，不称已找到最优或饱和点。
校准按正确性/同质量/资源通过后选择较低完整冷查询时间，精确tie选p1；测量前冻结selection及配置哈希。

当前记录CLI每次新建runtime/graph/model owner；本提案只定义这一**冷启动实验**。
原生LOTUS当前接口为query独占的localhost HTTP服务，保留原LM/LiteLLM并发，记录ready/calls/close同一service UID；
服务加载/关闭进入完整冷成本，HTTP与embedded的协议和token差异须在实机资格时核对，不能将全差异归于调度。
常驻完整查询目前单列`unavailable`，要测常驻需新的原生生命周期入口、逐query调用上界和另行预算；重复冷CLI不能充作常驻。

## 可运行的无模型准备与验证

以下变量指向已有本机资产和**仓库外新目录**，无需下载。`RUNTIME_BINDING`可先用本目录
`runtime.example.json`，其占位路径只能验证配置形状；真实部署必须换成仓库外完整绑定并做runtime preflight。

```sh
python3 -B deploy/autodl/sembench_records/prepare.py \
  --input-csv "$FIXED_MOVIE_CSV" --upstream-csv "$UPSTREAM_MOVIE_CSV" \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --runtime-json "$RUNTIME_BINDING" \
  --output-dir "$NEW_PRIVATE_PACKAGE" --run-id "$NEW_UNIQUE_RUN_ID"

HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 LITELLM_LOCAL_MODEL_COST_MAP=True \
MPLCONFIGDIR="$PRIVATE_MPL_CACHE" "$DRIVER_PYTHON" -B deploy/autodl/sembench_records/verify_cpu.py \
  --package "$NEW_PRIVATE_PACKAGE" --sembench-checkout "$SEMBENCH_CHECKOUT" \
  --evidence-dir "$NEW_PRIVATE_CPU_EVIDENCE"
```

准备只用标准库和既有source类型；CPU差分需已安装固定LOTUS/数据依赖的driver环境。
`commands.json`包含Core/Ray/Daft既有`check`精确argv和`run`资格模板（含显式模型flag，但不执行）。
IMLane `verify_filter.py --config ... --output ...`是实际资格命令，会启动模型，不冒充无模型check。
准备包中独立unit的`formal_run_argv`为null，不能单独绕过外层预算；每unit附有
`formal_campaign_argv_template`与`formal_worker_argv_template`，后者context只由控制器在永久reserve后写入。
正式控制器的真实入口如下，所有执行门槛在模型之前核对：

```sh
"$DRIVER_PYTHON" -B code/scripts/experiments/run_sembench_records_formal.py check --package "$NEW_PRIVATE_PACKAGE"
# 仅在parent正式预算批准、最终source/runtime/adapter资格齐全后执行；本轮不运行：
taskset -c 0-7 "$DRIVER_PYTHON" -B code/scripts/experiments/run_sembench_records_formal.py run \
  --package "$NEW_PRIVATE_PACKAGE" --authorization "$FORMAL_AUTHORIZATION" \
  --output-dir "$NEW_PRIVATE_FORMAL_OUTPUT" --execute-model
```

authorization schema为`semloom.sembench_records_authorization.v1`，字段为`approved`（必须true）、唯一`budget_id`、
`limit_calls`4200、`max_wall_s`30150、`package_integrity_sha256`、`runtime_source_lock_sha256`与`runtime_preflight_sha256`。
文件在Git外由批准后的运行owner提供；没有该文件或字节不符则拒绝。`worker`是外层监督的内部入口，还检查
原授权、包/配置身份、祖先拥有的normal lease和一次性预算claim；不能借worker绕过controller。
控制器使用现有SQLite cell ledger，每查询永久预留120、worker只claim一次；无退款、重启或隐式续跑。
RSS/SHM/CPU亲和性监测回调仅停止本次supervisor已观察的owned进程；SHM是独占lease下全机用量增量的保守观测，
不声称内核硬限或未观察detached进程已退出。失败原件保留，责任不明则normal lease留在held状态。

生成物包括`source.json`、标签隔离的`input/`、35份`units/*.json`、每个候选的精确`configs/*.json`、
`schedule.json`、`commands.json`、`package-identity.json`和逐文件`integrity.json`。
每条unit给出120个main/filter call ID、发生项身份、输入和配置SHA、上界、原生owner、输出路径、未运行和未分配状态。
CPU证据为独立目录的`verification.json`与`original-q3-fixture.json`，不写入待执行包改变其哈希。

## 正式指标、证据与停止条件

主查询时钟须从所有臂相同、实际query-scoped读取/核验和后端构造前开始，到完整COUNT结果交付结束；
冷CLI墙钟另从driver启动覆盖至结果持久化和owned进程/GPU清理。离线投影、runtime、加载、扫描、prepare、model、fan-in、
evaluator、关闭/物理退出与证据写入分别记录。现有`query_seconds`起点在preflight/expected/runtime之后，不能直接用于主表。
嵌套/并行阶段不可相加，未知字段用null/reason，不填零。

每个`<run-id>-formal/<query_id>/`至少要有：实际argv/env的脱敏清单、最终source/profile/硬件/版本和模型文件签名、外层墙钟、
query.json（完整结果和各调用）、已生效batch和实际load/worker/PID事件、resource时序、模型usage/tokens、
source/sink/quality审计、supervisor.json、关闭/独立GPU退出与lease/永久ledger回执。
当前三个records臂已有preflight/query/supervisor/worker日志，但部分正式字段尚无统一采集，不能写成已存在。
新公共worker落盘`context.json`、归一物理GPU UUID的`worker-binding.json`、`query.json`及`clock.json`；
外层写`physical.json`、`timing.json`、原parser/default使用的`quality-audit.json`，结束写`campaign.json`与永久budget.sqlite。
父进程CPU审计使用原CoT preprocessor统计default与双token，保留原positive优先策略，不重写逐行决策。
控制器只有在正常lease、GPU/owned进程退出和SHM确认后才写`lease-released.json`；lease获取失败保留`preflight-failure.json`。
`imlane_artifact_root`必须沿用同一主机已有owner共用的normal lease根，不能用新私有根绕过排他锁。

每臂5个完整查询样本，仅报告全部原值、median、min/max、按block配对比及明确时钟的吞吐。
P99/P99.99记为`unavailable`；120个行调用、同批共同结束时间、CPU测试数都不增加独立query样本数。
299/29956个独立样本也只给约95%概率见到一个真实P99/P99.99以上样本，并不自动获得可靠分位数估计。

正式控制器应在模型构造前持久claim全query上界，失败/unknown照耗，永不借用/清空旧ledger。
normal独占lease、指定8 CPU槽、单现有4090、采样RSS20GiB/SHM512MiB及完整外层墙钟是提议共同上限，须核对实际机器并监测。
任何调用关联/用量/质量不可比、超资源、签名漂移、失败或未知退出立即保存失败材料并停相应单元；
发现foreign owner或物理退出不明则停全队列，保留owner责任，不结束他人进程，不启动后继、不自动重试。

本轮不纳入原Q1/Q2/其他query、helper cascade、有限链替代原查询、Async候选、图像、作者Ray或多GPU。
旧request/小模型资格、空过滤未达到后继的运行、CPU人工响应与新source的真实模型资格分开。
Q1/Q2/Q3×五臂覆盖及最小增量见`query_coverage.json`：Q1/Q2只需Filter任务、源顺序head5/reviewId投影和原评价接线，
并非缺Join；但Q1全表身份仍待解决。同样35-query设计时Q2增4200、Q1增70000，三query合计78400次提案，绝未扩当前164工程预算。

集成者后续可在上级AutoDL README登记本目录入口；共享README/INFRA/台账/PROJECT_LOG由唯一集成者串行整合。
