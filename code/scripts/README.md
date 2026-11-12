# SemLoom 脚本入口

当前实验以 SemBench 为主；参数、工作量和生命周期由每次实际配置与回执确定。
脚本只装配库接口，不保存另一套执行策略。

<a id="current-entry-roles"></a>
## 当前入口

| 任务 | 入口 | 范围 |
|---|---|---|
| 单次 Movie Q1/Q2/Q3/Q4 普通 Filter | [run_lotus_records_movie.py](experiments/run_lotus_records_movie.py) | `check/run` 使用现有方法与执行工厂；Q4 计算原 positivity ratio |
| 原生 LOTUS 视觉运行 | [run_sembench_lotus_image_query.py](experiments/run_sembench_lotus_image_query.py) | 已有原生 VLM 服务上的完整查询、原评价和逐 POST 预算；[配置](#sembench-native-vlm-run) |
| SemBench 图像准备 | [check_sembench_image_query.py](experiments/check_sembench_image_query.py) | 只读输入和像素身份／调用预算，不启动模型；[范围](#sembench-image-preparation) |
| 补充 Movie／MMQA／Medical 文本 | [run_sembench_text_query.py](experiments/run_sembench_text_query.py) | 原查询的一个普通 F/M/E、原评价与既有原生图；[逐题规模和限制](../src/baselines/README.md#additional-sembench-text) |
| 原生 LOTUS MMQA Q5 聚合 | [run_sembench_lotus_mmqa_aggregate.py](experiments/run_sembench_lotus_mmqa_aggregate.py) | 原分层聚合、无模型首层预检、外部服务及逐 POST 预算；[配置](#sembench-native-mmqa-aggregate) |
| 矩阵／连续查询／原生 LOTUS 对照 | [run_movie_records_matrix.py](experiments/run_movie_records_matrix.py) | 由计划选择 `backend` 与生命周期，`lotus-native` 走本地模型 IPC |
| 独立作者 Kalypso | [服务](experiments/run_kalypso_native_server.py)、[查询](experiments/run_kalypso_sembench_query.py)、[CPU](experiments/check_kalypso_native_cpu.py) | Movie Filter Q1–Q4／Map Q8–Q10 共用维护 CLI；保留原题后处理与作者默认，Q9 非 TopK |
| 完整查询观测 | [record_observation/workflow.py](profiling/record_observation/workflow.py) | [契约和账本](profiling/record_observation/README.md)，不新增容量 |
| 文件图像与LOTUS检索 | [文件清单](data/build_image_file_manifest.py)、[检索](experiments/run_lotus_image_retrieval.py) | 文件输入与公共 CLIP 实现 |
| CLIP pooling能力检查 | [监督](profiling/run_vllm_clip_pooling_gate.py)、[worker](profiling/gate_vllm_clip_pooling.py) | 保留独立模型检查与退出状态；结构化环境记录复用现有脱敏，非凭据设置保持；[软件检查](../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#clip-harness-environment-recording) |
| 直接原生Daft Movie Q3 | [worker／监督入口](../src/experiments/native_daft_query.py) | READY原子结果、独立关闭时钟；[配置与调用](#native-daft-q3-worker) |
| DuckDB＋作者IMLane Q3 | [构建／接线](../integrations/imlane_duckdb/README.md)、[软件CPU](../src/experiments/native_imlane_query.py)、[模型worker](../src/experiments/native_imlane_model_query.py) | 作者SQL／Arrow；模型worker拥有独立baseline服务，software CLI不会启动模型 |
| 离线结果读取 | [summarize_offline_results.py](analysis/summarize_offline_results.py) | [格式／NA 说明](analysis/OFFLINE_RESULTS.md) |
| 准确历史树恢复 | [materialize_native_baseline.py](experiments/materialize_native_baseline.py) | 只还原与核验，不运行模型 |
| 实验原件恢复 | [restore_result_evidence.py](analysis/restore_result_evidence.py) | 按存储清单恢复原路径和字节，不重跑实验 |

同步原生函数体检查从[现有 unittest](../tests/records_cpu.md#sync-native-source-check)运行。
CSV／本地 records 不要求 PG。真实运行需要明确输入、模型／driver Python、源码身份、
资源所有权、持久次数额度和最终关闭证据；`--help`／CPU 检查不证明这些实际环境已可运行。
新原生 LOTUS 本地 IPC 已有 CPU 检查，官方完整 Q123 因本地缺 `overrides/cdlib/duckdb` 尚未通过，
没有将 Core 替身当作原生执行资格。

<a id="native-daft-q3-worker"></a>
## 直接原生Daft Q3的worker与监督

主driver在取得现有host租约、永久次数claim和当前环境preflight后，调用
`src.experiments.native_daft_query.supervised_run(config_path, new_private_directory, model_python=..., monitor=...)`。
`monitor`继续核对当前资源边界。该入口不安装、不claim、不重试，不替代最终GPU／SHM检查。
监督子进程使用`python -B -m src.experiments.native_daft_query worker`；不再使用首测按结果文件轮询却未锚定READY期限的一次性控制脚本。
所有配置／路径留仓库外；原有模型与driver环境分离。

配置schema为`semloom.native_daft_q3.v1`，显式字段为`reviews_csv/model/sampling_params/engine_kwargs/provider_options`和
`query_timeout_s=1200/cleanup_s=120/stop_grace_s=8`。生成使用temperature0、top_p1、max_tokens8192、stop[]、
零惩罚／repetition1／单输出；engine_kwargs和provider_options省略原生覆盖时给空对象。
读取完整Movie源，调用匹配的本地tokenizer，语义标签只用于driver侧离线评价。

`result-ready.json`先保存一次完整预测，`worker.json`记录执行／关闭回执，`deadline.json`记录实际READY时钟；
COUNT或READY已失败时保存`failed-result.json`而不报READY。附加写入失败不覆盖原异常或supervisor的清理报告。
关闭112秒和最终TERM/KILL的8秒共用一份120秒收尾预算；没有READY时按完整1200秒查询期限停止。
`completed`不是物理GPU退出证明。在driver环境调用`evaluate_q3(source, ready['result']['predictions'],
positive_review_cnt=ready['result']['positive_review_cnt'], checkout=sembench_checkout)`离线评价，
复用保存的预测，不重新运行AI图。失败和下一次规模见[诊断报告](../../experiments/results/diagnostics/native_sembench_q3_20261106/README.md)。

## Movie session 配置

Movie 查询评价和回执合并由
[`movie_records_query.py`](../src/experiments/movie_records_query.py)共用，普通模型构造／运行／关闭由
[`movie_records_worker.py`](../src/experiments/movie_records_worker.py)负责。Session、service和IMLane直接调用所属模块，
生产核心不依赖这些实验模块。

普通矩阵已有 `check`、准备、owner／附着与恢复子命令，实际帮助从上述标准入口读取。
计划的 `backend` 可选择 `semloom-core`、`semloom-static-reference`、`lotus-native`、`daft`、`ray-data`、`imlane`。
原生 LOTUS profile 只声明显式正整数 `native_lm_max_batch_size`；Core 的 token／rolling／admission 配置
不能混入原生 baseline。矩阵实际参数与容量规则见[Movie 入口说明](../src/experiments/movie_matrix.README.md)。

`resident`、`isolated`、Ray `runtime_reuse_only`、Daft `native_single_thread_session` 分别记录；
没有隐式暖机或通过省略关闭制造复用。原生 LOTUS 的冷查询在查询内启动 IPC owner，
常驻查询借用现成 owner，最后由 session 关闭；完整结果的 `result_ready` 先于质量评价和最终关闭。
`query_seconds` 若覆盖整个适配函数，不可替代纯结果可用时钟。

<a id="additional-sembench-text-entry"></a>
## 补充 SemBench 文本入口

入口支持 `prepare-mmqa-text / prepare-medical-text / check / fixture / run`。`check` 不 claim 额度或创建执行图；
`fixture` 使用真实 SDK 和原评价、明确标为零推理软件检查；`run` 不带 `--execute-model` 时只检查。
真实 run worker 仍在现有外部 host 租约／资源 supervisor 内运行，独立进程使用现有 CLI supervisor。

Ray 的原生完整查询使用 [ray_native 配置](../configs/sembench_ray_native.example.json)和同一 `check/run` 命令。
不加该配置仍是旧记录链；普通 `fixture` 也不证明新的原生接点。
新路径的软件完整回放使用 `python -m tests.baselines.text.replay_ray_sembench`
的 `--config / --sembench-checkout / --data-dir / --tokenizer / --reference / --output-dir` 参数。
保存答复按 occurrence 绑定，不能把模型答复按并发抵达顺序或提示去重后分配。
计时、原生版本、预算与退出条件见 [Ray 内存 sink](../src/baselines/README.md#ray-native-prepared-sembench)。

```sh
PYTHONPATH=code python code/scripts/experiments/run_sembench_text_query.py prepare-mmqa-text \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --source-dir "$MMQA_SOURCE_CSV" \
  --scale-factor 25 --output-dir "$MMQA_TEXT_DIR"

PYTHONPATH=code python code/scripts/experiments/run_sembench_text_query.py check \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --data-dir "$SEM_QUERY_DATA_DIR" \
  --config "$SEM_QUERY_CONFIG"
```

各变量由本机仓库外 runtime／packet 提供；准备输出必须是新的 Git 外私有目录。
Movie 数据目录需要 `Reviews.csv`，Q10 另需原 `Movies.csv`，后者仅用于评价。
Movie Q5/Q6/Q7 只允许[显式原生 LOTUS resident Join](../src/baselines/README.md#lotus-movie-exact-join)。
它复用同一 `check/run`，不进入 prepared F/M/E；8 条样本每题需完整 64 对预算。
零模型回放使用已冻结的前 8 条输入、真实 LOTUS SDK 和软件模型 IPC：

```sh
LITELLM_LOCAL_MODEL_COST_MAP=True PYTHONHASHSEED=3 PYTHONPATH=code "$DRIVER_PYTHON" \
  -m tests.baselines.text.replay_lotus_join --config "$JOIN_CPU_CONFIG" \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --data-dir "$JOIN_SAMPLE8_DIR" \
  --output-dir "$FRESH_CPU_OUTPUT"
```

该回放不使用模型答案，不能报告真实质量或延迟；目标 `run` 仍需精确源码、模型/输入绑定与外层监督。
LOTUS resident 可显式设置 `model_initialization=before_queries`，先初始化同一原生模型，
再执行 `ready_first/ready_repeat`，不发额外预热查询；省略时仍是查询内惰性启动的 `cold/warm`。
E-Commerce Q2／Q4／Q6／Q8–Q14 使用[完整 LOTUS 视觉服务入口](../src/experiments/sembench_lotus_image_query.py)，
设置 `scenario=ecomm`、所选 `query` 和 `--data-dir`；沿用配置模板、预付预算与外层服务监督。
真实中间关系交给原生后续算子，不进入单算子文本 CLI；Q12 上界为2N，其余预检记录首算子下界，
完整调用由永久预算封顶。Q1／Q3／Q5／Q7 仍使用文本入口；不为每题增加执行器。
LOTUS1.2.4 多列 Join 的实际选列限制及分组 TopK 生命周期见[接入范围](../src/baselines/README.md#e-commerce-原完整多模态查询)。
MMQA 准备仅调用原三种文本生成方法并复制固定表，不下载、不生成图片、不调用模型。
Medical 准备只复制所选已有规模的三份原 CSV，保留 `data/` 布局，不重新采样或启动图音准备：

```sh
PYTHONPATH=code python code/scripts/experiments/run_sembench_text_query.py prepare-medical-text \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --source-dir "$SEMBENCH_CHECKOUT/files/medical/data" \
  --scale-factor 11112 --output-dir "$MEDICAL_TEXT_DIR"
```

Medical driver 使用 Python 3.12；原查询与评价源码须在固定 checkout 中可读，稀疏 checkout 需包含
`files/medical/query/lotus` 和所选的原文本 CSV。准备后用 `--data-dir "$MEDICAL_TEXT_DIR"`，
配置设置 `scenario=medical`、`query=1/4/10`；可选 `scale_factor` 默认11112，仅选择已有原 CSV 文件名。
Cars 文本同样使用该共享 `check/run`，设置 `scenario=cars`、`query=1/4/10`，`scale_factor` 默认157376。
`--data-dir` 指向原 Cars 场景根，保留 `data/sf_N/` 执行 CSV 和 `data/full_data/` 标签 CSV；
实际模型行数由原投诉／车辆连接取得。原函数、评分与当前验证范围见[Cars 绑定](../src/baselines/README.md#cars-原文本查询)。
完整资产的三题各1200次；原 `_25` 只有1条症状，不将患者数当模型行数。
原 `src.runner` 导入只在固定 MMQA 模块加载期映射到同 checkout 的 canonical runner，随后恢复，
不替换 SemLoom `src` 或修改上游源码。字母题号在 JSON 的 `query` 直接写 `3a`／`6c` 等。

配置 schema 为 `semloom.sembench_text_run.v1`，必需字段为
`scenario/query/query_id/backend/model_name/generation/engine/resources/profile`。
`scenario` 为 `movie/mmqa/medical/ecomm`；题号和实际规模由[逐题清单](../src/baselines/README.md#additional-sembench-text)与 `check` 确定。
`ecomm` 的 Q3／Q5／Q7 直接使用所给目录的原 `styles_details.parquet`，不运行下载或抽样准备；
固定 SemBench checkout 还须包含 `files/ecomm/queries`。同一 `check/fixture/run` 与后端配置继续复用，
调用数来自原查询的实际选择，不按文件行数猜测 Q5 的执行规模。
Q7 的原价格谓词、完整 N×N Join、ID 拼接和 F1 继续保留，不是逐行 N 次。
可选择显式 LOTUS resident，或下述原生 Ray processor／独立 Daft prepared Join；
旧有限记录图仍不接 Join，不能借用普通 F/M/E 资格。
引擎与生成参数继续用现有 `EngineSpec/GenerationSpec`；五项 vLLM 可选参数未指定时交原生引擎解析。
Extract 的 `json_object` 来自原算子，`check.effective_operator_generation` 记录其有效设置。

| backend | profile 归属 | 真实运行所需外部上下文 |
|---|---|---|
| `lotus-native` | 仅显式正整数 `native_lm_max_batch_size`，属于原 LM 客户端 | 已预留的持久 query unit；现有本地异步模型 IPC |
| `ray-data` | 原 `ray.init` 参数；模型图的 batch／actor 资源仍由 `resources` 声明 | 独立冷 runtime；既有原生有限记录图 |
| `daft` | 可选 `runner_threads`；空对象保留原 native 默认 | 既有 native class／batch 有限记录图 |
| `imlane` | 空对象；使用现有作者 owner／resources | `owner_config` 指向现有作者配置、桥构建与显式有限容量 |

非 IMLane 的真实运行配置另含 `budget={path,id,limit,unit_id}`，指向既有已预留 SQLite 额度；
本入口只 claim，不创建／追加／退回额度。IMLane 沿用原 owner budget／guard。
CLI 只允许四种已接通对照；Kalypso Map／MMQA／Medical 不设替代算法，Q4 继续用旧 Filter 入口。

比较时冻结同一输入摘要、模型／有效 dtype、生成参数、DEFAULT 格式与生命周期。
冷查询的 CSV 扫描及身份检查、启动、算子解释／关闭、原关系后处理全部计入 `query_seconds`，原评价另计；
它不能与旧普通 records 图的其他时钟口径直接混算。新入口没有隐含预热或重试。
上游原 LM 上限为8192且无换行 stop；本补充入口各 scenario 的省略生成项现均按该值解析，显式值保持。
旧补充 Movie／MMQA 的省略项行为随此修正改变；公共类型及旧普通 Filter 的默认值不变。
旧短 Filter 的8 token／换行 stop 不可直接移给 JSON Extract，也不能把更改生成配置后的差异归因于执行加速。
只有显式上下文才提前做上限检查；未指定的实际容量与物理退出仍由模型 ready 和外部 supervisor 验证。

<a id="sembench-native-mmqa-aggregate"></a>
## 原生 LOTUS MMQA Q5 聚合入口

[run_sembench_lotus_mmqa_aggregate.py](experiments/run_sembench_lotus_mmqa_aggregate.py)调用完整原 Q5 和原 `sem_agg`，
使用已由宿主管理的文本模型服务；`check` 截止于原 SDK 首个 LM 调用之前，记录首层完整消息、token 计数与分块。
它不请求模型、不 claim 额度，也不代表服务已就绪。原生 SDK 批处理、层次归并和最终评价保持。

配置见[示例](../configs/sembench_lotus_mmqa_aggregate.example.json)，复制到 Git 外填写实际模型、
服务、`native_lm_max_ctx_len` 及本地 `token_counter`。上下文与 `service_profile.max_model_len` 一致；
tokenizer JSON 的路径／摘要、ID／revision 与所声明服务绑定，仍须核对实际服务的 tokenizer 和上下文。
示例的批大小只属于 LM 客户端，示例数值不是硬件校准或新增默认容量。

```sh
PYTHONPATH=code python code/scripts/experiments/run_sembench_lotus_mmqa_aggregate.py check \
  --config "$NATIVE_AGG_CONFIG" --sembench-checkout "$SEMBENCH_CHECKOUT" --data-dir "$MMQA_TEXT_DIR"

PYTHONPATH=code python code/scripts/experiments/run_sembench_lotus_mmqa_aggregate.py run --execute-model \
  --config "$NATIVE_AGG_CONFIG" --sembench-checkout "$SEMBENCH_CHECKOUT" --data-dir "$MMQA_TEXT_DIR" \
  --output-dir "$NEW_PRIVATE_QUERY_DIR"
```

生成项省略时用原8192 token／无 stop；不继承短 Filter 的8 token／换行 stop。
若首层只有一个请求，总量为1；否则后续归并请求依赖实际摘要长度，预检仅给已知首层和最少调用数。
运行读取既有 `budget={path,id,limit,unit_id}`，每次真实 POST 前消费，既不分配、退还，也不自动重试。
单请求示例的额度仅适用于预检确认为一个首层请求的组合；多层需另定有限实验预算，耗尽时保留已发送记录并失败。
查询子进程由现有 supervisor 监督；外部模型启动、退出及宿主资源计量另管，不能当作冷启动查询计时。

<a id="sembench-image-preparation"></a>
## SemBench 图像查询准备

[check_sembench_image_query.py](experiments/check_sembench_image_query.py)只检查实际输入、原查询范围与预算，
不启动模型、不创建额度。`--output-dir` 必须是 Git 外尚未存在的目录。

```bash
python code/scripts/experiments/check_sembench_image_query.py \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --scenario animals --query Q3 \
  --manifest "$ANIMALS_MANIFEST" --manifest-digest "$ANIMALS_MANIFEST_DIGEST" \
  --output-dir "$NEW_CHECK_DIR"

python code/scripts/experiments/check_sembench_image_query.py \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --scenario mmqa --query Q2b \
  --data-dir "$MMQA_IMAGE_DATA_DIR" --output-dir "$NEW_OTHER_CHECK_DIR"
```

Animals 使用现有官方 sf64／软件夹具 manifest，复核编码与原 RGB 像素，不 resize；支持 Q1/Q3/Q7/Q10。
MMQA 读取 `ap_warrior.csv` 和 `images/`，保留原 `os.listdir` 顺序及 `.png/.jpg` 选择规则，
按实际图数×表行数核算 exact Join；Q2b 的后续 Extract 预算上界相同，实际次数取决于匹配行。
Q7 改读 `tampa_international_airport.csv`，仍用同一 `images/`；原代码无欧洲目的地预筛，检查结果明确记录差异。
`check.json` 中五条 baseline 结果均为空，并明确当前本地视觉 provider 尚未接通。
已有原查询／评价可由调用者配置原生 LOTUS LM 后调用；CPU 夹具与旧 HTTP 历史不授予当前 VLM owner 的模型资格。
完整范围与大 Join／TopK 预算见[基线说明](../src/baselines/README.md#sembench-image-preparation)。

<a id="sembench-native-vlm-run"></a>
## 原生 LOTUS 视觉运行

[run_sembench_lotus_image_query.py](experiments/run_sembench_lotus_image_query.py)直接调用 LOTUS 的原 LM、
原算子与原 SemBench 查询／评价；支持 Animals Q1/Q3/Q7/Q10、MMQA Q2a/Q2b/Q7、E-Commerce Q2/Q4/Q6/Q8–Q14。
Cars Q3／Q8 复用相同入口，配置 `scenario=cars` 和可选 `scale_factor`（默认157376）；
`--data-dir` 指向含 `data/sf_N/` 与 `data/full_data/` 的原场景根，CSV 内图像路径须事先绑定可读原件。
独立输入检查 CLI 也支持 `--scenario cars --query Q3 --scale-factor N`，不启动服务或消费模型额度。
原 `check_sembench_image_query.py` 继续只做输入检查，其 `execution_enabled=False` 表示没有执行模型。
实际服务运行使用这个新入口，`check` 或 `run` 不带 `--execute-model` 均不连服务、不消费额度。

配置格式见[示例](../configs/sembench_lotus_image.example.json)，复制到 Git 外后填写实际服务和已预留预算。
API key 从 `api_key_env` 指定的环境变量读取，不能写入配置；本地无鉴权服务也由操作者显式配置该变量。
`service_profile` 是声明的模型／revision／dtype／processor 配置，不自动修改服务或冒充测得 ready 回执；
启动服务的 host owner 核对实际值并负责最终 GPU 清理。原生 SDK 的 `native_lm_max_batch_size` 与引擎容量分开。

```bash
python code/scripts/experiments/run_sembench_lotus_image_query.py check \
  --config "$NATIVE_VLM_CONFIG" --sembench-checkout "$SEMBENCH_CHECKOUT" \
  --manifest "$ANIMALS_MANIFEST" --manifest-digest "$ANIMALS_MANIFEST_DIGEST"

python code/scripts/experiments/run_sembench_lotus_image_query.py run --execute-model \
  --config "$NATIVE_VLM_CONFIG" --sembench-checkout "$SEMBENCH_CHECKOUT" \
  --manifest "$ANIMALS_MANIFEST" --manifest-digest "$ANIMALS_MANIFEST_DIGEST" \
  --output-dir "$NEW_QUERY_DIR"
```

MMQA 配置改为 `scenario=mmqa`、选择 `Q2a/Q2b/Q7`，参数改用 `--data-dir "$MMQA_IMAGE_DATA_DIR"`，
省略 Animals 的两个 manifest 参数。Q7 的机场表须与选定图片放在同一准备目录，不重新抽样；上游欧洲预筛差异保留。

入口只 claim 既有 SQLite 预留单元，不创建、追加或退回额度；Q2b 按 `2×图数×表行数` 覆盖最大完整调用量。
原生 SDK 在每条 POST 前消费次数，自动重试和结果缓存关闭；Extract 的 JSON 格式仍由原算子指定。
生成参数未声明时沿用原 SemBench 的8192 token／无 stop，显式值原样保留；不会借用短文本 Filter 的8 token／换行截断。
结果保留原质量、完整阶段调用数和未知／失败责任；私有 `native-http.jsonl` 保留请求内容摘要、文本消息、响应与图片 URI 摘要，
不把大幅 base64 图片反复写入事件队列。`query.json` 仅为本次结果，实际服务身份与物理关闭另由 host 回执证明。
`query_timeout_s` 是此 CLI 的整体监督预算，包含既有清理预留；查询内部仍记录数据扫描／PNG 格式化、
原算子、解析与关系结果的时间，评价另计。外部服务启动不计入该查询时钟。

## 原生 LOTUS CLIP 查询

[run_lotus_image_retrieval.py](experiments/run_lotus_image_retrieval.py)与
[qualify_lotus_clip.py](experiments/qualify_lotus_clip.py)使用明确的模型、输入和执行 owner。
检索CLI保留参数、执行和资源清理；[实验报告模块](../src/experiments/lotus_image_reporting.py)
负责向量、结果与写入回执，只释放已写出的观测快照。方法与执行模块不导入该实验层。
支持和实机范围见[方法说明](../src/semantic_methods/lotus/README.md#原生-clip-图像检索)，
不因共享 CLIP worker 就互授多卡、任意 DAG 或新模型性能资格。

## 本地时间与额度诊断

[预付计数探针](profiling/mapped_request_probe.py)只使用本地 CPU 替身，按实际剩余时间运行、取消和清理。
预付计数不退还。共享 Core、任务能力与独立组件检查继续维护；这些检查不代表模型或查询性能。

## 独立研究与离线分析

三个[离线代价入口](../src/experiments/operator_costs/README.md)使用共享的profile／比较模块。
比较命令必须选择`--data-csv`或`--ref-json`，context-LOO还须显式给`--output`。
输出路径由各命令参数确定；这些估计不控制当前单查询。

[SemFilter参考校准命令](analysis/build_semfilter_reference_calibration.py)直接使用
[离线构建器](../src/experiments/operator_costs/semfilter_reference_calibration.py)，保留显式`--source`／`--output`。
它只处理汇总观测和产物文件，不启动模型或数据库。

SAOR 的离线汇总与容量回放继续读取已有结果，接口见[研究组件](../src/research/multitask/README.md)。
`run_ai_operator_scenarios.py` 用于显式 profiler 装配和 CPU 行为检查。
命令行核对由[实验服务工具](../src/experiments/shared_vllm/preflight.py)维护，
固定版本声明由[源码审计模块](../src/experiments/saor/vllm_0251_source_audit.py)维护。
[audit_phase_change.py](analysis/audit_phase_change.py)只做状态、请求与动作的离线核对。

<a id="实验结果恢复"></a>
## 实验结果恢复

结果的 `raw/storage-manifest.jsonl` 记录原路径、字节数、摘要与共享保存位置。
从仓库根目录恢复到新的仓库外目录：

```sh
PYTHONPATH=code python code/scripts/analysis/restore_result_evidence.py \
  experiments/results/<result>/raw/storage-manifest.jsonl --output /tmp/<fresh-result-directory>
```

恢复会核对每个文件和归档对象的原字节；已存在的输出目录保持不变。此操作不启动模型或实验。
