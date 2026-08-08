# 项目日志

## 2026-08-08 开题文本 feeder 重校准前修复项目臂 manifest 透传

- 背景：首轮统一 database-E2E 中项目臂的同协议 feeding ratio 未达到 95% 门禁，按冻结规则降级为 failed-feeding 诊断，需要在相同运行签名下先校准项目静态 feeder。
- 修复：`multicard_scale_ramp.py` 的项目臂现在显式透传当前 scale 的 immutable request manifest，并启用 database-E2E timing boundary；此前 direct/DuckDB 使用该 manifest，而项目臂退回数据库顺序，不能作为同分片校准证据。
- 验证：新增回归测试锁定项目臂的 manifest 路径和计时边界。校准仍只单变量扫描 K/active-work，正式矩阵在选择最小饱和点后冻结参数。
- 新增可复现纠正校准模板：每个 workload 同 manifest 固定服务、token budget、active work 与 8×32=256 actor slots，三次重复扫描 project K32/64/128/256；K256 为既有正式合同点，必须包含在项目峰值参照中。按 97% 项目已测峰值与 95% direct feeding 双门选择最小点。此前 8×16 的 SQuAD K32/64/128 完整运行与 ShareGPT 未完成预热均仅保留为诊断，统一容量重跑后才冻结。首轮结果状态改为 failed-feeding 诊断，待整体替换重跑，不扩大开题 baseline。
- 新增版本化校准审计器：逐格重算 direct group service tokens/s，检查三次重复、exactly-once、manifest SHA、完整行数、worker/actor failure、resource metrics 与终态空队列；只有全部审计通过才按双门自动输出最小 K，否则 fail-closed。对应单元测试覆盖 K256 峰值参照下的最小 K 选择和跨臂 manifest 不一致拒绝。
- 替换正式矩阵不再复用全局 K：新增 workload-specific 校准合同入口，SQuAD 与 ShareGPT 分别读取选择 JSON；runner 在跑前核对 selected/audit 状态、3 repeats、0.95/0.97 阈值、manifest SHA、K、6144 token budget、65,536 active work 和 8×32 actor shape，direct/DuckDB 的每 endpoint 32 保持不变。
- 再确认开题与最终方法的层次：当前 `project_frozen_static` 仅用于先建立喂饱后的强静态控制，不代表最终 proposed。开题冻结后先补 image Daft built-in/Ray Data native/typed Ray actor 同机 formal，再在相同 K/W 上限下做 steady→阶段突变/突发→长短混合→1/2/4-job staggered/weighted/heterogeneous 的 frozen-static vs state-aware dynamic；图像复用同一策略抽象。现有文本 equal-workload 1/2/4-job 是先验证据，不能替代尚未完成的剧烈变化实验。
- 修复开题矩阵汇总器退出语义：旧实现虽在 `audit.json` 记录 feeding 失败，命令退出码却只检查 cell count/status。现在要求 project feeding≥95% direct、project GPU mean≥80%、exactly-once、sink、0 infrastructure failure、manifest/PG identity 一致才返回成功；DuckDB 等产品 baseline feeding 仍只报告，不为过项目门禁而调参。新增回归测试锁定该边界。
- 补齐 feeder 校准 MFU：raw 已有 vLLM estimated-FLOPs counter，但 profiler 因未注入 GPU peak 把原生 MFU 标为 unavailable。校准审计器现在按 4090 BF16 165 TFLOPS/GPU 显式恢复 direct 双 endpoint 聚合 MFU与 project per-GPU MFU，保存每次重复/中位数、公式和峰值假设；该指标只作资源利用证据，不替代 95% service feeding。
- SQuAD 统一 256-slot 校准的 K128 第三次出现一次空消息 HTTPX 异常，cell 保留为 failed incident，endpoint 仍健康、服务日志无 5xx/CUDA/engine error。Completion backend 错误现在附带具体异常类型，避免 `RuntimeError: ... failed:` 空尾无法区分 ReadError/timeout；失败重复不得静默删除，按同配置补跑并由校准审计器显式合并事故与 replacement。
- 新增校准 replacement 合同：失败后在新 root 只补同 K 一个成功重复，审计器通过 `--repair-root` 合并；要求每个 direct/K 组成功数恰好为 3，并把原失败记录永久保留在 `failed_incidents_preserved`。这避免为一次传输事故覆盖原证据，也禁止多跑后挑最好三次。
- 修正校准 direct group 计时：gate 已明确保存 `group_service_wall_s`，它比最大 shard JCT 多出微小 group join 开销。审计器改为用服务端总 token / group wall 重算正式 direct 吞吐与 MFU，并只用 max-shard JCT 检查 group wall 不短于 shard 且额外开销不超过 2%；不再把两种不同计时边界要求到浮点完全相等。
- 重启后第一次 SQuAD 校准在发出请求前 fail-closed：`_ensure_ray_head()` 对不存在的 6380 head 做复用探测时抛出未捕获的 60 秒 `TimeoutExpired`，没有进入 fresh-start 分支。失败目录与日志保留；修复为 10 秒有界探测并将超时显式降级到 stop/fresh-start，新增回归测试后使用全新 retry 目录运行。

## 2026-08-08 v6 通过 Microsoft PowerPoint 真实打开检查

- `opening/slides/opening_defense_20260807_v6.pptx` 已由 Microsoft PowerPoint
  实际打开并识别为 28 页；首页与缩略图正常，未出现文件修复或页面损坏提示。
- WPS Office 进程虽启动但未出现文档窗口，不计为通过；PowerPoint 的直接证据满足
  `opening/AGENTS.md` 的应用级检查门禁。
- 当前只剩飞书在线覆盖/图片插入/回读、平级 Wiki 镜像和 Git 发布收尾；不因此扩大
  开题实验或 baseline 范围。
- 飞书用户明确批准后，目标 docx 已从 revision 277 覆盖到 revision 289；八章目录、SQuAD
  89.93%、ShareGPT 91.38%、4,936/6,144 cap 语义失败、“不能声称”边界及四个带 caption
  的图片块均已回读通过。

## 2026-08-07 开题证据闭环：统一三臂负结果、四图与 v6 材料冻结候选

- 按冻结合同在 AutoDL 双 RTX 4090 完成 SQuAD uniform 与 ShareGPT controlled-skew 的
  direct / DuckDB AI / project frozen-static 统一 database-E2E：24/24 单元、18 formal，
  source manifest、exactly-once 与 PostgreSQL sink digest 全通过，基础设施失败 0。
- 项目臂 service tokens/s 相对 direct：SQuAD 89.93%、ShareGPT 91.38%，均未过预注册
  95% feeding 门；正确结论是冻结静态项目路径没有优势，只作为负结果与瓶颈诊断。
  DuckDB AI ShareGPT service tok/s≈direct，但三次 formal 有 4,936/6,144 行 fixed-cap
  产品语义失败；性能与语义代价分开报告。
- 新增正式结果目录 `experiments/results/opening_database_e2e_text_20260807/` 和独立汇总器；
  服务器保留全量 raw，Git 范围只含去敏配置、状态、聚合 CSV/JSON 与报告。
- 开题证据压缩为四张可复现核心图：serving capacity、work organization regime、image
  matched-resource、cost-model decision quality；每图有 claim/input/视觉 QA 合同。
- 重写 `PROJECT_OUTLINE.md`、开题报告、Claim Matrix 与问答库；本地飞书源稿与报告完全一致。
  生成 v5 模板继承的 28 页 `opening_defense_20260807_v6.pptx`，修复旧正文残留和图片帧
  透出，28/28 notes、0 空 placeholder、`slides_test.py` 无溢出。
- 外部阻塞：飞书用户 refresh token 已过期，重新授权又被当前 Codex 额度门禁拒绝（可重试
  2026-08-08 14:05）；平级 `../ai-operator-wiki/` 当前不存在。线上覆盖、wiki 镜像、Git
  stage/commit/push 与一次 WPS/PowerPoint 实际打开检查尚未完成，不伪称最终发布冻结。


## 2026-08-07 数据存放政策修订 + raw 全量下载到本地（撤回 git raw tarball）

- 用户修订政策：**raw 实验数据存本地磁盘 + AutoDL 服务器，不进 git；git 只放 aggregated CSV / summary / README / 代码 / 计划**。
  此前同日早些提交的 7 个 `<exp>_raw.tar.gz`（commit 40471bf，43.6M）按新政策**从 git 撤回**：reset 回 c250e19
  并 force-push（raw 已先全量下载到本地，无丢失；commit 是 <1h 前的 tip，仅本机本地库有，codex 仍在 c250e19）。
- **raw 全量下载**到开发者机 `C:\Users\ays\Desktop\results\`：experiments/results 917M + motivation/results 2.5M
  + experiment-artifacts 308M（去 retired-worktrees 代码快照）。3 个 transport tar.gz 的 sha256 与服务器逐一校验一致
  （见 `experiments/results/RAW_ARCHIVAL_20260807.md`）。git 远程因此不再承载 raw。
- **订正早先错误**：同日早些曾称"320-run per-request raw 已被服务器清理"——**错误**。该 raw 实际在
  `experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260807/`（67M），已随 experiment_artifacts.tar.gz 落本地。
- **仍保留在 git 的 aggregated summary**（属"整理汇总"，符合新政策）：enhanced ramps 的 ramp_aggregate.{json,md} +
  ramp_run.json（c250e19 已 track）、各实验 runs.csv / manifest / formal_summary / README。
- **下载方法**（可复用）：AutoDL 服务器 SSH 仅 exec 通道可用（SFTP 被 container 禁用），用 `rd.py recv`
  （`dd|base64 -w0` 分块 + Python 二进制解码 + 单连接多 chunk）下载，配 `MSYS_NO_PATHCONV=1`
  防 Git Bash 把 `/root/...` 转译成 Windows 路径；exec 通道有 \n→\r\n 换行转译，故必须 `base64 -w0`（无换行）。
- **仍残留在 git 的 raw tarball（待用户定夺）**：`operator_cost_profile_pilot_20260804/v1_diagnostic_raw.tar.gz` +
  `v2_raw.tar.gz` 是更早 commit 的 pilot/diagnostic raw，被该实验 README 引用；按新政策属应迁出项，但非本次新增，
  未单方面删除，待确认。


## 2026-08-06 保留 TPC-H-derived AI 查询计划代价验证（条件性）

- 用户确认希望代价估计最终能够辅助数据库优化器选择包含 AI 算子的执行计划；该方向正式登记为
  `planned-conditional`，仍属于数据组织与提交控制的共同使能组件，不扩张为第三项研究内容。
- 当前 cache-on 双 4090 320-run 合同保持不变。只有重跑数据完全有效，且至少一个可部署估计器通过
  已冻结的 candidate pairwise 与 median/macro/max regret 门槛，才进入计划级 capability。
- 条件实验只称 `TPC-H-derived`/`TPC-H-inspired`：比较 filter/join/materialize 位置与冻结运行配置，
  报告 whole-query Q-error、plan ranking/pick rate、selected/oracle JCT 与 regret；不得称官方 TPC-H
  或 TPCx-AI compliant。
- `idea-evaluator` 结论为 Accept with Revisions。主要风险是 scope creep 和候选计划语义/请求集合不等价；
  已通过分阶段启动、canonical request/result digest、阶段计时、actual invocation/token work 与负结果
  停止条件预注册防御。完整合同在 `experiments/plans/operator_cost_profile_dual4090_formal_20260804.md` §8。

## 2026-08-06 SQuAD endpoint 拓扑审计：单 endpoint 证据与双 endpoint 方法验证分轨

- 复核当前 `squad_database_e2e_runner.py`、DuckDB-ai adapter 和运行模板：runner 只有 singular
  `--endpoint-url`，adapter 每次只接受一个 endpoint shard；已归档 DuckDB/direct full 与远端 project_static
  256 gate 实际都只访问 endpoint 8000 / GPU 0。主机有两张 4090 不等于本次使用双 GPU。
- 官方 DuckDB community `ai` 页面只公开一个 `duckdb_ai_base_url` / secret `BASE_URL`；上游 README 对多后端
  部署建议接用户自己的 gateway，未发现 endpoint-list、round-robin 或 least-loaded 原生设置。历史
  `dual_gpu_duckdb_ai_capability_gate` 实际由实验 harness 预切 manifest 并各跑一个单 endpoint DuckDB 查询，
  现诚实降级为 `harness_sharded_diagnostic`，不再写成 DuckDB-ai 原生双 endpoint。
- 协议改为三轨：① 单 endpoint 产品语义轨（DuckDB 原生 SQL + direct/project controls，project 方法退化）；
  ② direct/bounded 强 control vs project 冻结静态与 endpoint-aware 候选策略的双 endpoint 方法轨（项目方法
  主证据，direct 只作 causal control）；③ DuckDB 经第三方 gateway 的可选完整系统轨（只能下系统级结论，
  不属于 DuckDB 原生 baseline，也不阻塞前两轨）。
  禁止自写 DuckDB 跨 endpoint
  分流进入产品原生 baseline，也禁止三臂都过同一 gateway 后声称验证项目路由。
- 单 endpoint runner 新增强制 topology evidence：endpoint_count=1、multi_endpoint_method_exercised=false；
  project_static 另记 `method_claim_admission=blocked_single_endpoint_degenerate`。误导性的
  `dual_gpu_squad_database_e2e.example.json` 更名并重写为
  `single_endpoint_squad_database_e2e.example.json`。
- 服务器 DuckDB-ai v0.4.14 进一步做常量性 probe：`secret => CASE ...` 与 `base_url => CASE ...` 均被拒为
  “must be a constant expression”，证实每条查询只能固定一个本地 `BASE_URL`。even/odd `WHERE` + 两条
  `ai_complete` + `UNION` 仍是人工静态切分，继续只允许作 diagnostic；probe 的原始 SQL/错误文本尚待归档。
- direct_client 服务器备份与 Git CSV 的 SHA 异常复核完成：两侧 `csv.DictReader` 均为 10,570 行且逐字段
  0 差异，仅 CRLF/LF 不同；不是运行/provenance 不一致，不需为此重跑。

## 2026-08-06 project_static 合同复审：撤回过早 GO，补 active-work 与实际扫描证据

- 基于 `0795216` 复跑目标测试，发现远端声称的“全部通过”不成立：组合测试仍有 2 个
  tuple-unpack 错误；同时 `project_static` 漏掉文本轨已标定的 per-endpoint active-work，
  “actual-scan hash”实际只是跑后第二次 DB 读取，completion evidence 对畸形/重复/缺失输出会静默吞掉。
- 修复冻结合同：新增必填 `--project-max-active-work-per-endpoint`（正式候选当前 65,536），显式锁定
  raw chat、temperature=0、httpx async、fixed-output-cap、per-endpoint K、actor topology；provenance 改为
  `project_ray_frozen_static`，不再把仅 static-K 的不完整路径称作 frozen-best。
- profiler 新增 opt-in `--source-scan-evidence-output`，从**实际交给 organizer 的 Arrow table**生成
  doc_id + prompt SHA256（不落原文）；runner 将其与跑后 DB 完整性/评分读取逐行核对，再用 importer
  structured hash 校验 references，区分“实际扫描身份”和“跑后数据库快照”。
- completion evidence 改为 fail-closed：完成行缺输出、doc/output 数量不等、重复或 trace 外 doc_id
  直接失败；CLI 强制 completion evidence 必须同时启用 request trace。
- 计时问题尚未伪装解决：profiler `e2e_s` 与进程内臂 wall 不同，project_static 明确
  `comparison_admission=blocked_unified_timing_boundary`。本轮只允许本地单测及远端 256 行正确性门禁，
  统一计时墙和同运行签名静态校准归档完成前，不启动三臂正式排名。

## 2026-08-05 database-E2E runner H7：scratch_dir 永久 repo-local + sink readback 内容核验 & fail-closed

- codex H6 复核后仍两阻断，本轮 H7 修完（不重跑 DuckDB full、不动原始机器证据）：
  1. **`_scratch_dir` 永久 repo-local**：原 `mkdtemp()` 在 codex Windows 沙箱成功、但往里写 `prov.json`
     被拒（PermissionError），fallback 只在 mkdtemp 自身抛错时触发→不够。改为**始终**在
     `code/tests/baselines/_e2e_runner_tmp/scratch_<n>`（`Path.mkdir` + 计数唯一 + `rmtree`）创建 scratch，
     完全不依赖系统 temp——repo 树在哪 checkout 哪就可写。
  2. **sink readback 内容核验 + fail-closed**：原只数 doc-id 数量（历史残留同 doc-id 也能过）且 mismatch
     不影响 `passed`。改为对 `(doc_id, completion_text)` 取 digest 核验（expected vs 实际读回），`matched`
     要求行数+内容都一致；新增 `_readback_ok`，纳入 `single_run_valid`/`passed`/EXIT——readback mismatch
     或查询报错 → status=failure、EXIT=1（即使 0 error/NULL）。`writeback_mode=none` 时跳过（vacuously ok）。
- 测试：加 `_readback_ok`（matched/mismatch/error/none）、`_scratch_dir`（repo-local 可写+清理）、
  readback-mismatch 集成（0 error/NULL 但 readback 失败 → EXIT 1）。**114 测试通过**（74 runner/gate/redact/
  scanner + 6 diagnostic + 28 scenario + 6 新）。
- **Wiki 同步记录**：`bounded_output_duckdb_comparison_protocol_20260805.md`（08d061c 协议更新）已于
  **本地机器** `C:\Users\ays\Desktop\ai-operator-wiki`（`sync-wiki.sh`，非服务器侧）同步到 wiki 的
  `experiments/plans/`，diff 与项目**完全一致**。wiki 是独立 repo、不在项目 GitHub；**canonical 源是项目
  git 里的 `experiments/plans/` 文件**（codex 可据此核验），wiki 仅作镜像。

## 2026-08-05 database-E2E runner codex 复核：6 项口径修复（不重跑、不覆写原始证据）

- codex 复核 `79a9d6c` 的 E2E runner，6 项问题全部修复（纯口径/审计，不重跑 DuckDB 全量、不覆写
  `squad_database_e2e_duckdb_ai_20260805/` 的机器原始文件；订正写在该目录 README §8）：
  1. **状态字段解耦**：`single_run_valid`（本次 0 error/NULL）/ `formal_run_gate_passed`（单次 runner
     恒 false；1w+3f 是另一协议）/ `comparison_admission`（`pending_formal_repeat`——单次不授予/排除
     准入）。不再"单次 clean 即 formal pass、失败即自动 eligible"。
  2. **failure_rate 去重**：原 `(error+null)/row` 把同一失败行计两次（报告 0.000189）→ 改
     `(row-success)/row`（正确 0.0000946），另分列 `error_rate`/`null_rate`/`max_tokens_rate`（允许重叠）。
  3. **operator_only_jct 用整体 span**：`max(completed)-min(started)`（原取 `results[0]`，对 DuckDB barrier
     碰巧正确，对 direct_client 会错）。抽纯函数 `_operator_span` 供单测。
  4. **"模型调用 99%"措辞**：E2E README §8 订正——adapter 占 wall 99.27%、operator query barrier 占
     93.54%，不能单独归因给模型（adapter 含 setup+DuckDB 执行+HTTP+排队+模型）。
  5. **sink 可读回性**：runner 新增 `sink_audit.csv`（doc_id/source_example_id/status/error/output_chars），
     把 sink 里的空串（失败行 NULL→""）回连到真实 status（不动 `write_completions` 共享合同）。
  6. **diagnostic 脚本技术债**：DuckDB 每个 cap 恰好 `repeats` 次（原 repeats+1）；`all()` 判据对全 HTTP
     失败防真空（加 `direct_http_200_at_cap64`）；**不重跑**历史诊断。
- 测试：runner 单测加 failure_rate 去重回归 + `_operator_span`（barrier/per-request/空）+ 解耦状态字段
  断言 + sink_audit 表头；集成测试改 repo-local tmp fallback（codex Windows 沙箱可跑）。**74 测试 + 28
  scenario 测试通过**（codex 本机此前 2 项因沙箱拒写 temp 未能跑——已修）。
- `79a9d6c` 保留为单臂 runner 可行性证据；指标订正写在 README §8，机器原始文件不动。下一步按 codex
  顺序：实现 `direct_client` 臂 → 冻结完整服务配置 → 三臂 1w+3f 正式。

## 2026-08-05 database-E2E runner 落地 + DuckDB-ai 臂实测

- 实现 `code/scripts/baselines/squad_database_e2e_runner.py`（`08d061c`）：一个计时墙包住 scan→construct→
  `run_duckdb_ai_complete`（operator-only 时间戳保留）→ 统一 sink（`write_completions`→`document_completions`）；
  抽出共享 helper 到 `code/src/baselines/common/squad_identity.py`（capability gate 与 runner 共用，行为不变，
  70 测试通过）；runner 层算 `correct_rows_per_s`（主 headline）/`successful_rows_per_s`/failure rate；状态字段
  拆 `capability_gate_status`/`formal_run_gate_passed`/`comparison_admission`，不削弱 zero-error validity。
  DuckDB 扩展继续拥有 batching/concurrency，不注入项目 credit/actor/backpressure。
- 服务器实测（DuckDB-ai 臂，全 10570，cap=64，strict-attribution）：**database-E2E wall 93.9s = scan 0.14 +
  construct 0.23 + adapter 93.2（op_jct 87.8）+ sink 0.26**。**关键观察**：本臂 database 开销（scan+construct+sink
  ≈0.63s）< wall 的 1%——模型调用独占 99%（DuckDB-ai barrier 执行一条 set-oriented SELECT 跑 10570 次调用，
  scan/sink 可忽略）。`correct_rows/s` 90.42、succ_rows/s 112.56、sunk 10570（psql COUNT 复核）；EM 80.32% /
  F1 89.42%（独立复算一致）；attribution attributable；pgvector 0.8.5（探针修后正确）；command 脱敏。
- fail-closed 触发（1 偶发 max_tokens 截断，与 full capability gate 同源、机制未定）→ 状态 `failure`/`false`/
  `eligible_with_documented_failure`。这是单臂 E2E 测量，**不是数据库系统排名**（direct_client/project 臂未实现）。
- 边界口径：PG 连接建立算 setup（不计入墙）；DuckDB 连接+扩展加载在 adapter 内、计入 adapter 段；metrics
  settle + after-scrape 在墙外。
- 下一步：补 `direct_client` 臂（直连 vLLM）→ 看 E2E 拆分差异；补 `project_static` 臂 → 三臂 E2E 正式排名；
  正式前填全当时名为 `dual_gpu_squad_database_e2e.example.json`、现已更正为
  `single_endpoint_squad_database_e2e.example.json` 的 REPLACE_ME（vLLM launch cmd/revision/dtype/parallelism/
  VRAM/env）并重算 service-config-hash。

## 2026-08-05 SQuAD full/diagnosis 审计订正：收紧措辞 + 拆状态字段 + 登记新文件

- codex 复核 `74552b3`：核心事实成立（观察到一次 max_tokens→NULL、孤立重放不可复现、full 维持 FAILURE、
  DuckDB arm 可入失败感知比较），但定性需收紧。本提交是**纯审计/严谨性**，不动实验、不重跑 full、不抬 cap。
- **收紧 1（根因）**：不归因到「高并发 vLLM 浮点抖动」。3 次孤立重放只证明「不是稳定单行属性」，不能
  证明根因是 batching/浮点规约/logit。统一改为：full-set query（DuckDB concurrency=32）中观察到一次
  截断；孤立重放未复现；**单次、机制未定的生成尾部事件**；并发/批状态是候选解释，未隔离验证。
- **收紧 2（并发规模）**：不是 10570 同时并发；写「10570-row full-set query、扩展最大并发 32」。
- **收紧 3（请求体）**：direct 多显式 `stream=false`，与 DuckDB 请求体**语义等价**、非字节相同。
- **收紧 4（质量影响）**：孤立重放的 46-token 文本本身 EM=0/F1=0（错答，已用共享 evaluator 实算确认）；
  故该行质量贡献不论 NULL 或该文本都 0 分——截断只影响可靠性指标，不改变质量。
- **状态字段拆开**：`capability_gate_status=failure` / `comparison_admission=eligible_with_documented_failure`
  / `formal_run_gate_passed=false`。失败 cell 完整保留并计 EM/F1、failure rate、successful/correct rows/s，
  **不削弱 zero-error validity gate**，不得冒充其 headline。
- **登记新文件**：`squad_truncation_diagnostic.py` 进 `code/scripts/README.md`；新结果目录
  （request_equivalence_gate / squad importer / capability_256_v4 / full_10570 / truncation_diag）
  进 `feasibility/results/README.md` 阅读顺序 + `PROJECT_INDEX.md`。
- **下一步**：database-E2E runner 开工，边界 = 统一 PG source/prompt/cap=64/服务配置/sink；顶层只做静态
  分片/计时/审计/写回；DuckDB 扩展继续拥有 batching/concurrency；不引入项目 credit/actor pool/动态 backpressure。

## 2026-08-05 SQuAD 截断定点诊断：推翻「确定性 rambling」，改记偶发尾部风险 + baseline eligibility

- 用户裁决：选 (b) 做定点诊断 + (a) 接受产品边界；不重跑全量、不抬正式 cap=64。
- **先修 pgvector 探针**（`9aefeba`）：原查 `extname='pgvector'` 错（实际扩展名 `vector`），故 full
  报告 `pgvector_version="not_installed"` 不可信；服务器实测 `vector 0.8.5`。已修查询；**不覆写已提交的
  full 原始证据**，仅 README §8 审计订正。不影响 SQuAD/EM-F1/截断结论。
- **定点诊断**（新脚本 `code/scripts/baselines/squad_truncation_diagnostic.py`，`9aefeba`）：对失败行
  `572700c8…` 的归档 prompt，direct vLLM + DuckDB `ai_try_complete` × cap{64,128,256} × 3 重复，cache/retry
  off，并用 `ai_completion_request_json` 证明两路径同请求。**结果：cap=64 孤立重放 3×3 全部 `stop`/46 token/
  文本一致；截断不可复现** → 推翻 `c20240e` README 的「模型 rambling/loop、temp=0 确定性」。
- **正确归因**：全量并发服务下 vLLM 批处理解码在 temp=0 的非确定性偶发把该行推过 64 token，DuckDB-ai
  truncation-as-error 语义硬转成 NULL。这是 DuckDB-ai baseline 在 cap=64 + 全量并发下的**可测量可靠性行为**，
  不是基础设施/数据故障，也不是「非 baseline 缺陷」（这正是要评价的 baseline 行为）。
- **定性**：full zero-error gate 维持 `FAILURE`（不改写为 pass）；baseline 标
  `eligible_with_documented_failure`，进入**失败感知**的系统比较。正式三臂比较对所有 arm 统一报
  success/error/NULL/truncation rate、全 manifest EM/F1（失败行 0 分）、`successful_rows/s`、
  `correct_rows/s`、exactly-once 与完整失败证据；不以 raw rows/s 单独排名。
- **待办**：service-config-hash `49cf2f…` 只是手填字段摘要，正式实验前须归档实际 vLLM 启动命令、模型
  revision、dtype、并行配置、显存比例、环境快照。database-E2E runner 可继续建设。

## 2026-08-05 SQuAD full 10570 gate：fail-closed 触发，cap=64 的 1/10570 截断边界

- 先按 codex 八审补 fail-closed + per-row CSV 加 `server_version`/`pgvector_version` 列
  （`d724edc`）：任何行级 error/NULL → status=failure + 非零退出（full report 仍写出便于审计）；
  pgvector extversion 与 PG 版本一次拉取、CSV + identity 复用。63 gate/redact/scanner 测试通过
  （含 3 个 fail-closed 集成测试）。
- 全量 10570 重跑（`--mode full --strict-attribution --service-config-hash 49cf2f803735b4a4`
  — 真实 hash = sha256(model+max_model_len+2×4090+prefix-cache+vLLM0.25.1)）：**fail-closed FAILURE**。
  exactly-once 10570/10570、workload_integrity verified、三 content hash 一致（2c2301f2…）、
  归因 attributable（request_success_delta==10570，`--strict-attribution` 通过），但 **1 行**（`572700c8…`）
  在 cap=64 下模型生成 >64 token → DuckDB-ai 当 max_tokens error 返回 NULL → fail-closed 触发。
- 失败行参考答案均为 5–7 词（"Europeans who were based in Britain" 等），正确答案远低于 cap；
  是模型在该题（temp 0.0 确定性）rambling/loop 超长，不是 baseline/数据缺陷。整集（扣除该 missing 行）
  EM **80.32%** / F1 **89.36%**（独立复算一致）；avg 5.71 gen tokens/row、prefix-cache hit-rate 0.7446；
  operator-only JCT 88.5s；vLLM 0.25.1、pgvector not_installed（如实）。
- **判断**：cap=64 对 SQuAD dev 的覆盖率 10569/10570（≈0.0095% 截断），是 DuckDB-ai
  truncation-as-error 语义的真实边界。**按协议不抬 cap 去「碰巧通过」**。codex 的「0 error/NULL」过门
  标准未达 → full gate 标 FAILURE，但能力（质量/归因/完整性）基本成立。
- **待裁决**：(a) 接受 1/10570 边界、full 标为「capability with 1 documented truncation」；
  (b) 单独诊断该行（更高 cap 单跑确认长度问题）；(c) 其它。

## 2026-08-05 SQuAD capability gate v4：codex 第七轮修复后 canonical 重跑

- 用 codex `735751b`（SQuAD-normalize 分桶 + `sample_manifest.jsonl` + /version + 脱敏修复）在服务器重跑
  256 行门禁，产出去 v4（取代 v3 的 canonical 样本）。
- **服务器同步**：`fd4f8bf..735751b` 的 ff-only pull 被服务器上未跟踪的 v3 证据目录（生成后未在服务器提交）
  阻塞；精确移除冲突的未跟踪 v3（已由 `4dca4fa` 提交 canonical 版）+ 废弃 v4（旧码首跑）后拉取成功。
- **结果**（`feasibility/results/squad_capability_256_v4_20260805/`，gate @ `735751b`）：256/256 成功、
  0 error/NULL/max_tokens；EM **81.640625%** / F1 **89.82133685%**（209/256）。sample_hash
  `d0e0e987…` **≠ v3** `b154c46a…`（SQuAD-normalize 分桶改变样本）。workload_integrity=verified、
  attribution=attributable（运行前后 idle，request_success_delta==256）、vLLM **0.25.1**（/version 修复后
  首次拿到）、avg 5.62 gen tokens/row、prefix-cache hit-rate 0.3619；operator-only JCT 4.209s；command 已脱敏。
- **独立复算**：从 `per_row_evidence.csv` 经共享 `squad_quality_metrics` 重算 EM/F1 = 报告值（完全一致）。
- v4 是当前 canonical 256 单臂样本；v3 保留作历史。下一步：① `--mode full` 全 10570；② database-E2E
  顶层 runner（结构性缺口）；③ 三臂正式对比。

## 2026-08-05 SQuAD capability gate 七审：v3 证据边界纠正与门禁复算能力补齐

- 独立复算 v3 `per_row_evidence.csv`：256 个唯一 source id，EM 207/256 = 80.859375%、
  token-F1 89.861139%，与 `report.json` 完全一致；相关 95 个 gate/redact/adapter/metrics/importer
  测试与 28 个 scenario-runner 测试通过。
- 发现 v3 仍有两个审计缺口：分桶 docstring/README 声称 “normalized token count”，实现却是
  `answer.split()`；目录也未归档 prompt-bearing sample manifest，故 `sample_content_hash` 不能只靠
  已提交文件独立复算。v3 保留为有效单臂 capability evidence，但不再称最终 canonical sample。
- 修正版门禁改用共享 SQuAD official normalize 后的多答案最大词数，输出
  `sample_manifest.jsonl`（id/prompt/references），并修复 vLLM `/version` 根路径双斜杠、`--force`
  旧成功/新失败证据混存、逐行/PG 错误脱敏。修正版须先重跑 256 行门禁，才允许启动 full 10570。

## 2026-08-05 secret scanner 全仓自检误报修复

- 在最新 `main` 复跑 `scan_git_secrets.py --all` 时发现扫描器源码和 6 条 detector 单测 fixture
  被自身判为 7 个 violation，故此前“全仓 0 violation”无法按公开命令复现。
- 在 `secret_scan_baseline.txt` 增加 7 条**精确到 fixture 行**的正则，不按整个测试路径放行；不同 token、
  私钥或外部 credential URL 仍会阻断。修复后须同时满足 scanner 单测与全仓扫描。
- 复验：全仓 3315 个 tracked files 为 0 violation（14 次 baseline suppression，含历史 4 条、
  7 条 fixture 以及 baseline 文件中 3 条自匹配），scanner 相关测试随本轮 111 个测试全部通过。

## 2026-08-05 新增 Git 隐私数据禁令 + 高精度 secret scanner

- 用户要求：api key、服务器 IP/host、口令、私钥等隐私数据禁止提交进 Git，并写入项目规则。
- **审计**（全仓 + `git log --all -S` pickaxe）：真实密钥确认**从未进仓库**——SSH 密码、HF token、
  真实主机 `connect.bjb1.seetacloud.com` 均无命中；`.gitignore` 已覆盖 `*.env`/`*.env.local`。
  api-key 在 SQuAD gate 一直是本地默认 `EMPTY`（非真 key），且新 `redact` 模块把任何 `--api-key`→`***`。
- **规则**：写入根 `AGENTS.md` §10（CLAUDE.md `@AGENTS.md` 自动同步，并在自身 Git 规则加了一行指针）。
  禁止提交 key/token/外部 IP-host/非 localhost 口令/私钥/`sshpass -p <pw>`；新代码连接串用环境变量引用；
  evidence 经 `src/baselines/common/redact.py` 脱敏；commit 前跑 scanner。
- **落地**：`code/scripts/environment/scan_git_secrets.py`（高精度：私钥 + hf_/sk-/ghp_/github_pat_/xox*/AIza token
  + `sshpass -p` + 外部 `user:pw@<真实 TLD 或 IPv4 host>`；localhost 任意凭据放行、模板 host 放行、
  example/fake host 放行）+ `code/scripts/environment/secret_scan_baseline.txt`（reviewed 误报 allowlist）
  + `.githooks/pre-commit`（一次性 `git config core.hooksPath .githooks` 启用）+ 10 个单测。
- **历史 `postgres:postgres@localhost`（60+ 处）不批量改写**：公开 PostgreSQL 默认、只连 localhost、非外部凭据，
  按"没泄漏就不动"原则保留；scanner 放行该本地默认。新文件仍优先用 `$DATABASE_URL` 引用。
- 唯一非默认发现：`experiments/results/rc1_data_organization/**/raw/**/requests.csv` 里 4 处
  LLM 生成的 YAML 示例 `user:password@ips-backend-db-...ondigitalocean.com`（占位符凭据 + 第三方 host，
  非我们的基础设施、非真实口令）→ 记入 baseline 放行，不修改证据。
- 全仓验证：3310 文件 0 violation（4 baseline-suppressed）。

## 2026-08-05 SQuAD capability gate v3：codex 第六轮 review 全修 + 可归因重跑

- codex 对 v2（`f82de93`）SQuAD 256 行 DuckDB-ai capability gate 提了第六轮 11 个问题：
  report 泄漏连接凭据、full 模式不验证 10570 行、workload hash 非 importer canonical、
  vLLM counter 不可归因、identity 缺 prefix-cache/vLLM version/GPU、分桶只用 answers[0]、
  比例配额非 largest-remainder、sample hash 裸 ID 拼接、exactly-once 非集合比较、
  output_len 误读为 token、失败运行不归档。
- 全部修复（`51b92f0`）：抽出共享 `src/baselines/common/redact.py`
  （`redact_argument_list`/`redact_database_url`/`redact_text`，gate 与 `run_ai_operator_scenarios`
  共用，消除第三份拷贝）；full 模式 + 两种模式都跑的 workload 完整性 fail-closed（10570 行 +
  unique doc_id/source_example_id + 非空 reference_answers + canonical content hash 对齐 importer）；
  结构化 JSON-per-row SHA256（与 importer `compute_content_hash` 单测钉死一致）；vLLM counter 归因
  门禁（endpoint 运行前后 idle + scrape 非空 + counter 单调 + request_success_delta==requests_sent）；
  max-答案分桶 + largest-remainder 配额；full-set exactly-once；`output_chars`；失败结构化归档 +
  非零退出；identity 扩充（prefix-cache、vLLM version、service config hash、GPU、metrics 状态）。
- 4 审查员对抗审 workflow 又抓出 2 个 blocker/major（已提交 report.json 仍含 `postgres:postgres`；
  failure_report 的 sanitized_error/traceback 未脱敏）+ 若干 minor（URL flag 泛化、`_vllm_version`
  root 路径、gauge-missing 检测、reference 空串收紧、死参数、failure identity 充实）—— 全修。
- 重跑（`fd4f8bf` 后，`squad_capability_256_v3_20260805/`）：256/256 成功、0 error/NULL/max_tokens，
  EM 80.86% / F1 89.86%（v3 抽样用 max-答案分桶 + largest-remainder，与 v2 样本不同，故 EM 高于
  v2 的 75.39%）；`workload_integrity=verified`（workload_content_hash == importer 2c2301f2…）；
  `attribution=attributable`（运行前后 idle，request_success_delta==256）；avg 5.33 gen tokens/row、
  prefix-cache hit-rate 0.3589；operator-only JCT 4.26s；command 已脱敏。
- v2 report.json 命令字段的 `postgres:postgres` 已单独脱敏（`fd4f8bf`，evidence 不变）；v2 保留为
  过渡期有效 evidence，被 v3 取代。`postgres` 是本地默认开发口令、仓库私有，未做 history 重写。
- 结论：DuckDB-ai 单臂已具备进入 bounded-output 正式三臂对比的前提。下一步按协议 §5：
  （可选）full 10570 → database-E2E 顶层 runner（结构性缺口）→ 三臂正式对比。

## 2026-08-05 正式 vLLM 性能主轨统一为 prefix cache-on

- 双 4090 cost-profile pilot/formal 当前入口升级为 cache-on，并使用新 experiment ID；
  cache-off 仅保留为独立机制消融，不再定义主 baseline。
- profiler CSV 新增 `service_prefix_caching`；scenario loader 交叉校验它与 manifest
  `service_metadata.prefix_caching`，live runner 继续核对真实 vLLM 进程参数。
- cost estimator 将 cache 状态加入 decision-context 身份，禁止把执行后才能观测的
  hit rate 输入同一次 pre-execution 预测；formal 额外审计 query/hit delta 合法性。
- 提交后 AutoDL 真实门禁在 `2b7da6c` 上完成 2/2、0 incident：每 run 512/512
  exactly-once、双 endpoint、shared Ray，CSV 均为 cache enabled，hit rate
  33.69%–34.51%，local-Ray 启动计数 0。首次 PostgreSQL 未启动的失败门禁单独保留。
- 正式计划示例输出目录改为全新 `formal_v2_cache_on` 路径；旧 2026-08-04 无效运行目录
  只保留事故证据，禁止 resume、覆盖或与有效重跑结果混合。

## 2026-08-05 双 4090 cost-profile formal 无效性审计与运行互斥修复

- 复核服务器两套各 320 行的 cost-profile 输出：行数和子进程状态虽然完整，但两个
  launcher 几乎全程重叠，共同竞争同一 vLLM/GPU；两份 manifest 还记录了空
  `--ray-address`，640/640 子运行日志均显示启动 local Ray。两套数据整体排除，不运行
  CE0–CE6，不挑选局部结果。
- `runner_lease` 新增 artifact-root host-scope 互斥，阻止不同输出目录的 runner 并发；
  scenario config loader 新增显式空 endpoint/model/database/metrics/Ray 参数门禁。
- 新增七步事故报告和紧凑哈希证据；正式计划增加“单 host runner + 共享 Ray + local-Ray
  启动计数为 0”门禁。修复后的最小 gate 通过前不启动下一轮 320-run。
- 复核 ResNet18 vendor-code parity：HF ImageNet token/许可不是 upstream S3 parquet
  workload 的准备证明；当前 Daft 0.6.2 venv 实际搭配 Ray 2.56.1，不符合冻结的
  Ray 2.49.2，服务器也未保留三份 pin 文件。状态修正为 `blocked-before-gate`，先验证
  exact object 带宽、版本和 SHA；必要时只镜像相同 upstream objects，不换数据语义。

## 2026-08-04 多机器自动识别与按签名校准

- `manage_environment.py check` 不再要求手工选择 profile：自动采集 CPU、GPU 型号/显存、
  driver，并优先匹配双 4090/单 5070 专用合同；其他 Linux NVIDIA GPU 使用保守通用
  profile。报告写入匿名稳定 machine ID 与 `automatic/explicit` 选择来源。
- 每台机器保留自己的仓库外 `runtime.env`；CLI 支持显式路径、
  `AI_OPERATOR_ENV_FILE` 和 `~/.config/ai-operator/runtime.env` 的确定性查找顺序。
- 参数自适应边界明确为“运行签名 + 稳态 scale ramp + 短校准 + 冻结复用”：batch/K/actor/active-work
  不能由 GPU 名称推导，使用现有约 97% 平台规则选满足正确性/SLO 的最小饱和点；机器、
  实际 GPU 拓扑、模型/服务、协议或 workload 分布变化会使旧选择失效，formal 禁止在线调参。
- 新增 profile 自动选择/通用回退测试；环境专项共 8 项通过。

## 2026-08-04 Agent 跨机器迁移强制路由

- 根 `AGENTS.md` 新增自动入口规则：任何新机器/容器、GPU/云切换、缺包、缺模型或缺
  数据任务，必须先读 `deploy/runtime/{AGENTS,README}.md`，运行只读 preflight，再做
  显式安装/下载、importer、correctness gate 与本机校准。
- `CLAUDE.md` 显式导入 `deploy/runtime/AGENTS.md` 并重复关键禁止项，使 Codex、Claude
  Code 和远端 agent 不依赖是否主动发现 README；禁止混装 driver/vLLM、绕过许可资产
  和把其他机器的最优参数直接用于正式实验。

## 2026-08-04 跨机器运行时、依赖与资产合同

- 新增 `deploy/runtime/`：用 machine profile 区分 AutoDL 双 4090 与本地单 5070
  Linux/WSL2，用统一资产清单维护 Python 能力、模型和数据集；LightGBM 作为通用
  `ml-estimators` 能力成员，不为代价估计单独建设部署架构。
- 新增 `manage_environment.py` 的只读 `check`、显式 `install-python` 与单资产
  `download`。公共 HTTP 文件支持 `.partial` 恢复，Hugging Face 使用 snapshot；
  ImageNet 等受许可资产 fail closed。下载与 PostgreSQL workload 导入保持分层。
- 图像矩阵与文本原生 baseline 配置统一支持严格 `${ENV_VAR}` 展开，unset 立即失败；
  当前图像模板去除 CLIP 绝对路径，AutoDL env 抽出 project/artifact/model/data/venv
  五个根目录。模型下载加速改为 AutoDL 可选能力，其他机器可走正常网络。
- 迁移合同只保证可运行与可审计，不搬运双 4090 的性能最优配置；新 GPU/模型仍须做
  correctness gate 与静态容量校准，MFU 峰值口径须按 GPU 和精度重新确认。

## 2026-08-04 image 重跑（schema-v12）+ 代价估计排序层补齐 + Track B baseline 可行性

- **schema-v12 三臂门禁（256 行）**：用 codex 的 single-writer matrix runner
  (`run_image_clip_matrix.py` + lease) 跑 3 臂 ×1 warmup ×1 formal，0 incident，
  验证 schema-v12 derived 字段（`joules_per_1k_images` / `gpu_seconds_per_image` /
  `images_per_cpu_core_second` / `first_output_fraction_of_e2e`）+ manifest
  `cross_scale_comparison_semantics` / `metric_definitions` 在真实 GPU 正确产出。
  单写 lease 根治了之前的并发双写 bug。
- **3 臂 12K 一致性重跑**：supersede 受污染双写 run（已排除出 git）。12 个 run
  （3 warmup + 9 formal）均 exactly-once；Daft ~65s@12K（约 185 img/s，/dev/shm
  干净下无 OutOfDisk），fast arm setup-dominated（`min_steady=0`，结构诊断非稳态排名）。
- **2 臂 60K matched-resource schema-v12 重跑**：16 个 run（4 warmup + 12 formal）
  均 exactly-once，formal E2E 最低 73.52s。project 在 matched CPU 两档的 JCT 分别低
  10.0%/18.5%，与 step-8 的 12.8%/15.1% 同向；四个对照的
  观测范围是 10.0%–18.5%，不能把中点包装成更窄置信区间。schema-v12 per-image
  指标显示 sampled energy 113 vs 121 J/1k@cpu16、首个完整 batch 24.2 vs 43.6s，
  GPU 仍饥饿 6–9%。raw 进 `experiments/results/image_ai_embed_operator_formal_20260803/raw/`。
- **raw 归档修复**：`9c7c5fb` 中两份 schema-v12 CSV 在本地保存时各被附加 3 个
  控制字节（`11 72 13`）；服务器权威 `runs.csv` 不含该尾部。已从服务器原件覆盖，
  正常记录和统计值未变；正式报告同时纠正“12/12 formal”与过窄 headline。
- **代价估计排序层补齐**：重跑 `estimate_operator_cost.py` ×5 seed（schema-v2
  `selection_metrics`）。**验证 Heinrich "精度≠选择"**：ridge 预测层显著优
  （MAE 11.68 vs 29.89s、Spearman 0.677 vs 0），但选择层 `decision_regret` 反而更高
  （22.76% vs 8.37%）——MAE 更低 ≠ 更安全的计划选择器。严谨边界：每 seed 仅 2–5 个
  decision context，selection 指标噪声大；pairwise/Top-K 未实现。填了
  `operator_cost_estimation_20260726` README "排序能力分析待补"。脚本
  `code/scripts/analysis/compare_cost_estimators.py`（复用 `estimate()`，未改原代码）。
- **Track B baseline 可行性**（6-reader workflow，691k tokens）：22 个外部 baseline
  分级——ready/高价值 = Daft 官方 ResNet18 vendor-code parity（pin `3f5bdd17`）+
  Apache Doris EMBED（唯一覆盖图像文件引用的自托管 DB）；needs_prep = SemBench/
  LOTUS/DuckDB-ai/ClickHouse/Spark；reference_only/blocked = PolarDB（闭源但同栈可复现
  开源臂）/Hologres/Galois/OceanBase（AutoDL seccomp）。硬件不同时只比 scaling/
  质量/rank 指标，不比绝对墙钟（PolarDB 与 Ray 官方对 Daft/Ray Data 排名方向相反即证据）。

## 2026-08-04 文献 PDF 批量下载 + REFERENCE_INDEX 更新

- 下载 15 篇此前缺失 PDF 的文献（knowledge_hub §3.8 新增 + REFERENCE_INDEX 已记录但实际缺失）：
  LOTUS、VTC、Llumnix、Abacus、Palimpzest、SemBench、FairServe、DLPM、Autellix、Chiron、
  TIE、Past-Future Scheduler、JITServe、Beyond Prediction (UniBoost)、FastServe (NSDI 2026)
- 全部通过 arXiv OA 渠道下载，经过 `%PDF` 签名 + pypdf 页数验证
- 更新 `research/reference/REFERENCE_INDEX.md`：PDF 总数 88（去重 87），精读笔记 49
- 仍缺失：Learned Query Optimizer (Zhu et al.) SIGMOD 2024（ACM 付费墙）

## 2026-08-04 AI 算子论文与数据库产品场景矩阵补全

- 在 `research/evaluation_metrics_survey_20260731.md` §9.1 为 LOTUS、Galois、
  GaussML、Smart、SmartLite、InferDB、LEADS、NeurDB、Cortex AISQL、Palimpzest、
  Abacus 与 SemBench 补充论文实际数据集、数据模态、算子执行方式和优化对象，避免脱离
  workload 只罗列指标或迁移 speedup。
- 同节补充 Clipper、Orca、vLLM、Sarathi-Serve、DistServe、SGLang、VTC，以及
  Heinrich learned-cost-model、GRACEFUL、COSTREAM、CONCERTO 的真实 trace/数据集、
  部署层级和指标，明确 serving/cost-estimation 论文只提供相应层的机制与评价合同，
  不能替代数据库 source→AI operator→sink 端到端 baseline。
- 将数据库产品按“可自托管同机对照、托管云产品、向量 read-side”拆分，逐项说明
  Doris、ClickHouse、StarRocks、OceanBase、Oracle、Db2、SQL Server、DuckDB 扩展、
  pgai、PostgresML，以及 PolarDB、Hologres、AnalyticDB、Snowflake、BigQuery、
  Databricks 等系统的输入→AI 算子→输出场景、scheduler/endpoint 边界和正确比较方式。
- 明确向量 ANN benchmark 只评价 embedding 生成后的检索链路；云厂商产品只比较可观察
  的质量—成本—时间、失败和配额；只有同机同模型同 source/sink 的自托管系统才进入
  raw performance 主排名。
- `experiments/plans/baseline_reference.md` 保持 baseline 身份、可安装性和运行合同的
  权威入口，并新增到上述场景矩阵的导航，避免两处重复维护易漂移的产品描述。

## 2026-08-04 数据库厂商 AI 算子可安装性与 baseline 扩展

- 使用厂商官方文档、官方仓库和 release notes 复核本地/云端数据库 AI 算子，按
  “数据库执行器是否拥有模型调用调度、能否本地安装、能否复用同一 OpenAI-compatible
  vLLM endpoint”三项拆分，而不是把向量类型、客户端 SDK 或自写 UDF 混称 AI 算子。
- 将可本地候选补入 `experiments/plans/baseline_reference.md`：首批为 Apache Doris
  4.1.3、ClickHouse 26.6、StarRocks 4.1.1+；Oracle AI Database 26ai Free、Db2
  12.1.5 Community 和 SQL Server 2025 分别补充文本生成/embedding 对照；OceanBase
  保留为需要 systemd VM 或特权容器的正式文本候选。
- 将 DuckDB `ai` 明确标为社区扩展，将已归档 pgai 标为历史扩展，将 PostgresML 标为
  in-database inference 机制对照；三者均不得冒充数据库 core 或与外部 vLLM 同机制排名。
- 拆开同品牌不同产品线：用户给出的 PolarDB-X RPM 可安装但未验证一等 AI SQL
  算子；有 AI 能力的是 PolarDB PostgreSQL Polar_AI 和 Lakebase Daft on Ray，属于
  云/商业产品，不能写成由该 RPM 提供。
- 新增 Snowflake、BigQuery、Databricks、Hologres、AnalyticDB、腾讯云数据库、
  SingleStore、HeatWave、AWS/Azure 数据库、MotherDuck、SAP HANA/Teradata 等云端
  capability 表，并规定只比较 E2E、质量、成本、失败/配额；不与本地 2×4090 raw time
  或内部 GPU/MFU 混排。
- 明确排除 self-managed TiDB/MariaDB/普通 MySQL 的 vector-only 能力、MatrixOne
  Python glue、openGauss 经典 DB4AI，以及证据不足的厂商；新增安装、一行协议、
  exactly-once/调用计数、缓存四道门禁和统一 `scheduler_owner` 审计合同。

## 2026-08-04 输出长度不确定性与决策导向代价估计文献补充

- 将 SFS、TIE、Past-Future、JITServe、Beyond Prediction 与 FastServe 按“动态 TTFT 估计、重尾输出长度分布、未来显存/SLA、渐进式 remaining-work 修正、prediction-free tail 风险、prediction-light 对照”补入 `research/knowledge_hub.md` 和 `research/ai_operator_literature_inventory.md`；FastServe 题录从旧 arXiv 状态更新为 NSDI 2026。
- 将 μ-Serve 按正式 USENIX ATC 2024 定位补入 GPU serving 能耗/资源成本文献，而非误归为输出长度预测；它支撑 GPU frequency scaling、功耗与 SLO attainment 的评价口径。
- 明确这些 serving 工作多数需要修改内部 scheduler，本项目固定 vLLM 为黑盒，只迁移 admission-time 估计、不确定性表示、评价指标与静态回退合同；不得把它们写成已经实现的直接 baseline。
- 在 `experiments/plans/baseline_reference.md` 增补代价估计七级 baseline 和三层晋级指标：点预测/区间、配置排序、下游 decision regret/SLO goodput；要求配置组、时间、workload、长度漂移和 burst 留出。
- 收紧“Daft+Ray 队列可控”的表述：它提高 pre-submit held work 和动作的可观测性，但不消除自然 EOS、continuous batching、KV/cache 与共享负载造成的 service 不确定性；低置信度或 OOD 时必须回退到同上限强静态策略。

## 2026-08-04 AI 算子评价指标与决策导向代价估计调研补充

- 在 `research/evaluation_metrics_survey_20260731.md` 增补按论文与数据库/厂商系统拆分的指标矩阵，覆盖语义算子质量、端到端性能、服务侧 TTFT/TPOT、资源成本、调用次数、可复现性与多 job 公平性，并给出本项目三层公平对比合同。
- 明确跨系统比较必须先对齐算子语义、模型质量、硬件资源、source/sink 和计时边界；闭源厂商仅作方法学与指标覆盖对照，不引用其 wall-time 倍数作性能排名。
- 补充 AI 算子代价估计的四层评价：点预测、不确定性、配置排序和下游决策；把 configuration ranking、oracle regret、SLO goodput 和性能回退率设为比单一 MAPE/R² 更接近研究目标的指标。
- 使用 `idea-evaluator` 审计“Daft+Ray 队列可控使代价估计更方便”的设想。结论为 Accept with Revisions：队列可控提高 pre-submit work 的可观测性、决策可辨识性和干预能力，但不消除自然 EOS、continuous batching、KV/cache、共享负载造成的 endpoint service 不确定性；代价模型应改写为带预测区间的 state-action conditional decision model。
- 登记最小决定性实验：强静态/解析/profile/residual/state-aware/oracle 多臂消融，开环预测与闭环 regret 联合评价，Ray/vLLM 队列迁移审计，受控 submit/hold 干预，以及独立时间、workload、模型/endpoint 泛化留出。

## 2026-07-29 开题答辩 PPT v6 设计规划与反馈修订

- 审阅 `opening_defense_20260720_v5.pptx` 后，确认下一版以 v5 的章节和
  页面为骨架，正文聚焦动机测试、两项策略设计和实验设计；大量阶段性正式
  结果进入答辩备份或讲稿。
- 新增并按用户反馈修订 `opening/slides/opening_defense_v6_design.md`，规划约 31 页正文、
  6 页备份的页面结构、核心架构图语义、内容修正项、同步范围和 QA 门禁。
- official baseline 正文页先讲实验设计；结果只有通过 row/tokenization/
  资源等价、exactly-once、计量口径、规模校准和正式重复门禁后，才模块化
  插入 1–2 页。
- 架构图采用一张高信息密度总体执行架构图、一张提交控制架构图和一张
  Runtime Credit Lifecycle 放大图；数据组织继续使用 v5 的三页机制图组。
  vLLM 保持黑盒，写回保持工程 baseline，completion 时释放
  endpoint-shared request/work credit。
- 当前等待设计复审，尚未修改 v5 或生成 v6 PPTX。后续必须从 v5 复制并使用
  `python-pptx` 增量编辑，不得重跑 `build_ppt.py` 覆盖人工版式。

## 2026-08-02 源码域重构第 0–3 阶段落地

- 在 `codex/code-architecture-refactor` 独立分支实施路径重构，不改策略算法、默认值、
  CLI 参数或 CSV schema。删除 6 个根级 `profile_*` 与 11 个 scheduling 兼容壳，所有
  调用方改到唯一 owning package。
- `src` 顶层功能实现已收进 `data/`、`planning/`、`scheduling/`、`serving/`、
  `modalities/`、`observability/`、`baselines/`、`experiments/` 和 `infrastructure/`；
  scheduling 进一步分成 core/organization/submission_control/endpoint_routing/runtime。
- 文本 baseline 落入 `baselines/text`，图像 native graph 落入 `baselines/image`，共享
  manifest/result/provenance/gate 落入 `baselines/common`；原 `image/` 的非 baseline
  实现迁入 `modalities/image`，防止项目执行代码与 native 对照身份混写。
- 修正原计划的一个边界矛盾：纯 `planning` 不得依赖 Arrow/Daft，因此引擎相关批次
  物化归 `data/materializers`，planning 只保留 cost/packing 决策。
- 新增 AST architecture boundary test，禁止 scheduling 反向依赖 data/modality/engine、
  planning 引入执行引擎、baseline 引入项目 scheduling，并防止已删除旧入口回归。
- 本地 `unittest` 共发现 601 条：路径迁移相关测试无新增失败；当前未通过项仅为本地
  缺 `psycopg`/Daft、macOS 沙箱禁止 Ray 进程枚举的既有环境门槛。服务器关机期间未做
  GPU gate；后续提交已完成 scripts/tests 物理分组和 metrics/backend/shared-vLLM 拆分，
  其余大文件继续逐个处理。

## 2026-08-02 外部多模态 baseline 体系与公开 benchmark 合同

- 明确 Daft/Ray 是项目实现手段而非 baseline 准入条件；外部对照按 AI 算子语义
  选择，拆为同栈官方 runtime、不同栈开源 runtime、数据库外部 endpoint、工业同类
  集成、闭源托管 SQL 和学术语义系统。PolarDB Daft AI Functions 归入同架构家族；
  Snowflake/BigQuery 归入托管产品；OceanBase 只进入文本轨道；SemBench 用于
  LOTUS/Palimpzest/ThalamusDB/BigQuery 的质量—成本—时间比较。
- 记录 Ray 与 PolarDB 官方的 image classification、document embedding、audio
  transcription、video object detection 八组公开 Daft/Ray Data（及 PolarDB Spark）
  数据。两方对 Daft/Ray Data 的排名方向相反，因此厂商 raw time 只作外部证据；正式
  比较冻结为公开 file/object 复现轨道与 PostgreSQL database-operator 轨道，在同一
  模型、数据、输出、物理资源和计时边界下独立校准后运行。
- 当前 COCO/CLIP GPU starvation 和 host-path matrix 仅承担动机、校准和机制归因，
  不替代市场/学术 baseline，也不单独承担项目优越性结论。

## 2026-08-02 图像 staged baseline、资源死锁修复与分类质量轨道

- 5000-image project-Ray 侵入式诊断显示每批 p50 completion 650ms，其中 CPU
  preprocess 316ms、未归因 dependency/queue wait 287ms、host copy 26ms、H2D
  3.5ms、forward 7.0ms；这是“CPU preprocess + framework bubble”为当前木桶、
  PCIe 暂非主瓶颈的初步信号，尚不能替代 R0–R4 正式曲线。
- 同次诊断发现隐藏资源混淆：4 个声明 `num_cpus=1` 的 actor 实际继承 Torch
  intra/inter-op=32/64，host busy 均值约 23.3 cores。Ray CPU 是准入 token 而非
  线程 quota，因此该结果不能称为“4 CPU matched-resource”。图像 runner 升级到
  schema v5，显式配置/记录每 worker Torch 线程；project Ray 在查询前校验实测值，
  正式 matched-resource 默认 1/1，线程容量扫描另列。
- 远端首次 schema v5 gate 暴露图像 runner 的部署缺口：driver 通过本地 `sys.path`
  可导入，但 Ray worker 在没有交互式 `PYTHONPATH` 时无法 import `src`。所有图像 Ray
  arm 改为显式传共享 `ray_runtime_env()`，同时传播项目代码路径和 OMP/MKL 等线程
  合同，避免依赖 shell 隐式状态。
- schema v5 首轮 fail-closed 校验确认 Ray CPU/GPU worker 的 Torch 实测值均为
  `1/1`；同时修正校验器只比较线程字段，不把 GPU `ready()` 返回的模型/进程元数据
  误判为线程不一致。失败尝试未写入 CSV，不属于实验结果。
- 线程收紧后的 5000-image 诊断为 324.4 images/s（隐式 32/64 线程旧诊断为
  333.2，差 -2.6%），host busy 由 23.3 降至 7.83 cores；preprocess/H2D/forward
  p50 分别 344/6.8/7.0ms。线程超卖消耗大量 CPU 却几乎不增吞吐，PCIe 仍非当前
  首要木桶。诊断同时发现 project 的 Daft native source 线程位于 Ray cluster 外；
  schema v6 新增 external CPU，默认配置修正为 Ray 6 + external source 4 = host
  declared total 10，并按该总量做物理超卖门禁。
- 木桶实验继续消除联动变量：schema v7 新增独立 `--source-cpu-threads`，不再强制
  Daft native source runner threads 跟随 preprocess actor 数。后续 CPU actor 容量扫描
  固定 source threads，只改变 preprocess stage；兼容默认仍跟随 `--cpu-workers`。
- 单次 screening：preprocess actor 1/2/4/8 的冷吞吐为 143/210/296/363 images/s；
  source threads 1/2/4/6 为 359/366/368/345，数据源线程不是主要杠杆；active batches
  4/8/16/32/64 为 279/350/375/398/359，32 后吞吐回落且批等待暴涨。16 CPU actor +
  active32 得到当前最佳冷 E2E 11.45s/436.7 images/s；32 actor 虽查询阶段略快，
  setup/first-output 恶化使冷 E2E 降到 318.0 images/s。以上均为 1-run screening，
  不能当 formal headline。
- 为定位 16-actor 点剩余 gap，schema v8 新增 driver `source_next`、Arrow/Python
  materialize、Ray submit 分段；它们缩小候选范围，但仍不冒充 DB 内部或 Ray
  serialization 的硬件级时间。
- 本轮 host-path screening 的报告按“实验设置→实验设计→严谨性自检→原始数据→
  事实/推断/不能声称→课题含义→下一步”七步结构归档；5 组扫描/诊断的 `runs.csv`
  与 16 个逐臂 manifest JSON 已从服务器临时目录复制到
  `motivation/results/gpu/image_host_path_screening_20260802/raw/` 并纳入 Git。
  原始文件归档不提升证据等级：各点仍只有一次，继续标记为 screening。
- 为把 screening 升级为 formal，新增 `run_image_clip_matrix.py` 与 60K project
  静态矩阵模板：固定 seed 交错 8/16 preprocess actors × active16/32，执行每点
  1 warmup + 3 formal，并对 unique rows、exactly-once 和至少 60 秒查询阶段
  fail closed。COCO 导入器新增 ZIP 流式读取，避免同时保留 19GB 压缩包、完整解压
  目录和 PostgreSQL BYTEA 三份数据；事务失败仍完整回滚。
- 首次60K导入被 legacy `PRIMARY KEY(doc_id)` 在 train/val source ID=9 冲突处
  fail closed，事务完整回滚，暴露多 workload 行身份缺口。新增幂等迁移 SQL 将主键
  改为 `(workload_name, doc_id)`；importer 在写入前强制复核该合同。禁止用 split
  专属数字偏移掩盖错误 schema，后续 source/correctness/writeback 均须携带 workload。
- 主键迁移后的60K写入已提交（60,000 distinct、9,341MiB JPEG），但 importer 的
  提交后验证暴露 psycopg3 生命周期 bug：`with conn:` 退出会关闭连接。改为先结束
  metadata 隐式事务，再用 `conn.transaction()` 包围 DELETE+INSERT，使同一连接可在
  commit 后完成行数验证；旧写入未丢失，也未把验证异常误报为回滚成功。
- 60K project 最快点时长探针得到 `operator_e2e=40.53s`、显式 worker setup
  `8.44s`，查询稳态代理仅约 `32.09s`，因此未直接启动不合格 formal。image source
  与五臂 runner 新增 `dataset_passes`，schema v9 分开记录 60K `unique_images`、2
  logical passes 和 120K processed `rows`；pass-qualified execution ID 继续接受
  exactly-once 审计。矩阵 unique 门禁读取真实 unique 字段，禁止重复行虚增数据规模。
- H2D 口径补充到学习材料：batch64 的 host float32 tensor约 38.5MB、device
  float16 tensor约 19.3MB；当前约 7.4ms 是同步 `torch.as_tensor` 阶段 wall，
  不是 PCIe counter。增大总行数只延长稳态，不增加单批传输压力；PCIe 是否值得
  优化仍按 R0/R1/R2 与 pinned/pageable GO/NO-GO 门槛判定。
- 新增 `profile_clip_transfer_ceiling.py`：batch16/64/256 下交错采集 R0
  GPU-resident、R1 pinned FP16 和 R2 read-only pageable FP32 的 ownership copy、
  CUDA-event H2D/forward、同步 wall 与逻辑带宽。该脚本明确标记 synthetic
  diagnostic，不含 PostgreSQL/Daft/Ray queue，不替代 R3/R4 或正式系统比较。
- 远端完成上述 R0/R1/R2 诊断：270/270 raw rows，输出 sum 完全一致、norm error
  ≤5.96e-8。batch64 中位数为 R0 forward 6.86ms；R1 pinned FP16 H2D 0.80ms、
  逻辑24.0GB/s；R2 pageable FP32 ownership copy 20.87ms、H2D/转换4.14ms、
  逻辑9.3GB/s。结果支持“纯 PCIe capacity 暂非首要木桶、host ownership/dtype
  边界需继续做 E2E 消融”，但仍不构成 PCIe NO-GO，七步报告与 raw 已归档到
  `motivation/results/gpu/image_clip_transfer_ceiling_20260802/`。

- 新增 Daft-on-Ray staged 与 Ray Data staged 两个强 baseline；先过 32-row smoke，
  随后在 `c0b5733` 完成 256-row 双卡 resource/correctness gate。两臂均通过
  exactly-once、512d、L2 norm，完整 embedding digest 一致且两卡激活；Ray Data
  记录 4 preprocess + 4 predictor tasks。单次冷启动吞吐不能作为性能排名，下一步才是
  两个 baseline 各自独立校准/formal。紧凑证据见
  `feasibility/results/image_staged_resource_gate_20260802/`。
- Ray Data 第二次门禁复现资源死锁：4 preprocess actor + 2 GPU actor 占满错误声明的
  6 CPU 后，SQL reader 无 slot，0 rows 无法推进。该次运行已中止并标记无效。
- staged resource gate 的 `runs.csv`、Daft manifest 与 Ray Data manifest 已从服务器
  临时目录归档到 `feasibility/results/image_staged_resource_gate_20260802/raw/`；
  45 列 `runs_summary.csv` 仅为读表摘要，原始证据现已随 Git 保存。
- 资源修复升级为通用合同：Ray Data、Daft staged、fused Daft Ray 均显式计算
  source + preprocess（如有）+ model actor CPU；在 `ray.init` 前按进程 CPU affinity
  拒绝物理超卖。CSV/manifest schema v4 记录 host slots、Ray cluster、三段声明和语义。
- 图像 runner 补 CPU core-seconds、内存、disk/network、context switch、GPU seconds、
  images/J、P99 与 Ray Data operator stats；这些是 host 级观测，不能替代 PCIe 硬件
  byte counter，PCIe 归因仍须 CUDA events/Nsight 代表点。
- workload 拆成两条质量轨道：ImageNet/ResNet18 报 top-1/top-5；COCO/CLIP
  multi-label 报 mAP、micro/macro-F1、precision/recall。当前 COCO PostgreSQL 表没有
  annotations/captions，只能做执行与数值等价门禁，不能声称分类准确率/检索 recall。
- 纠正产品 baseline：OceanBase 当前官方/本机确证的是文本 AI_COMPLETE/AI_EMBED/
  RERANK，不冒充图像分类对照；图像产品语义参考为 PolarDB `classify_image` 与
  Snowflake image `AI_CLASSIFY`，闭源且不同硬件时不与本项目 raw time 排名。
- OceanBase CE 4.5.0 在全新独立目录做 2026-08-02 复核，仍于 observer init step
  4/18 报 `prepare_dir_and_create_meta_ failed` / -9100，端口 2881 未监听；没有 B1
  CSV。相同普通容器条件下停止重复尝试，待 privileged/seccomp-unconfined 或 VM。

## 2026-07-31 baseline 同步：直接对比 vs Related Work + 补 OceanBase

- 用户 push：需明确"哪些 baseline 要数字对比、哪些只 Related Work 定位"，并补上
  漏掉的 **OceanBase AI 算子**（项目既定的数据库原生算子产品级 baseline）。
- 校正 scope §10.1 + image_clip plan §7 + msmarco plan §5：
  - **A. 直接 baseline**（同杠杆=执行，必须跑+比数字）：Daft native、**OceanBase
    AI_EMBED**（无 Daft/Ray，DB 原生；B1 门禁已过函数存在性，部署待可部署环境）、
    Ray Data、naive、bounded direct。
  - **B. Related Work**（不同杠杆=语义/计划，只引用+定位，不比数字）：LOTUS /
    Palimpzest / Abacus、Cortex / Oracle（闭源）、Smart / GaussML、SemBench。
- 审稿人"怎么不跟 LOTUS 比"标准答法：LOTUS 优化调用数/语义（不同杠杆，互补），
  本文优化执行调度；实验对比同杠杆执行层 baseline。
- OceanBase 状态：CE 4.5.0 含 AI_COMPLETE/DBMS_AI_SERVICE（B1 门禁过），当前
  AutoDL 容器 observer init 受阻，待特权容器/VM 复跑——见
  `experiments/results/oceanbase_b1_gate_20260731/` + install_runbook。

## 2026-07-31 评估口径校正：数据库 AI 算子论文（执行优化子方向）

- 用户 push：recall@10 跟"执行调度优化"没关系（它是向量检索质量，跟调度正交）；
  应以**数据库 AI 算子论文**（LOTUS/Cortex/GaussML/Smart/Galois/SemBench）为锚，
  不对标 vLLM/Sarathi（serving 内部，非本层）。
- 校正 scope 文档 §10.1：本项目 = 数据库 AI 算子 field 里的**执行优化子方向**，
  与 LOTUS 的语义优化**互补**（同领域不同杠杆）。recall@10 降为**质量门禁**
  （非主指标、非卖点）；性能主指标 = execution time/throughput + 阶段拆解 +
  cost + vs baseline speedup + scaling（6 项，按 LOTUS/GaussML 口径）。
- Baseline 校正：Daft native（关键，PolarDB 同款）+ naive + Ray Data +
  bounded direct；**LOTUS/Palimpzest 不作 baseline**（不同杠杆，仅 Related Work
  定位互补方向）。
- 执行层吞吐/搬运协议：无现成 benchmark（厂商全闭源），§7.5 自定，自定本身是贡献。

## 2026-07-31 workload 纠正：CLIP 回升首个，MS MARCO 降级（数据搬运判据）

- 学长判据校正：数据搬运瓶颈有两段——送 vLLM（拥挤）+ **DB 读出来 / CPU 搬到 GPU**
  （机会）。当前 prompt **文本每行 ~1KB、搬运太轻，瓶颈不显现**。workload 必须让
  "DB 读 + CPU→GPU 搬运"重到能显现。
- 据此**推翻上一条"MS MARCO 首选"**：MS MARCO 仍是文本，token ID 紧凑（~1KB/行），
  搬运轻，不满足判据 → **降级为"文本轻对照"**（仅证明文本下不显现）。
- **图像 CLIP 回升为首个 workload**：每行 CPU→GPU 搬运 ~600KB（文本 ~600×）+
  JPEG decode/resize 重，DB 读 + 搬运瓶颈能显现。与冷启动（机制，parked）无关。
- **benchmark 三层讲清**（scope §10.1）：① 数据集 ImageNet/COCO（公开经典）；
  ② 质量协议 ANN-benchmarks recall@10（CCF 认可）；③ 吞吐/搬运协议——无现成
  benchmark（厂商全闭源），项目 §7.5 自定（自定本身是贡献）。可引 BigVectorBench
  image 切片 + ANN-benchmarks。
- 同步翻转所有索引：scope §5/§10、image_clip plan（解冻回升）、msmarco plan
  （降级对照）、experiments/README、experiments/plans/README §〇、data/README、
  overview/current_direction_and_plan、PROJECT_INDEX。题目/官方方向不变。

## 2026-07-31 MS MARCO workload 设计 + 执行计划（首个锁定 workload）

- 新增 `experiments/plans/msmarco_embedding_workload_20260731.md`——首个
  锁定的 workload（当务之急，机制无关）。MS MARCO Passage 8.8M 批 embedding，
  作 BigVectorBench（VLDB'25）的 text 切片入口。
- 选定理由：被认可（MS MARCO leaderboard + BigVectorBench text 切片）+ fit
  18G + 大数据（8.8M 段）+ 异构（CPU tokenize vs GPU embed）+ 复用现有文本
  管线 + 机制无关（exercise 痛点①③，冷启动②解封后可升级多模态切片）。
- 设计要点：BGE-base-en-v1.5（1024d）→ pgvector；embedding 走独立 FastAPI
  endpoint（BGE 非 vLLM），复用项目 Ray→HTTP 机械。主 bar = 项目动态 vs 项目
  静态 >5%（不是 vs Daft Native）。Go/No-Go 门禁 = CPU tokenize/GPU embed
  时间比 >0.3。指标含 recall@10（ANN-benchmarks 协议）。
- 执行计划 11 步：下数据 → §6 go/no-go 画像 → 建 endpoint → 导入 PG → smoke
  → baseline（bounded + 项目静态）→ ours 动态 → formal 3 repeats → 决策点
  （动态>静态？）→ 扩 image CLIP → 远期 audio+冷启动。
- 升级路径：benchmark 名始终 BigVectorBench，text 切片 → image → audio，
  场景认可度一路保持。

## 2026-07-31 方向 reframe：数据库↔GPU 经 Daft 桥接（学长反馈 + 三痛点核实）

- 学长完整反馈把场景 reframe 成"数据库↔GPU 经 Daft 桥接、GPU 侧算子多样
  （不止 vLLM）、大数据量、流式 pipeline"，明确不能用 ShareGPT 这种对话式
  workload。记录到 `notes/communication_notes.md` §5.1.1。
- 新增 `research/daft_db_gpu_bridge_direction_scope_20260731.md`（academic-pipeline
  Stage 1 scoped 输出）：工作流 `w6xclfb0g` 用 Daft 源码一手核实学长三痛点
  全部真实——① `@daft.cls(gpus=N)` 写死（`daft/udf/__init__.py` L360-410）、
  ② 多算子冷启动 Daft 完全不做（无 model garden/swap/LRU）、③ 流式 dynamic
  batching 是 model-service-blind。可防御性排序 ② >> ① > ③。
- 核心发现：**可防御界面 = online vs offline 分界**——所有 scoop 先验
  （ServerlessLLM/Llumnix/AlpaServe/Clockwork/INFaaS/Chiron/Autellix/TORTA +
  Daft v0.6.9 prefix + llm-d/Preble）都是 online serving，结构性无法利用批
  dataflow 在 plan 阶段已知的两份 foreknowledge（算子 DAG + 各算子数据量）。
  这翻转了之前 §5.5 的 partially-scooped 判定——批 dataflow + foreknowledge
  + 多样异构算子调度是结构性空白。
- Fatal-flaw：2×4090(48G HBM) + 18G 盘下，模型 garden ≤18G < 48G HBM，自然
  条件下永远不触发冷启动——冷启动 regime 需"约束预算"构造或扩盘。
- 用户决定：方向 validate；**当务之急 = 锁 benchmark/workload**（学长原则：
  场景先被认可，机制后说；与冷启动无关，之前文档把优先级写反了已修正）；
  冷启动（机制候选）parked，后面做；题目精修暂缓。
- scope 文档 §10 推荐首个 workload = MS MARCO Passage 8.8M 批 embedding
  （被认可 + fit 18G + 大数据 + CPU tokenize vs GPU embed 异构 + 复用文本管线 +
  机制无关）。锁定后即可开跑，不必等冷启动/导师定机制。
- 同步：`experiments/plans/image_clip_workload_lock_20260731.md` 状态降级
  （CLIP 从旗舰降为 model garden 里一个算子/模态探针，设计冻结）。

## 2026-07-31 评估指标体系调研（文献 + 数据库厂商）

- 新增 `research/evaluation_metrics_survey_20260731.md`：以
  `nature-academic-search` + `deep-research` 工作流调研 AI 算子/推理服务文献
  与数据库厂商/标准基准的评估指标，按 10 类归目并对照项目现有指标做 gap
  分析。同步更新 `research/README.md`、`research/knowledge_hub.md` §9、
  `PROJECT_INDEX.md` 入口。
- 结论：throughput / 尾延迟 / SLO attainment / MFU+KV 利用率 / 能耗 /
  Jain+max-JCT 公平 / exactly-once 审计 / 控制 trace 八大类项目已覆盖或优于
  多数文献；细分缺口见该文件 §5。
- P0 缺口三条均为"vLLM 已暴露信号、采集端未落字段或折叠分布"，已亲自核实
  代码：`code/src/metrics.py:433-437` 仅采 prefill/decode 均值，未采
  `time_to_first_token`/`inter_token_latency` 分位；全文无 `prefix_cache`
  Counter；`code/src/baselines/ceilings/vllm_bench.py` 当时把 `ttfts+itls` 折叠
  成单条 e2e。补采改动集中在 metrics.py 与 vllm_bench.py，不触策略代码。
- 落点：prefix cache hit rate 直接服务当前 prefix 路由结论的隔离消融；
  TTFT/ITL 分位使 service_p99 的 prefill/decode 可解释；登记到
  `experiments/plans/experiment_status_and_gaps.md` 指标缺口区（待补）。
- 文档扩展（同日）：该调研文件追加**附录 A**（workload/数据集五类清单 +
  AI_COMPLETE 可用性判定——多数 SemBench/LOTUS 任务是 filter/classify 短输出，
  非 AI_COMPLETE；只有 `map` 形态匹配）和**附录 B**（7 家数据库厂商 AI 算子
  测试方法论：逐家怎么测 + 跨厂商共识 + 17 条项目启示 + PolarDB Lakebase
  同栈专项）。
- **PolarDB Lakebase 同栈核查**（用户提示 + 专项 agent 核实）：PolarDB
  Lakebase 集成**开源 Eventual-Inc/Daft on Ray**（非 fork），内置
  embed/classify/prompt——是迄今最贴近本项目技术栈的工业产品（相关度 4.5/5），
  强化工业正当性；但其卖点（异构调度/背压/util 60→80%）逐条对应项目方向，
  **新颖性门槛因此拉高**——项目不能把"Daft on Ray 异构调度+背压"当新颖性。
  新颖性边界切清：PolarDB 做通用数据流 backpressure，**不观测 vLLM 内部状态**
  （KV/prefix/queue）；项目能占的切片 = 模型服务状态感知请求成形 + 闭源产品
  未公开的上游调度开放消融。scoop 待确认（未见研究论文，但未穷尽学术检索）。
  PolarDB 命名陷阱：无 `AI_COMPLETE`（Snowflake 命名），等价物是 `polar_ai.*`
  + Daft `prompt()`。**题目不变。**

## 2026-07-30 双协议 baseline 与 feeding-first 门禁

- 重新定义 baseline 目标：vLLM Bench 是同机服务上限参照，项目不以超过它为
  目标；正式贡献必须先使上游路径达到同协议 bounded client 至少 95%，再与
  冻结的最佳静态配置比较 operator/database E2E、JCT、tokens/s、P99/SLO、
  active work 和多 job fairness。
- 保留原始 multi-prompt Completions 作为机制主线，并新增同协议、无 Ray、
  持久异步 fixed-row strong baseline；Chat 作为 vLLM Bench、Daft/Ray、
  OceanBase 等产品/官方 runtime 的兼容轨道。两个协议只做协议内排名，禁止
  用 Completions 数值直接声称超过 Chat baseline。
- 同一 512 行 Chat manifest 的远端复核显示：vLLM Bench C256 为
  11.931s/15,351 tokens/s，bounded Chat C256 为 12.569s/14,532 tokens/s，
  project 最佳已测约 31.227s/5,884 tokens/s；K256 反而退化到
  41.053s/4,592 tokens/s。因此当前是 feeding/transport 未过门禁，不是
  token-budget、动态 K 或 flush 策略的正式负结论。
- project completion actor 新增每 actor 一个 bounded persistent
  `httpx.AsyncClient`。Completions 保留一个 HTTP body 多个完整 prompt；
  Chat 使用 actor 内 async dispatch，每行仍是一条完整请求。新增 Chat
  transport/actor-shape 和 Completions fixed-row feeding 配置，门禁通过前
  禁止扩大策略网格。
- `_v3` 多 job 运行在 4-job warm-up 因 Ray worker/OpenBLAS 线程资源耗尽
  终止，且旧 trace writer 对缺失结果 `.get` 遮蔽根因；该批只有 warm-up，
  不作为 formal 结果。统一 runtime env 现限制 OMP/OpenBLAS/MKL/NumExpr 为
  单线程，失败 trace 保留 lifecycle error 后显式终止。
- f203257 再次确认 OMP 限制可使 j2 通过，但 j4 `ray_task` 仍扩张到 200+
  worker，并在只读 `vm.max_map_count=65530` 的 AutoDL 容器触发 raylet
  `SIGABRT`。共享矩阵改为固定 async Ray actor pool；loader 在外部工作前拒绝
  4-job `ray_task`。ec9b19e 的 j4 gate 在相同 VMA 容器完成
  independent/partition/shared-DRR 三臂、每臂 4×64 行、0 actor failure；
  默认 formal 因而恢复 j1/j2/j4，j4-only 模板保留作故障隔离。
- profiler 实现继续归入 `code/src/profiling/`；主入口已直接导入子包，根级
  `profile_*.py` 只作兼容层。baseline direct adapters 归入
  `code/src/baselines/`，避免策略代码和对照实现相互依赖。
- 文档明确区分容量 calibration、held-out 上冻结的最佳静态 baseline、
  per-workload static oracle 和 dynamic policy；动态策略允许一次安全边界
  校准，但不能针对每个 workload 人工精调。token-budget 实验在固定 active
  work 下扫描完整曲线，证明预算并非越大越好后才评价动态控制。
- 远端独立 worktree 的 524/524 测试、ruff、compileall 和三项 512 行真实
  smoke 通过。multi-prompt fixed16 的 project model-request wall 为
  11.164s、同协议 bounded 为 10.943s（约 97.8% capacity）；Chat async
  K256 为 12.552s，与 bounded Chat C256 12.569s 基本重合，而 K64 为
  23.464s。完整 project E2E 分别为 14.211s/13.916s，说明提交层已接近上限，
  但 PostgreSQL/Daft/编排开销仍需单独优化和报告。该证据仅是单次 smoke，
  正式晋级仍需 1 warm-up + 3 repeats。
- runs.csv 新增 `model_request_tokens_per_s` 与 `operator_tokens_per_s`；
  既有 `tokens_per_s` 继续明确表示完整 E2E 吞吐，避免把 source/organize
  时间混成 feeding 缺口。
- 后续策略模板不再回退到 threaded `urllib`：token-budget 与 data-organization
  使用 disjoint formal manifest、raw multi-prompt Completions 和持久 async
  transport；token-budget 在固定 `ACTIVE_WORK_PER_ENDPOINT` 下独立扫描
  2K/4K/8K/16K/32K/49K/65K。submission-policy 也改为 async batch-level，
  保留 multi-prompt 组织结果，不再用 request granularity 绕过 token-budget。

## 2026-07-29 文献基线版本升级

- 题录核验并新增 VTC、Llumnix、LOTUS、Palimpzest、Abacus、SemBench、
  FairServe、DLPM、Autellix、Chiron 的本地 PDF 与权威精读笔记。
- 纠正三项关键状态：LOTUS 为 PVLDB 18(11) 2025 正式论文；Abacus 为
  PVLDB 19(5) 2026 正式论文；SemBench 为 PVLDB 19(8) 2026 正式
  benchmark paper。`Database Perspective on LLM Inference Systems` 明确为
  PVLDB Tutorial，不占正式 research Top 15。
- Top 15 重排为 AI 算子 3、LLM 推理/公平 7、Ray 1、代价估计 4，共
  15/15 CCF-A 正式 research paper；每篇均有权威精读和本地可解析 PDF。
- 算子代价估计从补充讨论提升为数据组织和调度提交控制共同依赖的使能组件；
  首版限定为简单解析模型 + profile 校准 + residual correction，评价配置
  ranking、决策 regret 与预测区间，不扩张为独立 learned optimizer 贡献。
- 研究问题收敛为：固定资源下的最小饱和 active work/瞬态 ramp、相同 work
  的数据组织、多 job shared credit/idle borrowing/fairness。开题仍保留两项
  方法贡献，多模态仍为泛化验证。
- 同步更新文献索引、知识总汇、推理管线综述、开题报告/PPT、baseline 依据、
  项目总纲与入口。按用户要求未同步 Wiki。

## 2026-07-29 Official baseline AutoDL 部署与 gate 编排补全

- baseline 部署被固化为可恢复状态机：本地完整验证并推送 `main` → 远端
  runner/lease/endpoint/Ray/GPU/PostgreSQL 只读检查 → 保留未跟踪结果的
  fast-forward → base/vLLM 两套 Python 依赖检查 → immutable manifest →
  64 行双 endpoint core gate → 独立 calibration；gate 失败禁止继续。
- AutoDL 已安全同步到 manifest exporter 提交 `d8487b1`。聚焦测试采用
  `unittest discover` 后全部通过；直接使用 `python -m unittest code.tests`
  会与标准库 `code` 模块冲突，远程封装还必须在清理环境变量前保存退出码，
  否则可能把测试失败误报为成功。
- 从正式 PostgreSQL workload `sharegpt_burstgpt` 按 `doc_id` 顺序只读导出
  前 64 行，固定 `max_output_tokens=256`、`trace_target` 估计和双 endpoint
  largest-work-first 分片。manifest SHA-256 为
  `b1def6c9e89c5aed2b35b7fdcde4eca300410023f73e21084d799d9fbdaa3f9a`，
  两端预测 work 为 11,713/11,712，偏斜 0.0085%。
- 审计发现“单 shard CLI + 单 cell validator”之间缺正式编排层；远端手拼
  后台命令无法复现并发启动、失败即停和空队列盖章。新增
  `run_official_baseline_gate.py`：每个 core cell 同时启动两个 shard，保存
  命令/日志/原始结果，等待两个 vLLM queue 归零，归一化并 fail closed。
  项目 profiler cell 明确保持 blocked/独立执行，不复制成近似 baseline。
- 修正双 GPU gate 模板残留的本地历史模型名：当前 AutoDL 两个 endpoint
  served model 是 `qwen2.5-7b`，不能继续使用 `qwen2.5-1.5b` 模板值。
  远端 vLLM 0.25.1 源码审计还确认 `bench serve` 已不再默认 greedy；
  命令构造器现显式传 `--temperature 0`，避免 serving ceiling arm 与其它
  frozen Chat arm 使用不同采样语义。首轮 core gate 随后 fail closed：
  `python -m vllm.benchmarks.serve` 返回 0 却不执行 CLI，因此无结果 JSON；
  安装包确认官方入口为 `vllm.entrypoints.cli.main`，命令已改为
  `python -m vllm.entrypoints.cli.main bench serve`，失败目录完整保留。第二轮
  随后在请求前暴露 tokenizer 漏项：served alias 被当成 Hugging Face repo，
  两端均未发请求并出现网络重试。确认 queue 始终为 0 后终止无效 client，
  runner 正常写失败状态；模板、gate runner 与 shard CLI 现强制使用存在的
  本地 `MODEL_PATH` tokenizer 目录。安装官方 `vllm[bench]==0.25.1` extra
  后，第四轮已真实完成两端各 32 个请求并保存详细 JSON，但实际文件没有
  `e2els`；按 0.25.1 timeline 源码确认 E2E 应由 `ttft + sum(itls)` 重建。
  新归一化器同时使用相对 `start_times` 还原 JCT，并以顶层 duration 做
  100ms/2% 一致性门禁。官方 extra 安装日志保存在
  `/root/autodl-tmp/logs/vllm_bench_extra_20260729_1700.log`。
- 第五轮 `vllm_bench` 首次通过 64/64 exactly-once core gate；随后
  `bounded_http` endpoint 1 因“单本地 URL + 全局 endpoint_index=1”被误判
  越界。bounded client 现用显式 `endpoint_index_offset` 只映射本地
  semaphore/URL，immutable manifest、结果行和 gate 继续保留全局 endpoint id。
  已验证的依赖、未跟踪文件冲突备份、CRLF hash、Ray Serve extra、vLLM
  0.25.1 结果字段、失败证据保存和 service fingerprint 处理均记录在
  `deploy/autodl/README.md`，不依赖新会话重新探索。
- 提交 `5708e85` 在
  `dual_gpu_official_baseline_core_gate_20260729_1725_fix5708e85` 完成
  5/5 core 功能门禁；提交 `f2e82bd` 修复双重 chat template、固定 Ray Data
  actor pool 并标注观测粒度后，在
  `dual_gpu_official_baseline_equivalence_gate_20260729_f2e82bd` 再次
  5/5 通过、最终队列归零。小 gate 虽创建 4 个 Ray Data actor，但每端只有
  两个 16 行 task，实际仅 1 actor 执行；固定建池不能替代 actor/task 联合
  calibration。
- 进一步审计确认不同 adapter 的 client token 字段不能直接横比：vLLM Bench
  `input_lens` 是裸 prompt，bounded/Ray Data 是服务端 usage，Daft 不返回
  usage。gate runner 因此增加每 cell、每 endpoint 的 vLLM prompt/generation
  cumulative counter 前后快照、差分证据和 fail-closed 交叉核验。部署手册已
  记录 counter 污染、回退、非正差分和 accounting mismatch 的判定与处理；
  真实 service-counter gate 通过前 calibration/formal 继续阻塞。

## 2026-07-29 Shared-vLLM 1/2/4-job 正式矩阵条件性正结果

- 双 4090 正式矩阵 36/36 group runs 完成、0 incident；27 个 formal group
  run 包含 63 个 job、32,256 个 request。每 job 512/512 completed，
  request id 全局唯一，两个 endpoint 均有流量，runner 与租约正常退出。
- `shared_drr` 的 9 个 formal credit trace 均满足 endpoint 全局上限：
  active requests 峰值 197/256，active work 峰值 65,536/65,536，结束时
  active/waiting request/work 全部归零。有等待时 2/4-job active-work ratio
  均值为 0.9966/0.9960，未发现 credit 足以容纳最大请求却仍等待的采样点。
- 1-job shared 相对 static 吞吐 -0.02%，通过 3% 协调开销门槛；2-job
  shared 相对 independent 吞吐 -0.04%、max P99 -0.04%，没有 5% 增量。
- 4-job shared 相对 independent 吞吐 +9.57%、max P99 -22.52%、max JCT
  -15.89%；相对 static partition 为 +7.91%/-14.78%/-10.53%。Jain
  fairness median 为 0.9961，最低 normalized service/mean 为 0.9193，
  通过公平性门槛。
- 4-job 结果存在重复间异质性：shared 相对 independent 吞吐分别
  +8.43%、-0.28%、+22.60%，不能写成无条件稳定加速。当前结论是：
  shared credit/DRR 已通过容量安全与公平性验证，并在高竞争聚合数据上达到
  晋级门槛；2-job 无收益，4-job 仍需 held-out repeats。
- 结果落盘于
  `experiments/results/dual_gpu_shared_vllm_formal_20260729_1135/`。
  下一机制验证是 staggered idle borrowing、weighted overlap fairness，
  以及单独的 transient saturation/ramp 实验；不重复当前矩阵，也不把策略
  写成 vLLM 内部推理加速。

## 2026-07-29 SLO-aware EWMA flush 正式矩阵负结果

- 双 GPU 正式矩阵 24/24 runs 完成、0 incident、0 skipped，租约正常释放；
  18 个 formal run 共 36,864 request/36,864 submission，逐 run request、
  doc 与 submission exactly-once，request→submission 集合完全匹配，
  0 worker failure，resource/MFU 状态均为 `ok`。
- arrival-gap completion 修复在正式矩阵中保持有效：backend service-end 到
  scheduler completion 的 P95，high 三策略为 10.32–13.03ms，near 三策略
  为 4.01–4.13ms；未再出现数十到数百秒 stale-credit。
- high 下 fixed/queue/SLO-EWMA 吞吐均值为
  8037.4/8030.1/7995.6 tokens/s；SLO-EWMA 相对 fixed 为 -0.52%，P99
  -0.94%。near 下为 1667.0/1672.2/1668.6 tokens/s；SLO-EWMA 相对 fixed
  +0.10%，P99 -0.49%。所有 arm 的 30s SLO violation 均为 0，未达到预注册
  5% 吞吐/SLO-goodput或独立尾延迟晋升门槛。
- 控制器实现产生了可观测动作，但没有一阶决策空间：high SLO-EWMA
  selected wait mean/P50 为 48.84/50ms，near 为 42.55/50ms；fallback event
  分别占 65.72%/48.69%，且 formal trace 没有 SLO deadline 主导 reason。
  30s SLO 相对 P99 仍有 12.8–24.4s slack，而 25–50ms 控制幅度最多 25ms。
- `near_*` 只保留为预注册 scenario ID。formal 实测其 vLLM running 约 19、
  MFU 7.07%、active work 峰值约 19K，属于 arrival-limited/underloaded，
  不能称为真实 near-capacity；更早 flush 不能创造未到达的请求。
- 不晋升 SLO-EWMA，不继续在同一 25–50ms 动作空间调 alpha/deadband。
  单 job 默认保持 `request + active-work 65K + 1×256 + fixed-50`。下一安全
  方向是已有代码基础的 Shared-vLLM 1/2/4-job shared request/work credit 与
  work-conserving 公平队列门禁，使策略作用于秒级跨 job 排队与隔离。
- compact 结果、七步解释和绘图汇总归档到
  `experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/`；大体积 traces
  保留在远端同名目录。

## 2026-07-29 SLO-aware EWMA flush 实现与双负载门禁

- 根据 fixed-50 与旧 two-level adaptive 在单 job 饱和稳态下几乎无差异的结果，
  将下一轮问题改为“控制器是否能在存在关批决策空间的临界负载改善 SLO-goodput
  或尾延迟”，而不是继续增加饱和 offered work。
- 新增独立 `SloAwareEwmaFlush` 初版：用到达/服务 token rate EWMA 估计剩余
  token-budget fill time 与当前 pending service time，以 oldest request SLO
  slack 限制最长等待，以 deadband 抑制窗口振荡；service feedback 缺失或过期
  时回退到 fixed maximum window。策略保持纯模块，不导入 Ray、Daft、Arrow
  或 HTTP，也不修改 vLLM。
- profiler 增加 `slo_ewma`、EWMA alpha/deadband CLI 与正式 CSV 字段；arrival
  replay 将 token budget、arrival rate 和 per-endpoint service rate显式传给
  flush observation，现有 flush trace 保留选择窗口、理由和原始反馈。
- 新增 `dual_gpu_slo_ewma_flush.example.json`，固定 request-level、
  65,536 work/endpoint、1×256 actor pool，交叉比较 0.001 高压与 0.002
  临界 arrival scale 下的 fixed-50、queue-25/50 与 SLO-EWMA-25/50。
  先执行 128 行六场景门禁，正式结果完成前不声称性能提升。
- 首轮远端 128 行六场景 gate 与追加 512 行双 SLO arm 均 0 incident、
  exactly-once、租约正常释放，但所有 SLO-EWMA flush event 都是
  `fixed_fallback`，service-rate 字段为空。审计确认不是 GPU 或 metrics
  endpoint 故障，而是 profiler 创建后台 metrics provider 的条件仍只包含
  `queue_adaptive/service_quantum`，遗漏新 `slo_ewma`；replay 内部虽请求反馈，
  实际收到的却始终是空 observation。
- 将“是否需要 replay live feedback”收敛为一个共享谓词，由 provider 生命周期
  与 replay observation 同时调用，并增加 `slo_ewma=true/fixed=false` 回归测试。
  旧 gate 仅作为 wiring failure 证据，不进入性能比较；修复后必须重跑反馈 gate，
  确认出现正 service-rate 与非 fallback reason 才允许启动 formal。
- 修复后的 512 行 gate 出现 382/532 个正 service-rate event，并进入
  `busy_fill_ewma`，证明闭环接线生效；但 high/near 平均窗口仍分别为
  49.75/49.82ms。trace 显示填满 32K planning budget 的 p50 预测时间为
  1.54s/2.78s，远大于 25–50ms 控制时标，因此 full-budget fill-time 规则在
  此配置下必然退化为 fixed-50，未启动无意义的 24-run formal。
- 将 busy signal 改为 `global arrival EWMA / (per-endpoint service EWMA ×
  endpoint count)`：ratio≤0.9 取 25ms，ratio≥1.1 取 50ms，中间线性插值并
  保留 deadband/SLO hard limit。原 `0.002` arm 的 p50 ratio 仍约 2.9，
  不是真正临界负载；按同一 trace 预注册 `0.006` 作为近容量点。修订后需再次
  gate，确认窗口不再退化后才允许 formal。
- 第二版 512 行 gate 的 high/near p50 ratio 为 3.68/2.52，near 仍有 738/770
  个有效 event 落在 ratio>1.1。根因是 underloaded/bursty 时 observed service
  throughput 随 offered load 下降，不能作为 GPU service capacity；继续放大
  arrival scale 会形成自指控制。第三版引入前序饱和曲线标定的 4,000
  tokens/s/endpoint 容量下界，load denominator 取
  `max(service EWMA, calibrated capacity) × endpoint_count`。该参数进入 CLI、
  正式 CSV 和模板，且 `slo_ewma` 正式运行强制要求正校准值。
- 第三版 512 行双 SLO arm gate 完成 2/2、0 incident、512 request/512
  submission exactly-once、0 worker failure。high 的 selected wait
  mean/P50 为 46.86/50ms，near 为 37.29/29.03ms，证明独立容量分母终于产生
  负载区分；但 near request E2E P95 异常达到 223.21s，接近 replay 总时长，
  因此仍禁止启动 formal。
- request trace 反向审计确认了更基础的执行缺陷：早期 HTTP 请求的 backend
  service 已在约 1–5s 内结束，但 scheduler completion/credit 被延迟到约
  239s。`SynchronousScheduler` 在 `for envelope in arrival_replay_generator`
  上等待下一次到达，只有 admission 满或输入耗尽才调用 `ray.wait`；低负载
  arrival gap 因而阻塞已完成 ObjectRef 的回收、service feedback 和 request
  lifecycle 时间戳。这会把“更多 offered load 才更快”人为放大，也使旧的
  near-load SLO 数据失真。
- 修复采用文献笔记中的 Ray 有界队列 + completion polling 模式：1-slot
  producer 只负责按到达顺序生成 envelope，主 scheduler 在输入队列暂无新
  arrival 时每 1ms 非阻塞 `ray.wait(timeout=0)`；有新 arrival 时优先提交，
  达到 admission/active-work 上限时仍阻塞回收。Ray submit、credit、routing
  和 lifecycle 状态仍全部在主线程更新。最小回归测试先复现失败再通过；本地
  412 tests 通过。修复后的远端 replay gate 通过前，不使用第三版 gate 的
  SLO/P99 作为策略性能结论。
- 修复后同配置 512 行回归 gate 完成 2/2、0 incident、租约释放、两场景均
  512 unique request/512 submission、0 worker failure。high/near backend
  结束到 scheduler completion 的 P95 分别为 4.0/4.0ms，修复前分别为
  31.20/217.36s；near request E2E P95 从 223.21s 降至 4.86s，SLO violation
  从 40.43% 降至 0。high request E2E P95 同样从 35.82s 降至 7.72s。
- 回归 gate 的 e2e/tokens-per-s 基本不变（high 48.48→48.54s，
  near 245.11→245.25s），证明本次修复没有把 arrival-limited 总时长包装为
  吞吐收益；它修复的是 request lifecycle、credit 周转和反馈时效。near
  `max_active_work_per_endpoint_seen` 从伪占用的 65,532 降至 11,224，
  `max_inflight_seen` 从 372 降至 50，直接证明旧“大 credit 才快”现象中包含
  已完成 ObjectRef 未回收造成的虚假在途工作。
- 修复后 high/near selected wait mean/P50 为 46.73/50ms 与
  37.49/35.29ms，控制器仍能区分负载而未退化。该回归 gate 只证明执行与测量
  正确，不证明 SLO-EWMA 优于 fixed/queue；正式六场景 1 warmup + 3 repeats
  方可用于策略比较。

## 2026-07-29 complete-row service quantum 负结果与机制边界

- 双 GPU 正式矩阵 24/24 runs 完成、0 incident、租约正常释放；固定
  65,536 active work、1×256 actor pool、32,768 planning budget 和
  0.5 Ray CPU/endpoint，只改变 batch/quantum/request completion 粒度。
- 512/1024/2048/4096 quantum 相对 planning batch 的 formal 吞吐变化为
  -0.03%/+0.11%/+0.12%/+0.54%，request diagnostic 为 +1.75%；均未达到
  预注册 5% 吞吐或 SLO goodput 晋升门槛，MFU 和 P99 也基本重合。
- 机制修复有效但不是当前吞吐瓶颈：512/request 把平均 credit-held 从
  10.171s 降至约 8.53s、bounded wait 从 33.86s 降至约 28s；然而每 endpoint
  已有 65K active work，vLLM iteration-level batching 始终有足够请求，
  提前释放 credit 没有增加 GPU 可执行工作。
- 不晋升固定 service quantum 为默认性能策略。request-level 路径保留为
  精确 completion/credit、真实 request latency 和后续多 job 公平控制基础；
  该选择不包装成显著吞吐贡献。下一轮转向有 burst/gap、foreground/
  background 和不同 SLO 的动态负载，验证 SLO-aware EWMA flush。
- 完整结果归档到
  `experiments/results/dual_gpu_service_quantum_20260729/`；大体积 trace
  继续保留在远端。

## 2026-07-29 双 GPU Ray actor-pool 形状负结果

- 在已标定的每 endpoint 65,536 active work 下，固定 256 actor slots 和
  0.5 Ray CPU/endpoint，完成 1×256/2×128/4×64 三形状 64 行 gate 与
  12/12 正式 runs；0 incident、worker failure 为 0。
- 1×256/2×128/4×64 的 formal 吞吐均值为 7983.9/8143.8/8043.8
  tokens/s，相对 baseline 为 0%/+2.00%/+0.75%；MFU 均约 35.0%，P99
  均约 36.7–36.8s，credit-held 与 slot utilization 几乎重合。
- 多 actor 未达到预注册的 5% 吞吐/SLO goodput 晋升门槛；2×128 的
  Ray→service 均值还从 4.0ms 增至 24.3ms。因此当前同构、单 job 场景保留
  最简单的 1×256，不启动 least-active-work worker routing。
- 该结果只否定“增加 actor 数本身可提速”，不否定 Ray stateful admission、
  多 job 隔离、异构分池、故障迁移的后续价值。完整结果归档到
  `experiments/results/dual_gpu_actor_pool_shape_20260729/`。
- 随后的 complete-row service-quantum 64 行 gate 6/6 通过：所有场景
  64 行 exactly-once、无失败；512/1024 token 分别形成 64/48 个完整行
  quantum，oversized 单行未被拆分。正式 24-run 矩阵已启动。
- 正式矩阵第一次启动在 argparse 阶段因漏传 runner 的 profiler、Python、
  health 和 metrics 参数立即退出，未创建结果目录或占用 GPU；失败日志保留。
  随后按 `deploy/autodl/README.md` 的完整命令重新启动。该事故再次说明新会话
  必须复制 runbook 的完整 runner 参数，不能只传 config/output。

## 2026-07-29 Checkpoint B：complete-row service quantum 实现启动

- 新增纯策略 `slice_service_quanta`：按目标 token work 顺序累积完整行，
  超预算单行独占一个 quantum 并显式标记，禁止把单行 prompt 拆成多个请求。
- 该层只定义 planning batch 内的 HTTP/Ray completion 与 credit 释放边界，
  不依赖 Arrow、Daft、Ray 或 vLLM；5 项确定性/边界测试已通过。后续任务再将
  同一 helper 接入 offline 与 arrival replay，避免两套 membership 语义漂移。
- offline 与 arrival replay 现已共用同一 Arrow expansion helper；CLI 增加
  `service_quantum` 粒度和显式正数 token target，planning batch index 不随
  quantum 展开而漂移，请求 lifecycle 精确映射到所属 quantum。
- 正式汇总分别记录 quantum count/rows/work/P95/oversized rows，submission
  trace 升级到 schema 4 并记录两级 identity、credit-held 与 Ray-to-service
  时间。136 项相关模型、回放、CLI、schema、trace 和 scheduler 测试通过；
  尚未形成远端性能结论。
- Ray actor pool 现在显式维护每 worker running/active-work/完成/失败/峰值和
  slot-held 时间；round-robin 与 least-active-work 只从有空 slot 的 worker
  中选择，Ray 成功或失败都由 canonical handle 精确释放一次。
- effective per-endpoint admission 被 `worker 数 × actor concurrency` 物理
  slots 截断；正式 CSV 记录 routing、slots、逐 worker 峰值/失败和 slot-held
  utilization，submission trace 记录 worker ID/index/PID。相关 actor-pool、
  legacy cleanup、backend PID、CLI/capacity、schema 与 scheduler 定向测试通过；
  尚未形成远端性能结论。
- 执行前 fatal-flaw audit 否决了原定 16-slot 对照：当前远端已观测到单请求
  平均约 332 work、组织批次平均约 1337 work，16 slot 只能暴露约 5.3K/21K
  work，低于 65K–131K 饱和扫描范围，会再次把 offered load 差异误写成策略
  收益。Actor Pool 模板改为固定每 endpoint 256 slot 的
  1×256/2×128/4×64，并沿用 request-level 饱和基线；每 endpoint 的 Ray
  CPU reservation 同时固定为 0.5（每 actor 0.5/0.25/0.125），避免拓扑
  对照混入总 CPU 资源变化。
- 新增 `dual_gpu_actor_pool_shape.example.json` 和
  `dual_gpu_service_quantum.example.json`。后者按实测组织批次
  P95≈3366、max≈5892 选择 512/1024/2048/4096；删除 8192，因为它不会
  切分当前任何批次，只会静默复制 batch control。两份模板都固定 planning
  budget、active work 和总 slots，先做 64 行门禁再顺序运行正式矩阵。
- 本地完整回归 400 项通过，Ruff 与 `git diff --check` 通过；模板契约测试
  强制 pool 每端点总 slots=256，并保证 quantum arms 只改变 completion
  粒度/target，不改变 active-work 参考。
- 远端首次 gate 启动尝试未产生 Python runner/manifest/log：宽泛 `pgrep`
  匹配到当前 bash 包装命令，且把 `nohup ... &` 接在长 `&&` 链后只返回了包装
  shell PID。审计确认零结果污染后，改为只枚举 Python runner、前置检查独立
  `set -euo pipefail`、单独后台化 nohup 并立即保存 `$!`，随后 gate 正常完成。
  该坑已补入 AutoDL runbook。

## 2026-07-29 Checkpoint A：runner 可靠性与扩展饱和曲线

- 新增 output-directory 原子租约：runner 启动时记录 host、PID、进程启动身份、
  config fingerprint 与代码提交；活跃 owner 拒绝第二写者，只有
  `--resume --recover-stale-lease` 才能显式接管已确认死亡的 owner，并把恢复
  事件保留到 manifest。对应实现提交为 `e03480c`。
- Ray adapter 现在把 `ray.get` 异常转换为失败 completion 交回 typed
  scheduler，保证 worker/HTTP 失败时共享 active-work credit 只释放一次，
  不再因异常逃逸永久占用配额。对应实现提交为 `f7443b2`。
- 双 GPU active-work 模板从 5 档扩展为 8 档
  （16K/24K/32K/49K/65K/82K/98K/131K），预注册选择规则为首个达到最大
  已测吞吐 97% 且下一安全档增益低于 3% 的最小档；若未出现该点则报告
  `saturation_not_reached`，高负载 OOM/超时必须保留 incident。
- Checkpoint A 远端 64 行 gate 与正式曲线均完成：32/32 run 成功、每档
  3 formal、0 incident、lease 正常释放，request/submission exactly-once、
  resource/MFU 均为 `ok`。
- 65K formal 均值 8030 tokens/s，达到全曲线最大均值 97.80%，下一档
  82K 只增 0.92%；98K 与 131K 分别 8211.094/8210.874 tokens/s，完全
  持平，而 P99 从 65K 的 36.78s 升至约 40.05s。按预注册规则选择
  `ACTIVE_WORK_PER_ENDPOINT=65536`。
- 结果归档到 `experiments/results/dual_gpu_active_work_saturation_20260729/`。
  它证明当前链路已进入平台，后续策略固定 65K，不再以增加 offered work
  作为表面优化。

## 2026-07-29 饱和 Ray 执行基础实施计划

- 已批准
  `code_doc/superpowers/specs/2026-07-29-saturated-ray-actor-pool-replenishment-design.md`
  后，新增对应 TDD 实施计划
  `code_doc/superpowers/plans/2026-07-29-saturated-ray-execution-foundation-implementation.md`。
- 本地执行前基线使用项目 `.conda/pg-ai-profile` 环境在沙箱外完整通过
  `376` 项测试；系统 Anaconda 缺少 `psycopg`，沙箱内
  `TemporaryDirectory` 的 Windows DACL 会导致 7 项 runner 测试报
  `PermissionError`，两者均为环境入口问题而非代码断言失败。临时测试目录已清理。
- 第一远端检查点只包含 output-directory 原子租约、Ray 失败 completion
  清理和 16K–131K active-work 饱和曲线；第二检查点再验证 fixed service
  quantum 与固定总 slots 的 actor pool，避免把可靠性、offered load 和
  策略机制混在同一轮改动。原计划的 16 slot 在正式启动前已由实测 work
  分布审计否决，第二检查点改为每 endpoint 固定 256 slot。
- endpoint-local async dispatcher 按批准设计继续保留，但只有 driver-owned
  有界 actor pool 通过 trace 与性能门禁后才进入下一份实施计划；这不是删减
  Ray 方向，而是防止一次性大改无法定位收益或退化原因。

## 2026-07-29 饱和 active-work 与 Ray actor-pool 补位设计

- 审阅现有双 GPU active-work、request replenishment 和固定-work
  token-budget 证据后，明确把“填满 GPU”与“饱和后策略优化”拆成两个实验阶段：
  先扩展 65K/82K/98K/131K per-endpoint predicted-token work 曲线并按
  97% 最大吞吐与相邻增益低于 3% 的预注册规则选饱和点；未达到则明确报告，
  不把最高已测点改名为容量上限。
- 新增
  `code_doc/superpowers/specs/2026-07-29-saturated-ray-actor-pool-replenishment-design.md`。
  设计保留 planning batch 做 token-budget/length-align/prefix-aware 组织，
  另引入不拆单行的固定 service quantum 作为 HTTP/Ray 完成与 credit 释放单元，
  用 completion-driven replenishment 消除 whole-submission HOL、波次执行和
  credit 空转。
- 将 Ray actor pool 提升为独立策略维度：每 endpoint 设置有界 dispatcher，
  显式维护 pending queue、active-work credit、worker slots、service EWMA 与
  completion loop；在固定总 slots 下比较 1×16、2×8、4×4，避免把 actor
  数量增加带来的 offered-load 增加误判为调度算法收益。Ray worker 继续
  `num_gpus=0`，不把上游 actor 调度表述成 GPU kernel 调度。
- 固定-work 曲线中发生重复 runner 事故：错误进程名检查漏掉原 runner 后，
  第二个 `--resume` 同时写同一 CSV/manifest，破坏单写者行数不变量并产生
  21/36 `missing_expected_csv_row`。设计新增 output-directory 原子租约、
  PID/启动身份/config fingerprint 校验和显式 stale recovery；受影响结果只作
  诊断证据，在修复前禁止再次 resume 同一目录。
- 实施顺序固定为可靠性与 trace → active-work 饱和 → 固定总 actor slots →
  actor pool 形状 → service quantum → least-active-work worker 路由 →
  endpoint-local 补位 → 单项有效后再组合。若饱和后策略无稳定收益，则登记
  负结果并保留简单 baseline，不再用“喂得更多”充当策略贡献。

## 2026-07-29 AutoDL 新对话零探索入口

- 将 `deploy/autodl/README.md` 固定为 AutoDL 单一 runbook，并在顶部新增
  新对话判定表、固定路径表、全新实例从零准备、每次开机完整恢复、64 行
  gate、正式后台启动与 `--resume` 恢复流程。
- 根 `PROJECT_INDEX.md` 新增“要在 AutoDL 远端继续实验”的强制阅读顺序：
  项目规则 → 权威总纲 → 当前实验状态 → 部署规则 → AutoDL runbook →
  已提交配置模板。后续 agent 不再从聊天记录猜测 Python/CUDA/模型/PG/日志
  路径。
- 根 `README.md` 与 `deploy/README.md` 增加一级路由；详细安装和故障知识仍只
  保留在 `deploy/autodl/README.md`，不新建重复 runbook，避免两份流程漂移。
- 开机恢复顺序固化为：runner/Git 状态 → PostgreSQL + workload →
  runtime env → 双 vLLM endpoint → health/models/进程/GPU → gate →
  formal。明确禁止 `git clean` 未跟踪结果、删除失败证据或绕过 gate。

## 2026-07-29 固定 active-work 的 token-budget 曲线启动口径

- 消除 `PROJECT_OUTLINE.md` 与
  `experiments/plans/experiment_status_and_gaps.md` §10.3 的执行顺序冲突：
  双 GPU 下一轮先关闭 arrival replay 扫 token budget，再以最佳已测预算隔离
  whole-submission 与 request-credit；不直接进入 submission-policy 联合消融。
- 复核发现 `49K active work × 65K token budget` 会触发 oversized admission，
  破坏固定-work 语义。正式矩阵改为 49K 主点的
  8/16/32/49K 与 65K 敏感性点的 8/16/32/49/65K，共 9 个场景、每场景
  1 warm-up + 3 formal。
- `deploy/autodl/dual_gpu_token_budget_curve.example.json` 直接承载上述
  9 场景，并将工作量统一为 2048 行；不新增重复配置入口。
- 晋级标准保持为：相对重复波动改善 observed tokens/s 或 SLO goodput，且
  request P99、failure 与 exactly-once 不退化。49K 用于选择
  `BEST_TESTED_TOKEN_BUDGET`，65K 只报告高负载敏感性。
- 远端 64 行 gate 首次被 PostgreSQL 5432 未启动正确拦截；按 AutoDL
  重启 runbook 恢复 PostgreSQL 18.4 + pgvector 0.8.5，并确认
  `sharegpt_burstgpt` 2048 行后使用 runner `--resume` 成功。manifest 保留原
  incident 且标记 `recovered=true`，没有删除失败证据。
- 同步修正 AutoDL 重启恢复段：PG/workload 验证必须先于 runner，endpoint
  启动必须显式传 runtime env；`start_endpoints.sh` 使用受管 PID 文件而非
  宽泛 `pkill -f`。正式 9 场景 ×（1 warm-up + 3 formal）任务已从远端
  `main@2158afd` 启动，输出到
  `experiments/results/dual_gpu_token_budget_curve_20260729/`。

## 2026-07-28 双 GPU active-work 容量曲线完成与 worktree 收口

- 远端 detached worktree `44087ae` 完成
  `dual_gpu_active_work_curve`：5 个 per-endpoint predicted-token work 档位，
  每档 1 warm-up + 3 formal，共 20/20 成功、0 skipped、0 incident。
- 16K→65K 的 formal 吞吐均值为 4888、6076、6837、7703、8129 tokens/s，
  MFU 为 21.03%→35.16%；每档吞吐 CV 仅 0.22%–0.94%。
- 49K→65K 增加 33.3% active-work credit，只取得约 5.5% 吞吐增量，
  P99 从 34.99s 增至 36.63s；49K 的 30s SLO violation 最低（1.89%）。
  因此登记 49K 为 `KNEE_CANDIDATE`，65K 仅为
  `BEST_TESTED_THROUGHPUT_CAP`，不声称容量最优。
- 新增 `experiments/results/dual_gpu_active_work_curve_20260728/`，归档
  manifest、逐次汇总、plot-ready formal summary 与七步结果报告；33 MB 原始
  request/submission/flush/resource traces 继续保留在远端，不直接纳入 Git。
- 同步更新证据台账、项目索引、实验状态、总纲和快速参考。下一步固定 49K 主点
  与 65K 高负载敏感性点，对 batch barrier/request replenishment 及数据组织做
  matched-work 对照。
- 远端 `main` 已快进至 `8376999`；合并前服务器本地启动脚本的等价顺序调整
  保存在 `stash@{0}`，未覆盖实验结果。实验运行期间 worktree 与 main 相互
  隔离，main 更新未干扰本轮 20 次运行。

## 2026-07-28 双 GPU request-level replenishment 正式结果审计

- 登记远端 2× RTX 4090 Phase 3：5 个场景各 1 warm-up + 3 formal，
  15/15 formal 成功。global K32 与 per-endpoint K16 吞吐仅差 0.31%，
  确认双 endpoint admission 语义。
- 从 request trace 按 `prompt + estimated output` 重算 admission unit：
  batch 平均 1106.892 token/slot，request 平均 371.847 token/slot。
  因此 request K48 才与 batch K16 基本 work-matched；两者吞吐分别
  4248.01 与 4247.97 tokens/s，未隔离出 continuous replenishment 增量。
- request K64 达 4768.04 tokens/s（相对 batch K16 +12.24%），但名义
  offered work 同时高约 33.3%，且 request P99 高约 24%。结论收缩为
  `BEST_TESTED_REQUEST_K`，不能称为容量最优或补位机制胜出。
- 新增 `experiments/results/dual_gpu_request_replay_20260728/`，保存远端
  `runs.csv`、`manifest.json`、七步结果报告与可绘图汇总；下一轮固定
  per-endpoint active work，使用 30 s SLO 并显式记录 vLLM capacity。
- 同步更新根 `AGENTS.md`、`README.md`、`PROJECT_OUTLINE.md`、快速参考卡、
  实验状态审计、证据台账与项目索引，避免继续把 K64 写成已证明的机制最优。
- 远端 Phase 3 后 endpoint 自动恢复失败：第一层原因是 vLLM Python 虚拟环境
  未进入 PATH，FlashInfer JIT 找不到 `ninja`；补 PATH 后进一步确认 pip
  nvcc 13.2 与 CUDA 13.0 headers 不兼容。现成
  `/usr/local/cuda-13.0` 编译器+headers 最小 CCCL 编译已通过。
- `start_endpoints.sh` 显式暴露 `$VLLM_VENV/bin`；runtime env 示例固定
  `CUDA_HOME=/usr/local/cuda-13.0` 与对应 `CUDA_NVCC_BIN`。AutoDL runbook
  新增启动前检查、8192/256 capacity、LF 行尾、PID 安全停止、健康门禁与
  最短故障诊断表，供后续实验复用。
- 匹配组合重启成功：两个 Qwen2.5-7B endpoint 均 health/models 通过，每卡
  一个服务进程，命令显式包含 `max-num-batched-tokens=8192` 与
  `max-num-seqs=256`。64 行 active-work gate 完成，0 failure/incident，
  64 request/64 submission、endpoint 32/32，resource/MFU 均为 `ok`。
- 已直接启动 `dual_gpu_active_work_curve`：5 档 per-endpoint predicted work
  ×（1 warm-up + 3 formal）= 20 runs；启动检查时 manifest 为 running、
  0 failed/incident，两张 GPU 均 100%。

## 2026-07-28 双 GPU 容量曲线远端审计与修正设计

- 只读核验远端 Phase 1 的 24 次运行：32768 arm 仍为最高吞吐，但
  `batch_rows_mean=64` 已命中 `ray_batch_rows=64`，平均组织成本约 21.5k
  tokens、预算利用率约 72.6%；因此只能标为当前扫描范围内最佳，尚未找到容量
  甜点。
- request trace 明确为 submission 粒度；同一 64 行 submission 内共享完成时间，
  不能用当前 P99 声称真实逐请求 completion span/HOL。
- Phase 2 已完成但 sequential 为 8 个 batch（4/4 endpoint），row-cap-aware 与
  length-align 为 9 个 batch（4/5 endpoint）；后续解释必须拆出 batch-count 与
  endpoint work imbalance。
- 新增
  `code_doc/superpowers/specs/2026-07-28-dual-gpu-experiment-correctness-design.md`，
  设计 required service metadata 门禁、组织/提交双层指标、扩展容量模板和共享
  Ray address 契约；正式代码与远端 worktree smoke 待设计确认后实施。
- 设计已获确认；新增
  `code_doc/superpowers/plans/2026-07-28-dual-gpu-experiment-correctness-implementation.md`，
  按服务元数据 → 指标语义 → 共享 Ray cluster → 配置与文档 → 本地/远端
  worktree 验证五个独立 TDD 任务执行。

## 2026-07-28 双 GPU 策略与实验矩阵解耦

- 已验证审计分支 `7137b3d` 以 fast-forward 合并并推送到 `main`；后续未验证
  的实验设计继续留在 `codex/architecture-deployment-audit`。
- 定位优化不明显的主要实验混淆：旧双卡配置同时启用 accelerated arrival
  replay、50ms flush 与 token-budget；1024 行 gate 的 packing budget
  utilization 仅约 13.5%、平均约 3 行/batch，说明多数 batch 被 timeout 提前
  关闭，不能据此判断 token-budget/length-align 本身无效。
- 新增 capacity scaling、offline data organization 两个 AutoDL 模板，并修订
  request replay 模板。执行顺序固定为容量曲线 → 组织隔离 → 按实际
  `batch_rows_mean` 对齐 request credit 的持续补位；AIMD/HOL 暂停参数扫描，
  直到有能观察 Ray backlog/oldest-request slack 的 endpoint-local 信号。

## 2026-07-28 双 endpoint admission 语义与 GPU 采样修正

- static typed scheduler 新增独立 endpoint credit：`per_endpoint K` 同时施加每个
  endpoint 的硬上限与 `K × endpoint_count` 的全局安全上限；历史 global K 语义保持
  默认。自适应控制器仍为全局窗口，当前明确拒绝 per-endpoint scope，避免错误标注。
- profiler 正式 CSV 增加 admission scope、per-endpoint limit 和 effective global
  limit；AutoDL 双卡模板同时保留 global K32 control、per-endpoint K16 batch，以及
  per-endpoint K32/K48 request-level 场景。
- `nvidia-smi` 采样按 endpoint GPU ID 过滤，避免双卡主机上的单 endpoint control
  把空闲卡纳入 utilization 平均。旧单卡约 47% 的主机均值不能用于推断活动卡 SM
  利用率，更不能单独支撑“GPU util 与 MFU 反向”的结论。
- 旧数据按近似相同 per-GPU credit 重算，双卡 global K16 / 单卡 K8 约 1.74×，
  双卡 global K32 / 单卡 K16 约 1.57×；共享 K 是同 K 对照低扩展的重要原因，但
  不能写成“双卡扩展比只有 1.0”或“vLLM 无法跨独立请求 continuous batch”。
- 远端 1024 行单次机制 gate：双 endpoint per-endpoint K16 的实际 max inflight
  为 32，约 4302 tokens/s、12.82 rows/s、修正 MFU 0.183；相对旧 global K16
  的 2992 tokens/s 高约 43.8%，并与旧 global K32 的 4251 tokens/s 接近。该结果
  只验证 credit 语义与 offered-load 恢复，仍需随机交错的 3 次 formal repeat 才能
  作为正式性能结论。
- 远端 Ruff 通过；全量 346 个测试通过，其中包含 5 条真实 Daft→Ray task/actor
  contract。

## 2026-07-28 近两日代码审计与 profiler trace 边界拆分

- 按 7 月 27–28 日提交链审查 profiler、typed scheduler、Ray adapter、场景 runner、
  K_max runner、指标与部署脚本；保留已确认的项目进度与实验结论，不把旧 warm-up
  重新解释为 request-level 证据。
- 修复场景 runner idle gate：空 metrics URL、缺失 running/waiting gauge 或抓取异常
  均不能被判为 idle，且错误原因不再引用未赋值局部变量。
- 修复 scheduler 零在途拒绝准入时的空 fan-in；Ray adapter 将 ready ref 规范化为
  pending 中的原对象，保持 identity 删除契约。
- 将五类 trace CSV 序列化从主 profiler 拆到 `code/src/profile_traces.py`。control
  schema 2 补写实际 `hol_age_s`；submission schema 3 使用真实 request/batch lifecycle
  ID，并补 pool、endpoint、GPU、status、error，避免 request 粒度实验被伪 batch ID
  误导。
- K_max runner 增加多 endpoint/metrics URL 清洗、数量一致性与正 K 值校验。
- 修复 profiler 配置优先级：显式单/多 endpoint 与 metrics CLI 参数优先于环境变量；
  未传 CLI 时才读取 plural/single env，避免旧的双 endpoint env 静默覆盖当前命令。
- 收紧 AutoDL managed endpoint 停止契约：PID 命令必须同时匹配 vLLM server 与目标
  port；TERM 后 30 秒仍存活则保留 PID 管理信息并失败，不再继续启动重叠服务。
- 继续拆分主 profiler：新增 `profile_cli.py`、`profile_config.py` 与
  `profile_schema.py`，分别承载 argparse 参数面、CLI/env/Ray worker 配置解析和正式
  汇总字段契约；主脚本降到约 3340 行，仍保留数据库与单次 run 编排。
- 新增 `profile_replay.py`，集中 offline/replayed Arrow envelope、token-budget 关批、
  batch/request 粒度展开与生命周期种子；主脚本进一步降到约 2885 行。
- 新增 `profile_ray.py`，集中 endpoint topology、Ray task/actor submitter、typed
  scheduler、credit/fan-in 与保留的 legacy adaptive baseline；主脚本降到约 2268 行，
  只保留数据库、model backend 创建、单次 run 阶段编排和最终汇总。

## 2026-07-28 双 4090 配置审计、MFU 口径与 AutoDL 配置化

- 现场确认 7B `replenish` warm-up 误用了 `ray_batch_rows=1` 且仍为
  `submission_granularity=batch`；该结果只证明单行 Ray task 路径开销，不能用于
  判断 request-level continuous replenishment。新增远端可复用场景模板，保留
  token-budget/row-cap 组织边界并显式比较 request K64/K96。
- 修正多 endpoint MFU 聚合：工作量 counters 求和、KV usage 取最大值，但 vLLM
  明确标记为 per-GPU 的 FLOPs counter 在 endpoint 间取均值；旧双 endpoint MFU
  值因此属于高估口径，不直接与修正后数据混算。
- `aimd_hol` 不再因同时提供 metrics URL 而在 admission 决策线程同步抓取网络指标；
  新增 request replay 经真实本地 Ray task 单行、exactly-once 合约测试。
- AutoDL 新增统一 env 示例、强制启用学术加速的模型下载脚本、无宽泛 `pkill` 的
  配置化多 endpoint 启动脚本。模型、路径、GPU、端口、context 和 endpoint URL
  均从配置切换，不修改源码。场景 runner 支持严格 `${ENV_NAME}` 展开，缺失变量
  在外部工作前失败。
- 新增根 `pyproject.toml` 与固定 Ruff 开发依赖；先启用全仓可通过的
  correctness lint，避免把 4000 行 profiler 的纯格式重排混进性能机制修复。

## 2026-07-27 修正 Blackwell 章节过度结论 + 实验可比性纪律

- 修正 `deploy/autodl/README.md` §12.6 的过度表述:之前写"flashinfer 0.6.x 的 PyPI wheel
  没给 sm120 编译"超出了证据。分层诊断显示 GPU(torch.cuda.get_device_capability=(12,0))
  和 PyTorch(get_arch_list 含 sm_120、_get_cuda_arch_flags 生成 compute_120/sm_120)
  都正确认识 sm120,失败具体在 **FlashInfer 0.6.13 自己的 arch 探测**
  (TARGET_CUDA_ARCHS=set())。改为只声明"本 AutoDL 实例 + vLLM 0.25.1 + torch 2.11+cu130
  + flashinfer 0.6.13 这组固定版本的标准 pip 环境无法完成 sm120 架构检测",并明确"不等于
  FlashInfer/Blackwell 整体不支持",换 4090 是工程止损决定、不是普遍结论。
- 补 §11 实验可比性纪律:4090(sm89)vs 本机 5070(sm120)硬件不可比,正式 baseline/消融
  全在同一台 2×4090 重跑,本机只做功能验证,不跨硬件算优化比例。研究变量与 GPU 架构正交。

## 2026-07-27 Blackwell sm120 兼容性注记 + 换卡 4090 决策

- 在 2× RTX 6000D(sm120 Blackwell)上耗半天确认:**AutoDL 上 pip 装的 vLLM 跑不了
  Blackwell**——flashinfer 0.6.13 sm120 CC 检测 bug + CUDA 12/13 库混 + quack/cutlass
  API 不匹配,连环失败;7+ 种 workaround(LD_LIBRARY_PATH、FLASHINFER_DISABLE_VERSION_CHECK、
  TORCH_SDPA、enforce-eager、升 vllm 0.26.0、升 quack 0.6.1、cu13 优先)均不够。本机能跑
  是靠官方 Docker 镜像里专门 build 过的栈。
- `deploy/autodl/README.md` 新增 §12 完整记录此结论与所有试过的 workaround(备查,避免重复);
  §10 踩坑表 + §11 平台边界同步改为"非 Blackwell"口径。
- 决策:退 6000D,换 **2× RTX 4090(sm89)** 继续。vllm 0.25.1 在 sm89 上标准 pip 安装即可,
  和本机版本可比。
- 待办(换卡后):起双 endpoint → 多 endpoint 路由实验;按用户要求加大测试强度,扫多套
  调度策略(动态、论文设计、假设设计),CSV 留全指标(tokens/s、service_p99、inflight
  trace、per-request latency)供画图,数据落 experiments/results/。

## 2026-07-27 部署文档补全:data/README 数据集规格 + AutoDL 踩坑表补齐

- `data/README.md` 重写 Sources 小节为"Sources (exact)":修正 BurstGPT 来源为 **v2.0 release
  asset**(`https://github.com/HPMLL/BurstGPT/releases/download/v2.0/BurstGPT_1.csv`,
  非仓库 `data/` 树)与大小(52,283,111 字节);给出 ShareGPT/BurstGPT 直链 URL 表;
  新增"Fetch on a fresh environment"小节,写明 raw 被 gitignore、每个环境都要重下,
  并给出 `network_turbo + HF_HUB_DISABLE_XET` 的 wget 命令(指向 deploy/autodl/README.md)。
- `deploy/autodl/README.md` 补三处:§1 把"学术加速默认开"改为"非自动,每次 source";
  §4.2 补 torch 2.11.0 slim wheel 拖 CUDA13 nvidia libs(~2GB)、总下载 ~2.5GB / 30+ min
  的说明(避免误判卡死);§10 踩坑表补 4 行(torch→CUDA13、HF CLI 改名、下载带宽竞争、
  `rm -f` 删 gitignored data/raw 的协调教训)。
- 同步提交 GitHub(本次 push)。

## 2026-07-27 AutoDL 云部署指南沉淀

- 新增 `deploy/autodl/README.md`：把 2026-07-27 部署 AutoDL（2× RTX 6000D）的全流程
  经验沉淀为可复用 runbook，覆盖实例选型、连接驱动、GitHub 克隆、Python 依赖与版本
  兼容性、模型下载、PostgreSQL+pgvector、workload 数据、vLLM endpoint、实验运行与
  操作坑汇总（10 条踩坑表）+ 平台边界声明。
- 两项强约束（用户要求）：① 代码必须从 GitHub 克隆
  （`https://github.com/3444374/ai-operator-execution-optimization.git`），不再上传本地
  tar 拷贝——便于维护改动与文档同步；② 版本须与项目兼容、不冲突——vllm pin 0.25.1
  （与本机 Docker `vllm/vllm-openai:v0.25.1` 对齐），其依赖 torch 2.11.0 会覆盖镜像的
  torch 2.8.0+cu128（CUDA patch 不同，但 vLLM 版本对齐，报告标注差异）。
- 同步更新 `deploy/README.md`、`deploy/AGENTS.md`（扩大 deploy/ 范围以容纳云部署指南，
  修订"GPU 服务不放这里"为"单服务不放 / 完整云栈放 autodl/"）、`PROJECT_INDEX.md`。
- 关键经验：AutoDL `/etc/network_turbo` 是 HF/github 加速前置（不开则 modelscope /
  hf-mirror / HF 直连三源全慢或 stall）；HF 大文件需 `HF_HUB_DISABLE_XET=1`（否则
  cas-server 401）；长任务用 nohup 后台 + 短连接轮询（不用 exec 长连，避免 paramiko
  超时砍断与 stdout.read 卡死）；`pkill -f` 禁匹配自身命令行（自杀 3 次的教训）。

## 2026-07-26 Eager vLLM baseline captured after recovered preflight incident

- Recorded the exact running vLLM 0.25.1 eager service and added complete,
  validated 64/512 scenario configs for real ShareGPT/BurstGPT compatible-HTTP
  execution with ChatML, temperature 0, 512 output cap, token budget 6144,
  static K=8, fixed 50 ms flush, MFU/resource tracing, and no writeback.
- The 64-request gate completed with zero incidents and passed exact request,
  output-token, finish-reason, FLOP, MFU, energy, resource, database job, and
  CSV schema audits.
- The 512 warm-up completed, after which the first formal invocation failed
  before job creation because the new process's base preflight row keys did not
  match the fully expanded `runs.csv` header. After the stable-field preflight
  fix and an independent resume audit, `--resume` skipped the warm-up,
  completed all three formal repeats, and marked the retained incident
  recovered.
- The three formal repeats each contain 512 completed unique requests/docs,
  positive token/FLOP deltas, valid MFU/energy/resource traces, and finished
  database jobs. Mean E2E is 282.756 s, observed throughput 812.234 tokens/s,
  MFU 0.04025, and GPU energy 22.851 kJ.
- Boundary: this establishes only the eager baseline. The recovered preflight
  incident, overwritten original retry stderr path, and one formal-3 Ray
  shutdown access-violation stack fragment remain documented concerns; no
  eager-versus-CUDA-graph conclusion is made.

## 2026-07-26 Ray 执行基础整分支审查修复

- Ray actor 正式运行现在只构造一次 endpoint-local worker submitter 与 legacy
  endpoint round-robin 状态；跨 PostgreSQL fetch chunk 不再从首个 worker 或首个
  endpoint 重新开始。逐 worker 提交计数按单次 chunk 返回 delta，避免汇总时重复
  累加历史计数。
- 非 fake compatible HTTP backend 的 endpoint URL 校验提前到 dry-run、数据库连接
  和 Ray 初始化之前；Ollama completion 继续保留默认 `http://localhost:11434`。
- job 创建后的任意异常会先回滚失败事务，再将 job 标记为 `failed` 并写
  `finished_at`；状态更新失败只附注到原异常，不替换原始失败。
- 主 CSV 在任何非 dry-run 执行前用 dry-run 字段做 schema 预检；正式追加仍要求
  header 精确一致。K_max interference runner 默认写入新的 `20260726` 文件名，
  历史 `20260719` 结果未删除、未覆盖。
- 本次是执行正确性与失败可观测性修复，不新增或修改实验性能结论。

## 2026-07-26 Ray 执行契约结果 schema

- `postgres_ai_operator_profile.py` 的 dry-run 与真实 run row 新增
  `ray_version`、每 endpoint actor worker 数、actor 最大并发、Ray CPU/GPU
  资源、service endpoint 数、actor worker 总数和逐 worker 提交计数。
- 真实 Ray 版本仅在 Ray 可用后读取；Python executor 行保持空值。HTTP worker
  固定记录 Ray GPU 配额 0，正式 completion 的 task/actor 自动重试继续禁用。
- 后续回归补齐 Python executor 的非适用哨兵：actor concurrency/CPU 为 0/0.0，
  避免访问可选 `RayWorkerOptions`；Ray task 记录实际 CPU，但对外 actor-only
  concurrency 字段也为 0。
- review 修复让 fake Ray task/actor 统一应用 CPU、零 GPU、禁重试/重启 options；
  两处 submit metrics 合并循环抽为同一 helper，覆盖多 chunk 逐 worker 计数累加、
  缺字段兼容与 worker 宽度不一致拒绝。
- `append_metrics` 现在为新/空 CSV 写 header，并在非空 CSV 的既有 header 与 row
  keys 不精确一致时于追加前抛出 `ValueError`，防止 schema 演进造成静默列错位。
- 文档明确 service endpoint 不等于 actor worker，并给出
  `endpoint × workers/endpoint × actor max concurrency` 的配置并发上界。
  多 GPU 性能仍待独立 GPU-backed endpoint 验证，本次只建立执行与观测契约，
  不新增实验性能结论。

## 2026-07-26 Row-cap-aware packing 与非阻塞 adaptive 观测门禁

- **代码**：typed adaptive admission 的指标抓取从提交决策线程移到既有后台采样器，增加 `sample_age_s` 观测/控制轨迹字段和异常路径关闭测试；新增 BFD-inspired row-cap-first 纯装箱函数，Arrow 与 Daft 复用同一 membership 实现，顺序 token-budget 仍为默认。
- **验证**：完整测试 235/235 通过；真实 Daft→Ray task/actor 合约连续 3 轮、共 12/12 通过；64 行真实 PostgreSQL→Daft→Ray→vLLM 门禁最终 6/6 runs、384/384 requests、0 incident，行数/token 约束、外键、资源轨迹、FLOP 增量和 MFU 全部通过。
- **环境根因**：vLLM 0.25.1 未开启 `--enable-mfu-metrics` 时仍暴露零值 FLOP counter。首次门禁因此被判无效并废弃；保留旧容器为 `ai-operator-vllm-qwen-pre-mfu-20260726`，等价重建当前服务并只增加 MFU 开关，单请求确认 counter 增长后重跑。
- **边界**：64 行仅是基础设施正确性证据；单次 formal 数据不用于策略性能排序。下一步按预注册规则做 512 行 row cap × token budget × algorithm 筛选，未通过者不进入 1024。

## 2026-07-26 Output-aware BFD 真实 512/1024 规模验证

- 修复后 64 行门禁完成 12/12 runs；512 行六单元矩阵完成 24/24 runs、
  0 incident，18 个 formal run 共 9,216/9,216 request successes。1024 行
  三场景确认完成 12/12 runs、0 incident，9 个 formal run 也有 9,216 条
  逐请求记录。
- 正式服务为 vLLM 0.25.1 + Qwen2.5-1.5B BF16，禁用 prefix cache 并开启
  MFU counter。MFU 分母使用 NVIDIA 官方 RTX Blackwell 表中的 RTX 5070
  密集 BF16 Tensor、FP32 accumulate 61.7 TFLOP/s，不使用 988 AI TOPS。
- 512 行中，BFD trace 相对同成本 sequential trace：rows/s +12.019%、
  request P95 -11.203%、energy -18.639%、MFU +13.906%；但相对 strongest
  practical baseline `seq_fixed` 仅 rows/s +1.384% 且 energy +2.474%。
- 1024 行没有复现：BFD trace 相对 seq trace rows/s -5.156%、request P95
  +4.318%、energy +6.801%、MFU -5.130%；相对 seq fixed rows/s -14.293%。
  BFD submission 数为 87，seq trace 为 77，budget utilization 0.782 vs
  0.884。当前证据否定“经典 BFD 全规模更强”，支持 row-cap-aware 联合搜索。
- 报告与原始数据位于 `experiments/results/output_aware_bfd_*_20260726/`。
  分支继续隔离，未合并 `main`，本轮不自动同步 Wiki。
- 绘图汇总默认指标从 33 项扩展到 63 项，补齐 rows/s、SLO、stage time、
  submission/batch shape 和 vLLM latency；旧 CSV 缺失的新字段以 `n=0`
  输出，不破坏历史结果读取。512/1024 `summary_long.csv` 已重新生成。
- 最终回归在真实 arrival-replay→Ray contract 中复现 3.6ms 的时钟域竞态：
  理想 replay deadline 偶尔晚于实际 epoch-shaped flush，导致 trace 中 submit
  看似早于 flush。修复后以实际观测 flush 为边界，将超前的 intended arrival
  clamp 到该边界；不放宽 lifecycle 校验。54 项调度测试通过，真实
  Daft→Ray contract 连续 3 轮共 12 次通过。离线 512/1024 结果不受影响。

## 2026-07-26 Output-aware BFD、离线逐请求 E2E 与资源效率指标

- **数据组织**：新增严格的输出成本模式和确定性 best-fit-decreasing
  装箱；Arrow 与 Daft 共用同一 membership 逻辑，保持每行是一条完整请求，
  并显式记录 packing algorithm/scope、预算利用率、超预算行和成本分位数。
- **成本边界**：`trace_target_output` 明确标记为未配对的 BurstGPT trace
  metadata，不声称是当前 Qwen prompt/model 的 oracle；后端
  `completion_max_tokens` 不受成本估计模式影响。
- **逐请求时延**：request trace schema 升至 v2，区分
  `replayed_arrival` 与 `offline_job_start`；离线 BFD 现在可记录每个 prompt
  从作业开始（含读取与组织）到完成的 E2E，跨 fetch chunk 的 submission ID
  不复用。
- **资源效率**：正式 run row 新增 GPU 利用率、显存、vLLM 压力、功率、
  积分能耗和每千 observed token 能耗。MFU 仅在显式提供 reviewed
  GPU 峰值与精度时输出；优先使用 vLLM
  `estimated_flops_per_gpu_total` 增量，旧版服务才回退到显式
  FLOPs/token，保留估计方法与时间口径；
  GPU utilization 不冒充 MFU。
- **实验规模**：64 行仅作真实组件门禁；六组策略使用同一 512 文档、
  1 次 warm-up + 3 次正式重复；512 审计通过后，仅对选中的 baseline 与
  adaptive 配置做 1024 行、3 次正式复验，不混算不同规模的 effect size。
- **新增入口**：`code/scripts/analysis/summarize_output_aware_bfd.py` 输出
  scenario/metric 长表统计，覆盖吞吐、E2E/tail、packing、GPU、能耗与 MFU。
- **验证边界**：当前完成的是单元与真实本地 Daft→Ray task/actor contract；
  GPU-backed PostgreSQL+Daft+Ray+vLLM 的 64/512/1024 数据尚待运行，暂不形成
  性能优越性结论。

## 2026-07-25 加速 arrival replay 正式实验设计

- **问题**：BurstGPT 官方时间戳单位是秒；当前 1024 行覆盖 52,184 秒，
  前 512 行覆盖 39,757 秒，直接按原速回放不适合本地单 GPU 重复实验。
- **设计**：新增显式 `arrival_time_scale`，只缩放首行归零后的回放时钟偏移，
  不修改数据库原始时间戳，不缩放 flush timeout/hard max，并把比例写入所有
  运行与 manifest 产物。
- **矩阵**：64 行 × 0.0001 做一次快速产物门禁；随后 512 行 × 0.0005，
  immediate/fixed-timeout/queue-adaptive 各 1 warm-up + 5 正式重复。
- **边界**：结果属于单 GPU、受控加速的 BurstGPT-derived workload 对比，
  不代表原始生产到达率、多 GPU scaling 或内部 PostgreSQL 18.3 平台。
- **新增文件**：
  `code_doc/superpowers/specs/2026-07-25-accelerated-arrival-replay-design.md`。
- **实施计划**：
  `code_doc/superpowers/plans/2026-07-25-accelerated-arrival-replay-implementation.md`，
  分为回放时钟缩放、profiler/trace 接线、真实门禁与正式矩阵三项。

## 2026-07-25 Arrival replay 与独立 flush 运行链路完成

- **实现**：新增完整行粒度 pending batch builder、单调时钟 arrival replay、
  immediate/fixed-timeout/queue-adaptive flush 组合，以及独立 flush trace。
  batching、flush、admission 三层职责分离，策略层不依赖 Daft、Arrow、Ray
  或 HTTP。
- **生产接线**：`postgres_ai_operator_profile.py` 新增 `--arrival-replay`、
  flush 策略/超时/hard-max/trace 参数；仅显式开启时改变旧离线吞吐路径。
  queue-adaptive 指标由后台采样，回放循环不执行网络 I/O，正常关闭等待采样
  生命周期结束。
- **真实契约**：使用真实 Daft 和本地 Ray task/actor 验证
  `0,0,20,100 ms` 到达序列、固定超时边界、每行恰好一次和确定性 fan-in。
  测试发现并修复 Daft `RecordBatch` 与只接受 `Table` 的适配缺陷。
- **验证**：完整代码测试 161/161 通过；真实契约连续运行 3 轮通过；compileall
  与 diff check 通过。该证据属于单元/集成契约，不能作为 GPU 性能结论。
- **下一步门禁**：单 GPU 上以真实 PostgreSQL + Daft + Ray + vLLM，固定
  token budget 6144、静态 K_max=8，对 immediate/fixed-timeout/
  queue-adaptive 各做 1 warm-up + 1 smoke；全部运行、请求、flush、control、
  resource 和 manifest 产物非空后再进入正式重复。

## 2026-07-24 Top 15 精读按学术标准重排（Orca/DistServe 进，SABER/Multi-Bin 出）+ Clockwork 补入 inventory

- **触发**：用户质疑 Orca（continuous batching / iteration-level scheduling 开山、开题正文"vLLM/Orca"并称 5 次、8 个实验计划引用）竟不在精读 Top 15。核查发现 Orca 已精读，只是被旧"对本项目贡献度"标准以"vLLM 覆盖其机制"为由排到 #16——与项目把它当一等文献引用自相矛盾。
- **重排标准（用户定）**：从 66 篇按**学术研究标准**（基础工作/核心技术/相关工作）选前 15，CCF-A 优先，极重要 arXiv 可破例，正好 15 篇。
- **新 Top 15**（CCF-A/顶会 12 + 重要 arXiv 3）：基础 4——vLLM、**Orca**、Ray、Clipper；核心 7——Sarathi-Serve、SGLang、**DistServe**、Splitwise、CONCUR、Ray Data Streaming、BucketServe；相关 4——Cortex AISQL、NeurDB、Galois、DB Perspective。
- **与旧版差异**：进 Orca（iteration-scheduling 开山）、DistServe（goodput/prefill-decode，CCF-A）；出 SABER（USL 理论，AIMD 已被 Clipper+CONCUR 覆盖）、Multi-Bin（length-align 理论，已被 BucketServe 工程代表）。arXiv 由 5 降至 3，保留的每篇都是某核心策略无 CCF-A 替代的唯一来源。
- **Clockwork**：补入 `research/ai_operator_literature_inventory.md`（v5，65→66 篇，OSDI×6→7、CCF-A 37→38），归入推理服务系统组；knowledge_hub §5.2 已引其为 queue-adaptive flush 的调度思想来源。精确题录待用户放入 `research/reference/clockwork_osdi2020.pdf` 后以扉页核实并登记 REFERENCE_INDEX（67→68）。
- **更新文件**：`research/top15_ranked_papers.md`（按新标准重写）、`research/ai_operator_literature_inventory.md`（v5+计数+CCF 统计）、`opening/literature/top15_reading_notes/`（拷贝集删 saber/multibin、加 orca/distserve）+ 其 README 清单。
- **未做**：未 `git commit`；Clockwork PDF 未登记（待用户提供）。

## 2026-07-24 机制优先级并入 experiment_status_and_gaps §4（撤回新建文件）+ plans/ 文档维护纪律

- **撤回**：上一步新建的 `experiments/plans/mechanism_experiment_index.md` 是同一错误的第三次重复（前两次：`submission_control_autoregressive_basis.md` 被要求合并回现有文档、plans/ 结构治理时被指出文档增殖）。用户指出"现有文档不能记录这些内容吗"——确认应并入 `experiment_status_and_gaps.md` §4：该文档即"下一步实验第一参考"，索引回答的"先试哪个机制"与 §4 P0 改进方向是同一问题。机制优先级表已并入 §4 首「候选机制优先级（跨论文）」；fatal flaw 指向 `strategy_design_literature_basis.md` §3.1（不重复）；耦合/多模态两行删除（已有 `cross_layer_killer_experiment.md` / `daft_ray_multimodal_reference.md` 承接）。新建文件已删，`PROJECT_INDEX.md`、`plans/README.md` 登记回滚。
- **新增规则（plans/ 文档维护纪律，写入 `plans/README.md`「文档维护纪律」节）**：(1) **默认并入现有文档不新建**——某类内容找到自然归属就并入，深度进 reading_notes / `*_reference.md`；只有所有现有文档都不合适才新建，且必须在 PROJECT_LOG 说明理由。(2) **计划文档只保留待做内容**——实验完成（结果已记 results + experiment_status_and_gaps）后，设计/变量/矩阵从对应计划文档删除；前提是 results 报告自包含设计。
- **未做**：未对现有计划文档（`data_organization_batching.md`、`service_scheduling_backpressure.md` 等）做"完成内容清理"pass——其中可能仍有已完成实验的存量，待确认后另起。

## 2026-07-24 全项目文档新鲜度审计与批量修正

- **触发**：用户指出文献归位只是个例，要求检查全部规则/说明文档是否反映当前状态。
- **方法**：4 个并行审计 agent（索引文档 / 26 个 AGENTS.md / 断链机械扫描 / 数值版本漂移）+ 主线逐条复核，只报核实过的出入。
- **修正（全部 P0/P1/P2）**：
  - **P0**：根 `README.md` 状态冻结在 07-18 前（把已完成的 vLLM baseline 写成"下一步"、运行命令指向已弃用 fake 管道）→ 重写"当前证据/近期目标/运行命令"对齐 `PROJECT_OUTLINE.md`；目录树修 6 处断链 + 补 `code_doc/`、`data/`、扩 `research/` 子树。
  - **P1 篇数漂移**：inventory 已升 v4=65 篇、精读 33 篇，下游未跟随——修 9 处"57→65"（research/README、knowledge_hub ×3、current_direction_and_plan、PROJECT_INDEX、baseline_reference、code/AGENTS）、4 处"16/19→33"（inventory header、code/AGENTS、reference/README、strategy_design_implementation_reference）；top15"立即行动项"过期块重写；4 处"待精读→已精读"（Lance、SABER、vLLM×2：orca/serverlessllm）。
  - **P1 方向/状态**：`motivation/AGENTS.md` 当前状态/下一步从 fake/"方向未定"更新为 GPU-backed 已完成 + 方向已收敛；`experiments/AGENTS.md` 第三项从"写回瓶颈判定"改为"多模态泛化验证"；根 `AGENTS.md` §3"下一步①建立 baseline"过期 → 改为当前缺口。
  - **P1 断链（共 12 处）**：`overview/project_outline.md`（README 树 + overview/README + overview/AGENTS，按"删引用"处理）、`feasibility/guide.md`/`analysis.md`、`feasibility/results/feasibility_report.md`/`current_direction_analysis.md`、`opening/outline.md`、`opening/navigation.md` echarts_rules.md、`opening/slides`+`projects`+`templates` 旧 pptx 名、`code/README`+`deploy/postgres18.4` feasibility 旧文件名（→ pg18_4_connection_*）、`learning/experiment_walkthrough` 图路径缺 `../`。
  - **P2**：根 `AGENTS.md` §4 目录表补 `deploy/`/`projects/`/`code_doc/`/`data/`；`motivation/results/AGENTS` 补 `cpu/`；headline 37.5×（operator 阶段）与 13.4×（端到端）口径标注统一。
- **审计中 agent 漏报、由复核揪出的 2 处**：`code/README.md` bash 示例里第二处旧 CSV 名（L169）、`serverlessllm_osdi2024.md` 也有"vllm(待精读)"——已补修。
- **未改（历史/边界，正确保留）**：`PROJECT_LOG.md` 与 `archive/` 内的"57/16/19/44/69 篇"为当时真值；inventory L3 v3=57 版本史；PG 18.3/18.4 区分、模型版本——审计确认全部无矛盾。
- **未做**：未 `git commit`（等用户确认）。

## 2026-07-24 文献精读语料从 opening/ 迁至 research/；开题 Top 15 拷贝留 opening/

- **触发**：用户指出 opening/ 是开题（阶段性）工作区，但全部文献精读笔记（44 篇）、PDF（69 个）、文献清单与评估都放在 `opening/literature/`，与 `research/`（项目级"背景调研、文献依据"目录）职责错位——opening 自己的 README/navigation 都写着"文献参考 research/"，research/README 却要标注"扩展文献不在本目录"打补丁。
- **迁移**（`git mv`，保留历史）：`opening/literature/{reading_notes,reference}` → `research/{reading_notes,reference}`；`opening/literature/{ai_operator_literature_inventory,top15_ranked_papers,gpu_scheduler_data_placement_supplement_20260715,direction_assessment_20260715}.md` → `research/`。`research/` 成为文献唯一归属，knowledge_hub 与原料同目录。
- **opening 保留**：`opening/literature/reading_list.md`（开题精读优先级清单）+ 新增 `opening/literature/top15_reading_notes/`（开题要求精读的 15 篇笔记拷贝 + figs，自包含快照，权威版在 `research/reading_notes/`）。
- **链接/索引同步**：全仓库批量替换 6 类已搬走路径（`opening/literature/{reference,reading_notes}/` + 4 个 md）；手动修索引文档——`research/README.md`（"扩展文献不在本目录"段改为"本目录内"并补 reading_notes/reference 条目）、`research/knowledge_sync_guide.md`（手动映射表头 + 触发规则）、根 `AGENTS.md` §11 触发规则中的知识目录列表、`opening/README.md` 与 `opening/AGENTS.md` 的 `literature/` 职责、`figures/audit/strategy_figure_micro_design_points.md`、`PROJECT_INDEX.md`（补 `research/reading_notes/`、`research/reference/`、inventory、top15 等条目）。
- **连带影响（重要）**：同级 wiki 仓库 `../ai-operator-wiki/sync-wiki.sh` 的 reverse-sync 路由与 `[2/5]`、`[3/5]` 段全部耦合 `opening/literature/`，已同步改为 `research/`（`raw/inventory` 分流：`reading_list` 回 opening、其余回 research；`raw/analysis` 的 direction/gpu_scheduler 并入 research/ 分支）；否则下次同步会静默丢笔记/PDF。
- **未做**：未 `git commit`（等用户确认）；未改笔记内容语义（仅改路径）；笔记中指向项目外 `raw/papers/` 的 paper 库引用不动。

## 2026-07-23（第四次）PDF 全量规范化改名 + 误下载/重复清理 + 精读推荐 15 篇

- **触发**：用户要求精读下一批论文前，先把 `reference/` 下 69 个混乱命名的 PDF（arXiv 号、`pxxxx-author`、`osdi24-xxx`、中文标题混存）统一改名，并清理误下载/重复。
- **改名规范**：全部统一为 `短名_会议年份.pdf`，与 `research/reading_notes/` 精读笔记一一对应（如 `vllm_sosp2023.pdf` ↔ `vllm_sosp2023.md`）。15 个 git 跟踪文件用 `git mv` 暂存为 rename（保留历史），其余本地 `mv`。
- **清理误下载 3 篇**（arXiv ID 被重新分配导致内容错位）：`diskann_neurips2019.pdf`（实为凝聚态物理）、`milvus_sigmod2021.pdf`（实为 IR 词典翻译）、`dostoevsky_sigmod2018.pdf`（实为代数几何）。真 DiskANN/真 Milvus 已重新获取；Dostoevsky 暂不补（写回 LSM 背景，优先级低）。
- **清理重复 2 篇**：FlashAttention、FlexGen 各保留正式会议命名副本（NeurIPS/ICML），删除 arXiv 号重复副本。
- **补齐 3 篇**：真 Milvus（SIGMOD 2021, DOI:10.1145/3448016.3457550）、Clipper（NSDI 2017）、CoLoRA（ASP-DAC 2026, DOI:10.1109/ASP-DAC66049.2026.11420717）。
- **CoLoRA 撞名警示**：arXiv 上至少 4 个"CoLoRA/CoLoRa"不同领域论文（CNN-PEFT / PDE-降阶 / LoRa-无线网络 / 多租户 LLM 调度）。正确那篇仅 IEEE 付费墙可得（无 arXiv），已通过完整标题命中获取 `colora_aspdac2026.pdf`；三次 arXiv 搜索均命中撞名论文。
- **索引同步**：`REFERENCE_INDEX.md` 重写（67 篇按 7 类重组、计数 52→67、未下载清单修正、新增规范化记录附录）；`reference/README.md` 计数修正；全项目 `.md` 中旧 PDF 文件名引用经 sed 批量替换为新名（仅替换 `.pdf` 后缀的文件引用，纯 arXiv/DOI 文献引用不动）。
- **库现状**：67 个 PDF，全部规范命名，无错误/重复。
- **精读推荐**：用户要求按"全部未读"假设推荐 15 篇，应用 T1(综述)→T2(最近前人工作)→T3(核心技术) 排序；判断不可外包的 ⭐8 篇需用户亲自精读（Cortex AISQL、Galois、Ray Data Streaming Batch、vLLM、DB Perspective、Splitwise、Clipper、SGLang），其余交 agents 批量精读。
- **更新文件**：`research/reference/*.pdf`（改名）、`REFERENCE_INDEX.md`（重写）、`reference/README.md`、`PROJECT_LOG.md`、以及含旧文件名引用的若干 `.md`。

## 2026-07-23（第三次）编码规范与代码架构文档落地

- **触发**：用户确认综合评估与项目已有记录一致，要求将新增内容写入对应文档，并重申"设计优先使用知识库和精读论文中的知识"。
- **更新文件**：
  - `experiments/plans/strategy_design_implementation_reference.md` — 新增 §8 "目标代码架构与模块接口规范"，定义 4 个新模块（admission/routing/request_pool/pipeline）的接口规范、文献来源、实现优先级。每个设计决策标注文献出处（Clipper NSDI'17、CONCUR 2025、SABER 2025、CoLoRA 2026、SGLang NeurIPS'24、Parrot OSDI'24 等）。
  - `code/AGENTS.md` — 新增"编码规范"节，6 条规则：① 保持简单 <100 行（Ray ConcurrencyCap 废弃教训）；② 每行=独立完整请求（vLLM chunked prefill 语义安全）；③ 策略层不依赖引擎层（DataOrganizer 抽象）；④ 多模态复用文本代码路径；⑤ 文献优先——新机制从精读笔记提取；⑥ 新实验指标完整性（tokens/s + service_p99 + 时间序列）。每条规则标注文献来源。
- **设计原则重申**：所有机制设计、策略选择、基线对比，优先从项目 57 篇 CCF-A 文献 + 16 篇精读笔记中提取设计模式和候选方案，不凭空设计。方法论见 `research/README.md` §文献优先设计方法论 和 Wiki `设计方法论` MOC。

## 2026-07-23（第二次）全维度综合评估：Wiki 知识库 + 文献精读 + 代码架构 + 后续路线图

- **触发**：用户要求结合 Wiki 知识库（206 实体、4 MOC）、16 篇精读论文笔记、项目代码和实验状态，做全维度综合评估。
- **方法**：使用 idea-evaluator（五维打分）、nature-reviewer 视角、deep-research 文献验证、karpathy-guidelines 严谨性控制。
- **五维得分**：Higher=7, Faster=6, Stronger=5, Cheaper=6, Broader=7。Faster 和 Stronger 为当前瓶颈维度，需要 adaptive 控制器数据和 scale-out 验证来提升。
- **代码架构评估**：现有 6 模块（sources/organizers/model_backends/sinks/metrics/workloads）分离清晰，策略-引擎抽象合理。缺少 3 个核心模块：admission.py、routing.py、request_pool.py。pipeline.py 编排层缺失。`model_backends.py` 用 urllib 手工 HTTP 请求，无重试/连接池/streaming。bin-packing 分组策略未实现。无 CLIP/VLM backend。
- **目标架构**：8 模块（+ admission / routing / request_pool / pipeline），接口规范已定义。admission 使用 AIMD + EWMA + per-submission check，request_pool 按 operator_type 分 bucket。
- **代码实现注意事项**：(1) 保持简单 <100 行（Ray ConcurrencyCapBackpressurePolicy 废弃教训）；(2) 每行=独立完整请求（语义安全红线）；(3) 策略层不依赖引擎层（DataOrganizer 抽象）；(4) 多模态复用 token-budget 代码路径；(5) 文献优先——新机制从精读笔记提取；(6) 所有新实验包含 tokens/s + service_p99 + inflight 时间序列。
- **最短路径（6-8 周）**：Week 1-2 P0 修复 → Week 3-4 P1 补齐 → Week 5-6 多模态前置 + 代码补全 → Week 7-8 多模态实验 + 论文写作。
- **后备路径（adaptive 失败时）**：RC2 降级为"K_max 必要性论证 + 跨查询请求路由（方法补充）+ queue-adaptive 探索（Discussion）"。
- **风险最高项**：Adaptive 3 轮后仍不如 static（概率 40%）。后备路径已准备。
- **认知债务**：6 篇 2025-2026 新论文未精读、baseline_reference 20 个 baseline 仅 <5 运行、设计决策日志为空、Daft 引擎参数未探索、开题报告与实验状态不同步。
- **关键结论**：(1) 课题定位成立（四岛空白双重确认 + 57 篇 CCF-A 文献支撑）；(2) 最高优先级只有让 adaptive 工作，不成功则 4 周后切换后备路径；(3) 跨查询请求池 + 算子路由是多模态前置依赖，不是 afterthought；(4) 文献基础扎实但未充分利用，投稿前需补齐新论文精读笔记和精确的 Related Work 区分度论证。
- **文件**：`experiments/plans/experiment_status_and_gaps.md` §6（已有审计，可考虑补充代码架构部分）

## 2026-07-23 完整问题审计：P0/P1/P2 分级 + 认知债务清单

- **触发**：用户要求对项目当前状态做系统性审计，识别除"ML as Native Operator"叙事定位之外的全部问题。讨论中使用了 idea-evaluator（五维评分 + fatal-flaws audit + paradigm-shift probe）、nature-reviewer（三审稿人模拟）、deep-research、karpathy-guidelines 和 brainstorming 六种技能交叉评估。
- **"ML as Native Operator"叙事问题**：搁置至后续阶段。用户的三区别框架（语义感知查询重写 / 跨查询 continuous batching / 两层嵌套代价模型）是有价值的分析工具，但当前项目未实现区别 1（DB 内核改动），所有优化在 Ray 中间层。当前阶段聚焦外部执行链路优化，不涉及数据库内核。
- **跨查询 batching 澄清**：vLLM 内部做 continuous batching（隐式），但 Ray 层无显式的"跨查询请求融合"机制。Shared-vLLM K_max Interference 是两 job 共享同一 endpoint（跨查询共享服务），不是跨查询主动合并请求。如论文需 claim 此能力，需实现全局请求池 + 按算子类型/prefix hash 维度合并。
- **多模态场景下的跨查询 batching（2026-07-23 补充）**：纯文本场景下 vLLM 的 continuous batching 掩盖了"无跨查询请求池"的 gap——所有请求走同一 vLLM endpoint，vLLM 内部自动合并。但在多模态场景下，CLIP embedding 没有 continuous batching，不同查询的 AI_EMBED 请求必须显式合并才能保证 GPU 利用率。跨查询请求池 + 算子类型感知路由是多模态实验的工程前置条件，不是可选的。如果 RC2 adaptive 在 P0 阶段降级，跨查询合并可作为 RC2 的方法补充贡献。
- **核心发现（P0 阻塞级）**：
  1. RC2 核心策略为负结果：queue-adaptive flush 的 foreground E2E=10.2s vs static K_max=8 的 7.3s（~40% 差距）。放弃条件：3 轮改进后仍不达 static 的 90% → RC2 降级为"K_max 必要性论证 + adaptive 探索性讨论"。
  2. 两项策略联合消融（独立拼接 vs 联合 grid search）完全没有数据——AGENTS.md §1 写死的核心验证实验。
  3. `tokens/s` 指标缺失——`rows/s` 在 AI_COMPLETE 场景下是有偏指标（每行 token 量可差 13.9×）。此外 `service_p99`、inflight/queue 时间序列、per-request e2e latency 分布均缺失。
- **P1 严重级**：
  4. Prefix-aware 在自然 workload 上信号太弱（6.4% prefix ratio），受控 workload（0/30/70/100%）未做。
  5. Length-align + fixed rows 是负结果（token P95=33407），正确组合（length-align + token-budget）未做正式对照。
  6. 所有实验 512 行规模，无 scale-out 验证（2048 行）。
  7. Token-budget tradeoff 未系统表征（token tail vs HTTP call count）。
- **P2 方法论/设计问题**：
  8. Daft 引擎级参数实验空间为零——"策略级 + 引擎级"优化空间仅覆盖了策略级。
  9. 离线扫表（doc_id 序）与 arrival-aware 实验间存在叙事断层。
  10. Baseline 矩阵（baseline_reference.md 20 个 baseline）大量未实际运行。
  11. 无多 endpoint/多 GPU 实验。
  12. 跨查询 batching 是隐含效果而非显式策略（见上）。
- **认知债务**：文档承诺 vs 实际交付存在系统性差距——baseline 矩阵、引擎级参数表征、实验五阶段计划、actor pool 分池路由均存在文档写了但实验未做的情况。投稿前必须清理。
- **最短可交付路径**：第 1 周修 adaptive 控制器（≥90% static 或立即降级）+ 联合消融 + 补齐 `tokens/s`/`service_p99`；第 2 周后 prefix 受控 workload + 2048 行 scale-out。
- **更新文件**：`experiments/plans/experiment_status_and_gaps.md`（新增 §6 完整问题审计，含 P0/P1/P2 分级 + 认知债务清单）、`PROJECT_LOG.md`

## 2026-07-22 文献精读笔记批量完成（12 篇新增）

- **触发**：用户要求对 `research/reference/` 中的文献按 `tpl-文献精读-深度版.md` 模板做精读。
- **操作**：使用 12 个并行 Agent 同时阅读 PDF 并生成精读笔记，每篇严格遵循四层模板（基本信息 → 论文结构分析 → 批判性评估 → 与课题连接）。
- **新增笔记**：
  - DB4AI 组：`neurdb_cidr2025.md`、`leads_pvldb2024.md`、`inferdb_pvldb2024.md`、`smartlite_pvldb2024.md`
  - AI 推理服务组：`vllm_sosp2023.md`、`orca_osdi2022.md`、`sarathi_serve_osdi2024.md`、`serverlessllm_osdi2024.md`
  - 综述组：`llm4dm_pvldb2024.md`、`db_perspective_llm_pvldb2025.md`
  - 基础设施组：`ray_osdi2018.md`
  - 持久化组：`diskann_neurips2019.md`
- **总计**：`reading_notes/` 目录现有 16 篇精读笔记（4 篇旧 + 12 篇新）+ 2 模板，覆盖 `ai_operator_literature_inventory.md` 中 15 篇建议精读的全部文献 + DiskANN 补充精读。
- **已知问题（2026-07-23 已解决）**：原 `reference/diskann_neurips2019.pdf` 内容为凝聚态物理论文（arXiv ID 被重新分配）——当日已用真 DiskANN（arXiv:1811.01324）替换并清理误下载，详见 PROJECT_LOG 第四次条目。
- **更新文件**：`reading_notes/*.md`（16 篇精读笔记）、`reading_list.md`、`reference/README.md`、`PROJECT_LOG.md`

## 2026-07-21 Token 元数据来源与技术细节记录规范

- **触发**：导师追问 token-aware batching 中“每行 token 怎么获取、怎么用于分组”，用户要求把这类技术细节记录到合适文档，并明确后续代码完成时同步记录实现细节。
- **决策**：不新建单独技术细节文档；当前内容归入既有实验计划和实现参考，避免入口分散。
  - `experiments/plans/data_organization_batching.md` 记录每行 `prompt_tokens` 的来源、tokenizer 一致性要求、`prompt_tokens + completion_max_tokens` 组批公式、超长行边界和实验必须记录字段。
  - `experiments/plans/strategy_design_implementation_reference.md` 记录 Workload Profiler / DataOrganizer / `BatchRequest` 需要携带的 token 元数据，以及 CSV/审计指标口径。
- **后续规则**：以后完成涉及调度策略、workload 导入、DataOrganizer、CSV 字段或 vLLM 指标采集的代码时，同步检查上述两个文档；若新增字段、公式、fallback 或边界条件，必须在对应文档和具体结果 README 中记录。

## 2026-07-21 开题报告图位优化与正文分析强化

- **触发**：用户要求将图放到正文合适位置，在正文中提及并讲解分析每张图，报告可以比 PPT 图多、讲解更清晰。同时注意图文一致性。submission_control 的三张新图暂不写入报告。
- **操作**：对 `opening/report/opening_report.md` 中所有 9 张图进行了系统性的图位优化和正文分析强化（详见上一版记录）。随后将更新后的报告覆盖同步到飞书 docx（revision 244），上传 9 张图后在 XML 层获取 block ID，逐张移动到对应图注段落之后（revision 263–271），使每张图紧跟在正文中对应的图注文字下方。
- **飞书同步**：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`，revision 271，文本+图片均已到位。图片位于对应图注之后（"图注文字 → 图片"顺序，可读）。
- **更新文件**：`opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md`、`PROJECT_LOG.md`

## 2026-07-21 开题报告飞书 docx 同步

- **触发**：用户要求将开题报告最新修改同步到飞书，与答辩 PPT 内容一致。
- **操作**：
  - 将 `opening/report/opening_report.md` 的最新内容复制到本地源稿 `opening/feishu/opening_report_wiki.md`。
  - 使用 `lark-cli docs +update --command overwrite`（user 身份）覆盖写入飞书 docx：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`，飞书返回 `partial_success`（本地图片路径无法直接导入，预期行为），文档 revision 更新为 `221`。
  - 逐一上传 9 张图到飞书 docx（research_gap_three_islands / system_architecture_ai_data_execution / cross_layer_method_framework / runtime_strategy_control_loop / 10_e2e_operator_writeback_breakdown / 07_gpu_pgai_rerun_stage_writeback_20260714 / 08_gpu_pgai_rerun_endpoint_comparison_20260714 / 09_gpu_pgvector_writeback_comparison_20260714 / b26_arrow_vs_daft_stage_breakdown），均带中文图注。
- **本次同步的主要变更**（与旧版飞书内容相比）：
  - 题目从"面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究"改为"数据库 AI 负载的执行优化与调度研究方向"。
  - 写回 baseline 从"工程最优写入方案"统一改为"PostgreSQL + pgvector 的 COPY + deferred index"。
  - 研究内容中增加"多模态泛化验证"（图像 workload：AI_EMBED/AI_CLASSIFY，CLIP/Qwen2.5-VL）和"算子代价估计"补充讨论。
  - 研究方案新增"多模态泛化验证"实验段落，描述 token 预算到 frame 预算的参数迁移和 Daft pipeline 复用。
  - 进度安排更新：7 月已完成的 4 项打 ✅（开题报告、vLLM baseline、Daft 接入、token-tail revision）；10 月新增多模态泛化验证启动。
  - 新增 Daft vs Arrow 管线开销对比（图 4-7），支撑 Daft 作为后续 AI_COMPLETE 及多模态实验数据引擎的可行性。
- **图与正文一致性**：已检查图 4-3～4-7 的图注与正文描述一致，图 4-7 为新图，正文已包含对应的 arrow_postgres vs daft_postgres 分析段落。
- **未完成**：图片位于文档末尾，未移动到正文对应位置（与历史同步行为一致，受限于 markdown overwrite 不保留 block 级图片位置）。

## 2026-07-21 Ray 队列自适应提交机制调研与文献补充

- **触发**：用户提问"Ray 是否有现成的队列自适应提交机制，还是需要自己实现"。
- **调研方法**：WebSearch（Ray 官方文档、GitHub PR/issues）+ 学术文献搜索（2025-2026 LLM serving 论文）。
- **核心结论**：
  1. **Ray 无现成的 queue-adaptive flush 或 K_max 动态控制 API**——这些是课题需自建的核心贡献。
  2. Ray 提供丰富的 building blocks（`ray.wait()` 手动反压、`max_concurrency`、`max_tasks_in_flight` + `should_add_input()`、Serve queue-based autoscaling）。
  3. Ray Data 曾有一个几乎就是"自适应并发控制"的 `ConcurrencyCapBackpressurePolicy`（EWMA + deadband），但**已被废弃**——~400 行控制逻辑，性能反而不如简单方案。这是重要的 cautionary tale：我们的自适应策略必须保持简单。
  4. 发现 6 篇 2025-2026 年新论文与本课题直接相关——最相关的是 CONCUR（AIMD-based agent-level admission control, 4.09× throughput）和 BucketServe（按序列长度分组，3.58×，与我们的 length-aligned grouping 同构）。
- **更新文件**：
  - `research/ray_actor_dynamic_batching_reference.md`：
    - 新增 §1.6 `max_queued_requests` 准入控制
    - 新增 §1.7 Queue-Based Autoscaling（PRs #59430, #59548, #59351）
    - 新增 §1.8 Custom Autoscaling Policies（Ray 2.51+）
    - §3.7 大幅扩展：ConcurrencyCapBackpressurePolicy 废弃详情（EWMA/deadband/废弃原因）→ DownstreamCapacityBackpressurePolicy 替代方案 → `max_pending_calls` → `max_tasks_in_flight` + `should_add_input()` + `num_free_slots()` → `_actor_generator_backpressure_num_objects`
    - 新增 §6.7 CONCUR (2025)、§6.8 Scorpio (2025)、§6.9 SABER (2025)、§6.10 CoLoRA (2026)、§6.11 BucketServe (2025)、§6.12 ProServe (2025)
    - 附录 URL 清单扩充 15 条
  - `research/knowledge_hub.md`：
    - 新增 §5.5 从 6 篇新论文提取的设计原则
    - 新增 §5.6 Ray 现存机制的能力边界（building blocks vs 需自建）
    - §8 知识缺口新增 3 项（CONCUR 迁移可行性、USL 建模、ConcurrencyCap 教训）
    - §9 文件清单新增本次更新条目
- **对课题方向的影响**：无需改变方向——确认了"Ray 不提供现成机制，需要自己实现"的判断是正确的。新增文献进一步验证了 adaptive admission control + length-aligned grouping 两条线的研究活跃度。CONCUR 是最需要优先精读的论文。

## 2026-07-21 课题名称更新 + 实践计划表审查

- **触发**：用户审查实践计划表，要求将课题名称从"面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究"改为更精简的表述。
- **题目变更**：新题目为 **"数据库 AI 负载的执行优化与调度研究方向"**。
- **实践计划表发现**：四个阶段内容中存在多处与当前方向不一致的措辞：Phase Ⅱ "三项研究内容"应改为"两项"（写回已降为实验设置）、Phase Ⅱ "提交控制策略"应补全为"调度与提交控制策略"、Phase Ⅲ workload 列表（embedding 批量写入 / AI predicate 过滤与分类 / 离线 LLM 生成与抽取）需替换为当前主线（AI_COMPLETE 文本主场景 + AI_EMBED/AI_CLASSIFY 图像多模态泛化验证）、vLLM 不再是可选项。
- **更新文件**（题目替换）：
  - `AGENTS.md` §1
  - `README.md`
  - `PROJECT_OUTLINE.md`
  - `PROJECT_INDEX.md`
  - `opening/report/opening_report.md`
  - `opening/feishu/opening_report_wiki.md`
  - `opening/slides/opening_ppt.md`
  - `opening/practice_plan.md`
  - `research/literature_and_evidence_review.md`
- **未更新**：`PROJECT_LOG.md` 和 `opening/logs/project_log.md` 中历史条目保留旧题目（历史记录不应修改）。

## 2026-07-20 实验状态全面审计与缺口分析

- **触发**：用户要求对当前实验进展做系统性评估，回答"现在的实验能说明什么、还需要做什么"。
- **评估方法**：结合 idea-evaluator（五维评分 + fatal-flaws audit + paradigm-shift probe）、ars-reviewer（模拟三审稿人）、deep-research 和 vibe-research-workflow 四种视角。
- **核心发现**：
  1. **动机链完整**：token-tail revision 证明了"固定行 batch 是计算量的弱代理"，shared-vLLM interference 证明了"K_max 在共享 vLLM 下必要"——这两个动机实验质量好，可直接写进论文。
  2. **RC1 策略机制已验证**：token-budget batching 能约束 token tail（P95 从 26678→6144），但存在 tradeoff（4096 吞吐较低）。
  3. **RC2 策略未验证**：queue-adaptive flush 已实现但不如静态 K_max=8（foreground E2E 10.2s vs 7.3s）。这是当前最高风险的 gap。
  4. **两项策略联合消融缺失**：完全没跑过独立拼接 vs 联合 grid search。
  5. **指标盲区**：缺 `tokens/s`（比 rows/s 更公平的 AI_COMPLETE 效率指标）、缺 inflight/queue 时间序列、缺 per-request latency 分布、缺系统性 `service_p99`。
- **新建文件**：
  - `experiments/plans/experiment_status_and_gaps.md`：完整的状态-缺口-路线图文档，包含已完成/未完成实验表、证据链评估、指标盲区、P0/P1/P2 实验路线图、审稿人视角的拒绝风险。
  - `learning/metric_selection_methodology.md`：AI_EMBED vs AI_COMPLETE 观察变量选择方法论，解释为什么从"阶段时延拆分"转向"请求形状 + 服务端压力 + 端到端分布"的四层变量体系。
- **更新文件**：
  - `PROJECT_OUTLINE.md`：§当前最重要证据 重写为以本地 vLLM baseline 为首要证据；§近期优先级 重写为已完成项 + P0/P1/P2 缺口 + 指标盲区 + 新增 adaptive 放弃条件。
  - `experiments/plans/README.md`：新增状态审计文档入口。
  - `learning/README.md`：新增指标方法论文档入口。
  - `experiments/results/local_vllm_qwen15b_baseline/README.md`：§Remaining Formal Experiments 重写为结构化的下一步清单。
- **idea-evaluator 裁决**：Accept with Revisions。Higher 6, Faster 7, Stronger 8, Cheaper 5, Broader 6。Paradigm-shift potential possible（3.5/4）。两个 MAJOR flaw（adaptive < static、单 GPU 限制），均有明确修复路径。
- **ars-reviewer 共识**：动机实验扎实，但 adaptive < static 和缺乏联合消融是两个 MAJOR concern，如不修复在 VLDB/SIGMOD 级会议上大概率被拒。

## 2026-07-19 Shared-vLLM K_max interference experiment

- Added `code/scripts/experiments/run_kmax_interference_experiment.py`, a wrapper around
  `postgres_ai_operator_profile.py` that runs a foreground small job while a
  background bulk job shares the same local vLLM endpoint.
- Ran the first two-job `AI_COMPLETE` interference experiment:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv`
  and
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv`.
  Foreground: 128 rows, fixed16, `K_max=8`, `completion_max_tokens=64`.
  Background: 1024 rows observed in CSV, fixed16,
  `completion_max_tokens=64`, comparing `K_max=8` versus unbounded
  (`max_inflight=100000`). Both jobs share
  `http://localhost:8000/v1/completions`.
- Result: foreground small-job E2E averaged 4.923s solo, 6.535s during bounded
  bulk, and 11.384s during unbounded bulk. Foreground service P95 rose from
  2.580s under bounded bulk to 4.386s under unbounded bulk; vLLM queue mean
  rose from 0.001s to 0.445s.
- Interpretation boundary: this supports the K_max admission-control motivation
  under a shared vLLM service. It does not imply K_max is necessary when every
  job has an exclusive vLLM endpoint, and it is not yet a full fairness/SLO
  sweep.
- Added `figures/data/backup/b21_local_vllm_kmax_interference_small_job.*` and
  updated the local baseline README, code script README, figure README, audit,
  and `PROJECT_INDEX.md`.

## 2026-07-19 Batch policy x K_max AI_COMPLETE scheduling matrix

- Adjusted the scheduling baseline design after reviewing the role of `K_max`:
  `K_max` is admission control over already-formed Ray submissions, so it must
  be tested jointly with upstream batch shape rather than as a substitute for
  batch construction.
- Ran the local ShareGPT/BurstGPT `AI_COMPLETE` matrix:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_batch_policy_kmax_matrix_20260719.csv`.
  Matrix: fixed rows `32/64/128`, token budgets `4096/6144/8192`, and
  `K_max=2/4/8/16/unbounded`; 512 rows, `source_order=arrival_time`,
  `ray_task`, Daft source/organizer, local vLLM Qwen2.5-1.5B,
  `completion_max_tokens=16`, no writeback, warmup 1, formal repeats 3.
  All 120 rows completed with `status=ok`.
- Result boundary: too-small `K_max` increases Ray-side bounded wait and
  end-to-end time; larger `K_max` mostly improves or plateaus E2E in this local
  offline job, while raising vLLM queue/service-tail pressure at high inflight.
  This matrix does not prove that `K_max` is required for end-to-end
  improvement.
- Batch shape remains necessary in the analysis: `fixed128` creates only 4
  upstream requests, so `K_max>4` has little scheduling space, while token P95
  remains about 26680. Token-budget settings bound token P95 but create more
  requests, making admission control observable.
- Added `figures/data/backup/b18_local_vllm_batch_kmax_e2e.*`,
  `figures/data/backup/b19_local_vllm_batch_kmax_service_pressure.*`, and
  `figures/data/backup/b20_local_vllm_batch_kmax_request_granularity.*`.
  Updated the local baseline README, figure README, audit note, and
  `PROJECT_INDEX.md`. The earlier
  `sharegpt_burstgpt_arrival_kmax_token6144_20260719.csv` / `b17` run is now
  documented as a preliminary single-shape K_max sweep.
- Started and then stopped a heavier offline stress sweep after determining it
  would still not establish the right motivation for `K_max`. The next
  scheduling experiment should instead use a real admission-control objective:
  multi-job or burst arrival workload, shared vLLM endpoint, SLO tail latency,
  timeout rate, queue-length peak, and fairness.

## 2026-07-19 Token-budget vs fixed-row AI_COMPLETE baseline

- Added upstream `--batching-policy fixed_rows|token_budget` and
  `--token-budget` support to `code/scripts/profiling/postgres_ai_operator_profile.py`
  through `code/src/organizers.py`. Token-budget batching greedily groups rows
  by estimated `prompt_tokens + completion_max_tokens` before Ray submission;
  it does not modify Ray or vLLM internals.
- Added CSV fields `batching_policy`, `token_budget`, and
  `model_request_timeout_s`, plus organizer unit tests for token-budget batch
  construction.
- Ran the local ShareGPT/BurstGPT `AI_COMPLETE` policy matrix:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv`.
  Matrix: fixed rows `16/32/64/128` versus token budgets `4096/6144/8192`,
  512 rows, `ray_task`, Daft source/organizer, local vLLM Qwen2.5-1.5B,
  `max_inflight=8`, no writeback, warmup 1, formal repeats 3, request timeout
  300s.
- Result boundary: token-budget controls request token tail and queue pressure
  (`4096/6144/8192` token P95 near budget, versus fixed 64/128 at 16377/26677),
  but throughput is a tradeoff. `4096` is most queue-stable but slower;
  `6144/8192` approach fixed 32/64 throughput while keeping token P95 much
  lower than fixed 64/128. This supports dynamic batching motivation, not the
  full method claim.
- Added `figures/data/backup/b15_local_vllm_token_budget_throughput.*` and
  `figures/data/backup/b16_local_vllm_token_budget_tail_queue.*`, then updated
  the local baseline README, figure README, audit note, and script README.
- Recorded the remaining local baseline follow-up list in
  `experiments/results/local_vllm_qwen15b_baseline/README.md`: arrival-aware
  `K_max` sweep next, queue-adaptive flush after that, length-align and
  prefix-aware ablations later, and COPY + deferred-index writeback deferred.

## 2026-07-19 PostgreSQL source-order mode for AI_COMPLETE profiles

- Added `--source-order doc_id|arrival_time` to
  `code/scripts/profiling/postgres_ai_operator_profile.py` and propagated the value into
  CSV rows.
- Updated `code/src/sources.py` so both `PostgresArrowSource` and
  `DaftPostgresSource` share the same source-order semantics:
  `doc_id` for offline throughput/data-organization scans, and
  `arrival_time_s NULLS LAST, doc_id` for arrival-aware service scheduling
  experiments.
- Updated `code/tests/data/test_sources.py`, `code/scripts/README.md`,
  `experiments/results/local_vllm_qwen15b_baseline/README.md`,
  `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md`,
  `learning/local_vllm_ray_baseline_walkthrough.md`, and `PROJECT_INDEX.md`.
- Boundary: existing 2026-07-18/2026-07-19 local baseline CSVs should be read
  as `doc_id` offline-throughput runs. Future K_max, queue-adaptive flush, and
  backpressure experiments should use `--source-order arrival_time` when the
  claim depends on request arrival rhythm.

## 2026-07-19 Local vLLM fixed-row baseline token-tail revision

- Added the 2026-07-19 Ray task token-tail sweep CSV:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch128_token_sweep_20260719.csv`.
- Revised the baseline interpretation from a plain row-batch sweep to a
  fixed-row proxy limitation test: larger row batches reduce request count, but
  token P95 and service P95 grow sharply and local throughput plateaus around
  16-32 rows and remains flat through the 64/128 stress points.
- Updated `figures/scripts/generate_local_vllm_ray_baseline_charts.py` to add
  `b11_local_vllm_token_tail_performance.*` and
  `b13_local_vllm_token_tail_penalty.*` as the main token-tail motivation
  figures, then added `b14_local_vllm_service_tail_gap.*` to isolate the
  service P50-to-P95 tail gap.
- Revised `b10` into `b10_local_vllm_request_count_inflight.*`, using two
  aligned panels for model-service call count and in-flight utilization instead
  of mixing both quantities on one axis.
- Updated `experiments/results/local_vllm_qwen15b_baseline/README.md`,
  `figures/README.md`, and
  `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` with the
  revised baseline question, command, result table, boundaries, and figure
  roles.

## 2026-07-18 Local vLLM Ray baseline figures and learning note

- Added `figures/scripts/generate_local_vllm_ray_baseline_charts.py` to
  regenerate backup figures from the ShareGPT/BurstGPT local
  `AI_COMPLETE` baseline CSVs.
- Generated separate single-purpose figures instead of a dashboard:
  `b07_local_vllm_ray_throughput.*`, `b08_local_vllm_ray_e2e_time.*`,
  `b09_local_vllm_ray_task_stage_timing.*`,
  `b10_local_vllm_request_count_inflight.*`,
  `b11_local_vllm_token_tail_performance.*`, and
  `b12_local_vllm_latency_probe_breakdown.*`.
- Added `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md` and
  `learning/local_vllm_ray_baseline_walkthrough.md`.
- Boundary: these are local PG18.4 fixed row-batch baseline and metric
  observability support figures. They are not token-aware batching,
  queue-adaptive scheduling, writeback-inclusive, or PostgreSQL 18.3 internal
  platform results.

## 2026-07-18 AI_COMPLETE latency and vLLM metric probe

- Added batch-level result statistics to `code/src/metrics.py` and
  `code/scripts/profiling/postgres_ai_operator_profile.py`: batch row min/max/mean,
  batch token min/max/mean, and batch service latency P50/P95/P99.
- Added optional `--model-metrics-url` Prometheus scraping for vLLM run-level
  delta metrics: prompt/generation token deltas, request success delta, mean
  vLLM e2e/queue/inference/prefill/decode latency, and final running/waiting
  request gauges.
- Verified with
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_task_batch8_latency_metrics_20260718.csv`:
  4 rows, 3 formal rows, all `status=ok`, all `vllm_metrics_status=ok`.
- Boundary: this validates metric collection on a small local Daft + Ray +
  vLLM probe. It is not a full optimized scheduling result.

## 2026-07-18 ShareGPT/BurstGPT tokenizer-filtered Ray rerun

- Updated `code/scripts/data/import_ai_complete_workload.py` so the imported
  `sharegpt_burstgpt` workload can use the local Qwen2.5-1.5B-Instruct
  tokenizer for `prompt_tokens` and filter rows by
  `prompt_tokens + completion_max_tokens <= max_model_len`.
- Re-imported 1024 `sharegpt_burstgpt` rows into the local PostgreSQL
  rehearsal database with `max_model_len=2048` and `completion_max_tokens=16`.
  Current prompt-token range is 1..1851.
- Reran the local `AI_COMPLETE` baseline through
  `PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> Ray -> vLLM` for
  `ray_task` and `ray_actor`, batch sizes 1/2/4/8/16/32. Results are recorded
  in
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_ray_static_batch_sweep_rerun_20260718.csv`.

## 2026-07-18 Local vLLM Qwen static batch baseline

- Established the first local `AI_COMPLETE` baseline for `PostgreSQL -> DaftPostgresSource -> DaftOrganizer -> vLLM-compatible completion backend -> no writeback`.
- Used local `models/Qwen2.5-1.5B-Instruct` served by `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404` as `qwen2.5-1.5b`.
- Ran fixed row-batch sweep with `ray_batch_rows` in `1,2,4,8,16`, `total_rows=32`, `completion_max_tokens=8`, `executor=python`, `warmup_runs=1`, `repeats=3`.
- Result CSV and report: `experiments/results/local_vllm_qwen15b_baseline/static_batch_sweep.csv` and `experiments/results/local_vllm_qwen15b_baseline/README.md`.
- All 20 rows returned `status=ok`; formal rows only: mean throughput improved from 10.328 rows/s at batch size 1 to 91.976 rows/s at batch size 16.
- Boundary: this is a local fixed row-batch baseline, not a token-aware scheduling result, not an optimal batch-size claim, and not a PostgreSQL 18.3 internal-platform result.

## 2026-07-18 ShareGPT and BurstGPT raw workload downloads

- Downloaded ShareGPT Vicuna unfiltered prompt data to `data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json`; local check found 94,145 conversation records.
- Downloaded BurstGPT trace data to `data/raw/burstgpt/BurstGPT_1.csv`; local check confirmed columns `Timestamp`, `Model`, `Request tokens`, `Response tokens`, `Total tokens`, and `Log Type`.
- Added `data/README.md` to document raw data paths, intended use, and boundary.
- Updated `.gitignore` so `data/raw/**` payloads are not tracked by git.
- Boundary: this only establishes raw workload availability. Comparable baseline and optimized experiments should be generated from a normalized ShareGPT/BurstGPT workload table, not from the earlier synthetic seed rows.

## 2026-07-18 ShareGPT/BurstGPT workload import path

- Added `code/scripts/data/import_ai_complete_workload.py` to normalize ShareGPT prompts with BurstGPT timestamp/token metadata into the PostgreSQL `documents` table.
- Extended `documents` with workload metadata columns: `workload_name`, `prompt_tokens`, `target_output_tokens`, `arrival_time_s`, `session_id`, and `prefix_key`.
- Added `--source-workload-name` to `code/scripts/profiling/postgres_ai_operator_profile.py`, so different workloads can coexist in `documents` and profiling can select one explicitly.
- Imported local `sharegpt_burstgpt` workload rows into PostgreSQL with `start_doc_id=1000000`, `rows=1024`, `prompt_tokens=8..1797`, `target_output_tokens=2..2048`, and categories covering short/medium/long x ChatGPT/GPT-4.
- Verified a small `DaftPostgresSource -> DaftOrganizer -> Ray task -> vLLM` smoke under `tmp/sharegpt_burstgpt_daft_ray_vllm_smoke.csv` with `status=ok`, `total_rows=8`, `source_workload_name=sharegpt_burstgpt`.
- Boundary: this validates the final workload import/read path. It is not yet the full baseline sweep or an optimized scheduling result.

## 2026-07-18 vLLM local Qwen AI_COMPLETE smoke

- Started `vllm/vllm-openai:v0.25.1-cu129-ubuntu2404` with local Hugging Face model files from `models/Qwen2.5-1.5B-Instruct`, avoiding runtime Hub downloads.
- Required local Windows/WSL Docker settings for this machine: `VLLM_WSL2_ENABLE_PIN_MEMORY=1`, `VLLM_USE_V2_MODEL_RUNNER=0`, and `--enforce-eager`; the default vLLM V1/V2 runner path previously failed with `RuntimeError: UVA is not available`.
- Verified OpenAI-compatible `/v1/models` returned `qwen2.5-1.5b`; verified `/v1/completions` with a minimal prompt.
- Verified project E2E smoke under `tmp/vllm_local_qwen15b_ai_complete_smoke.csv`: `operator=ai_complete`, `data_source=daft_postgres`, `organizer=daft`, `model_backend=compatible_http`, `model_name=qwen2.5-1.5b`, `writeback_mode=json_text`, `total_rows=2`, `written_rows=2`, `status=ok`.
- Ran the layer-2 structural matrix under `tmp/vllm_local_qwen15b_layer2_matrix.csv`: `data_source` (`arrow_postgres`, `daft_postgres`) x `organizer` (`arrow`, `daft`) x `executor` (`python`, `ray_task`, `ray_actor`) x `writeback_mode` (`none`, `json_text`). All 24 rows returned `status=ok`; all `json_text` rows wrote `written_rows=2`; all `none` rows wrote `written_rows=0`.
- Boundary: this establishes the local vLLM + Qwen + Daft + PostgreSQL completion path. It is not yet a formal performance experiment or token-aware/prefix-aware batching result.

## 2026-07-18 Ollama AI_COMPLETE backend

- Added `ollama` as an `AI_COMPLETE` backend in `code/src/model_backends.py`, using Ollama native `/api/generate`.
- Updated `code/scripts/profiling/postgres_ai_operator_profile.py` so `--operator ai_complete --model-backend ollama` defaults to `http://localhost:11434` when no completion endpoint URL is provided.
- Verified local PG18.4 smoke with Docker Ollama `qwen2.5:1.5b`: `ollama_ai_complete_smoke` completed with `written_rows=2`; `ollama_daft_ai_complete_smoke` completed with `data_source=daft_postgres`, `organizer=daft`, and `written_rows=2`.
- Ran the layer-3 structural matrix under `tmp/ollama_ai_complete_layer3_matrix.csv`: `data_source` (`arrow_postgres`, `daft_postgres`) x `organizer` (`arrow`, `daft`) x `executor` (`python`, `ray_task`, `ray_actor`) x `writeback_mode` (`none`, `json_text`). All 24 rows returned `status=ok`; all `json_text` rows wrote `written_rows=4`.
- This is a local Ollama completion smoke. It does not replace the future vLLM-compatible `/v1/completions` path and is not a token-aware/prefix-aware batching result.

## 2026-07-18 AI_COMPLETE runtime skeleton

- Added `--operator ai_embed|ai_complete` to `code/scripts/profiling/postgres_ai_operator_profile.py`; default remains `ai_embed`.
- Extended `code/src/model_backends.py` with fake and vLLM-compatible `/v1/completions` completion backends.
- Extended `code/src/sinks.py` with `write_completions` and added `document_completions` to the local schema.
- `AI_COMPLETE` supports `none/json_text` writeback. `pgvector` remains embedding-only and is rejected for `AI_COMPLETE`.
- Added Ray worker `PYTHONPATH` runtime env so Ray task/actor workers can import `code/src` modules after the runtime split.
- Verified PG18.4 local smoke under `tmp/postgres_ai_complete_fake_smoke.csv`: fake `AI_COMPLETE` completed with `total_rows=16`, JSON-text writeback completed with `written_rows=8`, and `ray_task` completed with `status=ok`. This is a local function smoke, not a vLLM performance result or token-aware batching conclusion. The Windows Ray run printed a raylet shutdown access-violation warning after producing the result row.

## 2026-07-18 Runtime code boundary cleanup

- Split reusable runtime helpers out of `code/scripts/profiling/postgres_ai_operator_profile.py`:
  - `code/src/model_backends.py`: fake debug embedding backend and compatible HTTP embedding backend.
  - `code/src/sinks.py`: existing `none/json_text/pgvector` PostgreSQL writeback.
  - `code/src/metrics.py`: stage timer, GPU snapshot, and CSV append helper.
- Kept `fake` only as an offline smoke/control backend. vLLM-compatible runs should use `--model-backend compatible_http`; `http_openai` remains accepted as a compatibility alias.
- Added `code/tests/serving/test_model_backends.py` and `code/tests/data/test_sinks.py`.
- Updated `code/README.md`, `code/scripts/README.md`, and `PROJECT_INDEX.md` with the new code boundaries.

## 2026-07-17 Daft PostgreSQL data entry implementation

- Added `code/src/sources.py` with `PostgresArrowSource` and `DaftPostgresSource`, plus `code/tests/data/test_sources.py`.
- Updated `code/scripts/profiling/postgres_ai_operator_profile.py` with `--data-source arrow_postgres|daft_postgres`; default remains `arrow_postgres`.
- Kept writeback unchanged: `none/json_text/pgvector`. Lance remains a future optional sink and is not implemented in this step.
- Added Daft SQL runtime dependencies `sqlglot` and `connectorx` to `code/requirements.txt`.
- Verified local PG18.4 smoke under `tmp/postgres_daft_source_e2e.csv`: `source_arrow_smoke` and `source_daft_smoke` both completed with `total_rows=64` and `object_count=4`. This is a local smoke result, not a formal performance conclusion.

## 2026-07-17 Superpowers implementation plan for Daft PostgreSQL data entry

- **触发**：用户要求使用 `superpowers:brainstorming` / `superpowers:writing-plans` 构思后续代码，并明确当前写回按既有方案，Lance 仅作为后续可能方向。
- **新增**：
  - `code_doc/README.md`
  - `code_doc/superpowers/README.md`
  - `code_doc/superpowers/plans/README.md`
  - `code_doc/superpowers/plans/2026-07-17-daft-postgres-entry-existing-writeback.md`
- **范围**：计划聚焦 Daft 作为 PostgreSQL data entry；当前 writeback 保持 `none/json_text/pgvector`；Lance 仅作为 future optional sink，不进入本轮实现。

## 2026-07-17 Daft 文本 DataOrganizer smoke 接入

- **触发**：用户要求实际使用 Daft，并要求遵循 `karpathy-guidelines`、保证代码可维护性。
- **实现**：
  - 新增 `code/src/organizers.py`，实现 `ArrowOrganizer` 与 `DaftOrganizer`。两者接收 Arrow table，输出 downstream 可复用的 Arrow batch 列表和指标。
  - 新增 `code/scripts/profiling/daft_text_organizer_smoke.py`，通过 `--organizer arrow|daft` 验证 `rows -> Arrow Table -> organizer -> batches`，并支持显式 `--runner ray` 检查 Daft `into_partitions`。
  - 更新 `code/scripts/profiling/postgres_ai_operator_profile.py`：主链路的 `fetch_record_batch + split_batch` 已替换为 organizer 后端选择，新增 `--organizer arrow|daft`、`--organizer-partition-mode`、`--organizer-partitions`、`--daft-runner`。默认仍为 `arrow`，保留旧路径作为 baseline。
  - 新增 `code/tests/planning/test_organizers.py`，覆盖 Arrow 后端和 Daft native 后端的 batch 输出一致性。
  - 更新 `code/requirements.txt`：新增 `daft`，并将 `pyarrow` 约束为 `>=16,<25`，匹配 Daft 0.7.20 的依赖边界。
  - 更新 `code/README.md`、`code/scripts/README.md`、`PROJECT_INDEX.md`，登记新增入口和运行命令。
- **本地验证**：
  - NativeRunner：`--rows 256 --batch-size 64` 生成 4 个 64 行 batch。
  - Ray runner：`--runner ray --rows 32 --batch-size 8 --partition-mode into_partitions --partitions 4` 生成 4 个 8 行 batch。
- **边界**：主脚本已具备 Daft organizer 后端选择，但这仍不是正式性能实验；真实 PostgreSQL/vLLM/GPU-backed 结论需要后续 E2E 运行数据。

## 2026-07-17 多模态正文实验 + Daft 文本阶段直接接入 + 优化空间扩展

- **触发**：与导师讨论后明确多模态实验进入正文（§5.3 策略泛化性验证），不是仅 Discussion；用户确认 Daft 从文本阶段直接接入（不再经过 Arrow 中间态）；用户明确"参数优化也可以作为贡献"。
- **六项关键决策**：
  1. **多模态正式进入正文**：在图像 workload（ImageNet/HuggingFace，CLIP + Qwen2.5-VL）上使用同一套策略代码，验证 token-budget → frame-budget、queue-adaptive flush → 完全复用的模态无关性。VLM 生成实验标记为 optional。
  2. **Daft 文本阶段直接接入**：取消 Arrow→Daft 过渡方案。Daft DataFrame API 对文本（`df["prompt"]`）和图像（`df["image"]`）是同一套接口，后续多模态实验只需替换列类型。
  3. **优化空间扩展为"策略级 + 引擎级"双层**：策略级（token-budget、queue-adaptive flush、routing）+ 引擎级（Daft `into_batches`/`batch_size`/`max_concurrency`/`gpus`/`repartition`）。论文贡献不是"发明了某个 knob"，而是"在数据库 AI 算子外部执行场景中系统表征了优化空间 + 提出了策略级决策方法 + 跨模态验证"。
  4. **算子代价估计定位**：作为 §6.1 补充讨论（不作为独立研究内容），基于实验阶段已采集的 profile 数据，不新增实验。
  5. **完整优化实验清单建立**：P0（batch 粒度/分组策略/提交节奏 3 消融）→ P1（Daft 引擎参数 + 耦合验证）→ P2（多模态泛化 + 算子代价估计）。
  6. **Scope 缩减触发条件写死**：Month 1 无 vLLM baseline → 多模态降 Discussion；文本 RC1+RC2 未完成前不启动多模态 pipeline；VLM 生成始终 optional。
  7. **写回降级**：写回不作为独立研究内容或实验阶段，降为实验设置中的工程细节。PostgreSQL + pgvector + COPY + deferred index 作为默认写回路径。
- **idea-evaluator 重评估结果**：Accept with Revisions（两个 MAJOR 但可防御）。评分：Higher 8, Faster 8, Stronger 8, Cheaper 6, Broader 7。范式转移潜力 possible（3.5/4）。最大风险是单 GPU 限制多 endpoint 并行实验深度 + 串行依赖过多导致周期膨胀（有 scope 缩减触发条件）。
- **更新文件**：
  - `AGENTS.md` §1/§2/§3 — 新增 Daft + 多模态 + 算子代价估计 + scope 缩减条件
  - `PROJECT_OUTLINE.md` — 研究内容扩展为 5 项、近期优先级重排、新增 scope 缩减条件
  - `research/knowledge_hub.md` §10.5.1 — 重写为"Daft 文本阶段直接接入 + 优化空间三层框架 + 完整实验清单"
  - `experiments/plans/strategy_design_implementation_reference.md` — 此前已完成口径统一（三层策略 → 两项策略 + 验证），§4.2 已更新 Daft 引擎抽象
- **涉及文件**：`AGENTS.md`, `PROJECT_OUTLINE.md`, `research/knowledge_hub.md`, `experiments/plans/strategy_design_implementation_reference.md`

## 2026-07-17 Daft+Ray 多模态与具身智能调研

- **新建** `research/daft_ray_multimodal_reference.md`：Daft+Ray 多模态执行引擎技术手册，涵盖 Swordfish 流式引擎、Flotilla 分布式架构、@daft.cls GPU UDF 机制、与具身智能的连接、及与本课题的关系分析。
- **更新** `research/knowledge_hub.md`：新增 §10 "Daft+Ray 多模态执行引擎与具身智能负载"，含架构对比、Snowflake Cortex 多模态 AI 算子、具身智能管线、及与本课题的互补关系论证。
- **更新** `research/ai_operator_literature_inventory.md`：新增 8 篇文献（Daft SciPy Talk、Ray Data Streaming Batch、Flotilla、@daft.cls、Snowflake Cortex Multimodal、阿里云 EMR Daft 具身智能、IBM 具身数据缺口、HeteroHub），总数 57→65 篇。
- **核心结论**：
  1. Daft+Ray 优化引擎层的物理资源调度（CPU/GPU 重叠、内存管理），本课题优化策略层的调度决策（按什么规则组 batch、按什么节奏发请求）——两者互补而非竞争。
  2. Snowflake Cortex 已 GA 多模态 AI SQL 算子，数据库 AI 算子处理多模态数据是工业现实。
  3. 本课题的调度策略框架（token-budget→frame-budget、queue-adaptive flush、actor pool 路由）对多模态负载具有自然泛化能力。
  4. 建议在论文 Discussion (§6) 中以具身智能为 generalization case，不做主实验。
- **涉及文件**：`research/knowledge_hub.md`, `research/daft_ray_multimodal_reference.md`, `research/ai_operator_literature_inventory.md`

## 2026-07-16 推理管线交互文献系统性收集

- **新建** `research/inference_pipeline_interaction_literature.md`：系统性搜索和收集 28 篇 CCF-A 论文、技术报告和工业系统文档。
- **覆盖五个方向**：
  1. LLM 推理服务与连续批处理（vLLM, Orca, Sarathi-Serve, FastServe, DistServe, Splitwise, Mooncake, S-LoRA）
  2. 自适应批处理与推理服务调度（Clipper, Nexus, Clockwork, Triton）
  3. 数据管线与推理服务交互（Ray Data Streaming Batch, Ray Data LLM integration, NeuStream, HedraRAG）
  4. Token/Prefix-Aware 优化（Parrot, SGLang, KVFlow, ChunkAttention, EPIC）
  5. Ray-Specific 推理服务模式（Ray Serve LLM, Ray Compiled Graphs）
- **核心发现**：确认存在研究空白——无任何已有工作系统性研究"上游数据管线 batch 参数（batch_size, partition_count, concurrency, token-aware/prefix-aware 分组）如何影响下游推理引擎 continuous batching 效率及最优协调策略"。
- **最新 2026 论文**：收录 BatchLLM (MLSys 2026)、PKAS (HPDC 2026)、PLA-Serve (MLSys 2026)、Load-Aware Prefill Deflection、PEACE 等。
- `research/README.md` 和 `PROJECT_INDEX.md` 已在先前 session 预添加了该文件的索引条目。

## 2026-07-16 方向重大调整：AI_COMPLETE 为主线 + 上游动态 Batching + Ray 架构设计空间

- **触发**：用户明确 AI_COMPLETE（生成式 LLM 推理）才是真正目标场景，AI_EMBED 只是"能跑的先跑"的过渡；用户不想要静态 batch，希望借鉴 vLLM continuous batching 思路做上游动态 batching，并充分利用 Ray 的 actor 灵活性做架构设计。
- **方向调整（七项共识）**：
  1. **RC3 降格**：从"研究内容三"降为"端到端验证实验：写回瓶颈判定"，只使用当前最优写回方法（COPY + deferred index）
  2. **"协同"操作化定义**：协同 = 上游数据组织的"形状"（batch token 分布）和提交"节奏"（K_max、queue-adaptive flush）共同影响下游 vLLM continuous batching 的调度效率。不再是模糊的"跨层协同"。
  3. **vLLM 重定位**：vLLM 不是竞争对手，是部署平台 + baseline。课题研究"在 vLLM continuous batching 之上，上游 Ray 数据执行层如何最优地组织请求"。
  4. **AI_COMPLETE 为主线**：AI_EMBED 降为预研验证；AI_COMPLETE（生成式 LLM）成为论文主体 workload，引入 token 长度分布、shared prefix、TTFT/TPOT、generation straggler 等更丰富的交互变量
  5. **上游动态 Batching（借鉴 Continuous Batching 原理）**：计划层不再是静态选择 `batch_size`，而是设计动态 batching policy——token-budget batching（类似 vLLM `max_num_batched_tokens`）、length-aligned grouping、prefix-aware grouping
  6. **Ray 作为架构设计空间**：不是只把 Ray 当 task executor，而是利用 actor 异构化（ShortTokenActor / LongTokenActor / PrefixAffinityActor）、运行时自适应（queue-adaptive flush）、去中心化协调（每个 actor 自主决策）、actor pool 分池路由等架构设计杠杆
  7. **耦合验证前置**：独立最优拼接 vs 联合 grid search 作为第一个关键消融实验；无交互效应时 fallback 为"分层独立优化框架"，仍为合格硕士论文
- **文献确认**：多源检索确认无 CCF-A 论文研究"上游数据管道 batch 参数 × 下游 continuous batching 性能"这一交叉点，研究空白判断成立。
- **用户三层划分**：模型结构层（GQA/MQA）→ 计算执行层（Flash-Attention）→ 服务部署层（PagedAttention + In-Flight-Batching）。课题聚焦层级 3，前两层为模型/实现选型，不进入优化范围。
- **需同步更新**：`AGENTS.md` §1/§2/§3/§5、`experiments/plans/strategy_design_literature_basis.md` §7、`motivation/plans/workloads.md`、`opening/report/opening_report.md`、`PROJECT_OUTLINE.md`
- **注意**：同期三个评估 skill（idea-evaluator / ars-reviewer / nature-reviewer）接收的是旧 framing（AI_EMBED + 静态 batch）；新 framing（AI_COMPLETE + 动态 batch + Ray 架构）更强。评估结果到达后应做 framing 对比再最终确认。

## 2026-07-15 开题报告移除 fake/CPU 主文证据

- 根据当前已经完成 pgai SQL 触发面集成和真实 GPU-backed `AI_EMBED` 完整链路复测的事实，更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`。
- 删除 4.2 中历史 fake/CPU 预研图、表和相关表述，避免读者误解课题仍停留在 toy/fake benchmark 阶段。
- 4.2 可行性证据现在只保留 PG18.4 + pgvector 环境、GPU-backed `AI_EMBED` 链路和双 endpoint Ray 动机测试；调优变量依据改为文献机制 + 当前真实 GPU-backed 复测。
- 已覆盖同步新版开题报告飞书 docx，并重新插入 8 张正式 PNG；回读确认 revision 更新到 `72`，未检出 fake/CPU、图 4-7、表 4-4、Mermaid 旧图或本地 `figures/` 路径残留。

## 2026-07-15 开题报告飞书新版 docx 同步

- 使用 user 身份将 `opening/feishu/opening_report_wiki.md` 覆盖同步到新版开题报告飞书 docx：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`。
- 覆盖写入后飞书返回 `partial_success`，原因是 Markdown 中的本地图片路径不能直接导入为图片资源；随后逐张上传并插入 8 张 PNG：研究缺口图、总体研究框架图、三层上游执行策略图、运行时策略闭环图、粒度对比图、阶段时延图、endpoint 对比图、pgvector 写回对比图。
- 回读线上文档确认 revision 更新到 `51`，关键图注附近为真实飞书图片 URL；关键词检查未发现本地 `figures/` 路径和旧的“三岛/Killer/联合最优/边界确认/阶段画像/Ours-v0”等表述残留。

## 2026-07-15 策略设计与实现参考沉淀

- 新增 `experiments/plans/strategy_design_implementation_reference.md`，把 Ray OSDI 2018、Ray Data / Ray Serve、vLLM / Orca、Triton、GPU 数据放置和 DB AI 算子文献机制沉淀为三层策略参考。
- 明确三层策略：计划层负责数据库侧 `batch_size` / `partition_count` / `object_merge`；运行层负责 `K_max` / routing / backpressure / actor pool；服务端层负责 dynamic / continuous `micro-batch`。
- 文档同时给出每层可观测信号、可调变量、实现边界、实验指标、baseline 顺序和实现优先级，供后续实验设计和原型实现使用。
- 进一步补充“系统优化蓝图”和“机制到实现任务优先级”，将 Workload Profiler、Plan-time Data Organizer、Ray Admission Controller、Endpoint Router、Service-side Micro-batcher、E2E Guardrail 拆成可实现模块，并给出每个模块的借鉴来源、最小实现、验证问题和放弃条件。
- 同步更新 `experiments/plans/README.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`。

## 2026-07-15 GPU 调度与数据放置补充调研

- 新增 `research/gpu_scheduler_data_placement_supplement_20260715.md`，补充 GPU / LLM 推理调度、异构数据管线、GPU 数据库算子、GPU-resident 数据放置和数据库 AI 算子几条文献线索。
- 明确当前策略不应写成“重新发明 GPU scheduler”或“改造 Ray 调度器”，而是位于数据库外部执行链路和模型服务入口之间的轻量级 runtime strategy controller。
- 同步 `opening/README.md`、`opening/literature/reading_list.md` 和 `PROJECT_INDEX.md`，将该补充调研纳入开题文献入口。

## 2026-07-15 策略设计重新评判与三层收窄

- 根据用户反馈和补充调研，将策略从“全运行时控制器”收窄为 three-layer upstream execution strategy：计划层在执行前选择 `batch_size` / `partition_count` / `object_merge`，运行层调整 `K_max`、routing、backpressure，服务端用 dynamic / continuous batching 形成推理 `micro-batch`。
- 明确当前不采用“运行时重切数据库侧已物化 RecordBatch”作为主方案；动态 batch 借鉴 vLLM / Ray Serve / Triton 思路，放在模型服务侧尚未执行的请求队列中。
- 补充 Ray OSDI 2018 调度思想映射：task/actor、resource-aware scheduling、local/global scheduler、object store locality 和 actor pool 可迁移为 task 粒度、actor 池、资源约束、placement/locality、`K_max` 与 routing 等实验变量。
- 同步更新 `figures/scripts/generate_runtime_strategy_control_loop.py`、`figures/audit/runtime_strategy_control_loop_audit.md`、`figures/audit/top_venue_strategy_figure_design_notes.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`；重新生成 PNG/SVG 并通过边框、箭头和禁用术语自检。

## 2026-07-16 实验计划与开题报告同步更新

- **开题报告对齐实验计划**：6 项修改——
  1. §4.1 新增 Killer Experiment 六组对照（BL1-BL6）定义，明确核心 claim 的验证条件
  2. §4.2 新增"合理默认 vs 诊断工具"区分：逐行调用和无界 in-flight 仅作为诊断工具，不作为论文 §7 方法对照 baseline
  3. §3.2 研究内容三 扩展：提及 B 系列工程实验和三路写回架构对比（driver / worker-direct / queue-worker）
  4. §4.1 末尾新增 FILTER/COMPLETE 模拟 workload 诚实声明（参照 Orca 合成权重做法）
  5. §6 新增统计严谨性指标（中位数、IQR、重复次数、Ray 重启、随机种子）
  6. §2.3 补上 ColStorEval[50] 引用
- **PROJECT_INDEX.md 同步更新**：§3 新增四个实验计划文件列表、§7 更新研究内容三标题、§8 更新下一步优先级（P0/P1/P2 结构）
- **PROJECT_OUTLINE.md 同步更新**：研究内容三标题降级为"边界分析与轻量写回优化"、近期优先级改为 P0/P1/P2 三阶段
- 上述修改使开题报告与 `experiments/plans/` 下四份实验计划在 BL 矩阵、baseline 分级、统计规范和 workload 标注上口径一致

## 2026-07-16 实验计划六项评估方法论修正

- **修正四个实验计划文件**，统一遵循从 vLLM/Orca/TurboVecDB/GaussML/FlexPushdownDB 五篇 CCF-A 论文提取的六项评估标准：
  1. **前置依赖声明**（§0）：每个文件写明 P0 必须先完成 vLLM + B 系列，否则所有 baseline 是 suboptimal
  2. **假设先行**（§2）：每个实验段在参数矩阵之前先写"要推翻什么假设"，不是盲目扫参——每个假设标注对应实验段和推翻后的含义
  3. **模型 batch scaling 前置实验**（研究内容一 §4）：在讨论 batch_size 选择之前，先跑模型自身的 batch_size→吞吐曲线
  4. **FILTER/COMPLETE 诚实标注**（研究内容一/二 §3）：标注为 simulated workload（参照 Orca 合成权重的做法）
  5. **统计规范**（各文件 §10）：重复次数、中位数（不取平均值）、IQR、Ray 状态重置、warm-up 策略、随机种子——全部标准化
  6. **可验证边界**（各文件 §11）："When does it NOT help?" 的每个边界条件对应一个可跑实验点，不是空洞自省
- 修改文件：`data_organization_batching.md`（重写）、`service_scheduling_backpressure.md`（新增 §0/§2/§10，修正 §9/§11）、`sink_writeback_coordination.md`（新增 §0/§2/§10，修正 §9）、`cross_layer_killer_experiment.md`（新增 §0/§2）

## 2026-07-16 实验计划骨架填充 + 评估方法论标准化

- **四个实验计划文件新建**：`experiments/plans/` 下三份研究内容实验计划 + 一份跨层 Killer Experiment 计划。
  - `data_organization_batching.md`（研究内容一）：Grid search、workload 对比、selectivity-aware 策略、模型 batch scaling 前置实验
  - `service_scheduling_backpressure.md`（研究内容二）：K_max sweep、routing 策略、adaptive vs static K_max、vLLM baseline 前置实验
  - `sink_writeback_coordination.md`（研究内容三）：B 系列工程 baseline（UPSERT vs COPY, logged vs unlogged, online vs deferred index）、三路架构对比、sink 对照
  - `cross_layer_killer_experiment.md`（跨层核心）：BL1-BL4 + 联合方案的完整矩阵、代价模型 R²、消融瀑布、跨 workload 泛化、统计严谨性要求
- **评估方法论标准化**：所有四个计划遵循从 vLLM (SOSP 2023)、Orca (OSDI 2022)、TurboVecDB (VLDB 2025)、GaussML (ICDE 2024)、FlexPushdownDB (VLDB 2021) 五篇 CCF-A 论文提取的共同原则——曲线 > 单点、先暴露瓶颈再优化、同硬件公平 baseline、消融拆开、诚实报告边界、统计严谨。
- **实验前置依赖明确**：P0 必须先跑 vLLM 接入 + B 系列写回工程 baseline，否则所有 Grid Search 都基于 suboptimal baseline。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 Baseline 分级重构：移除 strawman C 级

- **Baseline 分级重构**：`experiments/plans/research_design_catalog.md` §10.1-§10.4。
  - 移除 "C 级（Naive）"——row-by-row 调用、无界 in-flight 等故意劣化配置降级为"诊断工具"，只用于 §4 理解瓶颈机制，不作为 baseline 对照。
  - "合理默认配置"（coalesced batch=64、driver 写回）取代旧 C 级作为 §4 动机展示的参照点——这是正常工程师会写的第一版代码，不是 strawman。
  - S/A/B 三级保留：S（文献最优）→ A（工程最优）→ B（单维调优）。§7 方法对照至少包含 A 级。
  - §10.1 新增原则 5："动机展示不用 strawman"。§10.3 检查清单新增两条防 strawman 项。
  - A2.1 baseline 描述从 "Unbounded in-flight" 改为 "Ray 默认行为（无显式 K_max）"。
- **未同步到其他文件**：`baseline_reference.md`、`AGENTS.md` 和 `experiments/plans/README.md` 的 strawman 相关措辞已经合理，无需修改。

## 2026-07-15 研究方案候选目录 + Baseline 设计考量

- **新增研究方案候选目录**：`experiments/plans/research_design_catalog.md`，覆盖三个研究内容和跨层协同优化的 28 个候选方案，每个方案在六个维度（文献支撑、工程可行性、硬件可行性、开源依赖、创新空间、实验可验证性）上评分。
- **方案来源**：基于 57 篇文献清单 + 2026 年 7 月前沿检索（Ray Serve 2025 Custom Router/Async Inference/Autoscaling、NexusSched 两层调度、Multi-Bin Batching 队列理论、MAB 反馈控制、GFS/DARIS 优先级抢占、Arrow Flight Ballista/Spark SPIP、Iceberg v3 Deletion Vectors、COSTREAM/CONCERTO/GRACEFUL Learned Cost Models）。
- **Baseline 设计考量**：目录第 10 节为每个候选方案指定了对应 baseline（文献最优 S 级 / 工程最优 A 级 / 常见实践 B 级 / Naive C 级），并给出实现优先级（P0: COPY deferred index + vLLM 接入）。
- **分阶段路线图**：Phase 0-4，覆盖 2026-07 至 2026-10，Phase 3 的 Killer Experiment（BL1-BL6）是论文核心 claim 的验证点。
- **风险分析**：6 项风险（vLLM 消除外部调度收益 / 单 GPU 限制 / 写回优化边际 / workload 扩展 / Joint Opt 增量 < 10% / PG18.3 平台），每项标注了反证条件。
- 同步更新：`experiments/plans/README.md`、`PROJECT_LOG.md`。

## 2026-07-16 写回文献调研 + Baseline 矩阵 + 文献优先设计规则

- **文献清单 v3**：`research/ai_operator_literature_inventory.md` 从 45 篇扩充至 57 篇，新增写回/持久化方向 12 篇 CCF-A 文献（第六组精读 + E 组补充）。
- **新增实验 Baseline 参考矩阵**：`experiments/plans/baseline_reference.md`，覆盖 GPU 调度侧（6 个）、写回侧（7 个）、数据组织侧（4 个）、跨层决策侧（3 个），所有 baseline 标注来源论文/系统。
- **新增文献优先设计规则（§6.5）**：根 `AGENTS.md` 加入"系统/算法/实验方案设计时，优先从 CCF-A 文献提取设计模式"的规则。完整方法论写入 `research/README.md` §文献优先设计方法论。
- **idea-evaluator 评估**：课题方向 Accept with Revisions，无 CRITICAL 缺陷，paradigm-shift probe 4/4 yes。五项调整建议已记录在对话中。
- 同步更新：`AGENTS.md` §6.5、`research/README.md`、`experiments/plans/README.md`、`experiments/plans/baseline_reference.md`（新建）。

## 2026-07-13 制图脚本目录归位

- 将 `code/scripts/make_chain_breakdown_figures.py` 迁移到 `figures/scripts/make_chain_breakdown_figures.py`。
- 明确 `code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口；绘图、图表复现和素材筛选脚本统一放入 `figures/scripts/`。
- 同步更新 `code/AGENTS.md`、`code/README.md`、`code/scripts/README.md`、`figures/scripts/README.md` 和 `figures/learning/README.md`，避免后续实验代码目录与图资产目录混用。

## 2026-07-13 图资产规则沉淀

- 根据今天系统架构图和实验数据图的多轮修改经验，扩展 `figures/AGENTS.md` 为项目级图表长期规则文件。
- 规则中明确：论文级核心图先用 `figure-designer` 判断图型和版式；投稿/论文级质检可用 `nature-figure`；报告转 PPT 可用 `nature-paper2ppt` 或 `ppt-master`；实验数据图优先用 Python + Matplotlib / Seaborn 从 CSV 可复现生成。
- 补充系统架构图常见返工点：箭头遮挡、编号越界、模块未对齐、框内内容不规整、观测层和执行层割裂、图文术语不一致。
- 补充实验图规则：哪些数据值得画图，哪些只适合表格或文字；正式图必须标注数据来源、warm-up 处理、证据层级和不能声称的结论。
- 在 `PROJECT_INDEX.md` 顶部补充图资产规则入口，提醒后续新增、修改、迁移或审查图表前先读 `figures/AGENTS.md`。

## 2026-07-13 项目目录一致性复核

- 复核根目录、`overview/`、`research/`、`motivation/`、`learning/`、`opening/` 和 `figures/` 中与当前开题方向相关的入口文件。
- 将 `overview/project_outline.md` 从旧的“数据库内置 AI 算子外部执行链路”口径重写为“数据库驱动 AI 工作负载的分布式数据执行与存储协同优化”口径。
- 同步更新 `AGENTS.md`、`PROJECT_INDEX.md`、`research/literature_and_evidence_review.md`、`research/existing_ai_operator_execution_chains.md`、`motivation/plans/integration.md` 和 `motivation/results/README.md` 中的旧表述。
- 对 `motivation/results/pg18_4_fake/system_profile.md` 与 `motivation/results/fake_cpu/analysis.md` 增加当前口径说明，保留历史实验语境，但明确真实瓶颈归因应优先引用 GPU-backed 结果。
- 本次复核只调整会影响项目规划、阅读入口和方向判断的文件；历史日志和旧实验过程记录不做大面积改写。

本文件记录项目级简要操作，便于日后复盘方向、入口和关键材料调整。详细实验日志仍放在对应结果目录；开题材料的详细修改记录见 `opening/logs/project_log.md`。

## 2026-07-13 开题主线调整为数据库驱动 AI workload

- 根据用户确认的判断，将开题题目从“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”调整为“面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究”。
- 同步更新项目级方向口径：数据库 AI 算子主要作为 workload 入口和验证场景，研究主体调整为 Daft/Arrow 数据组织、Ray 执行调度、GPU 模型服务和 Lance / pgvector / PostgreSQL sink 之间的数据执行与存储协同。
- 同步修改 `README.md`、`PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`AGENTS.md`、`overview/current_direction_and_plan.md`、`motivation/plans/integration.md` 以及 opening 相关源稿，避免项目规划与开题报告割裂。
- 已生成新的本地飞书源稿 `opening/feishu/opening_report_wiki.md`；飞书写入时 `lark-cli` 在用户目录刷新锁文件处返回 `Access is denied`，提升权限重试被自动审批拒绝，需后续获得权限后再同步线上 wiki。

## 2026-07-12 根目录总纲与项目日志

- 新增 `PROJECT_OUTLINE.md`，作为根目录项目总纲入口，汇总当前题目、研究内容、实验主线、关键证据、近期优先级和同步规则。
- 新增 `PROJECT_LOG.md`，作为项目级简要操作日志，用于记录跨目录、影响项目方向或入口结构的调整。
- 后续如果开题报告、实验主线、项目方向或关键入口发生变化，需要同步更新 `PROJECT_OUTLINE.md` 和本日志。

## 2026-07-12 实验主线入口调整

- 将项目实验主线入口从 `feasibility/guide.md` 调整到 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md`。
- 明确 `feasibility/` 只负责组件、环境和脚本可用性验证，不承担当前实验大纲、开题主线或 GPU-backed 性能结论职责。

## 2026-07-12 开题与项目规划双向同步

- 明确开题报告和项目规划不是单向关系：开题报告基于项目进展撰写；开题题目、研究内容、技术路线或侧重点调整后，也会反向影响项目规划、实验优先级和对外口径。
- 项目入口文档需要与 `opening/report/opening_report.md` 保持一致，不能长期出现不同方向。

## 2026-07-12 开题报告与飞书内容复核

- 按当前 `PROJECT_OUTLINE.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md` 复核开题报告与飞书源稿。
- 确认 `opening/report/opening_report.md` 当前主线基本合适：正式证据优先引用真实 GPU-backed 结果，PG18.4 / fake / CPU 结果有边界说明。
- 清理 `opening/feishu/opening_report_wiki.md` 的本地源稿说明，避免发布到飞书后出现工作流元话语。
- 补充飞书后续计划：后续进入 PostgreSQL 18.3 内部平台复测，避免把 PG18.4 本地同构预演写成正式平台结论。
- 修正 `motivation/results/README.md` 中 GPU-backed 结果入口的过时措辞。

## 2026-07-12 实验结论写作标准

- 根据用户反馈，将 `learning/AGENTS.md` 的实验讲解标准提升为项目级实验结论写作参照。
- 更新 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md` 和 `opening/work_rules.md`，要求实验结论、数据分析、开题可行性分析和飞书实验摘要都说明实验目的、链路流程、参数含义、数据来源、结果读法、不能证明什么、结论类型和下一步验证。
- 后续正式报告可以比学习材料更凝练，但结论边界和分析精细程度不能低于 `learning/AGENTS.md` 的要求。

## 2026-07-12 开题实验飞书页与 PPT 生成

- 新增 `opening/feishu/motivation_feasibility_wiki.md`，按真实 GPU-backed 证据、fake/CPU 历史预研、可行性验证边界和下一步实验组织动机测试与可行性测试内容。
- 使用 user 身份覆盖写入动机测试与可行性测试飞书 wiki：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`，飞书返回成功并生成 5 个 Mermaid whiteboard。
- 基于学校 PPT 模板生成开题汇报 PPTX：`opening/slides/opening_defense_20260712.pptx`，内容来自开题报告、GPU-backed 动机实验和当前项目总纲。
- 已将 PPTX 以 user 身份导入为飞书在线幻灯片：`https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg`。
# 2026-07-14 pgvector(384) writeback comparison

- Updated `code/scripts/profiling/postgres_ai_operator_profile.py` so `--setup --embedding-dim 384` creates `document_embeddings.embedding_vector` as `vector(384)`.
- Ran the same GPU-backed Ray actor chain for no writeback, JSON text writeback, and pgvector `vector(384)` writeback.
- Added result report and CSV under `motivation/results/gpu/`.
- Added report-main figure `figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png`.
- Updated opening report, learning walkthrough, figure indexes, and result indexes. Boundary: PG18.4 local rehearsal, not PostgreSQL 18.3 internal platform.

# 2026-07-14 合并 agent/postgres18-local-profile 分支并全项目校准

- 将 `origin/agent/postgres18-local-profile` 合并到 `main`，恢复 `opening/` 开题材料目录。
- 分支带来的重构：`validation/` → `feasibility/`，`motivation/` 脚本 → `benchmarks/`、设计文档 → `plans/`、结果按 `fake_cpu/cpu/gpu/pg18_4_fake` 分类。
- 新增目录：`deploy/`、`experiments/`、`figures/`、`learning/`、`opening/`、`projects/`。
- 创建 `CLAUDE.md` 作为 Claude Code 环境规则入口，导入全部 `AGENTS.md`。
- 全项目文档路径校准（12 个文件）：
  - 根 `AGENTS.md`：§3 证据更新为 GPU-backed 结果，§4 目录加新结构，§5 实验规则更新。
  - 根 `README.md`：目录树重写、标题对齐 `PROJECT_OUTLINE.md`、证据和运行命令更新。
  - `PROJECT_INDEX.md`：全文重写，所有路径更新，新目录入口，当前证据优先级。
  - `overview/current_direction_and_plan.md`、`overview/project_outline.md`：加弃用声明，指向根 `PROJECT_OUTLINE.md`。
  - `motivation/results/README.md`：从扁平文件列表重写为子目录结构。
  - `feasibility/benchmarks/README.md`：命令路径和脚本引用全部更新。
  - `opening/ppt_rules.md`：图表规则重写，引用 `figures/` 为权威来源，Python+Matplotlib 优先于 ECharts。
  - `opening/work_rules.md`：过期引用更新。
  - `experiments/AGENTS.md`：新增 `karpathy-guidelines` 和图表 skill 引用。
- 镜像同步规则：`CLAUDE.md` 和 `AGENTS.md` §9 包含相同的 6 行变更→更新清单，互相指向对方。

# 2026-07-15 开题报告研究方案图补充

- 将 `figures/architecture/cross_layer_method_framework.png` / `.svg` 调整为研究方案图，明确三类数据库 AI 算子、阶段画像、数据组织策略、模型服务调度策略、联合调优验证和写回瓶颈判定实验。
- 重绘 workload 区块为三张卡片：场景名、SQL 算子名、调度压力三行排版；图中移除 `RC` / `BL` 缩写、`Workload 入口`、`边界确认` 和未解释的 `vs` 表达。
- 在 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 的 Killer Experiment 段落后插入该图作为图 4-1，并顺延后续第 4 章图号。
- 新增 `figures/audit/cross_layer_method_framework_audit.md`，并更新 `figures/README.md` 的正式图资产说明。

# 2026-07-15 研究方案图作图规则同步

- 将研究方案图的版式和审查经验同步到 `figures/AGENTS.md`：方案图必须回答“我要做什么”，并按 workload、阶段画像、策略设计、联合验证和写回瓶颈判定组织。
- 明确禁止在正式可见图中使用 `RC/BL` 内部缩写、未解释的 `vs`、`边界确认` 等模糊标签；workload 区块优先使用“三行卡片”排版。
- 补充遮挡和越界检查要求：卡片边框必须完整可见，文字不得裁切，生成后同时执行程序化像素/关键词残留检查和人工 PNG 预览。
- 同步更新 `opening/ppt_rules.md`，要求 PPT 中的研究方案图也遵守同一套语义和排版规则。

# 2026-07-15 开题主线调整为上游链路调优与端到端效果评估

- 根据用户确认，将开题主叙事从“独立最优组合 vs 跨层联合最优”调整为“上游执行链路调优 + 端到端效果评估”：优化侧重点在数据组织与模型服务调度，尤其是模型服务状态感知调度；写回纳入端到端效果评价。
- 更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`：研究路线改为“分阶段性能剖析 -> 上游执行链路调优 -> 加入写回的全链路验证 -> 多 workload 验证”，并将独立最优拼装对照降级为阶段间耦合明显时的增强对照。
- 更新 `figures/architecture/cross_layer_method_framework.*`：中心卡片改为“上游执行链路调优”，评价标准改为加入写回后的端到端耗时、吞吐、排队和写回占比整体改善。
- 同步调整 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`experiments/plans/` 和 `figures/audit/` 中的入口说明，避免把跨层联合优化写成当前唯一核心 claim。

# 2026-07-15 上游执行链路策略设计图

- 新增 `figures/architecture/upstream_strategy_design.png` / `.svg`，用于说明阶段画像之后的已定位瓶颈如何转化为数据组织优化、模型服务调度、写回约束处理、执行配置与端到端验证。
- 新增 `figures/scripts/generate_upstream_strategy_design.py`，统一生成 PNG/SVG，并检查所有核心卡片边界和箭头是否越界或穿过无关卡片。
- 新增 `figures/audit/upstream_strategy_design_audit.md`，记录该图不声称最终 learned optimizer，而采用“已定位瓶颈 -> 优化动作 -> 执行配置 -> 端到端验证”的保守方法图定位。

# 2026-07-15 策略设计文献依据与边界

- 新增 `experiments/plans/strategy_design_literature_basis.md`，将策略设计从文献中站住：区分 Cortex/Smart、vLLM/Orca、Ray/Daft、COPY/pgai/Delta/TurboVecDB 等工作中可借鉴的优化思想、只能作为 baseline/边界的部分，以及本文自己的上游执行链路策略定义。
- 明确当前策略推荐写成 “workload-aware 数据组织 + 模型服务状态感知调度 + 写回约束验证”，不提前声称 finalized learned optimizer、通用 Ray Serve 调度器或存储引擎优化。
- 同步更新 `experiments/plans/README.md` 和 `PROJECT_INDEX.md`，要求后续更新策略设计图或方法口径前先查阅该文件。

# 2026-07-15 Ours-v0 优化策略逻辑图

- 新增 `figures/architecture/optimization_strategy_logic.png` / `.svg`，将优化策略细化为“输入信号 -> 规则表选择器 -> 策略动作与配置 -> 端到端验证与回填”。
- 新增 `figures/scripts/generate_optimization_strategy_logic.py`，统一生成 PNG/SVG，并检查核心卡片边框、底部验证框和 7 条箭头是否越界或穿过无关卡片。
- 新增 `figures/audit/optimization_strategy_logic_audit.md`，记录该图的文献边界和可见标签要求：不声称 finalized learned optimizer，不把写回或跨层联合最优作为当前主 claim。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`，将该图纳入正式架构/方法图资产。

# 2026-07-15 顶会系统论文策略图范式整理

- 新增 `figures/audit/top_venue_strategy_figure_design_notes.md`，从 vLLM、Orca、Cortex AISQL、Ray Data 等系统论文图形中抽取方法图范式：优先画运行时机制、running example、data/control path 区分和紧凑规则表，而不是三列术语堆叠。
- 明确下一版策略图建议采用 “control-loop + running example + compact rule table”：上半部画 database AI query 到 batch queue、strategy selector、actor/endpoint、sink、E2E metrics 的控制循环，下半部画 Trigger -> Action -> Guardrail 规则表。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`，要求后续重绘策略图前先阅读该设计备忘。

# 2026-07-15 策略图小机制拆分与论文下载清单

- 新增 `figures/audit/strategy_figure_micro_design_points.md`，将后续策略设计图拆分为可独立绘制的小优化点：workload-aware batch/partition、bounded in-flight 反压、endpoint routing、写回约束和 Trigger -> Action -> Guardrail 规则表。
- 为每个小机制记录优化对象、参考论文图形范式、建议画法、所需实验证据和 reviewer 风险，避免继续画成“大而全”的术语堆叠图。
- 补充 vLLM、Orca、Ray Data Streaming Batch、Cortex AISQL、Sarathi-Serve、DistServe、Splitwise、FlexPushdownDB 等优先下载/精读链接，并同步更新 `figures/README.md` 和 `PROJECT_INDEX.md`。

# 2026-07-15 本地参考文献 PDF 子集登记与图形阅读

- 新增 `research/reference/README.md`，登记用户已下载的 14 篇本地 PDF 子集，包括 Ray Data、vLLM、Ray、Sarathi-Serve、ServerlessLLM、GaussML、Galois、LEADS、NeurDB、Lance 等；明确该目录只是部分文献，不替代完整文献清单。
- 新增 `figures/audit/local_reference_figure_reading_notes.md`，记录从本地 PDF 图中提取的图形经验：用 `AI_EMBED` running example 锚定主图、把策略动作贴到执行位置、区分数据/控制/反馈流、用规则表或 mini timeline 补充机制。
- 更新 `opening/README.md`、`opening/literature/reading_list.md`、`figures/README.md` 和 `PROJECT_INDEX.md`，将本地 PDF 子集和图形阅读笔记纳入项目入口。

# 2026-07-15 Ours-v0 运行时策略闭环图

- 新增 `figures/architecture/runtime_strategy_control_loop.png` / `.svg`，用一个 `AI_EMBED` SQL 运行例子贯穿 RecordBatch queue、Ray submit gate、Endpoint queues、GPU model service、Results/sink 和 E2E metrics，直接展示 batch/partition、K_max、routing 和 writeback guardrail 的作用位置。
- 新增 `figures/scripts/generate_runtime_strategy_control_loop.py`，统一生成 PNG/SVG，并执行核心卡片边框、主数据流箭头和禁用术语自检；本次生成已通过程序化检查和 PNG 人工预览。
- 新增 `figures/audit/runtime_strategy_control_loop_audit.md`，记录该图为策略机制主图；`cross_layer_method_framework.*` 保留为研究方案总览，`upstream_strategy_design.*` 保留为过渡图，`optimization_strategy_logic.*` 降级为规则表草图。
- 同步更新 `figures/README.md` 和 `PROJECT_INDEX.md` 的图资产入口。
# 2026-07-15 策略图迭代版本归档

- 将暂时不用的策略图迭代版本移入 `figures/archive/architecture/20260715_strategy_iterations/`：`upstream_strategy_design.*`、`optimization_strategy_logic.*` 和内部字体测试图 `_font_test.png`。
- 当前策略设计说明优先使用 `figures/architecture/runtime_strategy_control_loop.*` 与 `figures/architecture/runtime_strategy_rule_table.*`。
- 同步更新 `figures/README.md`、`PROJECT_INDEX.md` 和相关审计文件中的路径说明，避免旧图继续出现在当前主图清单中。

# 2026-07-15 策略闭环图中文化与箭头修正

- 将 `runtime_strategy_control_loop.*` 和 `runtime_strategy_rule_table.*` 的可见标签尽量中文化，仅保留 `AI_EMBED`、`SQL`、`GPU`、`K_max`、`P99`、`token` 等必要技术记号。
- 将主流程框从泛泛的状态字段改为“观测量 / 调节项 / 判定项 / 约束项 / 评价项”，说明这些框是策略选择器读取的信号来源及其作用。
- 收窄主流程卡片、拉大间距并缩小箭头头部，使主数据流箭头有完整线段；重新生成 PNG/SVG 后通过边框、箭头和禁用术语自检。

# 2026-07-15 开题报告 architecture 图同步到三层策略

- 重绘 `figures/architecture/system_architecture_ai_data_execution.*`，将总体架构图同步为计划层数据组织、运行层入口调度、服务端 dynamic micro-batch 与写回瓶颈判定的当前口径。
- 重绘 `figures/architecture/cross_layer_method_framework.*`，将研究方案图从“上游链路调优”进一步明确为“三层上游执行策略与端到端评价”。
- 将 `figures/architecture/runtime_strategy_control_loop.*` 补入 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 作为图 4-2，替代原 Mermaid 链路示意，用于解释策略机制。
- 同步更新 `figures/README.md`、`figures/audit/*` 和 `PROJECT_INDEX.md`，去除当前主图入口中的 `Ours-v0`、`下一轮配置`、`边界确认` 等旧表述。

# 2026-07-15 architecture 图颜色语义修正

- 将 `cross_layer_method_framework.*` 中的三层策略改为三个并列中性卡片：计划层、运行层、服务端层，避免被误读为两个策略框或与上方 workload 颜色一一对应。
- 将 `system_architecture_ai_data_execution.*` 底部研究内容卡片统一改为中性色；上方系统阶段继续保留数据层、Ray 执行层、GPU 模型服务和结果存储的颜色编码。
- 将研究内容 2 标题调整为 `运行层调度与服务端批处理`，更准确表达当前方案横跨 Ray 入口调度、endpoint routing 和模型服务侧 `micro-batch`。

# 2026-07-15 研究缺口图与候选规则表口径修正

- 重绘 `research_gap_three_islands.*`，将底部研究内容同步为数据组织与批处理构造、运行层调度与服务端批处理、写回瓶颈判定，避免旧的“GPU 服务感知调度”与当前三层策略不一致。
- 将 `runtime_strategy_rule_table.*` 从“策略规则表”改为“候选策略规则表”，明确表中规则是待实验验证的触发逻辑，不代表已证明结论。
- 同步更新 `figures/README.md`、`PROJECT_INDEX.md` 和相关审计记录。

# 2026-07-15 开题报告正文同步三层策略口径

- 更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`，将文献综述、研究目标、研究内容、研究方案、进度安排和预期成果同步到当前方向。
- 研究内容二统一表述为“运行层调度与服务端批处理协同方法”，覆盖 `K_max`、endpoint routing、actor pool、backpressure 和服务端 `micro-batch`。
- 将方向三改为“写回瓶颈判定与端到端收益检查”，避免把写回写成当前独立主贡献。
- 清理旧的“岛”“GPU 调度优化”“联合最优/Killer Experiment”等主叙事表述，保留其作为后续增强对照的可能性。
## 2026-07-20 数据组织策略机制图正式化

- 使用 `figure-designer` 和 `nature-figure` 的论文图规则审计新增的数据组织策略机制图，将 `rc1_*` 草图口径调整为正式可引用的 `data_organization_*_mechanism.*` 系列。
- 新增三张 architecture 机制图：`data_organization_token_budget_mechanism.*`、`data_organization_length_align_mechanism.*`、`data_organization_prefix_aware_mechanism.*`，分别解释 token-budget batching、length-aligned grouping 和 prefix-aware grouping。
- 重写 `figures/scripts/generate_data_organization_strategy_mechanism.py`，输出 PNG/SVG，并对正式 SVG 执行 `RC/BL` 等禁用可见术语检查。
- 新增 `figures/audit/data_organization_strategy_mechanism_audit.md`，明确这些图是候选机制说明，不是实验结果图；prefix-aware 图仅声称创造 prefix locality，不提前声称 APC 收益。
- 更新 `figures/README.md` 和 `PROJECT_INDEX.md` 的图资产入口，说明旧 `rc1_*` 文件不再作为正式报告/PPT/论文入口。
- 根据 PPT 预览反馈修正 `data_organization_length_align_mechanism.*` 的标题字体混排问题，将含 `batch` 的粗体混排标签改为更稳定的纯中文机制标签，并同步替换 v5 PPT 中的对应图片。
## 2026-07-20 开题 PPT v5 增量版

- 新增 `opening/slides/opening_defense_20260720_v5.pptx`，由 v4 拷贝后增量修改生成，未重跑 `opening/slides/build_ppt.py`。
- 在研究内容一后新增三页数据组织机制图（token-budget、length-align、prefix-aware），图源来自 `figures/architecture/data_organization_*_mechanism.png`。
- 将原研究内容一中的 prefix-aware 表述收紧为候选验证口径，避免提前声称 KV-cache / APC 收益。
- 更新 `opening/README.md`、`opening/slides/README.md`、`opening/logs/project_log.md` 和 `PROJECT_INDEX.md`。

## 2026-07-21 提交控制策略机制图正式化

- 重写 `figures/scripts/generate_submission_control_mechanisms.py`，修复原脚本编码损坏问题，并统一生成三张提交控制策略机制图的 PNG/SVG。
- 更新 `submission_control_queue_adaptive_mechanism.*`、`submission_control_kmax_admission_mechanism.*` 和 `submission_control_pool_routing_mechanism.*`，将叙事收敛为“提交时机、提交数量、提交去向”三类上游提交控制决策。
- 图中统一使用保守表述：只说明候选机制和验证指标，不把当前设计写成已验证性能结论，也不暗示修改 vLLM 内部调度、Ray 调度器或数据库优化器。
- 新增 `figures/audit/submission_control_strategy_mechanism_audit.md`，记录图的角色、证据边界、验证指标和 QA 检查。
- 更新 `figures/README.md` 和 `PROJECT_INDEX.md`，将 submission-control 机制图纳入正式图资产入口。

## 2026-07-22 提交控制策略机制图重绘修正

- 根据图面反馈重绘 `submission_control_queue_adaptive_mechanism.*`、`submission_control_kmax_admission_mechanism.*` 和 `submission_control_pool_routing_mechanism.*`，将大面积红绿对照块改为白底卡片式机制图，降低草图感。
- 修复 K_max 图中请求槽位越出 vLLM 服务入口框的问题：所有槽位改为在父框内部按宽度居中计算。
- 修正分池路由图箭头语义：箭头从“请求形态判别”组件边界出发，并分别落到短请求池、长请求池、前缀相似池的左边界。
- 重新运行 SVG 坐标越界检查、PNG 非背景边界检查和 SVG 禁用词/乱码扫描，结果均通过。
- 更新 `figures/audit/submission_control_strategy_mechanism_audit.md` 记录本次 redesign QA。

## 2026-07-22 图表箭头边界 QA 规则补充

- 根据 submission-control 机制图反馈，在 `figures/AGENTS.md` 新增箭头方向与边界检查规则。
- 后续所有机制图、架构图、流程图除画布越界外，必须检查箭头是否从源组件边界出发、是否指向目标组件边界、方向语义是否一致，以及是否穿过无关卡片或文字。
- 同步更新 `figures/audit/submission_control_strategy_mechanism_audit.md`，将箭头边界关系纳入本组图的 QA 记录。

## 2026-07-23 P1/P2 文献精读批量完成（8 篇）+ 知识库同步

- 按用户给定的 P0/P1/P2 优先级清单，完成 **P1 四篇 + P2 四篇**深度精读（沿用 `tpl-文献精读-深度版` 四层模板），连同此前完成的 P0 四篇（Clipper / CONCUR / CoLoRA / SABER），精读笔记总数由 16 增至 **28 篇**。
- 新增笔记（`research/reading_notes/`）：
  - P1：`scorpio_llm_serving_2025`、`bucketserve_2025`、`sglang_neurips2024`、`splitwise_isca2024`
  - P2：`proserve_2025`、`distserve_osdi2024`、`flashattention_neurips2022`（自读全文）、`flexgen_icml2023`（自读全文）
- 全部笔记已两次同步至知识库 `../ai-operator-wiki/raw/papers/`（每完成四篇同步一次）。
- FlashAttention、FlexGen 两篇 PDF 此前未下载，本次补下到 `research/reference/`（arXiv 2205.14135 / 2303.06865，已校验 `%PDF` + `%%EOF`）。
- **精读勘误（重要，已写入 `reading_list.md`）**：原始任务描述两处与论文实际内容不符，精读代理据原文修正——(1) DistServe 全文用 simple FCFS，**无** AFGM fairness 与 prediction-based pairing（已 pdftotext 全文核实，§4.3 原文）；(2) ProServe 真实主题是**多优先级请求调度**（TDG + SlideBatching + GoRouting），**非** "预测式 prefill/decode 分离调度"。笔记均按论文真实内容撰写。
- **对课题的含义（策略补强方向，详见各笔记第四层）**：
  - RC2 自适应控制器形成三候选对比：Clipper AIMD（整体 batch size）/ Scorpio TRP+Credit（per-request 频率）/ CONCUR EWMA；Scorpio 的解析 ITL 模型同时是研究内容四（算子代价估计）的直接模板。
  - RC1 数据组织：BucketServe 的 padding 形式化（Eq.2/3）+ length 分桶、SGLang RadixAttention（Theorem 3.1 DFS 最优排序）+ 与 vLLM APC 互补、Splitwise/DistServe 的 Lm 饱和阈值（512 token 饱和 A100）共同支撑 token-budget / length-align / prefix-aware 分组。
  - 背景与对照：FlashAttention 提供理解 vLLM 内部 memory-bound 行为的底层理论链（→ Sarathi-Serve → 本课题 token-budget），并把 database join 与 GPU attention 并列于 IO-aware 谱系；FlexGen 作为"离线吞吐优先"对照锚点，明确本课题 online serving 定位。
- 更新 `opening/literature/reading_list.md` 精读笔记索引（16→28，新增 P0/P1/P2 三组 + 勘误说明）。
- 环境备注：本环境 Read 工具无法渲染 PDF（缺 pdftoppm），精读改用 `pdftotext`（xpdf 4.06）提取全文；已确认 `reference/` 与 `reading_notes/` 无 `.txt` 中间文件残留。

## 2026-07-24 提交控制（K_max/flush）与自回归生成特性：厘清与合并进现有文档

- 核心厘清：自回归生成的两个特性（decode 阶段 memory-bound、输出长度不可预测/完成时间异质）是**提交控制（K_max 自适应 / queue-adaptive flush）的物理前提**，而**数据组织（token-budget）依据已知输入 prompt、不依赖自回归**——由 `code/src/organizers.py:230` `_row_token_cost = prompt_tokens + completion_max_tokens` 代码确证。
- **关键增量（假设 H，待验证）**：RC2 adaptive 负结果（P0-1，foreground E2E 10.2s vs static K=8 的 7.3s）的现有归因是“控制器粗糙”；提出**未被识别的混淆变量——实验 `--completion-max-tokens 64` 固定 output** 消除了自回归变异源，adaptive 运行时动态优势可能无从发挥。建议 3 轮控制器改进前先用变长 output 重验。
- **内容归位（不另建文件，合并进现有文档）**：物理前提 + 架构边界 → `service_scheduling_backpressure.md` §0.5；adaptive 负结果归因 + 变长 output 实验方向 → `experiment_status_and_gaps.md` P0-1 + §4 P0；实现注意事项（EWMA 默认关闭 / AIMD 作对照 / 抓取节流 / flush 口径）→ `strategy_design_implementation_reference.md` §8.2；fatal flaws 缺口（Clipper Poisson / CONCUR 中段抖动 / BucketServe prefill-only）→ `strategy_design_literature_basis.md` §3.1。
- 同步修正：`PROJECT_INDEX.md:366` 补 adaptive 负结果限定（口径超前）。
- 待办（本次未动）：① 文献笔记因果链错误归因（`flexgen:201`/`sarathi:152,203,250`/`flashattention:149`/`top15:156` 把 token-budget 动机错挂 decode memory-bound）；② 开题材料 K_max/flush 段未引论文、未讲清与 vLLM 内部 continuous batching 互补关系——用户指示开题材料本次不动。

## 2026-07-24 plans/ 文档导航治理（方案 A，不移动文件）

- 问题：`experiments/plans/` 下混了三类性质不同的文档（实验计划 / 设计参考 / 状态审计），命名未体现性质，且两个 `strategy_design_*` 名字接近易混。
- 处理（导航治理，非物理移动/合并/改名）：
  - 重写 `experiments/plans/README.md`，按性质分三组（一、实验计划；二、设计参考；三、状态审计），并在设计参考组点明两个 strategy_design 的分工（`literature_basis`=边界论证 / `implementation_reference`=工程映射）。
  - 两个 `strategy_design_*.md` 开头各加"与对方分工"的交叉说明。
  - 不移动文件路径、不改名、不合并——避免破坏全项目引用路径（surgical changes）。
- 明确：不在 plans/ 再建技术文档层；技术基础（decode memory-bound / AIMD / continuous batching）单一来源在 `research/` 与 `research/reading_notes/`，plans/ 只引用、不重复。

## 2026-07-24 精读笔记配图重做（9 张，确定性抽取 + 完整性验证）

- 背景：`reading_notes` 引用的 9 张论文配图此前裁剪有问题（内容错位、切边、带正文、留白不均/歪斜）。本环境 Read 无法直接看小图、vision MCP 不可靠（对彩色图假报"文字"、坐标估计失准），改用**确定性像素分析**。
- 方法：①嵌入栅格图直抽（cortex_fig1/fig7，像素级精确）；②矢量图按"图题锚定底部 + 列/页面文本宽度定左右 + 彩色像素/水平墨线定图框"裁剪；③每张用墨迹 bbox 验证四周白边≥22px 确认不切边；④最后统一收紧到 ~28px 均匀留白（只裁白边，不动图内容）。
- 结果（9 张，三处一致：`research/reading_notes/figs/`、`opening/literature/top15_reading_notes/figs/`、wiki `raw/papers/figs/`）：cortex_fig1/fig7、galois_fig3、neurdb_fig2、orca_fig11、ray_fig8、sarathi_fig4/fig9、vllm_fig12——每张内容对应笔记引用的 Figure N，完整未切边。
- **orca 更正**：top15 清单已更新（orca/distserve 进，saber/multibin 出），orca 是 top15 成员，其 fig11 须保留——核实为**单栏左图**（plot 框仅在左栏 y72-220，右栏为正文），按左栏裁剪即完整。中途曾误删，已恢复。
- 修正 `opening/literature/top15_reading_notes/README.md` #10-15 顺序与权威源 `research/top15_ranked_papers.md` 一致（原 README 误把 Cortex/NeurDB/Galois/DB-Perspective 排在 Ray-Data/BucketServe 之前）。

## 2026-07-24 为 14 篇精读笔记补充论文配图（架构图/支撑图）

- 评估：15 篇精读笔记中 7 篇原有图，8 篇无图。逐篇核对"笔记讲解内容是否需要图 + 论文有无合适图"——**db_perspective** 是 perspective 论文、本身无图，跳过；其余 7 篇各选 1 张最贴合讲解的图：clipper Fig1（两层架构）、sglang Fig3（RadixAttention 操作）、distserve Fig3（两阶段吞吐，支撑"阶段特征不同"核心论点；该文无纯架构图）、splitwise Fig10（三池系统图）、concur Fig4（System Overview）、ray_data_streaming Fig1（Logical dataflow graphs）、bucketserve Fig4（Architecture）。
- 抽取方法（矢量图，无嵌入栅格）：图题锚定底部 + **彩色范围 ∪ 矢量范围**定图框（彩色覆盖 plot/栅格插图，矢量覆盖灰度架构图，单独用任一会漏）+ 列/页面宽度定左右 + 智能底部（图与图题间隙>40pt 则按矢量底，否则按图题）+ getbbox 收紧 + 28px 留白。
- 关键修正：图题查找器最初返回**正文里的"Figure N 引用"**（非真图题）——改用"上方 280pt 内有>5 个矢量对象"判定真图题，排除正文引用。此 bug 曾导致 sglang（误用 p3 正文引用，真图题在 p4）、concur 裁错。
- 验证：7 张墨迹 bbox 四周均 28px（完整不切）；逐张视觉确认内容与笔记讲解一致。
- 嵌入：7 篇笔记（权威版 `research/reading_notes/` + 快照 `opening/literature/top15_reading_notes/`）各加"## ▎配图（辅助讲解）"区块，嵌图 + 1-2 句说明 tying 到核心论点。同步 wiki `raw/papers/figs/`。
- 结果：15 篇中 14 篇有配图（仅 db_perspective 无）；三处一致 16 张图（原 9 + 新 7）。

## 2026-07-24 补 top15 精读的来源说明（provenance）

- 用户反馈："开题的 top15 文献精读来自 research 全量文献排名前 15，但项目内无文档说明此关系。"
- 核查：`research/top15_ranked_papers.md` 第 4 行已写"候选池：`ai_operator_literature_inventory.md`（66 篇）"，但开题交付面 `opening/literature/top15_reading_notes/README.md` 未说明选取链路，故用户在 opening/ 侧看不到来源。
- 处理（合并进现有 README，不新建文件）：在 `top15_reading_notes/README.md` 加"来源与选取链路（provenance）"段，显式写出三步链路：候选池 `ai_operator_literature_inventory.md`（66 篇，v5）→ `top15_ranked_papers.md` 学术排名选前 15 → `research/reading_notes/`（33 篇精读含此 15）权威版 / 本目录为快照拷贝。
- 文档链路现状：`reading_list.md` → 指向 `top15_ranked_papers.md` + 本目录；`top15_ranked_papers.md` → 标候选池；本 README → 完整 provenance。
- 用户进一步指出"`research/reading_notes/`（Top 15 的权威来源库）本身无 README"：新建 `research/reading_notes/README.md`，说明本目录作用（33 篇精读笔记 + `figs/` + 模板）、provenance 链路（inventory 66 → 精读 33 → Top 15 → 开题快照）、与 top15 快照及 wiki 的关系、配图与编辑规则；同步更新 `PROJECT_INDEX.md` 该目录条目。

## 2026-07-25 RC2 adaptive admission controller 设计确认

- 对现有 shared-vLLM 结果复核：静态 `K_max=8` foreground mean E2E 为 `6.602s`，现有两档 adaptive 为 `10.214s`；当前只能证明 admission guardrail 必要，不能证明 adaptive 优于静态策略。
- 确认采用“先可观测、再改控制律”的最小方案：控制器与 Prometheus/Ray/Daft 解耦，移除热路径 `sleep`，增加 K_max/inflight/queue/KV 时序记录，再实现无 EWMA 的死区非对称 AIMD。
- 新增 `code_doc/superpowers/plans/2026-07-25-adaptive-admission-controller-design.md`，明确范围、架构、异常语义、固定输出混淆变量实验、成功/放弃条件、TDD 与正式 GPU 验证边界。
- 本次只落盘已批准设计，尚未修改生产代码或产生新实验结果。

## 2026-07-25 运行层策略套件总体设计确认

- 用户将实现范围扩展为完整运行层策略套件：独立 queue-adaptive flush、actor pool 分池与动态路由、多 endpoint/未来多 GPU 拓扑、PID/EWMA/UCB 控制器，以及 batching × submission 联合搜索。
- 硬件边界确认：当前仅一张 RTX 5070 12GB。代码保留多 GPU endpoint topology 扩展，但当前正式实验只形成单 GPU 证据；同 GPU 多 endpoint 不用于声称多 GPU 扩展收益。
- 学习型控制器首版选择有限动作空间 UCB，不提前实现需要大量训练数据的监督式代价模型。
- 新增 `code_doc/superpowers/plans/2026-07-25-runtime-scheduling-strategy-suite-design.md`：采用分层策略接口，将 batching、flush、admission、pool routing、endpoint routing、topology、scheduler 和 search 解耦；明确失败/回退语义、TDD/不变量/集成测试、9 点核心联合矩阵、12 点含 flush 扩展矩阵、2048 行 held-out evaluation 和统计规则。
- 指标扩展为 `runs.csv`、`submissions.csv`、`requests.csv`、`control_trace.csv`、`resource_trace.csv` 与 `manifest.json`，区分 batch-level submission 与 row/model-sequence 粒度，并覆盖 tokens/s、全阶段 latency、tail、SLO、公平性、控制器轨迹、路由、endpoint/GPU 资源和 instrumentation overhead，为后续批量绘图保留原始数据。
- 原 `adaptive-admission-controller-design.md` 保留为 AIMD 子模块细化设计；本次仍仅完成总体设计，未修改生产代码或产生新 GPU 结果。

## 2026-07-25 运行层策略套件第一阶段实施计划

- 总体设计经用户审阅确认后，新增 `code_doc/superpowers/plans/2026-07-25-scheduling-foundation-implementation.md`。
- 第一阶段严格收敛为 typed request/topology schemas、静态 admission、round-robin routing 和 deterministic synchronous scheduler；先验证 exactly-once 与 bounded-inflight 不变量，不在同一变更中接入动态 flush、PID/EWMA/UCB 或生产 Ray 路径。
- 计划规定逐行为 RED/GREEN、项目 `.conda/pg-ai-profile` 测试环境、完整 test suite、compile/import 检查和分任务提交。
- 用户进一步确认所有后续设计与计划必须位于 Daft + Ray 框架内。总设计和实施计划已补充正式链路 `PostgreSQL -> Daft -> Arrow payload boundary -> Ray task/actor -> endpoint`；纯策略模块保持引擎无关仅为可测试性，fake/synchronous adapter 只用于测试。
- 第一阶段新增 Daft organizer -> Arrow payload -> 单节点 Ray task contract smoke；该 smoke 未通过前不进入 adaptive 策略实现。
- 执行 contract smoke 时确认当前 Ray 已移除 `local_mode=True`；测试改为 `ray.init(num_cpus=1)` 的本机单节点 Ray task，仍使用真实 Ray runtime，不以 mock 替代。

## 2026-07-25 Scheduling foundation 第一阶段实现与验证

- 在隔离分支 `feat/runtime-scheduling-foundation` 新增 `code/src/scheduling/`：不可变 `BatchRequest`/endpoint/topology schema、静态 K_max admission、健康 endpoint 过滤、deterministic round-robin 和 synchronous policy-composition scheduler。
- 策略包依赖扫描无 `daft`/`pyarrow`/`ray` import；正式 framework contract 仍为 `PostgreSQL -> Daft -> Arrow payload -> Ray task/actor -> endpoint`。
- 新增 4 个测试模块：schema 3 tests、policy 6 tests、scheduler 2 tests、真实 Daft→Arrow→单节点 Ray task contract 1 test。
- 项目 `.conda/pg-ai-profile` 环境全量验证：11 个测试模块、54 tests、0 failures；`compileall`、public import smoke 和策略层 engine-import 扫描通过。
- 当前完成的是 typed foundation 与真实 Daft/Ray contract，不是性能实验：尚未替换生产 Ray 提交循环，未实现 queue-adaptive flush/AIMD/PID/EWMA/UCB，也没有新增 vLLM 吞吐或延迟结论。

## 2026-07-25 Static Ray task/actor 接线实施计划

- 新增 `code_doc/superpowers/plans/2026-07-25-ray-static-wiring-implementation.md`，第二阶段只把 typed scheduler 接入 profiler 的静态 Ray task/actor 路径。
- 先扩展 typed collection timing 与通用 Ray adapter，再分别验证 task/actor 行为和原 CSV metric keys 等价。
- 旧 adaptive 分支原样隔离保留；本阶段不同时改变控制算法，避免把重构影响与策略收益混淆。
- 用户要求代码、测试和单 GPU 实验全部跑通并产出数据后再合并 `main`；开发使用项目内 `.worktrees/scheduling-foundation` 隔离 worktree，根 `.gitignore` 增加 `.worktrees/`。

## 2026-07-25 Static Ray task/actor 正式接线完成

- `postgres_ai_operator_profile.py` 的静态 `ray_task` 与 `ray_actor` 路径已统一委托给 typed `SynchronousScheduler` 和 `RaySubmissionAdapter`；旧 `queue_adaptive` 循环被隔离保留，尚未宣称控制算法改进。
- Arrow batch 在进入 Ray 前转换为 `BatchRequest` 元数据，但 payload 保持原 Arrow 对象；endpoint/actor 显式进入 `TopologySnapshot`，当前单卡实验统一标记 `gpu_id=0`。
- 保持既有 profiler 指标键：`operator_invocations`、`max_inflight`、bounded wait、fan-in、submit timing 与 adaptive 兼容字段。
- 项目 `.conda/pg-ai-profile` 环境验证：全量 15 个测试模块、70 tests、0 failures；真实 Daft→Arrow→单节点 Ray task/actor 契约均通过，`compileall`、public import 与策略层 engine-import 扫描通过。
- 该结果只证明静态执行接线和行为契约，不是 GPU 性能收益。queue-adaptive flush、AIMD/PID/EWMA/UCB、分池动态路由、联合搜索和正式单 GPU 数据仍待实现；完成前不合并 `main`。

## 2026-07-25 Adaptive controller family 实施计划

- 新增 `code_doc/superpowers/plans/2026-07-25-adaptive-controller-family-implementation.md`，按 typed observation/decision → AIMD/EWMA → PID → UCB → Ray scheduler 接入的顺序实施。
- 控制器保持引擎无关，不执行网络、sleep、Ray 或文件 I/O；缺失/陈旧指标统一 hold，静态策略不读取 adaptive metrics。
- 旧 two-level adaptive 暂时保留为显式 baseline；新控制器完成单元/契约测试后再进入单 GPU 正式对照，仍不合并 `main`。
- 复核 `experiments/plans/` 后补齐执行约束：正式 adaptive 对比前先做固定 64 与 EOS-permissive 256 output cap 混淆变量检查；优先无 EWMA AIMD，EWMA/PID/UCB 为后续对照；正式记录 tokens/s、service P99 与 inflight/queue/K_max 时序，并沿用三轮改进放弃条件。
- 联合搜索口径纠正为两层：先完成状态审计要求的 token_budget `{4096,6144,8192}` × K_max `{4,8,16}` 共 9 点核心实验，再做包含三种 flush 的 12 点扩展网格；UCB 单独报告，不替代独立拼接 vs joint grid。

## 2026-07-25 Adaptive controller core 与 profiler 接入

- 新增 typed `AdmissionObservation`、`WindowDecision` 与 diagnostics；实现无平滑 AIMD、可选 EWMA-AIMD、bounded PID、有限动作 UCB1 和 SLO-constrained reward。
- 新增 250ms 默认缓存 observation provider、stale/missing hold、无 sleep 的 dynamic admission gate 与逐决策 trace；动态降窗时 scheduler 的 bounded-inflight 不变量已有确定性测试。
- profiler CLI 新增 `aimd|ewma_aimd|pid`，在整个 run/多 DB fetch chunk 间保持控制器状态，并统一走 Daft→Arrow→Ray task/actor 路径；control trace 记录 inflight、K_max、running、waiting、KV、action、reason 与 allowed。
- legacy `queue_adaptive` 继续隔离作为对照。UCB 只完成策略/reward core，尚未在缺少 epoch 指标时暴露为 CLI，避免伪集成。
- 当阶段项目 `.conda/pg-ai-profile` 全量回归为 105 tests、0 failures，包含真实 Daft→Arrow→单节点 Ray task/actor 契约；compileall、public import 和策略层 engine-import 扫描通过。CLI AIMD dry-run 通过。
- 尚无新增 GPU 性能数据，不能声称 adaptive 优于静态 K=8；正式对比前仍须完成 output-length 混淆变量检查。分支继续隔离，不合并 `main`。

## 2026-07-25 Actor pool、endpoint topology 与独立 flush core

- 新增 request-cost pool router、least-queued endpoint router、确定性 rendezvous prefix affinity 与 unhealthy least-queued fallback；scheduler 可组合 pool/endpoint 两级路由。
- profiler 可记录并使用每个 Ray actor/task endpoint 的 pool ID 与 GPU ID；真实 Daft→Arrow→Ray actor 契约验证短/长 batch 进入不同逻辑 actor pool。当前两池仍共享 RTX 5070，只是行为证据。
- 新增 immediate、fixed-timeout、queue-adaptive 三类独立 flush policy，覆盖 budget、低负载、拥塞、missing/stale metrics 和 hard max-wait。
- fatal-flaw audit：现有 `source_order=arrival_time` 只排序不按时间 replay，不能产生真实 pending-wait/flush 证据；正式 flush 实验前必须增加 Ray pending queue 与 arrival-paced enqueue。
- 加入上述变更后的新鲜全量回归：122 tests、0 failures，包含真实单节点 Ray task/actor 与分池 actor contract。仍未产生 GPU 性能结果，未合并 `main`。

## 2026-07-25 加速到达 flush 策略真实单 GPU 实验

- 新增 arrival time scale、完整 flush/submission/resource trace，并在真实
  `PostgreSQL -> Daft -> Arrow -> Ray task -> vLLM` 链路完成三策略门禁。
- 正式矩阵共 18 次运行：immediate、fixed timeout、queue adaptive 各 1 次
  预热 + 5 次正式重复；每次 512 个文档均 exactly-once，未使用 fake。
- fixed timeout 相对 immediate 减少 8.984% submissions，但 tokens/s 仅提高
  0.185%，95% CI 重叠；当前 queue adaptive 没有形成多行 batch，tokens/s
  低 0.966%，不能声称动态策略有效。
- 第一次 queue-adaptive 预热触发 vLLM 请求超时和 7 个遗留 running 请求；
  失败数据未进入 CSV。仅重启实验 vLLM 容器并等待空闲后完整重跑，限制已写入
  manifest 和结果报告。
- profiler 新增直接输出 `tokens_per_s`，使用 vLLM 实际 token 增量；结果目录为
  `experiments/results/accelerated_arrival_flush_20260725/`。分支继续隔离，
  尚未合并 `main`。
- 后续 1024 条同密度单次探针中，queue adaptive 仍为 1024 submissions、
  平均 batch rows=1；扩大行数没有修复 batch formation，因此暂不直接运行
  2048 条正式重复，先修正 adaptive 窗口和事件时间处理。

## 2026-07-25 Adaptive flush 双窗口改进设计

- 根因复核确认：硬编码 `low_load_running=64` 与 `K_max=8` 不匹配，250 ms
  采样慢于 25–50 ms flush horizon，低负载立即 flush 放弃 fixed-timeout
  合并机会，且下游背压后 runtime 会在吸收 deadline 前到达行之前先超时关闭。
- 用户确认吞吐优先、P99 guardrail 的双窗口方向：低负载/缺失指标退化为 25 ms
  fixed baseline，服务压力下扩展到 50 ms；窗口在 batch 打开时选择并保持不变。
- 新增
  `code_doc/superpowers/specs/2026-07-25-adaptive-flush-window-design.md`，
  明确事件时间 catch-up、exactly-once、trace schema、64/1024/2048 分级门禁和
  claim boundary。本步骤只固化设计，尚未修改 adaptive flush 行为。

## 2026-07-25 Adaptive flush 双窗口实施计划

- 用户审阅并确认双窗口设计后，新增
  `code_doc/superpowers/plans/2026-07-25-adaptive-flush-window-implementation.md`。
- 计划分为显式窗口选择、event-time catch-up、profiler/trace 接线和真实单 GPU
  分级门禁四项；每项严格 RED→GREEN，64/1024 门禁失败即停止，不直接消耗
  2048 正式实验时间。

## 2026-07-25 Adaptive flush 双窗口实现与真实单 GPU 复验

- 实现显式 `FlushWindow`，queue-adaptive 在低负载/缺失指标时使用 25 ms
  fallback，在 waiting/KV/running 压力下使用 50 ms；running 阈值绑定本次
  `max_inflight`，窗口在 pending batch 打开时只选择一次。
- arrival replay 改为 event-time catch-up：下游 Ray 背压后，deadline 前已经
  到达的完整行仍进入当前 batch；`SystemReplayClock` 对系统提前醒来循环等待。
- flush trace 升级到 schema 2，新增 `selected_wait_s` 和 `window_reason`。
  全量回归 172 tests 通过，包含 3 条真实 Daft→Arrow→Ray task/actor 契约，
  `compileall` 通过。
- 64 行门禁和 1024 行行为探针均通过 exactly-once、batch formation、tokens/s
  与 service P99 guardrail；未使用 fake backend。
- 512 行正式重复共 18 次运行（每策略 1 次预热 + 5 次正式）。adaptive 相对
  新版 fixed timeout：observed tokens/s +3.671%、submissions -23.500%、
  平均 batch rows +30.732%、batch service P99 -8.010%；每轮 512 个文档
  exactly-once。
- 该结果是单 GPU、加速到达、固定 16-token 输出、固定策略组顺序下的正向候选
  证据；尚缺逐 repeat 随机化、变长输出、per-request E2E P99 和 2048 行
  held-out，不合并 `main`。结果见
  `experiments/results/adaptive_flush_window_20260725/`。

## 2026-07-25 实验基础设施与后续优化路线确认

- 复核总体计划、当前代码和项目文献后，确认 request lifecycle trace、随机化
  scenario runner、`target_output_tokens` 成本利用、Best-Fit bin-packing、
  UCB profiler 接线、联合搜索、actor-local async queue、多模态和代价估计仍是
  主要缺口。
- 用户要求先把 infra 搭稳，使后续多模态和代价估计容易接入；同时明确不需要为
  尚无真实调用方的功能提前创建空接口。
- 新增
  `code_doc/superpowers/specs/2026-07-25-ai-operator-execution-infra-design.md`：
  将总体定位调整为数据库 AI 算子外部执行 infra，分为数据进入与组织、运行时
  控制、Ray 执行、模型服务边界、可观测性与实验控制；定义
  request/submission/run 三层 schema、客户端可观测单 prompt E2E、seeded
  随机化 runner、output-aware BFD、控制器与联合搜索、actor runtime，以及
  多模态/代价估计/`@daft.cls` 的触发边界。
- 总体范围拆为四个独立子项目，第一项为 request lifecycle 与 scenario runner；
  每项单独设计、TDD、真实 Daft→Ray→vLLM 验证，不在同一提交同时改变计时、
  batch membership 和 admission control law。

## 2026-07-25 AI 算子执行 infra 第一阶段实施计划

- 用户确认按代码主要缺口和文献候选技术依次完善；先实施 request lifecycle 与
  seeded scenario runner，再进入 output-aware packing、控制器/联合搜索和 actor
  runtime。
- 新增
  `code_doc/superpowers/plans/2026-07-25-request-lifecycle-scenario-runner-implementation.md`，
  按 submission lifecycle → row seed/join → profiler request CSV/SLO → seeded
  runner → 真实 64 行门禁拆为五个可独立审查的 TDD 任务。
- 第一阶段明确不改变 batch membership、flush window、admission law 或 routing
  决策；batch endpoint 的逐行 E2E 标记为 `latency_granularity=submission`，
  不冒充 vLLM 内部单 sequence 完成时刻。
- compatible endpoint 仅提供 submission aggregate usage，因此逐请求
  `actual_output_tokens` 允许为空；客户端输出 token 估算使用独立字段和来源
  标签，不拆分 aggregate usage，不把估算值冒充服务端实际值。
- 完成 request lifecycle profiler 接线：新增 request CSV、client E2E
  P50/P95/P99、SLO violation/goodput、scenario/seed 字段，并保持既有 Ray
  submit API 的两值返回契约。
- Daft→Arrow→Ray task/actor 四行合同验证 request/submission exactly-once 映射。
  调试中发现直接混用多次 `time.time()` 会因 wall-clock 微调产生亚毫秒倒序，
  改为共享 monotonic-backed epoch clock；backend service epoch 单独标记为
  `service_clock_domain=backend`，跨时钟域无法可靠排序时不生成
  `submit_to_service_s`。

## 2026-07-25 Seeded 场景运行器

- 新增纯函数场景调度模块：warm-up 保持配置顺序，formal 每轮使用记录的 seed
  独立洗牌，输出连续 `order_index`，便于复现实验顺序。
- profiler 新增单次运行身份参数 `--run-phase` 与
  `--run-repeat-index`；场景运行器逐次启动独立 profiler 进程，并在每次运行前
  检查 vLLM health、running 与 waiting 空闲状态。
- runner 在非零退出或缺少预期 run CSV 行时立即停止并记录 incident；每次成功后
  原子更新 manifest。持久化命令与配置会脱敏 API key、认证 token、secret、
  password 和数据库 URL 密码，但保留 token budget 等实验控制量。
- 当前仅完成运行器代码与单元测试，真实 64 行
  PostgreSQL→Daft→Arrow→Ray→vLLM 门禁仍待执行；尚未产生新的性能结论，也未合并
  `main`。

## 2026-07-25 Request lifecycle 真实单 GPU 门禁

- 全量回归 191 tests 通过，包含 3 条真实本地 Daft→Arrow→Ray task/actor
  contract；`compileall` 与 diff check 通过后才启动真实门禁。
- 本机环境为 PostgreSQL 18.4、pgvector 0.8.2、vLLM 0.25.1、
  Qwen2.5-1.5B 和 RTX 5070 12GB；使用 64 个 ShareGPT/BurstGPT prompt，
  fixed timeout 与 queue-adaptive 各运行一次，未使用 fake backend。
- 首次 preflight 因错误参数名 `--model-request-timeout-s` 被 runner 以 exit
  code 2 阻断，未发送模型请求；incident 证据保留。修正为
  `--completion-request-timeout-s` 后继续。
- 首轮数据审计发现 legacy submission trace 没有显式 `submission_id`。按 TDD
  升级为 schema 2 后重新生成最终数据；两场景均为 64 request rows、64 唯一
  request/doc IDs、vLLM success delta=64，request→submission 外键、时间顺序、
  分位数重算、版本字段和最终 service idle 全部通过。
- 最终单次数据：fixed/adaptive request E2E P99 为 2.473097/2.351755s，
  observed tokens/s 为 2972.920/3073.893，submissions 为 22/19。该 64 行、
  每策略一次且 fixed 先运行的门禁只证明基础设施正确，不能声称 adaptive
  性能显著优于 fixed；1 秒 SLO 两者 violation ratio 均为 1.0。
- 结果位于 `experiments/results/request_lifecycle_gate_20260725/`；分支继续隔离，
  未合并 `main`，本次不自动同步 Wiki。

## 2026-07-26 输出成本与确定性 BFD 设计

- 用户确认下一阶段先完善 output-aware cost 与离线 Best-Fit Decreasing
  packing；global BFD 只用于完整可见的离线 organizer input，arrival replay
  保持到达顺序并仅复用成本计算。
- 新增
  `code_doc/superpowers/specs/2026-07-26-output-aware-bfd-design.md`，规定两个小型
  engine-independent core、Arrow/Daft 单一 BFD 实现、exactly-once/oversized
  不变量、packing scope 指标以及 64→512→1024→2048 分级门禁。
- 提交前 fatal-flaw 自检确认：当前 importer 将 ShareGPT prompt 与独立
  BurstGPT trace 逐行配对，因此 `target_output_tokens` 只能标记为
  `burstgpt_unpaired_trace_metadata`。它可用于成本敏感性和装箱验证，但不能称为
  当前 Qwen prompt 的真实输出 oracle，也不能单独支撑输出预测或 GPU 工作量匹配
  结论。
- 真正的离线 oracle 需要同 prompt、同模型、同 tokenizer、同生成参数和同停止条件
  的校准输出，并在不相交的 evaluation run 中回放；标签存在前不创建空接口。
- 根据用户后续会替换 prompt、模型和多模态输入的要求，BFD core 改用中性的
  `cost_units`/`capacity` 边界，不读取 prompt、tokenizer、模型 ID 或图像字段；
  当前仅实现真实文本 adapter，未来新增真实模态 adapter 而不重写 packing、Ray
  调度、lifecycle 和 scenario runner。多资源约束出现时新增真实算法，不把不可比
  单位强行压成伪标量。
- 实施计划映射时发现现有 request lifecycle 只覆盖 arrival replay，而 global BFD
  必须走离线 organizer。设计补充 `request_time_origin`：replay 保持
  `replayed_arrival`；离线 sequential/BFD 统一以 `offline_job_start` 为
  `arrival_epoch_s`、organized batch ready 为 `flush_epoch_s`。该补充只完善观测，
  不改变 batch membership、Ray 调度或模型请求。
- 本次仅固化设计与索引，尚未编写实施计划、生产代码或新增性能实验，分支继续隔离，
  不合并 `main`，不自动同步 Wiki。

## 2026-07-26 输出成本与确定性 BFD 实施计划

- 用户审阅设计并要求开始实施后，新增
  `code_doc/superpowers/plans/2026-07-26-output-aware-bfd-implementation.md`。
- 计划按共享 output cost、纯 `cost_units` BFD、Arrow/Daft 单实现、
  profiler/replay 接线、离线 request lifecycle、真实 Daft-Ray contract、
  64 行门禁与 512 行六单元矩阵七个任务执行；每个行为变更严格 RED→GREEN 并独立
  提交。
- 64 行真实 PostgreSQL→Daft→Ray→vLLM 门禁失败即停止，不直接消耗 512/1024/2048
  实验时间；本阶段最高只运行 512 行三次正式重复，1024/2048 留给重复证据通过后的
  独立确认阶段。
- 计划继续保持单 GPU、分支隔离、禁止 fake 正式数据、禁止把未配对 BurstGPT target
  称为 oracle，并要求记录模型/tokenizer/cost source、global/local packing scope、
  packing utilization、逐请求 E2E 和 service P99。
- 用户要求测试规模尽量一致：64 行仅作 infra 门禁，不进入性能比较；正式六单元矩阵
  全部固定为同一批 512 个 doc、相同 source order/fetch size/model/generation cap/
  token budget/max rows/K_max/writeback，仅改变 packing algorithm 与 output-cost
  mode，并逐 repeat 审计 `(doc_id, prompt_tokens)` 集合完全一致。

## 2026-07-26 Output-aware BFD 512 行预实验约束缺口

- 64 行真实链路门禁验证了 request/submission/resource trace、vLLM FLOP
  counter、功耗与 MFU 字段能够落盘；该规模只作基础设施检查，不作性能结论。
- 首次 512 行矩阵在第 22/24 轮停止。审计发现 sequential token-budget
  只应用 token 容量，而 BFD 同时应用 token 容量与 `ray_batch_rows=16`，
  导致前者单 submission 达 71--94 行，比较混入了不一致的行数上限；失败轮次还
  触发 180 秒 HTTP timeout。该批数据保留为 incident 证据，不进入正式结论。
- 按 TDD 为 sequential token-budget 补齐与 BFD、arrival replay 一致的
  `ray_batch_rows` 硬上限。新增回归测试先复现 `[5]` 与 `[2,2,1]` 的不一致，
  修复后 organizer、profiler scheduling、真实 Daft-to-Ray contract 与全量
  224 tests 均通过。正式矩阵必须从干净 vLLM 服务重新运行。

## 2026-07-26 Row-cap-aware packing 与非阻塞观测设计

- 用户确认继续优化，但明确 BFD 如果造成性能下降可以不使用；候选技术不要求全部
  进入最终系统。
- 新增
  `code_doc/superpowers/specs/2026-07-26-row-cap-aware-packing-and-observation-design.md`，
  将 classic BFD 固定为实验 baseline，默认继续使用 sequential token-budget，
  只有真实单 GPU 512 行筛选与 held-out 1024 行确认均支持时才提升新策略。
- 下一阶段只包含两个可独立验证的改动：把 AIMD/EWMA-AIMD/PID 正式路径接到已有
  非阻塞 metrics provider；新增同时尊重 row cap 与 token budget 的确定性
  row-cap-first best-fit 候选。Daft 流式化、多 endpoint、UCB、prefix 和多模态拆为
  后续独立阶段，避免一次改动引入不可归因的性能变化。
- 正式主比较使用同一模型、文档、固定 output cost、K_max 和测量配置，classic BFD
  与未配对 BurstGPT trace 只作 secondary sensitivity；每轮继续强制真实 vLLM
  FLOP delta、MFU、能耗、逐请求 E2E、submission 数和 exactly-once 审计。
- 用户进一步澄清 BFD 不应按“整体采用/整体删除”二选一。设计调整为机制级消融：
  即使 classic BFD 整体负向，仍可在 row-cap-aware 混合策略中复用经独立对照有效的
  cost 降序、确定性 tie-break、共享硬约束、oversized 单例和 packing diagnostics；
  只有 placement objective 等造成退化的部分被排除。

## 2026-07-26 Row-cap-aware packing 与非阻塞观测实施计划

- 用户确认机制级设计并要求继续，新增
  `code_doc/superpowers/plans/2026-07-26-row-cap-aware-packing-and-observation-implementation.md`。
- 计划分为 observation age/schema、typed adaptive 非阻塞生产接线、纯
  row-cap-first packing、Arrow/Daft 共享 organizer 接线、全量回归、真实 64 行门禁
  和 512→1024 筛选确认七个任务；所有生产行为严格先 RED 后 GREEN。
- 主比较固定 output cost，并以 sequential、classic BFD、BFD-inspired
  row-cap-aware 三组完成最小机制消融；未配对 BurstGPT target-output 只保留为
  secondary sensitivity，不再作为主配置。
- 512 行先单次筛选，只有同时通过 correctness/MFU 门禁且未出现无补偿的显著性能
  退化的候选才运行三次重复；1024 只验证 512 胜出配置，不重新调参。若无候选胜出，
  直接保留 sequential 并停止，不为使用复杂技术而继续扩展。

## 2026-07-26 Row-cap-aware 真实结果与 Infra 状态闭环

- 完成非阻塞 typed-adaptive metrics provider、sample-age control trace、
  row-cap-first packing、Arrow/Daft 共享接线以及 64 行真实门禁。
- vLLM 0.25.1 的 FLOP counter 必须显式启用 `--enable-mfu-metrics`；
  仅看到 metric 名称不足以证明 MFU 有效。本轮通过真实请求验证正 FLOP delta。
- 初始 512 筛选发现 prefix cache 实际开启，导致重复 prompt 的场景顺序依赖和
  180 秒超时。污染数据保留作 incident 审计，不进入性能结论；服务按相同配置
  重建并增加 `--no-enable-prefix-caching`。
- 无 prefix cache 的 512 行三次重复中，row-cap-first 相对 sequential：
  tokens/s +0.68%、request P95 -0.55%、energy/1k tokens -2.81%、
  MFU +1.62%，因此进入 1024 held-out。
- 1024 行三次重复中，row-cap-first tokens/s +0.82%，但 10 秒 SLO
  violation 从 50.39% 上升到 88.67%，SLO goodput 从 37.66 降到
  8.67 req/s。Sequential token-budget 保持默认，classic BFD 不采用，
  row-cap-first 仅保留为研究消融点。
- 场景运行器新增 TDD 覆盖的安全 resume、recovered incident、显式失败场景
  pruning，以及进入 manifest/resume 校验的 `service_metadata`。
- 新增 `code/INFRA_STATUS.md`，统一说明 batching、flush/admission、
  actor pool/endpoint routing、观测与实验基础设施的当前流程、完成度和后续
  实施顺序。
- 所有改动继续位于隔离特性分支，尚未合并 `main`；本次未自动同步 Wiki。

## 2026-07-26 变长输出观测、Adaptive Flush 与联合实验闭环

- Compatible vLLM completion 路径新增显式 token-ID opt-in、per-request
  actual output tokens、finish reason、ChatML prompt envelope、temperature
  与 context-safe source filter；generic compatible endpoint 默认行为不变。
- 自然 EOS 门禁确认 64 请求中 48 个 `stop`、16 个 `length`。512 请求、
  每策略 5 次随机化正式重复中，queue-adaptive 相对 fixed-25：
  tokens/s `+30.09% ± 2.66%`、E2E `-23.05% ± 1.60%`、request P99
  `-27.38% ± 1.87%`。单次 fixed-50 探针与 adaptive 相当，因此不声称
  动态性优于最佳静态窗口。
- 完成 18 单元 token budget × K_max × flush 真实筛选，所有 K16 配置均因
  1.76%–3.13% SLO violation 被 guardrail 排除。
- 完成 4 候选、每项 1 warm-up + 3 formal 的随机化重复。独立拼接相对
  fixed-25 tokens/s `+4.76% ± 2.29%`；联合候选相对独立拼接
  `-0.26% ± 2.07%`；adaptive 8192/K8 相对 fixed-50
  `-0.75% ± 0.97%`。
- 设计决策：本地单 GPU 当前采用 sequential token-budget + static K8 的
  分层优化；当前 accelerated-replay workload 使用 fixed-50。联合搜索保留为
  验证工具，adaptive 保留为跨 arrival-rate 候选，不增加联合在线控制器。
- 新结果位于
  `experiments/results/adaptive_flush_randomized_20260726/` 与
  `experiments/results/joint_batching_submission_512_20260726/`。仍未合并
  `main`，也未自动同步 Wiki。

## 2026-07-26 自然 EOS Fixed-25 / Fixed-50 / Adaptive 三组复验

- 在相同 512-request ChatML 自然 EOS workload 上，对 fixed-25、fixed-50、
  queue-adaptive 各运行 1 warm-up + 3 formal，formal 顺序按 repeat 随机化；
  12/12 成功、0 incident、逐请求 actual output/finish 与 MFU 审计通过。
- fixed-50 相对 fixed-25：tokens/s `+32.23% ± 3.90%`、E2E
  `-24.69% ± 2.72%`、request P99 `-29.27% ± 2.70%`、submissions
  `-31.50%`。
- adaptive 相对 fixed-25：tokens/s `+32.09% ± 6.22%`；adaptive 相对
  fixed-50：tokens/s `-0.10% ± 4.13%`、E2E `+0.13% ± 4.72%`，
  没有可分辨增量，且 submissions 平均多 1.70%。
- 当前单 GPU、当前 arrival rate 的在线候选正式收敛为 fixed-50；
  adaptive 仅保留为跨 arrival-rate 自动选择候选。下一步不继续增加控制器
  复杂度，先验证负载变化时最佳静态窗口是否改变。

## 2026-07-26 单 GPU 文本主线证据闭环

- 完成约 51.4/12.85 req/s 两档新增回放强度的 fixed-25/fixed-50/adaptive
  真实筛选；6/6 场景、3072/3072 请求、0 incident。两档 fixed-50 相对
  fixed-25 tokens/s 分别 +22.50%/+25.80%，adaptive 相对 fixed-50
  -0.61%/-1.32%，未显示跨负载动态切换价值。
- 新增 vLLM `/tokenize` workload 入口，不依赖本机 tokenizer 缓存；从原始
  ShareGPT/BurstGPT 生成 2048 条上下文安全独立请求，未截断或复制。
- 完成 2048 自然 EOS 留出：fixed-50 相对 adaptive tokens/s +1.75%、
  E2E -1.81%、P99 -2.61%；4096/4096 请求完成、0 incident。相对 512 锚点
  吞吐下降约 10%，持续积压仍放大尾延迟。
- 构造 0/30/70/100% 受控 prefix workload。真实筛选暴露并修复唯一 prefix
  哈希重排、prefix grouping 隐式叠加 length-align 两个语义问题；最终
  prefix-only 策略在 cache-off 下无稳定收益，sequential token-budget 保持默认。
- 新增无执行后特征泄漏的 E2E 代价估计：283 行真实 profile、70 个配置组。
  五个 grouped held-out seed 的 ridge 平均 MAE 11.68s、MAPE 50.60%、
  RMSE 25.89s、R² 0.776；MAPE 对小目标敏感，只作为粗粒度编排提示。
- 当前单 GPU 文本默认收敛为 sequential token-budget + static K_max=8 +
  fixed 50ms。adaptive、prefix-aware、BFD 均保留为有显式触发门槛的候选。
  分支继续隔离，未合并 `main`，未自动同步 Wiki。

## 2026-07-26 多 endpoint 路由就绪设计

- 用户确认未来会提供真实多 GPU 环境；当前不通过同一张 GPU 上的双 vLLM
  进程模拟多 GPU 性能，也不把两个逻辑 endpoint 指向同一服务的结果写成扩展性证据。
- 代码审查发现 `LeastQueuedEndpointRouter` 在多个 endpoint 的
  `running + waiting` 相同时固定选择 endpoint ID 最小者，可能使新鲜拓扑的初始
  burst 偏向单端点。该发现属于静态代码证据，当前单 endpoint 正式结果不能量化其影响。
- 新增
  `code_doc/superpowers/specs/2026-07-26-multi-endpoint-routing-readiness-design.md`：
  设计 tie-fair least-queued 和独立的 least-estimated-work 候选；当前用真实
  vLLM 的双逻辑 endpoint 仅验证路由、trace、exactly-once 和客户端开销，未来在
  独立 GPU endpoint 上完成吞吐、尾延迟、MFU、公平性与故障迁移矩阵后才能晋级策略。
- 本次仅固化设计与交接边界，尚未修改生产路由代码或运行新的性能实验。

## 2026-07-26 Ray 与 vLLM 执行层调优设计

- 用户确认把尚未系统使用的 Ray 参数优化加入主线，并要求继续审计其余可优化点。
  现有 2048 held-out 正式配置为 `ray_task`、Daft native、无 partition；
  Ray submit 约 1.54s、fan-in 约 0.17s，而 E2E 约 456.55s，因此不优先做
  object-store、显式 `ray.put` 或 fan-in 微优化。
- 只读审计当前 vLLM 0.25.1 容器确认服务使用 `--enforce-eager`、
  `--gpu-memory-utilization 0.75`、`--enable-mfu-metrics` 和
  `--no-enable-prefix-caching`，且没有显式记录/设置
  `max_num_batched_tokens` 与 `max_num_seqs`。当前服务配置适合作为 eager
  baseline，但不是唯一稳态性能 baseline。
- 代码审查进一步发现 Ray actor 数量当前被直接建模为 endpoint 数量，
  混淆了“独立 vLLM/GPU 服务容量”和“客户端 actor worker”。新增
  `code_doc/superpowers/specs/2026-07-26-ray-vllm-execution-tuning-design.md`，
  要求一个 endpoint 可拥有多个 actor worker，endpoint routing 与 endpoint-local
  worker selection 分层。
- 新设计按 CUDA Graph 门禁、Ray task/actor 有效并发、vLLM scheduling capacity、
  prefix-cache 机制和未来多 endpoint/multi-GPU 的顺序执行；Daft Ray runner
  sweep 仅在数据组织超过 E2E 5% 或多模态预处理到来时触发。
- 本次仍只更新设计与交接规则，尚未重启服务、修改生产代码或新增性能结论。

## 2026-07-26 Ray 执行基础与 vLLM 调优实施计划

- 用户审阅通过 Ray/vLLM 执行层设计后，新增两份相互独立、顺序执行的计划：
  `code_doc/superpowers/plans/2026-07-26-ray-execution-foundation-implementation.md`
  负责 endpoint-local actor pool、Ray CPU/并发/零 GPU/零自动重试契约和结果字段；
  `code_doc/superpowers/plans/2026-07-26-vllm-ray-tuning-experiments.md`
  负责真实 CUDA Graph、Ray task/actor 与 vLLM capacity 门禁和重复实验。
- 代码计划含六个独立 RED→GREEN 任务；实验计划固定同一 512-request workload，
  每一层只晋级上一层胜出配置，并要求 64-request correctness/MFU 门禁先通过。
- vLLM 容器切换采用停止并重命名 eager 容器的可恢复方式；不会删除原容器，
  不把编译/graph capture 启动成本混入 steady-state E2E。
- 本次仅编写计划，尚未执行生产代码改动或重启 vLLM。

## 2026-07-26 vLLM CUDA Graph 真实对照

- 保留式停止并重命名 eager 容器为
  `ai-operator-vllm-qwen-eager-backup`，使用同一 vLLM 0.25.1 镜像、
  同一只读 Qwen2.5-1.5B 挂载和 0.75 GPU memory utilization 启动
  CUDA Graph 服务；唯一执行变量为移除 `--enforce-eager`。
- 64-request graph gate 通过：64/64 请求 exactly-once，201 字段 formal
  schema、request/output/finish、resource/MFU/energy 和数据库 job 审计完整，
  0 incident。
- 完成 graph 1 warm-up + 3 formal 的 512-request 实验。三轮 formal
  相对 eager 均值：E2E 282.76s → 79.85s（-71.76%），observed tokens/s
  812.23 → 2875.68（+254.05%），request P99 259.82s → 57.63s
  （-77.82%），MFU 4.02% → 14.51%，每千 observed tokens 能耗
  99.95J → 55.52J（-44.46%）。
- 两侧每轮 prompt tokens=63,970、packing/submission=137，且相同 512 个
  doc ID 顺序；实际 generation tokens 相差约 0.57%，因此吞吐按各轮实际
  observed tokens 计算，不声称逐 token 输出完全相同。
- `graph_startup_evidence.json` 保存带 Docker 时间戳的启动证据：Created
  到 application startup complete 为 148.512s，模型加载 23.139s、
  compile 13.51s、graph capture 4s、engine init 72.26s；启动成本不计入
  steady-state E2E。两侧 service 记录均为可机器读取的单对象 JSON。
- Windows Ray shutdown stderr 在 eager formal 3 以及 graph warm-up、
  formal 1、formal 2 中出现非致命 `access violation` 文本，但对应
  profiler exit=0，manifest、vLLM success delta、exactly-once trace 和
  数据库 finished 状态均完整。该问题保留为两侧共有的独立 infra 缺陷，
  不能称任一侧日志完全干净。
- 后续本地 Ray task/actor 与 capacity 调优采用 CUDA Graph 作为部署 baseline；
  该提升属于 vLLM 配置选择，不作为论文上游调度策略贡献。结果与绘图数据见
  `experiments/results/vllm_cuda_graph_512_20260726/`。

## 2026-07-26 实验与机制证据文档收口

- 审计 `experiments/results/` 全部 19 个一级结果目录，确认每个目录均有
  `README.md`，并新增 `EXPERIMENT_EVIDENCE_REGISTRY.md` 作为统一入口。
- 台账逐项映射 fixed/token-budget/length/prefix/BFD/row-cap、K_max、adaptive
  flush、AIMD/EWMA/PID、UCB、多 endpoint routing、联合搜索、CUDA Graph、
  代价估计和多模态预留，明确区分设计、功能测试、真实门禁、GPU 筛选和重复/
  留出证据。
- 明确 UCB 目前只有纯控制器和 SLO reward 测试，未接入 profiler，也没有端到端
  GPU 结果；AIMD/EWMA/PID 同样不能由代码测试推出性能有效。真实多 endpoint/
  多 GPU、公平性和故障迁移仍待后续硬件验证。
- 修复 `output_aware_bfd_512_20260726/README.md` 等历史报告的编码和信息缺口，
  补充统一复现命令、原始证据路径、incident、排除理由和后继结果；根结果索引
  补登记修复前 `output_aware_bfd_gate_20260726/`。
- 本次没有改变任何实验数字或机制结论，因此不改研究计划和论文结论；只收口
  可追溯性、入口和结论边界。

## 2026-07-26 Adaptive admission 真实 GPU 矩阵

- 在现有 CUDA Graph vLLM 0.25.1/Qwen2.5-1.5B、RTX 5070 单 GPU 链路上，
  完成 64 请求 static K8/AIMD/EWMA-AIMD/PID 门禁，以及 512 请求每策略
  1 warm-up + 3 formal 的随机交错矩阵。
- 12 个四策略 formal runs 共 6,144/6,144 请求 completed；prompt tokens
  均为 63,970，vLLM success delta 均为 512，201 列 schema、request/control/
  resource/MFU/energy 证据完整。
- AIMD/EWMA-AIMD/PID 相对 static K8 的 E2E 分别 -32.04%/-31.94%/
  -30.32%，tokens/s 分别 +46.26%/+46.21%/+42.43%；但三个控制器平均
  admission limit 均为 15.78–15.93，暴露缺失 static K16 机制 control。
- 追加随机交错 AIMD vs static K16，各 1 warm-up + 3 formal。AIMD 相对
  static K16：E2E +0.66%、tokens/s -0.69%、request P99 -0.07%、goodput
  -0.66%、energy/1k tokens +0.37%、MFU -0.13%，均不可分辨。
- 当前结论是动态控制器相对 K8 的收益来自更高并发上限，不是反馈控制增量。
  单作业稳态优先简单静态窗口；shared-vLLM 仍以 K8 为 guardrail，后续如继续
  adaptive，只做 foreground/background 保护验证，不在稳态单作业上调 PID。
- 三组 runner 共 28/28 runs、0 incident。多轮 Windows Ray shutdown stderr
  保留非致命 access violation 文本，但两侧均出现，profiler exit=0、manifest、
  exactly-once 请求和数据库完成状态均通过。结果见
  `experiments/results/adaptive_admission_controller_20260726/`。
- 合并前完整测试在项目 Python 3.10 实验环境暴露错误清理路径直接调用
  Python 3.11 `BaseException.add_note()` 的兼容性缺口；增加 3.10 fallback，
  保持原始异常和 `__notes__` 语义。沙箱外完整 `code/tests` 为 311 passed。

## 2026-07-26 Shared-vLLM adaptive admission 与 flush 复验

- 扩展 `run_kmax_interference_experiment.py`：typed AIMD、真实 token IDs、
  request/submission/resource/flush/control trace、token-budget、MFU 参数、
  deterministic per-repeat shuffle 和 fixed/adaptive flush 均可配置。
- 实验后审计发现子 profiler 未继承外层 seed/scenario ID；本轮仍可由唯一
  experiment_id 和显式顺序完整还原。runner 已补齐字段转发，防止后续主 CSV
  继续记录默认 `random_seed=0`、`scenario_id=manual`。
- static K8/static K16/AIMD 在前台 128、后台 512、同一 vLLM endpoint 上各
  完成 3 次正式重复；21/21 进程成功，6144/6144 请求 exactly-once，0 失败。
- static K8 前台 E2E/P99 为 40.214/23.003s；static K16 为
  55.743/38.307s，后台真实 tokens/s 从 2596.6 升到 3603.1，确认共享服务的
  吞吐—前台尾延迟 tradeoff。
- AIMD 三轮共 774 个 control event，只有 12 increase、0 decrease，窗口均值
  15.953；相对 K16 前台 E2E +1.22%、P99 +1.98%、后台 tokens/s -1.45%，
  没有反馈控制增量。
- 追加 queue-adaptive flush 25–50ms 的 K8/AIMD 分支：15/15 进程成功，
  4224/4224 请求 exactly-once。2948 条 flush 决策中 2636 条选择 50ms；
  AIMD 下相对 fixed-50 四项差异均小于 0.3%，K8 下约 1–3% 延迟改善伴随
  约 1% 吞吐损失。由于为连续时间分块，不能声称 adaptive flush 胜出。
- 当前 shared single-endpoint 默认收敛为 static K8 + fixed 50ms；后续扩展
  foreground size、arrival offset 和 job 数量，而不继续稳态 PID/AIMD 调参。
- 结果与绘图 CSV 位于
  `experiments/results/shared_vllm_adaptive_admission_20260726/`。

## 2026-07-26：补充文献驱动优化指南与上游持续补位缺口

- 新增 `experiments/plans/literature_driven_pipeline_optimization_guide.md`，
  统一记录三层 batch 边界、Orca/vLLM continuous batching 与 Ray 上游
  whole-submission barrier 的区别、request-level continuous replenishment、
  SLO-aware EWMA flush、文献机制卡、假设迁移、fatal-flaw audit、候选池和
  晋级/放弃条件。
- 校正完成度口径：当前 25/50ms `QueueAdaptiveFlush` 是已接入真实链路的
  two-level baseline，不是 Clipper/Clockwork/CONCUR 等文献机制的完整复现；
  vLLM 内部已有 continuous batching，不代表 Ray 上游已经按逐请求完成补位。
- 更新 `AGENTS.md`、`README.md`、`PROJECT_OUTLINE.md`、
  `overview/current_direction_and_plan.md`、`code/INFRA_STATUS.md`、
  `experiments/plans/README.md`、`experiment_status_and_gaps.md`、
  `service_scheduling_backpressure.md`、`research/knowledge_hub.md` 和
  `PROJECT_INDEX.md` 的缺口、实验入口和导航。
- 修正两处过时状态：根 README 不再把已经完成的 adaptive/联合实验写成下一步；
  缺口表不再把跨 arrival-rate 和 2048 held-out 写成未完成。
- 本轮仅更新设计与状态文档，没有修改代码或生成新的性能结论。

## 2026-07-27 Shared-vLLM 多任务实验审查与文档补全

- 对 07-26 shared-vLLM typed AIMD + adaptive flush 实验（128 前台 / 512 后台）
  做完整审查，补全此前在 `experiment_status_and_gaps.md` 中遗漏的关键诊断：
  AIMD 三轮 0 次 decrease 的根因是 vLLM `waiting` 始终为 0——请求在 Ray 侧排队
  形成"软拥塞"（请求在 Ray 侧排队但 vLLM waiting=0），而 AIMD 盯的拥塞信号（waiting > 0 / KV usage 高）不反映此状态。
  前台已慢 38.9%，控制器观测不到任何异常。
- 更新 `experiment_status_and_gaps.md`：
  - §1.2 表：Shared-vLLM 07-19 行标注为"已被 07-26 取代"；07-26 行补全
    根因诊断与剩余缺口；
  - §1.2 RC2 当前状态：增加信号盲区诊断；
  - §6.1 P0-1：从旧 07-19 数据重写为"已从负结果推进为信号盲区诊断"，
    补全单作业 + shared-vLLM 两组 07-26 证据与演进判断；
  - §9：补齐单作业 admission 矩阵与 shared-vLLM 实验条目、当前结论与缺口；
  - §10.2：新增信号盲区诊断补充，将 request-level replenishment 优先级从
    "工程改进"提升为"可能解锁动态控制价值的必要前置"。
- 更新 `PROJECT_OUTLINE.md` §当前最重要证据：shared-vLLM 条目从旧 07-19
  数据（2.3×）替换为 07-26 复验数据与诊断。
- 本轮仅修订文档状态与诊断，没有修改代码或生成新性能数据。

## 2026-07-27 算子代价估计用途评审与文档补全

- 评审算子代价估计（283 条 profile、70 配置组、5-seed grouped held-out）
  的当前结论与后续方向，明确两个预期用途：
  1. **数据库优化编排**（主要）：为查询优化器提供 AI 算子代价估计，
     辅助执行计划选择与资源分配。当前 R² 0.776、MAE 11.68s，排序能力
     大概率可支撑编排决策，但尚未显式计算排序指标；
  2. **提交策略辅助**（探索性）：作为 vLLM Prometheus 信号的补充，提供
     pending batch 粗粒度工作量预估，但不能替代 Orca 式持续供给和反馈
     驱动的提交机制。
- 更新 `experiments/results/operator_cost_estimation_20260726/README.md`：
  - §目标：新增两个预期用途的明确定义；
  - 新增"待补充：排序能力分析"节：列出 Spearman、pairwise accuracy、
    Top-K precision 三项排序指标及其对编排/提交策略的具体意义；
  - 新增"后续工作"节：排序指标补充、提交策略集成预研（轻/中/重分档）、
    独立 workload 留出、预测区间。
- 更新 `experiment_status_and_gaps.md` §1.5：从一句话状态扩展为双用途
  定位 + 四个当前缺口（排序能力未评估、提交集成未验证、无外推验证、
  无预测区间）。
- 本轮仅修订文档，没有修改代码。

## 2026-07-27 新增 aimd_hol 控制器与 HOL-age 诊断实验（代码+配置，未运行）

- 实现「诊断优先」方案的两块代码改动，直接回应信号盲区诊断（控制器
  读 vLLM waiting 恒为 0，看不见 Ray 侧排队；credit 按 submission 整体回收）：
  1. **HOL-age 信号**：`AdmissionObservation`/`AdmissionTraceEvent` 新增
     `hol_age_s` 字段（非 service metric，来自 scheduler）；`scheduler.run`
     计算 Ray 侧排头请求年龄 `now - min(submit_epoch_s)`，经
     `ObservationProvider.latest`/`DynamicAdmissionGate.decide`（新增可选
     `hol_age_s` 形参，默认 None，向后兼容）透传到控制器；`aimd_hol` 不依赖
     service metrics，故 profiler 不再强制其 `--model-metrics-url`（sampler
     条件化、service metrics 可缺省），支持切换非 vLLM / 无 `/metrics` 引擎。
  2. **`HolAgeAimdAdmissionController`**（`adaptive_admission.py`）：AIMD
     键控 HOL-age 而非 vLLM waiting，完全不读 service metrics；profiler
     新增 `aimd_hol` 策略与 `--hol-age-congestion-s`/`--hol-age-low-load-s`；
     `run_kmax_interference_experiment.py` 加 `--include-aimd-hol` arm。
- request-level replenishment 走配置（`--ray-batch-rows 1`），未改
  `_collect_one`/`wait_one` 协议，不触发 `latency_granularity=submission`
  等契约改动；保持引擎无关纯函数层不变。
- 测试：新增 HolAgeAimd 行为、HOL-age 透传、aimd_hol arm 用例；同步更新
  `test_postgres_profile_scheduling` 的 `_build_adaptive_config` 调用与
  `test_kmax_interference_script` 的 arm；可运行测试全绿（6 个模块因
  pyarrow/psycopg 缺失在此 shell 无法 import，属环境限制非回归）。
- 新增 `experiments/results/hol_age_diagnostic_512_20260727/`
  （scenario_config.json + README）：6 arm × 3 formal，预注册判据为
  "新 arm 相对同上限 static K=16 在 SLO-goodput 提升 >5% 且超 95% 区间，
  同时 SLO violation < 1%"；满足则转正面方法贡献，否则落回刻画型 framing。
- **本轮修改代码与配置，未生成新性能数据；实验未运行**（本机此 shell
  无 vLLM/GPU/PG 环境）。运行与结论回写（PROJECT_OUTLINE §证据、
  experiment_status_and_gaps、本日志）待 GPU 环境就绪后进行。

## 2026-07-27 修正 AutoDL 部署指南（PG18.4 + lsb_release + uv/venv）

- 在 2× RTX 4090（sm89，Driver 595.58.03 / CUDA 13.2）实例上实测后，纠正 `deploy/autodl/README.md` 几处：
  1. **PG 版本 16 → 18.4**（§6/§11）：与本机 baseline 18.4 Docker 对齐；原 PG16 是保守默认。
  2. **`$(lsb_release -cs)` → 硬编码 `jammy`**（§6/§10）：最小镜像 lsb_release 未装/不在 PATH 时该变量为空，repo 行变成 `apt -pgdg` 报 "does not have a Release file"（实测踩到）。
  3. **新增 §4.3.1 uv + 独立 venv**：4090 实测 `uv pip install vllm==0.25.1` 约 5 分钟（plain pip 30+ 分钟）；vllm 装独立 venv（`/root/autodl-tmp/venvs/vllm-4090`）与 driver 的 base 隔离，避免 vllm 的 torch 2.11.0 覆盖镜像 base 的 torch 2.12.1+cu130；缓存放数据盘。
- 验证：4090 上 `vllm 0.25.1 / torch 2.11.0+cu130 / flashinfer 0.6.13 / capability (8,9) / 2 GPU` 全部正常（sm89 不触发 §12 的 Blackwell sm120 flashinfer bug）。
- 本轮仅修订部署文档，未改代码、未生成性能数据。

## 2026-07-28 Token-budget 容量曲线与多 job 共享调度实现

- 新增 `deploy/autodl/dual_gpu_token_budget_curve.example.json`，在关闭
  arrival replay/flush 混淆的条件下正式扫描
  `{1024,2048,4096,8192,16384,32768}`；预注册“小预算固定开销高、大预算
  completion barrier/HOL 加重”的非单调假设，不再把 8192 当先验最优值。
- 数据组织模板改为读取 `BEST_TOKEN_BUDGET`，容量曲线标定预算后才比较
  sequential、row-cap-aware 和 length-align，分离预算大小与 membership
  算法两个因素。
- 明确动态 token budget 只能在静态容量曲线标定的安全动作集中，依据 pending
  work、arrival/service-rate EWMA 和 oldest slack 调整；不得把上游组织预算、
  active-work admission 与 vLLM 内部 `max_num_batched_tokens` 混为一谈。
- 将多 job 从远期附加场景提升为正式调度问题：job-local K 的总和不能保护共享
  endpoint；候选架构为 shared endpoint-local request/work credit +
  deficit/weighted fair queue，并以 solo-normalized slowdown、饥饿、P99、
  SLO goodput 和 normalized-work fairness 评估。
- 修正 shared-vLLM 实验脚本的双 GPU 默认语义：static admission 默认改为
  per-endpoint、routing 默认 `least_queued`；对仍是 global-only 的 adaptive
  策略显式拒绝 per-endpoint 配置，防止再次产生不可比结果。
- 实现 static/service-quantum token-budget 控制器：动态策略只从静态曲线
  标定的安全候选中选择，按 arrival/service-rate 量子逐批至多移动一档，
  metrics 缺失时 hold；flush trace 记录实际 budget、理由和反馈速率。
- 实现 per-endpoint active-work admission 和 least-work routing，按
  `prompt + estimated output` 预测工作量记账，避免长短请求或不同 batch
  都消耗一个等价 K credit。
- 实现多 job shared endpoint credit：Ray named actor 持有 request/work
  容量，纯策略层使用带权 deficit round robin 和 work-conserving borrowing；
  配额键使用 `(job_id, request_id)`，避免不同作业重复 batch ID 串扰。
- 将 scheduling 代码按 `organization/`、`submission_control/`、
  `endpoint_routing/`、`runtime/` 分包；旧根级模块保留薄兼容导入，避免部署
  脚本和已有测试发生一次性迁移。
- 将平铺的 `profile_*.py` 实现收拢到 `code/src/profiling/`，按
  CLI/config、schema/traces、replay、Ray runtime 分文件；根级同名模块只保留
  兼容导入，使通用 source/organizer/backend/sink 与画像应用边界分离。
- 新增 `dual_gpu_active_work_curve.example.json` 和
  `dual_gpu_submission_policy.example.json`，要求先标定静态预算和 active
  work，再逐项消融动态预算、least-work 与 adaptive flush。
- 当前只有本地代码/契约测试，尚未生成新的 GPU 性能结论；合并 main 前仍需
  远端独立 worktree 全量测试和 Ray smoke。

## 2026-07-28 双 GPU 实验因果口径复审

- Phase 1 的 `K=4` 实际约束的是每 endpoint 的 batch 数，而不是 request 数或
  token work。随 token budget 增大，平均每 batch 行数约从 2.3 增至 64，
  可供给的 request envelope 约从每 endpoint 9 增至 256；vLLM mean running
  requests 同时约从 15.5 增至 310.7。因此当前吞吐上升主要证明“增加 offered
  load 可继续填充服务”，不能单独归因于 token-budget 数据组织更优。
- 用户确认重排正式实验：先以 request-level submission 标定 per-endpoint
  active-work 容量，再在固定 active work 下扫描 token budget，随后在固定预算和
  work 下比较 membership；之后才比较 replenishment、动态 workload 和多 job
  公平性。
- 已完成的 token-budget 与数据组织结果保留为诊断证据。前者存在 offered-load
  混淆，后者同时改变 batch count 与 endpoint 4/4、4/5 奇偶分配，均不提升为
  单因素因果结论。
- 设计仍不修改 vLLM/Ray 内部调度器；优化价值转向达到饱和所需的最小上游 work、
  SLO/突发条件下的有界排队，以及共享服务下的公平隔离，而非在单 job 稳态下追求
  无上限增大 batch。

## 2026-07-28 双 GPU 实验正确性实现

- 正式 service metadata 现在拒绝 `unknown`/非正整数容量、空版本/编译模式和
  非布尔执行开关；scenario loader 支持从环境展开 metadata 并将完整引用恢复为
  JSON 标量，确保校验的是实际数值而不是占位字符串。
- profiler 新增 `organization_batch_count`、行数/成本分布和 row-cap hit ratio，
  与 Ray 实际 `batch_rows_*` 分开；request-level submission 不再把预展开组织
  形状覆盖为单行提交形状。
- shared credit 启用时强制要求 `--ray-address` 或 `RAY_ADDRESS`；多 job runner
  将同一地址转发给所有 profiler 子进程，防止 named actor 分裂到隐式本地集群。
- AutoDL 模板改为 active-work-first：request-level active-work curve 先标定
  offered work；8192–65536 token-budget 与 membership 模板随后固定
  `ACTIVE_WORK_PER_ENDPOINT`。所有六个模板均读取具体 vLLM capacity metadata，
  正式 SLO 改由运行环境显式提供。
- 本地 TDD 覆盖了元数据拒绝、组织/提交双层指标、共享 Ray 地址门禁和模板展开。
  完整测试 374 项通过，6 个 AutoDL JSON 均可解析且无 `unknown`，`compileall`
  与 `git diff --check` 通过。本地环境未安装 Ruff，留给远端独立 worktree
  补跑；尚未启动任何正式 GPU 矩阵。
- 分支提交 `5961457774c0419019695ed5a89eb63550eb9823` 已推送，并在远端
  `/root/autodl-tmp/worktrees/dual-gpu-correctness-5961457` detached worktree
  复验：完整 374 tests 通过，主 checkout 的未跟踪实验结果未修改。
- 远端显式地址 smoke 使用独立 Ray head `172.17.0.4:6399` 和三个独立 Python
  进程：job-a/job-b 成功获取两份 named credit，job-c 在共享容量已满时被拒绝，
  证明跨进程复用同一 actor。测试集群已停止，两套 vLLM 服务 PID 350094/350096
  保持运行。
- 本地和远端环境均未预装 Ruff；远端临时安装因包镜像缺失、官方 PyPI 超时未
  完成，残留下载进程已清理。该环境限制不记作 Ruff 通过；以 374 tests、
  `compileall`、JSON 解析和 `git diff --check` 作为本次已完成门禁。

## 2026-07-29 Shared-vLLM 1/2/4-job 实验预注册

- SLO-aware EWMA 正式矩阵已完成并封板，25–50ms alpha/deadband 不再继续调参；
  下一安全方向切换为 endpoint-shared request/work credit 与 work-conserving
  fairness。
- 复审旧 interference runner 后确认其不能直接承担正式矩阵：只支持两个前后台
  job、每个并发 profiler 都携带 `--setup`、全局 vLLM token delta 会重叠，
  且缺少 coordinator 精确峰值与按 job 服务量证据。
- 在 `experiments/plans/service_scheduling_backpressure.md` §13 预注册
  `independent_full`、`static_partition`、`shared_drr` 三臂的 1/2/4-job
  矩阵、fatal-flaw audit、双 GPU gate、exactly-once/容量/公平性硬门槛和
  5% 晋升条件。
- 新实施计划
  `code_doc/superpowers/plans/2026-07-29-shared-vllm-fairness-implementation.md`
  要求测试先行补齐精确 shared-credit 观测、同步 replay 起点、正式 group
  runner、组级指标与 AutoDL gate/formal 模板；gate 未通过禁止 formal。
- 已按独立代码审阅补齐正式 runner 的恢复与证据边界：每组先生成 durable
  record，再由 manifest 原子确认并重建 `group_runs.csv`；无 record 的残留
  artifact 会安全拒绝恢复，避免覆盖日志或重复追加 CSV。resume 同时强制匹配
  repository commit。
- replay 现在使用 runner 配置的共同 epoch 作为生命周期原点，并硬校验每 job
  启动迟到与跨 job skew；非零 worker failure、不可用的组级 vLLM/resource/MFU、
  endpoint 未覆盖、共享 credit 越界或结束未归零都会使整组失败并保留
  `failure.json`、日志和 trace。
- coordinator 名称包含由物理 output path 派生并持久化的 run-instance ID；
  同一目录 resume 保持确定性，新目录不会复用失败 gate 留下的 detached actor。
- 本地 141 项直接相关测试通过；完整 424 项中 414 项通过，剩余 10 项仅因当前
  本地 Python 缺少 Ray/Daft/psycopg。`compileall` 与 `git diff --check`
  通过；依赖完整的全量测试留待远端同步后、GPU gate 前执行。
- 当前仍没有新的 Shared-vLLM GPU 性能结果；真实双 GPU gate 尚未启动，不能
  声称 shared DRR 的公平性、MFU 或性能收益。
- 用户明确要求本轮不再同步 Wiki；项目文档仍作为唯一事实来源维护。

## 2026-07-29 Shared-vLLM 远端门禁首次启动失败与路径修复

- AutoDL 主 checkout 在确认无 runner、无租约、双 vLLM endpoint 空闲且 tracked
  worktree 干净后，快进到 `c39f569782b86713ac01a5b079ff8900e11ed674`。
  与 incoming commit 冲突的两份未跟踪 SLO-EWMA compact 结果经哈希核对：
  `manifest.json` 完全相同，`runs.csv` 仅 CRLF/LF 不同；原始字节副本保存在
  `/root/autodl-tmp/result-backups/premerge_c39f569_dual_gpu_slo_ewma_flush_formal_20260729/`，
  其余 929 个未跟踪产物未修改。
- 远端依赖完整环境执行 `code/tests`：433 项全部通过；`compileall` 和 gate/formal
  JSON 解析通过。随后按固定地址 `127.0.0.1:6380` 启动唯一 Ray head，资源为
  32 CPU / 2 GPU，无 pending demand 或 node failure。
- 首次 gate 输出
  `experiments/results/dual_gpu_shared_vllm_gate_20260729_1047/` 在第一组两个
  profiler 子进程均以退出码 2 失败；目录、manifest、commands、failure
  evidence 和 stderr 全部保留，未复用或删除。根因不是策略或 GPU：runner
  固定用 `code/` 作为 child cwd，但 CLI 保留相对
  `code/scripts/profiling/postgres_ai_operator_profile.py`，导致 child 尝试打开
  `code/code/scripts/profiling/postgres_ai_operator_profile.py`。
- 测试先行新增 CLI 路径回归用例，确认修复前失败；随后让 shared-vLLM CLI
  在切换 child cwd 前把 config、profiler、Python 和 output 路径解析为绝对
  路径。相关 142 项测试全部通过。修复提交同步后必须使用全新 gate 输出目录；
  旧失败目录不得恢复为正式结果。
- 路径修复提交 `96a24a85612b872a4a2cc5e6a53d442d17c21425` 同步后，
  远端完整测试增至 434 项并全部通过。第二个全新 gate
  `experiments/results/dual_gpu_shared_vllm_gate_20260729_1056/` 已进入真实
  双 GPU 执行：两 job 子进程退出码均为 0，每 job 64/64 request trace 均为
  profiler schema 的 `status=completed`、空 `error_type`，GPU0/GPU1 均达到
  100% utilization。
- 第二次 gate 仍被 runner 错误标为失败：新 validator 把 request trace 成功
  状态写成了 `ok`，而 `ok` 只属于 runs summary；request trace 的正式成功值是
  `completed`。该失败目录及 128 条完整 trace 保留，不作为正式 gate 结果。
  测试先行新增 schema 契约用例，集中用
  `_request_trace_succeeded` 严格接受 `completed + empty error_type`，并继续
  拒绝 `ok` 或带错误类型的行；相关 143 项测试全部通过。修复发布后仍必须使用
  第三个全新 gate 目录，不得复用两次失败现场。
- 状态契约修复提交 `983e6e13f4a0420b9b2a35fb448f4df63a65c978`
  同步后，远端完整测试增至 435 项并全部通过。第三个全新 gate
  `experiments/results/dual_gpu_shared_vllm_gate_20260729_1103/` 的
  `independent_full` 已完成并形成 durable record；随后 `shared_drr` 两 job
  均成功执行，但 job0 首提交比统一 replay epoch 晚 3.900816s，超过预注册 2s
  门槛，因此整组按设计失败并停止，未进入 formal。
- trace 排除随机不同步：两 job 的 barrier observed epoch 与 configured epoch
  误差均小于 0.2ms，实际首提交彼此仅差 11.8ms；3.9s 延迟只出现在
  `shared_drr`。共享 credit 最终 active/waiting request/work 全部归零，每
  endpoint 两 job 各获 32 次 grant，request 峰值 47、work 峰值不超过
  22191，均未触及 256/65536 上限。这些是功能诊断证据，不构成性能收益结论。
- 根因是 shared-credit Ray actor/client 在 replay barrier 之后由 profiler
  首次懒创建。没有放宽门槛；测试先行新增控制面预热契约，并让 group runner
  在计算未来 replay epoch 之前创建、核对配置并 snapshot 所有 endpoint，
  profiler child 只复用已存在 actor。相关 144 项测试全部通过。发布后必须用
  第四个全新 gate 目录验证首提交迟到是否消失。
- 预热修复提交 `e45fe1c869e45f230eec878f42e305eaa5e249bf` 同步后，
  远端完整测试增至 436 项并全部通过。第四个全新 gate
  `experiments/results/dual_gpu_shared_vllm_gate_20260729_1112/` 再次完成
  `independent_full`，但 runner 预创建 `_FairCreditActor` 时 Ray worker 报
  `ModuleNotFoundError: No module named 'src'`，因此在 shared arm 执行前失败。
  原因是 profiler 连接 Ray 时会把 `code/` 注入 worker `PYTHONPATH`，而新加的
  group-runner observer 只传了 address，没有复用该 runtime environment。
- 保持 barrier 前预热设计不变；测试先行新增 Ray init 契约，observer 现在把
  `_CODE_ROOT` 与已有 `PYTHONPATH` 合并后通过 `runtime_env.env_vars` 传给 worker。
  相关 145 项测试全部通过。发布后仍使用第五个全新目录验证，不复用已有失败
  actor 或输出。
- worker 路径修复提交 `e322183c1c2166fd6c603d9221feb6e848d7d338`
  同步后，远端完整测试增至 437 项并全部通过。第五个 gate
  `experiments/results/dual_gpu_shared_vllm_gate_20260729_1119/` 首次达到
  manifest 3/3 completed、0 incident：6 份 job trace 均为 64/64 exactly-once，
  0 worker failure，两 endpoint 均有请求；最大首提交迟到 0.144058s、最大
  跨 job skew 0.027164s；shared credit 最终全部归零，两个 endpoint 的
  request/work 峰值分别为 46/20976 与 45/21210；runner、lease 和 named actor
  均清理，端点健康空闲。
- 独立审计同时发现 gate 汇总统计无效，故仍不放行 formal：raw resource trace
  在执行窗口内的 GPU utilization mean 分别为 73.01%/74.25%/71.16%，max
  均为 100%，但 `group_runs.csv` 的 P95 均错误为 0。根因是公共
  `percentile()` 接受 0–100 参数，新 runner 的 `_distribution_fields` 和
  per-job latency 分别误传 `0.95` 与 `0.99`，因此 GPU P95 与 job P99 都退化
  到最小样本附近。
- 测试先行新增 GPU P95 与 job P99 nearest-rank 契约，修正调用为 95/99；
  相关 146 项测试全部通过。第五个目录只证明功能链路通过，不能作为统计有效的
  正式 gate；发布后必须用第六个全新目录重新生成完整汇总。

## 2026-07-29 Baseline 口径重构：数据库 AI 算子 + 官方 Runtime

- 根据对“力大砖飞”、单 job 瞬态饱和和现有系统对照不足的复核，暂停继续
  增加 Ray 内部策略。下一优先级改为先建立同规模同条件强 baseline。
- 核心产品 baseline 定义为无 Daft/Ray 的现有数据库 AI 算子，首选
  OceanBase `AI_COMPLETE`。官方文档确认它可以注册 OpenAI-compatible Chat
  Completions endpoint；正式 arm 仍需通过 Community Edition 版本、同机
  vLLM 直连、原生并行和可观测性门禁。
- 为避免 OceanBase/PostgreSQL 数据库差异污染归因，增加同 PostgreSQL
  bounded AsyncIO 强因果 baseline；该 arm 必须独立标定，不能使用串行
  strawman。
- 为避免遗漏“官方框架已经足够”的解释，保留第二层必测 baseline：
  Daft `prompt()` Native Runner、Daft `prompt()` Ray Runner 和 Ray Data
  HTTP Processor。LOTUS `sem_map` 仅在 cache/prompt/token 语义门禁通过后
  作为第二阶段扩展。
- vLLM Bench 只作为 serving ceiling。两层正式对照均统一重跑
  `/v1/chat/completions`；旧 `/v1/completions` 结果只保留为历史机制证据，
  禁止直接横比。
- 新增
  `experiments/plans/archive/database_ai_operator_baseline_matrix_20260729.md`（后于
  2026-08-03 归档）和
  `code_doc/superpowers/plans/2026-07-29-same-condition-official-baselines-design.md`，
  预注册固定 manifest、双 endpoint 等价性、独立 calibration、32–256
  瞬态与 2,048 held-out、time-to-ceiling/ramp-regret/minimum-saturating-work
  指标，以及 5%/2-of-3 晋级门槛。
- 同步更新 `PROJECT_OUTLINE.md`、`overview/current_direction_and_plan.md`、
  `experiments/README.md`、`experiments/plans/README.md`、
  `experiments/plans/baseline_reference.md`、
  `experiments/plans/experiment_status_and_gaps.md`、`code/INFRA_STATUS.md`、
  `research/existing_ai_operator_execution_chains.md` 与 `PROJECT_INDEX.md`。
  按用户要求不执行 Wiki 同步。

## 2026-07-29 同条件 baseline 执行基础设施

- 实现统一 Chat Completions 请求协议，同时保留旧 Completions 默认路径供历史
  profiler 使用；新 baseline 全部固定一行一个完整请求、`temperature=0`。
- 新增不可变 manifest、canonical hash、largest-work-first 双 endpoint 固定
  分片、共同 request/result schema、exactly-once 与吞吐/延迟汇总。
- 新增 vLLM Bench、bounded AsyncIO、Daft `prompt()` Native/Ray、Ray Data
  HTTP Processor 和 OceanBase `AI_COMPLETE` 适配器。OceanBase 仍为可选产品
  capability gate，不替代无 Daft/Ray 的 bounded HTTP 因果对照。
- 新增 `run_official_baseline.py` 薄 CLI、64 行 gate 与 calibration 规格；
  gate 对 endpoint work skew、请求完整性、服务元数据、worker failure 和最终
  vLLM 队列 fail closed。交叉审计修正了一个会把 8000/8001 地址差异误判成
  服务配置不一致的指纹问题，并增加回归测试。
- 本地代码/契约完成不等于远端性能 baseline 已建立。下一步仅允许在全新目录
  运行一次真实双 GPU gate；通过后停止分析，不能自动进入 calibration/formal。
- 首次远端依赖门禁发现 Ray 2.56.1 的 `ray.data.llm` 间接导入 Ray Serve；
  仅声明 pandas/aiohttp 会在干净 base 环境报缺少 `starlette`。修正项目依赖
  为 `ray[data,serve]`，并增加 requirements 契约测试；远端必须按现有 Ray
  相同版本补官方 extras，不能以手工安装单个传递依赖掩盖契约缺口。
- 按用户要求把 official baseline 的部署状态机、base/vLLM 环境职责、core
  adapter 顺序、结果停止条件与已遇事故集中写入 `deploy/autodl/README.md`。
  已记录远端未跟踪结果阻止快进、CRLF 规范化 hash、安全仓库外备份、
  Ray Serve extra、vLLM Bench 0.25.1 详细字段、失败证据先落盘和 endpoint
  指纹语义，供新对话直接复用。
- manifest 准备审计发现 Daft Ray 已接受显式 Ray address，而 Ray Data HTTP
  adapter 未调用 `ray.init(address=...)`，新进程可能隐式创建另一 cluster。
  测试先行增加 runtime 与 CLI 契约；Ray Data 现在显式连接同一 address，
  Daft Ray/Ray Data 缺少 `--ray-address` 时连 dry-run 也 fail closed，双 GPU
  gate/calibration 模板冻结为现有 `127.0.0.1:6380`。
- 远端正式 gate 前继续审计发现薄 CLI 只有 JSONL 再分片入口，没有从正式
  PostgreSQL workload 生成 JSONL 的可复现路径。新增独立
  `postgres_manifest` 模块与 `export-postgres-manifest`：只读
  `documents`，按 workload + `ORDER BY doc_id` + limit/offset 选择完整行，
  固定 output cap/estimated-output 模式，计算 source-row hash 后再使用共同
  largest-work-first endpoint 分片；拒绝短结果与重复 doc_id。
- 64 行真实双 GPU core gate 在全新目录连续保留失败证据并逐项修复：
  vLLM Bench 的 console entry、显式本地 tokenizer、`vllm[bench]` extra、
  0.25.1 timing schema 和 bounded HTTP 全局 endpoint offset 均已用回归测试
  锁定。`dual_gpu_official_baseline_core_gate_20260729_1730` 中 vLLM Bench、
  bounded HTTP、Daft Native 和 Daft Ray 已分别通过 exactly-once、双 endpoint
  work skew 与空队列门禁；Ray Data 单元因 worker 无法导入项目 `src` 失败。
- Ray Data 失败日志证明两个 driver 均已连接同一个 6380 Ray cluster；所谓
  `0 CPU/pending` 是 actor 构造失败后的伴随告警，根因是 driver 的临时
  `sys.path` 不会传播到 Ray worker。测试先行要求
  `ray.init(runtime_env={"env_vars": {"PYTHONPATH": ...}})` 显式注入仓库
  `code/` 根目录，同时保留既有 `PYTHONPATH`。修复后只能使用全新目录重跑
  最小 gate，不能覆盖 `_1730` 或直接进入 calibration。
- 提交 `5708e85` 在全新目录
  `dual_gpu_official_baseline_core_gate_20260729_1725_fix5708e85` 完成首轮
  5/5 core gate：每项 64/64 exactly-once、0 incident、双 endpoint、
  work skew 0.0085%，最终 vLLM running/waiting 均为 0。
- gate 后等价性审计没有把功能通过误写成性能 baseline。vLLM 0.25.1
  `CustomDataset.sample()` 默认先套 chat template，而 openai-chat 请求又由
  服务端套一次；首行 input token 为 92 vs bounded 的 63。测试先行增加
  `--skip-chat-template`，要求 re-gate 逐行核对 input token。
- Ray 2.56 的整数 concurrency 在该 Processor 中可解释为 `1..n` autoscaling，
  首轮 Ray Data 日志实际只有 1 actor；包装器改为 `(n,n)` 固定池，避免小作业
  underscale。官方 HTTP UDF 的 batch 内执行语义保持不改，后续仍独立扫描
  batch size × actor 数。
- Daft 只返回文本且没有逐请求 usage/timing，Ray Data 包装器当前也只能观察
  shard barrier。共同 summary 新增 `timing_granularity` 与
  `token_accounting`，禁止把 barrier P95、manifest prompt token 当作可与
  request-level/server-usage 直接比较的指标。全新等价性 re-gate 通过前，
  calibration/formal 继续阻塞。
- 按用户要求不执行 Wiki 同步。

## 2026-07-29 Official baseline 256 行 scale gate 与校准入口

- 256 行双 GPU scale gate 完成 5/5、0 incident、256/256 exactly-once、最终
  队列归零。vLLM Bench C32 与 bounded HTTP C32 的 total tokens/s 分别为
  4930/4926，JCT 均为 20.37s；Daft Native official default 单次为
  9818 total tokens/s、10.20s。该结果只证明 C32 直接客户端可能未饱和，
  不构成 Daft 加速 vLLM 或统计性能排名。
- 审计确认旧 gate runner 只能固定运行五个 core arm 与 C32。为避免远端临时
  JSON/手拼 shard，测试先行新增重复 `--include-cell` 和
  `--concurrency-override id=N`；未知、重复、非正与覆盖未选 cell 均在请求前
  fail closed，最终选择写入 `resolved_config.json`。
- 下一步只在同一 256 行 manifest 上运行 vLLM Bench/bounded HTTP C64，门禁
  通过后再运行 C128；每档使用全新输出目录，不重复运行 Daft/Ray Data。以
  total/generation tokens/s、JCT、服务端 counter、空队列与 3% 饱和阈值决定
  最小安全并发。
- 按用户要求不执行 Wiki 同步。
## 2026-07-29 Direct baseline C64/C128 与 8K ceiling 纠偏

- C64 vLLM Bench/bounded 均通过 256/256 exactly-once、0 incident、服务端
  counter 与空队列门禁，total tokens/s 分别为 8,342/8,333，JCT 均约
  12.02s；相对 C32 约提升 69%，确认 C32 欠载。
- vLLM Bench C128 日志确认 peak concurrency=128，达到 12,762 total
  tokens/s、JCT 7.849s，相对 C64 再提升 53%。这直接否定“约 8.0–8.2K 是
  双 4090/vLLM 物理极限”的旧解释；8K 仅是历史 project
  profiler/arrival-replay/请求语义的平台，不能跨协议外推。
- bounded C128 虽完整性门禁通过，但仅 8,711 total tokens/s。fatal-flaw
  audit 定位为 httpx 0.28.1 默认 `max_connections=100`、keepalive=20，
  配置 C128 被隐式截断。测试先行把连接池总容量显式设为
  `concurrency_per_endpoint × endpoint_count`。全新 bounded-only C128
  re-gate 实测 running=124/125、12,472 total tokens/s、JCT 8.048s；相对
  旧污染点吞吐 +43.2%、JCT -30.1%，与有效 vLLM C128 只差约 2.3%。
- 现有 256 行 manifest 每 endpoint 只有 128 行，不能有效运行 C256。下一
  ceiling 点至少使用 512 行；同时优先让 project profiler 在同 manifest、
  Chat Completions、no replay 条件下运行。未完成该同条件对照前，不新增上游
  策略，也不据 direct gate 宣称 ours 更慢。
- 按用户要求不执行 Wiki 同步。

## 2026-07-29 同条件 project runtime 对比实施顺序冻结

- 用户确认先完成单 Job 同条件对比，再独立进入多 Job：使用互不重叠的
  512 行 calibration 与 2,048 行 held-out Chat manifest，所有可比 arm 关闭
  arrival replay，并保持一行一次 Chat Completions 请求。
- 新实施计划把缺口拆成 manifest 锁定、离线 request-level 补位、固定 endpoint
  路由、项目 static/token-work 校准和正式矩阵；direct/官方 baseline 分别独立
  校准，OceanBase 仅在 CE 能力与语义门禁通过时进入数值对照。
- 结论门槛同时覆盖吞吐/JCT 加速与压力效率：未达到 5%/2-of-3 门槛时，不把
  相同吞吐下的更低 active work、P99 或更快爬坡写成 GPU 推理加速。
- 开题 PPT 由另一对话并行修改，本轮隔离 worktree 不接触或暂存其文件。
- 按用户要求不执行 Wiki 同步。

## 2026-07-29 512 行 direct hard ceiling 与 project 同条件执行护栏

- 冻结的 512 行 Chat manifest SHA-256 为
  `7205f7ec2b9d52d8f0a4546a044cbbdaff644c0f88d06e9fc11a9a0c86077ced`，
  两 endpoint 各 256 行、预测 work 73,329/73,328。vLLM Bench C256 与
  bounded C256 分别达到 15,351/14,532 total tokens/s，JCT 11.931/12.569s；
  C128→C256 仍增长 24.3%/33.0%，所以 C256 只称当前 `max_num_seqs`
  配置硬上限，不称经验平台。
- project profiler 增加离线 request-level continuous replenishment、
  immutable manifest 行语义校验、manifest-pinned endpoint routing 和正式
  CSV 证据。公平契约强制 raw Chat Completions、`temperature=0`、
  trace-target output work、no arrival replay；错误行、token 估计、路由或
  payload 配置均 fail closed。
- 新增 project 512 校准和 2,048 formal AutoDL 模板。校准扫描 static K
  32/64/128/256 与 active work 16K/32K/49K/65K/98K；formal 只接受校准后
  冻结的最小 97%-ceiling 参数。
- 远端只读预检确认持久 Ray head `127.0.0.1:6380`、双 endpoint、GPU 和
  runner 均空闲；旧 runtime env 缺 Chat URL，因此新环境模板显式区分
  `/v1/completions` 与 `/v1/chat/completions`。
- 数据门禁发现 `sharegpt_burstgpt` 只有 `doc_id=0..2047`。校准占用
  `0..511` 后只能得到 1,536 个 disjoint 行，不能运行 2,048 formal。
  profiler/CSV 增加 `source_row_offset`，formal 固定 offset 512；正式执行
  前必须新增 512 个独立行或导入独立 held-out workload，禁止复制或回用
  calibration 行。
- 开题 PPT 由另一对话并行修改，本轮隔离 worktree 未接触或暂存其文件。
- 按用户要求不执行 Wiki 同步。

## 2026-07-29 Project 同条件门禁：输出 work 口径修复与 held-out 安全补数

- 首次 64 行 project gate 在任何 HTTP 请求前 fail closed；远端保留目录
  `dual_gpu_same_condition_project_gate64_20260729_33c278b`。`doc_id=2`
  的数据库 trace target 为 276，官方 manifest 按请求 cap 记录 256，
  `source_row_hash` 一致，排除数据漂移。512 calibration 未启动。
- 系统化调试确认 project `trace_target_output` 既直接校验 raw target，也把
  未裁剪值计入 active work，与 official manifest 的有效请求 work 不一致。
  测试先行统一为 `min(trace target, completion_max_tokens)`；manifest guard
  同时用 workload、arrival、prompt、token 字段和 raw target 重算
  `source_row_hash`，因此 raw target 即使同在 cap 之上发生变化也会拒绝；
  没有跳过或放宽源行身份校验。
- importer 新增按过滤后 eligible rows 计数的 `--source-row-offset`、既有
  prefix 逐字段核验、显式 `--max-prompt-tokens` 和 `--append-only`。远端
  审计确认两份 raw hash、2,048 行文本/session/tokenizer 全部一致且原始数据
  足够，但历史 shell 命令缺失；因此不声称恢复 exact CLI，而以当前正式
  prompt 上限 1,500 重建 0..2047 并逐字段核验。不一致即在写入前停止，
  doc ID 冲突由数据库事务失败，禁止 upsert 覆盖旧行。
- 本地完整 suite 通过 508 tests；ruff、compileall 与 `git diff --check`
  通过。下一步只允许在新提交与全新远端目录重跑 64 行 gate，通过后再启动
  512 行 K/active-work 校准。
- 开题 PPT 由另一对话并行修改，本轮未触碰；按用户要求不执行 Wiki 同步。

## 2026-07-29 Project active-work 校准：健康状态与容量背压分离

- 修复输出 work 口径后，全新 64 行门禁
  `dual_gpu_same_condition_project_gate64_20260729_beeee20` 通过：64/64
  exactly-once、endpoint 32/32、0 incident、0 worker failure，manifest
  64/64 校验通过，服务端 prompt/generation/success counter 为
  12058/13554/64，最终双 endpoint 队列均为 0。
- 随后 512 行校准首个交错场景 `work16384` 在第 89 个请求、任何该请求 HTTP
  提交前失败。endpoint-0 已有 44 个请求、16,161 active work；固定到该端点
  的新请求 work=234，加入后会到 16,395，超过 16,384。endpoint-1 尚有容量，
  但旧调度器把“当前请求在 endpoint-0 暂时无 credit”覆盖写成
  `healthy=false`，因此 pinned router 误报服务不健康。外部 `/health` 始终
  正常，失败后队列归零；失败目录
  `dual_gpu_same_condition_project_calibration_20260729_beeee20`、stderr、
  manifest 与 incident 原样保留，未重试且无 `runs.csv`。
- 测试先行把 `EndpointSnapshot.healthy` 与 request-specific `available`
  分离；固定 endpoint 同时固定其所属 pool，容量不足抛可重试的 typed
  backpressure，调度器先收集完成再重试，绝不改投另一 endpoint。公开
  `healthy_endpoints()` 保持纯健康语义，新增 `schedulable_endpoints()`；
  fixed-pool、multi-pool pinned、oversized shared-credit fail-fast 均有回归。
- 最终本地全量 512 tests、相关 170 tests、ruff、compileall 和
  `git diff --check` 通过，独立代码审阅无 Critical/Important。下一步只能在
  新提交和全新远端目录重跑 64 行门禁；通过后才重新开始 512 行校准。
- 开题 PPT 仍由另一对话修改，本轮未触碰；按用户要求不执行 Wiki 同步。

## 2026-07-29 Baseline 优势验证：首次高并发等价性门禁

- `0c370ce` 的全新 64 行 gate 通过 64/64 exactly-once、endpoint 32/32、
  0 incident/failure 和最终空队列。随后 512 行 9-cell calibration 完成
  9/9，但 static K256 与理论 nonbinding W98K 分别为 11,736/4,153 total
  tokens/s，单次结果不能用于参数选择。
- 只读系统化诊断排除 active-work credit、actor 数、manifest/payload、
  output work 和汇总计算。W98K 的额外约 28.6s 位于 HTTP/vLLM request
  wall；actor ramp 只多约 3s。W98K 恰为首个 full-concurrency cell，并出现
  endpoint 不对称的逐波接纳，当前保留为客户端/OS/vLLM ingress 冷路径假说。
- 用户批准预注册门槛：单 job ours 吞吐至少为 bounded HTTP 95%；在至少
  97% ceiling 下压力降低 20%才称压力效率；transient time-to-ceiling/ramp
  改善 20%；多 job 聚合吞吐至少 95%，并使 P99/SLO/fairness 至少改善 10%
  且无饥饿。未通过则记录无可证明优势。
- 新增 staged validation 规格与实施计划、K256/W98K 等价性模板；actor-ready
  barrier 移到 measured E2E 前并单列 `actor_ready_s`，非流式 HTTP 与
  submission trace 增加 request/headers/body timing。每臂 1 same-pressure
  warm-up + 3 formal repeats；5% 等价门禁未通过禁止 broad calibration。
- 所有主 baseline 继续使用同一双单-GPU vLLM endpoint。OceanBase-style
  lightweight arm 仅作明确标注的次级模拟，不冒充官方产品；pgai 保持
  embedding 对照。
- 开题 PPT 由另一对话并行修改，本轮未触碰；按用户要求不执行 Wiki 同步。

## 2026-07-30 双 GPU 校准合同与远端结果目录规整

- 远端 f203257 结果确认 Completions fixed16 project/direct model-request
  throughput 为 16,036/16,416 tokens/s，达到 97.7%；固定 offered work 的
  token-budget 曲线在 32K 达到最高正式重复中位数 15,007 tokens/s。
- 审计发现旧 runtime env 仍默认 8K，submission-policy 模板硬编码 K64，
  导致后续实验虽可运行但没有继承本轮校准结果。旧数据保留为诊断证据，不进入
  饱和策略排名。
- 新增 `select_strategy_calibration.py` 和 `src/calibration.py`：要求 feeding
  ≥95%、至少三次正式重复，并按 97%-ceiling/下一档增益 <3% 选择预算；输出
  选择 JSON 和环境覆盖。data-organization、submission-policy 和 shared-vLLM
  formal 在任何外部请求前核对同一合同。
- token-budget 合同不再把一个预算写成所有目标的通用最优：32K 仍是当前
  throughput-oriented 冻结点；在吞吐不低于峰值 95% 的候选中另记录最大
  request SLO goodput 的预算。f203257 上该点为 49K，必须在 held-out 重复后
  才能作为 SLO-oriented static 对照。
- data-organization 与 submission-policy 不再硬编码 8K/K64/actor shape，
  改用冻结的 32K、K、active work 和 actor 参数。AutoDL 新运行时结果统一写到
  `/root/autodl-tmp/experiment-artifacts/`，仓库只接收审计后的摘要与报告。
- 调度实验重新明确“动态”的比较目标：direct-vLLM 是容量上界、一次校准后
  冻结的 static 是主要可部署 baseline、per-phase static oracle 只作诊断
  上界。动态策略不以改变单请求 kernel 速度为目标，而在运行中 workload
  漂移或多 job 竞争下，以容量退化不超过 3% 为护栏，比较 SLO goodput、JCT、
  P99、time-to-ceiling 和 adaptation regret。
- 在动态控制器头对头之前新增“存在性门禁”：固定其他变量后先验证不同
  workload 的最佳安全 static K 是否至少迁移 2×或 97%-ceiling 区间不重叠，
  且错用静态点是否造成至少 5% 的 SLO-goodput/JCT 损失。门禁不成立就停止
  adaptive formal 排名，避免为没有实际代价的静态差异设计控制器。
- 双 GPU adaptive formal 增加硬前置并完成代码拆分：typed AIMD/EWMA/PID/HOL
  controller state、服务指标和 action trace 均改为 endpoint-local；
  control trace 新增 endpoint ID。global adaptive 与 endpoint-local static
  limit 的混搭、dynamic K 与 active-work 动态混搭继续 fail closed，防止聚合
  指标或多变量联动污染正式策略结论；正式运行前仍需远端双 endpoint gate。
- actor pool shape 升级为 calibration contract 的独立证据：同协议、
  同 token budget/K/work、固定 256 slots 与 0.5 CPU/endpoint，扫描
  1/2/4/8/16 actors，并选择达到峰值中位数 97% 的最小 actor 数。Chat
  512-row 曲线的 4–8 actor 平台只作 feeding 诊断，不跨协议冻结
  Completions；选择脚本新增必填 actor-shape CSV 与 repeat 离散度记录。
- 新增可执行 static-K workload surface 与判定脚本：low/near/burst 到达压力
  分别扫描 K64/128/256，先过 95% capacity floor，再验证 K 迁移/可接受集合、
  ≥5% cross-workload regret 和 ≥2/3 paired repeats 同向。另增双 endpoint
  adaptive 256-row gate，只验证独立 controller/metrics/trace，不产出性能结论。

## 2026-07-30 Short/long 静态 credit 筛选审计与动态判决纠错

- 从远端同步 short/long prompt 两组 48/48 成功运行的 config、manifest、
  runs 和独立汇总。short/long server-observed prompt tokens/row 分别为
  16.95/566.23；prefix cache 关闭，模型为双 4090 Qwen2.5-7B。
- 远端初始报告使用 E2E tokens/s 算术平均，得到 short/long 均为 W65K；
  项目正式 model-request 中位数却选择 short W98K、long W65K。short
  W65K/W98K throughput CV 为 18%/34%，不能把平均值交叉表作为动态
  NO-GO。
- 机制审计发现 short K256/W65K/W98K 均无 bounded wait、一次性放行
  512 请求，work 高水位仅 49,318/endpoint；两个 work cap 未绑定，理论
  等价臂的 model-request 中位数仍分裂 48.5%。同时配置实际使用 urllib、
  未启用 output token IDs、short/long 未跨 workload 交错，六臂也不是
  K×work factorial。
- 因此本轮证据登记为 real-GPU screening / mechanism audit，机器判定为
  `inconclusive`，不能声称动态 K 已被否决，也不能声称 short/long
  精确 oracle 均为 65K。long W65K 的低 CV 正信号与 K256 的 SLO 负结果
  保留为后续候选依据。
- 新增 `summarize_static_credit_workload_surface.py`：统一使用中位数，
  检查 repeat CV、未施压等价臂、per-request token-ID 覆盖和交叉 regret，
  审计失败时 fail closed。新增 async/token-ID 单 runner 等价臂模板，先比较
  short/long K256、W65K、W98K；通过后才扩展 W49K 与 K×work 交互面。

## 2026-07-31 Prefix-affinity routing 消融（cache ON）收口 prefix 方向

- 完成 prefix-affinity routing 实验（`experiments/results/prefix_cache_routing_req_20260730/`）：
  route_least_queued / route_affinity (prefix_affinity) / route_affinity_pala (prefix_affinity
  + prefix_aware_length_align 二级排序)，1 warmup + 3 formal，seed 20260729，cache ON，
  request 粒度。12/12 ok，0 incident。
- model-request tok/s 中位数：least_queued 16093 / affinity 16078 / pala 16382，CV ≤0.5%。
  纯路由效应 −0.1%（中性，cache 碎片化假设不被支持）；pala +1.8% 来自 length-align，
  repeat 不重叠但低于 5% 门禁，不晋级。
- 与 07-30 batching 消融（cache ON，batch 粒度，上游 batching 顺序中性，within 1.2%）
  一致：vLLM APC 在多轮 ShareGPT 上自动复用 prefix，上游 batching + routing 均无额外
  空间。**prefix 方向收口，转 OceanBase baseline。**
- submission 粒度：`manifest_guard.py:82-93` 只在 request 粒度允许 prefix_affinity
  （batch 粒度强制 least_queued）。故 routing baseline 与 batching（batch 粒度）不直接
  可比；routing 三臂为干净 A/B。
- 过程问题：
  1. 环境漂移——batching 后 runtime env 的 BEST_TOKEN_BUDGET 改 8192→32768、
     SOURCE_WORKLOAD_NAME 改 multiturn→burstgpt。已硬编码回 batching 值避免 silent 不可比。
  2. manifest 元数据 bug——`service_metadata.prefix_caching` 声明 false 但 live vLLM 实际
     ON（进程参数 + 日志 ~71% 命中率）。runner 从 env 默认填该字段、未探测 live vLLM。
     不影响有效性（cache 确实开着），待修：runner 启动时从 vLLM /metrics 或进程参数探测
     实际开关，并在 resources 增采 vLLM prefix_cache_hit_rate（本次 per-arm 命中率因此未记录）。

## 2026-07-31 OceanBase B1 门禁验证：CE 有 AI_COMPLETE，但容器部署受阻

- 在 `claude/oceanbase-baseline` 分支尝试 matrix §2 的 B1（OceanBase AI_COMPLETE → 双 vLLM）。
- **门禁 #1 通过**：远端 apt 装 oceanbase-ce 4.5.0.0，observer 二进制（`T_FUN_SYS_AI_COMPLETE`、
  全套 `DBMS_AI_SERVICE_CREATE_AI_MODEL[_ENDPOINT]`）+ seed SQL（`dbms_ai_service_*.sql`）
  静态确证 `AI_COMPLETE`/`DBMS_AI_SERVICE` 在 **Community Edition**（非企业版独占）。
  见 `experiments/results/oceanbase_b1_gate_20260731/README.md`。
- **部署阻塞**：observer 在此 AutoDL 容器 init step 4/18（`clog/log_block_mgr`，errcode -9100
  `prepare_dir_and_create_meta_ failed`）自杀（`tgkill SIGKILL`）。已修复：obd/obclient 缺失
  → 直用 observer 二进制；`libaio1`；`memory_limit=6G`（2G 低于最小值、8192 被当 bytes）；
  `-N` nodaemon（无 systemd PID1）。已 strace 排除 max_map_count（257 mmap 零失败）、overlayfs
  （md0 真实盘同样）、磁盘、配置。容器 seccomp（Seccomp=2）拦 clone3（ENOSYS），但 observer 起了
  ~20 线程，非直接死因；真因未完全定位，从容器内部不可修（seccomp/kernel 只读）。
- 按 matrix §2：OceanBase 暂降为"工业参考/待部署"，不伪造 B1。复跑需特权容器
  （`seccomp=unconfined`/`--privileged`）或带 systemd 的 VM；复跑时复用 `code/src/baselines/products/oceanbase.py`
  （其对 DBMS_AI_SERVICE/AI_COMPLETE 的调用已确证 CE 支持）。
- 远端保留证据：oceanbase-ce 安装 + `/root/obdata/strace{2..7}.log` + `/etc/oceanbase.cnf`。

## 2026-07-31 代码修复：runner 校验 service_metadata.prefix_caching 与 live vLLM 一致

- **Bug**：scenario config 的 `service_metadata.prefix_caching` 是声明值，
  `code/src/experiment_scenarios.py:validate_service_metadata` 只校验类型、不比对
  live vLLM。后果：prefix-cache 实验在 cache-ON 的 vLLM 上跑，manifest 却记
  `prefix_caching: false`（声明值），silent 不一致——07-30 prefix-cache batching/
  routing 实验的 manifest 元数据就是因此失真。
- **根因**：vLLM 不经 `/metrics` 暴露 prefix-cache 开关（只有运行时命中率、且需流量）；
  唯一可靠信号是 vLLM 进程 cmdline 的 `--enable-prefix-caching` / `--no-enable-prefix-caching`。
- **修复（3 个文件）**：
  1. 新增 `code/src/vllm_probe.py`：`parse_prefix_caching_flag(cmdline)`（纯函数，
     argparse last-wins 语义，支持 `--flag` 与 `--flag=bool`）+ `probe_live_prefix_caching()`
     （best-effort：`ps -eo args` 找 vLLM api_server 进程、解析、取共识；探不到/进程间
     不一致/非 Linux 返回 None）。
  2. `code/scripts/experiments/run_ai_operator_scenarios.py`：加 `_verify_prefix_caching_matches_live`
     预检——声明值与 live 不符 → **fail-closed**（ValueError）；探不到 → stderr warn 后继续。
     **挂在 `main()` 而非 `run_experiment`**，使直接驱动 `run_experiment` 的 9 处单元测试
     保持 hermetic（不依赖宿主 vLLM 状态）。
  3. 新增 `code/tests/serving/test_vllm_probe.py`（15 测试）：parse 各分支 + probe（mock
     `_list_process_cmdlines`）+ verify helper 的 mismatch/match/none/skip。
- **怎么改的（关键决策）**：探测设计为 best-effort + fail-closed-on-detectable——
  能确证不一致时拒绝跑（防 silent 失真），探不到（Windows/CI/无同机 vLLM）时 warn 不 block。
  放 `main()` 而非 `run_experiment` 是为避免单元测试依赖宿主 vLLM 进程状态。
- **验证**：本地 `python code/tests/serving/test_vllm_probe.py` 15/15、
  `python code/tests/experiments/test_experiment_scenarios.py` 26/26 全绿；`--help` 正常。
  （`test_postgres_profile_scheduling` 本地缺 pyarrow 无法 import，与本次改动无关。）
- **边界**：探测只覆盖同机 vLLM（runner 与 vLLM 共宿）；vLLM 远程部署时探不到→warn。
  仅校验 `prefix_caching`（最易飘、影响最大）；其他 `service_metadata` 字段仍按声明值。

## 2026-07-31 4-endpoint prefix-affinity routing 消融（1.5B，跨过 5% 门禁）

- **背景**：2-endpoint/7B routing 实验中 prefix_affinity 相对 least_queued 完全中性
  （−0.1%，`experiments/results/prefix_cache_routing_req_20260730/`）。其 §6.2 把
  「>2 endpoint 下重测」列为可选扩展。本实验在 4×Qwen2.5-1.5B（2 endpoint/卡）上
  重测，检验「高淘汰压力 regime 下路由是否重新有空间」。
- **结果**（3 formal，seed 20260729，0 incident，manifest completed）：
  prefix_affinity 46,943 vs least_queued 44,317 model-request tok/s = **+5.9%**，
  raw 不重叠、CV≤0.9%；SLO 违约 25.1% vs 31.4%（−6.3pp），P95 36.16s vs 39.31s（−3.15s）。
  **跨过 5% 晋升门禁**——首个非中性的路由结果。报告：
  `experiments/results/prefix_cache_routing_4ep_1.5b_20260731/README.md`。
- **谨慎边界**：同时改了 model（1.5B vs 7B）、endpoint 数（4 vs 2）、per-endpoint KV
  大小，**不能干净归因于 endpoint 数单一变量**；两臂 SLO 违约 25–31% 处于过饱和
  /cache 抖动 regime，相对比较成立、绝对值是 thrashing 区间。跨门禁但需隔离消融
  （4-ep/7B 或 2-ep/1.5B）后才正式晋级。per-arm APC 命中率仍未单独记录（与 0730 同缺口）。
- **为了 4 endpoint 做的调整（4 项）**：
  1. **代码（使能点，commit `a26c1e2`）**：`code/src/profiling/manifest_guard.py`
     `endpoint_count != 2` → `< 2`，错误消息 "two endpoints" → "at least two endpoints"。
     注释明确：分片数上精确、超出变松，只允许 routing 消融、不能用于 pinned 排名。
     新增测试 `test_profile_manifest_contract_accepts_more_than_two_endpoints`。
  2. **配置**：`prefix_cache_routing_4ep_1.5b.json` 由 0730 的 req 配置派生——4 个
     endpoint URL、`--endpoint-gpu-ids 0,0,1,1`、模型 qwen2.5-1.5b（completion/cost/tokenizer
     三处）、4 metrics URL、scenario 去掉 pala 只留 2 臂。
  3. **vLLM 部署**：`4ep-1.5b.env`，4×Qwen2.5-1.5B-Instruct，2 endpoint/卡，
     `VLLM_GPU_MEMORY_UTILIZATION=0.43`，prefix-caching ON。换 1.5B 是为制造真实淘汰压力
     （7B/2-ep 下 APC 已覆盖 working set → 路由中性；1.5B/4-ep 下 APC 不够 → 路由效应显现）。
  4. **环境（主机重启后）**：清理 stale `/tmp/ray/ray_current_cluster`（见下条）。
- **stale Ray pointer 事故**：主机重启后首次 launch 在 warmup 的 `ray.init()` 卡死 ~14 分钟
  后 `ConnectionError`，0 请求发出。根因：`/tmp/ray/ray_current_cluster` 残留重启前死地址
  `172.17.0.8:6380`（重启后容器 IP 变为 172.17.0.3），ray.init 读取 stale 指针反复连死 GCS。
  修复：删除该指针（无活跃 Ray 进程需 stop）。失败首跑目录保留为
  `..._4ep_1.5b_20260731_failed_raystale/` 作事故证据。回归防范写入
  `deploy/autodl/README.md` 开机恢复流程。
- **对课题含义**：prefix 路由方向**有条件重新打开**（2-ep/7B 中性结论在该 regime 仍成立）；
  25–31% SLO 违约 + affinity 收益共同指向 KV cache 淘汰/重算瓶颈，为用户讨论的
  Mooncake/共享 KV cache 方向提供了首个动机数据点（待与导师确认是否纳入为第二贡献）。

## 2026-07-31 prefix 实验数据回填 git + 跨数据集（agent/concentrated）补分析

- **问题**：盘点发现构成当前 prefix 证据基底的三个 registry 引用目录——
  `prefix_cache_data_org_20260730/`、`prefix_cache_routing_req_20260730/`、
  `prefix_cache_routing_4ep_1.5b_20260731/`——**git 里只有 README.md，底层 runs.csv/manifest/per-run
  CSV（13–22 MB）全部只在远端**；4ep 目录更是 0 文件（README 仅本地 untracked）。一旦远端释放，活结论（+5.9%
  重开 prefix 方向）将无原始证据。属「有意义数据滞留远端」风险。
- **动作（分支 `claude/sync-prefix-cache-evidence`，未 push）**：
  1. 从 AutoDL 拉回 5 个目录的 runs.csv + manifest.json + per-run requests/submissions/resources/flush
     CSV（排除 gitignore 的 `*.log`）：上述 3 个 + `prefix_routing_agent_20260730/` +
     `prefix_routing_concentrated_20260730/`。
  2. 新写 Tier 2 跨数据集报告：`prefix_routing_agent_20260730/README.md`（agent + concentrated 合并分析，
     七步结构）+ `prefix_routing_concentrated_20260730/README.md`（自包含简表 + 指回 agent 报告）。
  3. 同步 `EXPERIMENT_EVIDENCE_REGISTRY.md`（§2 Prefix-aware 行、§3 新增两目录行、§6 item 5）、
     `experiments/plans/experiment_status_and_gaps.md`（§1 表 + P1 段）、`PROJECT_OUTLINE.md`（P1-4）：
     prefix 状态从「2-ep/7B 收口」细化为「2-ep/7B 跨三数据集吞吐中性 + 高淘汰压力 regime 双数据点（4-ep/1.5B +5.9%、agent-trace pala P50 −7.8%）有条件重开」。
- **新发现（agent-trace pala 信号）**：2-ep/7B、lmcache_agent（851 行、高 cache 压力 workload）下 pala
  相对 least_queued：吞吐 −1.9%（**未过门禁、负向**），但 **P50 64.2 vs 69.6s = −7.8%、SLO 78% vs 82% = −3.8pp、
  goodput +17%**。concentrated（cache 压力低）同信号弱（P50 −1.3%）。**信号随 cache 淘汰压力增大而增强**，
  与 4-ep/1.5B +5.9% 同机制——为「cache 淘汰压力是 prefix 方向价值是否显现的开关」补第 2 个独立数据点（仅改
  workload、不混淆 model/endpoint）。agent/concentrated 均 12/12 ok、0 incident、CV≤0.9%。
- **不同步（判定）**：4 个 `checkpoint_*`/`slo_ewma_flush_gate128` 是 1-repeat 门禁 screen，结论已被 git 里
  3-repeat 正式目录（active_work_saturation/actor_pool_shape/service_quantum/slo_ewma_formal）完整覆盖，
  不进 git；~30 个带时间戳 debug 变体 + 早期无日期目录属迭代噪声/被取代，保留远端不同步。
- **待办**：per-arm APC 命中率指标仍缺（runner resources 只采样 KV 用量）；agent pala P50 改善需人为缩 KV
  制造可控淘汰率单调验证；4-ep/1.5B +5.9% 仍需 4-ep/7B 或 2-ep/1.5B 隔离 model×endpoint×cache 解耦。

## 2026-07-31 KV-budget 扫描（2-ep/1.5B）隔离 4-ep +5.9%：endpoint 数是驱动，非 per-endpoint KV

- **回答上一条待办**：「4-ep/1.5B +5.9% 需 2-ep/1.5B 隔离 model×endpoint×cache」。固定 2 endpoint（1/GPU），扫
  `gpu_mem_util ∈ {0.3,0.45,0.6,0.9}`（+ 复用 2-ep/0.9 ablation 点），每点 `least_queued` vs `prefix_affinity`，
  sharegpt_multiturn 2048。结果存**新存储约定** `experiments/results/rc1_prefix_routing/kv_budget_sweep_20260731/{README.md, raw/}`。
- **结果**：2-ep 全 KV 范围 prefix_affinity **中性**（Δ ∈ [−0.1%, +1.0%]，含 util 0.3–0.6 的 13–15% SLO 抖动点）；
  util 0.9（~22.8GB、working set 全放下、0% SLO）吞吐回升到 ~64.8k（vs 抖动点 ~54.2k）。32 run、0 incident、CV≤1.0%。
- **matched-KV 对比（关键）**：2-ep/0.45（~12GB 显存、~7–8GB KV）= **−0.1%** vs 4-ep/0.43（~7GB KV/端）= **+5.9%**——
  per-endpoint KV 量级相当、**只差 endpoint 数（2 vs 4）**。→ **驱动是 endpoint 数（consolidation 拓扑），非 per-endpoint KV 大小**。
- **修正上一条 framing**：「cache 淘汰压力是开关」在 1.5B/multiturn 下被证伪——2-ep 即便 13–15% SLO 抖动也无 affinity
  收益；**endpoint 数才是开关**。agent-trace（2-ep/7B/不同 workload）的 P50 信号是独立数据点，跨 workload 是否
  cache-pressure 驱动待验。
- **诚实边界**：4-ep regime SLO 违约（25–31%）比 2-ep 最高（14%）更深，未完全分离「endpoint 数」与「抖动深度」；
  matched-KV 对比强烈指向 endpoint 数。util 0.9 为 n=2（第 3 rep 偶发 subprocess_nonzero 失败，2 rep CV≤0.3% 仍稳）。
- **同步**：`EXPERIMENT_EVIDENCE_REGISTRY.md` 新增 `rc1_prefix_routing/kv_budget_sweep_20260731/` 行；
  `experiment_status_and_gaps.md` §1.1 prefix 行细化（KV 排除、endpoint 数指向、cache-pressure-开关 假设证伪）；
  新存储约定（方向分组 + raw/ + README）首次采用，后续新数据（RC1 重测等）按此存。
- **对方向**：跨引擎共享 KV（Mooncake/LMCache）价值定位在**多 endpoint consolidation**（现实 DB-AI 部署：多模型
  endpoint 共享 GPU），不在小 KV。2-ep 作为 RC1 数据组织重测（#21–24）的干净基线合理（策略效应不被 routing/consolidation 混淆）。

## 2026-07-31 RC1 数据组织策略系统重测（2-ep + 4-ep，1.5B，cache-ON）：regime-dependent 闭合

- **动机**：07-18/19/25/26 早期 RC1 数据组织实验在旧数据集/rows(s)/单 5070/未喂饱（07-30 cache-OFF run GPU 67.7%）下，
  策略结论不可比。在干净平台（2×4090 + sharegpt_multiturn 2048 + tokens/s + httpx_async + token-IDs + **P0 指标**
  prefix_cache_hit_rate/TTFT/TBT，#27 新增采集）系统重测 5 策略 × {2-ep/0.9, 4-ep/0.43}。结果存新存储约定
  `experiments/results/rc1_data_organization/{README.md, dataorg_2ep_1.5b_cacheON_20260731/raw/(102),
  dataorg_4ep_1.5b_cacheON_20260731/raw/(102), bounded_2ep_1.5b_cacheON_20260731/raw/}`。
- **主结论（regime-dependent）**：
  - **2-ep（KV max 7–10%，无压力）**：5 策略 E2E 50–56k 紧凑，排名 fixed≈seq>bestfit>rowcap>lenalign；prefix 命中 0.60–0.76。
  - **4-ep（KV max 98–100%，饱和）**：5 策略 E2E 39–50k 分化两簇，**排名反转为 seq>fixed>>rowcap≈bestfit>lenalign**；
    **prefix 命中崩塌**——重排序类（length_align/best_fit/row_cap）0.60–0.76 → **0.06–0.07**，保序类 fixed/sequential 0.47–0.48。
  - **机制闭合（`prefix_group_ratio` 是 smoking gun）**：重排序类 organizer 打散 prefix 组（ratio 0.03）→
    4-ep KV 饱和 + least_queued 散到 4 端 → 同 prefix 淘汰前无法复用 → 命中崩 → prefill 重算激增 → TTFT 翻倍（0.2–0.3s→0.6–1.1s）、
    best_fit/row_cap SLO 60%。保序类 ratio 0.13–0.29，受影响小。
  - **consolidation 是惩罚**：4-ep 比 2-ep −10～−26%，能耗 +40%（17.2 vs 12.3 J/1k tok）。多 endpoint 小池 + 高 churn + 局部性丢失。
- **与 #28 / KV-sweep 闭环**：三者共同支撑「上游调度/组织策略的价值在模型服务饱和 regime（4-ep）才显现」；
  2-ep 无压力 regime 是干净对照基线。本重测从**数据组织侧**（#28 从 routing 侧）独立确认 4-ep KV 压力下局部性决定性。
- **合规**：GPU util 2-ep 79–85%（borderline）/4-ep 86–90%；CV 1–6% 稳定。**⚠️ feeding-saturation 门禁未正式算出**：
  (a) 2-ep bounded 是 batch-1 c256（46,947 tok/s，2,047/2,048 完成、1 瞬时 ReadError 排除），策略 107–120% **超过**它 → batch-1 太弱非真上限；
  (b) 4-ep bounded gate 硬编码 `exactly two completions endpoints`，4 endpoint 直接 ValueError 无法测。
  喂饱用 GPU util + 绝对 tok/s（与 #19 2-ep/0.9 ~57k 同量级）间接确认。**batched bounded（batch 16/32）+ 4-ep bounded 客户端列为待办。**
- **执行教训（harness 调试）**：4 个配置坑依次修复——runner 必需 `--metrics-urls`（非仅 `--health-url`）；
  profiler manifest 行数 guard（total-rows 必须=manifest 2048，512 行 smoke 不可行）；bounded gate 对输出目录已存在 fail-closed（须让 gate 自建目录）；
  bounded shard 1/1024 瞬时 ReadError 触发 `failed_rows:0` 硬门禁（已解耦：bounded best-effort，不影响 data-org）。
  每坑秒级发现（监控 60s 内断 halt），vLLM 跨 attempt 不重启、未浪费 GPU。
- **同步**：`experiment_status_and_gaps.md` §1.1 新增「RC1 数据组织系统重测」行 + 状态行更新（regime-dependent 闭合）；
  本目录 README（8 段全组件）；旧 07-25/26 gropy 标 superseded；07-18/19 最原始动机保留作历史参照。
- **下一步**：prefix_aware_token_budget 正文实验（能否回收 4-ep 重排序类命中率）；batched/4-ep bounded 补 feeding 门禁；
  `sharegpt_concentrated` + 7B 泛化对照；MFU 采集修复。

## 2026-07-31 补 feeding-saturation 门禁：准入控制是吞吐杠杆、效应随 regime 反向

- **动机**：RC1 数据组织重测（上一条）留了"feeding 门禁未正式算出"的缺口（2-ep batch-1 bounded 太弱、4-ep bounded gate 硬限 2-endpoint）。本条补齐。
- **改动**：`code/src/baselines/gate_runner.py` 把 `len(endpoint_urls) != 2` 放宽到 `< 2`（"at least two"）——2-endpoint 硬限是早期 2-ep-only 遗留，4-ep 实验需要；其余 shard/校验逻辑本就 N-endpoint 通用。2 cell（b16-c64/b32-c32）× 2 拓扑，2,048/2,048 完成、0 失败、`status: passed`。结果存 `rc1_data_organization/bounded_{2ep_batched,4ep_1.5b_cacheON}_20260731/raw/`。
- **bounded 真上限**：**2-ep = 79,488 tok/s**（b32-c32，wall 20.8s）；**4-ep = 24,733 tok/s**（b16-c64，wall 66.9s，**病态**）。
- **主结论（门禁细化，不是简单过/不过）**：
  - **2-ep**：策略 E2E 50–56k = 真上限的 **63–71%（严格 ≥95% 门禁不过）**。但 `model_request_wall`(27.5s) ≈ `operator_wall`(27.5s) → **非模型开销可忽略、无 pipeline 瓶颈**。缺口 = **active-work 准入门 W65536 把 inflight 压到 4–22（远低于 K256）**——故意节流（换 SLO/公平），非饿死 vLLM。GPU 79–85% 印证"在干活但没榨干"。
  - **4-ep**：unthrottled batched bounded **自己搞慢自己**——小 KV 池（0.43、KV max 98–100%）上一次打 256 并发 batched → 淘汰风暴 + 重 prefill → 24k（比策略 39–50k 还低）。**策略的准入节流反而帮忙**（inflight 8–22 → 少 thrash）。→ unthrottled bounded 在 4-ep **不是有效上限**；准入节流是 4-ep 解法的一部分。
  - **跨 regime**：**准入控制是吞吐 binding 杠杆，效应随 regime 反向**——2-ep 压住上限（放开 W 可提速）、4-ep 防 thrash（应保留）。这把 feeding 门禁从"过/不过"细化成一个**研究内容二（调度/准入）的实证信号**。
- **诚实边界**：策略**确实喂饱 vLLM**（GPU 80–90%、model_wall≈operator_wall、非饥饿），但**不榨干 raw 上限**——不能声称"策略已达理论上限"。4-ep bounded 病态值不能当上限用。2-ep 放开 W 测能否逼近 79k = 下一步验证。
- **同步**：`rc1_data_organization/README.md` §3 合规自检 + §6/§8 更新；本条记入 PROJECT_LOG；`experiment_status_and_gaps.md` / `EXPERIMENT_EVIDENCE_REGISTRY.md` / `PROJECT_OUTLINE.md` 的"feeding 待补"口径改成"已补 + 准入杠杆结论"；`gate_runner.py` 放宽 ≥2 endpoint（本地+远程，待 commit）。

## 2026-08-01 image-CLIP 多模态环境准备就绪（首个多模态 workload）

- **决策背景**：prefix 轨暂停（vLLM APC + Daft v0.6.9 已覆盖大半，与 §0 scoop 一致）；下一步 workload 锁定 image AI_EMBED（CLIP）找 DB-read/CPU→GPU 数据搬运瓶颈。用户要求远端准备环境。
- **完成**（远端 AutoDL 2×4090）：① 代码 git 同步 a26c1e2 → ba35e93（条件 reset，fetch 受下载抢带宽拖 ~7min）；② CLIP ViT-B/32 下到 `models/clip-vit-base-patch32`（~1.7G）；③ COCO val2017 下到 `data/raw/coco_val2017/`（~780M，5000 图 smoke 集）；④ GPU 验证 CLIP load + `get_image_features` → 512d embedding OK。
- **两个坑（已记入 `deploy/autodl/README.md` image-CLIP 节）**：
  1. `huggingface_hub 1.x`（1.25.1）的 `huggingface-cli download` wrapper 解析参数失败 → 改用 Python `snapshot_download`。
  2. `transformers 5.x` 的 `CLIPModel.get_image_features` 返回 `BaseModelOutputWithPooling`（非裸 tensor）→ 取 `.image_embeds`，不能直接 `.shape`。
- **下一步（serving 引擎，待建）**：CLIP 是 embedding 非 vLLM 生成；按 image_clip plan "ours 路径 B" = CLIP embedding HTTP endpoint（FastAPI）+ 上游 Ray CPU decode，项目 scheduler 观测 endpoint 队列；baseline A = Daft `@daft.cls` Native。观测层换：vLLM 的 prefix_cache_hit_rate/KV/running 在 CLIP 无对应物，改采 CPU decode/resize + CPU→GPU transfer + GPU embed 分阶段计时（"找搬运瓶颈"的画像）。

## 2026-08-01 image-CLIP §6 瓶颈画像门禁通过（GO）+ 代码质量总则

- **动机**：image-CLIP 锁为首个 workload 后、建 runner 前的 fatal-flaw go/no-go 门禁（`image_clip_workload_lock_20260731.md` §6）——CPU 数据准备相对 GPU CLIP forward 有多重？ratio > 0.3 才有异构调度舞台。
- **脚本**：`code/scripts/profiling/profile_image_clip_bottleneck.py`（~330 LOC，单进程、走 PG bytea、分阶段计时；按新「代码质量总则」写成可复用 stage 函数 `load_clip/pil_decode/cpu_preprocess/clip_encode`，path-B runner 后续直接复用）。
- **结果（GO）**（`motivation/results/gpu/image_clip_bottleneck_profile_20260801.{md,csv}`）：ratio = (decode+preprocess)/embed，实用 batch（≥16）**13–17**，远超 0.3。
  - 瓶颈 = **CLIPProcessor resize+normalize（cpu_preprocess ~5.2 ms/img）**，不是 JPEG decode（0.04 ms）、不是 CPU→GPU transfer（0.07–0.19 ms）、不是 pg_read（0.83 ms/img bulk 摊销）。
  - B=128 单 batch：CPU preprocess 655 ms vs GPU embed 38 ms → 串行下 GPU 忙 ~5.5%、空转 ~94%。量化了 path-B（分离 CPU preprocess 与 GPU embed 并 overlap）的必要性。
- **口径澄清**：ratio 分子不含 pg_read（pg_read 单独一列）；不算 DB 读 ratio 仍 13–17，结论不变。"数据搬运瓶颈"更准确是 **CPU 预处理计算瓶颈**。
- **更正上条 #32 记录**：transformers 5.x `get_image_features` 取 **`.pooler_output`**（512d），非 `.image_embeds`（5.x 无此属性）；脚本与 `image_serving.md §3.3` 均已用对。
- **规模边界 + redo（已完成 5K 规范跑）**：首跑 1024×50 iters 后，按用户要求加大规模重做——新增 `code/scripts/data/import_coco_images.py`（TRUNCATE+INSERT 单事务原子、记版本、path-B 可复用）载入完整 COCO val **5K**（815MB/33.8s），重跑 `--limit 5000 --iters 100 --batch-sizes 1,16,32,64,128,256`（~5min）。5K 结果 ratio **13.8–18.3**（B=256 渐近 ~18），p95 紧贴 p50，与 1024 首跑完全一致——结论（GO、CPU preprocess 主导）确认。pg_read 0.755ms/img（5K bulk 摊销）。
- **同步**：`experiment_status_and_gaps.md` §0/§1.4 已统一为 5K canonical GO；`image_clip_workload_lock §0`「暂停 build」→ 解除；`motivation/results/gpu/README.md` 索引；`code/AGENTS.md` 新增「代码质量总则（模块清晰 / 框架分明 / 低耦合 / 目标清晰）」。

## 2026-08-01 文档状态对账：统一 image-first、5K canonical 与 prefix 归因

- **权威关系**：明确 `experiments/plans/experiment_status_and_gaps.md` §0 记录内部执行顺序；内部已锁 A+B image-first，外部“DB↔GPU 经 Daft 桥接”scope/题目仍待导师和学长确认，二者不再混写。
- **5K 状态**：`AGENTS.md`、`PROJECT_OUTLINE.md`、`overview/current_direction_and_plan.md`、`code/INFRA_STATUS.md` 和证据台账统一为 COCO val 5K × 100 iterations 已通过 GO；当前进入 path-B runner + image 强 baseline，不再保留 redo pending。
- **文本轨道**：遗留 feeding/static-credit/prefix/multi-job/runtime baseline 统一标为 `parked-conditional`，不再阻塞 image build；文本历史证据保留。
- **prefix 归因修正**：证据台账与总纲移除“cache 淘汰压力是开关”的过时确定表述。matched-KV 结果更支持 endpoint consolidation 是驱动；4-ep 饱和深度仍是残余混淆。
- **代码边界**：`code/INFRA_STATUS.md` 明确区分“5K motivation/profile 已完成”和“image source/frame-cost、CLIP endpoint、path-B runner、正式方法对照尚未实现”，避免把画像写成系统完成。
- **快速入口**：`overview/current_direction_and_plan.md` 收缩为一页式当前状态卡片，删除被 pivot 取代的旧文本 P0/P1 执行清单。

## 2026-08-01 图像代码架构审阅与 serving 选型校正

- **官方能力校正**：vLLM 当前 pooling runner 已正式支持 CLIP/SigLIP 图像
  embedding；删除“CLIP 不能复用 vLLM / 没有服务端 batching”的过时前提。
- **实验边界**：vLLM pooling 接收 encoded image 并在服务内部预处理，不能与
  `Daft/Ray CPU preprocess -> tensor endpoint` 不加区分地比较。前者作为统一部署
  和强服务 baseline；主方法路径保留上游预处理边界，并使用 typed backend adapter。
- **工程规则**：`code/AGENTS.md` 增加中性 work-unit、图像输入表示、预处理归属、
  embedding 语义、隐藏 batching、流式 collect 禁令和引擎隔离合同；禁止正式 runner
  复用 `code/scripts/` 内实现。
- **审阅结论**：现有 text scheduler 可复用，但 source/organizer/BatchRequest/model
  adapter 尚为 text/token-specific；正式 image runner 前需先完成中性合同和流式
  Daft->Ray 边界，不能把图像逻辑继续堆入 profiler 单体。
- **部署约束二次校正**：审阅 AutoDL runbook 后确认当前实例明确不使用 Docker，
  而 Triton 官方推荐 NGC 容器；主方法第一实现因此改为常驻 Ray CLIP GPU actor，
  与既有 actor pool/backpressure 直接衔接。Triton保留为容器环境 production upper
  bound，vLLM pooling 保留强服务 baseline。
- **代码基础合同**：`BatchRequest` 新增兼容式 `work_units/work_unit`，scheduler、
  least-work 和 Ray adapter 已消费中性 `estimated_work_units`；新增 `code/src/image/`
  （embedding semantics、lazy Daft image source、CPU CLIP preprocessor、tensor-only
  GPU actor），并把 profiler 的 decode/output extraction 改为复用 `src.image`。
- **导入安全**：COCO importer 改为保留文件名中的稳定 source ID、按 workload
  原子替换而非 TRUNCATE 全表，并用安全 SQL identifier。

## 2026-08-01 合并远端 CLIP 子阶段画像并收紧证据边界

- **远端合并**：fast-forward 合并 `fa8f77a`，保留 slow-pt processor 子阶段脚本、
  5K 采样池 CSV 和对原画像“resize 不是大头”的事实修正。
- **审计修正**：旧脚本以 `p50(total)-Σp50(stage)` 近似 residual，且未记录
  PG/pgvector；新版改为逐 iteration 求未归因时间再计算 p50/p95，迁移到 psycopg3，
  补 processor/backend/torch/transformers/PG/pgvector 元数据。历史 CSV 保留原数字，
  补齐已知运行元数据，不覆盖重算。
- **结论降级**：能直接声称的只有 resize 约 1.3ms（约 25%）；其余约 3.8ms 是
  method-wrapper 未覆盖时间，不能归因成 PIL→NumPy、tensor stacking 或 Python 循环。
  “GPU 空转 95%”改为由串行阶段时间推导的理论非-forward占比；profile 只证明存在
  overlap 候选空间，不证明 path-B E2E 必然更快。
- **实现边界复测**：新增 `profile_image_clip_preprocess_variants.py`，在相同图片批次
  上随机交错 production-np、legacy-pt 与 torchvision 对照，经同一
  `ClipTensorActor` 输出并执行逐行 embedding cosine 门禁。正式实验不得故意保留
  slow processor 制造优化空间；fast/production 路径若消除瓶颈，应撤回旧外推。
- **远端 gate 部署坑**：仓库外旧 runtime env 尚无 `IMAGE_MODEL_PATH`，首次 gate 在
  模型加载前因空路径 fail。脚本新增非空 fail-fast，runbook 对旧 env 使用当前固定
  模型目录 fallback，并在运行前 `test -d`；失败 gate 保留作部署诊断，不当成实验。
- **首次 formal gate 的质量计算 bug**：540 条数据完整，但旧 parity 直接用点积
  代替 cosine；float16 归一化范数不精确，使完全相同（max_abs=0）的 embedding
  被误判为 0.998907。改为带范数分母的真 cosine，并让 actor 在 float32 归一化。
- **fast baseline 修正**：torchvision processor 若仍输入 PIL，会先做转换，实测与
  slow path 几乎相同，不能代表官方 fast-path 能力。复测拆为 torchvision+PIL 与
  torchvision tensor-decode 两臂；后者才检验 tensor backend 的性能/质量 trade-off。

## 2026-08-01 CLIP 当前实现边界正式画像完成并同步

- **远端运行**：AutoDL 单卡，提交 `f3d17af`，COCO val 5000 图采样池，batch
  1/16/32/64/128/256，每格 5 warmup + 30 formal，四变体随机交错；720/720 raw
  rows 完整，PG18.4/pgvector0.8.5/Torch2.12.1/Transformers5.14.1 元数据齐全。
- **质量**：修复真 cosine 与 float32 normalization 后，四臂最小 cosine=1、
  max_abs=0；无 silent skip。首次空模型 env gate 和错误 cosine formal 均保留在远端
  独立失败目录，不混入正式结果。
- **性能事实**：torchvision tensor-decode 相对 production-np 的配对串行 profile
  提升 1.14–1.22×（B≥16 为 30/30 repeats 同向）；但 fast CPU prepare 仍为
  4.44–4.78ms/image、是 actor 的 13.8–31.2×，阶段失衡未消失。
- **证据边界**：结果只支持继续建设 E2E overlap runner；不能声称胜过 Daft Native、
  Ray Data 或 vLLM pooling，也不能把 profile speedup 写成调度策略收益。
- **同步**：raw CSV、manifest、run log 和七步报告纳入
  `motivation/results/gpu/image_clip_preprocess_variants_20260801/`。

## 2026-08-01 图像 operator-E2E 强 baseline runner

- **方法学分层**：新增 operator E2E（每 query 模型 worker 建立/执行开始 → 最后一批 embedding 返回，排除 Ray 框架启动）
  与 system E2E（再含统一 pgvector sink）两层边界；micro-profile 不再代替动机
  baseline，operator gate 也不冒充完整数据库作业时间。
- **强 baseline**：新增同语义 `daft_native` 与 `daft_ray` 两臂，复用同一个
  `@daft.cls(gpus=1)` fast torchvision tensor processor + CLIP UDF，仅切换 runner。
- **项目臂**：新增 Daft lazy source → 有界 Ray CPU preprocess actors → tensor-only
  GPU actors 流水线；不在 driver 全量 collect，限制 active batches，复用 typed image
  batch/result 合同。
- **严谨性**：三臂固定数据行、模型/processor revision、dtype、batch 和 GPU 数，
  输出 streaming exactly-once、512d/finite/L2 norm/checksum 审计；记录 operator JCT、
  first-output、images/s 和 per-device GPU util，不再只报告吞吐。
- **运行顺序**：先 256 行三臂 gate，再 COCO val 5000、3 repeats、Latin-square
  交错 formal。首轮排除 writeback 以隔离执行器；通过后给三臂接相同 pgvector sink。
- **生命周期校正**：远端最小 smoke 确认 Daft UDF actor 按 query 重建；project-Ray
  formal 同样在 warmup 后销毁并重建 pool，并把 worker/model setup 计入 job JCT，
  防止持久 project actor 与冷 Daft actor 的不公平比较。
- **Daft 分区校正**：256 行双 GPU gate 显示 PostgreSQL scan 的单输入 partition
  只激活一个 Daft GPU UDF worker；Daft 0.7.21 NativeRunner 对 `repartition` 和
  `into_partitions` 都明确 no-op。最终改为 source 端生成两个不重叠 PostgreSQL lazy
  shards，再 `daft.concat` 保留独立输入 partitions；三臂统一使用同一 sharded source。
  修正前单卡 gate 仅作配置诊断，不进入性能比较。
- **强 baseline 追加**：首轮 5K×3 证明 stage-separated project-Ray 稳定快于
  one-actor-per-GPU Daft，但 Daft UDF 尚未独立标定 fractional-GPU actor shape。
  正式结论前追加 1/2/4 actors-per-GPU screening；冻结 Daft 最佳形状后，与相同
  source shards 的 project 重跑，避免把额外 CPU preprocess 并发只给 proposed 臂。

## 2026-08-01 图像 Daft Native/Ray 强基线校准与 operator-E2E formal

- **baseline 校准**：单卡 Native 从 1→2→4 fused actors 持续改善，冻结 4 actor；
  双卡 Daft Ray 冻结 4 actor，6 actor 退化到 28.23s，8 actor 退化到 179.62s。
  因此 one-actor-per-GPU 结果只保留为“为什么必须校准”的诊断，不作为正式基线。
- **formal**：提交 `ba1b710`，PG18.4 COCO val 5000 BYTEA、batch64、cold worker
  lifecycle、3 repeats。单卡 project median 17.09s/292.54 img/s，相对 Native
  22.15s/225.76 img/s 为 1.296×；双卡 project 15.77s/316.96 img/s，相对
  Daft Ray 17.95s/278.50 img/s 为 1.138×。12/12 exactly-once，norm/error 合同通过。
- **诚实边界**：这是同物理机器各自校准最佳点，不是相同 Ray CPU reservation；
  当前静态 bounded stage separation 也不是状态感知策略。pgvector system-E2E、
  bounded direct ceiling、CPU-budget-normalized curve 与 Ray Data baseline 仍待补。
- **归档**：原始 CSV、逐 run manifest、派生 summary 与七步报告纳入
  `motivation/results/gpu/image_clip_native_baseline_20260801/`。

## 2026-08-01 图像 baseline 指标审计与 host data path 动机实验预注册

- **历史字段纠错**：schema v1 的 `batch_service` 实为 submission→result wall，
  包含 CPU ObjectRef 依赖、actor queue、host copy、H2D、forward、D2H 与返回，不能
  称纯 GPU service；Daft `worker_setup_s=0` 表示 setup 折叠在 timed query，不表示
  setup 未计入；单卡旧 GPU mean 还平均了第二张空闲卡。
- **证据降级**：旧结果保留 13.8%–29.6% operator-E2E 事实，但不再由它声称 CPU
  已饱和、PCIe 已饱和/可忽略或 GPU MFU；第一维 checksum 只作粗粒度异常检查，
  不能证明完整逐行 embedding 等价。
- **schema v2**：图像 runner 增加 explicit/folded setup 语义、completion/actor/
  preprocess/host-copy/H2D/forward/D2H 字段、system per-core CPU、active-device GPU、
  功耗/时钟/估算能耗、PCIe current/max link、pending peak/未归因 wait、各阶段
  逻辑 bytes、全维 sum 与按 doc_id 的 rounded digest。CUDA 分段同步只允许
  diagnostic 模式；MFU 仅在显式输入经校准 FLOP 口径时估算。
- **新动机计划**：新增 `motivation/plans/image_host_data_path_bottleneck.md`。
  用 R0 GPU-resident compute ceiling → R1 pinned H2D → R2 pageable/Ray tensor →
  R3 in-memory JPEG → R4 PostgreSQL/Daft 的表示阶梯，按预注册门槛判定
  CPU-preprocess、framework/host-copy、PCIe/H2D、GPU compute 或 mixed。
- **负载定义**：不是盲目增加总行数或追求 `nvidia-smi=100%`；总 work volume 只
  用于获得至少 60 秒稳态，真正扫描 batch 与 active batches/producer concurrency。
  连续两个点吞吐增益 <3%、CV≤5% 定义平台，97% 平台吞吐的最小 active work 是
  minimum saturation point。增加队列只涨 JCT 不涨吞吐时不得继续称“喂得更满”。
- **研究问题修正**：不预设“主流系统普遍 GPU 空转”。正式问题改为 matched
  input/model/quality/resource 下，Daft Native/Ray、Ray Data、vLLM pooling 与项目
  静态路径各自由哪一阶段形成木桶，谁能以更少 GPU bubble 获得更高 E2E 有效工作
  效率且不牺牲 JCT/SLO。官方系统均先独立校准，matched-resource 与各自最佳上限
  分表报告。

## 2026-08-01 图像 baseline 分层与主流执行链口径修正

- **关键纠错**：现有 1.296×/1.138× 对照的 Daft UDF 把 CPU preprocess 与 GPU
  forward 融合在同一个 GPU-reserved actor 中，只能称校准后的 fused Daft baseline。
  PolarDB/Daft 官方已支持 CPU 算子→GPU 类 UDF 的 staged 异构流水线，因此旧结果
  不能代表最强 Daft/PolarDB-style baseline。
- **baseline 补齐**：图像正式矩阵增加 Daft-on-Ray staged 和 Ray Data staged；
  compute ceiling、direct service、fused framework、staged framework、product SQL、
  frozen project static、project adaptive 分层报告。架构增量与策略增量分别对比 staged
  system baseline 和 frozen project static。
- **产品路线梳理**：把数据库 AI 执行链归纳为 in-database、SQL→remote endpoint、
  queue-worker、distributed data pipeline 四类；PolarDB 同时存在 Polar_AI→EAS 与
  Daft-on-Ray 两条路线。不同云硬件只作工业参考，不参与 raw throughput 排名。
- **传输口径**：存储/DB、序列化/网络、Ray object store/host copy、PCIe H2D、D2H/
  writeback 分段判定；不再把约 600KB tensor 直接写成 PCIe/数据搬运已成为 binding。
  GDS 只在存储 I/O 位于关键路径或与 GPU decode 联合成独立臂时考虑。
- **实现缺口**：`run_image_clip_e2e.py` 当前只覆盖 fused Daft 与 project-Ray；下一步
  需新增 staged Daft-on-Ray/Ray Data arms，再做 R0→R4、统一 pgvector sink 和策略实验。

## 2026-08-02 数据库 AI 算子评价指标合同

- **外部口径审计**：综合 SemBench、LOTUS、Palimpzest、Cortex AISQL、vLLM serving、
  Ray Data 与 PolarDB 多模态 benchmark，确认正式评价不能只报告吞吐；最低证据链为
  质量、JCT/E2E、容量、尾延迟/SLO、成本/work、内存、失败与扩展性。
- **指标合同**：在 `experiments/plans/baseline_reference.md` 固化每个正式 run 的身份、
  工作量、正确性、任务质量、时间、容量、成本、资源、调度、扩展和统计字段，并区分
  managed product 可观察指标与同机开源 baseline 的内部诊断指标。
- **图像缺口**：schema v9 已覆盖 stage timing、CPU/GPU/能耗/传输与执行正确性，
  但 ground-truth 质量、失败 run 结构化落盘、统一 system sink、Ray object-store/spill
  和逐行尾延迟仍待补。任务质量、失败记录和 system sink 被列为正式排名阻断项。

## 2026-08-02 Baseline 检索流程与过期文档清理

- **可复核检索**：在 `baseline_reference.md` 固化“先定义算子/层级→官方 capability→
  官方 benchmark/code→数据库论文→部署平台”的来源顺序、来源卡片、A–E 证据等级、
  最小强 baseline 集合与过期触发条件。
- **公开 benchmark 纠错**：删除“多模态 benchmark 无现成协议、厂商全闭源”和
  “统一叫 BigVectorBench”的旧说法，改为 Ray/PolarDB 公开 file/object track、
  任务质量协议与项目 PostgreSQL database-operator track 分层。
- **状态同步**：Daft staged 与 Ray Data staged 已从“待实现/未测”更新为“runner 和
  256 行资源/正确性 gate 已通过，独立 calibration/formal 未完成”；gate 不进入性能排名。
- **入口去重**：`experiments/README.md` 不再复制大段容易过期的文本参数和下一步，
  历史数字回归 evidence registry/结果目录，当前执行顺序只指向 status §0。
- **规则同步**：`code/AGENTS.md` 的“所有实验必须 tokens/s”旧规则改为按算子语义
  采集文本、分类和 embedding 指标；`code/INFRA_STATUS.md` 同步 staged gate 状态，
  文本 baseline matrix 增加历史阅读范围提示。
- **研究口径纠错**：`research/daft_db_gpu_bridge_direction_scope_20260731.md` 删除
  “执行优化空白”“厂商全闭源”“统一 BigVectorBench”和 OceanBase 文本 embedding
  冒充图像 CLIP baseline 的旧表述，改为公开 benchmark + 同机 DB track 双轨协议。

## 2026-08-02 图像 baseline 原生性门禁

- **口径收紧**：正式 baseline 必须直接运行 vendor benchmark、内置 AI Function 或
  vendor-native API graph；项目只能适配 source/sink/审计/指标。项目自写 actor pool、
  inflight/credit/backpressure 或重写执行图不得进入 baseline 主排名。
- **代码调整**：新增 Daft 内置 `decode_image → embed_image` arm；Ray Data graph 删除
  项目 `max_active_batches`，由 Ray Data 自己管理 actor task/backpressure。旧
  `daft_native/daft_ray/daft_staged` 明确降级为 diagnostic reference，formal 默认
  fail closed，只有显式 diagnostic override 才能运行且 eligibility 仍为 false。
- **可审计 provenance**：schema v10/manifest 新增 implementation provenance、
  scheduler owner、custom scheduling、formal eligibility 和 upstream source；新增
  `code/src/image/baseline_contract.py` 与对应单测。
- **官方代码固定**：Daft image-classification vendor-code parity 固定到
  `Eventual-Inc/Daft@3f5bdd175b7de3dcdf35765e1ba604b5c1cb8e15`，并记录官方
  `README.md`、`daft_main.py`、`ray_data_main.py` 的 SHA256、803,580-row workload
  和适配白名单；禁止重写 vendor batching/actor/backpressure。
- **实验路线修正**：旧 1.296×/1.138× 保留为项目自写 Daft UDF 的阶段耦合动机，
  不再称官方 Daft baseline。正式图像比较改为 Daft built-in、固定 upstream commit 的
  官方 803,580-row ResNet18 Daft/Ray Data 脚本、Ray Data database native graph、
  bounded direct 和 frozen project static。

## 2026-08-02 文本 baseline 原生性审计与复测准备

- **角色纠错**：文本 harness 不再把全部 arms 混称 official baseline。vLLM Bench
  固定为 service ceiling；项目 `bounded_http`/`bounded_completions` 固定为 direct
  controls；Daft built-in `functions.prompt`、Ray Data HTTP Processor 与通过部署门禁的
  OceanBase `AI_COMPLETE` 才具有 vendor-native baseline 资格。
- **代码分层**：删除扁平 `baselines/official_runtime.py`，拆为
  `baselines/runtime/{common,daft_prompt,ray_data_http}.py`；新增 `provenance.py` 统一
  arm 身份、调度所有者、custom scheduling、formal eligibility 与 upstream source。
  进一步把 vLLM Bench、bounded controls、OceanBase 分别归入 `ceilings/`、`controls/`、
  `products/`，根层只保留共享合同、结果和编排，避免后续新增 adapter 再次扁平堆积。
- **Fail-closed**：CLI summary、resolved gate config 和 validity gate 记录/核验 provenance；
  缺字段或原生 arm 含项目调度即失败。服务端 counter 新增 prompt/generation/total
  tokens/s，保证 Daft 无 output usage 时仍可按统一服务工作量比较。
- **配置纠错**：删除 Daft adapter 未接线的 `partition_count` calibration 假因子；Ray
  Data 只扫描官方 batch/concurrency。新增 4,096 held-out、≥60 秒、1 warmup + 3
  interleaved repeats 的 formal 合同。
- **执行边界**：旧 64/256 行数据继续作为 gate/screening；因缺少长稳态、交错三重复和
  新 provenance 字段，不进入正式排名。用户已关闭 AutoDL，本次只完成本地代码/文档/
  测试准备，远端重测等待开机。
- **学习材料**：新增 `learning/text_native_baseline_guide.md`，用数据链路解释
  ceiling/control/native/project、Chat/Completions 分轨、64→512→4096 流程和双 endpoint
  group throughput，避免后续只看单个 tokens/s 或把 barrier 当逐请求 P99。

## 2026-08-02 全项目代码结构重构规划

- **现状确认**：此前只整理了文本 baseline；`src` 顶层仍有 22 个 Python 文件，
  `scripts/tests` 分别有 22/58 个顶层文件，并存在 profiler/scheduling 双入口与多个
  500–1,923 行单体文件。
- **双维度架构**：公共执行核心按 data/planning/scheduling/serving/observability/
  sinks/experiments 分层；文本与图像只在 `modalities/` 保存合同、代价、预处理、payload、
  后端和质量评价，不复制两套 scheduler。
- **baseline 分轨**：目标结构为 `baselines/common|text|image`，文本与图像分别维护
  ceiling/control/framework/product/vendor adapter，共用 provenance/result/gate 合同。
- **迁移纪律**：先冻结边界和清除兼容入口，再整理模态/baseline，之后抽公共层并逐个拆
  大文件；路径迁移不与算法修改或全仓格式化混合。完整计划见
  `code/ARCHITECTURE_REFACTOR_PLAN.md`。

## 2026-08-02 metrics、backend 与 shared-vLLM 大文件拆分

- `observability/metrics.py` 拆为 timing、CSV、statistics、resources、vLLM 五个职责模块；
  `serving/backends.py` 拆为 common、embedding、completion，公开包导入保持兼容。
- 1,923 行 shared-vLLM 单体拆为 config、runtime、evidence、metrics、runner；配置、执行、
  Ray 观测、exactly-once/resume 证据和组级统计不再相互混放。
- 本次只改变模块归属与测试 patch 位置，不改变 CLI、算法、默认值或 CSV schema。
- 依赖无关测试 580/580 通过；Daft/psycopg 和 macOS Ray 权限相关用例仍需在完整远端环境
  验证。scripts/tests 物理迁移保留为下一独立提交。

## 2026-08-02 scripts 与 tests 物理分组

- 22 个 CLI 入口按 data/services/baselines/profiling/experiments/analysis 分组；58 个测试
  文件按 data/planning/scheduling/serving/modalities/observability/baselines/
  experiments/infrastructure/architecture 镜像归档。
- 当前 README、部署指南、实验计划和结果 README 中的复现命令同步到新路径；已执行实验
  的 raw JSON manifest 不改写，保留其原始命令证据。
- 所有脚本/测试使用向上查找 `code/src` 的稳定 root 解析，不依赖固定 `parents[n]`；
  unittest discovery 显式指定 `-t code`，避免 tests/experiments 与 src/experiments 冲突。
- 目录迁移后依赖无关测试为 586/586 通过；完整 607 项中的其余环境用例需要远端
  Daft、psycopg 和 Ray
  权限环境。

## 2026-08-03 架构重构远端门禁与根入口同步

- 在独立 AutoDL worktree `6380a96` 完成完整依赖测试，622/622 通过；主仓库中的
  未跟踪历史实验数据未清理、未覆盖。
- 双 4090 文本链路用匹配的 512 行 immutable manifest 完成 Daft→Ray actor→双 vLLM
  smoke；图像 Daft staged 与 Ray Data staged 均完成 256/256 exactly-once gate，输出
  digest 一致。上述均为可运行性/正确性证据，不进入正式性能排名。
- 发现服务器旧 `rc1_dataorg_2ep_smoke.json` 声明 512 行却引用 2048 行 manifest；
  fail-closed 正确拒绝，正式运行不得复用该旧本地 gate。
- 将代码架构重构快进合入并推送 `main`；根 `README.md` 同步当前代码分层、baseline
  身份、已完成 gate、当前证据边界和近期执行顺序，移除重构前扁平脚本树与过期目标。
## 2026-08-03 baseline 总入口收敛与 embedding parity 诊断修复

- 将 `experiments/plans/baseline_reference.md` 收敛为 AI_COMPLETE、AI_EMBED、
  AI_CLASSIFY 三类算子的统一 baseline/benchmark 入口；专项文件继续分别承担文本执行、
  图像 workload、状态审计和厂商/论文证据，避免物理合并造成重复与过期。
- 修复 `282e09f` 中 `--save-embeddings` 误读 `ExecutionResult.embeddings` 的问题：
  默认流式路径不保留矩阵，小规模 gate 仅通过可选 `EmbeddingCapture` 捕获已验证输出；
  capture-enabled timing 明确禁止用于性能结论。

## 2026-08-03 Daft built-in / project 图像 embedding 语义门禁

- AutoDL commit `6092b84` 上完成 Daft built-in `embed_image` 与 `project_ray` 的同一
  256 图逐行 capture；两臂均 256/256、512 维、finite、exactly-once，无重复或漏行。
- Daft raw norm P50=10.4718，项目 raw norm P50=1.0；分别 L2 normalize 后逐行 cosine
  P1=0.999788、P50=0.999985、min=0.999716，非自身 overlap@10 mean=0.9949，超过预注册
  门槛，判定为 `SCALE_NORMALIZATION_ONLY`。
- 正式 AI_EMBED baseline 可使用统一 normalized contract，但归一化成本必须计入每个 arm
  的 E2E，并保留 vendor raw 辅助结果。capture timing 与 256 图冷启动 gate 均不进入性能
  排名；两条默认无 capture 路径也已在远端通过。
- 报告与派生摘要保存于
  `motivation/results/gpu/image_embedding_parity_20260803/`；原始 `.npz`、逐行 CSV 与
  manifest 保留在 AutoDL experiment-artifacts，不提交大矩阵。

## 2026-08-03 baseline / benchmark 文档收敛

- 冻结 `experiments/plans/baseline_reference.md` 为三类算子的唯一 baseline 总入口；
  文本执行、图像 workload、状态审计、外部指标证据、学习讲解和结果目录各自只保留
  单一职责，不再复制总表或“当前下一步”。
- 将混有旧预注册、逐日结果和过期执行顺序的
  `database_ai_operator_baseline_matrix_20260729.md` 完整移入 `plans/archive/`，保留历史
  证据但从当前 README、索引、runner 说明和结果入口移除。
- 当前运行只读取 `baseline_reference.md`、对应模态执行合同和
  `experiment_status_and_gaps.md` §0；`code_doc/superpowers/` 与 `plans/archive/` 仅作
  设计历史。

## 2026-08-03 图像长稳态实验执行顺序

- 将服务器后续任务收敛为五段 fail-closed 流程：当前代码 project static 复验 →
  normalized output contract/官方 vendor-code 门禁 → 各原生 arm 独立 calibration →
  frozen operator formal → 统一 pgvector system E2E 与方法消融。
- 明确 60K unique×logical passes、单 run≥60s、固定 seed 交错 1+3、3% 近最优选简单
  点、CV>10% 补重复、动态只对 frozen static 等规则；不以扩大无效网格换取运行时长。
- 修正 AutoDL 后台启动模板：runtime env 必须通过 `set -a` 导出，否则 matrix 子进程
  在第 0 个 run 缺少 `DATABASE_URL`；补充唯一输出目录、监控、resume 和 0-run 清理
  边界。科学合同在 image workload §10，部署文档只保留可执行命令。

## 2026-08-03 图像原生 baseline 独立校准完成（campaign §10 step 3）

campaign §10 五步：① project static ✅ → ② normalized output contract/官方 vendor-code
门禁 ⏸（codex WIP）→ **③ 各 arm 独立 calibration ✅（本轮完成）** → ④ frozen operator
formal ⏸（gated on ②）→ ⑤ system E2E + 方法消融 ⏸（gated on ④）。

**③ 本轮完成**（commit `0f66017` + `f1cb248`）：
- **Daft built-in `embed_image`**（vendor-native，`scheduler_owner=daft`）：
  batch {16,32,64,128,256}×2 rep @ 5000 COCO/双卡。平台点 batch=64≈**177 img/s**
  （CV 1.1%）。GPU 平均利用率仅 **1.2–4.1%**（双卡均 claim），近末端发射（first_output≈e2e）。
- **Ray Data `map_batches`**（framework-native，`scheduler_owner=ray_data`，`normalize=True`）：
  Phase 1 batch 扫（batch 几乎无影响，321–344）+ Phase 2 cpu_workers 扫（平台 cpu=8）。
  最佳 batch=64/cpu=8≈**346 img/s**（CV 1.2%），GPU 平均利用率 **1.1–3.9%**，first_output≈9s（真流式）。
  Ray Data stats 显示 binding stage=**CPU preprocess**（171 rows/single-task，GPU predictor 1662 rows/single-task 被饿）。
- **关键动机信号**：两个 framework-native baseline 在真实 bytea-in-PG 链路上都把两张 4090
  闲置到 ~2–4%，binding 在 CPU preprocess/喂入侧（与 R0–R2 ceiling ~9.7K img/s、R3 CPU preprocess ~5ms/img
  一致）。Ray Data ~1.95× Daft built-in（5K/双卡/cpu≈4 同条件对照，**校准条件下的 cross-check，非正式排名**）。
- **修正点**：先前误用 `--limit 60000 --dataset-passes 2`=120K 校准导致 Daft 单 PhysicalScan 漏斗
  （458% CPU/189GB RSS）饿死 GPU 池；改回文档 §5.4 的 5000 规模后双卡均激活、正常流式。
- **③ 不排名**：统一 L2-normalized contract 是 ②（codex），未推送前不做 ④ formal ranking。
  Daft raw vs Ray Data/project normalized 的正式横向比较待 ②。

报告与派生摘要：`motivation/results/gpu/daft_builtin_calibration_20260803/`、
`motivation/results/gpu/ray_data_calibration_20260803/`（七步 README + summary + raw runs.csv）。
原始 per-run manifest + calibration.log 保留在 AutoDL experiment-artifacts。

## 2026-08-03 图像 embedding 统一输出合同

- runner schema 升至 v11，新增 `arm_default/l2_normalized` 输出合同；正式跨系统排名必须
  显式使用 `l2_normalized`，并记录 requested/effective contract、normalization owner
  与计时归属。
- Daft built-in 仍使用官方 `decode_image→embed_image` 原生图；adapter 仅在消费官方
  embedding 后执行计时内 CPU L2 normalization，不注入项目 batching、credit、router
  或 actor 调度。历史 raw-output Daft 数据只保留作 batch screening。
- normalized parity 远端门禁在 `6f0954b` 上通过：Daft/project 均为单位 norm、
  exactly-once，cosine P1=0.999800、min=0.999727、non-self overlap@10=0.9949。
- 新增 Ray Data 原生 batch 上界复核模板：固定 cpu8/gpu2/source4，只扫官方
  batch16/64/256/512。25K×1 预跑 formal 仅约 30 秒，被 60 秒门禁拒绝；正式模板改为
  60K unique×2 passes、1+2 交错；512 未改善 3% 即停止。
- 60K×2 复核在 `d73fbfb` 上完成 12/12、0 incident。formal 中位吞吐：batch16
  935.109、batch64 957.100、batch256 919.193、batch512 883.221 img/s；各点 CV
  0.03%–2.08%，全部 exactly-once、L2-normalized。冻结 batch64；512 相对慢 7.719%，
  按 stop condition 不测 1024。原始 runs、matrix manifest 与派生 summary 已同步 Git。
- normalized parity 的两份 256×512 `.npz`、capture sidecar 与逐行 CSV 同步 Git；本地用
  `probe_embedding_parity.py` 重算与服务器 summary/per-row 完全一致。审计同时发现旧
  sidecar `note` 未随输出合同变化；raw 保持不变，runner 改为在 sidecar 记录实际
  requested/effective contract、normalization owner 与 timed-boundary 标志。
- `238f261` 已在服务器执行全量 `unittest discover`，628/628 通过；另跑 64 图 Daft
  normalized capture 代码门禁，sidecar 正确写入 effective contract、owner、timed boundary
  和 `timing_valid_for_performance=false`。该临时 gate 不产生研究结论，核验后删除；成功
  parity 与 60K×2 calibration artifact 继续保留。


## 2026-08-03 project_ray 静态配置选择证据归档 + 状态修正

- 把只在服务器的两轮 project-static 矩阵归档进 Git：
  `motivation/results/gpu/image_project_static_60k_x2_20260803/`（两轮 runs.csv + matrix_manifest
  + summary + 七步报告）。两轮 commit `1f2e4fe`(08-02) + `29b256b`(08-03)，4 配置 cpu{8,16}×active{16,32}×3 formal @60K×2。
- **冻结 project 静态点 `cpu16/active32/batch64`**：两轮 formal 中位 **1701.0 / 1681.0 img/s**（~1.2% 差）、
  exactly-once、120000/60000、max_norm_error=0。cpu16 是主杠杆（cpu8→16 +45–60%），active32 在 cpu16 时再 +15%。
- **定位为"静态配置选择证据"，不是跨系统正式排名**：两轮均为旧 schema（无 `schema_version`、
  无 `embedding_output_contract` 字段），早于 `03b815d` 统一合同。最终排名须在当前 commit + `l2_normalized` 合同下重跑。
- 修正过期状态：`experiment_status_and_gaps.md` §0 的"② 待独立校准与 formal"改为反映
  原生校准已完成（Daft built-in + Ray Data native）、project 静态点已冻结、统一合同已落地；
  下一步 Daft built-in 60K 长门禁 → 四臂同机 formal。
- 同时确认 codex `238f261`+`f450e07` 已归档 Ray Data 60K×2 长稳态 crosscheck（batch64≈957 img/s）
  并收紧了我两份校准报告的 claim 边界（GPU busy 采样≠MFU、"候选限制阶段"非定论）——接受这些收紧。


## 2026-08-03 Daft built-in 60K 物化-cap 决策 + 2-arm formal 启动

- **Daft built-in 60K×1 长门禁结论**：3 次公平环境尝试（默认 / `RAY_TMPDIR` /
  `/tmp/ray`→大盘 symlink）全部 `OutOfDiskError`。根因：Daft built-in 的
  `DistributedActorPoolProject` **物化执行（不流式）**，60K×1 已超 Ray object store
  → spill 填满小 `/tmp`（30G overlay 仅 7G free）；60K×2（formal 规模）需 ~190GB
  物化（即 120K 的 189GB 漏斗 RSS），远超 77GB object store + 磁盘。**Definitive：
  Daft built-in 无法 scale 到 60K×2 formal**。runner 的 `ray.init` 不传 `_temp_dir`/
  `object_store_memory`，故 env 重定向无效。保留 symlink gate log 作 cap 证据。
- **用户决策（option A）**：正式排名 = **Ray Data native vs project static @ 60K×2
  held-out**（都流式、≥60s、同合同）；**Daft built-in 单列**（5K 校准 177 img/s +
  60K 物化-cap 发现，本身是"物化 native baseline 不可扩展"的证据，支持课题执行结构论点）；
  direct ceiling 单独容量参照。
- **2-arm formal 启动**（commit `37dc8fd`，dir `ai_embed_formal_2arm_60kx2_20260803`）：
  Ray Data(batch64/cpu8/gpu2/source4) + project(cpu16/active32/gpu2/source4)，workload
  `coco_train2017_heldout`(58287 unique，与 calibration 60k 完全 disjoint)×2=116574 行/run，
  1 warmup + 3 formal，alternate interleave（R1 ray→proj, R2 proj→ray, R3 ray→proj），
  `--embedding-output-contract l2_normalized`。主指标 `verified_operator_jct_s`=
  `operator_e2e_s`(exactly_once filtered)。成功门槛：project 相对 Ray Data ≥5% + 3/3 同向。
- **heldout 数据加载**（`import_coco_images.py` 新增 `--offset`，commit `37dc8fd`）：
  zip sorted [60000:118287] = 58287 行，与 60k calibration disjoint（验证：heldout 不在
  任何跨 workload doc_id dup 中；那 956 个 dup 是 val∩60k 的 `int(stem)` 撞零头、不同图，
  formal 不用 val，不影响）。
- cron `8741a68c` 监控 formal 完成。


## 2026-08-03 AI_EMBED operator 正式对比结果（step 6 + step 8）——修正 headline

**核心修正**：step-6 的 "project 比 Ray Data 快 45.7%" **是虚高**——Ray Data 用了 5K 校准冻结的
cpu8（其 60K×2 弱配置）。matched-resource（step 8）抓出：Ray Data 在 60K×2 下 cpu8→cpu16 涨 56%
（905→1415 img/s），真实强配置是 cpu16。

**Table B matched-resource 2×2（60K×2 held-out，l2_normalized，3 formal/cell，CV≤3.2%）operator_jct**：
- @cpu8：Ray Data 128.75s vs project 112.24s → project **−12.8%**
- @cpu16：Ray Data 82.41s vs project 69.95s → project **−15.1%**
→ 同 CPU 下 project 两档都显著快（≥5%、方向一致）→ **执行结构收益真实，约 13–15%**（非纯资源）。

**Table A best-achievable（修正）**：project(cpu16) vs Ray Data(cpu16) = **project −15.1%**（公平的最强对最强）。
step-6 的 45.7% 只能作"Ray Data 低估配时的伪差距"旁证。

其它：project 更早出首条（22s vs 40–46s）、略更省能（matched img/J 略高）；两臂 **GPU 都饥饿**
（busy 6–10%，双卡均 claim，远未饱和——瓶颈在喂入侧）；两臂都 scale CPU（+56–60% cpu8→16）。

**Daft built-in**：物化执行，30K×1 即 OutOfDisk（max < 30K，远小于先前估计的 ~59K；每行 ~2.5MB 物化）。
按 option-A 单列。Daft-max 探针重测中（12K/20K/25K）。注意：Daft max（~20-25K）远小于 project 可靠
测量规模（~100K），3-arm 同规模一致性 run 在 fast arm 侧可能太短——待 Daft max 定后评估可行性。

报告 + summary + raw：`experiments/results/image_ai_embed_operator_formal_20260803/`。

## 2026-08-04 观测指标缺口代码闭合

- 文本 profiler schema 增加 vLLM TTFT/ITL histogram 分位、prefix cache、SLO token
  goodput、显式 token 单价成本、prompt padding waste、P99 SLO scale 和调度开销占比。
- shared-vLLM group schema 升至 v2，增加 actual token work、SLO token goodput、最终及
  overlapping-active-job service disparity；理论公平上界未证明，字段明确 unavailable。
- 代价估计增加 Q-error 多分位、Spearman、pick rate、selected/oracle runtime、regret、
  selected rank/surpassed plans；新增 formal repeat CI/CV/regression 后处理器。
- 新增 AI_EMBED 显式 relevance 的 Recall@K/MRR/nDCG evaluator；不以 checksum 代替
  任务质量，不把 diagnostic capture 时间混入性能排名。
- profiler schema 已变化，后续实验必须创建新结果目录；旧 CSV 继续作为历史证据，
  不允许混入新字段行。

## 2026-08-04 图像跨规模观测口径闭合

- 纠正“把文本 TTFT/ITL/token-goodput 直接用于 CLIP”的错误迁移：图像继续使用
  `first_output_s`，并新增 first-output/E2E、post-first-output 与显式跨规模语义；
  raw first output 只在同规模排名，比例只作物化/流式结构诊断。
- 图像 runner 升至 schema v12，增加 60s duration gate、J/1K-images、
  GPU-seconds/image、images/CPU-core-second 和 host disk/network bytes/image。这些是
  已观测总量的零额外开销派生量；60s 只证明时长，不自动证明吞吐平台。
- 新增历史 CSV 旁置增强器，使 12K Daft 三臂门禁与 60K×2 Ray/project formal 无需
  为纯派生字段重跑。正式证据改为“Daft 最大可完成规模门禁 + Ray/project 长稳态 formal
  + Daft 更大规模结构化失败”三层，不跨规模排名 absolute JCT/first output。
- schema-v12 manifest 与历史增强 CSV 的旁置 `*.metrics.json` 统一保存指标字典，逐项
  标明中文含义、单位、公式、直接/采样/派生来源、比较范围和误差边界，便于复核错误
  字段、采样失真和跨规模误读。
- 远端 `ai_embed_3arm_12k_1p3_20260804` 被两份相同 shell runner 同时写入同一
  `runs.csv`：出现重复 formal index、并发占用双卡和 manifest 覆盖风险；停止后共有
  15 行/12 formal，违反单写者与串行 Latin-square 合同，整目录排除性能结论、不入 Git。
  后续 formal 必须走 `run_image_clip_matrix.py` 的输出目录 lease，禁止手工并发启动 raw
  `run_image_clip_e2e.py`。
- main/服务器同步至 `ed2fde5`；去掉测试命令人为注入的 `PYTHONPATH=code` 后，远端
  全量 639 tests 与图像专项 49 tests 均通过。clean 2026-08-03 12K schema-v11 数据和
  两份 60K×2 formal raw 已旁置验证派生计算；仓库归档 clean raw gzip、紧凑 summary
  与 README，完整 derived CSV/metric definitions 可用分析脚本重建，不重复保存。

## 2026-08-04 算子代价估计 decision-context LOO 审计

- 修复原 context-LOO 只打印平均表、无法独立复算的问题：现保存源 CSV/代码 SHA256、
  逐 context 候选与 repeat、真实/预测均值、macro 分布和 pooled selection。
- 服务器独立 analysis venv（LightGBM 4.7.0）复算 283 行、17 contexts，其中 13 个
  multi-candidate：CE5 hybrid 的 MAE 7.69s、macro regret 2.14%、pooled regret
  0.31%、pick 9/13；但预注册行级 pairwise 0.705 < 0.75，结论仍为不晋级。
- repeat 聚合后的候选 pairwise 0.828、Top-K 0.821 是更贴近计划选择的新诊断口径，
  只能在下一轮新数据前预注册，不能事后替换旧晋级指标。
- 配置覆盖精算纠正为 38 个新 cells（现有 contexts 补齐 26 + 新增 3×4=12），按
  1 warmup+3 formal 为 152 runs；总耗时必须先用 4-cell pilot 实测，不再沿用
  “24 cells / 30–45 分钟”的无依据估计。

## 2026-08-04 vLLM CLIP pooling 能力门禁收敛

- 用进程组监管器在当前 2×4090 环境执行两次 1-image offline pooling gate：默认
  sampler 与 `VLLM_USE_FLASHINFER_SAMPLER=0` 均在 600 秒退出 124，且没有生成
  embedding `result.json`。
- 可证明 vLLM 0.25.1 能解析 `CLIPModel`/pooling 配置，但当前
  PyTorch 2.11.0+cu130/容器组合未通过可运行门禁；禁止继续在线、5K 和 60K 性能实验。
- 原始命令、完整日志、退出状态、环境/资产 SHA256 和七步报告归档至
  `feasibility/results/vllm_clip_pooling_gate_20260804/`。禁用 sampler 仍超时，只能排除
  单一 sampler 开关，不能把根因写成 FlashInfer JIT、权重加载或其它具体步骤。
- direct vLLM pooling 继续定位为服务 ceiling 候选，不是数据库 AI 算子系统 baseline；
  当前状态为 `blocked/unavailable`，Daft built-in 与 Ray Data native graph 不受影响。

## 2026-08-04 代价估计新数据采集前合同修复

- 在设计双 4090 的 4-cell pilot 时发现旧 15-feature vector 不含 active-work、
  per-endpoint K、actor concurrency、endpoint 数与 service quantum；改变这些候选时，
  CE3/CE4/CE5 可能看到相同输入，属于实验设计缺陷。
- pre-execution schema 扩为 23 项，并加入 per-GPU TFLOPS/显存容量；没有加入实际输出、
  E2E、vLLM、能耗或 MFU 等执行后信息。
- decision context 加入数据库版本、model backend、completion 协议/transport 及规范化
  GPU model/per-GPU memory；endpoint 数保留为候选动作。单/双同型号 GPU 的 context
  环境身份一致，但 candidate 不同；5070 与 4090 不会静默合并。
- 该修复只建立新数据的可比合同，不声称已有 15-feature 结论自动适用于 23-feature；
  必须在服务器独立 analysis venv 复算 LOO，再决定 pilot/formal。

## 2026-08-04 代价估计 formal-only 收口与双 4090 门禁

- 修复代价估计数据加载合同：只有 `status=ok, phase=formal` 的 profile 可进入模型、
  candidate repeat 聚合和 context-LOO；缺失 phase fail-closed。旧 283-row all-phase
  结果整体移至 `operator_cost_estimation_20260726/archive/allphases_pre_20260804/`。
- 修复 selection exact tie 依赖 CSV 首行的问题：固定选择字典序最小 candidate ID，
  同时保存 tie count/policy；修复 NumPy integer 导致证据 JSON 无法序列化的问题。
- 服务器 formal-only 23-feature context-LOO 复算为 204 行、17 contexts、13 个
  multi-candidate contexts。CE5：MAE 7.91s、candidate pairwise 0.800、macro/pooled/max
  regret 4.58%/0.62%/26.23%、pick 7/13、row pairwise 0.684；未过既有门槛，不晋级。
- 双 4090 四候选 cost-profile pilot v2 完成 8/8、0 incident；每个 formal 有 512 个
  unique completed requests、双 endpoint 均活跃、23 维候选向量可区分。完整 v1/v2 raw、
  SHA256、summary 和七步报告归档至 `operator_cost_profile_pilot_20260804/`；n=1 不排名。
- 预注册独立双 4090 formal：5 workloads × 2 rows × 2 output caps × 4 active-work，
  共 80 cells/320 runs。语义审计将 legacy `sharegpt_burstgpt` 替换为当前重建的
  `sharegpt_concentrated`；服务器实查五个 workload 均不少于 256 行。
- 按用户要求不由本地 agent 执行长实验。一次后台启动因服务器缺少 `/usr/bin/time`
  在首个 run 前退出，仅产生失败日志/快照，空目录已清理；运行文档移除该依赖并标记
  `PARKED`，后续由远端 agent 从 `main` 启动。
- 本地门禁：scenario runner 26 tests、planning cost 21 tests 通过；正式配置测试锁定
  80 unique scenarios、20 contexts、5 个当前 workload 和每 context 四个 credit 候选。
- 服务器 `49e1dd2` 完整依赖环境在清除外部 `RAY_ADDRESS` 干扰后通过 675/675 tests；
  config loader 展开 80 个唯一 scenario。当前 runtime env 缺 `RAY_ADDRESS`，正式运行前
  必须启动 Ray 并写入实际地址；模板保持 fail-closed，不以虚假默认值绕过。

## 2026-08-04 baseline 发布前语义审计

- 文本文档明确区分当前可执行的 64-row validity gate 与尚未实现统一 runner 的
  512-row calibration / 2,048-row held-out formal；后两份 JSON 仅为预注册合同，禁止
  远端手工循环单 cell 后冒充完整交错正式实验。
- native baseline 与 project formal 统一冻结为同一 2,048-row Chat manifest 合同；只有
  manifest SHA、rows、model、protocol、service config 和 output cap 全部一致才可并表。
  若单 run 不足 60 秒，双方共同冻结更大 manifest 并重新预注册，禁止只扩一个 arm。
- validity gate 模板不再混入当前 runner 明确阻塞的 project-profiler cells；模型从机器
  runtime env 的 `COMPLETION_MODEL` 展开，避免换模型后模板仍静默写死 7B 名称。
- 修复 `endpoint_predicted_work_skew_max` 仅写在 JSON、runner 却忽略配置的问题：现在
  布尔门禁必须为 true、失败/队列计数必须为数值 0、skew 阈值必须为 `[0,1)` JSON
  number，未知或弱化门禁 fail-closed，并把解析值写入 resolved config。
- 图像计划、证据注册表与部署入口统一把 vLLM CLIP pooling 标为当前环境 blocked 的
  direct-service ceiling 候选；两次 1-image offline 600s timeout、无 embedding，不能
  继续在线/5K/60K，也不能外推成“vLLM 普遍不支持 CLIP”。
- 双 4090 320-run 的 CE0–CE5 明确为算子代价估计方法 baseline，不替代 Daft/Ray
  Data/OceanBase 等系统原生 baseline。长实验仍保持暂缓。
- 未提交改动先同步到服务器临时 detached worktree 做完整依赖验证；JSON/py_compile
  门禁与最终 679/679 tests 全部通过。临时 worktree 随后删除，服务器主 worktree 保持
  `c8d1d92`、无实验 runner，不触碰历史未跟踪原始数据。

## 2026-08-05 DuckDB-ai 接入 baseline 框架 + 320-run 审计

- 审计 320-run formal：codex 已定位 8-04 双 runner 并发 + 空 `--ray-address` 各起 local Ray
  两类根因，修正真实落地且有测试——host-scope lease（`runner_lease.acquire_host_runner_lease`，
  artifact_root 级互斥，防不同输出目录 runner 并发）+ runner 的 `_validate_runtime_endpoints`
  拒绝 `--ray-address` 等关键 flag 存在但为空 + live prefix-caching 探测 fail-closed
  （`config_env.py` 仍只拒缺失变量，未改；空值门禁正确落在 runner 校验层）。cache-on gate
  已验证 0 个 local-Ray、共享 Ray、exactly-once、cache counter 一致；修正后 320-run 可开跑
  （按优先级，先完成 baseline 再跑）。
- DuckDB `ai` 社区扩展已安装到 driver 隔离 venv（`duckdb==1.5.4`；1.5.5 的 ai 扩展二进制未构建，
  `INSTALL ai FROM community` 会 404，已实测 v1.5.4 二进制 200）。扩展默认 provider 是 ollama，
  指向 vLLM 必须显式 `SET duckdb_ai_provider='openai_compatible'` + `CREATE SECRET (TYPE duckdb_ai,
  AI_PROVIDER 'openai_compatible', BASE_URL 'http://host/v1', API_KEY 'EMPTY')`，再 `SET duckdb_ai_model`。
- DuckDB-ai 接入 baseline 框架为新 adapter `duckdb_ai`（`code/src/baselines/text/products/duckdb_ai.py`）：
  set-oriented `SELECT ai_complete(prompt, max_tokens => N, temperature => 0.0)`，原生扩展调度
  （扩展自有 `duckdb_ai_max_concurrent_requests` 等），不注入项目 credit/router；provenance 标
  `database_product_native_baseline` / `duckdb_ai_community_extension`，`formal_baseline_eligible=True`，
  observability 与 OceanBase 同形（`query_barrier`/`unavailable`）。CLI 复用现有 `--endpoint-url`/
  `--model`/`--api-key`，不新增参数。新增 6 个单元测试，全绿；oceanbase/provenance 回归测试不受影响。
- 计时观测脚本 `code/scripts/baselines/time_duckdb_ai_baseline.py` 复用 `PeriodicSampler` +
  vLLM counter delta + nvidia-smi + `load_postgres_requests`，在投入 formal gate 前估测 DuckDB-ai
  cell 时长，供 codex 评判可行性。
- LOTUS 已在服务器 smoke 验证（`pip install lotus-ai` + LiteLLM `api_base` 指向 vLLM，
  `sem_map` 返回 DataFrame 输出在 `_map` 列），但政策上 LOTUS 是语义算子 SDK、不进 chat-track
  吞吐榜，需独立质量-成本-时间轨；本轮只提交 DuckDB-ai，LOTUS 留作后续独立轨。
- 写回方向：用户预告后续写回改用 Lance（替换/并行范围待定），当前 image runner 仍无写回代码、
  文本历史用 pgvector；细节在进入写回实现阶段再定。
- 证据精度勘误：fine vs coalesced **37.5×（推理执行阶段）/ 13.4×（端到端）是 2026-07-12 文本
  AI_EMBED 预研的数字，不是图像 CLIP 的**（图像动机是 GPU 利用率 1–4%、CPU 预处理瓶颈，另见
  `motivation/results/gpu/image_*`）。已纠正根 `AGENTS.md` §3、`motivation/AGENTS.md`、开题报告
  正文+飞书镜像第 71 行（"37.5× 的端到端差异"→"推理执行阶段差异 37.5×，端到端约 13.4×"）、
  报告/飞书第 270/279 行结论句、`opening/slides/build_ppt.py` 第 524 行 PPT 源（只改源、不重生成
  .pptx，保护手动调整）。来源 `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`。

## 2026-08-05 文本数据库原生 baseline fail-closed 修订

- 修复 DuckDB `ai_try_complete` 语义错误：旧 adapter 只读取 `response`，丢弃行级
  `error`，并把截断产生的 NULL response 标为 completed。新实现以 materialized CTE
  单次调用函数并同时保留 `{response,error}`；error 或无解释 NULL 均为 failed，旧 probe
  数字不得作为有效吞吐结果。
- 同条件 DuckDB 主轨显式冻结扩展自有控制：response cache=false、provider prompt-cache
  hints=false、retry=0、rate limit=0、timeout=120s；仅独立校准
  `duckdb_ai_max_concurrent_requests`。这里不关闭 vLLM prefix cache，主服务继续 cache-on。
- DuckDB cell 正式接入 64-row validity gate，并使用锁定 `duckdb==1.5.4` 的 cell 级 Python
  runtime。gate 现在在每个 cell 前等待服务空闲、使用 host-scope runner lease、双 shard
  最长 900s，并记录声明的 vLLM prefix-cache/max-seqs/max-batched-tokens 身份与可用的
  prefix query/hit counter。
- calibration/formal 文件仍是预注册合同而非统一 matrix runner；新增 DuckDB 独立校准和
  formal 冻结项不代表已完成正式实验。后续只允许 validity gate 通过后再启动 calibration。
- 服务器最小验证进一步收紧准入：4 行、1024-cap capability 请求本身 4/4 成功，记录
  DuckDB v1.5.4、`ai` v0.4.14、双 endpoint 与 prefix-cache counters；但 64 行 ShareGPT
  在 256-cap 和 1024-cap 均出现 `finish_reason=length` 行级错误。因此 DuckDB 从默认
  ShareGPT core gate/formal 主排名移至独立 bounded-output 产品轨；新配置为
  `dual_gpu_duckdb_ai_capability_gate.example.json`，只有同轨所有 comparator 共享同一
  bounded-output manifest 时才能做性能比较。
- 短门禁同时发现并修复 gate runner 的历史路径错误：命令曾指向不存在的
  `code/scripts/run_official_baseline.py`，现统一解析并验证实际
  `code/scripts/baselines/run_official_baseline.py`；新增文件存在性回归测试。
- 三次短门禁的最小证据已归档到
  `feasibility/results/duckdb_ai_semantic_gate_20260805/`：不保存原始 prompt/输出文本，
  只保存 resolved config、退出状态、服务计数、逐分片摘要与失败日志；七步报告明确
  capability 数据不进入正式性能排名。

## 2026-08-05 bounded-output 产品对比轨方法论（DuckDB-ai 兼容性三轨）

DuckDB `ai` 把 `finish_reason=length` 当行级 error，与 ShareGPT fixed-cap 主轨（接受截断）
语义不兼容。归档证据（`feasibility/results/duckdb_ai_semantic_gate_20260805/`）只证明：ShareGPT
cap=256 → 43/64 行失败、cap=1024 仍 1/64 失败、4 行 capability 4/4 成功；**句子计数 64 行零错误、
≤10 词摘要 ~9% 失败仅为服务器临时 screening，尚未归档，2048 行门禁未完成**。据此确定 DuckDB
对比走**独立 bounded-output 轨**，
原 ShareGPT 实验全部保留不动（仍讲项目内部策略/服务上限/动静态对比）。三轨结构：

1. **synthetic bounded-output capability track（句子计数）**：cap=16，**仅作能力/微基准**。
   1-token 输出只测 SQL/框架调用开销、prompt prefill、请求并发与双 endpoint 利用、每行物化成本；
   不测 decode/流式/长短输出差异/提交调度。即使项目更快，也只能声称"项目在短标量 LLM 调用链路
   上执行效率优于 DuckDB `ai`"，**不能外推为 AI_COMPLETE 普遍优于 DuckDB**。要求全量 2048 行
   零失败 + 确定性句子切分器 ground-truth exact-match accuracy + 整数正确性校验。
2. **正式产品对比轨（主）**：**SQuAD 短答案**（cap=64 固定，仍是文本生成、有公开 reference answer、
   可算 Exact Match + token-level F1、输出天然短、语义真实）为主 bounded AI_COMPLETE 对比；AG News/SST-2
   标签为可选扩展。完整协议见
   `experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md`：从"数据库 AI_COMPLETE 实际
   工作方式"出发（分类/摘要/问答都是 AI_COMPLETE workload，项目不实现独立分类引擎）、5 类共同指标
   （headline = correct rows/s、SLO-compliant correct rows/s、cost/correct row，非 raw rows/s）、
   **operator-only vs database-E2E 两个计时边界必须分开**（不可拿 DuckDB operator-only JCT 比项目含
   PG/Daft 读取的 E2E）、请求等价门禁（`ai_completion_request_json()` + vLLM prompt-token 校验）。
   句子计数在 ShareGPT 上 accuracy 仅 ~5%（对话歧义；来自未归档的 64 行 screening），只留 microbenchmark。
3. **中等输出轨（可选，补 AI_COMPLETE 生成特性）**：短输入一句话摘要/短答案抽取，输入长度按规则
   预筛（非按模型输出事后筛），cap 128/256，**必须全 manifest 零截断预检**；达不到零截断则诚实
   记录 DuckDB 产品语义限制，**不继续抬 cap 直到"碰巧通过"**。

三臂对照（每轨同 manifest）：DuckDB `ai` 原生 / bounded direct client / 项目冻结最佳静态
（项目最终优化方案确定后再补）。正式实验**增加 unique 行数**而非重复同批（避免 prefix cache
与重复 prompt 污染）。SQuAD 主路径不等待句子计数：①SQuAD 短答案导入与语义 gate →
②三臂校准 → ③operator-only 对照；database-E2E runner 完成后才能发布数据库系统正式排名。
句子计数仅为可并行补做的非阻塞 microbenchmark。新 importer
`code/scripts/data/import_bounded_output_workload.py`（`--template` 支持任意 bounded wrap）、
句子计数门禁脚本 `code/scripts/baselines/duckdb_ai_sentence_count_gate.py`。

### #5 请求等价门禁：`ai_completion_request_json()` capability probe（2026-08-05）

按 codex 二审"先查实际签名与返回结构、禁止猜测"执行服务器单请求探查，结论：
- DuckDB `ai` 扩展 `ai_completion_request_json(prompt, ...命名参数)` 是 scalar 函数，返回 `VARCHAR`
  （JSON 串），官方描述 **"Returns the completion request JSON without making a network call"**——
  纯本地构造、不实际请求，适合做确定性请求等价门禁。
- 实际返回体：`{"model":"qwen2.5-7b","messages":[{"role":"user","content":"..."}],`
  `"temperature":0,"max_tokens":16}`。
- **无隐藏 system prompt**（messages 仅 `{role:user, content:prompt}`）。
- **默认 temperature=0.1（非 0）**：不显式传时 DuckDB-ai 发 0.1；adapter 显式 `temperature => 0.0`
  才发 0。故请求等价门禁**必须校验 temperature 被显式设成 0.0**，否则与项目路径不一致。
- 门禁已在 `feasibility/results/request_equivalence_gate_20260805/` 完成并归档：canonical、
  DuckDB `ai_completion_request_json` 与项目生产 `build_completion_request_body` 逐字段相等；
  隔离单请求 vLLM prompt-token delta 为 37=37，`passed=True`。这只证明请求语义等价，
  不构成吞吐或 database-E2E 结果。

## 2026-08-05 Daft/Ray benchmark 来源分层与服务器选型门禁

- 在 baseline 唯一入口补齐 Daft-on-Ray / Ray Data 常见 AI workload：ImageNet/ResNet18
  image classification、PDF/MiniLM embedding、Common Voice/Whisper transcription、
  Hollywood2/YOLO detection、大图 embedding 与 LLM offline inference；明确官方
  803,580-row image workload 是 80,358 unique images 重复 10 次。
- 权威性分层修正：Daft 与 Ray 官方页面是两条 vendor-code evidence，公开排名方向可反转，
  不存在专门中立排名二者的第三方套件；MLPerf/TPCx-AI 只复用任务、质量和审计合同，改编
  runner 不得冒充正式 submission/compliant result。同机性能采用“双厂商原生代码 + 第三方
  质量合同 + database-E2E”三层证据。
- 核实 OceanBase 也公开采用 Daft on Ray：OceanBase AI Database/Lakebase 以共享对象存储、
  统一 catalog 和多模表为底座，由 Daft on Ray 执行多模态 inference；这与 OceanBase Database
  `AI_COMPLETE/AI_EMBED/AI_RERANK` SQL Function 是两条产品面。当前无公开可运行的 OceanBase
  vendor benchmark runner，故只作工业集成/capability evidence，不进入本机数字排名。
- 服务器选型改为 fail-closed：先做 256–1K capability、≥60s 饱和曲线、50K–80,358 unique
  规模/稳健性三次门禁，再根据 CPU/source、H2D、GPU、memory/spill、sink 的实际木桶选择机器；
  当前双 4090 审计只形成候选规格，不提前得出“需要更多 GPU”的结论。

## 2026-08-05 OceanBase AI 算子与 Daft-on-Ray 评价口径审计

- 将 OceanBase 拆为 Database SQL AI Function、Cloud AI Services/MaaS 与
  Lakebase/DataStudio Daft-on-Ray 三条证据线；前者提供真实 SQL 算子语义，中者公开
  24h success rate、TTFT、token output rate 和配额/限流，后者提供常驻 actor、
  micro-partition、CPU/GPU pipeline 的工业架构证据。
- 截至本次审计，未找到 OceanBase 公开的 AI Function 性能报告或 Lakebase-owned
  Daft-vs-Ray Data benchmark runner、数据/硬件合同、warm-up/repeats 和 raw logs；因此
  不从产品博客推导性能数字，Daft/Ray 排名仍采用双方官方代码同机复现。
- 明确 Sysbench/TPC-H 是数据库引擎 benchmark，VectorDBBench 是已有向量的检索侧
  benchmark，均不能替代 AI Function 或 Daft-on-Ray pipeline benchmark；VectorDBBench
  只可用于 AI_EMBED 写回后的 retrieval closure。
- OceanBase 官方 publications 已登记 PVLDB 2026 accepted 的 IMLane，但本次检索未找到公开正文；
  只列为 `pending-publication` watchlist，不杜撰其 workload、baseline、指标或结果。
- 同步更新 `research/evaluation_metrics_survey_20260731.md`、
  `research/existing_ai_operator_execution_chains.md` 与 baseline 唯一入口。

## 2026-08-05 IMLane vendor paper summary 补证与新颖性边界修正

- 用户提供 OceanBase 官方账号转载的论文介绍，补齐 IMLane 的系统、硬件、Q1–Q7 workload、
  internal ablation 和外部 baseline 摘要；纠正上一轮“除标题外实验字段均未知”的过度保守表述。
- IMLane 在 OceanBase Paetica 4.3 与 DuckDB 0.10.1 上验证 C++ DBEnd、ArrowLane shared
  memory、process-level Python execution、decoupled resource-aware coordinator 和 async
  batched scheduling；可插拔 Ray Executor，并以 IMBridge、pandas、SparkSQL、Ray.data 对照。
- 文章披露 OceanBase/DuckDB 平均汇总加速和相对 Ray.data 的倍数，但仍缺论文正文、代码、
  数据集名称、per-query raw results、重复/CV、质量和尾延迟，统一标为
  `vendor-authored paper summary`，不与本项目 2×4090 数字混排。
- 新颖性边界收紧：进程并行绕 GIL、Arrow 共享内存、decoupled resource-aware scheduling、
  async batching 与 scan/inference overlap 已有强相关工作；项目贡献必须聚焦 endpoint runtime
  state 感知、token/frame work credit、多 job fairness/SLO 和开放 database-E2E 消融。

## 2026-08-05 SQuAD EM/F1 统一评估器

- 新增 `code/src/observability/metrics/squad.py`，以纯函数实现 SQuAD v1.1 官方式
  normalize、Exact Match、token-F1 和多参考答案 max；DuckDB/direct/project 后续共用，
  禁止 comparator-specific 后处理。
- 聚合分母固定为完整 reference manifest；缺失/失败预测计 0 分并显式记录，额外 example ID
  与空 reference fail-closed。输出百分比明确使用 0–100，`squad_exact_match_rows` 留作
  runner 按 operator-only 或 database-E2E 边界计算 correct rows/s。
- 本提交只完成离线质量组件与单元测试，不调用 endpoint、不产生 256 行 capability 或正式
  性能结论；下一步由 gate 负责输出解析、错误/truncation/finish-reason 和 EM/F1 联合审计。

## 2026-08-05 direct_client 臂实现 + full single-shot：截断语义差异首次可比

- 实现 `code/src/baselines/text/products/direct_client.py`（`ff50c8b`）：httpx async + `asyncio.Semaphore(32)`
  固定并发、per-request `/v1/chat/completions`（共享 `build_completion_request_body`）、记录 finish_reason +
  completion_tokens + per-request latency。runner 加 arm dispatch（duckdb_ai | direct_client）、arm-aware identity
  （direct 无 duckdb 字段）、finish_reason 摘要、per_row_evidence 加 finish_reason/output_tokens 列、--limit smoke。
  96 测试通过（+9 direct_client）。`pyarrow` lazy-import 解决本地 import 问题。
- **256 行 smoke gate 通过**（8s）：finish_reason 全 stop（256/256）、0 error/NULL、readback matched。
- **full 10570 single-shot 通过**（99s）：**status=success，0 error/0 NULL**。finish_reason `{stop: 10569, length: 1}`
  ——1 行截断但 direct_client 返回 partial text（非 error），与 DuckDB-ai（把截断当 NULL → fail-closed FAILURE）
  **同一事件不同结论**。E2E wall 91.9s（vs DuckDB 93.9s），correct_rows/s 92.29（vs 90.42），EM 80.22%（vs 80.32%）。
  readback matched=True；state `single_run_valid=true` / `formal_run_gate_passed=false` / `pending_formal_repeat`。
- **两臂核心差异确认**：不在吞吐（两臂都 operator-dominated，scan/sink <1%），而在**截断的产品语义**
  （NULL vs partial text → FAILURE vs success）。这是三臂对比要 surfacing 的关键维度。
- 下一步：`project_static` 臂 → 三臂齐全 → 填全服务配置 → 三臂 `1w+3f` 正式排名。

## 2026-08-05 direct_client 审计收尾：文档登记 + 正文措辞订正

- 完成 direct_client 复核（DA）剩余两项：① 登记 `code/src/baselines/text/products/direct_client.py`、
  `code/tests/baselines/text/test_direct_client.py` 与证据目录 `feasibility/results/squad_database_e2e_direct_client_20260805/`
  进 PROJECT_INDEX / code/scripts/README / feasibility/results/README；② 按 codex 要求直接修 direct_client 证据
  README §2/§6/§7 正文措辞（不只 §8 审计节）。
- 订正的 4 处措辞：「同一种截断事件」→「同一 source row（`572700c8…`）在两次独立 full 中都触顶 cap=64」；
  「E2E wall direct 稍快（~2s，无扩展 barrier 开销）」→「约 2s（单次观察，不能归因为没有扩展 barrier 开销）」；
  「两臂 operator-dominated（模型调用 >99%）」→「adapter/operator-dominated（本 direct 臂 adapter 占 wall 99.26%，不能单独归因给模型调用）」；
  「共享 build_completion_request_body——与 DuckDB-ai 语义等价」→「direct 用该 builder；request_equivalence_gate 验证两路径关键请求字段语义等价，DuckDB 不复用该 builder」。
- 顺带修正同源陈旧口径：code/scripts/README 与 protocol spec（`bounded_output_duckdb_comparison_protocol_20260805.md`）
  的状态字段名 `capability_gate_status` → `single_run_valid`（H 系列已改 runner 输出，文档遗留），并把两处
  「direct_client/project_static 臂留 stub」更新为「direct_client 臂已实现，project_static 臂留 stub」。
  feasibility/results/README 第 14 条同步去掉「模型调用独占 99%」与耦合状态字段。
- 订正上一条 PROJECT_LOG 的措辞：「**同一事件不同结论**」应读作「**同一 source row 在两次独立 full 中触顶**，
  给出不同可靠性结论」（两次独立运行，非同一次事件）。结论不变：差异在截断的产品语义（NULL vs partial text），非吞吐。
- 纯文档提交，无代码改动、无重跑；机器原始 report.json/per_row_evidence.csv 全部保持不变。下一步：`project_static` 臂 → 三臂齐全。

## 2026-08-05 direct_client provenance 诚实化 + smoke 短读校验（DA3）

- codex 指出两个代码问题，本轮合并一个提交修复（无 rerun，纯代码+测试+文档）。
- **provenance 诚实化**：direct_client 上一版登记为 `comparison_role="direct_service_control"`
  —— 该值根本不在 `ComparisonRole` Literal 里（只有 service_ceiling / direct_client_control /
  framework_native_baseline / database_product_native_baseline），且 `custom_scheduling_code=False`
  撒了谎（`asyncio.Semaphore(32)` 就是项目自写调度代码）。订正为复用已有 `direct_client_control` 角色、
  `custom_scheduling_code=True`、`scheduler_owner="project_asyncio_semaphore_control"`。
  invariant 仍通过（`formal_baseline_eligible=False` 与 `custom_scheduling_code=True` 不触发 native+custom 冲突）。
- **runner 写 provenance 进 report**：runner 之前完全不调 `adapter_provenance()`，导致 report 缺 scheduler owner /
  implementation source 等审计字段。现 `_run` 在 arm 守卫后调 `adapter_provenance(args.arm)`，把 `summary_fields()`
  写进每次 `report.json` 的 `provenance` 键。归档 direct/DuckDB full 的 report.json 早于此修复、无该字段，需正式 rerun 落盘。
- **smoke 短读校验**：`_smoke_integrity` 之前只拒空集，不校验返回行数。要求 256、DB 只返回 100 行也会标
  `verified_smoke_limit_256`。订正为 `_smoke_integrity(rows, limit, importer_count)`，要求
  `len(rows) == min(limit, importer_count)`（`--limit` 超过 workload 规模时 clamp 到 importer_count）。
- **回归测试**：`test_baseline_provenance` 加 direct_client 诚实性断言（角色/custom_scheduling/scheduler_owner）；
  `test_squad_database_e2e_runner` 加 `SmokeIntegrityTests`（短读拒、精确通过、limit 超规模 clamp、空集拒）
  并在两个集成测试里断言 `report["provenance"]`（duckdb_ai=database_product_native_baseline/custom=False；
  direct_client=direct_client_control/custom=True）。
- 本地无 psycopg/duckdb，已 `py_compile` 四文件通过、纯 provenance 单测 4/4 通过；runner 全套需服务器跑。
- 同步订正文档里残留的旧 provenance 字串：PROJECT_INDEX 两行 + direct_client README §8。

## 2026-08-05 project_static 臂实现（shell-out profiler + 诚实 provenance）

- SQuAD bounded-output database-E2E runner 第三臂 `project_static` 实现完成（三臂齐全）。经 codex/用户裁决：
  **shell-out**（不是进程内重写）——`code/src/baselines/text/products/project_static.py` 子进程调用
  `postgres_ai_operator_profile.py` 跑冻结最佳静态 K，profiler 独占 scan+organize+model+sink；wrapper 合并
  request-trace(时间戳/status/finish_reason) + `document_completions` readback(output_text) → `BaselineRequestResult`。
  runner 在通用 scan 前分流 `_run_project_static`，避免重复 scan/写回。
- **诚实 provenance**：`ComparisonRole` 新增 `project_scheduled_method`（项目方法 under test，区别于 baseline/control）。
  `project_static` 登记 `custom_scheduling_code=True` / `formal_baseline_eligible=False` /
  `scheduler_owner=project_ray_static_k_and_active_work`；`formal_control_eligible=True` 按本仓库语义=可进正式比较矩阵
  （非 control arm，`project_scheduled_method` 角色是判别器）。补 3 条回归约束（项目方法必须 custom_scheduling=True、
  不得 formal_baseline_eligible=True、不得标为 direct_client_control 或任何 native baseline）。
- **计时段**：project_static 的 timing 来自 profiler `--output` CSV（`e2e_s`→`database_e2e_wall_s`、`db_fetch_s`→`scan_s`、
  `operator_wall_s`→`adapter_wall_s`、`writeback_s`→`sink_s`；`construct_s` 由 Arrow build + organizer 合成，与进程内臂
  结构不同）。headline `correct_rows_per_s` = EM 行 ÷ `e2e_s`，跨臂可比。
- **对抗式验证 workflow**（3 lens：profiler flag 名 / no-double-scan + fail-closed / output_text readback + doc_id join）：
  flag lens 全绿（33 flag 全部正确，20 必填齐全）；抓到 **1 个 major 静默正确性 bug**——`--force` 重跑同一 output_dir
  时 profiler 的 append-mode summary CSV 不清空，`read_summary_timing` 返回**陈旧**首行 formal-ok → 新结果混旧计时，
  `correct_rows_per_s` 静默错误，无门禁拦截。已修：wrapper 每次 invoke 前 `rmtree(work_dir)`，且 `read_summary_timing`
  改取**最后**一个 formal-ok 行（defense-in-depth）。
- 同时修了 workflow 报的 minor：`writeback_mode=none` 对 project_static 自毁（output_text 无来源）→ 前置拒绝；
  `sink_category` 经 config 透传（消除 wrapper 硬编码 'squad' 与 runner `--sink-category` 双源）；failed-row `started`
  回退到 `completed`（不拉低 min(started) 灌水 operator span）；`_fetch_scoring_ground_truth` 加 source_example_id
  唯一性 fail-closed；模块 docstring「does NOT scan」精确化为「不做 OPERATOR scan / 不 sink 自己」。
- 未修（minor，非回归）：conn 单 close 模式与进程内臂一致，runner-wide try/finally 连接管理留作单独 refactor；
  strict-attribution SystemExit vs sink-readback status=failure 的产物不对称是有意设计。
- 本地：6 文件 py_compile 通过；provenance 5/5 + project_static 纯函数 17/17 绿。runner 集成测试需服务器
  （psycopg/duckdb）。下一步：服务器 smoke（`--limit` 小规模）→ 再决定 full/三臂 1w+3f。

## 2026-08-06 project_static 7 项契约修复（PS6，codex 复核 13a8746 后）

- codex 复核 `13a8746` 暂不批准 256 smoke，列 7 项阻断。本轮按裁决全修，未跑 smoke、未跑 full、未改 cap。
- **#1 测试**：2 个 project_static 测试错误 `assertRaises(SystemExit)`，但 `main()` 捕获 BaseException 返回 1 +
  写 `failure_report.json`。改为断言返回码（rc==1）+ failure_report，不断言异常。
- **#2 effective K**：wrapper 只传 `--max-inflight=8`，未传 actor 拓扑，profiler 默认 2 actors×1 → 有效 K 被静默
  夹到 2。修复：`ProjectStaticConfig` 必填 `actor_workers_per_endpoint`/`ray_actor_max_concurrency`，且强制
  `actor_workers × concurrency >= max_inflight`（fail-closed），argv 显式传两者，report identity 记 `effective_k`/`declared_max_inflight`/拓扑。
- **#3 请求语义**：argv 未冻结 temperature/transport/prefix-caching。修复：显式传 `--completion-temperature 0`、
  `--completion-http-transport httpx_async`（默认 urllib 会破坏与 direct 臂的请求等价）、`--service-prefix-caching`。
  **请求 manifest guard 是 2-endpoint pinned-comparison 机制（`validate_profile_manifest_contract` 要求 endpoint_count>=2），
  单 endpoint 臂不用**；argv 单测锁定 transport/temperature/prefix-caching 并断言 `--request-manifest` 不出现。
- **#4 sink readback 循环自证**：wrapper 先从 `document_completions` 读 output_text，又把同一内容当 expected digest
  核对同表。修复：profiler 新增 opt-in `--completion-evidence-output`（`traces.write_completion_evidence`，从 in-process
  `operator_results` 展平 output_text，独立于 sink；zero behavior change when unset），wrapper 从该 evidence 文件取
  output_text，runner 的 `_sink_readback` 比较 evidence vs `document_completions`（两个独立来源）。wrapper 变无连接。
- **#5 workload integrity 高估**：原把 importer hash 直接填入 `workload_content_hash`，未散列 profiler 实际扫描内容。
  修复：runner 读取 workload（doc_id/text/source_example_id/answers，NOT operator scan），`_structured_content_hash`
  计算，full 用 `_validate_workload_integrity` fail-closed 核对 importer，smoke 记 subset hash；`workload_content_hash`
  填实际 hash；source_example_id 唯一性 fail-closed。
- **#6 计时不可比**：profiler `e2e_s` 含 metrics scrape + trace IO + finish_job、排除 actor-ready，比进程内臂 wall 更宽。
  撤回「IS comparable」声明：report timing note + docs 明确「NOT directly comparable」，改荐 `operator_wall_s`/`wrapper_wall_s`，
  跨臂绝对 wall 比较需未来统一边界。
- **#7 provenance 与执行不一致**：`scheduler_owner` 声称 `active_work` 但未传 `--max-active-work-per-endpoint`（默认 0=禁用）。
  诚实化：重命名为 `project_ray_static_k`（去掉 active_work）；argv 不冻结 active-work credit（正式 ranking 若需再冻结）。
- 本地：全 py_compile 通过；provenance 5/5 + project_static unit 18/18 + completion-evidence emit 3/3 绿；
  runner 集成测试（含 #1 rc==1 + workload-integrity fail-closed + 6 个 project_static 用例）需服务器跑 196/196。
- 下一步：服务器验 196/196 + argv 锁定后，才允许 256 smoke；仍不跑 full、不改 cap。

## 2026-08-06 manifest partition-policy（equal_rows + preexecution_token_work_balanced）+ policy-aware gate

- 多卡静态分片 baseline 的分片策略活在 manifest 的 endpoint_index 分配里（core gate 强制 endpoint∈{0,1}）。本次给现有 export 入口加可选分片策略，不新建 harness、不新建 SQuAD generator。
- **manifests.py**：新增 `assign_endpoint_equal_rows(requests, n, seed)`——用 `sha256(f"{seed}:{doc_id}")` 排序后 round-robin（**非** Python `hash()`，避免 hash-seed 随机），256/2 严格 128:128、奇数差≤1、输入顺序不变映射不变、同 seed 同结果；原 `assign_endpoint_shards` 重新标注意图为 `preexecution_token_work_balanced`（largest-work-first on `estimated_work = prompt+est_output`，est_output 是提交前估计如 fixed_cap，**不是 oracle**）。新增 `assign_endpoints(*, policy, seed)` 分发 + `partition_summary` 元数据（per-endpoint rows/prompt_tokens/est_output_work/total_work + row_diff + work_skew）。
- **cli.py**：`export-manifest` + `export-postgres-manifest` 加 `--partition-policy {equal_rows, preexecution_token_work_balanced}`（默认后者=旧行为，向后兼容）+ `--partition-seed`；走 `assign_endpoints`，返回 metadata 含 policy/seed/完整 partition_summary/sha256。
- **gate.py**：`validate_gate` 加 `partition_policy` + `max_endpoint_row_skew`，policy-aware 硬门禁——`equal_rows` 硬卡 endpoint 行数差（work-skew 只记录，因那是该 baseline 要暴露的问题）、`preexecution_token_work_balanced` 硬卡 work-skew（行数只记录）；无 policy 时保留旧 work-skew 硬门禁（向后兼容）。metrics 加 endpoint_row_counts/row_count_diff/partition_policy。
- **gate_runner.py**：`CoreGateConfig.partition_policy`（默认 None=向后兼容）+ load 校验 ∈ PARTITION_POLICIES + `_validate_cell` 传给 `validate_gate`。
- 测试 `test_partition_policy.py` 11 例（用户要求的 9 项全覆盖 + dispatch 路由/拒未知 policy）：256→128:128、奇数差≤1、输入顺序不变、同 seed 同结果、duplicate fail-closed、CLI 两 policy 路由 + 元数据、equal_rows 不因 work-skew 失败、work-balanced 在 skew>2% 失败。本地全绿；现有 gate/contracts/manifest/cli 测试无回归。
- 下一步：服务器 4/16 行 export capability 验证 → equal_rows 256 DuckDB-ai 2×1 smoke → preexec_balanced 256 smoke。`run_command_pair` 是 popen-pair 近并发（非真 barrier），smoke 报告标 `launch_mode=popen_pair_near_concurrent, start_barrier=false, formal=false`。

## 2026-08-06 修 code-review + nature-reviewer 指出的 3 个代码 bug（A/B/C）

- nature-reviewer 方向建议（3 reviewer + 综合）：当前 4 臂 256 单次结果**不能下结论**——三套计时边界不可比、项目用了错的 static 臂、SQuAD 均匀长度测不出调度价值、缺路由隔离对照。优先级：①统一 database-E2E 计时边界（fatal）②双 workload（SQuAD 质量 + 倾斜长度调度）③路由隔离表 T1（bounded_http×路由）与全栈表 T2 分开 ④规模 ≥2048 + 1w+3f ⑤修 bug A ⑥修 B/C。policy-aware gate 设计**确认正确**（不改为中性 gate），但要上报软量。
- **Bug A（validity-breaking）修**：gate 的 partition_policy 之前只信 config 声明，不与 manifest 实际策略交叉校验。修：manifests 新增 `write_manifest_metadata`/`read_manifest_metadata`/`manifest_metadata_path`——export 时写 `<manifest>.meta.json` sidecar 记录真实 partition_policy/seed/sha256/分布；gate_runner `_resolve_partition_policy` 读 sidecar 作 ground truth，config 声明 ≠ manifest sidecar 即 fail-closed；legacy manifest（无 sidecar）回退到 config 声明。`_validate_cell` 用解析后的 manifest policy 喂 `validate_gate`。
- **Bug B（compatibility）修**：`partition_summary` 保留 `endpoint_work` 作 `endpoint_total_estimated_work` 的别名（两个都 emit），不破坏读旧 key 的消费方。
- **Bug C（coverage）修**：`test_partition_policy` 加 7 个测试——assign→gate 集成（equal_rows/work_balanced 输出必过各自 policy gate）、sidecar roundtrip + endpoint_work 别名、legacy manifest 无 sidecar 回退、**config↔manifest policy 不一致 fail-closed**、manifest policy 优先于 config 声明。
- 本地：partition-policy 19/19（原 11 + 新 7 + 1 roundtrip）+ 现有 gate/cli/contracts/manifest 21 全过，无回归。py_compile 全过。
- 下一步（实验，待统一计时边界 + 双 workload 设计）：项目对比需 project_smart（非 static）+ 倾斜长度 workload + 规模 ≥2048 + 喂饱 GPU（feeding-saturation 门禁）+ 1w+3f。这些是 codex 的方向/计划领域。

## 2026-08-06 复审 P1 修复 + Phase 2 并发扫描 + lb_rr 规模爬坡（5 commit，全 push）

- **Phase 2 并发扫描**（`experiments/results/multicard_concurrency_sweep/phase2_2048_tb/`，2048，c=1..64，text-baselines venv）：duckdb 修复（DuckDB 1.5.4 + ai extension 0.4.14；首跑 base conda 1.5.5 全失败——ai extension 在 v1.5.4 路径）。完整并发曲线（group 口径）：bounded c32=**87393** 峰 / duckdb c2=79008（**c 无关 set-oriented**）/ project K32=77381；C_total=64 顺序 **bounded>project>duckdb**（旧 total/max_jct 口径误排 bounded>duckdb>project）。bounded c64（C_total=128）failed（vLLM 过载）。1 rep/cell diagnostic。
- **lb_rr 规模爬坡**（`experiments/results/multicard_lbrr_scale_ramp/`，64..10570，C_total=64，warmup_per_cell=false）：9/9 passed（含 8192/10570，**不像 duckdb_sharded 大尺度 cap-64 失败**）。ttft 口径峰值 **72934@2048**，4096+ 下降。256 门禁 0 error/未观察 max_tokens-truncation（finish_reason 空≠审计非 length）+ nginx 8000/8001 完美对称。**uncontrolled-cache + diagnostic_observation_pending_evidence_fix**（不引用跨四臂 cache-thrash，cache 控制不统一）。身份 comparison_role=gateway_system_diagnostic（主字段=系统角色；协议 §2.6 gateway 完整系统轨，**非** harness 预切）；component_comparison_role=database_product_native_baseline；scheduler_owner=duckdb_ai_extension+nginx_round_robin+vllm。
- **复审 P1 修复**（codex 第二轮只读复审 5 code-comment + 7 阻塞点）：
  - **A 聚合口径**：gate 臂用 gate.json `group_service_total_tokens/group_service_wall_s`（复审 #2，旧 total/max_jct 高估 + 误排）；lb_rr 用 ttft 两后端 `Σ(prompt+gen delta)`（复审 #5，旧 duckdb 输入 token 估算漏 generation/chat-template，~5% 低）。12 tests。
  - **B lb_rr warmup**：单端点 manifest（endpoint_count=1）两后端全 prompt（复审 #4，旧 ep1 空 bug——独立 vLLM cache 不共享）。
  - **C lb_rr backend-balance fail-closed gate**：`vllm_request_success_delta` skew>10% fail cell（复审 #6，旧仅事后人工）。
  - **D raw 归档**：Phase 2 + lb_rr + 256 gate 裁剪 raw（requests.csv 排除，239 files，6f6ef75）——复审 #1（Phase 2 只 README 不可复现）解决。
  - **#7 文档**：saturated p=**0.0284**（非 0.127 rich 误用）+ sample stdev(n-1) + project **显著高于 duckdb harness**（≠ 优于产品）；ADDENDUM **ps8_collapse 已提交**（非未提交，114 files）+ collapse group 口径 36560/24836 + c256 shard 级表述 + c64_f0 `ValueError ReadError`（非 vLLM 崩溃）+ 强结论软化；Phase 2/lb_rr README group/ttft 口径 + project prefix-hit K1 0.91（非全 0.96，路由独立）+ raw 已提交 + uncontrolled-cache + provenance。
- **commit 链**：6d1c263（codex 第一轮 7 点修复 + Phase 2）/ 1cd52be（identity sidecar，#1 根本）/ 565726e（lb_rr README）/ 6f6ef75（raw 归档）/ 0459d72（复审 P1 修复 + ramp_aggregate 重生成）。
- **仍待**：driver 测试覆盖（warmup/balance/atomic/Ray/config 仅 py_compile，复审工程缺口）+ PROJECT_INDEX 登记 + project same-manifest（复审 #3 根治，warmup 按 manifest 预热但 project 自有路由）+ ADDENDUM §C/诚实边界清理 + 60s 稳态重扫（#9）。**复审裁决**：先修聚合口径/LB warmup/provenance/gate/归档（done），再决定重跑。

## 2026-08-06 复审第四轮收尾：strict preflight 测试化 + query timing 字段改名 + identity 残留清零（codex 第四轮裁决）

codex 第四轮只读复审确认 a22cdf6 六项核心修复大体正确、27/27 测试通过、三份 aggregate 可由代码重建、identity 主字段迁移正确、query_barrier 的 request_e2e 已 null、example 均 vllm_config_strict=true、actual config 可追溯、nginx SHA 匹配、secret scan 0 violation；裁决补最后一轮再冻结 driver。本轮闭环：

- **strict preflight 测试化（codex #1）**：`_verify_vllm_config` 拆为 3 个 pure helper（`_cmdline_for_port` / `_flag_value_present` / `_prefix_cache_flag_enabled`）+ 1 个 pure verifier `_verify_endpoint_cmdlines`（无 /proc、无 subprocess，直接喂合成 cmdline 串测），`_verify_vllm_config` 只留进程发现。**关键修**：`_prefix_cache_flag_enabled` 改 token-based，`--enable-prefix-caching=false` 不再被子串误判为 ON（codex #8）。15 个新测试覆盖 8 case：无进程/缺 endpoint/单 endpoint ok/max-num 缺失/值不符/prefix-cache 缺失/两 endpoint 全匹配/non-strict 只告警/`=false` 非 ON。
- **query timing 字段改名（codex #2）**：query_barrier 的 JCT 原存在 `model_serving_wall_s`——但那是整条 SQL query barrier（operator wall），**非**纯模型 serving wall（换了个误名继续用）。改：`timing_granularity==query_barrier` → `query_jct_s=wall` + `model_serving_wall_s=None`；`request`（bounded）→ `model_serving_wall_s=wall` + `query_jct_s=None`；project 臂 → `model_serving_wall_s` + `query_jct_s=None` + `timing_granularity="request"`。`_MEAN_CV_METRICS` 加 `query_jct_s`。**新增 mixed-granularity fail-closed**：两 shard 粒度不一致 → status=failed，不输出可排名数（codex #3）。3 个 timing 测试 + `_gate_cell` fixture 加 `timing_granularity` 参数。
- **identity 残留清零（codex #4）**：grep 全仓 `system_comparison_role` 残留 → 改 PROJECT_INDEX:58、aggregator `_identity_role_fields` docstring、provenance.py 注释、helper-test 类 docstring、checklist §7.4/§8-step-2、PROJECT_LOG:4716。残留现仅剩历史"反例（已修）"描述与 `assertNotIn("system_comparison_role")` 断言（两者正确，保留）。
- **前向说明（不重生成历史 diagnostic）**：query timing 改名为前向——committed lbrr/concurrency raw 早于 `timing_granularity` 字段（raw summary 无该字段），重生成 aggregate 是 no-op（request 语义，`model_serving_wall_s` 保留=旧名的 query-barrier JCT）；scale_ramp raw 带该字段。**正式重跑**用新 driver 会为 lb_rr 写 `timing_granularity=query_barrier` → 正确产出 `query_jct_s`，取代历史 diagnostic。
- **测试**：42/42 通过（27→42：+15 strict +3 timing）。py_compile 全过。
- **下一步（冻结 driver，上服务器正式重跑）**：服务器 git pull → vLLM 重启带 `--max-num-seqs 256 --max-num-batched-tokens 8192 --enable-prefix-caching` → **strict preflight 在服务器真实通过为先决** → 冻结正式合同（vllm_config_strict=true + warmup_per_cell=true + reps=3=1w+3f + identity sidecar + group/ttft 口径）→ 重跑 lb_rr + duckdb 两臂 → 不边跑边改。

## 2026-08-06 规则固化：多路径 sweep 结果边界 + 跑完归档清单（写入 AGENTS.md）

用户指出本轮 lb_rr→bounded+duckdb sweep（缺 project_static）的报告边界与归档要求须从"会话内提醒/记忆"升级为项目规则，落进 AGENTS.md。已写入：

- **`experiments/AGENTS.md`** 新增 `## 结果边界与归档（多路径 scale/calibration sweep）`：① 缺臂如实命名——缺臂 sweep 称"N 条系统路径 scale/calibration sweep"，**非**"完整三臂正式排名"，只答容量曲线/稳定性/拐点差异，不答"项目方法优于 baseline"（须补齐同合同重跑）；② 指标必附代码公式+行号——后端 skew = `_backend_skew`=`abs(a-b)/max(a,b)`=(max-min)/max（`multicard_scale_ramp.py:366`），127:129=1.55%，**不**用 /sum(0.781%)；③ finish_reason 空→写"0 error/NULL、未观察到 max_tokens truncation error"，不写"已审计 0 length"；④ 跑完归档清单（vLLM 完整 cmdline+strict 输出 / revision·dtype·TP·gpu-mem / nginx SHA / 每 cell warmup-formal 身份+counters+skew / query JCT 与 request E2E 分列 / 失败 cell 完整 / sample CV(n-1)+全单次值）。
- **`experiments/plans/experiment_report_honesty_checklist.md`**：新增 §8（同源详细可勾选投影 + 归档清单）、勾选流程加第 9 步（边界+归档）、§7.1 "待补"→"已实现（复审第四轮 query_barrier→query_jct_s）"。
- 同步更新 Claude 记忆 `feedback-evidence-precision.md` ⑤：报指标必附代码精确公式+行号（skew 教训）。
- 正式 sweep 仍由 supervisor pid 482476 跑（lb_rr 36 cell → scale 72 cell，reps=3）；cron `2b470dd4`（:09/:34）带上述边界+归档清单自动监视，ALL DONE 时按 §8.3 归档并出三条路径对比简报。

## 2026-08-07 全网格扫掠 DESIGN 计划（含 project_static hang 根因/修复）——纯规划未执行

用户要求规划 4 臂（bounded/duckdb/lb_rr/project）× 规模 × 并发全网格（含先修 project_static 2-endpoint hang），但**先不执行**，待本轮 3-path scale-ramp 收尾取有效数据。起 workflow（5 agent：hang 根因 / 网格矩阵 / 校准合同 / 成本排序 / 综合，~12 min）产出 `experiments/plans/full_grid_sweep_plan.md`（已审、已注册）。

**project_static hang 根因（已亲自 Read 验证 crux 两点）**：
- F1 `code/src/scheduling/runtime/ray_adapter.py:273` `wait_until_ready` 的 `ray.get(ready_refs)` **无 timeout**（任一 actor ready 不返回 → 无限挂）；
- F2 `code/src/baselines/text/products/project_static.py:411` `subprocess.run(cmd, capture_output=True, text=True)` **无 timeout**（对照 lb_rr `multicard_scale_ramp.py:413` 有 `timeout=900` 能 fail-fast）；
- F3 触发层：stale `/tmp/ray/ray_current_cluster` 指针 → project `ray.init()` 卡死（gate 臂不经 Ray 故不受影响 = "project 挂、gate 过"症状）；`_ensure_ray_head`（commit 140eefd）已修触发层但未修 F1/F2 症状层。
- **提议修复三层**：层1 把 F1 改有界 `ray.wait(...timeout=READY_TIMEOUT_S)` 循环 + 超时输出 un-ready actor + `ray.cluster_resources()`；层2 给 F2 加 `timeout=`（与 lb_rr 对等）；层3（视 256 门）per-cell 重跑干净 Ray head。层1+层2 **无论触发为何都必须做**（结构性，非可选）。
- **256 go/no-go 门**：scale=256/K=32/2-endpoint/reps=3，须 3/3 passed + GPU util>0 + exactly-once + 两 backend 均用 + cell wall<60s；另 1-endpoint parity cell 隔离。门通过前 project_static 整臂 BLOCKED。

**网格推荐**：**十字切片**（并发@峰值 scale 2048 × 4 臂 + 规模@峰值并发 C_total=64 × 4 臂 ≈ 59 cells，~6× phase2），**非**完整 729-execution 矩形（~17–21h，交互项预期弱故 largely 冗余）。**最廉价下一步**（门过后）：现 ramp 已覆盖 81/108，仅缺 project_static K32×9×3 ≈ 27 cells (~0.32h) → 产出干净 4 臂峰值并发规模 ramp 回答核心对比。

**校准合同要点**：frozen = vLLM 三 flag（strict=true）+ warmup_per_cell=true + manifests SHA + model/protocol/cap + project 8×4=32 actor 拓扑；swept = scale 或 concurrency（归因必须拆成正交两条单变量 sweep，对角 cell 不可独立归因）；可比性 = 只有 tokens/s 与 rows/s 四臂同口径（timing_granularity 不兼容：bounded/project=request，duckdb/lb_rr=query_barrier），lb_rr 分轨（gateway），project 分轨（非 baseline）。

**边界（§6）**：网格能答 4 路径容量/稳定性/拐点 + feeding-saturation 门禁；**不能**答"项目方法优于 baseline"（须 §1 修复+256 门后）或 lb_rr 同柱排名或 per-row E2E 四臂横比。

**下一步由用户决定执行时机**：先修 §1 层1+2 → 256 门 → 补 project K32×9×3 → 十字切片。本轮 ramp 继续（cron 监视）。

## 2026-08-07 执行：project_static hang 修复落地 + 256 门通过 → project 臂 UNBLOCKED → #19 ramp 在跑

用户授权自主推进（环境确认干净后修 project hang → 复审 → 补未完成实验 → 全部扫完后启动 320-run 算子代价实验；全程不询问、严守规则、老实记录）。

- **环境确认干净**：服务器无 screen/无 ramp/supervisor 进程；两 vLLM(8000/8001) 运行且带 strict 三 flag（前轮正式 sweep 前重启的那对）；Ray head 活着；GPU 0% idle。
- **修复（commit `e49ac53`，4 文件 +231/-9）**：
  - 层1 `ray_adapter.py` `wait_until_ready`：无界 `ray.get(ready_refs)` → 有界 `ray.wait(num_returns=all, timeout=90s)`，超时 raise RuntimeError 报 un-ready actor 名 + `cluster_resources`/`available_resources`；`timeout_s=None` 保留 legacy 无界；空 pool guard。新模块常量 `ACTOR_READY_BARRIER_TIMEOUT_S=90` + `_describe_ray_resources` 诊断 helper。
  - 层2 `project_static.py` `run_project_static`：`subprocess.run(..., timeout=profiler_timeout_s)`，默认 900s（= lb_rr cell 对等，F3）；`TimeoutExpired` → 返回 failed `ProjectStaticRun`(exit_code=124, formal_row_found=False)，cell 记 failed 不挂。新 config 字段 `profiler_timeout_s` + `>0` 校验。
  - 测试 47 通过（+3 ray_adapter：timeout diagnostics / None 无界 / happy-path wait+get；+4 project_static：默认 900 / reject≤0 / timeout fail-closed / kwarg-到-subprocess 守卫）。440 scheduling+baselines 全过无回归。生产 caller `postgres_ai_operator_profile.py:2091`（位置 ray_module + 2-tuple 解包）签名兼容、且默认即获有界 barrier。
  - 复审（codex 严格度）：py_compile / 调用方核对 / 空 pool 守卫 / 诊断 helper getattr+try / monkeypatch try-finally 还原 / evidence count 校验保留。scan_git_secrets 0 violation。无 AI 署名（§10）。
- **256 go/no-go 门通过（commit `6d3f59b`，2-endpoint gate + 1-endpoint parity）**：scale=256/K=32/reps=3，§1.4 a–f 全过——3/3 passed exit0 K=32；formal status=ok；GPU max=100%；exactly-once 256 行 0 failed；两 endpoint 125/131·128/128·125/131（skew≤4.6%）；e2e<4s。1-endpoint parity 也 3/3 passed。**未触发**任一 timeout（90s barrier/900s subprocess）→ 触发层(140eefd)+症状层(e49ac53)共同闭合，task #119 关闭。**project_static 整臂 UNBLOCKED**（§0 非目标解除）。归档标为"修复验证门，非正式排名；256 行未饱和，不声称吞吐排序"。
- **#19 project K32×9×3 ramp 在跑**（screen `proj-ramp`，2-endpoint，全 9 scales，reps=3，warmup_per_cell=true，vllm_config_strict=true）：补全 4 臂峰值并发规模 ramp（与 multicard_scale_ramp_formal_20260806 bounded+duckdb + multicard_lbrr_scale_ramp_formal_20260806 拼成 4 路径对比）。2048 cell 兼作 256 门的回归项。~0.3–0.5h。
- **后续**：#20 并发前置（bounded httpx Limits / duckdb effective==c 核验 / manifest SHA 冻结）→ #21 十字切片（并发@2048×4 臂 + 规模@C64 project）→ #22 可选切片 C + duckdb 崩溃根因 → 全部扫完后启动 320-run 算子代价实验（`operator_cost_profile_dual4090_formal_20260804.md`，独立 screen，~3.5h+reserve，与 ramp 同 GPU 故串行）。

## 2026-08-07 #19 收尾（4 路径规模 ramp 闭环）+ 320-run 算子代价实验启动

- **#19 完成（commit `5026c76`）**：project_static K32×9×3 全 **27/27 passed**（含 8192/10570 全 3/3，对照 duckdb 10570 全 fail）。与前序 bounded/duckdb/lb_rr 拼成同冻结合同 4 路径峰值并发规模 ramp。**事实**：4 路径均 2048→4096 tok/s 腰斩后平台（bounded 88k→42k、duckdb 77k→42k、lb_rr 74k→39k、project 76k→42k）→ 瓶颈在 vLLM 服务端（大规模 prefix-hit 0.95→0.64 + TTFT 53→155ms 佐证 KV 饱和）；project ≈ bounded（同 offered-load ordering，method 非 baseline 分轨）；project 大规模稳健。60s 稳态门仅 10570（operator_wall 59s）满足；4096 拐点靠 4 臂一致 + 饱和规模交叉印证。归档 `experiments/results/multicard_proj_scale_ramp_formal_20260807/`（README 全指标 + 边界；raw per-request evidence 留服务器端）。
- **#20 复审降级为文档项**：经查 `async_http.py:167` `connection_capacity=c×endpoints` 显式设 httpx Limits（随 c 缩放，"默认 100" 顾虑失效；c=64 塌陷是 vLLM overload 非连接数）；`duckdb_ai.py:138` SQL 设 `max_concurrent_requests=c`（1..64 校验，c≤32 无 clamp）。故并发扫掠（C_total≤64）无需代码修复，#20 仅剩 manifest SHA 记录 + scope 文档。
- **决策：优先 320-run，#21 并发扫掠列为 next-step**。依据：用户明确强调"那个未完成的算子代价测试实验"为最终目标；#19 已闭环"未完成"的 project 臂 + 规模 ramp（scale 轴已答 4096 拐点）；并发扫掠（饱和/过载点）是可选扩展，非"未完成"补全，且会推迟 ~1.5h 的显式目标。
- **320-run 启动（screen `cost-formal-v2`，`dual_gpu_cost_profile_formal_v2_cache_on_20260807`）**：preflight 全过（RAY_ADDRESS 非空共享 / 无并发 runner lease / 两 endpoint 200 / config 80 scenarios / 5 workload 各 ≥256 行：short_prompt_lt50=512·long_prompt_ge150=325·concentrated/multiturn=2048·lmcache_agent=851 / prefix_caching 一致）。用 text-baselines python（#19 证实可跑 profiler）。**gate 10 关键验证**：已跑 12 run，0 个 stdout 含 "Started a local Ray instance"（每 run 连 172.17.0.3:6380 共享 Ray）——正是 v2 对 2026-08-04 首次无效 run（空 --ray-address → local Ray）的修复闭环。~2–3h（短 prompt 快，长 prompt/lmcache 慢）。完成后按 §4 门禁（320/320 + exactly-once + 两 endpoint + cache 一致 + hit∈[0,1]）验证 + 归档到 `experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/`。
- **320-run 完成 + 归档 + CE LOO 评估（commits `b3edd01` 数据 / `2473196` LOO）**：**320/320 有效、0 incident、§4 11 门禁 9 全过 + gate 6 由构造保证 + gate 8（CV>5% 补跑）未做**（见下 erratum）。CE 信号：20/20 context 0 退化（e2e spread 12–86%），最优 active-work context-dependent（98304 胜 11/20）。**CE0–CE6 context-LOO（plan §5）已跑**（harness `compare_cost_estimators_contextloo.py`，wrapper 把 `load_rows` 指向本 runs.csv；240 行/20 context/4 candidate）。**经 6-dim 对抗式复审修正后的口径**（见结果 README §9 erratum）：CE3_ridge/CE5_hybrid pooled regret 3.70%、**candidate pairwise 0.758（过 §6）**、median fold regret 0%，**但 macro-mean regret 6.42%（>5%）+ max 39.77%（>15%）FAIL → 无估计器过完整 promotion contract**；CE0/CE2 退化（49.7%）、CE1 12.1%、CE4 LightGBM skipped。CE3≈CE5。**⚠️ 复审发现先前 2 处 HIGH 口径误**（已修）：(a) 曾把 pooled 3.70% 当"过 5% 门槛"（实 §6 用 median/macro/max，macro/max 不过）；(b) 曾把行级 pairwise 当 contract blocker（实 §6 指 candidate pairwise 0.758 过；真 blocker 是 macro/max regret）。**另**：gate 8 的"全 short-fast 3-4s"归因不实（实测 median 8.70s、13 cell >15s，高 CV 集中 o64），补跑未做是已记录限制；max regret 39.77% 与高 CV fold 同源 → 补跑高 CV cell 是降 max regret 的首选。下一步：gate 8 补跑 + CE4（装 lightgbm）+ harness 改 `--data-csv` + contract 口径用 candidate pairwise。raw 66MB 留服务器，SHA256 `a4f9cd52...4e8f`。

## 2026-08-07 高 CV 6-rep 补跑闭环 + CE5 过 §6 contract（gate 8 转 ✅）

- **gate 8 补跑完成**（audit F10/F18）：63 高 CV scenario × 6 reps rerun，441/441 runs、0 error/0 failed/0 local-Ray。合并（63×6 + 17 低 CV×3 = 429 formal）→ `merged_runs_6rep_20260807.csv`。
- **CE LOO 重评**（修好的 harness `--data-csv` + lightgbm 已装 → CE4 首跑）：**CE5_hybrid 过完整 plan §6 promotion contract**（pooled regret 1.67%、median 0%、macro 2.90%、**max 14.72%**（<15%，marginal）、candidate pairwise 0.808）——**首个过 contract 的估计器**。CE3_ridge（max 22.71%）/ CE4_lightgbm（max 26.89%）仍 fail max；CE0/CE2 退化、CE1 17.8%。
- **假设证实**：6-rep tighten mean 把 CE5 max regret **39.77%→14.72%（−63%）**——高 CV 噪声确是先前 max regret 过高的根因。CE5 的残差校正在紧数据上现增益（max 14.72 vs CE3 22.71；3-rep 时 CE3≈CE5）。CE5 row MAE 3.98 > CE3 3.23 但 regret 更低 → accuracy≠selection。CE4 LightGBM 未增益（20 context 小数据）。
- **⚠️ 边际警告**：CE5 max 14.72% **贴 15% 线，marginal pass**，非稳健通过；换 split/更多 context 可能翻转。**不能声称** CE5 稳健过门 / 优于系统 baseline（本实验无系统 baseline）。
- 归档：`merged_runs_6rep_20260807.csv` + `ce_context_loo_rerun_20260807.json` 入 `experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/`，README §3(gate 8 ✅)/§5.3(6-rep 表含 CE4)/§6/§7/§9.1 全更新。条件性下一步：CE5 过 §6 满足 plan §8 TPC-H-derived 计划级 capability 前置（须计划级再验证 + max marginal 程度确认）。

## 2026-08-07 ramp-enhanced 重跑：补齐 §7.5D during-cell 观测（task #33 完成）

bounded/duckdb/lb_rr 用增强 instrumentation（`VllmGaugeSampler` 每 0.5s during-cell 轮询 vLLM gauges）重跑 scale ramp（bounded 27/27、duckdb 22/27 带 5 个预期大规模崩溃、lb_rr 27/27、0 error）。project 不需重跑（profiler 已采样 gauges，aggregator 现已 surface `vllm_running_prof_*`）。三目录用增强 aggregator re-aggregate。

**新观测（原 ramp 缺，本次补齐）**：
- **MFU 4 臂横比（首次）**：2048（未饱和）0.19–0.24（memory-bound），10570（饱和）0.62–0.68（compute-bound）。util% ≠ MFU 教科书特征。project 0.244 @ 2048 ≈ bounded 0.230。
- **during-cell running_max 52–62（Σ≈64 inflight 的 81–97%）**——**§7.5C(1) 喂饱门证据**（原 ramp 只有 before/after idle=0）。feeding-saturation 成立。
- **KV_max 0.033–0.067**（低，working set 只占 3–7%）；4096+ prefix-hit 塌是多样性驱逐非 KV 容量饱和。
- **能耗 8→21 J/1k-tok**（2048→10570）。
- **口径**：bounded/duckdb/lb_rr `vllm_running_*_total_*` = Σ 两 endpoint（VllmGaugeSampler）；project `vllm_running_prof_*` = profiler per-run，**caliber 不同分列不混比**。MFU = `[0,1]` 分数（`_compute_efficiency`，peak=165 TFLOPS bf16）。

归档：`multicard_scale_ramp_enhanced_20260807/` + `multicard_lbrr_scale_ramp_enhanced_20260807/`（ramp_run+aggregate）+ proj re-aggregate + 4-path ramp README §9（完整 §7.5D 4 臂表）。**正式 raw 归档**（服务器侧 git，只正式结果、控大小）待下一步。
## 2026-08-07 开题 framing 与 Claim Matrix 冻结

- 新增 `opening/claim_matrix.md`，冻结题目、AI Data Execution Layer 系统抽象、两项研究内容、共同使能组件、跨模态边界和四级 claim 状态。
- 开题前新增数据收敛为两组统一 database-E2E 文本三臂：SQuAD short-answer 均匀控制组和 ShareGPT controlled-skew 异质组。两组完成后停止增加开题 baseline。
- 明确现有 scale-ramp 的 request/query-barrier timing granularity 不一致，只用于 serving capacity 与 overload 证据，不能替代三臂统一 per-row database-E2E 排名。
- 同步 `AGENTS.md`、根 `README.md`、`PROJECT_OUTLINE.md`、`overview/current_direction_and_plan.md`、`experiments/plans/experiment_status_and_gaps.md`、`opening/README.md`、`opening/navigation.md` 和 `PROJECT_INDEX.md`。
- cost-model 最新口径更新为 429 formal；CE5 pooled/macro/max regret 为 1.67%/2.90%/14.72%，candidate pairwise 0.808，定性为 marginal pass。

## 2026-08-07 开题统一 database-E2E 合同与 runner

- 新增 `experiments/plans/opening_database_e2e_p0_20260807.md`，把 SQuAD 均匀控制组与 ShareGPT controlled-skew 的三臂、source、manifest、sink、计时、质量、资源和停止规则冻结为单一合同。
- 新增 `code/scripts/baselines/opening_database_e2e_matrix.py` 与 AutoDL 配置模板。runner 只暴露 direct static sharded、DuckDB AI static sharded、project frozen static 三臂，按确定性随机顺序执行 1 warmup + 3 formal。
- project profiler 增加 opt-in clean database-E2E timing boundary；默认历史计时语义不变。request-manifest guard 同时支持无未来信息的 `fixed_output_cap`。
