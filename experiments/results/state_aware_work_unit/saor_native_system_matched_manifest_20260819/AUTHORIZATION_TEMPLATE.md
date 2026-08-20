# SAOR native-system matched formal 授权 artifact 模板（2026-08-19）

> **性质**：`run_saor_native_system_matched.py` 非 rehearsal 运行所要求的
> `--formal-authorization` JSON 的签发模板。**本文件本身不是授权**——字段值在
> 合同分支 merge 进 main 之后、由签发人（项目开发者）按下述步骤计算并独立落盘，
> 仓库不携带有效授权 artifact（runbook 规定）。

## 字段 schema（runner 逐字段精确匹配，多一个键都拒绝）

```json
{
  "schema_version": 1,
  "status": "authorized",
  "scope": "saor_native_system_matched_formal",
  "formal_authorized": true,
  "repository_commit": "<merged main HEAD>",
  "config_sha256": "<sha256 of deploy/autodl/saor_native_system_matched.example.json at merged main>",
  "resolved_config_sha256": "<sha256 of canonical resolved_matched_system_identity JSON>",
  "manifest_sha256": "72dc51b7a63ce8a35c410d3050eb9b110cb08a68a9e45928770be428058bf56f",
  "mfu_contract": {
    "status": "unavailable",
    "gpu_peak_tflops_per_gpu": 165.0,
    "precision": "bf16_dense_fp32_accumulate",
    "reason": "cross-system FLOP numerator is not uniformly available"
  },
  "job_manifests": [
    {"job_id": "job0", "rows": 512, "sha256": "8e532819f045f85ff4e92b61c688e2d50f180d438dc577eed79c57e19cfce9c1"},
    {"job_id": "job1", "rows": 512, "sha256": "85b3f90cdc4045ae9fdb48f1d30772649c25d86375b72bab0fbd903f2a01c971"}
  ]
}
```

`resolved_config_sha256` 的计算以服务器 env 展开后的 resolved identity 为准；`mfu_contract`
还会作为授权 artifact 的直接字段逐项匹配，因此 peak/precision 变化即使遗漏其他审计也会 fail closed。
（`DATABASE_URL` 在 canonical payload 中替换为其 SHA），生成命令：

```bash
PYTHONPATH=code "$DRIVER_PYTHON" - <<'PY'
import hashlib, json
from pathlib import Path
from src.experiments.saor.native_system_matched import (
    load_matched_system_config, resolved_matched_system_identity, sha256_payload,
)
config = load_matched_system_config(Path("deploy/autodl/saor_native_system_matched.example.json"))
print(sha256_payload(resolved_matched_system_identity(config)))
PY
```

## 签发流程（merge 不构成 formal 授权）

1. 合同分支 review 后 merge 只冻结实现，不签发实验；
2. 在服务器上同步已审核 commit，source `ai-operator-runtime.env` +
   `deploy/autodl/saor_native_system_matched.env.example`（复制到仓库外）；
3. 只运行小规模 correctness/rehearsal，封存 root、validation 与 archive SHA，完成独立审核；
4. 审核通过后，项目开发者另行作出 formal 决定；按上式计算 SHA 并把本模板保存为仓库外文件（如
   `/root/autodl-tmp/runtime/saor-native-matched-authorization.json`）；
5. 以 `--formal-authorization <该文件>` 启动矩阵。runner 在创建任何 output root
   之前逐字段精确校验，任何漂移（含审核后改配置）直接拒绝。

仓库中的模板、分支 merge、rehearsal 通过和配置中的布尔字段均不能替代第 4 步的独立授权 artifact。

## 已冻结锚点（本分支确定）

- combined manifest（Git 外）：SHA `72dc51b7a63ce8a35c410d3050eb9b110cb08a68a9e45928770be428058bf56f`
- Job0/Job1：各 512 行，SHA 分别为 `8e532819…ce9c1`、`85b3f90c…c971`
- MFU：`status=unavailable`，但 4090 dense peak 与 precision 必须以最终 env 解析值直接写入授权 artifact。
- config/resolved-config SHA：只在最终审核 commit 与服务器 runtime env 均冻结后计算，不在模板中预填。
