# 历史 Native Q3：失败资格与独立复验（2026-10-20）

本目录仅迁入旧Q3证据分支的两份脱敏紧凑记录，保持原JSON字节；不导入records执行器或修改main默认。原分支两个独有提交为[失败观测13d63374](https://github.com/3444374/ai-operator-execution-optimization/commit/13d633745b295d610ff9e515e8e9328cba458ae9)与[独立复验452505e8](https://github.com/3444374/ai-operator-execution-optimization/commit/452505e80f7d13dcbfbc838fe7fae6f127790b62)。更早报告、源码与完整历史继续按[固定Q3报告](https://github.com/3444374/ai-operator-execution-optimization/blob/452505e80f7d13dcbfbc838fe7fae6f127790b62/experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#native-q3-runtime-20261020)追溯，不在此重复复制。

两次生产源码均为[`85133c6f4a688efe03f80d2c1cac0cd214406782`](https://github.com/3444374/ai-operator-execution-optimization/tree/85133c6f4a688efe03f80d2c1cac0cd214406782)，source lock均为`fade0a104e48053437bc622f74fe8aaeb0fc07e10a870466db868a368567c79f`。不同run_id及证据指纹表示两个独立运行；复验修复了私有终验先保存库存/资源再断言的流程，不表示换了生产模型执行代码。

## 两次结果及范围

| 运行 | 紧凑记录 | 资格和不能推导的结论 |
|---|---|---|
| q3-85133c6f-qualification-r1-lotus-native | [失败观测](native_q3_runtime_20261020.json) | Native查询completed、120调用成功，但campaign仍failed；unit-final报foreign work时缺失原始现场，身份/归属unknown，后验清理不能重建现场或把失败升级 |
| native120-revalidation-20261020-r1 | [独立复验](native_q3_revalidation_20261020.json) | 新单臂passed_native_only，原失败不改；实际token/template/decode与独立物理退出沿该次原审计解释，不认证五臂、正式比较或准确率 |

输入是historical_sf2000的2000源行中120个合格发生项；该CSV摘要为`ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf`，不据此声称官方整表身份。原LOTUS1.2.4/LiteLLM调度、batch4，Qwen2.5-7B-Instruct/vLLM0.25.1；模型、tokenizer、模板、generation和输入摘要按JSON固定，不沿用后来配置。

每次120次POST/回执/成功调用，无missing/failed/unknown调用；两次合计240次，不含更早13份账本。两组usage各为14566输入+480输出=15046 tokens。COUNT均7/gold14，TP7/FP0/TN106/FN7，relative_error0.5、绝对误差7；执行资格通过不等于质量达标。公开摘要没有逐行原始token数组，本次迁移只复算紧凑摘要内部一致性，不重做原始HTTP、decode、模板或模型核验。

含服务冷启动的query分别为77.43545482307673s与55.468263070099056s；各自评价/关闭、worker、supervisor和controller时间保留原范围，不相减推导调度加速。各只有一次观测，无resident latency、跨后端加速或P99结论。

JSON中的pending、其他四臂未启动、formal35/4200未启动、原480不作为重试授权等字段，均属于2026-10-20各次运行时点，保留历史叙事。它们不表示[后来八配置/Kalypso矩阵](../../../../PROJECT_OUTLINE.md#8-当前执行顺序)尚未完成，也不证明最新九配置、通用有限链或生产records已进入main。本批没有新模型、GPU、服务器或预算动作。

## 字节身份与复算

| 文件 | bytes | SHA-256 |
|---|---:|---|
| native_q3_runtime_20261020.json | 8503 | `a956abf37ccc7fdda38aa43f5dc4d1d30b30516f7a11db364640e69700838c6c` |
| native_q3_revalidation_20261020.json | 5046 | `92004a20f2047d49d42155b71df77ec435b687866630051df54300c53db7cde0` |

从仓库根目录可用Python标准库检查公开摘要；不需要加载模型：

```python
import hashlib, json
from pathlib import Path
p = Path("experiments/results/diagnostics/lotus_records_local_model_20261016")
records = []
for name, expected in [
    ("native_q3_runtime_20261020.json", "a956abf37ccc7fdda38aa43f5dc4d1d30b30516f7a11db364640e69700838c6c"),
    ("native_q3_revalidation_20261020.json", "92004a20f2047d49d42155b71df77ec435b687866630051df54300c53db7cde0"),
]:
    raw = (p / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == expected
    records.append(json.loads(raw))
a, b = records
assert a["source_commit"] == b["source_commit"]
assert a["execution"]["qualification_status"] == "failed"
assert b["qualification"]["new_attempt"] == "passed_native_only"
assert b["qualification"]["original_attempt"] == "failed_unchanged"
assert a["calls"]["succeeded"] + b["calls"]["succeeded"] == 240
for usage in [a["actual_token_observation"]["known_usage"], b["quality"]["usage"]]:
    assert usage["prompt_tokens"] + usage["completion_tokens"] == usage["total_tokens"] == 15046
for quality in [a["quality"], b["quality"]]:
    c = quality["classification"]
    assert c["true_positive"] + c["false_positive"] + c["true_negative"] + c["false_negative"] == c["evaluated_rows"] == 120
    assert c["true_positive"] + c["false_negative"] == 14
    assert c["true_positive"] + c["false_positive"] == 7
print("two historical runs, 240 calls; compact-summary checks passed")
```

原失败归档为`q3-85133c6f-r1-runtime-evidence-20261020.tar.gz`，976704 bytes，SHA-256 `29f022b63b993ea38ec10fe44367d34ef4616ac4b1fc2409ce65a27b3ae79d8c`。原件与后续审计按JSON的run_id、handoff/摘要指纹及获批artifact交接取得；未把服务器字段、凭据、本地路径、原始大日志或私有账本复制到Git。摘要迁移不替代完整原件，两个原运行的失败/unknown和来源保持。
