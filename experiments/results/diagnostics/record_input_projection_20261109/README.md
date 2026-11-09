# 临时输入列投影候选：放弃

复核纠正了原型的关键假设：现有真实 SemBench 文本接点已经提前投影。
[基底 `_operation`](https://github.com/3444374/ai-operator-execution-optimization/blob/f44f174a27e564734774c015fa0847334698a321/code/src/baselines/text/sembench_text.py#L85)
遍历 `frame[list(columns)]`，只把模型声明列写入 `RecordOccurrence.values`。
因此 Movie Q9 Map 的记录只有 `reviewText`，Medical Q10 Extract 只有 `symptoms`；
原关系的其它字段由 `TextOperation.frame` 保存，不进入这次临时表。
原型没有在这些实际路径中发现可省的宽列，不能把合成宽表当作研究对象。
这一判断只覆盖已核对的接点；服务器独立 worker 若使用别的 producer，须另核进入
`prepare_rows` 前的实际 values 列，不能从 CSV 总列数推断。

候选放弃，撤回方法选项、辅助函数、夹具、微测脚本及重复状态说明；不进入 main。
[原型准确提交 `fc31b37fb98f31358b9075b8d6089409d8f5cfa1`](https://github.com/3444374/ai-operator-execution-optimization/commit/fc31b37fb98f31358b9075b8d6089409d8f5cfa1)
保留其源代码及原报告，供主会话复核。当前分支不保留生产实现。
传统列投影、LOTUS 自身的列选择、Ray 流水、vLLM continuous batching／prefix cache、
Kalypso 的依赖与 KV 管理及 MLSys2025 行列重排均不是本候选的研究增量。

## 原始证据及保存

[容器微测](raw/cpu.json.gz)保留168个观测；[检查记录](raw/validation.json.gz)保留
30项通过、1项SDK跳过及首次两项夹具错误。旧微测仅计 Python 容器：
512行／128列中位数4.638→15.763 μs，不包含 pandas／LOTUS，不能支持净收益。
1／8／64／128列均是合成夹具，仅保留作历史对照，不替代上述实际单列输入。
本机现有 pandas 读取在15／45秒超时；未占用服务器、安装下载或运行模型。

两份 gzip 已逐字节解压核对，原JSON可无损恢复；原始格式也保留在准确提交中。
解压后 `cpu.json` 为38066 bytes，SHA-256
`be2e1f884be37ac0bf835ccc29c5b51002dfab403e0929ee6fa45e17447d4821`；
`validation.json` 为8472 bytes，SHA-256
`2027693e029cd60a934cf9783ce3e3e01977091c502fbdd0bec70ca5f911bca6`。

## 主会话的最小 SDK 复核命令

以下由主会话在现有SDK环境排期，本任务没有执行真实数据CPU或SDK验证。
检验工作树须准确位于原型提交；`SDK_PYTHON` 使用已有LOTUS／pandas解释器。
第一条只做原 formatter 消息差分，无模型调用：

```sh
set -e
test "$(git rev-parse HEAD)" = fc31b37fb98f31358b9075b8d6089409d8f5cfa1
"$SDK_PYTHON" -B -c 'import pandas, lotus'
PYTHONPATH=code "$SDK_PYTHON" -B -m unittest tests.semantic_methods.test_lotus_input_projection.NativeInputProjectionTests -v
```

完整准备复核使用现有 Movie Q9 与 Medical Q10 的运行配置、原数据目录和
已核对的 SemBench checkout。以下变量指向主会话已有文件，不下载或构造替代数据：
`MOVIE_Q9_CONFIG`、`MOVIE_DATA_DIR`、`MEDICAL_Q10_CONFIG`、`MEDICAL_DATA_DIR`、
`SEMBENCH_CHECKOUT`、`PREP_RESULT`。配置继续决定模型、生成参数和准备批大小。
计时覆盖每次 `prepare_rows` 的校验、pandas构造、LOTUS formatter、完整消息JSON及身份生成；
CSV、原接点投影与LM构造另记setup，不当作查询时间。宽表仅作额外对照。

```sh
test "$(git rev-parse HEAD)" = fc31b37fb98f31358b9075b8d6089409d8f5cfa1 || exit 1
PYTHONPATH=code "$SDK_PYTHON" -B - <<'PY'
import hashlib, json, os, time
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path
import lotus
from src.experiments.sembench_text_query import configuration, query_from_config
from src.infrastructure.redact import redact_text
from src.semantic_methods.lotus.record_method import LotusRecordMethod
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS

reports = []
with lotus.settings.context(enable_cache=False):
    for scenario, number, cfg_var, data_var in (
        ('movie', '9', 'MOVIE_Q9_CONFIG', 'MOVIE_DATA_DIR'),
        ('medical', '10', 'MEDICAL_Q10_CONFIG', 'MEDICAL_DATA_DIR')):
        started = time.perf_counter_ns()
        config, _, generation, resources = configuration(os.environ[cfg_var])
        assert (config['scenario'], str(config['query']).lower().removeprefix('q')) == (scenario, number)
        query = query_from_config(config, os.environ['SEMBENCH_CHECKOUT'], os.environ[data_var], generation)
        op = query.operation
        method = LotusRecordMethod(op.stage)
        setup_ns = time.perf_counter_ns() - started
        real = list(op.records)
        assert real and all(len(r.values) == 1 for r in real)
        wide = [replace(r, values=dict(r.values, **{'control_' + str(i): 'synthetic' for i in range(127)})) for r in real]
        for label, rows in (('actual_records', real), ('wide_control_only', wide)):
            chunks = [rows[i:i + resources.prepare_batch_rows] for i in range(0, len(rows), resources.prepare_batch_rows)]
            samples, reference = [], None
            for repeat in range(-2, 7):
                for project in ((False, True) if repeat % 2 == 0 else (True, False)):
                    ready, elapsed = [], 0
                    for chunk in chunks:
                        start = time.perf_counter_ns()
                        part = method.prepare_rows(chunk, query_id=query.query_id, project_input_columns=project)
                        elapsed += time.perf_counter_ns() - start
                        ready.extend({k: r[k] for k in (*IDENTITY_FIELDS, 'messages_json')} for r in part)
                    if reference is None:
                        reference = ready
                    assert ready == reference
                    if repeat >= 0:
                        samples.append(dict(repeat=repeat, projected=project, complete_prepare_ns=elapsed))
            reports.append(dict(scenario=scenario, query=number, input=label, setup_ns=setup_ns,
                rows=len(rows), record_columns=list(rows[0].values), model_columns=method.columns,
                preparation_batch_rows=resources.prepare_batch_rows, stage=op.stage.describe(),
                messages_sha256=hashlib.sha256(json.dumps(reference, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
                samples=samples))
Path(os.environ['PREP_RESULT']).write_text(redact_text(json.dumps(dict(model_calls=0,
    packages={n: version(n) for n in ('pandas', 'lotus-ai')}, reports=reports), ensure_ascii=False, indent=2)) + '\n')
PY
```

保留所有真实输入重复值。即使单列的容器路径有小幅差异，也不能恢复宽列假设，
或以GPU实验放大它；本候选不再扩展。
