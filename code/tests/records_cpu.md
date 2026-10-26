# Records CPU 检查的本地资产绑定

本页仅用于已有CPU/SDK与合成响应回归，不加载模型、不启动GPU或SSH。使用已配置的driver解释器；本页不安装依赖或下载数据。

`SEMBENCH_CHECKOUT`指向固定SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2`源码目录。
`SEMBENCH_Q3_MANIFEST`指向本机新生成的manifest。路径只由测试环境提供，不写入生产代码或公开配置。
固定历史CSV的SHA-256为`ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf`，
全2000行、Q3普通谓词命中120个发生项；这是historical_sf2000 profile，不声称官方整表身份。

旧封包manifest可能记录原机器绝对路径。保留旧封包与CSV字节，用现有prepare入口在新的仓库外目录重新绑定本地路径：

```sh
export CUDA_VISIBLE_DEVICES=''
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 PYTHONDONTWRITEBYTECODE=1
export LITELLM_LOCAL_MODEL_COST_MAP=True
export PYTHONPATH=code
export SEMBENCH_CHECKOUT="$PINNED_SEMBENCH_CHECKOUT"
"$DRIVER_PYTHON" -B code/scripts/experiments/run_lotus_records_movie.py prepare \
  --input-csv "$RECORDS_CPU_INPUT_CSV" \
  --input-sha256 ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf \
  --max-rows 2000 --role sembench_movie_subset --task movie-q3 \
  --output-dir "$NEW_CPU_INPUT_OUTPUT"
export SEMBENCH_Q3_MANIFEST="$NEW_CPU_INPUT_OUTPUT/manifest.json"
"$DRIVER_PYTHON" -B -m unittest \
  tests.baselines.text.test_lotus_native_local_model.NativeLocalTests.test_full_120_occurrence_native_http_profiles_match_original_q3 -v
```

调用者须提供真实CSV、固定源码及新目录变量。prepare验证CSV摘要，再由`MovieRecordSource`核对原CSV、records、references、行序和120个发生项；不伪造缺失资产，不修改旧manifest或参考标签。资产或固定SDK缺失时，既有可选用例明确skip；一旦提供无效路径/摘要，应失败，不能把失败改成skip。

该用例使用原LOTUS和SemBench方法/评价器，但响应来自`httpx.MockTransport`与`FixtureService`。
它对比并发4/8下的120发生项、消息、原parser、调用关联及合成usage；不认证模型质量、真实token、GPU性能或正式预算资格。
如需保存脱敏CPU回执，可显式设置`NATIVE_CPU_EVIDENCE_DIR`，调用者须先创建新的私有目录；省略该变量不写该份回执。

Daft session回归使用真实Daft CPU图和合成kernel。其本机Unix socket须由运行环境允许；沙箱拒绝bind是环境错误，应保留失败记录并在获准的CPU环境重跑，不能替换原生图或跳过断言取得通过。

普通合成fixture无需这份真实CSV。与本页相关的全部CPU通过仍不替代当前生产源码的独立source/input/token/model/env/ledger/owner绑定和GPU资格。
