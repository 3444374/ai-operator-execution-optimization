# SemLoom原提交历史保全

本普通分支仅保留原Git提交图和可检索来源，供复核与精确恢复。它不是可运行的集成版本；没有采用、合并实现或重新验证这些旧代码、实验及文档迁移。
当前tree只展示本说明与来源映射。每个原tip作为汇聚提交的parent，原代码、测试、证据和全部祖先仍可按原SHA查看。main、稳定性及原生canonical不因本分支移动。

31个来源在[sources.json](sources.json)中保存branch→原tip→原tree→恢复命令。被清理的旧名称不会改变原提交的内容或实验身份，未来恢复仍先核具名保全ref及祖先关系。

| 原分支 | 原tip | 历史组 |
|---|---|---|
| `codex/arrow-payload-batches` | [a5f59a61ea4d5d941f75534f9da10eae134f38df](https://github.com/3444374/ai-operator-execution-optimization/commit/a5f59a61ea4d5d941f75534f9da10eae134f38df) | review_blocked_pr_history |
| `codex/core-completion-progress-20261011` | [cb014152ab26a910cc4d4c889b8934dfc11e34a9](https://github.com/3444374/ai-operator-execution-optimization/commit/cb014152ab26a910cc4d4c889b8934dfc11e34a9) | core_patch_history |
| `codex/core-offer-cost-20261011` | [a87242f1223a617b46c83a857372e51f68a99d6b](https://github.com/3444374/ai-operator-execution-optimization/commit/a87242f1223a617b46c83a857372e51f68a99d6b) | core_patch_history |
| `codex/core-static-validation-reuse-20261011` | [299aff6fbe30dc7ecf982ee78a6ed31031fb590c](https://github.com/3444374/ai-operator-execution-optimization/commit/299aff6fbe30dc7ecf982ee78a6ed31031fb590c) | core_patch_history |
| `codex/duckdb-pipeline-diagnosis-20261011` | [78a3c8e5ad2fbd21c2951823b718421b94f77f15](https://github.com/3444374/ai-operator-execution-optimization/commit/78a3c8e5ad2fbd21c2951823b718421b94f77f15) | native_adapter_history |
| `codex/duckdb-query-job-20261011` | [bc9d9dfef850ce2c819e2766967ae77fbbb12a56](https://github.com/3444374/ai-operator-execution-optimization/commit/bc9d9dfef850ce2c819e2766967ae77fbbb12a56) | native_adapter_history |
| `codex/lotus-adapter-overhead-20261009` | [f087eecadb8d00d0dd86cd2f9e05e1f868287055](https://github.com/3444374/ai-operator-execution-optimization/commit/f087eecadb8d00d0dd86cd2f9e05e1f868287055) | native_adapter_history |
| `codex/lotus-control-preparation-20261011` | [1e94f6d8f937a56445d7c149255b01e10fcde77a](https://github.com/3444374/ai-operator-execution-optimization/commit/1e94f6d8f937a56445d7c149255b01e10fcde77a) | native_adapter_history |
| `codex/native-performance-repair-20261009` | [bb81fca7a88760a81930c0daa6b24307c4231c4f](https://github.com/3444374/ai-operator-execution-optimization/commit/bb81fca7a88760a81930c0daa6b24307c4231c4f) | native_adapter_history |
| `codex/repo-reorg` | [6cb13a44c2c964f7fc76f573050625c692c2acdd](https://github.com/3444374/ai-operator-execution-optimization/commit/6cb13a44c2c964f7fc76f573050625c692c2acdd) | document_history |
| `codex/repo-slimming` | [e6a22ad4490c021c5545dc0fb08cb88eed9e6179](https://github.com/3444374/ai-operator-execution-optimization/commit/e6a22ad4490c021c5545dc0fb08cb88eed9e6179) | document_history |
| `codex/saor-feeding-gap-diagnostic-20260817` | [f1844c0f24913ad9fb8eb61da1f121570357eb3f](https://github.com/3444374/ai-operator-execution-optimization/commit/f1844c0f24913ad9fb8eb61da1f121570357eb3f) | experiment_history |
| `codex/saor-feeding-gap-rerun-20260818` | [034e2b6a67f301c5c700ecd01779993055b7b5b7](https://github.com/3444374/ai-operator-execution-optimization/commit/034e2b6a67f301c5c700ecd01779993055b7b5b7) | experiment_history |
| `codex/saor-feeding-gap-transport-evidence-20260818` | [2e4c27239f871af28d8c1d7800c438fabccf2707](https://github.com/3444374/ai-operator-execution-optimization/commit/2e4c27239f871af28d8c1d7800c438fabccf2707) | experiment_history |
| `codex/sema-deferred-observation-20261011` | [5853b0810b913abea4da8f0db0dfcc3ed15136c8](https://github.com/3444374/ai-operator-execution-optimization/commit/5853b0810b913abea4da8f0db0dfcc3ed15136c8) | native_adapter_history |
| `codex/sema-event-pump-20261011` | [b3eb0a72fc56669c3191d823054a7f912656dc7d](https://github.com/3444374/ai-operator-execution-optimization/commit/b3eb0a72fc56669c3191d823054a7f912656dc7d) | native_adapter_history |
| `codex/sema-imlane-adapter-20261012` | [650f65c8fe0f834009bc714257ffde0a2ec3716b](https://github.com/3444374/ai-operator-execution-optimization/commit/650f65c8fe0f834009bc714257ffde0a2ec3716b) | native_adapter_history |
| `codex/sema-native-library-checks-20261012` | [7f18911e80386795714b7b624f74213531d998e0](https://github.com/3444374/ai-operator-execution-optimization/commit/7f18911e80386795714b7b624f74213531d998e0) | native_adapter_history |
| `codex/sema-performance-repair-20261009` | [ded6441ea96b8043e7105ce4f0f72aa1420e8b86](https://github.com/3444374/ai-operator-execution-optimization/commit/ded6441ea96b8043e7105ce4f0f72aa1420e8b86) | native_adapter_history |
| `codex/sema-resident-executor-20261011` | [069c7156983c80892227fe9a84fc14a91b72ca9e](https://github.com/3444374/ai-operator-execution-optimization/commit/069c7156983c80892227fe9a84fc14a91b72ca9e) | native_adapter_history |
| `codex/semantic-duckdb-adapter` | [53c8729943eb903db63c72d28ff732bc92494bf1](https://github.com/3444374/ai-operator-execution-optimization/commit/53c8729943eb903db63c72d28ff732bc92494bf1) | native_adapter_history |
| `codex/semantic-duckdb-performance-20261009` | [2de2fc230c810787bd2f4acfe5663453e43871de](https://github.com/3444374/ai-operator-execution-optimization/commit/2de2fc230c810787bd2f4acfe5663453e43871de) | native_adapter_history |
| `codex/semantic-evidence-compact-20261010` | [9a3ae1ef84c91de923530c7b4e7d834a7b1c4964](https://github.com/3444374/ai-operator-execution-optimization/commit/9a3ae1ef84c91de923530c7b4e7d834a7b1c4964) | document_history |
| `codex/semantic-lotus-adapter-20261009` | [0085dfb55525923a477c93115d533afb5553db15](https://github.com/3444374/ai-operator-execution-optimization/commit/0085dfb55525923a477c93115d533afb5553db15) | native_adapter_history |
| `codex/semantic-native-execution-20261009` | [7eec3f85ef2767c9a25b98a825b4701d050f2641](https://github.com/3444374/ai-operator-execution-optimization/commit/7eec3f85ef2767c9a25b98a825b4701d050f2641) | native_adapter_history |
| `codex/semantic-sema-adapter-20261009` | [3ae8a0b893517227eac470624e2fa5c9b3132932](https://github.com/3444374/ai-operator-execution-optimization/commit/3ae8a0b893517227eac470624e2fa5c9b3132932) | native_adapter_history |
| `codex/text-map-hot-path-timing` | [cf3a9798fcfdf62b93368d2bd5788f699847c4fa](https://github.com/3444374/ai-operator-execution-optimization/commit/cf3a9798fcfdf62b93368d2bd5788f699847c4fa) | review_blocked_pr_history |
| `codex/text-map-matched-comparison` | [b855f4cc411eb9b5399e08e41175968f702fc736](https://github.com/3444374/ai-operator-execution-optimization/commit/b855f4cc411eb9b5399e08e41175968f702fc736) | review_blocked_pr_history |
| `codex/text-map-startup-and-first-row` | [96e861a0f4b38f343eff67fc0b599b84a2ed3ae3](https://github.com/3444374/ai-operator-execution-optimization/commit/96e861a0f4b38f343eff67fc0b599b84a2ed3ae3) | review_blocked_pr_history |
| `codex/text-map-stream-decomposition` | [0e220ce58b88b72f21a1ff1b623e70754dad6238](https://github.com/3444374/ai-operator-execution-optimization/commit/0e220ce58b88b72f21a1ff1b623e70754dad6238) | review_blocked_pr_history |
| `codex/threaded-map-accounting` | [6414304e7ad7f2c7a22e1edfc4bb5756abecd708](https://github.com/3444374/ai-operator-execution-optimization/commit/6414304e7ad7f2c7a22e1edfc4bb5756abecd708) | review_blocked_pr_history |

恢复一个旧名称示例（需明确授权恢复，不能覆盖现有不同tip）：

```sh
git fetch --no-tags origin refs/heads/codex/semloom-history-preservation
git merge-base --is-ancestor <original-tip> FETCH_HEAD
git push origin <original-tip>:refs/heads/<original-branch>
```

source包含关系只证明历史可取得；是否采用某项Core优化、原生接入、目录精简或实验方法须另行内容审阅和适用验证。原始失败与负结果不会改记为当前成果。
