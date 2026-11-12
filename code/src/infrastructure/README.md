# 基础设施工具

本目录维护执行代码共用的文件、身份、资源和运行环境工具；它们不定义查询算法或成功标准。

## 私有产物

[private_artifacts.py](private_artifacts.py) 负责私有 JSON、文件摘要及有界调用方的证据复制。
`copy_private_tree(source, destination)` 要求源已停止写入，目标是 Git 外的新目录：

- 普通文件逐块复制，并核验 SHA-256 和字节数；记录源节点的 mode、device、inode、uid、gid、size、mtime。
- symlink 不跟随、不在目标重建，只在回执中保存原链接目标；相对、绝对和断链采用同一规则。
- socket、FIFO 和其他特殊节点不读取、不复制、不删除，只保存类型与元数据。
- 读取、写入、校验错误原样抛出，已复制部分保留；它不是原子文件系统快照。

调用方须将返回的节点清单保存到复制目录之外，和原始成功／失败记录一起留存：

```python
inventory = copy_private_tree(source, output / 'RUNS')
write_private_json(output / 'ARCHIVE-COPY.json', inventory)
```

归档回执只说明哪些字节已保存及哪些节点仅记录元数据，不证明 socket 已退出、资源清理成功或查询通过。
控制器原有异常处理、租约释放和成功／失败条件保持不变，不用归档成功覆盖旧失败。
冻结的旧控制器与原件不改写；调用方升级时绑定包含此函数的准确支持源码。

CPU 验证使用临时文件、相对／绝对／断链、socket 和 FIFO：

```sh
PYTHONPATH=code python -B -m unittest tests.baselines.common.test_private_artifacts -v
```
