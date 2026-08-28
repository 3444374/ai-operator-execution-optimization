# Local Reference PDF Subset

更新日期：2026-08-28

本目录保存当前工作区实际存在、已通过 PDF 解析检查的论文。权威题录和用途见 `REFERENCE_INDEX.md`；泛读笔记见 `../reading_notes/`，精读笔记见 `../精读文献笔记/`。

## 当前状态

- 当前工作区可解析 PDF 实体：7 份。
- 当前 Top 15 实体：2/15（Galois、Abacus）；其余名称保留在历史题录索引中，使用前需要重新放回本目录并核验。
- 当前核心补充实体：Palimpzest、Sema、Kalypso；Sema 只按 arXiv v1 记录，不由文件名确认正式 venue，Kalypso 只按 arXiv v2 记录。
- 当前其他已精读正式论文实体：Parrot（OSDI 2024 proceedings PDF）、IMLane（PVLDB 2026 正式论文）。
- 旧索引中的较大下载数描述的是曾经登记的外部/历史子集，不符合当前工作区实体文件；当前状态只按可验证文件报告。

## 当前 Top 15 PDF

```text
abacus_pvldb2026.pdf
galois_sigmod2025.pdf
```

## 当前核心补充 PDF

```text
palimpzest_cidr2025.pdf
sema_vldb2026.pdf
kalypso_arxiv2026.pdf
```

## 当前其他已精读 PDF

```text
parrot_osdi2024.pdf
IMLane_PVLDB2026.pdf
```

## 使用规则

1. PDF 仅用于本地核验、精读和图表定位；正式引用以 DOI、会议/期刊官方题录为准。
2. arXiv 版本若已有正式发表，笔记同时记录“本地文件版本”和“正式题录”。
3. Companion、Demo、Tutorial、CIDR、MLSys、arXiv 不自动写成 CCF-A。
4. 新增 PDF 后同步更新本 README、`REFERENCE_INDEX.md`、对应泛读/精读笔记和 `PROJECT_INDEX.md`。
5. 不根据摘要直接改 Top 15；必须完成题录核验和全文精读。
