# impl-01: 上游收口 — spec §⑦ 逐字落笔

Type: task
Status: ready-for-agent
Blocked by: None (can start immediately)

**What to build:** 把 spec 第 ⑦ 节的逐字修订清单写进上游文档，让施工会话对「SRS 说什么、gdd 说什么、HLD 说什么」不再有歧义。本票不建任何代码，只改文档——但**改完即冻结**：此后施工会话按 spec 施工，不再回头判断上游文档的原话。

覆盖范围：

- **SRS 十一条**（意图类已签字）：`REQ-F-GEN-07` 双迟到注入器、`REQ-F-GEN-07` 配套的 §3.3 规则①相对下界、`REQ-NF-CAP-01`/`REQ-F-ORCH-02` 容量双口径、`REQ-F-ORCH-01` 一条命令= `make init`、`REQ-NF-CAP-03`/`04` 冷启判据、`REQ-F-VERIFY-01` 第三项 P1 SKIP/`REQ-F-VERIFY-02` 幂等不进每次 verify、`REQ-F-BI-02` 刷新判据、`REQ-F-LAKE-06` catalog/database 层级（附录 A·M，2026-10-21 签）
- **gdd 十六条**：`spec.md` 空指针四条、漏斗修正语义、UV 对账、三态载体、方格地图改横条、`valid_from_ms`/`valid_to_ms` 拉链改名、`database_uuid` 硬约束、zip 定位，以及六处官方事实订正（`dynamic_partition_enable` 作废、Kafka retention 按配置层级重算、`partial_columns` 非表属性、Group Window Aggregation deprecated、DataStream-only 的 allow-lateness、`gdd D16` 错引）
- **HLD 九条**：`spec.md` 指针两条、改上游的自述订正、体积数字第二真相源（10.25/1.43 → 10.5/1.18）、`schema_version` 唯一所有者、Paimon 部署面（warehouse 卷 / jar COPY / `sql/replay_*.sql` 布局 / 迟到计数表名入冻结面 / `artifacts/verify/`）、冷启口径偏离登记、Paimon 主键表默认 MOR 的空间放大前提复核（附录 A·N，2026-10-21 签）
- **§7.5 #37**：`research-notes.md` 两处非官方清单（`$files`/`$snapshots` 列名、`partial_columns` 作为表属性）按事实类直接订正

同时把两张已批冻结项变更写成文档事实：FC-1（Makefile 新增 `lint`/`typecheck`）与 FC-2（`dwd_late_event_count` 入 Paimon 表名冻结面清单）。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

## Acceptance criteria

- [ ] spec §⑦ 每一条（#1–#39，含 2026-10-21 新签的 #38/#39）在对应上游文件的对应位置都能逐字找到，**无遗漏、无「大概这么改」**
- [ ] SRS §3.3 新增「schema_version 兼容规则」小节，承载四条铁律的字段语义；HLD §2.5 横切面① 指向 `canonical-truth.md` §3.1 作为唯一所有者（不复制全文，防第二真相源）
- [ ] 每个数字都带 `官方给出` 或 `设计值` 标注，**没有任何设计值被写成官方保证**
- [ ] `HLD.md` 与 `gdd.md` 中「不修改 SRS.md/FSR.md」的自述已订正（这两文件从未获得该背书）
- [ ] 修订后全仓不存在第二真相源：HLD.md:294 的体积数字与 §4.2 一致（10.5 GiB / 10,752 MiB，余量 1.18 GiB）
- [ ] 全部已解决策票（`issues/01`–`09`）的 `Status:` 仍是 `resolved`，**未被改动**；`map.md` 未被回开
- [ ] 本地提交，不推送

## Notes

- **别重做**：本票只改文档。`make init` 补齐属 impl-02，`research-notes.md` 的事实订正在本票内（#37）。
- 若落笔时发现 spec 自身有错（而非上游有错），按 [09] Q10 走：记 spec 附录 A，**不回开地图**。