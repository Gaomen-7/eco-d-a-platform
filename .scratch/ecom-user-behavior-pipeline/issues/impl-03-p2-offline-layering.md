# impl-03: P2 离线分层 — 五层建表 · 全 SQL replay · 断言与对账

> **母票**。2026-10-22 拆为六张副票施工，正文保留为**决策档案**（不删改）。开工看副票，收口回这里核对 10 条验收。

Type: task
Status: ready-for-agent（拆票中，见下）
Blocked by: impl-02

## 副票（2026-10-22 拆）

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-03a** | [impl-03a-p2-deployment-surface.md](impl-03a-p2-deployment-surface.md) | impl-02 | 部署面：warehouse 具名卷同路径挂 JM/TM ＋ Paimon jar 进每个 Flink 进程 |
| **impl-03b** | [impl-03b-lake-ddl-and-ods-dwd-replay.md](impl-03b-lake-ddl-and-ods-dwd-replay.md) | impl-03a | 门槛批 ＋ 七表 DDL 形状 ＋ `replay` 跑通 ODS→DWD |
| **impl-03c** | [impl-03c-dim-dws-ads-five-layer-replay.md](impl-03c-dim-dws-ads-five-layer-replay.md) | impl-03b | DIM 拉链 ＋ DWS 区间 join ＋ ADS 五层贯通 |
| **impl-03d** | [impl-03d-lake-assert-sentinels-and-idempotency.md](impl-03d-lake-assert-sentinels-and-idempotency.md) | impl-03c | 哨兵断言面 ＋ A1/A2/A3 ＋ 幂等 ＋ 快照区间 ＋ 维护段 |
| **impl-03e** | [impl-03e-lake-assert-counterexample-falsification.md](impl-03e-lake-assert-counterexample-falsification.md) | impl-03d | 反例证伪 F1–F6：断言能判、不是恒真 |
| **impl-03f** | [impl-03f-paimon-doris-reconciliation-and-verify.md](impl-03f-paimon-doris-reconciliation-and-verify.md) | impl-03e | Paimon↔Doris 对账 ＋ `verify` 第三项转真断言 ＋ 母票收口 |

**切分依据**（各自可单独验证、单独证伪）：

- **03a 单独**：纯部署面，不依赖任何 SQL，且可单独证伪（挂错卷 / jar 只进 JM 都在本票内验完），是 03b 的物理前提。
- **03b / 03c 切在「能否跑通」**：03b 证明形状与前两层，03c 证明拉链时态 join 与五层贯通。
- **03d / 03e 切在「写断言 / 证断言」**：这是最要紧的一条。impl-02e 的 Answer 记过判据自身恒真（healthcheck 补成恒真、夹具棘轮把判据推成恒假）。把两者放进同一张票容易**两头都绿**；切开后 03d 的断言在经 03e 证伪前一律不算数。
- **03f 单独收口**：`verify` 从 4 真 + 1 SKIP 转 5 真只发生在最后，前五张票不得改 `verify_runner.py`，避免 P2 途中把 P1 已验收的判据改坏。

---

## 原票正文（决策档案，2026-10-21）

**What to build:** Paimon 湖仓五层按定案形状建成、能被单目标 `make replay` 全 SQL 零 jar 跑通、并用三条各自可判的断言 + 幂等 + 快照区间 + 对账证明它算成。跑完本票，`make verify` 第三项「Paimon 五层有行」从 impl-02 留下的显式 SKIP 转为**真断言并参与退出码判定**——五项全真。

**① 七张表按形状表建成**（分区键统一 `dt STRING` = `yyyy-MM-dd` UTC，`hour` 已否）：
ODS `ods_behavior_event`（append，`dt`=落地日，`payload` 原文零解析，`bucket=-1`）· DWD `dwd_behavior_event`（主键 `(dt, event_id)`，固定 `bucket=1`，`is_late`/`late_lag_ms`，`upsert-materialize=NONE`，**不设 `sequence.field`**）· 拒绝 `dwd_behavior_event_reject`（append，`dt`=摄入日处理时间，`reject_reason` 封闭三值无 `duplicate`）· DIM `dim_item`/`dim_category`/`dim_user`（拉链 `valid_from_ms`/`valid_to_ms` BIGINT ms，主键 `(id, valid_from_ms)`，时间源＝维度变更流 `change_time`）+ 静态 `dim_region`/`dim_channel`（无拉链）· DWS `dws_subject_window`（主键含窗口起点，指标列 `pv/uv/gmv`）· ADS `ads_window_metric`（**键与 Doris `ecom_ads.ads_window_metric` 同构**，这是对账前提）· 迟到计数 `dwd_late_event_count`（append，`dt` × 60 s 档，**只存计数**）。主键表默认 **MOR 而非 MOW**（2026-10-21 签，附录 A·N），官方 bucket 指引是 MOR 口径。时态维 join 走 batch 区间 join（lookup join 被官方否定：processing-time，回放必错）。

**② `make replay` + `LAYER=` 全 SQL 零 jar** — 串联 `sql/replay_00_catalog.sql`（CREATE CATALOG，warehouse=`file:/warehouse`）→ `10_ods` → `20_dwd` → `30_dim` → `40_dws`（dwd × dim 区间 join）→ `50_ads` → `90_maintain`（`sys.compact` ×N + `sys.expire_snapshots(retain_max=>20)` ×N）→ `assert_lake.sql`。不新增 jar、不需要 JDK/Maven。日志行 `layer=dwd rows=…` 提供可观察性。门槛批由生成器 `dump --events 1048576 --logical-span 3d --seed <固定>` 现产，落 `data/` **不入 git**（3 天跨度让 `dt` 有 3 个分区、拉链有跨日变更对象；密度 ≈4 ev/s 与实时 200 ev/s 不同源，是两个用途不同的参数）。**不重读 Kafka**。

**③ 部署面两条硬约束** — warehouse 落具名卷并以**同一路径**挂进 JM 与 TM（两个容器不共享 `/opt/flink/...`）；Paimon jar 构建期 `COPY` 进 `/opt/flink/lib`，**JM 和 TM 都要**（官方逐字 "Install one matching Paimon bundled connector in **each** Flink process"，`ADD JAR` 宿主路径不行）。

**④ 断言执行面与断言集** — `sql-client.sh -f` 报错后**仍以 0 退出**（`CliClient` 丢弃返回值、无 `System.exit`）⇒ **退出码不可用** ⇒ 每条断言写成哨兵 SQL（`SELECT '<名>' AS assertion, COUNT(*)>0 AS ok FROM …`），`scripts/lake_assert.sh` 判定「无 `[ERROR]` **且** 每条哨兵 `ok=true` 字符串出现」。断言集：
- **A1 分流完备**：`ods_distinct = dwd_distinct + reject_distinct`（全局比对；ODS 无事件日分区故降为全局粒度，可判性不变）
- **A2 重复记账**：`count(*) − count(DISTINCT event_id)` **只入日志、不作断言**（承认恒真；它是「为什么迟到标记差异不可加」的实例）
- **A3 迟到对账**：`count(*) FROM dwd WHERE is_late=1` ＝ `sum(cnt) FROM dwd_late_event_count`
- **幂等**：同批 dump 重放两遍，DWD `count(*)` 与逐列校验和 `SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))` 两次一致
- **快照 [5,20]**：`SELECT count(*) FROM <t>$snapshots` 落 [5,20]（min=5 显式 + max=20 把「受控」变成有限数 + time-retained 保持 1 h 默认；`retain_max` 是**上界**不是「钉住」）
- **拉链两条**：批读会丢 DELETE ⇒ 区间校验走当前快照的普通 SELECT，「变更发生过」走 `t$audit_log`

**⑤ Paimon↔Doris 对账** — 逐 `metric_name` 比对 Paimon `ads_window_metric`（离线口径基准）与 Doris `ecom_ads`（看板供数点）两套表，**只比 `is_settled=1`**，逐值严格相等。**两套物化、一个口径**：Paimon 侧不直接供看板，Doris 侧是唯一供数点。

**Blocked by:** impl-02

**Status:** ready-for-agent

## Acceptance criteria

> **这 10 条由六张副票共同兑现**，收口勾选归 **impl-03f**。括号内是主责票，勾选前先看那张票的验收。

- [ ] `make replay` 默认跑完五层退出 0；`make replay LAYER=dws` 只跑到 DWS 停住，日志逐层有 `layer=<层> rows=…`（**03b / 03c**）
- [ ] 七张表的分区键、主键、bucket、表属性与形状表逐项一致；DWD/DIM/DWS/ADS 主键表均 `upsert-materialize=NONE` 且未设 `sequence.field`（**03b**）
- [ ] 拒绝表的 `dt` 取摄入日（处理时间）而非事件日；`reject_reason` 取值域恰为三值，**没有 `duplicate`**（**03b**）
- [ ] 同批 dump 跑两遍后 DWD 行数与逐列校验和完全一致（幂等成立）（**03d**）
- [ ] `lake_assert.sh` 在人为破坏一条断言（如改掉 A3 的一侧）时判定失败并返回非 0（证哨兵面真的会判，不是恒真）（**03e**）
- [ ] 快照 count 落在 [5,20] 内；批读整表时拉链区间在当前快照下自洽，`$audit_log` 能查到变更发生过（**03d**）
- [ ] Paimon 与 Doris 两套 `ads_window_metric` 在 `is_settled=1` 行上逐值严格相等；人为改动 Doris 侧一个落定值后对账断言失败（**03f**）
- [ ] `make verify` 第三项在本票完成后由 SKIP 转为真断言，五项全真且退出码 0（**03f**）
- [ ] 门槛批文件落 `data/` 且被 `.gitignore` 覆盖；仓库内无原始数据（`git check-ignore` 验证）（**03a / 03b**）
- [ ] 官方未给出的清单按 spec §8.3 登记（`sys.compact` 对主键表的逐句适用声明、`file://` compaction 成本数字等）——**遇到就登记，不臆造**（**03d / 03f**）

## Notes

- **强序入口**：impl-02 通过才开本票。Paimon catalog 下承载五层表的是 **`ecom_lake` database**（2026-10-21 签，附录 A·M；原文「单一 catalog（库名 `ecom_lake`）」一词混说两层）。
- **不实现清单**（spec §8.2）：排序 compaction（官方仅支持 `bucket=-1`＋batch，本仓主键表是固定 bucket）· lookup join / zipper / SCD（Paimon 全站无官方能力）· `sequence.field` · 「离线重算覆盖实时行」（记 COULD 留档）· `minio`/`spark` profile（属未签字意图类 #36，见 impl-05）。
- **幂等不进每次 `verify`**：它是重操作，由本票的离线幂等断言 + impl-02 的 `verify-fault` 恢复重放共同覆盖。