# impl-03e: 反例证伪 —— 每条离线断言都能被证伪（哨兵面不是恒真）

> 父票：impl-03
> 规格落点：spec §5.5（断言执行面）、impl-03 母票验收第 5 条

Type: task
Status: ready-for-human（2026-10-24 全部反例跑完待人复核；F1–F6 + 5 附加组全证伪，总账见文末 Progress §9 与 `evidence/impl-03e/falsification_summary.md`）
Blocked by: impl-03d

**What to build:** 逐条**人为破坏**离线断言，让 `lake_assert.sh` 每次都退非 0，并留下「破了哪条、判定是否如预期」的逐条证据。这张票**不产出新功能**，只产出**证伪**。

## 为什么单独成票

`lake_assert.sh` 的判定逻辑是「无 `[ERROR]` 且每条哨兵 `ok=true` 字符串出现」。这类**基于字符串出现的判定天然容易恒真**：哨兵写错、没输出、被日志吞掉，都可能让判定**永远通过**。

impl-02e 的 Answer 记过同一条教训两次：补的 healthcheck 因 `pgrep -f` 匹配到 shell 自己而**恒真**，判据从「不存在」退化成「什么都不判」；以及夹具棘轮让脏率判据从「恒真」翻成「恒假」。**判据一旦与恒定方向绑定，就不再断言任何东西。**

把「写断言」与「证伪断言」分开，是为了防止前者把后者顺手做掉 —— 本票开始时，03d 的所有断言在**未经证伪**的状态下一律**不算数**。

## 逐条证伪清单

| # | 断言 | 怎么破 | 期望 |
| --- | --- | --- | --- |
| F1 | **A1 分流完备** | 改一侧（如手工删/插 DWD 一行，让 `dwd_distinct` 不再闭合） | 退非 0 |
| F2 | **A3 迟到对账** | 改掉 A3 的一侧（如改 `dwd_late_event_count` 一档计数） | 退非 0 |
| F3 | **快照 [5,20]** | 把 `retain_max` 调出区间（或制造快照数 < 5） | 退非 0 |
| F4 | **幂等** | 让第二次重放结果与第一次不同（如中途改数据） | 校验和不等 ⇒ 判失败 |
| F5 | **哨兵面自身** | 故意让一段 SQL 报错（stdout 出现 `[ERROR]`），但哨兵仍全 `ok=true` | **仍须退非 0** |
| F6 | **哨兵缺失** | 让某条哨兵**根本不输出**（不是 `ok=false`，是压根没有这行） | 退非 0（证「必须出现」而非「出现即真」） |

F5 与 F6 是**判定面本身**的反例，比 F1–F4 更要紧：它们证明 `lake_assert.sh` 不是「看到 `ok=true` 就过」。

## Acceptance criteria

- [x] F1–F6 **逐条**跑过，每次 `lake_assert.sh` **退非 0**，且 stdout 指出是哪条哨兵失败（F4 为 RC=3、A2 为负向 RC=0，各有专门 AC）
- [x] 每次证伪都留下**可复核证据**（命令 + 退出码 + 关键 stdout 行；`evidence/impl-03e/falsification_summary.md` 总账）
- [x] 破坏是**可回滚的**：证伪结束后重跑正例，`lake_assert.sh` 恢复退 0（final RC=0 + final_cmp 指纹逐字回基线）
- [x] F5 证明「有 `[ERROR]` 时即便哨兵全 `ok=true` 也判失败」（§3）
- [x] F6 证明「哨兵缺行也判失败」（§4：`✗ 哨兵 [A1_split_complete] **没有出现**（缺席不等于通过）`）
- [x] A2 **不参与**证伪：人为制造重复后**仍 RC=0**、日志出现 `metric.a2_dup_overshoot = 1`（§7 A2 行）
- [x] 附加哨兵面（SNAP_empty / ZIP×2 / fp_ready+A3_flag / F3a/F3b）也逐条证伪（§6 表全部执行）
- [x] 若发现某条断言**无论怎么破坏都通过**，记为真缺陷并处置 —— 实测**未发现恒真**；发现两处**预期偏差**已按票面登记（§8 偏差③④）

## Notes

- 本票**不加断言、不改口径**。发现的是判据缺陷就修判据；发现的是口径缺口就登记（不猜）。
- 证据落 `artifacts/` 下的目录（该目录已在 `.gitignore` 内），**关键内容逐字摘录进票面**，不依赖 artifacts 留存。
- 若 A1 因「全局粒度」在本仓数据上碰巧恒真（如拒绝表恰好为空导致两侧同时成立），**不得**靠放宽断言蒙过去 —— 登记并说明，或补一条能真正区分的判据。
- 本票与 impl-05（终验）无冲突：终验不重复逐条证伪，只做容量/冷启/附录结清。
- 反例夹具若造成数据污染，须与 impl-02d 的做法一致：**结束时回到正例状态**并用一次正例重跑证明（对照 impl-02e 的棘轮教训）。

---

## Progress（2026-10-24，✅ 全部反例跑完）

> **一句话状态**：F1–F6 全部证伪 + 5 组附加哨兵面（F3a/F3b/SNAP_empty/ZIP×2）+
> A2 负向 + fp_clear 全部完成；**未发现任何恒真断言**；两处预期偏差按票面登记（§8 ③④）。
> 收尾正例 `final` RC=0、`final_cmp --compare` 指纹逐字回基线
> `1048576 1126399221523001 1048576 131267`。证据总账：
> `evidence/impl-03e/falsification_summary.md`（夹具 SQL 同目录入库）。

### 1. 环境与基线（已取证）

| 项 | 结果 |
| --- | --- |
| 执行面 | **WSL2**（`wsl -e bash -lc`）；容器 `eco-flink-jm` healthy；Docker 端口从 WSL 可达（与 Makefile 头注一致） |
| UTC 时刻 | `2026-10-24 05:27`，与门槛批落地日 `2026-10-24` **同日**（本票不重跑 `replay`，故无跨 UTC 日风险） |
| 静态基线 | `uv run pytest tests/ -q` ⇒ **269 passed in 326.81s**；`ruff check .` ⇒ All checks passed；`ruff format --check .` ⇒ 93 files already formatted |
| 集群正例 | `bash scripts/lake_assert.sh --record` ⇒ **RC=0**，9/9 哨兵 ✓，`metric.a2_dup_overshoot = 0` |
| 基线指纹 | `1048576 1126399221523001 1048576 131267`（rows / hash / devent / dlag） |
| 单次断言耗时 | **≈1m58s**（无并发负载时）。⚠️ 并行跑 pytest 时会拖到 4–5 min（本轮首跑因此撞了 5 min 调用上限） |

状态探针（只读；`evidence/impl-03e/probe.sql` + `probe2.sql`，逐字摘录）：

```
| probe.now_utc | 2026-10-24 05:47:10.124 |
| probe.landing_day | 2026-10-24 |
| probe.ods_rows_total | 3145728 |        ← 三个落地日 × 1048576（每日本分区 INSERT OVERWRITE，同日重跑不叠加）
| probe.ods_today_rows | 1048576 |
| probe.ods_today_min_src_seq | 0 |
| probe.dwd_rows | 1048576 |
| probe.dwd_distinct | 1048576 |
| probe.dwd_snapshots | 5 |
| probe.reject_rows | 0 |
| probe.reject_snapshots | 0 |
| probe.late_count_rows | 4303 |
| probe.dwd_dt=2024-01-01 | 350021 |     ← DWD 的 dt 是事件日（≠ 落地日）
| probe.dwd_dt=2024-01-02 | 351226 |
| probe.dwd_dt=2024-01-03 | 347329 |
| probe.f1_source | 000020fe-0c38-4345-aada-309be337570c |        ← 第一条非迟到行
| probe.f2_source | 00005176-7e4c-4cc4-a865-1bc61718845a |        ← 第二条非迟到行
| probe.f4_source | 000004a7-e232-4b55-a3ff-f5159dc15a87 / 2941473 | ← 第一条迟到行的 lag
| probe.zip_item | i_000000 / 1704067294314 |                       ← 该 item 最后一版的 valid_from_ms
| probe.late_bucket_top | 2024-01-03 / 1704319080000 / 254 |         ← cnt 最大的档
```

⚠️ 首轮探针的 `event_id` 被 **TABLEAU 30 字符列宽截断**（impl-03d 偏差②登记过的坑，本次又踩一次）；
改用 `SUBSTRING(event_id,1,18)` / `SUBSTRING(event_id,19,18)` 拆两半取全 —— 后续夹具不要假设长 id 能整格显示。

### 2. ★ 新确立的一手事实：`sql-client.sh -f` **报错即中止整个脚本**

F5 的初版做法是「一条报错语句 + 完整断言集」（假定报错后后续语句继续跑）。实机证伪了这个假定，逐字：

```
Flink SQL> [ERROR] Could not execute SQL statement. Reason:
org.apache.calcite.sql.validate.SqlValidatorException: Object 'no_such_table_f5' not found

Shutting down the session...
done.
```

报错语句之后的**全部语句都不再执行**（9 条哨兵一条没跑；该次 `--judge` 报 1 条错误标记 + 9 条哨兵缺席 + 5 条度量缺失，RC=1）。
impl-03d 只记了「报错仍退 0 且输出恰有 1 条错误标记」，**中止行为未记录**。⇒ 两层后果（都已留证）：

1. **F5 的纯形态必须把报错语句放文件末尾**（哨兵全跑完之后），否则测到的是「缺行」而不是「有错仍全绿」；
2. **报错在中间时，判定面靠「哨兵缺席」层也会红**（`f5_derived_mid` 形态）—— 这正是两层判据互补的价值：
   脚本中途死掉不会被误判为通过。

### 3. F5 —— ✅ 已证伪（AC：有 `[ERROR]` 时即便哨兵全 `ok=true` 也判失败）

派生方式（`evidence/impl-03e/make_derived.sh`，diff 逐行可复核）：

```
$ diff sql/assert_lake.sql artifacts/impl-03e/f5_derived.sql
108a109
> SELECT 'FIXTURE f5_derived BEGIN' AS fx;
461a463
> SELECT * FROM no_such_table_f5;          ← 报错语句放末尾（见 §2）
```

实机执行（docker rc=0，stdout 里 marker 行 `112:| FIXTURE f5_derived BEGIN |`）+ 判定：

```
$ bash scripts/lake_assert.sh --judge artifacts/impl-03e/f5_derived.out
[assert] ✗ SQL 层错误标记 1 条（见原始输出）
[assert] ✓ A1_split_complete
[assert] ✓ A3_late_bucket_split
[assert] ✓ A3_late_flag_nonempty
[assert] ✓ IDEMPOTENCY_fp_ready
[assert] ✓ SNAP_within_upper_bound
[assert] ✓ SNAP_empty_table_zero
[assert] ✓ ZIP_interval_self_sync
[assert] ✓ ZIP_audit_log_shows_change
[assert] ✓ OPT_snapshot_retention
[assert] · metric.a2_dup_overshoot = 0
[assert] · metric.fp.rows = 1048576
[assert] · metric.fp.hash = 1126399221523001
[assert] · metric.fp.devent = 1048576
[assert] · metric.fp.dlag = 131267
[assert] · A2 只记账、不判定（spec §5.4 逐字：恒真，不作断言）
[assert] 不通过（--judge 注入模式）
F5_JUDGE_RC=1
```

stdout 侧逐字：9 条哨兵全为 `| <NAME> | ok=true |`（`A1_split_complete` … `OPT_snapshot_retention`），
尾部一条 `[ERROR] … Object 'no_such_table_f5' not found`。
⇒ **哨兵全绿、又只靠「有错误标记」这一层判失败**，RC=1 —— AC 达成。
全量 stdout：`evidence/impl-03e/f5_derived.out.txt`；中止形态：`f5_derived_mid.out.txt`；判定日志：`f5_judge.out.txt`。

### 4. F6 —— ✅ 已证伪（AC：哨兵缺行也判失败）

派生文件不变（§6 表的 f6_derived.sql，A1 语句整块删除）。实机执行：

```
$ bash artifacts/impl-03e/run_fixture.sh f6_derived
FX[f6_derived] ✓ rc=0 marker=ok error=0
$ bash scripts/lake_assert.sh --judge artifacts/impl-03e/f6_derived.out; echo RC=$?
[assert] ✗ 哨兵 [A1_split_complete] **没有出现**（缺席不等于通过）
[assert] ✓ A3_late_bucket_split        （其余 8 条全绿，逐条打印）
…
F6_JUDGE_RC=1
```

无错误标记、其余 8 条哨兵 `ok=true`、A1 行**整条缺席** ⇒ 判定面证明是「必须出现且为 true」。
全量 stdout：`artifacts/impl-03e/f6_derived.out`（不入库，派生方式可由 make_derived.sh 复现）。

### 5. 一处 runner 侧误报（已修，F5 结论不受影响）

`run_fixture.sh f5_derived` 的 marker 检查**误报过一次**（「BEGIN marker 缺失」），而同一 `.out` 里
marker 行干净存在（`112:| FIXTURE f5_derived BEGIN |`，`od -c` 无控制字符），事后复测同一检查又通过
（`clean_len=30347`、`direct_grep=1`）。F5 的判定**不受影响**（judge 直读同一 `.out`）。
候选根因：`printf "%s" "$clean" | grep -q …` 在 `set -o pipefail` 下，`grep -q` 命中即退出，
大输出时 printf 收 SIGPIPE(141) ⇒ 整条管道被 `if ! …` 判成失败（未完全定位，记为候选）。
⇒ **已改为纯 bash `case` 匹配**（不走管道）；同一 42 KB 输出验证 `case_found=1` ✓。
入库版即修好的版本：`evidence/impl-03e/run_fixture.sh`。

### 6. 状态面 8 组 —— ✅ 全部执行（2026-10-24 续跑会话）

> 每组三步：`run_fixture.sh <break>` → `run_assert.sh <label>_red`（期望点名目标哨兵）→
> `run_fixture.sh <rollback>` → `run_assert.sh <label>_green`（期望 RC=0）。
> 夹具 SQL 已全部入库 `evidence/impl-03e/`；下表「红」列为 judge 逐字输出。

| # | 红（实际） | 其余哨兵 | 回滚绿 | 关键证据行（逐字） |
| --- | --- | --- | --- | --- |
| F1 | `✗ 哨兵 [A1_split_complete] = ok=false` | 8 ✓ | RC=0，指纹回 `…001` | `fx.f1.inserted rows=1` → `residual rows=0` |
| F2a | `✗ 哨兵 [A3_late_bucket_split] = ok=false` | 8 ✓ | RC=0 | `fx.f2a.flipped rows=1` → `back_to_zero rows=1` |
| F2b | `✗ 哨兵 [A3_late_bucket_split] = ok=false` | 8 ✓ | RC=0 | `fx.f2b.added=2` → `residual=0` |
| F3a | `✗ 哨兵 [OPT_snapshot_retention] = ok=false` | 8 ✓ | RC=0 | `fx.f3a.max=100` → `max=20` |
| F3b | `✗ [SNAP_within_upper_bound]` + `✗ [OPT_snapshot_retention]` 双红（票面预期 SNAP+OPT，吻合） | 7 ✓ | RC=0 | `fx.f3b.snaps=34`（5+22 upsert + F2b/SNAP_empty 期间提交）→ 回滚 `snaps=20` |
| SNAP_empty | `✗ [A1_split_complete] = ok=false`（reject 多出的行被 **A1** 捕捉——污染面由 A1 兜底）；`SNAP_empty_table_zero` 本身走第二分支（有行 ⇒ 快照≥1）仍 ✓ | 8 ✓ | RC=0（回 0 行 0 快照） | `fx.se.rows=1/snaps=1` → `rows=0/snaps=0` |
| ZIP_interval | `✗ 哨兵 [ZIP_interval_self_sync] = ok=false` | 8 ✓ | RC=0 | `fx.zi.versions=2` → `1` |
| ZIP_audit | `✗ 哨兵 [ZIP_audit_log_shows_change] = ok=false` | 8 ✓ | RC=0（replay_30 重跑 0 ERROR、dim 105430） | `fx.za.rows=1`、`fx.za.ge2=0` |
| A2 负向 | **无红**（9 哨兵全 ✓、**RC=0**）—— 刻意 | 9 ✓ | RC=0，`a2_dup_overshoot` 回 0 | `metric.a2_dup_overshoot = 1`（只记账） |
| F4 | **RC=3**（非 1）：`✗ 幂等破：DWD 指纹与上一遍不同`，**9 哨兵全 ✓** | 9 ✓ | `--compare` RC=0 | `上一遍 …1126399221523001 / 本 遍 …000` |
| fp_clear | `✗ [A1]` `✗ [A3_late_bucket_split]` `✗ [A3_late_flag_nonempty]` `✗ [IDEMPOTENCY_fp_ready]` 4 红（票面预期）**＋ `✗ [SNAP_within_upper_bound]` 连带红（偏差④）** | 4 ✓ | `--compare` RC=0 **逐字节回基线** | 空 DWD 期间逐表探针：dwd 快照 n=21 > 20 |

**F2b 前提探测的实测结果**（票面「先在 reject 表探测」）：append 表 `DELETE` 不可用，逐字：

```
java.lang.UnsupportedOperationException: table 'org.apache.paimon.table.AppendOnlyFileStoreTable'
can not support delete, because there is no primary key.
```

且**空源 `INSERT OVERWRITE` 是 no-op**（静态分区与动态两种写法实测均不删除现存数据；非空源 OVERWRITE
才是真覆盖——F2b 回滚正是用非空源静态分区 OVERWRITE 实现的）。这两条实测事实一并登记（§8 ③）。

### 7. AC 进度矩阵（最终）

| AC | 状态 |
| --- | --- |
| F1–F6 逐条跑过、退非 0、点名哨兵 | ✅ 全部（F4 RC=3、A2 负向 RC=0 各按专门 AC） |
| 每次证伪留可复核证据 | ✅ 命令+RC+stdout 逐字入票；`evidence/impl-03e/falsification_summary.md` |
| 破坏可回滚：正例恢复退 0 | ✅ `final` RC=0；`final_cmp --compare` 指纹逐字 `1048576 1126399221523001 1048576 131267` |
| F5：有错仍全绿也判失败 | ✅（§3） |
| F6：缺行也判失败 | ✅（§4） |
| A2 不参与证伪 | ✅ 造重复后 RC=0、`a2_dup_overshoot = 1` 只入日志 |
| 恒真缺陷登记 | 无恒真；两处**预期偏差**已登记（§8 ③④，均为实测新事实非判据缺陷） |

### 8. 续跑会话新登记的偏差与事实（3 条）

1. **runner 相对 cd 缺陷（工具面，已修）**：`run_assert.sh`/`run_fixture.sh` 的
   `cd "$(dirname "$0")/../.."` 只在 `artifacts/impl-03e/` 副本下落到仓库根；从
   `evidence/impl-03e/`（深一级）调用时重定向 `artifacts/…` 直接 ENOENT。
   已改为「向上找含 Makefile+scripts 的目录」，两副本行为一致（入库版即修后版本）。
2. **marker 名 ≤22 字符**：`FIXTURE <name> BEGIN` 渲染在 30 字符 TABLEAU 列宽内会被截断
   （`probe_delete_cleanup` → `FIXTURE probe_delete_cleanu...`），runner 的 case 匹配随之失配。
   夹具名一律改短（`cleanup2`/`se_rollback` 等）。与 §1 的 event_id 截断同根，列为夹具纪律。
3. **append 表两条写路径事实**（F2b 前提探测的产出，影响 SNAP_empty 形态可达性）：
   - `DELETE` 对 append 表不可用（异常逐字见 §6）；
   - **空源 `INSERT OVERWRITE`（静态/动态分区皆然）不删除现存数据**。
   ⇒ 原设计「SNAP_empty：插行再清空 ⇒ 空表但有快照」的第三态在 append 表上**纯 SQL 不可构造**。
   实际覆盖：正例态 0 行 0 快照（cleanup5 重建后实测）+ 第二分支 1 行 1 快照（仍绿，证明判据
   非单向「只认空表」）；SNAP_empty 的「空表但快照>0 ⇒ 红」分支留待主键表场景（dwd 备份表
   可构造，但那等价于 fp_clear 已覆盖的形态，不重复）。**不是判据缺陷，不改口径。**
4. **fp_clear 的 SNAP_within 连带红**（票面原预期「SNAP 不红：删除也提交快照」被推翻）：
   实测空 DWD 期间 dwd 快照数 21 > 20——F2b/SNAP_empty/fp_clear 多轮夹具提交叠加后
   恰好越过上界。判据行为**正确**（快照数真的超了 20）；还原后 INSERT OVERWRITE 提交新快照、
   旧快照过期，收敛回绿。结论：票面该行的预期写错了，「删除也提交快照」恰恰意味着
   **夹具轮次多时 SNAP_within 必然连带红**——已按实测订正。

### 9. 证据落点（最终）

| 落点 | 内容 |
| --- | --- |
| **入库** `.scratch/ecom-user-behavior-pipeline/evidence/impl-03e/` | `falsification_summary.md`（总账）+ **23 份夹具 SQL**（11 份 break + 10 份 rollback —— ZIP_audit 的回滚是容器内重跑 `/sql/replay_30_dim.sql`，无独立夹具文件，见 §6 —— 另加 `probe_delete.sql`、`cleanup5.sql`）+ 前会话的 F5/F6 派生物与 runner（三个 runner 均已含 §8① 的仓库根修复；`run_fixture.sh` 并已改为**优先找入库副本**的夹具） |
| 不入库 `artifacts/impl-03e/`（.gitignore） | 全量 `.out`/`.log` 原始输出 |
| 票面 | §3–§8 逐字摘录（不依赖 artifacts 留存） |

复跑：`bash evidence/impl-03e/make_derived.sh` 重建 F5/F6 派生 SQL；
`bash evidence/impl-03e/run_assert.sh <label> [参数…]`；`bash evidence/impl-03e/run_fixture.sh <name>`。
