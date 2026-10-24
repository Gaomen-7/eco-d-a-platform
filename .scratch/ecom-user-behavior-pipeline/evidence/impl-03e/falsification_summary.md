# impl-03e 证伪总账（2026-10-24 续跑会话自动汇总）

| 反例 | 红（RC=1 除注明外） | 回滚绿 | 关键行（逐字） |
| --- | --- | --- | --- |
| F5（前会话） | ✗ SQL 层错误标记 1 条（9 哨兵全绿） | — | `[ERROR] … no_such_table_f5` |
| F6 | 哨兵 [A1_split_complete] **没有出现** | —（派生文件，不动集群） | `✗ 哨兵 [A1_split_complete] **没有出现**（缺席不等于通过）` |
| F1 | ✗ 哨兵 [A1_split_complete] = ok=false（8 绿） | RC=0，指纹回基线 | `fx.f1.inserted rows=1` → `residual rows=0` |
| F2a | ✗ 哨兵 [A3_late_bucket_split] = ok=false（8 绿） | RC=0 | `fx.f2a.flipped rows=1` → `back_to_zero rows=1` |
| F2b | ✗ 哨兵 [A3_late_bucket_split] = ok=false（8 绿） | RC=0 | `fx.f2b.added=2` → `residual=0` |
| F3a | ✗ 哨兵 [OPT_snapshot_retention] = ok=false（8 绿） | RC=0 | `fx.f3a.max=100` → `max=20` |
| F3b | SNAP_within + OPT 双红（7 绿） | RC=0（snaps 34→20） | `fx.f3b.snaps=34` → 回滚后 20 |
| SNAP_empty | A1=false（reject 多行被 A1 捕捉）；SNAP_empty 走第二分支仍绿 | RC=0（0 行 0 快照） | `fx.se.rows=1/snaps=1` → `rows=0/snaps=0` |
| ZIP_interval | ✗ 哨兵 [ZIP_interval_self_sync] = ok=false（8 绿） | RC=0 | `fx.zi.versions=2` → `1` |
| ZIP_audit | ✗ 哨兵 [ZIP_audit_log_shows_change] = ok=false（8 绿） | RC=0（replay_30 无 ERROR，dim 105430） | `fx.za.rows=1/ge2=0` |
| A2 负向 | **RC=0**（9 哨兵全绿） | RC=0，metric 回 0 | `metric.a2_dup_overshoot = 1` → `0` |
| F4 | **RC=3**（9 哨兵全绿） | compare RC=0 | `上一遍 …001 / 本 遍 …000` |
| fp_clear | A1/A3_bucket/A3_flag/fp_ready 4 红 + SNAP_within 连带红（4 绿） | compare RC=0 逐字节回基线 | 空 DWD 期间 dwd 快照 21>20 |
