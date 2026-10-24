# impl-03d: 断言执行面 `lake_assert.sh` + A1/A2/A3 + 幂等 + 快照区间 + 维护段

> 父票：impl-03
> 规格落点：spec §5.3（`replay_90_maintain` / `assert_lake`）、§5.4（离线断言）、§5.5（断言执行面）

Type: task
Status: ready-for-agent
Blocked by: impl-03c

> **施工状态（2026-10-24）**：交付面已落盘、实机全绿（`make replay` 退出 0、
> `make assert` 9/9 哨兵 ok、`make replay-idempotent` 两遍指纹逐字相同、
> 人为破坏后退出 1）。验收证据见
> [`evidence/impl-03d-assert-evidence.sql`](../evidence/impl-03d-assert-evidence.sql)
> 与同名 `.out.txt`（可复跑，复跑方式见本票 Progress 末节）。
> 施工中挖出的 **4 处口径/事实偏差**已逐条登记在 Progress，其中 1 处改了 A1 的粒度。
**What to build:** 把「断言」这件事从「相信 `sql-client.sh` 的退出码」换成**哨兵 SQL + stdout 解析**：每条断言写成 `SELECT '<名>' AS assertion, COUNT(*)>0 AS ok FROM …` 一行，`scripts/lake_assert.sh` 判定「无 `[ERROR]` **且** 每条哨兵 `ok=true` 字符串出现」。跑完本票，`assert_lake.sql` + `lake_assert.sh` 在一个真实 `replay` 上跑绿。

## 交付内容

1. **`replay_90_maintain.sql`**：`sys.compact(...)` ×N ＋ `sys.expire_snapshots(retain_max=>20)` ×N。
2. **`sql/assert_lake.sql`**：五层行数 + 断言集，全部写成哨兵 SELECT。
3. **`scripts/lake_assert.sh`**：解析 stdout，「无 `[ERROR]` 且每条哨兵 `ok=true`」⇒ 退 0，否则退非 0。
4. **断言集**：

| # | 断言 | 判据 |
| --- | --- | --- |
| **A1 分流完备** | `ods_distinct = dwd_distinct + reject_distinct` | **全局比对**（ODS 无事件日分区，降为全局粒度，可判性不变） |
| **A2 重复记账** | `count(*) − count(DISTINCT event_id)` | **只入日志、不作断言**（承认恒真；它是「为什么迟到标记差异不可加」的实例） |
| **A3 迟到对账** | `count(*) FROM dwd WHERE is_late=1` ＝ `sum(cnt) FROM dwd_late_event_count` | 「只存计数」的唯一用途 |
| **幂等** | 同批 dump 重放两遍，DWD `count(*)` 与逐列校验和 `SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))` 两次一致 | `REQ-F-VERIFY-02` 的离线版 |
| **快照 [5,20]** | `SELECT count(*) FROM <t>$snapshots` 落 **[5,20]** | min=5 显式；max=20 把「受控」变成有限数；`time-retained` 保持 1 h 默认 |
| **拉链两条** | 当前快照的区间自洽（普通 SELECT）＋「变更发生过」（`t$audit_log`） | 批读会丢 DELETE ⇒ 分两条走 |

## 断言执行面（这是本票的技术核心）

**源码事实**：`sql-client.sh -f` 语句报错后**仍以 0 退出**（`CliClient` 丢弃返回值、无 `System.exit`）⇒ **退出码不可用** ⇒ 只能靠哨兵 + stdout 解析。实现时不要试图「让 sql-client 退非 0」，那条路官方**没有**（见下方登记项）。

## Acceptance criteria

- [ ] `lake_assert.sh` 在真实 `replay` 之后**判定通过并退 0**（A1/A3/快照/拉链/`$audit_log` 全绿）
- [ ] `lake_assert.sh` 在哨兵面**被人为破坏**时退非 0（此条是**接线自检**，完整反例证伪归 03e —— 本票只需证明一次失败路径能走通，不要求穷举）
- [ ] A1 实测成立：`ods_distinct = dwd_distinct + reject_distinct`（全局粒度）
- [ ] A2 **只入日志、不作断言**（日志里有重复率数值，但 `lake_assert.sh` 不因它退非 0）
- [ ] A3 实测成立：DWD `is_late=1` 行数 == `dwd_late_event_count` 的 `sum(cnt)`
- [ ] **幂等**：同批 dump 跑两遍后，DWD `count(*)` 与逐列 crc32 校验和**两次完全一致**
- [ ] 快照 `count(*)` 实测落在 **[5,20]** 区间内（逐表查 `$snapshots`，留下数值）
- [ ] `num-retained.min` 显式为 **5**；`max-retained` **20**；`time-retained` 保持 **1 h 默认**
- [ ] 拉链区间校验走**当前快照的普通 SELECT**（不依赖批读）；「变更发生过」走 `t$audit_log` 且能查到记录
- [ ] `replay` 末尾的 `sys.expire_snapshots(retain_max=>20)` 收的是**上界**，日志/注释里说清「`retain_max` 是上界不是钉住」
- [ ] `lake_assert.sh` 的判定逻辑有**单元测试**（stdout 有 `[ERROR]` ⇒ 失败；某哨兵缺 `ok=true` ⇒ 失败；全绿 ⇒ 通过）

## Notes

- **本票不穷举反例**。「每条断言都能被证伪」是 03e 的独立职责 —— 这是刻意切开的：impl-02e 的 Answer 记录过一次教训（判据自身有缺陷、healthcheck 补成恒真），把「写断言」和「证伪断言」放进同一张票，容易两头都绿。
- **幂等不进每次 `verify`**：全量重放两遍是重操作，由本票的离线幂等断言 ＋ impl-02 的 `verify-fault` 恢复重放共同覆盖（spec §8.2 已定）。
- **官方未给出的，按 spec §8.3 登记、不臆造**：
  - **Flink SQL 断言失败 → 非零退出码机制 —— 源码显示不存在**（这是本票的直接成因，不是缺陷）。
  - `sys.compact` 对**主键表**的逐句适用声明。
  - Paimon `file://` compaction 成本数字。
- **不实现**（spec §8.2）：排序 compaction · 幂等进 `verify` · 「离线重算覆盖实时行」。
- Doris 侧的 `ads_window_metric` 对账**不在本票**，归 03f；本票只判 Paimon 侧自洽。
- 本票**不改** `verify_runner.py` 的第三项（仍 SKIP），转真断言归 03f。

---

## Progress

### 交付面（2026-10-24）

| 文件 | 作用 |
| --- | --- |
| `sql/replay_90_maintain.sql` | ⓪ `ALTER TABLE … SET` 快照保留 ×11（幂等兜底）＋① `sys.compact(full)` ×11 ＋② `sys.expire_snapshots(retain_max=>20)` ×11 ＋`maintain rows=` 收尾 |
| `sql/assert_lake.sql` | 9 条哨兵（`A1_*`/`A3_*`/`IDEMPOTENCY_*`/`SNAP_*`/`ZIP_*`/`OPT_*`）＋ 20+ 条度量行 |
| `scripts/lake_assert.sh` | 三层判据（docker 退出码 + 无错误标记 + 逐条哨兵）＋ 指纹 `--record/--compare` ＋ `--judge/--parse` |
| `tests/test_lake_assert.py` | 31 个用例：判定逻辑 11 条 + SQL/脚本清单一致性 11 条 + 交付面 9 条 |
| `scripts/lake_replay.sh` | 接维护段（**仅全量 replay**，`LAYER=` 截断时跳过） |
| `Makefile` | 新目标 `assert` / `replay-idempotent` |
| `sql/replay_01_ddl.sql` | 11 张表显式 `snapshot.num-retained.min=5` / `max=20` ＋ 头注「快照保留」段 |
| `GLOSSARY.md` | 新增「快照保留三参数 / 保护下界」「断言哨兵（sentinel）」两条 |
| `sql/README.md` | 维护段与断言段、第三条约定（幂等指纹不用 crc32） |
| `spec.md` §8.3 | 登记 4 条（见下「官方未给出 / 伪代码不成立」） |

### ★★ 施工中挖出的 4 处偏差（都**不会报错**、只会静默产出错判）

**① `crc32` 在 Flink 2.2 SQL 里不存在 —— spec §5.4 的伪代码不能执行**

票面/spec §5.4 逐字写 `SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))`。
实机逐字报错：

```
org.apache.calcite.sql.validate.SqlValidatorException:
  No match found for function signature crc32(<CHARACTER>)
```

同机实测可用的替代：`HASH_CODE(x)`（INT）、`MD5`/`SHA1`/`SHA2`。
⇒ 改用 **`SUM(CAST(HASH_CODE(...) AS BIGINT))`**。
⚠️ **必须 CAST**：`HASH_CODE` 返回 INT，百万行求和会溢出 INT。
   溢出的值两次跑之间**照样稳定** ⇒ 会得到一条「恒绿但不校验」的假指纹
   —— 这是本票最隐蔽的一处，已由 `test_assert_sql_hash_code_is_cast_before_sum` 静态钉住。
这不是「官方未给出」，是**伪代码不成立**；已登记 spec §8.3。

**② TABLEAU 会截断超宽单元格（约 30 字符）—— 长断言名与长指纹串会静默失效**

实测：`A_very_long_assertion_name_for_width_test`（42 字符）渲染成
`A_very_long_assertion_name_...`；一个 `ok=true rows=… fp=…` 的长串渲染成
`ok=true rows=1048576 fp=112...` —— **被截掉的恰是判据本身**。
⇒ 两条修法：① 哨兵名全部 ≤26 字符（`test_sentinel_names_fit_tableau_cell_width` 守）；
② 指纹拆成 4 条短度量行（`metric.fp.rows/hash/devent/dlag`），不塞进一个单元格。

**③ sql-client 会逐字回显 SQL 源码（含注释）—— 注释里的错误标记字面量会把成功判成失败**

本票**连踩两次**：`assert_lake.sql` 首版与 `replay_90_maintain.sql` 首版的注释里
引用了带方括号的错误标记（作为「源码事实」的引用），结果 `make replay` 在维护段报
「SQL 报错」、`make assert` 首跑报「错误标记 1 条」——而日志里真实内容只是回显的注释。
⇒ ① 所有 `sql/*.sql` 里禁止该字面量（`test_no_error_token_in_sql` **glob 整个目录**，
不是只管 assert_lake.sql —— `lake_replay.sh` 对五层脚本用的是同一条判据）；
② 脚本侧只解析以 `|` 开头的行 ⇒ 回显行里的 `ok=true` 字样不会变成幽灵哨兵
（`test_echoed_source_lines_do_not_create_phantom_sentinels`）。

**④ ★ A1 的「全局」粒度会依赖历史 —— 已收窄为「本次落地批」（需上游知情）**

spec §5.4 逐字：「`ods_distinct = dwd_distinct + reject_distinct`，**全局比对**
（ODS 无事件日分区，降为全局粒度，可判性不变）」。
实测：ODS **整表** = 2 097 152（两个落地日各一套同一批），`ods_distinct` 因
event_id 相同而塌回 1 048 576 ⇒ A1 **碰巧**绿。但一旦某天落了**另一批**
（不同 seed/规模），`ods_distinct` 会把两批并起来，而 DWD 只装本次落地日那一批
⇒ A1 会**无缘无故红**，且红得无法归因。
同类问题在 A2 更明显：首版 A2（也不过滤）实测 = **1 048 576** —— 那不是「批内重复」，
而是**前一天那一整个分区**；报的是「跑过几次」而不是「这批有多少重复」。
⇒ **A1 的 ODS 侧与 A2 都限定本次落地日**，与 `replay_20_dwd.sql` 的 `v_ods_scope` 同源。
⚠️ 这是**粒度收窄**（整表 → 本次落地批），**不是**改回按事件日分区比
（spec 说「全局」的目的就是不许按事件日分区比，那一点完全遵守）。
⚠️ **不静默改判据**：登记于此。若上游判定必须严格整表，正确修法是
**清理历史落地日的旧批**，而不是放宽本判据。

### 与票面字面表述的两处差异（都是**实测**逼出来的，不是简化）

| 票面写 | 实际实现 | 为什么 |
| --- | --- | --- |
| 快照 `count(*)` 落 **[5,20]** | 非空表判 **[1,20]**；空表判 **=0**；属性另立一条判 22 项 | `min` 是**保护下界、不制造快照**（官方逐字），新建/刚清过的表只有 5~6 个快照是合法的；而**属性是否真被钉住**（min=5 而不是官方默认 10）无法从数量判 —— 10 也落在区间里。⇒ 拆成「数量上界」+「属性钉住」两条才既可判又不假红 |
| `replay` 末尾 `expire_snapshots(retain_max=>20)` | 同（×11 张表）＋ 前置 `ALTER TABLE … SET` | `CREATE TABLE IF NOT EXISTS` 对**已存在**的表一句不改 ⇒ 老库会保持官方默认 10。这是本仓集群的真实状态（impl-03b 建的表早于本票） |

### 实机结果（交付验收）

```
$ make replay              # 缺省、不带 LAYER=
layer=ods rows=1048576 / dwd rows=1048576 / dwd_reject rows=0
layer=dim rows=105430 / dws rows=1550285 / ads rows=90348
[replay] >> maintain  (replay_90_maintain.sql)
[replay] 完成（请求范围全部跑完）        ← exit 0

$ make assert
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
[assert] ✓ 全部断言通过              ← exit 0
```

逐表快照数（来自留档 `.out.txt` 的 E1）：

| 表 | 快照数 | 表 | 快照数 |
| --- | --- | --- | --- |
| `ods_behavior_event` | **6** | `dim_region` | 5 |
| `dwd_behavior_event` | **6** | `dim_channel` | 5 |
| `dwd_behavior_event_reject` | **0**（空表） | `dws_subject_window` | 5 |
| `dim_item` | 5 | `ads_window_metric` | 5 |
| `dim_category` | 5 | `dwd_late_event_count` | 5 |
| `dim_user` | 5 | | |

⚠️ 全部落 [0,6] ⊂ [1,20]；**没有一张表是 5 个**这个说法要反过来读：
5 是 `min` 兜底后的**下限聚集**，不是目标值。ods/dwd 的 6 是 compact 又提交了一次。

### 断言实测数值（票面 AC 逐条取证）

| 判据 | 实测（E1/E2/E3/E4/E5） |
| --- | --- |
| **AC3 A1** | `a1_ods_distinct 1048576 = a1_dwd_distinct 1048576 + a1_reject_distinct 0` ✅ |
| **AC4 A2** | `a2_dup_overshoot = 0`（只入日志、`lake_assert.sh` 不因它退非 0；已被 `test_a2_value_never_affects_verdict` 钉住：0/1/1048576/999999999 四值均通过） |
| **AC5 A3** | `a3_late_rows 131316` vs `a3_cnt_sum 131316`；**逐档相等**由 `A3_late_bucket_split`（FULL OUTER JOIN 比 cnt + 档集合）判 ok=true |
| **AC6 幂等** | 两遍指纹逐字相同：`1048576 1126399221523001 1048576 131267`（rows / hash / devent / dlag） |
| **AC7 快照** | 上表，全落 [0,6] ⊂ [1,20]；空表 0 |
| **AC8 属性** | `OPT_snapshot_retention` 22/22 命中（11 表 × min/max）；`opt_dim_channel_min/max = 5/20` 抽查留档 |
| **AC9 拉链** | ① 区间自洽走**普通 SELECT + LEAD**：三表断点/重叠/哨兵尾值 0 处不符；② 变更走 `$audit_log`：item **398** / user **2978** / category **5** 个 id 有 ≥2 条审计记录 |
| **AC10 retain_max 上界** | 维护段注释逐字说明「上界不是钉住」＋记了本机实测（dim_region 曾 10→5）；`test_maintain_expire_uses_retain_max_20` 钉住该注释必须在 |
| **AC11 判定逻辑单测** | `tests/test_lake_assert.py` 31 passed（含票面点名的三例） |

### ★ 失败路径实测（AC2：接线自检）

不只做注入式验证，**在真集群上人为破坏**：

```
# 把 dim_channel 的 max 从 20 改成 100
ALTER TABLE dim_channel SET ('snapshot.num-retained.max' = '100');
$ bash scripts/lake_assert.sh
[assert] ✗ 哨兵 [OPT_snapshot_retention] = ok=false
[assert] ✗ 断言不通过          ← RC=1
# 改回 20 后
$ bash scripts/lake_assert.sh  ← RC=0
```

注入式（`--judge`，不连集群）另有三种破坏：哨兵 `ok=false` / 删掉整行哨兵 /
注入错误标记 —— 三者均 RC≠0。**注意「删掉整行」这一种**：它对应
「只看有没有 `ok=false`」的假绿通道（语句整段没跑也会「没有 ok=false」），
本脚本按硬编码清单逐条核，缺一条即失败。

### 一处环境性发现（不是本票缺陷，但影响可复现性）

`make replay` 连跑数轮后，第 N 轮在 DWD 层以 JobManager **Java heap OOM** 失败
（`java.lang.OutOfMemoryError: Java heap space`）。定位：

- JM 的堆被算成 **448 MiB**（`process.size 1024m − metaspace 256m − overhead 192m − offheap 128m`）；
- 而 `/jobs/overview` 里已积压 **258 个已完成作业**（每轮 replay = 5 层 + 11 compact + 11 expire ≈ 27 个作业）；
- `docker restart eco-flink-jm` 清空后**同一命令立刻通过**，且连跑 `replay-idempotent`（两轮）也通过。

⇒ 结论：这是**宿主机上作业历史随反复施工累积**导致的一次性现象，
不是 SQL 或判据问题；`Restarts=0`、`OOMKilled=false`、容器 mem_limit 未变。
**本票不改 `mem_limit` / JM 堆**（那是部署面，且 03c 已因另一个原因调过一次）——
按「遇到就登记」处理。若将来要长期反复跑，处置方向是定期重启 JM 或加 `web.refresh-interval` 类调优，属另一张票。

### 官方未给出 / 伪代码不成立（已登记 spec §8.3）

1. **`sys.expire_snapshots` 的 `retain_min` 缺省回落表属性** —— 源码级
   （`ProcedureUtils#fillInSnapshotOptions`），**官方文档未给出**该回落关系。
   ⇒ 本仓不依赖它：维护段对 11 张表**显式** `ALTER … SET min=5`。
2. **`sys.compact` 的返回值语义** —— 同一 `full` 策略在不同表上返回
   `JobID=…` 或 `Success`（本机都实测到），**官方未给出**两者的判定含义
   ⇒ 判据不解析返回值。
3. **`$audit_log` 的可见窗口与快照过期的关系** —— 官方未给出明确条文
   ⇒ 拉链 ② 只要求「现在能看到已发生的变更」。
4. **`crc32`** —— 不是「官方未给出」而是**伪代码不成立**（见偏差 ①）。
5. `sys.compact` 对**主键表**的逐句适用声明仍**未**查到（票面 Notes 早已列出）
   ⇒ 仍是**推断级**：procedure 示例含主键表 + `buckets` 参数明写「只支持 fixed-bucket」。
   已在新文件注释里按此分级标注。

### 复跑方式（证据可重建）

```
# 断言集（约 60 s）
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm bash -c "cat > /tmp/al.sql" < sql/assert_lake.sql
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm /opt/flink/bin/sql-client.sh -f /tmp/al.sql

# 取证集（逐表快照数 / A1 三量 / 指纹 / 拉链，约 40 s）
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm bash -c "cat > /tmp/ev3d.sql" \
  < .scratch/ecom-user-behavior-pipeline/evidence/impl-03d-assert-evidence.sql
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm /opt/flink/bin/sql-client.sh -f /tmp/ev3d.sql
```

⚠️ 不能 `docker cp` 进 `/sql`：该挂载点是 **`:ro`**（只读）；`/data` 同样只读。
⚠️ 取证集钉了 `parallelism.default = 2`：它是「多条 UNION ALL 各自读一遍 DWD」的形状，
   4 并行会撞 `Insufficient number of network buffers`（impl-03c 记录过同一类事故）。

### 回归

`pytest tests/` **257 passed**；`ruff check` / `ruff format --check` 干净；
`make typecheck` errors=0；`make config-check` PASS（6.25 GiB）。