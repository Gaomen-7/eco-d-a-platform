# impl-03b: 七表 DDL 形状 + 门槛批 + `replay` 跑通 ODS→DWD

> 父票：impl-03
> 规格落点：spec §5.1（门槛批）、§5.2（形状表）、§5.3（`replay` 编排前段）

Type: task
Status: resolved（2026-10-23，两轮 code-review 后收口；实机复跑见 §P0-7）
Blocked by: impl-03a

**What to build:** 生成器现产门槛批灌进 ODS、ODS 解析成 DWD（含拒绝旁路），`make replay` 能从零跑到 DWD 停住并打印逐层 `layer=<层> rows=…`。跑完本票，`sql/` 里有一份可执行的 DDL + 一条可截断的编排链。

## 交付内容

1. **门槛批现产**：生成器 `dump --events 1048576 --logical-span 3d --seed <固定值>` 落 `data/`，**不入 git**。3 天跨度让 `dt` 有 3 个分区、拉链有跨日变更对象；密度 ≈4 ev/s（与 `REQ-NF-PERF-03` 的 200 ev/s **不同源**——批次密度 ≠ 实时速率）。
2. **`replay_00_catalog.sql`**：CREATE CATALOG，warehouse=`file:/warehouse`（路径＝impl-03a 挂载的那个），承载五层的 database 名 **`ecom_lake`**。
3. **ODS**：`ods_behavior_event`，append，分区 `dt`（**落地日**），`payload` 原文列**零解析**，不写 bucket。
4. **DWD**：`dwd_behavior_event`，主键 `(dt, event_id)`，`dt`＝**事件日 UTC**，固定 `bucket=1`，带 `is_late`/`late_lag_ms`，**必须**设 `upsert-materialize=NONE`，**不设 `sequence.field`**。
5. **拒绝表**：`dwd_behavior_event_reject`，append，`dt`＝**摄入日（处理时间）**，不是事件日；`reject_reason` 封闭三值 `schema_invalid`/`value_out_of_range`/`time_out_of_range`，**三值里没有 `duplicate`**（重复走幂等吸收通道）。
6. **`replay_10_ods.sql` / `replay_20_dwd.sql`** + `make replay LAYER=` 截断开关 + 逐层 `layer=… rows=…` 日志行。
7. **DIM/DWS/ADS/迟到计数表的 DDL 先落盘**（形状一次对齐），但本票只跑到 DWD 为止。

## Acceptance criteria

- [x] `make replay LAYER=dws` 在本票阶段**只跑到 DWD 就停住**（03c 未完成，DIM 段为空或跳过），日志逐层有 `layer=ods rows=…`、`layer=dwd rows=…`
- [x] 七张表的**分区键、主键、bucket、表属性与 spec §5.2 形状表逐项一致**（逐条列表核对，不靠「看着对」）
- [x] 分区键统一 `dt STRING` = `yyyy-MM-dd` **UTC**，全表无 `hour` 分区
- [x] DWD/DIM/DWS/ADS 四张主键表均 `upsert-materialize=NONE` 且**未设 `sequence.field`**；主键表默认 **MOR 而非 MOW**（未开 `deletion-vectors.enabled`）
- [x] 拒绝表 `dt` 取**摄入日（处理时间）**而非事件日；`reject_reason` 取值域**恰为三值**，`SHOW` 或等价手段能证明**没有 `duplicate`**
- [x] ODS `payload` 确为原文零解析（抽查若干行与 dump 源逐字节相同）
- [x] 门槛批落 `data/` 且被 `.gitignore` 覆盖；`git check-ignore` 验证通过
- [x] **零业务 jar**（原第 8 条，按 §D4/F6 口径改准）：不新增**业务** jar、不需要 JDK/Maven；`ADD JAR` 一条也没有。环境类 jar `paimon-hadoop-uber-2.1.0.jar` 经用户裁决 A 批准、构建期 `COPY` 进镜像（归**部署面**，见 §D1/§D4），不计入本条
- [x] **不重读 Kafka**：门槛批来源是 dump 文件，不是 Kafka topic
- [x] 本票的 compose 改动仍满足 impl-03a 的部署约束（不推翻它）

## Notes

- **强序入口**：impl-02 已结清，spec §4.3 的 P1 闸门**已开**（impl-02e，2026-10-22）。
- 「单一 catalog（库名 `ecom_lake`）」原文混说 catalog 与 database 两层（2026-10-21 签，附录 A·M）——落地时 catalog 与 database 分别取名，别混。
- **官方未给出的，按 spec §8.3 登记、不臆造**：`sys.compact` 对主键表的逐句适用声明（本票还不涉及 compact，但遇到即登记）。
- **不实现**（spec §8.2）：`sequence.field`（同 `event_id` 重复行该值相同 ⇒ 回落内部序，白设；靠重放确定性）· 排序 compaction（官方逐字仅支持 `bucket=-1`＋batch，本仓主键表是固定 bucket）· lookup join / zipper / SCD（Paimon 全站无官方能力）· 「离线重算覆盖实时行」（记 COULD 留档）。
- 时态维 join 的形态（batch 区间 join，非 lookup）在 03c 落地；本票只写 DDL。
- 03d 之前**不写任何断言**，`assert_lake.sql` 在本票可不存在。
---

## Answer（2026-10-22 收口）

**一句话**：`make replay LAYER=dwd` **现产门槛批**→建十一表→跑到 DWD 停住，实机输出 `layer=ods rows=1048576` / `layer=dwd rows=1048576` / `dwd_reject rows=0`；十一表 DDL 已按 §5.2 逐项核对；全链 SQL、零**业务** jar（环境类 hadoop jar 属部署面，见 §D4）、不读 Kafka。

### A·1 实机证据（2026-10-22 本会话产生，可直接采信）

> ⚠️ **本块于 2026-10-23 按实机输出重取**。旧块有两处与代码不符：
> ① 它写的是 `layer=reject rows=0`，而代码早已改名 `dwd_reject rows=0`
> （`sql/replay_20_dwd.sql` 尾注解释了为什么拒绝表不是第六层）；
> ② 它写 `[replay] 完成` + 隐含 exit 0，而 `LAYER=dws` 在本票阶段**故意**非 0 退出
> （DIM 未实现 ⇒ exit 3、make 归一化为 2）。谎报成功正是 §P0-2 第 5 项修掉的东西。

```
$ make replay LAYER=dwd          # 本票的绿路径
[replay] LAYER=dwd ⇒ 跑到该层为止（含；不是只跑该层——层间有数据依赖）
[replay] -- 门槛批已存在（data/threshold/raw.jsonl），跳过现产（强制：FORCE_GEN=1）
[replay] >> catalog  (replay_00_catalog.sql)
[replay] >> ddl  (replay_01_ddl.sql)
[replay] >> ods  (replay_10_ods.sql)
[layer] layer=ods rows=1048576
[replay] >> dwd  (replay_20_dwd.sql)
[layer] layer=dwd rows=1048576
[layer] dwd_reject rows=0
[replay] -- 停在 dim 之前（LAYER=dwd 的边界）
[replay] 完成（请求范围全部跑完）
EXIT=0

$ make replay LAYER=dws          # 诚实非绿：DIM 未实现
…（同上，到 dwd_reject rows=0）
[replay] -- dim：sql/replay_30_dim.sql 不存在
[replay] ✗ 层 dim 未实现（sql/replay_30_dim.sql 不存在）。
[replay]   本阶段（impl-03b）范围内的绿路径是：make replay LAYER=dwd
make: *** [Makefile:92: replay] Error 3
EXIT=2
```

（依据：spec §5.3 五标签只有 ods/dwd/dim/dws/ads；`dwd_reject` 是 dwd 层的旁路产物，
不是第六层。退出码语义见 `scripts/lake_replay.sh` 头注。）

⚠️ 拒绝表 `rows=0` 是**正确**结果，不是没跑：门槛批由生成器现产，`validate_event` 已在生成侧筛过，故 1048576 行全部合法。三值分流能力另用 7 行注入脏批单独证明（见 A·4）。

### A·2 分发完备与迟到的实测数字

| 判据 | 实测值 |
|---|---|
| ODS `count(distinct event_id)` | 1048576 |
| DWD `count(distinct event_id)` | 1048576 |
| 拒绝表 `count(distinct event_id)` | 0 |
| DWD `is_late=0` | 917260 |
| DWD `is_late=1` | 131316（`late_lag_ms` 51 … 258090735） |
| ODS `dt` 分区数 / 范围 | 1 / 2026-10-22（落地日＝今日，正确） |
| DWD `dt` 分区数 / 范围 | 3 / 2024-01-01 … 2024-01-03（事件日 UTC，正确） |

事件类型分布与 `data/threshold/manifest.json` **逐类相等**：page_view 469924 / view_item 338451 / add_to_cart 152534 / begin_checkout 57688 / purchase 29979。

`is_late` 的 131316 与独立离线测算的 `lag>0` = 131316 **逐位相同** ⇒ 到达序语义落地正确，A3 不会退化成恒真。

### A·3 逐字节 payload 判据

取 dump 前 3 行（src_seq 0/1/2），两侧 MD5 逐行相同：

| src_seq | dump 字节/字符 | Flink `CHAR_LENGTH` | MD5（两侧同值） |
|---|---|---|---|
| 0 | 323 / 317 | 317 | `798b53ff48781dc996f88aa15c66ec2d` |
| 1 | 320 / 314 | 314 | `960cf6ac5ee580ae2fe4380a4ed4357c` |
| 2 | 321 / 315 | 315 | `aec56dbcdafb6c5637f095d562c7ca6a` |

字节数与字符数差 = 3（中文 UTF-8 三字节），符合预期。**payload 存的是整行原文**（含 `_src_offset`），不再只存 `{"session_id":…}` 子串。

### A·4 拒绝表三值分流（7 行注入脏批，独立表 `zz_dirty_probe`，跑完即 DROP）

| 注入 | 期望 | 实测 `reject_reason` |
|---|---|---|
| 合法 page_view | 接受 | `<ACCEPTED>` |
| `event_id` 为空串 | schema_invalid | `schema_invalid` |
| purchase 缺 amount | schema_invalid | `schema_invalid` |
| `event_type='refund'` | value_out_of_range | `value_out_of_range` |
| purchase `amount=-5` | value_out_of_range | `value_out_of_range` |
| `event_time` 远在过去 | time_out_of_range | `time_out_of_range` |
| `event_time=9999999999999` | time_out_of_range | `time_out_of_range` |

四类错误各命中一次、合法行放行一次。**取值域无 `duplicate`**：三值来自 DWD SQL 里 `reject_reason` 的 CASE 分支（`schema_invalid` / `value_out_of_range` / `time_out_of_range` 各一支，其余 `CAST(NULL AS STRING)`），源码层面即可证伪。

拒绝表 `dt` 取 `DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd')`＝**处理时间**；DWD 的 `dt` 取 `DATE_FORMAT(TO_TIMESTAMP_LTZ(event_time,3),'yyyy-MM-dd')`＝**事件时间**。两者不同源，符合 §5.2。

### A·5 七表形状逐项核对（`SHOW CREATE TABLE` + `$options` 系统表实机读回）

| 表 | 主键 | 分区 | bucket | upsert-materialize |
|---|---|---|---|---|
| `ods_behavior_event` | 无（append） | `dt` | `-1` | — |
| `dwd_behavior_event` | `(dt, event_id)` | `dt` | `1` | `NONE` |
| `dwd_behavior_event_reject` | 无（append） | `dt` | `-1` | — |
| `dim_item` / `dim_category` / `dim_user` | `(item_id\|category_id\|user_id, valid_from_ms)`（真实列名，见 §A·10） | **不分区** | `1` | `NONE` |
| `dim_region` / `dim_channel` | `(province)` / `(channel)` —— **无拉链**，故无 `valid_from_ms` | **不分区** | `1` | `NONE` |
| `dws_subject_window` | `(dt, subject_type, subject_id, window_start)` | `dt` | `1` | `NONE` |
| `ads_window_metric` | `(dt, metric_name, dim_key, window_start)` | `dt` | `1` | `NONE` |
| `dwd_late_event_count` | 无（append） | `dt` | `-1` | — |

`SHOW TABLES` 实机返回 11 张表（7 张规格表 + 维度/计数扩展表）。
**`sequence.field` 与 `deletion-vectors.enabled` 在 \$options 里均不出现** ⇒ MOR 默认、无 MOW。

### A·6 零业务 jar / 不读 Kafka / compose 不推翻 impl-03a

- `sql/` 四个文件（`00_catalog` / `01_ddl` / `10_ods` / `20_dwd`）全为 SQL；`ADD JAR` 出现 0 次。
- **业务 jar 零新增**；唯一新增的是环境类 `paimon-hadoop-uber-2.1.0.jar`（用户裁决 A，见 §D1/§D4），构建期 `COPY` 进 `/opt/flink/lib`，不是 `ADD JAR` 语句、不进 `sql/`。
- ODS 输入是 `'connector'='filesystem','format'='raw','path'='file:///data/threshold/raw.jsonl'` ⇒ **不碰 Kafka**。
- compose **新增四条只读挂载行**（`./data:/data:ro` 与 `./sql:/sql:ro`，两个挂载点、JM/TM 各一行），未改动 impl-03a 的 `paimon-warehouse:/warehouse` 同路径约束、ports、内存、healthcheck、镜像。

### A·7 本票相对首版的四处实质修正（都是实测逼出来的）

1. **payload 存整行**：`amount`/`currency`/`quantity`/`order_id` 在 dump 行的**顶层**，payload 里只有 `session_id`。首版从 payload 取 ⇒ 恒 NULL ⇒ 29979 条 purchase 全被判 `schema_invalid`。修正后 `with_amount=29979`（全中）。
2. **`UNIX_TIMESTAMP(TIMESTAMP_LTZ)` 不存在**，改无参 `UNIX_TIMESTAMP()*1000`。
3. **`time_out_of_range` 下界用 `MIN(event_time)-120000`** 而非 `MAX`；SRS §3.3 是**相对下界**。
4. **`is_late` 用到达序**（running max by `src_seq`），不是「批次最大 event_time」。后者会让 `is_late` 全表为 1、A3 退化成恒真断言。

### A·8 已知未做（明确留给 03c/03d）

- `sql/replay_30_dim.sql` / `40_dws.sql` / `50_ads.sql` 尚未落盘 ⇒ `LAYER=dws` 到 DIM 即停（本票设计要求如此）。
- 不写任何断言（`assert_lake.sql` 不存在），A3 的断言归 03d。
- 幂等「重放两遍同结果」的 `crc32` 判据：Flink SQL **无 `CRC32` 函数**（实测 `No match found for function signature CRC32(<CHARACTER>)`）⇒ 03d 须改用 `MD5` 或其它可用摘要。**本票已用 `INSERT OVERWRITE` 保证重跑确定性**。
- 小 N 的 `_params` 会话余量不足（N≤11 会少产行、N≤4 出不了 3 个 dt）——生产规模 1048576 不受影响，已记在 `tests/test_threshold_batch.py` 的已知限制里。

### A·9 本会话踩到的新坑（补 E7）

- `CRC32` 在 Flink SQL 不存在（Calcite 内置集里没有）。
- `sql-client.sh -f` 的**每一行**都被当独立语句解析：`USE CATALOG paimon; USE ecom_lake;` 写在同一行 ⇒ `only single statement supported`。
- `CREATE TEMPORARY TABLE` **不跨 sql-client 会话**存活；临时表 + INSERT 必须同一次 `-f` 执行。
- `TABLEAU` 结果模式默认截断宽列（`payload` 只剩 30 字符）⇒ 做逐字节判据不能靠表格，必须用 `MD5()`/`CHAR_LENGTH()`。
- 本机 WinNAT 排除 TCP 8024-8123（含 8081）：重建 flink 容器须叠加 `.scratch/tmp/port-override.yml`，否则 `ports are not available`。

### A·10 两处与原文不同、但属**有意为之**的口径声明

1. **DIM 三个拉链表的主键列名**：spec §5.2 写完「拉链字段改名 `start_date`/`end_date`
   →`valid_from_ms`/`valid_to_ms`」后仍写「主键 `(item_id, valid_from_ms)`」，
   而 `id` 在表头列里是**主键语义的占位写法**。落地取 `(item_id, valid_from_ms)` /
   `(category_id, valid_from_ms)` / `(user_id, valid_from_ms)`——即用**真实列名**而非
   字面 `id`。理由：`id` 不是任何一张表的列名，字面照写会建不出来。
   **若你要求逐字 `id`，须先改 §5.2 的列清单**（两处本就互相约束）。
2. **DIM 静态表主键**：§5.2 写 `(province)`/`(channel)`，落地即
   `PRIMARY KEY (province)` / `PRIMARY KEY (channel)`——逐字一致，无偏离。
   此处列出仅因为「无拉链 ⇒ 无 `valid_from_ms`」容易被后来者误当作漏写。

### A·11 编排链多了一步 `replay_01_ddl.sql`（相对 spec §5.3 的清单）

§5.3 的代码块列了 `00_catalog` / `10_ods` / `20_dwd` / `30_dim` / `40_dws` / `50_ads`
/ `90_maintain` / `assert_lake`，**没有** `01_ddl`。落地必须补这一步：七表形状
（§5.2）不落在任何既有文件里，而从零跑时层脚本的 `INSERT` 会立刻报
`Cannot find table ... in any of the catalogs`（本会话实测过一次）。

- `01_ddl` 全用 `CREATE TABLE IF NOT EXISTS` ⇒ 重复执行幂等，可常驻链首。
- 它**不计入 `LAYER=` 截断**：`LAYER=dwd` 也要建表，否则同样无表可写。
- 因此 `sql/replay_01_ddl.sql` 是**新增的编排步**，不是对 §5.3 的改写；
  §5.3 若要逐字对齐，应补一行 `sql/replay_01_ddl.sql -- 七表形状（§5.2）`。

### A·12 八张主键表的 `$options` 实机读回（一条 SQL 覆盖验收第 4 条）

```sql
SELECT 'dwd_behavior_event' AS tbl,
       MAX(CASE WHEN `key`='bucket' THEN `value` END) AS bucket,
       MAX(CASE WHEN `key`='table.exec.sink.upsert-materialize' THEN `value` END) AS mat,
       MAX(CASE WHEN `key`='sequence.field' THEN `value` END) AS seq,
       MAX(CASE WHEN `key`='deletion-vectors.enabled' THEN `value` END) AS dv
FROM `dwd_behavior_event$options`;   -- 其余七表同构，UNION 或逐条
```

| 表 | bucket | upsert-materialize | sequence.field | deletion-vectors.enabled |
|---|---|---|---|---|
| `dwd_behavior_event` | 1 | NONE | NULL | NULL |
| `dim_item` | 1 | NONE | NULL | NULL |
| `dim_category` | 1 | NONE | NULL | NULL |
| `dim_user` | 1 | NONE | NULL | NULL |
| `dim_region` | 1 | NONE | NULL | NULL |
| `dim_channel` | 1 | NONE | NULL | NULL |
| `dws_subject_window` | 1 | NONE | NULL | NULL |
| `ads_window_metric` | 1 | NONE | NULL | NULL |

`sequence.field` 与 `deletion-vectors.enabled` 全为 **NULL**（= 键不存在），
不是「值为空字符串」——即**未设**，与 §5.2「不设 `sequence.field`」「MOR 而非 MOW」逐项一致。

### A·13 已知口径收窄：「零解析」只限定在 payload 这一列（**须向用户明示**）

> 本节是票面要求「向用户明示」的唯一一处口径收窄（§P0-3 第 6 项）。此前
> `sql/replay_01_ddl.sql` 的 ODS 段注释已引用本节，但正文没写 —— 现在补齐。
> 它不是隐式偏离：形状表原话与落地面在**同一张表**上指了两件不同的事，必须择一说清。

**收窄的内容**：spec §5.2 的 ODS 行写「`payload` 原文列**零解析**」。落地后，
`ods_behavior_event` **没有**对 `payload` 做任何解析（不拆子对象、不做类型归属、
不做时间单位换算）—— 这一条**完整成立**，未收窄。

但本表**另有 11 个信封列**（`event_id` / `event_time` / `user_id` / `item_id` /
`category_id` / `event_type` / `schema_version` / `page` / `source` / `channel` /
`province`）以及 2 个运行列（`src_seq` / `dt`），它们是 `replay_10_ods.sql` 用
`JSON_VALUE` 从**同一行原文**里投影出来的。也就是说：**整个 ODS 层每行都做了 JSON 取值**
（13 个路径），只是没有解析 payload 的内容。

**为什么必须这样收窄，而不是「ODS 一个字段都不拆」**：

1. 形状表的 ODS 行**没有列全**信封列，但分区键 `dt`、下游 DWD 的 join 与校验
   都**必须**有 `event_id`/`event_time`/`event_type` 等列 —— 表里没有这些列，
   §5.2 自己就不可实现（`PARTITIONED BY (dt)` 都需要 `dt` 列存在）。
2. 「一切解析归 DWD」的原始出处是 gdd **Q25**，它讲的是 **payload 内容**
   （嵌套业务字段的归属与口径），不是「信封字段不许投影成列」。
3. `payload` 存的是**整行原文**（见 `replay_10` 头注），13 个投影列不丢失任何字节：
   原文逐字节可复原，验收第 6 条（与 dump 源逐字节相同）实测通过（§A·3）。

**因此本票对外表述统一为**：
> ODS 的「零解析」= **payload 列零解析**（不拆、不推断、不换算）；
> 信封字段的结构化投影是形状表自身「按列存 `dt`/信封列」所隐含的要求，
> 不属于「解析 payload」。

审查与接手者若认为该收窄不可接受，可选的替代只有两条，都会动已验收的形状表：
① 从 §5.2 的 ODS 行删掉「零解析」措辞；② 把 13 个列改成 `payload` 单列 + 视图。
两条都超出本票授权面（改规格），故本票按上面口径落地并在此登记。

---

## Progress（2026-10-22 收口后快照 —— 供接手者续跑）

> **本节取代下方全部 §P1–§P7 的「进行中」表述**；那些小节保留为过程档案，
> 其中「停止点：Paimon 无 Hadoop 类」早已解除（见 §D），别再照着它停。
>
> **当前状态一句话**：功能面已跑通并实机取证（`make replay LAYER=dwd` 现产→建表→ODS→DWD 全绿），
> 验收 12 条**全部勾选**（§A 有逐条证据）；审查遗留 6 条**已全部处理完**（见 P0-3 表），
> §A·13 正文**已补**；工作区**已提交**（P0-4）。

### P0-1 · 已实机取证的事实（可直接采信）

| 判据 | 实测 | 取证方式 |
|---|---|---|
| `make replay LAYER=dwd` | 绿，退出 0 | 本会话终端，日志见 §A·1 |
| `layer=ods rows=` | 1048576 | §A·1 |
| `layer=dwd rows=` | 1048576 | §A·1 |
| `dwd_reject rows=` | 0（门槛批全合法，正确） | §A·1 |
| 从空 warehouse 跑到 DWD | 绿（先 DROP 三表再跑） | §A·1 |
| ODS payload 与 dump 逐字节 | 3/3 行 MD5 相同 | §A·3 |
| 八张主键表 options | bucket=1 / NONE / 无 seq / 无 dv | §A·12 |
| 拒绝三值 | 7 行注入脏批四类各命中 | §A·4 |
| `data/` 被 gitignore | `git check-ignore -v` 命中 `.gitignore:10` | §A·6 |
| `ADD JAR` | sql/ 内 0 次 | §A·6 |

### P0-2 · 本轮（收口后）改了什么 —— 都是代码审查逼出来的

两个审阅子代理（spec 轴 / standards 轴）各交一份。**4 个 BLOCKER 已全部修掉并复验**：

1. **`lake_replay.sh` 对死容器返回 0**（standards 轴实测复现 `EXIT=0`）。
   根因：判据只看 stdout 里有没有 `[ERROR]`，而「容器根本不存在」时一个 `[ERROR]` 都没有。
   修法：先查 `docker exec` 的 `$?`，非 0 直接 `return 1`；另加**强制正向证据** ——
   每层必须真的出现自己的 `layer=<层> rows=` marker（absence of error ≠ success）。
   复验：`LAKE_FLINK_CONTAINER=eco-no-such-container` ⇒ `EXIT=1` 且打印「docker exec 退出 1」。
2. **Makefile 依赖未跟踪的 exec 位**（`core.filemode=false`，scripts/ 全 100644）⇒ 干净检出会
   `Permission denied`。改成 `bash scripts/lake_replay.sh`，与 Makefile 其余脚本调用（:53/:75/:77）一致。
3. **零 jar 契约与部署矛盾**（spec 轴 BLOCKER）：镜像里 `COPY` 了 `paimon-hadoop-uber-2.1.0.jar`，
   而验收第 8 条仍写「不新增 jar」、Answer 仍宣称「零 jar」。§D4/F6 早就**要求收口时改写**该条却漏做了。
   已按 §D4 的裁决口径改写为「**零业务 jar**；环境类 hadoop jar 归部署面」，并同步 headline 与 §A·6 标题。
4. **`make replay` 没有前置生成器现产**（spec §5.1 逐字要求「前置生成器 `dump --events … --seed <固定>` 现产」）：
   此前批次是手工产物、seed 只固定在测试里 ⇒ **干净检出根本跑不了这条链**。
   已在 `lake_replay.sh` 链首补 `gen` 步：`uv run python -m ecomgen dump --events 1048576 --logical-span 3d --seed 20241022`，
   带守卫（`raw.jsonl` 已存在且非空就不重产；`FORCE_GEN=1` 强制重产）。

顺带修掉的 SHOULD-FIX：

| 项 | 处置 |
|---|---|
| 默认 run 只跑 2/5 层却 exit 0（谎报成功） | 改成**诚实的退出码**：0=请求范围全跑完 / 3=范围内有未实现的层 / 1=执行失败。复验：`LAYER=dws` ⇒ 非 0（脚本 3、make 归一化为 2）。同时把本阶段绿路径写明为 `LAYER=dwd` |
| SRS §3.4 冻结目标名被 `replay-ods`/`replay-dwd` 越界 | 删除两个别名目标，只留 `replay`（冻结名不动） |
| 「七张表」实为 11 张 | 文件头注改「十一张表」 |
| bucket 注释说「不写」而 `WITH` 里写了 -1 | 注释改准：append 表显式写 `-1`=不分桶，与主键表 `bucket=1` 是两套语义，写出来更可核对 |
| A3 等价式被写成已成立 | 注释加「本票不写这张表（无 INSERT 指向它），A3 此刻**必然不成立**，写入归 03d」 |
| `layer=reject` 是 §5.3 五标签外的第六个 | 改名 `dwd_reject rows=`，不再伪装成一层 |
| `seq/format` 等无关项 | `tests/test_verify_gaps.py` 空行、compose 末尾换行已恢复 |

### P0-3 · ✅ 审查发现已**全部处理完**（2026-10-23 本会话）

原 6 条逐条处置，均已落进代码/票面并有测试守卫：

| # | 审查发现 | 处置 | 守卫 |
|---|---|---|---|
| 1 | `replay_20_dwd.sql` 的 `-120000`/`+300000` 是 Python 常量的影子副本（无守卫、静默漂移） | 新增 `tests/test_replay_sql.py`：正反两向守卫 | `test_dwd_lower_bound_literal_matches_events_constant` / `test_dwd_upper_bound_literal_matches_events_constant` / `test_dwd_has_no_stale_shadow_constants`（后者挡住「改了 Python 却把旧字面量留在 SQL」） |
| 2 | `truncate_proportionally` 是**死代码**，注释自相矛盾 | **删除**函数 + 其 4 条测试 + 上方那段自相矛盾的注释块（`max_events` 才是解，等比裁从未接进生产路径） | 无残留引用（`grep` 零命中） |
| 3 | `max_events` 截停时 `_session_steps` 记完整步进表 | 改为记**实际发出**的步（`emitted_steps` 逐条 append），空会话记空列表 | `test_producer.py::test_session_steps_records_only_emitted_steps`（断言「步数 == 该会话事件数 − 1」且每步都有对应事件） |
| 4 | carrier-session 公式抄三份 | 收进 `ThresholdBatchPlan.carrier_sessions`（唯一权威 `_carrier_sessions()`），`cli._params` 与两处测试都改读 `plan.carrier_sessions` | 三处均已无字面公式（`grep '2\.0 \* 1\.2'` 零命中） |
| 5 | `replay_00_catalog.sql` 缺 `result-mode=TABLEAU` | 已加 `SET 'sql-client.execution.result-mode' = 'TABLEAU';`（含注释说明「今日无害、将来加 SELECT 会炸」） | —— |
| 6 | §A·13 欠正文 | **已补**（见 §A·13，含「这是本票唯一需向用户明示的口径收窄」的完整论证与两条替代路） | —— |

**另修一个本会话新发现的实机缺陷**（不在原审阅清单里，是做验收取证时撞到的）：

> **`layer=ods rows=` 会随「跑过几次、跨不跨天」漂移。**
> ODS 的 dt 是**落地日**，append 表的 `INSERT OVERWRITE` 只改写**本次 dt 分区**
> （历史落地日分区按设计保留 —— 那正是「数据哪天到的湖」的语义）。隔天再跑一遍，
> 整表 1048576 → **2097152** / 2 个分区，而本次确实只写了 1048576 条。
> 整表计数让日志行无法回答「本次写全了没」。
> ⇒ `replay_10_ods.sql` 的层报告加 `WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd')`，
> 与写入侧同源。**复验**：加过滤后再跑一遍，`layer=ods rows=1048576`（此前是 2097152）。
> 守卫：`test_ods_report_counts_only_the_current_landing_partition` +
> `test_ods_insert_targets_current_landing_day`（报告与写入必须同源，否则过滤出空分区）。
> ⚠️ **不要**为了「让数字好看」把 ODS 改成整表覆盖：落地日分区跨天累积是形状表定的语义。

### P0-4 · 工作区状态：**已提交**（2026-10-23 本会话）

上一轮「`git add -A` 被中止」在此轮已解决：P0-3 全部处理完 → 复跑
`pytest`（**183 passed**）+ `ruff check` / `ruff format --check`（全过）+
`basedpyright`（**0 error**，6 个 warning 全在 `src/ecomgen/__init__.py` 的
`__all__`，是既有问题，与本票无关）→ 提交到 `feat/ecom-pipeline-impl`。

```
 M Makefile                         replay → bash 脚本；删两个别名；PHONY 归并
 M docker-compose.yml               ./data 与 ./sql 只读挂两容器（4 行）；末尾换行已修
 M docker/flink/Dockerfile          COPY paimon-hadoop-uber jar（裁决 A，impl-03a 遗留）
 M scripts/fetch-paimon-jar.sh      同上，取 jar 的脚本
 M src/ecomgen/cli.py               --events / --logical-span / _src_offset / dt 摘要
 M src/ecomgen/producer.py          max_events / plan_threshold_batch / watermark_step_ms
 M tests/test_verify_gaps.py        空行（无关噪声）
 M .scratch/.../impl-03b-*.md        本票
?? scripts/lake_replay.sh           编排链（gen→catalog→ddl→按层，含诚实退出码）
?? sql/replay_00_catalog.sql
?? sql/replay_01_ddl.sql            十一表
?? sql/replay_10_ods.sql
?? sql/replay_20_dwd.sql
?? tests/test_threshold_batch.py    50 例，全绿（test-author 产出）
```

`data/threshold/raw.jsonl`（329 692 935 B / 1 048 576 行）与 `manifest.json` 被 `.gitignore:10` 覆盖，**不入库**。

### P0-6 · 第二轮审阅（spec 轴）逼出的两处 BLOCKER，已修并复验（2026-10-23）

第一轮（§P0-2/P0-3）修完 6 条后，本轮又做了一次两轴审阅。**spec 轴抓到两条 BLOCKER**，
都是「上一轮的修法只修了一半」：

| # | 问题 | 实机证据 | 修法 | 复验 |
|---|---|---|---|---|
| 1 | **`gen` 步根本跑不起来** —— `scripts/lake_replay.sh` 用 `uv run python -m ecomgen`，而本包**没有** `src/ecomgen/__main__.py`（`pyproject` 只声明 console script `ecomgen = "ecomgen.cli:main"`） | `No module named ecomgen.__main__; 'ecomgen' is a package and cannot be directly executed`（审阅实跑 rc=1） | 改 `"$UV" run ecomgen dump …` | 实跑 `uv run ecomgen dump --events 200 …` → 退出 0、`dt_count=3` |
| 2 | **ODS 漂移修复只做了一半** —— §P0-3 只给 ODS 的**层报告**加了落地日过滤，而 `replay_20_dwd.sql` 的 `FROM ods_behavior_event o` 仍读**整表** ⇒ 隔天重跑会把前一天 1 048 576 行再喂一遍，DWD 翻倍，而 spec §5.4 的 A1 是**全局比对**（`ods_distinct = dwd_distinct + reject_distinct`）⇒ 等式当场破。且 `replay_10_ods.sql` 里「整表计数即本次结果」那句注释是**错的** | 第二遍 ODS 表 2 097 152 行（§P0-3 已实测过该现象） | 新增 `v_ods_scope` 视图（`WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd')`），**批次基准量**与**校验视图**两个读取点都改从它取；并改准 `replay_10` 的错误注释 | 重跑 `LAYER=dwd`：`layer=ods rows=1048576` / `layer=dwd rows=1048576` / `dwd_reject rows=0`，退出 0。守卫：`test_dwd_reads_only_the_current_ods_landing_partition` + `test_ods_and_dwd_share_one_landing_day_filter` |

另外两条审阅发现（**已修**）：

- **(c)-5「逐条同源」是假的**：`amount="abc"`（字段存在但不可转数字）在 Python 侧
  `float('abc')` 抛 ValueError ⇒ `value_out_of_range`；而 SQL 侧 `TRY_CAST` 静默变 NULL
  ⇒ 被「purchase 缺 amount」抢走判成 `schema_invalid`。已在 `v_checked` 里保留
  `amount_raw` 并新增一支 `WHEN amount_raw IS NOT NULL AND amount IS NULL`
  ⇒ `value_out_of_range`，且该支**排在**「purchase 缺 amount」之前。
  **两侧实测对照**：

  | 注入 | Python `validate_event` | SQL `reject_reason` |
  |---|---|---|
  | `amount="abc"` | `value_out_of_range` | `value_out_of_range` ✅ |
  | `amount=-5` | `value_out_of_range` | `value_out_of_range` ✅ |
  | purchase 缺 amount | `schema_invalid` | `schema_invalid` ✅ |
  | `event_type='refund'` | `value_out_of_range` | `value_out_of_range` ✅ |

  ⚠️ 仍有一处**有意不同源**并已在 `replay_20_dwd.sql` 头注⑥ 写明：`event_time` 是
  「存在的非整数」时 Python 跳过时间规则、SQL 判 `schema_invalid`（从严，因为类型错的
  时间值不可信、不能进 dt 分区）。门槛批与脏批探针都不含该形态。
  探针 SQL 由 `.scratch/tmp/build_reject_probe2.py` 从 `replay_20_dwd.sql` **逐字抽取** CASE 生成
  （不另写一份逻辑），8 行 8 个结论，全部符合上表。

- **(b)-3 dump 行格式改为含 `_src_offset`**：这不是无关改动，是到达序语义的**必要**前提
  （§A·7 第 4 条：迟到必须按到达序定义，而 batch 下 `ROW_NUMBER() OVER ()` 不可用）。
  已在 `cli._dump` 与 `replay_10_ods.sql` 头注写明「传输元数据、`_` 前缀与业务字段区分」。
  代价是 dump 产物字节与 P1 不同 —— 属**有意为之**；P1 的哈希判据针对的是 `--seed` 确定性，
  同 seed 仍逐字节稳定（`test_dump_output_is_byte_identical_for_same_seed`）。

- **(b)-4 噪声**：`tests/test_verify_gaps.py` 删掉的那个空行**不是噪声** —— 它是
  `ruff format --check` 本来就在报的违规（HEAD 上就红）。保留。
  `Makefile` 与三个 SQL 缺的末尾换行已补齐（`replay_00_catalog.sql` 本来就有，现已一致）。

### P0-7 · 本轮复跑的全部判据（2026-10-23，全绿）

```
$ make replay LAYER=dwd      → EXIT=0
[layer] layer=ods rows=1048576
[layer] layer=dwd rows=1048576
[layer] dwd_reject rows=0
[replay] 完成（请求范围全部跑完）

$ make replay LAYER=dws      → EXIT=2（make 归一化脚本的 exit 3），停在 dim 之前，不谎报成功
$ pytest tests/ -q           → 188 passed
$ ruff check .               → All checks passed
$ ruff format --check .      → 86 files already formatted
$ basedpyright               → 0 errors, 6 warnings（全在 __init__.py 的 __all__，既有问题）
```
### P0-5 · 环境坑（接手必读，都是本会话实测）

- **`eco-flink-jm` 有 1 GB 内存上限且会被 OOM kill**：本会话实撞一次，`docker exec` 退出 **137**，
  随后 `<State.RestartCount>=5`、`health: starting`，约 45 s 自愈。
  所以「退出 137」不是 SQL 错、也不是脚本错 —— 先 `docker inspect` 看 `RestartCount`/`OOMKilled`
  与 `health`，等 healthy 再重跑。**别把它当成链路缺陷去改脚本**。
- **端口**：本机 WinNAT 排除 TCP 8024–8123（含 8081/8030/8088），重建 flink 容器**必须**叠加
  `.scratch/tmp/port-override.yml`，否则 `ports are not available`（见 §P6）。
- **路径**：`wsl -e bash -lc 'cd "/mnt/f/..." && …'` 是唯一可靠形式；`MSYS_NO_PATHCONV=1` 必须给
  任何传给 docker 的容器内路径（否则 `/sql`、`/data` 被改写成 Windows 路径）。
- **`.venv` 会被清空**：本会话中途 `.venv/` 只剩 `lib64` 与 `pyvenv.cfg`，
  `uv sync` 才恢复。测试/ruff/typecheck 前先确认 `.venv/bin/python` 存在。
- **Flink SQL 无 `CRC32`**（实测 `No match found for function signature CRC32(<CHARACTER>)`）
  ⇒ §5.4 幂等判据的 `SUM(crc32(...))` 须改 `MD5` 或其它可用摘要，交 03d。
- **`sql-client.sh -f` 每行独立解析**：`USE CATALOG paimon; USE ecom_lake;` 同一行 ⇒
  `only single statement supported`。每个语句独立成行。
- **`CREATE TEMPORARY TABLE` 不跨 sql-client 会话**存活，临时表+INSERT 必须同一次 `-f`。
- **TABLEAU 结果模式默认截断宽列**（payload 只剩 30 字符）⇒ 逐字节判据必须用
  `MD5()`/`CHAR_LENGTH()` 而非看表格。


## Progress（2026-10-22，进行中 —— 供接手者续跑）

> 本节是**进度快照**，不是 Answer。验收勾选与结论待本票收口时补。
> **停止点**：ODS 写入被「Paimon 无 Hadoop 类」挡住（见 §P4），需决策后继续。

### P1 · 已完成并实证的部分

| # | 交付物 | 状态 | 证据 |
| --- | --- | --- | --- |
| 1 | 门槛批现产（1 048 576 条 / 3 天 / 3 个 dt） | ✅ 已产 | `[ecomgen] dump 完成：1048576 条 dt=[2024-01-01..2024-01-03] dt_count=3`，耗时 44.6 s，落 `data/threshold/raw.jsonl`（329,692,935 B） |
| 2 | `sql/replay_00_catalog.sql` | ✅ 已写并实机跑通 | catalog `paimon` / database `ecom_lake` 两层分开取名（附录 A·M）；`CREATE CATALOG` 实机 succeeded |
| 3 | 七表 DDL 形状 | ✅ 已写并实机建表成功 | `sql/replay_01_ddl.sql`，11 张表全部 `CREATE TABLE` succeeded（见 §P3） |
| 4 | `sql/replay_10_ods.sql` | ⚠️ 已写，**未跑通** | 卡在 §P4 的 Hadoop 缺口 |
| 5 | compose 门槛批只读挂载 | ✅ | `./data:/data:ro` 进 flink-jm 与 flink-tm 两侧，容器内 `/data/threshold` 可见 |
| 6 | 生成器 `--events` / `--logical-span` | ✅ | 见 §P2 |

### P2 · 生成器改造（`src/ecomgen/producer.py` + `cli.py`）

新增 `ProducerParams.max_events` / `watermark_step_ms` / `epoch_base_ms`，新增
`plan_threshold_batch()` 纯函数与 `truncate_proportionally()`。**探针证伪掉两个坑**：

- **坑①「裁最后那个列表」会清空超期通道**：`_apply_late_delivery_order` 把迟到事件搬到尾部，
  `events[:N]` 取到的几乎全是正常事件。实测 seed=7 / 1075 条取前 1000：
  `correctable=31 expired=0`（全量为 `31/22`）。⇒ 改为「产够 N 条即停」。
- **坑②「步长 = 跨度 / 请求条数」算错跨度**：`--sessions 10000` 实产 **22 289** 条，
  按 N 算的步长使跨度 ×2.2。⇒ 步长按**实际条数**回算。
- **坑③（新发现）dt 会出 5 个分区而非 3 个**：迟到事件把 `event_time` 往回拨（超期档最多 300 s），
  基座压在 00:00:00Z 时最早一批落到**前一天**、末尾又顶上第 4 天。
  ⇒ `plan_threshold_batch` 把起点前移 300 s、跨度扣掉这 300 s + 1 s 余量，
  实测 dt 恰为 `2024-01-01..2024-01-03` 三个分区，三通道全保留（`31740 / 21012`）。

`dump` 现打印可解析行：`[ecomgen] dump 完成：N 条 dt=[..] dt_count=K → 路径`。

### P3 · 形状逐表核对（`SHOW CREATE TABLE` 实机输出，11 表 0 ERROR）

> **（2026-10-23 修订）** 本节原本自抄了一份形状表，与 §A·5、§A·12 内容重复且**互相矛盾**
> （本节的 `dim_item` 主键写 `(id, valid_from_ms)`，而 §A·5/§A·10 已改准为真实列名
> `(item_id, valid_from_ms)`；`dws_subject_window` / `ads_window_metric` 的键少列；
> 且「五张主键表」实为**八张**）。同一份证据抄三处，一处改、三处不同步 —— 正是本仓
> impl-02e 记过的病。现改为**单一权威**：形状逐表以 **§A·5**（主键/分区/bucket/属性）
> 与 **§A·12**（`$options` 实机读回）为准，本节不再维护第二份。

核对入口（三处一致，改其一必改另二）：
- `sql/replay_01_ddl.sql` —— 形状的**定义**（11 张表）
- §A·5 —— `SHOW CREATE TABLE` 实机读回的主键/分区/bucket/属性
- §A·12 —— 八张主键表 `$options` 的 bucket/materialize/sequence.field/deletion-vectors

全表**无** `deletion-vectors.enabled`（⇒ MOR 非 MOW）、**无** `sequence.field`、**无** `hour` 分区。
**八张**主键表均 `bucket=1` + `upsert-materialize=NONE`。

### P4 · ⛔ 阻塞：Paimon 在 `file:` warehouse 上缺 Hadoop 类（需决策）

**症状**（本票实机，2026-10-22）：任何**实际读写 Paimon 表**的语句都失败 —

```
INSERT INTO ta VALUES (...)          → java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration
SELECT count(*) FROM ods_behavior_event → 同上
```

而 `CREATE CATALOG` / `CREATE TABLE` / `SHOW DATABASES` / `SHOW TABLES` **全部成功**
⇒ 元数据面通，**文件 IO 面不通**。

**已排除的猜测**（逐条实证，不是推断）：

| 猜测 | 实证结果 |
| --- | --- |
| `fs.default=LocalFileIO` 能绕开 | ❌ 仍报同一异常 |
| `fs.file.impl=LocalFileIO` 能绕开 | ❌ 仍报同一异常 |
| 换 `file.format`=orc/avro/parquet + `file.compression=none` | ❌ `CREATE TABLE` 就失败 |
| 只是 DDL 写错 | ❌ 新建最小表 `ta(id, v, dt)` 的 `INSERT` 同样失败 |
| jar 里其实有 Hadoop | ❌ 解包 `paimon-flink-2.2-2.1.0.jar`（27 319 entries）**零** `org/apache/hadoop/**`；只有 Paimon 自己的 `org/apache/paimon/fs/hadoop/HadoopFileIO` 包装类 |

**根因**：Paimon bundled jar **不含 Hadoop 依赖**，但 `FileIO` 的 ServiceLoader
发现列表里有 `HadoopFileIOLoader`（`META-INF/services/org.apache.paimon.fs.FileIOLoader`
逐字列出 `LocalFileIOLoader` / `HadoopFileIOLoader` / `HadoopViewFsFileIOLoader`），
实例化时触发 `org.apache.hadoop.conf.Configuration` 解析 ⇒ 类缺失即失败。
基线镜像 `apache/flink:2.2-java17` 的 `/opt/flink/lib` 只有 15 个 jar、无 hadoop；
`/opt/flink/opt` 只有 s3/oss/azure/gs 的 fs 插件，**都不含 hadoop 核心类**。

**与票面验收的张力**：票面第「全 SQL、零 jar」条要求「不新增 jar、不需要 JDK/Maven」。
补 Hadoop 进镜像会**新增 jar** ⇒ 与本条正面冲突，故**必须由用户裁决**，我不自行加 jar。

**待裁决的两个方向**：

- **A（改部署面）**：impl-03a 的镜像补一个 hadoop-client 类 jar（或用 official flink
  带 hadoop 的镜像变体）。代价：违反「零 jar」的字面；须登记为环境依赖而非业务 jar。
- **B（改存储面）**：warehouse 从 `file:/warehouse` 换到对象存储/带 hadoop 的 scheme
  （如 s3 插件已在 `/opt/flink/opt`）。代价：动 impl-03a 已验收的部署面，且引入外部依赖。
- **C（查 Paimon 官方 `file:` 用法是否本来就要 hadoop）**：若官方文档明确 `file:` 需要
  hadoop，则这是**官方前提**而非本仓缺陷 ⇒ 应按 spec §8.3 登记，并回改 impl-03a。

**建议**：先做 C 取证（查 Paimon 官方是否要求 `file:` 配 hadoop），再据结论选 A/B。
**本会话已停在此处，未改 Dockerfile、未加任何 jar。**

### P5 · 未完成（接手清单）

> **（2026-10-26 归正，impl-06f-2）** 以下 4 项已于 2026-10-22/23 施工全部兑现
> （本节写于进行中快照，收口后勾选状态未再维护）。逐项兑现证据见行内标注；
> 勾选状态以票面 Acceptance criteria（12 条全勾）与顶行 `Status:` 为准。

- [x] `sql/replay_20_dwd.sql`（ODS → DWD + 拒绝旁路，`reject_reason` 三值 CASE）——兑现于 §A·1（`layer=dwd rows=1048576`、`dwd_reject rows=0`，EXIT=0）＋ §A·4（7 行注入脏批，三值分流四类各命中）＋ §P0-7（2026-10-23 复跑仍全绿）
- [x] `make replay LAYER=` 截断开关 + 逐层 `layer=… rows=…` 日志行——兑现于 §A·1（`LAYER=dwd` 绿 / `LAYER=dws` 诚实退 2）＋ §P0-2 第 1 项（`lake_replay.sh` 强化后接进 Makefile `replay:` 目标，现行 Makefile 即 `bash scripts/lake_replay.sh`）
- [x] DIM/DWS/ADS DDL 已落盘（✅ 见 §P3），但本票只跑到 DWD 为止——兑现于 §A·5 / §A·12（11 表 `SHOW CREATE TABLE` 0 ERROR、八张主键表 `$options` 实机读回）；后续层贯通归 [impl-03c](impl-03c-dim-dws-ads-five-layer-replay.md)（五层 exit 0、ads=90348）
- [x] 验收 12 条逐条核对（目前形状类已可核对，链路面被 §P4 卡住）——§P4 阻塞已由 §D 裁决 A 解除；12 条核对兑现于票面 Acceptance criteria 全勾 ＋ §A·1–A·13 逐条证据 ＋ §P0-7 复跑全绿

### P6 · 部署面现状（重要，接手者须知道）

本机 **WinNAT 排除 TCP 8024–8123**（impl-03a §C 已登记），冻结端口 8081/8030/8088 不可绑。
本会话用 `.scratch/tmp/port-override.yml`（gitignore 覆盖）叠加生效，**必须用**：

```
docker compose -f docker-compose.yml -f .scratch/tmp/port-override.yml up -d
```

⚠️ override 里 ports 用 `!override` 标签 —— Compose 默认对 ports 是**追加**语义，
直接写会同时发布 8081 与 18081，仍撞被排除端口（本票实测踩到）。**不要**用
`COMPOSE_EXTRA_FILES` 环境变量直接传（它只被 `scripts/deploy_surface_check.sh` 消费，
`docker compose` 本身不认；须传完整 `-f` 参数）。

当前七服务：doris-fe/be、flink-jm/tm、kafka、superset 全 `healthy`；kafka-init `exited(0)`。

### P7 · 已改文件（工作区未提交）

```
 M docker-compose.yml        (+12)  ./data:/data:ro 挂 JM 与 TM
 M src/ecomgen/cli.py        (+108) --events / --logical-span / dt 摘要行
 M src/ecomgen/producer.py   (+219) max_events / watermark_step_ms / epoch_base_ms / plan_threshold_batch
?? sql/replay_00_catalog.sql
?? sql/replay_01_ddl.sql
?? sql/replay_10_ods.sql
```

`data/threshold/raw.jsonl` 与 `manifest.json` 经 `git check-ignore` 验证**被 `.gitignore:10 (**/data/)` 覆盖**。
---

## §C 官方取证（2026-10-22）：`file:` 到底要不要 Hadoop

> 本节回答 §P4 阻塞点的**唯一问题**：Paimon 的 `file://` warehouse 是否**官方要求**
> Hadoop 存在？结论先行：**不是官方要求，是 Paimon 自身的已知缺陷（官方 open issue）**。

### C1 · 官方文档：`file://` 是 **Built-in Support**，且**不**标需要 hadoop

出处：[Paimon 官方 Filesystems 页](https://paimon.apache.org/docs/master/maintenance/filesystems/)（master 分支，2026-10-22 读取）

官方「Supported FileSystems」表逐字（只摘与本票相关的行）：

| FileSystem | URI Scheme | Pluggable | Description |
| --- | --- | --- | --- |
| Local File System | `file://` | **N** | **Built-in Support** |
| HDFS | `hdfs://` | N | Built-in Support, **ensure that the cluster is in the hadoop environment** |

**读法（关键）**：官方把「需要 hadoop 环境」这句话**只写给 HDFS 那一行**，
`file://` 一行只写 `Built-in Support`。同一张表里**逐行的差异就是官方的口径**
⇒ 官方口径下 `file://` **不**以 hadoop 为前提。

### C2 · 官方 issue：`CatalogContext` 在**非 Hadoop 场景**下硬依赖 Hadoop（open）

出处：[apache/paimon#6654](https://github.com/apache/paimon/issues/6654)
标题逐字：`[common] CatalogContext requires Hadoop dependency even in non-Hadoop environments`
状态：**Open**（2025-11-21 由 tchivs 提出，2026-10-22 读取时仍未关闭）

正文逐字（关键句）：
> `CatalogContext` currently has a **hard dependency** on Hadoop Configuration, causing
> `NoClassDefFoundError` in environments where Hadoop is **not needed or available**.

该 issue 里贴出的用户侧堆栈**与本票实机撞到的完全同类**（Windows + Flink CDC + Paimon）：

```
Caused by: java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
    at org.apache.paimon.catalog.CatalogContext.<init>(CatalogContext.java:53)
```

⇒ **同一病症已被 Paimon 官方承认为「不该发生的依赖」，并挂在 open 状态。**
这不是「官方要求你装 hadoop」，而是「Paimon 里 hadoop 的耦合尚未解耦」的缺陷。

### C3 · 本仓自己的堆栈：失败点与 #6654 **不同**，但同源

本票实机（`flink--sql-client-flink-jm.log`）的失败链是**序列化触发**，不是 `CatalogContext`：

```
at java.io.ObjectOutputStream.writeObject(...)
at org.apache.paimon.flink.sink.FlinkSinkBuilder.mapToInternalRowWithUriReaderFactory(FlinkSinkBuilder.java:307)
at org.apache.paimon.flink.sink.FlinkSinkBuilder.build(FlinkSinkBuilder.java:230)
at org.apache.paimon.flink.sink.FlinkTableSinkBase.lambda$getSinkRuntimeProvider$1(FlinkTableSinkBase.java:167)
at org.apache.paimon.flink.PaimonDataStreamSinkProvider.consumeDataStream(PaimonDataStreamSinkProvider.java:54)
Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration
```

**差异要说准**（否则会误判成「照 #6654 的修法就能好」）：
本仓是 **写（sink）阶段**序列化 Paimon 的配置对象时，触发了对
`HadoopUtils`/`Configuration` 的类解析；#6654 报的是 **`CatalogContext` 构造**时。
两处**都**引用了 hadoop 的 `Configuration` 类 ⇒ **同一个未解耦面**的两个入口。
⇒ 结论不因此改变，但**不能**把本仓的阻塞简单等同于 #6654 已修就好。

### C4 · 三个方向逐条裁定

| 方向 | 裁定 | 依据 |
| --- | --- | --- |
| **C**（查官方是否要求 `file:` 配 hadoop） | ✅ **已完成，结论为否** | C1 表格逐行差异 + C2 官方 open issue 承认这是缺陷 |
| **A**（补 hadoop jar 进镜像） | **技术上可行，但违背票面「零 jar」** | 官方文档另有 `paimon-hadoop-uber` 包，逐字说「若你的应用没有 hadoop 依赖，可用它」。**但那是新增 jar** ⇒ 与本票验收第 8 条正面冲突，须用户裁决 |
| **B**（换 warehouse 到对象存储） | **不解决本问题** | 官方表格里 s3/oss 等是 `Pluggable=Y`，其插件同样构建在 Hadoop `FileSystem` 之上；且会动 impl-03a 已验收的部署面与具名卷 |

### C5 · 官方未给出（按 spec §8.3 登记，不臆造）

- **官方没有说过**「`file://` 需要 hadoop」——恰恰相反，C1 把 hadoop 前提只写给 HDFS。
  ⇒ 本仓遇到的**不是官方前提**，是**实现缺陷 + 未解耦**。
- **官方没有给出**「哪些 Paimon 操作会触发 hadoop 类解析」的完整清单；
  本票实测到的触发面是：`INSERT`（sink 序列化）**会**、`SELECT`（本仓实测）**会**、
  `CREATE CATALOG` / `CREATE TABLE` / `SHOW DATABASES` / `SHOW TABLES` **不会**。
  ⚠️ 这份「会不会」清单是**本仓实测**（`设计值` 口径），不是官方保证。
- **官方没有给出** `paimon-hadoop-uber` 与 Flink 2.2 / Paimon 2.1.0 的版本匹配声明
  （该包在文档里只以 `2.2-SNAPSHOT` 示例形式出现，本仓**未验证**其与 2.1.0 的兼容性）。

### C6 · 待用户裁决（我不自行加 jar、未改 Dockerfile）

C 的结论把选择收窄成两条**都要用户点头**的路：

- **A（推荐）**：接受一个 hadoop-client 类 jar 进 impl-03a 镜像，并把票面第 8 条
  「全 SQL、零 jar」的**口径改准**为「零**业务** jar；环境类 jar 归部署面」。
  理由：这是 Paimon 官方承认的实现缺陷，绕不开；且官方自己就为此提供了
  `paimon-hadoop-uber` 这种「补 hadoop」的包，说明这条路是官方**预期**的用法之一。
- **D（新增，C 取证后才浮现）**：**降版本**到该缺陷已修的 Paimon 版本。
  代价：动 HLD §3 的技术栈定案（2.1.0 是冻结点），且要先查明是否真有已修版本
  ⇒ 若用户不接受改票面口径，这条是唯一不动「零 jar」的路。

**本会话停在此处。** 未改 Dockerfile、未加任何 jar、未降版本。


---

## §D 裁决 A 的落地与实测（2026-10-22，阻塞解除）

用户 2026-10-22 裁决 **选 A**（补环境类 jar 进部署面）。本节记录落地与实测。

### D1 · 落地物

| 文件 | 动作 | 说明 |
| --- | --- | --- |
| [scripts/fetch-paimon-jar.sh](../../scripts/fetch-paimon-jar.sh) | 改 | 从「单 jar 一次性下载」改为**表驱动**（`JAR_TABLE`），两 jar 各自幂等 + size/sha256 双校验；任一失败即非 0（不允许部分成功） |
| [docker/flink/Dockerfile](../../docker/flink/Dockerfile) | 改 | 新增 `COPY lib/paimon-hadoop-uber-2.1.0.jar` + 构建期 sha256 自检；LABEL 增三条审计键（sha256/size/reason） |
| `docker/flink/lib/paimon-hadoop-uber-2.1.0.jar` | 新增（**不入 git**） | 50,615,937 B；`.gitignore:66` 的 `docker/flink/lib/` 已覆盖 |

### D2 · 修复前后对照（同一套 DDL，只换镜像）

| 动作 | 修复前 | 修复后 |
| --- | --- | --- |
| `CREATE CATALOG` / `CREATE TABLE` / `SHOW` | ✅ 通 | ✅ 通 |
| `INSERT INTO <paimon表>` | ❌ `ClassNotFoundException: org.apache.hadoop.conf.Configuration` | ✅ **Submitted, Job ID: 3bce3b33…** |
| `SELECT count(*) FROM <paimon表>` | ❌ 同一异常 | ✅ 返回 **2** |
| 文件系统落盘 | — | ✅ `/warehouse/probe_fix.db/tfix/` 下见 `dt=2024-01-01` / `schema` / `manifest` |

### D3 · 类冲突不是推断，是实测

两 jar 的 `.class` 重叠**仅 1 个**（`META-INF/versions/9/module-info.class`，非业务类）；
`org/apache/paimon/fs/`、`org/apache/paimon/catalog/` 关键类**零重叠**
（uber 的 hadoop 类走 `paimon/fs/shade/hadoop3/` 影子路径）。
⇒ 共存于同一 `/opt/flink/lib` 安全，实测也确实没出现 `NoSuchMethodError` / 类遮蔽。

### D4 · 票面口径的调整（**须在收口时同步改验收第 8 条**）

原第 8 条「**全 SQL、零 jar**：不新增 jar、不需要 JDK/Maven；`ADD JAR` 一条也没有」。

按 A 裁决，口径改为：**零「业务」jar；环境类 jar 归部署面（impl-03a）管理**。
理由：这不是本仓想加的连接器，而是 **Paimon 上游缺陷**（#6654，Open）的官方指定绕法
（官方文档逐字提供 `paimon-hadoop-uber` 用于「应用侧没有 hadoop 依赖」的场景）。

**仍然成立的三条子判据**（未因本 jar 松动）：
- ✅ `ADD JAR` 一条也没有（jar 是构建期 `COPY`，不是会话内 ADD）
- ✅ 不需要 JDK/Maven
- ✅ 不重读 Kafka（门槛批来源仍是 dump 文件）

⚠️ **收口时须把第 8 条原文改写**，不要留着「不新增 jar」的字面自相矛盾。

### D5 · 新发现的运维坑（写给接手者）

`sql-client -f` 里的 `INSERT` 是**异步提交**（`Submitting SQL update statement to the cluster...`
→ 立刻返回 `Job ID`），**不等作业完成**就执行下一条。
本票实测：紧接着的 `SELECT count(*)` 返回 **0**，再跑一次才返回 **2**。
⇒ **判据不能「INSERT 后立即 SELECT 断言行数」**，必须
① 等作业 `FINISHED`（`docker exec eco-flink-jm flink list -r` / REST `/jobs`），或
② 把 `SELECT` 放到**下一次** sql-client 调用里（本票实测可行），或
③ 设 `'table.dml-sync'='true'`（本票 `replay_10_ods.sql` 已设，**尚待**实测其是否真能阻塞）。
这条直接决定 `layer=… rows=…` 日志行能不能读到真实行数——**先解决它再写编排**。


---

## §E 交接快照（2026-10-22，换 session 前）

> 本节是**给下一个 session 的唯一入口**。读 §P1–P7 + §C + §D + 本节即可续跑，
> 不必重读上游文档。**最关键一条**：`sql/replay_20_dwd.sql` **已写但尚未执行过**，
> 可能含 SQL 语法错误，接手第一步就是把它跑一遍。

### E1 · 当前状态一句话

**ODS 已通、DWD 未验证。** 阻塞（hadoop 缺类）已由 A 裁决解除并实测证实；
11 张表已建、ODS 1,048,576 行已落、payload 原文保留；DWD 的 SQL 写完了但**没跑过**。

### E2 · 已验证的事实（可直接采信，不必重做）

| 事实 | 证据 |
| --- | --- |
| 门槛批 1,048,576 条 / dt 3 分区 / 44.6 s | `[ecomgen] dump 完成：1048576 条 dt=[2024-01-01..2024-01-03] dt_count=3` |
| 11 张表全部建成，形状与 spec §5.2 逐项一致 | `SHOW CREATE TABLE` 输出 0 个 ERROR；主键/bucket/`upsert-materialize=NONE` 全对；无 `deletion-vectors`、无 `sequence.field`、无 `hour` |
| Paimon 读写已修好（hadoop jar 解掉） | `INSERT` → `Job ID: 3bce3b33…`；`SELECT count(*)` → **2**；`/warehouse/probe_fix.db/tfix/` 落盘 |
| **ODS 写入成功：1,048,576 行** | `SELECT count(*), count(DISTINCT dt) FROM ods_behavior_event` → **1048576 / 1** |
| ODS dt = 落地日（单分区 `2026-10-22`） | `GROUP BY dt` → `\| 2026-10-22 \| 1048576 \|` |
| ODS payload 为原文 | `SELECT payload` → `{"session_id":"u_000000"}`，与 dump 源逐字相同 |
| `table.dml-sync='true'` **确实阻塞到作业完成** | 输出 `Complete execution of the SQL update statement.`，紧跟的 `SELECT` 读到 **3**（含本次新插的行），无需重跑 |
| 两 jar 无类冲突 | `.class` 重叠仅 `module-info.class` 1 个；fs/catalog 零重叠 |

### E3 · ⚠️ 未验证 / 可能有问题的地方（接手第一优先）

1. **`sql/replay_20_dwd.sql` 从未执行过。** 写它的时候我已经踩到过两个 Flink SQL 陷阱
   （`CHECK` 不被解析器支持、`value` 是保留字），所以它**大概率还有语法或语义错误**。
   已知的**高风险点**（我写的时候就存疑，请逐个验）：
   - 用了 `FROM_UNIXTIME` / `UNIX_TIMESTAMP` / `GREATEST` / `TRIM` —— **未在任何一次实跑中验证过**
     Flink 2.2 里这些函数的真实签名与时区行为。若 `FROM_UNIXTIME(x,'yyyy-MM-dd')` 不成立，
     改用 `DATE_FORMAT(TO_TIMESTAMP_LTZ(...), ...)`。**日期算错会直接毁掉 dt 分区语义。**
   - `v_checked` 里对 ODS 做的子查询 `(SELECT wm_ms FROM v_batch_ref)` 是**相关子查询式写法**，
     Flink 对流/批的子查询支持有限，可能报 `UnsupportedOperationException`。
     备选：先把 `wm_ms` 物化成一个临时表再 join。
   - `v_checked` 的内层 `SELECT o.*, JSON_VALUE(o.payload,...)` 依赖 ODS 表里
     **没有** `amount/currency/quantity/order_id` 列这个事实 —— 若列名冲突会静默取错。
   - `GREATEST(0, ...)`：Flink 里 `GREATEST` 存在但**遇 NULL 会返回 NULL**；`event_time` 若为
     NULL 则 `late_lag_ms` 为 NULL，而 DDL 里该列可空，A3 断言会漏行。
2. **拒绝表一条行都还没产生。** 门槛批是生成器产的**合法**数据，理论上
   `reject_reason` 全为 NULL ⇒ 拒绝表 **0 行**。这本身是对的，但意味着
   **拒绝旁路的分支逻辑完全没被验证过**。要证明它真的会判，必须造脏数据
   （见 E5 的 F 项）——否则就是「恒真判据」，正是本仓 impl-02e 记过的病。
3. `is_late` / `late_lag_ms` 的**语义实现**待验：我用「批次最大 event_time 当水位」，
   但 `dwd_late_event_count` 表（A3 的对照面）**还没有任何写入逻辑**，
   ⇒ A3 迟到对账现在必然不成立。该表归 03d/03c，但**别以为现在能对账**。

### E4 · 环境与复现命令（下一个 session 必须照这个来）

**⚠️ 宿主机 WinNAT 排除 TCP 8024–8123**，冻结端口 8081/8030/8088 绑不上（impl-03a §C）。
**必须**叠加端口 override，且 ports 用 `!override` 标签（Compose 默认对 ports 是**追加**语义）。

```bash
# 起栈（唯一正确姿势）
cd "F:/Anaconda practice project/eco-a-d-platform"
MSYS_NO_PATHCONV=1 docker compose -f docker-compose.yml -f .scratch/tmp/port-override.yml up -d

# 跑 SQL（stdin 喂文件；-f /dev/stdin 是本仓实测可用的姿势）
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm /opt/flink/bin/sql-client.sh -f /dev/stdin \
  < sql/replay_20_dwd.sql

# 生成门槛批（约 45 s；已产出过，raw.jsonl 还在，可跳过）
wsl -e bash -lc 'cd "/mnt/f/Anaconda practice project/eco-a-d-platform" && \
  .venv/bin/ecomgen dump --events 1048576 --logical-span 3d --seed 20241022 \
  --out data/threshold/raw.jsonl --manifest data/threshold/manifest.json'

# 测试
wsl -e bash -lc 'cd "/mnt/f/Anaconda practice project/eco-a-d-platform" && \
  .venv/bin/python -m pytest tests/ -q'      # 上一轮：124 passed
```

**容器现状**（2026-10-22 交班时）：doris-fe/be、flink-jm/tm、kafka、superset **全 healthy**
（kafka-init 为一次性服务，`exited(0)` 属正常）。
**warehouse 现状**：`/warehouse/ecom_lake.db/` 下 11 张表目录都在；
`ods_behavior_event` 已有 1,048,576 行；另有探针库 `probe_fix.db`（`tfix` 表）可删。

### E5 · 下一步（按顺序做，别跳）

> **（2026-10-26 归正，impl-06f-2）** 以下 F1–F7 共 7 项已于 2026-10-22/23 施工
> 全部兑现（收口时未回勾本清单）。逐项兑现证据见行内标注；本票勾选状态以票面
> Acceptance criteria 与顶行 `Status:` 为准。

- [x] **F1 跑通 `replay_20_dwd.sql`**：先只跑到 DWD，把 E3 列的四个高风险点逐个验/改。
      判据：`SELECT count(*) FROM dwd_behavior_event` 应 = 1,048,576（本批无脏值）。
      ——兑现于 §A·1（`layer=dwd rows=1048576`）＋ §A·7（四个高风险点全部实测修正：
      payload 存整行 / `UNIX_TIMESTAMP` 无参 / 下界 MIN / `is_late` 到达序）＋ §P0-7 复跑
- [x] **F2 造脏数据验拒绝旁路**：往 ODS 插 3 条分别触发三值（`schema_invalid` /
      `value_out_of_range` / `time_out_of_range`），确认**恰好**落进拒绝表且
      `dt` = 摄入日、`reject_reason` 取值**恰在三值内**。**这一步不做，拒绝表就是恒真判据。**
      ——兑现于 §A·4（7 行注入脏批，四类各命中一次；`dt`=摄入日、取值域无 `duplicate` 均实测）
- [x] **F3 `make replay LAYER=` 截断开关 + `layer=… rows=…` 日志行**。
      ⚠️ 行数必须取自 **`dml-sync` 完成之后**的查询（D5 已证 `dml-sync` 会阻塞），
      否则日志行全是 0 —— 这正是票面第 1 条验收要判的东西。
      另：`make replay` 要接进 Makefile（现在 `replay:` 目标还是三行 echo 占位）。
      ——兑现于 §A·1 ＋ §P0-2 第 1 项（`lake_replay.sh` 诚实退出码 + 层 marker 强制
      正向证据）；`replay:` 已接进 Makefile（`bash scripts/lake_replay.sh`）
- [x] **F4 `git check-ignore` 复验** `data/threshold/raw.jsonl` 与
      `docker/flink/lib/paimon-hadoop-uber-2.1.0.jar`（两者**当前都已验证**被覆盖，
      但 F3 若动 `.gitignore` 要重验）。
      ——兑现于 §A·6（`git check-ignore -v` 命中 `.gitignore:10`）＋ impl-03 母票验收
      第 9 条（03f 收口复核 `.gitignore:10` 再命中）
- [x] **F5 补生成器单测**：`--events`/`--logical-span`/`plan_threshold_batch` 目前
      **没有单测**（我只用探针脚本验过行为）。至少覆盖：恰好 N 条、dt 恰 3 个、
      三通道全非空、`3d/72h` 归一化等价、非整天输入报错。
      ——兑现于 §P0-4（`tests/test_threshold_batch.py`，test-author 产出，50 例全绿；
      点名的五类用例各有对应 test 函数）。2026-10-26 复跑：**49 passed (9.54s)**，
      文件在仓、覆盖面逐类核对一致
- [x] **F6 改票面验收第 8 条**（见 §D4）：把「不新增 jar」改成「零业务 jar，
      环境类 jar 归部署面」，否则留下字面自相矛盾。
      ——兑现于 §P0-2 第 3 项（已按 §D4 裁决口径改写，同步 headline 与 §A·6 标题）；
      现行票面第 8 条即改写后文本
- [x] **F7 收口**：12 条验收逐条核对 → `code-review` skill → 勾票 → commit。
      ——兑现于顶行 `Status: resolved（2026-10-23，两轮 code-review 后收口…）` ＋
      §P0-3（审查发现全部处理完）＋ §P0-6（第二轮两处 BLOCKER 修毕复验）＋
      §P0-7（复跑全绿）＋ §P0-4（已提交 feat/ecom-pipeline-impl）

### E6 · 已改文件清单（工作区**全部未提交**）

```
 M .scratch/.../impl-03b-lake-ddl-and-ods-dwd-replay.md   (+283) 本票 P1–P7 / §C / §D / §E
 M docker-compose.yml                                     (+12)  ./data:/data:ro 挂 JM 与 TM
 M docker/flink/Dockerfile                                (+51)  COPY hadoop-uber jar + sha256 自检 + LABEL
 M scripts/fetch-paimon-jar.sh                            (+149/-77) 改成表驱动，两 jar 各自幂等校验
 M src/ecomgen/cli.py                                     (+108) --events / --logical-span / dt 摘要行
 M src/ecomgen/producer.py                                (+219) max_events / watermark_step_ms /
                                                                epoch_base_ms / plan_threshold_batch /
                                                                truncate_proportionally
?? sql/replay_00_catalog.sql      catalog paimon + database ecom_lake
?? sql/replay_01_ddl.sql          11 张表 DDL（已实机建表成功）
?? sql/replay_10_ods.sql          dump JSONL → ODS（已实机跑通 1,048,576 行）
?? sql/replay_20_dwd.sql          ODS → DWD + 拒绝旁路（**已写，未执行**）
```

**未跟踪的产物**（都被 gitignore 覆盖，勿误删、勿入库）：
`data/threshold/raw.jsonl`（329,692,935 B）、`data/threshold/manifest.json`、
`docker/flink/lib/*.jar`（两个）、`.scratch/tmp/`（端口 override 与探针脚本）。

### E7 · 踩过的坑（一条都别再踩）

1. **`Flink SQL 不支持 CHECK 约束** —— `ParseException Encountered "CHECK"`。取值域只能靠产出侧 CASE。
2. **`value` 是 SQL 保留字** —— 用它当列别名会 ParseException。
3. **`sql-client -f` 非交互必须 `SET 'sql-client.execution.result-mode'='TABLEAU'`**，否则任何 SELECT 都报错。
4. **`INSERT` 默认异步**，不设 `dml-sync` 就查不到刚写的行（§D5）。
5. **`json` format 不保 payload 原文** —— 它返回规范化后的 `{}`。必须用 `format='raw'` + 正则子串截取。
6. **容器看不到宿主目录** —— 必须 `./data:/data:ro` 挂进**两个** Flink 容器（TM 才是真正读文件的那个）。
7. **Compose 的 ports 是追加语义** —— override 必须用 `!override`，否则 8081 与 18081 同时发布、仍撞被排除端口。
8. **`COMPOSE_EXTRA_FILES` 不是 `docker compose` 的变量** —— 它只被 `scripts/deploy_surface_check.sh` 消费；命令行要传完整 `-f`。
9. **`--sessions` ≠ 条数** —— 每会话约 2.227 条事件；要定条数必须用 `--events`。
10. **裁最后那个列表会清空超期通道** —— 迟到事件被重排到尾部，取前 N 几乎全是正常事件（实测 `expired=0`）。