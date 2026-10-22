# impl-03b: 七表 DDL 形状 + 门槛批 + `replay` 跑通 ODS→DWD

> 父票：impl-03
> 规格落点：spec §5.1（门槛批）、§5.2（形状表）、§5.3（`replay` 编排前段）

Type: task
Status: ready-for-agent
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

- [ ] `make replay LAYER=dws` 在本票阶段**只跑到 DWD 就停住**（03c 未完成，DIM 段为空或跳过），日志逐层有 `layer=ods rows=…`、`layer=dwd rows=…`
- [ ] 七张表的**分区键、主键、bucket、表属性与 spec §5.2 形状表逐项一致**（逐条列表核对，不靠「看着对」）
- [ ] 分区键统一 `dt STRING` = `yyyy-MM-dd` **UTC**，全表无 `hour` 分区
- [ ] DWD/DIM/DWS/ADS 四张主键表均 `upsert-materialize=NONE` 且**未设 `sequence.field`**；主键表默认 **MOR 而非 MOW**（未开 `deletion-vectors.enabled`）
- [ ] 拒绝表 `dt` 取**摄入日（处理时间）**而非事件日；`reject_reason` 取值域**恰为三值**，`SHOW` 或等价手段能证明**没有 `duplicate`**
- [ ] ODS `payload` 确为原文零解析（抽查若干行与 dump 源逐字节相同）
- [ ] 门槛批落 `data/` 且被 `.gitignore` 覆盖；`git check-ignore` 验证通过
- [ ] **全 SQL、零 jar**：不新增 jar、不需要 JDK/Maven；`ADD JAR` 一条也没有
- [ ] **不重读 Kafka**：门槛批来源是 dump 文件，不是 Kafka topic
- [ ] 本票的 compose 改动仍满足 impl-03a 的部署约束（不推翻它）

## Notes

- **强序入口**：impl-02 已结清，spec §4.3 的 P1 闸门**已开**（impl-02e，2026-10-22）。
- 「单一 catalog（库名 `ecom_lake`）」原文混说 catalog 与 database 两层（2026-10-21 签，附录 A·M）——落地时 catalog 与 database 分别取名，别混。
- **官方未给出的，按 spec §8.3 登记、不臆造**：`sys.compact` 对主键表的逐句适用声明（本票还不涉及 compact，但遇到即登记）。
- **不实现**（spec §8.2）：`sequence.field`（同 `event_id` 重复行该值相同 ⇒ 回落内部序，白设；靠重放确定性）· 排序 compaction（官方逐字仅支持 `bucket=-1`＋batch，本仓主键表是固定 bucket）· lookup join / zipper / SCD（Paimon 全站无官方能力）· 「离线重算覆盖实时行」（记 COULD 留档）。
- 时态维 join 的形态（batch 区间 join，非 lookup）在 03c 落地；本票只写 DDL。
- 03d 之前**不写任何断言**，`assert_lake.sql` 在本票可不存在。