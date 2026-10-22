# impl-03c: DIM 拉链 + DWS 区间 join + ADS 五层贯通

> 父票：impl-03
> 规格落点：spec §5.2（形状表 DIM/DWS/ADS 三行）、§5.3（`replay_30`–`50_ads`）

Type: task
Status: ready-for-agent
Blocked by: impl-03b

**What to build:** 维度变更流灌出拉链维表（跨日多版本），DWD 事件按事件时刻做**区间 join** 命中当时有效的维度版本并回填到 DWS 窗口聚合，DWS 再落成 ADS。跑完本票，`make replay`（缺省、不带 `LAYER=`）**五层一次跑通退出 0**。

## 交付内容

1. **三张拉链表** `dim_item`/`dim_category`/`dim_user`：主键 `(id, valid_from_ms)`，**不分区**，拉链区间 `valid_from_ms`/`valid_to_ms`（`BIGINT` UTC epoch 毫秒；`valid_to_ms = 9999999999999` 表仍有效），时间源＝维度变更流的 `change_time`（事件时间语义，满足 D-3）。
2. **两张静态维表** `dim_region`（主键 `(province)`）/`dim_channel`（主键 `(channel)`）：无拉链，同样固定 `bucket=1`、`upsert-materialize=NONE`。
3. **`replay_30_dim.sql`**：维度变更流 → `dim_*`（区间）。
4. **DWS `dws_subject_window`**：主键 `(dt, subject_type, subject_id, window_start)`，`dt`＝窗口起点日，`subject_type`∈{user,item,category}，指标列 `pv/uv/gmv`。
5. **ADS `ads_window_metric`**：主键 `(dt, metric_name, dim_key, window_start)` —— **与 Doris `ecom_ads.ads_window_metric` 键同构**，这是 03f 对账的前提。
6. **迟到计数 `dwd_late_event_count`**：append，`dt` × 60 s 档，**只存计数**。
7. **`replay_40_dws.sql`**（dwd × dim 区间 join）与 **`replay_50_ads.sql`**。

## 时态维 join 的硬形状

```sql
ON d.item_id = e.item_id
AND e.event_time_ms BETWEEN d.valid_from_ms AND d.valid_to_ms
```

走 **batch 区间 join**，**不是 lookup join** —— lookup join 被官方逐字否定（processing-time，回放必错）；Paimon 全站**无 zipper/SCD 官方能力**。

## Acceptance criteria

- [ ] `make replay`（缺省、不带 `LAYER=`）跑完五层**退出 0**，日志逐层有 `layer=<层> rows=…` 且五层**行数均 > 0**
- [ ] 拉链维表能查到**同一 id 的多个版本**，且版本区间**首尾相接不重叠**（`valid_to_ms` 与下一版 `valid_from_ms` 衔接一致）
- [ ] 抽查若干条事件，其回填到的维度值与**事件时刻**的版本一致（不是最新版本，也不是随机版本）—— 时态 join 真的按事件时刻生效
- [ ] `dws_subject_window` 与 `ads_window_metric` 的 `window_start` 语义一致；`subject_type` 取值域恰为三值
- [ ] ADS 表主键列**逐列同名同义**于 Doris `ecom_ads.ads_window_metric`（03f 对账依赖它；不一致现在改最便宜）
- [ ] `dwd_late_event_count` 只有计数列，**没有事件明细**
- [ ] 三天跨度下 `dt` **有 3 个分区**（门槛批跨度兑现）
- [ ] `dwd_late_event_count` 的 `is_late` 侧与 DWD 有对应量（本票只记录，03d 才做 A3 断言）
- [ ] 仍**全 SQL 零 jar**：未新增 jar、未引入 `ADD JAR`

## Notes

- **不实现**（spec §8.2）：lookup join / zipper / SCD · `sequence.field` · 排序 compaction · CUMULATE 作提前发射替代路径（走 TVF 会被 `checkEmitConfiguration` **直接抛异常**，[08] D-14）。
- **Paimon ↔ Doris 是「两套物化、一个口径」**：Paimon 侧 `ads_window_metric` 是**离线口径基准**、**不直接供看板**；Doris 侧是**唯一看板供数点**（`REQ-F-OLAP-01` 冻结）。本票只保证两边**键同构 + 同公式**，逐值相等由 03f 判。
- `pv/uv/gmv` 的指标语义在 gdd 里是「由 [03] 推定回填」——**依据不如其它决策硬**。若施工中发现口径不自洽或与 ADS 表对不上，**停下来登记**（spec §8.3 口径，不猜），不要自行发明定义。
- 本票**不写断言**；`assert_lake.sql` 与 `lake_assert.sh` 从 03d 开始。
- `replay_90_maintain.sql`（`sys.compact` ×N + `sys.expire_snapshots(retain_max=>20)` ×N）也从 03d 开始 —— 本票可先把五层数据落稳。