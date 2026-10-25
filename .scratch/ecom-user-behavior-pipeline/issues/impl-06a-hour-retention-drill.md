# impl-06a: HOUR 分级保留演练实机

Type: task
Status: resolved
Blocked by: —（impl-05 已结清）

> ✅ **2026-10-25 用户批准开票**（spec §8.2 三张实机 task 之一，裁决「**开票不实施**」——实施须用户另行触发）。
> 出处：spec §8.2 / map.md D8 ③ / `REQ-F-OLAP-03` 判据「配置 + 小时级演练」的行为层。
> ✅ **2026-10-25 本会话触发实施并结清**（用户 `/implement` 发起）：全部 5 项验收达成，Answer 见文末。

**What to build:** 用一张**独立演练表**补齐「HOUR 分级保留真的会删分区」的行为层证据——`REQ-F-OLAP-03` 判据的配置层（`start=-30` 保留恰 30 个日分区、`create_history_partition=true` 建表当场 34 分区）已由 impl-05 前序实机证实，本票补行为层：`time_unit=HOUR` + 小 `start` 的独立表 ≤1200 s 可观测真实分区删除。

**风险与边界（获批前提，实施时复述给用户）**：
- 只建**独立演练表**（`ecom_ads` 外或独立命名），**不碰** `ads_window_metric` / `ads_funnel_step` / `ads_late_expired_count` 三张已验证表；
- 保留策略到期删除只作用于演练表自身分区，不构成对已验证卷态的破坏；
- 实施前置：确认磁盘余量、`SHOW FRONTENDS`/`SHOW BACKENDS` 全 Alive、六服务 healthy。

**验收标准**

- [x] 演练表建表成功：`time_unit=HOUR`、小 `start`、`create_history_partition=true`，动态分区调度间隔 600 s（`官方给出`）生效
- [x] 观测到至少一轮「过期 HOUR 分区被调度器真实删除」（起止时刻记录，≤1200 s 可观测窗口——1200 s＝2×600 s 为**设计值**）
- [x] 已验证三张 ADS 表在演练全程行数不变（演练前后 `COUNT(*)` 对账）
- [x] 结论回填 HLD §8（`REQ-F-OLAP-03` 行为层证据列或 R7 表），数字标 `官方给出`/实测
- [x] 全部本地提交，未推送

## Answer（2026-10-25 结清）

### 核心结果

**行为层证据到手**：独立演练表上，Doris 4.1.5 的 `DynamicPartitionScheduler` 在
**T0 后 505 s（≤1200 s）** 的一个 600 s tick（12:27:50 UTC）内**真实删除了全部 4 个过期 HOUR 分区**。

| 量 | 值 | 口径 |
| --- | --- | --- |
| 演练库/表 | `ecom_rehearsal.ads_window_metric_rehearsal`（`ecom_ads` 之外，独立命名） | 实测 |
| 表配置 | `time_unit=HOUR`、`start=-2`、`end=1`、`prefix=p`、`create_history_partition=true`、`replication_allocation=tag.location.default: 1` | 实测（DDL 全文见 02 号凭证） |
| 建表当场分区 | **4 个**（`p2026102510`–`p2026102513`，FE 时区 UTC，名形 `p<yyyyMMddHH>`，与 research 07 §4.3 一致） | 实测 |
| 调度间隔 | `dynamic_partition_check_interval_seconds=600`（`官方给出`）；tick 实际间隔 12:17:50→12:27:50 **恰 600 s** | 配置 `官方给出` + 实测互证 |
| 预置过期分区 | 4 个：`p2026102506`–`p2026102509`（range [06:00,10:00) UTC，全落在保留窗 `[now-2h,∞)` 之外），各写 1 行 + 当前小时 1 行，共 5 行 | 实测 |
| T0 | **2026-10-25 12:19:48 UTC**（04 观测脚本启动；预置 03 于 12:19:09 完成，03 首行 12:18:05 是其脚本启动时刻、不作观测起点） | 实测 |
| 删除 tick | **12:27:50 UTC**（FE 日志：`DynamicPartitionScheduler|43` 线程逐个 `succeed in dropping partition[p2026102506/07/08/09]`、`forceDrop:false`、逐个 `CatalogRecycleBin.recyclePartition()`） | 实测（FE 日志逐字） |
| T_END | **12:28:13 UTC**（轮询首次见 4/4 消失），**elapsed 505 s**；自预置完成起算最坏 544 s，均 ≤ 1200 s＝2×600 s（1200 s **设计值**，interval 600 s `官方给出`） | 实测 |
| 数据随分区回收 | 演练表 5 行 → **1 行**（过期分区内 4 行随分区消失，只剩当前小时行） | 实测 |
| 三张已验证表对账 | `ads_window_metric` **30132** / `ads_funnel_step` **2427** / `ads_late_expired_count` **9146**，前后逐表相等 | 实测 |
| 实施前置 | 磁盘：BE `AvailCapacity` 910.173 GB（volume 所在 ext4 932 GB avail）；`SHOW FRONTENDS`/`SHOW BACKENDS` 全 Alive；六服务全 healthy | 实测 |

### 两处判据面修正（新发现，已随 HLD §8-R7-j 登记）

1. **动态分区表禁止手工 ADD/DROP PARTITION**（4.1.5 实机，报错原文
   "Cannot add/drop partition on a Dynamic Partition Table, Use command
   `ALTER TABLE ... SET ("dynamic_partition.enable" = "false")` firstly."）。
   预置过期分区须走官方指定的三步绕法：**临时关表级 enable → ADD ×4 → 开回**。
   删除仍由调度器执行，演练语义不受影响。
2. **`LastDropPartitionMsg` 在 4.1.5 上成功删除后仍为 `NULL`**——research 07 §7.2
   建议的「LastDropPartitionMsg 有记录」判据**本版不成立**（其更新语义官方未给出）。
   行为层可观测物改定为：**`SHOW PARTITIONS` 分区消失 + FE 日志
   `DynamicPartitionScheduler`/`logDropPartition` 逐字记录**。

### 边界执行情况

- 未触碰三张已验证表（行数对账互证）；过期删除只作用于演练表自身分区；
- 收尾 `DROP DATABASE ecom_rehearsal`（非 force，进 catalog recycle bin），
  BE 侧磁盘异步回收（时限官方未给出——**回盘实测归 impl-06b**）；
- **不声称**验证过「30 天窗口的边界时刻」（维持 D8 ③口径：那需要真实 31 天数据）。

### 凭证

`evidence/impl-06a/`：`00-SUMMARY.md`（汇总与对账表）+
01 前置基线 / 02 建表 / 03 预置过期分区 / 04 观测轮询 / 04b FE 日志取证 / 05 收尾对账，
每步 `.sh` + `.out.txt` 成对。HLD §8 新增 **R7-j**。全部本地提交，未推送。
