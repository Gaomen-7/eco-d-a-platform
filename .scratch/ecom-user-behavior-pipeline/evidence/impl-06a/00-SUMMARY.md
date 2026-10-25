# impl-06a 演练证据包 — HOUR 分级保留实机（2026-10-25）

## 结论

`REQ-F-OLAP-03` 的**行为层证据到手**：`time_unit=HOUR` + 小 `start` 的独立演练表上，
**Doris 4.1.5 的 `DynamicPartitionScheduler` 真实删除了过期 HOUR 分区**，
观测窗 **505 s ≤ 1200 s**（= 2 × `dynamic_partition_check_interval_seconds` 600 s，`官方给出`）。

- **T0 = 2026-10-25 12:19:48 UTC**（04 观测脚本启动；预置 03 于 12:19:09 完成——03 步首行的 12:18:05 是脚本启动时刻、早于预置，不作观测起点）
- **删除 tick = 12:27:50 UTC**（FE 日志逐字：`DynamicPartitionScheduler|43` 线程逐个
  `succeed in dropping partition[p2026102506/07/08/09]`，`forceDrop:false`，逐个进
  `CatalogRecycleBin.recyclePartition()`——与 research 07 §2.3/§3.3 的 4.1.5 源码推导一致）
- **T_END = 12:28:13 UTC**（轮询首次见到 4/4 消失），**elapsed = 505 s**
  （自预置完成 12:19:09 起算最坏 **544 s**，均 ≤ 1200 s＝2×600 s——1200 s 为**设计值**，interval 600 s `官方给出`）
  ⚠️ 读数说明：04 轮询行里的 `elapsed` 是**循环起点**算的，随后两条阻塞查询（≈11 s）
  才打印时刻 ⇒ 末行 `elapsed=494s` 比同刻真实值偏旧一个查询时长；收口行的
  505 s 由脚本内 `date -u +%s` 差值（T_END−T0）得出，是权威数。

## 验收对账

| 票面验收 | 结果 | 凭证 |
| --- | --- | --- |
| ① 演练表建表成功（HOUR + 小 start + create_history_partition + 600 s 调度生效） | ✅ | `02-create-table.out.txt`：建表当场 4 分区（p2026102510–13，FE 时区 UTC）；`SHOW FRONTEND CONFIG` `dynamic_partition_check_interval_seconds=600`；tick 实际间隔 12:17:50→12:27:50 恰 600 s |
| ② ≤1200 s 观测到过期 HOUR 分区被调度器真实删除（起止时刻） | ✅ 505 s | `04-observe-drop.out.txt`（逐 30 s 轮询）+ `04b-drop-msg-and-fe-log.out.txt`（FE 日志逐字） |
| ③ 三张已验证 ADS 表全程行数不变 | ✅ | `01-…out.txt` 基线 vs `05-postcheck-cleanup.out.txt` 终态：ads_window_metric **30132**、ads_funnel_step **2427**、ads_late_expired_count **9146**，逐表相等 |
| ④ 结论回填 HLD §8 R7 | ✅ | HLD §8 新增 **R7-j**（数字标 `官方给出`/实测） |
| ⑤ 全部本地提交，未推送 | ✅ | 本次 commit（不推送） |

## 演练形态（与 research 07 §4.4/§7.2 的两处修正）

1. **预置过期分区必须绕一道官方禁令**（新发现，4.1.5 实机）：动态分区表上手工
   `ADD PARTITION` 直接被拒——报错原文 "Cannot add/drop partition on a Dynamic
   Partition Table, Use command `ALTER TABLE ... SET ("dynamic_partition.enable" =
   "false")` firstly."。实际走了 **临时关表级 enable → ADD ×4 → 开回** 三步
   （`03-seed-expired-partitions.out.txt`）。删除本身仍由调度器执行，演练语义不受影响。
2. **`LastDropPartitionMsg` 在 4.1.5 上成功删除后仍为 `NULL`**（新发现）：
   research 07 §7.2 建议的「LastDropPartitionMsg 有记录」判据**本版不成立**。
   行为层可观测物改定为：**`SHOW PARTITIONS` 分区消失 + FE 日志
   `DynamicPartitionScheduler`/`logDropPartition` 逐字记录**。
   （该项更新语义官方未给出，已随 R7-j 登记。）

## 演练边界（票面风险边界执行情况）

- 演练对象＝独立库 `ecom_rehearsal` 的 `ads_window_metric_rehearsal`，**未触碰**
  `ads_window_metric` / `ads_funnel_step` / `ads_late_expired_count`（行数对账见上表）。
- 过期删除只作用于演练表自身分区；`DROP DATABASE ecom_rehearsal`（非 force）收尾，
  表与分区进 catalog recycle bin，BE 侧磁盘异步回收（时限官方未给出——回盘实测归 impl-06b）。
- 实施前置全过：磁盘（BE AvailCapacity 910.173 GB / volume 所在 ext4 932 GB avail）、
  `SHOW FRONTENDS`/`SHOW BACKENDS` 全 Alive、六服务 healthy（`01-…out.txt`）。
- **不声称**验证过「30 天窗口的边界时刻」——那需要真实 31 天数据（维持 D8 ③口径）。

## 文件清单

| 文件 | 内容 |
| --- | --- |
| `01-preconditions-baseline.sh/.out.txt` | 实施前置（磁盘/FE/BE/healthy）+ 三表基线 + 调度配置 |
| `02-create-table.sh/.out.txt` | 建演练库/表 + 建表当场分区清单 + 调度注册态 |
| `03-seed-expired-partitions.sh/.out.txt` | 预置 4 个过期分区（含官方禁令绕法）+ 写 5 行 |
| `04-observe-drop.sh/.out.txt` | 逐轮观测轮询（名义 30 s sleep ＋单次查询 ≈11 s ⇒ 实际节奏 ≈41 s；T0→T_END 505 s）+ 收口证据 A/B/C |
| `04b-drop-msg-and-fe-log.sh/.out.txt` | `LastDropPartitionMsg` 全行 + FE 日志 12:27:50 逐字 |
| `05-postcheck-cleanup.sh/.out.txt` | 三表行数终态对账 + `DROP DATABASE ecom_rehearsal` + 清理后状态 |

⚠️ 04/04b 内的分区名（p2026102506–09）与日志时刻均为 **2026-10-25 本次演练的日期耦合取证**，不可原样改日重跑（重跑须按当日 UTC 时钟重推算分区名）。03 步 `SHOW PARTITIONS` 的 `RowCount=0` 为统计异步滞后，直查 `COUNT(*)`=5（见同凭证 [4] 第二条）。
脚本执行面：`wsl bash` → 仓库根相对路径；MySQL 访问一律经
`docker compose run --rm client mysql -h doris-fe -P 9030 -u root`（HLD §4.4 两跳约束；脚本内 `dq` 即此封装）。
