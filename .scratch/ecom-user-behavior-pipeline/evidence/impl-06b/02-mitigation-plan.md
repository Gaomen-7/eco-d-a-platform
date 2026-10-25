# impl-06b 缓解方案（DROP 动手前落盘，票面要求）

> 落盘时刻：2026-10-25 12:47 UTC（01 前置基线已完成、任何 DROP 动作发生之前）。
> 本文档是票面「缓解方案须先落盘再动手」的执行凭证：下列每一项在动手前已成立。

## 1. 破坏面裁定：只动演练对象（票面缓解①）

- 全部 DROP 动作只作用于 **`ecom_rehearsal` 演练库**内的表：
  - `ecom_rehearsal.ads_window_metric_rehearsal`（06a 遗产，当前在 recycle bin）
  - `ecom_rehearsal.reclaim_rollback_check`（回滚演练小表，1 行）
  - `ecom_rehearsal.reclaim_probe`（本次 1 GB 实测主对象）
- **不触碰** `ecom_ads.ads_window_metric` / `ads_funnel_step` / `ads_late_expired_count` 三张已验证表；
  基线 30132 / 2427 / 9146 已记（01 号凭证 [4]），收尾逐表对账。**故票面缓解②（Stream Load 导出备份）按其前置条件「若必须动三张 ADS 表」不触发。**
- 全程 **不使用 `DROP ... FORCE`**（强制跳过 recycle bin）——所有 DROP 走默认（非 force）路径，
  一切被删对象都留在 catalog recycle bin 内可 RECOVER。

## 2. 回滚手段：RECOVER 命令路径（票面缓解③，先验证后使用）

| 层级 | 命令 | 状态 |
| --- | --- | --- |
| 库级 | `RECOVER DATABASE ecom_rehearsal;` | **03 步实机先验证**（06a 留下的 bin 条目恢复，恢复后行数核对），再用于后续 |
| 表级 | `RECOVER TABLE ecom_rehearsal.<name>;` | 03 步在 `reclaim_rollback_check` 上实机验证（DROP→bin 可见→RECOVER→行数=1） |
| 分区级 | `RECOVER PARTITION <name> FROM <db>.<table>;` | 本票不触发（无分区级 DROP），仅记路径 |

若观察期发现误伤（任何非演练对象消失）：立即 `RECOVER` 对应条目，对账 `SHOW CATALOG RECYCLE BIN`。

## 3. 配置变更清单（实验参数，逐条可回退）

| 配置 | 现值（实测） | 实验值 | 回退动作 | 依据 |
| --- | --- | --- | --- | --- |
| FE `catalog_trash_expire_second` | **86400**（`IsMutable=true`，01 号凭证 [6]） | **300** | `ADMIN SET FRONTEND CONFIG ("catalog_trash_expire_second"="86400");` | 默认下 bin 条目滞留 24 h——bin 里 02:10 的 `p20260924` 三条已滞留 ≈10.6 h 即为旁证；不调小则「磁盘回落」观测窗达一天级。运行时改动 FE 重启失效（官方语义），收尾仍显式恢复 |
| BE `trash_file_expire_time_sec` | **0**（4.1.5 源码口径；文档写 86400，两处冲突——research 07 §3.1） | **不动** | — | 0 = trash 无保留窗，天然最短路径，无需变更 |
| BE `min_garbage_sweep_interval` | **180**（`is_mutable=false`，运行时不可改） | **不动** | — | GC sweep 节奏保持出厂行为，实测才有效 |

**实验口径披露（进 HLD R7-k 与票面 Answer）**：实测得到的「DROP→磁盘回落」T 值是
**`catalog_trash_expire_second=300` 条件下的实测数**，不冒充默认参数下的数值；
默认参数（86400）下的 T 只能给结构性推断（86400 + bin 清扫周期 + BE sweep），标**推断**。
06a 留下的 bin 条目（DropTime 12:29:55，滞后 ≈18 min 仍在）与 02:10 的三条（≈10.6 h 仍在）
作为默认参数下「FE 侧不主动释放」的行为旁证。

## 4. 实施前置（01 号凭证已全过）

- 六服务 healthy；`SHOW FRONTENDS` / `SHOW BACKENDS` 全 Alive；
- 磁盘余量：WSL ext4 932 GB avail，BE `AvailCapacity` 910.145 GB——1 GB 级探针数据占比可忽略；
- 三表基线已对账；recycle bin 基线快照已存（01 号凭证 [5]）。

## 5. 观察窗与中止判据

- 观察窗上限 **90 min**（`catalog_trash_expire_second=300` + FE recycle 清扫周期 + BE sweep
  `min_garbage_sweep_interval=180` / `max_garbage_sweep_interval`（源码默认 3600，可变面实测见 01）；
  若 90 min 内磁盘未回落：登记「自然路径未在窗内回落」+ 尝试 `ADMIN CLEAN TRASH`（若 4.1.5 支持）
  作为**人工触发点**，与自然路径明确分开标注，不混算 T。
- 中止判据：三张已验证表任一行数变化 / 任一非演练对象进入 recycle bin / FE 或 BE 报内部错误——
  立即停止后续 DROP 动作并 `RECOVER` + 收尾恢复配置。
