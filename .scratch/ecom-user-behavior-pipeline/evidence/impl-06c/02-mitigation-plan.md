# impl-06c 缓解方案（动手前落盘，2026-10-25）

> 按 impl-06b 同款纪律：缓解方案先于一切写操作落盘。本票破坏面比 06b 更小（不 DROP 任何既有对象），
> 但 20 GB 级写入的**资源占用面**是新的，逐条列明。

## 1. 破坏面（明示）

- **新增**：库 `ecom_collision`、表 `compaction_probe`（DUPLICATE KEY(id)、BUCKETS 8、副本 1、payload VARCHAR(2048) 同 06b 形状）。
- **零接触**：三张已验证 ADS 表（`ads_window_metric`/`ads_funnel_step`/`ads_funnel_step_v2`，基线行数 30132/2427/9146 以实测为准）、Paimon 五层、Kafka topic、Superset——全程不动，前后行数对账。
- **零 FORCE**：全部 DROP 走非 force（回收站可 RECOVER，06b 已实机验证 RECOVER 路径）。
- **FE 全局配置**：仅回盘观测阶段临时 `catalog_trash_expire_second` 86400→300（06b 同款实验参数），观测完即恢复 86400 并复核。

## 2. 实施前置（逐条检查后才开始灌数）

1. 六容器 healthy；
2. 磁盘余量 ≥ 对撞量×2 = 40 GB（实测 WSL ext4 932 GB 可用，远超）；
3. BE du 基线、BE metrics 基线、`docker stats` 基线落盘（01 号凭证）；
4. FE `catalog_trash_expire_second` 确认在默认 86400（01 号凭证）。

## 3. 资源风险与边界

- **磁盘**：20 GB 原始写入 + ×1.4 compaction 余量口径；WSL ext4 实测余量 ≫ 需求，BE 所在卷无外溢路径（vhdx 1 TB 盘）。
- **compaction 带宽**：对撞期间本会话不跑任何其他验证活动（`make verify`/replay/回盘观测一律不做）——时间窗独占。
- **内存（只记录不施救）**：`doris-be` 3g 上限下观测压力形态；**若 BE 因 compaction/load OOM，按实机事实登记（本身即证据），恢复走 `docker restart eco-doris-be`，不调任何配置施救**。
- **观测频率**：后台观测器 15 s 采样（docker stats + BE metrics + du），灌数与沉降全程连续。

## 4. 回滚路径

| 对象 | 回滚 |
| --- | --- |
| `ecom_collision.compaction_probe` | 非 force DROP → recycle bin →（实验参数下）600 s 级自动回收；RECOVER 可回滚窗口 ≥600 s（06b R7-k 口径） |
| `catalog_trash_expire_second` | 恢复 86400 + `SHOW FRONTEND CONFIG` 复核 |
| BE OOM | `docker restart eco-doris-be`（volume 态保留） |
| 三张 ADS 表 | 前后行数对账；RECOVER 路径已验证可用（06b 03 号凭证） |

## 5. 时间窗声明

本会话独占：20 GB 灌数（预计 40 min 级）+ compaction 沉降观测（时间不定，观测到客观沉降判据为止）+ 回盘时间线（600 s 级）全程无其他验证活动并行。
