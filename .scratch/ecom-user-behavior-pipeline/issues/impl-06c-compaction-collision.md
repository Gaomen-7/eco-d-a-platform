# impl-06c: compaction ×20 GB 对撞实测

Type: task
Status: resolved
Blocked by: —（impl-05 已结清）

> ✅ **2026-10-25 用户批准开票**（spec §8.2 三张实机 task 之一，裁决「**开票不实施**」——实施须用户另行触发）。
> 出处：spec §8.2 / map.md D8 ⑥——compaction 耗时与放大数字**官方未给出**；唯一官方系数＝env-checking 页「BE＝总量×3×**1.4**」（×1.4 逐字＝"40% reserved space for background compaction"，integrated+3 副本口径，本仓 1 副本仅 ×1.4 适用——**推导**，非官方保证）。
> ✅ **2026-10-25 本会话触发实施并结清**（用户 `/implement` 发起）：全部 6 项验收达成，Answer 见文末。

**What to build:** 用 20 GB 量级写入对撞 BE compaction，实测三个官方欠账数字：compaction 耗时、空间放大、对 `doris-be 3g` 内存上限的压力形态。产出与 HLD §4.2 预告的「带载压力形态」互补的第 3 种形态证据。

**风险与边界（获批前提，实施时复述给用户）**：
- 大批量写入会占满 BE 磁盘与 compaction 带宽，**实施须安排在无其他验证活动的时间窗**；
- 对撞数据落**独立表/独立分区**，结束后整表 DROP 并衔接 impl-06b 的回盘观测（两票可联动）；
- 内存压力观测只记录不施救——若 BE 因 compaction OOM，按实机事实登记（本身即证据），恢复走容器重启；
- 实施前置：磁盘余量 ≥ 对撞量 ×2（预留 ×1.4 放大 + 写入原始量）。

**验收标准**

- [x] 对撞实测：写入 20 GB 级数据，记录 compaction 前后磁盘占用、compaction 完成耗时（BE compaction 指标/日志逐点记录）
- [x] 空间放大实测数与「×1.4 推导」比对，结论标**实测**与**推导**两个口径，不互相冒充
- [x] `doris-be 3g` 上限下的内存压力形态记录（峰值、是否触发 memtable flush 限流）
- [x] 对撞数据清理：DROP + 回盘实测时间线（可与 impl-06b 联动取证），磁盘回到对撞前基线
- [x] 结论登记 HLD §8-R7 与 §4.2 带载形态注，全部数字标口径
- [x] 全部本地提交，未推送

## Answer（2026-10-25 结清）

### 核心结果

20 批 Stream Load 共 **12,000,000 行 / 原始 20.51 GB**（13:49:46–14:20:03 UTC，1817 s，20/20 Success）对撞独立表 `ecom_collision.compaction_probe`（DUPLICATE、BUCKETS 8、副本 1）。**三个官方欠账数字全部拿到实测**：

| 欠账 | 实测 | 口径 |
| --- | --- | --- |
| compaction 耗时 | 任务层 **89 s**（T_LOAD_END→本表末条 compaction）；磁盘层 **843 s（≈14 min）**（du 阶梯回落至终值 14:34:06，旧 rowset 异步删除） | **实测**（两口径不混用；官方未给出） |
| 空间放大 | **×0.53**（落盘净增 10,964,914,176 B ＝ 10.96 GB 十进制 ÷ 原始 20,507,488,896 B；随机 hex 2:1 压缩 + DUPLICATE 无索引膨胀，换形态不外推）vs **×1.4 ≈28.7 GB**（env-checking 页逐字，**磁盘规划下限系数**） | **实测** 与 **推导** 两口径并列，回答不同问题，不互相冒充 |
| 回盘时间线 | **DROP→磁盘释放 925 s**（T0 14:48:30 → T_RECLAIMED 15:03:55；DropTime 起算 921 s，`catalog_trash_expire_second=300` 实验参数） | **实测**（06b 同款条件，跨 20 倍量级联动互证：FE eraseTable 同为 DropTime+617 s ⇒ 「600 s 保底」＝best-fit 推断（R7-k）与数据量无关） |

### 对撞与内存压力形态（验收③核心）

- **对撞确认**：灌数开始后 243 s 本表 compaction 启动、全程并行，122 条 succeed（cumulative 77 条合计 2877.26 s + base 45 条合计 2.50 s）**零失败**、merged=0；全局 compaction score 峰值 19（阈值 20 之下——20 为本机配置快照实测，07c）。
- **内存形态**：cgroup 峰值 **2.711 GiB/3 GiB（90.4%）**，**无 OOM**（OOMKilled=false）；**memtable 限流真实触发＝写线程等待反压 637 次**（p50 1 s / p90 3 s / **max 8 s**，全在灌数窗内），而 **flush 线程池 queue_size 全程 0**——瓶颈在进程内存水位不在 flush 带宽；pswpout 3600→13648（轻量）。限流线由「进程内存 + 系统可用内存（本机仅 ~1.2 GB）」共同驱动。
- 与 HLD §4.2 预告互补的**第 3 种带载形态证据**已落 §4.2 带载形态注（此前：空载、Flink JM cgroup OOM 两种）。

### 新发现（已随 HLD §8-R7-l 登记）

1. 「是否触发 memtable flush 限流」的观测物必须押 **`memtable_memory_limiter.cpp:254` waited 行 + flush 线程池 queue_size** 两处——本次 waited 637 次与队列恒 0 并存，只看其一会误判。
2. **全局 compaction 计数器不能当沉降判据**（`ecom_ads`/`__internal_schema` 背景小 compaction 每分钟 +300/+150 永不稳定）；沉降判据＝**tablet 级日志静默 + du 稳定**双判据。
3. `SHOW RECYCLE BIN` **静默空输出（非合法形态）**，正确为 `SHOW CATALOG RECYCLE BIN`。
4. BE 限流线依赖系统可用内存——cgroup 3g 上限不是唯一水位依据（运维排障时 `docker stats` 百分比会低估触发点）。

### 边界执行情况

- 缓解方案先于一切写操作落盘（02 号凭证）；全程只动 `ecom_collision`，无 FORCE，无施救（未触发恢复路径）。
- 三张已验证 ADS 表前后行数逐表相等（30132/9146/2427）；BE `RestartCount=0`、FE/BE 全程 Alive。
- 配置逐条回退复核（`catalog_trash_expire_second` 300→86400）；收尾 `DROP DATABASE ecom_collision`（非 force）同 06b bin 滞留终态；观测终态 du 回基线（storage 95,728 KB ≈ 基线 95,540 KB——04-observer.out.txt 末样本 15:08:26 对 01 号基线）。

### 诚实记录（五处脚本瑕疵，不影响证据链）

① 观测器 v1 用错指标名（4.1.5 无 `tablet_max_compaction_score`，实际 `doris_be_disks_compaction_score`）→ v3 权威；v2 因热编辑脚本提前退出（教训：后台脚本运行中不可编辑）。② 07 号 grep 漏 `-a`（be.INFO 二进制混合文件）→ 07b 补齐 122 条逐字。③ 08 号 `SHOW RECYCLE BIN` 语法踩坑 → 08b 补取证。④ 10 号限流计数含历史会话噪声 → 10b/10c 按时间窗重做。⑤ settle-watch 全局判据被背景噪声淹没 → 06b 双判据替代。另：观测窗内 `-235` 全量命中 92 处逐字核验均为 `output_version=[227-235]` 类区间号，非错误码。

### 凭证

`evidence/impl-06c/`：`00-SUMMARY.md`（三欠账 + 两口径对账 + 新发现）+ `02-mitigation-plan.md`（动手前落盘）+ 01 前置基线 / 03 建表 / 04-observer.out.txt（260 样本全程 15 s 采样）/ 05 灌数逐批 + 05-chunk-resp/（20 批全响应）/ 06 沉降观测 / 07+07b+07c 放大与逐字取证 / 08+08b 回盘时间线 / 09+10+10b+10c 内存形态。HLD 新增 **R7-l** 与 §4.2 带载形态注。全部本地提交，未推送。
