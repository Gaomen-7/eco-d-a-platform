# impl-06c · 20 GB compaction 对撞实测 — 总结（2026-10-25）

> D8 ⑥「compaction 耗时与放大数字官方未给出」的本机实测落点。全程只动独立库 `ecom_collision`，
> 三张已验证 ADS 表前后行数逐表相等（30132/9146/2427），无 FORCE、无 OOM、配置逐条回退。
> 凭证：01–10c 号脚本与 .out.txt + 05-chunk-resp/（20 批 Stream Load 全响应）+ 04-observer.out.txt（260 样本全程采样）+ 07-compaction-words.out.txt（122 条逐字）。

## 1. 实验对象与量级（实测）

| 项 | 值 | 口径 |
| --- | --- | --- |
| 表 | `ecom_collision.compaction_probe`，DUPLICATE KEY(id)、BUCKETS 8、副本 1、payload VARCHAR(2048) | 设计值（形状同 06b） |
| 写入 | **20 批 × 600,000 行**，Stream Load（label coll_chunk_*，BE 容器内 curl → localhost:8040） | 实测 |
| 原始 CSV 总量 | **20,507,488,896 B（20.51 GB 十进制 / 19.10 GiB）** | 实测（逐批 stat 累加） |
| 行数 | **12,000,000**（COUNT(*) 核对） | 实测 |
| 灌数窗 | 13:49:46 → 14:20:03 UTC，**1817 s（30.3 min）**，20/20 Success | 实测 |

## 2. compaction 对撞（验收①）

- **对撞确认**：灌数开始后 **243 s**（13:53:49）本表第一条 compaction 成功行出现，此后灌数与 compaction 全程并行；观测窗内全局 compaction score 峰值 **19**（`base_compaction_max_compaction_score=20` 之下——20 为本机配置快照实测，07c）；size_based score 峰值 **23**。
- **本表 compaction 总账**（07-compaction-words.out.txt，122 条 succeed、**零失败**、merged_row_num 合计 0——DUPLICATE 表如预期无合并）：
  - cumulative **77 条，elapsed 合计 2877.26 s**；base（ordered data）**45 条，合计 2.50 s**；compaction 输入行合计 41,475,001（行被多轮 compaction 反复重写）。
- **耗时两个口径（不混用）**：
  1. **任务层**：灌数结束（14:20:03）→ 本表最后一条 compaction 成功（14:21:32）＝ **89 s**；
  2. **磁盘层**：du 从 14.73 GB（14:23）阶梯回落至 10.72 GB（**14:34:06 起稳定**）＝灌数结束后 **843 s（≈14 min）**（旧 rowset 异步删除期；判据＝du 连续稳定 + 本表 compaction 日志静默，两者先后达成）。官方未给出任何口径 ⇒ 均标**实测**。
- 沉降判据方法论修正：settle-watch 初版押全局 compaction 计数器，被 `ecom_ads`/`__internal_schema` 的背景小 compaction 噪声淹没（每分钟 +300/+150 持续不动点）⇒ 改押**本表 tablet 日志静默 + du 稳定**双判据（本目录 `06b-tablet-activity.sh` 辅助脚本留档判定过程）。

## 3. 空间放大：实测 vs ×1.4 推导（验收②，两口径不冒充）

| 口径 | 值 | 性质 |
| --- | --- | --- |
| **实测落盘净增**（8 tablet du 求和 **10,707,924 KiB** ＝ 10,964,914,176 B ＝ **10.96 GB 十进制 / 10.21 GiB**；data 目录较灌数前基线净增 10,697,948 KiB） | **≈10.96 GB（10.21 GiB）** | **实测** |
| **实测放大率**（落盘净增 10,964,914,176 B ÷ 原始写入 20,507,488,896 B） | **×0.53**（字节精确 0.5346，即压到原始量的 53%） | **实测**（前提：随机 hex payload 4-bit 熵可 2:1 压缩 + DUPLICATE 无索引膨胀；**换数据形态此数不外推**） |
| **×1.4**（env-checking 页逐字 "40% reserved space for background compaction"，integrated+3 副本口径；本仓 1 副本适用 ×1.4） | 1.4 × 20.51 GB ≈ **28.7 GB 规划预留** | **推导**（官方给出的用途是**磁盘规划下限**，非实测放大率） |
| FE `SHOW DATA` 视图 | 10.211 GB / 12M 行（统计口径与 du 同量级） | 实测（FE 视图） |

**结论**：×1.4 与实测 ×0.53 回答的不是同一个问题——前者是「为 compaction 预留多少磁盘」的规划系数，后者是「本数据形态压实后真实占用」。对撞实测**不推翻也不证实** ×1.4 作为规划系数；但给出经验上界参考：本形态下真实占用 ≈ 原始量 53%，×1.4 的预留相当于真实占用的 2.6 倍余量。

## 4. `doris-be 3g` 内存压力形态（验收③，只记录未施救）

| 观测物 | 值 | 出处 |
| --- | --- | --- |
| cgroup 内存峰值 | **2.711 GiB / 3 GiB（90.4%）** @13:54:02（灌数开始后 4 min） | 04-observer.out.txt（15 s 采样 ×260） |
| BE 进程内存（官方日志口径）峰值 | **2.27 GB**（vm/rss）@14:08:10 | 10c（waited 行内） |
| `doris_be_memory_allocated_bytes` 峰值 | 2.159 GiB @14:11:17 | 04-observer.out.txt |
| **memtable 限流真实触发** | **waited 637 次（全部在灌数窗内）**：p50 **1.0 s** / p90 **3.0 s** / **max 8.0 s**（13:50:31 → 14:20:00 全程存在）；触发时 load mem 峰值 61.41 MB、写入线程被 memtable memory limiter 卡住等 flush | 10c（BE 日志逐字 `memtable_memory_limiter.cpp:254 waited ... for memtable memory limit`） |
| flush 线程池队列 | 全程 0（`MemTableFlushThreadPool` queue_size 恒 0；`memtable_flush_running_count_limit=2` 未排队）——**瓶颈不在 flush 带宽，在进程内存水位** | 04-observer.out.txt + BE 配置 |
| `reached hard limit` | 窗内 4121 次（13:49:46→15:01:00；注意 13:00 背景段即有 KB 级触发——该日志行由进程内存水位驱动，非 load 专有） | 10c |
| swap | pswpout 3600 → 13648（≈40 MB 换出，轻量） | 04-observer.out.txt |
| OOM | **无**（`OOMKilled=false`、窗口内 0 条真实 OOM 日志；全量检索的 92 个 "-235" 命中均为 `output_version=[227-235]` 类区间号，10b 逐字排除） | 10/10b |
| BE 配置背景 | `mem_limit=80%`（≈2.4 GB 进程上限）、`load_process_max_memory_limit_percent=50`、`compaction_memory_bytes_limit=1 GB`、`flush_thread_num_per_store=6` | 07c |

**压力形态结论**（与 HLD §4.2 预告互补的第 3 种形态）：`doris-be 3g` 灌 20 GB 级**不会 OOM**，但会持续触发 **memtable 内存限流反压**（写入线程等待最长 8 s），compaction score 全程在阈值之下，磁盘才是 compaction 的主要吞吐维度（77 轮 cumulative 合计 2877 s CPU 时间 vs **14 min** 磁盘沉降）。另登记环境事实：本机 WSL VM 的 `sys available memory` 在 BE 基线时仅 ~1.2 GB——限流线由「进程内存 + 系统可用内存」共同驱动，不单看 3g cgroup 上限。

## 5. 清理与回盘时间线（验收④，联动 06b）

| 时刻（UTC） | 事件 | 对照 06b |
| --- | --- | --- |
| 14:48:30 / 14:48:34 | T0 `DROP TABLE`（非 force）/ bin DropTime | T0 12:57:57 |
| 14:50:12 | `SHOW CATALOG RECYCLE BIN` 条目可见（DataSize **10.211 GB**） | 5 s 内可见（同） |
| 14:58:51 | FE `eraseTable eraseNum: 1`（**DropTime+617 s**——跨 20 倍量级与 06b 的 617 s 精确互证「600 s 保底」＝best-fit 推断（R7-k），非官方给出） | DropTime+617 s（同构） |
| 15:00:29.5–30.2 | BE 8 tablet **0.7 s 内**逐个 `delete tablet dir ... directly due to trash_file_expire_time_sec is 0`（官方逐字）；距 FE 擦除 **98 s**（06b 63 s） | 传播间隔 63 s → 98 s |
| 15:03:55 | T_RECLAIMED：du(data)=9924 KiB ≈ 灌数前基线 9976 KiB | du ≈ 灌数前基线 |
| **合计** | **DROP→磁盘释放实测 925 s**（T0 命令 14:48:30 起算；DropTime 14:48:34 起算 921 s） | 06b：691 s（1 GB 级） |

参数与收尾：`catalog_trash_expire_second` 300（实验参数）→ 恢复 **86400** 复核 ✓；`DROP DATABASE ecom_collision`（非 force）bin 滞留终态同 06b ✓；三张已验证 ADS 表前后行数逐表相等 ✓；FE/BE 全程 Alive、BE `RestartCount=0`。

## 6. 新发现 / 方法论教训（已随 HLD §8-R7-l 登记）

1. **memtable 限流的真形态**：3g 上限下触发的是 `memtable_memory_limiter` 的**写线程等待反压**（waited 最长 8 s），不是 flush 线程排队（队列恒 0）——「是否触发 memtable flush 限流」的观测物应押 `memtable_memory_limiter.cpp:254` 的 waited 行 + flush 线程池 queue_size 两处，缺一误判。
2. **全局 compaction 计数器不能当沉降判据**：`ecom_ads`/`__internal_schema` 背景小 compaction 持续运行，全局计数永不稳定；必须 tablet 级日志静默 + du 稳定。
3. **`SHOW RECYCLE BIN` 不是合法形态**（静默空输出）；正确为 **`SHOW CATALOG RECYCLE BIN`**（08 号踩坑，08b 补取证）。
4. **600 s 表条目保底跨量级互证**：20 GB 级（10.96 GB 落盘）与 06b 的 1 GB 级 DropTime→eraseTable 时延同为 617 s——保底与数据量无关（best-fit 推断加强，非官方给出）。
5. BE 的限流线由「进程内存 + **系统可用内存**」共同驱动（waited 行逐字含 sys available 字段）——cgroup 上限并非唯一水位依据。

## 7. 诚实记录（脚本瑕疵）

① 观测器 v1 用了不存在的指标名 `tablet_max_compaction_score`（4.1.5 实际为 `doris_be_disks_compaction_score`）→ v1/v2 日志留档废弃，v3 为权威；v2 因热编辑脚本提前 EOF 退出（教训：后台脚本运行中不可编辑）。② 07 号 [5] 的 grep 漏 `-a`（be.INFO 二进制混合文件，06b 同款坑）→ 07b 补齐，122 条逐字无缺。③ 08 号误用 `SHOW RECYCLE BIN` → 08b 以正确语法补 bin 可见性。④ 10 号限流计数为全量（含历史会话）→ 10b/10c 按时间窗重做。⑤ settle-watch 全局判据被背景噪声淹没提前终止（20 tick）→ 沉降改用 06b 双判据。各瑕疵均不影响主证据链。
