# impl-06b 证据包 — DROP 后回盘实测（2026-10-25）

## 结论（时间线）

**非 force DROP → 磁盘真实释放，本机实测 691 s（≈11.5 min）**，
条件＝`catalog_trash_expire_second=300`（实验参数，缓解方案 §3）。
数据量级：600 000 行 / 1,024,688,895 原始字节（随机 hex payload），
落盘后 du 净增 534,864 KB（96,768 → 631,632 KB ≈ 原始的 52%，hex 为 4-bit 熵、
列存编码实测压缩约 2×——不影响「净增可测、回落可测」的实验目的），
释放后回落 96,032 KB（≈基线）。

| # | 时刻（UTC） | 事件 | 凭证 |
| --- | --- | --- | --- |
| 1 | 12:56:47–12:57:47 | 灌数后 du 稳定采样 ×3：632,124 / 632,180 / 631,632 KB | 04b [2] |
| 2 | **12:57:57（T0）** | `DROP TABLE ecom_rehearsal.reclaim_probe`（非 force）发出 | 05 [T0] |
| 3 | 12:58:02.929 | FE 日志：`dropTable ... is force: false` + `recycleTable ... is force drop: false`；bin 条目当刻可见（DropTime 12:58:02，**DataSize 522.409 MB**）；du 631,708 KB **未动** | 06b [A] + 05 [T0+x] |
| 4 | 12:58→13:06 | bin 滞留段：名义过期 13:03:02（300 s）**未被擦**——13:03:19–13:06:49 连续 tick `eraseTable eraseNum: 0`；期间 RECOVER 窗口一直开着 | 06b [C] 说明 |
| 5 | **13:08:19.491** | FE `eraseTable():484` erase table[1792919869634]，eraseNum:1——**恰在 DropTime+600 s 后的首个 30 s tick** | 06b [C] |
| 6 | 13:09:22.7–23.3 | BE：8 个 tablet 逐个 `erase tablet_id=... reason=move to trash` + `delete tablet dir ... directly due to trash_file_expire_time_sec is 0`——**直接删目录、无 trash 停留**（距 FE 擦除 ≈63 s，元数据传播） | 06b [E] |
| 7 | 13:09:28 | du 96,032 KB ≈ 灌数前基线 96,768 KB——**磁盘真实释放**；T_END，elapsed **691 s** | 05b 收口 |

trash 全程为 0（`doris_be_disks_trash_used_capacity` 恒 0、trash 目录从未出现）——
`trash_file_expire_time_sec=0` 实测生效（BE 日志逐字），与 4.1.5 源码一致、与 4.x 文档页 86400 冲突
（维持 research 07 §3.1 冲突登记）。BE sweeper 每 180 s 一条旁证（06b [F]）。

## 「T 内释放」口径（验收②）

| 口径 | 值 | 性质 |
| --- | --- | --- |
| **`catalog_trash_expire_second=300` 条件下 DROP→磁盘释放** | **691 s（DROP 命令起算）/ 680 s（FE 日志 dropTable 起算至 BE 删除完成）** | **实测**（本票一手） |
| 默认参数（86400）下 | 86400 + 表条目超额滞后 ≈317 s + FE→BE 传播 ≈63 s + <1 s ≈ **24.3 h** | **推断**（结构性外推，未实测） |
| 默认参数旁证 | bin 里 02:10 的 3 条 `p20260924` 分区条目滞留至 12:49（≈10.6 h）未被 FE 动过，直到本轮配置切 300 才被清（06b [B]） | 实测旁证 |

与 D8 ④「官方未给出『磁盘 T 内释放』」对照：本票把该空白在**本机、单副本、1 GB 级、
expire=300** 条件下变成实测数；**不冒充默认参数下的数值**，默认参数只给推断并注明。

## 新发现（登记 HLD §8-R7-k）

1. **表条目的 bin 滞留不受 `catalog_trash_expire_second<600` 加速**：配置切 300 后，
   **分区条目**即时可清（12:49:49 首个合格 tick 擦 7 条，reason: expired），
   **表条目**却留在 bin 里直到 DropTime+600 s 后的首个 tick（实测 617 s）——
   与「表条目存在 ≥600 s 保底下限」精确吻合。该下限的存在与数值**官方未给出**
   （4.1.5 源码本轮网络不可达未取得；best-fit 推断 + 实测互证）。
   ⚠️ 推论：**RECOVER 的可回滚窗口对表级条目至少 600 s，调小 expire 不能把窗口压到 600 s 以下**。
2. **BE 删除路径官方日志逐字**："delete tablet dir ... **directly** due to trash_file_expire_time_sec
   is 0"——0 配置下无 trash 停留、直接物理删除（`tablet_manager.cpp:1400` reason=move to trash
   随后 `data_dir.cpp:947` 直接删）；FE 擦元数据 → BE 物理删除间有 ≈63 s 传播间隔。
3. `SHOW DATA FROM <table>` 灌数后立即查得 Size 0.000 / RowCount 0（统计异步滞后，同 06a 现象）；
   且 `SHOW DATA FROM <db>` 不是合法形态（Doris 解析为表名 → "No database selected"）。

## 验收对账

| 票面验收 | 结果 | 凭证 |
| --- | --- | --- |
| ① 时间线实测（DROP→bin 可见→GC 后回落，逐点时刻+字节） | ✅ 7 点时间线 | 05 [T0]+[T0+x] / 05b 全程轮询 / 06b [A][C][E] |
| ② 「T 内释放」本机实测数（标实测）+ 与 D8 ④ 对照登记 | ✅ 691 s 实测（expire=300 条件）+ 24.3 h 推断（默认参数） | 本文件 §口径 |
| ③ 若动过已验证表：备份回灌对账（前置未触发）+ RECOVER 路径 + FE/BE 全 Alive | ✅ 三表全程零接触（30132/2427/9146 前后逐表相等）；RECOVER 库级/表级实机验证（06a 遗产恢复行数=1；rollback_check DROP→bin→RECOVER→行数=1） | 03 [2]–[5] / 06 [5][6] |
| ④ 结论登记 HLD §8-R7（含 recycle bin 行为与 GC 参数官方出处） | ✅ **R7-k** | HLD §8 |
| ⑤ 全部本地提交，未推送 | ✅ | 本次 commit |

## 缓解方案执行情况（票面风险边界）

- 全程**只动演练库对象**，未用 `DROP ... FORCE`；三张已验证 ADS 表行数前后逐表相等（30132 / 2427 / 9146）。
- 缓解方案先行落盘（02 号凭证，12:47 UTC，先于一切 DROP）。
- 配置变更逐条回退：`catalog_trash_expire_second` 300 → 86400 已恢复并复核（06 [3]）。
- 收尾 `DROP DATABASE ecom_rehearsal`（非 force，13:17:42，06b [D]），回到 06a 同款终态
  （bin 内滞留，默认参数 24 h 后由 FE 清）。

## 诚实记录（本票四处脚本瑕疵，不影响证据有效性）

1. 04 号脚本在 [5] 因 `SHOW DATA FROM ecom_rehearsal`（Doris 把库名解析为表名）报错被
   `set -e` 掐断，[6][7] 缺席——由 04b 补齐采样与清理（灌数本身 Success，凭证完整）。
2. 05 号观测脚本终止判据写成「du ≤ 灌数后基线+20 MB」——DROP 后数据还在盘上、du 本就未变，
   24 s 即假 SUCCESS 退出；**05 号凭证只当「T0 时刻 + bin 立即可见 + 盘未动」的时间线前两点用**，
   全程轮询由 05b（判据改回「回落到灌数前基线」）承接，T0 以 05 记录的 12:57:57 为权威。
3. 06 号凭证 [1] 的 awk 时间窗因 docker exec 双层引号转义输出空（其行内注释
   「空＝本身即事实」**是错的**——窗口内 eraseTable 实际发生了，凭证尾部已追加更正块），
   FE 日志逐字以 **06b 直查**替代；另有两处 code-review 补录（eraseNum:0 tick 序列、
   sweeper 10-25 行逐字）进 06b [F][G]。
4. 05b 的 `trash_dir` 列因壳函数封装 bug 恒显示 0 KB（不可信）；**该列不作判据**，
   trash 停留为 0 的判据以 `doris_be_disks_trash_used_capacity`（独立采样，全程 0）与
   BE 日志逐字 `directly due to trash_file_expire_time_sec is 0`（06b [E]）为准。

## 凭证清单

| 文件 | 内容 |
| --- | --- |
| `01-preconditions-baseline.sh/.out.txt` | 前置（healthy/Alive/磁盘）+ 三表基线 + bin 基线快照 + GC 配置基线 |
| `02-mitigation-plan.md` | **DROP 动手前落盘的缓解方案**（范围/回滚/配置清单/中止判据） |
| `03-recover-and-rollback-drill.sh/.out.txt` | RECOVER 库级+表级实机验证 + 配置切 300 + probe 建表 |
| `04-seed-probe.sh/.out.txt` | 1 GB 随机数据生成 + Stream Load Success（600000 行）+ 中止点说明 |
| `04b-settle-and-cleanup.sh/.out.txt` | du 稳定采样 ×3 + SHOW DATA 修正形态 + 临时文件清理 |
| `05-drop-and-observe.sh/.out.txt` | T0 DROP + bin 立即可见 + 盘未动（判据 bug 后 24 s 退出，见诚实记录） |
| `05b-resume-observe.sh/.out.txt` | **全程轮询（权威）**：bin 消失 @650 s、du 回落 @691 s |
| `06-postcheck-restore.sh/.out.txt` | BE 日志取证 + 配置恢复 86400 + DROP DATABASE 收尾 + 三表对账 + Alive |
| `06b-fe-log-extract.out.txt` | FE/BE 日志直查逐字（A 时刻 DROP / B 分区条目即时擦 / C 表条目 617 s 擦 / D 收尾 / E BE 物理删除 / F sweeper 节奏 / G eraseNum:0 tick 序列 / H 引用更正）。**唯一无配对 .sh 的凭证**：06 [1] awk 窗口 bug 后的直查替代 + code-review 补录，采集命令已在文中逐字给出 |

脚本执行面：`wsl bash` → 仓库根相对路径；MySQL 一律 `docker compose run --rm client mysql -h doris-fe`（HLD §4.4 两跳）；
Stream Load 直发 BE（`docker cp` + 容器内 `curl localhost:8040`，`Expect: 100-continue`）。
日期耦合警示同 06a：本包所有时刻为 2026-10-25 取证，不可原样改日重跑。
