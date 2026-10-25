# impl-06b: DROP 后回盘实测

Type: task
Status: resolved
Blocked by: —（impl-05 已结清）

> ✅ **2026-10-25 用户批准开票**（spec §8.2 三张实机 task 之一，裁决「**开票不实施**」——实施须用户另行触发）。
> 出处：spec §8.2 / map.md D8 ④——DROP 非立即回盘：走 catalog recycle bin、BE 经 GC 异步回收，「磁盘 T 内释放」**官方未给出** ⇒ 只能实机取证。
> ✅ **2026-10-25 本会话触发实施并结清**（用户 `/implement` 发起）：全部 5 项验收达成，Answer 见文末。

**What to build:** 实测「Doris DROP 到磁盘真实释放」的完整时间线，把 D8 ④ 的「官方未给出」变成一张本机实测曲线。产出：DROP → recycle bin → BE GC → 磁盘占用回落的时间轴记录。

**风险与边界（获批前提，实施时复述给用户）**：
- **破坏面最直接**：需 DROP 已验证卷态中的表/分区。缓解方案须先落盘再动手——
  ①优先 DROP **演练表**（先跑 impl-06a 建）或离线层非关键表；
  ②若必须动三张 ADS 表，实施前先做 Stream Load 可重放的导出备份（或 `SELECT ... INTO OUTFILE`），验证可回灌；
  ③记录 recycle bin 恢复命令路径（`RECOVER`）作为快照回滚手段。
- 实施前置：六服务 healthy、磁盘余量确认、备份落盘验证完成。

**验收标准**

- [x] 时间线实测：DROP 时刻 → recycle bin 可见 → BE GC 后磁盘占用回落（`du`/`df` 或 BE storage 指标），逐点记录时刻与字节
- [x] 「T 内释放」给出本机实测数（标**实测**，不冒充官方保证）；与 D8 ④ 的官方未给出口径对照登记
- [x] 若动过已验证表：备份回灌后行数对账逐表相同，`SHOW FRONTENDS`/`SHOW BACKENDS` 全 Alive
- [x] 结论登记 HLD §8-R7（或 §8-R5 补注），含 recycle bin 行为与 GC 参数的官方出处引用
- [x] 全部本地提交，未推送

## Answer（2026-10-25 结清）

### 核心结果

**非 force DROP → 磁盘真实释放，实测 691 s（≈11.5 min）**，条件＝`catalog_trash_expire_second=300`（缓解方案 §3 实验参数，收尾已恢复 86400）。数据量级 600,000 行 / 原始 1,024,688,895 B（随机 hex payload，列存编码实测压缩约 2×——hex 为 4-bit 熵，**并非不可压缩**；净增可测即达实验目的），落盘净增 534,864 KB、释放后 du 回落 ≈ 灌数前基线。

| 时间线（UTC，2026-10-25） | 事件 | 口径 |
| --- | --- | --- |
| 12:57:57（T0） | `DROP TABLE ecom_rehearsal.reclaim_probe`（非 force） | 实测 |
| 12:58:02.9 | FE 日志 `dropTable/recycleTable ... is force drop: false`；bin 条目当刻可见（**DataSize 522.409 MB**）；du 631,708 KB **未动** | 实测（06b [A]） |
| 12:58→13:06 | bin 滞留段：名义过期 13:03:02（300 s）**未被擦**，`eraseTable eraseNum: 0` 连续 8 tick | 实测 |
| 13:08:19.491 | FE `eraseTable` eraseNum:1——恰在 DropTime+**600 s** 后首个 30 s tick | 实测（06b [C]） |
| 13:09:22.7–23.3 | BE 8 个 tablet 逐个 `erase ... reason=move to trash` + `delete tablet dir ... directly due to trash_file_expire_time_sec is 0`（官方日志逐字）——**无 trash 停留、直接物理删除**；距 FE 擦除 ≈63 s（元数据传播） | 实测（06b [E]） |
| 13:09:28 | du 96,032 KB ≈ 灌数前基线 96,768 KB；T_END，elapsed **691 s**（FE dropTable 起算 680 s） | 实测 |

| 「T 内释放」口径 | 值 | 性质 |
| --- | --- | --- |
| `catalog_trash_expire_second=300` 条件 | **691 s** | **实测** |
| 默认参数 86400 | ≈24.3 h（86400+表条目超额滞后 317 s+传播 63 s+<1 s） | **推断**（旁证：bin 里 02:10 的 3 条分区条目滞留 10.6 h 未被 FE 动过） |

### 新发现（已随 HLD §8-R7-k 登记）

1. **表条目的 bin 滞留不受 `catalog_trash_expire_second<600` 加速**：同配置下**分区条目**名义过期后首个 30 s tick 即被擦（12:49:49 七条 `reason: expired`），**表条目**却留到 DropTime+600 s 后首个 tick（实测 617 s，与「≥600 s 保底下限」精确吻合）。该下限**官方未给出**（4.1.5 源码本轮网络不可达，best-fit 推断+实测互证）。⇒ 运维口径：**RECOVER 可回滚窗口表级条目至少 600 s，调小 expire 压不到 600 s 以下**。
2. **BE 删除路径官方日志逐字**：`trash_file_expire_time_sec=0` 下 `delete tablet dir ... directly`——与 4.1.5 源码一致、与 4.x BE 配置文档页 86400 冲突（维持 research 07 §3.1 冲突登记）；FE 擦元数据→BE 物理删除 ≈63 s 传播间隔。
3. `SHOW DATA FROM <table>` 灌数后立即查为 0.000/0（统计异步滞后，同 06a）；`SHOW DATA FROM <db>` 不是合法形态（解析为表名 → "No database selected"）。

### 边界执行情况

- 全程只动演练库对象、未用 `DROP ... FORCE`；缓解方案先于一切 DROP 落盘（02 号凭证，12:47 UTC）。
- 三张已验证 ADS 表零接触：30132 / 2427 / 9146 前后逐表相等；FE/BE 全 Alive。
- RECOVER 库级/表级路径实机验证（06a 遗产恢复行数=1；`rollback_check` DROP→bin 可见→RECOVER→行数=1）——票面缓解②「若动三张 ADS 表须备份」前置未触发，缓解③已超额完成。
- 配置变更逐条回退（`catalog_trash_expire_second` 300→86400 复核）；收尾 `DROP DATABASE ecom_rehearsal`（非 force）回到 06a 同款 bin 滞留终态。

### 诚实记录（四处脚本瑕疵，不影响证据）

① 04 号脚本因 `SHOW DATA FROM <db>` 语法中止（04b 补齐）；② 05 号观测终止判据误用灌数后基线、24 s 假 SUCCESS（05 号凭证只当时间线前两点，全程轮询以 05b 为权威）；③ 06 [1] 的 awk 时间窗引号链 bug 输出空（FE 日志逐字以 06b 直查替代，eraseNum:0 tick 序列与 sweeper 行经 code-review 补录进 06b [F][G]）；④ 05b 的 `trash_dir` 列因壳函数 bug 恒 0 不作判据（trash=0 以 metrics 与 BE 日志逐字为准）。另：首版「随机 hex 不可压缩」表述已更正（实测压缩约 2×）；`DropPartitionClause` 引用节号更正为 research 07 §3.3（06b [H]）。

### 凭证

`evidence/impl-06b/`：`00-SUMMARY.md`（时间线+口径对账）+ `02-mitigation-plan.md`（动手前落盘）+
01 前置基线 / 03 RECOVER 演练 / 04+04b 灌数 / 05+05b 观测 / 06 收尾恢复 / 06b 日志直查逐字。
HLD §8 新增 **R7-k**。全部本地提交，未推送。
