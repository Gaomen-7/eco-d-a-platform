# impl-06b: DROP 后回盘实测

Type: task
Status: ready-for-agent
Blocked by: —（impl-05 已结清）

> ✅ **2026-10-25 用户批准开票**（spec §8.2 三张实机 task 之一，裁决「**开票不实施**」——实施须用户另行触发）。
> 出处：spec §8.2 / map.md D8 ④——DROP 非立即回盘：走 catalog recycle bin、BE 经 GC 异步回收，「磁盘 T 内释放」**官方未给出** ⇒ 只能实机取证。

**What to build:** 实测「Doris DROP 到磁盘真实释放」的完整时间线，把 D8 ④ 的「官方未给出」变成一张本机实测曲线。产出：DROP → recycle bin → BE GC → 磁盘占用回落的时间轴记录。

**风险与边界（获批前提，实施时复述给用户）**：
- **破坏面最直接**：需 DROP 已验证卷态中的表/分区。缓解方案须先落盘再动手——
  ①优先 DROP **演练表**（先跑 impl-06a 建）或离线层非关键表；
  ②若必须动三张 ADS 表，实施前先做 Stream Load 可重放的导出备份（或 `SELECT ... INTO OUTFILE`），验证可回灌；
  ③记录 recycle bin 恢复命令路径（`RECOVER`）作为快照回滚手段。
- 实施前置：六服务 healthy、磁盘余量确认、备份落盘验证完成。

**验收标准**

- [ ] 时间线实测：DROP 时刻 → recycle bin 可见 → BE GC 后磁盘占用回落（`du`/`df` 或 BE storage 指标），逐点记录时刻与字节
- [ ] 「T 内释放」给出本机实测数（标**实测**，不冒充官方保证）；与 D8 ④ 的官方未给出口径对照登记
- [ ] 若动过已验证表：备份回灌后行数对账逐表相同，`SHOW FRONTENDS`/`SHOW BACKENDS` 全 Alive
- [ ] 结论登记 HLD §8-R7（或 §8-R5 补注），含 recycle bin 行为与 GC 参数的官方出处引用
- [ ] 全部本地提交，未推送
