# impl-06c: compaction ×20 GB 对撞实测

Type: task
Status: ready-for-agent
Blocked by: —（impl-05 已结清）

> ✅ **2026-10-25 用户批准开票**（spec §8.2 三张实机 task 之一，裁决「**开票不实施**」——实施须用户另行触发）。
> 出处：spec §8.2 / map.md D8 ⑥——compaction 耗时与放大数字**官方未给出**；唯一官方系数＝env-checking 页「BE＝总量×3×**1.4**」（×1.4 逐字＝"40% reserved space for background compaction"，integrated+3 副本口径，本仓 1 副本仅 ×1.4 适用——**推导**，非官方保证）。

**What to build:** 用 20 GB 量级写入对撞 BE compaction，实测三个官方欠账数字：compaction 耗时、空间放大、对 `doris-be 3g` 内存上限的压力形态。产出与 HLD §4.2 预告的「带载压力形态」互补的第 3 种形态证据。

**风险与边界（获批前提，实施时复述给用户）**：
- 大批量写入会占满 BE 磁盘与 compaction 带宽，**实施须安排在无其他验证活动的时间窗**；
- 对撞数据落**独立表/独立分区**，结束后整表 DROP 并衔接 impl-06b 的回盘观测（两票可联动）；
- 内存压力观测只记录不施救——若 BE 因 compaction OOM，按实机事实登记（本身即证据），恢复走容器重启；
- 实施前置：磁盘余量 ≥ 对撞量 ×2（预留 ×1.4 放大 + 写入原始量）。

**验收标准**

- [ ] 对撞实测：写入 20 GB 级数据，记录 compaction 前后磁盘占用、compaction 完成耗时（BE compaction 指标/日志逐点记录）
- [ ] 空间放大实测数与「×1.4 推导」比对，结论标**实测**与**推导**两个口径，不互相冒充
- [ ] `doris-be 3g` 上限下的内存压力形态记录（峰值、是否触发 memtable flush 限流）
- [ ] 对撞数据清理：DROP + 回盘实测时间线（可与 impl-06b 联动取证），磁盘回到对撞前基线
- [ ] 结论登记 HLD §8-R7 与 §4.2 带载形态注，全部数字标口径
- [ ] 全部本地提交，未推送
