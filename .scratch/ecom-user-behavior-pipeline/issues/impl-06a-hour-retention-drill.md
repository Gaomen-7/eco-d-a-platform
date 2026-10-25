# impl-06a: HOUR 分级保留演练实机

Type: task
Status: ready-for-agent
Blocked by: —（impl-05 已结清）

> ✅ **2026-10-25 用户批准开票**（spec §8.2 三张实机 task 之一，裁决「**开票不实施**」——实施须用户另行触发）。
> 出处：spec §8.2 / map.md D8 ③ / `REQ-F-OLAP-03` 判据「配置 + 小时级演练」的行为层。

**What to build:** 用一张**独立演练表**补齐「HOUR 分级保留真的会删分区」的行为层证据——`REQ-F-OLAP-03` 判据的配置层（`start=-30` 保留恰 30 个日分区、`create_history_partition=true` 建表当场 34 分区）已由 impl-05 前序实机证实，本票补行为层：`time_unit=HOUR` + 小 `start` 的独立表 ≤1200 s 可观测真实分区删除。

**风险与边界（获批前提，实施时复述给用户）**：
- 只建**独立演练表**（`ecom_ads` 外或独立命名），**不碰** `ads_window_metric` / `ads_funnel_step` / `ads_late_expired_count` 三张已验证表；
- 保留策略到期删除只作用于演练表自身分区，不构成对已验证卷态的破坏；
- 实施前置：确认磁盘余量、`SHOW FRONTENDS`/`SHOW BACKENDS` 全 Alive、六服务 healthy。

**验收标准**

- [ ] 演练表建表成功：`time_unit=HOUR`、小 `start`、`create_history_partition=true`，动态分区调度间隔 600 s（`官方给出`）生效
- [ ] 观测到至少一轮「过期 HOUR 分区被调度器真实删除」（起止时刻记录，≤1200 s 可观测窗口）
- [ ] 已验证三张 ADS 表在演练全程行数不变（演练前后 `COUNT(*)` 对账）
- [ ] 结论回填 HLD §8（`REQ-F-OLAP-03` 行为层证据列或 R7 表），数字标 `官方给出`/实测
- [ ] 全部本地提交，未推送
