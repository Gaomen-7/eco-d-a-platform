# impl-02b: 解超期 sink 阻断，让超期表真正落行

> GitHub: https://github.com/Gaomen-7/eco-d-a-platform/issues/2
> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: ready-for-agent
Blocked by: impl-02a

**What to build:** 让 `ads_late_expired_count` 真正落行——票面验收第 3 条「超期段确有迟到计数条目」可达，`make verify` 不再因这张表空表而 FAIL。

## 背景

超期链断点已由 impl-02a 定位到三处之一。已排除的可能性（不要重做）：生成器不可达（真实 topic 重放得 expired=565）、表不可写/分区不覆盖（与可写表策略一致）、sink 异步线程静默抛错（真实日志 0 条 ERROR）、`bufferFlushMaxRows < 10000` 被 connector 拒绝（硬校验，必须 ≥10000）。

## Acceptance criteria

- [ ] `ads_late_expired_count` 在实机上 `COUNT(*) > 0`，且行内容合理（`window_start` 落在真实事件窗口、`late_lag_ms` 为正、`cnt` 为正整数）
- [ ] 修法落在**已定位的那一处断点**上，且有因果证据（不是「碰巧能写了」）
- [ ] 若修法涉及 connector 参数：说明为何该参数能突破「几十行批次达不到 10000 行阈值」这一约束（例如随 checkpoint 触发 flush），并标注该数字为**设计值**而非官方保证
- [ ] `make verify` 的 `doris-ads-rows` 项转为 PASS（三张表都 >0）
- [ ] 三表行数、Stream Load 次数的复测命令写在交付说明里，且**读 `/opt/flink/log/*.log` 而非 `docker logs`**
- [ ] 全量 pytest 仍全绿；`make verify` 退出码 0

## Notes

- **不要擅自改表**：把超期行改写进 `ads_window_metric` 属**契约变更**，票面验收第 3 条明写落点在 `ads_late_expired_count`，改表要走冻结项变更流程，先问用户。
- `docker/doris/init.sql` 的三表 DDL 不要改。
- 不跑 `down -v`（销毁 P0 已验证的卷态成果；L0 归 impl-05）。
- 若 `LateCount` POJO 类型推断仍可疑：注意 `java.time.LocalDateTime` 会被退化成 `RAW(...)` 导致 Calcite 报 `Cast function cannot convert`，当前代码用 `java.sql.Timestamp` + 侧 `CAST(... AS TIMESTAMP(3))` 是已验证可行的组合。