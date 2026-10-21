# impl-02d: make verify-fault 实机跑通 + 反例证伪（验票 6/7/9）

> GitHub: https://github.com/Gaomen-7/eco-d-a-platform/issues/4
> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: ready-for-agent
Blocked by: impl-02b, impl-02c

**What to build:** 让 `make verify-fault` 在实机上跑通并退出 0——证明确有重放发生、且重放被幂等吸收。票面验收第 6、7、9 条都落在这里。

## 背景：码已写好，但从没在实机上跑过

`fault.py` 的判据与流程已实现（六步顺序冻结：输入冻结 → 等 lag==0 → 拍基线 → kill TM → 等 RUNNING（120 s 设计值）→ 等 checkpoint（300 s 设计值）→ 比对三条件），且 `judge_recovery()` 是纯函数、已有单测覆盖反例。**但整链从未实机跑过一次**，所以：

- 票面验收第 6 条（超时内完成、退 0、stdout 明示两结论）：未证
- 票面验收第 7 条（故意改坏输入 ⇒ 退非 0）：未证
- 票面验收第 9 条（脏率分子只含 DLQ 增量；跑两遍重复升而脏率不升）：未证

已就绪的正例证据（作基线参考）：`duplicate_absorbed` 行 35 条、`is_settled` 两态 12/242、漏斗 `cnt_inferred` 沿 step 1→4 为 `[10,8,6,6]` 非增。

## Acceptance criteria

- [ ] `make verify-fault` 在超时内跑完、退出码 0，stdout 明示「重复通道计数上升」与「DLQ 增量==0」两条结论
- [ ] **证伪**：故意改坏一个输入让 DLQ 上升时，`make verify-fault` 退出码**非 0**（证明断言真的在断言，不是恒真）
- [ ] **幂等**：kill TM 后两表 count/sum 与基线**全等**，而重复通道计数**上升**（条数一致但重复计数升，才说明确有重放且被吸收）
- [ ] 脏率分子只含 DLQ 增量：同一批数据跑两遍，重复通道计数上升而**脏率不升**
- [ ] 超时数字 120 s / 300 s 在代码与 stdout 中均标注**设计值**，未标为官方保证
- [ ] 恢复判据的**纯函数部分**（`judge_recovery`）新增反例单测：条数一致但 DLQ 上升 ⇒ fail
- [ ] 失败时把关键快照（基线、恢复后、三条件实测值）落进 `artifacts/` 供事后复盘

## Notes

- **执行面**：宿主无 `make`，须在 WSL 下跑；`docker` 命令需 `MSYS_NO_PATHCONV=1`。
- kill TaskManager 会中断 Doris Stream Load，日志里会看到 `InterruptedException` —— 这是取消噪声**不是**故障，别当阻断（这点本会话已被误导过一次）。
- 复测 Stream Load 次数读 `/opt/flink/log/*.log`，**不要用 `docker logs`**（stdout 已轮转，读到的是过期内容）。
- 超期顶点的算子计数器不可信（名称冲突，见 impl-02a），本票的基线比对以 **Doris 侧 count/sum** 为准，不要依赖 Flink 顶点级 `numRecordsOut`。
- 不跑 `down -v`。