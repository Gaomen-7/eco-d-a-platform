# impl-02d: make verify-fault 实机跑通 + 反例证伪（验票 6/7/9）

> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: in-progress（2026-10-21 夜中断，未结案；续跑入口见文末「进度」节）
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
  - ⚠️ **措辞已过时**：实机证伪「重复通道计数上升」不成立，改用「源端重读条数 >0」作为重放证据，
    见下方「进度」节 §3。验收文本待与用户确认后改写。
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

---

## 进度（2026-10-21 夜中断，**未结案**）

### 1. 已完成的代码改动（`src/ecomgen/fault.py` + `tests/test_fault.py`）

流程从「等 lag==0 → 拍基线 → kill」重写为能证重放的 8 步：

```
1 等外部输入冻结（源端 read-records 连续 2 次不增长）
2 等一次 checkpoint 完成（时序锚点，1 s 粒度）
3 喂固定 dump（seed=71 / 120 会话 / rate=600）并等源端读完
4 等 Doris 落库稳定（count/sum 指纹连续两次全等）后拍基线
5 kill TM  →  docker start eco-flink-tm
6 等 RUNNING（running==total>0）+ 重放读完 + 新 checkpoint（编号严格更大）
7 等 Doris 稳定后拍恢复后快照 → 判三条件
8 写报告；失败时落 verify-fault-<ts>.json 快照
```

判据改动：第三条子判据由「重复通道计数上升」换成 **`replay_observed`
（源端重读条数 >0）**，理由见 §3。`judge_recovery()` 新增必填关键字参数
`replay_observed`。新增单测覆盖：正常通过、count/sum 变化、DLQ 上升（反例）、
无重放（反例）、stdout 两结论。**`pytest tests/test_fault.py` = 7 passed**。

实机过程中修掉的**四个环境级坑**（都已写进代码注释）：

| 坑 | 实测现象 | 修法 |
|---|---|---|
| `/jobs/overview` 无 `lastCheckpoint` | 编号恒 `None`，checkpoint 判据恒假 FAIL | 改读 `/jobs/<jid>/checkpoints` 的 `latest.completed.id` |
| `state==RUNNING` 假通过 | TM 刚死时 JM 仍报 RUNNING，`tasks.running=0` | 要求 `running==total>0` |
| `compose kill` 后 TM 不回来 | 容器停在 **exited**（`restart: unless-stopped` 不生效） | `docker start eco-flink-tm`（**不要** `compose up`） |
| `compose up -d <svc>` 连带重建 | 重启过 **flink-jm**（session 模式无 job graph 持久化 ⇒ 作业被抹掉，日志逐字 "Successfully recovered 0 persisted job graphs"），也重启过 **doris-fe**（9030 拒连） | 一律 `docker start`；**禁止**在故障流程里用 `compose up` |
| 快照太慢吃掉时序窗口 | 每次 `compose run --rm client` 约 8~12 s，6 个标量 = 1 分钟，30 s 的 checkpoint 周期被整个吃掉 ⇒ 重放恒 0 | 合并成 1 条 SQL（`SNAPSHOT_SQL`）+ 复用常驻 `eco-client` 走 `docker exec`（<1 s） |

### 2. 实机结果（最后一轮 vf4 起被 FE 重启打断，未完成）

最好的一轮（报告 `20261021T160248Z` 之前那次）：

```
重放证据（源端重读条数未上升）：否
DLQ 增量==0：是
两表 count/sum 与基线全等：是
=== verify-fault 完成：PASS=8 FAIL=2 ⇒ 退出码 1 ===
```

即 **`counts 全等` 与 `DLQ 增量 0` 两条已在实机成立**，只差「重放证据」。
FAIL 的两条是 `replay-observed` 与 `fault-idempotent-absorb`（后者挂在前者上）。

### 3. 关键判断：为什么换掉「重复通道计数上升」

两条独立原因，均已实机验证：

1. `ads_window_metric` 是 `UNIQUE KEY (window_start, metric_name, dim_key)`，
   重复行 `dim_key=''` ⇒ 每分钟只留 1 行、`metric_value=1`（实测 **49 行 / SUM=49**）。
   同一分钟再吸收一次也不会变。
2. 重放事件在**恢复点之后**，不在被恢复的去重状态里 ⇒ 被当成**新事件**，
   根本不进 `duplicate` 侧输出。吸收发生在 Doris 唯一键，不在重复通道。

⇒ `duplicate_absorbed` 观测的是「在流重复」（同 seed 重复投递），不是故障重放。
**用户已裁决**：改用「确有重放」的证据（保留 counts + DLQ 两条）。

> 补充（与用户原选的偏差，需回看）：用户原选「消费组 lag 回退重读」，实机发现
> **lag 用的是已提交 offset，而 Flink 只在 checkpoint 时提交** ⇒ 数据读完了 lag 仍 >0，
> 恢复后 lag 归零也只说明「提交追上」，区分不了重放。故判据落在
> **源端 `read-records` 增量**（同一件事的直接观测），lag 只作辅助信息打印/记录。

### 4. 未解决：重放窗口抓不到

`make verify-fault` 仍 FAIL 在「重放=0」。已定位到的时序约束：

- checkpoint 间隔实测 **恰好 30 s**（`/checkpoints` 的 `trigger_timestamp` 间隔 29997~29999 ms）；
- 锚点（步骤 2）发现「刚完成」≈ +2 s，若快照/落库稳定判定耗时 <28 s 就能卡进窗口；
- 但实测 `checkpoint_before` 仍比锚点大 3~4 号（快照曾耗时 1 分钟），dump 已被 checkpoint 覆盖。

**下一步要验证的假设**（已改好代码，但还没跑到）：
把快照压到 <1 s、稳定判定压到 ~8 s 后，步骤 3→5 总计应约 12~15 s，
应当能落在 30 s 窗口内。若仍为 0，则说明 Flink 恢复时**不是**从 checkpoint 里的
source offset 回退（需查 `KafkaSource` 的 offset 恢复口径），那是新的取证点。

### 5. 续跑入口（醒来从这里开始）

```bash
# 0) 先确认环境（故障测试后 TM / client / 作业都可能不在位）
docker compose ps -a --format "{{.Name}} {{.State}}"
docker exec eco-flink-jm sh -c "wget -qO- http://localhost:8081/jobs/overview"

# 缺 TM：      docker start eco-flink-tm
# 缺 client：  docker start eco-client
# 作业不在 RUNNING（JM 重启会丢掉会话作业）：
docker cp "F:\Anaconda practice project\eco-a-d-platform\flink\target\ecom-flink-0.1.0-SNAPSHOT.jar" \
  eco-flink-jm:/opt/flink/lib/ecom-flink.jar
docker exec eco-flink-jm /opt/flink/bin/flink run -d \
  -c com.ecom.flink.P1RealtimeJob /opt/flink/lib/ecom-flink.jar

# 1) 跑
cd '/mnt/f/Anaconda practice project/eco-a-d-platform' && make verify-fault > /tmp/vf.log 2>&1
grep -nE "kill 前|--- [0-9]/8|重放证据|DLQ 增量|全等|结论" /tmp/vf.log
```

**判读要点**：看 `kill 前：checkpoint=<N>` 与锚点得到的编号差。
若差 ≥1，dump 已被 checkpoint → 重放必然为 0，先压缩快照耗时或改用更小的 dump
（`DUMP_SESSIONS`）把窗口挤回来。

### 6. 环境现状（离开时实测，2026-10-21 夜里）

```
eco-client running      eco-doris-be running   eco-doris-fe running
eco-flink-jm running    eco-flink-tm running   eco-kafka running
jobs/overview → 4dc247b0… RUNNING 28/28
```

- 曾发生的两次误伤（已恢复，卷态未动）：
  1. `docker compose up -d flink-tm` 连带重建 `flink-jm` ⇒ session 模式无 job graph 持久化，
     作业 `7809fdab…` 被抹掉（JM 日志逐字 "Successfully recovered 0 persisted job graphs"）；
     后重拷 jar + `flink run -d` 得到现役作业 `4dc247b0…`。
  2. 同一次 `compose up`（带 client profile）连带重启 `doris-fe`，9030 短暂拒连；
     已自恢复 healthy，数据未丢。
- 未跑 `down -v`，未做任何破坏性清理。
- 未提交的改动：`src/ecomgen/fault.py`、`tests/test_fault.py`、本票
  （本轮结束时一并提交）。
