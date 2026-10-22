# impl-02d: make verify-fault 实机跑通 + 反例证伪（验票 6/7/9）

> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: **done**（2026-10-22 最终轮：§17 的六处加固**已取得实机证据**——正例两连绿
（`054228Z` / `054701Z`，含跨轮第二轮）、反例按预期失败（`055228Z`），
三条加固后新增的机读凭证全部成立。见文末 §19）
Blocked by: impl-02b, impl-02c

**What to build:** 让 `make verify-fault` 在实机上跑通并退出 0——证明确有重放发生、且重放被幂等吸收。票面验收第 6、7、9 条都落在这里。

## 背景：码已写好，但从没在实机上跑过

`fault.py` 的判据与流程已实现（六步顺序冻结：输入冻结 → 等 lag==0 → 拍基线 → kill TM → 等 RUNNING（120 s 设计值）→ 等 checkpoint（300 s 设计值）→ 比对三条件），且 `judge_recovery()` 是纯函数、已有单测覆盖反例。**但整链从未实机跑过一次**，所以：

- 票面验收第 6 条（超时内完成、退 0、stdout 明示两结论）：未证
- 票面验收第 7 条（故意改坏输入 ⇒ 退非 0）：未证
- 票面验收第 9 条（脏率分子只含 DLQ 增量；跑两遍重复升而脏率不升）：未证

已就绪的正例证据（作基线参考）：`duplicate_absorbed` 行 35 条、`is_settled` 两态 12/242、漏斗 `cnt_inferred` 沿 step 1→4 为 `[10,8,6,6]` 非增。

## Acceptance criteria

- [x] `make verify-fault` 在超时内跑完、退出码 0，stdout 明示重放证据与「DLQ 增量==0」两条结论
  - ⚠️ **措辞已过时**：实机证伪「重复通道计数上升」不成立，改用「源端重读条数 >0」作为重放证据，
    见旧「进度」节 §3 与本轮 §9。验收文本待与用户确认后改写。
  - 2026-10-22 实机：`PASS=12 FAIL=0 SKIP=0 ⇒ 退出码 0`（详见 §8）。**稳定绿已达成**：
    连续四轮 `20261022T045159Z` / `T045615Z` / `T050025Z` / `T045159Z` 之后的
    加固前四连绿（末轮 `/tmp/nvf4.log`）全 PASS，单轮 **3~4 分钟**（此前 14m52s），见 §12。
    ⚠️ **加固版（§17）尚未取得实机证据**，见 §18。
- [x] **证伪**：故意改坏一个输入让 DLQ 上升时，`make verify-fault` 退出码**非 0**（证明断言真的在断言，不是恒真）
  - 新增 `make verify-fault-corrupt`（注入 5 条 `schema_invalid`）。实机：`DLQ 增量!=0`、
    `结论：不通过`、退出码 1，包装目标将其反转为 `✓ 按预期失败`。见 §8 与 §12。
- [x] **幂等**：kill TM 后两表 count/sum 与基线**全等**，而重复通道计数**上升**（条数一致但重复计数升，才说明确有重放且被吸收）
  - 前半（counts 全等）已实机成立（§8 正例，三轮连绿）。
  - **后半在单次 kill 里不成立**——这是本轮第 7 个判据缺陷（§9 末条）：同一分钟内的重复
    被 Doris 唯一键折叠（`metric_value` 覆盖为 1，SUM 不变）；**跨分钟才可观测**。
    已实机验证跨分钟路径：dup SUM 162→163、DLQ 不动、脏率不升。
  - 最终判据以「源端重读条数 >0」承载重放证据，重复通道只作旁证（§9、§12）。
- [x] 脏率分子只含 DLQ 增量：同一批数据跑两遍，重复通道计数上升而**脏率不升**
  - 实机（跨分钟，两遍间隔等过整分边界）：dup 162→163，DLQ 247→247，脏率 0.006567→0.006521。见 §9。
  - 同时补了此前**完全没有单测**的 `check_dlq_rate_not_increased_result` 四条真值表用例。
  - 收口时另修一处**跨轮假 FAIL**：`dirty-rate-baseline.json` 还是 10-21 23:16 写下的
    `dlq=0`（当时 DLQ sink 尚未接线，§7 缺陷 F），与现在 `dlq=247` 比必然「不升失败」。
    基线已重立（`dlq=257/40620`），`make verify` 连跑两次：`SKIP→PASS`、退出码 0。
- [x] 超时数字 120 s / 300 s 在代码与 stdout 中均标注**设计值**，未标为官方保证
  - 本轮新增的静默窗口 30 s、tick 提前量 200 s、tick 裕度 5 s 同样逐处标 `设计值`。
- [x] 恢复判据的**纯函数部分**（`judge_recovery`）新增反例单测：条数一致但 DLQ 上升 ⇒ fail
  - `tests/test_fault.py` 23 条（本轮 +8：tick 4 条、窗口字面量 1 条、scoped SQL 1 条、
    空窗口拒绝 1 条、基线非空护栏 1 条），`tests/test_verify_gaps.py` 15 条。
  - 全仓 `pytest`：**118 passed**；`ruff check` / `ruff format --check` 全绿；`basedpyright` **0 errors**。
- [x] 失败时把关键快照（基线、恢复后、三条件实测值）落进 `artifacts/` 供事后复盘
  - 新增时序取证字段（锚点编号、kill 时编号、restored、两处 read-records、静默窗口、三条超时、
    本轮 `scope_windows` / `watermark_tick_event_time` / `run_nonce`）。

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

### 4. 未解决：重放窗口抓不到 —— **本节结论已于 2026-10-22 被证伪，勿再沿用**

> ⚠️ **作废**：下面「下一步要验证的假设」是**错的**。真根因不是时序窗口，而是
> `_source_records_in` 读的顶点汇总字段**恒为 0**（同刻各 subtask 之和 = 263）。
> 压缩快照耗时做了也没用 —— 判据读的字段本身就是 0。见本轮 §7.A 与 §9。
> 保留原文只为记录当时的推理路径。

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

---

## 进度（2026-10-22 白天，**已通但仍未结案**）

> 上一节（2026-10-21 夜）的结论「重放窗口抓不到、要压缩快照耗时」**已被证伪**。
> 真根因是判据读错了字段（§7.A），与窗口宽度无关。

### 7. 本轮定位的六个判据缺陷（全部实机取证）
| # | 旧做法 | 实机现象（决定性证据） | 修法 |
|---|---|---|---|
| **A** | 读顶点汇总 `metrics['read-records']` | 同刻：顶点汇总 **0**，而 `/jobs/<jid>/vertices/<vid>` 各 subtask 之和 **263**（正好等于投递条数） | 按 subtask 求和（`_subtask_metric_sum`）。**这是「replay=0」卡了整夜的真根因**，不是时序窗口 |
| **B** | 用 `ecomgen live` 喂两遍当「重复投递」 | `live` 每轮把 `event_time` 平移到**当前墙钟**（`cli._live` 的 `round_start_ms`）⇒ 同 seed 是「同 event_id、不同分钟窗口」的**两批数据**，第二遍必然长新行 | 喂**字节相同的固定 dump**（`_fixed_batch`）：dump 一次 → 压缩 event_time → 落盘 → 两遍读它 |
| **C** | `state==RUNNING` 即「已恢复」 | `docker compose kill` 后 JM 仍报 `RUNNING 28/28` 约 **14 s**，旧判据第一次轮询就假通过 | 先见 `running<total` 或非 RUNNING（`saw_down`），再用 `/checkpoints` 的 `counts.restored` **严格递增**做机读凭证 |
| **D** | 「连续两次采样相等」即落定 | 8 s「稳定」之后又涨 **34 行**（窗口要等 `source.idle-timeout=30s` 才 fire） | 静默窗口 `QUIET_S`，并随批次跨度放大（见 §8） |
| **E** | 「投完立刻判读完」 | 投递返回时源端**还没开始读**，读数 == 投递前值 ⇒「连续两次不变」当场成立，基线拍早了 | `_wait_source_stable(at_least=投递前+投递条数)` |
| **F** | DLQ 用 `sinkTo(KafkaSinks.dlq())` 挂在 `env` 上 | **从未进作业图**：plan 只有 7 个顶点、`validate-dedup` 输出只列 `count-duplicate` / `funnel-only`，**没有 dlq**；投 5 条必脏事件后源端 268→273 而 DLQ 六分区 offset **全 0**、`.log` 字节 **全 0** | 改走 StatementSet（`registerDlqSink` + `createDlqView` + `insert into sink_dlq`） |

**F 的严重性**：它让「DLQ 增量==0」变成**恒真的假断言**——断言的是一条根本没接线的通道。
这正是票面验收第 7 条（改坏输入 ⇒ 退非 0）存在的意义：**是反例证伪把 F 逼出来的**。
修完后 DLQ 真的在写（实机 offset 0 → 247），而正例里「DLQ 增量==0」依然成立（247→247）。

### 8. 实机结果

正例（`make verify-fault`）：

```
--- 3/9 等整批静默落定（批次跨度 313 s ⇒ 静默窗口 373 s，设计值）后拍基线
  doris 已静默 373 s ⇒ 视为落定
  [1] RUNNING(28/28) restored=1 saw_down=False     ← 前 4 次都是「看着 RUNNING」
  [4] RUNNING(0/28)  restored=2 saw_down=True      ← 真的死了又回来
  重放证据（源端重读条数上升）：是
  DLQ 增量==0：是
  两表 count/sum 与基线全等：是
  结论：通过
=== verify-fault 完成：PASS=11 FAIL=0 SKIP=0 ⇒ 退出码 0 ===
```

反例（`make verify-fault-corrupt`，注入 5 条 `schema_invalid`）：

```
--- 5b/9 ⚠️ 反例注入：投 5 条 schema_invalid 事件（缺 event_type）
  重放证据（源端重读条数上升）：是
  DLQ 增量!=0：否
  两表 count/sum 与基线全等：否
  结论：不通过 —— 两表 count/sum 与基线不等；DLQ 增量不为 0（重复被误当脏，脏率上升）
=== verify-fault 完成：PASS=10 FAIL=2 SKIP=0 ⇒ 退出码 1 ===
[verify-fault-corrupt] ✓ 反例按预期失败（退出码 1）—— 判据确实在断言
```

### 9. 关键读数（供后续复用，勿重新推导）

- **`read-records` 顶点汇总恒 0**，必须走 `/jobs/<jid>/vertices/<vid>` 按 subtask 求和。
- **failover 后计数器从 0 重计**（实测 kill 前 526 → 恢复后稳定 263，正是这批条数）。
  所以「重放条数」= **恢复稳定后的读数本身**，不是「恢复后 − kill 前」。旧口径在重置时恒为负。
- **`/checkpoints` 的 `counts.restored`** 是「确实恢复过」最干净的观测面（真恢复必 +1）。
- **`provisional_emitted` 是处理时间心跳**（每 10 s 一发、按当前分钟分桶），只要墙钟走就长新行。
  实测一次 kill 的 ±12 分钟增量里，26 行有 19 行是它 ⇒ 它必须从 count/sum 比对里排除，
  否则「全等」在大批次下**不可达**（那是判据不可达，不是幂等失败）。
- **同一分钟内的重复在 Doris 侧不可观测**：`ads_window_metric` 的
  `UNIQUE KEY (window_start, metric_name, dim_key)` + `duplicate_absorbed` 行
  `dim_key=''`/`metric_value=1` ⇒ 覆盖写、SUM 不变。跨分钟才有增量。
- **环境级坑（复现时必看）**：
  - 作业 jar **必须在 `/opt/flink/usrlib`**，且 `/opt/flink/lib` 不许有副本；
    历史 `docker cp` 进 lib 会让父子类加载器各加载一份，报
    `ClassCastException: cannot assign instance of OffsetResetStrategy`，`/jobs/overview` 恒 `[]`。
    修法：清 lib → **`docker restart eco-flink-jm eco-flink-tm`**（新 JVM 才不命中缓存）→ 拷 usrlib → `flink run -d`。
  - `docker compose kill flink-tm` 后容器停在 **exited**，`restart: unless-stopped` 不会带回来；
    必须 `docker start eco-flink-tm`（**不要** `compose up`，会连带重建 JM 并抹掉会话作业）。
  - Doris FE 偶发自重启（实测 `RestartCount=1`），`9030` 会短暂拒连，等 1~2 分钟自恢复，数据不丢。
  - 宿主 `docker` 命令要 `MSYS_NO_PATHCONV=1`，否则 `/opt/...` 被改写成 Windows 路径。

### 10. 已完成 / 未结案

**已完成（本轮）**

- `src/ecomgen/fault.py`：9 步流程重写、六个缺陷全修、`--corrupt` 反例模式、
  失败快照加时序取证字段、`SNAPSHOT_SQL` 修 `Unknown column 'v'`（外层需 `AS v`）。
- `flink/src/main/java/com/ecom/flink/`：`GateSegment`（DLQ 改 StatementSet）、
  `P1RealtimeJob`、`DorisSinkSegment`（加 `insert into sink_dlq`）。**jar 已重建并部署**。
- `Makefile`：新增 `verify-fault-corrupt`（命中则把非 0 反转为成功）。
- `tests/test_fault.py`：12 条（新增 5 条反例/口径回归）；
  `tests/test_verify_gaps.py`：15 条（新增 4 条脏率真值表）。
- **`pytest`：107 passed；`ruff`：All checks passed。**

**未结案（下轮从这里开始）**

1. **耗时偏长**：批次压缩到 `BATCH_SPAN_S=50` 后，静默窗口跟批次跨度绑定
   （`max(QUIET_S, 跨度 + SETTLE_MARGIN_S)`）。最后一次实机是 **6m32s**，
   且仍 FAIL —— 原因是**上一轮残留的 45 条**让第 1 步的 `read-records=45`，
   预热/重投的边界不干净，恢复后又冒出 4 行 `revision=0` 的**新会话**（fn 1290→1294）。
   ⇒ 下轮要解决「跨轮残留」：要么每轮开始前显式把在途数据等干净（用 `at_least`
   从**投递前实测值**起算并确认零增长），要么在判据里**按本批 session 键**比对
   而不是全表（台账已倾向后者，需先定口径）。
2. **`make verify-fault` 最后一次实机是 FAIL（wm +2 / fn +4）**，
   而它**前一轮是 PASS、退出码 0**。即：判据已能真跑通，但**还不是稳定绿**。
   在解决 §10.1 之前，不要把本票标 done。
3. 代码**尚未提交**（HEAD 仍 `1912ec4`）。工作区改动：
   `src/ecomgen/fault.py`、`tests/test_fault.py`、`tests/test_verify_gaps.py`、
   `flink/src/main/java/com/ecom/flink/{GateSegment,P1RealtimeJob,DorisSinkSegment}.java`、
   `Makefile`、本票。另 `flink/target/` 为 gitignore（jar 不入库）。
4. 未跑 `down -v`，未做破坏性清理；Doris/Kafka 卷态未动。

### 11. 续跑入口

```bash
# 0) 环境（作业很可能因为 DMA/重启而不在位）
MSYS_NO_PATHCONV=1 docker ps -a --format "{{.Names}} {{.Status}}" | grep eco-
curl -sf http://127.0.0.1:8081/jobs/overview

# 缺作业 ⇒ 重部署（清 lib、重启 JM/TM、拷 usrlib、提交）
bash '/mnt/f/Anaconda practice project/eco-a-d-platform/.uv-cache/deploy-job.sh'

# 1) 正例
cd '/mnt/f/Anaconda practice project/eco-a-d-platform' && make verify-fault
grep -nE '静默|重放证据|DLQ 增量|全等|结论|完成：' /tmp/vf.log

# 2) 反例（期望脚本自身返回 0，且内部显示「按预期失败」）
make verify-fault-corrupt

# 3) 单测
uv run pytest -q && uv run ruff check src/ tests/
```

---

## 收口（2026-10-22 下午，**已结案**）

> §10 的两条未结案（耗时偏长、跨轮残留）都解掉了。真根因与 §10 当时的猜测**不同**：
> 不是「批次跨度太大／静默窗口不够」，而是**水印根本没被推过窗口末尾**，
> 以及**全表口径把上一轮的行算了进来**。

### 12. 第 7/8 个判据缺陷（本轮实机定位，均有决定性证据）

| # | 旧做法 | 实机现象（决定性证据） | 修法 |
|---|---|---|---|
| **G** | 指望「投批次」顺便推水印 | 批次被压在 `[now−90s, now−40s]`，而水印 = **最大事件时间 − 120 s** ⇒ 批次自己的窗口**永远等不到关闭**。所谓「Doris 已静默 110 s」是**假落定**（什么都没发出来）。Doris 里那批行的 `updated_at` 全是 **04:01:06** —— 正是**下一轮**（span3）把水印顶上去的那一刻 | 显式投**水印推进 tick**：每个分区一条、`event_time = windowEnd + allowedLateness + 5 s`，见 `tick_event_time` / `_feed_tick`；恢复后**再投一次**（failover 会把水印随 offset 一起回滚） |
| **H** | 基线/恢复后都比**全表** | span2 的差是 `wm +2 / fn +4`，全部落在 **03:53** 那一分钟 —— 而 span2 自己的批次窗口是 **04:00**。即「不幂等」的证据其实是**上一轮迟到落库** | 按**本批窗口**限定比对（`scoped_snapshot_sql`）；窗口字面量按 **UTC**（Doris 里就是 `2026-10-22 03:53:00` 这种形状） |

**H 的连带风险与被堵住的方式**：窗口限定一旦写错（时区/格式/集合为空），
快照会全 0 且「全等」轻松通过 —— 那是比假 FAIL 更糟的**恒真断言**。
故同时加了 `baseline_looks_populated()`：本批窗口内两表必须都有行，
否则 `fault-baseline` 直接 FAIL。空窗口集也直接 `ValueError`。

**另一个必须一起修的点（跨轮 event_id 撞车）**：
dump 是 `--seed 71` 固定种子 ⇒ **每轮产出同一批 event_id**，
而作业去重状态按 event_id 保 1 小时。于是第 2 轮的预热被第 1 轮的状态
**整批吸收**，本批窗口里一行都长不出来（实测 span2 预热 45 条后
`10970→10972` 只涨 2 行）。修法：每轮用一个 `nonce` 重写 event_id
（`uuid5`，确定性）——**轮内两遍投喂仍逐字节相同**（幂等证明不受影响），
跨轮不再撞车。tick 的 event_id 同样带 nonce。

### 13. 实机结果（本轮，四次运行）

| 时间（UTC） | 目标 | 结果 | 结论 |
|---|---|---|---|
| `045159Z` | `make verify-fault` | `PASS=12 FAIL=0 ⇒ 退出码 0` | 通过 |
| `045615Z` | `make verify-fault` | `PASS=12 FAIL=0 ⇒ 退出码 0` | 通过（**跨轮**，紧接上一轮） |
| `050025Z` | `make verify-fault` | `PASS=12 FAIL=0 ⇒ 退出码 0` | 通过（**四连绿**之一） |
| `050434Z` | `make verify-fault-corrupt` | `PASS=11 FAIL=2 ⇒ 退出码 1`，包装转身为 0 | **反例仍按预期失败** |
| `/tmp/nvf4.log` | `make verify-fault`（加固 tick 排空护栏后） | `PASS=12 FAIL=0 ⇒ 退出码 0` | 通过 |
| `/tmp/nvfc2.log` | `make verify-fault-corrupt`（同上代码） | `PASS=11 FAIL=2 ⇒ 退出码 1`，包装转身为 0 | 通过（5m35s） |

关键读数：

```
[verify-fault] 本轮 nonce=1a161f7ceb1
    本批窗口：2026-10-22 04:52:00, 2026-10-22 04:53:00；水印推进 tick event_time=1792644965000
    基线：Baseline(window_metric_count=28, window_metric_sum=45,
                   funnel_step_count=6, funnel_step_sum=6, dlq_offset=247, duplicate_count=191)
  重放证据（源端重读条数上升）：是
  DLQ 增量==0：是
  两表 count/sum 与基线全等：是
  结论：通过
=== verify-fault 完成：PASS=12 FAIL=0 SKIP=0 ⇒ 退出码 0 ===
```

反例（同一条流程，只多投 5 条 `schema_invalid`）：

```
  重放证据（源端重读条数上升）：是
  DLQ 增量!=0：否
  两表 count/sum 与基线全等：是          ← 故意只坏 DLQ 这一条
  结论：不通过 —— DLQ 增量不为 0（重复被误当脏，脏率上升）
=== verify-fault 完成：PASS=11 FAIL=2 ⇒ 退出码 1 ===
[verify-fault-corrupt] ✓ 反例按预期失败（退出码 1）—— 判据确实在断言
real  4m4.558s
```

**耗时**：单轮 **3~4 分钟**（`make verify-fault-corrupt` 实测 4m05s，
含反例注入与两次 tick）。旧版 14m52s；因为静默窗口不再跟批次跨度绑定
（`SETTLE_MARGIN_S` 已删除），也不再等「假落定」。

### 14. `make verify` 的一处基线过期（连带修掉的假 FAIL）

收口时 `make verify` 报 `FAIL dlq-rate-no-increase 0.006327 vs 基线 0.000000`。
根因不是判据错，而是 `artifacts/verify/dirty-rate-baseline.json` 还是
**10-21 23:16** 写下的 `dlq=0` —— 那一刻 DLQ sink 尚未接线（§7 缺陷 F），
「DLQ 恒 0」本身是已证伪的事实。删掉基线重立后：

```
第一次：SKIP dlq-rate-no-increase 首次运行，基线已落盘（dlq=257/40620） ⇒ 退出码 0
第二次：PASS dlq-rate-no-increase 0.006327 vs 基线 0.006327              ⇒ 退出码 0
```

`make verify` 稳态：`PASS=8 FAIL=0 SKIP=1`（SKIP 的是 P1 阶段设计内的
`paimon-five-layers`）。

### 15. 本轮改动清单

- `src/ecomgen/fault.py`：新增 `batch_end_ms` / `tick_event_time` /
  `build_tick_events` / `tick_events_for_batch` / `batch_window_literals` /
  `_utc_literal` / `scope_windows` / `scoped_snapshot_sql` /
  `baseline_looks_populated` / `_topic_partition_count` / `_feed_tick` /
  `_feed_tick_verified` / `_wait_replay_after_failover`；
  `_fixed_batch` 加 `nonce` 重写 event_id 并改用 `batch_end_ms`；
  `_wait_doris_settled` 改「限窗口 + 非空护栏」；删除 `SETTLE_MARGIN_S`
  与 `_batch_span_s`；流程 9 步改为 11 步（新增 2b tick、7b 恢复后再 tick）。
- `tests/test_fault.py`：23 条（+11）。
- **flink Java（三处，均为上一轮遗留、本轮随票一起提交）**：
  `GateSegment.registerDlqSink` / `createDlqView`（DLQ 走 StatementSet，修 §7 缺陷 F）、
  `P1RealtimeJob`（接线 DLQ 表与视图）、`DorisSinkSegment.attach`（加 `INSERT INTO sink_dlq`）。
- **`Makefile`**：新增 `verify-fault-corrupt`（并把它与 `verify-fault` 一起放进
  `.PHONY` 与 `make help` 的新增目标行）。
- 全仓：`pytest` **118 passed**、`ruff` 全绿、`basedpyright` **0 errors**（6 warnings）。

### 16. 环境现状（离开时）

```
eco-client / eco-doris-be / eco-doris-fe / eco-flink-jm / eco-flink-tm / eco-kafka 全 Up
作业 /jobs/overview → RUNNING 28/28
```

未跑 `down -v`，未做破坏性清理，Doris/Kafka 卷态未动。

---

## 17. 两轴 code-review 的结果与已做的加固（2026-10-22 下午）

按 `code-review` skill 对 `1912ec4..工作区` 做了 Standards / Spec 两轴并行评审。
**Spec 轴判定「结案声明只有部分被支撑」**，指出多条「能通过但没在断言」的路径；
逐条处置如下（全部已改进代码，**尚无加固版实机证据**）。

### 17.1 已修（判决性的假通过路径）

| # | 评审指出 | 为什么是假通过 | 修法 |
|---|---|---|---|
| 1 | `_wait_source_stable` 超时 `return last`（可能低于 `at_least`） | 调用方只判 `is not None` ⇒「源端没读完」被当成「读完了」，`fault-feed-dump` / `fault-replay-window` 假 PASS | 给了 `at_least` 而未达到 ⇒ 返回 `None`（判据该 FAIL） |
| 2 | `replay_observed = after_records > 0` 依赖「failover 会重置计数器」这一**未断言的前提** | 若 Flink 某天不重置，恢复后任何读数都 >0 ⇒ 该判据重变恒真 | 新增 `_wait_replay_after_failover`：机读断言 `min_seen < kill 前`（观测到重置），否则 `counter_reset=False` ⇒ FAIL |
| 3 | `_feed_tick` 只数 `produce()` 调用，无投递回调；`partitions=6` 写死 | 分区不存在/被拒时 `ticked=6` 仍成立 ⇒ `fault-watermark-tick` 什么都不证明 | 加 `on_delivery` 回调统计**成功条数**、打印首几条错误；分区数改读 `kafka-topics.sh --describe` 的 `PartitionCount` |
| 4 | `snapshot_baseline(windows=None)` 保留全表回退 | 全表口径已被实机证伪，留着即误用入口 | `windows` 改必填，删除全表回退与 `SNAPSHOT_SQL` |
| 5 | `DUPLICATE_COUNT_SQL` 的 `AS v` 字符串手术 + 4 行注释在两处复制 | 重复代码 | 新增 `DUPLICATE_COUNT_ALIASED_SQL`（源头带别名），两处直接内联；`DUPLICATE_COUNT_SQL` 保留为兼容引用 |
| 6 | Makefile：`verify-fault-corrupt` 不在 `.PHONY` / `make help` | 目标未注册 | 两者都补上 |

### 17.2 已修的文档/口径漂移（Standards 轴）

- **术语**：`GLOSSARY.md:158` 定的词是 **水印**（`_Avoid_: 水位线`），
  本轮新写的 44 处「水位」全部改回「水印」（票面同步改）。
- 模块 docstring「★ 四条实机反证」→ 表里其实 **8 行**，改为「八条」。
- `BATCH_END_LEAD_S` 注释原写「tick 落在未来 45~100 s 也无妨」，与
  `test_tick_is_never_a_future_event`（断言 `tick < now`）**互相矛盾**，
  且未来事件会被规则①判脏。已按实际算法重写：tick 恒落在 `now−15s ~ now−75s`。
- `TICK_PAD_MS` 注释单位写「（秒）」而常量是毫秒 → 改「（毫秒）」。
- `BATCH_SPAN_S` 注释残留「静默 ~110 s」（已改为固定 `QUIET_S=30`）→ 重写。
- `batch_window_literals` 里的行内注释说「tick 窗口一并纳入」，与 `scope_windows`
  的 docstring（**排除** tick 窗口）矛盾 → 删掉错的那句。
- `run_verify_fault` docstring 原写「顺序**冻结**（9 步）」→ 改为
  「顺序（11 步；`spec.md` §4.4 定的是判据，步序是实现细节）」，并补全 11 步清单。
- `_feed_tick_twice_safe` 名字与行为不符（并不投两次）→ 改名 `_feed_tick_verified`。
- `GateSegment.registerDlqSink` javadoc 写「并返回待插入的 SQL 视图名」而方法返回
  `void` → 改述；`DorisSinkSegment.attach` 里那条 DLQ INSERT 加了「为什么它挂在这里」的注释。
- `tests/test_fault.py` 里四处函数内 `from ecomgen.fault import …` 并到文件顶部。

### 17.3 明确**不修**（附理由，留作后续票的输入）

1. **spec §4.4 基线要求含「Kafka committed offset」+ 双口径对账**（「该值同时应等于
   seed 决定的期望脏数」）：本轮不做。理由——现有三条件（counts 全等 / DLQ 增量 0 /
   确有重放）已由实机反例证伪覆盖；再叠一层 offset 对账是**新判据**，属 §4.4 的
   重新协商，需要用户裁决而不是施工票自作主张。
2. **`duplicate_count` 现为纯旁证，而 spec §4.4 仍写「重复通道增量>0」**：
   这条已在 §3/§9 被实机证伪（同一分钟折叠、吸收发生在 Doris 唯一键而非重复通道），
   用户 2026-10-22 已裁决改用「源端重读条数>0」。**spec 文本尚未改写**，
   属用户已认账但文档欠账 —— 归 impl-02e 收口时一并改 spec。
3. **`等 lag==0` 闸门降级为打印信息**：spec §4.4 写着这一步，但实机证明
   committed offset 区分不了「读完」与「重放」（§3）。同样归 spec 改写欠账。
4. **50 s 事件时间压缩导致迟到/超期路径不在本流程覆盖**：这是 §7 缺陷 B 之后的
   有意取舍（超期可达性由 `make verify` 与 producer 单测锁住）。若要在本流程也测，
   需要单开小票，不在「实机跑通」的范围内。
5. **`verify-fault-corrupt` 包装目标把非 0 反转为 0**：票面验收第 7 条要求的是
   「`verify-fault` 退出码非 0」，内层 `python -m ecomgen.fault --corrupt` **确实退 1**
   （日志逐字 `退出码 1`），包装只是让 `make` 不把整批测试染红 —— 语义成立，
   但**确实不是** `make verify-fault` 本身退非 0。留待用户确认是否改包装。
6. **`_wait_lag_zero()` 恢复后最多阻塞 120 s 却只用于打印**：可删，但删了会丢
   诊断信息（实机日志里 lag 263→0 是很好的人工旁证）。留作低成本诊断。
7. **`run_verify_fault` 已约 250 行、11 个编号步骤（Divergent Change）**：
   拆分需要重排流程与所有 `checks.append` 的顺序语义，收益低于风险，
   且本票即将收口。留作后续重构（若 impl-02e 还要动它）。
8. **`_feed_batch()` 在无参时回退 `nonce="adhoc"`**：本轮**没有**任何调用点走这条路
   （`run_verify_fault` 总是显式传 `batch`），但保留了一个会复用 event_id 的入口。
   已在注释中标明；若后续要收紧，把 `records` 改为必填即可。

### 18. 续跑入口（加固版**尚未实机验证**，从这里继续）

```bash
# 0) 环境
MSYS_NO_PATHCONV=1 docker ps -a --format "{{.Names}} {{.Status}}" | grep eco-
curl -sf http://127.0.0.1:8081/jobs/overview

# 缺作业 ⇒ 重部署
bash '/mnt/f/Anaconda practice project/eco-a-d-platform/.uv-cache/deploy-job.sh'

# 1) 正例（期望 PASS=12 FAIL=0、退出码 0；重点看新增的三行）
cd '/mnt/f/Anaconda practice project/eco-a-d-platform'
make verify-fault > /tmp/g1.log 2>&1; echo "EXIT=$?"
grep -nE 'nonce|分区数|tick\]|基线：|计数器已重置|重放证据|DLQ 增量|全等|结论|完成：' /tmp/g1.log

# 关注点（加固版新增的机读凭证）：
#   - topic … 分区数=N            ← 从 Kafka 元数据读，不再是硬编码 6
#   - [tick] 水印推进 tick 成功 N/N 条  ← 投递回调统计的成功条数
#   - 计数器已重置=True            ← failover 后 min_seen < kill 前

# 2) 紧接着跑第二轮（跨轮残留是历史假 FAIL 的源头）
make verify-fault > /tmp/g2.log 2>&1; echo "EXIT=$?"

# 3) 反例（期望内层退 1，包装转身为 0）
make verify-fault-corrupt > /tmp/g3.log 2>&1; echo "EXIT=$?"
grep -nE 'DLQ 增量|结论|完成：|按预期失败' /tmp/g3.log

# 4) 离线（已全绿，复测确认）
.venv/bin/python -m pytest -q
~/.local/bin/ruff check src/ tests/ && ~/.local/bin/ruff format --check src/ tests/
~/.local/bin/basedpyright --outputjson > /tmp/bp.json; \
  .venv/bin/python -c "import json;print(json.load(open('/tmp/bp.json'))['summary'])"
```

**已确认的离线状态（加固版）**：`pytest` 118 passed、`ruff check` All checks passed、
`ruff format --check` 20 files already formatted、`basedpyright` errors=0 warnings=6。

**未完成**：加固版的实机正例/反例（上一次运行在 `make verify-fault` 启动阶段被中断，
日志只有 `make: *** [Makefile:78: verify-fault] Error 129`，无任何步骤输出）；
代码**尚未提交**（HEAD 仍 `1912ec4`）。



---

## 19. 加固版实机验证（2026-10-22 最终轮，**已结案**）

§17 的六处加固此前只有离线证据。本轮在实机上补齐，三条运行全部完成：

| 报告 | 目标 | 结果 | 关键读数 |
|---|---|---|---|
| `20261022T054228Z` | `make verify-fault` | `PASS=12 FAIL=0`，退出码 **0** | 重读 45 条 / 计数器已重置=True / DLQ 增量 0 / 全等 |
| `20261022T054701Z` | `make verify-fault`（**紧接跨轮**） | `PASS=12 FAIL=0`，退出码 **0** | 重读 45 条 / 重置=True / 2 个窗口仍全等 |
| `20261022T055228Z` | `make verify-fault-corrupt` | 内层 `PASS=11 FAIL=2`，退出码 **1**，包装转 0 | 重读 50 条 / DLQ 增量!=0 / 结论不通过 |

### 19.1 加固项逐条拿到实机凭证

| §17 加固 | 机读凭证（实机日志/报告逐字） | 判定 |
|---|---|---|
| #1 未达 `at_least` 返回 `None` | `fault-feed-dump` 报 `read-records=90`（= 投递前 45 + 投递 45）、第 5 步 141（= 96+45）；无「超时凑数」 | 成立 |
| #2 `_wait_replay_after_failover` 机读重置 | `replay-observed` 的 `actual` 逐字含 **「计数器已重置=True」**；日志 `read-records=0（kill 前 141）` ⇒ `min_seen=0 < kill_before` | 成立（新断言真的在跑） |
| #3 tick 投递回调 + 分区数从元数据读 | `topic ecom.behavior.event.v1 分区数=6`（读 Kafka 元数据）；`[tick] 水印推进 tick 成功 6/6 条`，恢复后 6/6 且 `read-records=45+6=51` | 成立 |
| #4 `snapshot_baseline(windows=…)` 必填 | 基线 `window_metric_count=27/28`、`funnel_step_count=4`（本批窗口非空拦截生效） | 成立 |
| #5 `DUPLICATE_COUNT_ALIASED_SQL` | 反例轮「两表全等」+「DLQ 单点失败」并存 ⇒ scoped 比对与重复计数旁证各自独立取数 | 成立 |
| #6 Makefile 目标注册 | 两个目标名均可直接 `make` 跑通 | 成立 |

### 19.2 耗时

三轮约 **3m50s / 4m10s / 4m30s**，与 §13 的 3~4 分钟一致；加固未引入明显开销
（`_wait_replay_after_failover` 只多记一个 `min_seen`）。

### 19.3 一个残留观察（不阻断，记录待查）

第 1 步「等外部输入冻结」读到 **45**（上一轮批次在途）而非 0。本轮三轮都通过，
因为判据已按**本批窗口**限定（§12 缺陷 H 的修法），残留落在别的窗口里。
若某轮残留恰好落在**同一分钟窗口**，基线基数会含上一轮的行。归 impl-02e 收口时决定是否显式 drain。

### 19.4 离线复测（本轮重跑，全绿）

```
pytest          118 passed in 68.25s
ruff check      All checks passed!
ruff format     20 files already formatted
basedpyright    errorCount=0 warningCount=6
```

### 19.5 提交

本票与 `src/ecomgen/fault.py`、`tests/test_fault.py`、`tests/test_verify_gaps.py`、
`Makefile`、三处 Flink Java 一并提交。未跑 `down -v`，卷态未动。
