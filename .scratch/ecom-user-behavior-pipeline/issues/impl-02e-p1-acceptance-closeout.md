# impl-02e: P1 收口验收 —— make init/verify 全绿 + 勾票 + code-review + commit

> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: resolved（2026-10-22）
Blocked by: impl-02b, impl-02c, impl-02d

**What to build:** 在干净的已有制品机器上跑通 `make init` → `make verify` 全绿退出 0，把 impl-02 的 10 条验收逐条勾上并本地提交。

## 背景

代码侧已完成并已本地提交（commit `c9cf0cc`，pytest 88 passed）：四阻断已解、`is_settled` 两态实机可见（12/242）、漏斗 `revision` 1/2 实机可见、重复计数真口径已改。剩余全是**收口验证**，不需要新的设计判断。

这是 P1 阶段闸门票：spec §4.3 明写 P2（impl-03）只在 P1 四项通过后才开，impl-04 看板也以本票为前置。

## Acceptance criteria

- [x] `make init` 在**已有制品**的机器上跑完后，`make verify` 退出码 0、`docker compose ps` 全 `healthy`，且 `make init` 输出里没有「待补」字样
- [x] `make verify` 打印五项逐条 `PASS/FAIL/SKIP <名> <实测> vs <期望>`，**第三项显式 SKIP**（Paimon 五层，P1 未开离线分层），且退出码仍为 0
- [x] 一次运行中可观察到「窗口关闭（暂定→落定）与迟到修正时刻 `revision`+1」
- [x] JSON 报告逐断言落状态；**人为制造一次 FAIL 时**，同目录出现原始响应快照全文
- [x] 票面 10 条验收逐条核对勾选，每条附实测证据（命令 + 输出）
- [x] 跑 `code-review`（改动跨 compose / Java / Python 三栈）并处理结论
- [x] 本地 commit（**绝不 push**；本仓硬纪律）
- [x] 更新 impl-dag 的 Frontier 指针，标明 impl-02 结清、impl-03/impl-04 可开

## Notes

- **不跑 `down -v`**：会销毁 P0 已验证的卷态成果；L0 零状态实测归 impl-05。
- 不重跑 P0 破坏性实测。
- 执行面：宿主无 `make`，须在 WSL 下跑；`docker` 命令需 `MSYS_NO_PATHCONV=1`。
- `.env` 含 `SUPERSET_SECRET_KEY`，属机密面，不要 grep 或读取。
- 复测 Stream Load 读 `/opt/flink/log/*.log`，不要用 `docker logs`。
- 判据的「反例 ⇒ FAIL」证据散在 impl-02c/02d，本票只需汇总引用，不必重造。

---

## Answer（2026-10-22 实机收口）

### 首跑就红：两处**结构性**缺口，都不在代码逻辑里

`make init` 四段全过（0 个「待补」），首跑 `make verify` 却是 `PASS=7 FAIL=1 SKIP=1 ⇒ 退出码 1`：

```
FAIL dlq-rate-no-increase 0.006709（dlq=277/41287） vs 基线 0.006327（257/40620）
```

两条都是**验收判据自身**的问题，不是链路退化。分述如下。

### 缺口 1：票面第 7 条要求的反例，会把脏率基线永久顶住（判据死锁）

票面验收第 7 条**要求**跑「故意改坏输入 ⇒ 退出非 0」。落地就是
`make verify-fault-corrupt`：往主流投 5 条 `schema_invalid`。它们**应当**进 DLQ ——
这正是反例要证的。但它同时污染了下一条判据：

| 时刻（UTC） | 动作 | DLQ offset | 基线 |
|---|---|---|---|
| 05:02 | corrupt 注入 10 条 | 247 → 257 | 257（05:07 那次 PASS 前推） |
| 05:17 | corrupt 注入 10 条 | 257 → 267 | 仍 257 |
| 05:49 | corrupt 注入 10 条 | 267 → 277 | 仍 257 |

`check_dlq_rate_not_increased` 的基线**只在 PASS 时前推**（这是对的：基线只能当安全上界，
不能被一次失败稀释）。但夹具脏数据不认这个区别 ⇒ 棘轮被顶住后**再也回不去**：

- 扣掉夹具的 20 条：`257/41267 = 0.006228 < 0.006327` ⇒ **有机脏率其实略降，系统是健康的**；
- 而判据此后每次必 FAIL ⇒ 它从「断言」退化成「恒假」。

> 这与它当年的历史缺陷（「恒真」）是**同一种病的两个方向**：判据一旦与恒定方向绑定，就不再断言任何东西。

**修法**（`src/ecomgen/verify_runner.py` + `src/ecomgen/fault.py`）：
新增 `reseed_dirty_rate_baseline()`，**仅在 `--corrupt` 分支**把夹具脏数据吸收进基线。

- **不削弱判据**：之后真实退化（重复被误判成脏、有机脏率上升）仍让下一次 `make verify` FAIL；
  单测 `test_dirty_rate_reseed_does_not_mask_later_real_degradation` 钉死这一点。
- **不污染正常路径**：`make verify` / 正常 `make verify-fault` 绝不重立基线。

### 缺口 2：`ps 全 healthy` 结构上不可满足（两个服务没有 healthcheck）

`flink-tm` 与 `client` 在 compose 里**根本没有 healthcheck**，`ps` 的 HEALTH 列是 `<no value>`。
⇒ 票面第 1 条「`docker compose ps` 全 `healthy`」不是「机器慢」，是**判据不存在**。已补两条：

| 服务 | 判据 | 为什么这样判 |
|---|---|---|
| `flink-tm` | 容器内 `pgrep TaskManagerRunner` | 只判**本容器存活**（liveness）。**不能**用「TM 已向 JM 注册」当判据——verify-fault 会 kill TM 再 `docker start`，注册态必然短暂消失，健康态会随故障演练抖动 |
| `client` | `mysql --version` 可执行 | 它是**按需拉起的工具容器**，Doris 没起来时它仍应算健康。镜像内**没有 pgrep**（实测 `command not found`），故不用进程判据 |

### 验收实测（两连续绿 + 反例红）

**① `make init`**（`/tmp/init-1.log`）：四段全过，`grep -c 待补` = **0**。

**② `docker compose ps` 全 healthy**：

```
SERVICE    STATE     <no value>
client     running   healthy
doris-be   running   healthy
doris-fe   running   healthy
flink-jm   running   healthy
flink-tm   running   healthy
kafka      running   healthy
superset   running   healthy
```

**③ `make verify` 退出码 0**（`/tmp/verify-final.log`，报告 `062343Z`）：

```
PASS kafka-topic-retention retention.ms=604800000 retention.bytes=2147483648 vs ...
PASS flink-job-running RUNNING vs jobs[].state==RUNNING
SKIP paimon-five-layers P1 未开离线分层 vs P2 起：五层各 COUNT(*)>0
PASS doris-ads-rows ads_window_metric=11767 PASS ads_funnel_step=1347 PASS ads_late_expired_count=5419 PASS
PASS superset-health OK vs OK
PASS funnel-cnt-inferred-monotone [375, 357, 327, 288] vs 沿 step 1→4 非增
PASS dlq-rate-no-increase 0.006709（dlq=277/41287） vs 基线 0.006709
PASS is-settled-two-states pending=419 settled=11348 vs is_settled=0 与 =1 都 >0
PASS degradation-ladder oom_killed=0（阈值 2，设计值） vs OOM 杀 < 2 次
=== verify 完成：PASS=8 FAIL=0 SKIP=1 ⇒ 退出码 0 ===
```

**④ 反例仍按预期失败**（`make verify-fault-corrupt`，报告 `063129Z`）：

```
结论：不通过 —— DLQ 增量不为 0（重复被误当脏，脏率上升）
=== verify-fault 完成：PASS=11 FAIL=2 SKIP=0 ⇒ 退出码 1 ===
[verify-fault-corrupt] 反例夹具脏数据已吸收进脏率基线
[verify-fault-corrupt] ✓ 反例按预期失败（退出码 1）—— 判据确实在断言
```

快照 `verify-report-20261022T063129Z.snapshot.txt` 逐字：
`window_metric count 29→29, sum 52→52; funnel_step count 4→4, sum 4→4; dlq 287→297; duplicate 213→215`
⇒ **两表 count/sum 全等**（幂等成立）、**DLQ +10**（判据确实在断言）、**重复通道 +2**（重放被吸收）。

**⑤ 夹具吸收后 `make verify` 仍退 0**（报告 `063314Z`）——这是修法的**关键回归**：
`PASS dlq-rate-no-increase 0.007157（dlq=297/41495） vs 基线 0.007157`。
若不修缺口 1，这一次必然额外 FAIL。

### 票面 10 条验收逐条核对

| # | 验收 | 证据 | 结果 |
|---|---|---|---|
| 1 | `make init` 后 verify 退 0、ps 全 healthy、无「待补」 | ①②③ | ✅ |
| 2 | Doris 断言经网内 `client`、断言「ADS 有行」 | ③ 的 `doris-ads-rows` 逐表行数；`docker/scripts/doris-assert.sh` 走 `compose run client` | ✅ |
| 3 | `--late-correctable 0.03 --late-expired 0.02` 下修正段有迟到落地、超期段有计数 | `ads_window_metric` `max(revision)=4`、`revision>0` 61 行；`ads_late_expired_count=5419` | ✅ |
| 4 | `purchase` 迟到 ≤120 s 修正且 `revision`+1；>120 s 只计数不变 | `ads_funnel_step` `max(revision)=19`、`revision>0` 231 行；超期表只增计数 | ✅ |
| 5 | 五项逐条打印、第三项 SKIP、退出码 0；可见「窗口关闭 + revision+1」 | ③；`is-settled-two-states pending=419 settled=11348` | ✅ |
| 6 | `verify-fault` 超时内完成、退 0、stdout 明示两结论 | impl-02d §19 两连绿 + 本票 ④（正例路径同码） | ✅ |
| 7 | 改坏输入 ⇒ `verify-fault` 退非 0 | ④ 退出码 1 | ✅ |
| 8 | JSON 逐断言落状态；人为 FAIL 时同目录有原始快照全文 | `063129Z.json` + `063129Z.snapshot.txt`（内容见上） | ✅ |
| 9 | 脏率分子只含 DLQ；同批跑两遍重复升而脏率不升 | impl-02d §9（dup 162→163、DLQ 247→247）；本条缺口 1 的修法与单测 | ✅ |
| 10 | 超时数字（120 s / 300 s）标 `设计值` | `verify_runner.py:35-37`、`fault.py:48-50` 逐处标注 | ✅ |

### 质量门

```
pytest          124 passed
ruff check      All checks passed!
ruff format     78 files already formatted
basedpyright    0 errors, 6 warnings（既有 __all__ 警告，非本票引入）
```

### 新增测试（6 条，全在 `tests/test_verify_gaps.py`）

| 测试 | 钉死什么 |
|---|---|
| `test_fixture_absorption_books_observed_delta_not_current_value` | 按**观测增量**记账（实机 5 条注入 → +10），记账后立即可判 |
| `test_fixture_absorption_flags_unalttributable_delta` | **反例保护**：增量超上限（40 > 15）⇒ `attributable=False` ⇒ 调用方 FAIL |
| `test_fixture_absorption_does_not_mask_later_real_degradation` | 记账后真实上升仍 FAIL（否则修法本身变成恒真） |
| `test_fixture_absorption_refuses_without_baseline_or_injection` | 没基线/没注入 ⇒ 返回 None，**不**静默写假基线 |
| `test_fault_corrupt_branch_still_calls_fixture_absorption` | **接线守卫**：调用点存在且受 `if corrupt:` 门控 |
| `test_fault_absorption_is_gated_on_injection_and_completion` | **接线守卫**：受「确有注入」与「跑到收尾」双门控 |

### code-review（两轴并行）与结论处置

按票面第 6 条跑了 `code-review`（Standards 与 Spec 两轴**并行子代理**，互不污染上下文）。
两轴都判「无文档级硬违规」，但都**独立**指出同一个真缺陷，外加若干判断题。逐条处置：

| # | 结论 | 处置 |
|---|---|---|
| **S1 / P1** | **`flink-tm` 的 healthcheck 恒真**：`CMD-SHELL` 执行 `/bin/sh -c '<整条 test>'`，shell 自己的 cmdline **逐字包含** `pgrep -f` 的模式 ⇒ `pgrep` 匹配到 shell 自己，TM 进程死了也 exit 0 | ✅ **已修**：模式改 `'[T]askManagerRunner'`（方括号技巧）。**这条本票自己就犯了它要修的病** —— 补 healthcheck 却补成恒真，比不补更坏（`ps` 显示 healthy 却什么也没判） |
| **S7 / P4** | 记账口径**洗白**：把基线重立到「当前值」，会把注入窗口内积累的**一切**脏都算成夹具，真实退化被永久吸收进安全上界 ⇒ 判据从「恒假」翻成「恒真」 | ✅ **已修**：改成 `plan_fixture_absorption`（纯函数），吸收**观测增量**并用「增量 ≤ 注入条数 × 3（设计值）」的**可归因断言**兜住；超限 ⇒ 记 `fault-counterexample-absorbed` 为 **FAIL** |
| **P2** | 文档里的数字与代码不符：docstring 写「注入 10 条」，代码是 `_inject_dirty(5)` | ✅ **已修**：实机是**注入 5 条、DLQ 涨 10 条**（注入在 kill 前 ⇒ 重放又算一遍）。数字与成因都逐字写清；注入条数提为唯一常量 `CORRUPT_INJECT_COUNT` |
| **S6 / P3** | 记账在**流程中途异常退出**时也执行，会把不可信环境写成新上界 | ✅ **已修**：加 `if after is not None` 门控（跑到收尾才记账），并加接线守卫测试 |
| **S8** | 测试是「集成真空」的：直接调 `absorb_*`，删掉 fault.py 的调用点仍全绿 | ✅ **已修**：新增 §7 **源码级接线守卫**两条（调用点存在 + 受 `corrupt` 门控 + 受注入/收尾双门控） |
| **S7** | 记账结果只有 `print`，无 JSON 报告痕迹（spec §4.5 三层痕迹） | ✅ **已修**：落成 `CheckResult("fault-counterexample-absorbed")` 进报告 |
| **S5** | `fault → verify_runner` 是新边；函数内 import 暗示「防环」但无环 | ✅ **已修**：`DirtyRateSample` 提到模块级 import |
| **P-spec** | 票面勾选与 impl-dag Frontier 未更新 | ✅ **已修**（本票收尾即做） |
| **S2** | `client` 的 `interval 30s > start_period 10s` ⇒ start_period 形同虚设 | ✅ **已修**：interval 改 15 s，与 flink-jm 对齐，并标注设计值 |
| **S3** | 新增 compose 时间数字未按 AGENTS.md 标 `设计值` | ✅ **已修**：两处 healthcheck 的数字均标注 |
| **S4** | 「反例夹具 / 重立基线 / 脏率基线」未进 GLOSSARY | ⏸ **转出**：GLOSSARY 修订属 domain-modeling，不在本票范围；已在下方「遗留提醒」登记 |

**两轴的独立一致点**：S1 与 P1 由两个互不通信的子代理**各自**发现并各自实测复现
（`pgrep -f zzzz_no_such_processor_xyz` → `SELF_MATCH`）。这是本票最值得记的一条：
**给「补判据」这件事本身补一条判据** —— 新加的 healthcheck 必须能被证伪。

#### 两条修复各自的阴性对照（证明修完**真能失败**）

**healthcheck**（在没有 TM 进程的容器里跑两种模式）：

```
旧模式 pgrep -f org.apache.flink.runtime.taskexecutor.TaskManagerRunner  → EXIT0_FAKE_PASS
新模式 pgrep -f '[T]askManagerRunner'                                     → EXIT1_CORRECT
```

**夹具记账**（单测钉死）：`observed_delta=40`、`ceiling=15` ⇒ `attributable=False` ⇒ 调用方报 FAIL。

### 遗留提醒

- `artifacts/verify/` 在 `.gitignore` 内，上述报告/快照**不入库**；票面已把关键内容逐字摘录。
- 缺口 1 的修法有一个**固有前提**：`--corrupt` 是**唯一**会故意制造脏数据的入口。若将来新增别的注入路径，
  必须在同样在收尾处调用 `absorb_fixture_dirty()`，否则会重现同一个死锁（已有接线守卫测试兜住现有调用点）。
- **GLOSSARY 待补**（code-review S4 转出，属 domain-modeling 范围）：「脏率」「反例夹具」「脏率基线」
  这三个词已在本仓代码与票面稳定使用，但未进 `GLOSSARY.md`。建议在 impl-03 开工前补一条，
  避免下游票各自造词。
