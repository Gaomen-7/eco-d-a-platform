# impl-06f-1: README 现状对齐 — 让「写下来的现状」可被一条命令复核

Type: task
Status: resolved
Blocked by: None（可立即开工）

**What to build:** 评审者只读 README 就能准确判断「这台机器现在能做什么」。
现状是 README 的「诚实声明」段停留在 **impl-02 开工前**，与代码已经走了 20 多张票
的事实相反——照着它排查会走错方向。本票把每一处「现状」断言改成**可复核的真话**。

**Blocked by:** None（可立即开工）

## 已核实的六处漂移（2026-10-26 逐条查证）

| # | README 现说法 | 代码实际 | 复核方式 |
| --- | --- | --- | --- |
| 1 | `make init` 「只接好了 ①；②③④ **尚未接线**…仅打印『待补』」 | 四段**全部接线**：② `--profile doris up -d --wait`、③ `superset-init.sh`、③增补 `superset-provision.sh`、④ `doris-assert.sh` | 读 `Makefile` 的 `init:` 目标；`make init` 输出无「待补」字样（impl-02e 已实测） |
| 2 | `verify` 「3/5 Paimon、4/5 Doris 现仅打印 `PENDING`…实际只有 3 项 gate」 | P2 起**5 项全真断言**，第三项查不到即 **FAIL**（不是 SKIP） | `src/ecomgen/verify_runner.py` 的第三项 docstring；`make verify` 实测 10 PASS / 退 0 |
| 3 | 未提部署面判据脚本 | `scripts/deploy_surface_check.sh` 已是**11 条**判据（①②③④ 4b 4c 4d ⑤⑥⑦⑧），且 ④ 已按 impl-06d 实测改判 | `grep -n 'check_' scripts/deploy_surface_check.sh` 与 `main()` 调用清单 |
| 4 | 未提作业 jar 的目录约束 | 作业 jar **必须** `/opt/flink/usrlib/`，`/opt/flink/lib` 不得有副本，否则 `LinkageError: loader constraint violation … committedOffsets`（shade jar 含 `org/apache/kafka/**`，`flink-dist-2.2.1.jar` 不含） | impl-06d 票面 §四环境事实 ④；`scripts/deploy_surface_check.sh` 条目 ④ |
| 5 | `spike` 目标列为「尖峰模式：短时 ≥1000 events/s」 | 实际**只跑 `--dry-run`**，从未真发；速率判据无一手实测 | 读 `Makefile` 的 `spike:` 目标 |
| 6 | 端口段未提本机限制 | 本机 WinNAT 下冻结端口**只有 8081 通**（`8030/8040/8088/9030/9060/9092/9010` 均连不上） | 逐个 `curl 127.0.0.1:<port>`；impl-03a §C |

## Acceptance criteria

- [x] 第 1 条改正：README 写清 `make init` 四段**已接线**，删除「尚未接线/待补」字样
- [x] 第 2 条改正：README 写清 `verify` 五项自 P2 起**全真**，第三项查不到即 FAIL，删除 `PENDING` 叙述
- [x] 第 3 条补写：README 指向 `scripts/deploy_surface_check.sh`，说明它是部署面判据的具名载体，条数以 `main()` 为准
- [x] 第 4 条补写：README（或「纪律」段）记下**作业 jar 放 `usrlib`、`lib` 不得有副本**这条环境事实与它的失败形态
- [x] 第 5 条改正：`spike` 如实标为「尚未真发，判据待 [impl-06g](impl-06g-spike-live-rate-proof.md)」，不得再暗示它已满足 ≥1000/s
- [x] 第 6 条补写：新增「已知环境限制」小节，写明本机 WinNAT 与仅 8081 可用的实测事实，并指向 impl-03a §C
- [x] **总判据**：README 里每一句「现状/已实现/未实现」都能在票面表格里找到一条复核命令；文中不再残留 `待补` / `尚未接线` / `PENDING`（除非该处确实仍未做且已如实标注为未做）

## Notes

- **只改 README 的「现状」面**，不改设计链叙述、不改端口清单事实、不改冻结目标名。
- 若某项在改的过程中发现**代码才是错的**（例如 README 描述的行为更符合 SRS），
  **不要顺手改代码**：登记成新票，本票只负责让文档与现状一致。
- `Makefile` 的 `spike:` 目标允许加一行指向 impl-06g 的注释，**不允许**在本票里真发。

## Answer（2026-10-26）

**六处全部落地，README「现状」面逐句可复核。** 改动面：`README.md`（六处＋顺手改准一处过时暗示）、`Makefile`（仅 `spike:` 目标上方两行注释，Notes 授权的唯一 Makefile 改动）、本票与 `impl-dag.md`（结案标注）。未碰设计链叙述、端口清单事实、冻结目标名。

### 逐条对照

| # | 落点 | 复核 |
| --- | --- | --- |
| 1 | init 现状段改「四段全部接线」，补 ③ 增补 `superset-provision.sh`、收尾 `doris-assert.sh` 与 impl-05 L0 实测 2m37.7s | 读 `Makefile` 的 `init:` 目标（② `--profile doris up -d --wait`、③ `doris-init.sh`、④ `superset-init.sh` 均在） |
| 2 | `verify` 表行改「P2 起五项全真断言；第三项查不到即 FAIL」 | `verify_runner.py` 第三项 docstring（L326-333）＋最新报告 `verify-report-20261026T062959Z.json`：`{"PASS": 10, "FAIL": 0, "SKIP": 0}`，`paimon-five-layers` PASS |
| 3 | 新增「部署面判据」小节，指向 `deploy_surface_check.sh`，条数 11、以 `main()` 为准 | `main()` 调用清单恰 11 个 `check_*`（L1082-1092） |
| 4 | 纪律段新增 usrlib 条目：LinkageError 失败形态＋根因＋条目 ④ 双向断言 | impl-06d §四 环境事实 ④；`deploy_surface_check.sh` 条目 ④ (a) 阳性 (b) 阴性 |
| 5 | `spike` 表行＋Makefile 注释均如实标「尚未真发，一手证据归 impl-06g」 | 读 `Makefile` 的 `spike:` 目标（`--dry-run` 实在） |
| 6 | 新增「已知环境限制」小节（见下节：口径有实测更新） | `netsh interface ipv4 show excludedportrange protocol=tcp` ＋ TCP 探测 |

### 漂移 6 的口径修正（本票最有价值的发现）

票面写「只有 8081 通」。**本票复核（2026-10-26 晚）推翻了这个静态断言**：`netsh` 排除段已不含任何冻结端口（当前仅 1029-1428 / 14067-14466 / 50000-50059 / 10722），八个冻结端口在 `netstat` 全部 `LISTENING`（PID = `com.docker.backend.exe`），纯 TCP 探测八个全部 OPEN。票面「仅 8081」来自当日收尾盘点口径（impl-dag 前沿行同口径）——WinNAT 排除段**会漂移**（impl-03a §C 的 8024-8123 → impl-03f 期已漂走一次 → 收尾盘点时再现 → 本票复核又漂走）。

⇒ README 若写死「仅 8081 通」，明天排除段一漂又会变成新的漂移。故第 6 条落成**机制性描述**：排除段动态漂移＋历史三个时点口径＋复核命令＋「协议层失败 ≠ 端口不通」（curl 对 MySQL/brpc/Kafka 端口报连不上但 TCP 层可达——这正是把「仅 8081」误判成 TCP 不通的可能成因之一）。

同时给 `impl-dag.md` 前沿行的「仅 8081 通」补了括号注记（排除段已漂走、八端口恢复），因为收尾门 impl-06f-4 直接读这条决定是否跑全量判据——过时口径会误导它。

### 票面六处之外的顺手改准（1 处）

纪律段「`artifacts/` …P1+ 才真正写入」→「P1 起已实际写入」：`artifacts/verify/` 下已有 **97 份** verify 报告＋脏率基线（`ls` 可复核），原句是过时的前瞻语气，属同一类「现状面漂移」，改它符合总判据。除此之外无越界改动。

### 未做（有意）

- **不真发 spike**（Notes 明令；归 impl-06g）。
- **不改设计链**：`reproducibility-boundary.md` §2 表格 ④ 仍写着「当前完全未实现，make init 只打印『待补』」——这是 04 号决策票的**历史裁决记录**，不是现状声明；改上游设计链不在本票范围。若要归正，应另开票。
- 未改 `Makefile` init 目标的段号 echo 标签（②③ 段号与 README 表格错位是历史 echo 文案，不影响行为；归正属 06f-2 的「票面状态归正」邻域，如需处理另说）。
