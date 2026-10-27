# impl-DAG — 施工票依赖图（2026-10-21 派生）

> 本图由 `.scratch/ecom-user-behavior-pipeline/spec.md` 派生，是**施工票的依赖视图**。
> 上游决策图见 [map-dag.md](map-dag.md)（D1–D12，已全部落子，前沿空）。
> 两张图互不覆盖：map-dag 记「为什么这么定」，本图记「先做哪张票」。

## 票面

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-01** | [impl-01-upstream-revisions.md](impl-01-upstream-revisions.md) | — | spec §⑦ 逐字落笔（SRS 11 + gdd 16 + HLD 9 + FC-1/FC-2） |
| **impl-02** | [impl-02-p1-realtime-closure.md](impl-02-p1-realtime-closure.md) | impl-01 | P1 实时闭环：生成器 + Flink→Doris + `make init` 四段 + `verify`/`verify-fault` |
| **impl-03** | [impl-03-p2-offline-layering.md](impl-03-p2-offline-layering.md) | impl-02 | P2 离线分层：七表 + `replay` 全 SQL + A1/A2/A3 + 幂等 + 快照 + 对账（**已拆六张副票**，见下） |
| **impl-04** | [impl-04-dashboard-delivery.md](impl-04-dashboard-delivery.md) | impl-02 | 看板：零账号 + 五面板 + 三态 + `database_uuid` |
| **impl-05** | [impl-05-final-acceptance.md](impl-05-final-acceptance.md) | impl-03, impl-04 | 终验：容量双口径 + 不删卷重启 + L0/L3 冷启 + 附录 A 结清 |

### impl-03 的六张副票（2026-10-22 拆）

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-03a** | [impl-03a-p2-deployment-surface.md](impl-03a-p2-deployment-surface.md) | impl-02 | 部署面：warehouse 具名卷同路径挂 JM/TM ＋ jar 进每个 Flink 进程 |
| **impl-03b** | [impl-03b-lake-ddl-and-ods-dwd-replay.md](impl-03b-lake-ddl-and-ods-dwd-replay.md) | impl-03a | 门槛批 ＋ 七表 DDL 形状 ＋ `replay` 跑通 ODS→DWD |
| **impl-03c** | [impl-03c-dim-dws-ads-five-layer-replay.md](impl-03c-dim-dws-ads-five-layer-replay.md) | impl-03b | DIM 拉链 ＋ DWS 区间 join ＋ ADS 五层贯通 |
| **impl-03d** | [impl-03d-lake-assert-sentinels-and-idempotency.md](impl-03d-lake-assert-sentinels-and-idempotency.md) | impl-03c | 哨兵断言面 ＋ A1/A2/A3 ＋ 幂等 ＋ 快照区间 ＋ 维护段 |
| **impl-03e** | [impl-03e-lake-assert-counterexample-falsification.md](impl-03e-lake-assert-counterexample-falsification.md) | impl-03d | 反例证伪 F1–F6（含哨兵面自身的 F5/F6） |
| **impl-03f** | [impl-03f-paimon-doris-reconciliation-and-verify.md](impl-03f-paimon-doris-reconciliation-and-verify.md) | impl-03e | 对账 ＋ `verify` 第三项转真断言 ＋ 母票收口 |
| **impl-03g** | [impl-03g-falsification-signoff.md](impl-03g-falsification-signoff.md) | — | 03e 人工复核签字（`ready-for-human` → `resolved`；零代码，只签总账 12 行）｜**✅ 2026-10-27 已结案（resolved）**：总账实 13 行逐行四段齐全、两特例（A2 RC=0 / F4 RC=3）判读成立、抽查 4 组证据文件零偏差；「12 行」口径差已在 03h 归正（总账实 13 行，以 03e Answer 核对表为准） |
| **impl-03h** | [impl-03h-mother-ticket-final-reconcile.md](impl-03h-mother-ticket-final-reconcile.md) | impl-03g | 母票 10 条验收终核 + 签字链接入 + DAG 措辞归正｜**✅ 2026-10-27 已结案（resolved）**：10 条逐条对账今日仍成立（`make verify` 10P0S0F 退 0、`lake_assert.sh --compare` 指纹逐字回基线 `…001`、`git check-ignore` 复跑命中）；母票 Answer 落终核节，「全结」措辞与 03e 状态归一 |

**拆分的两处刻意取舍**：

1. **03d / 03e 分开**（写断言 / 证断言）。impl-02e 的 Answer 记过判据自身恒真（healthcheck 因 `pgrep -f` 匹配到 shell 自己而恒真）与恒假（夹具棘轮）两个方向。同一张票里容易被「顺手一起做绿」，故 03d 的断言**未经 03e 证伪前不算数**。
2. **03f 独占 `verify` 改动**。前五张副票一律不改 `verify_runner.py` 第三项，避免 P2 途中把 impl-02e 已验收的 P1 判据改坏。

### impl-02 的五张副票（2026-10-21 拆，票面 10 条验收仍 0 勾）

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-02a** | [impl-02a-late-breakpoint-triage.md](impl-02a-late-breakpoint-triage.md) | — | 超期链断点定位（print 探针 + 可信指标口径） |
| **impl-02b** | [impl-02b-late-sink-fix.md](impl-02b-late-sink-fix.md) | impl-02a | 解超期 sink 阻断，超期表落行 |
| **impl-02c** | [impl-02c-verify-checks-fixes.md](impl-02c-verify-checks-fixes.md) | —（并行） | 修 kafka-retention / doris-ads-rows 两条判据自身缺陷 |
| **impl-02d** | [impl-02d-verify-fault-live.md](impl-02d-verify-fault-live.md) | impl-02b, impl-02c | verify-fault 实机**两连绿 + 反例证伪**（验票 6/7/9）；定位第 7/8 个判据缺陷（水印 tick、scoped 比对）；code-review 加固的六处假通过路径**已取得实机凭证**（§19） |
| **impl-02e** | [impl-02e-p1-acceptance-closeout.md](impl-02e-p1-acceptance-closeout.md) | impl-02b, impl-02c, impl-02d | P1 收口验收 + 勾票 + code-review + commit |

### impl-06 系列（2026-10-25 晚开票「开票不实施」；2026-10-26 增 06e/06f/06g/06h）

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-06a** | [impl-06a-hour-retention-drill.md](impl-06a-hour-retention-drill.md) | — | HOUR 分级保留演练实机（`REQ-F-OLAP-03` 行为层证据；独立表，破坏面最小） |
| **impl-06b** | [impl-06b-drop-reclaim-measurement.md](impl-06b-drop-reclaim-measurement.md) | — | DROP 后回盘时间线实测（recycle bin + BE GC；官方未给出 ⇒ 实机取证） |
| **impl-06c** | [impl-06c-compaction-collision.md](impl-06c-compaction-collision.md) | — | compaction ×20 GB 对撞（耗时/放大/内存形态三数；与 06b 可联动回盘） |
| **impl-06d** | [impl-06d-r7i-replay-watermark-fixes.md](impl-06d-r7i-replay-watermark-fixes.md) | — | R7-i 三案一张票收口（live 跨度 / committedOffsets / 时间戳-质量门次序）｜**✅ 2026-10-26 已结案（resolved）**：三案全部落地 + 反例证伪先行；`make r7i-falsify` 0、`_before_r7i.sh` 必红（4 条）、`make verify` 两连绿 10/10、`verify-fault` 12/12、`verify-fault-corrupt` 按预期失败、pytest 314 passed；HLD §8-R7-i 已回填（adjudication + 两条新环境事实），`reproducibility-boundary.md` §11.4 运维口径已划除；本地提交未推送 |

| **impl-06e** | [impl-06e-deploy-check-usrlib.md](impl-06e-deploy-check-usrlib.md) | impl-03a, impl-06d | 部署面判据 ④ 改口径：作业 jar 验在 `/opt/flink/usrlib/`（两端同一字节）+ `lib/` 不得有副本；原 Paimon connector 断言下移 4d｜**✅ 2026-10-26 已结案（反例 A/B/C 全被抓、还原后 PASS）** |

| **impl-06f** | [impl-06f-deploy-doc-closeout.md](impl-06f-deploy-doc-closeout.md) | — | **母票（索引）**：交付面收尾四切片——README 对齐 / 票面状态归正 / GLOSSARY 三词 / 收尾门｜**✅ 2026-10-26 已结案（resolved）**：四切片 1/2/3/4 全部收口，验收 5 条逐条有凭证；收尾门带出新票 impl-06i |
| **impl-06f-1** | [impl-06f-1-readme-reality-alignment.md](impl-06f-1-readme-reality-alignment.md) | — | README 六处现状断言与代码/脚本对齐（`make init` 四段已接线、`verify` 五真、11 条判据、usrlib 约束、spike dry-run、端口限制）｜**✅ 2026-10-26 已结案**：六处全落地＋总判据 grep 干净；漂移 6 落成机制性描述（WinNAT 排除段动态漂移＋复核命令），当晚复核八端口已全恢复 TCP 可达 |
| **impl-06f-2** | [impl-06f-2-ticket-status-normalization.md](impl-06f-2-ticket-status-normalization.md) | — | 票面状态归正：impl-02/03 中段残留 `ready-for-agent`、impl-03b 的 7 个过期复选框｜**✅ 2026-10-26 已结案（resolved）**：另发现并归正第 4 处镜像矛盾（impl-04 顶行过期 `in-progress`）；impl-03b 实为两段 11 框全部回勾；全仓 `## Comments` 段零存在（判据空转成立）；剩余 39 个 `- [ ]` 全为真未做、保留理由清单见票面 Answer |
| **impl-06f-3** | [impl-06f-3-glossary-three-terms.md](impl-06f-3-glossary-three-terms.md) | — | GLOSSARY 补「脏率 / 反例夹具 / 脏率基线」三词（impl-02e code-review S4 欠账）｜**✅ 2026-10-26 已结案（resolved）**：三词条按代码口径落地（脏率→§事件处置、反例夹具+脏率基线→§读者与验证；diff 23+/1−，唯一删除行是 EOF 换行伪影）；守卫测试 43 passed、lint/typecheck PASS；施工中发现面板侧基线「来源/写者」文档-实现偏差（普通 verify-fault 从不写基线）→ 登记新票 [impl-06h](impl-06h-dirty-rate-baseline-doc-divergence.md) |
| **impl-06f-4** | [impl-06f-4-closeout-gate-deploy-proof.md](impl-06f-4-closeout-gate-deploy-proof.md) | impl-06f-1, impl-06f-2 | 收尾门：不设 override 跑 `deploy_surface_check.sh` 全 11 条 + `make init`→`verify` 一绿（端口不通则如实登记）｜**✅ 2026-10-26 已结案（resolved）**：判据 10 PASS / 1 FAIL（唯一 FAIL＝判据 ⑤ 排除集不完备，归因为判据自身缺陷，立 [impl-06i](impl-06i-deploy-check-5-exclusion-defect.md)）；`make init` 127 s 退 0 + `make verify` 404 s **10P0S0F 退 0**；八冻结端口全可达（HTTP 4×200 + TCP 4×OPEN），**impl-03a §C 冻结端口遗留项销项**；作业恢复 RUNNING（lag=0）；lint/typecheck/pytest 314 全过 |
| **impl-06g** | [impl-06g-spike-live-rate-proof.md](impl-06g-spike-live-rate-proof.md) | — | `make spike` **真发** + `REQ-F-GEN-04`/`REQ-NF-PERF-03` ≥1000 events/s 一手速率证据（现只 dry-run，是唯一未验的功能条目）｜**✅ 2026-10-26 已结案（resolved）**：实测 **1011.8 events/s**（11134 条 / 11004 ms 含 flush，一手口径＝实际 produce 条数/发送墙钟）；四项机读判据全 PASS（速率/期间存活 8 采样全 RUNNING 零异常/lag 44 s 回落/DLQ 增量 0——窗尾对齐的实机验证）；尖峰后 `make verify` 10P0S0F 退 0、基线未动正常前推。判据进码：`src/ecomgen/spike.py`（编排+四判据+报告 `artifacts/spike/`）＋`cli.SendMeasurement`（measured_send 机读块）＋`test_spike.py` 26 条单测（全套 364 passed）；`make spike` 默认真发、`DRY_RUN=1` 预演、bootstrap 修正 127.0.0.1:29092（HOST listener，旧 9092 注释是 P0 已证伪的宿主不可达路）。过程实锤：逐条 pacing 结构性欠冲（首轮 969.2 FAIL）⇒ 按票面 Notes「生成器节奏」分支改分窗信贷节流（F3 裁决安全：内容与 seed 绑定不变），失败证据保留 `spike-report-20261026T145449Z.json` |
| **impl-06h** | [impl-06h-dirty-rate-baseline-doc-divergence.md](impl-06h-dirty-rate-baseline-doc-divergence.md) | — | 脏率基线「来源/写者」文档-实现偏差收口（06f-3 施工发现；核心是 A/B 裁决）｜**✅ 2026-10-27 已结案（resolved）**：用户裁决 **A 实现为准、改文档**；10 处落笔（面板侧脚本 6 + map D11 ④ + spec §6.4〔新发现〕+ sync :58 连带 + impl-04 与 06 两张已结案票指顶备注，后者为嫁接源头）；零代码改动，lint/typecheck PASS + superset 交付测试 6 passed；GLOSSARY 词条按预设不动 |
| **impl-06i** | [impl-06i-deploy-check-5-exclusion-defect.md](impl-06i-deploy-check-5-exclusion-defect.md) | — | 部署面判据 ⑤ 排除集不完备：`.mimosa/`（harness 状态目录，判据写就时不存在）+ 测试 docstring + README 判据名列举共 32 行非 SQL 命中把「零 ADD JAR」打成恒假（06f-4 收尾门实测发现）｜**✅ 2026-10-27 已结案（resolved）**：用户裁决 **C 白名单单用**（模拟实测推翻票面隐含假设——A/B 单独都到不了 PASS，README/tests 5 行是被跟踪叙述行）＋ **`.mimosa/` 入 `.gitignore`**；新发现 seg2（宿主路径 jar 引用段）同样被 `.mimosa` 18 行基线打成恒假（被 seg1 先 FAIL 挡住，票面未记），两段同改白名单（`*.sql/*.sh/*.java/Makefile/Dockerfile/*.yml/*.yaml`，注释过滤扩到 `#`/`--`/`//`）；反例两腿必红（sql/ 真 ADD JAR、.sh 宿主路径 jar 引用各 10P/1F 仅 ⑤ FAIL）、还原后不设 override 全门 **11/11 PASS 退 0**；pytest 340 + lint + typecheck 全过；过程撞 Docker Desktop zombie 容器事故（宿主环境，恢复后重跑，非判据回归）。凭证 `evidence/impl-06i/` |

> 06a/06b/06c 出处＝spec §8.2（**2026-10-25 用户批准**，实施前须复述风险再触发）；06d 出处＝HLD §8-R7-i（用户裁决三案同票）；06e/06f 出处＝2026-10-26 收尾盘点；06g 出处＝SRS `REQ-F-GEN-04`（尖峰 ≥1000/s 从未实测）；06h 出处＝impl-06f-3 施工中按「以代码为准」复核面板口径时发现（该票 Notes 预设登记情形）；06i 出处＝impl-06f-4 收尾门实跑（判据 ⑤ 恒假，32 行命中全为非 SQL 噪音）。除 06f-4 外均互不阻塞。

### impl-07 系列（2026-10-27 开票；impl-05 §5 登记却从未开票的三项 code-review 残留）

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-07** | [impl-07-panel-baseline-parse-and-interpreter.md](impl-07-panel-baseline-parse-and-interpreter.md) | impl-07a, impl-07b, impl-07c | **母票（索引 + 收口门）**：脏率基线链失败时报**真因**（解释器缺席/空壳/JSON 损坏/字段缺失）而非一律「基线文件不可解析」｜四类故障注入收口门 + 正常路径逐字零变化 + ③ 的不施工留档 |
| **impl-07a** | [impl-07a-dirty-rate-baseline-parse-true-cause.md](impl-07a-dirty-rate-baseline-parse-true-cause.md) | — | 脏率基线同步脚本：解释器**可执行性**自检（⚠️ 非存在性探测）＋ 四类真因分流 ＋ 基线只解析一次 ＋ 多余的「只打印不参与判断」读取处置｜**✅ 2026-10-27 已结案（resolved）**：七类真因八条实机凭证（四类必答＋类型/符号/日志键/噪音解释器）；`input_events` 选「仅日志参考」不再致命；正常路径 stdout/stderr/退出码三面 diff 与 HEAD **逐字一致**；13 条守卫测试（含「不得有裸 `2>/dev/null`」「自检不得把 stderr 折进哨兵比对」）；全量 pytest 353 + lint/format/typecheck 全 PASS；code-review 两轴抓到两个真缺陷（自检误报可用解释器、`dlq_offset` 类型裸炸）已修并加钉子｜`evidence/impl-07a/` |
| **impl-07b** | [impl-07b-provision-dataset-id-true-cause.md](impl-07b-provision-dataset-id-true-cause.md) | — | 看板置备脚本：「没解析出来」与「确实没有」分两类报，不再一律「找不到 dataset id」；与 07a **刻意不合流**（输入源不同：stdin vs 文件） |
| **impl-07c** | [impl-07c-dirty-rate-baseline-atomic-write.md](impl-07c-dirty-rate-baseline-atomic-write.md) | — | 脏率基线落盘改**原子替换**（先写临时文件再替换）⇒ 读者永不看到半截 JSON；只改落盘方式，不改基线格式与棘轮语义 |

> 07 系列出处＝impl-05 §5「code-review 登记的残留项（本票不改，留独立票）」——**三项从未开票**，
> 2026-10-27 复核后开票。★ **与 impl-05 原文的三处出入**（照原文做会做错，详见母票）：
> ①原文建议「先 `command -v` 探测」**已被实测证伪**——空壳解释器存在性探测会放行
> （本机实测：`python3` 指向应用商店安装器跳转程序，存在性通过但执行静默退 49 且无 stderr），
> 修法必须是**执行一次最小语句**做自检；②原文把两处内联 `python -c` 当作同一件事可收敛，
> 实则**输入源与语义都不同**（文件 vs stdin），故拆 07a/07b；③原文未记「第二次解析取的那个
> 输入事件数键**只被打印、从不参与判断或写入**」——多一个失败点零收益，07a 一并处置。
> ★ 残留③（取证辅助脚本 `JID` 硬编码）**判定不施工**：全仓零引用（取证存档件），
> 且 Flink 每次提交生成新 JobID ⇒ 硬编码本是一次性快照的固有属性，改为「运行时查最新作业」
> 等于给没人调用的取证件加功能——留档在母票，不开票。
> 07a/07b/07c 三票**互不阻塞**（各改各的面），母票 07 是它们的收口门。

## 依赖图

```
        impl-01  上游收口
            │
            ▼
        impl-02  P1 实时闭环  ←── spec §4.3 阶段闸门（P1 四项通过才开 P2）✅ 已结清
         │       │
         │       └──► impl-04  看板交付  ✅ 已结清（2026-10-25）
         ▼
        impl-03  P2 离线分层（拆六张副票）✅ 已结清（03a–03f 全勾）
         │
         ├──► impl-03a 部署面 ──► impl-03b 七表DDL+ODS/DWD ──► impl-03c 拉链+DWS+ADS
         │                              │
         │                              ▼
         │                         impl-03d 哨兵断言+幂等+快照
         │                              │
         │                              ▼
         │                         impl-03e 反例证伪 F1–F6
         │                              │
         ▼                              ▼
        impl-05  终验  ◄── impl-04 ✅  impl-03f 对账 + verify 转真 + 收口 ✅   （✅ 2026-10-25 结清——前沿空）
```

## Frontier

- **当前**（2026-10-25 晚）：**impl-05 已结清、spec 九节全部有施工落点**；前沿转入 **impl-06 系列四张**（开票未排期，实施须用户触发——06a/06b/06c 为获批实机 task，06d 为 R7-i 修复票）。`reproducibility-boundary.md` §11.1 判据①逐字新句已随同日裁决**追认签字**（spec §7.1 #41，SRS 两行落笔）。
- ✅ **impl-06a 已结清**（2026-10-25）：HOUR 保留演练实机全 5 项验收达成——505 s（≤1200 s）观测到 `DynamicPartitionScheduler` 真实删除 4 个过期 HOUR 分区（FE 日志逐字），三张 ADS 表行数对账逐表不变；两处判据面修正（动态分区表禁手工 ADD/DROP PARTITION 的官方绕法、`LastDropPartitionMsg` 4.1.5 成功删除后仍 NULL ⇒ 判据改走 SHOW PARTITIONS + FE 日志）随 HLD §8-R7-j 登记。凭证 `evidence/impl-06a/`。
- ✅ **impl-06b 已结清**（2026-10-25）：DROP 后回盘实测全 5 项验收达成——非 force DROP 1 GB 级表后**691 s 实测磁盘真实释放**（`catalog_trash_expire_second=300` 实验条件；FE eraseTable @DropTime+617 s → BE `delete tablet dir ... directly due to trash_file_expire_time_sec is 0` 官方日志逐字 → du 回落基线，trash 全程 0）；新发现**表条目 bin 滞留 ≥600 s 保底、不受 expire<600 加速**（分区条目即时清）随 HLD §8-R7-k 登记；RECOVER 库级/表级路径实机验证通过；三张 ADS 表零接触对账逐表相等。凭证 `evidence/impl-06b/`。剩余前沿：06c / 06d。
- ✅ **impl-05 已结清**（2026-10-25，8/8 验收勾选）。收口实录（票面 Progress）：①容量双口径——口径①声明 11.5 GiB（10.5 过期订正，R7-g）＋判据①「≤8 GB」结构性不可达待签；口径②带载五采样 5.08–6.04 GiB 全 ≤8 GB。②L2 三轮 `--profile doris down`→`make init` 周期全过（裸 down 不停 profile 服务＝R7-h，判据动作改定）。③L0 实测 2m37.7s（预拉镜像前提）/ L3 全链 save 3m12s + load 20m12s + init 7m14.9s ≤30 min。④附录 A M/N/O 结清核对全过。⑤#40 minio/spark 撤需求落笔。⑥重提作业水位归位（−1000 s 墙钟后侧）＋ `make verify` 两连绿（`T104723Z` 9P1S / `T105307Z` 10P0S0F）。
- ✅ **impl-04 已结清**（2026-10-25，8/9 验收勾选；GIF 留评审者，数据层对偶证据已取得）。
  收口实录（票面 Progress 下午段）：零账号链机读断言全绿（匿名页面/dashboard API/
  chart_data/`?standalone=1`）；三态全谱实机取证（暂定→落定→修订 rev=1/2）；
  重复写入反例（DLQ +0）；zip 清实例重导入（0 broken charts）；code-review 两轴
  13 项发现已处置（最重要：WindowSumProcess 修订路径死代码 → 两阶段发射重写）。
- ✅ **impl-03 已结清**（六张副票 03a–03f 全部完成，母票 10 条验收全勾；2026-10-27 签字链 03g→03h 走完：03e 人工签字收 resolved、03h 终核 10 条今日仍成立，「全结」名实相符）。
  03f 收口实录（票面 §3）：探针对账经三轮水位归因后实机首绿（24 事件
  `rows=3 逐值全等`，关键修法＝**逐分区踢水位**——operator watermark 取
  各 subtask MIN，单分区 tick 推不动）；篡改反例（UPDATE+1000 ⇒ FAIL
  差 1 行）与 is_settled=0 负例闭环；`make verify` 两连续绿
  （`T135604Z` / `T140141Z`，10 PASS / 0 SKIP / 退 0，第三项以真断言
  形态 PASS）；dlq-rate 增量归因为 fixture 段重放（环境冲击）后基线重立。
- ✅ **impl-06e 已结清**（2026-10-26）：部署面判据 ④ 从「Paimon jar 在两容器 lib + sha256」改为「作业 jar 在 `/opt/flink/usrlib/`（两端同一字节）+ `lib/` 零副本」，原断言语义下移为 4d。反例 A（lib 混入副本）/ B（半部署）/ C（usrlib 多版本）**各自 FAIL、还原后 PASS**，每轮各 1 条 FAIL（无重复计数）。⚠️ ③⑤⑥⑧ 本轮未复跑。
- **impl-02 已结清**（P1 实时闭环，含五张副票全部完成）；spec §4.3 的 P1 闸门**已开**
- **当前前沿（2026-10-27 impl-06i 收口后）**：判据 ⑤ 排除集裁决 [impl-06i](impl-06i-deploy-check-5-exclusion-defect.md) **已结案**（用户裁决 C 白名单 + `.mimosa/` 入 gitignore；反例两腿必红、不设 override 全门 **11/11 PASS 退 0**）——部署面 11 条判据首次全绿。同日早前 [impl-06g](impl-06g-spike-live-rate-proof.md) 已结案（实测 1011.8 events/s，尖峰判据首次有一手证据，**功能面条目全部结清**）、[impl-06h](impl-06h-dirty-rate-baseline-doc-divergence.md) 已结案（用户裁决 A 实现为准改文档）。impl-03 签字链 [impl-03g](impl-03g-falsification-signoff.md) → [impl-03h](impl-03h-mother-ticket-final-reconcile.md) **全部结案**（2026-10-27：03e 经人工复核签字收 `resolved`；03h 终核 10 条验收逐条对账今日仍成立——`make verify` 10P0S0F 退 0、`lake_assert.sh --compare` 指纹逐字回基线、`git check-ignore` 复跑命中——DAG 措辞归正含「总账 12 行 → 实 13 行」口径差）；**impl-03 系列无剩余待办**。**收尾门结论（2026-10-26）**：不设 override 全判据 10/11 PASS（唯一 FAIL 归因为判据 ⑤ 自身缺陷并立 06i，已修）；`make init`→`make verify` 一绿（127 s + 404 s，10P0S0F 退 0）；**impl-03a §C 冻结端口遗留项已销项**（八端口 HTTP 4×200 + TCP 4×OPEN，排除段已漂走）——原「本机 WinNAT 下冻结端口仅 8081 通」的外部卡点随排除段漂移解除，README「已知环境限制」已补当日实证记录。
- ✅ **impl-06f-4 已结清**（2026-10-26）：收尾门实跑全记录见[票面 Answer](impl-06f-4-closeout-gate-deploy-proof.md)；附带收获——③ down/up 后 usrlib 目录整体消失（作业 jar 走 docker cp 路线的固有运维事实）、作业重提后消费组 5 s 追平 lag=0（06d 案 2 续读位点生效，DLQ 不翻倍）、脏率 0.271960 ≤ 基线 0.272138。静态面 lint/typecheck/pytest 314 全过。
- **当前前沿（2026-10-27 开 impl-07 系列；07a 已结案）**：[impl-07](impl-07-panel-baseline-parse-and-interpreter.md) 母票 + 三张副票
  ——**07a 已收口**（见下），前沿剩 **07b / 07c**（两票互不阻塞）：
  [impl-07a](impl-07a-dirty-rate-baseline-parse-true-cause.md)（脏率基线同步脚本解析失败报真因 ＋
  解释器**可执行性**自检 ＋ 只解析一次 ＋ 多余读取处置）/
  [impl-07b](impl-07b-provision-dataset-id-true-cause.md)（置备脚本「没解析出来」与「确实没有」分两类报）/
  [impl-07c](impl-07c-dirty-rate-baseline-atomic-write.md)（基线落盘改原子替换 ⇒ 读者永不看到半截 JSON）。
  三票**互不阻塞**，可并行开工；母票 07 为收口门（四类故障注入 ＋ 正常路径逐字零变化）。
  ✅ **impl-07a 已结案**（2026-10-27）：只碰报错分支与解析次数——解释器判据改成
  「真的执行一条最小语句 + 核对 **stdout**」（把 stderr 折进比对会让可用但吐警告的解释器
  被误报，code-review 抓到后已改并加钉子）；七类真因分流各带下一步；基线由解析 2 次收到
  1 次；`input_events` 定为仅日志参考、缺失不再致命。四类必答故障 + 四类附带全有实机输出
  （`evidence/impl-07a/`），正常路径 stdout/stderr/退出码与 HEAD 逐字一致。
  出处＝impl-05 §5 登记却**从未开票**的三项 code-review 残留；★ 复核时发现 impl-05 原文的
  「先 `command -v` 探测」建议**已被实测证伪**（空壳解释器存在性探测会放行），修法必须是
  **执行一次最小语句**做自检——详见母票「三处出入」。残留③（取证辅助脚本 `JID` 硬编码）
  **判定不施工**（全仓零引用，存档件固有属性），留档在母票。

### impl-03 内部的 Frontier（2026-10-24 22:00 收口）

```
   impl-03a ✅ ──► impl-03b ✅ ──► impl-03c ✅ ──► impl-03d ✅ ──► impl-03e ✅ ──► impl-03f ✅
   部署面          七表DDL        拉链+五层        哨兵断言      反例证伪     对账+收口
                                                  （F1–F6 已证）            （P2 全结清）
```

- **六张副票全部完成**；impl-03 母票 10 条验收 2026-10-24 22:00 全勾收口；2026-10-27 经 impl-03h 终核：10 条逐条对账今日仍成立（逐项证据见母票 Answer 终核表），签字链 03g（03e 人工签字）→ 03h（终核）走完。
- ✅ **impl-03e 已结清**（2026-10-24）：F1–F6 六类反例全部证伪（判据面无一
  恒真；含哨兵面自身的 F5/F6——垃圾行假绿与层缺席假绿）。2026-10-27 经
  impl-03g 人工复核签字（`ready-for-human` → `resolved`，复核结论见 03e 票
  面 Answer 节），母票与副票状态口径归一。
- ✅ **impl-03d 已结清**（2026-10-24）：`make replay` exit 0（含维护段）、
  `make assert` 9/9 哨兵 ok、`make replay-idempotent` 两遍 DWD 指纹逐字相同
  （`1048576 / 1126399221523001 / 1048576 / 131267`）、人为破坏退 1。
- ✅ **impl-03c 已结清**（2026-10-23）：`make replay` 缺省跑完五层 exit 0
  （ods 1048576 / dwd 1048576 / dim 105430 / dws 1550285 / ads 90348）。
  两轮 code-review 各挑出真缺陷且都已处置：① ADS 两切面原本用两条 `INSERT OVERWRITE`
  写同一张主键表 ⇒ 后一条把前一条整表抹掉（**五层仍 exit 0、marker 齐全，只有数字错**），
  已并成单条语句并加静态守卫；② `flink-jm` 的 `mem_limit: 1g` 装不下「Flink 进程(1024m)
  ＋ 同 cgroup 的 sql-client」⇒ cgroup OOM-kill、`docker exec` 返 137、
  表面像 TaskManager 掉线，已上调 2g 并加 `tests/test_deployment_memory.py` 守卫；
  ③ 取证集原本落在被 gitignore 的 `artifacts/`（等于不可复现），已移入
  `.scratch/ecom-user-behavior-pipeline/evidence/`。
- **强序链**已全程走完：03a → 03b → 03c → 03d → 03e → 03f。
- **强序的理由不是「依赖顺序」而是「失败隔离」**：每张票都能**单独证伪**，不必等后面写完才发现前面的部署面错了。
- **03e 是唯一的证伪关**：03d 的断言在 03e 跑完 F1–F6 前**不算数**（已跑，全证）。

### impl-02 内部的 Frontier（2026-10-22 更新）

```
   impl-02a ──► impl-02b ──┬──► impl-02d ✅ ──► impl-02e  ◄── 当前 frontier
   （已完成）               │      ▲
                           └──────┘
   impl-02c（已完成）──────────────┘
```

- **impl-02a / 02b / 02c / 02d / 02e 全部完成**。impl-02d 的加固版（六处假通过路径）已于
  2026-10-22 最终轮取得实机证据：正例两连绿（`054228Z` / `054701Z`）、反例按预期失败
  （`055228Z`），详见票面 §19。
- **impl-02e 已结清**（2026-10-22）：`make init` 四段全过（0 个「待补」）、`make verify`
  两连续绿（`062343Z` / `063314Z`，均 `PASS=8 FAIL=0 SKIP=1 ⇒ 退出码 0`）、
  `docker compose ps` 七服务全 `healthy`、反例 `verify-fault-corrupt` 仍按预期退非 0
  （`063129Z`）。首跑红了两次，都是**判据自身**的缺口（反例夹具顶死脏率基线、
  `flink-tm`/`client` 无 healthcheck），已修并各有反例单测，详见票面 Answer。
- ✅ **spec §4.3 的 P1 闸门已开**：impl-03 / impl-04 可开工

## 与 spec 的对应

| spec 节 | 施工落点 |
| --- | --- |
| ① 头部定位与阅读顺序 | —（元信息，不需施工） |
| ② 全局铁律（三组规则） | 逐字落点进 **impl-01** |
| ③ P0 环境收尾（薄写） | 指针，不重做；冷启契约归 **impl-05** |
| ④ P1 实时闭环 | **impl-02** |
| ⑤ P2 离线分层 | **impl-03** |
| ⑥ 看板交付 | **impl-04** |
| ⑦ 逐字修订清单 | **impl-01** |
| ⑧ 不做清单 | 分散在 impl-02/03/04/05 的 Notes「不实现清单」 |
| ⑨ 追溯表 | 闸门已过（D1–D12 全命中）；施工落点由本图承接 |

## 编号约定

- 决策票 `01`–`09`：**已 resolved，保留原文作决策档案**（[09] Q4 裁决，不加新状态、不折叠）
- 施工票 `impl-01`–`impl-05`：**另起前缀**，避免与决策票号混淆
- impl-02 副票 `impl-02a`–`impl-02e`：**同前缀第三段**，与 impl-02 母票并存
- impl-03 副票 `impl-03a`–`impl-03f`：**同前缀第三段**（2026-10-22 拆），与 impl-03 母票并存；母票正文保留为决策档案，验收勾选收口在 03f
- impl-06 副票 `impl-06a`–`impl-06i` 与切片 `impl-06f-1`–`impl-06f-4`：**同前缀第三段**（2026-10-25/26 开），母票 `impl-06f` 为索引票
- impl-07 母票 + 副票 `impl-07a`–`impl-07c`：**同前缀第三段**（2026-10-27 开）；母票 07 为**索引 + 收口门**（副票全结才收）
- `Status:` 取值遵循 `docs/agents/triage-labels.md`（`ready-for-agent` 等五角色）