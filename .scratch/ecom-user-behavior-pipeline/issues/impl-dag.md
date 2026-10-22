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

## 依赖图

```
        impl-01  上游收口
            │
            ▼
        impl-02  P1 实时闭环  ←── spec §4.3 阶段闸门（P1 四项通过才开 P2）✅ 已结清
         │       │
         │       └──► impl-04  看板交付（可与 P2 并行）
         ▼
        impl-03  P2 离线分层（拆六张副票）
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
        impl-05  终验  ◄── impl-04   impl-03f 对账 + verify 转真 + 收口
```

## Frontier

- **当前**（2026-10-22 收口 **impl-03a** 后）：**impl-03b** 与 **impl-04** —— 二者互不阻塞，可同时开
- ✅ **impl-03a 已结清**（2026-10-22）：8 条验收全过，`scripts/deploy_surface_check.sh` 跑出 `9 PASS / 0 FAIL`（含新增 ④b 可写性判据），并完成反例证伪（chmod 555 后 4b/② 立刻 FAIL）。实机还发现并修掉一个票面未写的承重缺陷：**具名卷属主 root:root vs 镜像以 uid 9999 运行 ⇒ flink 用户写不进 /warehouse**，已在 Dockerfile 预建并 chown。详见票面 §A–§D。
- **impl-02 已结清**（P1 实时闭环，含五张副票全部完成）；spec §4.3 的 P1 闸门**已开**
- **impl-05 仍在等**：须 impl-03（= 03a–03f 全结清）与 impl-04 双结清

### impl-03 内部的 Frontier（2026-10-22 拆票后）

```
   impl-03a ✅ ──► impl-03b ──► impl-03c ──► impl-03d ──► impl-03e ──► impl-03f
   部署面          七表DDL       拉链+五层     哨兵断言      反例证伪     对账+收口
                  ◄── 当前 frontier
```

- **impl-03a 已完成**；**impl-03b 是当前 P2 入口**（依赖已满足）。
- **强序链**，一次会话一张：03a → 03b → 03c → 03d → 03e → 03f。
- **强序的理由不是「依赖顺序」而是「失败隔离」**：每张票都能**单独证伪**，不必等后面写完才发现前面的部署面错了。
- **03e 是唯一的证伪关**：03d 的断言在 03e 跑完 F1–F6 前**不算数**；03d 自己只要求「失败路径能走通一次」，不要求穷举。

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
- `Status:` 取值遵循 `docs/agents/triage-labels.md`（`ready-for-agent` 等五角色）