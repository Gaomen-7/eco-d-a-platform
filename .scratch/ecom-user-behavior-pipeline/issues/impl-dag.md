# impl-DAG — 施工票依赖图（2026-10-21 派生）

> 本图由 `.scratch/ecom-user-behavior-pipeline/spec.md` 派生，是**施工票的依赖视图**。
> 上游决策图见 [map-dag.md](map-dag.md)（D1–D12，已全部落子，前沿空）。
> 两张图互不覆盖：map-dag 记「为什么这么定」，本图记「先做哪张票」。

## 票面

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-01** | [impl-01-upstream-revisions.md](impl-01-upstream-revisions.md) | — | spec §⑦ 逐字落笔（SRS 11 + gdd 16 + HLD 9 + FC-1/FC-2） |
| **impl-02** | [impl-02-p1-realtime-closure.md](impl-02-p1-realtime-closure.md) | impl-01 | P1 实时闭环：生成器 + Flink→Doris + `make init` 四段 + `verify`/`verify-fault` |
| **impl-03** | [impl-03-p2-offline-layering.md](impl-03-p2-offline-layering.md) | impl-02 | P2 离线分层：七表 + `replay` 全 SQL + A1/A2/A3 + 幂等 + 快照 + 对账 |
| **impl-04** | [impl-04-dashboard-delivery.md](impl-04-dashboard-delivery.md) | impl-02 | 看板：零账号 + 五面板 + 三态 + `database_uuid` |
| **impl-05** | [impl-05-final-acceptance.md](impl-05-final-acceptance.md) | impl-03, impl-04 | 终验：容量双口径 + 不删卷重启 + L0/L3 冷启 + 附录 A 结清 |

### impl-02 的五张副票（2026-10-21 拆，票面 10 条验收仍 0 勾）

| 票 | 文件 | GitHub | Blocked by | 交付 |
| --- | --- | --- | --- | --- |
| **impl-02a** | [impl-02a-late-breakpoint-triage.md](impl-02a-late-breakpoint-triage.md) | [#1](https://github.com/Gaomen-7/eco-d-a-platform/issues/1) | — | 超期链断点定位（print 探针 + 可信指标口径） |
| **impl-02b** | [impl-02b-late-sink-fix.md](impl-02b-late-sink-fix.md) | [#2](https://github.com/Gaomen-7/eco-d-a-platform/issues/2) | impl-02a | 解超期 sink 阻断，超期表落行 |
| **impl-02c** | [impl-02c-verify-checks-fixes.md](impl-02c-verify-checks-fixes.md) | [#3](https://github.com/Gaomen-7/eco-d-a-platform/issues/3) | —（并行） | 修 kafka-retention / doris-ads-rows 两条判据自身缺陷 |
| **impl-02d** | [impl-02d-verify-fault-live.md](impl-02d-verify-fault-live.md) | [#4](https://github.com/Gaomen-7/eco-d-a-platform/issues/4) | impl-02b, impl-02c | verify-fault 实机跑通 + 反例证伪（验票 6/7/9） |
| **impl-02e** | [impl-02e-p1-acceptance-closeout.md](impl-02e-p1-acceptance-closeout.md) | [#5](https://github.com/Gaomen-7/eco-d-a-platform/issues/5) | impl-02b, impl-02c, impl-02d | P1 收口验收 + 勾票 + code-review + commit |

## 依赖图

```
        impl-01  上游收口
            │
            ▼
        impl-02  P1 实时闭环  ←── spec §4.3 阶段闸门（P1 四项通过才开 P2）
         │       │
         │       └──► impl-04  看板交付（可与 P2 并行）
         ▼
        impl-03  P2 离线分层
         │
         ▼
        impl-05  终验  ◄── impl-04
```

## Frontier

- **当前**：**impl-01**（唯一无阻塞票）
- **impl-02 解锁后**同时开两张：**impl-03** 与 **impl-04**（二者互不阻塞）

### impl-02 内部的 Frontier（2026-10-21）

```
   impl-02a ──► impl-02b ──┬──► impl-02d ──► impl-02e
   （无阻塞）               │      ▲
                           └──────┘
   impl-02c（无阻塞，可并行）───────┘
```

- **现在可开**：**impl-02a** 与 **impl-02c**（两张都无阻塞，互不依赖）
- impl-02b 完成后解锁 impl-02d；impl-02d 完成后解锁 impl-02e（收口票）
- ⚠️ impl-02e 完成前 **spec §4.3 的 P1 闸门不开**，impl-03/impl-04 不得开工

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
- `Status:` 取值遵循 `docs/agents/triage-labels.md`（`ready-for-agent` 等五角色）