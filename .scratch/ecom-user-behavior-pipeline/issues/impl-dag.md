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
- `Status:` 取值遵循 `docs/agents/triage-labels.md`（`ready-for-agent` 等五角色）