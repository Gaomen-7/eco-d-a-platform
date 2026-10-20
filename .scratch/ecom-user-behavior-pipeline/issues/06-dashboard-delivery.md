# 06 · 看板交付：五个面板怎么证（含暂定/落定/已修订）、脏率面板、刷新 ≤10s、零账号

Type: prototype
Status: open
Blocked by: 03
Blocked by (note): `03`＝[语义与判据](03-semantics-and-assertions.md)——「暂定/落定/已修订」能不能画，取决于窗口与 `revision` 的语义先定。
Created: 2026-10-20（原 11；并入原 09 中「脏率面板怎么判」与「交付形态是 zip 还是别的」两节）
Ticket: 06
Source: map.md · gdd §1.6 / §1.7 / GLOSSARY「落定值 / 暂定值」· SRS `REQ-F-BI-01..04`

## Question

`gdd §1.6` 要求看板必须**视觉区分暂定值与落定值**（`【已确认 Q13】`：「否则是假实时」），还要求 as-of / 已修订指示、脏率面板不飙升、漏斗面板同见观测值与推断值。`prototypes/dashboard.prototype.html` 已给出三种范式并跑过 16 条断言——**但那是纯 HTML，没有 Superset 的真实约束**：图表类型、自定义 SQL 数据集、CSS 定制能力，以及 P0 实测出的「镜像不含任何 DB 驱动、元数据库改 SQLite、`ecom_ads` 靠 PyMySQL」这一组限制（HLD §8-R7-c）。

这张票要一张**粗糙但具体**的第四份原型 + 一张面板判据表：

1. **三态载体**：在 Superset 6.1.0 上，「暂定 / 落定 / 已修订」用什么表达——颜色、图例、行徽标、还是两张并排图？原型里画清楚。
2. **刷新 ≤10s 的自相矛盾**：面板 10s 刷新 vs 数据只有 60s 粒度（1 分钟窗口）——「观察到数值变化」（SRS §4.3 的 GIF 证据）到底要证明什么？是「刷新到了」还是「值变了」？这个矛盾要在原型里展示出来。
3. **五个面板**：PV/UV、4 步漏斗、GMV、品类分布、地域分布（省份排行横条 + 自绘方格地图，不依赖 GIS/底图）。Superset 里靠哪张图表做得出（`deck.gl`？插件？纯 table+bar）？哪些在 P0 实测的镜像里根本装不上？
4. **脏率面板**（`REQ-F-BI-04`，SHOULD）：分子只含**真脏**、重复不进分子（gdd D2）；「故障演练期间不得飙升」怎么在面板层证明（基线从哪来）。
5. **零账号**（`REQ-F-BI-03`）：`AUTH_TYPE` / `PUBLIC_ROLE_LIKE` / 数据库免登的官方口径；原型上标出「哪一行配置对应哪一条判据」。
6. **交付形态**：`gdd §1.7` 写 `superset import-dashboards -p <.zip>`，但 `REQ-F-BI-01..04` 从未说交付物是 zip。若交付物是「打开浏览器就能看」，zip 是手段不是判据——两者要不要都保留。

## 约束

- HITL。产物是**一次性原型**（throwaway，单文件 HTML，双击可开），照 `gdd §E` 的原型纪律：验证过的结论上提到实现，外壳作为一手证据留在本分支。
- 不新增数据源（Superset 只连 Doris 的 MySQL 协议）；不改 `ecom_ads` 库名。
- 原型只回答「长什么样 / 怎么证」，**不写实现代码**；不重跑旧的三个原型。

## Answer

<!-- 待解。解析步骤：Skill "prototype" → 与用户对拍 → Status: resolved → 往 map.md 追加，并把原型路径挂在 Answer 下 -->
