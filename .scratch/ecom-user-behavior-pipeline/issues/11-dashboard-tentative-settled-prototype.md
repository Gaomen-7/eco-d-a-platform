# 11 · 看板：暂定/落定/已修订在 Superset 上到底怎么呈现，刷新 ≤10s 怎么证

Type: prototype
Status: open
Blocked by: 05, 09
Created: 2026-10-20
Ticket: 11
Source: map.md → gdd §1.6 / GLOSSARY「落定值 / 暂定值」/ SRS `REQ-F-BI-01..04` / 原型 `prototypes/dashboard.prototype.html`

## Question

`gdd §1.6` 要求看板必须**视觉区分暂定值与落定值**（`【已确认 Q13】`：「否则是假实时」），还要求 as-of / 已修订指示、脏率面板不飙升、漏斗面板同时可见观测值与推断值。而 `prototypes/dashboard.prototype.html` 已经给出三种看板范式（大屏镶边 / 左筛选网格 / 旅程时间轴）并跑了 16 条断言——**但那个原型是纯 HTML，没有 Superset 的真实约束**：Superset 的图表类型、自定义 SQL 数据集、CSS 定制能力（`SUPERSET_CUSTOM_*`）才是真边界。

这张票要一张**粗糙但具体**的原型来回答：

1. 在 Superset 6.1.0 上，「暂定 / 落定 / 已修订」三态用什么载体表达——颜色、图例、行徽标、还是两张并排图？（用 HTML 原型画出来，一张图说明清楚。）
2. 刷新 ≤10s（`REQ-F-BI-02`）**怎么证**：面板配置项（`SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT` 是警告阈值、非硬下限）+ 观察数值变化（SRS §4.3 要 GIF）。这两条里，「观察到数值变化」在 1 分钟窗口下意味着一个面板**最多每 60s 变一次**——那 ≤10s 的刷新还有意义吗？这个矛盾要在原型里展示出来（同一个面板 10s 刷新 vs 数据只有 60s 粒度）。
3. 「零账号」（`REQ-F-BI-03`）对 Superset 的配置要求：`AUTH_TYPE`、`PUBLIC_ROLE_LIKE`、数据库免登——官方文档口径是什么，原型上标出哪一行配置对应哪一条判据。
4. 五个面板（PV/UV、4 步漏斗、GMV、品类、地域）在**同一屏**里的信息密度：地域用「省份排行横条 + 自绘方格地图」（不依赖 GIS/底图），这个在 Superset 里靠哪张图表能做出米（`deck.gl`？`echarts` 插件？还是纯 table+bar）。

## 约束

- HITL。产物是**一次性原型**（throwaway，单文件 HTML，双击可开），照 `gdd §E` 的原型纪律：验证过的结论上提到实现，外壳作为一手证据留在本分支。
- 不改 `ecom_ads` 库名；不新增数据源（Superset 只连 Doris 的 MySQL 协议）。
- 原型只回答「长什么样 / 怎么证」，**不写实现代码**。

## Answer

<!-- 待解。解析步骤：Skill "prototype" → 与用户对拍 → Status: resolved → 往 map.md 追加，并把原型路径挂在 Answer 下 -->