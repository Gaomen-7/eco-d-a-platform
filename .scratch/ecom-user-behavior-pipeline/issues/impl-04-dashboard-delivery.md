# impl-04: 看板交付 — 零账号四步链 · 五面板 · 三态载体

Type: task
Status: ready-for-agent
Blocked by: impl-02

**What to build:** 评审者用无痕浏览器直接打开 `127.0.0.1:8088`，看到五块面板出数并按 10 s 自动刷新——全程**零账号**，不需要任何外部平台凭据。窗口从暂定跳到落定、迟到修正让 `revision` 递增时，面板上的值确实跟着变。

**① 零账号四步链** — `AUTH_TYPE = AUTH_ANONYMOUS`（前提）· `PUBLIC_ROLE_LIKE = "Gamma"`（匿名可见）· dataset **显式**授给 Public（Public 只有 UI/API 权限，不授具体 dataset 则面板无数据）· 评审 URL 带 `?standalone=1`（演示干净）。

**② 五个面板，全部用内置图表**（取证自 Superset 6.1.0 tag 源码）：
- 实时 PV/UV/GMV — Big Number + ECharts Timeseries Bar
- 4 步转化漏斗 — ECharts Funnel（legacy Funnel 已不在 6.1.0 注册清单）
- 品类分布 — ECharts Treemap（legacy Treemap 已移除，ECharts 版默认内置）
- 地域分布 — ECharts 横向 Bar **排行**
- 漏斗双列（观测/推断同屏）— Table 条件格式，或两张 Funnel 并排（ECharts Funnel 单值图做不到同屏两列）

⚠️ 唯一裁决点：**自绘省份方格地图无内置实现路径** —— `deck_geojson` 需要 GeoJSON 列 + 默认 OSM 底图，违背本项目「不依赖 GIS/底图」的约束 ⇒ 改横条排行（`REQ-F-BI-01` 只要求「地域分布」，不指定形式）。

**③ 三态载体：B 为主，A 式补充** — 主载体是 **B 单表行内状态**：Table + 自定义 SQL 列 `CASE WHEN is_settled=0 THEN '暂定' WHEN revision>0 THEN '已修订' ELSE '落定' END` + 条件格式，这是「同表同一行」的最直译，三态 + revision + as-of 同行可见，零插件。补充 **A 当前窗口双块**：暂定 Big Number 与落定值并排，让「跳动的那个数是暂定值」的直觉成立。**变体 C 单图双色叠加不实现** —— 「修订描边」需要 DOM 挂钩，而 CSP `script-src: 'self' 'strict-dynamic'` 禁自定义 JS（`style-src` 含 `'unsafe-inline'` 所以 CSS 可注入、JS 无路径）；原型证据留在 `prototypes/superset-constraints.prototype.html`。

**④ 刷新判据** — `DASHBOARD_AUTO_REFRESH_INTERVALS` 共 10 档，**最小自动档 = 10 s 恰压上限，无 1 s/5 s**。数据粒度 60 s ⇒ 相邻两次 10 s 刷新之间约 7 次「刷新到了但值没变」。判据（签字版）：「观察到数值变化」= **窗口关闭（暂定→落定）与迟到修正（revision+1）时刻，面板值确实变化**；10 s 刷新只承担「链路活着」。GIF 录窗口关闭瞬间的一次跳变 + 一次修订变色。

**⑤ 脏率面板** — 分子只含真脏（`dlq_count_delta / total_events`，**重复不进分子**）。基线 = `verify-fault` 输入冻结时的 DLQ 快照，面板只显示「当前值 vs 基线 Δ」。⚠️ 「故障演练期间脏率不得飙升」的判据是 **`verify-fault` 的退出码断言（DLQ 增量==0），不是面板目测**。

**⑥ 交付形态：zip 保留为 `make init` 的复现手段**（`import-dashboards -p <zip>`，经 `importexport.py` docstring 源码级确认），但**判据归 SRS**：浏览器能看能刷。⚠️ **新硬约束（源码级）**：dashboard importer 逐字 `config["database_id"] = database_ids[config["database_uuid"]]` ⇒ dataset 经 `database_uuid` 匹配目标实例的 database ⇒ `make init` 建 `ecom_ads` 连接时**必须固定/回填该 UUID**，否则导入后全部图表断链。

**Blocked by:** impl-02

**Status:** ready-for-agent

## Acceptance criteria

- [ ] 无痕浏览器直接开 `127.0.0.1:8088`（无 cookie、无登录）能看到五块面板出数并自动刷新
- [ ] Public 角色只授了 UI/API 权限时面板**无数据**，显式授 dataset 后有数据 —— 证明 dataset 显式授权这步真的必要
- [ ] 五块面板均用内置图表渲染；地域分布是横向条形排行而非方格地图；漏斗面板同屏可见观测列与推断列
- [ ] 三态可见：当前未关闭窗口显示「暂定」，窗口关闭后变「落定」，迟到修正后变「已修订」并伴随值变化
- [ ] 录制 GIF 能看到窗口关闭瞬间的一次跳变 + 一次修订变色（刷新判据的可证形态）
- [ ] 脏率面板的分子不因重复写入而上升；面板显示的是「当前值 vs 基线 Δ」
- [ ] 故意重复写同一批数据时，脏率面板不动但重复通道计数上升（反例保护生效）
- [ ] zip 重新导入到**清空过 dataset 的实例**后五块图表仍连得上（`database_uuid` 固定生效的证明）
- [ ] 交付形态判定走浏览器实测，不以「zip 导入无报错」代替

## Notes

- **并行**：本票不依赖 impl-03，可与离线分层并行；但依赖 impl-02 的 `make init` Superset 引导段与 Doris `ecom_ads` 供数点就绪。
- **不实现清单**（spec §8.2）：方格地图（`deck_geojson`）· 三态变体 C（留原型证据）· Paimon Catalog 直读 zero-ETL（`REQ-F-OLAP-04` 是 COULD 实验，不阻塞验收）。
- **判据纪律**：「UI 里能看到」「配置可见」**不是**判据 —— 面板的判据是「无痕浏览器能看能刷」。