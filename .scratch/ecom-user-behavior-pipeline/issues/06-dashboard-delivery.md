# 06 · 看板交付：五个面板怎么证（含暂定/落定/已修订）、脏率面板、刷新 ≤10s、零账号

Type: prototype
Status: resolved
Resolved: 2026-10-20
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

**Resolved 2026-10-20（frontier session）。** 五项裁决 2026-10-20 用户「全按推荐」一次通过（含裁决 #2 的 SRS 签字项）。产物：[`prototypes/superset-constraints.prototype.html`](../prototypes/superset-constraints.prototype.html)（单文件 HTML，双击可开，三个**三态表达范式**变体，`#variant=A/B/C` 或 ←→ 切换；加速时钟 10×，右上角可注入「重复涌灌 / 真脏」演示 Q4）。官方取证全部落在 **6.1.0 tag 源码**（config.py `7477f7f4`、MainPreset.ts `c28dea34`、importexport.py `50c8aa78`、dashboard importer `c3063ed5`、deckgl preset `d6e41c6e`），每条可按 URL+tag 复核；解码比对由两个独立通道完成（subagent 逐字读取 + 主会话 api base64 解码），互证一致。

### Q1 · 三态载体：三个范式，推荐 B 为主载体

| 变体 | 画法 | Superset 落法（全内置，零插件） | 判据映射 | 代价 |
| --- | --- | --- | --- | --- |
| **A 并排双图** | 暂定/落定物理隔离两张图 | 同 dataset 两张图，一张 `WHERE is_settled=0`、一张 `=1` | 最直观的「假实时」揭露；修订徽标 = CSS | 双倍版面；修订瞬间两图短暂不一致 |
| **B 单表行内状态**（**推荐**） | 一张 Table，行级 pill 徽标 + 底色 | 自定义 SQL 列 `CASE WHEN is_settled=0 THEN '暂定' WHEN revision>0 THEN '已修订' ELSE '落定' END` + Table 条件格式 | [03] Q2「同表同一行」的最直译；三态+revision+as-of 同行可见 | 实时感弱（无数值跳动） |
| **C 单图双色叠加** | 同一柱图，暂定半透明/落定实色/修订描边 | ECharts Bar 双系列（暂定/落定各一系列）可实现半透明+实色；**「修订描边」内置图表做不到**——DOM 无挂钩点且 CSP 禁 JS | Q13 的强形态 | 依赖最重的 CSS 补丁，边界最脆 |

推荐：**B 为三态主载体**（语义直译 + 实现最稳），当前窗口的「暂定感」用 A 式单块 Big Number 并排补充（跳动的那个数是暂定值，旁边钉死落定值）。C 保留为原型证据，不推荐实现。

### Q2 · 刷新 ≤10s 的自相矛盾：判据应改写为「窗口关闭/修订时刻值变化」

一手事实（config.py 6.1.0 逐字）：`DASHBOARD_AUTO_REFRESH_INTERVALS` 档位只有 `[0, "Don't refresh"], [10, "10 seconds"], [30, …] … [86400]` 共 10 档——**最小自动档就是 10s，恰好压线 `REQ-F-BI-02` 上限，无 1s/5s 可选**。于是矛盾显性化：数据粒度 60s ⇒ 相邻两次 10s 刷新之间，8 次里约 7 次「刷新到了但值没变」（原型右上刷新记录面板逐条演示这一点）。

**推荐裁定**：`REQ-F-BI-02` 的「观察到数值变化」**不是**「每次刷新值都变」，而是「**窗口关闭（暂定→落定）与迟到修正（revision+1）时刻，面板值确实变化**」。GIF 证据（SRS §4.3）应录**窗口关闭瞬间的一次跳变** + 一次修订变色，10s 刷新只承担「链路活着」。此为 SRS 措辞澄清，属意图类、**需你签字**，落笔 [09]。

### Q3 · 五面板 vs 内置图表：四处全过，一处冲突需裁决

MainPreset.ts 6.1.0 逐字注册（源码级）：

| 面板 | 内置图表 | 证据 |
| --- | --- | --- |
| 实时 PV/UV/GMV | Big Number + ECharts Timeseries Bar | `BigNumberChartPlugin` / `EchartsTimeseriesBarChartPlugin` 注册 ✅ |
| 4 步漏斗 | **ECharts Funnel** | `new EchartsFunnelChartPlugin().configure({ key: VizType.Funnel })`——legacy Funnel 已不在 6.1.0 注册清单 ✅ |
| 品类分布 | **ECharts Treemap** | `new EchartsTreemapChartPlugin()`——legacy Treemap 已移除（UPDATING.md `[23741]`），ECharts 版默认内置 ✅ |
| 地域·排行 | ECharts Bar 横向（orientation） | 无独立 Horizontal Bar 插件，横向靠 Bar 设置 ✅ |
| 地域·**省份方格地图** | **⚠ 无对应内置图表** | deck.gl choropleth＝`deck_geojson`，需 GeoJSON 数据列 + 默认 OSM 底图（`OpenStreetView has been promoted as the new default…`）——**违背 gdd「不依赖 GIS/底图」** |

**冲突（需你裁决）**：gdd §1.6「自绘省份方格地图」在 Superset 6.1.0 内置图表里**没有**实现路径（CSS 只能改样式不能造 DOM；CSP `script-src: 'self' 'strict-dynamic'` 禁自定义 JS）。三选一：
1. **改用横条排行单图**（推荐：零偏离官方能力，SRS `REQ-F-BI-01` 只要求「地域分布」不指定形式）；
2. 方格地图降级为**纯 CSS 近似**（Table 图 + 条件格式按省上色，行列排布靠数据透视——能做但丑，且属 CSS 边界玩法）;
3. 接受 deck_geojson（自带底图依赖，违背 gdd 既定原则，不推荐）。
裁定后 gdd §1.6 修订归 [09]。

漏斗双列（`REQ-F-BI` gdd 新增判据「同屏观测/推断」）：ECharts Funnel 单值图做不到同屏两列 ⇒ 用 Table 条件格式或两张 Funnel 并排（原型 A 变体给了双列表格范式）。

### Q4 · 脏率面板：基线＝kill 前 DLQ 快照，判据在 verify 不在面板

- **分子只含真脏**：SQL 层定义 `dirty_rate = dlq_count_delta / total_events`，重复不进分子（gdd D2 冻结；重复走幂等吸收计数通道，[03] Q6/Q7）。
- **基线从哪来**：`verify-fault` 的输入冻结快照（kill 前 DLQ 计数，[03] Q7 已定）——面板层只显示「当前值 vs 基线 Δ」，**「故障演练期间不得飙升」的判据是 `verify-fault` 的退出码断言（DLQ 增量 == 0），不是面板目测**（HLD §0.4：「UI 里能看到」不是判据）。
- 原型演示：点「故障重放（重复涌灌）」脏率不动；点「注入真脏」才动。
- 基线脏率数值本身是**设计值**（`--chaos-ratio` 决定，官方未给出）。

### Q5 · 零账号：配置链四步，逐行映射判据

config.py 6.1.0 逐字 + 官方 security 文档：

| # | 配置 | 官方逐字 | 对应判据 |
| --- | --- | --- | --- |
| 1 | `AUTH_TYPE = AUTH_ANONYMOUS` | config.py 默认 `AUTH_TYPE = AUTH_DB`；AUTH_ANONYMOUS 未在注释列举（**官方未给出**逐字说明，FAB 常量） | `REQ-F-BI-03` 前提 |
| 2 | `PUBLIC_ROLE_LIKE = "Gamma"` | 「`# This is useful if one wants to enable anonymous users to view dashboards. Explicit grant on specific datasets is still required.`」 | 匿名可见 |
| 3 | 显式 dataset 授权给 Public | 官方 security：「`The Public role only grants UI/API permissions. You must also grant access to specific datasets…`」 | 面板有数据 |
| 4 | 评审 URL `?standalone=1` | 官方 embedding：「`standalone | 0,1,2,3 | 1: hide nav…`」 | 演示干净 |

原型「哪行配置对应哪条判据」即上表；面板层判据＝无痕浏览器直接开 `127.0.0.1:8088` 看到刷新。CSP/Talisman 不挡同源访问（`TALISMAN_ENABLED` 默认 True 但同源无碍）。

### Q6 · 交付形态：zip 是手段、判据是浏览器，两者都留，但有一条新硬约束

- `gdd §1.7` 的 `superset import-dashboards -p <zip>`（源码级证据：`importexport.py` docstring 逐字「Export dashboards to ZIP file」）**保留**为 `make init` 的复现手段；`REQ-F-BI-01..03` 的判据**本来就是**「打开浏览器能看能刷」，不因 zip 存在而改变。推荐：**两者都保留，表述上 zip 归 §1.7（编排），判据归 SRS（看板）**。
- **新硬约束（本票发现，源码级）**：dashboard importer v1 逐字 `config["database_id"] = database_ids[config["database_uuid"]]`——**dataset 经 `database_uuid` 匹配目标实例的 database**。⇒ zip 里的 dataset 引用要接上 `ecom_ads`，`make init` 建 database 连接时必须**固定（或回填）该 database 的 UUID**，否则导入后全部图表断链（官方文档页未给此说明——「官方未给出」，判定为源码事实）。此约束应写进 gdd §1.7，落笔 [09]。

### 官方未给出清单（本票新增 8 条）

| # | 事项 |
| --- | --- |
| 1 | `AUTH_ANONYMOUS` 在 config.py 注释中的逐字说明 |
| 2 | legacy Funnel 的精确移除版本号（6.1.0 源码已无注册，事实可确认） |
| 3 | Horizontal Bar 的官方文档定义（由 ECharts Bar orientation 实现） |
| 4 | 刷新设置随 zip 导入保留的说明 |
| 5 | 匿名模式下自动刷新的已知限制 |
| 6 | dashboards zip 导入的专门官方文档页（CLI 文档只在源码 docstring，与 gdd §1.7 原判断一致） |
| 7 | 看板自定义 CSS 的能力边界清单（Edit CSS 无 apache 官方文档页；CSP 逐字可证 style 可注入/JS 被禁） |
| 8 | 「同表多份 SQL 口径」的官方警告（仅 DATASET_HEALTH_CHECK 单表性能提示） |

### 需你裁决（对拍清单）

| # | 事项 | 推荐 |
| --- | --- | --- |
| 1 | 三态载体选型（A/B/C 或组合） | **B 为主 + A 式当前窗口双块补充**，C 留证据不实现 |
| 2 | `REQ-F-BI-02`「观察到数值变化」改写为窗口关闭/修订时刻值变化（意图类，需签字，落笔 [09]） | 按推荐改 |
| 3 | 方格地图冲突三选一（改横条 / CSS 近似 / deck_geojson） | **改横条排行**，gdd §1.6 修订归 [09] |
| 4 | zip 定位：手段归 §1.7、判据归 SRS，两者都留 | 按推荐 |
| 5 | `make init` 必须固定/回填 Superset database UUID 的新硬约束写入 gdd §1.7 | 按推荐 |

### 落笔分工（[09] 输入，待裁决后生效）

1. SRS `REQ-F-BI-02` 签字句（裁决 #2）。
2. gdd §1.6：三态载体段落（裁决 #1）+ 方格地图条目修订（裁决 #3）。
3. gdd §1.7：zip 定位句（裁决 #4）+ database_uuid 硬约束（裁决 #5）。
4. HLD：无新增（图表注册事实属一手证据，引用处标注 6.1.0 tag 源码 URL 即可）。

> 原型路径：[`prototypes/superset-constraints.prototype.html`](../prototypes/superset-constraints.prototype.html)。验证过的结论已上提至本 Answer；外壳按 gdd §E 纪律作为一手证据留在本分支。
> 签字记录：2026-10-20 用户「全按推荐」覆盖裁决 #1–#5（含 #2 的 SRS `REQ-F-BI-02` 意图类签字）。
> 环境备注：本会话 shell 通道（pwsh/ops_bash）不可用，node 语法校验未能实测，以人工逐行静态审查代替（子 agent 复核同结论：括号/引号/id/CSS 无误）；遗留临时文件 `.scratch/tmp-*.js` 已清空内容，物理删除待有执行通道的会话。
