> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（R3 降级阶梯的看板替代项）｜ 上游：FSR §3/§8 R3、SRS §3.1.6/§6.1、GDD §1.6 ｜ 状态：一手核查，未实测的已标注

# HLD 前置调研 · R3 降级阶梯的看板替代项

**题目**：R3 降级序列里「Superset → 某替代项」那一级，候选到底有谁、各自的一手事实是什么、哪一条**天然做不到**。**本文只给事实与弃选证据，不改任何锚点**（Superset 6.1.0 仍是选中项）。

## 0. 方法与只读边界

| 项 | 状态 |
| --- | --- |
| 本轮动作 | 只读。未安装、未拉镜像、未启停容器、未改配置、未提交 git；除本文件外未写任何路径 |
| **Docker 守护进程** | **仍未运行**——`docker images` / `docker version` 的 Client 段可读（29.8.0 / API 1.56），守护进程连接失败：`npipe:////./pipe/dockerDesktopLinuxEngine` → `The system cannot find the file specified`。**故镜像体积与容器实测内存本轮无法取得** |
| 磁盘实测（`df -h`，只读） | `F:` 600 G / 已用 523 G / **可用 78 G**（88%）；`C:`（bash 根）可用 **9.6 G**；`D:` 13 G；`E:` 37 G；`G:` 75 G；`H:` 483 M |
| 一手来源 | 官方 `LICENSE` 文件 / 官方仓库源码 / 官方文档页 / 官方博客 / GitHub API 元数据。二手讨论（论坛）**只用于标注「官方未给出」** |
| 未执行 | 「落地动作」一律不由本 agent 执行（本仓规矩：先经用户同意） |

## 1. 本仓特有判据（三轴 + 四条硬门槛）

**裁决用四门槛**（任一条天然做不到 = 弃选的决定性证据）：
① **零账号只读**（GLOSSARY「零账号」：评审者不需任何外部平台账号）；② **刷新 ≤10 s**（`REQ-F-BI-02`）；③ **视觉区分暂定值/落定值/已修订**（GDD §1.6 / GLOSSARY「落定值 / 暂定值」）；④ **`ecom_ads` 直连**（Doris 4.1.5 前端走 MySQL 协议）。

**三轴记法**：A agent 友好度（确切配置键、错误/状态可机读、官方 compose/镜像/导入导出 CLI）｜B 本机资源占用（官方数字优先；官方未给就写「官方未给出」）｜C 与既有接缝的兼容性（`ecom_ads` 直连、冻结端口与 `127.0.0.1`、≤8 GB 预算重算、看板交付形式 `superset import-dashboards -p <.zip>` 换成别家后要重做什么、§6.1 范围、`REQ-F-BI-04` 脏率面板）。

## 2. Grafana OSS —— 「排除还是保留」的结论

| 判据 | 一手事实 | 来源 |
| --- | --- | --- |
| 许可证 | **AGPLv3 全文**（`LICENSE` 34 523 B，含第 13 条 Remote Network Interaction 原文） | [grafana/LICENSE](https://github.com/grafana/grafana/blob/main/LICENSE)（逐字核对） |
| 迁移公告 | 官方博客原文：*「we will be relicensing our core open source projects (Grafana, Grafana Loki, and Grafana Tempo) from the Apache License 2.0 to the Affero General Public License (AGPL) v3」*（2021-04-21，作者 Raj Dutt） | [官方公告](https://grafana.com/blog/grafana-loki-tempo-relicensing-to-agplv3/) |
| 第 13 条边界 | 逐字原文：*「**if you modify the Program**, your modified version must prominently offer all users interacting with it remotely … an opportunity to receive the Corresponding Source」* → 条件从句是「修改」；**不改、本机自用、不对外提供修改版 ⇒ 无源码义务**。**FSR §3 的边界分析逐字成立** | 同 `LICENSE` 第 13 条 |
| **告警引擎位置（决定性）** | 官方原文：*「**In Grafana OSS, the alert engine runs in the same process as the UI and data source proxy**, so alert CPU saturation directly competes with dashboard query performance.」*；同页把 sizing 覆盖范围定义为 *「the UI, data source proxy, **alert engine**, and image renderer」* | [Install Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)（逐字） |
| 形态与内存 | Go 单二进制；官方 **最低 512 MB / 1 核**；官方 Small 档 **2 核 + 2–4 GB**（磁盘 10–20 GB SSD）；Medium **4–8 核 + 8–16 GB**；默认元数据库为**内嵌 SQLite** | 同上 |
| MySQL 数据源 | **内置、开箱即用**。官方原文：*「The data source ships with Grafana out of the box. It's preinstalled in both Grafana OSS and Grafana Enterprise, so there's nothing for you to install」*；支持 *MySQL 5.7 and newer*、MariaDB 10.5+ 等 | [MySQL data source](https://github.com/grafana/grafana/blob/main/docs/sources/datasources/mysql/_index.md) |
| 刷新下限 | 官方 `[dashboards] min_refresh_interval` 默认 **5 秒**原文：*「The default interval value is 5 seconds.」*；`[server] http_port` 默认 **3000** | [_index.md（配置）](https://github.com/grafana/grafana/blob/main/docs/sources/setup-grafana/configure-grafana/_index.md) 逐字（行 914–917 / 行 http_port） |
| 零账号只读 | 支持。官方 `[auth.anonymous]`：`enabled` / `org_name` / `org_role`（**Viewer**）/ `hide_version` / `device_limit`；等价环境变量 `GF_AUTH_ANONYMOUS_ENABLED`（键映射规则 *「GF_<SECTION NAME>_<KEY>」*） | [Anonymous authentication](https://github.com/grafana/grafana/blob/main/docs/sources/setup-grafana/configure-access/configure-authentication/anonymous-auth/index.md) |
| 判据 ③（暂定/落定/已修订） | **官方未给出**任何「暂定值/已修订」语义。Grafana 面板只有「时间范围 + 阈值带 + annotations」，没有窗口 revision 概念 → 需自建（如把 `revision` 做成 annotation 或文本面板） | 官方文档检索无对应能力 |
| A 轴 | 配置键**确切**（`[dashboards] min_refresh_interval`、`[auth.anonymous] enabled/org_role`）；健康端点 `GET /api/health` 返回 **JSON**（`{"database":"ok","version":…}`）；官方 Docker 镜像与 `conf/defaults.ini` 齐备；**无** import/export 看板 CLI（有 provisioning 目录 + Git Sync） | 官方文档 |
| C 轴 / §6.1 | **正面冲突**（两条独立理由，与 `hld-research-observability.md` §2.3 一致）：① 官方自证 alert engine 与 UI/数据源代理**同进程** ⇒ 引入即引入告警引擎；② Doris 官方把 Prometheus 定义为 *monitoring **and alerting** suite*。且 Grafana 是「指标看板」范式，**不做 SQL 驱动的转化漏斗面板** | 同上 + [Doris Monitoring & Alerting](https://doris.apache.org/docs/4.x/admin-manual/maint-monitor/monitor-alert/) |

**结论：排除（比 FSR §3 的理由更强）。**
- 许可证理由**不足以**排除：AGPLv3 第 13 条在本机自用场景**确实无义务**，FSR 的边界分析成立——**这一条要在面试里讲准，别再把「AGPL ⇒ 不能用」当理由**。
- 真正的排除理由是 **§6.1 + 判据 ③**：引入 Grafana 就是引入一个**与 UI 同进程的告警引擎**，撞 SRS §6.1「明确不做监控告警系统」；且它没有「暂定/落定/已修订」的原生表达，5 面板中的**步进转化漏斗**（`user_id` 步进、双列并陈 `observed`/`inferred`）在 Grafana 里只能靠大量 SQL + 文本面板硬拼。
- **保留的残余价值**：若 R3 真的触发且**其他项全部不成立**，Grafana 的 512 MB / 单二进制 / 内置 MySQL 数据源是**资源上最优**的「堪用」选项——但必须同时接受 §6.1 边界被击穿，**这不是本仓能自行决定的事**（需 U 级授权 + 范围边界改写）。

## 3. Metabase

| 判据 | 一手事实 | 来源 |
| --- | --- | --- |
| **许可证（决定性）** | OSS 版 = **AGPL**。仓库 `LICENSE.txt` 逐字：*「Outside of the top-level "enterprise" directory, source code in a given file is licensed under the **AGPL**.」*；*「Binaries located at hub.docker.com/metabase/metabase … are released under the AGPL」*；`LICENSE-MCL.txt`：enterprise 目录与 EE 功能走 **Metabase Commercial License**（需有效 license）。**即：OSS 版是 AGPL，不是「无 copyleft」**；同时存在 `LICENSE-AGPL.txt`（37 KB 全文）与 `LICENSE-EMBEDDING.txt` | [LICENSE.txt](https://github.com/metabase/metabase/blob/master/LICENSE.txt)、[LICENSE-MCL.txt](https://github.com/metabase/metabase/blob/master/LICENSE-MCL.txt) |
| 「宽松许可」对照 | **AGPL 与 Superset 的 Apache-2.0 相比是收紧**（Superset = Apache-2.0，[官方仓库 API](https://github.com/apache/superset) `license.spdx_id=Apache-2.0`）。Metabase 的**豁免不来自 OSS 许可，而来自「不改 + 本机自用 + 不对外」** —— 与 Grafana 同一套边界；但 Grafana 只需这一条，Metabase 还多一条 §6.1 之外的问题（见下） | — |
| 形态与内存 | **JVM/JAR**（Clojure）。官方数字（Metabase 官方 learn 页）：起手 **≥1 核 + 1 GB**，之后**每 20 并发用户 +1 核 +2 GB**（原文并注明 *「before v52 we recommended only 1GB … we raised this requirement in newer versions」*）；同页给出 40 并发 = 3 核 / 5 GB 的算式。另一位官方页（Metabase at scale）另给 *「a single core machine with 4 Gbyte of RAM can scale Metabase to hundreds of users」* → **两页口径不一致，本文两者并列不改写** | [Metabase in production](https://metabase.com/learn/metabase-basics/administration/administration-and-operation/metabase-in-production)、[Metabase at scale](https://metabase.com/learn/metabase-basics/administration/administration-and-operation/metabase-at-scale) |
| 应用数据库 | 官方`Docker` 页提供 **官方 compose**（`metabase` + `postgres:16` 两服务）与 `metabase/metabase` 官方镜像；官方 Docker 文档自带 `healthcheck: curl --fail -I http://localhost:3000/api/health`（**可机读**）。默认端口 3000 | [Running Metabase on Docker](https://github.com/metabase/metabase/blob/master/docs/installation-and-operation/running-metabase-on-docker.md) |
| MySQL 数据源 | **内置**（官方导航列出 MySQL 与 MariaDB 为 supported databases）→ Doris 前端协议层接得上 | 官方 docs 导航 + [Running on Docker] |
| **刷新（决定性）** | **Auto refresh 只有固定档位**：官方原文 *「You can set your dashboard to update in **1, 5, 10, 15, 30, and 60 minute intervals**」*（URL 参数 `#refresh=60`，同样按分钟）。**没有秒级档位 ⇒ 判据②（≤10 s）天然做不到**。官方另有「Caching query results」页，其 Duration/Schedule/Adaptive 策略与 *Refresh cache automatically* 属 **Pro/Enterprise（商业）** 功能 | [Introduction to dashboards](https://github.com/metabase/metabase/blob/master/docs/dashboards/introduction.md)、[Caching query results](https://www.metabase.com/docs/latest/configuring-metabase/caching) |
| 零账号只读 | 支持。**Public links / public embeds**：官方原文 *「Since public embeds don't do any authentication or authorization, anyone with the URL can view the data.」*；`public-links` 页另有 「Allow public access」开关 | [Securing embedded Metabase](https://metabase.com/docs/latest/embedding/securing-embeds)、[Public sharing](https://www.metabase.com/docs/latest/embedding/public-links) |
| 判据 ③ | **官方未给出**「暂定/落定」语义。Metabase 是「问题（question）】+ 卡片」范式，无窗口 revision；能做的是 `revision` 列 + 文本卡片 + 条件格式 → 需自建 | 官方文档 |
| 判据定义交互 | public/guest embed 的 auto-refresh `auto-refresh-interval`（秒）**标注可用范围是 Pro/Enterprise 与 Guest embed** → OSS 自托管不能用它做秒级刷新 | [Dashboard component reference](https://metabase.com/docs/latest/embedding/dashboard-reference) |
| C 轴：交付形式代价 | `superset import-dashboards -p <.zip>` **无对应物**。Metabase 的类比物是 **Serialization**（`serialization.md`，官方文档 33 KB）——但官方明示它不是为「评审者零账号看板」设计；实际替代路径是手工建卡/建问题 + public link。**换成 Metabase ⇒ 看板交付必须重做** | 官方 `serialization.md` / 导航 |

**结论：排除。决定性证据 = 判据② 天然做不到（官方刷新档位最小 1 分钟）**；次要证据 = OSS 版是 **AGPL**，所谓「Metabase 许可更宽松」在本仓的一手文件上**不成立**。

## 4. Redash

| 判据 | 一手事实 | 来源 |
| --- | --- | --- |
| 许可证 | **BSD-2-Clause**（GitHub API `license.spdx_id=bsd-2-clause`；`LICENSE` 文件 1 320 B 为 BSD 两条款全文，版权行 `Copyright (c) 2013-2020, Arik Fraimovich. All rights reserved.`）→ **宽松，且比 AGPL 干净** | [redash/LICENSE](https://github.com/getredash/redash/blob/master/LICENSE)、[GitHub API](https://github.com/getredash/redash) |
| 技术栈 / 容器数 | Python + **Celery** + Redis + Postgres。**官方生产 compose 有 8 个服务**：`server` / `scheduler` / `scheduled_worker` / `adhoc_worker` / `worker` / `redis` / `postgres` / `nginx`（官方 setup 仓 `data/compose.yaml`）；仓库内 `compose.yaml`（开发用）为 `server` / `scheduler` / `worker` / `redis` / `postgres` / `email`(maildev) 共 **6 服务** → **开发 6 / 生产 8** | [getredash/setup data/compose.yaml](https://github.com/getredash/setup/blob/main/data/compose.yaml)、[redash/compose.yaml](https://github.com/getredash/redash/blob/master/compose.yaml) |
| 内存 | 官方 help 页：*「For basic deployments we recommend a minimum of **4GB of RAM** and reasonable amount of CPU allocation」*；官方 mintlify 安装页：CPU 2 核、**RAM 4 GB（最低 2 GB）**、磁盘 10 GB；生产建议 4 核 / 8 GB。**两处口径并列** | [Setting up a Redash Instance](https://redash.io/help/open-source/setup)、[Installation Guide](https://mintlify.com/getredash/redash/installation) |
| MySQL 数据源 | **内置**（`redash/query_runner/mysql.py` 存在，72 个 runner 之一）→ `ecom_ads` 协议层接得上 | [query_runner/](https://github.com/getredash/redash/tree/master/redash/query_runner) |
| **刷新（决定性）** | 官方原文：*「**The allowed refresh intervals are expressed in seconds: 60, 300, 600, 1800, 3600, 43200, and 86400.**」* → **最小 60 s，档位里连 10 s 都没有** ⇒ 判据② 天然做不到。官方并注明自动刷新 *「only in-effect as long as a logged-in user has the dashboard open in their browser」* | [Dashboard editing](https://redash.io/help/user-guide/dashboards/dashboard-editing) |
| 零账号只读 | 支持但**是「秘密链接」而非真匿名**：官方原文 *「generate a secret link to share safely outside your organization. External users can see the dashboard widgets but will not be able to navigate within the Redash application or view the underlying queries」*；可被 `REDASH_DISABLE_PUBLIC_URLS=true` 全局关闭。⚠️ 官方**自曝安全弱点**：*「any user can modify them [parameters], which makes Redash the wrong tool for embedded analytics. Only share dashboards with trusted stakeholders.」* | [Sharing and Embedding Dashboards](https://redash.io/help/user-guide/dashboards/sharing-dashboards)、[Embedding Visualizations](https://mintlify.com/getredash/redash/advanced/embedding) |
| A 轴 | 配置键确切（`REDASH_WEB_WORKERS: 4`、`WORKERS_COUNT`、`QUEUES`）；无正式「暂定/落定」机读语义；官方 setup 脚本 `setup.sh` + 固定 `redash/redash:__TAG__` 镜像 | 同 compose |
| 判据 ③ | **官方未给出**。且其刷新是**前端轮询**，窗口 revision 只能靠 SQL 列 + 表格呈现自建 | 官方文档 |

**结论：排除。决定性证据 = 判据② 天然做不到（最小档 60 s，且官方用得最重的 Docker 栈是 8 容器）**。许可证是本文全部候选里**最宽松的一档（BSD-2-Clause）**，值得在面试里作为「许可对了但形态不对」的反例。

## 5. Evidence.dev

| 判据 | 一手事实 | 来源 |
| --- | --- | --- |
| 许可证 | Evidence **Core = MIT**（仓库 `LICENSE` 逐字：`MIT License / Copyright (c) 2026 Evidence`）；官方文档原话 *「Evidence Core is an **MIT-licensed** framework … No account required.」* | [evidence/LICENSE](https://github.com/evidence-dev/evidence/blob/main/LICENSE)、[What is Evidence?](https://docs.evidence.dev/) |
| 形态（**关键区分**） | **双形态**：官方 Core 是**构建期生成静态站**（*「It builds to a static site you can self-host anywhere — Vercel, Netlify, Docker, or your own infrastructure」*）；但**自托管时跑的是常驻服务**——官方 Docker 方式为 `FROM evidencedev/serve:latest` + `evidence serve`，*「similar to `evidence dev` but hardened as a production server」*（端口默认 3000） | [What is Evidence?](https://docs.evidence.dev/)、[Self Host](https://docs.evidence.dev/deployment/self-host/firebase) |
| 刷新 | **无「自动刷新面板」概念**。查询时机有两种：**Direct connectors**「每次加载报告都向仓库发查询」（*「Always live: viewers see the current state of your warehouse on every load — no sync schedule」*）；**Evidence Warehouse**（Studio 托管）按 *5 mins / 15 mins / 1 hour / 1 day / 1 week* 同步，且 Object storage 仅「行变更即时、**schema 变更需重新 Publish**」。**没有任何 ≤10 s 的官方表述** ⇒ 判据② 做不到 | [Databases/connectors](https://docs.evidence.dev/evidence-warehouse)、[Publishing](https://docs.evidence.dev/publishing) |
| MySQL/Doris 连接（**决定性**） | **Direct connectors 官方只列**：Snowflake / Microsoft Fabric / ClickHouse / BigQuery / Databricks / Postgres / Cube / MotherDuck → **没有 MySQL**。MySQL 只出现在 **Evidence Warehouse（Studio 托管、需账号）**的 Databases 连接器里。而 Doris 前端走 MySQL 协议，**不在任何直连清单内** ⇒ 判据④ 断链 | [Sentinel: direct connector 清单](https://docs.evidence.dev/evidence-warehouse)（官方页逐条） |
| 零账号 | Core 自托管可做，但官方默认**要求 HTTP Basic Auth**（`EVIDENCE_BASIC_USER` / `EVIDENCE_BASIC_PASSWORD`，标注 "Yes*"）；要真匿名需显式 `EVIDENCE_AUTH_DISABLED`（官方措辞是 *"Skip auth entirely on trusted private networks"*） | [Self Host](https://docs.evidence.dev/self-host/) 环境变量表 |
| 判据 ③ | 有原生能力吗？官方**未给出**「暂定/落定」语义。但它是**代码优先**（Markdown + SQL + 组件），可自写组件/条件样式表达 revision —— **表达力不是瓶颈，实时性与连接是瓶颈** | 官方文档 |
| C 轴 | 与 Superset 的 `import-dashboards` **完全不同范式**：Evidence 的交付物是**仓库里的 .md + .sql**，看板=一次构建产物；换成它 ⇒ 面板全部重写为 SQL+Markdown，**且要自己接受「非实时」** | 官方 Publishing 页 |

**结论：排除。两条独立决定性证据：① 官方直连清单**无 MySQL**（`ecom_ads`/Doris 接不上，除非上 Studio 托管并引入账号，撞零账号）；② **无 ≤10 s 刷新**（连「自动刷新」概念都没有）。它的价值定位是**「报表/数据产品发布」，不是「实时看板」**——这一点必须在面试里讲清，别把 Evidence 当成「更轻的 Superset」。

## 6. Superset 自身降配（**这一级才是 R3 的实际可行解**）

> 这条不改锚点（Superset 6.1.0 仍是选中项），只是把「R3 降级」从「换工具」改写成「减服务」。

| 项 | 一手事实（官方 `docker/` 骨架逐文件核对） | 来源 |
| --- | --- | --- |
| **生产型 compose 的进程清单** | `docker-compose-non-dev.yml` 共 **6 个常驻/一次性服务**：`redis`（`redis:7`）、`db`（`postgres:17`）、`superset`（`docker-bootstrap.sh app-gunicorn`）、`superset-init`（一次性，`docker-init.sh`）、`superset-worker`（celery worker）、`superset-worker-beat`（celery beat）；另有 **`superset-websocket` 走 `profiles: [websocket]`（可选）** | [docker-compose-non-dev.yml](https://github.com/apache/superset/blob/master/docker-compose-non-dev.yml) |
| 开发型 compose 更多 | `docker-compose.yml` 再加 `nginx`、`superset-node`（webpack dev server，端口 9000）、`superset-tests-worker`（`profiles: [optional]`）→ **开发栈 9~10 服务**；官方注释明写 *「We don't support docker compose for production environments.」* | [docker-compose.yml](https://github.com/apache/superset/blob/master/docker-compose.yml) |
| **哪些是可选的（官方一手依据）** | ① **示例数据**：`docker-init.sh` 用 `if [ "$SUPERSET_LOAD_EXAMPLES" = "yes" ]` 才跑第 4 步 `superset load_examples` —— 不设即跳过（`docker/.env` 默认是 `yes`，本项目应显式关）；② **Celery/beat**：`config.py` 原话 *「Set celery config to None to disable all the above configuration」*（`CELERY_CONFIG = None`）⇒ **worker/beat 两个容器可整段不写**；③ **Redis**：`docker/pythonpath_dev/superset_config.py` 里 `CACHE_CONFIG`/`DATA_CACHE_CONFIG`/`CELERY_CONFIG` 都指向 Redis，而 `config.py` 默认是 **`NullCache`** 与 **`sqla+sqlite:///celerydb.sqlite`** => Redis 在**默认配置下不是必需**，按「缓存/异步/Celery 是否需要」决定；④ **websocket**：官方注释 *「Opt-in — start it with `docker compose --profile websocket up`」* | [docker-init.sh](https://github.com/apache/superset/blob/master/docker/docker-init.sh)、[config.py](https://github.com/apache/superset/blob/master/superset/config.py)、[pythonpath_dev/superset_config.py](https://github.com/apache/superset/blob/master/docker/pythonpath_dev/superset_config.py) |
| **「只跑 web + 外部元数据库」是否官方支持？** | **是官方形态，但带一句免责**：`docker-compose-non-dev.yml` 头部原话 *「We don't support docker compose for production environments. If you choose to use this type of deployment make sure to create your own docker environment file (docker/.env) with your own unique random secure passwords and SECRET_KEY.」*；且 `superset` 服务显式 `depends_on: superset-init: condition: service_completed_successfully`（初始化必须先跑完）。**即：web-only 是被官方骨架默认出来的形态，但「官方不支持」要照抄进文档** | 同 non-dev compose |
| 官方可调项与默认值 | `docker/entrypoints/run-server.sh`：`--bind ${SUPERSET_BIND_ADDRESS:-0.0.0.0}:${SUPERSET_PORT:-8088}`、**`--workers ${SERVER_WORKER_AMOUNT:-1}`**、`--worker-class ${SERVER_WORKER_CLASS:-gthread}`、`--threads ${SERVER_THREADS_AMOUNT:-20}`、`--timeout ${GUNICORN_TIMEOUT:-60}`；worker 并发官方默认 **2**（`docker-bootstrap.sh` 原文 *「setting up only 2 workers by default to contain memory usage in dev environments」*，`--concurrency=${CELERYD_CONCURRENCY:-2}`）；`config.py` 的 `SUPERSET_WEBSERVER_TIMEOUT = 60`（1 分钟）、`SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT = 0`（**只是警告阈值**，GDD §1.6 已核实无误） | [run-server.sh](https://github.com/apache/superset/blob/master/docker/entrypoints/run-server.sh)、[docker-bootstrap.sh](https://github.com/apache/superset/blob/master/docker/docker-bootstrap.sh)、config.py |
| 官方内存数字 | **官方未给出**（Superset 官方 installation 页无内存数字；`docker/README.md` 只说 *「If you are attempting to build on macOS and it exits with 137 you need to increase your Docker resources」*）→ 与 FSR R4 / SRS A2 一致，**不得编数字** | [docker/README.md](https://github.com/apache/superset/blob/master/docker/README.md) |
| A 轴 | 强：**确切配置键** + `superset import-dashboards -p <.zip>`（源码级证据，GDD §1.7 已记）+ `GET /health` 返回**纯文本 `OK`**（GDD §1.8 已记）+ 官方 compose/entrypoint/init 脚本齐备 | 本仓既有 + 官方源码 |
| C 轴（与既有接缝） | **零改动**：`ecom_ads` 直连不变、端口 `127.0.0.1` 不变、交付形式 `import-dashboards` 不变、§6.1 不涉及、`REQ-F-BI-04` 脏率面板仍是原来的面板。**这是唯一「什么都不用重做」的降级方式** | — |

**降配后的预算重算（设计值，非官方数字）**：

| 方案 | 组成 | 预算变化 | 依据 |
| --- | --- | --- | --- |
| 现方案 | Redis + Postgres + Superset web + init + worker + beat | 1.5 GB（FSR §2） | 设计值 |
| **降配 A（首选）** | 去掉 `superset-worker` + `superset-worker-beat`（`CELERY_CONFIG = None`）、`SUPERSET_LOAD_EXAMPLES` 不设 | **≈ −0.3 ~ −0.5 GB**（估算，官方未给出） | 官方 `CELERY_CONFIG = None` 一手可行使；**注意**：异步查询、定时报告、缩略图随之不可用——本项目**都不需要** |
| 降配 B | 再合并元数据库：用**外部元数据库**（Doris 或复用已起的 Postgres 实例）替掉独立的 `db` 服务；缓存留 `NullCache` 或元数据库 | **≈ −0.2 ~ −0.3 GB**（估算） | `config.py` 默认 `NullCache` + `SQLALCHEMY_DATABASE_URI` 可指向任意 `postgresql://` / `mysql://` |
| 降配 C | 单容器合并（web + init 同一容器，init 后 exec gunicorn） | 少一个一次性容器的启动峰值 | 官方 init 与 app 用**同一 build target**（`x-common-build`），合并无镜像代价 |

**结论：R3 的降级对象应从「Superset → Grafana」改写为「Superset 减服务（worker/beat → 外部元数据库 → Redis）」**。理由：① 其余候选**至少有一条判据天然做不到**（见下表）；② 降配**不动任何锚点与接缝**；③ 官方给了 `CELERY_CONFIG = None` 这条**明确的一手开关**。

## 7. 其它候选（只写找到一手许可与形态证据的）

| 候选 | 许可证（一手） | 形态 | 判据②（≤10 s） | 判据①（零账号） | 判据④（`ecom_ads` MySQL） | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| **Apache Hue** | **Apache-2.0**（GitHub API `cloudera/hue`：`license.spdx_id=apache-2.0`；⚠️ 仓库已由 `apache/hue` 迁至 **`cloudera/hue`**，`apache/hue` 现 404） | Django/Python 常驻服务；SQL 编辑器 + 查询/Dashboard 应用 | **未核查**（官方 querying 页只讲查询与图表，未见面板自动刷新档位）→ 不确定 | 可做到：官方有 `desktop.auth.backend.AllowAllBackend`——*「does not require a password for users to log in」* | 支持（官方连接器清单含 MySQL 系） | **保留观察**：许可干净、零账号可配，但**「看板自动刷新」这条官方证据本轮未取到**，且它是「SQL 编辑器」不是 BI 面板工具，5 面板要重做 |
| **Apache Zeppelin** | **Apache-2.0**（`LICENSE` 18 011 B 全文 + 子组件 Apache/MIT/BSD/OFL/EPL/Jython 混合清单） | JVM（Maven/Scala）、notebook 服务 | **官方未给出**：Zeppelin 是 notebook（段落执行），**无面板自动刷新概念** | 默认要认证（Shiro）→ 需额外配置 | 支持 JDBC（Doris 可接） | **排除**：notebook ≠ 看板，判据②③ 均需自建 |
| **Streamlit** | **Apache-2.0**（GitHub API `streamlit/streamlit`：`apache-2.0`） | Python 常驻服务（自建面板） | **可做到**：官方 `st.fragment(run_every=…)`——*「Streamlit will also rerun the fragment at the specified interval … An `int` or `float` specifying the interval in seconds」* | **默认零账号**（认证是可选配置：`st.login` 要**显式**配 OIDC） | 支持（自写 MySQL 连接器/SQLAlchemy） | **保留观察（非 BI）**：唯一能同时满足①②的候选，代价是**5 面板 + 步进漏斗 + 暂定/落定/已修订全部手写**（GDD §1.6 已要求自绘省份方格图/treemap/delta/sparkline，故新增量主要是「实时刷新 + revision 视觉」这层） |
| **Gradio** | **Apache-2.0**（GitHub API `gradio-app/gradio`） | Python 常驻服务 | **未核查**（未取到官方定时刷新的一手表述） | 默认零账号（可 `auth=`） | 支持（自写） | **本轮未核查**（刷新与看板能力） |
| **sqlpad** | **MIT**（`LICENSE.md` 逐字 + GitHub API `mit`） | Node.js 单容器（`sqlpad/sqlpad`） | **不适用**：它是 **Web-based SQL editor**（GitHub description 原文），**无 dashboard 面板概念** | 可配 | 支持 MySQL | **排除（且已停维）**：GitHub API `archived = true`、`pushed_at = 2025-08-23`、`open_issues = 0` |
| **Lightdash** | **MIT**（`LICENSE`）+ **例外**：`packages/backend/src/ee/` 走 **Lightdash Source Available License**（原文：*「may only be used in production, if you … have a valid Lightdash Enterprise Subscription」*；且 *「it is forbidden to copy, merge, publish, distribute, sublicense, and/or sell」*） | Node.js 服务 + 必须配 **dbt 项目** | 未核查 | 可配 | **不支持**：官方连接清单为 BigQuery / Postgres / Supabase / Redshift / Snowflake / Databricks / Trino / ClickHouse / DuckDB / Athena → **无 MySQL** | **排除**：判据④ 断链 + 引入 dbt 依赖（与「不引入调度器/不增组件」口径相冲）+ EE 目录非宽松 |
| **Apache Superset 自身** | Apache-2.0（见 §6） | 见 §6 | ✅ 无硬下限（`PERIODICAL_REFRESH_LIMIT=0` 仅警告） | ✅ `Public` 角色（`PUBLIC_ROLE_LIKE` / `AUTH_ROLE_PUBLIC`） | ✅ MySQL 内置 | **选中不变** |

## 8. 判据② 与判据③ 横向对照（本文的裁决表）

| 候选 | ① 零账号 | ② 刷新 ≤10 s | ③ 暂定/落定/已修订 | ④ `ecom_ads` 直连 | 任一断链？ |
| --- | --- | --- | --- | --- | --- |
| **Superset 6.1.0（选中）** | ✅ `Public` 角色 | ✅ 无硬下限（默认无刷新，可设 10 s） | ⚠️ 需自建（用 `revision` + 面板文案；**这正是 GDD §1.6 追加项**） | ✅ MySQL 内置 | 无阻断 |
| Superset 降配 | ✅ | ✅ | ⚠️ 同上 | ✅ | **无阻断（推荐路径）** |
| Grafana OSS | ✅ `[auth.anonymous]` | ✅ `min_refresh_interval` 默认 5 s | ❌ 无原生语义 + §6.1 撞车 | ✅ MySQL 内置 | **③ + §6.1** |
| Metabase | ✅ public link | ❌ **官方档位最小 1 分钟** | ❌ | ✅ MySQL 内置 | **②（决定性）** |
| Redash | ⚠️ 秘密链接（官方自曝参数可改） | ❌ **官方最小 60 s** | ❌ | ✅ `mysql.py` | **②（决定性）** |
| Evidence.dev | ⚠️ 默认 Basic Auth，需显式关 | ❌ 无「自动刷新」概念 | ⚠️ 代码可表达 | ❌ **直连无 MySQL** | **② ④** |
| Streamlit | ✅ 默认无认证 | ✅ `st.fragment(run_every=…)` | ⚠️ 全手写 | ✅ 自写连接 | 无阻断（但非 BI） |
| Hue | ✅ `AllowAllBackend` | ❓ 未核查 | ❓ | ✅ JDBC | 未核查 |
| Zeppelin | ❌ 默认要认证 | ❌ 无此概念 | ❌ | ✅ JDBC | **②** |
| sqlpad | — | ❌ 无面板 | ❌ | ✅ | **已归档** |
| Lightdash | ⚠️ | ❓ | ❓ | ❌ **无 MySQL** | **④** |

## 9. C 轴：换成别家后「必须重做什么」（对 R3 各候选的代价）

| 候选 | `superset import-dashboards -p <.zip>` 的替代 | 5 面板（PV/UV、4 步步进漏斗、GMV、品类、地域）+ 脏率面板 | `make verify` 第 5 项（`GET /health` 纯文本 `OK`） | 端口/`127.0.0.1` |
| --- | --- | --- | --- | --- |
| **Superset 降配** | **不用重做** | **不用重做** | **不用重做** | 不变 |
| Grafana | 需自写 dashboard JSON provisioning（有官方 provisioning 目录） | 全重写（漏斗最难） | `GET /api/health` 返 JSON，断言要改 | 新增 3000 |
| Metabase | Serialization 是类比物但非等价；实际手工重建 | 全重写 + 刷新档位不达标 | `curl -I /api/health`（官方 compose 自带） | 新增 3000 |
| Redash | 无导入 CLI；查询/看板手工重建 | 全重写 | 官方未给出健康端点（本轮未取到） | 新增 5000 + nginx 80 |
| Evidence | 交付物变成 .md/.sql + 构建产物 | 全重写（且非实时） | 官方未给出（`evidence serve` 无文档化 health） | 新增 3000 |
| Streamlit | 无交付 CLI；`app.py` 即交付物 | 全手写（可做到 ③ 但工作量最大） | 官方未给出（需自写 `/healthz`） | 新增 8501 |

## 10. 未决 / 官方未给出 / 不确定

**官方未给出（不得编数字）**
1. **Superset 官方未给出**单机最低内存（`docker/README.md` 只提 137 退出码）；官方 compose **未写任何 `mem_limit`**（non-dev 与 dev 均无）。
2. **Grafana 官方未给出**「暂定值/落定值/已修订」这类窗口语义；其 sizing 页明确只覆盖 *「the Grafana server process only」*，**不含数据源侧**。
3. **Metabase 官方未给出**任何「秒级自动刷新」档位；官方两处内存口径**互相不一致**（1 GB 起手 vs 4 GB 单核可撑数百用户），本文并列不改写。
4. **Redash 官方未给出**可达的 `≤10 s` 刷新（档位本身就写死了 60 s 起）；**官方未给出**健康检查端点。
5. **Evidence 官方未给出**「自动刷新面板」概念与任何 ≤10 s 表述；**官方未给出**直连环的 MySQL 支持。
6. **Hue 官方未给出**面板自动刷新档位（本轮未取到）；**Zeppelin 官方未给出**任何面板刷新语义。
7. **Streamlit / Gradio 官方未给出**「BI 面板 / 窗口 revision」语义（它们提供的是通用 rerun 原语）。

**未逐字核对 / 不确定（本文件内标 ❓）**
- **Hue**：① 面板自动刷新与「暂定/落定」表达；② 其 Docker 部署镜像与内存；③ 从 `apache/hue` 迁到 `cloudera/hue` 后 ASF 治理与许可证文件的一手路径（`apache/hue` 已 404，本文许可结论取 GitHub API 的 `cloudera/hue` 元数据，**未取 `LICENSE` 正文**）。
- **Gradio**：定时刷新的一手表述、看板能力。
- **Lightdash**：刷新机制与零账号（判据④ 已足以排除，故未深挖）。
- **Superset 降配的量化**：−0.3 ~ −0.5 GB / −0.2 ~ −0.3 GB 是**估算**；官方未给内存数字，实现阶段必须实测填充。
- **本机镜像体积与容器内存**：`docker` 守护进程未运行 → **本轮未取得**（与 `hld-research-observability.md` §0 同一状态，未变化）。
- **Grafana `min_refresh_interval` 与其 UI 档位清单的关系**：官方原文只说「prevents users from setting … lower than a given interval」+ 默认 5 s；**其内置档位列表（5s/10s/30s/1m…）本轮未在官方文档中逐条取到**（只取到配置键与默认值），若要把「10 s 可选」写死需再核一次源码。
- **R3 序列本身**：本文**不改锚点**、也不改 FSR/SRS/GDD；若采纳「Superset 降配」为新的 R3 级，需由上位文档（FSR §8 R3 / GDD §C F9）另行收口。
