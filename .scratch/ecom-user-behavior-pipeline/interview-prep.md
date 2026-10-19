# 面试问答提纲 — 电商用户行为数据全链路

> **面向**：你自己。用法是**先自答、再看要点**，不是背稿。
> **日期**：2026-10-18（Asia/Shanghai）
> **配套**：`FSR.md`（结论与决策理由）、`research-notes.md`（每条要点的一手来源 URL）
> **纪律**：以下要点中，凡官方来源支持的事实都带来源标注；凡属「你的设计判断」的，写 **【判断】**——面试时这两类要分清，把判断说成官方事实是硬伤。

## 0. 三句话开场

> 「我做了一条**单机一条命令可复现**的电商用户行为全链路：Python 生成器按 200 events/s 写 Kafka，Flink 2.2 做 1 分钟窗口聚合，一边走 Flink-Doris-Connector 写进 Doris 供实时看板，一边落 Paimon 做 ODS→DWD→DIM→DWS→ADS 五层离线数仓。整条链路锚定精确版本、容器群压进 8 GB，在一台 16 GB 的机器上跑。」

**这一段本身就能引出三个追问点，主动把它递出去**：为什么是 Flink 2.2 不是 2.3？为什么选 Paimon？16 GB 怎么装下这么多组件？

---

## 1. 技术选型：为什么是这一套

### Q1.1 为什么 Kafka 而不是 Pulsar / RabbitMQ？

**要点**
- Kafka 4.0.0 起**已完全移除 ZooKeeper，KRaft 为唯一模式**（[4.0.0 官方公告](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/)）——部署比老教程少一个组件，这正是单机复现需要的。
- 官方单容器镜像 `apache/kafka:4.3.1` 一条 `docker run` 即起 broker（[官方 downloads](https://kafka.apache.org/downloads)、[官方 Docker 文档](https://kafka.apache.org/41/getting-started/docker/)）。
- Pulsar 我也看过：5.0.0 LTS、Apache-2.0、同样活跃（[官方 release notes](https://pulsar.apache.org/release-notes/)），但它的存算分离（BookKeeper）在单机上是**多一组要维护的组件**；且 Kafka+Flink 的生态与岗位覆盖面更广。**面试里能说「我评估过 Pulsar，取舍是 X」比只会 Kafka 强。**

**可能的追问**
- *Kafka 单机最低内存多少？* → **官方未给出**（我笔记里如实标注，实测为准）。别说一个编的数字。
- *Kafka 能撑多少吞吐？* → 官方文档引用的 LinkedIn 峰值：60 broker、5 万分区、80 万 msg/s 入（[官方 Java 文档](https://kafka.apache.org/documentation/#java)）。但**单机吞吐量级与分区数硬上限，官方未给出**。

### Q1.2 ⭐ 为什么 Flink 用 2.2.x 而不是最新的 2.3.0？

**这题是送分的，因为大多数人会踩坑。**

- 官方 connector 文档原话：**"There is no connector (yet) available for Flink version 2.3"**（[Kafka connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)）。
- 已发布的 Kafka connector **5.0.0 覆盖 Flink 2.1.x / 2.2.x**，4.0.1 覆盖 2.0.x（[官方 downloads connector 清单](https://flink.apache.org/downloads)）。
- 所以：**跟最新版 = 主链路断掉**。这是「版本锚点必须精确 pin」的活教材——可复现性不是洁癖，是能不能跑起来的问题。

### Q1.3 为什么湖存储选 Paimon 而不是 Iceberg？

**要点**
- Paimon 是 **Apache 顶级项目**（2024-04-16 毕业，[ASF 官方公告](https://news.apache.org/foundation/entry/apache-software-foundation-announces-new-top-level-project-apache-paimon)），官网**把 Flink 列为第一引擎**，Quickstart 就是「本地 Flink 集群流式写 Paimon」（[Paimon Flink Quick Start](https://paimon.apache.org/docs/master/flink/quick-start/)）。
- 关键能力：**主键表 + LSM + changelog producer**，天然产生变更流，直接喂实时看板的回撤语义（[官网首页](https://paimon.apache.org/docs/master/)）。
- Iceberg 的优势我也认：**生态最广、Flink/Spark 官方文档最对称、面试认知度最高**（[iceberg.apache.org docs](https://iceberg.apache.org/docs/latest/)）。不选它的理由是 **【判断】**：我的主线是实时链路，Iceberg 的流式 upsert/changelog 弱于 Paimon 主键表，选它要多拼一层组件、多占一份内存天花板。
- 落点是**锚定 2.1.0 而不是 master**：Paimon 2.x 发布节奏快、行为变更多，作品集要 stable（[官方 releases](https://github.com/apache/paimon/releases)）。

### Q1.4 ⭐ 为什么 OLAP 选 Doris，而不是 StarRocks 或 ClickHouse？

**这题最能体现你「真的查过一手文档」。**

- **StarRocks 被排除的原因不是许可证，是版本相容性。** 先纠正一个流传的说法：StarRocks 官方 `LICENSE.txt` 就是 **Apache-2.0** 全文，代码可自由使用与再分发（[LICENSE.txt](https://github.com/StarRocks/starrocks/blob/main/LICENSE.txt)），**未发现任何「换成 Elastic License」的官方声明**。它真正的问题是：官方 Flink connector v1.2.16 的 release 资产**只有 flink-1.16/1.17/1.18/1.19/1.20 的 jar，没有 Flink 2.x artifact**（[v1.2.16](https://github.com/StarRocks/starrocks-connector-for-apache-flink/releases/tag/v1.2.16)）——与我的 Flink 2.2.x 不兼容。另外它的 Paimon Catalog 是 **Beta 且只读**（[paimon_catalog.md](https://github.com/StarRocks/starrocks/blob/main/docs/en/data_source/catalog/paimon_catalog.md)）。
- **ClickHouse 被排除的原因是内存与 Paimon。** 官方 sizing 文档给出的最硬下限是 **「总量不应低于 8 GB」**（[sizing 文档](https://github.com/ClickHouse/ClickHouse/blob/master/docs/guides/oss/best-practices/sizing-and-hardware-recommendations.mdx)）——单这一项就吃满我 8 GB 的常驻预算；且它**官方完全未提供 Paimon 支持**（其 Data catalogs 页只收录 Iceberg/Delta，且顶部挂 Beta 标，[data-catalogs.mdx](https://github.com/ClickHouse/ClickHouse/blob/master/docs/integrations/data-catalogs.mdx)），**官方也未给出 Flink connector**。
- **Doris 是唯一三个条件同时满足的**：Apache-2.0（[LICENSE.txt](https://github.com/apache/doris/blob/master/LICENSE.txt)）+ 官方 Flink connector **26.3.0 明确支持 Flink 2.2.x**（[README 版本表](https://github.com/apache/doris-flink-connector/blob/master/README.md)）+ 官方有 **Paimon Catalog**（可读可写）（[paimon-catalog.mdx](https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/lakehouse/catalogs/paimon-catalog.mdx)）。
- **诚实补一句（会加分）**：Doris 官方**未给出**单机最低内存数字，所以「16 GB 跑得动 Doris」是**待实测**的事，不是官方保证。而且官方明确 Docker 部署**仅用于本地开发测试，不要用于生产**（[quick-start.mdx](https://github.com/apache/doris-website/blob/master/docs/getting-started/quick-start.mdx)）——这句话在面试里被我主动说出来，比被追问出来强。

### Q1.5 为什么看板选 Superset 而不是 Grafana？

**要点**
- Superset：Apache-2.0（[LICENSE.txt](https://github.com/apache/superset/blob/master/LICENSE.txt)）、官方 Docker 目录（[docker/](https://github.com/apache/superset/tree/master/docker)）、官方文档**明确列出 Apache Doris / ClickHouse / StarRocks** 端点（[docs/docs/index.mdx](https://github.com/apache/superset/blob/master/docs/docs/index.mdx)）。
- Grafana：**AGPLv3**——官方公告 **2021-04-20 起核心项目由 Apache-2.0 迁至 AGPLv3**（[grafana.com/licensing](https://grafana.com/licensing/)）。第 13 条的源码义务**只在「修改该程序并经网络对外提供」时触发**，本机自用不改不分发**实际无义务**；但它不符合我「只接受宽松许可」的选型口径，所以列为**可替换项**。
- **【判断】一个真实取舍**：Grafana 官方最低 **512 MB / 1 核**（[Install Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)），远轻于 Superset 的 Python 栈——而 **Superset 的单机内存要求官方未给出**。所以它是我「内存实测不足时的降级选项」，不是被否掉。
- DataEase 也评估过：v3.1.0、24,615 stars、活跃（[repo](https://github.com/dataease/dataease)），但 `LICENSE` 是 **modified GPLv3 + 附加条件**（不得移除前端 LOGO、厂商可随时收紧协议）（[LICENSE](https://github.com/dataease/dataease/blob/dev-v3/LICENSE)）——不符合宽松许可口径。

---

## 2. 数仓建模

### Q2.1 ⭐ 为什么分层是 ODS→DWD→DIM→DWS→ADS 五层？

**要点（按层说「它存在的理由」）**
- **ODS**：保真 + 可重放。所以我用 **append-only 表**、保留**原始 JSON payload 一列**——ODS 不做任何有损加工，出问题能从头重跑。
- **DWD**：清洗与规范化后的事件明细。统一 **UTC epoch 毫秒**（UserBehavior 原始是秒级 epoch，转毫秒是清洗步骤之一）、统一成超集 schema。
- **DIM**：独立维表层，**用 Paimon 主键表做时态维表**。这层是我故意留的——它让我能讲「维表 join / 缓慢变化维」，是实时链路的高频考点。
- **DWS**：主题宽表聚合（按用户 / 商品 / 品类 / 时间粒度）。
- **ADS**：看板直接消费的应用层指标。

**核心追问：为什么 ODS 用 append 表、DWD 起才用主键表？**
→ **ODS 要的是保真与可重放**（不去重、不合并、不吃 upsert）；**DWD 起需要 upsert 与 changelog 语义**（用户行为会重复上报、需要修正）。这是**存储形态跟着建模目的走**，不是拍脑袋。

### Q2.2 一套 schema 怎么同时接实时流和公开数据集回放？

**要点**
- 我定义**一套超集 schema**，事件命名对齐 **GA4 电商事件规范**：`page_view` / `view_item` / `add_to_cart` / `begin_checkout` / `purchase`，字段带 `amount` / `province` / `channel`（[GA4 Measure ecommerce](https://developers.google.com/analytics/devguides/collection/ga4/ecommerce)、[Recommended events](https://support.google.com/analytics/answer/9267735?hl=en)；Snowplow 的 [iglu schema](https://docs.snowplow.io/docs/events/ootb-data/ecommerce-events/) 与 Segment 的 [Ecommerce V2 Spec](https://www.twilio.com/docs/segment/connections/spec/ecommerce/v2) 是另两份可引的官方规范）。
- 天池 UserBehavior 的 `pv` / `cart` / `fav` / `buy` 是这套 schema 的**子集**，映射进同一套表——所以**实时流与离线回放能落到同一张 DWD 上对账**。这是我选「混合数据来源」的技术前提。
- UserBehavior 的硬限制要知道：**5 列、无金额、无地域**（[天池数据集 649](https://tianchi.aliyun.com/dataset/649)），单靠它做不出丰富 ADS 指标——所以才需要自研生成器补维度。

### Q2.3 公开数据集的许可怎么处理？

**要点**
- UserBehavior 许可为 **CC BY-NC-SA 4.0**（非商业 + 相同方式共享），且**需天池登录**（默认审批，学生认证可免审；官方未给出免登录直链）（[天池数据集 649](https://tianchi.aliyun.com/dataset/649)）。
- 我的处置：**仓库零原始数据**，只放下载脚本 + md5 校验说明 + 许可署名。理由：作品集最怕「别人 clone 下来跑不了因为你没数据」和「数据许可是 NC 的」两个坑，我把数据获取变成一条可选路径，而不是仓库依赖。
- **【判断】** NC/SA 条款会约束作品集后续的商业使用，所以不进 git 是稳妥解。

---

## 3. ⭐ 实时深挖点（主攻 A + B）

### Q3.1 乱序数据怎么处理？Watermark 怎么用？

**要点**
- WatermarkStrategy 挂在 source 上；Kafka source **默认以 record 时间戳作为事件时间**（[Kafka connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)）。
- **一个真实的坑**：并行度大于分区数时，空闲分区会把 watermark 卡住 → 需要 `withIdleness`（[Generating Watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)）。
- 我的链路里**乱序是天然存在的**：生成器带「大促尖峰」模式（短时 ×5~10），事件时间与到达时间会拉开；这就是我制造和演示乱序场景的手段。
- 处理手段：`allowedLateness` + 侧输出流接迟到数据。**面试要点是能说清「迟到数据我选择丢弃还是补写」这个业务决策**，不是只报 API 名。

### Q3.2 ⭐ 端到端 exactly-once 怎么保证？

**要点（分三段说：source / 内部 / sink）**
- **Flink 侧**：官方文档开篇即 "Flink provides an Apache Kafka connector … with exactly-once guarantees"（[Kafka connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)）——机制是 **checkpoint + 两阶段提交**。
- **Sink 侧**：我走 Flink-Doris-Connector，Doris 的 **Stream Load 提供 two-phase commit**，且官方保证**单批原子性（全成功或全失败）**，通过 `label` 实现幂等/At-Most-Once（[Stream Load 手册](https://github.com/apache/doris-website/blob/master/docs/data-operate/import/import-way/stream-load-manual.md)）。
- **状态侧**：`EmbeddedRocksDBStateBackend` 是**唯一支持增量 checkpoint** 的后端，状态仅受磁盘限制，但吞吐低于堆后端（[State Backends 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)）。
- **能加分的一句**：end-to-end exactly-once 不只是 Flink 的事，**source 的 offset、checkpoint 的存储、sink 的幂等或事务**三处都要对齐；我这条链路三处都落在官方支持过的组合上。

### Q3.3 为什么 UV 用近似算法？

**要点**
- **【判断 + 已知事实】**：全量去重状态会把内存吃满，而我的机器只有 8 GB 容器预算；所以实时侧用 **HyperLogLog / Theta sketch** 做近似 UV，**离线侧算精确 UV 并对账，把误差量化写进文档**。
- 这正好咬住 Q3.2 的状态后端话题：近似算法 = 用可控误差换内存。**能说出「误差量级是多少、业务能否接受」比说出算法名重要。**

### Q3.4 窗口为什么选 1 分钟？

**要点**
- **【判断】**：1 分钟滚动窗口下，200 events/s 约 1.2 万事件/窗口——状态压力可控，看板 5~10s 轮询有持续刷新的观感（这直接对应我的成功判据之一）。
- 为什么不做 5~10s 窗口：单机小流量下无收益，反而徒增 checkpoint 频率与状态压力。为什么不做 5 分钟：刷新观感太弱。

---

## 4. 工程与复现性

### Q4.1 ⭐ 16 GB 的机器怎么装下整条链路？（这题要答得漂亮）

**要点（先给官方事实，再给你的设计）**
- **官方事实**：WSL2 的 `memory` **默认值就是宿主机总内存的 50%**，16 GB 机器 ≈ **8 GB**；`processors` 默认等于全部逻辑核，不封顶（[.wslconfig 参考](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)）。
- **官方事实**：WSL2 后端下 **Docker Desktop 不提供 CPU/内存滑块**，官方原文 "In WSL 2 mode, configure memory, CPU, and swap limits on the WSL 2 utility VM"（[Docker Desktop settings](https://docs.docker.com/desktop/settings-and-maintenance/settings/)）。
- **官方事实**：容器**默认无资源约束**，超限时内核 OOM killer 杀进程（[Resource constraints](https://docs.docker.com/engine/containers/resource_constraints/)）。
- **【我的设计】**：① 每个服务在 compose 里显式写 `mem_limit`，总预算压进 ≈8 GB；② 把 `.wslconfig` 的 `memory` 提到 10~12 GB（官方要求改后 `wsl --shutdown` 生效、取值必须带单位）；③ 砍掉一切不必要的常驻——不跑 Spark 常驻、不跑调度器、Iceberg 只写文档不实现。
- **这是我最想讲的一段**：资源受限下的**裁剪决策**比堆组件更能证明工程判断力。每个被砍掉的组件我都能说清「为什么砍、砍了损失什么」。

### Q4.2 什么是「可复现」？你怎么保证？

**要点**
- **精确 pin 版本**（Kafka 4.3.1 / Flink 2.2.x / connector 5.0.0 / Paimon 2.1.0 / Doris 4.1.5 / connector 26.3.0 / Superset 6.1.0），不跟 `latest`。
- 一条 `docker compose up` + 初始化脚本。
- **数据自给**：生成器在仓库里，公开数据集只是可选加成——别人 clone 下来不需要账号也能跑起来。
- 一句话总结：「可复现 = 锚点不漂移 + 数据不依赖外部账号 + 一条命令」。

### Q4.3 为什么不用调度器（Airflow / DolphinScheduler）？

**要点**
- **【判断】**：每个调度器常驻要额外 2~3 GB 内存天花板，在 8 GB 预算里不划算。
- 替代：**Makefile + 脚本 + 手动触发**，任务依赖关系在 Makefile 里显式表达。
- **关键话术**：「我知道生产环境这里应该接调度器（Airflow/DolphinScheduler），也知道调度器的核心能力是依赖管理、重试、回填与 SLA 告警；我这里裁掉它是因为单机可复现性优先——**能力认知没缺，只是没在这个环境里用**。」这比硬塞一个跑不动的容器强。

### Q4.4 踩过哪些坑？

**准备 2~3 个真实故事（实现阶段补）**，建议优先选：
1. **Flink 2.3 无 Kafka connector** —— 「跟最新版导致主链路断掉，从此我的选型第一原则是查兼容矩阵」。
2. **Doris Paimon Catalog 依赖 Paimon 1.4.2** —— 「官方 Catalog 直读依赖的 Paimon 版本和我锚定的 2.1.0 有落差，所以我把看板供数主路径改成 Flink-Doris-Connector 写内表，Catalog 直读降级为可选实验」。
3. **WSL2 默认内存天花板 ≈8 GB 与预算相撞** —— 「不写 .wslconfig 时天花板默认就是宿主机一半」。

---

## 5. 反问环节（面试官问「你有什么想问的」）

准备这三条，它们都能传递专业性：

1. 「你们实时链路的**端到端一致性**是怎么保障的——sink 侧走事务还是幂等？」
2. 「数仓分层是几层？**ODS 是保真还是已清洗**？」
3. 「离线与实时的**指标口径**是怎么对齐的——有没有统一的口径层或指标平台？」

---

## 6. 一张自检清单（面试前 10 分钟看）

- [ ] 能不看稿说出：Flink 为什么是 2.2.x 而不是 2.3。
- [ ] 能说出三个被排除的候选**各自的真正原因**（StarRocks=连接器版本、ClickHouse=内存+Paimon、Grafana=许可口径）。
- [ ] 能说清 ODS 用 append 表、DWD 起用主键表的**建模理由**。
- [ ] 能分 source / 内部 / sink 三段讲 exactly-once。
- [ ] 能说出 WSL2 默认内存 = 宿主机 50% 这条**官方事实**。
- [ ] 每个「官方未给出」的数字都能诚实说「官方未给出，我是实测的」——**这条最加分**。

## 7. 诚实边界（红线）

以下内容**绝不能说成官方结论**：

| 说法 | 正确处理 |
| --- | --- |
| 「Kafka / Flink / Doris / Superset 单机最低内存是 X GB」 | 这些**官方均未给出**；只能说实测值 |
| 「StarRocks 用的是 Elastic License」 | 其官方 `LICENSE.txt` 是 **Apache-2.0**；排除原因是 Flink connector 无 2.x artifact |
| 「Grafana 用 AGPL 所以不能用」 | 第 13 条义务只在**修改并经网络对外提供**时触发；自用不改无义务 |
| 「Paimon 2.1 与 Doris Catalog 官方兼容」 | 官方依赖的是 **Paimon 1.4.2**，与 2.1.0 有落差，是已知风险 |
| 「Doris Docker 可以用于生产」 | 官方明确**仅限本地开发测试** |
