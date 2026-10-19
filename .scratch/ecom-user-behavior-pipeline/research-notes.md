# 电商用户行为数据全链路 · 可行性调研笔记

> 调研时间：2026-10-18（Asia/Shanghai）。由 5 路并行后台调研汇总，**每条结论均附一手来源**（官方文档 / 官方仓库 / 官方 LICENSE / 官方公告）。凡官方未给出者，明确写「官方未给出」，不作估算、不编造。
>
> 本机硬约束（用户确认）：**6 核 12 线程 / 16 GB 内存 / Docker Desktop（WSL2 后端）可用**；常驻容器群预算目标 ≈ 8 GB。
>
> 关联文档：同目录 `FSR.md`（可行性分析与推荐路线）、`interview-prep.md`（面试提纲）。

## 0. 本机平台约束：WSL2 / Docker Desktop 资源上限

- **WSL2 默认资源分配**：官方 `[wsl2]` 键默认值表中，`memory` 默认 **"50% of total memory on Windows"**（16 GB 机器即约 8 GB）；`processors` 默认 **"The same number of logical processors on Windows"**（默认可占满全部逻辑核，本机 12 线程）；`swap` 默认 25% 内存向上取整到 GB，`swapFile` 默认 `%Temp%\swap.vhdx`（[.wslconfig 参考](https://learn.microsoft.com/en-us/windows/wsl/wsl-config)）。官方口径是「内存默认按宿主机比例封顶、CPU 默认不封顶」，并非完全无上限。Win10/Win11 默认值差异：**官方未给出**（文档仅以脚注区分平台可用键）。内存自动回收：官方实验段 `autoMemoryReclaim` 默认值 **`dropCache`**（另有 `gradual`/`disabled`），WSL 发行记录亦写明默认回收模式为 dropcache（[.wslconfig 实验段](https://learn.microsoft.com/en-us/windows/wsl/wsl-config#experimental-settings)、[WSL 2.1.3 Release Notes](https://github.com/microsoft/WSL/releases/tag/2.1.3)）。
- **`.wslconfig` 官方支持的键**：`[wsl2]` 段明确列出 `kernel`、`kernelModules`、`memory`、`processors`、`localhostForwarding`、`kernelCommandLine`、`safeMode`、`swap`、`swapFile`、`guiApplications`、`defaultVhdSize`、`nestedVirtualization`、`vmIdleTimeout`、`networkingMode`、`firewall`、`dnsProxy`、`dnsTunneling`、`autoProxy`（同上链接）。官方示例：`memory=4GB`、`processors=2`、`swap=8GB`、`localhostforwarding=true`；取值规则原文为 **"Entries with the `size` value default to B (bytes), and the unit is omissible. To use other units, the size unit must be appended, e.g.: `8GB` or `512MB`"**（即**必须带单位**）。生效要求：官方明确 **"you may need to run `wsl --shutdown` … for these changes to take effect"**，并给出 `wsl --list --running` 校验。**`pageReporting` 不在当前官方键表中（官方未给出）**，旧博客常引，勿采用。
- **数值上限**：官方**未给出** `memory` 的数值上限，也**未说明**能否超过宿主机物理内存或超设后果（**官方未给出**）。
- **Docker Desktop 侧资源控制**：官方 Settings → Resources → Advanced 表中，**CPU / Memory / Swap / Disk 限制的平台标注只有 "Mac, Linux, Windows Hyper-V"**，并在表下明确 **"In WSL 2 mode, configure memory, CPU, and swap limits on the WSL 2 utility VM"**（链接到微软 .wslconfig 文档）——**WSL2 后端下资源由 WSL 层决定，Docker Desktop 不提供同等滑块**（[Docker Desktop settings](https://docs.docker.com/desktop/settings-and-maintenance/settings/)）。WSL2 为动态内存分配、按需申请；**"Docker Desktop runs inside its own `docker-desktop` WSL distribution"**（[WSL 2 backend](https://docs.docker.com/desktop/features/wsl/)）。**Resource Saver 在 WSL 下只暂停 CPU、不返还 Docker 内存**，回收内存须靠 `autoMemoryReclaim`（[Resource Saver](https://docs.docker.com/desktop/use-desktop/resource-saver/)）。
- **容器级内存限制**：Compose Spec 定义 `mem_limit` 为服务级字段「configures a limit on the amount of memory a container can allocate」，并规定 **"When both are set, `mem_limit` must be consistent with the `limits.memory` attribute in the Deploy Specification"**（[Compose Spec services](https://docs.docker.com/reference/compose-file/services/)）；`deploy` 被表述为 **"an optional part of the Compose Specification"**，**"If not implemented the `deploy` section is ignored and the Compose file is still considered valid"**——是否生效以「实现是否支持」为准，**官方未给出「docker compose up 一律忽略 deploy」的现行表述**（属旧 v3 口径）（[Deploy Spec](https://docs.docker.com/reference/compose-file/deploy/)）。超限行为：官方明确默认容器**无资源约束**、超限时内核 OOM killer 杀进程（`OOME`），"Any process is subject to killing, including Docker"，故应显式设 `-m/--memory`（[Resource constraints](https://docs.docker.com/engine/containers/resource_constraints/)）。
- **Windows 侧官方已知限制**：bind mount 数据应位于 **Linux 文件系统** —— "Performance is much higher when files are bind-mounted from the Linux filesystem, rather than accessed from the Windows host filesystem"，官方明确避免 `docker run -v /mnt/c/users:/users`；inotify 事件仅在 Linux 文件系统投递（[WSL 2 best practices](https://docs.docker.com/desktop/features/wsl/best-practices/)）。官方列出旧版 WSL「`vmmem.exe` to consume all memory」等故障，要求 WSL ≥ 2.1.5（同上）。
- **对本项目的直接含义（推理，非官方数字）**：官方**未给出**任何推荐数值。依据官方默认值推理：16 GB 宿主机上 WSL2 内存天花板**默认即 ≈8 GB**，与常驻容器群 8 GB 预算同量级；WSL2 自身与 Docker Desktop 自身仍需余量。故「不写 `.wslconfig`」时，两者相加极易触顶。

## 1. 消息队列与流计算

### Apache Kafka

- **版本**：官方支持版本中最新稳定版 **4.3.1**（2026-06-25 发布），同页支持 4.2.2（2026-09-29）、4.1.2（2026-03-17）（[官方 downloads](https://kafka.apache.org/downloads)）。
- **KRaft 现状**：**4.0.0（2025-03-18）是首个完全无 ZooKeeper 的大版本，ZooKeeper 模式已移除，KRaft 为唯一模式**（[4.0.0 官方公告](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/)）。
- **许可证**：Apache-2.0（[GitHub apache/kafka](https://github.com/apache/kafka)）。
- **官方 Docker 镜像**：`apache/kafka:4.3.1`（JVM）与 `apache/kafka-native:4.3.1`（GraalVM 原生），官方 downloads 页直接给出镜像 tag，单容器 `docker run -p 9092:9092 apache/kafka:4.3.1` 即可起单机 broker（[downloads](https://kafka.apache.org/downloads)、[官方 Docker 文档](https://kafka.apache.org/41/getting-started/docker/)、[hub.docker.com/r/apache/kafka](https://hub.docker.com/r/apache/kafka)）。**单机最低资源要求：官方未给出**。
- **性能/规模（仅官方口径）**：官方 Java 文档引用 LinkedIn 峰值集群：60 broker、5 万分区（RF=2）、80 万 msg/s 入、300 MB/s 入 / 1 GB/s+ 出，90 分位 GC 停顿 ~21 ms（[官方 Java Version 文档](https://kafka.apache.org/documentation/#java)）。**单机吞吐量级、分区数硬上限：官方未给出**。
- **依赖**：Java 17/21/25 完全支持，Java 11 仅 clients/streams 等子模块，推荐最新 LTS（[同上](https://kafka.apache.org/documentation/#java)）。KRaft 后无 ZooKeeper；Connect/Streams 内置于发行包，无必备外部组件。

### Apache Flink

- **版本现状**：最新稳定版 **2.3.0**（downloads 页标注 "latest stable release"）（[官方 downloads](https://flink.apache.org/downloads)）；2.x 始于 2.0.0（2025-03-24 官方公告，官方支持 Java 21）（[2.0.0 公告](https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/)）；1.x 线 1.20.5 仍在维护列表。**2.x 生产采用度：官方未给出**统计。
- **⚠️ Kafka connector 断档（对选型有直接影响）**：官方 Kafka connector 文档明确 **"There is no connector (yet) available for Flink version 2.3"**——**Flink 2.3.0 尚无已发布 Kafka connector**；已发布 connector 5.0.0 支持 Flink 2.1.x/2.2.x、4.0.1 支持 2.0.x（[connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)、[downloads connector 清单](https://flink.apache.org/downloads)）。Flink CDC 3.7.0 亦只支持 Flink 1.20.x / 2.2.x。
- **许可证**：Apache-2.0（[GitHub apache/flink](https://github.com/apache/flink)）。
- **exactly-once**：官方文档开篇即 "Flink provides an Apache Kafka connector … with exactly-once guarantees"（[Kafka connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)）。
- **单机 / Docker**：官方给出 Session/Application 模式 `docker run` 与 docker-compose 示例；镜像双渠道 Docker Hub 官方库 `flink`（Docker 审核构建、官方推荐）与 `apache/flink`，源码在 Apache flink-docker 仓库，Web UI `:8081`（[官方 Docker 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/standalone/docker/)、[hub.docker.com/_/flink](https://hub.docker.com/_/flink)）。**内存最低要求：官方未给出统一数字**，只有 TM/JM 内存模型文档（total process memory = Flink memory + JVM 开销）（[TM 内存文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_setup_tm/)）。
- **Watermark / 状态后端**：WatermarkStrategy 挂在 source 上，Kafka source 默认以 record 时间戳为事件时间，并行度 > 分区数时需 `withIdleness` 防 watermark 卡死（[Kafka connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)、[Generating Watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)）。后端：默认 HashMapStateBackend（堆对象）；EmbeddedRocksDBStateBackend 状态仅受磁盘限制、**唯一支持增量 checkpoint**，吞吐低于堆后端；ForSt 后端实验性（[State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)）。

### 对照：Apache Pulsar

- 当前版本 **5.0.0（LTS，2026-10-05 发布）**，同日发布 4.0.14 / 4.2.5 维护版，2026 年内 10+ 个版本（[官方 release notes](https://pulsar.apache.org/release-notes/)、[release policy](https://pulsar.apache.org/contribute/release-policy/)、[GitHub releases](https://github.com/apache/pulsar/releases)）。Apache-2.0（[GitHub apache/pulsar](https://github.com/apache/pulsar)）。
- 对照结论：同为 Apache-2.0、社区活跃，但 Kafka+Flink 的生态与岗位覆盖面更广；作品集以 Kafka 为主线，Pulsar 作「为什么不用它」的对照。

## 2. 离线批处理与湖存储

### Apache Spark

- 最新稳定版 **4.2.0**（2026-07-14 发布），同时维护 4.1.3 / 4.0.4 / 3.5.9 三条线（[Spark 官网下载页](https://spark.apache.org/downloads.html)）。Apache-2.0。
- 官方 Docker 支持：Apache 官方维护 `apache/spark` 镜像（[apache/spark-docker 官方仓库](https://github.com/apache/spark-docker)），另有 Docker Official Image `spark`（[Docker Hub](https://hub.docker.com/_/spark)）。
- Structured Streaming：核心一级 API，4.x 持续演进（[官方编程指南](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)）；微批为主、延迟秒级。

### Apache Hive

- 最新版 **4.2.1**（2026-08-24），4.2.0 于 2025-11-23、4.1.0 于 2025-07-31（[Hive 官网 news](https://hive.apache.org/general/downloads)）；官方 GitHub 仓库**无 GitHub Releases 条目**（releases 接口返回空），发布记录以官网 news 页为准。
- **无官方「维护模式 / 不推荐新项目使用」声明——官方未给出**；相反 3.x/2.x/1.x 已 EOL、仅 4.x 活跃维护，且 4.2 重点转向 Iceberg 集成与 Hive Metastore 独立组件化（官网 news 原文）。可见其角色已收敛为 **Metastore + 兼容查询入口**，作为新项目离线数仓主引擎已非主流。Apache-2.0。

### Apache Paimon

- **2024-04-16 从孵化器毕业为 Apache 顶级项目**（[ASF 官方公告](https://news.apache.org/foundation/entry/apache-software-foundation-announces-new-top-level-project-apache-paimon)）。
- 最新版 **2.1.0**（2026-10-12，[官方仓库 releases](https://github.com/apache/paimon/releases)），官网标注 2.1 为 stable（[Paimon 官网文档](https://paimon.apache.org/docs/master/)）。Apache-2.0。
- Flink 集成深度：官网将 **Flink 列为第一引擎**，Quickstart 即演示本地 Flink 集群流式写 Paimon（datagen → 流式 INSERT → 批/流双读），要求开启 checkpoint（[Flink Quick Start](https://paimon.apache.org/docs/master/flink/quick-start/)）；主键表 + LSM 提供「流式更新、多 merge engine、changelog producer」（[官网首页定位](https://paimon.apache.org/docs/master/)）。**适合作为流式湖仓主存储**。注意：2.x 演进快，作品集应锁定 2.1 stable。

### Apache Iceberg

- 最新版 **1.12.0**（2026-09-30，[官方仓库 releases](https://github.com/apache/iceberg/releases)，官网文档 "Latest (1.12.0)" 一致）。Apache-2.0。
- Flink/Spark 集成官方支持**最高且最对称**：官网分别维护完整文档集（Spark: Getting Started/DDL/Procedures/Writes/Structured Streaming；Flink: Connector/DDL/Queries/Writes/TableMaintenance），并有 Spark/Flink/Hive 三个 Quickstart（[iceberg.apache.org docs](https://iceberg.apache.org/docs/latest/)）。
- 维护活跃度极高：一年多个大版本，每次 release 数百 PR、数十新贡献者（[releases 记录](https://github.com/apache/iceberg/releases)）。

### Apache Hudi

- 最新版 **1.2.1**（2026-09-24）；1.1.2 于 2026-10-17（1.1.x 修复线，属正常的分支并行维护）（[官方仓库 releases](https://github.com/apache/hudi/releases)）。Apache-2.0。
- 社区活跃度：release 节奏稳定、单版本数百 PR、持续有新贡献者（同上）；但相比 Iceberg/Paimon 在表格式标准之争中的声势近年偏弱（**此句为综合判断，无单一官方来源可引**）。

### 组合结论（该路调研 agent 的判断）

- 单机 Docker + 离线分层 + 兼职支撑实时链路的约束下，**首选 Spark（离线批）+ Flink（实时）+ Paimon 湖存储**，次选 Spark + Iceberg。
- Paimon 原生为 Flink 流式读写设计，主键表 + changelog producer 天然支撑看板回撤语义，一套存储同时服务批与流；取舍是 Paimon 2.x 演进快、面试认知度略低于 Iceberg。
- Iceberg 生态最广、Flink/Spark 官方文档最完整、面试认知度最高；取舍是流式 upsert 与 changelog 能力弱于 Paimon 主键表，实时链路要多拼一层组件。
- **Hive 仅作 Metastore，不作计算引擎**：官方重心已转向 Metastore 独立化与 Iceberg 集成，单机 Docker 里跑 HiveServer2+Tez 复杂度高、收益低。
- **Hudi 不作首选**：社区活跃但声势偏弱，单机 Docker 调试链路相对繁琐。

## 3. OLAP 引擎与看板

> 方法学说明：该路调研 agent 执行时 `web_search` 全部引擎不可用，改走 `github_api` / `github_file` / `read_page` 直接读官方仓库文件，仍属一手来源。

### Apache Doris

- **最新稳定版：4.1.5，发布 2026-10-15**（[release](https://github.com/apache/doris/releases/tag/4.1.5-release)）。
- **许可证：Apache-2.0**，官方 `LICENSE.txt` 正文即 Apache License Version 2.0 全文（另含第三方组件清单）（[LICENSE.txt](https://github.com/apache/doris/blob/master/LICENSE.txt)）。
- **官方 Docker 支持**：形态是**官方启动脚本而非官方 docker-compose**，Quick Start 路径为 `curl -fsSL https://doris.apache.org/files/start-doris.sh | bash`（[quick-start.mdx](https://github.com/apache/doris-website/blob/master/docs/getting-started/quick-start.mdx)）。仓库内 `docker/` 只是**开发/编译环境**（[docker/README.md](https://github.com/apache/doris/blob/master/docker/README.md)）。官方明确：**Docker 部署仅用于本地开发与测试，不要用于生产**，容器销毁即丢数据（同上 quick-start）。
- **单机最低资源要求：官方未给出内存/CPU 数字**。官方 `cluster-planning.md` 只给节点数与端口（生产建议：FE Follower ≥3、BE ≥3 副本），无内存数字（[cluster-planning.md](https://github.com/apache/doris-website/blob/master/docs/install/preparation/cluster-planning.md)）。Quick Start 只给 JDK 17+ 与操作系统要求。
- **能否直接读 Paimon：能**。官方有独立 **Paimon Catalog** 文档页，支持 `filesystem / hms / dlf / jdbc`（`jdbc` 自 4.1.0 起），**同时支持读与写回**（场景含 Query Acceleration、Data Integration、**Data Write-back**）（[paimon-catalog.mdx](https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/lakehouse/catalogs/paimon-catalog.mdx)）。**官方依赖的 Paimon 版本为 1.4.2**（4.1.4 之前为 1.3.1）（同上 "Supported Paimon Versions" 节）→ **与本项目锚定的 Paimon 2.1.0 存在版本落差，官方未声明支持 2.x**。
- **Flink-Doris-Connector**：官方仓库 `apache/doris-flink-connector`，最新 **26.3.0**（2026-09-21）（[release](https://github.com/apache/doris-flink-connector/releases/tag/26.3.0)）。**支持 Flink 2.2.x**：README 版本表列出 `2.2.x → JDK 17; JDK 21 (experimental)`（[README.md](https://github.com/apache/doris-flink-connector/blob/master/README.md)）。仓库 Apache-2.0。
- **Stream Load 官方要点**（[stream-load-manual.md](https://github.com/apache/doris-website/blob/master/docs/data-operate/import/import-way/stream-load-manual.md)）：HTTP 同步导入，客户端直连 FE 后由 FE 轮询重定向到 Coordinator BE；**保证单批原子性（全成功或全失败）**；建议单文件 <10 GB（BE `streaming_load_max_mb` 默认 10240 MB）；FE `stream_load_default_timeout_second` 默认 259200 s；通过 `label` 实现 **At-Most-Once**；支持 CSV/JSON/Parquet/ORC、`jsonpaths`、`merge_type: DELETE` + `function_column.sequence_col`、列映射与派生列、部分列更新；BE 需开 `enable_stream_load_record=true` 才能查历史作业。**两阶段提交（2PC）**用于 exactly-once（见下集成路径）。

### StarRocks

- **最新版本 tag 4.1.3**，release note 标注 **Release Date: July 14, 2026**（GitHub 页面 published 时间 2026-09-17）（[release](https://github.com/StarRocks/starrocks/releases/tag/4.1.3)）。
- **许可证：Apache-2.0，不是 Elastic License**。官方 `LICENSE.txt` 首行 `Copyright 2021-present StarRocks, Inc. All rights reserved.`，其后为 Apache License Version 2.0 全文（[LICENSE.txt](https://github.com/StarRocks/starrocks/blob/main/LICENSE.txt)）。源码可自由使用与再分发。**未发现「某版本后更换许可证」的官方声明**——据查到的官方一手 LICENSE 文件，Elastic License 的说法不成立。（仍建议面试前复核。）
- **Docker 支持与单机资源**：仓库 `docker/docker-compose/README.md` 与 `docker-compose.dev.yml` 均为**开发环境**（构建 FE/BE、跑单测），非生产部署编排（[docker-compose.dev.yml](https://github.com/StarRocks/starrocks/blob/main/docker-compose.dev.yml)）。官方 `plan_cluster.md` 给出的是**生产规划建议**：**每 FE 建议 8 核 + 16 GB RAM；每 BE/CN 建议 16 核 + 64 GB RAM**；FE 生产至少 3 Follower、BE 至少 3 个（[plan_cluster.md](https://github.com/StarRocks/starrocks/blob/main/docs/zh/deployment/preparation/plan_cluster.md)）。
- **能否直接读 Paimon：能，但为 Beta 且只读**。官方 Paimon catalog 页标注 `<Beta />`，**v3.1 起支持**，元数据侧支持 `filesystem` / `hive`，存储支持 filesystem、HDFS、S3、Azure、GCS、MinIO 等（[paimon_catalog.md](https://github.com/StarRocks/starrocks/blob/main/docs/en/data_source/catalog/paimon_catalog.md)）。官方明确：**Paimon catalog 只能查询，不能 drop/delete/insert**（同上 "Usage notes"）。支持的 Paimon 版本号：**官方未给出**。
- **Flink-Connector-StarRocks**：官方仓库 `StarRocks/starrocks-connector-for-apache-flink`，最新 **v1.2.16**（2026-09-24）（[release](https://github.com/StarRocks/starrocks-connector-for-apache-flink/releases/tag/v1.2.16)）。**关键约束：该 release 的 jar 只有 flink-1.16 / 1.17 / 1.18 / 1.19 / 1.20，没有 Flink 2.x 版本**——与 Flink 2.2.x 选型不兼容（同上 assets 列表）。

### ClickHouse

- **最新稳定版 v26.7.34.3-stable（2026-10-18）**（[release](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.7.34.3-stable)）。
- **许可证：Apache-2.0**（官方 `LICENSE` 首行 `Copyright 2016-2026 ClickHouse, Inc.`，正文为 Apache-2.0 全文）（[LICENSE](https://github.com/ClickHouse/ClickHouse/blob/master/LICENSE)）。
- **官方 Docker 镜像**：`clickhouse/clickhouse-server`，官方 README 给出 `docker run`、端口 8123（HTTP）/9000（native）、卷 `/var/lib/clickhouse/` 与 `/var/log/clickhouse-server/`、env `CLICKHOUSE_DB/USER/PASSWORD`，并提示 amd64 需 x86-64-v3（[docker/server/README.md](https://github.com/ClickHouse/ClickHouse/blob/master/docker/server/README.md)）。
- **单机最低资源建议**：官方 sizing 文档给出的最硬下限是 **「总量不应低于 8 GB」**（低数据量、1:1 内存/存储比场景），并给出内存:CPU 核比建议 **M 型 4 GB:1 核、R 型（数仓）8 GB:1 核、C 型 2 GB:1 核**；文档明确这些是通用建议而非「最低可行」（[sizing-and-hardware-recommendations.mdx](https://github.com/ClickHouse/ClickHouse/blob/master/docs/guides/oss/best-practices/sizing-and-hardware-recommendations.mdx)）。
- **能否读 Paimon / Iceberg**：**不支持 Paimon**；Iceberg 支持但明确标 **Beta**。官方 "Data catalogs" 页顶部即挂 `<BetaBadge/>`，收录 AWS Glue / Unity Catalog(Iceberg、Delta) / REST Catalog / Microsoft OneLake / Polaris / BigLake，底层通过 `DataLakeCatalog` database engine 读 Iceberg/Delta，且注明 "**ClickHouse only reads Iceberg tables from Glue**"；要求 **ClickHouse 25.12+**（Polaris/BigLake 26.2+），另有 "Tables may take up to 3 minutes to list" 的可用性提示（[data-catalogs.mdx](https://github.com/ClickHouse/ClickHouse/blob/master/docs/integrations/data-catalogs.mdx)）。**官方完全未提供 Paimon 支持**。
- **Flink 官方 connector：官方未给出**。已检 `docs/integrations/connectors/` 目录（子项为 data-ingestion / data-integrations / data-sources / data-visualization / sql-clients / tools），**未在其中检到官方 Flink 连接器页面**，故不能断言官方支持（[connectors 目录](https://github.com/ClickHouse/ClickHouse/tree/master/docs/integrations/connectors)）。

### 看板工具

- **Apache Superset**：最新 **6.1.0**（2026-05-13）（[release](https://github.com/apache/superset/releases/tag/6.1.0)）。**LICENSE.txt = Apache-2.0**（[LICENSE.txt](https://github.com/apache/superset/blob/master/LICENSE.txt)）。**官方 Docker 支持**：仓库有官方 `docker/` 目录（compose、entrypoint、初始化脚本）（[docker/](https://github.com/apache/superset/tree/master/docker)）。**官方数据库连接支持矩阵**：官方首页明确列出 **Apache Doris、ClickHouse、StarRocks** 三个端点，并说明可查询任何「有 Python DB-API 驱动 + SQLAlchemy dialect」的 SQL 数据源（[docs/docs/index.mdx](https://github.com/apache/superset/blob/master/docs/docs/index.mdx)）。**Superset 单机内存数字：官方未给出**（官网对应页 404）。
- **Grafana**：**LICENSE = GNU AGPLv3**（官方 LICENSE 正文即 AGPLv3 全文）（[LICENSE](https://github.com/grafana/grafana/blob/main/LICENSE)）。官方公告：**2021-04-20 起核心开源项目（Grafana/Loki/Tempo）从 Apache-2.0 迁至 AGPLv3**（[grafana.com/licensing](https://grafana.com/licensing/)）。AGPLv3 第 13 条要求：**若修改该程序并通过网络提供，必须向所有远程交互用户提供对应源码**——自用不改不分发无义务，但对外提供修改版服务即触发源码义务。**数据源插件**：ClickHouse 官方插件 `grafana/clickhouse-datasource` 为 Apache-2.0（[LICENSE](https://github.com/grafana/clickhouse-datasource/blob/main/LICENSE)）；**Doris/StarRocks 官方插件：官方未给出**。**单机资源**：官方安装页给出**最低建议 512 MB / 1 核**；Small 档 **2 核 + 2–4 GB**；Medium **4–8 核 + 8–16 GB**（[Install Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)）。
- **DataEase**：最新 **v3.1.0**（2026-09-24）；仓库 `pushed_at` 2026-10-18、24,615 stars、默认分支 `dev-v3`，活跃度良好（[release v3.1.0](https://github.com/dataease/dataease/releases/tag/v3.1.0)、[repo](https://github.com/dataease/dataease)）。**许可证：不是 Apache-2.0**——其 `LICENSE` 原文为「**modified version of GPLv3, with additional conditions**」：①不得移除/修改前端 LOGO 与版权信息；②贡献者同意厂商可随时收紧或放宽开源协议、贡献代码可被商用（含云业务）。GitHub license 字段为 `Other / NOASSERTION`（[LICENSE](https://github.com/dataease/dataease/blob/dev-v3/LICENSE)）。支持数据源：v3.1.0 release note 明确「数据引擎配置正式支持 **StarRocks**」，repo topics 含 `apache-doris`（同上）。**单机内存数字：官方未给出**。

### Paimon → OLAP 集成路径

- **External Catalog 直读**：Doris Paimon Catalog 支持**读 + 写回**（ZeroETL），官方依赖 **Paimon 1.4.2**（[paimon-catalog.mdx](https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/lakehouse/catalogs/paimon-catalog.mdx)）；StarRocks Paimon Catalog **Beta、只读**（v3.1+）（[paimon_catalog.md](https://github.com/StarRocks/starrocks/blob/main/docs/en/data_source/catalog/paimon_catalog.md)）；ClickHouse **无 Paimon 支持**（[data-catalogs.mdx](https://github.com/ClickHouse/ClickHouse/blob/master/docs/integrations/data-catalogs.mdx)）。
- **Flink connector 写入 OLAP**：Doris 路径**官方支持 Flink 2.2.x**，走 Stream Load 2PC、可 exactly-once（[README](https://github.com/apache/doris-flink-connector/blob/master/README.md)、[stream-load-manual.md](https://github.com/apache/doris-website/blob/master/docs/data-operate/import/import-way/stream-load-manual.md)）；StarRocks connector **无 Flink 2.x artifact**（[v1.2.16](https://github.com/StarRocks/starrocks-connector-for-apache-flink/releases/tag/v1.2.16)）；ClickHouse **官方未给出 Flink connector**。
- **省资源 vs 像生产**：直读 External Catalog **不额外占常驻内存、零搬运**，最省资源，但只读（StarRocks）或写回受 Doris 实现约束；Flink connector 写 OLAP **更接近生产形态**（有 2PC、可做分层物化与加速），代价是多一份 OLAP 侧存储与常驻内存。

### 16 GB 单机取舍结论（该路调研 agent 的判断）

- **最合适的 OLAP：Apache Doris 4.1.5**。理由：①许可证 Apache-2.0 干净；②**唯一同时满足「官方支持 Flink 2.2.x」+「官方 Paimon Catalog（可读可写）」**的候选；③官方明确 Docker 单容器可跑（**仅限开发测试**），且官方**未给出**会否决该部署的内存下限。
- **最合适的看板：Apache Superset 6.1.0**。Apache-2.0 + 官方 Docker + 官方支持 Doris/ClickHouse/StarRocks。DataEase 可作中文面试场景的**可替换选项**，但其修改版 GPLv3 附加条款不符合「仅宽松许可」约束。
- **明确排除**：
  - **StarRocks**：许可证干净，但**被 Flink 连接器版本排除**——官方 release 无 Flink 2.x artifact，与 Flink 2.2.x 选型冲突；且其 Paimon Catalog 只读 + Beta。其官方单机规划建议（每 BE 16 核/64 GB）远超预算（注意：这是**生产建议**，非官方最低可行值）。
  - **ClickHouse**：许可证干净、官方镜像成熟、单容器轻，但**无 Paimon 支持**、**官方未给出 Flink connector**；官方内存建议**不低于 8 GB**，单这一项即吃掉 8 GB 预算的全部。
  - **Grafana**：**许可证排除**（AGPLv3 第 13 条网络源码义务），只能作可替换选项；其内存本身不高（最低 512 MB）。

## 4. 数据来源与参考架构

### 公开电商用户行为数据集

- **阿里天池 UserBehavior（重点核实）**：官方页面为天池数据集 649 号《[User Behavior Data from Taobao for Recommendation](https://tianchi.aliyun.com/dataset/649)》，现状正常在线。① **规模与口径**——约 100 万用户在 2017-11-25 至 12-03 的行为，官方维度为 987,994 用户 / 4,162,024 商品 / 9,439 类目 / **100,150,807 条交互**，单文件 `UserBehavior.csv.zip`（905.8 MB，附 md5 校验）；字段 5 列：User ID、Item ID、Category ID、Behavior type（`pv`/`buy`/`cart`/`fav`）、timestamp。② **获取方式**——页面明确 "After Login → Download Datasets"，**需登录天池（阿里云账号）**，并标注 "Complete student verification to download without approval"（默认需审批、学生认证可免审）；**官方未给出任何免登录直链**。③ **许可**——License 节明确为 [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)（非商业 + 相同方式共享）。
- 其他候选（各一句）：
  - [RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)：Kaggle 上真实电商 4.5 个月数据（events.csv 含 view/addtocart/transaction，另有商品属性与类目树），需 Kaggle 账号，页标 CC 许可。
  - [天池 Ali Mobile 2015](https://tianchi.aliyun.com/dataset/46)：阿里移动电商真实用户-商品行为+位置数据，同样需登录天池。
  - [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii)：英国在线零售商 2009–2011 约 107 万行交易级数据，CC BY 4.0，**无需登录可直接下载**——但为订单/发票级，缺点击流行为。
  - [TAOBAO-MM](https://taobao-mm.github.io/)：约 9,900 万样本的淘宝多模态行为数据集，Hugging Face 可 CLI 下载，但 139 GB 体量不适合单机。

### 自研 mock 埋点生成器参考

- **事件 schema 一手来源**：Google 官方 GA4 电商事件参考（[Measure ecommerce](https://developers.google.com/analytics/devguides/collection/ga4/ecommerce)、[Recommended events](https://support.google.com/analytics/answer/9267735?hl=en)）定义 `view_item`、`add_to_cart`、`begin_checkout`、`purchase` 及 items 字段；Snowplow 官方文档给出完整 [iglu JSON Schema](https://docs.snowplow.io/docs/events/ootb-data/ecommerce-events/)（枚举 add_to_cart/remove_from_cart/product_view/checkout_step/transaction 等，schema 版本 1-0-2）；Twilio Segment 的 [Ecommerce V2 Spec](https://www.twilio.com/docs/segment/connections/spec/ecommerce/v2)（Product Viewed / Product Added / Order Completed 等命名）可作对照。
- **造数工具许可**：Python [Faker 为 MIT License](https://pypi.org/project/Faker/)；[Mockaroo](https://www.mockaroo.com/pricing) 免费版限每文件 1,000 行，大规模造数建议用 Faker。

### 参考架构（一手来源）

- **Flink 官方**：[apache/flink-playgrounds](https://github.com/apache/flink-playgrounds)（约 555 star，2026-09 仍有 push，Apache-2.0）内含 docker-compose 的 **Table Walkthrough**：Kafka → Flink Table API → MySQL → Grafana 实时看板端到端示例，配套 Flink 官网 "Real Time Reporting with the Table API"（Try Flink 章节）；这是最接近「实时数仓+看板」的官方现成 demo。
- **Paimon 官方**：[Flink Quick Start](https://paimon.apache.org/docs/master/flink/quick-start/) 提供本机 Flink SQL Client 建表/写流/流读最小示例，另有 [CDC Ingestion](https://paimon.apache.org/docs/master/cdc-ingestion/)（MySQL→Paimon 管道）；**官方未给出整条电商实时数仓端到端 demo**。
- **Kafka 官方**：[Quickstart](https://kafka.apache.org/quickstart/) 提供 Docker 镜像起步；**官方未给出电商专属 demo**。
- **同类高星开源项目（GitHub API 查证）**：[water8394/flink-recommandSystem-demo](https://github.com/water8394/flink-recommandSystem-demo)（4,475 star，1,474 fork，最后 push 2024-02，已不活跃）；[MrSuiChuan/data-warehouse-learning](https://github.com/MrSuiChuan/data-warehouse-learning)（1,261 star，2026-04 仍在 push，覆盖 Flink+Paimon/Doris/Hudi/Iceberg 离线+实时数仓，含 Java 造数模块写 MySQL/Kafka）与本项目目标最接近；[IronmanJay/UserBehaviorAnalysis](https://github.com/IronmanJay/UserBehaviorAnalysis)（145 star，2020 年后停更）。注：**`wh-yxh/` 用户在 GitHub 上不存在（API 返回 404）**。

### 三条数据来源路线的取舍（该路调研 agent 的判断）

- **公开数据集回放**：真实感强、1 亿条体量单机可承受（~3.4 GB 解压）；但需登录+审批/学生认证，**CC BY-NC-SA 的 NC+SA 条款使「原始数据进公开作品集仓库」有合规顾虑**，且 5 列字段做不出丰富 ADS 指标。
- **自研生成器**：字段自主（可对齐 GA4 并扩展金额/地域/渠道）、可控流速与大促尖峰、**无许可问题**；需参照 UserBehavior 官方漏斗形态设计分布。
- **混合（建议）**：以自研生成器为主链路（可复现、无许可风险），UserBehavior 作回放/对账素材但**不提交原始文件**、README 附官方链接与许可；面试既讲真实淘宝漏斗形态，又展示 schema 对齐 GA4/Segment 规范的设计能力。

## 5. 证据矩阵（候选 × 关键约束）

| 候选 | 版本（发布日） | 许可证 | 关键约束（有来源） | 与「Flink 2.2.x + Paimon」的相容性 |
| --- | --- | --- | --- | --- |
| Kafka | 4.3.1（2026-06-25） | Apache-2.0 | 4.0.0 起仅 KRaft、无 ZK；官方单容器镜像 | 无冲突 |
| Flink | 2.3.0 最新 / **2.2.x 为 Kafka connector 可用线** | Apache-2.0 | 官方：2.3 **无**已发布 Kafka connector；connector 5.0.0 覆盖 2.1.x/2.2.x | **须锚定 2.2.x** |
| Paimon | 2.1.0（2026-10-12） | Apache-2.0 | Apache TLP；Flink 为第一引擎；主键表 + changelog producer | 主选湖存储 |
| Iceberg | 1.12.0（2026-09-30） | Apache-2.0 | Flink/Spark 官方文档最对称、认知度最高 | 备选（流式 upsert 较弱） |
| Spark | 4.2.0（2026-07-14） | Apache-2.0 | 官方 Docker 镜像；微批延迟秒级 | 可选/按需 profile |
| Hive | 4.2.1（2026-08-24） | Apache-2.0 | 官方无「维护模式」声明；重心转向 Metastore + Iceberg | 仅作 Metastore |
| Hudi | 1.2.1（2026-09-24） | Apache-2.0 | release 节奏稳定；声势偏弱（综合判断） | 不作首选 |
| Doris | 4.1.5（2026-10-15） | Apache-2.0 | 官方 Paimon Catalog **依赖 Paimon 1.4.2**；官方 Docker 仅限开发测试；Flink connector 26.3.0 **支持 Flink 2.2.x** | **连接器相容；Catalog 与 Paimon 2.1 有版本落差** |
| StarRocks | 4.1.3（release note 2026-07-14） | Apache-2.0（**非 Elastic License**） | Flink connector v1.2.16 **无 Flink 2.x artifact**；Paimon Catalog Beta 只读；生产规划 64 GB/BE | **被连接器版本排除** |
| ClickHouse | 26.7.34.3（2026-10-18） | Apache-2.0 | **无 Paimon 支持**；官方 sizing「不低于 8 GB」；官方未给出 Flink connector | **被内存与 Paimon 支持排除** |
| Superset | 6.1.0（2026-05-13） | Apache-2.0 | 官方 Docker；官方支持 Doris/ClickHouse/StarRocks | 看板主选 |
| Grafana | — | **AGPLv3** | 2021-04-20 起迁至 AGPLv3；无官方 Doris/StarRocks 插件；最低 512 MB | 仅作可替换项 |
| DataEase | v3.1.0（2026-09-24） | **modified GPLv3 + 附加条件** | 不得移除 LOGO；厂商可变更协议 | 仅作可替换项 |
| Pulsar | 5.0.0 LTS（2026-10-05） | Apache-2.0 | 存算分离（BookKeeper） | 对照项，不作主线 |

## 6. 官方未给出清单（不得估算）

以下数据在本轮调研中**未在任何官方来源找到**，实现阶段需自行实测或另寻官方来源，**不得以估算值写入文档**：

- Kafka：单机最低资源要求；单机吞吐量级；分区数硬上限。
- Flink：单机内存最低要求（仅有 TM/JM 内存模型文档）；2.x 生产采用度统计。
- Spark/Hive/Paimon/Iceberg/Hudi：单机最低资源数字（本轮未检索）。
- Doris：单机最低内存与 CPU 数字。
- StarRocks：Paimon catalog 支持的 Paimon 版本号；单机最低内存（仅生产建议）。
- ClickHouse：官方 Flink connector（未检到）。
- Superset：单机内存要求（官网对应页 404）。
- DataEase：单机内存要求。
- Grafana：对 Doris/StarRocks 的官方数据源插件。
- WSL2：`memory` 的数值上限；能否超过宿主机物理内存及后果；Win10/Win11 默认值差异的官方说明。
- Docker Desktop：WSL2 后端下的 CPU/内存滑块（官方明确由 WSL 层配置）。

## 7. 调研方法与边界

- 5 路并行后台调研：①消息队列与流计算 ②离线批处理与湖存储 ③OLAP 引擎与看板 ④数据来源与参考架构 ⑤WSL2/Docker Desktop 资源上限。
- 来源优先级：官方文档 > 官方仓库文件（README / LICENSE / docs）> 官方公告 / release notes。二手博客与论坛内容仅出现于检索阶段，未被采信。
- 交叉核对：第 ⑤ 路经 MicrosoftDocs/wsl 仓库源文件与 docker/docs 仓库源文件互证；第 ③ 路因 `web_search` 不可用，改以 `github_api`/`github_file`/`read_page` 直读官方仓库文件。
- 标注纪律：凡官方未给出者一律写「官方未给出」；综合判断处明确标注「此句为综合判断，无单一官方来源可引」。
