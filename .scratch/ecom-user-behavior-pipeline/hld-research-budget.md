> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（资源预算与降级阶梯）｜ 上游：FSR §2/§8 R3、SRS §3.2.2、GDD §A.2 ｜ 状态：一手核查，未实测的已标注

# HLD 前置调研 · 8 GB 预算重算与 R3 降级阶梯裁决材料

**目标**：把 FSR §2 的「设计预算」换成一列**有官方出处（或明确「官方未给出」）**的数，并给 R3 的必要性一个可裁决的结论。
**边界**：只读。未安装、未拉镜像、未启停容器、未改配置、未提交 git；除本文件外未写任何路径。**不推翻任何冻结项**。

## 0. 只读边界与本机实测（2026-10-19）

| 项 | 实测值 | 命令 / 出处 |
| --- | --- | --- |
| Docker CLI / Compose | `29.8.0` / `v5.5.1` | `docker version` |
| **Docker daemon** | **未运行**——`npipe:////./pipe/dockerDesktopLinuxEngine` 不存在 | `docker info`（故镜像体积与 `docker stats` 本轮**无法实测**） |
| WSL MemTotal | **11962 MB**（≈11.68 GB）；`Swap 2048`；已用 585 MB | `wsl -e free -m` → A3/`REQ-NF-CAP-01` 的天花板数字**复现成立** |
| WSL nproc | **12** | `wsl -e nproc` |
| 磁盘 | `C:` **9.6 GB** 可用（92%）｜`F:` 78 GB｜`G:` 75 GB | `df -h` |
| Docker 数据盘 | `CustomWslDistroDir = F:\Docker Desktop\DockerDesktopWSL`；**`UseContainerdSnapshotter: true`** | `settings-store.json` 逐字 |
| WSL 发行版 | `IntegratedWslDistros: ["Ubuntu"]`；`C:\Users\Administrator\AppData\Local\wsl` 与 `…\Local\Docker` **存在** | 同上 + `ls`（**体积未测**） |
| **uv cache** | `%LOCALAPPDATA%\uv` = `C:\Users\Administrator\AppData\Local\uv` **存在**；`UV_CACHE_DIR` **未设** → 走 C 盘默认 | env + `ls` |
| 可达性 | `nightlies.apache.org` / `kafka.apache.org` / `doris.apache.org` / `api.github.com` `200`；**`hub.docker.com` / `registry-1.docker.io` / `raw.githubusercontent.com` 超时** | `curl` 探测 |

> ⚠️ **注册表不可达 ⇒ 镜像体积一手数字本轮一律「未取得」**，包含 Kafka 双镜像与 Doris 镜像。**不编数字。**
> ⚠️ **本轮 firecrawl 额度耗尽（429）**，多数官方页由 local 引擎直取——故部分页（Evidence MySQL 页）取不到正文，已单列不确定。

---

## 1. Flink 2.2：内存模型官方**默认值**（逐键，源码 `release-2.2`）

官方文档给出**键名**但**不给默认值**；默认值以发布分支源码为准（同为官方一手）。

| 键 | 官方默认 | 出处 |
| --- | --- | --- |
| `jobmanager.memory.process.size` | **无默认值** | [JobManagerOptions.java@release-2.2](https://github.com/apache/flink/blob/release-2.2/flink-core/src/main/java/org/apache/flink/configuration/JobManagerOptions.java) |
| `jobmanager.memory.flink.size` | **无默认值** | 同上 |
| `jobmanager.memory.heap.size` | **无默认值**；官方注「minimum **recommended** JVM Heap size is **128 MB**」（`MIN_JVM_HEAP_SIZE`） | 同上（常量 + 描述逐字） |
| `jobmanager.memory.off-heap.size` | **128 MB** | 同上 |
| `jobmanager.memory.jvm-metaspace.size` | **256 MB** | 同上 |
| `jobmanager.memory.jvm-overhead.min` / `.max` / `.fraction` | **192 MB** / **1 GB** / **0.1** | 同上 |
| `taskmanager.memory.process.size` / `.flink.size` | **均无默认值** | [TaskManagerOptions.java@release-2.2](https://github.com/apache/flink/blob/release-2.2/flink-core/src/main/java/org/apache/flink/configuration/TaskManagerOptions.java) |
| `taskmanager.memory.framework.heap.size` | **128 MB** | 同上 |
| `taskmanager.memory.framework.off-heap.size` | **128 MB** | 同上 |
| `taskmanager.memory.task.heap.size` | **无默认值**（由 Total Flink Memory 减去其余项**推导**） | 同上 |
| `taskmanager.memory.task.off-heap.size` | **0** | 同上 |
| `taskmanager.memory.managed.fraction` | **0.4**（占 Total Flink Memory） | 同上 |
| `taskmanager.memory.network.min` / `.max` / `.fraction` | **64 MB** / `Long.MAX_VALUE` / **0.1** | 同上 |
| `taskmanager.memory.jvm-metaspace.size` | **256 MB** | 同上 |
| `taskmanager.memory.jvm-overhead.min` / `.max` / `.fraction` | **192 MB** / **1 GB** / **0.1** | 同上 |
| `taskmanager.numberOfTaskSlots` | **1** | 同上 |

**官方硬规则（决定性）**：官方原文——「**One of the three mentioned ways has to be used to configure Flink's memory (except for local execution), or the Flink startup will fail.**」三选一子集（无默认值）为：`process.size` / `flink.size` / （`task.heap.size` + `managed.size`）——JM 侧为 `heap.size`。
→ **本仓必须显式写 `jobmanager.memory.process.size` 与 `taskmanager.memory.process.size`，否则容器起不来**；官方**不会**给一个"默认就能跑"的值。
**官方未给出**：Flink **单机最小可行内存**数字（全站无 quantity 表）。「非内存开销」有键但**无独立数字**：它就是 `jvm-overhead.*`（min 192 MB / fraction 0.1）与 `jvm-metaspace`（256 MB）、`framework.off-heap`（128 MB）。
出处：[Set up Flink's Process Memory](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/memory/mem_setup/)、[TaskManager](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/memory/mem_setup_tm/)、[JobManager](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/memory/mem_setup_jobmanager/)。
**脚手架**：`flink-dist/src/main/flink-bin/conf/` 下**没有 `flink-conf.yaml`**（只有 log4j/logback/masters/workers/zoo.cfg）→ 官方发行包**不含默认 flink-conf.yaml 值表**，配置必须自写。

## 2. Kafka 4.3.1 与 GraalVM 原生镜像

| 项 | 一手事实 | 出处 |
| --- | --- | --- |
| 单机内存指引 | **官方未给出堆设置，也未给 KRaft 单机建议**。硬体页给的**只有吞吐反推公式**：「compute your memory need as **write_throughput\*30**」+ 参考机「dual quad-core Xeon with **24GB**」 | [Hardware and OS 4.3](https://kafka.apache.org/43/operations/hardware-and-os/) |
| 官方 Docker 镜像 | 存在脚本与流程（KIP-975 = JVM 官方镜像） | [docker/README.md@4.3.1](https://github.com/apache/kafka/blob/4.3.1/docker/README.md) |
| **原生镜像** | **KIP-974 = GraalVM `native-image`**。官方定性原话：「can launch brokers with **sub-second startup time** and **minimal memory footprint**」；**但全篇无任何数字** | [docker/native/README.md@4.3.1](https://github.com/apache/kafka/blob/4.3.1/docker/native/README.md) |
| 原生的代价（官方自述） | ①「**experimental and intended for local development and testing purposes only; not recommended for production**」；②**GC 只有 `serial`**（社区版 GraalVM **不支持 G1**）；③**不支持运行时新增 jar**（需重编 native 二进制）；④reachability metadata **静态** | 同上 |
| 官方路线取舍 | 「**KIP-1028 only aims to release JVM based Docker Official Images and not GraalVM based native**」→ 原生镜像**不在 Docker Official Image 路线内** | [docker/README.md@4.3.1](https://github.com/apache/kafka/blob/4.3.1/docker/README.md) |
| 两者镜像体积 | **未取得**（registry 不可达） | — |

## 3. Doris 4.1.5：真正的锚点是 **`fe.conf` 里的 -Xmx8192m**

| 项 | 一手数字 | 出处 |
| --- | --- | --- |
| **FE 出厂默认 JVM 参数** | `JAVA_OPTS_FOR_JDK_17` **逐字含 `-Xmx8192m -Xms8192m`** | [conf/fe.conf@branch-4.1](https://github.com/apache/doris/blob/branch-4.1/conf/fe.conf) |
| `start_fe.sh` 行为 | 先 `export JAVA_OPTS="-Xmx1024m"`，随后**被 fe.conf 的 `JAVA_OPTS_FOR_JDK_17` 覆盖**；若该变量为空 → `exit 1`。**JDK 必须恰为 17**：非 17 打印 `ERROR: The jdk_version is N, must be 17.` 后 `exit 1` | [bin/start_fe.sh@branch-4.1](https://github.com/apache/doris/blob/branch-4.1/bin/start_fe.sh) |
| **BE 出厂默认 JVM 参数** | `JAVA_OPTS_FOR_JDK_17` 含 **`-Xmx2048m`**；`JEMALLOC_CONF` 含 `percpu_arena:percpu,background_thread:true,…`；端口 `be_port=9060 / webserver_port=8040 / heartbeat=9050 / brpc=8060 / arrow_flight_sql=8050` | [conf/be.conf@branch-4.1](https://github.com/apache/doris/blob/branch-4.1/conf/be.conf) |
| **BE `mem_limit`** | **默认 `90%`**（「Limit the percentage of the server's maximum memory used by the BE process」；必须 >0；>100% 视为 100%） | [BE Configuration（4.x，Last updated Oct 16 2026）](https://doris.apache.org/docs/4.x/admin-manual/config/be-config/) |
| BE compaction 内存相关默认 | `disable_auto_compaction=false`；`max_base_compaction_threads=4`；`vertical_compaction_max_row_source_memory_mb=200` | 同上 |
| 官方内存口径 | dev/test：**FE 8 GB + BE 16 GB**；prod：FE 16 GB / BE 核数×4 GB；**「Doris does not enforce a memory limit.」** | [env-checking.md（version-4.x）](https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/install/preparation/env-checking.md) |
| 官方「小内存/单机开发」最低配置示例 | **官方未给出**。官方 dev/test 档就是上表，**没有比它更低的档** | 同上 |
| 官方 Docker 部署 | `docker/runtime/` 是**用二进制包自建镜像**（`FROM apache/doris:base-latest`），**不是可直接 pull 的 4.1.5 镜像**；`start-doris.sh` 类脚本**本轮未取得**（该 README 更新于 2025-02-12，与 4.1.5 不同代） | [docker/runtime/README.md@branch-4.1](https://github.com/apache/doris/blob/branch-4.1/docker/runtime/README.md) |
| `start-doris.sh` 里的 `-Xmx`/`mem_limit` 默认 | **官方未给出**（未获取到该脚本；镜像内脚本非仓库文件） → 与 §未决 | — |

> ★ **与本仓预算的正面冲突（新增、比 §4.1 更硬）**：FSR 给 Doris 的预算是 **~3.0 GB**，而 **FE 一个进程的出厂默认堆就是 8192 MiB = 8.0 GiB**。**FE 默认堆单独一项即等于整个 8 GB 预算**，再加 metaspace/线程栈/jemalloc/文件缓存，FE 实测常驻必然 **>8 GB**。这不是「官方推荐值偏高」，是**发行版默认参数**（`fe.conf`）。
> ★ **`mem_limit` 默认 90%** 意味着 BE **默认按"机器最大内存的 90%"自我约束**：在 11.68 GB 天花板下即 **≈10.5 GB** ——与 FE 8192m 相加 ≈18.5 GB，**远超天花板**。故 `mem_limit` **必须显式下调**，否则天花板正面击穿（这正是 R3 要裁决的事）。

## 4. Superset 6.1.0 与 Paimon 2.1.0

| 项 | 一手事实 | 出处 |
| --- | --- | --- |
| gunicorn worker 数默认 | `--workers ${SERVER_WORKER_AMOUNT:-**1**}`、`--worker-class gthread`、`--threads **20**`、`--timeout **60**` | [docker/entrypoints/run-server.sh@6.1.0](https://github.com/apache/superset/blob/6.1.0/docker/entrypoints/run-server.sh) |
| Celery worker 并发默认 | `--concurrency=${CELERYD_CONCURRENCY:-**2**}`，注释逐字：「setting up only 2 workers by default **to contain memory usage in dev environments**」 | [docker/docker-bootstrap.sh@6.1.0](https://github.com/apache/superset/blob/6.1.0/docker/docker-bootstrap.sh) |
| 官方「最小可用」形态 | **有**：`docker-compose-light.yml` 头部逐字「This includes only **essential services: database and Superset app (no Redis)**」→ 真正必需 = **元数据库(PG) + superset app + 一次性 init** | [docker-compose-light.yml@6.1.0](https://github.com/apache/superset/blob/6.1.0/docker-compose-light.yml) |
| 官方全量 compose 跑了几个进程 | `docker-compose.yml` = db + redis + superset + superset-worker + superset-worker-beat + superset-init + superset-node(前端 dev)。**helm chart 默认**：`supersetNode` 1 副本 + `supersetWorker` 1 副本 + `supersetCeleryBeat.enabled=false` + `supersetWebsockets.enabled=false` + postgres + redis | [docker-compose.yml@6.1.0](https://github.com/apache/superset/blob/6.1.0/docker-compose.yml)、[helm/superset/values.yaml@6.1.0](https://github.com/apache/superset/blob/6.1.0/helm/superset/values.yaml) |
| 官方内存/资源指引 | **官方未给出**：`resources: {}` 并注明「We usually recommend **not** to specify default resources」；helm 注释里 `SERVER_WORKER_AMOUNT: 4` 是**注释示例**，非默认 | 同上 |
| Paimon Flink 侧内存相关 | `lookup.cache-max-memory-size` 官方默认 **256 mb**；`taskmanager.memory.managed.fraction`（Flink 侧 0.4）同时被 RocksDB/排序/缓存与 **Python UDF** 共享 → Paimon 的 lookup 缓存**与 Flink managed memory 争同一池** | [Paimon 2.1 配置页](https://paimon.apache.org/docs/2.1/maintenance/configurations/)（承接 `hld-research-datalayer.md` §2.3）+ Flink `TaskManagerOptions` |
| Paimon 磁盘侧职责边界 | `file://` 走 Built-in（无需 jar）；物理删除**只在快照过期时**发生；**官方未给出** Paimon 自身内存/磁盘数字 | 承接 `hld-research-datalayer.md` §2.5 |

## 5. 预算重算表（i）：**维持 ≤8 GB 的可行性判断**

**列义**：`官方给出` = 有官方 URL 的数字；`官方未给出` = 官方确无数字，本列是**设计值**（须显式标注，不得当官方）。

| 服务 | 预算（FSR §2，设计） | **官方数字（口径）** | 8 GB 内可行性 |
| --- | --- | --- | --- |
| **Doris FE** | *（与 BE 合计 3.0）* | ★**官方给出**：`fe.conf` 默认 **-Xmx8192m -Xms8192m**；dev/test 口径 **8 GB** | ❌ **不可能**。仅默认堆 8.0 GiB = 整个 8 GB 预算 |
| **Doris BE** | 同上 | ★**官方给出**：`be.conf` 默认 **-Xmx2048m**；`mem_limit` 默认 **90%**（→ 天花板下 ≈10.5 GB）；dev/test 口径 **16 GB** | ❌ 默认 `mem_limit` 即击穿；必须显式下调 |
| Flink JM | *（合计 2.0）* | **官方给出**（仅分项默认）：offheap 128 + metaspace 256 + overhead 192…1024；**`process.size` 无默认** | ⚠️ 可行**但必须自配**：官方明示不配则启动失败 |
| Flink TM | 同上 | **官方给出**（分项）：framework heap 128 + framework offheap 128 + task offheap 0 + managed 0.4 + network 0.1 + metaspace 256 + overhead 192…1024 | ⚠️ 同上；**并行度 ≤4**（SRS `REQ-F-RT-08`）与 slots 默认 1 需一致化 |
| Kafka（KRaft 单容器） | ~1.0 | **官方未给出**单机堆与 KRaft 建议；只有 `write_throughput*30` 公式 | ✅ 设计值可行；**官方无数字可引** |
| Superset | ~1.5 | **官方未给出**内存；**官方给出**进程形态：gunicorn workers **1**、Celery concurrency **2**、light 形态可**去 Redis** | ⚠️ 1.5 GB 仅在「去 Redis + 保留 1 个 worker」下成立；**官方未背书** |
| 生成器/初始化 | ~0.5 | 官方未给出（Python 生成器，非官方组件） | ✅ 设计值 |
| **合计** | **≈8.0** | **官方可引部分即已超额** | ❌ **缺口 = FE 默认堆与预算之差 ≈5 GB**（8.0 − 3.0）**+ BE `mem_limit` 未下调的风险** |

**裁决**：8 GB **在有官方依据的口径下不成立**——**唯一必须改的不是服务数，而是 Doris 的两个默认参数**（FE `-Xmx`、BE `mem_limit`）。这条把 R3 从「万一不够」改判为「**按默认必不够，必须调参**」。

## 6. 预算重算表（ii）：放开到 10–11 GB（天花板 11.68 GB 之内）

**前提**：Doris 两项必须显式下调（下表标 **设计值**）；Flink 两项必须显式自配。

| 服务 | 分配 | 具体键（必须显式写入） | 数字性质 |
| --- | --- | --- | --- |
| Doris FE | **2.0 GB** | `-Xmx1536m -Xms1536m`（改 `fe.conf` 的 `JAVA_OPTS_FOR_JDK_17`）→ 容器 `mem_limit: 2g` | **设计值**（官方 dev/test 是 8 GB；此处**低于官方下限**，须文档明写） |
| Doris BE | **2.5 GB** | `mem_limit = 25%`（替代默认 90%）→ 容器 `mem_limit: 2.5g` | **设计值**（官方默认 90% 被显式覆盖） |
| Flink JM | **1.0 GB** | `jobmanager.memory.process.size: 1024m` | **设计值**（官方无默认；分项默认合计已含在内） |
| Flink TM | **2.0 GB** | `taskmanager.memory.process.size: 2048m` + `taskmanager.numberOfTaskSlots` 与并行度 ≤4 对齐 | **设计值**（managed 0.4/network 0.1 分项为**官方默认**） |
| Kafka KRaft 单容器 | **1.0 GB** | 容器 `mem_limit: 1g` | **设计值**（官方无数字） |
| Superset web + init | **1.0 GB** | gunicorn `SERVER_WORKER_AMOUNT=1`（**官方默认**）、`mem_limit: 1g` | 进程形态=官方；内存=设计值 |
| Superset 元数据库（PG） | **0.4 GB** | 走 light 形态（**官方 compose 明示去 Redis**） | 形态=官方；内存=设计值 |
| Superset Celery worker / beat | **0.0 / 0.0** | **`CELERYD_CONCURRENCY=2`（官方默认）但进程数=0**：SRS 无告警/报表需求 → worker 可裁 | 裁撤依据=`SRS §6.1` 范围，非官方数字 |
| 生成器/脚本 | **0.5 GB** | — | 设计值 |
| **合计** | **≈10.4 GB** | 天花板 11.68 GB → **余量 ≈1.3 GB** | 余量用于 WSL2/Docker Desktop 自身与波动 |

> **注**：若 DWD/DWS 走 RocksDB，TM 的 managed memory（默认 0.4×Total Flink）是**状态真正落脚处**；`REQ-F-RT-05` 要求增量 checkpoint。1 分钟窗口 + 200 ev/s 的状态量级小，但**官方未给出「需要多少 managed memory」**，须 P1 用 `checkpointed_size`（可机读，见 `hld-research-observability.md` §2.1）实测回填。

## 7. R3 降级序列：替换「Superset→Grafana」这一级的候选（**只做事实核查，不推荐**）

**先记 GDD G4**：`apache/kafka-native:4.3.1` 是**官方存在**的（[native README](https://github.com/apache/kafka/blob/4.3.1/docker/native/README.md)），但**能否降内存只有定性说法**（"minimal memory footprint"），**无官方数字**；且被官方标为 **experimental / 非生产**、**GC 仅 serial**、**不在 Docker Official Image 路线**（KIP-1028）。→ **该级可作为"减少堆占用"的候选，但不能作为"有官方数字的降级"**；本轮**未实测**。

| 候选 | 许可证（SPDX，一手） | 内存/资源形态 | 常驻？ | 零账号只读 | 刷新下限 | 连 Doris（MySQL 协议） |
| --- | --- | --- | --- | --- | --- | --- |
| **Metabase** | **AGPL**（`LICENSE.txt` 逐字：「Outside of the top-level "enterprise" directory … licensed under the **AGPL**」；GitHub 标 `NOASSERTION`） | **JVM（Clojure）** + 自带元数据库 → 与 Grafana **同一许可证问题**，且为 JVM 栈 | 是 | 有「public link/static」共享能力（**本轮未逐字核对**） | 有缓存/定时刷新（**未核对具体下限**） | 走 JDBC/MySQL 驱动（**未核对**） |
| **Redash** | **BSD-2-Clause**（`LICENSE` 全文逐字核对，GitHub API `spdx_id: BSD-2-Clause`，**宽松**） | **Python + Celery + Redis + Postgres 元数据库** → 进程数**不少于** Superset，**不降内存** | 是（web + worker + scheduler） | 有公开 dashboard 链接（**未核对**） | 查询缓存 + 定时刷新（**未核对**） | 有 MySQL 数据源（**未核对**） |
| **Evidence.dev** | **MIT**（`LICENSE` 逐字「MIT License, Copyright (c) 2026 Evidence」）；官方口径：Core 是 **MIT-licensed framework** | **Node 构建期** → 产出**静态站**，可自托管 Docker/Vercel/Netlify。**不是常驻服务**（构建期取数） | **否**（构建期） | 静态站天然零账号（前提是自托管） | **构建/重建间隔**，**达不到 `REQ-F-BI-02` 的 ≤10 s 连续刷新**（结构性推论） | ⚠️ **未确认**：官方「Direct Connectors」列表含 Postgres/Snowflake/ClickHouse/BigQuery/Databricks/Cube/MotherDuck；**MySQL 页 404**（`/warehouse/mysql/`、`/warehouse/databases/` 均 404）→ **官方未确认 MySQL/Doris 支持** |
| **直接用 Superset 但降配** | **Apache-2.0**（`LICENSE.txt`@6.1.0） | 官方**已有**降配旋钮：`SERVER_WORKER_AMOUNT=1`（默认）、Celery `-c 2`（默认）、**light 形态去 Redis**、单元数据库 | 是（最少 2 个：app + PG） | Superset 有 public/guest role（`AUTH_ROLE_PUBLIC`）机制，但**需显式配置**；默认 init 只建管理员 → **默认非零账号**（`REQ-F-BI-03` 需配置才成立） | 面板级刷新可设秒级（**未逐字核对**） | **已验证**：Doris 前端 MySQL 协议兼容 + Superset 官方 MySQL 数据源 |

**核查小结（事实，不含推荐）**：四者中**许可证最宽的是 Evidence（MIT）与 Redash（BSD-2）**；Metabase 与 Grafana **同落 AGPL**；**只有 Superset 已被本仓证明能连 Doris 且整链已被冻结**。Evidence 的结构性问题是**构建期取数 ⇒ 刷新下限与 `REQ-F-BI-02` 不兼容**（本仓 R3 是内存降级，不是刷新降级）；Redash 的结构性问题是**进程数不低于 Superset ⇒ 降不了内存**。

## 8. 磁盘侧 20 GB（`REQ-NF-CAP-02`）

| 内容 | 官方/实测量级 | 落盘位置与裁决 |
| --- | --- | --- |
| Paimon `file://` warehouse | **官方未给出**量级；物理删除**只在快照过期时**发生（`hld-research-datalayer.md` §2.5）；`REQ-F-LAKE-08` 下限 5 快照 | **必须落 WSL Linux 文件系统**（`R6`：bind mount 走 `/mnt/c` 掉性能且 inotify 不投递）→ 即在 Docker Desktop WSL 数据盘内 → **该盘已在 `F:\Docker Desktop\DockerDesktopWSL`**（`settings-store.json` 逐字）→ **不占 C:** |
| Kafka 日志段 | `REQ-F-MQ-03`：**7 天或 2 GB 取小** ⇒ **≤2 GB** | named volume → WSL 数据盘 → F: |
| Doris 数据 | BE `storage_root_path` 默认 **`${DORIS_HOME}/storage`**；官方 dev/test 磁盘口径 **FE 10 GB+ / BE 50 GB+** ⇒ **官方下限 60 GB > 20 GB 预算** | 容器卷 → F:；⚠️ **官方磁盘下限与 20 GB 目标同样正面冲突**，须在文档标为"低于官方 dev/test 下限的设计值" |
| 镜像层 | **官方未给出**（registry 不可达，且 Doris **本就不发可直接 pull 的 4.1.5 镜像**，`docker/runtime` 是本地构建） | `CustomWslDistroDir` 已指 **F:**，且 **`UseContainerdSnapshotter: true`** → **默认不落 C:** |
| **必须改向 F:/G: 的东西** | ①**`uv` cache**：`%LOCALAPPDATA%\uv` 实测存在于 C 盘且 **`UV_CACHE_DIR` 未设** ⇒ **Python 依赖轮子默认落 C:**（若生成器/镜像内用 uv 装包，必须设 `UV_CACHE_DIR=F:\…`）②**WSL Ubuntu 发行版自身**：`IntegratedWslDistros: ["Ubuntu"]`，`C:\…\AppData\Local\wsl` 存在 → **该发行版的 ext4.vhdx 很可能在 C:**（**体积未测**；迁移= `wsl --export/--import` 到 F:，属 U 级动作，**本 agent 不执行**） | `C:` 现仅 **9.6 GB** 可用 |
| Docker Desktop 程序本体 | `C:\Program Files\Docker`（不可移，体积**未测**） | — |

**C 盘裁决**：**Docker 的容器数据与镜像层已不在 C 盘**（`CustomWslDistroDir=F:`）；**真正占 C 盘的是 `uv` cache 与用户自己的 Ubuntu 发行版**。若 20 GB 目标含镜像层，而镜像层已在 F:，则 **C 盘的 9.6 GB 不是本项目的 20 GB 预算对手**——但**必须显式设 `UV_CACHE_DIR`**，否则每次 Python 依赖安装都吃 C 盘。

## 9. 三轴（A 友好度 / B 资源 / C 兼容）

| 候选 | A. agent 友好度 | B. 本机资源（一手优先） | C. 既有接缝 |
| --- | --- | --- | --- |
| **Flink 2.2** | **高**：键名/默认值可逐字引（源码级）；`/jobs/overview`、`/checkpoints` 机读（obs 文档已核）；**但发行包无 `flink-conf.yaml`**、官方不给单机最小数 | **官方给出**分项默认；`process.size` **官方未给出（且不配则启动失败）** | ✅ 冻结链闭合（connector 5.0.0 / Flink-Doris 26.3.0） |
| **Kafka 4.3.1** | **中**：CLI 无 JSON 输出（obs 文档 U-d 已核）；docker/native README 完整 | **官方未给出**单机内存；仅吞吐公式。原生镜像缺点官方自述（experimental/serial GC） | ✅ `:9092`、KRaft、RF=1 均不冲突 |
| **Doris 4.1.5** | **中高**：配置项默认值成表（BE 页逐项）；`mem_limit`/`-Xmx` **可逐字引**；`/api/health` JSON 机读 | ⚠️ **官方给出且三重冲突**：FE 默认堆 8 GiB、BE `mem_limit` 90%、dev/test 8+16 GB、磁盘 10+50 GB | ⚠️ **JDK 必须恰为 17**（`start_fe.sh` 逐字 `must be 17`）→ 与本机无 JDK17 **正面阻塞** |
| **Superset 6.1.0** | **中高**：`/health` 纯文本 `OK`（obs 已核）；官方 compose + **light compose** + helm 三套脚手架齐全；gunicorn/Celery 默认值可引 | **官方未给出**内存；形态数字官方给出（workers 1 / celery 2 / light 去 Redis） | ✅ 端口绑 127.0.0.1；⚠️ 默认非零账号，`REQ-F-BI-03` 需配置 |
| **Paimon 2.1.0** | 高（承接 datalayer：键名/默认值齐全） | **官方未给出**；`lookup.cache-max-memory-size=256mb` 与 Flink managed memory 争池 | ✅ `paimon-flink-2.2-2.1.0.jar` 实际存在 |

---

## 未决 / 官方未给出 / 不确定

**A. 官方未给出（不得编数字）**
1. **Flink 单机最小可行内存**——官方未给出（只有分项默认值与"三选一必配否则启动失败"的规则）。
2. **Kafka 单机堆设置与 KRaft 单机建议**——官方未给出；只有 `write_throughput*30` 反推公式。
3. **Kafka 原生镜像的内存数字**——官方只有定性「minimal memory footprint」，**无数值**。
4. **Superset 最小内存/CPU**——官方未给出（helm `resources: {}` 明确建议不设默认）。
5. **Doris 的「小内存/单机开发」最低配置示例**——官方未给出；官方最低档即 dev/test（FE 8 GB + BE 16 GB）。
6. **Doris 官方 Docker 镜像内 `start-doris.sh` 的 `-Xmx`/`mem_limit` 默认值**——**未取得**（该脚本非仓库文件；`docker/runtime/README.md` 更新于 2025-02-12，与 4.1.5 不同代）。
7. **Paimon 自身内存/磁盘数字**——官方未给出（承接 datalayer §A）。
8. **Kafka 双镜像体积、Doris 镜像体积**——**registry 不可达**（`hub.docker.com`/`registry-1.docker.io` 超时），**本轮未取得**。

**B. 不确定 / 未实测（不得当事实使用）**
9. **★8 GB 预算的最终裁决未实测★**：本文件只证明「**按官方出厂默认参数必然超支**」。下调到 **(ii) 10.4 GB 分配表**后 FE 1536m + BE 25% 是否真能跑起 Doris 并供数，**必须 P0 用 `docker stats` 实测**；本 agent 未启任何容器。
10. **Doris 官方磁盘下限（FE 10 GB+ / BE 50 GB+）与 20 GB 预算的冲突未量化**——20 GB 是本仓设计值，**低于官方 dev/test 磁盘口径**。
11. **本机 WSL Ubuntu 发行版 ext4.vhdx 的实际位置与体积**——仅确认 `IntegratedWslDistros: ["Ubuntu"]` 与 `C:\…\AppData\Local\wsl` **目录存在**；**未测量**。
12. **`uv` cache 的体积**——`UV_CACHE_DIR` 未设、默认目录在 C 盘**已确认存在**，体积**未测**。
13. **Evidence.dev 是否支持 MySQL/Doris**——**官方未确认**：官方 Direct Connectors 列表无 MySQL，且 `/warehouse/mysql/`、`/warehouse/databases/` **均 404**；本轮 firecrawl 额度耗尽，**未能取得 MySQL 页正文**。
14. **Metabase 的零账号只读、刷新下限、Doris 连接方式**——**本轮未逐字核对**（只核了许可证）。
15. **Redash 的零账号只读、刷新下限、Doris 连接方式**——**同上未核对**；其许可证（BSD-2-Clause）与 GitHub 元数据（28.8k stars / pushed 2026-10-16 / BSD-2-Clause）已核。
16. **Superset `docker/.env` 是否覆盖 `SERVER_WORKER_AMOUNT`**——本轮只读了 `run-server.sh` 的**脚本默认 1** 与 helm 的**注释示例 4**，**未读 `.env`** → 实际默认 worker 数以 `.env` 为准，须实测。
17. **Compose v5.5.1 下 `REQ-F-ORCH-02` 的 `mem_limit` 与 `docker compose config` 的呈现**——**未核对**（daemon 未运行）。
18. **Doris FE 下调至 1536m 后是否满足 `REQ-F-OLAP-01..03`**——**未实测**；FE 堆过小可能触发 GC 抖动或元数据操作失败，官方无"最小可用堆"数字可引。
