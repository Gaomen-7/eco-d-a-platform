> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（观测性工具）｜ 上游：FSR/SRS/GDD 冻结项 ｜ 状态：一手核查，未实测的已标注

# HLD 前置调研 · 观测性工具候选

**目标**：让 GDD §C 失败路径登记表的「检测信号」列**可机读、可断言、可在 CI 判退出码**，同时不击穿 8 GB **天花板**与 SRS §6.1 范围边界。

## 0. 方法与只读边界（先说清我能证到什么）

| 项 | 状态 |
| --- | --- |
| 本轮动作 | 只读。未安装、未拉镜像、未启停容器、未改配置、未提交 git；除本文件外未写入任何路径 |
| **Docker 守护进程** | **未运行**——`docker images` / `docker ps -a` 均报 `npipe:////./pipe/dockerDesktopLinuxEngine` 连接失败。**故本机镜像清单与镜像体积的一手数字本轮无法取得**，一律标「未取得」 |
| 磁盘实测（`df -h`，只读） | `F:` 600 G / 已用 523 G / **可用 78 G**（88%）；`C:` 可用 9.6 G；`D:` 13 G；`E:` 37 G；`G:` 75 G；`H:` 483 M |
| 一手来源优先级 | 官方文档 / 官方源码 / 官方 release > 二手。本文件每条结论带 URL |
| 未执行的动作类建议 | 「落地动作」列只给出动作**类型**与回滚，**一律不由本 agent 执行**（本仓规矩：先经用户同意） |

## 1. 判据轴：「看」还是「判」

本仓已把「配置可见」「UI 里能看到」判为**不合格判据**（GDD §1.8、§1.10）。故新增一条二分轴：

| 轴 | 定义 | 合格性 |
| --- | --- | --- |
| **看** | 只有人眼可读的呈现（HTML 面板、UI 图表、无 schema 的自由文本） | **不合格**（不能进 `make verify`） |
| **判** | 进程可解析的**确切字段/退出码**：JSON 字段、Prometheus 文本格式指标名、纯文本定值、`docker stats --format json`、SQL 标量 | **合格**（可断言、可判退出码） |

**关键结论**：SRS §3.1.8 + GDD §1.8/§1.10 已经把「**判**」需求化（五项断言、`verify-fault`、lag 脚本超阈值退出码非 0）。所以**观测性里「判」的那一半不是新增范围，它就是 §3.1.8 本身**；§6.1 排除的是另一半（长期存储 + 告警 + 仪表盘）。

## 2. 候选逐项核查

### 2.1 基线（零新增常驻组件）

| 接口 | 确切路径 / 字段 | 格式 | 看/判 | 一手来源 |
| --- | --- | --- | --- | --- |
| Flink REST 根 | `rest.port` 默认 **8081**（与 dashboard 同端口、不同 URL） | JSON | **判** | [REST API 2.2](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/rest_api/) |
| Flink job 态 | `GET /jobs/overview` → `jobs[].state`，枚举 11 值含 `RUNNING`/`RESTARTING`/`FAILED` | JSON | **判** | 同上（逐字段核对） |
| Flink checkpoint | `GET /jobs/:jobid/checkpoints` → `counts{completed,failed,in_progress,restored,total}`、`history[].checkpointed_size` | JSON | **判** | 同上 |
| Flink 背压 | `GET /jobs/:jobid/vertices/:vertexid/backpressure` → `backpressure-level`（`ok`/`low`/`high`）、`subtasks[].busyRatio`/`idleRatio` | JSON | **判** | 同上。⚠️ 同一响应里**同时**出现 kebab 与 camelCase 两个别名（`backpressure-level` 与 `backpressureLevel`），解析时按哪个都行，但别假设只有一个 |
| Flink **水印** | `GET /jobs/:jobid/vertices/:vertexid/watermarks` | JSON | **判** | 端点清单取自官方文档；**响应字段结构本轮未逐字核对** |
| Flink 异常 | `GET /jobs/:jobid/exceptions` → 恢复期 `IllegalStateException` 可查（对应 §C F5） | JSON | **判** | 同上 |
| Flink 通用指标 | `/jobs/:jobid/metrics`、`/jobs/metrics`、`/jobmanager/metrics`、`/taskmanagers/:tm/metrics`；查询参数 `get`、`agg`（`min,max,sum,avg,skew`） | **响应 schema 在官方文档里写作 `{"type":"any"}`——即官方未给出字段结构** | 判（需先实测） | 同上 |
| Kafka lag | `kafka-consumer-groups.sh --describe --bootstrap-server ...` 表头（非 verbose）：`GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID`；`--verbose` 多 `LEADER-EPOCH`；缺值占位符固定为 `-` | **定宽纯文本，非 JSON** | 判（需自写解析） | [ConsumerGroupCommand.java](https://github.com/apache/kafka/blob/trunk/tools/src/main/java/org/apache/kafka/tools/consumer/group/ConsumerGroupCommand.java)（`printOffsets`） |
| Kafka 组清单 | `--list` 表头 `GROUP`，加 `--type`/`--state` 则多 `TYPE`/`STATE` | 纯文本 | 判（同上） | 同上（`printGroupInfo`） |
| Doris FE 健康 | `GET :8030/api/health` → `{"msg":"success","code":0,"data":{"online_backend_num":10,"total_backend_num":10},"count":0}` | **JSON** | **判** | [Health Action](https://doris.apache.org/docs/4.x/admin-manual/open-api/fe-http/health-action/)（逐字段核对） |
| Doris FE 指标 | `GET fe_host:fe_http_port/metrics`；另有 `GET /api/metrics?type=core\|json` | Prometheus 文本格式（`# HELP`/`# TYPE`/`name{label="v"} value`） | **判** | [Monitoring & Alerting](https://doris.apache.org/docs/4.x/admin-manual/maint-monitor/monitor-alert/)（给出真实样例输出）+ [Metrics Action](https://doris.apache.org/docs/4.x/admin-manual/open-api/fe-http/metrics-action/)。⚠️ 两页路径不同（`/metrics` 与 `/api/metrics`），且 Metrics Action 页的 Response 节写 **TO DO**——**官方未给出该页响应 schema** |
| Doris BE 指标 | `GET be_host:be_web_server_port/metrics`，BE `webserver_port` 默认 **8040**；FE `http_port` 默认 **8030** | Prometheus 文本 | **判** | 同上。Broker **官方明确不提供 `/metrics`** |
| Superset 健康 | `GET /health`（别名 `/healthcheck`、`/ping`）→ 返回字面量 `"OK"` | **纯文本，非 JSON** | **判**（精确等值匹配） | [health.py](https://github.com/apache/superset/blob/master/superset/views/health.py)（源码核对；GDD §1.8 的说法正确） |
| Superset 版本 | `GET /version` | JSON | 判 | 同上 |
| 容器内存 | `docker stats --no-stream --format "{{ json . }}"` → `.MemUsage`/`.MemPerc`/`.CPUPerc` | **JSON** | **判**（需自写阈值比较） | [docker container stats](https://docs.docker.com/reference/cli/docker/container/stats/) |
| 结构化日志 | 容器 stdout/stderr；日志轮转 10 MB × 3（SRS `REQ-F-ORCH-07`） | 文本 | 看（除非自带 JSON layout） | 本仓既有 |

> **基线小结**：§C 的 13 条失败信号里，**F1/F3/F5/F6/F8/F9/F10/F12/F13 共 9 条可用零新增常驻组件做机读断言**（F2/F4/F7 需一个 Kafka 侧机读 lag 源或自制计数器）。基线的全部代价是「自写解析 + 断言脚本」，**零常驻内存增量**（复用已开的 8081/8030/8040/8081 端口与已装的 CLI）。

### 2.2 Flink 官方 metric reporters（2.2 文档逐项）

配置键形式统一为 `metrics.reporters`（名单）+ `metrics.reporter.<name>.factory.class`（**必填**）。

| Reporter | `factory.class` | 确切配置键 | 类型 | 代价 / 与 §6.1 |
| --- | --- | --- | --- | --- |
| **Prometheus** | `org.apache.flink.metrics.prometheus.PrometheusReporterFactory` | `port`（**默认 9249**）、`filterLabelValueCharacters` | pull / tags | **拉模型，需另一个进程来抓** → 单独用等于空转；配合 Prometheus 才有意义（范围问题） |
| PrometheusPushGateway | `...PrometheusPushGatewayReporterFactory` | `hostUrl`、`jobName`、`randomJobNameSuffix`、`deleteOnShutdown`、`groupingKey`、`interval` | push / tags | 需 pushgateway 常驻组件 |
| JMX | `org.apache.flink.metrics.jmx.JMXReporterFactory` | `port`（建议写范围如 `9250-9260`） | pull / tags | 零容器，但消费端要另配 JMX exporter；入口是 JVM 本地/远程接口 |
| Slf4j | `org.apache.flink.metrics.slf4j.Slf4jReporterFactory` | 仅 `interval` | push / identifier | **零额外组件**，但落到日志行——机读性取决于日志 layout |
| InfluxDB | `org.apache.flink.metrics.influxdb.InfluxdbReporterFactory` | `scheme/host/port/db/username/password/retentionPolicy/consistency/connectTimeout/writeTimeout/interval` | push / tags | 需 InfluxDB 常驻（时序库）→ 范围 |
| StatsD | `org.apache.flink.metrics.statsd.StatsDReporterFactory` | `host`、`port` | push / identifier | 需 StatsD 常驻 |
| Graphite | `org.apache.flink.metrics.graphite.GraphiteReporterFactory` | `host`、`port`、`protocol`（TCP/UDP） | push / identifier | 需 Graphite 常驻 |
| Datadog | `org.apache.flink.metrics.datadog.DatadogHttpReporterFactory` | `apikey` 等 | push / tags | **需外部 SaaS 账号** → 直接违反**零账号** |
| OpenTelemetry | `org.apache.flink.metrics.otel.OpenTelemetryMetricReporterFactory` | `exporter.endpoint`、`exporter.protocol`（`gRPC`/`HTTP`） | push | 需 OTLP 后端 |

来源：[Metric Reporters (2.2)](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/metric_reporters/)，源码 [metric_reporters.md@release-2.2](https://github.com/apache/flink/blob/release-2.2/docs/content/docs/deployment/metric_reporters.md)。

**artifact 与打包（易踩）**

- groupId/artifactId：`org.apache.flink:flink-metrics-prometheus`；2.2 线最新 **2.2.1**（另有 2.2.0）；parent 版本写 `2.2-SNAPSHOT`（[pom.xml@release-2.2](https://github.com/apache/flink/blob/release-2.2/flink-metrics/flink-metrics-prometheus/pom.xml)）→ 含 shaded `io.prometheus:simpleclient* 0.8.1`。
- **打包位置**：[flink-dist/pom.xml@release-2.2](https://github.com/apache/flink/blob/release-2.2/flink-dist/pom.xml) 里 **8 个 metrics reporter 全部是 `<scope>provided</scope>`**，注释写明「packaged in `opt/`」。而文档原话是「All reporters documented on this page are available by default」且「The jar containing the reporter must be accessible when Flink is started. Reporters are loaded as plugins」。
  → **可执行的解读**：类在发行包里，但不在启动 classpath 上，**必须把它放进 Flink 的 plugin 目录**才生效。**官方 docker 镜像里的确切 plugin 目录路径本轮未取到一手原文**（列为不确定项 U-a）。
- **Flink 官方未给出**：任何 reporter 的内存/CPU 开销数字。标记为**官方未给出**，不得编数字。

### 2.3 Prometheus + Grafana

| 项 | 一手事实 | 来源 |
| --- | --- | --- |
| Prometheus 官方**最小内存/CPU** | **官方未给出**（installation 与 FAQ 页均无内存数字；storage 页只给磁盘公式） | [Storage](https://prometheus.io/docs/prometheus/latest/storage/)、[Installation](https://prometheus.io/docs/prometheus/latest/installation/) |
| Prometheus 磁盘公式（**官方**） | `needed_disk_space = retention_time_seconds × ingested_samples_per_second × bytes_per_sample`；官方原话 **「stores an average of only 1-2 bytes per sample」**；`retention.time` 默认 **15d**，`retention.size` 默认 **0/禁用** | 同 Storage 页 |
| Prometheus 磁盘余量建议（**官方**） | retention size 最多设为已分配磁盘的 **80–85%**（余量给 compaction 同时持有新旧 block） | 同上 |
| Prometheus 文件系统硬约束（**官方**） | **不支持非 POSIX 文件系统，NFS 不支持，否则可能不可恢复损坏**；强烈建议本地文件系统。WAL 为 **128 MB** 段、**至少保留 3 个** | 同上 |
| Grafana 许可证 | **AGPLv3 全文**（源码 LICENSE 逐字核对） | [grafana/LICENSE](https://github.com/grafana/grafana/blob/main/LICENSE) |
| AGPLv3 第 13 条边界 | 义务措辞为「**if you modify the Program**, your modified version must … offer … the Corresponding Source」——**自用、不改、不对外提供修改版 → 无义务**。FSR §3 的说法**成立** | 同上（第 13 条原文） |
| Grafana 健康端点 | `GET /api/health` → JSON `{"commit":…,"database":"ok","version":…}`（**比 Superset 的纯文本 OK 更机读**） | [Grafana HTTP API](https://grafana.com/docs/grafana/latest/developers/http_api/other/) |
| Grafana 官方 sizing | 最低 **512 MB / 1 核**；Small 档 **2 核 + 2–4 GB**；Medium **4–8 核 + 8–16 GB** | [Install Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/) |
| **告警引擎位置（决定性）** | 官方原话：**「In Grafana OSS, the alert engine runs in the same process as the UI and data source proxy」**——即**引入 Grafana 就是引入告警引擎** | 同上 |
| Doris 官方监控路线 | 官方**主动推荐** Prometheus + Grafana，且提供官方 dashboard JSON（`doris-grafana-dashboard.json`）与预留的 **Alertmanager** 配置位 | [Monitoring & Alerting](https://doris.apache.org/docs/4.x/admin-manual/maint-monitor/monitor-alert/) |

**§6.1 边界讨论（监控 vs 告警，分开谈）**

- **「监控」**：上游 Doris 官方文档**自己**把 Prometheus 定义为 *an open-source system **monitoring and alerting** suite*。→ 引入 Prometheus **就是**引入监控告警系统，**正面撞 SRS §6.1**。
- **「告警」**：`vmalert`、Alertmanager、Grafana Alerting 都是告警引擎；`make verify` 的**退出码**是**断言**不是告警——两者不能混为一谈。
- **结论**：`make verify` / `verify-fault` / lag 脚本（退出码非 0）**属于 §3.1.8，不受 §6.1 影响**；而 **Prometheus / Grafana / Loki / VictoriaMetrics / Alertmanager 作为常驻栈一律落在 §6.1 排除区**。Grafana 被排除现在有**两条独立理由**：①AGPLv3（FSR §3 既有）；②§6.1（新，且官方自证其 alert engine 内建同进程）。

### 2.4 VictoriaMetrics 单二进制

| 项 | 一手事实 | 来源 |
| --- | --- | --- |
| 形态 | 单二进制单节点版；默认 HTTP 端口 **8428**；自身 `/metrics` | [Single-node version](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/) |
| 无需 Grafana？ | **`/vmui` 内置 Basic Web UI**（可以看）；官方同时说「How to query VictoriaMetrics via Grafana」。**告警需 vmalert 或 Prometheus** | 同上 |
| 官方 vs Prometheus 对照 | 官方原话：在**百万级唯一时间序列（高基数）**下 *「uses 10x less RAM than InfluxDB and up to 7x less RAM than Prometheus, Thanos or Cortex」*、*「up to 7x less storage space」* | 同上。⚠️ 这是**特定高基数前提下的相对倍数**，不是绝对数字；本项目基数极小（用户池 10 万），**倍数不适用** |
| 官方 sizing 指引 | 原话：*「capacity scales linearly with the available resources… highly depends on the workload… recommended setting up a test VictoriaMetrics … iteratively scaling CPU and RAM」* + *「50% of free RAM for reducing the probability of OOM」* → **官方未给出最小内存数字** | 同上 |
| 官方 compose | 「Docker-compose helps to spin up VictoriaMetrics, **vmagent and Grafana** with one command」——**官方 compose 自带 Grafana**（=自带 §6.1 冲突） | 同上 |
| 许可证 | **本轮未核对** | 列入不确定项 |

**对照小结**：VM 在**资源上确实可能优于 Prometheus**，但「优于一个已被 §6.1 排除的东西」不构成采纳理由；且官方 compose 自带 Grafana。

### 2.5 Loki + Promtail + Dozzle

| 项 | 一手事实 | 来源 |
| --- | --- | --- |
| **Promtail** | **官方原话：end of life (EOL) as of March 2, 2026. Commercial support has ended. No future support or updates**；迁移目标 = **Grafana Alloy** | [Promtail agent](https://grafana.com/docs/loki/latest/send-data/promtail/) |
| Loki 官方 sizing 覆盖范围 | **官方明确「only documented for microservices/distributed mode at this time」**——**未给出单二进制/单机 sizing** | [Size the cluster](https://grafana.com/docs/loki/latest/setup/size/) |
| Loki 最小档官方数字 | 档位 `<3TB/day`：Ingester 6×（2 vCPU/4 Gi）、Distributor 4×（2/0.5）、Index gateway 4×（0.5/2）、Querier 10×（1/1）、Query-frontend 2×（1/2）、Query-scheduler 2×（1/0.5）、Compactor 1×（2/10）→ **合计 ≈26 vCPU / ≈49 GiB 内存** | 同上 |
| 结论 | 最小官方档**单内存一项 ≈49 GiB ≈ 8 GB 预算的 6 倍**；且单机单体内存**官方未给出** → **Loki 路线在官方一手数字层面即被排除**（排除理由与 §6.1 同向） | 同上 |
| **Dozzle** | 只读容器日志**实时**查看，**不存储日志**；容器 **约 7 MB（compressed）**、默认镜像 `FROM scratch`；需挂载 `/var/run/docker.sock`；要求 Docker Engine 19.03+/API 1.40+；MIT | [dozzle README](https://github.com/amir20/dozzle) |
| Dozzle 内存 | **官方未给出**（README 仅定性说 "Small memory footprint"） | 同上 |
| Dozzle 两个必须记的副作用 | ①挂 `docker.sock` = **等价宿主机 root 的权限面**（安全边界，与 `REQ-NF-SEC-01` 的「不暴露局域网」无关但同等要紧）；②README 自述**默认向 Google Analytics 上报匿名配置**，需 `--no-analytics` 关闭 | 同上 |
| Dozzle 看/判 | **只能「看」**：官方未给出任何可断言的健康/指标端点 | 同上 |

### 2.6 OpenTelemetry Collector

| 项 | 一手事实 | 来源 |
| --- | --- | --- |
| 定位 | 官方：*"receive, process and export telemetry data"*，目标是**「removes the need to run, operate and maintain multiple agents/collectors」**；单二进制可作 agent 或 collector | [opentelemetry-collector README](https://github.com/open-telemetry/opentelemetry-collector) |
| 官方最小内存 | **官方未给出**（README 无资源数字；docs 页为导航页） | 同上 |
| 对本规模的判断 | 它是**「管道/搬运」而非「判据源」**：本身不产生断言，必须再接一个后端（Prometheus/Loki/VM）才能落地 → **接入后必然连带 §6.1 冲突的常驻栈**。本项目端点只有 4 个且**全都已经能直接读**，Collector 无消费者 | 结构性推论（非官方结论） |
| 许可证 | **本轮未核对** | 列入不确定项 |

### 2.7 Kafka / Doris exporter 生态

| 项 | 一手事实 | 来源 |
| --- | --- | --- |
| Kafka 官方有 Prometheus exporter 吗？ | **没有**。Apache Kafka 官方站点未提供 prometheus exporter；文档中的 JMX 路线指向**独立项目** `prometheus/jmx_exporter`（本页未逐字核对） | [kafka_exporter README](https://github.com/danielqsj/kafka_exporter)（其自述「For other metrics from Kafka, have a look at the JMX exporter」） |
| 事实标准 | `danielqsj/kafka_exporter`：Go、**Apache-2.0**、默认监听 `:9308`、telemetry path `/metrics`；兼容 Kafka **0.10.1.0+** | 同上 + GitHub API 元数据 |
| 维护状态（GitHub API，2026-10-19 读取） | `created_at` 2017-09-15；`pushed_at` **2026-10-16**；`updated_at` 2026-10-17；**not archived / disabled=false**；**stars 2540 / forks 692 / open_issues 268 / subscribers 32**；最新 release badge **1.10.0** | `GET /repos/danielqsj/kafka_exporter` |
| **fingerprint 可信度** | **不是 ASF 项目、单一维护者、268 个未关闭 issue** → 「事实标准但非官方」。采纳即接受该单点维护风险 | 同上 |
| 它能给的机读字段（**正好覆盖 §C F4/F7**） | `kafka_consumergroup_lag{consumergroup,partition,topic}`、`kafka_consumergroup_lag_sum{consumergroup,topic}`、`kafka_consumergroup_current_offset`、`kafka_consumergroup_members`、`kafka_brokers`、`kafka_topic_partition_current_offset`、`kafka_topic_partition_oldest_offset`、`kafka_topic_partition_under_replicated_partition` 等 | 同上（指标名逐条核对） |
| 已知行为坑（**官方自述**） | Kafka **3.8+** 才依赖 `ListGroups` 返回的 group type 来选 `ConsumerGroupDescribe`/`DescribeGroups`；**Kafka 3.7 的 KIP-848 不支持**，且必须把 `kafka.version` 设成 broker 版本。⚠️ 本项目 Kafka **4.3.1** ⇒ 落在支持侧，但**必须显式设 `kafka.version`** | 同上 |
| 资源占用 | **官方未给出**内存/镜像体积 | 同上 |
| Doris 的 Prometheus exporter | **官方未给出独立 exporter**。Doris **自身** FE/BE 就暴露 `/metrics`（见 2.1）→ **不需要 exporter** | [Monitoring & Alerting](https://doris.apache.org/docs/4.x/admin-manual/maint-monitor/monitor-alert/) |

## 3. §C 检测信号 → 可机读断言映射（本表是本次调研的主要产出）

| §C | 失败模式 | 现状检测信号 | 可机读断言 | 需新增常驻组件？ |
| --- | --- | --- | --- | --- |
| F1 | TM 被 kill | UI 态 / REST `state` | `GET /jobs/overview` → `jobs[].state ∈ {RUNNING,RESTARTING}`；`/jobs/:jobid/checkpoints` → `counts.failed`/`counts.restored` 增量 | **否**（Flink 自带 8081） |
| F2 | 重复涌入 | 重复通道计数 | 需**作业自己发一个 counter**（`DuplicateCounter`），经 `/jobs/:jobid/metrics` 或 Prometheus reporter 读出 | 否（用内建 metrics），但需**写代码** |
| F3 | 迟到超期 | 迟到计数表 | Doris SQL：`SELECT COUNT(*) FROM …late…` | **否**（Doris SQL） |
| F4 | 脏数据涌入 | DLQ topic 增长 + 脏率面板 | ✅ 首选 `/metrics` 的 `kafka_topic_partition_current_offset{topic="ecom.behavior.event.v1.dlq"}`；备选 `kafka-consumer-groups --describe` 的 `LAG` 列 | 若走 `/metrics` 需 kafka_exporter；若走 CLI 则**否** |
| F5 | schema 演进致恢复失败 | `IllegalStateException` | `GET /jobs/:jobid/exceptions` | **否** |
| F6 | 分区空闲致**水印**停滞 | 窗口不关闭 | `GET /jobs/:jobid/vertices/:vertexid/watermarks` | **否** |
| F7 | offset 过期 | `auto.offset.reset` 触发 | `kafka_topic_partition_oldest_offset` vs `kafka_topic_partition_current_offset`（exporter）；CLI `--describe` **只给 LOG-END-OFFSET，不给 oldest** | 走 exporter **是**；走 CLI **否但不足** |
| F8 | Doris 容器重启丢数据 | 查询空表 | `GET :8030/api/health` 的 `online_backend_num == total_backend_num` + `SELECT COUNT(*)` | **否** |
| F9 | OOM kill | `docker stats` 超预算 | `docker stats --no-stream --format "{{ json . }}"` → 解析 `MemUsage` 求和 vs 8 GB | **否** |
| F10 | 磁盘击穿 20 GB | 卷使用率 | `df` / Doris 自身磁盘指标 | **否** |
| F11 | Paimon 小文件膨胀 | 文件数/快照数 | Paimon system table 计数（`REQ-F-LAKE-08` 快照 ≤5） | **否**（走 Paimon 查询） |
| F12 | 规则①误判 | 拒绝表 100% 命中 | `SELECT COUNT(*) FROM dwd_behavior_event_reject` 与摄入数之比 | **否** |
| F13 | 漏斗状态无限增长 | 状态大小单调上升 | ✅ `/jobs/:jobid/checkpoints` → `history[].checkpointed_size` 作为**状态大小的机读代理** | **否** |

> **这张表的意义**：13 条里有 **10 条**在零新增常驻组件下即可判；只有 **F4/F7 的「更机读」版本**才需要 kafka_exporter（一个额外容器）。

## 4. 资源预算重算

| 项 | 数字 | 性质 |
| --- | --- | --- |
| 既有常驻预算（FSR §2） | **≈8.0 GB**，且**已逐项排满**（Kafka 1.0 + Flink 2.0 + Doris 3.0 + Superset 1.5 + 其余 0.5） | 设计目标 |
| **天花板**（U1 后实测） | **11.68 GB** → 余量 **≈3.7 GB** | 实测 |
| 新增：Flink Prometheus reporter | **官方未给出**；它是**进程内**插件，无新增容器 | 官方未给出 |
| 新增：kafka_exporter 容器 | **官方未给出**（内存与镜像体积均无官方数字）；本轮 daemon 未运行，**无法实测** | 官方未给出 |
| 新增：Prometheus | **官方未给出最小内存**；磁盘按官方公式算（本项目指标量极小，1–2 B/sample ⇒ 即便 15d 保留也远小于 20 GB 磁盘目标）；**WAL 最少 3×128 MB 起** | 官方未给出（内存） |
| 新增：Grafana | **512 MB（官方最低推荐）**，官方 Small 档建议 2–4 GB | 官方数字 |
| 新增：VictoriaMetrics | **官方未给出**；官方要求「留 50% 空闲 RAM」 | 官方未给出 |
| 新增：Loki 单机 | **官方未给出**（官方只有分布式最小档 ≈49 GiB） | 官方未给出 |
| 新增：Alloy（Promtail 替代） | **官方未给出** | 官方未给出 |
| 新增：Dozzle | **官方未给出**；镜像 **约 7 MB** | 镜像有官方数字，内存无 |
| 新增：OTel Collector | **官方未给出** | 官方未给出 |

**预算结论**：8.0 GB 已排满 → **任何新增常驻组件都必须同时做两件事**：①列入 U 级动作（用户单独授权）；②给出 `mem_limit` 与降级序列。天花板 11.68 GB 支持「**最多再塞一个轻量组件**」（如 Grafana 512 MB / exporter 层），**不支持 Loki（≈49 GiB）也不支持 Prometheus+VM+Grafana+Loki 的组合**。

## 5. 兼容性轴（C）汇总

| 候选 | 接进 §C 检测信号列？ | 升级「配置可见」类判据？ | 零账号 | 端口（仅 127.0.0.1） | ≤8 GB | §6.1 | Makefile 目标名 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 基线（REST/CLI/SQL/health/docker stats） | ✅ 10/13 | ✅ 把 `state`/`counts`/`online_backend_num`/`checkpointed_size` 变成断言 | ✅ | 复用既有 8081/8030/8040/9092 | ✅ 零增量 | ✅ 属 §3.1.8 | `verify` / `verify-fault` / `logs` |
| Flink Prometheus reporter | ✅ | ✅ | ✅ | 需新增端口（默认 9249，多实例用范围 9250-9260） | 官方未给出 | ⚠️ 单独用=空转；仅在有抓取端时才有意义 | 无新增（配置项） |
| Prometheus | ✅ | ✅ | ✅ | 新增 9090 | 官方未给出 | ❌ 官方自定位 monitoring+alerting suite | — |
| Grafana | 仅自身健康 | — | ✅（自用） | 新增 3000 | 512 MB 起 | ❌（alert engine 内建同进程） | — |
| VictoriaMetrics | ✅ | ✅ | ✅ | 新增 8428 | 官方未给出 | ⚠️ 本体是时序库；官方 compose 带 Grafana | — |
| Loki + Promtail | — | — | ✅ | 新增 3100 | ❌ 官方最小档 ≈49 GiB | ❌ | — |
| Dozzle | ❌ 只能看 | ❌ | ✅ | 新增 8080 | 官方未给出 | ⚠️ 偏「运维便利」而非断言 | `logs` 的可视化替代 |
| OTel Collector | ❌ 本体不产出断言 | ❌ | ✅ | 新增（OTLP） | 官方未给出 | ❌ 必然连带后端 | — |
| kafka_exporter | ✅ F4/F7 | ✅ | ✅ | 新增 9308 | 官方未给出 | ⚠️ 是 exporter，非告警；但**只有配合抓取端才有意义** | `verify`（可直接 curl `/metrics` 断言，**无需 Prometheus**） |

> **一个被低估的选项**：kafka_exporter 的 `/metrics` 是 **Prometheus 文本格式**，`make verify` 可以直接 `curl` 它并 grep `kafka_consumergroup_lag_sum{…}` **判退出码**——**不需要 Prometheus 常驻**。这让 F4/F7 在不引入任何 §6.1 组件的前提下变机读。

## 6. 落地动作（**本 agent 一律不执行**）

| 候选 | 动作类型 | 具体动作 | 回滚 |
| --- | --- | --- | --- |
| 基线全部 | **无安装动作** | 只写脚本（`make verify` 的解析与断言） | 删脚本 |
| Flink Prometheus reporter | 配置 + 放置 jar | 把 `flink-metrics-prometheus-2.2.x.jar` 放到 Flink plugin 目录；加 `metrics.reporter.prom.factory.class`（+ `port`） | 删 jar + 删配置行 |
| kafka_exporter | 拉镜像 + 加 compose 服务 | 固定 tag（**禁用 `latest`**，符合 `REQ-NF-MNT-01` 锚点）、`mem_limit`、端口绑 `127.0.0.1` | 删 compose 服务段 + `docker image rm` |
| Prometheus / Grafana / VM / Loki / OTel | 拉镜像 + compose + 卷 | **建议不采纳**（§6.1） | — |
| Dozzle | 拉镜像 + 挂 `docker.sock` | 需先决议安全边界与 `--no-analytics` | 删服务 + 撤销 socket 挂载 |

> 所有动作**必须先经用户同意**（本仓规矩，SRS §6.2 的 U 级流程）。本文件只登记，不执行。

## 7. 三条最影响架构决策的发现（摘要）

1. **§3.1.8 与 §6.1 不冲突，而是互补**：13 条失败信号里 **10 条可零新增常驻组件机读断言**（Flink 6 个 REST 端点、Doris JSON 健康、Superset 纯文本 OK、`docker stats --format json`、SQL 标量）。**不加任何观测容器即可满足「可机读、可断言、可判退出码」**——新增常驻组件不是达标的前提。
2. **Loki/Promtail 路线在一手层面已被否**：Promtail **官方 EOL（2026-03-02）**，迁移目标 Alloy；Loki 官方 sizing **只覆盖分布式模式**，最小档合计 **≈49 GiB 内存**（>8 GB 预算 6 倍），单机单体内存**官方未给出**。
3. **R3 降级序列里的「Superset→Grafana」与 §6.1 正面冲突（不只是许可证）**：Grafana 官方自证 OSS 版**告警引擎与 UI/数据源代理同进程**——引入 Grafana 即引入告警引擎；而 Prometheus 被 Doris 官方文档定义为 *monitoring **and alerting** suite*。二者都落在 §6.1 排除区，使 R3 需改写降级路径。

## 未决 / 官方未给出 / 不确定

**官方未给出（不得编数字）**
1. Prometheus 官方**未给出**最小内存/CPU（只有磁盘公式与 1–2 B/sample）。
2. VictoriaMetrics 官方**未给出**最小内存；只有「按资源线性扩展、自行迭代实测」+「留 50% 空闲 RAM」。
3. Loki **官方未给出**单机/单体 sizing 与 Promtail/Alloy 的内存数字；唯一数字是分布式最小档 ≈49 GiB。
4. Dozzle 官方**未给出**内存数字（镜像 7 MB 有官方数字）。
5. OTel Collector 官方**未给出**最小内存。
6. Flink 官方**未给出**任何 metric reporter 的内存/CPU 开销。
7. kafka_exporter 官方**未给出**内存与镜像体积。
8. Doris 官方**未给出** FE/BE 单机最低内存（与 SRS A2 一致）。
9. 本机 `docker images` / `docker ps -a` 因 **daemon 未运行**而失败 → 镜像清单与体积的一手数字**本轮未取得**。

**未逐字核对 / 不确定（已在本文件标 ⚠️）**
- **U-a**：Flink 官方 docker 镜像内 metric reporter 的确切 **plugin 目录路径**（`plugins/` 还是 `opt/`+`lib/`）——文档只说「loaded as plugins」，未取到镜像内路径原文。**这是「加 Prometheus reporter」动作的第一步，必须实测确认。**
- **U-b**：`GET /jobs/:jobid/vertices/:vertexid/watermarks` 的**响应字段结构**（端点存在已确认，字段未核对）；`/jobs/:jobid/metrics` 等指标端点的响应 schema 官方写作 `{"type":"any"}` = **官方未给出结构**。
- **U-c**：Doris FE 指标路径 `/metrics` 与 `/api/metrics` 的**并存关系**（Monitoring 页用前者、Metrics Action 页用后者，且后者 Response 节写 TO DO）。
- **U-d**：Kafka CLI `--describe` **无官方 JSON 输出模式**（已核对源码；若需要 JSON 只能自写解析或用 exporter）。
- **U-e**：`kafka-get-offsets.sh --time earliest` 之类取得 **oldest offset** 的官方 CLI 方式（F7 用），本轮未核对。
- **U-f**：**VictoriaMetrics 与 OTel Collector 的许可证**本轮未核对（只核对了 Grafana AGPLv3、Dozzle MIT、kafka_exporter Apache-2.0）。
- **U-g**：Prometheus HTTP API 的具体端点（如 `/-/healthy`、`/api/v1/query`）本轮未逐字核对；官方提供 JSON 查询 API 这一点未在本文件断言。
- **U-h**：`prometheus/jmx_exporter` 作为 Kafka/Doris JVM 指标路线的官方性与维护状态，本轮未核对。
- **U-i**：以上候选**全部未在本机实测**（daemon 未运行，且本 agent 按硬规则不得拉镜像/启容器）——**表中每个「官方未给出」都必须在实现阶段以实测填数**（对齐 SRS A2 与 FSR R4）。
