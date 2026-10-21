# SRS — 电商用户行为数据全链路（离线 + 实时）

> **性质**：软件需求规格说明书（Software Requirements Specification），结构按 **ISO/IEC/IEEE 29148 裁剪版**。
> **主读者**：实现 agent（照此施工）。次要读者：评审者/面试官、未来的你自己。
> **日期**：2026-10-18（Asia/Shanghai）
> **关系**：本文件落在 issue-tracker 约定的 feature 目录内（`.scratch/<feature-slug>/`），但**文件名采用 `SRS.md`**，不使用 `spec.md`。
> **上游**：`FSR.md` 冻结了「选哪条路线、为什么」；本文件只声明「系统必须做到什么」。**选型论证不再复制**——两份文档靠 §5 追溯矩阵咬合。

## 0. 这份文档怎么用

| 你要做的事 | 读哪节 |
| --- | --- |
| 开始施工，确认要建哪些模块 | §3.1 功能需求（8 主模块 + 3 加强项） |
| 写生成器 / 清洗 DWD，需要字段口径 | §3.3 数据需求（事件 schema 字段表） |
| 定 topic、库表、端口、Makefile 目标名 | §3.4 接口与命名 |
| 确认性能/容量/可靠性门槛 | §3.2 非功能需求 |
| 判定「做到没有」 | §4 验证与验收 |
| 想知道某条需求从哪个决策来 | §5 追溯矩阵 |
| 确认哪些已明确不做 | §6 范围边界与未决项 |

两条贯穿文档的锚定词（与 `FSR.md` 共用同一套语言，读到即想起约束）：

- **天花板**：本机 WSL2 给容器群的可用内存上限（≈8 GB）。任何新增组件或副本，先问它吃多少天花板。
- **锚点**：每个组件的精确版本号。**锚点不漂移**即可复现。

**需求优先级图例**：`MUST`（P0/P1 必须成立，缺则最小闭环不成立）｜`SHOULD`（P1 尾声/P2，成本低价值高）｜`COULD`（P2 可选，不阻塞）。

---

## 1. 引言

### 1.1 目的

定义这条电商用户行为数据全链路（离线 + 实时）**必须实现的功能、非功能门槛、接口与数据口径**，使实现者可照此施工、验收者可据此判定完成与否。

### 1.2 范围

单机 Docker Compose 形式交付的端到端链路：行为事件生成 → Kafka → Flink 实时计算 → Doris 供数 + Superset 看板；同一批数据经 Flink 批模式落 Paimon 五层离线数仓。**不含**多节点集群、K8s、调度器、监控告警体系。

### 1.3 术语与缩写

| 术语 | 含义 |
| --- | --- |
| **天花板** | 本机 WSL2 给的容器群可用内存上限（≈8 GB） |
| **锚点** | 组件的精确 pin 版本；可复现性的核心 |
| **超集 schema** | 一套覆盖自研生成器与公开数据集的事件字段定义，使两路数据可落同一张 DWD 对账 |
| **五层** | ODS / DWD / DIM / DWS / ADS |
| **DLQ** | Dead Letter Queue，脏数据侧输出落点 |
| **水印** | Watermark，事件时间进度标记 |
| **侧输出** | Side output，Flink 中把迟到/违规数据分流到独立流的机制 |
| **2PC** | Two-Phase Commit，两阶段提交，端到端 exactly-once 的 sink 侧机制 |
| **烟测** | Smoke test，`make verify` 的一组最小断言 |
| **零账号** | 评审者不需要任何外部平台账号即可看到结果 |

### 1.4 引用文件

| 文件 | 作用 |
| --- | --- |
| `FSR.md`（同目录） | 路线冻结、候选排除证据、版本锚点矩阵、风险 R1–R8、里程碑 P0–P3 |
| `research-notes.md`（同目录） | 每条技术结论的一手来源 URL；含「官方未给出清单」 |
| `interview-prep.md`（同目录） | 面试问答提纲（非施工依据） |
| `AGENTS.md` / `docs/agents/issue-tracker.md` / `docs/agents/domain.md` | 仓库级约定 |

---

## 2. 总体描述

### 2.1 产品视角

链路形态（与 `FSR.md` §3 链路图一致）：

```
Python 生成器(Faker, MIT)
   │ live 模式：按速率写 Kafka
   ▼
Kafka 4.3.1（KRaft，无 ZooKeeper）
   ▼
Flink 2.2.x  ← 唯一常驻计算引擎
   │ 实时：1 分钟窗口聚合 → Flink-Doris-Connector 26.3.0 → Doris
   │ 离线：批模式跑五层分层 → Paimon 2.1.0
   ▼
Doris 4.1.5（OLAP，看板供数）
   ▼
Superset 6.1.0（看板）
```

### 2.2 功能概览

八个主模块：①生成器 ②消息队列 ③实时作业 ④湖仓分层 ⑤OLAP ⑥看板 ⑦编排 ⑧可复现校验。
三个加强项（SHOULD）：数据质量校验、Kafka lag 检查、可选 profile（MinIO / Spark）。
**明确不加**：监控告警系统、调度器容器、多节点高可用。

### 2.3 用户特征（Actors）

| 角色 | 特征与诉求 |
| --- | --- |
| **开发者**（你） | 执行 `make`/compose、跑回放、查表、调参。容忍手工触发，不接受手工修数据 |
| **评审者 / 面试官** | **只读**看板与文档，**零账号**、零命令行即可看到结果 |
| **系统自身** | 批作业由 Makefile 依赖关系触发（无调度器）；流作业常驻自恢复 |

### 2.4 约束

- **C1**：6 核 12 线程 / 16 GB 内存单机；**天花板**约 8 GB，因此常驻容器群 **≤ 8 GB**，逐服务显式 `mem_limit`。
- **C2**：WSL2 后端下 Docker Desktop 不提供资源滑块，资源只能在 WSL 层配置（改 `.wslconfig` 属未决项 U1，见 §6.2）。
- **C3**：**锚点**必须精确 pin（版本表见 `FSR.md` §4 与本文 §3.5），禁止 `latest`。
- **C4**：不引入调度器容器，改用 Makefile 编排。
- **C5**：**仓库零原始数据**——公开数据集（UserBehavior）只放下载脚本与许可说明，原始文件不入 git。
- **C6**：端口仅绑 `127.0.0.1`。

### 2.5 假设与依赖

- **A1**：Docker Desktop（WSL2 后端）已安装且可用（已确认）。
- **A2**：官方**未给出**多家组件的单机最低内存（Kafka / Flink / Doris / Superset），故容量门槛为**设计目标**而非官方保证，以实测填充。
- **A3**：`.wslconfig` 的 `memory` 已设为 **12 GB**（U1，2026-10-19 执行，实测 WSL 内 `MemTotal` = 11.68 GB）。**天花板**由 7.72 GB 升至 11.68 GB，对 8 GB 常驻预算留出约 3.7 GB 余量。回滚 = 恢复 `%UserProfile%\.wslconfig.bak-2026-10-19-before-u1` 后执行 `wsl --shutdown`。
- **A4**：已知版本落差——Doris Paimon Catalog 官方依赖 Paimon 1.4.2，与本项目 Paimon 2.1.0 不一致（`FSR.md` R1），故看板供数主路径不依赖它。

---

## 3. 具体需求

### 3.1 功能需求

#### 3.1.1 数据生成器（模块①）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-GEN-01` | MUST | 单程序双模式：`live` 常驻按速率写 Kafka；`dump`/`replay` 预生成并按原始时间戳回放，用于离线批输入 | 两模式均可独立运行且退出码为 0 |
| `REQ-F-GEN-02` | MUST | 事件命名对齐 GA4 电商事件规范，覆盖 5 类：`page_view`/`view_item`/`add_to_cart`/`begin_checkout`/`purchase`；字段定义符合 §3.3 | 生成样本逐字段通过 §3.3 枚举与非空校验 |
| `REQ-F-GEN-03` | MUST | 默认速率 ≈200 events/s，可通过参数配置 | 默认运行 60 s 产出 12,000 ±10% 条 |
| `REQ-F-GEN-04` | SHOULD | 大促尖峰模式：短时速率 ≥1000 events/s | 尖峰模式触发期间实测速率 ≥1000/s |
| `REQ-F-GEN-05` | MUST | 漏斗分布符合真实形态：`page_view` 占绝对多数，逐级递减至 `purchase` | 单次运行后各事件类型计数满足递减关系 |
| `REQ-F-GEN-06` | MUST | **`--seed` 随机种子**：同一种子 + 同参数产出同一批数据 | 同种子跑两遍，输出文件哈希一致 |
| `REQ-F-GEN-07` | MUST | **`--late-correctable`（默认 3%）／`--late-expired`（默认 2%）**：分别把该比例事件的事件时间置于水位线之前（`--late-correctable` 落 2 分钟允许延迟内、`--late-expired` 超期只计数），偏移**一律相对水印**而非墙上时钟计算，制造可复现乱序 | 设 `--late-correctable 0.03 --late-expired 0.02` 后，实时侧窗口修正段确有迟到事件落入、超期段确有迟到计数条目 |
| `REQ-F-GEN-08` | MUST | 产出并维护维度数据（商品/品类/地域/渠道），供 DIM 层装载 | DIM 层可装载出非空维表 |

#### 3.1.2 消息队列（模块②）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-MQ-01` | MUST | 主 topic `ecom.behavior.event.v1`；死信 topic `ecom.behavior.event.v1.dlq` | 两 topic 均存在 |
| `REQ-F-MQ-02` | MUST | 单机 KRaft 模式，副本因子 1，分区数固定并写入配置（不随环境漂移） | topic 描述显示 RF=1 且分区数等于配置值 |
| `REQ-F-MQ-03` | MUST | retention 为 **7 天或 2 GB 取小** | topic 配置项与之一致 |
| `REQ-F-MQ-04` | MUST | 消息体为 JSON，含 `schema_version` 字段；字段命名一律 `snake_case` | 采样消息可解析且键名符合规范 |

#### 3.1.3 实时作业（模块③）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-RT-01` | MUST | **1 分钟滚动窗口**聚合 | 窗口触发周期实测 ≈60 s |
| `REQ-F-RT-02` | MUST | 指标覆盖：实时 PV、UV（近似）、4 步转化漏斗、GMV、品类分布、地域分布 | Doris 对应指标表均有数据 |
| `REQ-F-RT-03` | MUST | UV 用近似算法（HyperLogLog / Theta sketch），**离线侧算精确 UV 并对账** | 文档给出误差量化；对账跑通 |
| `REQ-F-RT-04` | MUST | 事件时间处理：Watermark 策略 + `withIdleness` + 允许延迟 + 迟到数据走**侧输出** | 配置可见；迟到事件确实进侧输出 |
| `REQ-F-RT-05` | MUST | **checkpoint 开启**，状态后端 RocksDB（增量 checkpoint） | Flink UI 可见 checkpoint 成功记录 |
| `REQ-F-RT-06` | MUST | 经 **Flink-Doris-Connector 26.3.0** 以 Stream Load 两阶段提交写入 Doris | Doris 表数据持续增长；重启后无重复无丢失 |
| `REQ-F-RT-07` | MUST | 脏数据（违反 §3.3 规则）侧输出至 DLQ topic | DLQ topic 有消息且内容可读 |
| `REQ-F-RT-08` | MUST | 作业并行度 **≤ 4** | Flink 配置中并行度 ≤4 |

> 说明：`REQ-F-RT-06` 是端到端 exactly-once 的落点，其**故障验证**见 `REQ-F-VERIFY-03`。

#### 3.1.4 湖仓分层（模块④）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-LAKE-01` | MUST | **ODS** 为 append-only 表，保留**原始 JSON payload 一列**，不做有损加工 | ODS 行数 ≥ 摄入事件数；`payload` 列非空 |
| `REQ-F-LAKE-02` | MUST | **DWD** 为主键表：清洗 + 规范化 + 统一 **UTC epoch 毫秒** + 统一超集 schema | 查到 DWD 行数 > 0；`event_time_ms` 为毫秒整数 |
| `REQ-F-LAKE-03` | MUST | **DIM** 为主键表，做**时态维表**（商品/品类/用户/地域/渠道） | DIM 各表可 join 出结果 |
| `REQ-F-LAKE-04` | MUST | **DWS** 主题宽表聚合（用户/商品/品类/时间粒度维度） | DWS 表有数据 |
| `REQ-F-LAKE-05` | MUST | **ADS** 应用层指标表，供看板与 OLAP 消费 | ADS 表有数据且字段与看板面板对应 |
| `REQ-F-LAKE-06` | MUST | 五层**全部落单一 Paimon catalog**，该 catalog 下的 **`ecom_lake` database** 承载全部五层表（原文「单一 catalog（库名 `ecom_lake`）」一词混说 catalog 与 database，已按 [08] 一手事实订正） | 五层表均存在于该 catalog 的 `ecom_lake` database 内（`SHOW TABLES IN <catalog>.ecom_lake` 逐层可查） |
| `REQ-F-LAKE-07` | MUST | 离线分层由 **Flink 批模式 + Makefile 触发**（无调度器） | `make replay` 可完整跑完五层 |
| `REQ-F-LAKE-08` | MUST | Paimon 表维护：快照过期保留最近 **5 个** + 定期 compaction 合并小文件 | 表属性配置可见；快照数受控 |
| `REQ-F-LAKE-09` | COULD | 公开数据集（UserBehavior）映射回放：下载脚本 + md5 校验 + 许可署名，**原始文件不入 git** | 脚本存在；`.gitignore` 覆盖数据目录；仓库内无原始数据 |

#### 3.1.5 OLAP（模块⑤）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-OLAP-01` | MUST | 看板直连库为 `ecom_ads` | Superset 中该库连接可用 |
| `REQ-F-OLAP-02` | MUST | 表模型支持 upsert（unique key 模型），保证重复写入幂等 | 同批数据写两遍，结果一致（见 `REQ-F-VERIFY-02`） |
| `REQ-F-OLAP-03` | MUST | DWS/ADS 按分区保留 **30 天** | 分区/保留策略配置可见 |
| `REQ-F-OLAP-04` | SHOULD | **Paimon Catalog 直读（zero-ETL）作为可选实验**，不阻塞验收（因版本落差 R1） | 实验成则记录；不成则记录失败原因，不影响验收 |

#### 3.1.6 看板（模块⑥）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-BI-01` | MUST | 面板：实时 PV / UV、4 步转化漏斗、实时 GMV、品类分布、地域分布 | 5 个面板均可渲染出数据 |
| `REQ-F-BI-02` | MUST | 自动刷新间隔 **≤ 10 s** | 面板刷新配置 ≤10s；「观察到数值变化」＝**窗口关闭/修订时刻值确实变化**（非「每次刷新值都变」），10 s 刷新只证「链路活着」 |
| `REQ-F-BI-03` | MUST | **评审者零账号**只读可访问（本机浏览器即可） | 无任何外部平台账号即可打开并看到刷新 |
| `REQ-F-BI-04` | SHOULD | 增加「脏数据率」面板 | 面板存在且随 DLQ 变化 |

#### 3.1.7 编排（模块⑦）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-ORCH-01` | MUST | **`make init`** 一键拉起整条链路（**含 Doris profile 与初始化脚本**） | **在已有 Docker + make 的机器上**（L0 制品态）`make init` 后 `make verify` 退出码 0、`docker compose ps` 全 `healthy`；四档契约见 `reproducibility-boundary.md` §1 |
| `REQ-F-ORCH-02` | MUST | **逐服务显式 `mem_limit`**，常驻总和 ≤ 8 GB（判据同 `REQ-NF-CAP-01` 双口径） | 同 `REQ-NF-CAP-01` 双口径断言 |
| `REQ-F-ORCH-03` | MUST | Makefile 目标：`up` / `down` / `init` / `replay` / `verify` / `logs` / `spike` | 各目标均可执行成功 |
| `REQ-F-ORCH-04` | MUST | 重启策略 `restart: unless-stopped` | 主机重启后链路自动恢复 |
| `REQ-F-ORCH-05` | MUST | Kafka / Paimon / Doris / Superset 元数据用 named volume 持久化 | `docker compose down` 后再 `up`，数据仍在 |
| `REQ-F-ORCH-06` | MUST | 端口仅绑 `127.0.0.1`（含 Flink UI :8081、Superset、Doris、Kafka） | 从局域网其它机器不可访问 |
| `REQ-F-ORCH-07` | MUST | 容器日志轮转上限 **10 MB × 3** | 日志驱动配置可见 |
| `REQ-F-ORCH-08` | SHOULD | 可选 profile：`minio`（S3 协议演示）、`spark`（离线对照，按需拉起用完即停） | 不启用 profile 时不影响默认链路 |

#### 3.1.8 可复现校验（模块⑧）

| 编号 | 优先级 | 需求 | 验收判据 |
| --- | --- | --- | --- |
| `REQ-F-VERIFY-01` | MUST | `make verify` 烟测五项：Kafka 有数据 / Flink job RUNNING / Paimon 五层有行 / Doris ADS 有行 / Superset 健康。**「Paimon 五层有行」在 P1 里程碑判定中显式 SKIP、不计入通过条件；P2 里程碑判定中该项必须为真断言且退出码参与判定** | 五项断言按上句分层判定，P1 判定通过时退出码 0 |
| `REQ-F-VERIFY-02` | MUST | **幂等验证**：同 `--seed` 同批数据跑两遍，Doris 结果一致；**不进 `verify` 每次运行**，改由离线幂等断言（spec §5.4）＋ `verify-fault` 恢复重放覆盖 | 两次结果比对一致 |
| `REQ-F-VERIFY-03` | SHOULD | **`make verify-fault` 故障注入**：kill TaskManager → 作业从 checkpoint 自动恢复 → 校验 Doris 结果条数与 kill 前一致 | 恢复后条数一致（不重不丢） |
| `REQ-F-VERIFY-04` | SHOULD | 数据质量校验：按 §3.3 规则检查，实时侧进 DLQ、离线侧单落 `dwd_behavior_event_reject` 并计数 | 违规样本被正确分流且有计数 |
| `REQ-F-VERIFY-05` | SHOULD | Kafka lag 检查脚本 | 脚本可输出各消费组 lag |

### 3.2 非功能需求

#### 3.2.1 性能

| 编号 | 优先级 | 需求 | 性质 |
| --- | --- | --- | --- |
| `REQ-NF-PERF-01` | MUST | 端到端延迟（事件产生 → 看板可见）**≤ 60 s** | **设计目标**，非官方承诺 |
| `REQ-NF-PERF-02` | MUST | 看板自动刷新间隔 ≤ 10 s | 设计目标 |
| `REQ-NF-PERF-03` | MUST | 默认 200 events/s；尖峰模式 ≥1000 events/s | 设计目标 |
| `REQ-NF-PERF-04` | MUST | 实时作业并行度 ≤ 4 | 设计约束 |

#### 3.2.2 容量与资源

| 编号 | 优先级 | 需求 | 性质 |
| --- | --- | --- | --- |
| `REQ-NF-CAP-01` | MUST | 容器群常驻内存 **≤ 8 GB**（天花板约束，**设计值**）。判据按**两个口径**分别断言：①**声明上限**（全部服务 `mem_limit` 之和，含 profile 内服务）≤ 8 GB；②**实测占用**（`docker stats --no-stream` 合计）≤ 8 GB。⚠️ 两者不可互相冒充：P0 实测空载六服务合计 ≈3.83 GiB，声明上限合计 10.5 GiB（10,752 MiB，含 `client` 256m），已按官方默认下调后落在 11.68 GiB 天花板内。**P1 带载后须复测，带载未实测即未通过** | 设计目标；官方未给出各家最低内存（A2） |
| `REQ-NF-CAP-02` | MUST | 磁盘占用（含数据卷）**≤ 20 GB** | 设计目标 |
| `REQ-NF-CAP-03` | MUST | **预拉镜像前提下**，`make init` 到**链路全 healthy** **≤ 15 分钟**（**不含出数**：看板出数依赖 P1 的真实 Flink 作业与 Doris 写数，P1 未实现即不可测）。「到看板出数 ≤ 15 分钟」作为**同一需求的第二截**，判据转 P1 实测 | 两档之一 |
| `REQ-NF-CAP-04` | MUST | **预构建镜像经 `docker save`/`docker load` 导入后**，L3 跨机搬运冷启动 ≤ 30 分钟。⚠️ **「本地构建 Doris 镜像」不计入本判据**（实测构建 ≈35 min；Doris 无官方版本化镜像，构建不可避免） | 两档之二 |
| `REQ-NF-CAP-05` | MUST | 离线分层最小可跑数据量 ≥ 100 万事件 | 设计目标 |
| `REQ-NF-CAP-06` | COULD | 可选亿级回放（约 1 亿事件 / ~3.4 GB 解压） | 不阻塞验收 |
| `REQ-NF-CAP-07` | SHOULD | 数据卷使用率 > 80% 时给出提示 | 防护性 |

#### 3.2.3 可靠性

| 编号 | 优先级 | 需求 |
| --- | --- | --- |
| `REQ-NF-REL-01` | MUST | 容器 `restart: unless-stopped`，主机重启后链路自动恢复 |
| `REQ-NF-REL-02` | MUST | `docker compose down` 不丢数据（named volume） |
| `REQ-NF-REL-03` | MUST | 端到端 exactly-once：source offset + checkpoint + sink 2PC 三处对齐 |

#### 3.2.4 安全

| 编号 | 优先级 | 需求 |
| --- | --- | --- |
| `REQ-NF-SEC-01` | MUST | 所有服务端口仅绑 `127.0.0.1`，不暴露局域网 |
| `REQ-NF-SEC-02` | MUST | 仓库零原始数据、零密钥；公开数据集只放脚本与许可说明 |

#### 3.2.5 可维护性

| 编号 | 优先级 | 需求 |
| --- | --- | --- |
| `REQ-NF-MNT-01` | MUST | **版本精确 pin**（**锚点**矩阵见 `FSR.md` §4；本文 §3.5 摘录相容链条） |
| `REQ-NF-MNT-02` | MUST | 容器日志轮转 10 MB × 3 |
| `REQ-NF-MNT-03` | MUST | 端口清单与用途写进 README |
| `REQ-NF-MNT-04` | MUST | 分层建模理由与口径差异成文（供 `interview-prep.md` 消费） |

### 3.3 数据需求（事件 schema 字段表）

**公共字段**（所有事件共有）：

| 字段 | 类型 | 必填 | 取值范围 / 枚举 | 说明 |
| --- | --- | --- | --- | --- |
| `event_id` | string | 是 | UUID v4，全局唯一 | 幂等键 |
| `event_time` | integer | 是 | UTC epoch **毫秒** | 事件时间（Watermark 依据） |
| `user_id` | string | 是 | 非空 | 用户标识 |
| `item_id` | string | 是 | 非空 | 商品标识 |
| `category_id` | string | 是 | 非空 | 品类标识 |
| `event_type` | string | 是 | 五类枚举（见下） | 事件类型 |
| `schema_version` | string | 是 | 如 `1.0`/`1.1`/`2.0` | 消息版本（分级与兼容规则见 §3.3.1） |
| `page` | string | 否 | 页面标识 | 来源页 |
| `source` | string | 否 | 端类型 | 来源端 |
| `channel` | string | 否 | 渠道枚举 | 渠道维度 |
| `province` | string | 否 | 省级行政区 | 地域维度 |
| `amount` | decimal | 否（`purchase` 必填） | ≥ 0 | 金额 |
| `currency` | string | 否 | 如 `CNY` | 币种 |
| `quantity` | integer | 否 | ≥ 1 | 数量 |
| `order_id` | string | 否（`begin_checkout`/`purchase` 必填） | 非空 | 订单标识 |
| `payload` | object | 是 | 原始 JSON | ODS 保真列 |

**五类事件的必填子集**：

| `event_type` | 额外必填 | 口径说明 |
| --- | --- | --- |
| `page_view` | — | 浏览量，占比最高 |
| `view_item` | — | 商品详情浏览，漏斗第 1 步 |
| `add_to_cart` | `quantity` | 加购，漏斗第 2 步 |
| `begin_checkout` | `order_id` | 下单（未支付），漏斗第 3 步。**公开数据集回放无此步 → 回放漏斗降级为 3 步，差异须在文档写明** |
| `purchase` | `amount`、`currency`、`order_id` | 支付，漏斗第 4 步 |

**数据质量规则**（`REQ-F-VERIFY-04` 依据）：① `event_time` 不晚于当前 +5 分钟，且**不早于「本批次最早事件时间 − 允许乱序幅度」**（批次基准时间 = 本批摄入事件的最早 `event_time`；允许乱序幅度取实时侧允许延迟，默认 2 分钟）；该下界为**相对**下界，对生成器产出与公开数据集回放同时成立；② `amount` ≥ 0；③ `event_type` ∈ 枚举；④ `user_id` / `item_id` / `event_id` 非空；⑤ `event_id` 唯一（重复走独立通道：计数、不进 DLQ、不计脏率，见 gdd D2）。
**违规处理**：实时侧 → DLQ topic；离线侧 → `dwd_behavior_event_reject` 单表 + 计数。**丢弃必须有痕迹**。

#### 3.3.1 schema_version 兼容规则（铁律唯一所有者见 `canonical-truth.md` §3.1，本节只承载字段语义）

| # | 铁律 | 字段语义 |
| --- | --- | --- |
| **S1** | 版本分级 | 加**可选**字段＝minor（`1.0` → `1.1`）；**必填字段增删、字段类型或语义变更**＝major（`2.0`）。`schema_version` 字符串 `MAJOR.MINOR`，只比 major 用于兼容性判定 |
| **S2** | 消费端容忍度 | 消费端**必须**容忍同 major 的更高 minor（未知可选字段**丢弃并计数**）；**必须拒绝**更高 major ⇒ 进 DLQ，原因码 **`schema_major_unsupported`** |
| **S3** | 版本号不要求相等 | 不要求生产端与消费端版本号相等；要求消费端支持的 major **≥** 生产端 major，且能处理生产端 minor。契约是「能力下界」，不是「值相等」 |
| **S4** | checkpoint 恢复 | 跨版本恢复**不禁止**，按「**缺省值 + 标记**」放行；恢复后产生的行必须带 **`schema_version_upgraded`** 标记 |

### 3.4 接口与命名（冻结项，不得漂移）

| 类别 | 冻结值 |
| --- | --- |
| Kafka topic | `ecom.behavior.event.v1`；DLQ `ecom.behavior.event.v1.dlq` |
| 消息格式 | JSON + `schema_version`；字段一律 `snake_case`；兼容规则见 §3.3.1 |
| Paimon | 单一 Paimon catalog，库名对应其下 **`ecom_lake` database**（见 `REQ-F-LAKE-06`）；表 `ods_behavior_event` / `dwd_behavior_event` / `dwd_behavior_event_reject` / **`dwd_late_event_count`**（FC-2，2026-10-20 已批）/ `dim_*` / `dws_*` / `ads_*` |
| Doris | 看板直连库 `ecom_ads` |
| Makefile 目标 | `up` / `down` / `init` / `replay` / `verify` / `verify-fault` / `spike` / `logs` ＋ **`lint` / `typecheck`**（FC-1，2026-10-20 已批；静态检查与运行时烟测分开，不新增 `check`） |
| 本机端口 | Flink UI `:8081`、Superset、Doris（FE/BE）、Kafka `:9092` —— **全部仅绑 `127.0.0.1`** |

### 3.5 设计约束（版本锚点，摘录自 `FSR.md` §4）

**锚点必须精确 pin**。相容链条：

```
Flink 2.2.x  ── Kafka connector 5.0.0 ── Kafka 4.3.1
     │
     └── Flink-Doris-Connector 26.3.0 ── Doris 4.1.5 ── Superset 6.1.0
                  配套：Paimon 2.1.0（五层全落）／Python 3.11+ 与 Faker(MIT)
```

**施工红线**：不得使用 Flink 2.3.0——官方明确 2.3 尚无已发布 Kafka connector（`FSR.md` R2）。

---

## 4. 验证与验收

### 4.1 验收方式

| 编号 | 方式 |
| --- | --- |
| `REQ-F-VERIFY-01` | `make verify` 五项烟测按 §3.1.8 分层判定（P1 含显式 SKIP），判定通过即退出码 0 |
| `REQ-F-VERIFY-02` | 同 `--seed` 跑两遍，自动比对 Doris 结果；不进每次 `verify`，由离线幂等断言＋`verify-fault` 覆盖 |
| `REQ-F-VERIFY-03` | `make verify-fault`：kill TM → 恢复 → 比对条数 |
| 其余功能需求 | 按 §3.1/§3.3 的「验收判据」列逐条人工确认 |

### 4.2 顶层成功判据（**权威归属 `FSR.md` §1**，本节只做指针，不复制）

`FSR.md` 持有顶层 5 条成功判据；本 SRS 持有可执行验收判据的细化版。两者靠 §5 追溯矩阵连接，**不互相复制**。

### 4.3 演示证据要求

- README 含一键启动说明与架构图。
- 演示 GIF ≥ 1 个，须体现**看板数值持续变化**（对应「实时链路活着」）。
- 数仓分层文档须包含各层建模理由（ODS append 表 / DWD 起主键表的理由）。

---

## 5. 追溯矩阵

**格式**：需求 ↔ 来源决策（`FSR.md` / Q 号） ↔ 验收步骤。完整清单按模块展开，以下为骨架与代表条目。

| 模块 | 需求编号段 | 主要来源决策 | 验收落点 |
| --- | --- | --- | --- |
| ①生成器 | `REQ-F-GEN-01..08` | FSR §3/§5、Q3、Q21、Q26、Q33、Q34、**Q55** | §3.1.1 判据列 |
| ②消息队列 | `REQ-F-MQ-01..04` | FSR §3/§4、Q30、Q47、Q49 | §3.1.2 + `verify` |
| ③实时作业 | `REQ-F-RT-01..08` | FSR §5、Q20、Q27、Q28、Q29、Q38、**Q56** | §3.1.3 + `verify` / `verify-fault` |
| ④湖仓分层 | `REQ-F-LAKE-01..09` | FSR §6、Q14、Q16、Q18、Q24、Q31、Q32、Q47 | §3.1.4 + `verify` |
| ⑤OLAP | `REQ-F-OLAP-01..04` | FSR §3、Q36、Q38、R1 | §3.1.5 |
| ⑥看板 | `REQ-F-BI-01..04` | FSR §3、Q6、Q37 | §3.1.6 + 演示 GIF |
| ⑦编排 | `REQ-F-ORCH-01..08` | FSR §2/§3、Q4、Q13、Q15、Q24、Q48 | §3.1.7 + `up` |
| ⑧可复现校验 | `REQ-F-VERIFY-01..05` | FSR §1（判据 1/2）、Q7、Q15、Q52、Q56 | §3.1.8 |
| 非功能 | `REQ-NF-*` | FSR §2、§4、Q45、Q46、Q47、Q48 | §3.2 + `docker stats` |

代表条目（细粒度示例）：

| 需求 | 来源决策 | 验收步骤 |
| --- | --- | --- |
| `REQ-F-LAKE-02` DWD 主键表 + UTC ms | FSR §6、Q18、Q31、Q33 | `make verify` 查到 DWD 行数 > 0 且 `event_time_ms` 为毫秒整数 |
| `REQ-F-RT-06` 经 connector 2PC 写 Doris | FSR §3、Q38、R1 | 看板出数 + `make verify-fault` kill TM 后条数一致 |
| `REQ-F-GEN-07` `--late-correctable`/`--late-expired` 注入乱序 | Q20、Q55 | 窗口修正段确有迟到事件落入、超期段确有迟到计数条目；看板可演示 |
| `REQ-NF-CAP-03/04` 冷启动两档 | Q46、Q7 | 分别计时，分开判定 |
| `REQ-F-OLAP-04` Catalog 直读降级为实验 | FSR R1、Q38 | 实验成败均不阻塞验收 |

---

## 6. 范围边界与未决项

### 6.1 明确不做（Out of Scope）

多节点/高可用集群、K8s、调度器容器（Airflow/DolphinScheduler）、监控告警系统、安全权限体系、真实支付对接、生产级可观测性。
**Spark 不常驻**（仅作可选 profile）；**Iceberg 只作对照**（不实现）。

### 6.2 未决项（需用户单独授权后才可执行）

| # | 项 | 现状 | 说明 |
| --- | --- | --- | --- |
| **U1** | `.wslconfig` 的 `memory` 提至 12 GB | **已执行**（2026-10-19，用户授权） | 已备份原文件 → 逐行 diff → `wsl --shutdown` → 验证 WSL 内 `MemTotal` 由 **7.72 GB → 11.68 GB**。备份：`%UserProfile%\.wslconfig.bak-2026-10-19-before-u1`；**回滚 = 恢复备份后执行 `wsl --shutdown`**。依据：WSL2 默认内存 = 宿主机 50%（实测 7.72 GB < 8 GB 预算）。**冲突披露**：原配置注释含「不设 memory= 硬顶…（ADR-0005）」，而仓库内**不存在** ADR-0005 文件；判定为「放宽天花板而非收紧」，且不新增 C 盘占用（`swap=2GB` 未动） |
| **U2** | 仓库根 `.gitignore`（挡 `data/`、`*.zip`、`*.log`） | **已执行**（2026-10-19，用户授权） | 已创建仓库根 `.gitignore` 并逐条 `git check-ignore` 验证；`REQ-F-LAKE-09` 依赖已满足。回滚 = 删除该文件 |
| **U3** | 沙箱可执行性 | **已恢复**（2026-10-19） | 2026-10-18 时 `ops_bash` 报 `EXECUTOR_UNAVAILABLE`、`pwsh` 报 ACL 故障；2026-10-19 会话改为 `danger-full-access` 后命令可正常执行，无需修 ACL |
| **U4** | 仓库**尚无任何提交**，已配远程 `origin → github.com/Gaomen-7/eco-d-a-platform` | **已执行**（2026-10-19，用户授权提交+推送） | 首次提交 `b39be03`（9 文件 / 1244 行），已推送 `main` 至 `origin`。作者身份：`Gaomen-7 <323154867+Gaomen-7@users.noreply.github.com>`（本仓库 `--local` 配置，避免真实邮箱进公开历史）；`core.autocrlf=false` 消除行尾噪声 |

### 6.3 待实现期细化（P2/P3，本文只写目标级）

- P2 离线分层的具体表 DDL 与字段级清洗规则。
- P3 文档、架构图、GIF、简历 bullet 的具体内容。
- 亿级回放（`REQ-NF-CAP-06`）是否真跑，按磁盘/时间由用户决定。

---

## 7. 里程碑映射（顺序与判据；不含具体完成日期）

| 阶段 | 覆盖需求 | 完成判据 |
| --- | --- | --- |
| **P0 环境** | `REQ-F-ORCH-01/02/06/07`、U1/U2/U3 | `docker compose up` 拉起骨架；`docker stats` 总值 ≤ 8 GB |
| **P1 实时闭环** | `REQ-F-GEN-*`、`REQ-F-MQ-*`、`REQ-F-RT-01..08`、`REQ-F-OLAP-01..03`、`REQ-F-BI-01..04`、`REQ-F-VERIFY-01/02` | 看板持续刷新；`make verify` 通过；`verify-fault` 通过 |
| **P2 离线分层** | `REQ-F-LAKE-01..08`、`REQ-F-VERIFY-04/05` | 五层表可查；分层理由成稿 |
| **P2 可选** | `REQ-F-LAKE-09`、`REQ-NF-CAP-06` | 抽样回放跑通即可 |
| **P3 文档** | `REQ-NF-MNT-03/04`、§4.3 | 演示 GIF + README + 分层文档齐备 |
