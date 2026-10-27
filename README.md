# eco-a-d-platform

一个**能在一台笔记本上跑起来、也能被完整重跑**的电商用户行为数据平台：
自己造数据 → 实时流处理 → 离线数仓分层 → 看板出数。

它的重点不是"数据量大"，而是**每一层都能验证、每一条结论都有可复现的证据**。

> 新手请从 [§1 这是什么](#1-这是什么) 读起；想直接跑起来跳到 [§3 五分钟跑起来](#3-五分钟跑起来)。
> 只想看结论的跳到 [§6 已验证到什么程度](#6-已验证到什么程度)。

---

## 目录

- [1. 这是什么](#1-这是什么)
- [2. 架构与数据流](#2-架构与数据流)
- [3. 五分钟跑起来](#3-五分钟跑起来)
- [4. 常用命令](#4-常用命令)
- [5. 怎么判断"跑通了"](#5-怎么判断跑通了)
- [6. 已验证到什么程度](#6-已验证到什么程度)
- [7. 项目结构](#7-项目结构)
- [8. 常见问题](#8-常见问题)
- [9. 技术栈与版本](#9-技术栈与版本)
- [10. 设计与决策文档](#10-设计与决策文档)
- [11. 纪律与边界](#11-纪律与边界)
- [12. 维护者速查](#12-维护者速查)

---

## 1. 这是什么

### 一分钟版本

假设你是一个电商网站的数据工程师。你要回答这些问题：

- 今天有多少人来了？（**UV**）
- 看了多少页？（**PV**）
- 卖了多少钱？（**GMV**）
- 用户从"看商品"到"下单"每一步流失多少？（**漏斗**）
- 有没有脏数据混进来？占比多少？（**脏率**）

要回答这些，你得有一条**数据管道**：产生行为事件 → 实时清洗 → 离线汇总 → 报表展示。

本项目就是**把这条管道完整搭出来**，并且做到：

| 特点 | 什么意思 |
| --- | --- |
| **单机可跑** | 一台 16 GB 内存的笔记本（WSL2）就能跑全套，不需要集群 |
| **数据自产** | 自带数据生成器，不依赖任何外部数据源或账号 |
| **可复现** | `make init` 一条命令拉起全部服务；同样的参数产出同样的数据 |
| **可验证** | `make verify` 用 10 条机读判据自动检查链路是否真的活着，不是"看起来在跑" |
| **零账号** | 看板匿名可访问，不需要注册登录任何东西 |

### 完整版本（这条链路在做什么）

```
① 造数据           ② 实时流             ③ 离线分层          ④ 看板
ecomgen 生成器  →  Kafka  →  Flink  →  Paimon（五层）  →  Doris  →  Superset
（五类事件）        （缓冲）    （清洗/分流）  （ODS→DWD→DIM→DWS→ADS）  （供数）  （图表）
```

1. **造数据**：`ecomgen` 用固定随机种子生成五类用户行为事件
   （`page_view` / `view_item` / `add_to_cart` / `begin_checkout` / `purchase`），
   并故意掺入少量"脏数据"（字段缺失、数值越界、时间越界）和"迟到数据"——因为真实的管道必须能处理这些。
2. **实时流**：事件发到 Kafka；Flink 作业消费、按质量规则三分流
   （正常 → 主表；真脏 → 死信队列 DLQ；重复 → 幂等吸收，不计脏率）。
3. **离线分层**：同一批数据落进 Paimon 湖仓，按经典五层加工：
   - `ODS` 原始落地 → `DWD` 明细清洗 → `DIM` 维度（带拉链，保留历史版本）→
     `DWS` 汇总 → `ADS` 应用层（直接给指标）
4. **看板**：ADS 结果同步到 Doris（OLAP 库，查询快），Superset 出五组图表，
   匿名即可打开。

---

## 2. 架构与数据流

### 组件一览

| 组件 | 版本 | 在本项目里干什么 | 端口（只绑 127.0.0.1） |
| --- | --- | --- | --- |
| **Kafka** | 4.3.1 | 事件总线，接收生成器与 Flink 之间的数据 | `9092`（网内）`29092`（宿主可达） |
| **Flink** | 2.2.1（含 Paimon connector） | 实时流处理：清洗、分流 DLQ、写 Paimon | `8081`（Web UI） |
| **Paimon** | 随 Flink 镜像 | 湖仓存储，承载离线五层 | —（文件系统） |
| **Doris** | 4.1.5 | OLAP 查询库，唯一给看板供数的库 | `8030`(HTTP) `9030`(MySQL) |
| **Superset** | 6.1.0 | 看板展示，匿名可访问 | `8088` |
| **ecomgen** | 本仓 Python 包 | 数据生成器（`dump` 静态产出 / `live` 实时发送） | — |

### 数据流向（含三条关键路径）

```
                        ┌─────────────── 实时路径 ───────────────┐
 ecomgen live ──▶ Kafka ──▶ Flink 作业 ──┬─▶ Paimon DWD（正常）
                                        ├─▶ DLQ topic（真脏：schema/value/time 三类）
                                        └─▶ 幂等吸收（重复：单独计数，不进 DLQ）

                        ┌─────────────── 离线路径 ───────────────┐
 Paimon ODS ─▶ DWD ─▶ DIM（拉链） ─▶ DWS（区间 join） ─▶ ADS ──┬─▶ Doris ecom_ads ─▶ Superset
                                                          └─▶ sparkline「离线口径基准」
```

**为什么要两条路径**：实时路径回答"刚刚发生了什么"，离线路径回答"昨天到底发生了什么"。
两条路径用同一份口径定义（同一个指标必须由同一个公式产出），并由**对账断言**保证两者一致。

### 三个容易搞混的概念

| 概念 | 含义 | 不要和什么搞混 |
| --- | --- | --- |
| **可复现** | 环境和链路能重起来（`make init` 一条命令） | 不是"数据结果可重复"，那是**确定性** |
| **确定性** | 同 `--seed` 同参数 → 数据字节完全相同 | 不是"环境可重建" |
| **脏率** | 死信队列增量 / 输入总事件数 | 重复数据**不进**脏率（它走幂等吸收） |

术语全集见 [`GLOSSARY.md`](GLOSSARY.md)——它是本仓唯一术语表，遇到不认识的词先查它。

---

## 3. 五分钟跑起来

### 前置条件

| 需要 | 说明 |
| --- | --- |
| **Windows 10/11** | 本项目在 Windows + WSL2 下开发和验证 |
| **WSL2**（Ubuntu） | **所有命令都在 WSL2 里跑**，不要在 Windows PowerShell / CMD 里跑 |
| **Docker Desktop** | 需启用 WSL2 integration（Settings → Resources → WSL Integration） |
| **内存 ≥ 16 GB** | 容器群的上限是 8 GB（设计值），宿主留余量 |
| **磁盘 ≥ 20 GB** | 镜像 + 数据卷 |

> **为什么必须用 WSL2**：Windows 侧的 `uv` / `mvn` / JDK 版本与项目锚点不一致，
> 直接跑会产生"在我机器上能跑"的假象。这是本项目的硬约定，不是偏好。

### 第 0 步：进入 WSL 并切到仓库

```bash
wsl                      # 从 Windows 进入 WSL
cd /mnt/f/Anaconda\ practice\ project/eco-a-d-platform   # 换成你的实际路径
```

确认环境对了：

```bash
docker version           # 应能看到 Server 段（说明 WSL 能访问 Docker）
python3 --version        # 应为 3.12.x
```

### 第 1 步：先构建本地镜像（只需一次）

```bash
make build-flink-image
```

这一步做两件事：下载 Paimon connector jar、构建带 Paimon 的 Flink 镜像。
**跳过它会导致 `docker compose up` 报 `image not found`**——这是新手最常踩的坑。

### 第 2 步：一条命令拉起全链路

```bash
make init
```

`make init` **不是** `docker compose up`。它按固定顺序做四段（顺序不可换）：

| 段 | 做什么 | 为什么必须这样 |
| --- | --- | --- |
| ① | 拉起默认 profile（Kafka / Flink JM+TM / Superset） | — |
| ② | `--profile doris up -d --wait` | 默认 `up` **不含 Doris**，缺这段链路不完整 |
| ③ | 在 compose 网络内跑 `doris-init.sh` 建库建表 | Doris 建库必须从网内发起（Docker Desktop 两跳限制） |
| ④ | Superset 初始化 + 看板置备 | `db upgrade` → `create-admin` → `init` → 建 dataset/图表 |

首次执行约 **2.5–3 分钟**（实测 L0 档 157 秒）。完成后应看到 `✓ 完成`。

### 第 3 步：验证它真的活了

```bash
make verify
```

这一条是**判据**，不是"看看有没有报错"。它会跑 10 项检查，
**退出码 0 = 全过**，并把报告写进 `artifacts/verify/`。

期望输出结尾：

```
PASS=10 FAIL=0 SKIP=0  ⇒ 退出码 0
```

### 第 4 步：打开看板

浏览器访问 **<http://127.0.0.1:8088>** → 无需登录，进入 `ecom-live` 仪表盘。

五组图表：PV / UV / GMV 大数字 + PV 分钟趋势 + 四步转化漏斗 + 品类与地域分布 + 脏率。

> 看板是 **10 秒自动刷新**。因为数据粒度是 60 秒，大部分刷新会"值没变"——
> 那是正常的。要看到明显跳变，等一个窗口关闭（整分钟边界后）。

---

## 4. 常用命令

### 八个冻结目标（名字不得改，SRS §3.4）

| 命令 | 作用 | 什么时候用 |
| --- | --- | --- |
| `make up` | 拉起默认 profile（**不含 Doris**） | 只想动 Kafka/Flink/Superset |
| `make down` | 停容器，**保留卷**（数据不丢） | 日常收工 |
| `make init` | 一条命令做完四段初始化 | 首次启动 / 完全重启 |
| `make replay` | 跑离线五层 ODS→DWD→DIM→DWS→ADS | 想重新计算离线数据 |
| `make verify` | 10 项机读判据，退出码判成败 | **每次改完都跑** |
| `make spike` | 尖峰测试：短时 ≥1000 events/s | 测吞吐 |
| `make verify-fault` | 杀掉一个 TaskManager，验证从 checkpoint 恢复 | 测容错 |
| `make logs` | 看容器状态 + 最近 120 行日志 | 出问题时先看它 |

### 设计面目标（非冻结）

| 命令 | 作用 |
| --- | --- |
| `make lint` | `ruff check` + `ruff format --check` |
| `make typecheck` | `basedpyright` 类型检查（要求 errors=0） |
| `make assert` | 跑离线断言集（`replay` 之后用） |
| `make replay-idempotent` | 同批数据重放两遍，比对指纹（验证幂等） |
| `make config-check` | 断言各服务内存上限与端口绑定配置 |
| `make client` | 进入 `client` 容器（网内调试用） |
| `make help` | 列出全部目标 |

### 测试与质量门

```bash
# 全部单元测试（约 400 条，1 分钟内）
uv run pytest -q

# 只跑某个文件
uv run pytest tests/test_verify_checks.py -q

# 质量三连（提交前跑）
make lint && make typecheck && uv run pytest -q
```

### 自己造一批数据

```bash
# 静态产出（同 seed 同参数 → 字节完全相同，可复现）
uv run ecomgen dump --seed 42 --sessions 200 --out /tmp/events.jsonl

# 实时发送到 Kafka（走宿主可达的 HOST listener 29092）
uv run ecomgen live --seed 42 --rate 500 --bootstrap 127.0.0.1:29092 \
  --topic ecom.behavior.event.v1

# 只看发送计划，不真发（推荐的第一次尝试）
uv run ecomgen live --seed 42 --rate 500 --dry-run
```

常用参数：`--seed`（随机种子）、`--sessions`（会话数）、`--rate`（事件/秒）、
`--late-correctable` / `--late-expired`（迟到比例）、`--chaos-ratio`（混沌跳步比例）。

---

## 5. 怎么判断"跑通了"

本项目的核心纪律：**"配置可见"、"UI 里能看到"、"日志没报错"都不算证据**。
必须有机读判据。`make verify` 的 10 项就是这套判据：

| # | 判据 | 检查什么 |
| --- | --- | --- |
| 1 | `kafka-topic-retention` | Kafka topic 的保留策略符合设计值 |
| 2 | `flink-job-running` | Flink 作业状态是 `RUNNING` |
| 3 | `paimon-five-layers` | 五层表**都真有行**（走 Flink SQL 真查，不是看元数据） |
| 4 | `doris-ads-rows` | Doris 三张 ADS 表都有行 |
| 5 | `superset-health` | Superset 健康 |
| 6 | `funnel-cnt-inferred-monotone` | 漏斗各步人数单调递减（逻辑自洽） |
| 7 | `dlq-rate-no-increase` | 脏率没有相对基线增长 |
| 8 | `is-settled-two-states` | 窗口"暂定/落定"两态都成立 |
| 9 | `recon-offline-mirror-doris` | 离线口径与 Doris 供数点**对账一致** |
| 10 | `degradation-ladder` | 降级阶梯（容器 OOM 次数不超阈值） |

报告落在 `artifacts/verify/verify-report-<时间戳>.json`，可直接读 JSON 复核。

> **判据不看退出码的先例**：`sql-client.sh -f` 语句报错后仍以 0 退出，
> 所以离线断言改用"哨兵行"机制而不是退出码。这类坑在
> [`sql/README.md`](sql/README.md) 有完整记录——它专业但非常值得读。

---

## 6. 已验证到什么程度

这一节是**实测数字**，不是设计目标。每条都能在 `artifacts/` 或票面 `Answer` 里复核。

| 能力 | 实测结果 | 时间 |
| --- | --- | --- |
| 一条命令冷启（L0 制品档） | **157 秒**，四段全过 | 2026-10-25 |
| 全链路判据 | **10 PASS / 0 FAIL**，退出码 0 | 2026-10-27 |
| 尖峰吞吐 | **1011.8 events/s**（11134 条 / 11.0 秒，含 flush） | 2026-10-26 |
| 离线五层行数 | ods 3,145,728 / dwd 1,048,576 / dim 105,430 / dws 1,550,285 / ads 90,348 | 2026-10-27 |
| Doris 三表行数 | ads_window_metric 32,935 / ads_funnel_step 3,654 / ads_late_expired_count 55,611 | 2026-10-27 |
| 容错恢复 | kill TaskManager → 从 checkpoint 恢复 → 条数一致 | 2026-10-22 |
| 部署面判据 | **11 / 11 PASS** | 2026-10-27 |
| 离线幂等 | 同批重放两遍，DWD 指纹逐字节相同 | 2026-10-24 |
| 删除后磁盘回收 | DROP 1 GB 表后 **691 秒**磁盘真实释放 | 2026-10-25 |

### 诚实的边界声明

以下都是**设计值或实测单点**，不是官方保证，别当成 SLA：

- **8 GB 内存上限**是设计值（宿主天花板约束下定的），不是组件官方推荐值。
- **157 秒冷启**是在预拉镜像前提下的实测；冷镜像或首次拉取会慢得多。
- **L0 档**（镜像已在）才有"一条命令 30 分钟内"的承诺；**L1 档**（无镜像无卷）**豁免**该时长
  ——Doris 需要本地构建。
- 宿主的 **WinNAT 动态端口排除段会漂移**，历史上多次覆盖冻结端口（详见 §8）。
  这是宿主环境事实，不是仓库缺陷。
- 所有性能数字都是**单机单点实测**，用于证明"能力存在"，不代表生产容量规划。

---

## 7. 项目结构

```
eco-a-d-platform/
├── README.md              ← 你在这里
├── GLOSSARY.md            ← 唯一术语表（遇到生词先查它）
├── Makefile               ← 八个冻结目标 + 设计面目标
├── docker-compose.yml     ← 六个长驻服务 + 资源上限与端口绑定
│
├── src/ecomgen/           ← Python 数据生成器 + 验证器
│   ├── events.py          ← 事件 schema 与质量规则（唯一权威）
│   ├── world.py           ← 用户/商品模拟世界
│   ├── cli.py             ← dump / live 双模式入口
│   ├── producer.py        ← Kafka 发送
│   ├── verify_runner.py   ← make verify 的 10 项判据
│   └── fault.py           ← make verify-fault 的容错演练
│
├── flink/                 ← Flink 作业（Java / Table API）
├── sql/                   ← 离线五层 SQL（replay_*.sql）
│   └── README.md          ← 五层形状与三个最容易写反的约定
│
├── docker/scripts/        ← 初始化与置备脚本
├── scripts/               ← 断言与部署面检查
├── data/threshold/        ← 门槛批数据（生成产物样例）
├── tests/                 ← 单元测试（约 400 条）
├── docs/
│   ├── adr/               ← 架构决定记录
│   └── agents/            ← 给 AI agent 的仓库约定
│
└── .scratch/              ← 设计与决策档案（不是运行时代码）
    └── ecom-user-behavior-pipeline/
        ├── map.md         ← 设计链入口（从这里进）
        ├── SRS.md / gdd.md / HLD.md   ← 需求 / 功能 / 结构
        └── issues/        ← 施工票与验收证据
```

**关键入口**：

| 想干什么 | 看哪里 |
| --- | --- |
| 搞懂某个术语 | [`GLOSSARY.md`](GLOSSARY.md) |
| 改离线 SQL | [`sql/README.md`](sql/README.md) |
| 知道判据怎么写的 | [`src/ecomgen/verify_runner.py`](src/ecomgen/verify_runner.py) |
| 了解为什么用这个技术栈 | [`.scratch/ecom-user-behavior-pipeline/HLD.md`](.scratch/ecom-user-behavior-pipeline/HLD.md) §3 |
| 看施工进度与证据 | [`.scratch/ecom-user-behavior-pipeline/issues/impl-dag.md`](.scratch/ecom-user-behavior-pipeline/issues/impl-dag.md) |

---

## 8. 常见问题

### `make up` 报 `image ... not found`

先跑 `make build-flink-image`。Flink 镜像用的是本地 tag，干净检出后不构建就没有。

### 端口连不上

**先别怀疑代码**。Windows 宿主的 WinNAT 会动态排除一批端口，历史上覆盖过
`8030/8040/8081/8088`。检查：

```powershell
# Windows 宿主侧
netsh interface ipv4 show excludedportrange protocol=tcp
```

排除段存在时非管理员无法释放，只能等它漂走或临时改宿主侧发布端口
（`COMPOSE_EXTRA_FILES` 叠加 override，见 impl-03a §C）。

> ⚠️ **不要用 HTTP curl 判断 `9010/9030/9060/9092` 的连通性**——它们是 MySQL / brpc /
> Kafka 协议，curl 会报"连不上"但 TCP 层其实是通的。协议层失败 ≠ 端口不通。

### `make spike` 连不上 Kafka

`make spike` 从宿主真发时走 **`127.0.0.1:29092`**（HOST listener），不是 `9092`。
compose 的 `9092` 口 advertised 成容器名 `kafka:9092`，宿主解析不了。

### 看板打不开 / 无限登录

置备脚本可能没跑完。重跑置备：

```bash
bash docker/scripts/superset-provision.sh
```

它负责建 database / dataset / 图表 / dashboard / 匿名授权。

### 看板数字不跳变

正常。数据粒度 60 秒，看板 10 秒刷新，所以大部分刷新值不变。
等整分钟边界后的窗口关闭，或者跑一次 `make replay` 再等同步。

### 改了 SQL 之后 `make assert` 变红，但 SQL 明明没报错

读 [`sql/README.md`](sql/README.md) 的「三条最容易写反的约定」。最常见原因：
注释里出现了带方括号的错误标记字面量，被 sql-client 逐字回显后误判成失败。

### 彻底重来（删掉所有数据）

```bash
make down
docker compose down -v          # ⚠️ -v 会删卷，数据全没
make build-flink-image && make init
```

---

## 9. 技术栈与版本

**版本锚点纪律**：每个组件精确 pin 版本号，可复现性 = 锚点不漂移。
Python 工具链的唯一权威是 [`uv.lock`](uv.lock)，不是"我机器上装了什么"（[ADR-0001](docs/adr/0001-uv-lockfile-is-toolchain-authority.md)）。

| 层 | 选型 | 版本锚点 |
| --- | --- | --- |
| 数据生成 | Python + Faker | Python `>=3.12,<3.13`；Faker 钉到补丁号 |
| 消息 | Apache Kafka | `4.3.1` |
| 流处理 | Apache Flink | `2.2.1`（[ADR-0002](docs/adr/0002-flink-2-2-1-patch-anchor.md)：只用 2.2.x，不用 2.3.0——2.3.0 无已发布 Kafka connector） |
| 湖仓 | Apache Paimon | 随 Flink 镜像（bundled connector） |
| OLAP | Apache Doris | `4.1.5` |
| BI | Apache Superset | `6.1.0` |
| 编排 | Docker Compose | v2 |

**弃选并记录理由的**（详见 HLD §3 定案表）：`ty`（Flink 未支持）、Grafana、Hudi、ClickHouse。

> **关于 ClickHouse**：如果你在别处看到本项目关联 ClickHouse，那是过时信息——
> 本项目用 **Doris** 做 OLAP，ClickHouse 是被评估后**弃选**的选项。

---

## 10. 设计与决策文档

本项目把"为什么这么定"完整留档了。**定位从
[`map.md`](.scratch/ecom-user-behavior-pipeline/map.md) 进**——它是路线图（wayfinder 地图），
记录了已落的决策、当前前沿与未解迷雾。

```
SRS.md（必须做到什么）
  └─▶ gdd.md（每个功能怎么做）
        └─▶ HLD.md（结构、技术栈定案、运行约束）
              └─▶ map.md（走到哪了：决策落子 + 前沿）
                    └─▶ issues/impl-*.md（每张施工票的问题与答案）
```

**阅读顺序建议**：新手只看本 README 就够；想知道"为什么不用 X"再读 HLD §3；
要接手施工就看 `map.md` 和 `impl-dag.md`。

---

## 11. 纪律与边界

### 仓库纪律

- **零原始数据、零密钥**：`.env` 是机密面，不入库、勿读勿抄。运行产物 `artifacts/` 不入库。
- **版本锚点不可协商**：改技术栈或版本要改锚点，得走冻结项变更流程，不能随手升。
- **作业 jar 只放 `/opt/flink/usrlib/`**，`/opt/flink/lib` 不得有副本——
  否则父子类加载器各加载一份 Kafka 类，会触发 `LinkageError` 导致作业起不来。
- **agent 只本地 commit，不 push**（人工决定何时推送）。

### 已知的范围外

本项目**不做**这些（不是没做完，是刻意排除）：

- 不做多节点集群 / 高可用（单机可复现是目标）
- 不做数据权限与多租户
- 不做实时告警与运维监控体系
- 不接真实业务系统（数据全自产）
- GIF 演示录制留人工

---

## 12. 维护者速查

这一节给**改这个仓库的人**，不是给第一次跑的人。前面的章节是"怎么用"，这里是"怎么改并复核"。

### `make init` 四段的复核方式

四段**全部接线**（impl-02 起）——② `--profile doris up -d --wait`、③ `client` 容器内跑
`doris-init.sh`、④ `superset-init.sh`（`db upgrade` → `create-admin` → `init`）；
另有 ③ 增补 `superset-provision.sh`（看板置备）、收尾断言 `doris-assert.sh`
（经 `client` 容器查 ADS 真有行，不是只看建表成功）。

**复核**：读 `Makefile` 的 `init:` 目标；`make init` 的输出里**不应出现「待补」字样**。

### 健康检查的判据边界

`docker compose ps` 里状态不能一刀切：

| 服务 | 要求 | 原因 |
| --- | --- | --- |
| `kafka` / `flink-jm` / `doris-fe` / `doris-be` / `superset` | 必须 `healthy` | 定义了 healthcheck |
| `flink-tm` / `kafka-init` / `client` | 只要求 `Up` | **没有** healthcheck，不能要求 healthy |

误把无 healthcheck 的服务判成"不健康"，是历史上踩过的假红。

### 部署面判据（11 条）

具名载体是 [`scripts/deploy_surface_check.sh`](scripts/deploy_surface_check.sh)，
覆盖：具名卷同路径挂载、跨容器互见、down/up 数据仍在、作业 jar 目录、Paimon connector、
Flink 补丁锚点、`ADD JAR` 扫描、`.gitignore` 覆盖、七服务健康、数字标注审查。

```bash
bash scripts/deploy_surface_check.sh     # 退出码 0 = 11 条全 PASS
```

⚠️ **条数以脚本 `main()` 的调用清单为准**，头注声明可能滞后于演进。
复核：`grep -n 'check_' scripts/deploy_surface_check.sh` 对照 `main()`。

### 全部冻结端口（九个绑定，全部只绑 `127.0.0.1`）

| 端口 | 协议面 | 用途 |
| --- | --- | --- |
| `8030` | HTTP | Doris FE（`/api/health`） |
| `8040` | HTTP | Doris BE webserver（`/metrics`） |
| `8081` | HTTP | Flink Web UI |
| `8088` | HTTP | Superset 看板 |
| `9010` | Doris 内部 | Doris FE edit_log |
| `9030` | MySQL 协议 | Doris FE 查询入口 |
| `9060` | brpc | Doris BE |
| `9092` | Kafka 协议 | 网内 listener（advertised `kafka:9092`） |
| `29092` | Kafka 协议 | **HOST** listener（advertised `127.0.0.1:29092`，宿主进程用这个） |

复核：`grep -oE '127\.0\.0\.1:[0-9]+:[0-9]+' docker-compose.yml | sort -u`。

⚠️ `8030/8040/8081/8088` 是 HTTP 面，可用 curl 判；`9010/9030/9060/9092/29092` 是
MySQL / brpc / Kafka 协议，**不可**用 HTTP curl 判连通性（见 §8）。

### 运行产物落点

`artifacts/` 是 verify 报告的**设计落点**（L0 起约定，P1 起实际写入）：
`artifacts/verify/` 有逐次报告与脏率基线，`artifacts/spike/` 有尖峰报告。
该目录**不入库**（`.gitignore`）。

### 改判据时的纪律

- 判据**不能**用"退出码 0"当唯一证据——`sql-client.sh -f` 报错后仍退 0（源码事实）。
  离线断言用哨兵行，见 [`sql/README.md`](sql/README.md)。
- 每加一条判据，要同时想清楚**它怎么变红**。一个永远不可能失败的判据等于没有判据。
  （本项目在 2026-10-27 就抓到过三条这样的假绿守卫，教训写在
  [`impl-07`](.scratch/ecom-user-behavior-pipeline/issues/impl-07-panel-baseline-parse-and-interpreter.md) 票面。）
- 数字一律标 `官方给出` 或 `设计值`，**不得把设计值当官方保证**。

---

## 许可与状态

本仓库为私有项目，未附开源许可。所有验收证据与施工记录在
[`.scratch/ecom-user-behavior-pipeline/issues/`](.scratch/ecom-user-behavior-pipeline/issues/)，
进度总览见 [`impl-dag.md`](.scratch/ecom-user-behavior-pipeline/issues/impl-dag.md)。

---

<sub>本 README 面向第一次接触本项目的人。如果你发现某条与代码不符，
以代码和 `make verify` 的实测为准，并请提 issue 指出——文档漂移是本项目明令禁止的。</sub>
