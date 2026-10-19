# FSR — 电商用户行为数据全链路（离线 + 实时）

> **状态**：推荐路线已冻结，实现细节留给后续 issue。
> **日期**：2026-10-18（Asia/Shanghai）
> **性质**：可行性研究报告（Feasibility Study Report）。本文只回答「做什么路线、为什么、还有什么没定」，不写 DDL、不写作业代码、不写 compose 文件。

## 0. 这份文档怎么用

| 你要做的事 | 读哪节 |
| --- | --- |
| 开始实现前确认技术栈与版本 | §3 冻结路线、§4 版本锚点矩阵 |
| 忘掉某个组件为什么被排除 | §3 候选与排除证据（**完整来源见 `research-notes.md`**） |
| 卡在内存 / 容器起不来 | §2 平台天花板与资源预算 |
| 写埋点、分层、指标口径 | §5 数据与口径决策、§6 分层与存储形态 |
| 上线前确认还剩什么没定 | §7 未决项、§8 风险登记 |
| 准备面试问答 | **`interview-prep.md`**（独立文档） |

两条贯穿全文的「锚点」概念，读的时候带着它们：

- **天花板**：本机 WSL2 给容器群的内存上限（见 §2）。任何加组件、加副本的提议，先问它吃多少天花板。
- **锚点**：每个组件的精确版本号（见 §4）。可复现性 = 锚点不漂移。

## 1. 目标、成功判据、非目标

### 目标

搭建一条**单机一条命令可复现**的电商用户行为数据全链路：离线数仓分层 + 实时流处理 + 实时看板，作为大数据开发岗位的能力证明与面试问答素材。

### 成功判据（全部成立才算完成）

1. 干净机器上克隆仓库，一条命令（`docker compose up` + 初始化脚本）拉起整条链路。
2. 实时看板持续刷新，指标由生成器经全链路无人工干预地流到看板。
3. 离线链路完成五层分层建模，结果可被 OLAP 查询。
4. 每层的建模理由写成文档，且能覆盖面试问答（见 `interview-prep.md`）。
5. 以上均有 README + 演示截图/GIF 佐证。

### 非目标（明确裁掉，裁掉理由本身是面试素材）

- 不做多节点 / 高可用集群，不做 K8s。
- 不引入调度器容器（DolphinScheduler / Airflow），改用 Makefile + 脚本 + 手动触发。理由：每个调度器常驻要额外 2~3 GB 天花板，16 GB 机不划算；「生产会用调度器、此处为可复现性裁掉」在面试里同样讲得清。
- 不做安全与权限体系、不做真实支付对接、不做生产级监控告警（只留基础日志与 Flink Web UI）。
- **Spark 不常驻**（见 §3）：它是可选 profile，只在做离线回放时按需拉起，用完即停。
- **Iceberg 只作对照**，不实现。理由：主线实时链路是 Flink + Paimon，Iceberg 的流式 upsert / changelog 能力弱于 Paimon 主键表，实现它要多拼一层组件、多占一份天花板。
- 亿级回放是 **P2 可选里程碑**，不是作品集成立的前提（见 §9）。

## 2. 平台天花板与资源预算

### 本机事实（用户确认）

- 6 核 12 线程 / 16 GB 内存 / Docker Desktop（WSL2 后端）已装且可用。

### 天花板（官方口径 + 实测，来源见 `research-notes.md` §0）

- **WSL2 的 `memory` 官方默认值 = 宿主机总内存的 50%**；本机 16 GB 实测默认 **7.72 GB**（`MemTotal`）。`processors` 默认等于全部逻辑核（12，不封顶）。
- **2026-10-19 已执行 U1**：`memory=12GB`，实测天花板由 7.72 GB → **11.68 GB**（宿主 FreeRAM 6.36 GB，未压垮 Windows）。
- **WSL2 后端下 Docker Desktop 不提供 CPU/内存滑块**，资源只能在 WSL 层配置（官方原文："In WSL 2 mode, configure memory, CPU, and swap limits on the WSL 2 utility VM"）。
- 官方**未给出** `memory` 的数值上限，也未说明超设物理内存的后果。
- 容器默认**无资源约束**，超限即 OOM kill（官方明确 "Any process is subject to killing, including Docker"）。

### 由此得到的约束

常驻容器群预算压进 **≈8 GB**，并在 compose 里**逐个服务显式写 `mem_limit`**（不给任何容器放任生长的机会）。天花板现在为 11.68 GB，对 8 GB 预算留出约 3.7 GB 余量（WSL2 自身 + Docker Desktop 自身 + 波动），预算与天花板不再正面相撞。

> **既有决定冲突披露**：改动前 `.wslconfig` 注释写着「不设 `memory=` 硬顶：保 Spark 500 万行性能基准余量（ADR-0005，offline-05+ 再评估）」，而仓库内**不存在** ADR-0005 文件（`docs/adr/` 至今未创建，全仓 grep 无引用）。判定本次改动**不冲突**：不设值时默认天花板即 7.72 GB，**低于** 8 GB 预算；设 12 GB 是**放宽**天花板（对 Spark 基准有利），且 `memory=` 只占物理内存、不新增 C 盘占用（`swap=2GB` 未动）。

### 常驻预算分配（设计目标，非官方数字）

| 服务 | 角色 | 预算 |
| --- | --- | --- |
| Kafka（KRaft 单容器） | 消息队列 | ~1.0 GB |
| Flink JobManager + TaskManager | 流处理 | ~2.0 GB |
| MinIO（可选 profile）/ 本地文件系统 | 湖存储载体默认本地 | 0（默认）/ ~0.4 GB |
| Doris FE + BE | OLAP | ~3.0 GB |
| Superset | 看板 | ~1.5 GB |
| 其余（生成器、初始化脚本） | — | ~0.5 GB |
| **合计** | | **≈8.0 GB** |

> 这些是**设计预算**，不是任何官方给出的最低要求。官方未给出的数字见 `research-notes.md` §6，实现阶段以实测为准。

## 3. 冻结的推荐路线

### 推荐路线（已冻结）

```
Python 生成器(Faker, MIT)
   │  live 模式：按速率写 Kafka
   ▼
Kafka 4.3.1（KRaft，无 ZooKeeper）
   ▼
Flink 2.2.x  ← 唯一常驻计算引擎
   │  实时：1 分钟窗口聚合 → Flink-Doris-Connector 写入
   │  离线：批模式跑五层分层，落 Paimon
   ▼
Paimon 2.1.0（流式湖仓主存储，五层全落这里）
   ▼
Doris 4.1.5（OLAP，看板供数）
   ▼
Superset 6.1.0（看板）
```

看板供数**主路径 = Flink-Doris-Connector 26.3.0 写 Doris 内表**：两侧版本组合（Flink 2.2.x ↔ connector 26.3.0 ↔ Doris 4.1.5）都是官方支持过的。Doris 的 Paimon Catalog 直读（zero-ETL）降级为**可选实验**，理由见 §8-R1。

### 候选与排除证据

完整一手来源见 `research-notes.md` §5 证据矩阵。

| 位置 | 选中 | 排除的候选 | 排除的决定性证据（一句话） |
| --- | --- | --- | --- |
| 消息队列 | Kafka 4.3.1 | Pulsar 5.0.0 LTS | 同为 Apache-2.0、同样活跃，但 Kafka+Flink 生态与岗位覆盖面更广；Pulsar 留作「为什么不用它」的对照 |
| 流处理 | Flink 2.2.x | Flink 2.3.0 | 官方文档原话：2.3 **尚无已发布 Kafka connector**（connector 5.0.0 只覆盖 2.1.x/2.2.x） |
| 湖存储 | Paimon 2.1.0 | Iceberg 1.12.0 | Iceberg 生态更广、认知度更高，但流式 upsert / changelog 弱于 Paimon 主键表，实时链路要多拼一层 |
| 离线批 | Flink 批模式 + 可选 Spark 4.2.0 profile | Spark 常驻、Hive 作引擎 | 16 GB 跑不起双引擎常驻；Hive 4.x 官方重心已转向 Metastore 与 Iceberg 集成，单机跑 HiveServer2+Tez 复杂度高收益低 |
| OLAP | Doris 4.1.5 | StarRocks 4.1.3 | 官方 Flink connector v1.2.16 只有 flink-1.16~1.20 的 jar，**无 Flink 2.x artifact**，与 Flink 2.2.x 不相容；其 Paimon Catalog 为 Beta 只读 |
| | | ClickHouse 26.7 | **无 Paimon 支持**、官方未给出 Flink connector；官方 sizing 下限「不应低于 8 GB」单一项即吃满预算 |
| 看板 | Superset 6.1.0 | Grafana | **AGPLv3**（官方公告 2021-04-20 由 Apache-2.0 迁移）；官方未给出 Doris/StarRocks 数据源插件 |
| | | DataEase v3.1.0 | `LICENSE` 为 modified GPLv3 + 附加条件（不得改 LOGO、厂商可变更协议），不符合「仅宽松许可」口径 |

> **StarRocks 许可证更正**：官方 `LICENSE.txt` 是 **Apache-2.0** 全文，「Elastic License」的流传说法在其一手文件上不成立。它被排除的真实原因是**版本相容性**，不是许可证——这条区分在面试里要讲准。
>
> **Grafana 的 AGPL 边界**：第 13 条的源码义务只在「修改该程序并经网络对外提供」时触发；本机自用、不改、不对外分发**实际无义务**。它被列为「内存实测不足时的可替换项」——优势是官方最低 512 MB（Go 栈，远轻于 Superset 的 Python 栈，而 Superset 官方**未给出**内存数字）。是否采纳取决于 §2 的实测结果。

### 湖存储载体

- **默认走本地文件系统（`file://`）**：零额外组件、零额外天花板。
- **MinIO 作可选 profile**：需要演示 S3 协议 / 对象存储场景时按需拉起（`--profile minio`），约 +0.4 GB。
- 两者配置可切换，默认 file。

## 4. 版本锚点矩阵

**锚点 = 精确 pin**，不跟 `latest`。可复现性的核心就是这列不漂移。

| 组件 | 锚点版本 | 出处（调研时点的稳定版） |
| --- | --- | --- |
| Kafka | **4.3.1** | 官方 downloads 页（2026-06-25 发布） |
| Flink | **2.2.x**（取该线最新补丁） | 官方 downloads；为 Kafka connector 可用线 |
| Flink Kafka connector | **5.0.0** | 官方 connector 清单，覆盖 Flink 2.1.x / 2.2.x |
| Paimon | **2.1.0** | 官方 releases（2026-10-12），官网标注 2.1 为 stable |
| Doris | **4.1.5** | 官方 release（2026-10-15） |
| Flink-Doris-Connector | **26.3.0** | 官方 release（2026-09-21），README 版本表列出支持 2.2.x |
| Superset | **6.1.0** | 官方 release（2026-05-13） |
| Python 生成器 | Python 3.11+ / **Faker（MIT）** | PyPI Faker 许可证 |

**版本相容性（这是必须一起看的链条）**：

```
Flink 2.2.x  ── Kafka connector 5.0.0 ── Kafka 4.3.1
     │
     └── Flink-Doris-Connector 26.3.0 ── Doris 4.1.5 ── Superset 6.1.0
```

## 5. 数据与口径决策

### 数据来源（混合）

- **实时链路**：自研 Python 生成器（`live` 模式），字段完全自主。
- **离线链路**：同一生成器的 `dump` / `replay` 模式产出，另可选用公开数据集回放。
- **公开数据集（可选加成）**：阿里天池 UserBehavior（约 1 亿条交互）。
  - **仓库零原始数据**：只放下载脚本 + md5 校验说明 + 许可署名，原始文件不入 git。
  - 理由：该数据集许可为 **CC BY-NC-SA 4.0**（非商业 + 相同方式共享），且需天池登录（默认审批、学生认证可免审，官方未给出免登录直链）。
  - 同时：它只有 5 列（User、Item、Category、Behavior、Timestamp），**没有金额与地域**，单靠它做不出丰富的 ADS 指标——这正是生成器必须存在的原因。

### 事件模型口径

- **一套超集 schema**，事件命名对齐 **GA4 电商事件规范**（`page_view` / `view_item` / `add_to_cart` / `begin_checkout` / `purchase`），字段含 `amount` / `province` / `channel`。
- UserBehavior 的 `pv` / `cart` / `fav` / `buy` 是这套 schema 的**子集**，映射进同一套表，因此实时流与离线回放能落到同一张 DWD 上对账。
- 面试可讲点：schema 不是瞎编的，有 GA4 / Snowplow iglu / Segment 三份官方规范可引。

### 指标与计算口径

| 口径 | 决策 | 理由 |
| --- | --- | --- |
| 转化漏斗 | **4 步主口径**：浏览 → 加购 → 下单（`begin_checkout`）→ 支付 | 「下单未支付」是漏斗最有价值的环节，只有生成器能产出 |
| 回放数据漏斗 | 降级为 **3 步**，文档写明差异 | UserBehavior 无 `begin_checkout`；差异本身是可讲的「口径一致性」话题 |
| UV 去重 | **实时近似 UV（HyperLogLog / Theta sketch）+ 离线精确对账**，文档量化误差 | 全量去重状态会顶满天花板；近似 + 对账既省资源又咬住深挖点 A |
| 窗口粒度 | **1 分钟滚动窗口**，看板 5~10s 轮询 | 状态压力可控、刷新观感够；5~10s 窗口在单机小流量下无收益 |
| 时间口径 | 存储统一 **UTC epoch 毫秒**，展示层转 Asia/Shanghai(+08)，Watermark 基于事件时间 | 避免时区混淆；UserBehavior 是秒级 epoch，转毫秒是 ODS→DWD 清洗的可讲细节 |
| 生成器速率 | 默认 **≈200 events/s，可配置**；另带大促尖峰模式（短时 ×5~10） | 1 分钟窗口约 1.2 万事件，看板跳动明显、Paimon 写入压力可控；尖峰是背压/乱序的现成素材 |

### 实时深挖点

- **主深挖**：A 乱序与 Watermark（`withIdleness`、允许延迟、侧输出）；B 端到端 exactly-once（checkpoint + 两阶段提交 + 幂等 sink）。
- **延伸提及**：C RocksDB 大状态与增量 checkpoint；D 维表时态 join（Lookup join / 维表更新）。

## 6. 分层与存储形态

### 五层模型（全部落 Paimon，单一 catalog）

| 层 | 内容 | Paimon 表形态 |
| --- | --- | --- |
| **ODS** | 原始事件保真，含原始 JSON payload 一列 | **append-only 表** |
| **DWD** | 清洗 + 规范化后的事件明细（统一 UTC ms、超集 schema） | 主键表 |
| **DIM** | 维表（商品 / 品类 / 用户 / 地域 / 渠道） | **主键表做时态维表** |
| **DWS** | 主题宽表聚合（按用户 / 商品 / 品类 / 时间粒度） | 主键表 |
| **ADS** | 看板直接消费的应用层指标 | 主键表 |

- 单一 catalog 的好处：组件最少、元数据统一、ODS 保真且可重放。
- 建模决策可讲点：**为什么 ODS 用 append 表、DWD 起才用主键表**——ODS 要保真与可重放（不去重、不合并），DWD 起才需要 upsert 与 changelog 语义。

### 离线调度

不引入调度器容器。用 **Makefile + 脚本 + 手动触发**编排分层任务，README 写明「生产环境此处应接调度器，为 16 GB 可复现性裁掉」。

## 7. 未决项（需要用户亲手确认的改动）

按用户既定规矩：**任何本地文件/配置改动，先征得同意、先备份、说明回滚方案，再执行。** 以下两项尚未执行。

| # | 改动 | 现状 | 需要的动作 |
| --- | --- | --- | --- |
| U1 | `%UserProfile%\.wslconfig` 的 `memory` 提到 12 GB | **已执行**（2026-10-19，用户授权） | 备份 → diff → `wsl --shutdown` → 验证 `MemTotal` 7.72 → **11.68 GB**；回滚 = 恢复 `.wslconfig.bak-2026-10-19-before-u1` + `wsl --shutdown` |
| U2 | 仓库根 `.gitignore`（挡 `data/`、`*.zip`、`*.log` 等） | **已执行**（2026-10-19，用户授权） | 已创建并验证；回滚 = 删除该文件 |
| U4 | 仓库尚无任何提交，已配远程 `origin` | **待授权** | 首次提交与推送需用户单独授权（推送属对外动作） |

> 依据（U1）：WSL2 `memory` 官方默认 = 宿主机 50% ≈ 8 GB，与常驻预算同量级；官方要求改后 `wsl --shutdown` 生效，取值必须带单位（如 `memory=12GB`）。
> 备注：2026-10-18 会话因沙箱 ACL 故障与 `ops_bash` 缺 IPC 兼容凭据无法执行命令；**2026-10-19 会话改为 `danger-full-access` 后命令已可正常执行**，已核实该仓库 git 仓库已初始化（`main` 分支）、**尚无任何 commit**、远程 `origin` 指向 `github.com/Gaomen-7/eco-d-a-platform`。

## 8. 风险登记

| ID | 风险 | 证据 | 处置 |
| --- | --- | --- | --- |
| **R1** | **Doris Paimon Catalog 官方依赖 Paimon 1.4.2，与锚定的 Paimon 2.1.0 存在版本落差**，官方未声明支持 2.x | `research-notes.md` §3（Doris Paimon Catalog 文档 "Supported Paimon Versions"） | 看板供数**主路径改走 Flink-Doris-Connector 写 Doris 内表**（两侧版本均官方支持）；Catalog 直读降级为**可选实验**，风险写入本文 |
| **R2** | Flink 2.3.0 已发布但**无 Kafka connector**；未来升 2.3 会断链 | 官方 connector 文档原话 "There is no connector (yet) available for Flink version 2.3" | 锚定 2.2.x；升级前先查 connector 清单 |
| **R3** | **天花板相撞**：WSL2 默认内存 7.72 GB < 8 GB 常驻预算，实测可能 OOM | `research-notes.md` §0 官方默认值 + 本机实测 | **已缓解**：U1 已于 2026-10-19 执行，天花板升至 11.68 GB；仍逐服务写 `mem_limit`；若仍不足，按序降级：MinIO→本地文件、Superset→Grafana（见 §3 可替换项） |
| **R4** | 大量官方**未给出**内存数字（Kafka / Flink / Doris / Superset 单机最低要求） | `research-notes.md` §6 官方未给出清单 | **不得以估算值写入文档**；以实测填充 |
| **R5** | UserBehavior 许可为 CC BY-NC-SA 4.0，NC/SA 会约束作品集后续商用 | 天池数据集 649 页 License 节 | 仓库零原始数据（只放脚本 + 署名 + 许可说明） |
| **R6** | bind mount 走 Windows 文件系统会严重掉性能 | 官方：Windows 文件系统挂载性能远低于 Linux 文件系统，且 inotify 事件不投递 | 数据卷放 Linux 文件系统侧，避开 `/mnt/c/...` 挂载 |
| **R7** | Paimon 2.x 演进快，配置/行为变更频繁 | 官方 release notes 中大量行为变更 | 锚定 2.1.0 stable，不跟 master |
| **R8** | 沙箱 ACL 故障 + `ops_bash` 缺 IPC 凭据，命令执行受限 | 本会话实测报错 | **已解除**：2026-10-19 会话改为 `danger-full-access` 后命令正常，无需修 ACL |

## 9. 里程碑骨架

| 阶段 | 内容 | 完成判据 |
| --- | --- | --- |
| **P0 环境** | 修沙箱 → 确认 git → U1/U2 → compose 骨架（Kafka+Flink+MinIO profile）+ `mem_limit` 全量写入 | `docker compose up` 拉起骨架，`docker stats` 总占用 ≤ 8 GB |
| **P1 实时闭环** | 生成器 live → Kafka → Flink 1 分钟窗口 → Connector 写 Doris → Superset 看板 | 看板指标持续刷新，端到端 exactly-once 可演示 |
| **P2 离线分层** | 生成器 dump/replay → Flink 批模式跑 ODS→DWD→DIM→DWS→ADS → Paimon | 五层表可查，分层理由文档成稿 |
| **P2（可选）亿级回放** | UserBehavior 下载脚本 + 全量/抽样回放 | 抽样跑通即可；全量 1 亿条由用户按磁盘/时间决定是否执行 |
| **P3 文档** | README + 架构图 + 数仓分层文档 + 演示 GIF + `interview-prep.md` 完整化 | 成功判据 5 条全绿 |

## 10. 证据指针

- **完整调研证据**：同目录 `research-notes.md`（5 路调研，每条结论带一手来源 URL；含证据矩阵与「官方未给出清单」）。
- **面试问答素材**：同目录 `interview-prep.md`。
- **仓库约定**：`AGENTS.md`、`docs/agents/issue-tracker.md`（issue 与 spec 存放约定）、`docs/agents/domain.md`（域文档布局：`GLOSSARY.md` + `docs/adr/`，**尚未创建，按需生成**）。

## 11. 交付物清单

1. Git 仓库：`docker-compose.yml`（含 profile）、生成器、Flink 作业代码、五层 DDL、看板配置。
2. 架构文档 + 数仓分层设计文档。
3. README（含演示 GIF 与一键启动说明）。
4. `interview-prep.md`（面试问答，独立成文）。
5. 简历 bullet 3~5 条。

> 部署视频不做，GIF + README 足够。
