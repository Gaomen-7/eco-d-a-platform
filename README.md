# eco-a-d-platform

单机可复现的电商用户行为湖仓：Kafka → Flink → Paimon/Doris → Superset。

## 前置

- **Docker daemon**（含 Docker Compose v2）。
- **WSL2**：全部命令在 WSL2 内执行（Windows 侧 uv / mvn / JDK 版本不对，勿用）。

## 一条命令：`make init`

`make init` 是「一条命令」的唯一具名者，内容冻结为四段（`reproducibility-boundary.md` §2），顺序不可换：

| 段 | 动作 | 说明 |
| --- | --- | --- |
| ① | `docker compose up -d` | 默认 profile：Kafka / Flink JM+TM / Superset；`kafka-init` 由 `depends_on` 自动执行 |
| ② | `docker compose --profile doris up -d --wait` | 默认 `up` **不含** Doris，缺此段链路不完整 |
| ③ | `docker compose run --rm client /scripts/doris-init.sh` | Doris 建库建表必须从 compose 网内发起（Docker Desktop 两跳约束） |
| ④ | Superset 初始化：`superset db upgrade` → `fab create-admin` → `superset init` | `SUPERSET_LOAD_EXAMPLES=no` |

**现状（诚实声明）**：`make init` 目前只接好了 ①；②③④ **尚未接线**——`init` 依赖的 `up` 只跑默认 profile 的 `docker compose up -d`，Makefile 中没有任何 `--profile doris`（故 ② 的 Doris 拉起缺失），③④ 仅打印「待补：Doris 建库/建表、Superset db upgrade + create-admin + init」，且该提示亦未提及 ② 缺的 `--profile doris` 一步。四段是冻结的契约，不是已完成的实现。

判据：在已有 Docker + make 的机器上（复现档位 L0），`make init` 后 `make verify` 退出码 0；`docker compose ps` 中**定义 healthcheck 的服务**（`kafka` / `flink-jm` / `doris-fe` / `doris-be` / `superset`）须 `healthy`，其余无 healthcheck 者（`flink-tm` / `kafka-init` / `client`）只要求 `Up`。

## 端口（全部只绑 `127.0.0.1`）

全部八个（来源：`docker-compose.yml` 各行内联注释）：
`8030` Doris FE HTTP（`/api/health`）· `8040` Doris BE webserver（`/metrics`）· `8081` Flink UI · `8088` Superset · `9010` Doris FE edit_log · `9030` Doris FE MySQL 协议 · `9060` Doris BE brpc · `9092` Kafka

## 八个冻结目标（SRS §3.4，名字不得改）

| 目标 | 作用 |
| --- | --- |
| `up` | 拉起默认 profile 的全部服务（不含 Doris） |
| `down` | 停止并移除容器（不加 `-v`，保留卷） |
| `init` | 一条命令：四段初始化（见上） |
| `replay` | 五层离线分层 ODS→DWD→DIM→DWS→ADS（P2 实现） |
| `verify` | 链路运行时五项烟测（退出码判成败；其中 3/5 Paimon、4/5 Doris 现仅打印 `PENDING`，P1/P2 前**不参与**判据，故当前实际只有 3 项 gate 退出码） |
| `spike` | 尖峰模式：短时 ≥1000 events/s（P1 实现） |
| `verify-fault` | kill TaskManager → 从 checkpoint 恢复 → 校验条数一致 |
| `logs` | `docker compose ps` + 最近 120 行日志 |

设计面目标（非冻结）：`lint` / `typecheck` / `client` / `config-check`。

## 设计链（从这里进）

定位与前沿看 `.scratch/ecom-user-behavior-pipeline/map.md`——它是 wayfinder 地图；`map.md` 与 `AGENTS.md` **优先于** HLD §0.1 自称的「唯一入口」。要「为什么」时再往下读：

- `.scratch/ecom-user-behavior-pipeline/SRS.md`（必须做到什么）→ `gdd.md`（每个功能怎么做）→ `HLD.md`（结构、技术栈定案、运行约束）
- `GLOSSARY.md`：术语与反例（说「版本的唯一权威」，不说「唯一真相源」）
- `docs/adr/`：架构决定（ADR-0001 工具链版本权威是 uv.lock；ADR-0002 Flink 2.2.1 补丁锚点）
- `.scratch/repo-skeleton/spec.md`：本骨架的施工规格与验收接缝 S1–S4；`sql/README.md` 指向五层形状表

## 纪律

- **只本地 commit，绝不 push**。
- 仓库零原始数据、零密钥：`.env` 是机密面，不入库、勿读勿抄。
- 八个冻结目标名、端口、库名、topic 名一概不改；上游 `SRS.md` / `gdd.md` / `HLD.md` / `init.sql` 一字不动。
- 运行产物（`artifacts/`）是 verify 报告的**设计落点**（L0 起约定，P1+ 才真正写入），不入库。
