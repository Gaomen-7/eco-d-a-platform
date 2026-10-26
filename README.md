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

**现状（诚实声明）**：四段**全部接线**（impl-02 起）——② `--profile doris up -d --wait`、③ `client` 容器内跑 `doris-init.sh`、④ `superset-init.sh`（db upgrade → create-admin → init）；另有 ③ 增补 `superset-provision.sh`（看板置备，impl-04）与收尾断言 `doris-assert.sh`（经 client 查 ADS 真有行）。impl-05 实测 L0 档 `make init` real **2m37.7s** 四段全过、`doris-assert` 全 PASS（`reproducibility-boundary.md` §11.2）。复核：读 `Makefile` 的 `init:` 目标；`make init` 输出无「待补」字样。

判据：在已有 Docker + make 的机器上（复现档位 L0），`make init` 后 `make verify` 退出码 0；`docker compose ps` 中**定义 healthcheck 的服务**（`kafka` / `flink-jm` / `doris-fe` / `doris-be` / `superset`）须 `healthy`，其余无 healthcheck 者（`flink-tm` / `kafka-init` / `client`）只要求 `Up`。

## 端口（全部只绑 `127.0.0.1`）

全部八个（来源：`docker-compose.yml` 各行内联注释）：
`8030` Doris FE HTTP（`/api/health`）· `8040` Doris BE webserver（`/metrics`）· `8081` Flink UI · `8088` Superset · `9010` Doris FE edit_log · `9030` Doris FE MySQL 协议 · `9060` Doris BE brpc · `9092` Kafka

## 已知环境限制（宿主实测，随时间漂移）

- **Windows 宿主 WinNAT 的动态端口排除段会漂移**，历史上多次覆盖冻结端口：
  2026-10-22 覆盖 `8030/8040/8081/8088`（[impl-03a §C](.scratch/ecom-user-behavior-pipeline/issues/impl-03a-p2-deployment-surface.md)，非管理员无法释放）；
  2026-10-26 收尾盘点时八个冻结端口中仅 `8081` 可达；同日晚复核时排除段已漂走、
  八个端口全部恢复 TCP 可达。⇒ 「端口连不上」先查排除段再下结论——这是宿主环境
  事实，不是仓库缺陷。
- **`make spike` 从宿主真发走 HOST listener `127.0.0.1:29092`**（impl-06g）：
  compose 的 `9092` 口 advertised 成 `kafka:9092`（容器名），宿主解析不了
  （P0 实测 FAIL）；`29092` 才是 advertised 成宿主可达地址的那个 listener。
  这解释了旧 `spike:` 目标「真发需 compose 网内可达 kafka:9092」注释的误导面。
- **2026-10-26 收尾门实证（[impl-06f-4](.scratch/ecom-user-behavior-pipeline/issues/impl-06f-4-closeout-gate-deploy-proof.md)）**：
  不设 `COMPOSE_EXTRA_FILES` 跑全判据 + `make init`→`verify` 一绿的同一窗口内，八个
  冻结端口实测：HTTP 面 `8030/8040/8081/8088` 全 200、协议面 `9010/9030/9060/9092` 全
  TCP OPEN、`docker port` 映射逐个与 compose 声明一致——impl-03a §C「冻结端口」遗留项
  即以此销项。该实证只代表**当时**排除段状态，再次漂移时仍以上一条的复核命令为准。
- 复核：`netsh interface ipv4 show excludedportrange protocol=tcp`（Windows 宿主）＋
  逐端口 TCP 探测。⚠️ `curl http://127.0.0.1:<port>` 对 `9010/9030/9060/9092`
  （MySQL / brpc / Kafka 协议）会报「连不上」但 TCP 层可达——**协议层失败 ≠ 端口
  不通**，勿用 HTTP curl 判这四个端口的连通性。
- 端口真被排除时的处置先例：`COMPOSE_EXTRA_FILES` 叠加只改宿主侧发布端口的
  override（impl-03a §C，落 `.scratch/tmp/`）；端口恢复后须**不设**该变量复跑
  （impl-03f 已按此归位）。

## 八个冻结目标（SRS §3.4，名字不得改）

| 目标 | 作用 |
| --- | --- |
| `up` | 拉起默认 profile 的全部服务（不含 Doris） |
| `down` | 停止并移除容器（不加 `-v`，保留卷） |
| `init` | 一条命令：四段初始化（见上） |
| `replay` | 五层离线分层 ODS→DWD→DIM→DWS→ADS（P2 实现） |
| `verify` | 链路运行时五项烟测＋附加断言（P2 起五项**全真断言**，退出码判成败；第三项「Paimon 五层有行」查不到即 **FAIL**，不是 SKIP）。复核：`src/ecomgen/verify_runner.py` 第三项 docstring；`make verify` 实测 10 PASS / 退出码 0（报告落 `artifacts/verify/`） |
| `spike` | 尖峰模式：短时 ≥1000 events/s。**已实测 1011.8 events/s**（2026-10-26，11134 条 / 11004 ms 含 flush，一手口径：分子=实际 produce 条数、分母=首条 produce 至 flush 完成的墙钟；报告 `artifacts/spike/`）。判据机读化：`make spike` 真发并判四项（速率/期间存活/lag 回落/DLQ 归因），`make spike DRY_RUN=1` 退回预演。链路面证据：尖峰期间 Flink 采样全 RUNNING、异常零新增、lag 44 s 回落、DLQ 增量 0（[impl-06g](.scratch/ecom-user-behavior-pipeline/issues/impl-06g-spike-live-rate-proof.md)） |
| `verify-fault` | kill TaskManager → 从 checkpoint 恢复 → 校验条数一致 |
| `logs` | `docker compose ps` + 最近 120 行日志 |

设计面目标（非冻结）：`lint` / `typecheck` / `client` / `config-check`。

## 部署面判据

部署面验收实证的具名载体是 [`scripts/deploy_surface_check.sh`](scripts/deploy_surface_check.sh)：
具名卷同路径挂载、跨容器互见、down/up 数据仍在、作业 jar 目录、Paimon connector、
Flink 补丁锚点、ADD JAR 扫描、`.gitignore` 覆盖、七服务健康、数字标注审查。
当前 **11 条**（①②③④ 4b 4c 4d ⑤⑥⑦⑧）——条数**以脚本 `main()` 的调用清单为准**
（头注声明的条数可能滞后于演进）。复核：`grep -n 'check_' scripts/deploy_surface_check.sh`
对照 `main()` 调用清单；`bash scripts/deploy_surface_check.sh` 退出码 0 = 全 PASS。

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
- 运行产物（`artifacts/`）是 verify 报告的**设计落点**（L0 起约定，P1 起已实际写入——`artifacts/verify/` 下有逐次报告与脏率基线），不入库（`.gitignore:36`）。
- **作业 jar 只放 `/opt/flink/usrlib/`，`/opt/flink/lib` 不得有作业 jar 副本**：作业 shade jar 含 `org/apache/kafka/**` 而 `flink-dist-2.2.1.jar` 不含，副本混入 `lib` 会使父子类加载器各加载一份 Kafka 类 ⇒ `LinkageError: loader constraint violation … committedOffsets`（[impl-06d](.scratch/ecom-user-behavior-pipeline/issues/impl-06d-r7i-replay-watermark-fixes.md) §四 环境事实 ④；由 `deploy_surface_check.sh` 条目 ④ 阳性＋阴性双向断言）。
