# impl-03a: P2 部署面 —— warehouse 具名卷同路径挂 JM/TM · Paimon jar 进每个 Flink 进程

> 父票：impl-03（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-03-p2-offline-layering.md`）
> 规格落点：spec §5.6「P2 部署硬约束（两条，HLD §5 未写过）」

Type: task
Status: done (2026-10-22)
Blocked by: impl-02

**What to build:** Flink 的 JM 与 TM 两个容器能通过**同一个容器内路径**读到同一份 Paimon warehouse，且两者都装上匹配的 Paimon bundled connector —— 不靠 `ADD JAR`、不靠共享 `/opt/flink/...`、不需要 JDK/Maven。这条通了，后面四张票（03b–03f）才有物理落点；不通则每一张都会以「表不存在 / 类找不到」的形式失败，白烧时间。

## 为什么单独成票

- 这是纯部署面，**不依赖任何 SQL**，且**可单独证伪**：卷挂错路径 / jar 只进了 JM，都能在本票内验完，不必等 03b 写完表才炸。
- 它是 03b 的物理前提，且失败模式最容易让后续所有票一起卡住 ⇒ 放在最前面。
- 违反的是**官方逐字要求**，不是本仓偏好：官方把 `file:` warehouse 背书到 "single-machine tutorial"，分布式集群须 shared storage；Paimon 侧逐字 "Install one matching Paimon bundled connector in **each** Flink process."。

## Acceptance criteria

- [x] 新增 named volume（形如 `paimon-warehouse`）挂进 **flink-jm 与 flink-tm 两个容器**的**同一路径**（catalog DDL 里写的那个路径）—— `paimon-warehouse:/warehouse` 两处逐字相同；实机 `docker inspect` 两侧 `Name=eco-a-d-platform_paimon-warehouse Destination=/warehouse` 完全一致（见 §A①②）
- [x] 实测证明两端看到的是**同一份数据**而非各自空目录：在 JM 侧写入一个探针文件后，TM 侧可读到该文件（JM/TM 容器内各查一次，留下输出）—— JM 写 `ts=2026-10-22T11:08:06Z rand=858 marker=impl03a`，TM 逐字读回（见 §A②）
- [x] `docker compose down`（**不带 `-v`**）后 `up`，volume 内数据仍在 —— 证明是具名卷而非容器内临时路径（见 §A③）
- [x] Paimon jar 在**构建期** `COPY` 进 `/opt/flink/lib`，**JM 与 TM 两个镜像/容器都验到**（两端 `ls /opt/flink/lib` 各查一次）—— 两端均见 57,697,871 B 的 `paimon-flink-2.2-2.1.0.jar`（见 §A④）
- [x] **没有**任何 `ADD JAR` 语句指向宿主路径（见 §A⑤）
- [x] 门槛批的 raw 数据落 `data/` 且被 `.gitignore` 覆盖；`git check-ignore` 验证通过（仓库内无原始数据，C5）—— `.gitignore:10:**/data/` 对 `data/` 与 `data/threshold/raw.parquet` 均命中（见 §A⑥）
- [x] `docker compose ps` 七服务（+新增服务后）仍全 `healthy`，未破坏 impl-02e 已验收的 P1 判据（见 §A⑦）
- [x] compose 里新增的**时间/容量数字**按 AGENTS.md 逐处标 `设计值` 或 `官方给出`（见 §A⑧）

## Notes

- **不跑 `down -v`**：会销毁 P0/P1 已验证的卷态成果；L0 零状态实测归 impl-05。
- 本票**不建任何表、不写任何 DDL**。`sql/` 目前只有 README（「本目录暂无 DDL」），DDL 从 03b 开始落。
- 官方未给出的，按 spec §8.3 登记、**不臆造**：Paimon `file://` 的 compaction 成本数字、分布式下 warehouse 的官方推荐存储形态。
- **不实现**（spec §8.2）：`minio` / `spark` profile（属未签字意图类 #36，归 impl-05）。
- 执行面：宿主无 `make`，须在 WSL 下跑；`docker` 命令需 `MSYS_NO_PATHCONV=1`。
- 反例提醒：本条与 impl-02e §S1 同源 —— **卷/挂载这类判据本身也可能恒真**（如挂了两个同名卷却各自独立）。故第 2 条验收要求的是「跨容器互见」的实证，不是「compose 文件里有这一行」。
---

## Answer（2026-10-22）

**结论：8 条验收全部通过，实证脚本 `scripts/deploy_surface_check.sh` 跑出 `9 PASS / 0 FAIL`（退出码 0），并做了反例证伪。**

### 落地物

| 文件 | 动作 | 说明 |
| --- | --- | --- |
| [docker/flink/Dockerfile](../../docker/flink/Dockerfile) | 新建 | 官方 flink:2.2-java17 + 构建期 `COPY` Paimon jar 进 `/opt/flink/lib` + **预建 `/warehouse` 并 chown 9999:9999**（实机取证后补，见 §B） |
| [scripts/fetch-paimon-jar.sh](../../scripts/fetch-paimon-jar.sh) | 新建 | 幂等预取 jar（size + sha256 双校验，`.part`→校验通过才 `mv`） |
| [docker-compose.yml](../../docker-compose.yml) | 改 | `paimon-warehouse` 具名卷；JM/TM 共用本地镜像 `${FLINK_IMAGE:-eco/flink:2.2-paimon}`，两处 `paimon-warehouse:/warehouse` 逐字相同 |
| [.gitignore](../../.gitignore#L52-L66) | 改 | 新增第 4b 节忽略 `docker/flink/lib/`（57 MB 二进制不入库） |
| [scripts/deploy_surface_check.sh](../../scripts/deploy_surface_check.sh) | 新建 | 8 条验收 + 4b 可写性的实证脚本（9 项判据，含反例设计说明） |

### §A 实机证据

**① 具名卷同路径（两层断言：compose 配置 + 实机 inspect）**
```
eco-flink-jm  Type=volume Name=eco-a-d-platform_paimon-warehouse Dest=/warehouse
eco-flink-tm  Type=volume Name=eco-a-d-platform_paimon-warehouse Dest=/warehouse
```
> 为什么两层：只查 compose 文件是**恒真判据**——若两处写的是两个不同卷名，compose 照样打印两行。故再取实机 Mounts 比对 `Name` 与 `Destination` 逐字相等。

**② 跨容器互见（本票核心）**
```
probe=/warehouse/.probe-20261022T110806Z-1939
payload=ts=2026-10-22T11:08:06Z rand=858 marker=impl03a
JM_WRITE_OK
JM=[ts=2026-10-22T11:08:06Z rand=858 marker=impl03a]
TM=[ts=2026-10-22T11:08:06Z rand=858 marker=impl03a]
RESULT: CROSS_CONTAINER_IDENTICAL ✓
```
> 探针名带 UTC 时间戳 + 随机数、内容带时间戳 + 随机数 + 标记，TM 逐字比对。不以「文件存在」为判据。

**③ down（不带 -v）→ up 后数据仍在**
```
=== 卷是否仍在 ===  eco-a-d-platform_paimon-warehouse
BACKUP=[ts=2026-10-22T11:08:06Z rand=858 marker=impl03a]
AFTER =[ts=2026-10-22T11:08:06Z rand=858 marker=impl03a]
RESULT: PERSISTED_ACROSS_DOWN_UP ✓
```
> down 前先把探针内容备份到宿主（down 后卷内取不到），up 后读同一路径逐字比对。脚本内 `compose_down_preserving_volumes()` 会**逐字扫描参数**，发现 `-v`/`--volumes` 即拒绝执行。

**④ jar 在两端**
```
eco-flink-jm  -rwxr-xr-x 1 root root 57697871 Oct 22 10:00 paimon-flink-2.2-2.1.0.jar
eco-flink-tm  -rwxr-xr-x 1 root root 57697871 Oct 22 10:00 paimon-flink-2.2-2.1.0.jar
```
镜像内 sha256 与宿主 jar **字节级一致**：`43a6416a…f859107dd1`。

**⑤ 无 ADD JAR 指向宿主路径**
全仓 15 处 `ADD JAR` 字样**全部是注释或检测脚本自身**，零可执行语句。

**⑥ data/ 被 .gitignore（C5）**
```
.gitignore:10:**/data/	data/
.gitignore:10:**/data/	data/threshold/raw.parquet
```

**⑦ 七服务**
```
client|exited|            ← profile 隔离按需容器，不计入
doris-be|running|healthy
doris-fe|running|healthy
flink-jm|running|healthy
flink-tm|running|healthy
kafka|running|healthy
kafka-init|exited|        ← 一次性服务，判 ExitCode=0（实测 State=exited ExitCode=0）
superset|running|healthy
```
> **判据坑**（impl-02e 踩过，本票复核）：kafka-init 是 `restart: "no"` 的一次性服务、**没有 healthcheck**，要求它 `healthy` 结构性不可能。故拆成两条判据：(i) 常驻且有 healthcheck 的 6 个必须 healthy；(ii) kafka-init 必须已成功退出。

**⑧ 数字标注**
compose / Dockerfile 新增处逐条标 `官方给出` / `设计值`：uid/gid `9999` 标 `官方给出`（官方 flink 镜像的 flink 用户，实测 `id` 输出）；权限 `755` 标 `设计值`；jar size/sha256 标「本仓实测钉死的设计值，**非官方保证**」（Paimon 官方未公布该 artifact 校验值）。

### §B 反例证伪（证明判据不是恒真）

**B-1 权限反例**：把 `/warehouse` 改成 `chmod 555` 后重跑脚本：
```
[FAIL] 4b /warehouse 对 flink 用户可写（eco-flink-jm 侧写入被拒 rc=2 => flink 用户对 /warehouse 不可写）
[FAIL] 4b /warehouse 对 flink 用户可写（eco-flink-tm 侧写入被拒 rc=2 => flink 用户对 /warehouse 不可写）
[FAIL] ② 跨容器互见（JM 写 / TM 读）（JM 侧写入失败）
汇总：6 PASS / 5 FAIL   → 退出码 1
```
权限改回后即恢复 `9 PASS / 0 FAIL`。⇒ 4b 与 ② 确实在断言，不是恒真。

**B-2 实机发现并修掉的真实缺陷（票面未写，但会让 03b 起全线失败）**

首次跑 ② 就撞到：
```
docker exec eco-flink-jm sh -c 'echo x > /warehouse/probe'
  → sh: 1: cannot create /warehouse/.probe-...: Permission denied
```
**根因**：官方 flink 镜像以 `uid=9999(flink)` 运行，而 Docker **具名卷首次挂载时以 root:root 建目录**（实测 `ls -ldn /warehouse` → `drwxr-xr-x 2 0 0`）⇒ flink 用户只有 r-x、没有 w。**Paimon 要在 `/warehouse` 下建 database/表目录并写 bucket 文件 ⇒ 必然失败**，而且失败形态是「表不存在 / 类找不到」，与本票要防的形态一模一样。

**修法**：Dockerfile 里 `USER root; mkdir -p /warehouse && chown -R 9999:9999 /warehouse; USER flink`。Docker 新建具名卷时会把镜像内该路径的属主/权限复制进卷，故新卷天然可写（实机验证：全新卷 + 本镜像 → `ls -ldn` = `9999 9999`，写入成功）。
> ⚠️ **只对新建卷生效**；已存在的 root 属主卷不会因重建镜像而改变，需手工 `docker run --rm -v <vol>:/w alpine chown -R 9999:9999 /w` 修一次。本项目 P2 首次启用的卷都是新建，故本条足够。

⇒ **「挂对了卷」不等于「能写」**。故新增 ④b 判据，作为本票的独立验收项。

### §C 环境阻塞（如实登记）

**本机 WinNAT 排除了 TCP 8024–8123 段**，覆盖冻结端口 8030/8040/8081/8088：
```
netsh interface ipv4 show excludedportrange protocol=tcp
      8024        8123
```
非管理员无法释放（`Restart-Service winnat` → `Cannot open winnat service`）；本会话禁止提权。

**已做反事实取证，证明与本票改动无关**：用**改动前**的 `git show HEAD:docker-compose.yml` 起 superset，同样报
```
Error response from daemon: ports are not available: exposing port TCP 127.0.0.1:8088 ...
```
⇒ 环境漂移，不是 impl-03a 引入的回归（会话刚开始时七服务曾全 healthy）。

**处置**：功能性实证通过 `COMPOSE_EXTRA_FILES` 叠加一个**只改宿主侧发布端口**的临时 override 完成（容器内端口、卷挂载、mem_limit、healthcheck、镜像名**一律不动**）。该 override 落在 `.scratch/tmp/`（已被 `.gitignore:80` 覆盖），不随本票入库。

> ⚠️ **遗留项**：端口恢复后须**不设** `COMPOSE_EXTRA_FILES` 重跑一次脚本，让「冻结端口」这条也成立。此为宿主环境问题，不阻塞本票的功能面结论，但必须在 impl-05 终验前复跑。

### §D 未验证 / 交接给下游

- **未验证** JM/TM 实际以 SQL 加载 Paimon（`CREATE CATALOG ... 'connector'='paimon'`）—— 本票明确「不建任何表、不写任何 DDL」，归 **impl-03b**。
- **未验证** 镜像名与 `${FLINK_IMAGE}` 覆盖路径在本机之外的行为。
- `sql/` 仍只有 README，未新增任何 DDL（符合票面 Notes）。
- 未实现 `minio` / `spark` profile（spec §8.2，归 impl-05）。
