# impl-03a: P2 部署面 —— warehouse 具名卷同路径挂 JM/TM · Paimon jar 进每个 Flink 进程

> 父票：impl-03（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-03-p2-offline-layering.md`）
> 规格落点：spec §5.6「P2 部署硬约束（两条，HLD §5 未写过）」

Type: task
Status: resolved（2026-10-22）
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

## Answer（2026-10-22，含 code-review 修正）

**结论：票面 8 条验收全部通过。实证脚本 `scripts/deploy_surface_check.sh` 共 10 项判据（票面 8 条 + 新增 4b 可写性 + 4c ADR-0002 补丁锚点），本轮实跑 `10 PASS / 0 FAIL`（退出码 0）。**

> ⚠️ **本节证据均取自脚本自身的本次运行**（`bash scripts/deploy_surface_check.sh`，2026-10-22T11:50:46Z，日志见文末复现命令）。
> 上一版 §A 贴的是更早的手工探针输出，与交付脚本的探针格式不同（脚本用 `stamp=/nonce=/host=`，`WROTE` 标记）——**已按 code-review 意见全部替换**，现在每个数字都可从同一条命令重新产出。

### 落地物

| 文件 | 动作 | 说明 |
| --- | --- | --- |
| [docker/flink/Dockerfile](../../docker/flink/Dockerfile) | 新建 | 官方 flink:2.2-java17 + 构建期 `COPY` Paimon jar 进 `/opt/flink/lib` + **预建 `/warehouse` 并 chown 9999:9999**（见 §B） |
| [scripts/fetch-paimon-jar.sh](../../scripts/fetch-paimon-jar.sh) | 新建 | 幂等预取 jar（size + sha256 双校验，`.part`→校验通过才 `mv`） |
| [docker-compose.yml](../../docker-compose.yml) | 改 | `paimon-warehouse` 具名卷；JM/TM 共用本地镜像 + `build:` 段；两处 `paimon-warehouse:/warehouse` 逐字相同 |
| [Makefile](../../Makefile) | 改 | 新增 `build-flink-image`（取 jar → 构建镜像），且 `up:` 依赖它 ⇒ 构建顺序**固化进代码**而非注释 |
| [.gitignore](../../.gitignore#L52-L66) | 改 | 新增第 4b 节忽略 `docker/flink/lib/`（57 MB 二进制不入库） |
| [scripts/deploy_surface_check.sh](../../scripts/deploy_surface_check.sh) | 新建 | 10 项判据的实证脚本，含反例设计说明与 `-v` 自检 |

### §A 实机证据（本轮脚本原始输出）

**① 具名卷同路径 —— 两层断言**

compose 配置层：
```
flink-jm: 卷条目 2 个, 匹配 source=paimon-warehouse&target=/warehouse 的 1 个
   - source=paimon-warehouse target=/warehouse type=volume
flink-tm: 卷条目 2 个, 匹配 source=paimon-warehouse&target=/warehouse 的 1 个
   - source=paimon-warehouse target=/warehouse type=volume
```
实机层（`docker inspect` 的 Mounts 才是能抓反例的那一半）：
```
eco-flink-jm Mounts -> Name=[eco-a-d-platform_paimon-warehouse] Destination=[/warehouse] (rc=0)
eco-flink-tm Mounts -> Name=[eco-a-d-platform_paimon-warehouse] Destination=[/warehouse] (rc=0)
=> 两侧 Name 逐字相等，Destination 逐字相等
```
> 为什么两层：只查 compose 是**恒真判据**——两处若写两个不同卷名，config 照样打印两行。故再比对实机 Mounts 的 `Name`。

**② 跨容器互见（本票核心）**
```
探针路径：/warehouse/.probe-20261022T115046Z-532731433
JM 侧 stdout/rc=0:
WROTE
impl-03a-probe stamp=20261022T115046Z nonce=532731433 host=flink-jm
TM 侧 stdout/rc=0:
impl-03a-probe stamp=20261022T115046Z nonce=532731433 host=flink-jm
逐字比对：
  JM 行 = [impl-03a-probe stamp=20261022T115046Z nonce=532731433 host=flink-jm]
  TM 行 = [impl-03a-probe stamp=20261022T115046Z nonce=532731433 host=flink-jm]
```
> 探针含 stamp + nonce + 主机名（`host=flink-jm` 证明**写入方确实是 JM**），TM 逐字读回。不以「文件存在」为判据。

**③ down（不带 -v）→ up 后数据仍在**
脚本先把探针备份到宿主，`down` → `up -d --wait` → 读同一路径与备份逐字比对，PASS。
`compose_down_preserving_volumes()` 在真正执行前**逐字扫描参数**，发现 `-v`/`--volumes` 即拒绝（rc=99）。

**④ jar 在两端（本轮已加体积+sha256 断言）**
```
eco-flink-jm: size=57697871 sha256=43a6416a…f859107dd1
eco-flink-tm: size=57697871 sha256=43a6416a…f859107dd1
```
> 上一版只 `grep` 文件名（code-review 指出那抓不住截断/替换）。现按票面要求的**具体**数字逐字节校验。

**4c ADR-0002 补丁锚点（code-review 后新增）**
```
[eco-flink-jm] Version: 2.2.1, Commit ID: 450c63e   → 补丁 = 2.2.1
[eco-flink-tm] Version: 2.2.1, Commit ID: 450c63e   → 补丁 = 2.2.1
```
> ADR-0002 把锚点定为 2.2.1 并要求「核对镜像实际运行补丁」。本地 tag `2.2` 是**浮动**的，故把这次核对机器化：换成 2.2.0 的 FROM 会立刻 FAIL。

**⑤ 无 ADD JAR 指向宿主路径**
全仓 15 处 `ADD JAR` 字样**全部是注释或检测脚本自身**，零可执行语句（脚本已排除注释行与自身）。

**⑥ data/ 被 .gitignore（C5）**
```
.gitignore:10:**/data/	data/
.gitignore:10:**/data/	data/threshold/raw.parquet
```

**⑦ 七服务**
```
doris-be|running|healthy      flink-jm|running|healthy    kafka|running|healthy
doris-fe|running|healthy      flink-tm|running|healthy    kafka-init|exited| (ExitCode=0)
superset|running|healthy      client|exited|  ← profile 隔离按需容器，不计入
```
> **判据坑**：kafka-init 是 `restart:"no"` 的一次性服务、**无 healthcheck**，要求它 healthy 结构性不可能。故拆两条：常驻 6 个须 healthy + kafka-init 须 ExitCode=0。

**⑧ 数字标注（本轮已从空转改为真断言）**
compose 与 `docker/flink/` 两边都有标注词、Dockerfile 有标注、compose 的 flink 段有标注，任一为零即 FAIL；全文照旧打印供人工复核语义。
逐处标注：uid/gid `9999` = `设计值`（本仓实测，**非**官方逐字声明，见 §B）；`chmod 755` = `设计值`；jar size/sha256 = 本仓实测钉死的设计值（Paimon 官方未公布校验值）。

### §B 反例证伪（证明判据不是恒真）

**B-1 权限反例**：`chmod 555 /warehouse` 后重跑 → `4b` 两侧与 `②` 均 FAIL，退出码 1；改回 `755` 即恢复 `10 PASS / 0 FAIL`。
> 上一版此处误记为「6 PASS / 5 FAIL」—— 那是计数重复缺陷（内层循环与末尾各记一次 FAIL）造成的不可能数字。**该缺陷已修**：现在每个判据最多贡献一个 FAIL，总数不会超过判据数。

**B-2 实机发现并修掉的承重缺陷（票面未写）**

首次跑 ② 即撞：`stderr: cannot create /warehouse/.probe-…: Permission denied`。

**根因（两个条件叠加）**：
1. 官方 flink 镜像以 `uid=9999(flink)` 运行（实测 `id`）；
2. **镜像内原本没有 `/warehouse`**（实测 `ls: cannot access '/warehouse': No such file or directory`）。

⇒ Docker 创建容器时只能**以 root 新建挂载点**（实测 `drwxr-xr-x 2 0 0`），首次建卷时这份 root 属主留在卷根 ⇒ flink 用户对该卷只有 r-x、没有 w。Paimon 要在此建表目录、写 bucket ⇒ **必然失败**，且失败形态正是「表不存在 / 类找不到」，与票面要防的一模一样。

**修法**：Dockerfile `USER root; mkdir -p /warehouse; chown -R 9999:9999; USER flink`。Docker 对空具名卷的初始化语义会把镜像内该目录的内容与属主复制进卷，故新卷天然可写（实测：全新卷 + 本镜像 → `9999 9999`，写入成功）。
> ⚠️ **只对新建卷生效**；继承下来的 root 属主卷需手工 `chown` 修一次。P2 首次启用的卷都是新建。

⇒ **「挂对了卷」不等于「能写」**，故新增 ④b 独立判据。

### §C 环境阻塞（如实登记；遗留项已于 2026-10-26 销项，见下方销项行）

**本机 WinNAT 排除 TCP 8024–8123**，覆盖冻结端口 8030/8040/8081/8088；非管理员无法释放，本会话禁止提权。

**已反事实取证**：用**改动前**的 `git show HEAD~:docker-compose.yml` 起 superset，同样报 `8088 ports are not available` ⇒ 与本票改动无关（会话开始时七服务曾全 healthy）。

**处置**：功能性实证经 `COMPOSE_EXTRA_FILES` 叠加**只改宿主侧发布端口**的 override 完成（容器内端口/卷/内存/healthcheck/镜像名一律不动），该 override 落 `.scratch/tmp/`（被 `.gitignore:80` 覆盖）。

> ⚠️ **遗留项（明确不勾销）**：⑦「冻结端口」这半边**尚未**在真实端口上验证过。端口恢复后须**不设** `COMPOSE_EXTRA_FILES` 重跑一次。这是宿主环境问题，不改变本票功能面结论，但必须在 impl-05 终验前补跑。
> ✅ **销项（2026-10-26，[impl-06f-4](impl-06f-4-closeout-gate-deploy-proof.md) 收尾门实跑）**：不设 `COMPOSE_EXTRA_FILES` 复跑 `deploy_surface_check.sh`（10 PASS / 1 FAIL，唯一 FAIL 是判据 ⑤ 的排除集不完备，已归因为判据自身缺陷并立 [impl-06i](impl-06i-deploy-check-5-exclusion-defect.md)），判据 ⑦ PASS；八个冻结端口 HTTP 面 4×200 + 协议面 4×TCP OPEN，`docker port` 映射逐个核对一致。「冻结端口」半边自此有**不设 override 的一手实证**。

### §D code-review 修正清单（两轴审查后）

| # | 审查指出 | 处置 |
| --- | --- | --- |
| 1 | §A 证据取自**更早的手跑探针**，与交付脚本不符 | **已用本轮脚本真实输出整体重写 §A** |
| 2 | §B-1「6 PASS / 5 FAIL」算术不可能 | 确认是计数重复缺陷；**已修脚本**，并改正本票记录 |
| 3 | ⑧ 只 `grep|wc -l > 0`，一条注释即可通过（恒真） | **已改为四部分真断言**（两边须有标注、Dockerfile 须有、flink 段须有） |
| 4 | 无 `build:`/Makefile 目标，构建顺序只在注释里 ⇒ 干净检出不可复现 | **已加 compose `build:` 段 + Makefile `build-flink-image`，且 `up:` 依赖它** |
| 5 | ④ 只验 jar 名，未验「57,697,871 B」 | **已加 size + sha256 逐字节断言** |
| 6 | ADR-0002 补丁锚点 2.2.1 无人核对 | **已加 4c 判据**（两端 `flink --version` 须为 2.2.1），实跑证实镜像正是 2.2.1 |
| 7 | `.gitignore` 声称 `git ls-files` 只有 config.yaml —— 与 HEAD 不符 | **已改准**：实测返回 Dockerfile + conf/config.yaml |
| 8 | Dockerfile 卷语义自相矛盾（root:root 建目录 vs 复制镜像元数据） | **已改准**：根因是「镜像内无该路径 ⇒ 以 root 新建挂载点」；引用官方 Volumes 句并明确标注本仓**未逐字留源**故不标 `官方给出` |
| 9 | compose「`file:` 仅 single-machine tutorial」是无源官方断言 | **已改为可确证事实 + 工程结论（`设计值` 口径）**，不再冒充官方逐字 |
| 10 | 脚本 section banner 错位（⑤ 压在 4b 上、① 压在 ensure_stack_up 上） | **已重排**，每个函数正上方都有对应标题 |
| 11 | `uid 9999` 标 `官方给出` 属误标 | **已改 `设计值`**（本仓实测，非官方逐字声明） |
| 12 | 六个新票 + DAG 推进夹在部署提交里（scope creep） | **承认**：拆票与 DAG 属母票收口动作，与代码同提交不理想；已在提交信息与本票登记，不再扩大。 |

**未采纳（附理由）**：
- *抽公共 helper 消除 9 个判定函数的 pass/fail 尾巴与两处 grep 排除集* → 保留。两段 grep 判的是**不同对象**（可执行 ADD JAR 语句 vs 宿主路径 jar 引用），排除集必须不同；统一会让判据变糊。每个判据的失败文案都指向**具体病因**（卷名不同/属主不符/写入被拒/状态 MISSING），这是本票的主要价值。
- *4 处内联 Python JSON 解析器重复* → 暂留。它们是各自判据的取证逻辑，抽出来需同步改 4 处调用约定，收益低于风险；记为后续清理项。

### §E 未验证 / 交接下游

- **未验证** Paimon catalog 的实际 SQL 加载（`CREATE CATALOG … 'connector'='paimon'`）—— 本票明确不建表、不写 DDL，归 **impl-03b**。
- ⑦ 的**冻结端口**半边待端口恢复后复跑（见 §C 遗留项）。
- 未实现 `minio`/`spark` profile（spec §8.2，归 impl-05）。

### §F 判据口径变更（2026-10-26，随 impl-06d 实测回填；施工票 [impl-06e](impl-06e-deploy-check-usrlib.md)）

**④ 已改判据对象**：原「Paimon jar 在 JM/TM **两个容器** `/opt/flink/lib` + size/sha256」
→ 现「**作业 jar 在 `/opt/flink/usrlib/`**（两端同一字节）+ `/opt/flink/lib` 不许有作业 jar 副本」。
脚本内函数：`check_jar_both_containers` → `check_job_jar_usrlib`；原断言语义**未丢**，
整体下移为 **4d**「Paimon connector 在两端 lib」（`check_paimon_connector_in_each_process`）。

- **为什么改**（一手证据来自 [impl-06d](impl-06d-r7i-replay-watermark-fixes.md) §四 环境事实 ④）：
  作业 jar 放 `lib/` 会 `LinkageError: loader constraint violation … committedOffsets`
  —— 作业 shade jar 含 `org/apache/kafka/**`，`flink-dist-2.2.1.jar` 不含
  ⇒ 父子类加载器各加载一份 Kafka 的 `OffsetResetStrategy`。
- **原判据的结构盲区**：它只查 `lib/` 有没有 Paimon connector，**完全不看作业 jar 在哪个目录**
  ⇒ 放错目录时它照样 PASS。这是「判据恒真」的第三种形态（不是恒真，是**判错了对象**）。
- **新判据的反例证伪（2026-10-26 实机，均为一次性注入、事后已还原）**：

  | 反例 | 注入 | 新 ④ 结果 | 还原后 |
  | --- | --- | --- | --- |
  | A：`lib/` 混入作业 jar 副本（LinkageError 形态） | JM `cp usrlib/ecom-flink.jar lib/` | **FAIL**（「…里有作业 jar 副本 ⇒ 父子类加载器双份」） | PASS |
  | B：只给一端拷过（半部署） | TM `mv usrlib/ecom-flink.jar …jar.off` | **FAIL**（「tm 侧 /opt/flink/usrlib/ 里没有 ecom 作业 jar」） | PASS |
  | C：usrlib 多版本共存 | JM `touch usrlib/ecom-old.jar` | **FAIL**（「有 2 个 ecom jar，期望恰 1」） | PASS |

  B/C 两轮各只产生 **1 条** FAIL —— 计数重复缺陷（本票 §D #2）未回归。
- **正路径实机读数**（同一轮）：两端 `/opt/flink/usrlib/ecom-flink.jar` 各一份，
  `size=73693749`、`sha256=9031125a…b87324` 逐字相同；`lib/` 零 ecom 副本；
  4d 两端 `paimon-flink-2.2-2.1.0.jar` size/sha256 仍与钉死值一致（PASS）。
- **未复跑**：③（down/up，会重启整栈并杀掉 RUNNING 作业 `cdf679cd…`，36 tasks）与 ⑤⑥⑧
  **本轮未跑**（⑤ 在全仓逐文件 `grep -r` 上超时；⑥⑧ 未执行）。
  这三条**未改动一支**，口径变更不影响其语义。

### §G 判据口径变更（2026-10-27，用户裁决方向 C；施工票 [impl-06i](impl-06i-deploy-check-5-exclusion-defect.md)）

**⑤ 的扫描面已改判据对象**：旧「全仓 `grep -r`（仅排除 .git/.scratch/artifacts）」
→ 现「**可执行面白名单**」（`*.sql` / `*.sh` / `*.java` / `Makefile` / `Dockerfile` /
`*.yml` / `*.yaml`，两段共用同一组 `SCAN_INCLUDE`/`SCAN_EXCLUDE`）。

- **为什么改**（一手证据来自 [impl-06f-4](impl-06f-4-closeout-gate-deploy-proof.md)
  收尾门实跑）：旧口径被 32 行非 SQL 噪音打成恒假——`.mimosa/hook-state/**` 会话
  基线 27 行（判据写就时该目录不存在）+ 守卫测试 docstring 4 行 + README 判据名
  1 行。判据对象是「**可能出现 ADD JAR 语句的文件**」，不是「工作树里的一切文件」。
- **票面未记的新发现**（06i 复盘实测）：⑤ 的附加扫描段（宿主路径 jar 引用）同样被
  `.mimosa` 18 行基线副本打成恒假，只是 seg1 先 FAIL 提前 return 挡住了它。
  故两段同改，且 seg2 也出了反例凭证。
- **反例证伪（2026-10-27 实机，均为一次性注入、事后已还原）**：
  | 反例 | 注入 | 新 ⑤ 结果 | 还原后 |
  | --- | --- | --- | --- |
  | A：sql/ 真 ADD JAR 宿主路径语句 | `sql/zz_impl06i_red.sql` 一行 | **FAIL**（命中注入行，其余 10 条照绿） | PASS |
  | B：.sh 宿主路径 jar 引用（无 ADD JAR 字样，seg2 腿） | `scripts/zz_impl06i_red.sh` 一行 | **FAIL**（seg1 零命中、seg2 抓到，其余 10 条照绿） | PASS |
- **正路径**：不设 override 全门复跑 **11/11 PASS 退 0**（凭证 `evidence/impl-06i/04-clean-full-gate.out.txt`）。
- **附带裁决**：`.mimosa/` 入 `.gitignore` §7（与 `.dsh/`、`.agents/` 同族，
  harness 会话状态不入库）。
- **维护点**：新增可执行源码类型时须把扩展名加进 `SCAN_INCLUDE`，否则该类型脱管
  （已写进脚本 ⑤ 段头注）。注释行过滤同步扩到 `#` / `--` / `//` 三种形态。

### 复现命令

```bash
# 1. 取 jar + 构建镜像（顺序已固化，幂等）
wsl -e bash -lc 'cd "/mnt/f/Anaconda practice project/eco-a-d-platform" && make build-flink-image'

# 2. 跑全部 10 项判据（端口正常时不要设 COMPOSE_EXTRA_FILES）
bash scripts/deploy_surface_check.sh
```
