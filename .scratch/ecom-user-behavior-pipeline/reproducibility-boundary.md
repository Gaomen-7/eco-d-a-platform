# 可复现的边界（票 [04] 产物）

Status: resolved · 2026-10-20
Source: `issues/04-reproducibility-boundary.md` · 裁决全文见该票 `## Answer`
上游: `SRS.md` `REQ-F-ORCH-01` / `REQ-NF-CAP-03/04` · `FSR.md` §2 判据 1 · `HLD.md` §4/§7.3 · `canonical-truth.md` §0 G1

> **本文件回答**：「可复现」这句话，**承诺给谁、承诺到哪、用什么动作和什么判据兑现**。
> **本文件不回答**：数据结果的确定性（同 `--seed` 同哈希、幂等对账）——那归 [02] 的迷雾 **F3**，本仓用「**确定性**」一词，不与「可复现」混用。

---

## 0. 一条总纲

**可复现的主体是「制品」，不是「平台」。**

可复现承诺覆盖的是这一组东西：**`docker-compose.yml` + 镜像锚点 + 相对路径纪律 + 内存/端口契约 + 判据脚本**。
主机平台（Windows/WSL2、Linux、macOS）、网络位置（镜像站可达性）、工具链安装手法（`~/.p0-toolchain` 的无 sudo `.deb` 解包）**不进入承诺**，一律标注为「**本机环境事实**」，在 §5 的成立性表里降级为「降级」或「只作文档声明」。

**为什么不是「平台」**：本仓有两处事实让「任何装有 Docker 的机器」这一口径当场落空——
① 起链路的 Flink 作业构建需要 **JDK 17 + Maven ≥3.8.6**，而本仓的装法是 **Linux/WSL 专属**（`apt-get --print-uris` → `dpkg-deb -x` 解包，`handoff/archive/…222535` §1.1），macOS/Windows 原生无对应物；
② Doris 的 Stream Load 两跳约束是 **Docker Desktop 专属**（官方原文见 §4.1），原生 Linux 上根本不存在该约束 ⇒ 同一份 compose 在两个平台上**语义不同**。
若承诺绑在「平台」上，就得为每个平台写一份 compose 分叉——那是把一个设计问题变成一个维护问题。

---

## 1. 四档复现契约（取代含糊的「干净机器」）

仓库此前对「干净机器」**只有一处定义**（`FSR.md:31`），且不可判定：「干净机器上克隆仓库，一条命令拉起整条链路」。
现拆成四档**互不冒充**的命名契约。每档各自带起始状态、必做动作、判据、计时口径。**「一条命令」只认 L0**（见 §2）。

| 档 | 名称 | 起始状态 | 必做动作 | 判据（可机读） | 计时口径 | 状态 |
| --- | --- | --- | --- | --- | --- | --- |
| **L0** | **制品态** | 镜像已存在（`docker image inspect` 可见）；**卷可存在也可不存在** | `make init`（见 §2 的四段） | `make verify` 退出码 0；`docker compose ps` 全 `healthy` | 受 `REQ-NF-CAP-03` 约束（15 min，**不含出数**） | **待 P1 实测** |
| **L1** | **零状态冷启** | **无镜像、无卷**（fresh clone 之后） | 先构建 Doris 镜像（≈35 min，见 §6），再走 L0 | 同 L0 | **豁免 30 min**（构建不是「拉镜像」） | **待测** |
| **L2** | **带卷重启** | 卷存在、容器可停 | `docker compose down`（**不加 `-v`**）→ `make init` | Kafka offsets 不变；Doris `online_backend_num == total_backend_num == 1`；三张 ADS 表仍在；FE `healthy` | 不计时 | **P0 已实测通过**（HLD §7.3 Bug H 修复后两轮 `down`/`up`） |
| **L3** | **跨机搬运** | 目标机有 Docker、无镜像 | 源机 `docker save` → 目标机 `docker load` → 走 L0 | 同 L0 | 受 `REQ-NF-CAP-04` 约束（30 min） | **待测** |

**四条边界规则**：

1. **L1 不受 30 min 约束**——依据：Doris 官方**不发可直接拉取的版本化镜像**（`apache/doris:4.1.5` / `all-in-one-4.1.5` / `base-4.1.5` 全部 `denied`），只能本地构建；实测构建 ≈35 min（下载 457 s + 解压 712 s + 导出/解包 862 s，HLD §7.3 U-ν）。**「构建」不是「拉镜像」，把它塞进 `REQ-NF-CAP-04` 就是让判据永不可达。**
2. **L2 是「能重起来」的判据，不是附加项**——HLD §7.3 的方法论：凡把状态持久化进卷的组件，验收必须含一次「不删卷的重启周期」；Bug H（Doris 把自身 IP 写进元数据 ⇒ 重建后 IP 变 ⇒ FE 永久卡 `feType:UNKNOWN`）只在 L2 上暴露，L0/L1 全绿也看不出来。
3. **L3 是「另一台机器」的唯一可操作形式**——「另一台机器也能复现」若不给动作，就不是承诺。`docker save` / `docker load` 是本仓认可的动作。
4. **卷是否存在不改变 L0/L1 的分档**——「干净」判的是**镜像**与**卷**两件事；L0 允许卷存在（更接近真人使用），L1 要求两者都无。

---

## 2. 「一条命令」= `make init`（具名）

**事实**：在 2026-10-20 之前，**「一条命令」并不存在**——
- 仓库**无 README**（而 `REQ-NF-MNT-03` 要求端口清单进 README）；
- `docker compose up` **不起 Doris**（`doris-fe`/`doris-be` 在 `profiles: ["doris"]` 里）；
- `make init` 自己写着「**待补**：Doris 建库/建表、Superset db upgrade + create-admin + init」（`Makefile:51`）。

**裁决**：`REQ-F-ORCH-01` 的「一条命令」**具名为 `make init`**，内容冻结为四段，顺序不可换：

| 段 | 动作 | 为什么不可省 |
| --- | --- | --- |
| ① | `docker compose up -d`（默认 profile） | Kafka / Flink JM+TM / Superset 起来；`kafka-init` 由 `depends_on: service_healthy` 自动跑 |
| ② | `docker compose --profile doris up -d --wait` | **默认 `up` 不含 Doris**；不加这段则链路不完整 |
| ③ | `docker compose run --rm client /scripts/doris-init.sh` | Doris 初始化**必须从 compose 网内发起**（§4.1）；该脚本自带 FE 轮询与 BE Alive 等待（`doris-init.sh:41-50`、`:90-94`） |
| ④ | Superset 初始化：`superset db upgrade` → `fab create-admin` → `superset init` | `SUPERSET_LOAD_EXAMPLES=no`；**当前完全未实现**，`make init` 只打印「待补」 |

**为什么是 `make init` 而不是别的**：
- Makefile 目标名是**冻结面**（SRS §3.4：`up/down/init/replay/verify/spike/verify-fault/logs`）⇒ **不需要走冻结项变更**；新增别名的目标（如 `up-all`）则要走 FC 变更流程，为零收益。
- `init` 的语义正是「初始化」，且**已经存在**。
- `docker compose --profile doris up -d` 不能被叫作「一条命令」：`REQ-F-ORCH-01` 原文是「**含初始化脚本**」，而 compose 不会跑 `doris-init.sh` 与 Superset 初始化 ⇒ 必然是第二条命令。

**代价（须签字）**：`REQ-F-ORCH-01` 的原文是「`docker compose up` 一键拉起…」；改成 `make init` 属**意图类改写上游**，需用户签字（已签，见 §8）。

---

## 3. 计时记账口径（两个数，别混）

**镜像体积一律用 `docker image inspect` 的 `.Size`**，不用 `docker images`
依据：本机 `UseContainerdSnapshotter=true` 时官方警告 `docker images` 会把压缩 blob 叠加进读数（gdd §1.7 / HLD §4.2）。

**2026-10-20 实测（本次核实，`docker image inspect`）**：

| 镜像 | 字节 | GiB | 备注 |
| --- | --- | --- | --- |
| `apache/kafka:4.3.1` | 685,875,752 | 0.64 | 与 HLD §4.2 已记 654.1 MiB 一致 |
| `apache/flink:2.2-java17` | 1,563,802,590 | 1.46 | 与已记 1491.4 MiB 一致 |
| `apache/superset:6.1.0` | 1,266,472,606 | 1.18 | 与已记 1207.8 MiB 一致 |
| `apache/doris:base-latest` | 1,962,155,389 | 1.83 | 与已记 1.96 GB 一致 |
| **`eco/doris:4.1.5`（自建）** | **13,526,796,833** | **12.60** | ★ 见下 |
| `mysql:8.0`（`client` 用） | 1,085,715,177 | 1.01 | 新增记录 |

> ★ **本条推翻了票面的一处猜测**：票面与 `gdd §1.7` 怀疑「U-ν 写的 `docker images = 13.5 GB` 是 containerd snapshotter 下的虚高读数」。实测 **`image inspect` = 12.60 GiB ≈ 13.53 GB（十进制）**，与 `docker images` 的 `13.5GB` **一致**。
> ⇒ **那个数不是虚高，是它真的这么大**。口径规则仍然成立（记账用 `image inspect`），但**纠正理由要换**：不是「虚高」，而是「同一个镜像在不同工具下用 GiB/GB 两种单位显示，容易误读」。
> **正确的表述**：`eco/doris:4.1.5` 体积 **12.60 GiB（13,526,796,833 B，`docker image inspect` 实测）**。

**体积的含义**：Doris 自建镜像 **12.60 GiB**，是 Kafka + Flink + Superset 三者之和（**3.28 GiB**）的 **3.8 倍**。
这是「**必须本地构建**」的体积代价，也是 L1 档豁免 30 min 的根因之一（另一根因是 35 min 的构建耗时）。

---

## 4. `client` 的必需性与拓扑

### 4.1 两跳约束是 **Docker Desktop 专属**

官方原文（Doris `docker/runtime/all-in-one/4.1/README.md`，本地证据 `hld-research-sandbox.md:68`）：

> On Docker Desktop the container addresses are not routable from the host, so **a stream load from the host cannot follow FE's redirect to a BE; run it from the `client` service instead**, which is inside the network. **Linux hosts can reach the nodes directly.**

⇒ **「`client` 必须存在」不是全局真理，是 Docker Desktop 的后果。**

| 运行面 | `client` 是否必需 | 理由 |
| --- | --- | --- |
| **Docker Desktop（Win+WSL2 / macOS）** | **必需** | 宿主不可路由容器地址 ⇒ 两跳的第二跳 follow 不到 |
| **原生 Linux + Docker Engine** | **可选（降级）** | 官方原话「Linux hosts can reach the nodes directly」 |

### 4.2 拓扑裁决（并纠正 HLD 的一句话）

**事实（compose 现状）**：`client.depends_on` **只有 `kafka: service_healthy`**（`docker-compose.yml:314-316`）。
**事实（HLD 的旧说法）**：`HLD.md:311` 声称「`depends_on` **全链路** `service_healthy`」。

**裁决**：**以 compose 为准（只等 `kafka`），改 HLD 的字。**

**为什么「全链路 `service_healthy`」是错的（成环论证）**：

```
若 client 依赖 doris-fe healthy
  → doris-fe 的 healthcheck 通过 ≠ 库表就绪（它还缺 init.sql）
  → 库表就绪要走 doris-init.sh
  → doris-init.sh 只能在 client 内跑
  ⇒ client 等 Doris 就绪，而 Doris 就绪等 client 运行 = 真环
```

**不成环的判据（可机读）**：
> `client` **不得出现在任何服务的 `depends_on` 被依赖侧**。即：`docker compose --profile doris --profile client config --format json` 的展开结果里，**没有任何服务的 `depends_on` 键包含 `client`**。
> 反向允许：`client` 可以依赖别人（当前只依赖 `kafka`）。

**剩下的等待由脚本承担**：`doris-init.sh` 自己轮询 FE（`SHOW FRONTENDS`，60×3 s）与 BE（`SHOW BACKENDS` 的 `Alive=true`，60×3 s）——**这正是「不能靠 compose 依赖来表达的等待」**，写进脚本才是对的。

**常驻还是按需**：**按需（`run --rm`）**。
- 不占常驻内存（`config_check.py` 的「常驻上限」口径按「有 `mem_limit` 且**无** `profiles` 键」统计 ⇒ 报 **5.25 GiB**，不含 Doris 也不含 `client`）。
- 但**计入「全部服务声明上限」口径**（含 `client` 256m ⇒ **10.5 GiB**）。
- **两个口径都合法，文档引用时必须写明用的是哪一个**（这条已在 `canonical-truth.md` 冲突 I 的签字句里）。

---

## 5. 跨平台成立性表（能力 × 平台）

**判级**：
- **成立**＝本仓已实测、且机制不依赖具体平台；
- **降级**＝能跑但语义变弱 / 需要额外动作 / 本机做法不可移植；
- **只作文档声明**＝本仓不提供实现，只说明「应如何」；
- **未验证**＝有依据但**没有一手实测**（按 HLD §0.4，未验证不得写成「成立」）。

| # | 能力 | 本机 Win10 19045 + WSL2 + Docker Desktop 29.8.0 | 原生 Linux + Docker Engine | macOS + Docker Desktop | Podman（书面备选） |
| --- | --- | --- | --- | --- | --- |
| 1 | `docker compose up` 起骨架（L0 前半） | **成立**（P0 实测） | **成立**（compose 一等公民） | **成立**（compose 一等公民） | **降级**：`config` 无校验无正常化 ⇒ GDD 的 `mem_limit` 列出式判据**无等价物** |
| 2 | Doris 镜像本地构建（L1） | **成立**（实测 ≈35 min） | **成立**（×86_64） | **降级**：Apple Silicon 需 amd64 模拟（未验证，且 Doris 二进制包仅 x64/`-noavx2`） | **降级**：`podman build` 可用但构建缓存语义不同（未验证） |
| 3 | 逐服务 `mem_limit` + `memswap_limit` 生效 | **成立**（cgroup 层实测 `memory.max`/`memory.swap.max`） | **成立** | **成立** | **降级**：`memswap_limit` 在 `podman-compose` v1.6.0 **无处理** |
| 4 | 端口仅绑 `127.0.0.1` | **成立**（P0 实测） | **成立** | **成立** | **成立**（`host_ip` L1157-1162） |
| 5 | named volume 跨 `down`/`up` 保数据 | **成立**（Kafka 3→3、Doris 三表仍在，L2 实测） | **成立** | **成立** | **未验证** |
| 6 | **Doris FE/BE 固定 IP**（Bug H 的修法） | **成立**（`172.31.88.40/.50`，两轮 `down`/`up` 实测） | **成立**（同一根因：Docker 重建容器会重分配 IP） | **成立**（同上） | **未验证** |
| 7 | **Doris Stream Load 两跳 ⇒ 网内 `client`** | **成立（必需）** | **降级（可选）**：官方「Linux hosts can reach the nodes directly」⇒ 可直连，`client` 非必需 | **成立（必需）**（Docker Desktop 同源约束） | **未验证** |
| 8 | Doris 初始化（建库/建表） | **成立**（经 `client` 的 `doris-init.sh`，实测通过） | **降级**：可直连，无需 `client` 中转 | **成立**（同 Win） | **未验证** |
| 9 | Superset 初始化（`db upgrade`/`create-admin`/`init`） | **只作文档声明**：顺序与命令有官方依据，但**当前未实现**，`make init` 只打印「待补」 | 同左 | 同左 | 同左 |
| 10 | 内存天花板判据（≈8 GB 目标 / 11.68 GiB 天花板） | **成立**（`.wslconfig memory=12GB` ⇒ MemTotal 11962 MiB，实测） | **降级**：天花板 = 宿主 RAM，本仓的 11.68 GiB **是本机数值，不是通用值** | **降级**：天花板由 Docker Desktop 设置决定，本仓未测 | **未验证** |
| 11 | 镜像可得性（拉官方镜像） | **降级（本机环境事实）**：`registry-1.docker.io` 不可达 ⇒ 走 `${REGISTRY_MIRROR:-docker.m.daocloud.io}`；**这不是平台事实，是网络位置事实** | **成立**（若网络可达）；不可达时同样用 `REGISTRY_MIRROR` 覆盖 | 同 Linux | 同 Linux |
| 12 | Python 侧零安装（`uv` + `confluent-kafka` 等 wheel） | **成立**（三平台预编译 wheel，`hld-research-toolchain.md:151`） | **成立** | **成立** | **成立** |
| 13 | JDK 17 + Maven ≥3.8.6（**构建 Flink 作业用**） | **降级**：靠 `apt-get --print-uris` + `dpkg-deb -x` 无 sudo 解包到 `~/p0-toolchain`，**Linux/WSL 专属**，且 `Makefile:16` 还 `-include $(HOME)/.p0-toolchain-env.mk`（**仓库外**） | **成立**（apt/dnf 正常装） | **只作文档声明**（brew / Temurin，本仓不提供） | 同 Linux |
| 14 | 路径语义（仓库在 `F:\` ⇒ `/mnt/f` 的 DrvFs） | **成立**：compose 只写**相对路径**，不写 `/mnt/f` 形绝对路径（`Makefile:8` 的纪律） | **成立**（无此约束） | **成立**（无此约束） | **成立** |
| 15 | 跨机搬运（`docker save` / `load`） | **未验证**（机制成立，本仓未实测） | **未验证** | **未验证** | **未验证** |
| 16 | 体积记账（`image inspect`） | **成立**（本次实测） | **成立** | **成立** | **降级**：`podman images` 无 `.Size` 的等价口径（未验证） |

**读表纪律**：
1. **「成立」只表示本仓已实测**；未实测的一律「未验证」，**不得**按 HLD §0.4 写成成立。
2. **第 11 行的镜像站**是**网络位置事实**，不是平台事实——把它读成「Windows 特有」是最常见的误读。
3. **第 7 行是这张表存在的理由**：同一份 compose 在 Docker Desktop 与原生 Linux 上**语义不同**（`client` 必需 vs 可选）⇒ 这正是「主体＝制品」而不是「主体＝平台」的实证。

---

## 6. 「一条命令」的边界：含运行面，不含工具链自举

**裁决**：`REQ-F-ORCH-01` 的判据表述为「在**已有 Docker + `make`** 的机器上，`make init` 拉起整条链路」。
**工具链自举不在一条命令里**，理由是实测可分的：

| 动作 | 是否需要 JDK 17 / Maven |
| --- | --- |
| `make up` / `make init` / `make down` / `make logs` | **否** |
| `make verify` / `make config-check` | **否**（只要 `curl`、`python3`、`docker`） |
| `make lint` / `make typecheck` | **否**（只要 `ruff` / `basedpyright`，Python 侧） |
| **构建 Flink 作业 JAR** | **是** |

⇒ 迷雾 **F8「多 JDK / 多 Python 的现实包袱」在本票收敛为一句可判的话**：
> **JDK/Maven 只服务于「构建 Flink 作业」，不在起链路路径上**；因此它既不挡住 L0，也不该被写进 `REQ-F-ORCH-01` 的判据。
> 工具链安装脚本（`scripts/bootstrap-toolchain.sh`）留给 **P1 施工票**；本票不写。

**一条反例要留痕**：本机的 `~/p0-toolchain`（280 MB）+ `~/.p0-toolchain-env.{sh,mk}` **在仓库外**，故 fresh clone 的机器**没有** JDK/Maven/`make` 的完整环境。这是「降级（本机环境事实）」，不是「成立」。

---

## 7. 三份 P0 交接的合流清单

三份交接已移动到 `.scratch/handoff/archive/`（**移动，不删内容**）。`.gitignore:73-74` 已把 `handoff/` 挡在库外。
⚠️ **跟踪状态未改动**：`….222535-105-P0环境收尾.md` 在 `8ae097e` 已入库、**仍被跟踪**；`git rm --cached` 会删远端文件，**留待用户单独发令**。

逐条如下（并 → 并入何处 / 废 → 已废弃 / 核 → 仍待核实）：

| 交接内容 | 处置 | 落点 / 理由 |
| --- | --- | --- |
| **① 工具链无 sudo 装法**（apt --print-uris → deploy-deb -x，JDK17.0.20.1 / Maven 3.9.12 / make 4.4.1 / uv 0.13.1 / Python 3.12.15 / ruff 0.17.1 / basedpyright 1.40.2） | **并** | HLD §8-R3 已记完整手法；本文件 §6 补「它不在起链路路径上」这一判据 |
| **② `~/.p0-toolchain-env.{sh,mk}` 的引入方式与 `.bashrc` 早退坑** | **并** | HLD §8-R3 + `Makefile:16` 注释已记；本文件 §5 第 13 行、§6 标为「降级/本机事实」 |
| ③ Windows 侧三处改动（PATH、`UV_CACHE_DIR`、`.wslconfig memory=12GB`）与各自备份/回滚 | **并** | HLD §7.3 U-β/U-ζ/U-η 已记；**本文件不重复**，只保留「属本机环境事实」的定性 |
| ④ 六条坑（Flink `config.yaml`、空目录 bind 遮 config、Superset 无 DB 驱动、`kafka-topics.sh` 无 `--alter`、`log.dirs=/tmp`、探针太重） | **并（已并完）** | HLD §8-R7 a–e 五条 + 六条中的探针条见 `docker-compose.yml:62-71` 注释；**无需重复合流** |
| ⑤ 三处「agent 自己写错」（makefile heredoc / `$fail` 少 `$` / 代理测试做错） | **并（已并完）** | HLD §8-R4 与 §8-R7 教训栏 |
| ⑥ 12 → 14 个待推提交清单 | **废** | 是交接时的**瞬时状态**，且 `origin/main..HEAD` 已变；git 历史本身是权威，文档里留清单只会制造过期数据 |
| ⑦ 容器/镜像/job 的瞬时快照（`eco-kafka` healthy、`docker images` 内容、后台 job id） | **废** | 同上；**本次实测已刷新**（见 §3 与 §9） |
| ⑧ Bug A（重复 `ENV JAVA_HOME`） | **并（已并完）** | HLD §7.3 U-ν 修复清单 A |
| ⑨ Bug B（`client` entrypoint 吞参数）+ 三档验证表 | **并（已并完）** | HLD §7.3 修复清单 B；`docker-compose.yml:303-313` 注释 |
| ⑩ Bug C（`init.sql` 路径三方不一致；**不能**挂 `/scripts/doris`，`/scripts` 只读 ⇒ runc 建不了挂载点） | **并（已并完）** | HLD §7.3 修复清单 C；`docker-compose.yml:319-334` |
| ⑪ Bug D（BE 从未注册，官方 Step 3.3 `ALTER SYSTEM ADD BACKEND`） | **并（已并完）** | HLD §7.3 修复清单 D；`docker/doris/entrypoint.sh` |
| ⑫ Bug E（`vm.max_map_count`/`swapon`/`ulimit` 前置检查 ⇒ `SKIP_CHECK_ULIMIT=true`） | **并（已并完）** | HLD §7.3 修复清单 E；`docker/doris/Dockerfile:69-88` |
| ⑬ Bug F（`FE_SERVERS`/`FE_ID`/`BE_ADDR` 是死配置） | **并（已并完）** | HLD §7.3 修复清单 F |
| ⑭ Bug G（`ads_funnel_step` UNIQUE KEY 非列声明有序前缀） | **并（已并完）** | HLD §7.3 修复清单 G |
| ⑮ Bug H（Doris 把自身 IP 写进 `doris-meta` ⇒ `down`/`up` 后 IP 变 ⇒ FE 卡 `feType:UNKNOWN`）**＋固定子网修法** | **并（已并完）** | HLD §7.3 修复清单 H；`docker-compose.yml` 的 `networks:` 段（354-359） |
| ⑯ 「构建失败于 `COPY` 阶段 EOF」的旧叙事 | **废（已证伪）** | 物证：`docker buildx du` 曾有两条成功 COPY 层；改道后该路径整体作废（HLD §7.3 U-ν 记改道） |
| ⑰ 「首选 `curl -L <URL> \| tar -xJ`」 | **废（错）** | 文件是 **gzip**，必须 `-xzf`；`-xJ` 是 xz，会失败（`docker/doris/Dockerfile:42-43` 已记） |
| ⑱ 「不要 `builder prune`（会清掉唯一一次成功传输的缓存）」 | **废** | 构建已成功、缓存语义已变；**本次实测**：`docker system df` 的 Build Cache 20.88 GB 中**仅 367.4 MB reclaimable**，且**票面说的「Doris 两条 4.7 GB COPY 层」已不存在** |
| ⑲ 「两份 4.03 GiB tarball + `docker/doris/` 是构建上下文负担」 | **废（已执行）** | **本次已删两份**（合计 8.06 GiB）；Dockerfile 只读 URL（`ARG DORIS_TARBALL_URL`），`.dockerignore` 排除 `*.tar.gz` |
| ⑳ 「`.scratch/backups/Dockerfile.head-20261019` 未跟踪、待定是否入库」 | **核** | 仍存在（**3420 B**）；它**与 HEAD 逐字节相同** ⇒ 只是历史锚，**保留、不入库**（`.gitignore` 已挡） |
| ㉑ 「U-η：Ubuntu `ext4.vhdx` 是否迁出 C 盘」 | **核（可关闭）** | 实测 Ubuntu 在 **`F:\Migrating Files\WSL\Ubuntu`**、`docker-desktop` 在 **`F:\Docker Desktop\…`** ⇒ **本就都不在 C 盘**；建议下一张票直接标「已无必要」 |
| ㉒ 「`docker images` = 13.5 GB 口径可疑」 | **核（已结案，结论与票面相反）** | **本次实测**：`image inspect` = 12.60 GiB ≈ 13.53 GB ⇒ **不是虚高**；见 §3 的更正 |
| ㉓ 「`uv` 的三处偏差（不读 pip.conf / 无 `--user` / 默认不编译字节码）」 | **核 → 归 P1** | 属生成器施工面的环境包袱，与「起链路」无关；本票只在 §5 第 12 行标「Python 侧零安装成立」 |
| ㉔ 各交接里的「目标 goal id」与 skill 建议表 | **废** | 会话态，且两次交接都记了「id 查不到」；留在文档里只会误导 |

---

## 8. 待 [09] 落笔的清单（本票**不**改这些文件）

依 `canonical-truth.md` §5 的落笔分工：**SRS/gdd 的改字由 [09] 统一执行**；本票只给逐字新句。**签字已闭合**（2026-10-20 用户批准）。

### 8.1 `REQ-F-ORCH-01`（意图类，**已签字**）

| 项 | 内容 |
| --- | --- |
| 现文 | `REQ-F-ORCH-01` \| MUST \| `docker compose up` 一键拉起整条链路（含初始化脚本） \| 干净机器上一条命令起链路 |
| **逐字新句** | `REQ-F-ORCH-01` \| MUST \| **`make init`** 一键拉起整条链路（**含 Doris profile 与初始化脚本**） \| **在已有 Docker + make 的机器上**（L0 制品态）`make init` 后 `make verify` 退出码 0、`docker compose ps` 全 `healthy`；四档契约见 `reproducibility-boundary.md` §1 |
| 理由 | 「一条命令」此前**不存在**（无 README、`up` 不含 Doris、`make init` 自己写「待补」）⇒ 必须具名；`init` 是冻结目标名，不触发冻结项变更 |

### 8.2 `REQ-NF-CAP-03`（意图类，**已签字**，拆两截）

| 项 | 内容 |
| --- | --- |
| 现文 | `REQ-NF-CAP-03` \| MUST \| **预拉镜像前提下**，一条命令到看板出数 **≤ 15 分钟** \| 两档之一 |
| **逐字新句** | `REQ-NF-CAP-03` \| MUST \| **预拉镜像前提下**，`make init` 到**链路全 healthy** **≤ 15 分钟**（**不含出数**：看板出数依赖 P1 的真实 Flink 作业与 Doris 写数，P1 未实现即不可测）。「到看板出数 ≤ 15 分钟」作为**同一需求的第二截**，判据转 P1 实测 | `make init` 起止计时 ≤ 15 min；出数计时待 P1 |
| 理由 | 现判据「到看板出数」在 P1 之前**不可测**，把可测的那截单独钉住，判据才落地 |

### 8.3 `REQ-NF-CAP-04`（意图类，**已签字**，改 `docker load` 口径 + (§7.1) 登记不可达）

| 项 | 内容 |
| --- | --- |
| 现文 | `REQ-NF-CAP-04` \| MUST \| **冷启动（含拉镜像）≤ 30 分钟** \| 两档之二 |
| **逐字新句** | `REQ-NF-CAP-04` \| MUST \| **预构建镜像经 `docker save`/`docker load` 导入后**，L3 跨机搬运冷启动 ≤ 30 分钟。⚠️ **「本地构建 Doris 镜像」不计入本判据**（实测构建 ≈35 min；Doris 无官方版本化镜像，构建不可避免）——构建耗时的记录见 HLD §7.3 U-ν | 导入后起链路 ≤ 30 min；构建时长单独记录、不设上限 |
| 同时登记 | HLD §7.1 新增一行偏离：**原表述把「构建 Doris 镜像」隐含在「拉镜像」内，实测构建 ≈35 min ⇒ 原口径不可达** |

### 8.4 其他（事实类、免签，但仍归 [09] 落笔）

| 项 | 内容 |
| --- | --- |
| `HLD.md:294` 的 **F-1** | 「10.25 GiB（10,496 MiB）…余量 6.43 / **1.43** GiB」→「**10.5 GiB**（10,752 MiB）…余量 6.43 / **1.18** GiB」；依据 `canonical-truth.md` §4.3 F-1。**已于 2026-10-21 由施工票 impl-01 落笔完成**（HLD §4.2 表格现为 10.5/1.18，旧数清除） |
| `gdd.md:512`「Doris 无官方 compose」 | **已被 `gdd.md:197` 推翻**（官方有 `docker/runtime/all-in-one/4.1/compose/`）；**勿再引用** |
| `make verify` 第 4/5 项的执行位置 | `Makefile:76-78` 在**宿主侧** `curl :8030`，与 HLD §4.4/§8-R2 的「Doris 断言必须经 `client`」矛盾 ⇒ 待 §8.5 |

---

## 9. 已登记但**不在本票修**的实现缺口（doc ≠ reality）

本票是设计票（目的地「只到知道怎么施工」），实现改动不在此。三处已登记，归属如下：

| # | 缺口 | 文档说 | 实现是 | 归属 |
| --- | --- | --- | --- | --- |
| 1 | **Doris 断言在宿主跑** | HLD §4.4「必须经 `client`，宿主直跑即失败」；§8-R2 判定条件同 | `Makefile:76-78` 宿主侧 `curl 127.0.0.1:8030/api/health`（**只查 FE 健康，不查 ADS 有行**） | **[09]**（并 P1 补真实断言） |
| 2 | **`minio` / `spark` profile 不存在** | `REQ-F-ORCH-08`（SHOULD）要求两个可选 profile | compose 里只有 `doris` 与 `client` 两个 profile | **待签字的意图类**（实现 or 撤需求），挂 **[05]/[08]** |
| 3 | **`gdd.md:512` 自相矛盾** | 称「Doris 无官方 compose」 | `gdd.md:197` 已证官方**有** | **[09]**（就地订正） |

---

## 10. 本次实测证据（可复跑）

```bash
# ① 镜像体积（记账口径）
docker image inspect eco/doris:4.1.5 --format '{{.Size}}'     # 13526796833
docker image inspect apache/kafka:4.3.1 --format '{{.Size}}'  # 685875752（经镜像站 tag 同）

# ② 四档契约的判据（L2 档）
docker compose down            # 不加 -v
make init
docker compose ps             # 期望全 healthy
curl -s http://127.0.0.1:8030/api/health   # 期望 online_backend_num == total_backend_num

# ③ 不成环判据：client 不得出现在任何 depends_on 的被依赖侧
docker compose --profile doris --profile client config --format json | jq -r '.services[].depends_on // {} | keys[]' | sort -u | grep -x client && echo 'RING!' || echo 'OK: no ring'

# ④ 执行面（§4.7 的 A 面）
wsl -e bash -lc 'test -S /var/run/docker.sock && echo socket-present'
```

**本次实测记录（2026-10-20）**：
- `docker version` Server **29.8.0**；`eco-*` 8 个容器**全部 Exited**（上次收工关闭）⇒ WSL 内 `curl :8081` = **000**，属「容器没起」而非 integration 坏。
- 7 个命名卷全在（`eco-a-d-platform_{kafka-data,doris-fe-meta,doris-fe-log,doris-be-storage,doris-be-log,superset-home,superset-db-data}`）⇒ 当前状态是 **L2 的前置**（卷已存在），不是 L1。
- `docker system df`：Images 25.69 GB / Containers 1.253 GB（100% reclaimable，**全部是别的项目** `jobproject-*`/`bd-*`/`hive-*`，**未动**）/ Build Cache 20.88 GB（**仅 367.4 MB reclaimable**）/ Local Volumes 2.246 GB。
- 删除两份 tarball 后 **F: 41.79 → 49.84 GB**。
