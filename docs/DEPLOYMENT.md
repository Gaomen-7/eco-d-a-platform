# 部署指南（跨机搬运，从零手动）

> **读者**：要把本项目搬到另一台电脑、并且**不靠 agent** 手工做完的人。
> **配套**：末尾有「交给 agent 的部署 prompt」，可整段复制给另一个 agent 会话。
>
> **本文的事实基础**：`docker-compose.yml`、`Makefile`、`docker/`、`scripts/`、`flink/`，
> 以及 `.scratch/ecom-user-behavior-pipeline/reproducibility-boundary.md`（L0–L3 四档契约）。
> 文中每个数字都标了 `官方给出` / `设计值` / `实测`——本仓规矩：**不得把设计值当官方保证**。

---

## 0. 三分钟速览：这条链到底要什么

```
ecomgen（造数据）→ Kafka → Flink 作业 → Paimon（五层）→ Doris → Superset 看板
```

| 组件 | 版本锚点 | 镜像来源 | 备注 |
| --- | --- | --- | --- |
| Kafka | 4.3.1 | `${REGISTRY_MIRROR}/apache/kafka:4.3.1` | 官方镜像 |
| Flink | 2.2.1（ADR-0002 冻结 2.2.x） | **本地构建** `eco/flink:2.2-paimon` | 需烘焙 Paimon connector |
| Paimon | 2.1.0 connector | 随 Flink 镜像 | 构建期 COPY 进 `/opt/flink/lib` |
| Doris | 4.1.5 | **本地构建** `eco/doris:4.1.5` | 官方**无**版本化镜像，必须自建 |
| Superset | 6.1.0 | `${REGISTRY_MIRROR}/apache/superset:6.1.0` | 官方镜像 |
| ecomgen | 本仓 Python 包 | `uv run` | 数据生成器 |

### 一句话：完整部署链是**四步**

```bash
make build-flink-image    # ① 预取 Paimon jar + 构建带 Paimon 的 Flink 镜像
make build-doris-image    # ② 构建 Doris 镜像（官方无版本化镜像；≈35 min）
make init                 # ③ 拉起容器 + 建库建表 + 置备看板（四段，顺序不可换）
make deploy-job           # ④ 构建并提交 Flink 作业
make verify               # ⑤ 判据：PASS=10 FAIL=0 SKIP=0
```

**① ② ④ 是换机后最容易漏的三步**——它们在 `make init` 里**都不存在**，
漏掉的表现是：`make verify` 的第 2 项（`flink-job-running`）FAIL，
或 `make init` 第二段报 Doris `image not found`。

**三条你必须先知道的「反直觉」事实**（都是本仓实测踩出来的）：

1. **`make init` 不含 Doris 镜像构建，也不含 Flink 作业 jar 的构建与提交。**
   `make init` 只做「拉起容器 + 建库建表 + 置备看板」四段。
   ⇒ 完整链路是 **四步**：`make build-flink-image` → `make build-doris-image`
   → `make init` → `make deploy-job`。
   跳过任何一步，`make verify` 都会有判据 FAIL。
2. **Doris 镜像在 compose 里没有 `build:` 段**，只有 `image: ${DORIS_IMAGE:-eco/doris:4.1.5}`。
   干净检出后必须**手工** `docker build -f docker/doris/Dockerfile -t eco/doris:4.1.5 docker/doris`
   （或 `make build-doris-image`）。
   > 顺带解释一个设计选择：为什么不像 Flink 那样给 compose 加 `build:` 段？
   > 因为 compose v2 的 `up` 会在镜像缺失时**自动构建** ⇒ Doris 那 35 min 会并进
   > `make init` ⇒ 打破 L0/L1 分档（L1 的 30 min 豁免理由正是「构建不是拉镜像」）。
3. **作业 jar 只能放 `/opt/flink/usrlib/`，不能放 `/opt/flink/lib/`。**
   放错目录 ⇒ `LinkageError: loader constraint violation`（作业 jar 是 shade jar，
   内含 `org/apache/kafka/**`，与 `flink-dist` 的父子类加载器各加载一份 Kafka 类）。
   这是 `scripts/deploy_surface_check.sh` 的判据④在守的东西。

---

## 1. 目标机器要求

| 项 | 要求 | 口径 |
| --- | --- | --- |
| 操作系统 | Windows 10/11 + WSL2（Ubuntu）**或**原生 Linux | 本仓在 Win10 + WSL2 + Docker Desktop 29.8.0 上验证 |
| Docker | Docker Desktop（Windows/macOS）或 Docker Engine（Linux），Compose **v2** | compose 是一等公民 |
| 内存 | **≥ 16 GB**（容器声明上限合计 11.5 GiB） | 11.5 GiB 是 `设计值`；本机天花板 11.68 GiB |
| 磁盘 | **≥ 40 GB**（镜像 25.69 GB + Doris 自建镜像 12.60 GiB + 数据） | `实测`（`docker image inspect`） |
| 网络 | 能拉镜像 + 能下载 Maven artifact + 能下 Doris 4.32 GB tarball | 网络位置事实，非平台事实 |

> ⚠️ **磁盘口径**：`docker image inspect` 实测——Kafka 0.64 GiB、Flink 1.46 GiB、
> Superset 1.18 GiB、Doris base-latest 1.83 GiB、**Doris 自建镜像 12.60 GiB**、
> mysql:8.0 1.01 GiB。**只算镜像就 ≈ 25.7 GB**，还没算 Docker 的 Build Cache。

---

## 2. 搬代码的三条路径（选一条）

### 路径 A：Git 克隆（推荐，前提是远端已同步）

```bash
git clone https://github.com/Gaomen-7/eco-d-a-platform.git
cd eco-d-a-platform
git checkout feat/ecom-pipeline-impl    # ← 当前工作分支；main 上没有全部成果
```

> ⚠️ **分支纪律**：本项目全部成果在 `feat/ecom-pipeline-impl` 上。
> 截至 `a188f31`（2026-10-27），远端 `main` 与 `feat/ecom-pipeline-impl`
> **指向同一个 commit**（已用 GitHub API 核对），所以 clone 下来默认在哪个分支都一样。
> 但**默认分支名随 clone 走**，仍建议显式 `git checkout feat/ecom-pipeline-impl`，
> 并确认 `git log --oneline -1` 是 `a188f31` 或其后。

### 路径 B：整目录拷贝（离线 / 仓库还没 push 时用）

从源机拷整个目录到目标机，**排除**下列大件与本机状态：

```bash
# 在目标机执行（把 /mnt/f/... 换成你的实际路径）
rsync -a --exclude='.git' --exclude='.venv' --exclude='.uv-cache' \
      --exclude='flink/target' --exclude='data' --exclude='artifacts' \
      --exclude='.pytest_cache' --exclude='.ruff_cache' \
      /mnt/f/Anaconda\ practice\ project/eco-a-d-platform/ ~/eco-a-d-platform/
```

**排除清单的理由**：

| 排除项 | 为什么 |
| --- | --- |
| `flink/target/` | JVM 构建产物；换机要重新 `mvn package`（且 jar 不该跨机拷贝——它里面的类要跟目标机的 Flink 版本对上） |
| `data/` | 门槛批原始数据（351 MB），由 `make replay` 现产 |
| `artifacts/` | verify 报告等运行产物 |
| `.uv-cache/`、`.venv/` | 本机 Python 环境，换机用 `uv sync` 重建 |
| `.git/` | 路径 A 已经拿到 git 历史 |

### 路径 C：连镜像一起搬（L3 跨机搬运，已实测）

源机打包 5 个镜像 → 目标机导入 → 直接走 L0：

```bash
# ── 源机 ──────────────────────────────────────────────
docker save -o eco-images.tar \
  apache/kafka:4.3.1 \
  apache/flink:2.2-java17 \
  apache/superset:6.1.0 \
  apache/doris:base-latest \
  eco/doris:4.1.5 \
  mysql:8.0
# 实测：6.05 GiB 单 tar，耗时 3m12s

# ── 目标机 ──────────────────────────────────────────────
docker load -i eco-images.tar
# 实测：20m12s（12.6 GiB 解包主导）
```

> ⚠️ **`docker save` 要连 base 镜像一起存**：Doris 的自建镜像是 `FROM
> apache/doris:base-latest`，不存 base 就得目标机重新下 1.83 GiB。
> Flink 同理（`FROM apache/flink:2.2-java17`）。
>
> **实测时间账（L3 档）**：save 3m12s / load 20m12s / 载入后 `make init` **7m14.9s**。
> 判据是「**载入后**起链路 ≤ 30 min」——搬运耗时单独记，不进判据。
>
> ⚠️ **但路径 C 解决不了两件事**：① 作业 jar 仍需在目标机编译；
> ② `data/` 门槛批仍需现产。所以路径 C **不能替代** §5、§6 的手工步骤。

---

## 3. 装工具链

### 3.1 Docker（必需）

- **Windows/macOS**：装 Docker Desktop → Settings → Resources → WSL Integration →
  勾选你的 Ubuntu 发行版 → Apply & restart。
  **验证**（在 WSL 内）：
  ```bash
  docker version --format '{{.Server.Version}}'   # 期望有输出（Server 段）
  test -S /var/run/docker.sock && echo socket-present
  ```
  这两条是本仓的执行面判据（`.scratch/.../HLD.md` §4.7）：WSL 内必须能拿到
  `/var/run/docker.sock`，否则 Docker 发布端口在 WSL 内不可达。
- **原生 Linux**：装 Docker Engine + compose v2 插件即可。注意 §8 有一条平台差异。

### 3.2 `make`（必需）

```bash
sudo apt-get update && sudo apt-get install -y make
```

### 3.3 `uv`（跑 `make verify` / `make replay` / `make spike` 需要）

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

Python 版本由 `uv.lock` 钉死为 **`==3.12.*`**（ADR-0001：锁文件是工具链唯一权威）。
`uv` 会自己装 3.12，**不要**用系统 Python。

### 3.4 JDK 17 + Maven（**只在构建 Flink 作业时需要**，不在起链路路径上）

Flink 2.2 与 Doris `start_fe.sh` 都要求 JDK **恰为 17**（非 17 直接 exit 1）。

**有 sudo 就直接装**：

```bash
sudo apt-get install -y openjdk-17-jdk-headless maven
java -version    # 期望 17.x
mvn -version
```

**没有 sudo 也能装**（本仓在 WSL 上用过的无 sudo 手法）：

```bash
bash scripts/p0-wsl-install-nodeb.sh   # 下 .deb → dpkg-deb -x 解到 ~/p0-toolchain
bash scripts/p0-wsl-fix-symlinks.sh    # 修 .deb 绝对符号链接悬空
```

产物在 `~/p0-toolchain`，环境变量脚本 `~/.p0-toolchain-env.sh` / `~/.p0-toolchain-env.mk`。
`Makefile:16` 会 `-include $(HOME)/.p0-toolchain-env.mk`——**这文件在仓库外**，
所以干净检出的机器没有它，`make` 就找不到 JDK/Maven（`scripts/build-flink.sh` 里也硬编码了
`$HOME/p0-toolchain/...`，那是本机事实，**换机必须改路径**）。

> 💡 **注意**：`scripts/build-flink.sh` 里有两行**硬编码的本机绝对路径**：
> ```bash
> cd "/mnt/f/Anaconda practice project/eco-a-d-platform/flink" || exit 1
> export PATH="$HOME/p0-toolchain/usr/share/maven/bin:..."
> ```
> 换机后**不能直接用这个脚本**，请改用下一节的通用命令，或自己改这两行。

---

## 4. 准备 `.env`（密钥面，不入库）

`.env` 被 `.gitignore` 覆盖，**Git 克隆后没有这个文件**，必须手工创建：

```bash
cp .env.example .env
```

至少要填两项：

```dotenv
# ① 必填：Superset 拒绝在没有它的情况下启动
SUPERSET_SECRET_KEY=<用下面这条命令生成>
#    python -c "import secrets;print(secrets.token_urlsafe(48))"

# ② 按你的网络位置选镜像站（国内推荐 daocloud；官方源可达就写 docker.io）
REGISTRY_MIRROR=docker.m.daocloud.io

# ③ 其余保持 .env.example 的默认值即可
KAFKA_RETENTION_MS=604800000
KAFKA_RETENTION_BYTES=2147483648
GEN_USER_POOL=100000
GEN_RATE=200
```

> ⚠️ **`.env.example` 里 `DORIS_IMAGE=` 是空的**——那是 P0 阶段的遗留（当时 Doris 未启用）。
> 现在**留空即可**（compose 会回落到默认值 `eco/doris:4.1.5`，正是我们要构建的 tag）。
> 只有当你想用**别的 tag** 时才需要填。
>
> ⚠️ **零密钥纪律**：`.env` 永远不入库，**不要**把它拷进任何会提交的目录。

---

## 5. 构建两个本地镜像（**最耗时的一步**）

### 5.1 Flink 镜像（带 Paimon connector）

Flink 官方镜像**不含** Paimon connector，必须本地烘焙。顺序不可换：
**先预取 jar，再构建镜像**。

```bash
# 第一段：预取两个 Paimon jar（幂等；已存在且校验通过会打印 SKIP）
bash scripts/fetch-paimon-jar.sh
```

> 这一步从 Maven Central 下载约 **108 MB**（`paimon-flink-2.2-2.1.0.jar` 57,697,871 B
> + `paimon-hadoop-uber-2.1.0.jar` 50,615,937 B），脚本会校验 **size + sha256**。
> 两个 jar 都是 `docker/flink/lib/` 的**构建上下文**内容，**不入库**（`.gitignore` 第 4b 节）。
>
> ⚠️ **两个 jar 都必须下**：`paimon-hadoop-uber` 是补 Paimon 上游缺陷
> （apache/paimon#6654：`file:` warehouse 仍要 hadoop 的 `Configuration` 类）用的。
> 少下第二个 ⇒ `CREATE CATALOG` 全成功，但**读写表时**抛
> `ClassNotFoundException: org.apache.hadoop.conf.Configuration`。

```bash
# 第二段：构建镜像（等价于 make build-flink-image）
docker compose build flink-jm
# 或显式：
# docker build -f docker/flink/Dockerfile -t eco/flink:2.2-paimon docker/flink
```

> 💡 其实 `make build-flink-image` 就是这两步的固化版，**推荐直接用它**：
> ```bash
> make build-flink-image
> ```
> 它先跑 fetch 脚本再 `docker compose build flink-jm`，已构建时秒过（有层缓存）。

### 5.2 Doris 镜像（最重的一步）

Doris 官方镜像站**没有**可直接拉取的 4.1.5（`apache/doris:4.1.5` /
`all-in-one-4.1.5` / `base-4.1.5` 全部 `denied`），只有 `base-latest` 是构建基座
（不含可运行的 FE/BE）⇒ **必须本地构建**。

```bash
docker build -f docker/doris/Dockerfile -t eco/doris:4.1.5 docker/doris
# 等价于：make build-doris-image
```

> ⏱ **实测 ≈ 35 min**（下载 457 s + 解压 712 s + 导出/解包 862 s）。
> Dockerfile 在**构建期容器内**下载 4.32 GB 的 `apache-doris-4.1.5-bin-x64.tar.gz`
> 并解压——**不要**改成 `COPY` 到构建上下文：实测那样上下文传输要 **3276 秒**后 EOF，
> Docker Desktop 直接被压垮退出。
>
> ⚠️ **文件是 gzip 不是 xz**：Dockerfile 用的是 `tar -xzf`。用 `-xJ` 会直接失败。
>
> ⚠️ **CPU 必须支持 AVX2**：默认包是 avx2 版（另有 `-noavx2` 变体）。
> 极老的 CPU 或某些 ARM 机器上构建会起不来。

---

## 6. 起链路 + 提交 Flink 作业

### 6.1 `make init`（四段，顺序不可换）

```bash
make init
```

| 段 | 做什么 | 为什么必须这样 |
| --- | --- | --- |
| ① | 默认 profile `up -d --wait`（Kafka / Flink JM+TM / Superset） | — |
| ② | `--profile doris up -d --wait` | 默认 `up` **不含 Doris** |
| ③ | 在 compose 网络内跑 `doris-init.sh` 建库建表 | Doris 建库必须从网内发起（Docker Desktop 两跳限制） |
| ④ | Superset 引导 + 看板置备 + Doris 断言 | `db upgrade` → `create-admin` → `init` → 建 dataset/图表 |

> ⏱ **实测（L0 制品态，镜像已在）**：`make init` **2m37.7s**（判据 ≤ 15 min）。
> 首次冷启（镜像要现拉）会慢得多，**L1 档豁免 30 min 判据**。

**跑完后检查**：

```bash
docker compose --profile doris ps
```

期望：

| 服务 | 要求 | 原因 |
| --- | --- | --- |
| `kafka` / `flink-jm` / `flink-tm` / `superset` / `doris-fe` / `doris-be` | **必须 healthy** | 都定义了 healthcheck |
| `kafka-init` | **只要已成功退出**（State=exited, ExitCode=0） | 一次性服务，**没有** healthcheck，HEALTH 列是 `<no value>` |
| `client` | **不计入常驻** | profile 隔离的按需工具容器 |

> ⚠️ **误把无 healthcheck 的服务判成「不健康」是本仓踩过的假红**。
> `docker compose ps` 不带 `-a` 时**看不到**已退出的 `kafka-init`，要加 `-a`。

### 6.2 ⚠️ 补做：构建 Flink 作业 jar（`make init` 不做）

```bash
cd flink
mvn -DskipTests package
```

产物：`flink/target/ecom-flink-0.1.0-SNAPSHOT.jar`

> ⚠️ **Maven 必须能联网**。`pom.xml` 里 Flink 2.2.1 / Kafka connector 5.0.0-2.2 /
> Doris connector 26.3.0 都是 `provided` 或需从 Central 拉。
> ⚠️ **IPv4 问题**（本仓在 WSL 上踩到）：WSL 的 `resolv.conf` 指向 `172.17.32.1`，
> `repo.maven.apache.org` **只解析出 IPv6** 而本机 IPv6 不通 ⇒
> `Temporary failure in name resolution`（不是 404）。加参数：
> ```bash
> MAVEN_OPTS="-Djava.net.preferIPv4Stack=true" mvn -DskipTests package
> ```
> 或改用 `repo1.maven.org`（走 IPv4 正常）——`scripts/fetch-paimon-jar.sh` 就是这么做的。

> ⚠️ **shade 插件的 `ServicesResourceTransformer` 不能删**：`pom.xml` 里它负责把
> `META-INF/services` 按行**合并**而不是覆盖。删掉它 ⇒ Doris connector 的
> `DynamicTableFactory` 被静默丢弃 ⇒ 作业提交时报
> `Could not find any factory for identifier 'doris'`。

### 6.3 ⚠️ 补做：提交 Flink 作业（`make init` 也不做）

**这一步仓库里原本没有现成脚本**（只有被 gitignore 的 `.uv-cache/deploy-job.sh`）。
现已固化为**入库的** `scripts/deploy-job.sh`（也可用 `make deploy-job`）：

```bash
make deploy-job
# 等价于：bash scripts/deploy-job.sh
```

它做四件事：清 `lib/` 里的作业 jar 副本 → **两端** `usrlib/` 各拷一份并比对 sha256 →
`flink run -d` 提交 → 等作业 `RUNNING` + 消费组 lag 归零。

如果 jar 已经在 `flink/target/`（不想重新编译）：

```bash
bash scripts/deploy-job.sh --no-build
```

**手工等价做法**（要理解每一步再照抄）：

```bash
# 在仓库根执行
JAR=flink/target/ecom-flink-0.1.0-SNAPSHOT.jar

# ① 先清干净：lib/ 里不许有作业 jar 副本（否则 LinkageError）
for c in eco-flink-jm eco-flink-tm; do
  docker exec "$c" bash -c 'rm -f /opt/flink/lib/ecom-flink.jar /opt/flink/usrlib/ecom-flink.jar'
  docker exec "$c" mkdir -p /opt/flink/usrlib
done

# ② 两端都拷到 usrlib（必须同一字节；只拷一端 = 半部署）
for c in eco-flink-jm eco-flink-tm; do
  docker cp "$JAR" "$c":/opt/flink/usrlib/ecom-flink.jar
done

# ③ 提交
docker exec eco-flink-jm /opt/flink/bin/flink run -d \
  -c com.ecom.flink.P1RealtimeJob /opt/flink/usrlib/ecom-flink.jar

# ④ 等 RUNNING（最多 200 s）
for i in $(seq 1 40); do
  out=$(curl -sf -m 3 http://127.0.0.1:8081/jobs/overview 2>/dev/null)
  case "$out" in *'"state":"RUNNING"'*) echo "RUNNING after $((i*5))s"; break;; esac
  sleep 5
done
```

> ⚠️ **为什么 jar 放 `usrlib` 而不是 `lib`**：作业 jar 是 shade jar，内含
> `org/apache/kafka/**`，而 `flink-dist-2.2.1.jar` 不含 ⇒ 放 `lib/` 会让父子类加载器
> 各加载一份 Kafka 类 ⇒ `LinkageError: loader constraint violation … committedOffsets`。
> `usrlib` 走用户类加载器，与父子关系不冲突。
>
> ⚠️ **拷完 jar 后不必重启 JM/TM**——只要 `lib/` 里**从来没有**过作业 jar。
> 若之前拷错过到 `lib/`，必须 `docker restart eco-flink-jm eco-flink-tm` 让新 JVM
> 不命中缓存类。
>
> ⚠️ **作业的 Kafka / Doris 地址是硬编码的容器名**（`JobConfig.java`：
> `KAFKA_BOOTSTRAP = "kafka:9092"`、`DORIS_FE = "doris-fe:8030"`）——
> 只要 compose 的服务名不变就无需改；改了服务名必须同步改 Java 常量并重编译。

### 6.4 灌一批数据（可选，但看板要有数）

```bash
# 实时发送（走宿主可达的 HOST listener 29092，不是 9092）
uv run ecomgen live --seed 42 --rate 200 \
  --bootstrap 127.0.0.1:29092 --topic ecom.behavior.event.v1

# 尖峰测试（判据 ≥1000 events/s）
make spike
```

> ⚠️ **bootstrap 用 `127.0.0.1:29092`**：compose 的 `9092` 口 advertised 成容器名
> `kafka:9092`，宿主解析不了那个主机名。

---

## 7. 验证「真的跑通了」

### 7.1 全链路判据

```bash
make verify
```

这是 **10 项机读判据**，**退出码 0 = 全过**：

| # | 判据 | 检查什么 |
| --- | --- | --- |
| 1 | `kafka-topic-retention` | topic 保留策略符合设计值 |
| 2 | `flink-job-running` | Flink 作业状态是 `RUNNING` |
| 3 | `paimon-five-layers` | 五层表**都真有行**（走 Flink SQL 真查） |
| 4 | `doris-ads-rows` | Doris 三张 ADS 表都有行 |
| 5 | `superset-health` | Superset 健康 |
| 6 | `funnel-cnt-inferred-monotone` | 漏斗各步人数单调递减 |
| 7 | `dlq-rate-no-increase` | 脏率没有相对基线增长 |
| 8 | `is-settled-two-states` | 窗口「暂定/落定」两态都成立 |
| 9 | `recon-offline-mirror-doris` | 离线口径与 Doris 供数点对账一致 |
| 10 | `degradation-ladder` | 降级阶梯（容器 OOM 次数不超阈值） |

期望输出结尾：

```
PASS=10 FAIL=0 SKIP=0  ⇒ 退出码 0
```

报告落在 `artifacts/verify/verify-report-<时间戳>.json`。

> ⚠️ **第 3 项要跑离线五层才有数据**：
> ```bash
> make replay          # 跑 ODS→DWD→DIM→DWS→ADS 五层
> make assert          # 离线断言集
> ```
> `make replay` 会**现产门槛批**（`ecomgen dump --events 1048576 --logical-span 3d
> --seed 20241022`，落 `data/threshold/raw.jsonl`，**351 MB，不入库**）。
> 首次跑要现产 + 五层 SQL，耗时不短。

### 7.2 部署面判据（11 条）

```bash
bash scripts/deploy_surface_check.sh     # 退出码 0 = 全 PASS
```

覆盖：具名卷同路径挂载、跨容器互见、down/up 数据仍在、作业 jar 在 `usrlib`、
Paimon connector 在两端 `lib`、Flink 补丁锚点 = 2.2.1、`ADD JAR` 零命中、
`.gitignore` 覆盖、七服务健康、数字标注审查。

> ⚠️ **这条脚本会 `docker compose down` 再 `up -d`**（不带 `-v`）——期间容器会重启，
> **作业 jar 随容器可写层丢失**！跑完要把 §6.3 重做一遍（作业回到 RUNNING）。
> 这是本仓的既定纪律：部署面验收的副作用必须归位。

### 7.3 打开看板

浏览器访问 **<http://127.0.0.1:8088>** → 无需登录 → 进 `ecom-live` 仪表盘。

五组图表：PV / UV / GMV 大数字 + PV 分钟趋势 + 四步转化漏斗 + 品类与地域分布 + 脏率。

> 看板 **10 秒自动刷新**（`DASHBOARD_AUTO_REFRESH_INTERVALS` 的最小档恰是 10s）。
> 因为数据粒度是 60 秒，大部分刷新会「值没变」——**那是正常的**。
> 要看到跳变，等整分钟边界后的窗口关闭。

---

## 8. 换机后最容易踩的坑（按踩到概率排序）

| # | 症状 | 真因 | 处置 |
| --- | --- | --- | --- |
| 1 | `make verify` 第 2 项 FAIL（`flink-job-running`） | **`make init` 不提交作业** | 补做 §6.2 + §6.3 |
| 2 | `make init` 报 Doris `image not found` | **Doris 镜像必须本地构建，compose 无 `build:` 段** | 补做 §5.2 |
| 3 | `make up` 报 Flink `image not found` | Flink 镜像未构建 | `make build-flink-image`（§5.1） |
| 4 | 作业报 `LinkageError: loader constraint violation` | 作业 jar 放进了 `/opt/flink/lib` | 只放 `usrlib`，并清 `lib` |
| 5 | 作业报 `Could not find any factory for identifier 'doris'` | shade 的 `ServicesResourceTransformer` 被删 | 恢复 `pom.xml` 的 transformer |
| 6 | `CREATE CATALOG` 全成功但读写表抛 `ClassNotFoundException: org.apache.hadoop.conf.Configuration` | 少下 `paimon-hadoop-uber` | 重跑 `scripts/fetch-paimon-jar.sh` |
| 7 | `No backend available` / FE 卡 `feType:UNKNOWN` | Doris 把自身 IP 写进元数据，容器重建后 IP 变 | compose 已用固定子网钉死 IP；**别改 `networks:` 段** |
| 8 | BE 起来就 exit 1 | `vm.max_map_count` / `swapon` / `ulimit` 前置检查不过 | Dockerfile 已设 `SKIP_CHECK_ULIMIT=true`；别删 |
| 9 | 端口 bind 失败（`8030/8040/8081/8088`） | Windows 宿主的 WinNAT 动态排除段 | `netsh interface ipv4 show excludedportrange protocol=tcp`；非管理员无法释放，只能等它漂走 |
| 10 | `make spike` 连不上 Kafka | 用了 `9092`（advertised 成 `kafka:9092`，宿主解析不了） | 改用 `127.0.0.1:29092` |
| 11 | Maven `Temporary failure in name resolution` | WSL 的 IPv6 不通，`repo.maven.apache.org` 只解析出 IPv6 | `MAVEN_OPTS="-Djava.net.preferIPv4Stack=true"` |
| 12 | `No module named 'pymysql'` / 看板连接断链 | Superset 容器不是走 bootstrap 引导的 | `docker compose up -d --force-recreate superset` |
| 13 | `make init` 卡在 `doris-init` 等 FE | BE 未注册 | `doris-init.sh` 自带轮询；看 `docker compose logs doris-be` |
| 14 | `sql-client.sh -f` 明明报错，退出码却是 0 | 官方 `CliClient` 丢弃返回值、无 `System.exit` | 离线断言改用**哨兵行**机制（`scripts/lake_assert.sh`） |

> ⚠️ **不要用 HTTP curl 判断 `9010/9030/9060/9092/29092` 的连通性**——它们是
> MySQL / brpc / Kafka 协议，curl 会报「连不上」但 TCP 层其实是通的。
> **协议层失败 ≠ 端口不通**。

---

## 9. 平台差异（原生 Linux vs Docker Desktop）

本仓 `reproducibility-boundary.md` §5 有完整的「能力 × 平台」表。与部署最相关的三条：

| 项 | Windows/macOS + Docker Desktop | 原生 Linux + Docker Engine |
| --- | --- | --- |
| `client` 容器（`mysql:8.0`） | **必需**：Doris Stream Load 是 FE→BE 两跳，第二跳是容器地址，宿主 follow 不到 | **可选**：官方原话「Linux hosts can reach the nodes directly」⇒ 可直连 FE |
| 内存天花板 | 由 Docker Desktop 设置决定 | = 宿主 RAM（本仓的 11.68 GiB 是**本机数值**，不是通用值） |
| JDK/Maven 装法 | 常用无 sudo `.deb` 解包到 `~/p0-toolchain` | apt/dnf 正常装 |

> 💡 **原生 Linux 上仍然照跑本指南**：`client` 只是「可选」不是「禁用」，
> compose 里它照常定义、脚本照常调它，只是它不再是**必需**的中转。
> 跑通后再考虑简化。

---

## 10. 日常运维速查

| 动作 | 命令 |
| --- | --- |
| 起 / 停 | `make up` / `make down`（**保留卷**，数据不丢） |
| 完全重来（**删数据**） | `make down && docker compose down -v && make build-flink-image && make build-doris-image && make init && make deploy-job` ⚠️ `-v` 删卷 |
| 重建并重提作业 | `make deploy-job`（改了 Java 之后） |
| 重算离线五层 | `make replay`（`LAYER=dwd` 可截断到某层，含） |
| 离线断言 | `make assert` |
| 幂等校验 | `make replay-idempotent` |
| 尖峰测试 | `make spike`（`DRY_RUN=1` 退回预演） |
| 容错演练 | `make verify-fault`（kill TM → 从 checkpoint 恢复） |
| 反例证伪 | `make verify-fault-corrupt`（**期望退出非 0**；返回 0 说明判据恒真） |
| 看状态 + 日志 | `make logs` |
| 进网内调试 | `make client` |
| 静态检查 | `make lint && make typecheck` |
| 单元测试 | `uv run pytest -q`（约 400 条） |
| 配置断言 | `make config-check` |

### 冻结目标名（**不得改名**，SRS §3.4）

`up` / `down` / `init` / `replay` / `verify` / `spike` / `verify-fault` / `logs`

---

## 11. 部署完成检查清单

逐条打勾，全绿才算搬完：

- [ ] `docker version` 有 Server 段；WSL 内 `/var/run/docker.sock` 存在
- [ ] `java -version` 是 17；`mvn -version` 可用（**只在构建作业时必需**）
- [ ] `uv --version` 可用；`uv.lock` 未改动
- [ ] `.env` 已创建，`SUPERSET_SECRET_KEY` 已填
- [ ] `docker/flink/lib/` 下**两个** paimon jar 都在，size+sha256 校验通过
- [ ] `docker image inspect eco/flink:2.2-paimon` 存在
- [ ] `docker image inspect eco/doris:4.1.5` 存在（12.60 GiB）
- [ ] `make init` 跑完无「待补」字样
- [ ] `docker compose --profile doris ps` 六个常驻服务全 `healthy`；`kafka-init` 已退出 0
- [ ] `flink/target/ecom-flink-0.1.0-SNAPSHOT.jar` 已构建
- [ ] `make deploy-job` 跑完，作业 `RUNNING`
- [ ] 作业 jar 在**两端** `/opt/flink/usrlib/`，`/opt/flink/lib/` 里**无**作业 jar 副本
- [ ] `curl http://127.0.0.1:8081/jobs/overview` 含 `"state":"RUNNING"`
- [ ] `make replay` 五层跑完；`make assert` 通过
- [ ] `make verify` → `PASS=10 FAIL=0 SKIP=0`，退出码 0
- [ ] `bash scripts/deploy_surface_check.sh` → 11 条全 PASS（**跑完记得重做 §6.3**）
- [ ] 浏览器开 <http://127.0.0.1:8088> 无痕可见 `ecom-live` 面板且有数字

---

## 12. 附：交给 agent 的部署 prompt

> **用法**：把下面整段（代码块内）复制给目标机上的 agent 会话。
> 它已经把「查什么、怎么判、失败了怎么办」写全，agent 不需要重新推导。

````markdown
# 任务：把 eco-a-d-platform 部署到当前这台机器

仓库已在 `<把仓库路径填这里>`。请按下面的顺序执行，**每一步都要用机读判据确认，
不要只看「看起来在跑」**。本仓的纪律是「配置可见 / UI 里能看到 / 日志没报错」都不算证据。

## 铁律（先读，不可协商）

1. **执行面 = WSL2**（Linux shell）。所有 `make` / `docker` / `bash` 命令都在 WSL 内跑，
   不要在 Windows PowerShell / CMD 里跑。仓库在 `F:\` 时 WSL 侧是 `/mnt/f`。
   ⚠️ 但**不要**在命令里写死 `/mnt/f` 绝对路径——compose 与脚本一律用**相对路径**。
2. **判据必须有退出码**。`sql-client.sh -f` 报错后仍以 0 退出（官方 `CliClient` 丢弃返回值），
   所以离线断言用哨兵行机制（`scripts/lake_assert.sh`），不要信退出码。
3. **数字要标性质**：凡时间/容量/阈值，标 `官方给出` 或 `设计值`，不得把设计值当官方保证。
4. **不要改版本锚点**（Flink 2.2.1 / Kafka 4.3.1 / Doris 4.1.5 / Superset 6.1.0 /
   Python 3.12.*）。要改走冻结项变更流程。
5. **不要 push**。本仓约定 agent 只本地 commit。
6. **失败要报真因**，不要把「接口里没有这个 dataset」「服务没起来」这类不同失败压成同一句话。

## 现状与缺口（我已勘察过，你不要重新推导）

仓库当前的**部署面有三个缺口**，都会让 `make verify` 失败，必须补齐：

- **缺口 A**：`docker-compose.yml` 里 `doris-fe` / `doris-be` **只有 `image:` 没有 `build:` 段**，
  而 Doris 官方**没有**可直接拉取的 4.1.5 版本化镜像 ⇒ 必须本地构建
  `docker build -f docker/doris/Dockerfile -t eco/doris:4.1.5 docker/doris`。
  ⚠️ **不要**把它改成 `COPY` 一个 4.32 GB tarball 到构建上下文——实测上下文传输
  3276 秒后 EOF。Dockerfile 现在的做法（构建期容器内 curl 下载）是踩坑后的正解。
- **缺口 B**：`make init` **不构建也不提交 Flink 作业**。作业 jar 要用
  `cd flink && mvn -DskipTests package` 构建，再 `docker cp` 到**两端**的
  `/opt/flink/usrlib/`，然后 `docker exec eco-flink-jm /opt/flink/bin/flink run -d
  -c com.ecom.flink.P1RealtimeJob /opt/flink/usrlib/ecom-flink.jar`。
  ⚠️ **作业 jar 绝对不能放 `/opt/flink/lib/`**——那是 shade jar，内含 kafka 类，
  放错目录 ⇒ `LinkageError: loader constraint violation`。
  ✅ 仓库里已有一个入库的 `scripts/deploy-job.sh`（`make deploy-job`）做这件事——
  **优先用它**，不要另写一份。
- **缺口 C**：`.env` 不入库，换机后必须 `cp .env.example .env` 并填
  `SUPERSET_SECRET_KEY`（`python -c "import secrets;print(secrets.token_urlsafe(48))"`）。
  ⚠️ **不要**把 `.env` 内容打印出来或提交。

## 执行步骤

### 第 1 步：环境前置（只做检查，不装系统级东西除非我批准）

```bash
docker version --format '{{.Server.Version}}'    # 要有 Server 段
docker compose version
test -S /var/run/docker.sock && echo socket-present   # WSL integration
java -version ; mvn -version ; uv --version ; make --version
df -h . ; free -g
```

缺什么先报给我，**不要**自己 `sudo apt-get install`（WSL 的 sudo 要交互密码，你代劳不了）。
无 sudo 装 JDK17/Maven 的手法仓库里有：`scripts/p0-wsl-install-nodeb.sh`
+ `scripts/p0-wsl-fix-symlinks.sh`（解包到 `~/p0-toolchain`，产出 `~/.p0-toolchain-env.mk`，
`Makefile:16` 会 include 它）。

### 第 2 步：`.env`

`cp .env.example .env`，生成并写入 `SUPERSET_SECRET_KEY`。
`REGISTRY_MIRROR` 按本机网络可达性选（国内 `docker.m.daocloud.io`）。
**不要提交 `.env`**。

### 第 3 步：构建两个本地镜像

```bash
make build-flink-image      # = fetch-paimon-jar.sh + docker compose build flink-jm
make build-doris-image      # = docker build -f docker/doris/Dockerfile -t eco/doris:4.1.5 docker/doris
```

- fetch 脚本要下**两个** jar（`paimon-flink-2.2-2.1.0.jar` + `paimon-hadoop-uber-2.1.0.jar`），
  校验 size+sha256。少第二个 ⇒ 读写 Paimon 表时抛
  `ClassNotFoundException: org.apache.hadoop.conf.Configuration`。
- Doris 构建 ≈35 min，是正常耗时，**不要**因为慢就中断或改 Dockerfile。

### 第 4 步：起链路

```bash
make init
docker compose --profile doris ps
```

判据：六个常驻服务（kafka / flink-jm / flink-tm / superset / doris-fe / doris-be）全 `healthy`；
`kafka-init` 是**一次性服务**，判「State=exited 且 ExitCode=0」而**不是** healthy
（它没有 healthcheck，HEALTH 列是 `<no value>`）；`client` 是 profile 隔离的按需容器，不计入常驻。

### 第 5 步：构建并提交 Flink 作业（缺口 B）

```bash
make deploy-job        # = bash scripts/deploy-job.sh
```

它会：构建 jar（`mvn -DskipTests package`）→ 清 `lib/` 副本 → 两端拷 `usrlib` 并比对 sha256
→ `flink run -d` → 等 RUNNING → 等消费组 lag=0。

⚠️ Maven 报 `Temporary failure in name resolution` 就是 IPv6 问题——脚本已默认带
`-Djava.net.preferIPv4Stack=true`（`repo.maven.apache.org` 只解析出 IPv6 而本机 IPv6 不通）。
⚠️ 报 `LinkageError: loader constraint violation` ⇒ 作业 jar 被放进了 `lib`；
用 `bash scripts/deploy-job.sh --restart` 重做（让新 JVM 不带缓存类）。

### 第 6 步：灌数据 + 离线五层

```bash
uv run ecomgen live --seed 42 --rate 200 --bootstrap 127.0.0.1:29092 --topic ecom.behavior.event.v1
make replay          # 现产门槛批 351 MB + 五层 SQL
make assert
```

⚠️ bootstrap 用 **`127.0.0.1:29092`**（HOST listener）；`9092` advertised 成容器名
`kafka:9092`，宿主解析不了。

### 第 7 步：判据（全部必须过）

```bash
make verify                                  # 期望 PASS=10 FAIL=0 SKIP=0，退出码 0
bash scripts/deploy_surface_check.sh         # 期望 11 条全 PASS，退出码 0
make config-check                            # 期望 PASS
uv run pytest -q                             # 期望全绿
```

⚠️ **`deploy_surface_check.sh` 会 `docker compose down` 再 `up -d`**（不带 `-v`），
容器重建 ⇒ **作业 jar 随可写层丢失** ⇒ 跑完**必须重做第 5 步**把作业恢复到 RUNNING，
并在最终报告里说明这一点。

⚠️ `make verify` 的第 3 项（paimon-five-layers）依赖第 6 步的五层数据；
若报 SKIP 属正常（场景未激活），但**不许**把它当 PASS 交差。

### 第 8 步：看板

`curl -s http://127.0.0.1:8088/health` 期望返回纯文本 `OK`；
匿名 `GET /superset/dashboard/ecom-live/` 期望 200（不重定向 login）；
`GET /superset/dashboard/ecom-live/?standalone=1` 期望 200。

## 交付物

1. **一份部署记录**（Markdown，放仓库 `docs/` 或 `artifacts/` 下，你定）：
   记录每一步的**实际**输出与耗时、镜像体积（用 `docker image inspect` 的 `.Size`，
   **不要**用 `docker images`）、最终 `make verify` 与 `deploy_surface_check.sh` 的
   完整报告、遇到的坑与真因。
2. **凡是你为了让部署跑通而改动的东西**，单独列一份清单，说明「改了什么 / 为什么 /
   会不会破坏既有判据」。**任何改动都要先跟我确认再落盘**（尤其 compose、Dockerfile、
   pom.xml、脚本的路径与锚点）。
3. **不要 push**。

## 排障速查（按概率排序）

- `flink-job-running` FAIL → 第 5 步没做或作业没起来
- Doris `image not found` → 第 3 步没做
- Flink `image not found` → `make build-flink-image`
- `LinkageError: loader constraint violation` → 作业 jar 放进了 `lib`，只放 `usrlib`
- `Could not find any factory for identifier 'doris'` → `pom.xml` 的 shade
  `ServicesResourceTransformer` 被删了
- `ClassNotFoundException: org.apache.hadoop.conf.Configuration` → 少下 hadoop-uber jar
- `feType:UNKNOWN` / `No backend available` → 动了 compose 的 `networks:` 固定子网段
- BE exit 1 → Dockerfile 的 `SKIP_CHECK_ULIMIT=true` 被删了
- 端口 bind 失败（8030/8040/8081/8088）→ Windows WinNAT 动态排除段漂移，
  `netsh interface ipv4 show excludedportrange protocol=tcp` 查看；非管理员无法释放
- `make init` 卡在等 FE → `docker compose logs doris-be` 看 BE 是否注册成功
- ⚠️ **不要用 HTTP curl 判断 `9010/9030/9060/9092/29092` 的连通性**——它们是
  MySQL / brpc / Kafka 协议，curl 失败但 TCP 层可能是通的。协议层失败 ≠ 端口不通。
````

---

## 附：本文与仓库其它文档的关系

| 文档 | 管什么 | 什么时候读 |
| --- | --- | --- |
| `README.md` | 项目是什么、怎么用、八个冻结目标 | 第一次接触这个项目 |
| **本文** | **换机部署：从零到跑通** | **要把它搬到另一台电脑** |
| `docs/adr/0001-uv-lockfile-is-toolchain-authority.md` | 为什么锁文件是工具链权威 | 工具链出怪问题 |
| `docs/adr/0002-flink-2-2-1-patch-anchor.md` | 为什么 Flink 钉 2.2.1 | 升级 Flink 前 |
| `.scratch/ecom-user-behavior-pipeline/reproducibility-boundary.md` | L0–L3 四档契约、跨平台成立性表 | 要改部署面设计 |
| `.scratch/ecom-user-behavior-pipeline/HLD.md` §4 | 运行环境与沙箱约束（为什么这么写） | 改 compose / Dockerfile 前 |
| `GLOSSARY.md` | 唯一术语表 | 遇到生词 |
| `sql/README.md` | 五层形状 + 三个最容易写反的约定 | 改离线 SQL |

> ⚠️ **文档漂移是本仓明令禁止的**。如果你执行本文时发现某条与代码不符，
> **以代码和 `make verify` 的实测为准**，并请提 issue 指出——请顺手改掉本文。
