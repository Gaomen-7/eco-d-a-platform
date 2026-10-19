> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（沙箱与部署形态）｜ 上游：FSR/SRS/GDD 冻结项 ｜ 状态：一手核查，未实测的已标注

## 0. 命名：本仓「沙箱」的两个所指（必须分开）

| 代号 | 所指 | 约束来源 | 本文结论落点 |
| --- | --- | --- | --- |
| **甲 · 运行面沙箱** | 跑这条链路的**容器运行面**（Docker Desktop / Podman / devcontainer / WSL 原生） | `REQ-F-ORCH-01/02/04/05/06/07`、C1/C2/C6、R3/R6 | 甲-1…甲-5 |
| **乙 · 会话沙箱** | **agent 会话**的文件写权限模式（`read-only` / `workspace-write` / `danger-full-access`） | U3、R8、P0「修沙箱」 | 乙-1…乙-3 |

两者**无因果关系**：甲决定 compose 里能不能写 `mem_limit`，乙决定 agent 能不能写下这个文件。既有文档把它们混在 R8/U3 一处，是术语债。

## 甲. 容器/运行面沙箱

### 甲-1 候选对照（本机实测 + 一手文档）

本机实测（只读）：Docker Client **29.8.0**（API 1.56）、Compose **v5.5.1**、Buildx v0.37.0；
Context `desktop-linux` → `npipe:////./pipe/dockerDesktopLinuxEngine`；**daemon 当前未运行**（探测时停止，故 `docker info`/`docker images` 无 server 段、本机镜像清单不可得）。
WSL：**3.0.3.0**，内核 **6.18.54.1-1-microsoft-standard-WSL2**，Windows **10.0.19045.6456**。
`wsl -l -v`：`Ubuntu`(Stopped, v2)、**`docker-desktop`(Stopped, v2)** —— 与官方「Docker Desktop runs inside its own `docker-desktop` WSL distribution」一致（[WSL2 backend](https://docs.docker.com/desktop/features/wsl/)）。

| 候选 | agent 友好度（A） | 本机占用（B，实测） | 接缝兼容（C） | 对「一条命令起链路」的影响 |
| --- | --- | --- | --- | --- |
| **① Docker Desktop / WSL2（现状）** | `docker compose config` **实测可用且无需 daemon**，会把 `mem_limit` 正常化为字节值、保留 `host_ip`/`logging.options`/`restart`；错误结构化（exit code + 明确文本）。官方 compose 骨架：Doris/Superset/Flink/Kafka **皆有官方来源** | 已装；引擎数据在 **F:**（`settings-store.json` 的 `CustomWslDistroDir=F:\Docker Desktop\DockerDesktopWSL`），实测 `disk\docker_data.vhdx`=**15.85 GB**、`main\ext4.vhdx`=0.09 GB；`UseContainerdSnapshotter=true` | **全绿**：`mem_limit`、`-p 127.0.0.1:H:C`、named volume、`restart`、`profiles`、logging 轮转全为一等公民 | **达成**：`docker compose up` 即判据本身；且是**唯一已被 SRS A1 确认可用**的形态 |
| **② Podman Desktop + podman-compose** | 源码级核查（`podman_compose.py` v1.6.0）：支持 `mem_limit`**与** `deploy.resources.limits.memory`（L1098–1123）、`host_ip` 端口（L1157–1162）、`logging.driver/options`（L1511–1513）、`restart`（L1577）、`profiles`（L3121+） | **未安装**（本机无 podman）。Windows 上 `podman machine` 支持 `wsl*`/`hyperv` 两 provider（[podman-machine(1)](https://github.com/containers/podman/blob/main/docs/source/markdown/podman-machine.1.md)）。将新增**第二套 VM/栈** | **部分**：`config` 子命令只 `print(compose.merged_yaml)`，**无校验无正常化** → GDD「`docker compose config` 列 `mem_limit`」判据**没有等价物**；`logging` 未指定 driver 时默认 `k8s-file` 而非 `json-file`（L1512）；`memswap_limit` 无处理 | **回归**：需重写 Makefile 目标与验证脚本；「零账号/干净机器一条命令」多一个前置（装 Podman Desktop） |
| **③ devcontainer（docker-outside-of-docker）** | 官方 feature README 质量高、选项表清晰；但引入 devcontainer CLI/VS Code 前置 | 复用宿主 Docker，额外一层容器 | **受限**：官方明确「does not currently enable bind mounting the workspace folder by default, **and cannot support folders outside of the workspace folder**」；变通是 `LOCAL_WORKSPACE_FOLDER` 或改 `workspaceFolder` | **不达成**：把「一条命令」变成「先装 VS Code + devcontainer + 正确配 socket 与路径」，且路径语义被改写 |
| **④ WSL2 里原生装（不用 Docker）** | 无 compose 骨架；Kafka/Flink/Paimon 可原生、**Doris 官方只给 Docker 路径**、Superset 依赖多 | 免掉 Docker Desktop 自身；但失去逐容器 cgroup 边界 | **不可用**：`mem_limit`、`restart: unless-stopped`、10 MB×3 日志驱动、named volume、`docker compose down` 不丢数据——五项判据**全部落空** | **不达成**，且破坏 C3/C5 之外的可复现性核心 |

**甲-1 结论**：维持 ①。② 的正确定位是**书面备选**（Podman Desktop 的收益是免 Docker Desktop 授权与 daemon，本仓不需要）；③④ 排除。若未来要换 ②，需先补一份 `config` 等价校验脚本（本仓可自写 parser，成本约 `make verify` 一项断言）。

> **GDD D15 / §1.7【G2】须改写（本次最重要的单条）**：Doris **有官方 compose**——
> `docker/runtime/all-in-one/4.1/compose/multi-node.yml`（3 FE + 3 BE）与 `cloud.yml`（[目录](https://github.com/apache/doris/tree/master/docker/runtime/all-in-one/4.1/compose)），
> 且有官方 **all-in-one 单容器镜像** `apache/doris:all-in-one-<ver>`（1 FE + 1 BE，**<20 s healthy**、自带 `HEALTHCHECK`、
> `replicas` 强制 1 故 `CREATE TABLE` 免 `replication_num`、持久化挂 `/opt/apache-doris/fe/doris-meta` + `/opt/apache-doris/be/storage`、启停幂等）
> —— [官方 README](https://github.com/apache/doris/blob/master/docker/runtime/all-in-one/4.1/README.md)。
> 「官方无 compose、须自写并自担 FE/BE 注册」→ 改为「官方 compose 可作骨架，自写量降为**裁剪+加 `mem_limit`/端口绑定**」。
> 官方 README **同时给出一条对本仓有约束力的坑**（见甲-3）。

### 甲-2 资源约束的确切写法与真实生效层

| 问题 | 一手结论 | 出处 |
| --- | --- | --- |
| `mem_limit` vs `deploy.resources.limits.memory`，非 Swarm 下谁生效 | `mem_limit` 是 **service-level** 字段：「configures a limit on the amount of memory a container can allocate」，且「When set, `mem_limit` must be consistent with the `limits.memory` attribute in the Deploy Specification」。`deploy` 是**可选**段：「**If not implemented the `deploy` section is ignored and the Compose file is still considered valid**」 | [services.md](https://github.com/docker/docs/blob/main/content/reference/compose-file/services.md)（`mem_limit`）、[deploy.md](https://github.com/docker/docs/blob/main/content/reference/compose-file/deploy.md) |
| 官方是否明说「`docker compose up` 一律忽略 `deploy.resources.limits.memory`」 | **官方未给出**现行明确表述（旧 v3 口径称非 Swarm 时忽略）。**可操作结论**：**只写 `mem_limit`，不与 `deploy.limits.memory` 并存**——并存时官方要求二者「必须一致」，写错一个即自相矛盾 | 同上 |
| `cpus` | 同构：`cpus` 必须与 `deploy` 的 `cpus` 一致；`0.000` 表示不限 | [services.md](https://github.com/docker/docs/blob/main/content/reference/compose-file/services.md) |
| WSL2 内核 cgroup v2 下是否真被内核执行 | **是**。本机 WSL 探测：`/proc/self/cgroup` = `0::/non-systemd`（纯 v2）；`mount` = `cgroup2 on /sys/fs/cgroup`；`cgroup.controllers` = `cpuset cpu io memory hugetlb pids rdma`；`cgroup.subtree_control` 已启用 `cpu memory pids`。内核文档：`memory.max` 即硬限，「This is the main mechanism to limit memory usage of a cgroup. If a cgroup's memory usage reaches this limit and can't be reduced, **the OOM killer is invoked**」 | [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files)；[kernel 源码 rst L1405–1411](https://github.com/torvalds/linux/blob/master/Documentation/admin-guide/cgroup-v2.rst) |
| 容器级默认与超限语义 | 「By default, a container has **no resource constraints**」；超限即 `OOME`「**Any process is subject to killing, including Docker**」；`-m` 最小值 `6m`；官方**劝阻** `--oom-kill-disable`。⚠️ 「Inside the container, tools like `free` report the **host's** available swap, not what's available inside the container」 | [Resource constraints](https://github.com/docker/docs/blob/main/content/manuals/engine/containers/resource_constraints.md) |
| `memswap_limit` 语义（本仓未设，但会影响 8 GB 判据） | 若只设 `memory` 不设 `memswap_limit`，容器**可用等量 swap**（`300m`→总计可 600m）；设成与 `memory` 相等 = 禁用 swap | [services.md](https://github.com/docker/docs/blob/main/content/reference/compose-file/services.md) |
| `.wslconfig` 的 `memory`/`processors`/`swap` 语义 | `memory` 默认 **「50% of total memory on Windows」**；`processors` 默认 **「The same number of logical processors on Windows」**；`swap` 默认 **「25% of memory size on Windows rounded up to the nearest GB」**；`swapFile` 默认 `%Temp%\swap.vhdx` | [wsl-config.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/wsl-config.md) |
| 取值是否必须带单位 | **必须**（除非要字节）：「Entries with the `size` value default to **B (bytes)**, and the unit is omissible. To use other units, the size unit must be appended, e.g.: `8GB` or `512MB`」。`memory=12` 只有 12 字节 | 同上 |
| 是否必须 `wsl --shutdown` | **官方未给出强制性**：原文是「you **may need** to run `wsl --shutdown`」，并给出「8 second rule」与 `wsl --terminate <distroName>` 两个替代路径。→ FSR/SRS 写的「官方要求改后 `wsl --shutdown` 生效」**偏强**，应改为「may need」 | 同上（"The 8 second rule"、`.wslconfig` 注） |
| 是否新增 C 盘占用 | `memory` **不占磁盘**（只封顶物理 RAM，动态分配）。但 **`swap=2GB` 落在 `swapFile`**，默认路径 `%Temp%\swap.vhdx` 即 **C 盘**。实测 C: 剩 **9.57 GB**（`Get-PSDrive`），WSL 内 `df /mnt/c` 剩 9.6 GB → **本仓 C 盘是真的紧** | 同上 + 本机实测 |
| sparse VHD 行为 | `[experimental] sparseVhd` 默认 **`false`**：「When set to `true`, **any newly created VHD** will be set to sparse automatically」。本仓 `.wslconfig` **未设**此项 → 新 VHD 非稀疏。`defaultVhdSize` 默认 **1099511627776（1 TB）** 只封顶发行版文件系统 | 同上 |
| 本机 `.wslconfig` 现存键（只读） | `[wsl2] memory=12GB`、`swap=2GB`；`[experimental] autoMemoryReclaim=gradual`。**未设** `processors`/`networkingMode`/`firewall`/`localhostForwarding`/`sparseVhd` → 全走默认（processors=12、NAT、firewall=true、localhostForwarding=true、autoMemoryReclaim 官方默认实为 **`dropCache`**，本仓选了 `gradual` 属**偏离默认**） | 本机 `%UserProfile%\.wslconfig` |

**甲-2 结论**：逐服务显式 `mem_limit` 是**正确且唯一可移植的写法**；内核确实执行；**但 `mem_limit` 管不到 swap** —— 要在 8 GB 判据上不留后门，应同时显式 `memswap_limit` 等于 `mem_limit`（官方明文：二者相等 = 容器无 swap 可借）。本仓至今未写这一条，是 `REQ-F-ORCH-02` 的可渗漏点。
`docker compose config` **实测**会把 `mem_limit: 128m` 正常化为 `"134217728"`、并原样保留 `host_ip: 127.0.0.1`、`logging.options.max-size/max-file`、`restart: unless-stopped` —— 故 GDD 新增判据「config 能列出每个服务的 `mem_limit`」**成立**；但 **config 不会报缺**，`REQ-F-ORCH-02` 的「不允许有服务缺省」仍需脚本自行断言。

### 甲-3 端口与网络

| 问题 | 一手结论 | 出处 |
| --- | --- | --- |
| `-p 127.0.0.1:H:C` 到底绑在哪里 | **绑在 Windows 宿主侧**，不是 WSL 内。官方：「All inbound connections pass through the Docker Desktop backend process（`com.docker.backend.exe` on Windows）, which handles port forwarding into the VM」 | [networking-how-tos](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/networking/networking-how-tos.md) |
| **能不能保证局域网不可访问（`REQ-F-ORCH-06` 的判据）** | **能，且本机版本已排除历史漏洞**：官方明确「If you include the localhost IP address (`127.0.0.1`, or `::1`) with the publish flag, **only the Docker host can access** the published container port」；并在同段挂出警告：「In releases **older than 28.0.0**, hosts within the same L2 segment … can reach ports published to localhost」（moby/moby#45610）。本机 Docker **29.8.0 > 28.0.0** → 该 L2 渗漏**已修**。另：`nat` 网关下「publishing a port to an address on the loopback interface means **remote hosts cannot access it**」 | [port-publishing](https://github.com/docker/docs/blob/main/content/manuals/engine/network/port-publishing.md) |
| 不写 host IP 的后果 | 官方警告：「Docker binds to all interfaces (`0.0.0.0`), **bypassing host firewall rules**」。→ 本仓**每个** `ports` 都必须带 `127.0.0.1` | [services.md `ports`](https://github.com/docker/docs/blob/main/content/reference/compose-file/services.md) |
| WSL 侧还有没有第二条暴露路径 | 有，但本仓不触发：WSL2 默认 **NAT**，「This isn't the default case in WSL 2… you will need to go through the same steps as you would for a regular virtual machine」（官方示例用 `netsh interface portproxy add … listenaddress=0.0.0.0`）。**不建 portproxy = 无第二条** | [networking.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/networking.md) |
| 容器→容器 vs 宿主→容器 | 容器间用 compose 网络内 **service name**（DNS）。宿主侧：官方明文「**Per-container IP addressing is not possible** — because the Docker `bridge` network is not reachable from the host」 | [networking-how-tos](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/networking/networking-how-tos.md) |

> **★ 由「宿主不可路由到容器」推出的硬约束（官方 Doris 原话）**：「On Docker Desktop the container addresses are not routable from the host, so **a stream load from the host cannot follow FE's redirect to a BE; run it from the `client` service instead**, which is inside the network. Linux hosts can reach the nodes directly.」（[README](https://github.com/apache/doris/blob/master/docker/runtime/all-in-one/4.1/README.md)）
> FE 的 Stream Load 是「FE 收标 → **重定向到某个 BE**」的两跳；第二跳是**容器地址**，宿主侧 follow 不到。
> ⇒ 这**直接冲击 `REQ-F-RT-06`**：`make verify` 若在**宿主**上跑 Stream Load / 查 BE，会失败。本仓必须把「发起导入」的落点放进 compose 网络内部（官方给的做法就是加一个 `client` 服务，其 `depends_on` 等全链路 `service_healthy` 后 idle，并在网络内持有 `mysql` 与 `curl`）。
> 这是**本次调研对架构影响最大的一条**，且 GDD/FSR/SRS 三份文档**都没有**。

### 甲-4 存储形态

| 问题 | 一手结论 | 出处 |
| --- | --- | --- |
| named volume 落在哪个文件系统 | volume 挂载点 `/var/lib/docker/volumes/<name>/_data`，「volumes write **directly to the host filesystem**」（相对容器可写层）。Docker Desktop WSL2 下 `/var/lib/docker` 位于 **`docker-desktop` 发行版的 VHDX**，而该 VHDX 由 `CustomWslDistroDir` 决定 —— **本机是 F:**（实测 `F:\Docker Desktop\DockerDesktopWSL\disk\docker_data.vhdx`，15.85 GB） | [volumes.md](https://github.com/docker/docs/blob/main/content/manuals/engine/storage/volumes.md) + 本机 `settings-store.json` |
| bind mount 跨 Windows 文件系统的性能事实（R6 官方原文） | 「**Performance is much higher when files are bind-mounted from the Linux filesystem**, rather than accessed from the Windows host filesystem. Therefore avoid `docker run -v /mnt/c/users:/users`」；且「Linux containers only receive file change events, "inotify events", **if the original files are stored in the Linux filesystem**」 | [best-practices](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/wsl/best-practices.md) |
| 「避免 `/mnt/c/...`」结论是否仍成立 | **成立，且本机风险面比 `/mnt/c` 更大**：仓库在 `F:\…`，`/mnt/f` 同属 DrvFs Windows 文件系统路径。**但默认路径已经安全**——named volume 不进 DrvFs。风险只在**主动 bind 仓库路径**（DDL/配置/脚本）时出现 | 同上 + 本机实测 |
| 有没有「官方加速」可豁免 | **没有适用于本机的**：Docker Desktop 的 **Synchronized file shares** 平台列只标 **「Mac, Linux, Windows Hyper-V」**，**不含 WSL2**，且需付费订阅 | [settings](https://github.com/docker/docs/blob/main/content/manuals/desktop/settings-and-maintenance/settings.md) |
| 空卷既有数据 | 「If you mount a **non-empty** volume into a directory in the container in which files exist, the pre-existing files are **obscured** by the mount」→ 影响 Doris/Superset 初始化脚本的挂载顺序 | [volumes.md](https://github.com/docker/docs/blob/main/content/manuals/engine/storage/volumes.md) |
| 本机磁盘实测 | WSL 内 `/dev/sdd` 1007 G（用 24 G）；`/mnt/c` 112 G（用 102 G，**92%**）；F: 600 G（剩 77.58 GB）。→ `REQ-NF-CAP-02`（≤20 GB）**必须落在 F:**，不能在 C: | 本机 `df`/`Get-PSDrive` |

**甲-4 结论**：**R6 判定不变，且默认形态已满足**——named volume 天然落在 Linux 文件系统（F: 上的 VHDX 内），无需额外动作。要新增的纪律只有一条：**凡 bind 进容器的路径，一律走 image 内 COPY 或 named volume**，不 bind 仓库路径。

### 甲-5 冷启动 / 镜像拉取体量（**未执行任何 pull**）

| 镜像（本仓锚点） | 体积（一手/标注） | 出处 |
| --- | --- | --- |
| `apache/kafka:4.3.1` | **227.8 MB**（compressed, linux/amd64） | [layers 页](https://hub.docker.com/layers/apache/kafka/4.3.1)（经检索聚合，非直连） |
| `apache/flink:2.2-java21` | **639.32 MB**（compressed, linux/amd64）；Flink 官方库镜像计 640.1 MB | [apache/flink tags](https://hub.docker.com/r/apache/flink/tags)、[library/flink](https://hub.docker.com/r/library/flink) |
| `apache/doris:all-in-one-4.1.3` | 官方 README：base **2.68 GB** / `-full` **3.21 GB**（**未压缩层和**）；`docker image inspect --format '{{.Size}}'` 报 **1.73 / 2.18 GiB** | [README](https://github.com/apache/doris/blob/master/docker/runtime/all-in-one/4.1/README.md) |
| `apache/superset:6.1.0` | **官方未给出**（已核 `docker/README.md`，无体积数字）。二手镜像站报 ~877–878 MB，**不采信** | — |

> **★ 与禁锢件的一处机读交互（官方 README 明确警告，本机恰好命中）**：Doris 官方 README 说体积差异的一个来源是「`docker images` can print a **much larger figure when the containerd image store is enabled**, because it adds the compressed blobs to the unpacked snapshot」。
> 本机 `settings-store.json` 实测 **`"UseContainerdSnapshotter": true`** → **本机 `docker images` 报出的 Doris 体积会显著大于真实占用**，用它做「冷启动体量」记账会失真；应改用 `docker image inspect`。
> 结论：`REQ-NF-CAP-04`（冷启动 ≤30 min）**无官方数字可依**，锚点镜像 compressed 合计约 **3.5 GB 量级**；本机 `UseContainerdSnapshotter=true` 会放大 `docker images` 读数。

## 乙. agent/会话沙箱

### 乙-1 三模式对照（DSH 侧语义，源码/包 README 一手）

| 模式 | 文件影响（官方包 README 原表） | 本仓可执行性 | 备注 |
| --- | --- | --- | --- |
| `read-only`（默认） | 「任何位置都不可写；在 `/dev` 中只有 `/dev/null` 节点可写」 | **不可用**：无法写 `docker-compose.yml`（P0 第 4 项），`docker compose config` 也无法落盘 | 且 pwsh 起在 **ConstrainedLanguage**（`Add-Type`、非核心 .NET 静态、COM、反射失败） |
| `workspace-write` | 「只能写入策略的工作区根目录加 `/tmp`」 | **理论可行**：够写 compose/Makefile/log | ⚠️ 2026-10-18 实测**失败**（见乙-2） |
| `danger-full-access` | 「不作限制；**绝不咨询提供方**」 | **当前会话正在用** | 无文件边界；且**无审批通道**（本会话 approval 被禁用） |

出处：[dsh-bash-sandbox README](file:///C:/Users/Administrator/.dsh-win/versions/0.2.1-rc.1/node_modules/.pnpm/node_modules/@deepseek-ai/dsh-bash-sandbox/README.zh.md)（模式表）；同包：**「如果没有 runner 能实施受限模式，命令会以 `SANDBOX_UNAVAILABLE` 失败，绝不会无隔离地运行」**。
**不确定**：2026-10-18 记录的字符串是 **`EXECUTOR_UNAVAILABLE`**，在**当前 0.2.1-rc.1 checkout 全部包内 grep 不到**（`SANDBOX_UNAVAILABLE` 才是本版 seam 文本）→ 属**跨版本错误码漂移**，不能据旧字符串推断现行机制。

三模式的**关键非差异**（易误判，故单列）：
- **读、网络、进程可见性不受限**：「写入与删除受限；读取、网络与进程可见性不受限」→ `docker` CLI 走 **npipe**（`//./pipe/dockerDesktopLinuxEngine`）**不属文件读写**，故**有理由**在 `workspace-write` 下照常可用；但**本会话无降级模式可测 → 未实测，列入未决**。
- **沙箱约束的是 agent 的宿主侧文件操作，不是 Docker daemon 的写**。容器/卷数据由 Docker Desktop 自己的 VMM 写入 `F:\Docker Desktop\…`，不经过 agent 的令牌。→ 「放宽沙箱才能跑 docker」是**错误因果**。

### 乙-2 ACL 故障的可修复边界（读 `.dsh/skills/diagnose-windows-sandbox-acl/` 后）

SKILL 与包 README 合起来给出**三分**的机制，必须分开：

| 症状（2026-10-18） | 机制 | 是否可修 | 边界 |
| --- | --- | --- | --- |
| `pwsh` 报 sandbox-local **windows-acl 故障** | `init()` 在任何 Win32 失败时抛出、**绝不无隔离 spawn**；runner 打 `windows-acl-run: <detail>` 并 **exit 127**，seam 归类为「**损坏的沙箱**，而非拒绝」。根因常见为授权目录缺有效 **`WRITE_DAC`/`WRITE_OWNER`**：官方 README 明写「所有者隐式获得的只有 `READ_CONTROL` 与 `WRITE_DAC`；标签位于 SACL，因此合并应用还需要 `WRITE_OWNER`（完全控制目录——即正常工作区情形——本就具备）。**DACL 只授予 Modify 的目录现在会大声失败**」 | **可修** | **修法唯一**：随包技能脚本**一条命令**（诊断+修复同一次运行）：`& '<skill>\scripts\diagnose-windows-sandbox-acl.ps1' -Path '<failing-path>' -AllowRoot '<authorized-dir>' -Out '<recovery-dir>'`。它补当前用户完全控制 ACE、按祖先优先移除外来 `S-1-15-2-*` 包 ACE、逐处改后重读验证、每处留 `acl-backup-*.json` + 回滚脚本 |
| `ops_bash` 报缺 **IPC 兼容凭据** | 与上条**不同因**：官方明确「受限孙进程的**管道 stdio 捕获不可用**。libuv 的管道 stdio 用的是 **NAMED pipe**，其 client 端打开所请求的写访问没有任何 restricting SID 被授予（是 Win32 层的默认 SD 模板），因此受限进程内 `spawn(..., { stdio: 'pipe' })` 以 **EPERM** 失败；继承与忽略 stdio 的 spawn 可用，**匿名管道（PowerShell 的管道）可用**」 | **不可修** | 这是**设计边界**，不是 ACL 缺陷；**ACL 修复脚本对它无效**（README：诊断「无法修复 Low 标签的可执行文件影响」等）。绕法只有 `stdio: 'inherit'/'ignore'` 或改用 PowerShell 原生管道 |

**两条硬约束（决定「最小放宽面」的形状）**：
1. **技能本身需要不受限调用者**：「随包发布的诊断技能需要**不受限**的调用者…因此会话必须为那一次调用升权；**没有审批通道时升权 fail-closed，该路径保持未诊断**」（README.zh 「已知限制」）。→ **本会话 approval 被禁用**，故在 `read-only`/`workspace-write` 下**连自查自修都做不到**。
2. **失败即停、不重复**：技能规则明令「**Stop after any failed or refused repair, or failed verification.** … do not repeat it or start another repair, and **never remove a deny ACE by hand**」；也不得提权运行（`runas`/UAC）。

### 乙-3 最小权限但仍能跑 docker/compose 的可行形态

| 形态 | 能否跑 `docker`/`compose` | 能否写 compose/Makefile | 自查自修 ACL | 判定 |
| --- | --- | --- | --- | --- |
| `read-only` | 可能可以（npipe 非文件读写，**未实测**），但 pwsh 落 ConstrainedLanguage | **否** | 否（需升权） | 排除 |
| `workspace-write`（root=仓库） | 有理由可以（同上，**未实测**） | 是（仓库内 + `/tmp`） | **仅当审批通道可用**；本会话禁审批 → 否 | **最小可行形态**（前提：审批通道打开 + 先跑一次 ACL 修复） |
| `workspace-write` + 一次性升权修 ACL，**之后长期保持** | 同上 | 是 | 修完即可 | **推荐** |
| `danger-full-access`（现状） | 是（已证） | 是 | 无需 | 功能最强、权限最大 |

**最小可行形态（给出可执行形态，不执行）**：
1. **先把 workspace-write 修成可用的**：在**审批通道可用**的会话里，对仓库根跑**一次**技能脚本（`-Path` = 触发失败的路径、`-AllowRoot` = 仓库根、`-Out` = 仓库旁用户可写目录），然后**在被限制的会话里重放原操作**验证。若 `nextAction=verify_original_confined_operation` 即修复成立。
2. **再把会话降到 `workspace-write`**，`workspaceRoot` = 仓库根。日常读写 compose/Makefile/日志全在仓库内，`docker`/`compose` 只读 npipe。
3. **本仓特有的例外**：Doris/Superset/Kafka 的**数据**写入由 Docker Desktop 的 VMM 完成，不经 agent 令牌 → **不需要为它们放宽**。

**若必须放宽，最小放宽面与风险**：

| 放宽项 | 最小面 | 风险 | 缓解 |
| --- | --- | --- | --- |
| 会话模式 `workspace-write` → `danger-full-access` | 只在需要 agent 写**仓库外**路径时（如 `/mnt`、`%UserProfile%`、`F:\Docker Desktop\…`） | agent 可改宿主任意文件；与「只读、不改配置」纪律冲突 | **本仓不需要**：`.wslconfig`、Docker settings、卷数据都在 agent 职责外 → **维持 workspace-write 即可** |
| 一次性审批跑 ACL 修复脚本 | **单条路径、单次调用**；`-AllowRoot` **不得**上提到祖先 | 脚本会写 DACL 并（在来源处）移除外来 `S-1-15-2-*` 包 ACE；**会移除**这些包对该目录的访问 | 每处改动有 `acl-backup-*.json` + 独立回滚脚本；**改后重读验证**；只改 `-AllowRoot` 之内 |
| 解除 approval 禁用 | 仅为那一次修复 | 一旦常开，后续所有升权不再经人确认 | 修复后立即复原为禁用 |
| PowerShell 语言模式 | **不需放宽**：`workspace-write` 下探针能完成 → pwsh 保持 **FullLanguage**（除非主机 WDAC/AppLocker 另有规定） | — | 无需动作 |
| 端口/网络 | **不需放宽**：沙箱不做网络限制，compose 的 `127.0.0.1` 才是边界所在 | — | 无需动作 |

## A/B/C 三轴记法 · 汇总

| 轴 | ① Docker Desktop/WSL2 | ② Podman Desktop+podman-compose | ③ devcontainer DooD | ④ WSL2 原生 | 乙 · workspace-write |
| --- | --- | --- | --- | --- | --- |
| **A 友好度** | 高：`compose config` 实测可用；四家官方骨架齐；错误有 exit code | 中：源码支持字段齐全，但 `config` 无校验、driver 默认值有坑 | 中：官方 feature 文档清楚，但 socket/路径语义被改写 | 低：无 compose，Doris 官方无原生路径 | 中：拒绝是**结果事实**（`sandbox:{mode,denied}`），且**不静默降级**；但 pwsh 语言模式随模式变 |
| **B 本机占用** | 已装；引擎数据 **F:**（VHDX 15.85 GB 实测）；C: 仅剩 9.57 GB | **未安装**（+1 VM/栈） | 复用宿主 Docker（+1 容器） | 省 Docker Desktop，但失去逐容器 cgroup | 0（不新增进程） |
| **C 接缝** | 全绿 | 部分（`config`/driver/`memswap` 缺） | 受限（仓库外 bind 不可用） | **五项判据全落空** | 与既有冻结项无冲突 |

## 未决 / 官方未给出 / 不确定

**官方未给出（不得估填）**
1. `docker compose up` **是否**应用 `deploy.resources.limits.memory`——现行官方只有「deploy 为可选、未实现即忽略」+「与 `mem_limit` 必须一致」，无正面表述。
2. `docker compose config` 会不会**报缺** `mem_limit`——官方未给出任何「要求全部服务都写」的校验；**实测不报**。
3. Superset 单机内存与镜像体积。
4. Kafka/Flink/Doris/Superset 单机最低资源（延续 FSR R4）。
5. `.wslconfig` `memory` 的数值上限、超设物理内存的后果；Win10/Win11 默认值差异的官方说明。
6. WSL2 后端下 Docker Desktop 的 CPU/内存滑块（官方明确由 WSL 层配置）。
7. 任一镜像的官方 pull 时长 / 冷启动时长数字。
8. Windows 10 上 Docker Desktop 是否仍启用了 WSL 侧 Hyper-V firewall（该功能官方口径为「Windows 11 22H2 + WSL 2.0.9+ 默认开」；**本机为 Win10 19045，不适用**）。

**本机实测（已证）**
- Docker Client 29.8.0 / Compose v5.5.1 / Context `desktop-linux`；**探测时 daemon 未运行**。
- WSL 3.0.3.0 / 内核 6.18.54.1-microsoft-standard-WSL2 / Windows **10.0.19045.6456**；`wsl -l -v` = Ubuntu + docker-desktop（均 v2、均 Stopped）。
- WSL 内 `MemTotal` **11962 MB / 12 核**（与 FSR「11.68 GB」一致）；`Swap: 2048 MB`；cgroup **纯 v2**、`memory` 控制器已启用、根 `memory.max = max`。
- `docker compose config` 无需 daemon 即可运行并正常化 `mem_limit: 128m → "134217728"`。
- C: 剩 **9.57 GB**、F: 剩 **77.58 GB**；`CustomWslDistroDir = F:\Docker Desktop\DockerDesktopWSL`；`UseContainerdSnapshotter = true`。
- WSL 内 `vm.max_map_count = 1048576`。

**不确定 / 未实测**
1. **`docker` CLI 在 `workspace-write`/`read-only` 下是否真能连 npipe** —— 本会话固定 `danger-full-access` 且**审批被禁用**，无法降级实测。有理由（「网络与进程可见性不受限」、npipe 非文件读写）但**未证**。→ 建议父 agent 在有审批的会话里做一次最小验证：`workspace-write` 下跑 `docker compose version` 与 `docker version`。
2. **ACL 故障的确切根因未定位**：本会话无法读 F: 仓库根的 DACL（未跑技能脚本、且审批禁用）。只能确认「缺 `WRITE_DAC`/`WRITE_OWNER` 即大声失败」是**官方记载的**触发条件，**不能**断言本机就是它。
3. `EXECUTOR_UNAVAILABLE` 与 `SANDBOX_UNAVAILABLE` 的关系：旧字符串在当前 checkout 检不到。
4. 镜像体积数字中 Kafka/Flink 来自 Docker Hub 页面聚合、Superset 来自二手站 → **仅 Doris 是官方仓库自述**；三者均**未在本机 pull 验证**（本机 `docker manifest inspect` 因 daemon 未运行 + registry 不可达而失败，`web_fetch` 对 hub.docker.com 亦失败）。
5. Doris 官方 compose 只用 `BE_MEM_LIMIT`（BE 进程内 `mem_limit` 百分比）**而非** compose `mem_limit`；`-full` 与 base 的取舍、以及 all-in-one 单容器能否满足 `REQ-F-ORCH-05`（`down`→`up` 数据仍在：官方称「storage persists if you mount the two dirs」，但官方多节点 compose 明写「`down` → nothing persists」）——**需按我们的目标裁剪后实测**。
6. 本机 `vm.max_map_count = 1048576` 低于 Doris 官方「heavy workloads」建议的 2000000，且官方明确「it is **not a namespaced sysctl**, so the container cannot set it」；本仓规模是否需要上调、以及上调要落在 `docker-desktop` 发行版（非 Ubuntu）——**未验证**。
