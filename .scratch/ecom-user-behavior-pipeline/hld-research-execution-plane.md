> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（执行面落点的网络事实）｜ 上游：hld-research-sandbox.md 甲-3、hld-research-toolchain.md §8 ｜ 状态：一手核查，daemon 未运行故未实测，已给出 P0 实测命令

## 0. 判定：不是二选一，而是按「是否触碰已发布端口」切面

| 面 | 落点 | 判据 |
| --- | --- | --- |
| **A · 触碰已发布端口**（`make verify` 的 HTTP/TCP 探测、Kafka 9092、Flink UI 8081、Doris 9030、Superset 8088） | **Windows 宿主（唯一可行）** | WSL2(NAT) 连不上任何绑到 Windows `127.0.0.1` 的监听（§2 一手实测） |
| **B · 只做编排**（`make up/down/logs`、`docker compose`、Python 生成器） | **WSL2 + 开 Ubuntu 的 WSL integration**（首选）；宿主次之 | 只经 Docker API 不经端口；WSL 侧换来 Linux 路径/`make`/shell 语义（toolchain §8.1） |
| **C · 数据面**（`REQ-F-RT-06` 的 Stream Load 两跳） | **compose 网内的 `client` 服务** | 沙箱甲-3：Doris 官方明文「stream load from the host cannot follow FE's redirect to a BE」 |

**故本仓默认单点执行面是「Windows 宿主」**：`make verify` 放宿主，A 面天然成立、C 面仍归 `client` 服务。把 `make` 整体放 WSL2 会在 A 面**静默失败**（连接被**拒绝**而非超时，§2.3）——这是本文件要拦住的错误落点。

## 1. `localhostForwarding` 官方语义：**单向，Windows ← WSL**

| 问题 | 结论 | 出处 |
| --- | --- | --- |
| 原文 | 「Boolean specifying if **ports bound to wildcard or localhost in the WSL 2 VM** should be **connectable from the host** via `localhost:port`」；默认 `true` | [wsl-config.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/wsl-config.md)（同键首见 [release-notes Build 18945](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/release-notes.md)） |
| 方向 | **单向**：主语=WSL 2 VM 内的端口，宾语=from the host。官方**从未**声明可反向把宿主端口送进 WSL | 同上 |
| 本机 | **未设** → 走默认 `true`（WSL 3.0.3.0） | 本机 `.wslconfig` |
| 何时失效 | 「Setting is **ignored** when `networkingMode=mirrored`」——Win10 无法用 mirrored，故该键**生效** | 同上 |

**NAT 下「WSL → 宿主服务」的官方指定地址**（不是 `localhost`）：官方明文「A program running inside a Linux distribution via WSL2 wants to know the Windows host's IP address… **you need to use the IP address of your host machine**」，并注「**typically required** when WSL2 is running with the default NAT network mode」，给的两条命令即 `ip route show | grep -i default | awk '{ print $3}'` 与 `cat /etc/resolv.conf`（[networking.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/networking.md)）。
本机实测：`default via 172.17.32.1`、`nameserver 172.17.32.1`、`hostname -I`=`172.17.35.3` → **宿主地址 = `172.17.32.1`**。
同页 NOTE 反向印证：只有 mirrored 才「Windows host and WSL2 VM can **connect to each other** using `localhost`」；本机 **Win10 19045 → mirrored 官方标 Win11 22H2+，不可用** → 无任何官方途径让 WSL 内的 `localhost` 指向 Windows。

## 2. 一手实测：WSL(NAT) 能否连 Windows 侧监听（**决定性**）

方法：不依赖 Docker（daemon 未运行），改用**当前在跑的 Windows 监听**做等价判定——Docker Desktop 发布的端口同样是「Windows 侧的一个监听」。仅只读探测。

| # | Windows 侧监听（`Get-NetTCPConnection` / DSH GUI） | 绑定 | WSL→`127.0.0.1:p` | WSL→`172.17.32.1:p` | Windows→ |
| --- | --- | --- | --- | --- | --- |
| 2.1 | 5040 / 2179 / 135 | `0.0.0.0` 通配 | **FAIL**（refused） | **OK** | — |
| 2.2 | 10724（DSH Web GUI） | `127.0.0.1` **环回专用** | **FAIL** | **FAIL** | **OK**（`Test-NetConnection`=`True`） |

**2.3 读法（全文最重要）**
- 2.1 ⇒ **WSL 内的 `127.0.0.1` 是 WSL 自己的环回，永远不是 Windows 的**；WSL→Windows 唯一通路是**网关 `172.17.32.1`**（官方给的正是这个地址）。
- 2.2 ⇒ **只绑 Windows `127.0.0.1` 的监听，连网关地址也连不上**——socket 根本没在那个接口 `listen`。
- **套到本仓**：`REQ-F-ORCH-06` 要求每个 `ports` 写 `127.0.0.1:H:C`；Docker 官方说明它**绑在 Windows 宿主**：「Docker Desktop's backend process **listens on the specified host port**」（[networking/_index.md](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/networking/_index.md#how-exposed-ports-work)），且「If you include the localhost IP address (`127.0.0.1`…) with the publish flag, **only the Docker host can access** the published container port」（[port-publishing.md](https://github.com/docker/docs/blob/main/content/manuals/engine/network/port-publishing.md)）⇒ 该类监听 = 2.2 → **WSL 内 `127.0.0.1:9092` 与 `172.17.32.1:9092` 均不可达；宿主 `127.0.0.1:9092` 可达**。
- 失败形态是 **refused（立即）不是 timeout** → 在 WSL 里跑 `make verify` 得到「拒绝连接」，**易被误诊为容器没起来**。

**反向确认（绑点确在宿主）**：官方「All inbound connections pass through the Docker Desktop backend process (`com.docker.backend.exe` on Windows), which handles port forwarding into the VM」（[networking-how-tos.md](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/networking/networking-how-tos.md)）。与前序沙箱甲-3 一致。

### 2.4 WSL integration 会不会改变这一点？**官方无明文，机制上不改变**

| 问题 | 结论 | 证据 |
| --- | --- | --- |
| integration 开的是什么 | 官方只说「enabling WSL integration for a distribution gives you **direct access to `docker` commands** from that distribution's terminal」——**只谈 CLI，不谈端口** | [wsl/_index.md](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/wsl/_index.md#enable-docker-in-a-wsl-2-distribution) |
| 官方是否说过「发布端口在 distro 内可经 `localhost` 访问」 | **官方未给出**（正/反向及 integration 对端口的影响，docker/docs 均无明文）→ 入文末未决 | 同上 |
| 机制推断 | integration 注入 **CLI + socket**，不是 Windows 网络栈；**不改变 `com.docker.backend.exe` 仍在 Windows 环回上 listen** | 官方架构表：Windows(WSL2) 的 networking 由 `com.docker.backend.exe` 处理（[networking/_index.md](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/networking/_index.md#backend-components-and-responsibilities)） |
| 结论 | **A 面不受 integration 影响**：开不开，WSL 内都连不上 `127.0.0.1:H:C` | — |

## 3. WSL 内调 Windows `docker.exe`：可行，且本机**已经**在这么解

| 问题 | 结论 | 证据 |
| --- | --- | --- |
| 能否从 WSL 执行 Windows 二进制 | **能**。官方：「WSL can run Windows tools directly from the WSL command line using `[tool-name].exe`」；「handled similarly to native Linux executables」；「Parameters are passed to the Windows binary **unmodified**」 | [filesystems.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/filesystems.md) |
| 本机 `command -v docker` 解析到什么 | **不是原生 docker**，而是 Docker 注入的 **shim** `/mnt/c/Program Files/Docker/Docker/resources/bin/docker`（1359 B，`#!/usr/bin/env sh`）。**该 shim 拒绝执行**（`/usr/bin/docker` 不存在）→ 打印「could not be found in this WSL 2 distro. We recommend to activate the WSL integration…」并 `exit 1` | 本机实测（读 shim 源码 + 执行） |
| 真实 `.exe` 能用吗 | **能**。`"/mnt/c/.../docker.exe" version` → `Client 29.8.0 / API 1.56 / windows/amd64 / Context desktop-linux`，随即报 daemon 未运行 ⇒ **已成功经 npipe 把 API 请求送达**（失败只因 daemon 停着，不是通道不通） | 本机实测 |
| npipe 在 interop 下可见吗 | **Windows 二进制自持 npipe，WSL 侧无需看见 `/npipe`**。错误串回显 `npipe:////./pipe/dockerDesktopLinuxEngine` + `The system cannot find the file specified`（Win32 错误码，由 Windows 进程产生） | 同上 |
| WSL 侧有 UNIX socket 吗 | **没有**：`/var/run/docker.sock` 不存在；`/mnt/wsl/docker-desktop/` 不存在、`/mnt/wsl/` 下只有 `resolv.conf` = integration **未开**的直接证据 | 本机实测 |
| interop 开关 | `/proc/sys/fs/binfmt_misc/WSLInterop` 存在；`/etc/wsl.conf` 无 `[interop] enabled=false`（仅 `[boot] systemd=true`、`[user] default=gyx`）→ **interop 开着** | 本机实测 |

### 3.1 两条路对照

| 维度 | 路① WSL 内跑 Windows `docker.exe` | 路② 开启 Ubuntu 的 WSL integration（toolchain T6） |
| --- | --- | --- |
| CLI 载体 | `docker.exe`（Windows 进程） | `/usr/bin/docker`（Linux CLI 注入 distro） |
| API 通道 | Windows 进程走 npipe（**本机已验证可达**） | Docker Desktop 注入的 UNIX socket |
| daemon/context | `desktop-linux`；**context 由 Windows 侧配置决定** | 同左（社区证据：Docker Desktop 会把 context 注入 WSL2，[remus-software.org](https://remus-software.org/articles/kb_two_docker)） |
| **路径语义** | **按 Windows 语义**（§3.2） | **按 Linux 语义**：`/mnt/f/...` 合法，相对 `build context` 相对 compose 文件解析 |
| 相对路径 | **可用，但仅在 CWD 被正确翻译时**。实测 `cd /mnt/f/.../eco-a-d-platform && cmd.exe /c cd` → **`F:\Anaconda practice project\eco-a-d-platform`**，`wslpath -w` 同值 | 无此约束 |
| 仓库在 Linux fs 时 | **坏**：`cd /tmp && cmd.exe /c cd` → 「UNC 路径不受支持，默认值设为 Windows 目录」→ 落到 `C:\Windows`。**CWD 一落到 `\\wsl.localhost\...` 就退化为 `C:\Windows`** | 无此约束 |
| `-v /mnt/f/...` | **会被误判**；官方合法形只有 `C:\Users\user\work:/work` 与 `/c/Users/user/work:/work`，**无 `/mnt/f` 这一形** | `/mnt/f/...` 正常 |
| 结论 | 仅在「仓库在 `F:\` + 从对应目录发起 + 只用相对路径、绝不写 `/mnt/f` 形绝对路径」时可用；脆 | **推荐**：消掉全部路径分叉 |

**3.2 对本仓「仓库在 `F:\`」的确切后果**：走 Windows `docker.exe` 时 `docker compose` 由 **Windows 进程**执行 → `build.context`、`dockerfile`、bind `source`、`.env`、`COMPOSE_PROJECT_NAME` 的目录基名**全按 Windows 语义**解析。官方 Compose 规格：「a relative path… is **interpreted as relative to the project directory**」、`dockerfile`「**resolved from the build context**」；绝对路径会触发「Compose warns you about the absolute path… as those prevent the Compose file from being portable」（[build.md](https://github.com/docker/docs/blob/main/content/reference/compose-file/build.md)）。因 CWD 被 interop 翻成 `F:\…\eco-a-d-platform`（§3.1 实测），compose 里 `./ddl`、`./config` 类**相对路径照旧可用**；但**任何 `/mnt/f/...` 或含空格未转义的 `F:\...` 都会炸**。另：本机 `/mnt/f` 挂载为 `type 9p`（`mount` 实测 `aname=drvfs;path=F:\`）→ 与 §4 性能结论直接相关。

## 4. WSL 内 Python(3.12, uv) + 仓库在 `/mnt/f`(DrvFs)：官方怎么说

| 问题 | 结论 | 出处 |
| --- | --- | --- |
| 跨 OS 文件系统总原则 | 「We **recommend against** working across operating systems with your files… For the fastest performance speed, **store your files in the WSL file system** if you are working in a Linux command line」；反例直指 `/mnt/c/Users/<user>/Project` | [filesystems.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/filesystems.md) |
| WSL2 是否明确劣于 WSL1 | **是**：特性表「Performance across OS file systems」WSL1 ✅ / **WSL2 ❌**；并列为「exceptions for using WSL 1」：「you will achieve faster performance across the OS file systems by using WSL 1」 | [compare-versions.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/compare-versions.md) |
| inotify（WSL 侧官方原文） | **官方自相矛盾**：Build 14926「**Known issue: Modifying files from Windows applications does not generate any events**」→ Build 14942「**inotify support for notifications generated from Windows applications on DrvFs is now in**」→ 同页 Build 14936 又改口「**Correction: Inotify support for changes originating from Windows applications not available at this time.**」→ 取**保守基线=不产生事件** | [release-notes.md](https://github.com/MicrosoftDocs/wsl/blob/main/WSL/release-notes.md) |
| Docker 侧同向原文（沙箱甲-4 已确认） | 「Linux containers only receive inotify events **if the original files are stored in the Linux filesystem**」；「Performance is **much higher** when files are bind-mounted from the Linux filesystem… **avoid** `docker run -v /mnt/c/users:/users`」 | [best-practices.md](https://github.com/docker/docs/blob/main/content/manuals/desktop/features/wsl/best-practices.md) |
| uv 官方是否**点名**劝退 DrvFs | **没有点名**。给的是等价硬规则：「For best performance, **locate the cache directory on the same file system as the Python environment**… Otherwise, uv **cannot link files from the cache into the environment and must fall back to slower copy operations**」；Linux 默认 cache=`$XDG_CACHE_HOME/uv` 或 `$HOME/.cache/uv`（**在 WSL fs 内**） | [uv cache.md](https://github.com/astral-sh/uv/blob/main/docs/concepts/cache.md) |
| ⇒ 可执行结论 | `.venv` 落仓库(`/mnt/f` DrvFs) 而 cache 默认在 `~/.cache/uv`(WSL fs) → **不同 fs ⇒ 必然退化为 copy**。要么 `UV_CACHE_DIR=/mnt/f/...`（同 fs 但 fs 本身慢）、要么 `.venv` 移出仓库（`UV_PROJECT_ENVIRONMENT`）。**官方未给二者取舍的定量依据**，只有「不同 fs ⇒ copy」这条硬规则 | 同上 |

## 5. 记法与接缝对照

| 轴 | 结论 |
| --- | --- |
| **A 友好度** | §1/§3/§4 三组结论**均可判定且可执行**（给到地址与命令）；仅 §2.4「integration 是否影响端口可达」**官方无明文** → 已列未决并给 P0 命令。Docker 官方对「WSL↔宿主 端口方向」的覆盖**弱于** Microsoft 侧 |
| **B 本机占用** | **两条路都≈0 额外常驻**：路① 只多一个按需启动的 `docker.exe` 子进程；路② 只把 CLI/socket 注入既有 distro，**不新增常驻进程**（官方：Docker Desktop runs inside its **own `docker-desktop` WSL distribution**——该 distro 本就存在，本机 `wsl -l -v` 可见、当前 Stopped）。差别只在「daemon context 来源」，不在资源 |
| **C · `REQ-F-ORCH-01`** | 不受影响：宿主(PowerShell) 或 WSL2(集成后) 都能达成 `docker compose up`；该判据是「一条命令起链路」，**不含端口探测** |
| **C · `REQ-F-ORCH-06`** | **正面相关**：`127.0.0.1:H:C` 仍是**唯一**同时满足「宿主可达 + 局域网不可达」的写法。若为让 WSL 能连而改 `0.0.0.0`，则**直接违反**该条（官方：不写 host IP 时「binds to all interfaces (0.0.0.0), **bypassing host firewall rules**」）。**不得为 WSL 便利而动端口绑定** |
| **C · 冻结端口清单** | 8081/9092/9030/8030/8088 在宿主侧可用；在 WSL2 侧**全部不可用**（§2.2 机制） |
| **C · `REQ-F-RT-06`** | **不受本次落点影响**：connector 在 **TaskManager 容器内**写 Doris，第一跳(FE)与第二跳(BE 重定向)**都在 compose 网内** |
| **C · 硬约束「Stream Load 须从网内发起」** | 强化本次结论：**必须保留 `client` 服务**（沙箱甲-3 的官方做法）；宿主跑 Stream Load 仍会因 follow 不到 BE 而失败 → **`make verify` 的落点规则是「HTTP/TCP 探测→宿主；Stream Load→compose 网内」两段，不可统一** |

## 6. P0 实测命令（daemon 起后逐条跑；**只读**，不 `wsl --shutdown`、不改 `.wslconfig`）

```powershell
# P0-1 发布端口的监听点与绑定地址（预期 127.0.0.1）
Get-NetTCPConnection -State Listen | Where-Object { $_.LocalPort -in 8081,9092,9030,8030,8088 } |
  Select-Object LocalAddress,LocalPort,OwningProcess
Get-Process -Id (Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 9092).OwningProcess |
  Select-Object ProcessName   # 官方说端口转发由 backend 承担，预期 com.docker.backend

# P0-2 宿主可达性基线（预期全 True）
foreach ($p in 8081,9092,9030,8030,8088) {
  [pscustomobject]@{Port=$p; HostOK=(Test-NetConnection 127.0.0.1 -Port $p).TcpTestSucceeded} }

# P0-3 ★决定性★ WSL→Windows 已发布端口（预期 127.0.0.1=False、网关=False）
$gw = (wsl -e bash -lc "ip route show default | awk '{print `$3}'").Trim(); "gateway = $gw"
foreach ($p in 8081,9092,9030,8030,8088) { foreach ($a in @('127.0.0.1',$gw)) {
  "WSL -> $a`:$p = " + (wsl -e bash -lc "timeout 3 bash -c 'exec 3<>/dev/tcp/$a/$p && echo OK' 2>/dev/null || echo FAIL") } }

# P0-4 若 P0-3 意外有 OK，先确认不是 mirrored 造成的
wsl -e bash -lc 'grep -i networkingmode /mnt/c/Users/$USER/.wslconfig || echo "(no networkingMode => NAT)"'

# P0-5 路①/路② CLI 通道对照（只读）
wsl -e bash -lc 'command -v docker; ls -l /var/run/docker.sock 2>&1'   # integration 未开 => shim + 无 socket
& "$env:ProgramFiles\Docker\Docker\resources\bin\docker.exe" context ls
wsl -e bash -lc '"/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe" version'
```

**P0-3 是本文件成败判据**：若 WSL 两列全 `FAIL`（静态证据要求的预期）→ A 面**必须落宿主**，按 §0 执行；若 `172.17.32.1` 列出现 `OK`，则「发布端口在 WSL 内可经网关访问」成立，需追加修订（且**不违反** `REQ-F-ORCH-06`，因绑点未变）。

## 未决 / 官方未给出 / 不确定

1. **官方未给出（最高优先）**：docker/docs **从未明文**说明「发布到 Windows `127.0.0.1` 的端口，在开启了 WSL integration 的 distro 内是否可达」。本文件的「不可达」是 **§2.1/2.2 一手实测机制 + §1 官方单向语义**的**推论**，非官方直述 → **须由 P0-3 收口**。
2. **官方未给出**：WSL integration 的具体注入形态（`/usr/bin/docker` + 哪个 socket 路径）。官方只写「direct access to `docker` commands」；本机因**未开启**无法观测注入物；`/mnt/wsl/docker-desktop/` 不存在与「未开启」自洽。**需开启后重测（属 T6，未授权，本文件未执行）**。
3. **官方自相矛盾（§4 已标出）**：WSL release-notes 对「Windows 应用改动 DrvFs 文件是否产生 inotify 事件」有三处不同口径（Build 14926 / 14942 / 14936 更正），**未被 Microsoft 公开澄清**。
4. **官方未给出**：DrvFs 的 I/O 性能**定量**数字（只有 WSL2 vs WSL1 的定性 ✅/❌）。官方「20x / 2–5x」是 **WSL2 vs WSL1 的 Linux fs** 数字，**不能**套用到 DrvFs。
5. **不确定**：网关 `172.17.32.1` **随 WSL 重启变化**（官方：「a unique IP address that will **change on restart**」）→ 写死进配置/脚本不可复现，只能运行时求值。
6. **社区证据（非官方，仅佐证方向）**：容器→WSL2 宿主的 `host.docker.internal` 在 Docker Desktop 下解析为 `192.168.65.254`（VM 网关），**不转发到 WSL2 宿主**（[remus-software.org](https://remus-software.org/articles/kb_two_docker)，社区）；「NAT 下 `127.0.0.1` 永不指向宿主」亦有多条同向社区描述（如 [clashverge-hl.com](https://clashverge-hl.com/en/guides/wsl-docker-proxy)）。**本文件判定不依赖这些社区源**，仅以 §2 本机实测 + §1 官方原文为准。
7. **本机唯一未覆盖的探测**：daemon 未运行 → 无法用 `docker port` / `docker inspect` 直接读 `HostIp`，故 §2 用**等价监听**替代。这是「未实测」边界的准确位置。
