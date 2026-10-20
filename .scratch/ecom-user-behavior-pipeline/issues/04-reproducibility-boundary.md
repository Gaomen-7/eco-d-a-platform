# 04 · 可复现的边界：跨平台成立性 + 「一条命令」的操作定义 + 本机环境交接未同步

Type: grilling
Status: resolved
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4（已落子）**。
Created: 2026-10-20（原 06；并入 D4 明确留给本票执行的「三份 P0 交接与 HLD/gdd 未合流」）
Ticket: 04
Source: map.md · FSR §1 / SRS §2.4 C1 / REQ-NF-CAP-03/04 / REQ-F-ORCH-01 · HLD §4.7 / §7.3

## Question

三条判据都在说「可复现」，但**没有一条被操作化**：

| 判据 | 原文 | 未定的部分 |
| --- | --- | --- |
| `REQ-F-ORCH-01` | 干净机器一条命令拉起整条链路 | 「干净」= 有数据卷？无数据卷？允许 `down` 后再 `up`？ |
| `REQ-NF-CAP-03` | 预拉镜像前提下 ≤15 分钟到看板出数 | 在本机测过吗？没测过就是「设计值」 |
| `REQ-NF-CAP-04` | 冷启动（含拉镜像）≤30 分钟 | 拉镜像受镜像站限速，怎么取数才可复现 |

要裁决的：

1. **可复现的主体**：任何装有 Docker 的 Linux 机器 / 本机 + 同款 Win10+WSL2 / 只要有一次成功的开箱记录即可？三者在 compose 写法上后果不同（`-p 127.0.0.1` 语义、`/mnt/f` 路径）。
2. **执行面的可移性**：HLD §4.7 定死「单一执行面＝WSL2」。macOS 评审者跑 `make` + 生成器时，哪些还能算「零安装」（`uv` / `confluent-kafka` 预编译 wheel 三平台都有）。
3. **`client` 的必需性**：它只服务 Stream Load 两跳与 Doris 断言；若换执行面（Podman 备选），它能否降为可选 profile。
4. **`client` 的拓扑**：`depends_on: service_healthy` × 「一条命令」× 「`down/up` 不丢数据」可能成环（`client` 等全链路健康，全链路里有 Doris，Doris 初始化又走 `client`）。P0 只证了 `client` 单独跑通，**没证这条链**。
5. **本机环境的合流**（原属 D4 的设计面，但合流时必须核实事实，故在此执行）：
   - `.scratch/handoff/` 三份 P0 交接（222535 / 234721 / 124947）逐条并入 HLD §4 / §7.3 后移到 `handoff/archive/`；
   - **待核实的一处事实**：HLD §7.3 的 U-ν 写「`docker images` = 13.5 GB」，而 HLD §4.2 用的是 `docker image inspect`（`docker images` 在 `UseContainerdSnapshotter=true` 下读数虚高，gdd §1.7 已记）。把 U-ν 那行改成 `image inspect` 口径，或显式标注它不可作体积记账；
   - `HLD §4.8` 尾部与 `§4.6` 重复粘贴（两段「本会话 `danger-full-access`…」），合并为一段；
   - 备份保留期与产物位置（`.scratch/backups/`、`docker/doris/apache-doris-4.1.5-bin-x64.tar.gz` 4.32 GB 是否留、留多久）。

## 约束

- HITL。**不要**顺手改 `.wslconfig` 或 Docker 设置——本机配置改动须先获批、先备份、说明回滚（USER.md 硬规则）。
- 不重开平台选型（Docker Desktop/WSL2 维持，Podman 只是书面备选，HLD §4.1）。
- `handoff/` 的归档是**移动**不是删除：`.gitignore` 目前把 `handoff/` 与 `backups/` 挡在库外，移动前先问「要不要入库」。
- 产物：一张**跨平台成立性表**（能力 × 平台 × 成立 / 降级 / 只作文档声明）+ 三份交接的合流清单（逐条：并到哪一节 / 已废弃 / 仍待核实）。

## Answer

**产物**：[`reproducibility-boundary.md`](../reproducibility-boundary.md)（跨平台成立性表 + 四档契约 + `client` 拓扑裁决 + 三份交接的合流清单 + 待 [09] 落笔的逐字新句）。
**已就地改**：`HLD.md` §4 引言指针 / §4.2（体积表补 Doris 与 mysql + 两口径结案）/ §4.4（`client` 拓扑 + 平台适用范围）/ §4.6 与 §4.8 重复段合并 / §4.7（执行面口径）/ §7.1 新增 D-H、D-I 两行偏离 / §7.3 U-ν 口径 / §8-R2（适用范围 + 实现现状登记）。
**已执行**：删两份 4.03 GiB tarball（F: 41.79 → **49.84 GB**）；三份交接移到 `.scratch/handoff/archive/`（**跟踪状态未动**）。

### A. 逐问裁决（2026-10-20 用户全批）

| 问 | 裁决 |
| --- | --- |
| **1 可复现的主体** | **主体＝制品**（compose + 锚点 + 相对路径纪律 + 内存/端口契约 + 判据脚本），**不是平台**。依据：JDK17/Maven 的装法是 Linux/WSL 专属；`client` 两跳约束是 Docker Desktop 专属 ⇒ 同一 compose 在两平台**语义不同**。平台专属事实（工具链解包、`.wslconfig`、WSL integration、镜像站可达性）一律标「**本机环境事实，不属可复现承诺**」 |
| **2 「干净机器」** | 拆四档，**互不冒充**：**L0 制品态**（镜像在、卷可有）/ **L1 零状态冷启**（无镜像无卷）/ **L2 带卷重启**（`down` 不删卷）/ **L3 跨机搬运**（`save`/`load`）。**`REQ-F-ORCH-01` 的「一条命令」只认 L0**；L1 **豁免** 30 min（构建不是拉镜像）；L2 是「能重起来」的判据（Bug H 只在 L2 暴露）；L3 是「另一台机器」的唯一可操作形式 |
| **3 15/30 分钟** | **签字改写**：`CAP-03` 拆两截（「`make init` 到全 healthy ≤15 min」现在可判；「到看板出数」推 P1）；`CAP-04` 改 `docker load` 口径（构建排除），另在 §7.1 登记原口径不可达（**D-H**） |
| **4 `client`** | ① 原生 Linux 上**可选**（官方「Linux hosts can reach the nodes directly」）；② `depends_on` **保持只等 `kafka`**（HLD 旧文「全链路」是错的，照它实现**成环**）；③ **按需 `run --rm`**，但计入「全部声明上限」口径（10.5 GiB）；④ **改 HLD 的字**，不迁就 compose。不成环判据：`client` 不得出现在任何 `depends_on` 的被依赖侧 |
| **5 交接合流** | 三份移入 `.scratch/handoff/archive/`（移动不删）+ `INDEX.md` + **24 条**逐项合流清单（**并 13 / 废 7 / 核 4**）。`.gitignore` 例外项（`….222535` 仍被跟踪）**未动**，`git rm --cached` 留用户发令 |
| **6 体积/产物** | U-ν 改 `image inspect` = **12.60 GiB**（票面的「虚高」猜测**被证伪**：与 `docker images` 13.5 GB 一致，差的是 GiB/GB 单位）；§4.2 补 Doris 与 mysql 两行 + 3.8× 的读法；`HLD.md:294` 的 **F-1 订正留给 [09]**（已在 §4.2 加指针，引用一律 10.5/1.18）；两份 tarball **已删**；buildx 缓存**不动**；别的项目容器**不动**；`.scratch/backups/Dockerfile.head-20261019` **保留** |
| **7 「一条命令」具名** | **`make init`**，冻结四段：`up -d` → `--profile doris up -d --wait` → `client` 跑 `doris-init.sh` → Superset `db upgrade`/`create-admin`/`init`。`init` 是冻结目标名 ⇒ **不触发冻结项变更** |
| **8 工具链边界（迷雾 F8）** | 一条命令的边界＝**运行面**；JDK/Maven **只服务于构建 Flink 作业**，`up/init/verify/lint/typecheck` **都不需要**它们 ⇒ F8 收敛为一句可判的话，bootstrap 脚本留 P1 |
| **9 doc≠reality** | **只登记不修**（实现不在本票目的地）：① Doris 断言在宿主跑；② `minio`/`spark` profile 不存在；③ `gdd.md:512` 自相矛盾。各带归属 |
| **10 13.5 GB** | 改 `image inspect` **12.60 GiB**，并在 §4.2 写「两口径一致 ⇒ 旧读数不虚高，虚的是单位混淆」 |
| **11 删文件** | 删两份 tarball（**已执行**，+8.05 GiB）；buildx / 他项目容器 / backups **不动** |
| **12 实测范围** | **纯设计，不做破坏性实测**；L0/L1/L3 标「待测」，L2 沿用 P0 已实测 |
| **13 落笔范围** | **混合**：与本票裁决绑定的 HLD 段落**已就地改**；`HLD.md:294` 的 F-1 与 SRS 三处逐字新句**留 [09]** |

### B. 待 [09] 落笔（逐字新句已备，见产物 §8）

1. `REQ-F-ORCH-01` → `make init`（**已签字**，§8.1）
2. `REQ-NF-CAP-03` → 拆两截（**已签字**，§8.2）
3. `REQ-NF-CAP-04` → `docker load` 口径（**已签字**，§8.3）
4. `HLD.md:294` 的 F-1：10.25/1.43 → **10.5/1.18**
5. `gdd.md:512`「Doris 无官方 compose」→ 已被 `gdd.md:197` 推翻，勿再引用

### C. 否决/降级的设想（留痕，防复发）

- ❌ 「主体＝任何装有 Docker 的 Linux 机器」——工具链装法与 `client` 两跳两处直接否掉。
- ❌ 「`client` 用 `depends_on` 全链路 `service_healthy`」（HLD 旧文）——**成环**。
- ❌ 「U-ν 的 13.5 GB 是 snapshotter 虚高」——**实测证伪**，真因是 GiB/GB 单位混淆。
- ❌ 「新增 `up-all` 目标做一条命令」——新增冻结目标名要走 FC 变更，`init` 已够。
- ❌ 「把工具链自举写进一条命令」——`.deb` 解包是 Linux/WSL 专属，跨平台表会多一列成本。
- ❌ 「本轮做 L0 零状态实测」——需 `down -v`，会销毁 P0 已验证的卷态成果。

> 解析步骤已完成：Skill `grilling`（13 问，两轮前沿）+ `domain-modeling`（术语「可复现/确定性」分离、GLOSSARY 补条）→ `Status: resolved` → 已追加 `map.md`。
