# 04 · 可复现的边界：跨平台成立性 + 「一条命令」的操作定义 + 本机环境交接未同步

Type: grilling
Status: open
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

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加 -->
