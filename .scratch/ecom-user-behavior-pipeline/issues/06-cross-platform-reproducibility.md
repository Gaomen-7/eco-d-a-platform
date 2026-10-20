# 06 · 跨平台可复现的边界：Windows/WSL2-only、三平台、还是一台“参考机器”

Type: grilling
Status: open
Blocked by: 04
Created: 2026-10-20
Ticket: 06
Source: map.md → Not yet specified F5 / F7 / F6 / SRS §2.4 C1 / FSR §1

## Question

`FSR §1` 的成功判据第一条是「**干净机器**上克隆仓库，一条命令拉起整条链路」，但全仓的实测结论全部绑定在一台具体机器上：Windows 10 19045 只能 NAT、Docker Desktop 29.8.0、WSL2 内存 11.68 GB、`/mnt/f` 是 DrvFs、引擎数据盘在 F:。`REQ-NF-CAP-03/04` 的冷启动两档同样假定「预拉镜像」这一台机器的状态。

几个绕不开的岔口：

1. **可复现的主体**：是「任何装有 Docker 的 Linux 机器」，还是「本机 + 同款 Win10/WSL2」，还是「只要有一次成功的开箱记录即可」？三者对 compose 写法的影响不同——`mem_limit` 在 Docker Desktop 与原生 Docker 上行为一致、但 `-p 127.0.0.1` 在远端 Docker 上语义会变；`/mnt/f` 路径只在 WSL 里成立。
2. **执行面的可移性**：HLD §4.7 定死「单一执行面＝WSL2」。若评审者用的是 macOS，`make` 与生成器必须能在原生 shell 上跑；`client` 服务的 Stream Load 两跳是 Docker 层的事实，跨平台成立——但**生成器**用 `confluent-kafka` 预编译 wheel，三平台都有；`uv` 也是。哪些还能不能算「零安装」？
3. **F5 / F7 两条迷雾**（见 map.md）：`client` 何时可从必需降为可选 profile；「一条命令」的操作定义（是否允许 `down` 后再 `up`、是否允许已有数据卷）。
4. **冷启动记账的口径**：`REQ-NF-CAP-03`（预拉 ≤15 min）与 `REQ-NF-CAP-04`（含拉镜像 ≤30 min）在这台机器上测过吗？如果没测过，是「设计值」还是「已证」——HLD §0.3 要求逐个数字标性质。

## 约束

- HITL。**不要**顺手把 `.wslconfig` 或 Docker 设置改了——那是本机配置改动，须先获批（USER.md 的硬规则）。
- 不重开平台选型（Docker Desktop/WSL2 维持，Podman 只是书面备选，HLD §4.1）。
- 产出应当是一张**跨平台成立性表**：能力 × 平台 × 结论（成立 / 降级 / 只作文档声明），以及 `client` 与「一条命令」的口径定义。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加，
     并把 F5 / F7 按产出毕业成票或被这条吞掉 -->