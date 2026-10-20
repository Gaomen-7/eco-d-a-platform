# eco-a-d-platform

生态相关平台工程。本文件是给编码 agent 的仓库级说明。

## Agent skills

### Issue tracker

本地 markdown：issue 与 spec 存放在 `.scratch/<feature-slug>/`，一张票一个文件。See `docs/agents/issue-tracker.md`.

### Triage labels

保留默认五角色：`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`，本地后端以票文件顶部的 `Status:` 行落地。See `docs/agents/triage-labels.md`.

### Domain docs

single-context：全仓共用根目录一份 `GLOSSARY.md`，架构决定放 `docs/adr/`。See `docs/agents/domain.md`.

### Design docs

`.scratch/ecom-user-behavior-pipeline/` 持有本仓的设计链：`SRS.md`（必须做到什么）→ `gdd.md`（每个功能怎么做）→ `HLD.md`（结构、技术栈定案、运行约束）。

**定位这一层从 `map.md` 进**——它是 wayfinder 地图（`Kind: map`）：已落子决策、当前前沿、迷雾。**HLD §0.1 自称「唯一入口」是上一代说法**，以本节为准；两者冲突时，`map.md` 与本节赢。读 `map.md` 看：

- 目的地是什么、什么在范围外、迷雾在哪 → `map.md` 的 Destination / Not yet specified / Out of scope
- 下一个该解哪个决策、依赖关系 → `map.md` 的 Decisions so far + [`issues/map-dag.md`](.scratch/ecom-user-behavior-pipeline/issues/map-dag.md)
- 每张票的问题与已解答案 → `.scratch/ecom-user-behavior-pipeline/issues/NN-*.md`

**HLD 细节指针**（`map.md` 负责「走到哪了」，HLD 负责「为什么是这么定的」）：

- 改技术栈、加组件、换版本，或问「为什么不用 X」→ HLD §3（定案表 + 弃选理由，含 `ty`/Grafana/Hudi/ClickHouse 等）
- 写 compose、定资源上限、端口绑定、沙箱形态，或排查「容器起不来/连不上」→ HLD §4（运行环境与沙箱约束）
- 确认模块职责、接缝、哪些名字碰不得 → HLD §2 与 §5
- 想知道上游文档哪条被一手证据证伪、或本项目与官方口径的偏离 → HLD §7

HLD 写下的每条约束都附检查方式；数字一律标 `官方给出` 或 `设计值`（**不得把设计值当官方保证**）。
