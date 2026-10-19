# Issue tracker: 本地 Markdown

本仓库的 issue 与 spec 以 markdown 文件形式存放在 `.scratch/` 下。（后端由 `/setup-matt-pocock-skills` 于 2026-10-18 与用户确认为「本地 markdown」，**不使用 GitHub Issues**。）

## Conventions

- 一个 feature 一个目录：`.scratch/<feature-slug>/`
- spec 写在 `.scratch/<feature-slug>/spec.md`
- 实现 issue 一个文件一张票：`.scratch/<feature-slug>/issues/<NN>-<slug>.md`，从 `01` 编号，**从不**把所有票合成一个文件
- 每张票文件顶部附近用一行 `Status:` 记录 triage 状态（字符串见 [triage-labels.md](./triage-labels.md)）
- 评论与讨论历史追加到文件底部 `## Comments` 标题下
- 被拒绝的请求另存于 `.out-of-scope/`（`/triage` 会读它来判断「以前拒绝过」）

## 各技能说「publish to the issue tracker」时

在 `.scratch/<feature-slug>/` 下新建文件（目录不存在就创建）。

## 各技能说「fetch the relevant ticket」时

读取该路径对应的文件。用户一般会直接给出路径或票号。

## Wayfinding operations

供 `/wayfinder` 使用。**map** 是一个文件，每张 **child** 票各占一个文件。

- **Map**：`.scratch/<effort>/map.md`（Notes / Decisions-so-far / Fog 正文）。在本地 markdown 后端里，map 由文件**路径与角色**标识（没有真实标签可打）；若需要显式标记，在 map.md 顶部写 `Kind: map` 与 `Label: wayfinder:map`。
- **Child ticket**：`.scratch/<effort>/issues/NN-<slug>.md`，从 `01` 编号，正文写问题本身。`Type:` 行记录票类型（`research` / `prototype` / `grilling` / `task`，等价于 wayfinder 的 `wayfinder:<type>` 标签）；`Status:` 行记录 `claimed` / `resolved`。
- **Blocking**：文件顶部附近的 `Blocked by: NN, NN` 行。当它列出的每个文件都是 `resolved` 时，本票解除阻塞。
- **Frontier**：扫 `.scratch/<effort>/issues/`，取 open、未被阻塞、未被认领的文件；编号最小者优先。
- **Claim**：先把 `Status: claimed` 写盘，再开始任何工作。
- **Resolve**：在 `## Answer` 标题下追加答案，置 `Status: resolved`，再往 `map.md` 的 Decisions-so-far 追加一条上下文指针（摘要 + 链接）。

## Wayfinder 标签

wayfinder 的五个标签在本地 markdown 后端里没有真正的标签系统，按**文件内的显式字段**落地：

| 技能里的标签 | 本地 markdown 的后端写法 |
| ------------ | ------------------------ |
| `wayfinder:map` | `map.md` 本人（可选 `Label: wayfinder:map` 行） |
| `wayfinder:research` | 票文件 `Type: research` |
| `wayfinder:prototype` | 票文件 `Type: prototype` |
| `wayfinder:grilling` | 票文件 `Type: grilling` |
| `wayfinder:task` | 票文件 `Type: task` |

`wayfinder:` 系列是 map 及其票**唯一**携带的标签，永不叠加 `ready-for-agent` 之类的 triage 标签：它们是决策票，不是实现工单。
