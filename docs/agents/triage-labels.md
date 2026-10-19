# Triage 标签

技能内部用五个规范的 triage 角色说话。本文件把角色映射到本仓库 issue tracker 里实际使用的字符串。

**已选：保留默认五角色**（2026-10-18 与用户确认，名称即标签字符串）。

| mattpocock/skills 里的标签 | 本仓库的标签 | 含义 |
| -------------------------- | ------------ | ---- |
| `needs-triage`             | `needs-triage` | 维护者需要评估这条 issue |
| `needs-info`               | `needs-info` | 等报告人补充信息 |
| `ready-for-agent`          | `ready-for-agent` | 规格完整，可交给离线 agent |
| `ready-for-human`          | `ready-for-human` | 需要人来实现 |
| `wontfix`                  | `wontfix` | 不予处理 |

本地 markdown 后端没有标签系统，所以「打标签」= 在票文件顶部附近写一行：

```markdown
Status: ready-for-agent
```

（`/to-tickets` 的票模板本来就带 `**Status:** ready-for-agent`；`/triage` 的结论写 `Status: <角色>`。）

技能提到某个角色时（例如「打上 AFK-ready 的 triage 标签」），使用本表对应的字符串。
后续打标签严格按技能规则走，不额外强制任何标签：wayfinder 的票只带 `wayfinder:` 系列，永不叠加 triage 标签。

要改词汇，直接编辑上表的「本仓库的标签」列。
