# eco-a-d-platform

生态相关平台工程。本文件是给编码 agent 的仓库级说明。

## Agent skills

### Issue tracker

本地 markdown：issue 与 spec 存放在 `.scratch/<feature-slug>/`，一张票一个文件。See `docs/agents/issue-tracker.md`.

### Triage labels

保留默认五角色：`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`，本地后端以票文件顶部的 `Status:` 行落地。See `docs/agents/triage-labels.md`.

### Domain docs

single-context：全仓共用根目录一份 `GLOSSARY.md`，架构决定放 `docs/adr/`。See `docs/agents/domain.md`.
