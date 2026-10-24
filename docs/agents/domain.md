# Domain Docs

本仓库的域文档布局：**single-context**（单上下文）——一个仓库共用**一份根目录的 `GLOSSARY.md`**，架构决定放在 `docs/adr/`。

本文件说明工程类技能在探索代码库时应当如何消费这些域文档。

## 探索之前先读这些

- **`GLOSSARY.md`**（仓库根目录，已存在）——单一术语表，全仓共用。
- **`docs/adr/`**（已存在）——读与你要动的那块区域相关的 ADR。

`GLOSSARY.md` 于 2026-10-20 创建；`docs/adr/` 于 2026-10-21 随 ADR-0001/0002 创建。二者仍**按需增长**：不为未来词条或决定预建空文件，新词条与新 ADR 由 `/domain-modeling`（经 `/grill-with-docs` 等到达）在结晶时写入。

若这些文件不存在，**静默继续**：不要提示它们缺失，也不要建议先建。

> 布局固定为 single-context，因此本仓库不使用 `GLOSSARY-MAP.md`，也不建 per-context 的 `GLOSSARY.md`。旧版技能用的 `CONTEXT.md` / `CONTEXT-MAP.md` 名称已废弃，新版只认 `GLOSSARY.md` / `GLOSSARY-MAP.md`。

## 文件结构（single-context）

```
/
├── GLOSSARY.md          ← 全仓唯一术语表（2026-10-20 创建，按需增长）
├── docs/
│   ├── adr/             ← 架构决定记录（2026-10-21 随 ADR-0001/0002 创建，按需增长）
│   └── agents/          ← 本目录：技能自身配置
└── src/
```

## 使用术语表里的词汇

当你的输出命名了一个领域概念（issue 标题、重构提案、假设、测试名），使用 `GLOSSARY.md` 里定义的用法，不要漂移到术语表明确回避的同义词。

需要的概念还不在术语表里，这是一个信号：要么你在发明项目不用的语言（重新考虑），要么确实有缺口（记下来交给 `/domain-modeling`）。

## 标出 ADR 冲突

如果你的输出与既有 ADR 矛盾，明确暴露出来，而不是悄悄覆盖：

> _与 ADR-0007（event-sourced orders）冲突，但值得重新讨论，因为……_

## 旧名迁移

本次初始化时仓库内**不存在** `CONTEXT.md` 或 `CONTEXT-MAP.md`，无需改名。今后若出现旧名文件，应改名为 `GLOSSARY.md` / `GLOSSARY-MAP.md`，并同步本文件内的引用。
