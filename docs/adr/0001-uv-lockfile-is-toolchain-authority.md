# Python 工具链版本的唯一权威是 uv.lock

Status: accepted (2026-10-21)

WSL2 里 ruff / basedpyright 已有用户级独立安装（`~/.local/bin/`，实测 0.17.1 / 1.40.2），但 Makefile 的 `lint` / `typecheck` 决定改为一律经 `uv run` 调用、全部依赖 `==` 精确 pin、`uv.lock` 入库：**工具链版本的唯一权威是 lockfile，不是机器上恰好装了什么**。独立安装版与 lockfile 版当前一致纯属巧合，不得据此把 Makefile「优化」回直调二进制——那会重新引入跨机漂移，违背锚点政策（SRS C3）。`requires-python` 钉 `>=3.12,<3.13` 与宿主 Python（3.14）解耦；Faker 钉到补丁号（官方 seeding 不保证跨补丁一致，HLD §3.2）。

> 本 ADR 落笔于文档会话（2026-10-21）；Makefile 与 `pyproject.toml` 的对应落盘随骨架施工执行，交接清单见 `.scratch/handoff/`（不入库）。
