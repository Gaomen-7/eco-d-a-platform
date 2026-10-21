# 01: Python 工程与门禁闭环（uv）

**What to build:** 仓库根出现 uv 管理的 `ecomgen` Python 工程——工程声明（src 布局、`requires-python` 钉 3.12 线）、运行组与门禁组依赖全部 `==` 精确 pin、锁文件入库、`.venv` 可由 `uv sync` 幂等重建；Make 目标 `lint`/`typecheck` 改经 `uv run` 调用并全绿。完成后 S1＋S2 两条接缝一次打通：门禁以退出码自证，工具版本唯一权威是锁文件（ADR-0001）。P0 遗留脚本只做 autofix 级清理，语义问题上报不静默改写。细节以 `.scratch/repo-skeleton/spec.md` 的 Implementation Decisions 与 handoff 施工清单为准。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] `uv sync` 成功且幂等（删 `.venv` 重建结果一致）
- [ ] `uv lock --check` 通过；锁文件已入库
- [ ] `make lint` exit 0（ruff check ＋ format --check，经 `uv run`）
- [ ] `make typecheck` exit 0（basedpyright JSON 解析，errors=0；档位显式 standard）
- [ ] `uv run python -c "import ecomgen"` 成功
- [ ] 遗留脚本清理仅限 autofix 级；如遇语义问题，记录并上报而非改写
- [ ] 完成后本地 commit，未推送
