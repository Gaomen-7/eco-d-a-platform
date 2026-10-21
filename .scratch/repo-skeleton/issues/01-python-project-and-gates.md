# 01: Python 工程与门禁闭环（uv）

**What to build:** 仓库根出现 uv 管理的 `ecomgen` Python 工程——工程声明（src 布局、`requires-python` 钉 3.12 线）、运行组与门禁组依赖全部 `==` 精确 pin、锁文件入库、`.venv` 可由 `uv sync` 幂等重建；Make 目标 `lint`/`typecheck` 改经 `uv run` 调用并全绿。完成后 S1＋S2 两条接缝一次打通：门禁以退出码自证，工具版本唯一权威是锁文件（ADR-0001）。P0 遗留脚本只做 autofix 级清理，语义问题上报不静默改写。细节以 `.scratch/repo-skeleton/spec.md` 的 Implementation Decisions 与 handoff 施工清单为准。

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] `uv sync` 成功且幂等（删 `.venv` 重建结果一致）——冷跑 51.8 s / 二次 `Checked 11 packages`，包集合 diff 为空
- [x] `uv lock --check` 通过；锁文件已入库（`uv.lock` 12 包，含 `nodejs-wheel-binaries`）
- [x] `make lint` exit 0（ruff check ＋ format --check，经 `uv run`）
- [x] `make typecheck` exit 0（basedpyright JSON 解析，errors=0；档位显式 standard）
- [x] `uv run python -c "import ecomgen"` 成功
- [x] 遗留脚本清理仅限 autofix 级；如遇语义问题，记录并上报而非改写——ruff 原本即全绿，**零 autofix 需要**；四条语义问题已上报未改写（见 `## Answer`）
- [x] 完成后本地 commit，未推送

## Answer

**落盘**（commit `60141d3`，合入 `feat/repo-skeleton`）：

- `pyproject.toml`：`name = "ecomgen"`、`requires-python = ">=3.12,<3.13"`、src 布局（`hatchling` 构建后端，`[tool.hatch.build.targets.wheel] packages = ["src/ecomgen"]`）。
- 运行组 `==` pin：`confluent-kafka==2.16.0` / `pydantic==2.14.1` / `faker==40.43.0`（Faker 补丁号，官方 seeding 不保证跨补丁一致）。
- 门禁组 `==` pin：`ruff==0.17.1` / `basedpyright==1.40.2`。
- `[tool.pyright]`：`typeCheckingMode = "standard"`（显式档，不用 fork 的 `recommended`）、`pythonVersion = "3.12"`、`venvPath = "."` / `venv = ".venv"`、`include = ["src"]`、`exclude = ["**/node_modules", "**/__pycache__"]`；`reportUnusedImport`/`reportUnusedVariable`/`reportUnusedClass`/`reportDuplicateImport` 显式关（归 ruff 独占，HLD §3.3.5）。**不建** `pyrightconfig.json`（它总是优先，会盖掉工程内声明）。
- `[tool.ruff]`：`target-version = "py312"`、`src = ["src"]`、`lint.extend-select = ["I"]`（formatter 不排 import ⇒ 组合纪律 `check --select I --fix` 后 `format`）。
- Makefile：`lint`/`typecheck` 改 `$(UV) run <tool>`；`RUFF`/`BASEDPYRIGHT` 两个直调变量**删除**（ADR-0001：唯一权威是 lockfile）。目标名与 `config-check` 未动。

**验收实测（WSL2，2026-10-21）**：`make lint` rc=0（`All checks passed!` / `47 files already formatted`）；`make typecheck` rc=0（`errors=0 warnings=0 files=1`）；`uv lock --check` rc=0；`uv sync` 冷跑 rc=0 且幂等；`uv run python -c "import ecomgen"` rc=0。

**反证（门禁非空转）**：改动前 `make lint` 展开为直调 `~/.local/bin/ruff`（不经锁文件），`make lint RUFF=/nonexistent/ruff` 得 exit 2；改动后该 override 失效。隐藏 `~/.local/bin/ruff` 后 `make lint` 仍 rc=0 ⇒ 工具确实来自 `.venv`/`uv.lock`。负向对照：坏文件 → lint exit 2；真类型错误 → typecheck exit 2 且 `errors=1`。

**P0 遗留脚本的语义问题（已上报、未改写；ruff 原本全绿故无 autofix 需要）**：

1. `docker/superset/superset_config.py` — `AUTH_USER_REGISTRATION` 在 Superset 6.1 不是真实配置键（镜像 `config.py` 里只有注释示例，且不能从 `superset.config` 导入）；「关掉匿名自助注册」实际靠 Flask 默认值成立，非该行生效。
2. 同文件 `PUBLIC_ROLE_LIKE = "Gamma"` 与其上方「零账号只读、暴露面收窄」的注释相悖：6.1 另有更严格的 `PUBLIC_ROLE_PERMISSIONS` 内置集，`"Gamma"` 给出的权限**多于**意图。
3. 同文件 `SUPERSET_LOAD_EXAMPLES = False` 是死配置——它不是 `superset/config.py` 的键；真正生效的是 compose 已设为 `"no"` 的 shell 环境变量。
4. `scripts/config_check.py` — compose JSON 里 `mem_limit`/`memswap_limit` 是**字符串**；`int(mem)` 遇非数字会抛异常而非按文档「exit 1 = 有违规」报违规；`check=True` 也把 compose 错误变成 traceback。

上述 4 条均超出本票写权（spec §Out of Scope 亦把 P1 语义面排除在外），按票面要求**只记录不改写**，留给对应会话裁决。
