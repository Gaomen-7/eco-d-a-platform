# HLD 前置调研 · 类型门禁候选：pyright

> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（类型门禁候选：pyright）｜ 上游：hld-research-toolchain.md §3/§4、SRS C3 ｜ 状态：一手核查，未实测的已标注

**范围**：只核查「pyright 能否承担本仓类型门禁（同一份代码同样的结论）」。冻结项（uv 唯一环境管理、Ruff 唯一 lint+format、Python 线 pin 3.12）不重开。**记法**：A=agent 友好度；B=本机资源占用（官方未给出的写「官方未给出」）；C=接缝兼容性；另加「落地动作」列（**本文件不执行任何一条**）。

## 0. 结论速览

| 题目 | 结论 |
| --- | --- |
| **能否做硬门禁** | ✅ **能，但必须精确 pin 版本**（如 `1.1.414`）。理由不是「1.1.x 所以稳」，而是**门禁依赖的两样东西有书面稳定性契约**（退出码 + JSON schema，源码注释「Do not change」）——见 §2.1 |
| 与 ty 的差别（关键） | ty 官方自述「breaking changes, **including changes to diagnostics**, may occur between any two versions」→ 门禁语义失效。**pyright 官方无此句**，且把退出码/JSON schema 明写为公开契约。**性质差别，不是程度差别** |
| 唯一硬约束 | 诊断集合本身**无任何书面冻结承诺**（官方 `docs/` 34 项中无版本政策/稳定性页）→ **pin 精确号是不可省前提**，`PYRIGHT_PYTHON_FORCE_VERSION=latest` 须禁止 |
| PyPI `pyright` 的真相 / 最优打包 | **不是微软发布**，是社区第三方包装器（RobertCraigie）；微软 `docs/installation.md` 称其为「**community-maintained** Python package」，且它**默认依赖 Node.js**。本仓最优 → **basedpyright**（自带 pyright + `nodejs-wheel-binaries`，PyPI 官方发布，无运行时下载）；若坚持上游 pyright → **npm**（本机 Windows 与 WSL2 **两侧 node 均已装**）或包装器 `[nodejs]` extra |
| 许可 | 全部 **MIT**，符合 FSR §3「仅宽松许可」 |

## 1. 许可（FSR §3）

| 组件 | 许可 | 一手证据 |
| --- | --- | --- |
| **microsoft/pyright** | **MIT** | `LICENSE.txt` 首行「MIT License / Pyright - A static type checker for the Python language / Copyright (c) Microsoft Corporation. All rights reserved.」；`packages/pyright/package.json` 与 npm `pyright@1.1.414` 均 `"license":"MIT"` |
| PyPI `pyright`（包装器）/ **basedpyright** | **MIT** | 前者 PyPI JSON `"license":"MIT"` + README 末「## License / [MIT]」；后者 `LICENSE.txt` 与 microsoft/pyright **逐字相同**（同为 1150 B）+ `pyproject.toml` classifier `License :: OSI Approved :: MIT License` + npm `"license":"MIT"` |
| `nodejs-wheel-binaries` | **MIT** | README「distributed under the same MIT license as Node.js」；PyPI classifier `MIT License` |

**结论**：全部宽松（MIT），**口径可接受**。⚠️ 机器可读层面有坑：GitHub API 对两个 repo 都报 `spdx_id: NOASSERTION`（因文件名是 `LICENSE.txt` 不是 `LICENSE`）→ 做 SBOM/许可断言**不能依赖 GitHub API 的 spdx_id**，要读文件本体或 npm/PyPI 元数据（「不确定」1）。

## 2. 成熟度与稳定性政策

**版本 / 节奏**：当前 **1.1.414**（npm 与 `packages/pyright/package.json` 一致），版本线 **`1.1.x`**（非 `0.x`，对照 ty `0.0.89`）；官方 benchmark 历史页列「Every stable release from the **prior year**」共 **9 个**（1.1.406，2025-10-01 → 1.1.414，2026-09-09）。
**年龄 / 活跃度**（API `/repos/microsoft/pyright`）：`created_at` **2019-03-12**、`pushed_at` **2026-10-16**、`stargazers` **15,687**、`open_issues` 355、**未归档**。
**背书**：官方称 VS Code 推荐 **Pylance**，而 Pylance「**incorporates the Pyright type checker**」（`docs/installation.md`）；README §Performance 指向官方维护的 weekly benchmark 与逐年 release 历史（含 peak memory）。
⚠️ PyPI `pyright` 的分类器 `Development Status :: 5 - Production/Stable` 属**包装器**，**不是微软对 pyright 的声明**。

### 2.1 稳定性政策（核心）

**官方文档里没有版本政策页**：`docs/` 目录一手列举 **34 项**，无 `versioning`/`stability`/`version-policy`；installation、getting-started、command-line、ci-integration 全文无稳定性声明 → **「官方未给出版本/稳定性政策」**。

**但源码里有两处显式、可引用的公契**（与 ty 的分水岭）：

```ts
// These values are publicly documented. Do not change them.
enum ExitStatus { NoErrors = 0, ErrorsReported = 1, FatalError = 2, ConfigFileParseError = 3, ParameterError = 4 }
// The schemas for these objects are publicly documented. Do not change them.
interface PyrightJsonResults { version; time; generalDiagnostics; summary; ... }
interface PyrightJsonDiagnostic { file; severity; message; range?; rule?; }
```
出处：`packages/pyright-internal/src/pyright.ts`（多处同一注释）。**一句话结论**：pyright **没有** ty 那句「诊断可任意版本间变更」的自述，反而把**门禁真正依赖的两件事明写为不可变更的公开契约**；**诊断集合本身**（哪条规则开、默认 severity）**无书面冻结**——正是不 pin 就会红绿漂移的位置。
**落地必配**：① 精确 pin（`pyright==1.1.414`）写进 `pyproject.toml`/`uv.lock`；② **禁止** `PYRIGHT_PYTHON_FORCE_VERSION=latest`（包装器明文支持该变量，会把版本变成「latest」= 违反 SRS §2.4 C3）；③ 升版走**显式变更**，升级后重跑基线、接受或修复新诊断。

## 3. 安装与打包形态（本仓最关心）

### 3.1 PyPI `pyright` **是否依赖 Node.js** → **是**

- 依赖声明：PyPI `requires_dist` 含 **`nodeenv>=1.6.0`**、`typing-extensions>=4.1`；extra `nodejs` → `nodejs-wheel-binaries`。
- 微软定性（`docs/installation.md`）：「A **community-maintained** Python package by the name of "pyright" is available on pypi and conda-forge. **This package will automatically install node (which Pyright requires) and keep Pyright up to date.**」
- npm 包 vs PyPI 包装器：微软**只发 npm**：`npm install -g pyright`。官方另明写 pyright「is written in TypeScript, **requiring node to be installed**, and is normally installed with **npm**」。
- **首次运行是否下 node / 下到哪**（包装器 README）：「first checking if node is in the `PATH`. **If it is not, then we download node at runtime using nodeenv**, then install the pyright npm package using `npm`」。缓存根按序取 `PYRIGHT_PYTHON_CACHE_DIR` → `XDG_CACHE_HOME` → **默认 `~/.cache`**。
- **是否落 C 盘**：⚠️ **是**。Windows 上 `~/.cache` = `C:\Users\Administrator\.cache`，而本机 **C: 仅剩 9.6 GB**（实测 112G/102G used/9.6G avail）→ 必须显式设 `PYRIGHT_PYTHON_CACHE_DIR`。

### 3.2 `uv tool install pyright` / `uv add --dev pyright` 是否官方支持

| 路径 | 官方支持？ | 依据 |
| --- | --- | --- |
| **微软侧** `uv add --dev pyright` / `uv tool install pyright` | ❌ **官方未给出**（`installation.md` 只写 `pip install pyright` / `conda install pyright` / `npm install -g pyright`；`ci-integration.md` 全为 npm/pip/第三方 action；**全文无 uv**） | microsoft/pyright `docs/installation.md`、`docs/ci-integration.md` |
| **basedpyright 侧** `uv add --dev basedpyright` / `uv tool install basedpyright` | ✅ **官方明确给出**，标题即 `uv (recommended)` | basedpyright `docs/installation/command-line-and-language-server.md` |

### 3.3 本机实测（2026-10-19，只读命令）

| 项 | Windows 宿主 | WSL2 (Ubuntu) |
| --- | --- | --- |
| `node` / `npm` / `npx --version` | **v24.21.0 / 11.19.0 / 11.19.0**（`.dsh-win/node/node`；`where node` 另有 `F:\node`、`F:\Anaconda` 两份） | **v22.22.1 / 9.2.0**（`/usr/bin/node`；npx 未单测，npm 自带） |
| `pyright` / `basedpyright`；`find_spec` | 可执行**均 NOT-FOUND**；`pyright`/`basedpyright`/`nodeenv`/`nodejs_wheel` 全 **False** | 未装 |
| `mypy` / npm 全局根 / 缓存 | PATH 上有 `/f/Anaconda/Scripts/mypy` 但当前 `python`（`F:\Python\python.exe`）**无法 import** → 非本仓实例；全局根 = **`F:\npm-global`**、`npm config get cache` = **`F:\Cache\npm`**（**均已非 C 盘**）；全局六个包（open-code-review、atomcode、codex、opencode-ai、pnpm@12.6.0、vue-cli）**无 pyright** | — |

> node 要求 `engines.node >= 14.0.0` → **两侧均满足**。但这些 node 是**宿主环境缺省**，不是本仓 pin 的值；走 npm 路径则 node 版本成为**新的未 pin 锚点**（与 C3 有张力）。

### 3.4 basedpyright（社区 fork）——是否更省事

**许可 / 打包（自带 node）**：**MIT**（`LICENSE.txt` 与 pyright 逐字相同；PyPI classifier、npm 均为 MIT）；✅ `requires_dist = ["nodejs-wheel-binaries>=20.13.1"]`，注释明写「only binaries are required (no cli) since the user **shouldn't have a node/npm binary unknowingly added to their PATH**」（basedpyright `pyproject.toml`）。
**官方自述对比点**：「unlike pyright, basedpyright can be installed from **PyPI without having to install NodeJS**」；「**Strict by default** … all rules enabled by default」；「**Baseline support**」；「when a new version of pyright is released, we merge it and release a new version within **a day**」；「Pylance features in any editor」（`docs.basedpyright.com/latest/`）。
**对 PyPI pyright 的批评（fork 自述，非中立结论）**：「there is an **unofficial version on pypi**, but by default it just installs node and the npm package the first time you invoke the cli, **which is quite flaky**」（并链 issue #231）。
**版本 / 节奏 / 门禁增益 / 风险**：**1.40.2**（2026-10-04）；`pushed_at` 2026-10-04；`stargazers` 3,633；`open_issues` **647**；`created_at` 2023-12-02（PyPI / API）。增益：`--baselinemode=lock`「**never writes** to the baseline file … instead **exits non-zero if the baseline file needs to be updated**」「you can think of it like a **lockfile**」，CI 环境**默认即 lock**，GitHub Actions 自动注解，`--gitlabcodequality`（`docs/configuration/command-line.md`）。风险：维护者**单一**（`DetachHead`；forks 137、subscribers 7）、**另加自有诊断规则**并改默认档（§5）、`--baselinemode` 官方自标「**experimental** … subject to **breaking changes**」。

**更省事的候选？→ 是（就「本仓安装代价 + uv 一体化」而言）**：自带 node 的 PyPI 包、官方给出 uv 指令、默认在 `./.venv` 找解释器。代价是换成**单一维护者的 fork**，并把上游诊断再叠一层自有规则。

## 4. 错误/诊断的机读性（本仓硬要求）

**4.1 `--outputjson` 确切结构**（官方 `docs/command-line.md`）：

```
{ version: string, time: string, generalDiagnostics: Diagnostic[],
  summary: { filesAnalyzed, errorCount, warningCount, informationCount, timeInSec } }
Diagnostic = { file: string, severity: 'error'|'warning'|'information', message: string,
               rule?: string,  // 无规则则不输出该字段
               range: { start:{line,character}, end:{line,character} } }
```
- **行/列从 0 起**（官方「Diagnostic line and character numbers are **zero-based**」）。
- 官方：「**Not all diagnostics have an associated diagnostic rule.** … If it's not, the `rule` field is **omitted**」→ 断言须容忍 `rule` 缺省。
- 源码 `range?: Range | undefined`（空 range 时缺省），文档未标可选 → **源码与文档不一致**（「不确定」2）。
- 内部 `DiagnosticCategory` 另有 `UnusedCode`/`UnreachableCode`/`Deprecated`：**文本输出过滤掉**，JSON 路径只收 Error/Warning/Information → **两种输出集合不同**，门禁应**只解析 JSON**。
- **退出码**（官方表格与源码 enum 一致）：**0** 无错误 / **1** 有错误 / **2** 致命错误（无错误或警告）/ **3** 配置无法读取或解析 / **4** 非法命令行参数。`--warnings` 时 warnings 也计入 1（「Use exit code of 1 if warnings are reported」）。
- **`--outputjson` 互斥**（源码逐字）：`incompatibleArgs = ['stats','verbose','createstub','dependencies']` → 同时给出即 **exit 4**；`--verifytypes`/`--createstub`/`--threads`/`--pythonpath` vs `--venvpath` 亦互斥，均返 4。
- JSON 模式下 **console 全部改道 stderr**（「redirect all console output to stderr so it doesn't mess up the JSON output, which goes to stdout」）→ stdout 可安全重定向后解析。

**4.2 诊断带稳定规则名 → 是**：`rule` 值即配置键（`reportMissingImports`、`reportArgumentType`、`reportAssignmentType`、`reportAttributeAccessIssue`、`reportCallIssue`、`reportUndefinedVariable`、`reportMatchNotExhaustive`…）；官方 `configuration.md` 的 **Diagnostic Settings Defaults** 表按 `off/basic/standard/strict` 逐条列出默认 severity（约 90 条规则）→ **可据此写断言**（例：断言 `generalDiagnostics[].rule` 不含 `reportMissingImports` 且 `errorCount==0`）。⚠️ 反向限制：**「规则名稳定」≠「哪些规则开/默认 severity 稳定」**；严格模式官方另注「overrides may **only increase** the strictness」——只保证上界方向，不保证集合不变。

## 5. 配置面

| 核查点 | 官方口径（一手） | 结论 |
| --- | --- | --- |
| 配置载体 | `pyrightconfig.json`（项目根）**或** `pyproject.toml` 的 `[tool.pyright]`；官方「`pyrightconfig.json` **always takes precedent over** `pyproject.toml`」 | 本仓已有 `pyproject.toml` → 用 `[tool.pyright]` |
| `include` / `exclude` | `include`（数组，支持 `**`/`*`/`?`；不写默认「配置文件所在目录」）；`exclude`（数组，**叠加**内建排除之上，**内建优先于 include**，可用 `useDefaultExcludes` 关） | 键名确切 |
| 内建排除（**对 uv 重要**） | 默认排除 `**/node_modules`、`**/__pycache__`、`**/.*`、`**/__editable__.*`、**auto-detected virtual environment directories** | ✅ **`.venv` 自动排除**，与 uv 共存零额外配置 |
| `venvPath` / `venv` / CLI | `venvPath`（「directory containing one or more subdirectories, each of which contains a virtual environment」）+ `venv`（string）；CLI `-v, --venvpath`（`--venv-path` 已 deprecated）；`--pythonpath` **不可与 `--venvpath` 同用** | 键名 `venvPath` / `venv`（驼峰） |
| `typeCheckingMode` 档位与默认 | `["off","basic","standard","strict"]`；官方「The default value for this setting is **"standard"**」；`off` = 类型规则全关但**语法/语义错误仍报** | 默认 **standard**（⚠️ basedpyright 文档称曾为 `basic` → 「不确定」3） |
| 关键环境键 | `pythonVersion`（`"M.m"`，如 `"3.12"`；不写则用当前解释器版本）；`pythonPlatform`（`Darwin`/`Linux`/`Windows`/`iOS`/`Android`/`All`）；`stubPath`（默认 `./typings`）；`extraPaths`；`executionEnvironments[]`（`root` + 可 per-env 覆盖 `pythonVersion` 与全部诊断规则） | 全确 |
| **uv `.venv` 下写哪些键** | ① 首选 `venvPath="."` + `venv=".venv"`（包装器 README 给的正是这组）；② CLI `--pythonpath .venv/Scripts/python.exe`（Win）/ `.venv/bin/python`（WSL）——官方推荐 `--pythonpath` 优先（「more robust… uses the configured python interpreter to determine the import resolution paths (`sys.path`)」）；③ `pythonVersion="3.12"` 明确锚点，**不要**依赖「用当前解释器」的回落 | 推荐 ①+③；② 作 CI 稳定解 |
| 官方对 `venvPath` 的保留 / uv+editable | 「This mechanism is **not recommended** for most users because it is **less robust**… relies on pyright's internal logic」；「best **not to specify** `venvPath` in the config file, since this path will typically differ for each developer」；`import-resolution.md` 另列「**uv with setuptools**」→ `[tool.uv] config-settings = { editable_mode = "compat" }`（避免 import hook，否则静态分析器找不到源文件） | 用**相对** `"."` 已足够；若本仓有 editable 安装须照抄该 config-setting |

## 6. 资源占用

**官方给出了内存数字**（与 hld-research-toolchain.md §3 的「官方未给出」不同）：

**官方 benchmark profile**（`typecheck-benchmark/history/history.json`）：`python_version 3.14.6`、`runner github-ubuntu-latest`、**`memory_limit_mb 8192`**、`node_options --max-old-space-size=6656`、`runs_per_package 1`、`warmup_runs 0`；语料 9 包（ansible/click/homeassistant/numpy/pandas/pytest/requests/torch/transformers）。**官方告警**：「Separate hosted runners introduce **machine variance**, so use the charts for **release-scale trends rather than small differences**」；「**1 measured run per package**」。

| 项 | 一手数字 |
| --- | --- |
| **peak memory（1.1.414）** | requests **351.0 MB**、click **363.1**、pytest **893.6**、ansible 1075.6、numpy **1851.6**、pandas 4425.5、torch 4789.9、transformers 5079.5、homeassistant **6166.5** |
| **可读地板 / 趋势** | 最小语料 requests：**334.1 MB（1.1.406）→ 351.0 MB（1.1.414）** ⇒ 工具基座约 **0.33–0.36 GB**（官方数字，非估算）；同语料跨 9 版整体**升**（click 349.9→363.1、numpy 1471.7→1851.6、homeassistant 5674.7→6166.5） |
| 安装体积 | npm `pyright@1.1.414` **unpackedSize 19,457,120 B（≈18.6 MiB）**、fileCount 5423；npm `basedpyright@1.40.2` **27,852,831 B（≈26.6 MiB）**、fileCount 5419 |
| PyPI 包体积 | `pyright` 1.1.414 wheel **6,225,049 B**（py3-none-any）/ sdist 4,154,638 B；`basedpyright` 1.40.2 wheel **13,694,487 B** / sdist 25,559,682 B |
| node 二进制 wheel / 本机磁盘 | `nodejs-wheel-binaries` **24.19.0**：`win_amd64` **42,448,503 B（≈40.5 MiB）**、`manylinux_2_28_x86_64` **61,123,607 B（≈58.3 MiB）**；本机 **C: 112G / 已用 102G / 剩 9.6G**（92%）→ 默认落 C 盘的缓存须显式改向（实测 `df -h`） |

**`--threads` 语义**（CLI 脚注 4 + 源码）：`threads>1` 时「**multiple copies of pyright are executed** in parallel」；不给 N 则按逻辑核数（≥4 取核数，否则 1）；**默认单进程**（显式 opt-in）。**官方未给出**：pyright 的**进程启动开销**与**本仓规模下的内存/耗时**（官方只给 9 个固定语料的 peak memory，对本仓**无数字** → 本文件**不推算**）。

> B 列小结：**基座 ≈0.35 GB / 次**（官方实测最小值），对本机 15.9 GB 内存与 9.6 GB 剩余 C 盘**不构成阻塞**；但**缓存落盘位置**必须改向非 C 盘。

## 7. 与既有接缝的兼容性

| 接缝 | 对照结果 |
| --- | --- |
| **Python 3.12 目标** | ✅ `pythonVersion="3.12"` 键确切；pyright 由 node 运行，**目标版本与装它用的 Python 版本解耦**（同 ty 性质）；包装器 `requires_python >=3.7`、basedpyright `>=3.10` 均满足 |
| **与 Ruff 分工是否重叠** | 主体**不重叠**（Ruff 不查类型；pyright 不 lint/format），但**存在真实重叠带**：pyright 有 `reportUnusedImport`/`reportUnusedVariable`/`reportUnusedClass`/`reportDuplicateImport` 等近似 lint 规则，与 Ruff F401 职责交叉 → **必须显式划界**（建议这类关闭、交给 Ruff）。更硬的实证：basedpyright 自身 `pyproject.toml` 写着 `reportImplicitStringConcatenation = false # conflicts with ruff formatter` —— **pyright 系规则与 Ruff formatter 有已记录的冲突**，与 toolchain §3「formatter 冲突清单」同类 |
| **与 uv `.venv` 协同** | ✅ 默认排除 auto-detected venv（`.venv` 不进检查）；解释器用 `venvPath`+`venv` 或 `--pythonpath`；官方另有 uv editable 的 `editable_mode="compat"` 指引。⚠️ `uv tool install`/`uv add --dev` 在**微软侧官方未给出**（§3.2） |
| **Windows 宿主 + WSL2 两侧** | ✅ 两侧 node 均已装（v24.21.0 / v22.22.1，均 ≥ `engines.node>=14`）；PyPI wheel 为 `py3-none-any`，`nodejs-wheel-binaries` 有 `win_amd64` 与 `manylinux_2_28_x86_64` 双 wheel，**WSL glibc 2.43 ≥ 2.28**（toolchain §0.1 实测）→ 两侧可装 |
| **`make verify` 断言 / 常驻性** | ✅ 最简只需判**退出码**（0 通过；1/2/3/4 失败，语义已分档）；更细则 `--outputjson > report.json` 后按 `generalDiagnostics[].rule` / `summary.errorCount` 断言。⚠️ JSON 模式与 `--verbose`/`--stats` **互斥**，make 里别叠。❌ **无冲突**于「不引入调度器/不加常驻容器」：pyright 是**一次性 CLI 工具**，不监听端口、不常驻、无 daemon（`--watch` 是显式 opt-in，门禁不用）→ **零常驻**，不动 `REQ-F-ORCH-02` 天花板 |
| SRS §2.4 C3「锚点精确 pin，禁止 latest」 | ✅ 可满足（`pyright==1.1.414` + `uv.lock` 固定 `nodejs-wheel-binaries` 解析结果），**但这是做门禁的前置条件**，不自动获得 |
| FSR §3「仅宽松许可」/ SRS §3.4 冻结项 / toolchain §4（ty） | ✅ MIT；**不改任何冻结值**，只给 `verify` 目标加一条内部断言；本文件**只回答**「pyright 可否取代 ty 的硬门禁位」，**不重开** §4 对 ty 的判定（ty 若引入仍按 §4 只作**非阻塞**） |

## 8. 对照表：pyright vs basedpyright vs mypy（ty 作已判不可门禁基线）

| 维度 | **pyright**（上游，微软） | **basedpyright**（fork） | **mypy** | *ty（基线）* |
| --- | --- | --- | --- | --- |
| 许可 | **MIT**（`LICENSE.txt` 可引原文） | **MIT**（同一文本） | **MIT**（PyPI `license_expression:"MIT"`，**机器可读**；含 `mypy/typeshed/LICENSE`） | MIT |
| 打包 / 安装代价 | 官方**只发 npm**（需 node）；PyPI `pyright` 是**社区包装器**，默认 nodeenv **首次运行下 node → `~/.cache`（C 盘）** | **自带 node 的 PyPI 包**（`nodejs-wheel-binaries>=20.13.1`，win_amd64 40.5 MiB）；**官方给出 uv 指令** | 纯 Python 轮子（win_amd64 cp312 **11.4 MB**、`py3-none-any` 2.8 MB）；**不依赖 node**，自带 typeshed | 独立安装器 |
| 机读输出 | ✅ `--outputjson`，`severity`/`rule`/`range` + 退出码 **0/1/2/3/4**（源码「Do not change」） | ✅ 同上 **+ `--gitlabcodequality`**、`--baselinemode=lock`、GH Actions 自动注解 | ✅ `-O json, --output FORMAT {json}`；错误码成体系（`error_codes.html`） | 退出码 0/1/2/101 + `--exit-zero` |
| **门禁可用性** | ✅ **可做硬门禁**（**须精确 pin**；退出码/schema 有书面契约） | ✅ 可做硬门禁（额外提供 lock 语义的 baseline） | ✅ 可做硬门禁（长历史 + Production/Stable） | ❌ 官方自述诊断可任意版本间变更 |
| 稳定性风险（门禁视角） | 诊断集合**无书面冻结**；1.1.x 发布**频繁**（近一年 9 个稳定版），同语料峰值内存**上升** | 上游诊断**再叠自有规则** + 默认档改 `recommended`；**单一维护者**；`--baselinemode` 自称 experimental / 将 breaking | 官方自述「Command line flags are **liable to change between releases**」「the exact list of flags enabled by `--strict` **may change over time**」→ 变更**有预告**，仍须 pin | 0.0.x + 诊断可任意版本间变更 |
| 成熟度标记 | 2019 起、15,687★、活跃、被 **Pylance（商业产品）内置** | 3,633★、647 open issues、单人；无 `Development Status` classifier | PyPI **`Development Status :: 5 - Production/Stable`**；cp310–cp315 win/manylinux **全矩阵** | PyPI **`4 - Beta`**；Stable 里程碑 open |
| 本仓安装代价（实测基线） | ① npm：node **两侧已装**，`npm i -g pyright@1.1.414` 即可（全局根已在 `F:\npm-global`）；② PyPI：需额外治 node 下载与 C 盘缓存 | `uv add --dev basedpyright==1.40.2` 一条，**无运行时下载** | `uv add --dev mypy==2.4.0`，**无 node** | — |
| A（agent 友好度） | **高**：官方 CLI 文档给全字段名/退出码表/JSON 结构；`configuration.md` 逐规则列默认值 | **高（更适合 agent）**：官方 uv 指令、`--outputjson` 同构、CI 集成内置、baseline 有 lock 语义 | **高**：错误码成体系、文档成熟；`--output json` 细节与退出码本轮未逐条核实 | 高 |
| B（本机资源） | 官方实测**最小语料 334–363 MB**、最大 6.2 GB（语料相关），内存上限配置 8 GB；包 ≈18.6 MiB | 包 ≈26.6 MiB（npm）/**13.7 MiB（PyPI wheel）** + node wheel **40.5 MiB**；**官方未给内存数字** | 轮子 11.4 MB；**官方未给内存数字** | 官方未给出 |
| C（接缝） | ✅ 3.12 / `.venv` 自动排除 / 双平台 / 零常驻；⚠️ uv 于微软侧**无官方背书**；⚠️ 与 Ruff 有**局部重叠带**须显式划界 | 同上，且**默认在 `./.venv` 找解释器**（与 uv 最贴） | ✅ 3.12（`requires_python>=3.10`）/ 无 node / 零常驻；⚠️ 与 Ruff 同样有 unused-import 类重叠 | — |
| **落地动作 / 回滚** | 装 → 改配置（`[tool.pyright]`）→ `make verify` 加断言。回滚：`uv remove --dev pyright` / `npm uninstall -g pyright` + 删 `[tool.pyright]` 段 + 删 Makefile 断言行 | 同左（换成 basedpyright）；回滚：`uv remove --dev basedpyright` | 同左；回滚：`uv remove --dev mypy` | — |

**选型提示（非决定，供拍板）**：要**「最小移动部件 + 门禁契约最硬」** → 上游 **pyright via npm**（微软一方产物，MIT，退出码/schema 有「Do not change」契约），代价是 node 版本成为新锚点；要**「uv 一体化 + 无运行时下载 + `.venv` 默认对齐」** → **basedpyright**，代价是 fork 与更严默认档。**两者都必须精确 pin。**

> 本表 A/B/C 三轴按题面记法排列；「落地动作」列在表尾统一给出，全部**待授权**，见 §9。

## 9. 落地动作清单（**待用户单独授权；本文件未执行任何一条**）

| # | 动作 | 类别 | 依据 | 回滚方式 |
| --- | --- | --- | --- | --- |
| P1 | 决定走 **上游 pyright(npm/PyPI)** 还是 **basedpyright** | 决策 | §8 | — |
| P2 | PyPI 路径：`uv add --dev "pyright==1.1.414"`（或 `pyright[nodejs]`）/ `uv add --dev basedpyright==1.40.2` | 安装 | §3.2/§3.4 | `uv remove --dev <pkg>` + 重生成 `uv.lock` |
| P3 | npm 路径：`npm i -g pyright@1.1.414`（全局根已在 `F:\npm-global`） | 安装 | §3.3 | `npm uninstall -g pyright` |
| P4 | `pyproject.toml` 加 `[tool.pyright]`：`pythonVersion="3.12"`、`venvPath="."`、`venv=".venv"`、`include`、`exclude`（含 `**/node_modules`、`**/__pycache__`）、**关闭与 Ruff 重叠的 unused/duplicate-import 规则** | 改配置 | §5/§7 | 删除 `[tool.pyright]` 整段 |
| P5–P7 | 设 `PYRIGHT_PYTHON_CACHE_DIR`（及 `PYRIGHT_PYTHON_ENV_DIR`）到非 C 盘；`make verify` 加 `pyright --outputjson > report.json` 并断言退出码 0 + `summary.errorCount==0`（**勿**与 `--verbose`/`--stats` 同用）；显式**禁止** `PYRIGHT_PYTHON_FORCE_VERSION=latest` 并把版本 pin 写进 `uv.lock` | 改配置 | §3.1（C: 仅剩 9.6 GB）/ §4.1 / §2.1（C3） | 移除环境变量 / 删断言行 / 撤销 pin 即回到「门禁可漂移」 |

---

## 未决 / 官方未给出 / 不确定

**官方未给出（不得用估算值代替）**
1. pyright **正式的版本政策/稳定性页面**：官方 `docs/` 目录 34 项中无此页；README / installation / getting-started / command-line / ci-integration 全文无稳定性承诺。**唯一稳定性证据是源码内注释**（`Do not change them` / `Do not change it`）。
2. pyright **在本仓规模下的内存与耗时**：官方只发布 9 个固定语料的 peak memory（最小 334–363 MB、最大 6.2 GB），对本仓**无对应数字**，本文件不推算。
3. **诊断集合（哪些规则开 / 默认 severity）的稳定性**：官方**未给出**任何冻结承诺。
4. **微软对 uv 的官方支持**：`installation.md` / `ci-integration.md` 全文无 uv；`uv add --dev pyright` / `uv tool install pyright` 是**推断可用，非官方声明**。
5. basedpyright 的**内存占用**：官方未给出（只给包体积与「一天内跟进上游」的节奏承诺）。
6. mypy 的**退出码逐档定义**与 `--output json` 的**确切字段名**：本轮未逐条核实（只取到 `-O FORMAT, --output FORMAT {json}` 与「flags liable to change」）。
7. **PyPI `pyright` wheel 6,225,049 B 的构成**：README 称首次运行才 `npm` 安装，而 wheel 达 5.94 MiB —— 二者关系本轮**未取得**解释（或内嵌 npm tarball），不作断言。

**不确定 / 落到实现前须复核**
8. **GitHub 许可识别与文件本体不一致**：两个 repo 的 GitHub API `license.spdx_id` 均为 **`NOASSERTION`**（文件名是 `LICENSE.txt` 而非 `LICENSE`），而**文件本体与 npm/PyPI 元数据均为 MIT**。许可断言以**文件本体 + 包元数据**为准。
9. **`range` 字段可选性**：官方 `command-line.md` 列为必有，源码写作 `range?: Range | undefined`（空 range 时缺省）→ **文档与源码不一致**，断言按「可能缺省」处理更安全。
10. **`typeCheckingMode` 默认值**：微软 `configuration.md` 明写「default value is **"standard"**」；basedpyright `better-defaults.md` 称该值「**used to be "basic"**」→ 以微软 `configuration.md` 为准，差异本身记录。
11. **JSON 与文本输出的诊断集合不同**：文本路径显式过滤 `UnusedCode`/`UnreachableCode`/`Deprecated`，JSON 路径按 Error/Warning/Information 三类纳入 → **同一份代码两种输出可能给出不同行数**；门禁应固定**只解析 JSON**。
12. **pyright 与 Ruff 的重叠边界须本仓自行裁定**：`reportUnusedImport`/`reportDuplicateImport` 等归谁；basedpyright 自身 `pyproject.toml` 已记录 `reportImplicitStringConcatenation` 与 ruff formatter 冲突。
13. **npm 路径下 node 版本成为未 pin 锚点**：本机 Windows node v24.21.0（PATH 上有 3 份 node：`.dsh-win`/`F:\node`/`F:\Anaconda`）、WSL node v22.22.1；走 npm 而不 pin node，与 C3「禁用漂移」精神相抵。
14. **截止日期提醒**：以上均为 **2026-10-19** 时点的一手核查。`pyright` 1.1.414（2026-09-09 发布）、`basedpyright` 1.40.2（2026-10-04）、`mypy` 2.4.0（2026-10-01）、`nodejs-wheel-binaries` 24.19.0 等版本**已会继续变动**，复用时须重新核对。
