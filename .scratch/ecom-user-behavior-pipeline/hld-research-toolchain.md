# HLD 前置调研 · 本项目自有代码的语言 / 框架 / 运行时 / 工程工具链候选

> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（工具链）｜ 上游：FSR/SRS/GDD 冻结项 ｜ 状态：一手核查，未实测的已标注

**范围**：只核查**本项目自有代码**（Python 生成器、Flink 作业、编排脚本）所用的语言/框架/运行时/工程工具链。不重开已冻结的锚定栈（Kafka 4.3.1 / Flink 2.2.x / connector 5.0.0 / Paimon 2.1.0 / Doris 4.1.5 / flink-doris-connector 26.3.0 / Superset 6.1.0）。
**记法**：A=agent 友好度；B=本机资源占用（官方未给出的写「官方未给出」）；C=与既有接缝兼容性；另加「落地动作」列（安装/拉镜像/改配置 + 回滚方式，**本文件不执行任何一条**）。

---

## 0. 结论速览

| # | 题目 | 推荐 | 可进 CI/门禁？ | 落地动作类别 |
| --- | --- | --- | --- | --- |
| 1 | Python 运行时线 | **3.12**（3.13 可接受，3.11 只作下限） | ✅ | 安装（uv python install 3.12） |
| 2 | 环境与依赖管理 | **uv** | ✅ | 改配置（PATH）+ 安装 |
| 3 | 静态检查与格式化 | **Ruff**（lint+format 一体） | ✅ | 安装 |
| 4 | 类型检查 `ty` | 可引入，但**只能非阻塞** | ❌ **现在不能做硬门禁** | 安装 |
| 5 | Kafka 客户端 | **confluent-kafka**（librdkafka） | ✅ | 安装（有预编译 wheel） |
| 6 | 事件 schema 校验 | **pydantic v2** | ✅ | 安装 |
| 7 | Flink 侧工程栈 | **JDK 17 + Maven** | ✅ | 安装（JDK 17 + Maven ≥3.8.6） |
| 8 | 编排工具 | **GNU Make（跑在 WSL2 侧）**；just/task 不采纳 | ✅ | 安装 + 改配置 |

---

## 0.1 本机现状（2026-10-19 只读实测；未装的如实标注）

| 项 | Windows 宿主 | WSL2（Ubuntu 26.04 LTS） | 一手命令 |
| --- | --- | --- | --- |
| Python | **3.13.0**（`F:\Python\python.exe`） | **3.14.4**（系统 `python3`） | `python --version` / `python3 --version` |
| uv | **0.12.19** 已装于 `C:\Users\Administrator\.local\bin\uv.exe`（**42,417,456 B**），**不在 PATH**（`Get-Command uv` = NOT-FOUND） | **未装** | 直调 exe / `uv --version` |
| uv 缓存 | `%LOCALAPPDATA%\uv\cache`，**实测 0.0 MB**（空） | — | `uv cache dir` + 目录求和 |
| ruff / ty | **均未装** | **均未装** | `ruff --version` / `ty --version` |
| Java | PATH 上是 **Oracle 18.0.2**；`JAVA_HOME=E:\space\09-tools\Jdk1.8` | **OpenJDK 11.0.32.1** | `java -version` |
| Maven | **3.6.0**，且运行时用的是 **JDK 1.8.0_144** | 同一个 3.6.0（`Maven home: /mnt/e/space/09-tools/Maven3.6`，即**借 Windows 二进制**） | `mvn -version` |
| Gradle / make / just / task | **四者全未装** | **make 未装**（`make: command not found`）；just/task 未装 | 各自 `--version` |
| Docker | 客户端 **29.8.0**，**守护进程不可达**（`open //./pipe/dockerDesktopLinuxEngine` 找不到） | distro 内**无 docker 命令**（WSL integration 未对 Ubuntu 开启） | `docker version` |
| 资源 | 12 逻辑核 / 6 物理核 / 15.9 GB；**C: 仅剩 9.50 GB**；D: 12.7 / E: 36.5 / F: 77.6 / G: 74.1 GB 空闲 | MemTotal **11962 MB（≈11.68 GB）**，与 FSR U1 实测一致；glibc **2.43**；内核 6.18.54.1-microsoft-standard-WSL2 | `Get-PSDrive` / `free -m` / `ldd --version` |
| Python 包 | `faker`/`confluent_kafka`/`pydantic`/`msgspec`/`aiokafka`/`kafka`/`ruff` **全部 MISSING** | — | `importlib.util.find_spec` |

> C 盘仅剩 9.5 GB 是一个**新暴露的约束**：任何默认落 C 盘的工具缓存（uv cache 默认就在 C 盘）都要显式改向，否则与 SRS 的磁盘口径打架。

---

## 1. Python 运行时线：3.11 / 3.12 / 3.13

**冻结口径**：SRS §3.5「Python 3.11+ 与 Faker(MIT)」。以下只核查「哪条线最稳」，不动版本口径。

| 依赖 | 官方 requires-python | 官方 classifier 覆盖 | 本机 wheel 可得性（一手） |
| --- | --- | --- | --- |
| Faker **40.43.0** | `>=3.10` | 3.10 / 3.11 / 3.12 / 3.13 / 3.14 | `py3-none-any`（纯 Python）；Windows 上官方额外拉 `tzdata` |
| confluent-kafka **2.16.0** | `>=3.8` | 未列 3.x 细分 | **cp310–cp314（含 cp314t）**：`win_amd64` 与 `manylinux_2_28_x86_64` 均有；实测 cp313-win_amd64 = **4,710,225 B**、cp313-manylinux_2_28_x86_64 = **4,963,469 B** |
| msgspec **0.22.0** | `>=3.10` | 3.10–3.15 | cp310–cp315，含 `win_amd64` |
| pydantic **2.14.1** | 3.10+（v2.14.0 起**移除 3.9**、**新增 3.15**） | 3.10–3.15 | pydantic-core 的 wheel 矩阵**本轮未核查** |

**三条线都可用**（Faker 与 confluent-kafka 都覆盖 3.11/3.12/3.13）。差异不在「能不能」，在**哪条线能让「锚点」真的立住**：

- 本机**环境版本是 3.13（Windows）/ 3.14（WSL）**，二者都不是本项目 pin 的值。若选 3.13/3.14，生成器会**随宿主漂移**——违反 C3「锚点必须精确 pin，禁止 `latest`」（SRS §2.4）。
- 选 **3.12** 强制走 `uv python install 3.12`，锚点由 uv 托管、可复现，且 3.12 距 3.11 下限有安全余量。
- 3.11 只贡献「更久的存在时间」，对本仓无额外收益；3.14 虽被 Faker/confluent-kafka 的 classifier 覆盖，但它是本机 WSL 的**环境缺省**，pin 它等于不 pin。

> **推荐：3.12**（理由：wheel 覆盖充分 + 把锚点从宿主环境里解耦；B 列：官方未给出运行时的内存数字，此处不填）。
> ⚠️ **对本仓的直接影响**：Faker 官方 seeding 文档原话——「results are **not guaranteed to be consistent across patch versions**. If you hardcode results in your test, make sure you **pinned the version of Faker down to the patch number**」。`REQ-F-GEN-06`（同种子产出同哈希）**必须把 Faker pin 到补丁号**，否则种子可复现性不成立。这是本文件对冻结项最硬的一条补充。

---

## 2. 环境与依赖管理：uv（Astral）

**版本与平台**：官方安装页当前示例版本 `0.13.1`；本机已装 **0.12.19**（`x86_64-pc-windows-msvc`）。uv 官方 **Tier 1** 平台含 **Windows (x86_64)** 与 **Linux (x86_64)** → 与「Windows 宿主 + WSL2」双面天然咬合。

| 核查点 | 官方口径（一手） | 结论 |
| --- | --- | --- |
| Windows 安装 | `PS> powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 \| iex"`；亦 `winget install --id=astral-sh.uv -e` / `scoop` / PyPI+pipx | **本机无需安装，只需把 `~/.local/bin` 加进 PATH** |
| WSL/Linux 安装 | `curl -LsSf https://astral.sh/uv/install.sh \| sh`（无 curl 用 `wget`）；官方 Docker 镜像 `ghcr.io/astral-sh/uv` | WSL 侧需安装 |
| `uv run` | 官方：「Locking and syncing are **_automatic_** in uv. For example, when `uv run` is used, **the project is locked and synced before invoking the requested command**」 | 一条命令即得可复现环境 |
| `uv lock` | 显式生成/更新；`uv lock --check`（等价 `--locked`）→ **lockfile 未过期才通过** | 可直接做门禁断言 |
| `uv python install` | `uv python install 3.12`；Python **无官方可分发包**，uv 用 Astral `python-build-standalone`；默认只装版本化可执行文件（`--default` 为实验性） | 3.12 锚点的落地手段 |
| lockfile 可复现性 | `uv.lock` 是 **universal / cross-platform** lockfile，跨 OS、架构、Python 标记**一套解析**；官方要求 **check into version control**；格式 **uv 专有、其它工具不可用**；可导出 `requirements.txt` / **`pylock.toml`（PEP 751）** / CycloneDX SBOM | 强于 `requirements.txt`，且留了逃生口 |
| 能否完全替代 venv+pip | uv **自建并管理 `.venv`**（官方：自动排除出 git，靠内部 `.gitignore`）；`uv pip install` 是 **drop-in**，但官方明说「uv is **_not_** intended to be an _exact_ clone of `pip`」 | **工程流可替代；但有三处官方明载偏差** |

**官方明载的三处偏差（写进 HLD 免得踩）**：① **不读** `pip.conf` / `PIP_INDEX_URL`（改用 `UV_INDEX_URL` / `uv.toml`）；② **不支持 `--user`**；③ **默认不做字节码编译**（需 `--compile-bytecode` 或 `UV_COMPILE_BYTECODE=1`，官方建议 Docker 构建里开）。

**对照 pip / poetry / pdm / conda**（只按已核查的一手差异陈述）：
- vs **pip**：uv 多出 lockfile + 自动同步；少 `--user`、少 pip 配置文件兼容。
- vs **poetry / pdm**：差异集中在「Rust 单二进制、无 Python 依赖、`uv run` 自动锁同步」，与 lockfile 的跨平台性；**poetry/pdm 的 lockfile 语义本文件未核查**，不作比较。
- vs **conda**：本仓 Python 侧依赖全部是 PyPI wheel（Faker 纯 py、confluent-kafka 有 win/manylinux wheel），**未发现需要 conda 通道的场景**；本机的 `F:\Python`（Anaconda 环境）只是宿主，不应被当作项目运行时。

**A/B/C**：A=官方文档可检索、键名确切（`--locked`/`--frozen`/`--no-sync`/`--check`、`UV_INDEX_URL`），退出码体系与 lockfile 门禁明确；B=uv.exe **实测 42,417,456 B**，缓存当前 0 MB，**官方未给出内存占用数字**；C=见下。

---

## 3. 静态检查与格式化：Ruff

| 核查点 | 官方口径（一手） | 结论 |
| --- | --- | --- |
| 替代 flake8/isort/black 的哪些部分 | Linter 是 **flake8（+数十插件）、isort、pydocstyle、pyupgrade、autoflake** 的 drop-in；Formatter 是 **Black 的 drop-in replacement**，官方原文「When run over extensive Black-formatted projects like Django and Zulip, **> 99.9% of lines are formatted identically**」 | lint+format **一体**，但分工不同 |
| 规则集规模 | 官方 README：**Over 900 built-in rules**，每条规则都是 Rust 一方实现（非插件转发） | 覆盖面明确 |
| `ruff format` 与 Black 兼容性声明 | 遵守 **Black 的 stable 代码风格**；有意的 deviance 单列在 `formatter/black/`；preview style 走 Ruff 自己的 `preview` 开关 | 兼容性有**书面声明**，非口头 |
| 官方明载的坑 | ① **formatter 不排序 import**——官方给的组合是 `ruff check --select I --fix` **然后** `ruff format`；② 一串与 formatter 冲突的 lint 规则（`W191`/`E111`/`E114`/`E117`/`D203`/`D206`/`D300`/`Q000`–`Q004`/`COM812`/`COM819`）默认不开，**开了要显式 ignore**；③ `E501` 与 formatter 只能 best-effort 并存 | 配置项必须照抄，否则 lint 与 format 互殴 |
| 退出码（门禁可用性） | `ruff check`：**0** 无违规 / **1** 有违规 / **2** 配置或内部错误；`ruff format --check`：**0** 无需格式化 / **1** 有文件待格式化 / **2** 异常。官方注明「mirrors that of tools like ESLint, Prettier, and RuboCop」 | ✅ 退出码约定清晰 |
| 版本/许可 | **0.17.1**（PyPI），`requires_python >=3.7`，**MIT**，官方标注「Python 3.15 compatibility」 | 宽松许可，符合本仓口径 |

**对照 mypy / pyright**：二者是**类型检查器**，与 Ruff 的 lint+format **不是替代关系**——Ruff 不检查类型。所以「Ruff 取代谁」不包含 mypy/pyright；若本仓要一道类型门禁，请看 §4。
**A/B/C**：A=配置走 `pyproject.toml`/`ruff.toml`、规则码与 `noqa`/`ruff: ignore`/范围抑制均有确切语法，退出码约定齐全 → **agent 友好度高**；B=**官方未给出**二进制体积（未实测）；C=默认 `target-version = "py311"` 与 `line-length = 88`（同 Black），对「Python 3.11+」口径天然对齐。

---

## 4. 类型检查 `ty`（Astral）——**点名候选，结论必须有**

**一手成熟度证据（这是本节的核心）**：

| 证据 | 原文 / 实测 | 出处 |
| --- | --- | --- |
| PyPI 分类器 | `Development Status :: 4 - Beta` | PyPI `ty` |
| README 自述 | 「ty is currently in **beta**」 | `astral-sh/ty` README |
| 版本政策 | 「ty uses **`0.0.x`** versioning. ty **does not yet have a stable API**; **breaking changes, including changes to diagnostics, may occur between any two versions**」 | README §Version policy |
| Stable 里程碑 | milestone **#4 "Stable"**，**仍 open**；`open_issues=103`、`closed_issues=729`（update 2026-10-16T11:23:15Z） | GitHub API `/repos/astral-sh/ty/milestones` |
| 当前版本 | **0.0.89**（2026-10-16 发布）；MIT | PyPI |
| 支持的 Python 版本 | 官方：**type checking 目标 3.10+**；3.7–3.9 仍可选但「may result in false negatives or false positives due to a lack of bundled standard library stubs」。**目标版本与装 ty 用的 Python 版本无关**，独立安装器甚至不需要 Python | README FAQ |
| 与 uv/Ruff 集成 | `uv add --dev ty` → `uv run ty`；或 `uvx ty`、`uv tool install ty@latest`；`uv lock --upgrade-package ty`。Ruff 侧官方给「接近 strict」的组合配置：`[tool.ty.rules]` + `[tool.ruff.lint] extend-select = ["ANN","PYI"]` + `preview = true` | ty 安装页 / coming-from-mypy-or-pyright |
| 官方给出的局限 | 迁移页逐条列「**None yet**（tracked in #xxxx）」的缺失检查（如 `reportImportCycles`、`reportMissingTypeStubs`、`reportMatchNotExhaustive`、`reportUnknown*` 系列…），并注明「several checks in mypy and pyright are **not yet implemented in ty**」 | ty 迁移页 |
| 退出码 | **0** 无 `warning` 及以上违规 / **1** 有 / **2** 参数或配置错、IO 错 / **101** 内部错误；`--exit-zero`、`--error-on-warning`、`--exit-zero-on-warning`（后者与前两者**不可同时给**） | ty Exit codes |

**结论（一手）：现在不能进 CI/门禁 ✅❌ → ❌**
**理由不是「它是 beta」这种印象话，而是官方自述里的那一句**：诊断结果**允许在任意两个版本之间变更**。门禁的语义是「同一份代码，同样的结论」；一个会随版本升级自行改判的门禁不是门禁，会把红绿变成噪声。里程碑 #4 未关（103 项未清）也说明官方自己没把它当稳定 API。

**可接受的中间态（推荐）**：
- **可以引入，但只能非阻塞**：`uv run ty check --exit-zero`（或 CI 里的 `continue-on-error`），并且**把 ty pin 到精确 0.0.x 版本**（否则非阻塞也会变成噪声）。
- **仍然需要一个能阻塞的类型门禁的话**，用 mypy 或 pyright 承担；ty 在 Stable 里程碑关闭后再升级为阻塞项。
- 与 Ruff 的分工：Ruff 管 **lint + format**，ty/mypy 管**类型**，两者**不重叠**。

---

## 5. Kafka 客户端：confluent-kafka vs kafka-python(-ng) vs aiokafka

**200→1000 ev/s 的适配性**：三者在**吞吐上都不构成瓶颈**（本仓尖峰 ≥1000 ev/s，单条 ~500 B ⇒ 约 0.5 MB/s）。官方**未给出**任一客户端的绝对吞吐数字，本文件**不填估算值**。真正的分水岭在**安装形态、错误可机读性、幂等/`acks` 语义**。

| 维度 | **confluent-kafka 2.16.0**（推荐） | kafka-python-ng 2.2.3 | aiokafka |
| --- | --- | --- | --- |
| 实现 | `librdkafka`（C 库）绑定 | 纯 Python | 纯 Python asyncio |
| 许可 | Apache-2.0 | Apache-2.0 | 未核查（本轮只取官方 README） |
| 最近发布 | **2026-10-07**（2.16.0） | **2024-10-02**（2.2.3）→ **近两年无新版** | 未核查 |
| wheel 形态 | **cp310–cp314（含 cp314t）**，`win_amd64` + `manylinux_2_28_x86_64` 等**预编译** | 仅 `py2.py3-none-any`（**232.8 kB**，纯 Python） | — |
| Windows / WSL 安装 | 两侧都**直接装 wheel、不需要编译器**。manylinux_2_28 需 glibc ≥ 2.28 —— **本机 WSL glibc 实测 2.43，满足** | 任意平台 `any` wheel | — |
| 错误可机读性 | `KafkaError` 暴露 **`.code()`（int）/ `.name()`（str）/ `.str()` / `.retriable()` / `.fatal()` / `.txn_requires_abort()`**；异常分层 `KafkaException` / `ProduceError` / `ConsumeError` / `SerializationError` / `KeyDeserializationError` … | 未核查到等价的错误码契约 | 未核查 |
| 幂等 / `acks` | librdkafka 官方配置表：`enable.idempotence` **默认 `false`**；置 `true` 时**自动**把 `max.in.flight.requests.per.connection=5`、`retries=INT32_MAX`、`acks=all`、`queuing.strategy=fifo`，且「Producer instantation **will fail** if user-supplied configuration is incompatible」。`acks`（=`request.required.acks`）**默认 `-1`(all)**；`transactional.id` **默认未设**，设了才开事务，且「Requires broker version **>= 0.11.0**」 | 未核查 | 未核查 |
| 官方自述定位 | 「built on librdkafka … **significantly outperforming pure Python implementations**」（**厂商自述，非中立第三方**） | PyPI 页面顶部横幅：「**DUE TO ISSUES WITH RELEASES, IT IS SUGGESTED TO USE https://github.com/wbarnha/kafka-python-ng**」 | README：「**Python 3.11 or newer is required**」（与本仓 3.11+ 下限**正好贴合**） |

**补充**：上游 `kafka-python` 文档站自称「compatible with (and tested against) broker versions **4.3 through 0.8.0**」「tested on python **3.8-3.14**」——但该页标题是 **`3.1.0.dev`**（**开发版文档，非已发布版**，标为不确定）；它仍是「零依赖纯 Python」的天然后备。

**推荐：confluent-kafka**。三条决定性理由，全部落在本仓需求上：
1. **安装形态**：有预编译 wheel，Windows 与 WSL 都免编译器 —— 与「干净机器一条命令起链路」（`REQ-F-ORCH-01`）同向。
2. **错误可机读**：`.code()` 是 int、`.name()` 是字符串 —— 直接支撑 `REQ-F-RT-07` 的 **DLQ 分流**与「丢弃必须有痕迹」的硬规则（按错误码分类写 DLQ）。
3. **幂等/`acks` 有书面语义**：生成器为 producer-only，DLQ 写入路径建议 `enable.idempotence=true`。注意本仓 `RF=1`（SRS §A.2）—— 官方 `-1`/`all` 的语义是「等到 **ISR 中所有副本**确认」，RF=1 时 ISR 只有 leader；`min.insync.replicas` 语义也须随 RF=1 一并核对（**本轮未核查 broker 侧默认值**）。
`aiokafka` 的 asyncio 复杂度在 200–1000 ev/s 下**换不到收益**；`kafka-python-ng` 已**近两年停更**且官方自发横幅提示改用 GitHub 源，不宜作为锚点。

---

## 6. 事件 schema 校验：pydantic v2 vs msgspec vs dataclasses+手写

本仓的特殊要求：处置流水线的**四关顺序冻结**（结构与取值 → 时间范围 → 唯一性 → 迟到），且**三类原因必须可区分**（GDD §1.9「GDD 新增：三类原因可区分（结构/取值、时间范围、重复）」）。这直接要求**机读原因码**。

| 维度 | **pydantic v2（推荐）** | msgspec 0.22.0 | dataclasses + 手写 |
| --- | --- | --- | --- |
| 成熟度 | `Development Status :: 5 - Production/Stable` | `Development Status :: 4 - **Beta**` | 标准库，稳定 |
| 版本 / 许可 | **2.14.1**（2026-10-11）；未核查许可表达式（v2.14.0 起移除 3.9、加入 3.15） | **0.22.0**（2026-09-29）；**BSD-3-Clause**；`requires-python >=3.10` | — |
| 错误可机读性 | `ValidationError` 携带**错误对象列表**，每项含 **`type`（机读码，如 `string_type`/`missing`/`literal_error`/`greater_than`）**、`loc`、`msg`、`input`，并附文档 `url`；错误码有**成体系的官方清单** | 官方 PyPI 示例：`msgspec.ValidationError: Expected \`str\`, got \`int\` - at \`$.groups[0]\`` → **单一异常 + 格式化字符串**，非结构化对象列表 | **零运行时校验**：官方 `dataclasses` 文档原话「nothing in `@dataclass` examines the type specified in the variable annotation」→ 错误契约**完全由你自建** |
| 吞吐 | 基准对照见下（msgspec 作者自测） | 同一基准：decode+validate **~12× 快于 pydantic V2**；库体积 **0.46 MiB vs 6.71 MiB（约 15×）**；struct 创建 **0.09 µs vs 1.54 µs** | 最快，但校验要你自己写（写多少就慢多少） |
| 依赖 | 未核查依赖树完整清单 | 官方：**no required dependencies** | 零 |

> 基准出处为 **msgspec 作者自测**（`msgspec.dev/benchmarks`，作者自带声明「I wrote msgspec, naturally whatever benchmark I publish it's going to perform well in」，环境是作者 2020 年 x86 Linux 笔记本 + CPython 3.11）→ **作补充证据，不作中立第三方结论**。且其对照的 pydantic 是 **2.5.2**（非当前 2.14.1）。

**推荐：pydantic v2**。理由是**本仓的需求结构**：`REQ-F-RT-07` + GDD §1.9 要求「三类原因可区分」，而官方**成体系且稳定的机读错误码清单**是三者中唯一现成的；吞吐差异在 200–1000 ev/s（≈0.5 MB/s）量级**不构成判据**。msgspec 在「零依赖 + 吞吐 + 体积」上全面更强，**其 `ValidationError` 是字符串**这一点，会让「原因码」变成自己解析字符串 —— 与本仓的「丢弃必须有痕迹」背道而驰。`dataclasses` 只在「愿意自建整个错误码体系」时成立。
**注**：pydantic-core 的 wheel 矩阵**本轮未核查**，落到 Windows/WSL 前须补一条（已记入未决清单）。

---

## 7. Flink 侧工程栈：JDK 17 发行版 + Maven vs Gradle

| 核查点 | 官方口径（一手，Flink 2.2 release 分支） | 结论 |
| --- | --- | --- |
| **JDK 基线** | `deployment/java_compatibility.md`：「We use **Java 17 by default in Flink 2.0.0** and is **the recommended Java version to run Flink on. This is the default version for docker images**」；**Java 21 为 experimental**（FLINK-33163）。Java 17 的未测项：Hive connector、Hbase 1.x connector | **JDK 17 确认**，与 GDD §A.3 一致 |
| 发行版选择（Temurin 等） | **官方未给出** —— Flink 文档未指定任何 JDK 厂商 | 「未给出」照写；Temurin 只是常见选择，无官方背书 |
| quickstart / archetype 现状 | 官方仍在维护：`mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=2.2.1`；或 `curl https://flink.apache.org/q/quickstart.sh \| bash -s 2.2.1`。**Gradle 无 archetype**，须手写 build script | **Maven 优先** |
| 官方要求（Maven 页） | 「Requirements: **Maven 3.8.6**, Java 11」 | ⚠️ **本机 mvn = 3.6.0 < 3.8.6**，且跑在 **JDK 1.8.0_144** 上 → **当前不满足官方要求** |
| 官方要求（Gradle 页） | 「Requirements: **Gradle 7.x**, Java 8 (deprecated) or Java 11」 | ⚠️ 本机 **Gradle 未装**；且这两页仍写 Java 11 而 `java_compatibility.md` 写 Java 17 → **官方文档内部不一致**（照实标注） |
| connector 打包与 shading 官方做法 | `maven.md` 给出 shade plugin 模板：`artifactSet.excludes` 排 `com.google.code.findbugs:jsr305`；`filters` 排 **`META-INF/*.SF` / `*.DSA` / `*.RSA`**（否则 SecurityException）；`transformers` 用 `ManifestResourceTransformer`（填 `mainClass`）+ `ServicesResourceTransformer`。核心 API 依赖**必须设 `provided` scope**，否则「worst case … clash」（Flink inverted classloading）。替代做法：把外部依赖直接放进发行版 classpath（`/lib`）。`advanced.md` 另警告：planner 两个 jar 不可共存，且官方「strongly suggest」作业改用 API 模块、不依赖 planner 内部 | ✅ 照抄即可 |
| connector 坐标（**关键更正**） | `repo.maven.apache.org` 的 `org.apache.flink:flink-connector-kafka` `maven-metadata.xml`：**`<release>5.0.0-2.2</release>`**，`<latest>5.0.0-2.2</latest>`，可用版本含 **`5.0.0-2.1`、`5.0.0-2.2`**（另有 `4.0.0-2.0`、`4.0.1-2.0`） | ⚠️ **FSR/SRS 冻结的「5.0.0」不是完整坐标**：Flink 2.2.x 对应的是 **`flink-connector-kafka:5.0.0-2.2`**（`-2.2` 是 Flink 线后缀）。HLD 必须写全 |

**推荐：JDK 17 + Maven**。
- JDK 17：有 Flink 官方一手背书（默认 + 推荐 + docker 镜像默认）；发行版**官方未给出**，任选其一但须 pin。
- Maven：官方 quickstart/archetype、shade 模板、connector 下载页**全部是 Maven 坐标形态**；Gradle 虽官方支持，但要手写 build script 且官方要求写 Gradle 7.x（本机未装）。
- **对既有接缝的影响**：GDD §A.3 的「JDK 基线 17」由本文件**以官方原文二次确认**；同时暴露**本机 mvn 3.6.0 + JDK 8 的组合不满足官方要求**，属待授权的安装动作。

---

## 8. 编排工具：GNU Make 的执行面 + just/task 取舍

**冻结项**：SRS §3.4 冻结「**Makefile 目标** `up` / `down` / `init` / `replay` / `verify` / `spike` / `verify-fault` / `logs`」；`REQ-F-ORCH-03` 要求各目标可执行成功。
**本机实测**：Windows 侧 `make` **未装**；WSL Ubuntu 侧 **`make: command not found`** → **当前没有任何 make 可用**。

### 8.1 执行面三选一

| 执行面 | 一手依据 | 判定 |
| --- | --- | --- |
| **Windows 宿主** | GNU make 官方手册 §5.3.2：**Windows/MS-DOS 上 `SHELL` 环境变量「IS used」**（与 Unix 相反，保证 makefile 里的 `SHELL = /bin/sh` 在装了 `sh.exe` 时可用），未设时回落到 **`COMSPEC`**；make 会沿 `PATH` 搜 `sh`、`.exe/.com/.bat/.btm/.sh` 等 | ❌ 不推荐：需新装 GNU make，且把 **shell 语义分叉**（`COMSPEC` 兜底 / 路径与 `.PHONY` 语义差异）引进来，与冻结目标名的可复现诉求相悖 |
| **WSL2（Ubuntu，容器外）** | Ubuntu 26.04 + glibc 2.43 实测；docker CLI 需在 Docker Desktop 开启该 distro 的 WSL integration（**当前未开启**） | ✅ **推荐** |
| **容器内** | — | ❌ 不采纳：会引入一个**额外容器**，与「**不引入调度器容器**」（FSR §1 非目标 / SRS C4）以及「常驻 ≤8 GB、逐服务 `mem_limit`」（`REQ-F-ORCH-02`）口径正面冲突 |

**推荐理由（WSL2 侧）**：① 仓库的 compose/脚本面本就是 Linux 语义；② 避开 Windows 上 `SHELL`/`COMSPEC` 分叉（GNU make 官方明说 Windows 的行为与 Unix 不同）；③ **避开 R6** —— 不是「把仓库 bind mount 进容器」，而是「在 WSL 侧执行 make、由 make 去调 `docker compose`」，仓库目录不挂进容器，绕开「Windows 文件系统挂载 performance 远低于 Linux 且 inotify 不投递」这条陷阱；④ 不新增常驻容器，天花板预算不动。

### 8.2 just / task 取舍

| 工具 | 一手事实 | 判定 |
| --- | --- | --- |
| **just** | 官方自述「**`just` is a command runner, not a build system**, so it avoids much of make's complexity and idiosyncrasies. **No need for `.PHONY` recipes!**」；「Linux, macOS, Windows, and other reasonable unixes are supported with **no additional dependencies**. (Although if your system doesn't have an `sh`, you'll need to choose a different shell.)」；安装方式为包管理器 / 预编译二进制 / `cargo install just`。静态解析错误、`.env` 加载、`just --list` 等 | ❌ 不采纳 |
| **task（Taskfile）** | 官方安装页：Windows 可 `winget install Task.Task`、`npm install -g @go-task/cli`、`choco install go-task`、`scoop install task`、`pip install go-task-bin`；亦有 install script 与二进制 | ❌ 不采纳 |

**不采纳的理由就一条，但是硬理由**：冻结项冻结的是「**Makefile 目标**」这个**载具 + 名字**的组合。换 just/task 会把调用语法从 `make up` 变成 `just up` / `task up` —— 名字能一对一映射保住（`up`/`down`/`init`/…），但**载具换了，等于冻结项漂移**。若要换，须先走**冻结项变更**，不能在 HLD 里单方面改。
**但建议记录其优势**：just/task 的 `--list` 类机器可读目标清单、静态错误检查，在 **agent 友好度（A 轴）**上确实优于 make（make 没有等价的官方目标枚举能力）。这属于「已知更好但不换」的条目，写进 HLD 作为将来复核的挂点。

---

## 9. 与既有接缝的逐条对照

| 接缝 | 对照结果 |
| --- | --- |
| FSR §4 版本锚点矩阵 | **不冲突、不改版本**；但补一条精确化：connector 完整坐标是 `flink-connector-kafka:5.0.0-2.2` |
| SRS §3.4 冻结项（topic / 字段 / Paimon 表 / Doris 库 / **Makefile 目标** / 端口） | 本文件**未建议改动任何冻结值**；just/task 的结论正是「为保冻结值而不换」 |
| SRS §2.4 C3「锚点精确 pin，禁止 latest」 | 强化：Python 线须 pin **3.12**（而非跟随宿主 3.13/3.14）；Faker 须 pin 到**补丁号**（官方 seeding 明确要求）；ty 若引入须 pin 精确 `0.0.x` |
| Python 3.11+ 口径 | **满足**（3.12 > 3.11）；本机现有 3.13/3.14 均为**环境缺省**，不构成 pin |
| Windows 宿主 + WSL2 | uv **Tier 1 同时覆盖** Windows(x86_64) 与 Linux(x86_64)；confluent-kafka 两侧均有**预编译 wheel**；WSL 侧 glibc 2.43 **满足** manylinux_2_28 |
| **R6**（bind mount 跨 Windows 文件系统掉性能） | 编排面按 §8.1 选 WSL2 侧执行，**不把仓库挂进容器**；uv 的 cache/`.venv` 与 JDK/Maven 的落盘位置需**显式指定盘符**（C 盘仅剩 9.5 GB） |
| SRS C5「仓库零原始数据」 | uv 官方明载 `.venv` 由内部 `.gitignore` 自动排除；`uv.lock` 官方要求**入库**——二者都不是原始数据，与 C5 不冲突 |
| `REQ-F-GEN-06`（同种子同哈希） | **风险点**：Faker 官方明确「results are not guaranteed to be consistent across patch versions」→ 必须把 Faker pin 到补丁号 |
| GDD §A.3「JDK 基线 17」 | **二次确认**（Flink 2.2 官方原文：Java 17 为默认/推荐）；同时暴露本机 mvn 3.6.0 + JDK 8 不满足官方要求 |
| GDD §1.9 / `REQ-F-RT-07`（三类原因可区分） | **决定了 §6 的选型**：pydantic v2 的机读错误码清单是唯一现成的结构化错误来源 |

---

## 10. 落地动作清单（**待用户单独授权；本文件未执行任何一条**）

| # | 动作 | 类别 | 依据 | 回滚方式 |
| --- | --- | --- | --- | --- |
| T1 | 把 `C:\Users\Administrator\.local\bin` 加入 Windows `PATH`，启用**已装的 uv 0.12.19** | **改配置** | uv.exe 已存在（42,417,456 B），仅 PATH 缺失 | 从 PATH 移除该目录即完全回滚 |
| T2 | WSL 侧安装 uv（`install.sh`）或 Windows 侧升到 0.13.1 | 安装 | 官方安装页 | 删除 `~/.local/bin/uv*` + `uv cache clean` |
| T3 | 安装 **JDK 17**（Windows 与/或 WSL），并 `JAVA_HOME` 指过去 | 安装 | Flink 2.2 `java_compatibility.md` | 还原 `JAVA_HOME` 原值（现 `E:\space\09-tools\Jdk1.8`） |
| T4 | 安装 **Maven ≥ 3.8.6**（现为 3.6.0，且跑在 JDK 8 上） | 安装 | Flink `maven.md`「Requirements: Maven 3.8.6, Java 11」 | 还原 `M2_HOME`/PATH 到 `Maven3.6` |
| T5 | WSL 侧 `apt install make` | 安装 | §8.1 推荐执行面 | `apt remove make` |
| T6 | 在 Docker Desktop 开启 **Ubuntu** 的 WSL integration（现 distro 内无 docker） | **改配置** | 本机实测 WSL 内 `docker` 不存在 | 在同一设置页关闭该 distro 的集成 |
| T7 | `uv python install 3.12` 并写进 `pyproject.toml` 的 requires-python | 安装 | §1 推荐 | `uv python uninstall 3.12` |
| T8 | 安装 ruff（`uv tool install ruff` 或 dev 依赖） | 安装 | §3 | `uv tool uninstall ruff` |
| T9 | 安装 ty（**仅非阻塞** `--exit-zero`，pin 精确版本） | 安装 | §4 结论 | `uv tool uninstall ty` |
| T10 | 把 uv cache / JDK / Maven 的落盘改向非 C 盘 | **改配置** | C 盘仅剩 9.50 GB | 移除 `UV_CACHE_DIR` 等环境变量 |

---

## 未决 / 官方未给出 / 不确定

**官方未给出（不得用估算值代替）**
1. uv / ruff / ty / confluent-kafka 的**二进制体积与内存占用**：官方均未给出（本文件只写实测的 uv.exe 42,417,456 B 与 uv 缓存 0 MB）。
2. Flink 对 **JDK 发行版（Temurin 等）**的选择：官方文档未指定任何厂商。
3. 各 Kafka 客户端的**绝对吞吐数字**：官方未给出，本文件不填。
4. Python 各线在本仓规模下的**内存/磁盘占用**：官方未给出。
5. Temurin 的 JDK 17 发布清单：`adoptium.net/temurin/releases` 在带查询参数抓取时返回「No releases found」（页面为动态渲染）→ **未能取到可信版本清单**，标为未核查。

**不确定 / 本轮未核查（落到实现前须补）**
6. **pydantic-core 的 wheel 矩阵**（本文件只核查了 msgspec 与 confluent-kafka 的 wheel）。
7. **aiokafka 的当前版本 / 许可 / 错误契约**：PyPI 页面被反爬（Client Challenge）拦截，只取到官方 README 的「Python 3.11 or newer is required」。
8. **kafka-python 3.1.0 是否已正式发布**：取到的兼容性页标题是 `3.1.0.dev`（开发版文档）。
9. **官方文档内部不一致**：Flink `maven.md`/`gradle.md` 的 Java 要求写 11，而 `java_compatibility.md` 写 Java 17 为 Flink 2.0+ 默认/推荐 —— 以 `java_compatibility.md`（部署篇、更权威）为准，但差异本身应记录。
10. **`acks=all` 在 `RF=1` 下的精确语义**（含 broker 侧 `min.insync.replicas` 默认值）：本轮未核查 Kafka broker 配置文档。
11. **`uv` 0.12.19（本机）与 0.13.1（官方文档示例）之间的差异**：未核对 changelog。
12. **Ruff 与 ty 的版本组合是否被官方声明为互操作**：官方只给了**配置示例**（`[tool.ty.rules]` + `[tool.ruff.lint]`），未给出兼容性保证。
13. **本仓磁盘 ≤20 GB 口径**：C 盘实测仅剩 **9.50 GB**，T10 若要落地，须先确认所选盘符的剩余空间是否够（F: 77.6 GB / G: 74.1 GB 空间充裕）。
14. **截止日期提醒**：以上均为 **2026-10-19** 时点的一手核查；`ty`（0.0.89，Stable 里程碑开放中）、uv（0.12.19 → 0.13.1）、Ruff（0.17.1）、pydantic（2.14.1）、msgspec（0.22.0）、confluent-kafka（2.16.0）、task（v3.54.0）等版本**已于本文件之后继续变动**，复用时须重新核对。
