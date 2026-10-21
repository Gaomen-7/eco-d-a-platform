# 03: 仓库收尾与全量验收（S1–S4 闸门）

**What to build:** 骨架的最后一段垂面：README（≤60 行——一句话定位、前置 daemon/WSL2、`make init` 一条命令四段冻结、八冻结目标表、设计链指针、本地提交不推送纪律，修 D-I①）＋ `sql/` 占位（一行指针至地图 D9 五层形状表，DDL 归 P2）＋ gitignore 增运行产物目录（verify 报告落点）。然后 **S1–S4 全量重跑**并贴实机输出：`make lint`/`make typecheck`/`make config-check` 全 exit 0、uv 三连（sync/`lock --check`/import 冒烟）、`mvn -q package` BUILD SUCCESS、`docker compose config -q` 通过（免 daemon）。全绿的承诺在这张票兑现——它是 integrate-and-verify 切片。

**Blocked by:** 01, 02.

**Status:** resolved

- [x] README ≤60 行且含八冻结目标表与 `make init` 四段说明——**59 行**
- [x] `sql/` 占位指针在位
- [x] gitignore 含运行产物目录（`/artifacts/`）
- [x] S1–S4 全量重跑，实机输出贴进交付回复（沿 P0「stdout 行＋退出码」形态）
- [x] 收尾本地 commit（`feat(skeleton)` 风格）、未推送；`git status` 干净

## Answer

**落盘**：`README.md`（59 行）、`sql/README.md`（7 行）、`.gitignore` 增 `/artifacts/`（commit `5bb4445`）；评审后两轮修正（`fb03890`、`2249c09`）。

**README 内容对照**（spec 要求的六项全在）：一句话定位 / 前置（Docker daemon ＋ WSL2）/ `make init` 四段冻结（①②③④ 逐段列出）/ 八个冻结目标名逐字（`up` `down` `init` `replay` `verify` `spike` `verify-fault` `logs`）/ 设计链指针（`map.md` 为 wayfinder 入口，明示其优先于 HLD §0.1）/ 本地提交不推送纪律。**修 D-I①**（「仓库无 README」登记缺陷）。

**诚实声明（评审两轮修正后）**：

1. `make init` **只有 ① 接线**；②（`--profile doris`）③ ④ 均未接——Makefile 的 `init` 依赖的 `up` 只跑默认 profile，全文无 `--profile doris`。"待补" 提示本身也没提到 ② 缺的那一步。
2. `verify` 的五项里 **3/5（Paimon）与 4/5（Doris）当前只打 `PENDING`、不置 `fail`** ⇒ 只有 1/5、2/5、5/5 三项真正 gate 退出码。
3. L0 判据里的 "全 `healthy`" 不可满足：只有 `kafka`/`flink-jm`/`doris-fe`/`doris-be`/`superset` 定义 healthcheck，`flink-tm`/`kafka-init`/`client` 无 healthcheck，只能 `Up`。
4. `artifacts/` 是 **设计落点**（D10⑨），当前**没有任何目标写入**，P1+ 的 verify 报告才落此处。

**端口清单（REQ-NF-MNT-03）**：八个已发布回环端口全部列全并注用途——`8030` Doris FE HTTP、`8040` Doris BE webserver、`8081` Flink UI、`8088` Superset、`9010` Doris FE edit_log、`9030` Doris FE MySQL 协议、`9060` Doris BE brpc、`9092` Kafka。

**`sql/README.md`**：指针至 `.scratch/ecom-user-behavior-pipeline/issues/05-offline-layer-scope.md` §「下半 · 五层形状」；明确本目录暂无 DDL、DDL 归 P2。

### S1–S4 全量实测（WSL2，合并后终跑）

| 接缝 | 命令 | 退出码 | 实测输出 |
| --- | --- | --- | --- |
| S1 | `make lint` | **0** | `All checks passed!` / `47 files already formatted` / `lint: PASS` |
| S1 | `make typecheck` | **0** | `errors=0 warnings=0 files=1` / `typecheck: PASS` |
| S1 | `make config-check` | **0** | 常驻 5.25 GiB / 天花板 11.68 GiB / 余量 6.43 GiB / PASS |
| S2 | `uv sync`（删 `.venv` 冷跑） | **0** | `Installed 11 packages in 51.77s` |
| S2 | `uv sync`（二次） | **0** | `Checked 11 packages in 40ms`，包集合 diff 空 ⇒ 幂等 |
| S2 | `uv lock --check` | **0** | `Resolved 12 packages` |
| S2 | `uv run python -c "import ecomgen"` | **0** | 无输出即成功 |
| S3 | `cd flink && mvn -q package` | **0** | `BUILD SUCCESS`，产物 2 个 jar |
| S4 | `docker compose config -q` | **0** | 静默通过 |

**未跑**（spec 明确排除）：`make verify`、任何容器拉起/镜像拉取。**`git status` 干净，未推送。**

### 评审发现项与处置（两轮，均已闭合）

第一轮（Lead 自查）：
1. README 原称「①② 已接」不实（② 未接）→ `fb03890` 修正为仅 ①。
2. 端口清单只列 5 个、漏 `8040`/`9010`/`9060` → `fb03890` 补全八个。

第二轮（dual-axis code review）：
3. `pyproject.toml` 的 `hatchling` 未 pin → `2249c09` 钉 `hatchling==1.32.4`（它属 PEP 517 构建环境，不进 lock 的运行时图，故 `uv.lock` 中仍无 —— 确定性来自 pyproject 的 `==`）。
4. `flink/pom.xml` 的 `slf4j-api:1.7.36` 无锚点来源 → `2249c09` 删除，改由 `flink-streaming-java` 传递提供（`runtime` 作用域仍在编译类路径；shaded 产物条目集 31,594 不变）。
5. `verify` 行过度声明、`healthy` 判据不可满足、`artifacts/` 以现在时陈述 → `2249c09` 三处改为诚实表述（见上「诚实声明」）。
