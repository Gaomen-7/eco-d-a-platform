# Spec — 仓库骨架搭建（repo-skeleton）

Status: ready-for-agent
Created: 2026-10-21
决策来源：骨架 grill 会话八问全按推荐（2026-10-21 用户裁决）＋ [ADR-0001](../../docs/adr/0001-uv-lockfile-is-toolchain-authority.md) ＋ [ADR-0002](../../docs/adr/0002-flink-2-2-1-patch-anchor.md)；可执行细节以 `.scratch/handoff/20261021-090134-620-skeleton-decisions.md` §3 施工清单为准。

## Problem Statement

仓库已有 P0 **骨架**（容器群编排、配置断言、Doris 自建镜像、`make verify` 五项烟测），但工程面残缺：没有 Python 工程（无 `pyproject.toml`/锁文件/`.venv`/生成器包），没有 Flink Maven 工程，没有 README（D-I① 登记在案的缺陷），没有 `sql/` 占位。工具链虽已体检可用，但只有版本号证明、没有门禁闭环的行为证明。骨架的八项决定已经裁决并落了 ADR，却全部未执行——下一个施工会话进场时没有可落的落点。

## Solution

按已决事项完成仓库骨架：仓库根建 uv 管理的 Python 工程（src 布局、包 `ecomgen`、依赖精确 pin、锁文件入库）、Makefile 门禁目标（`lint`/`typecheck`）切到 `uv run`、Flink 工程 pom 空壳（锚点 2.2.1）、最小 README（`make init` 一条命令）、`sql/` 占位、运行产物目录入 gitignore。验收＝四条接缝（S1–S4）全部以退出码/构建结果判定通过；不拉容器、不跑烟测（容器面 P0 已验收，属「能起来 ≠ 能重起来」教训里已收口的部分）。

## User Stories

1. 作为平台工程师，我想在仓库根有一个 uv 管理的 Python 工程，以便生成器与脚本有唯一的环境与依赖入口。
2. 作为平台工程师，我想所有依赖（含门禁工具）在锁文件里精确 pin 并入库，以便任何机器 sync 出同一套版本——可复现主体是制品，不是平台。
3. 作为平台工程师，我想 `requires-python` 钉在 3.12 线，以便与宿主 Python（3.14）解耦、锚点不随机器漂移。
4. 作为平台工程师，我想包名定为 `ecomgen`，以便模块①（数据生成器）有唯一可 import 的名字。
5. 作为施工 agent，我想 `make lint` / `make typecheck` 经 `uv run` 调用，以便门禁用的就是锁文件里的工具版本，不依赖机器上恰好装了什么（ADR-0001）。
6. 作为施工 agent，我想 `uv sync` 一条命令重建整个环境，以便换机或回档后 Python 面可复原（复现档位 L1 的工程面）。
7. 作为施工 agent，我想 `uv lock --check` 通过，以便锁文件与工程声明永远一致，没有「lock 过期」的隐性漂移。
8. 作为平台工程师，我想 Faker 钉到补丁号，以便「同种子同哈希」（确定性）的前提成立。
9. 作为平台工程师，我想类型检查档位显式 `standard`，以便 fork 的默认档不会随版本静默改判。
10. 作为平台工程师，我想不另建独立的 pyright 配置文件（工程内配置段优先被它覆盖），以便类型检查配置只有一处真相。
11. 作为平台工程师，我想 ruff 与类型检查按职责划界（未用 import 等归 ruff 独占），以便一个含义只有一个所有者。
12. 作为施工 agent，我想 Flink 工程目录有 Maven 骨架（compiler release 17、shade 排除规则齐全、零 Java 源码），以便 P1 写作业时只加代码、不再搭架子。
13. 作为施工 agent，我想空壳 `mvn -q package` 能 BUILD SUCCESS，以便 JDK17+Maven 工具链有行为证明而非版本号证明。
14. 作为平台工程师，我想 Flink 依赖锚在 2.2.1 线与 connector `5.0.0-2.2`，以便避开 2.3.0 无 Kafka connector 的断链（ADR-0002）。
15. 作为新贡献者/评审者，我想读 README 就知道一条命令是 `make init`（四段冻结）与八个冻结目标名，以便零上下文进场并核对冻结面没漂移（修 D-I①）。
16. 作为平台工程师，我想 `sql/` 有占位说明指向五层形状表，以便 P2 的 DDL 有明确落点、不被误读为遗漏。
17. 作为平台工程师，我想运行产物目录（verify 报告）进 gitignore，以便仓库保持零数据产物、零密钥入库。
18. 作为施工 agent，我想验收判据全部是退出码与构建结果，以便无人值守会话能自判成败——「配置可见」「UI 里能看到」不是判据。
19. 作为平台工程师，我想骨架验收不拉容器不跑烟测，以便不与 P0 已验收的容器面重复、验收在分钟级完成。
20. 作为平台工程师，我想 `docker compose config` 校验仍通过，以便骨架改动没碰坏 P0 定下的编排契约。
21. 作为未来维护者，我想工具链与版本决定都有 ADR 与术语表可查，以便「为什么是 2.2.1」「为什么走 uv run」不必重新考古。

## Implementation Decisions

- **Python 工程**：仓库根单 `pyproject.toml`；src 布局；包名 `ecomgen`（本 spec 只建空包，console 入口留 P1 定）；`requires-python = ">=3.12,<3.13"`。
- **依赖锚点**：运行组 `confluent-kafka` / `pydantic` / `faker`（必须补丁号）；门禁组（dev）`ruff` / `basedpyright`；全部 `==` 精确 pin；锁文件入库；**工具链版本的唯一权威是锁文件**（ADR-0001）。
- **工具调用**：Make 目标 `lint`/`typecheck` 从裸调用户级二进制改为 `uv run`；用户级独立安装只作引导，不作为版本依据。
- **类型检查配置**：工程内配置段、`standard` 档、pythonVersion 3.12、venv 指仓库内 `.venv`；不建独立 pyright 配置文件（它总是优先，会盖掉工程内声明）。
- **lint 分工**：import 排序与格式化按 ruff 官方组合纪律（先 `check --select I --fix` 再 `format`）；与类型检查职责交叉的规则显式关给 ruff 独占。
- **include 范围**：类型检查初始只含新包源码；P0 遗留脚本只做 autofix 级清理，语义问题升级问人、不静默重写。
- **Flink 工程**：pom 骨架——`flink-streaming-java` 2.2.1（provided）、`flink-connector-kafka` `5.0.0-2.2`、`flink-doris-connector` 26.3.0（坐标落盘前在 Central 逐条核实）；shade 排 `jsr305` 与签名文件；核心 API 一律 provided；空 `src/main/{java,resources}` 目录，零 Java 源码。
- **镜像核对**：compose 现用 2.2 线浮动 tag，落盘时核对镜像实际补丁＝2.2.1（`flink --version`），不一致以 ADR-0002 为准。
- **README**：≤60 行——一句话定位、前置（daemon/WSL2）、`make init` 一条命令、八冻结目标表、设计链指针、本地提交不推送纪律。
- **`sql/` 占位**：一行指针至地图 D9 五层形状表；DDL 本体归 P2。
- **gitignore**：增运行产物目录（verify 报告落点）。
- **冻结面不动**：八个 make 目标名、端口、库名、topic 名一概不改；上游四文件（SRS/gdd/HLD/init.sql）一字不动（§⑦ 修订归管线 spec 会话）。

## Testing Decisions

- **好测试只测外部行为**：退出码与构建结果。不测 ruff/basedpyright/uv/mvn 的内部；不为骨架新写任何测试代码。
- **接缝四条**：S1 Make 目标退出码（既有、最高层：`make lint`/`make typecheck`/`make config-check` 全 exit 0）；S2 uv 命令面（唯一新增接缝：`uv sync` 幂等、`uv lock --check` 一致、`uv run python -c "import ecomgen"` 冒烟）；S3 Maven 空壳构建（`mvn -q package` BUILD SUCCESS）；S4 `docker compose config -q`（免 daemon）。
- **Prior art**：P0 的 config-check 脚本断言（退出码判成败）与 verify 的 PASS/FAIL 行风格——骨架验收沿用同一「stdout 行＋退出码」形态，施工会话在交付回复里贴实机输出为证。
- **执行面**：全部在 WSL2 内执行（D3）；不需要拉起 Docker daemon。

## Out of Scope

- P1 语义面：PI 圈表、verify 分层断言、生成器任何功能代码、Flink 作业代码。
- `.scratch/ecom-user-behavior-pipeline/spec.md`——管线九节施工规格，归专门会话；与本 spec 同名不同物，互不取代。
- spec §⑦ 上游文件修订（SRS/gdd/HLD/init.sql 的逐字改写）。
- 三张须先获批的实机 task（HOUR 演练 / DROP 回盘实测 / compaction 对撞）。
- 容器拉起与 `make verify`（P0 已验收；L2「不删卷重启」判据属 Doris 收口，已达成）。
- 新增任何 make 目标名（如把 mvn 折进编排）——不在八项已决内，将来走设计面变更再议。
- GLOSSARY 与 ADR 的再修订——本 spec 消费它们，不改它们。

## Further Notes

- 施工时注意（handoff §4/§5）：daemon 默认不在跑（本 spec 验收不需要它）；`.env` 是机密面不碰；回退对话残留的孤儿 `uv.lock`/`.venv` 已删净，一切从零生成、不存在可复用的现成锁。
- 术语用词遵循 GLOSSARY：骨架 / 锚点 / 唯一权威 / 可复现主体 / 复现档位 / 烟测。
- 完成后按仓库纪律本地 commit（`feat(skeleton)` 风格）、绝不推送。
