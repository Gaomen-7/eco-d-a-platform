# HLD — 电商用户行为数据全链路（离线 + 实时）

> **性质**：高层设计文档（High-Level Design）。本文回答「**结构怎么分、模块各管什么、技术栈定在哪些版本、运行环境与沙箱有什么约束、最可能在哪里出架构级问题**」。
> **日期**：2026-10-19（Asia/Shanghai）
> **上游**：`SRS.md`（需求权威）｜`FSR.md`（路线冻结）｜`GDD.md`（功能设计，文件名 `gdd.md`）｜`GLOSSARY.md`（术语）
> **下游**：`spec.md`（实施规格，另行会话产出）
> **权威关系**：`SRS.md` 持「**必须做到什么**」→ `gdd.md` 持「**每个功能怎么做**」→ 本文持「**结构、栈定案、运行约束**」。本文**不覆盖**上游，凡与上游冲突处一律在 §7 显式登记，**不修改** `SRS.md` 与 `FSR.md`（`gdd.md` 中被本文调研证伪的条目已就地修订，见 §7）。
> **术语**：一律以仓库根 `GLOSSARY.md` 为准。

## 0. 这份文档怎么用

### 0.1 读者分支表

| 你要做的事 | 读哪节 |
| --- | --- |
| 知道系统边界与外部依赖 | §1 系统上下文 |
| 确认谁负责什么、接缝在哪 | §2 分层与模块职责 |
| 确认技术栈定在了什么版本、为什么否掉别的 | §3 技术栈定案表 |
| 本地起链路、写 compose、定资源上限 | §4 运行环境与沙箱约束 |
| 改名/加字段前确认哪些名字碰不得 | §5 数据契约（冻结面） |
| 让某个失败模式变成可判定断言 | §6 失败路径与可机读断言 |
| 发现文档自相矛盾 / 与上游不一致 | §7 偏离与待收口 |
| 评估「这东西会不会根本跑不起来」 | §8 架构风险 |
| 追溯每条结论的一手来源 | §9 上游证据 |

### 0.2 权威链与「本文不做什么」

```
SRS.md   必须做到什么（需求权威）
   │
gdd.md   每个功能怎么做（触发/流程/状态/判据）
   │
HLD.md   结构 / 栈定案 / 运行约束        ← 本文
   │
spec.md  实施规格（另行会话）
```

**本文不做三件事**：①不重新声明需求（那是 `SRS.md`）；②不复制功能流程（那是 `gdd.md`，本文只指针）；③不为「文档说错了」开票——只有需要真正施工动作时才开票（沿用 `gdd.md` §D 的既定政策）。

### 0.3 参数性质边界（**读本文前必须记住的一条**）

本文与全部上游文档里的每个数字，只有两种性质，**严禁混淆**：

| 标记 | 含义 | 例子 |
| --- | --- | --- |
| **`官方给出`** | 有官方文档 / 源码 / 配置文件的逐字出处 | Doris FE 出厂默认 `-Xmx8192m`（`fe.conf`）；Paimon `snapshot.num-retained.min` 默认 10 |
| **`设计值`** | 官方**没有**这个数字，是本项目自定的目标 | 常驻容器群预算；Doris 的 `-Xmx1536m`（低于官方 dev/test 口径）；Paimon 磁盘 ≤20 GB |

**本仓历史上最大的坑正是把设计值当官方保证**：`FSR.md` §2 写「官方未给出 Doris 单机最低内存」，而官方**给出了**（dev/test FE 8 GB + BE 16 GB），且与本项目的 ~3 GB 设计值正面冲突。见 §7 与 §8-R1。
**纪律**：本文出现的每个数字都带 `官方给出` / `设计值` 标记；不带标记的数字视为缺陷。

### 0.4 行为可证纪律（**约束本文自身**）

`gdd.md` 已把「配置可见」「UI 里能看到」判为**不合格判据**。本文把该纪律升为仓库级规范，并**同样约束本文**：

> **HLD 写下的每条约束，必须附「怎么检查、判定条件是什么」。没有检查方式的约束不许写进本文。**

因此 §4 的每条约束都带**检查方式**列，§8 的每条风险都带**检测信号**列。

---

## 1. 系统上下文

### 1.1 系统边界

**是什么**：单机、一条命令可复现的电商用户行为数据全链路——行为事件生成 → Kafka → Flink 实时计算 → Doris 供数 + Superset 看板；同一批数据经 Flink 批模式落 Paimon 五层离线数仓。

**边界外（明确不做，理由本身是面试素材）**：多节点/高可用集群、K8s、调度器容器、监控告警体系、安全权限体系、真实支付对接、生产级可观测性。`SRS.md` §6.1 为权威。

### 1.2 外部依赖（全部为「本机之外」的东西）

| 依赖 | 形态 | 备注 |
| --- | --- | --- |
| Docker Desktop（WSL2 后端） | 宿主已装，**daemon 需先启动** | `SRS.md` A1 已确认可用 |
| WSL2 发行版（Ubuntu） | 编排与工具链的执行面 | 宿主是 **Windows 10 19045** ⇒ 只能 **NAT**（`mirrored` 等键为 Win11 专属） |
| 镜像仓库（`registry-1.docker.io` 等） | 冷启动时拉取 | 本轮调研时**不可达**（超时），故体积数字未取得 |
| 公网（可选） | 公开数据集下载脚本 | 仓库零原始数据，只放脚本 + 许可说明 |

### 1.3 数据流（一句话版）

```
生成器(live) ──► Kafka ──► Flink(SQL 窗口 + DataStream 迟到侧输出) ──► Doris ──► Superset
                    │                      │
                    └── DLQ(脏)            └──► Paimon 五层(离线, dump/replay)
```

**关键区分（沿用 `gdd.md` §0）**：**事件结构分布**（同窗口内各事件类型的计数比）**不是**转化率；本文一律指**转化漏斗（步进式）**，其单位是 `user_id` 的步进推进，与 1 分钟窗口**正交**。

---

## 2. 分层与模块职责

### 2.1 两种分层口径并陈（逻辑 + 物理）

**逻辑分层**＝读者/责任面（沿用 `SRS.md` 的**八个主模块 + 三个加强项**，**不重命名、不新增第九个模块**，以保追溯矩阵不断裂）。
**物理分层**＝运行单元（进程/容器/构建产物）。两者靠 §2.3 的三级映射表咬合。

### 2.2 物理运行单元

| 物理单元 | 内容 | 常驻？ | 归属 |
| --- | --- | --- | --- |
| **容器群** | Kafka / Flink JM+TM / Doris FE+BE / Superset web+PG | 常驻 | 模块⑦编排 |
| **按需容器** | `client`（持 `mysql`+`curl`，网内发起 Doris Stream Load） | **否**（`run --rm`） | 模块⑦定义 / 模块⑧使用 |
| **可选 profile** | `minio`（S3 演示）、`spark`（离线对照，用完即停） | 否 | 模块⑦ |
| **宿主/WSL2 侧进程** | Python 生成器、`make` 编排、`make verify` 系列脚本 | 否 | 模块①⑧ |
| **构建产物** | Flink 作业 jar（Maven + shade） | 否 | 模块③④ |
| **资产（非运行单元）** | 五层 DDL、Flink SQL、Superset 导出 zip | 否 | 模块④⑤⑥ |

### 2.3 三级映射表（逻辑 → 物理 → 契约）

| 逻辑模块 | 物理单元 | 对外契约（**接缝**） |
| --- | --- | --- |
| ①数据生成器 | 宿主/WSL2 脚本 | Kafka topic `ecom.behavior.event.v1` 的消息体（含 `schema_version`） |
| ②消息队列 | 容器 | topic 名 + 分区键 + retention 配置 |
| ③实时作业 | 容器（Flink JM/TM）+ jar | Doris 库 `ecom_ads` 的表；DLQ topic |
| ④湖仓分层 | 容器（Flink 批）+ jar + DDL | Paimon catalog `ecom_lake` 的五层表 |
| ⑤OLAP | 容器（Doris） | `ecom_ads` 库 + FE HTTP/MySQL 端点 |
| ⑥看板 | 容器（Superset） | `/health`、看板导出 zip |
| ⑦编排 | compose + Makefile | `docker compose up`；八个冻结目标 |
| ⑧可复现校验 | 宿主/WSL2 脚本 + `client` | 断言结果与**退出码** |

### 2.4 模块边界的切法：**按数据契约切，不按目录切**

理由：**接缝是被下游消费的东西**，目录只是实现细节。上表「对外契约」列即边界。目录布局（`docker/`、`sql/`、`src/`、`flink/` …）是 §4 的设计面，改目录不动接缝。

### 2.5 横切关注点（4 条，各指定唯一所有者）

| # | 横切面 | 唯一所有者 | 说明 |
| --- | --- | --- | --- |
| 1 | **事件契约** | 模块①（生产者）/ 模块③（消费者校验） | 字段名、枚举、`schema_version` 语义；校验结果的**机读原因码** |
| 2 | **错误与退出码** | 模块⑧ | 每个脚本的退出码语义；失败必须有痕迹 |
| 3 | **配置与秘密** | 模块⑦ | `.env` 入 `.gitignore`；秘密不落仓库 |
| 4 | **幂等与重放** | 模块③⑤（实时）/ 模块④（离线） | 主防 + 兜底，**不是两道并列防线**（见 §7-D1） |

---

## 3. 技术栈定案表

> **锚点政策（三档，`SRS.md` C3）**：①**组件锚点**（`FSR.md` §4 那一列）本文**不得修改**，`FSR.md` 是唯一权威；②**工具锚点**（本文新引入的）**必须 pin 精确版本**，禁 `latest`；③**新增/替换组件的唯一合法通道**＝先登记为「可选实验、验收不阻塞」（沿用 `REQ-F-OLAP-04` 与 D9 的既有范式）。

### 3.1 组件锚点（**继承 `FSR.md` §4，本文不改**；仅补一处精确化）

| 位置 | 锚点 | 精确化/说明 |
| --- | --- | --- |
| 消息队列 | Kafka **4.3.1**（KRaft 单容器 combined） | — |
| 流处理 | Flink **2.2.x** | 不得 2.3.0（Kafka connector 断链） |
| Kafka connector | `flink-connector-kafka` **`5.0.0-2.2`** | ⚠️ **完整坐标**（`-2.2` 是 Flink 线后缀）。`FSR.md`/`SRS.md` 记的裸「5.0.0」不完整 |
| 湖存储 | Paimon **2.1.0** | artifact `paimon-flink-2.2-2.1.0.jar`（Central 实际存在） |
| OLAP | Doris **4.1.5** | — |
| Doris connector | `flink-doris-connector` **26.3.0** | — |
| 看板 | Superset **6.1.0** | — |
| JDK 基线 | **17** | `官方给出`：Flink 2.2 默认/推荐；**Doris `start_fe.sh` 要求恰为 17**（非 17 `exit 1`） |

### 3.2 工具锚点（**本文新引入**，全部 pin 精确版本）

| 工具 | 定案 | 角色 | 门禁？ | 理由（一手） |
| --- | --- | --- | --- | --- |
| **`uv`** | **已装 0.12.19**（`C:\Users\Administrator\.local\bin\uv.exe`，42,417,456 B），**不在 PATH** | **唯一环境与依赖管理器** | ✅ | Tier 1 覆盖 Windows + Linux；`uv.lock` 跨平台、官方要求**入库**；`uv run` **自动锁同步** |
| **Python** | **3.12**（由 `uv python install` 托管） | 生成器与脚本运行时 | — | 3.11/3.12/3.13 皆可用；选 3.12 是为**把锚点从宿主环境解耦**（宿主 Windows 是 3.13、WSL 是 3.14，跟随宿主＝违反 C3） |
| **`Ruff`** | pin 精确版本 | **唯一 lint + format 工具** | ✅ **硬门禁** | 退出码 0/1/2；>900 规则；MIT；官方声明 `ruff format` 对 Black 格式化「>99.9% 行一致」 |
| **类型检查** | **`basedpyright`** pin `1.40.2`（**自带 node**，原定 `ty`，见 §3.4） | 类型检查 | ✅ **硬门禁**（源码级契约，须精确 pin） | 自带 `nodejs-wheel-binaries` ⇒ **node 与检查器一起进 `uv.lock`**；官方给出 `uv` 指令（文档标题即 `uv (recommended)`） |
| **Kafka 客户端** | `confluent-kafka`（pin 精确版本） | 生成器 producer | — | 预编译 wheel（Win + manylinux **均免编译器**）；`KafkaError.code()` 是 int、`.name()` 是 str ⇒ **可直接支撑「按错误码分类写 DLQ」** |
| **事件校验** | `pydantic v2`（pin 精确版本） | 事件 schema 校验 | — | 唯一现成的**结构化机读错误码**清单（`type`/`loc`/`msg`/`input`）⇒ 支撑「三类原因可区分」 |
| **Faker** | **pin 到补丁号** | 生成器假数据 | — | ⚠️ 官方 seeding 原文：「results are **not guaranteed to be consistent across patch versions**」⇒ 不 pin 补丁号则 `REQ-F-GEN-06`（同种子同哈希）**不成立** |
| **Java 工程栈** | **JDK 17 + Maven ≥3.8.6**，**只在 WSL2 内** | Flink 作业构建 | — | 官方 quickstart/archetype/shade 模板全是 Maven 形态；宿主 JDK 18 + Maven 3.6（跑在 JDK 8 上）**不满足**官方要求 |
| **编排** | **GNU Make，执行面在 WSL2** | 唯一编排载具 | — | 冻结的是「**Makefile 目标**」这个载具+名字的组合；避开 Windows 上 `SHELL`/`COMSPEC` 分叉，且不新增容器 |

### 3.3 关键工具的具体纪律（官方明载的坑，逐条写死）

1. **Ruff 的分工**：官方明载 **formatter 不排序 import** ⇒ 组合必须是 `ruff check --select I --fix` **然后** `ruff format`。一串与 formatter 冲突的 lint 规则（`W191`/`E111`/`E114`/`E117`/`D203`/`D206`/`D300`/`Q000`–`Q004`/`COM812`/`COM819`）**默认不开**，开了须显式 ignore。
2. **uv 的三处官方偏差**：①**不读** `pip.conf`/`PIP_INDEX_URL`（用 `UV_INDEX_URL`/`uv.toml`）；②**不支持** `--user`；③**默认不做字节码编译**（`UV_COMPILE_BYTECODE=1`）。
3. **缓存落点（C: 仅剩 9.6 GB，逐条改向）**：`UV_CACHE_DIR` → **F:**；`PYRIGHT_PYTHON_CACHE_DIR`（如需）→ **F:**。⚠️ 另有一条官方硬规则影响本仓架构：uv 要求「**cache 与 Python 环境须在同一文件系统**，否则**无法 link、退化为 copy**」——而本仓 `.venv` 在仓库（`/mnt/f` DrvFs）、cache 默认在 `~/.cache/uv`（WSL fs）⇒ **必然 copy**。两条修法二选一：cache 指到 `/mnt/f`，或 `.venv` 移出仓库（`UV_PROJECT_ENVIRONMENT`）。**官方未给二者取舍的定量依据** ⇒ 属待实测项。
4. **Maven shade 模板**：`artifactSet.excludes` 排 `jsr305`；`filters` 排 `META-INF/*.SF|*.DSA|*.RSA`（否则 SecurityException）；核心 API 依赖设 **`provided`** scope（Flink inverted classloading）。
5. **Ruff 与类型检查的**职责划界（**一个含义只有一个所有者**）：pyright 系的 `reportUnusedImport`/`reportUnusedVariable`/`reportUnusedClass`/`reportDuplicateImport` 与 Ruff 的 `F401` 等**职责交叉** ⇒ 这类规则**显式关闭、交给 Ruff 独占**。已有实证冲突：basedpyright 自身 `pyproject.toml` 写着 `reportImplicitStringConcatenation = false # conflicts with ruff formatter`。
6. **静态检查与运行时烟测分开**：`SRS.md` 的 `make verify` ＝ **五项运行时烟测**（链路必须在跑）；Ruff/类型检查查的是**源码**、不依赖链路在跑 ⇒ **另设目标，不混入 `verify`**：
   | 目标 | 内容 | 退出码语义 |
   | --- | --- | --- |
   | **`lint`**（新增） | `ruff check` + `ruff format --check` | 0 通过 / 1 有违规或待格式化 / 2 配置或内部错误 |
   | **`typecheck`**（新增） | `basedpyright --outputjson` | 0 无错 / 1 有错 / 2 致命 / 3 配置不可读 / 4 非法参数 |
   | `verify`（**冻结，一个字不改**） | 五项运行时烟测 | 见 `gdd.md` §1.8 |
   > 新增 `lint`/`typecheck` 属**设计面**（HLD 可定），**不是**对冻结目标名的漂移。
7. **静态检查的配置面**：`basedpyright` 用 `pyproject.toml` 的 `[tool.pyright]`（官方：`pyrightconfig.json` **总是优先于** `pyproject.toml` ⇒ **不要**另建该文件）；档位**显式写 `standard`**（对齐上游官方默认，**不用** fork 的 `recommended` 档）；键 `pythonVersion="3.12"`、`venvPath="."`、`venv=".venv"`，并显式 `include`/`exclude`（`**/node_modules`、`**/__pycache__`）。

### 3.4 类型门禁定案：**`basedpyright`**（`ty` → `pyright` → `basedpyright`）

**决策链**：`ty` 被否 → 用户改提 `pyright` → 核查后定 **`basedpyright`**。

**`ty` 被否的一手理由**（留痕）：官方自述「ty uses **`0.0.x`** versioning… **breaking changes, including changes to diagnostics, may occur between any two versions**」；PyPI `Development Status :: 4 - Beta`；Stable 里程碑（#4）**仍 open**（103 项未清）。**会随版本自行改判的诊断做不了门禁**。

**`pyright` 能做硬门禁——这是「性质差别」，不是「程度差别」**。与 `ty` 的分水岭在**源码里明写为公开契约的两样东西**（正是门禁真正依赖的东西）：
```ts
// These values are publicly documented. Do not change them.
enum ExitStatus { NoErrors=0, ErrorsReported=1, FatalError=2, ConfigFileParseError=3, ParameterError=4 }
// The schema for this object is publicly documented. Do not change it.
interface PyrightJsonDiagnostic { file; severity; message; range?; rule? }
```
出处：`packages/pyright-internal/src/pyright.ts`。⚠️ **未冻结的是「诊断集合」**（哪些规则开、默认 severity 无书面承诺）⇒ **精确 pin 是门禁成立的前提条件，不是优化项**。

**为什么选 `basedpyright` 而非上游 `pyright`**：门禁确定性的真正命门是「**node 运行时是否也被 pin 住**」。三条路径对照：

| 路径 | node 来源 | 锚点是否闭合 | 判定 |
| --- | --- | --- | --- |
| **`basedpyright`（选中）** | **自带** `nodejs-wheel-binaries>=20.13.1`（设计意图原话：「the user **shouldn't have a node/npm binary unknowingly added to their PATH**」） | ✅ **检查器 + node 一并进 `uv.lock`** | ✅ **选中** |
| 上游 `pyright` + `[nodejs]` extra | `nodejs-wheel-binaries` | ⚠️ 包装器 README 自述「first checking if node is in **the PATH**. If it is not, then we download node」——本机 PATH 上**确有** node ⇒ **PATH 优先性未证**，换机器会静默改判 | ❌ |
| 上游 `pyright` via npm | 宿主 node（Win `v24.21.0` / WSL `v22.22.1`） | ❌ **node 版本成为新的未 pin 锚点**，与 C3 相抵 | ❌ |

**代价（明写，不隐藏）**：社区 fork、**单一维护者**（647 open issues）；**叠加自有规则并把默认档改为 `recommended`**；`--baselinemode` 官方自标 experimental 且将 breaking。**缓解**：档位显式写 `standard`（不用 fork 的默认档），并在升级时走**显式变更 + 重跑基线**。

**必配纪律**：①`pyright==`… 精确 pin 写进 `pyproject.toml` / `uv.lock`；②**禁止** `PYRIGHT_PYTHON_FORCE_VERSION=latest`；③门禁**只解析 JSON**（文本路径会过滤 `UnusedCode`/`UnreachableCode`/`Deprecated`，两种输出的诊断集合**不同** ⇒ 同一份代码可能给出不同行数）；④`--outputjson` 与 `--verbose`/`--stats` **互斥**（同时给出即 **exit 4**），Makefile 里别叠。

### 3.5 弃选表（含理由；**只登记，不改锚点**）

| 位置 | 弃选 | 决定性理由（一手） |
| --- | --- | --- |
| 消息队列 | Pulsar | 同为宽松许可且活跃，但 Kafka+Flink 生态与岗位覆盖面更广（对照项） |
| 流处理 | Flink 2.3.0 | **无已发布 Kafka connector** ⇒ 单点断链 |
| 湖存储 | Iceberg | ⚠️ **artifact 并不缺**（`iceberg-flink-runtime-2.2/1.12.0` 存在）⇒ 弃选理由**不能**是版本缺失，须回到**能力**：流式 upsert/changelog 弱于 Paimon 主键表，且要多拼一层组件、多占一份天花板 |
| 湖存储 | Hudi | ✅ **最干净的一条**：Central **无 `hudi-flink2.2-bundle`**（只有 flink2.0/2.1，至 1.2.1）⇒ 与锚定 Flink 2.2.x **版本层即不相容** |
| OLAP | StarRocks | Paimon Catalog **Beta 且只读**（官方：「You can only use Paimon catalogs to query data」）+ **无 Flink 2.x connector artifact** |
| OLAP | ClickHouse | ⚠️ **有**官方 Paimon 引擎（Experimental、默认关），但官方原话「Merge-on-read is not implemented, so **primary-key tables cannot be read** … therefore throws」⇒ **本仓四层皆主键表，不相容**；另官方 sizing 下限「不应低于 8 GB」单项即吃满预算 |
| OLAP | DuckDB | 无**官方** Paimon 支持（仅社区扩展）；进程内**单写入者**模型与「Superset 独立进程持续读」冲突 |
| 看板 | Grafana | ⚠️ **排除理由不是许可证**：官方 `LICENSE` 第 13 条的条件从句是「**if you modify the Program**」⇒ 本机自用、不改、不对外**确无义务**，`FSR.md` §3 的边界分析**逐字成立**。真正的排除理由是 **`SRS.md` §6.1 + 判据③**：官方自证 *「In Grafana OSS, the alert engine runs in the same process as the UI and data source proxy」* ⇒ 引入即引入**告警引擎**；且它**没有**「暂定值/落定值/已修订」原生语义，步进漏斗在 Grafana 里只能靠大量 SQL 硬拼 |
| 看板 | Metabase | **判据②天然做不到**（官方刷新档位只有 **1/5/10/15/30/60 分钟**）；次要：OSS 版是 **AGPL**（官方 `LICENSE.txt` 逐字「Outside of the top-level "enterprise" directory … licensed under the AGPL」），所谓「许可更宽松」**在官方文件上不成立** |
| 看板 | Redash | **判据②天然做不到**（官方原话「The allowed refresh intervals are expressed in seconds: **60**, 300, …」）；且官方**自曝**安全弱点（「any user can modify them [parameters] … the wrong tool for embedded analytics」）、生产 compose **8 个服务**。⚠️ 其许可证是本次候选里**最宽松的 BSD-2-Clause** ⇒ 现成的「**许可对了但形态不对**」面试反例 |
| 看板 | Evidence.dev | **两条独立断链**：① **无 ≤10 s 刷新**（连「自动刷新」概念都没有）；② 官方 Direct Connectors 清单**无 MySQL**（Doris 走 MySQL 协议 ⇒ 接不上）。它的定位是「**报表/数据产品发布**」，不是实时看板 |
| 看板 | Lightdash | **判据④断链**：官方连接清单**无 MySQL**；且 EE 目录走 Source Available License（非宽松），并强行引入 **dbt 项目**依赖 |
| 看板 | Zeppelin / sqlpad | Zeppelin 是 **notebook**（段落执行）、无面板刷新概念；sqlpad 是 **Web SQL editor**（无面板概念）且 GitHub API `archived = true` |
| 看板 | DataEase | `LICENSE` 为 modified GPLv3 + 附加条件，不符合「仅宽松许可」口径 |
| 类型检查 | `ty` | 诊断可在任意两版本间变更 ⇒ 不能做门禁（见 §3.4） |
| 观测性 | Prometheus + Grafana 常驻 | ⚠️ 排除理由是 **`SRS.md` §6.1`**（Grafana OSS 告警引擎与 UI **同进程**；Doris 官方把 Prometheus 定义为 *monitoring **and alerting** suite*）——**不是**许可证（见 §3.5 看板行的更正） |
| 观测性 | Loki + Promtail | Promtail 官方 **EOL 2026-03-02**（迁移目标 Alloy）；Loki 官方 sizing 只覆盖**分布式模式**，最小档合计 **≈49 GiB** ＝预算 6 倍 |
| 观测性 | OTel Collector 常驻 | 对本规模过重；§6 的可机读断言**不需要**它 |
| 编排 | `just` / `task` | 冻结的是「Makefile 目标」载具+名字的组合；换载具＝冻结项漂移，须先走冻结项变更。**记录其优势**（`--list` 机器可读目标清单）作为将来复核挂点 |
| 运行面 | Podman / devcontainer(DooD) / WSL 原生安装 | 分别：新增第二套 VM/栈且 `config` 无校验；路径语义被改写；`mem_limit`/`restart`/日志轮转/named volume **五项判据全部落空** |
| Kafka 运行时 | GraalVM 原生镜像 | 官方定性「minimal memory footprint」但**全篇无数字**；且自证 **experimental / 非生产**、**GC 仅 serial**、**不支持运行时新增 jar**、**不在 Docker Official Image 路线**（KIP-1028）。⇒ 可作**减少堆占用**的候选，**不能**作「有官方数字的降级」 |

### 3.6 观测性定案：**基线零新增常驻组件**

**核心结论**：`SRS.md` §3.1.8（可复现校验）与 §6.1（不做监控告警）是**互补而非冲突**——后者排除的是「**长期存储 + 告警 + 仪表盘**」那一半，而 `make verify` 的**退出码**属前者。
`gdd.md` §C 的 **13 条检测信号**里，**10 条可零新增常驻组件判定**：Flink REST 六个端点（`/jobs/overview` 的 `jobs[].state`、`/jobs/:jobid/checkpoints`、`/vertices/:vertexid/watermarks`、`/jobs/:jobid/exceptions` …）、Doris `GET :8030/api/health` 的 `online_backend_num`、Superset `GET /health` 返回**纯文本 `OK`**（⚠️ **不是 JSON**）、`docker stats --no-stream --format "{{ json . }}"`。
**可选、非门禁**：`danielqsj/kafka_exporter` 的 `/metrics` 是 Prometheus 文本格式，`make verify` 可**直接 curl + grep 判退出码、无需 Prometheus 常驻**（代价：非官方、单一维护者、268 open issues）。

---

## 4. 运行环境与沙箱约束

> 本仓「**沙箱**」有**两个所指**，必须分开命名（既有文档把它们混在 R8/U3 一处，是术语债）：
> **甲·运行面沙箱**＝跑这条链路的**容器运行面**；**乙·会话沙箱**＝**agent 会话**的文件写权限模式。两者**无因果关系**。
> **一条被证伪的因果**：「放宽沙箱才能跑 docker」是**错误**的——容器与卷数据由 Docker Desktop 自己的 VMM 写入，**不经 agent 令牌**。

### 4.1 甲·运行面沙箱：维持 **Docker Desktop / WSL2**

| 候选 | 判定 | 决定性理由 |
| --- | --- | --- |
| **Docker Desktop / WSL2** | ✅ **维持** | `docker compose config` **实测无需 daemon** 且会把 `mem_limit` 正常化为字节值；四家组件**皆有官方骨架来源**；`mem_limit`/`-p 127.0.0.1`/named volume/`restart`/`profiles`/日志轮转**全为一等公民** |
| Podman + podman-compose | ⚠️ 书面备选 | 源码级支持相关字段，但 `config` 子命令**无校验无正常化** ⇒ GDD 判据**没有等价物**；且新增第二套 VM/栈 |
| devcontainer (DooD) | ❌ 排除 | 官方明确不支持 workspace 之外的 bind；把「一条命令」变成「先装 VS Code + devcontainer」 |
| WSL2 原生装（不用 Docker） | ❌ 排除 | `mem_limit`、`restart`、日志驱动、named volume、`down` 不丢数据——**五项判据全部落空** |

### 4.2 资源上限（**硬化两条渗漏点**）

**★ P0 实测回填（2026-10-19 21:30，`docker compose up` 骨架已拉起）**

| 服务 | `mem_limit` | **实测常驻** | 占限额 |
| --- | --- | --- | --- |
| kafka | 1g | **532.6 MiB** | 52% |
| flink-jm | 1g | **311.9 MiB** | 30% |
| flink-tm | 2g | **403.5 MiB** | 20% |
| superset | 1g | **387.8 MiB** | 38% |
| **小计（实测）** | — | **≈1.60 GiB** | — |
| `kafka-init` / `client` | 256m | 按需（不常驻） | — |
| **配置契约声明的常驻合计** | — | **10.25 GiB**（含 Doris 的 2g+3g 预留） | 天花板 11.68 GiB ⇒ 余量 **1.43 GiB** |

> **读法（重要）**：`mem_limit` 是**上限**不是占用。实测值远低于上限（Kafka 只用了一半、Flink 只用 1/5），说明当前**空载**骨架很轻；**真正的压力在 P1 有数据流之后**——Flink 的 managed memory 会随 RocksDB 状态增长、Doris 的 BE compaction 会吃内存。故「10.25 GiB 合计」是**已声明的上限契约**，「1.60 GiB」是**当前空载实测**，两者不可互相冒充。
> **待回填项**：Doris（镜像须本地构建）与 P1 带载后的实测值。

| 约束 | 定案 | 检查方式 |
| --- | --- | --- |
| 逐服务显式 `mem_limit` | **每个服务都写**，且**同时写 `memswap_limit` ＝ `mem_limit`** | ✅ **P0 已证**：`config-check` 断言全 PASS（8 服务逐条比对） |
| 不与 `deploy.limits.memory` 并存 | **只写 `mem_limit`** | 官方：`deploy` 是**可选段**（未实现即忽略）；二者并存时官方要求**必须一致**，写错一个即自相矛盾 |
| 「不允许有服务缺省」 | 由 `make verify` / `config-check` **脚本断言** | ⚠️ **P0 实测确认**：`docker compose config` 能**列出** `mem_limit` 但**不报缺** ⇒ 判据必须脚本化。规则已写进 `Makefile` 的 `config-check` 目标 |
| 冷启动/体量记账 | **用 `docker image inspect`**，不用 `docker images` | 本机 `UseContainerdSnapshotter=true`，官方警告此时 `docker images` 会**把压缩 blob 叠加进读数、显著虚高**。**P0 实测镜像体积**（`image inspect`）：Kafka 654.1 MiB / Flink 1491.4 MiB / **Superset 1207.8 MiB**（官方未给出，此处为实测取得）/ Doris base-latest 1.96 GB |
| 常驻预算 | 见上表；`SRS.md` 的 ≤8 GB 是**设计值**，按官方出厂默认**不成立** | `docker stats --no-stream` 总值；P0 已实测空载 1.60 GiB |

### 4.3 端口与网络

| 项 | 定案 |
| --- | --- |
| 端口表 | Flink UI **8081**｜Kafka **9092**｜Superset **8088**｜Doris FE HTTP **8030** / FE MySQL **9030** / BE HTTP **8040** / BE brpc **9060**｜MinIO profile **9000**/**9001** |
| 绑定 | **全部** `127.0.0.1:H:C`，**每个**服务都要带 host IP |
| **禁止** | 任何 `netsh interface portproxy`；**不写 host IP** 的发布（官方：会绑 `0.0.0.0` 并**绕过宿主防火墙**） |
| 局域网不可达 | ✅ `官方给出`：带 `127.0.0.1`/`::1` 时「**only the Docker host can access**」；历史缺陷「同 L2 段可触及 localhost 端口」仅存于 **< 28.0.0**，本机 **29.8.0** 已修；本机 **Win10 ⇒ 只能 NAT**，NAT 下不建 `portproxy` 即无第二条暴露路径 |
| 检查方式 | ①`docker compose config` 断言**每个**服务的 `host_ip` 均为 `127.0.0.1`（缺一即不通过）；②从局域网另一台机器实测连接失败 |

### 4.4 ★ 网络可达性的硬约束（**本轮新增，三份上游文档都没有**）

**事实**：Docker Desktop 下**容器地址宿主不可路由**（官方：「**Per-container IP addressing is not possible** — the Docker `bridge` network is not reachable from the host」）。
**后果**：Doris 的 Stream Load 是「FE 收标 → **重定向到某个 BE**」的**两跳**，第二跳是**容器地址** ⇒ 官方原话：a stream load **from the host cannot follow FE's redirect to a BE; run it from the `client` service instead**, which is inside the network。
**处置**：
- compose 里**声明**一个 `client` 服务（持 `mysql` 与 `curl`，`depends_on` 全链路 `service_healthy`），但**不常驻**——以 `docker compose run --rm` 按需拉起，从而**不占常驻预算**。
- 所有 Doris 相关断言与导入演练**一律经该服务执行**；所有权：容器定义归**模块⑦**，断言内容归**模块⑧**。
- **不受影响**：Flink-Doris-Connector 的写入（TaskManager 本就**在 compose 网内**）。
**检查方式**：`make verify` 的 Doris 断言**必须**经 `client` 服务执行——在宿主上直接跑即为**失败**。

### 4.5 存储与磁盘

| 项 | 定案 | 依据 |
| --- | --- | --- |
| named volume 落点 | **天然落 Linux 文件系统**（本机引擎数据盘已在 **F:**） | ✅ R6 判定**不变且已满足**，无需额外动作 |
| bind 纪律 | 凡 bind 进容器的路径，一律走 **image 内 COPY 或 named volume**，**不 bind 仓库路径** | 官方：bind mount 走 Windows 文件系统**性能远低于** Linux 文件系统，且 **inotify 事件不投递**。仓库在 `F:\` ⇒ WSL 侧 `/mnt/f` 同属 DrvFs |
| `uv` cache | **显式设 `UV_CACHE_DIR` 到 F:** | `%LOCALAPPDATA%\uv` 在 **C 盘**且 `UV_CACHE_DIR` 未设（C: 仅剩 9.6 GB） |
| 容器数据与镜像层 | **已不在 C 盘** | 实测 `CustomWslDistroDir=F:\Docker Desktop\DockerDesktopWSL` + `UseContainerdSnapshotter: true` |
| 需记账的冲突 | Doris 官方 dev/test 磁盘口径 **FE 10 GB+ / BE 50 GB+**，与 `SRS.md` 的 ≤20 GB **正面冲突** | 见 §7、§8-R5 |

### 4.6 乙·会话沙箱（agent 权限面）

**现状**：本会话 `danger-full-access`。**调研结论**：本仓**不需要**它。
**最小可行形态（建议顺序）**：①在**审批通道可用**的会话里，对**仓库根**跑一次随包 ACL 修复脚本；②在受限模式下**重放原操作**验证；③验证通过后长期降至 **`workspace-write`**（root ＝ 仓库根）。
**两条必须分清的事实**：
- `docker`/`compose` 走 **npipe**，而沙箱「**读、网络与进程可见性不受限**」、npipe **非文件读写** ⇒ 受限模式下**有理由**照常可用（**未实测**：本会话审批被禁用、无法降级验证）。
- **`ops_bash` 缺 IPC 凭据的失败不可修**——它是 **named-pipe 设计边界**（受限进程内 `spawn(stdio:'pipe')` 以 EPERM 失败），**ACL 修复脚本对它无效**。绕法只有 `stdio:'inherit'/'ignore'` 或改用 PowerShell 原生管道。这与 ACL 故障是**不同因**。
- **一条被证伪的因果**：「放宽沙箱才能跑 docker」是**错的**——容器与卷数据由 Docker Desktop 自己的 VMM 写入，**不经 agent 令牌**。

### 4.7 ★ 执行面落点（**已由 P0 实测裁决；此前一轮的「推论」被推翻**）

**结论（P0 实测，2026-10-19 20:29，daemon 已起）**：**单一执行面 = WSL2**，端口探测**不需要**特殊处理——WSL 内直接用 `curl`/`python3` 访问 `127.0.0.1:<published>` **即可达**。

**★ 为什么推翻前一轮结论（留痕，避免再犯）**：此前（daemon 未运行时）用**等价 Windows 监听**做代理测试，得到「WSL 连 `127.0.0.1` 一律 FAIL」；P0 用**真的** Docker 发布端口复测，结论**相反**：

| 监听类型 | Windows 宿主 | WSL2 → `127.0.0.1` |
| --- | --- | --- |
| **Docker 发布端口** `-p 127.0.0.1:18080`（`owning process = com.docker.backend`） | HTTP 200 | **200（3/3 次 + 两个端口复现）** |
| **普通 Windows 监听**（DSH GUI `:10724`） | HTTP 401 | **FAIL** |

⇒ **代理测试不成立**：Docker Desktop 的 WSL integration 让**已发布端口**经 distro 的 `localhost` 可达，而普通 Windows 监听不具备该通路（机制细节官方未明文）。**故「必须用 `python.exe` 探测」这条约束被撤销**——它是那个误导性代理测试的产物。

**✅ 定案**：
- `make` **全在 WSL2** 执行（Linux shell 语义；`make` 经 apt 装）。
- **A 面**（Flink UI `8081` / Kafka `9092` / Doris `9030`·`8030` / Superset `8088` 探测）在 WSL2 内用 `curl`/`python3` 直连 `127.0.0.1`。
- **B 面**（编排、生成器）在 WSL2 内。
- **C 面**（Stream Load 两跳）仍归 **compose 网内 `client` 服务**（§4.4，与落点无关）。
- **触发条件（可逆）**：上述 A 面可达**依赖 Docker Desktop 对 Ubuntu 的 WSL integration 处于开启状态**（P0 实测：`/var/run/docker.sock` 存在、`/usr/bin/docker` 可用、`docker version` 返回 Server 29.8.0）。**若该集成被关闭，A 面须重测**。

**检查方式（可证伪）**：
```bash
# WSL2 内：对已发布端口应返回 2xx/3xx；对非 Docker 监听应失败（证明测的是 Docker 通路）
wsl -e bash -lc 'curl -s -m 5 -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8081'   # Flink UI，期望非 000
wsl -e bash -lc 'test -S /var/run/docker.sock && echo socket-present'                    # 期望 socket-present
```
**判定条件**：已发布端口返回非 `000` **且** `/var/run/docker.sock` 存在 ⇒ 落点成立。若 socket 消失，说明 integration 被关 ⇒ **须重新裁决**，不得沿用。

### 4.8 编排执行面的两条候选路径（WSL 侧）

WSL 内的 `docker` 有两条路，差别在**路径语义**：

| 路径 | CLI 载体 | 路径语义 | 判定 |
| --- | --- | --- | --- |
| **① WSL 内调 Windows `docker.exe`** | Windows 进程（**npipe 已验证可达**：`Client 29.8.0 / Context desktop-linux`） | ⚠️ **Windows 语义**：`build.context`/bind/`.env` 全按 Windows 解析；`cd /tmp` 会退化为 `C:\Windows`（UNC 不支持）；官方合法形**只有** `C:\…` 与 `/c/…`，**无 `/mnt/f` 这一形** | 仅在「仓库在 `F:\` + 只用**相对路径**、绝不写 `/mnt/f` 形绝对路径」时可用，**脆** |
| **② 开启 Ubuntu 的 WSL integration** | `/usr/bin/docker`（Linux CLI 注入 distro） | ✅ **Linux 语义**：`/mnt/f/...` 正常，相对路径相对 compose 文件解析 | ✅ **推荐**（消掉全部分叉） |

> ⚠️ 本机 `command -v docker` 命中的是 Docker 注入的 **shim**（1359 B，`#!/usr/bin/env sh`）——它因 `/usr/bin/docker` 不存在而**直接 `exit 1`** 并提示开启 integration。**即当前 WSL 内 `docker` 命令不可用**，须按 U-ε 开通。

**现状**：本会话 `danger-full-access`。**调研结论**：本仓**不需要**它。
**最小可行形态（建议顺序）**：①在**审批通道可用**的会话里，对**仓库根**跑一次随包 ACL 修复脚本；②在受限模式下**重放原操作**验证；③验证通过后长期降至 **`workspace-write`**（root ＝ 仓库根）。
**两条必须分清的事实**：
- `docker`/`compose` 走 **npipe**，而沙箱「**读、网络与进程可见性不受限**」、npipe **非文件读写** ⇒ 受限模式下**有理由**照常可用（**未实测**：本会话审批被禁用、无法降级验证）。
- **`ops_bash` 缺 IPC 凭据的失败不可修**——它是 **named-pipe 设计边界**（受限进程内 `spawn(stdio:'pipe')` 以 EPERM 失败），**ACL 修复脚本对它无效**。绕法只有 `stdio:'inherit'/'ignore'` 或改用 PowerShell 原生管道。这与 ACL 故障是**不同因**。

---

## 5. 数据契约（冻结面清单）

> **冻结面＝不可动**（改了就是破坏下游）；**设计面＝HLD 可定**（目录结构、模块内实现、脚本命名、容器内进程布局、错误码表、校验结果结构）。

| 类别 | 冻结值 |
| --- | --- |
| Kafka topic | `ecom.behavior.event.v1`；DLQ `ecom.behavior.event.v1.dlq` |
| 消息格式 | JSON + `schema_version`；字段一律 **`snake_case`** |
| Paimon | 库 `ecom_lake`；表 `ods_behavior_event` / `dwd_behavior_event` / `dwd_behavior_event_reject` / `dim_*` / `dws_*` / `ads_*` |
| Doris | 看板直连库 `ecom_ads` |
| Makefile 目标 | `up` / `down` / `init` / `replay` / `verify` / `spike` / `verify-fault` / `logs` |
| 端口 | 见 §4.3（**全部仅绑 `127.0.0.1`**） |
| 事件字段与枚举 | `SRS.md` §3.3 字段表；五类枚举 |
| 参数数值 | **`gdd.md` §A 是唯一权威**（本文不复制，避免第二个真相源） |

**设计面（本文可定）**：目录布局、Flink 作业内部结构、`client` 服务的实现、校验结果结构的**内部形状**、脚本命名。

---

## 6. 失败路径与可机读断言

**权威归属 `gdd.md` §C**（失败路径登记表）与 **§1.8**（五项烟测的确切断言形式）。本文**不复制**，只登记**升级原则**：

**「看」与「判」的二分**：只有**确切字段 / 退出码**算合格判据——JSON 字段（`jobs[].state`、`online_backend_num`）、纯文本定值（Superset `/health` 的 `OK`）、Prometheus 文本指标名、`docker stats --format json`、SQL 标量。**「Flink UI 里能看到」不是判据。**
**§C 的 13 条检测信号 → 10 条可零新增常驻组件判定**（见 §3.6）。

---

## 7. 偏离与待收口

> 本节**暴露**而非覆盖：`SRS.md` 与 `FSR.md` **一个字都不改**（等 `spec.md` 统一收口）；被本轮调研**证伪**的 `gdd.md` 条目已**就地修订**。

### 7.1 对 `FSR.md` / `SRS.md` 的偏离登记

| # | 上游陈述 | 事实（一手） | 处置 |
| --- | --- | --- | --- |
| **D-A** | `FSR.md` §2 / `SRS.md` A2：「官方**未给出** Doris 单机最低内存」 | ❌ **官方给出了**，且远高于预算：dev/test **FE 8 GB + BE 16 GB**（通用口径 FE 16 GB、BE 核数×4 GB）；官方自认「**Doris does not enforce a memory limit**」 | 登记；见 §8-R1 |
| **D-B** | `FSR.md` §2 给 Doris 的预算 **~3.0 GB**、`SRS.md` 常驻 ≤8 GB | ❌ 按官方出厂默认**必然不成立**：FE 一个进程默认堆 `-Xmx8192m -Xms8192m`（**源码逐字**）＝ 8.0 GiB ＝ 整个 8 GB 预算；BE `mem_limit` 默认 **90%** ⇒ 天花板下 ≈10.5 GB | 登记为**设计值不足**；P0 必须显式下调并实测 |
| **D-C** | `SRS.md` §3.5 版本链条写着 Kafka connector「**5.0.0**」 | ⚠️ **坐标不完整**：Maven metadata 实测 `<release>5.0.0-2.2</release>` | 登记；施工用 **`5.0.0-2.2`** |
| **D-D** | `FSR.md` §8 R3 的降级序列含「**Superset→Grafana**」 | ❌ 该级**已被否**，但**理由不是许可证**：官方第 13 条的条件从句是「**if you modify the Program**」⇒ 本机自用、不改、不对外**确无义务**（`FSR.md` §3 的边界分析**逐字成立**）。真正的理由是 **`SRS.md` §6.1**：官方自证 OSS 版 alert engine 与 UI/数据源代理**同进程** | 登记；**R3 第二级已改写为「Superset 减服务」**（§8-R6） |
| **D-E** | `FSR.md` §3 记 ClickHouse「**无 Paimon 支持**」、StarRocks 因「Elastic License」被排除 | ⚠️ 前者**不成立**（官方**有** Paimon 引擎，但**读不了主键表**）；后者许可证**是** Apache-2.0（排除的真实原因是**版本相容性**） | 登记；**弃选理由表已按一手证据重写**（§3.5） |
| **D-F** | `FSR.md` §3 记 Hudi 为对照、Iceberg 弃选 | ⚠️ Hudi 的**决定性证据**是 **`hudi-flink2.2-bundle` 不存在**（比原表述更硬）；Iceberg **artifact 齐全**（`iceberg-flink-runtime-2.2/1.12.0`）⇒ 弃选理由**不能**是版本缺失 | 登记；§3.5 已重写 |
| **D-G** | `SRS.md` §2.5 A2 笼统称多家组件「官方未给出最低内存」 | ⚠️ 需**逐组件区分**：Doris**给出了**（见 D-A）；Kafka/Flink/Superset/Paimon 确实未给出（但 Flink 给了**分项默认**，且**不配 `process.size` 则启动失败**） | 登记；§0.3 的参数性质边界即此事的制度化 |

### 7.2 `gdd.md` 的就地修订（**已证伪，会造成施工错误**）

| # | `gdd.md` 原陈述 | 修订为 |
| --- | --- | --- |
| **G-D15** | 「Doris **没有官方 docker-compose**，官方仅 `curl \| bash`；须自写并自担 FE/BE 注册」 | 官方**有** `docker/runtime/all-in-one/4.1/compose/{multi-node,cloud}.yml`，且**有 all-in-one 单容器镜像**（1 FE + 1 BE，<20 s healthy，自带 HEALTHCHECK，建表免 `replication_num`）。自写量降为**裁剪 + 加 `mem_limit`/端口绑定**。⚠️ 「官方 Docker 仅限开发测试 ⇒ named volume **必需**」**未被证伪** |
| **G-D8** | 分区保留只需 `dynamic_partition.start = -30` | ⚠️ **漏了 FE 级总开关**：`dynamic_partition_enable` **默认 `false`** ⇒ 表级写 `enable=true` 也不跑调度；另需 `create_history_partition=true` 与必填的 `time_unit`/`end`/`prefix`。**不修则 §1.5 判据必然不通过** |
| **G-D2** | 「快照数受控为 **5**」 | ⚠️ `snapshot.num-retained.min` 是**下限**（快照数**不会低于** 5），实际数量由 `snapshot.time-retained`（1 h）与 `num-retained.max`（MAX）决定；且官方支持系统表的引擎列表（Flink/Spark/Trino/StarRocks）**不含 Doris** ⇒ 该判据**必须走 Flink SQL** |
| **G-A** | `gdd.md` §A.3 JDK 基线 17 | ✅ 口径**正确**（官方二次确认）；补记**本机无 JDK 17**、宿主 JDK 18 与 `JAVA_HOME`→Jdk1.8、Maven 3.6（< 官方 3.8.6 且跑在 JDK 8 上） |
| **G-GEN** | `REQ-F-GEN-06`（同种子同哈希） | 补记**前提**：Faker **必须 pin 到补丁号**（官方 seeding 原文），否则不成立 |
| **G-§1.7** | 「逐服务显式 `mem_limit`」 | 补记：**同时**写 `memswap_limit`（否则可借等量 swap）；不与 `deploy.limits.memory` 并存；`config` **不报缺** |
| **G-§1.7-Doris** | 未提 Stream Load 可达性 | 补记 **§4.4 硬约束**：宿主**不能** follow FE→BE 重定向，导入须在 compose 网内发起 |

### 7.3 待决项（**需你单独授权才执行**）

| # | 项 | 现状 | 说明 |
| --- | --- | --- | --- |
| **U-α** | 安装 **Ruff**（pin 精确版本） | 已批 | 回滚＝`uv tool uninstall ruff` |
| **U-β** | 改 **PATH** 启用已装的 `uv 0.12.19` | 已批 | 回滚＝从 PATH 移除该目录 |
| **U-γ** | WSL2 内装 **JDK 17 + Maven ≥3.8.6** | **待批** | P0 确定性阻塞项；宿主 JDK 18 / `JAVA_HOME` **一概不动** |
| **U-δ** | WSL2 内 `apt install make` | **待批** | 编排执行面 |
| **U-ε** | Docker Desktop 对 Ubuntu 开 **WSL integration** | **待批** | 回滚＝同页关闭 |
| **U-ζ** | 设 **`UV_CACHE_DIR`** 到 F: | **待批** | 回滚＝删该环境变量 |
| **U-η** | Ubuntu 发行版 `ext4.vhdx` 是否迁出 C 盘 | **待定** | 体积**未测**；迁移＝`wsl --export/--import`，属动本机配置 |
| **U-θ** | `uv python install 3.12` | 已批 | 回滚＝`uv python uninstall 3.12` |
| **U-ι** | 安装 **`basedpyright==1.40.2`**（`uv add --dev`） | **待批** | 回滚＝`uv remove --dev basedpyright` + 重生成 `uv.lock`；含写 `[tool.pyright]` 段的回滚（删除该段） |
| **U-κ** | 若走包装器兜底，设 **`PYRIGHT_PYTHON_CACHE_DIR`** 到 F: | **待批** | 选 `basedpyright` 则基本不需要（node 由 wheel 提供）；保留以防兜底 |

---

## 8. 架构风险

> 每条附**检测信号**与**判定条件**（§0.4 的行为可证纪律）。按「会不会让链路本身跑不起来」排序。

### R1 · ★ Doris 出厂默认参数与天花板正面冲突
**性质**：**已源码级证实**的确定性冲突，缺口 ≈5 GB。
**证据**：`conf/fe.conf` 逐字 `-Xmx8192m -Xms8192m`（FE 单进程默认堆 ＝ **整个 8 GB 预算**）；BE `mem_limit` **默认 90%**（11.68 GB 天花板下 ≈10.5 GB）；两者相加 ≈18.5 GB；官方 dev/test 口径 **FE 8 GB + BE 16 GB**。
**处置**：P0 **必须**显式下调（FE `-Xmx`、BE `mem_limit`），并把预算表每项标 `官方给出`/`设计值`；`SRS.md` 的 ≤8 GB **不动**（按 §7.1 D-B 登记）。
**检测信号**：`docker stats --no-stream` 各服务与总值；Doris FE 启动日志（`fe.out`）。
**判定条件**：下调后 Doris 能在 `docker stats` 实测下**稳定供数**（`/api/health` 返回 `online_backend_num == total_backend_num` 且 ADS 表可查）——**未实测即未通过**。

### R2 · ★ Doris Stream Load 在 Docker Desktop 下宿主不可 follow
**证据**：官方原话（Doris README）「a stream load from the host **cannot follow FE's redirect to a BE; run it from the `client` service instead**」；官方同时说明「Per-container IP addressing is not possible」。
**处置**：见 §4.4（网内 `client` 服务，`run --rm` 按需拉起）。
**检测信号**：`make verify` 的 Doris 断言执行位置。
**判定条件**：断言**经 `client` 服务**执行并返回预期标量；在宿主直接跑即为**失败**。

### R3 · ★ P0 环境阻塞：JDK 17 缺失 + Maven 低于官方要求
**证据**：本机实测三处常见目录**无 JDK 17**；宿主 PATH 上是 **Oracle 18.0.2**、`JAVA_HOME=E:\space\09-tools\Jdk1.8`；WSL 是 **OpenJDK 11.0.32.1**；Maven **3.6.0**（官方要求 **≥3.8.6**）且运行在 **JDK 1.8** 上。而 Flink 2.2 要求 17、**Doris `start_fe.sh` 要求恰为 17**（非 17 `exit 1`）。
**处置**：U-γ（WSL2 内装 JDK 17 + Maven），宿主 Java 面**一概不动**。
**检测信号**：`java -version`（WSL 内）；`start_fe.sh` 的 `ERROR: The jdk_version is N, must be 17.` 直接 `exit 1`。
**判定条件**：WSL 内 `java -version` 为 17 且 `mvn -version` ≥3.8.6 且指向该 JDK；Flink 作业 jar 构建成功。

### R4 · 执行面落点（**已由 P0 实测裁决，并推翻了一轮错误推论**）
**证据**：P0 实测（2026-10-19 20:29）——**Docker 发布端口** `-p 127.0.0.1:18080/18081` 从 WSL2 内 `curl` **可达（200，3/3 次 + 双端口复现）**；**普通 Windows 监听** `:10724` 从 WSL2 **不可达**。此前「一律不可达」的结论源于用**等价监听做代理测试**，该代理**不成立**（详见 §4.7）。
**定案**：单一执行面 WSL2，A 面探测**直接用 WSL 内 `curl`/`python3`**；**「必须用 `python.exe`」这条约束已撤销**。
**残留风险（真实且已踩过一次）**：A 面可达**依赖 WSL integration 处于开启**（现 `/var/run/docker.sock` 存在、Server 29.8.0）。集成若被关，`curl` 会得到 `000`——**形态与「容器没起来」相同**，仍会误导。
**检测信号**：§4.7 的两条检查（已发布端口返回非 `000`；`/var/run/docker.sock` 存在）。
**判定条件**：两条同时成立。**若 socket 消失 ⇒ 须重新裁决，不得沿用。**
**教训（写进流程）**：**代理测试必须用真实同类对象复验**，否则会把「代理的性质」当成「被代理对象的性质」。

### R5 · 磁盘预算与官方口径冲突 + Paimon `file://` 峰值未实测
**证据**：Doris 官方 dev/test 磁盘口径 **FE 10 GB+ / BE 50 GB+**，与 `SRS.md` 的 ≤20 GB 正面冲突；Paimon 物理删除**只在快照过期时**发生（默认 `snapshot.time-retained=1h` 内所有历史版本保留），主键表在 compaction 前同一 key 有**多份物理版本** ⇒ **空间放大与写入放大都是真实的**。
**处置**：容器数据与镜像层**已在 F:**（实测）；须**显式设 `UV_CACHE_DIR`**；20 GB 是**低于官方口径的设计值**。
**检测信号**：`df` / 卷使用率；Paimon `<table>$snapshots` 与 `$files`（**须走 Flink SQL**，Doris 不在系统表支持列表内）。
**判定条件**：P1/P2 用**真实写入量**验证磁盘峰值 ≤20 GB——**未实测即未通过**。

### R6 · R3 降级阶梯的第二级已改写：**Superset 减服务**（不是换工具）
**证据**：① `FSR.md` §8 R3 原含「Superset→Grafana」，但该级被否——**理由不是 AGPL**（第 13 条条件从句是「if you **modify** the Program」⇒ 本机自用无义务），而是 **`SRS.md` §6.1**（Grafana OSS 官方自证 alert engine 与 UI/数据源代理**同进程**）；② 候选七项**至少各有一条判据天然做不到**：Metabase / Redash 死在**刷新档位最小 1 分钟**（`REQ-F-BI-02` 要求 ≤10 s）、Evidence 死在**无 MySQL 直连 + 无自动刷新**、Lightdash 死在**无 MySQL**、Zeppelin/sqlpad 不是看板工具。
**改写为**：R3 的第二级从「换工具」改为「**Superset 减服务**」——
| 级 | 动作 | 一手依据 | 预算变化 |
| --- | --- | --- | --- |
| 降配 A（首选） | 去掉 `superset-worker` + `superset-worker-beat`；`SUPERSET_LOAD_EXAMPLES` 不设 | 官方 `config.py` 原话「Set celery config to **None** to disable all the above configuration」（`CELERY_CONFIG = None`）；官方 `docker-init.sh` 用 `if [ "$SUPERSET_LOAD_EXAMPLES" = "yes" ]` 才加载示例 | ≈ −0.3~−0.5 GB（**估算，官方未给内存数字**） |
| 降配 B | 再合并元数据库（外部 DB 替掉独立 `db` 服务）；缓存留 `NullCache` | `config.py` 默认 `NullCache` + `SQLALCHEMY_DATABASE_URI` 可指向任意 DB | ≈ −0.2~−0.3 GB（**估算**） |
| 降配 C | 单容器合并（web + init 同容器） | 官方 init 与 app 用**同一 build target**（`x-common-build`） | 少一次性容器的启动峰值 |
**为什么这条改写是对的**：它是**唯一「什么都不用重做」的降级方式**——`import-dashboards` 交付形式、`GET /health` 纯文本 `OK`、`127.0.0.1` 绑定、脏率面板、`ecom_ads` 直连**全部不变**；零锚点改动、零接缝重做。
⚠️ **必须照抄进文档的官方免责**：`docker-compose-non-dev.yml` 头部原话「**We don't support docker compose for production environments.**」
**检测信号**：`docker stats` 单项与总值。
**判定条件**：降级后链路仍满足 `REQ-F-BI-01..04`，且总值仍在天花板内——**未实测即未通过**。

### R7 · ★ P0 实测新发现：三处「官方直觉」与实机不符（**均已踩到并修复**）
**性质**：这三条都不是「可能出问题」，而是**实际跑起来才会暴露的硬事实**，且**上游文档与调研笔记都没抓到**。

| # | 事实（P0 实测） | 后果 | 修复 |
| --- | --- | --- | --- |
| **R7-a** | **Flink 2.2 的配置是 `config.yaml`（YAML）**，不是 `flink-conf.yaml`，**也不吃 `FLINK_PROPERTIES` 那套点号键** | 按调研笔记的 `FLINK_PROPERTIES` 写法配置**完全不生效** | 改用 `docker/flink/conf/config.yaml`（只写差异项） |
| **R7-b** | **不能把空目录 bind 进 `/opt/flink/conf`** —— 会遮住镜像自带的 `config.yaml` | 启动失败：`The Flink config file '/opt/flink/conf/config.yaml' does not exist`（**已踩到**，flink-jm 变 unhealthy） | 改为**挂单文件** `./docker/flink/conf/config.yaml:/opt/flink/conf/config.yaml:ro` |
| **R7-c** | **`apache/superset:6.1.0` 镜像不含任何 DB 驱动**（`psycopg2`/`psycopg`/`mysqlclient`/`pymysql` 全部 MISSING） | ①元数据库**不能**直接用 Postgres（worker 启动即 `ModuleNotFoundError: No module named 'psycopg2'`，**已踩到**）；②`REQ-F-OLAP-01` 的 `ecom_ads`（Doris 走 MySQL 协议）**接不上** | ①元数据库改 **SQLite**（镜像自带 sqlite3 3.40.1，零驱动依赖）；②经**官方机制** `/app/docker/requirements-local.txt`（`docker-bootstrap.sh:35` 逐字确认）在引导时 pip 装 `PyMySQL` |
| **R7-d** | **Kafka 4.3.1 的 `kafka-topics.sh` 不再支持 `--alter` 改配置** | 建 topic 时施加 retention **直接失败**（报错原文：「To alter topic configurations, the **kafka-configs** tool can be used.」，**已踩到**） | 改用 **`kafka-configs.sh --alter --entity-type topics`**；**已证**：两 topic 最终配置为 `retention.ms=604800000` + `retention.bytes=2147483648` + 6 分区 + RF=1，与 `gdd §A.2` 完全一致 |

**检测信号**：`docker compose up` 后各服务 healthcheck；`kafka-topics.sh --describe` 的 `Configs:` 行；Superset 日志里的 `ModuleNotFoundError`。
**判定条件**：骨架**四个服务全部 healthy** 且 `config-check` 全 PASS（**P0 已达成**：Kafka/JM/TM/Superset 均 healthy，断言全绿）。
**教训（写进流程）**：**调研笔记给的是「官方文档说应该怎样」，P0 得到的是「实机实际怎样」**——两者在配置格式、镜像内容、CLI 版本行为上**已经三次不一致**。凡涉及「按某个格式写配置」「镜像里有什么」的结论，**必须实机验证一次**才能进施工路径。

---

## 9. 上游证据

> 本轮 6 份**只读**调研笔记（每条结论带一手 URL；官方未给出的显式写「官方未给出」，未实测的显式标注）。性质同 `research-notes.md`。

| 笔记 | 覆盖 |
| --- | --- |
| [hld-research-toolchain.md](hld-research-toolchain.md) | Python 运行时线、uv、Ruff、ty、Kafka 客户端、schema 校验、Flink 工程栈、编排工具 |
| [hld-research-datalayer.md](hld-research-datalayer.md) | Paimon/Doris 语义复核、替代面决定性证据、catalog 与存储载体 |
| [hld-research-observability.md](hld-research-observability.md) | 观测性候选、「看 vs 判」二分、§C 信号 → 可机读断言映射 |
| [hld-research-sandbox.md](hld-research-sandbox.md) | 运行面沙箱四候选、资源约束生效层、端口与网络、**Stream Load 硬约束**、会话沙箱三模式 |
| [hld-research-budget.md](hld-research-budget.md) | 逐组件官方内存默认值、两套预算重算、R3 候选事实核查、磁盘落点裁决 |
| [hld-research-bi-fallback.md](hld-research-bi-fallback.md) | R3 降级阶梯的看板替代项（七候选逐项裁决 + **Superset 自身减服务**路径） |
| [hld-research-pyright.md](hld-research-pyright.md) | 类型门禁候选（`ty` 对 `pyright` 对 `basedpyright`：稳定性契约、打包、机读输出、内存） |
| [hld-research-execution-plane.md](hld-research-execution-plane.md) | 执行面落点的网络事实（`localhostForwarding` 单向性、WSL→宿主可达性**实测**、interop 通道） |

**上游文档**：[SRS.md](SRS.md)（需求权威）｜[FSR.md](FSR.md)（路线冻结）｜[gdd.md](gdd.md)（功能设计）｜[GLOSSARY.md](../../GLOSSARY.md)（术语）
