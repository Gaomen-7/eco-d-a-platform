# HLD — 电商用户行为数据全链路（离线 + 实时）

> **性质**：高层设计文档（High-Level Design）。本文回答「**结构怎么分、模块各管什么、技术栈定在哪些版本、运行环境与沙箱有什么约束、最可能在哪里出架构级问题**」。
> **日期**：2026-10-19（Asia/Shanghai）
> **上游**：`SRS.md`（需求权威）｜`FSR.md`（路线冻结）｜`GDD.md`（功能设计，文件名 `gdd.md`）｜`GLOSSARY.md`（术语）
> **下游**：本 spec 施工规格；**收口**：`canonical-truth.md`
> **权威关系**：`SRS.md` 持「**必须做到什么**」→ `gdd.md` 持「**每个功能怎么做**」→ 本文持「**结构、栈定案、运行约束**」。本文**不覆盖**上游，凡与上游冲突处一律在 §7 显式登记；事实类冲突可直接改写上游（见 `canonical-truth.md` §0 G1），意图类保留原话并登记（`gdd.md` 中被本文调研证伪的条目已就地修订，见 §7）。
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
canonical-truth.md  收口  ↔  本 spec 施工规格（spec.md）
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
| 1 | **事件契约** | 模块①（生产者）/ 模块③（消费者校验） | 字段名、枚举、`schema_version` 语义；校验结果的**机读原因码**。**`schema_version` 规则语义的唯一所有者＝`canonical-truth.md` §3.1**（四条铁律 S1–S4 的权威全文只在那里；SRS §3.3.1 承载字段语义、gdd 各写本模块义务；本节只持指针，不复制全文，防第二真相源） |
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
>
> ★ **可复现的边界另见** [`reproducibility-boundary.md`](reproducibility-boundary.md)（票 [04] 产物）：主体＝制品而非平台、**四档契约 L0–L3**、「一条命令」＝`make init` 的四段、`client` 的两平台必需性、**能力×平台成立性表**、以及三份 P0 交接的合流清单。本节只写运行面与沙箱约束，**不重复那张表**。

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
| kafka | 1g | **647.8 MiB** | 63% |
| flink-jm | **2g**（2026-10-23 由 1g 上调，见下注） | **456.3 MiB**（1g 时的实测） | 22% |
| flink-tm | 2g | **567.9 MiB** | 28% |
| superset | 1g | **259.4 MiB** | 25% |
| **小计（四服务实测）** | — | **≈1.89 GiB** | — |
| `doris-fe`（profile `doris`） | 2g | **994.3 MiB** | **49%** |
| `doris-be`（profile `doris`） | 3g | **995.2 MiB** | **32%** |
| **小计（含 doris，六服务全起）** | — | **≈3.83 GiB** | — |
| `kafka-init`（一次性） | 256m | 不常驻 | — |
| `client`（profile `client`，`run --rm`） | 256m | 不常驻 | — |
| **声明上限合计（默认 up）** | — | **6.25 GiB** | 天花板 11.68 ⇒ 余 **5.43** GiB |
| **声明上限合计（--profile doris）** | — | **11.5 GiB**（11,776 MiB；2026-10-25 impl-05 订正，见下注） | 天花板 11.68 ⇒ 余 **0.18** GiB |

> **★ 两个口径别混用（P0 实测澄清）**：`scripts/config_check.py` 的「常驻上限合计」按**「有 `mem_limit` 且**无** `profiles` 键」**统计 ⇒ 它报的是 **6.25 GiB**（**不含** profile 隔离的 Doris）。而「全部服务声明上限」含 Doris 的 2g+3g。**两个数都对，只是口径不同**——写文档时必须说明用的是哪一个。
> **读法（重要）**：`mem_limit` 是**上限**不是占用。实测值远低于上限，说明当前**空载**骨架很轻；**真正的压力在 P1 有数据流之后**——Flink 的 managed memory 会随 RocksDB 状态增长、Doris 的 BE compaction 会吃内存。故「6.25 / 10.5 GiB」是**已声明的上限契约**，「1.89 / 3.83 GiB」是**当前空载实测**，两者不可互相冒充。
>
> ★★ **`flink-jm` 由 1g 上调到 2g**（2026-10-23，impl-03c 实机证伪）：`make replay` 不是「在宿主跑 SQL」，而是
> `docker exec eco-flink-jm /opt/flink/bin/sql-client.sh -f /sql/…` ⇒ **sql-client 的 JVM 与 JobManager 的 JVM 住同一个 cgroup**。
> 1g 只够 `jobmanager.memory.process.size: 1024m` 这一项，客户端只能靠内核挤余量 ⇒ 跑到 ADS 层（最重的一层）时
> 容器被 **cgroup OOM-kill**（`oom-kill:constraint=CONSTRAINT_MEMCG`），`docker exec` 返回 **137**、JM 被 `restart` 拉起。
> ⚠️ 这个症状**不会**置 `.State.OOMKilled=true`，现场看起来像「SQL 报错」或「TaskManager 掉线」——故已加**静态守卫**
> `tests/test_deployment_memory.py`（判据：`mem_limit >= jobmanager.memory.process.size + 512MiB`）。
> 512 MiB 是**设计值**（本仓给的客户端余量，不是官方最小堆）。
>
> ★ **Doris 实测已回填**（2026-10-20，U-ν 完成）：六服务**全起**时 `docker stats --no-stream` 合计 **≈3.83 GiB**，距 11.68 GiB 天花板余 **7.85 GiB**。判据 `curl :8030/api/health` 实测返回
> `{"msg":"success","code":0,"data":{"online_backend_num":1,"total_backend_num":1}}` ⇒ **`online_backend_num == total_backend_num == 1`**，R1 的「Doris 能在下调后稳定供数」这一半**已实测通过**（`SHOW BACKENDS` 的 `Alive=true`；三张 ADS 表建成并已有 30 天动态分区）。
> ⚠️ **口径更正（本条为实测修正，勿沿用旧数）**：旧表「--profile doris = 10.25 GiB」少算了 `client` 的 256m ⇒ 实为 **10.5 GiB**（`10,752 MiB` = 10.5 GiB），余量随之由「1.43」更正为 **1.18 GiB**。`client` 虽以 `run --rm` 按需起、**不常驻**，但计入「全部服务声明上限」口径。此更正是**修正而非估算**：`docker compose --profile doris --profile client config` 求和得 11,274,289,152 B。
> **仍待回填**：**P1 带载后**的实测值（当前是空载）。


| 约束 | 定案 | 检查方式 |
| --- | --- | --- |
| 逐服务显式 `mem_limit` | **每个服务都写**，且**同时写 `memswap_limit` ＝ `mem_limit`** | ✅ **P0 已证**：`config-check` 断言全 PASS（8 服务逐条比对） |
| 不与 `deploy.limits.memory` 并存 | **只写 `mem_limit`** | 官方：`deploy` 是**可选段**（未实现即忽略）；二者并存时官方要求**必须一致**，写错一个即自相矛盾 |
| 「不允许有服务缺省」 | 由 `make verify` / `config-check` **脚本断言** | ⚠️ **P0 实测确认**：`docker compose config` 能**列出** `mem_limit` 但**不报缺** ⇒ 判据必须脚本化。规则已写进 `Makefile` 的 `config-check` 目标 |
| 冷启动/体量记账 | **用 `docker image inspect`**，不用 `docker images` | 本机 `UseContainerdSnapshotter=true`，官方警告此时 `docker images` 会**把压缩 blob 叠加进读数、显著虚高**。**实测镜像体积**（`image inspect`）：Kafka 654.1 MiB / Flink 1491.4 MiB / **Superset 1207.8 MiB** / Doris base-latest 1.96 GB / **`eco/doris:4.1.5`（自建）12.60 GiB**（13,526,796,833 B，2026-10-20 实测）/ `mysql:8.0`（`client` 用）1.01 GiB |
| 常驻预算 | 见下表。**声明的 `mem_limit` 上限合计**：默认 `up` = **6.25 GiB**；`--profile doris` = **11.5 GiB**（11,776 MiB，含 `client` 256m；2026-10-25 impl-05 订正，见下注）⇒ **两种场景都在 11.68 GiB 天花板内**（余量 5.43 / **0.18** GiB） | `docker stats --no-stream` 实测；`make config-check` 断言。⚠️ 统计口径见下注 |

> ★ **Doris 自建镜像体积的读法（2026-10-20 实测）**：`eco/doris:4.1.5` = **12.60 GiB**（`image inspect`），是 Kafka+Flink+Superset 三者之和（**3.28 GiB**）的 **3.8 倍**。这是「Doris **必须本地构建**」（官方不发版本化镜像）的体积代价，也是「零状态冷启」档豁免 30 分钟判据的根因之一（另一根因是 ≈35 min 的构建耗时）。详见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §3。
> ★★ **口径核实已结案（2026-10-20，结论与旧猜测相反）**：U-ν 旧文的 `docker images` = **13.5 GB** 与本次 `image inspect` 的 **12.60 GiB（＝13,526,796,833 B ＝13.53 GB 十进制）** **一致** ⇒ **旧读数不是 containerd snapshotter 造成的虚高**。规则仍照旧（**记账一律用 `image inspect`**），但引用时**必须带单位**（GiB 与 GB 差 7%，这才是本例真正的坑）。
> ⚠️ ~~本表「`--profile doris`」一行的数字是旧值~~ **已订正（2026-10-21，施工票 impl-01）**~~：本表现在写的就是正确值 **10.5 GiB / 余 1.18 GiB**（与上方「★ 口径更正」段及 `canonical-truth.md` §4.3 **F-1** 一致）；全仓**只以本节为该数字的真相源**，旧数 10.25/1.43 已清除。~~（**本行自称「真相源」的宣称已被下方 2026-10-25 订正取代**——flink-jm 2g ⇒ 11.5 GiB / 余 0.18 GiB；原文保留作历史。）
>
> ★★ **再次订正（2026-10-25，施工票 impl-05）**：上方 2026-10-21 订正的 **10.5 GiB / 余 1.18 GiB 已过期**——impl-03c（2026-10-23）把 `flink-jm` 的 `mem_limit` 由 1g 上调到 2g（cgroup OOM 实机证伪，见上方 ★★ 段），「全部服务声明上限」（含 profile 与 `client`）随之 **10.5 GiB（10,752 MiB）→ 11.5 GiB（11,776 MiB）**，余量 1.18 → **0.18 GiB**。两张表已同步改写，旧数 10.5/1.18 保留在本注与 impl-01 订正注里作历史留痕。**声明口径的两个数字**：默认 `up` = 6.25 GiB（未受影响）；`--profile doris` = 11.5 GiB。
> ⚠️ **与 SRS 判据的张力（✅ 已结案，2026-10-25 用户追认·spec §7.1 #41）**：`REQ-NF-CAP-01` / `REQ-F-ORCH-02` 判据①原写「声明上限（含 profile 内服务）≤ **8 GB**」，与同段签字句承认的声明合计（10.5→现为 11.5 GiB）自相矛盾。声明口径压回 8 GB 须把经实机证伪钉死的下限（`flink-jm` 2g、`doris-be` 3g、`doris-fe` 2g——后者受 `-Xmx1536m` 约束）再压 ~3.5 GiB，**会破坏已验证的部署面**，判定为**结构性不可达**。**裁决**（原临时处置转正）：**8 GB 作为实测占用（口径②）的目标断言，声明口径（口径①）以 11.68 GiB 天花板为界、如实给数**；SRS 两行已按逐字新句改写，`reproducibility-boundary.md` §11.1 标已签。


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
- compose 里**声明**一个 `client` 服务（持 `mysql` 与 `curl`），但**不常驻**——以 `docker compose run --rm` 按需拉起，从而**不占常驻预算**。
  - **拓扑（2026-10-20 订正）**：`client` 的 `depends_on` **只等 `kafka`**（`docker-compose.yml:314-316`）。本节旧说法「`depends_on` **全链路** `service_healthy`」**是错的**，照它实现会**成环**——`client` 等 Doris healthy，而 Doris 的库表就绪要走 `doris-init.sh`，该脚本只能在 `client` 内跑。
  - **不成环判据（可机读）**：`client` **不得出现在任何服务的 `depends_on` 被依赖侧**。即 `docker compose --profile doris --profile client config --format json` 的展开结果里，**没有任何服务的 `depends_on` 键包含 `client`**。
  - 其余等待由 **`doris-init.sh` 自己轮询**（`SHOW FRONTENDS` 与 `SHOW BACKENDS` 的 `Alive=true`，各 60×3 s）——这类等待表达不了成 compose 依赖，写进脚本才对。
  - ⚠️ **适用范围（2026-10-20 补，官方原文）**：该两跳约束是 **Docker Desktop 专属**。Doris 官方 README 同段写着「**Linux hosts can reach the nodes directly.**」⇒ **原生 Linux 上 `client` 非必需**（可直连 FE）。故本节「必须」只在 Docker Desktop 面成立；能力×平台判定见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §5 第 7 行。
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

> ★ **本节曾在 §4.8 尾部被逐字重复粘贴（2026-10-20 合并，已删重复）**。留此注释，防止再被复制到别处。

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
- ⚠️ **口径（2026-10-20 补）**：「单一执行面＝WSL2」是**本机执行面事实**，**不是「可复现承诺」的一部分**。可复现的主体是**制品**（compose + 锚点 + 相对路径纪律 + 契约），执行面与主机平台**不进入承诺**——依据见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §0 与 §5 的跨平台成立性表。**不要把本节读成「只能在这台机器上复现」**。

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

> ⚠️ **乙·会话沙箱的完整论述见 §4.6**——本节尾部曾**逐字重复粘贴**了同一段（原 §4.8 尾部），2026-10-20 合并、删去重复，只留本行指针。

---

## 5. 数据契约（冻结面清单）

> **冻结面＝不可动**（改了就是破坏下游）；**设计面＝HLD 可定**（目录结构、模块内实现、脚本命名、容器内进程布局、错误码表、校验结果结构）。

| 类别 | 冻结值 |
| --- | --- |
| Kafka topic | `ecom.behavior.event.v1`；DLQ `ecom.behavior.event.v1.dlq` |
| 消息格式 | JSON + `schema_version`；字段一律 **`snake_case`**；兼容规则见 `SRS.md` §3.3.1（语义唯一所有者＝`canonical-truth.md` §3.1） |
| Paimon | 单一 Paimon catalog，其下 **`ecom_lake` database**（见 `REQ-F-LAKE-06`，2026-10-21 订正）；表 `ods_behavior_event` / `dwd_behavior_event` / `dwd_behavior_event_reject` / **`dwd_late_event_count`**（FC-2，2026-10-20 已批）/ `dim_*` / `dws_*` / `ads_*` |
| Doris | 看板直连库 `ecom_ads` |
| Makefile 目标 | `up` / `down` / `init` / `replay` / `verify` / `verify-fault` / `spike` / `logs` ＋ **`lint` / `typecheck`**（FC-1，2026-10-20 已批；静态检查与运行时烟测分开，不新增 `check`） |
| 端口 | 见 §4.3（**全部仅绑 `127.0.0.1`**） |
| 事件字段与枚举 | `SRS.md` §3.3 字段表；五类枚举 |
| 参数数值 | **`gdd.md` §A 是唯一权威**（本文不复制，避免第二个真相源） |

**设计面（本文可定）**：目录布局、Flink 作业内部结构、`client` 服务的实现、校验结果结构的**内部形状**、脚本命名。

**P2 设计面新增（[05] 定案，2026-10-21 随 impl-01 落笔）**：

- **`paimon-warehouse` named volume**：Paimon warehouse（`/warehouse`）落具名卷，**同路径挂进 JM 与 TM 两个容器**（本仓 JM/TM 不互通；官方 `file:` 背书仅 "single-machine tutorial"）。检查：`docker compose config` 可见两处同名卷挂载。
- **Paimon jar 构建期 COPY**：Paimon bundled connector 构建期 `COPY` 进**每个** Flink 进程的 `/opt/flink/lib`（官方逐字 "Install one matching Paimon bundled connector in **each** Flink process."；`ADD JAR` 宿主路径不可用）。检查：JM/TM 容器内 `ls /opt/flink/lib` 均见 jar。
- **`sql/replay_*.sql` 目录布局**：`sql/replay_00_catalog / 10_ods / 20_dwd / 30_dim / 40_dws / 50_ads / 90_maintain` ＋ `sql/assert_lake.sql`，`sql-client.sh -f` 串联；`LAYER=ods|dwd|dim|dws|ads` 缺省跑全部。检查：文件存在且 `make replay` 依赖序与编号一致。
- **`artifacts/verify/` 报告目录**：`verify`/`verify-fault` 逐断言状态写 `verify-report-<UTC时间戳>.json`，FAIL 时原始响应快照同目录；目录进 `.gitignore`。检查：跑一次 `verify` 后文件存在。
- **`scripts/lake_assert.sh`**：Paimon 断言执行面（`sql-client -f` 退出码不可用 ⇒ 解析 stdout 哨兵，判「无 `[ERROR]` 且每条哨兵 `ok=true`」）。检查：人为让一条断言失败时脚本退出码非 0。

---

## 6. 失败路径与可机读断言

**权威归属 `gdd.md` §C**（失败路径登记表）与 **§1.8**（五项烟测的确切断言形式）。本文**不复制**，只登记**升级原则**：

**「看」与「判」的二分**：只有**确切字段 / 退出码**算合格判据——JSON 字段（`jobs[].state`、`online_backend_num`）、纯文本定值（Superset `/health` 的 `OK`）、Prometheus 文本指标名、`docker stats --format json`、SQL 标量。**「Flink UI 里能看到」不是判据。**
**§C 的 13 条检测信号 → 10 条可零新增常驻组件判定**（见 §3.6）。

---

## 7. 偏离与待收口

> 本节**暴露**而非覆盖：按 `canonical-truth.md` §0 G1，**事实类冲突可直接改写上游**（已于 2026-10-21 由施工票 impl-01 按 spec §⑦ 统一落笔）；意图类保留原话并登记。被本轮调研**证伪**的 `gdd.md` 条目已**就地修订**。

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
| **D-H** | `SRS.md` `REQ-NF-CAP-04`：「**冷启动（含拉镜像）≤ 30 分钟**」 | ❌ **原口径不可达**：它把「构建 Doris 镜像」隐含在「拉镜像」内，而 Doris **无官方版本化镜像**（`apache/doris:4.1.5` / `all-in-one-4.1.5` / `base-4.1.5` 全部 `denied`）⇒ 必须本地构建，实测 **≈35 min**（`设计值`/实测），**单构建一步就超 30 min**，还没算拉 Kafka/Flink/Superset | 登记；改写为「**预构建镜像经 `docker save`/`load` 导入后**冷启动 ≤30 min」，构建耗时单独记录、**不设上限**（逐字新句见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §8.3；**已签字**，**已于 2026-10-21 落笔进 SRS**）。同批登记：`REQ-NF-CAP-03` 原口径「一条命令到看板出数 ≤15 分钟」把出数隐含在链路 healthy 内，已拆为「到全 healthy ≤15 min（不含出数）＋第二截出数判据转 P1 实测」（逐字新句同上 §8） |
| **D-I** | `SRS.md` `REQ-F-ORCH-01`：「`docker compose up` 一键拉起整条链路（**含初始化脚本**）」 | ❌ **该「一条命令」在仓库里不存在**：① **无 README**（`REQ-NF-MNT-03` 要求有）；② `docker compose up` **不含 Doris**（在 `profiles: ["doris"]`）；③ `make init` 自己写着「**待补**：Doris 建库/建表、Superset db upgrade + create-admin + init」（`Makefile:51`） | 登记；「一条命令」**具名为 `make init`** 并冻结四段内容（逐字新句见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §8.1；**已签字**，落笔归 **[09]**）。`init` 是冻结目标名 ⇒ 不触发冻结项变更 |

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
| **U-γ** | WSL2 内装 **JDK 17 + Maven ≥3.8.6** | ✅ **已完成（无需 sudo）** | 走的是「`.deb` 用户级解包」路径，**未动宿主 Java 面未用 sudo**；回滚＝删 `~/p0-toolchain` + `~/.p0-toolchain-env.sh` |
| **U-δ** | WSL2 内 `apt install make` | ✅ **已完成（无需 sudo）** | 同上，make 4.4.1 随同一批 .deb 解包；回滚同上 |
| **U-ε** | Docker Desktop 对 Ubuntu 开 **WSL integration** | ✅ **已生效**（实测 `/var/run/docker.sock` 存在、WSL 内 `docker version` 返 Server 29.8.0；设置里 `IntegratedWslDistros: ["Ubuntu"]`） | 回滚＝同页关闭；**关闭后 §4.7 的 A 面可达性须重测** |
| **U-ζ** | 设 **`UV_CACHE_DIR`** 到 F: | ✅ **已执行**（Windows 用户级环境变量 → `F:\...\.uv-cache`；备份 `%USERPROFILE%\.env-backup-2026-10-19-before-uvcache.txt`） | 回滚＝删除该环境变量。WSL 侧 uv cache 本就在 Linux 文件系统内，无需改 |
| **U-η** | Ubuntu 发行版 `ext4.vhdx` 是否迁出 C 盘 | **待定** | 体积**未测**；迁移＝`wsl --export/--import`，属动本机配置 |
| **U-ν** | **Doris 镜像本地构建** | ✅ **已完成（2026-10-20 实测）**。`eco/doris:4.1.5` 已建成并跑通（**`docker image inspect` = 12.60 GiB ＝ 13,526,796,833 B**，2026-10-20 复测）。路径＝**构建内下载**（Dockerfile 单条 `RUN curl -xzf` 下载 4.32 GB 二进制包）＋修 5 处缺陷。**实测**：下载 **457 s**（≈9.4 MB/s）、解压 **712 s**、导出/解包 **862 s**，全程约 **35 分钟**；上下文由 4.03 GiB 降至 **6.13 kB**（`.dockerignore`）。见本行下方「U-ν 修复清单」 |
| **U-θ** | `uv python install 3.12` | 已批 | 回滚＝`uv python uninstall 3.12` |
| **U-ι** | 安装 **`basedpyright==1.40.2`**（`uv add --dev`） | **待批** | 回滚＝`uv remove --dev basedpyright` + 重生成 `uv.lock`；含写 `[tool.pyright]` 段的回滚（删除该段） |
| **U-κ** | 若走包装器兜底，设 **`PYRIGHT_PYTHON_CACHE_DIR`** 到 F: | **待批** | 选 `basedpyright` 则基本不需要（node 由 wheel 提供）；保留以防兜底 |

**★ U-ν 修复清单（P0 实测，2026-10-20；每处都有实机证据）**

| # | 缺陷 | 根因 | 修法 | 复验判据（均已实测通过） |
| --- | --- | --- | --- | --- |
| **A** | FE 必 `exit 1` | Dockerfile 内 `ENV JAVA_HOME` **重复赋值**，后者把正确的 `/usr/lib/jvm/jdk-17` 覆盖成**不存在**的 `/usr/lib/jvm/java-17-openjdk-amd64` | 删掉第二次赋值，注释警示 | 镜像内 `echo $JAVA_HOME` = `/usr/lib/jvm/jdk-17`、`test -x $JAVA_HOME/bin/java` ✅ |
| **B** | `run client <cmd>` 的参数被吞 | `entrypoint: ["/bin/bash","-c","sleep infinity"]`：`bash -c` 只把**紧跟的单个参数**当脚本文本，其余进 `$0` ⇒ 多词命令全丢 | `["/bin/bash","-c","exec \"$@\"","dummy"]` + `command: ["sleep","infinity"]`（**双元素**） | 三档实测：默认常驻 ✅；单词脚本 ✅；**多词 `mysql -h doris-fe ...` 参数透传**（报 `ERROR 2005 Unknown host 'doris-fe'`＝已按主机名解析，**不再**退 `mysqld.sock`）✅ |
| **C** | 初始化 SQL 路径不存在 | 三方不一致：脚本默认 `/scripts/doris/init.sql`、compose 只挂 `./docker/scripts`、文件实在 `docker/doris/init.sql` | compose 加 **`./docker/doris:/doris:ro`**，脚本默认改 `/doris/init.sql`。⚠️ **不能**挂 `/scripts/doris`：`/scripts` 是只读绑定，runc 无法在其内建挂载点（实测 `mkdirat ... read-only file system`，容器**直接起不来**） | `ls /doris` 见 `init.sql` ✅ |
| **D** | BE 从未注册 ⇒ 无可用 backend ⇒ 建表必失败 | 无人发 `ALTER SYSTEM ADD BACKEND`（官方部署 Step 3.3 要求；官方 all-in-one `run_be()`＝先注册后起进程） | `entrypoint.sh` 在**起 BE 进程前**用**容器 IP** 注册并等 `Alive=true` | `SHOW BACKENDS` `Alive=true`；`:8030/api/health` `online_backend_num=1 == total_backend_num=1` ✅ |
| **E** | **BE 必 `exit 1`，根本起不来**（★ 本会话新查出，两份旧交接均无） | `be/bin/start_be.sh:194-216` 起进程**前**检查 `vm.max_map_count ≥ 2000000`、`swapon -s ≤ 1 行`、`ulimit -n ≥ 60000`。本机 VM 实测 `max_map_count=1048576`、`swapon` 2 行 ⇒ **双双不过**；而 `max_map_count` **非 namespaced sysctl**，容器内改不了 | Dockerfile 显式 `ENV SKIP_CHECK_ULIMIT=true`（官方 all-in-one 同款做法；官方原注：*"vm.max_map_count is not a namespaced sysctl, so a container cannot fix it -- the check has to be skipped here"*） | BE 容器 `Up (healthy)` 而非 `exit 1` ✅ |
| **F** | Doris 的 compose env 是**死配置** | 对真 tar 包 `fe/bin`+`be/bin` **整目录 grep** `FE_SERVERS`/`BE_ADDR` ⇒ **零命中**；`start_be.sh` 也不吃 FE 地址（无 `--helper`）⇒ 原设的 `FE_SERVERS`/`FE_ID`/`BE_ADDR` 无人消费 | 删死配置，改设 entrypoint 真正消费的 `FE_HOST`/`FE_PORT`/`FE_USER`/`BE_HEARTBEAT_PORT`/`START_TIMEOUT` | `docker compose config` 解析出的 env 与实际消费一致 ✅ |
| **G** | `ads_funnel_step` **必然建表失败**（★ 本会话实测暴露） | UNIQUE KEY `(session_date, step_no, window_start)` 不是列声明的有序前缀，Doris 报 `Key columns should be a ordered prefix of the schema` | **保持作者原 Key 顺序不变**，只把列声明调成同序（三列前置）⇒ Key 顺序与分区键均未变，仅声明位置变化 | `init.sql` 全量执行至 `[doris-init] 完成`，三表建成 ✅ |
| **H** | **`down`→`up` 后 FE 永久起不来**（★ 本会话在「重启复现性」复验中查出；**首次构建时不会暴露**，故极易漏） | Doris 把**自身 IP** 写进持久化元数据（`doris-meta`）：首启日志逐字 `initAddrUseIp(): local address: /172.22.0.6`、`getSelfHostPort(): HostInfo{host='172.22.0.6', port=9010}`。而 Docker 重建容器会**重新分配** IP（实测重建后变 `172.22.0.3`）⇒ 元数据里的地址与实际的自己对不上，FE 卡在 `Env.waitForReady(): feType:UNKNOWN isReady:false` **无限循环**（healthcheck `FailingStreak=18`） | **重定义 default 网络为固定子网 `172.31.88.0/24`，给 FE/BE 钉死 IP**（`172.31.88.40`/`.50`）——官方 `multi-node.yml` 同款，官方原注："Node addresses are pinned inside the private network, so a restarted container keeps the identity Doris knows it by." ⚠️ **只重定义、不新建网络**：其余服务不写 `networks:` 键 ⇒ 仍自动加入同一 default 网络 ⇒ **拓扑不变**，Flink TM 照样能按容器地址连 BE（保住 HLD §4.4 的 P1 写入路径） | `down`（**不删卷**）→`up` **两轮**：FE 每次 `health=healthy`、`fails=0`、IP 恒为 `172.31.88.40`；`:8030/api/health` `online_backend_num=1`；三张 ADS 表**重启后仍在** ✅ |

> ⚠️ **本会话自犯并已修正的两个坑**（记入教训，防复发）：
> ① **`mysql -N` 会抑制表头** ⇒ 按**列名**取值的 awk（`col["Alive"]`）因表头缺失而**恒假**，把「BE 明明 Alive」误判为「未注册」。凡按列名取值**绝不能带 `-N`**（官方 `lib.sh` 的 `node_alive` 亦不带）。
> ② **注册绝不能用主机名**：BE 已用容器 IP 注册后，再用 `doris-be:9050` 去 ADD，Doris 视其为**另一身份** ⇒ 注册出**幽灵 backend**（实测 `SHOW BACKENDS` 同时出现 `172.22.0.7` 与 `doris-be` 两行，后者 `TabletNum=0`）。**必须统一用容器 IP**（`enable_fqdn_mode` 默认 **false**，已由 `Config.java` 源码逐字确认：`public static boolean enable_fqdn_mode = false;`）。

> ★ **方法论收获（本会话新增，建议并入 §8-R7 教训栏）**：**「能起来」≠「能重起来」**。
> Bug A–G 都只在**首次** `up` 的路径上暴露，一路全绿；只有**刻意做 `down`→`up` 循环**才逼出 Bug H。
> ⇒ 凡「把状态持久化进卷」的组件（Doris FE 元数据即典型），验收判据**必须包含一次
> **「不删卷的重启周期」**，否则交付的是「一次性可跑」而非「可复现」。
> 本条与既有纪律（①配置格式②镜像内容③数据落点④CLI 版本行为**都必须实机验证**）同级。

---

## 8. 架构风险

> 每条附**检测信号**与**判定条件**（§0.4 的行为可证纪律）。按「会不会让链路本身跑不起来」排序。

### R1 · ★ Doris 出厂默认参数与天花板正面冲突
**性质**：**已源码级证实**的确定性冲突，缺口 ≈5 GB。
**证据**：`conf/fe.conf` 逐字 `-Xmx8192m -Xms8192m`（FE 单进程默认堆 ＝ **整个 8 GB 预算**）；BE `mem_limit` **默认 90%**（11.68 GB 天花板下 ≈10.5 GB）；两者相加 ≈18.5 GB；官方 dev/test 口径 **FE 8 GB + BE 16 GB**。
**处置**：P0 **必须**显式下调（FE `-Xmx`、BE `mem_limit`），并把预算表每项标 `官方给出`/`设计值`；`SRS.md` 的 ≤8 GB **不动**（按 §7.1 D-B 登记）。
**检测信号**：`docker stats --no-stream` 各服务与总值；Doris FE 启动日志（`fe.out`）。
**判定条件**：下调后 Doris 能在 `docker stats` 实测下**稳定供数**（`/api/health` 返回 `online_backend_num == total_backend_num` 且 ADS 表可查）——**未实测即未通过**。
**★ 2026-10-20 实测进度**：**下调有效（空载已通过）**。镜像内 `fe.conf` 确认 `-Xmx1536m -Xms1536m`、`be.conf` 确认 `mem_limit = 25%`（BE 启动日志实测 `Mem Limit: 768.00 MB, origin config value: 25%`）。六服务全起实测合计 **≈3.83 GiB** / 天花板 11.68 GiB；`/api/health` `online_backend_num=1 == total_backend_num=1`、`SHOW BACKENDS` `Alive=true`、三张 ADS 表建成。⚠️ **仍未完全通过**：判据后半「**稳定**供数」需 **P1 带载后**复测（Flink managed memory 随 RocksDB 状态增长、BE compaction 吃内存）——**带载未实测即未通过**。

### R2 · ★ Doris Stream Load 在 Docker Desktop 下宿主不可 follow
**证据**：官方原话（Doris README）「a stream load from the host **cannot follow FE's redirect to a BE; run it from the `client` service instead**」；官方同时说明「Per-container IP addressing is not possible」。
**处置**：见 §4.4（网内 `client` 服务，`run --rm` 按需拉起）。
**⚠️ 适用范围（2026-10-20 补）**：本约束**只对 Docker Desktop 成立**。官方 README 同段写明「**Linux hosts can reach the nodes directly.**」⇒ 原生 Linux 上可直连 FE/BE，`client` **非必需**。
**检测信号**：`make verify` 的 Doris 断言执行位置。
**判定条件**：断言**经 `client` 服务**执行并返回预期标量；在宿主直接跑即为**失败**。
**⚠️ 实现现状（doc ≠ reality，本票只登记不改）**：`Makefile:76-78` 第 4/5 项**在宿主侧** `curl 127.0.0.1:8030/api/health`（且只查 FE 健康、不查 ADS 有行）⇒ **当前判据未按本节实现**。归属 **[09]**（并 P1 补真实断言）。见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §9。

### R3 · ★ P0 环境阻塞：JDK 17 + Maven —— **已解除（且无需 sudo）**
**原阻塞证据**：本机实测三处常见目录**无 JDK 17**；宿主 PATH 上是 **Oracle 18.0.2**、`JAVA_HOME=E:\space\09-tools\Jdk1.8`；WSL 是 **OpenJDK 11.0.32.1**；Maven **3.6.0**（官方要求 **≥3.8.6**）且运行在 **JDK 1.8** 上。而 Flink 2.2 要求 17、**Doris `start_fe.sh` 要求恰为 17**（非 17 `exit 1`）。
**★ 处置（P0 实测，2026-10-19）——绕过 sudo 的路径**：WSL 侧 `sudo` 需**交互密码**（agent 不能代劳、也不该猜密码），但三项工具**无需 sudo** 即可用：
```
apt-get install --print-uris → 取 .deb URL → curl -4 下载 → dpkg-deb -x 解包到 ~/p0-toolchain
```
**实测结果**：**JDK 17.0.20.1** / **Maven 3.9.12** / **make 4.4.1**，全部可用。
**踩到的坑（已修）**：`.deb` 的布局假设自己装在 `/`，故 `conf/security/java.security`、`maven bin/m2.conf`、`maven conf/` 全是**指向 `/etc` 的悬空绝对链接**；脚本逐个重指到前缀内实体文件后 `javac`、`mvn` 才正常（`scripts/p0-wsl-fix-symlinks.sh`）。
**★ 端到端验证（不是只看版本号）**：Maven **成功解析并下载**了 `gdd §A.3` 冻结的完整坐标 `org.apache.flink:flink-connector-kafka:5.0.0-2.2`（`[INFO] BUILD SUCCESS`，jar 落到 `~/.m2/.../flink-connector-kafka-5.0.0-2.2.jar`）。同时**独立复核**了该坐标：Maven Central 上该版本 HTTP 200，`maven-metadata.xml` 的 `<release>` 就是 `5.0.0-2.2` ⇒ **§7.1 D-C 的坐标精确化得到第三方来源印证**。
**残留**：宿主 Java 面**一概未动**（`JAVA_HOME=E:\space\09-tools\Jdk1.8`、PATH 上 Oracle 18 原样）；工具链只在 WSL 用户级，**回滚 = 删除 `~/p0-toolchain` 与 `~/.p0-toolchain-env.sh`**（以及 `~/.bashrc`/`~/.profile` 里那两行标记注释之间的引用）。
**检测信号**：`javac -version` / `mvn -version` / `make --version`。
**判定条件**：**三者齐备且 mvn 能解析 Flink 依赖**——**P0 已达成**。

### R4 · 执行面落点（**已由 P0 实测裁决，并推翻了一轮错误推论**）
**证据**：P0 实测（2026-10-19 20:29）——**Docker 发布端口** `-p 127.0.0.1:18080/18081` 从 WSL2 内 `curl` **可达（200，3/3 次 + 双端口复现）**；**普通 Windows 监听** `:10724` 从 WSL2 **不可达**。此前「一律不可达」的结论源于用**等价监听做代理测试**，该代理**不成立**（详见 §4.7）。
**定案**：单一执行面 WSL2，A 面探测**直接用 WSL 内 `curl`/`python3`**；**「必须用 `python.exe`」这条约束已撤销**。
**残留风险（真实且已踩过一次）**：A 面可达**依赖 WSL integration 处于开启**（现 `/var/run/docker.sock` 存在、Server 29.8.0）。集成若被关，`curl` 会得到 `000`——**形态与「容器没起来」相同**，仍会误导。
**检测信号**：§4.7 的两条检查（已发布端口返回非 `000`；`/var/run/docker.sock` 存在）。
**判定条件**：两条同时成立。**若 socket 消失 ⇒ 须重新裁决，不得沿用。**
**教训（写进流程）**：**代理测试必须用真实同类对象复验**，否则会把「代理的性质」当成「被代理对象的性质」。

### R5 · 磁盘预算与官方口径冲突 + Paimon `file://` 峰值未实测
**证据**：Doris 官方 dev/test 磁盘口径 **FE 10 GB+ / BE 50 GB+**，与 `SRS.md` 的 ≤20 GB 正面冲突；Paimon 物理删除**只在快照过期时**发生（默认 `snapshot.time-retained=1h` 内所有历史版本保留），主键表在 compaction 前同一 key 有**多份物理版本** ⇒ **空间放大与写入放大都是真实的**。
**空间放大前提复核（附录 A·N，2026-10-21 签）**：Paimon **主键表默认 MOR 而非 MOW**（`官方给出`：官方 bucket 指引 "200 MB–1 GB per bucket" 逐字是 **MOR 口径**；MOW 需显式 `deletion-vectors.enabled`）⇒ 本条的空间放大估算**原按 MOW 心智、现按 MOR 复核**：本仓五张主键表（DWD/DIM×3/DWS/ADS）固定 **MOR**，放大来自多版本与 compaction，**无 deletion-vector 副本**；每分区 ≈170 MB 落官方指引量级下沿 ⇒ 主键表固定 `bucket=1` 成立。偏离已进 §8-R7 登记。
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
| **R7-e** | ★★ **named volume 挂上去 ≠ 数据落在里面**：官方 `apache/kafka:4.3.1` 镜像的默认 `log.dirs` 是 **`/tmp/kraft-combined-logs`**（`/opt/kafka/config/server.properties:73` 逐字）。`/tmp` 是**容器内路径**，`down/up` 后清空 | **`REQ-F-ORCH-05` 真实失效**（不是理论风险）：down 前 partition 2/3 各有 1 条记录，`down/up` 后 offsets **全归 0**；检查卷内容为 **空（total 4.0K）** | **显式设 `KAFKA_LOG_DIRS=/var/lib/kafka/data`**（与卷挂载点一致）。**已证**：`restart` → offsets 3→3；`down`+`up` → offsets 3→3（即 `REQ-F-ORCH-05` 原文判据），topics 与 retention 配置同样保住 |
| **R7-f** | **★ 2026-10-21 登记（附录 A·N）**：Paimon **主键表默认 MOR 而非 MOW**（`官方给出`：MOW 需显式 `deletion-vectors.enabled`；官方 bucket 指引 "200 MB–1 GB per bucket" 逐字是 MOR 口径） | R5 的空间放大估算原按 MOW 心智 ⇒ 与实际部署形态不符 | 本仓五张主键表**固定 MOR**；R5 已按 MOR 复核（无 deletion-vector 副本；每分区 ≈170 MB 落 MOR 指引量级下沿 ⇒ `bucket=1` 成立）。**设计面由此变更**：主键表不设 `sequence.field`、`upsert-materialize=NONE`、DWS/ADS 与 Doris `ecom_ads` 键同构 |
| **R7-g** | **★ 2026-10-25 登记（impl-05 终验）**：声明上限合计因 impl-03c 的 `flink-jm` 1g→2g **已涨到 11.5 GiB**（11,776 MiB），HLD §4.2 两处全量口径行漏同步（曾写 10.5 GiB / 余 1.18）；且 SRS `REQ-NF-CAP-01`/`REQ-F-ORCH-02` 判据①「声明上限（含 profile）≤ 8 GB」与同段签字句承认的 10.5 GiB 自相矛盾（见 §4.2 订正注） | 若按判据①字面执行，须把经实机证伪钉死的下限（`flink-jm` 2g、`doris-be` 3g、`doris-fe` 2g 受 `-Xmx1536m` 约束）再压 ≈3.5 GiB ⇒ **破坏已验证部署面，结构性不可达** | 事实类：§4.2 已订正为 11.5 GiB / 余 0.18 GiB。意图类（判据①字面）：✅ **2026-10-25 用户追认已签（spec §7.1 #41）**——**8 GB 作为口径②实测占用的目标断言；口径①（声明）以 11.68 GiB 天花板为界、如实给数**，SRS 两行已落笔，详见 `reproducibility-boundary.md` §11.1 |
| **R7-h** | **★ 2026-10-25 登记（impl-05 终验实测）**：普通 `docker compose down` **不会停 profile 内服务**——`doris-fe`/`doris-be` 在 `profiles: ["doris"]` 里，六容器在跑时 `docker compose down` 只停了四个（kafka/flink×2/superset），Doris 两容器原样存活，且网络删除因它们仍挂着而报 `Resource is still in use`（官方 compose profile 语义：down 默认只作用于当前 profile 集） | 若 L2 重启周期只跑裸 `docker compose down`，**Doris 根本没吃到重启周期**，`REQ-F-ORCH-05` 的验收对 Doris 是**假通过**；P0 时代的 L2 记录（`reproducibility-boundary.md` §1）用的正是裸 down ⇒ 其 Doris 侧证据只覆盖 `up` 时容器重建（IP 重分配由固定子网兜住），不覆盖「down 停机」 | L2 判据动作改为 **`docker compose --profile doris down`（不加 `-v`）→ `make init`**；impl-05 已按此实测（见 `evidence/impl-05/`），FE/BE IP `172.31.88.40/.50` 与数据卷态全数保住、无 `feType:UNKNOWN` |
| **R7-i** | **★★ 2026-10-25 登记（impl-05 终验实测，两级连锁）**：①`ecomgen live` 把整批事件平移到「本轮起点＝现在」但**保留批次逻辑跨度**（`cli.py` `_send_to_kafka`：`shift = round_start_ms − raw_min_ms`），span 随 `--sessions` 增长（实测 100 会话 ⇒ span ≈8 min > 生成器自己的「未来 5 min 裕度」；默认值 1000 会话更糟）⇒ 尾部事件 event_time 落在**未来**，被 P1 质量门正确判 `time_out_of_range` 进 DLQ；②P1 source 冷启动用 `OffsetsInitializer.earliest()`（`GateSegment.java:232`），**无视 committed group offsets、每次提交都全量重放**（TM 日志逐字 `Seeking to AutoOffsetResetStrategy{type=earliest}`；两次独立提交的 `numRecordsOut` 均恰等于 topic 全量 53,789），而**时间戳分配先于质量门** ⇒ 重放把 topic 里最晚的未来事件（et ≈ +2.1 h）读进来 ⇒ **水位被钉在未来 ~2 小时**，此后一切真实 et≈now 的事件命中 `WindowSumProcess` 的超期旁路（`watermark >= deadline` ⇒ return）被静默丢弃——探针批 24 条全丢（Doris 只剩 `provisional_emitted` 行、`event_*` 落定行零行），`recon-offline-mirror-doris` 必 FAIL，直到墙钟追过未来尾（实测需等 ~2 h） | 「能重起来」的真实形态被颠覆：**每次作业冷启动 = 全量重放 + 脏数据二次入 DLQ（时间型脏随墙钟陈化前后行为不同）+ 水位被 topic 内未来事件劫持**；live 子命令按现行参数（`--sessions` 默认 1000）注定产出质量门拒收的事件 | **本票只登记不改**（代码变更属独立票）：运维口径——live 带载用 `--sessions ≤ 40`（span ≈3.2 min < 5 min 容差）；作业冷启动前确认 topic 内无未陈化的未来事件，否则探针/实时写入在墙钟追上前不可用。修复候选（记档不执行）：①live 平移改为「窗尾对齐 now」（span 尾不超界）或硬性 span ≤ 裕度断言；②source 初始化改 `committedOffsets(EARLIEST)`；③时间戳分配与质量门次序重审（拒绝事件不应抬水位） |

**检测信号**：`docker compose up` 后各服务 healthcheck；`kafka-topics.sh --describe` 的 `Configs:` 行；`kafka-get-offsets.sh` 的 offsets；Superset 日志里的 `ModuleNotFoundError`。
**判定条件**：骨架**四服务全部 healthy**、`config-check` 全 PASS、且 **`down`→`up` 后 Kafka offsets 不变**（**P0 三项均已达成**）。
**教训（写进流程）**：**调研笔记给的是「官方文档说应该怎样」，P0 得到的是「实机实际怎样」**——两者在配置格式、镜像内容、CLI 版本行为、默认路径上**已经四次不一致**。凡涉及「按某个格式写配置」「镜像里有什么」「数据落在哪」的结论，**必须实机验证一次**才能进施工路径。
**同类的两条**（约束写了不等于生效）：`mem_limit` 管不到 swap（§4.2）；named volume 挂载点不等于应用写入路径（本条）。

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
