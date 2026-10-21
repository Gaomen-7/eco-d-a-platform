# 02: Flink 工程空壳（Maven 闭环）

**What to build:** `flink/` 出现 Maven 工程骨架：锚点齐（`flink-streaming-java` 2.2.1 provided、`flink-connector-kafka` `5.0.0-2.2`、`flink-doris-connector` 26.3.0——坐标落盘前在 Central 逐条核实）、compiler release 17、shade 排除规则（jsr305、META-INF 签名文件）、核心 API 一律 provided；零 Java 源码，空 `src/main/{java,resources}` 目录。空壳 `mvn -q package` BUILD SUCCESS——JDK17+Maven 工具链的行为证明（S3）。镜像补丁核对同票完成：compose 浮动 tag 实际运行补丁须为 2.2.1（`flink --version`，需先拉起 daemon；不符则停手上报，以 ADR-0002 为准）。细节以 spec 的 Flink 工程条目为准。

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] pom 三处锚点与 Central 坐标逐条核实一致——`2.2.1` / `5.0.0-2.2` / `26.3.0` 三条 pom + Doris jar 全 HTTP 200
- [x] shade 排除规则与 provided 语义就位
- [x] 空壳 `mvn -q package` BUILD SUCCESS——rc=0，产物 73,472,402 B / 31,594 条目
- [x] 镜像补丁核对完成：`flink --version` ＝ 2.2.1——`Version: 2.2.1, Commit ID: 450c63e`
- [x] 完成后本地 commit，未推送

## Answer

**落盘**（commit `1541e45`，合入 `feat/repo-skeleton`）：`flink/pom.xml`（111 行）＋ `flink/src/main/java/.gitkeep` ＋ `flink/src/main/resources/.gitkeep`。**零 Java 源码**（`find flink/src -name "*.java" | wc -l` → 0）。

**锚点表**（`flink/pom.xml`）：

| 坐标 | scope | 备注 |
| --- | --- | --- |
| `org.apache.flink:flink-streaming-java:2.2.1` | `provided` | ADR-0002 补丁锚点；Flink inverted classloading |
| `org.apache.flink:flink-clients:2.2.1` | `provided` | ⚠️ spec 未列，见下「spec 缺口」 |
| `org.apache.flink:flink-connector-kafka:5.0.0-2.2` | compile（shaded） | 坐标必须写全 `-2.2` 线后缀（HLD §7.1 D-C） |
| `org.apache.doris:flink-doris-connector-2.2:26.3.0` | compile（shaded） | ⚠️ artifactId 带 `-2.2` 后缀，裸 `flink-doris-connector` 在 Central 不存在 |

- `maven.compiler.release = 17`；`maven-compiler-plugin` 3.13.0。
- shade（`maven-shade-plugin` 3.6.0，phase `package`）：`artifactSet.excludes` = `com.google.code.findbugs:jsr305`；`filters` 对 `*:*` 排 `META-INF/*.SF` / `*.DSA` / `*.RSA`；`createDependencyReducedPom = false`。
- 产物实证：签名文件 0 个、jsr305 0 个、`org/apache/flink/streaming/api` 类 0 个（provided 未被 shade 进）、doris+kafka 类在位。

**坐标核实（Maven Central，HTTP 200 实测）**：`flink-streaming-java/2.2.1`、`flink-connector-kafka/5.0.0-2.2`、`flink-doris-connector-2.2/26.3.0`（pom 与 jar 皆 200；`maven-metadata.xml` 的 `<latest>`/`<release>` 均为 `26.3.0`）。

**镜像补丁核对（ADR-0002 Consequences）**：compose 用浮动 tag `apache/flink:2.2-java17`（`docker-compose.yml:95,121`）。实测
`docker run --rm docker.m.daocloud.io/apache/flink:2.2-java17 flink --version`
→ **`Version: 2.2.1, Commit ID: 450c63e`** ⇒ **与 ADR-0002 一致**，无需调镜像。

**S3 行为证明**：`cd flink && mvn -q package; echo rc=$?` → **rc=0**（`BUILD SUCCESS`，约 9–20 s）。未用 `-o`：本机 `~/.m2` 缺 surefire 依赖链，离线会失败。

### spec 缺口登记：`flink-clients` 未在 spec 中列出（保留，不删）

spec.md:47 只列三个依赖，未列 `flink-clients`。实测保留它是**必需**的，理由**不是**「提供 `StreamExecutionEnvironment`」（该类在 `flink-runtime`，经 `flink-streaming-java` 传递进来；移除 `flink-clients` 后编译仍 rc=0），而是：

- `flink-clients` 是**唯一**携带 `META-INF/services/org.apache.flink.core.execution.PipelineExecutorFactory`（`RemoteExecutorFactory` / `LocalExecutorFactory`）的 jar；`flink-runtime` 与 `flink-streaming-java` 均不含该 SPI（unzip 实测 1 / 0 / 0）。
- 运行时实测：移除后 `java ... SmokeJob` → `IllegalStateException: No ExecutorFactory found to execute the application`（executor-service-loader:88），**rc=1**；保留后作业真正运行（输出 `8> a`、`9> b`），**rc=0**。即 `env.execute()` 无它不可用，任何 P1 作业会在提交时挂掉。
- 它是 `provided` ⇒ 不改变 shaded 产物。

结论：这是 **spec 的遗漏，不是 pom 的缺陷**。若将来要收口，应在 spec 层补一行，而非删依赖。

### 同时收掉的未锚定依赖

`org.slf4j:slf4j-api:1.7.36` 曾被显式声明，但 spec / ADR-0002 / HLD §3.1 均无该锚点来源 ⇒ 已删除，改由 `flink-streaming-java:2.2.1` 的传递依赖提供（`mvn dependency:tree` 实测 `slf4j-api:jar:1.7.36:runtime`，位于编译类路径上）。删除前后 shaded 产物的条目集**完全相同**（31,594 = 31,594；slf4j 条目 38 个不变），产物中立。
