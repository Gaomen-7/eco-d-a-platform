# 02: Flink 工程空壳（Maven 闭环）

**What to build:** `flink/` 出现 Maven 工程骨架：锚点齐（`flink-streaming-java` 2.2.1 provided、`flink-connector-kafka` `5.0.0-2.2`、`flink-doris-connector` 26.3.0——坐标落盘前在 Central 逐条核实）、compiler release 17、shade 排除规则（jsr305、META-INF 签名文件）、核心 API 一律 provided；零 Java 源码，空 `src/main/{java,resources}` 目录。空壳 `mvn -q package` BUILD SUCCESS——JDK17+Maven 工具链的行为证明（S3）。镜像补丁核对同票完成：compose 浮动 tag 实际运行补丁须为 2.2.1（`flink --version`，需先拉起 daemon；不符则停手上报，以 ADR-0002 为准）。细节以 spec 的 Flink 工程条目为准。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] pom 三处锚点与 Central 坐标逐条核实一致
- [ ] shade 排除规则与 provided 语义就位
- [ ] 空壳 `mvn -q package` BUILD SUCCESS
- [ ] 镜像补丁核对完成：`flink --version` ＝ 2.2.1（或记档说明 daemon 不可用与后续核对路径）
- [ ] 完成后本地 commit，未推送
