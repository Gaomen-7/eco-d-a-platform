# Flink 补丁锚点定为 2.2.1

Status: accepted (2026-10-21)

FSR §4 冻结的组件锚点是「Flink 2.2.x（不得 2.3.0）」，但补丁号一直悬空；本 ADR 落为 **2.2.1**——Maven Central 上 2.2.x 线的最新补丁（`flink-streaming-java` metadata 核实，2026-10-21）。不取最新 2.3.0 的原因是一手断链：2.3.0 无已发布的 Kafka connector（HLD §3.1）；不取 2.2.0 是因为 2.2.1 为同线最新补丁且无弃选理由。**FSR 保持锚点权威不动——本 ADR 是补丁级的精确化，不是锚点变更**（不走冻结项变更通道）。

**Consequences**：compose 现用镜像 tag `apache/flink:2.2-java17` 浮动于 2.2 线，`flink/` 工程落地时须核对镜像实际运行补丁与 2.2.1 一致（`flink --version`），不一致以本 ADR 为准调镜像。pom 依赖（`flink-streaming-java` provided、`flink-connector-kafka:5.0.0-2.2`、`flink-doris-connector:26.3.0`）按此版本线落，坐标落盘前在 Central 逐条核实。
