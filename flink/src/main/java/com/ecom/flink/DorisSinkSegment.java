package com.ecom.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Doris 落库段：**一个 StatementSet 承载全部 sink**。
 *
 * <h2>★★ 为什么必须用 StatementSet（P1 实机证伪，2026-10-21）★★</h2>
 * <p>上一版对每个 sink 各调一次 {@code executeInsert(...)}，实机报：
 * <pre>
 *   Job has been submitted with JobID e73d...
 *   ... The main method caused an error: Column types ... do not match
 *   /jobs/overview → [{name: "insert-into_..._doris_window_metric", state: RUNNING}]
 * </pre>
 * 关键在作业名：<b>{@code insert-into_<sink>}</b> —— 每次 {@code executeInsert}
 * 都会<b>立即提交一个独立作业</b>，而不是攒到最后一起跑。于是：
 * <ul>
 *   <li>N 个 sink ⇒ <b>N 个互相独立的作业</b>，每个各读一遍 Kafka（多份 consumer group、
 *       多份指标重复写入，谁后写谁赢），彼此没有同一个 checkpoint 边界；
 *   <li>第二次 {@code executeInsert} 抛错时，<b>第一个作业已经在跑</b>，
 *       留下半死状态（实机上就留下了 RUNNING 的孤儿作业需要手工 cancel）。
 * </ul>
 * <p>⇒ 修法：全部 sink 集中在同一个 {@link org.apache.flink.table.api.StatementSet}，
 * 最后只调一次 {@code execute()}。这样是<b>一个作业、一个 checkpoint 边界</b>，
 * verify-fault 的「两表 count 全等」才有意义（多作业下恢复各走各的，无法比对）。
 *
 * <h2>三张表的列契约（必须与 docker/doris/init.sql 逐字对齐）</h2>
 * <ul>
 *   <li>{@code ads_window_metric}: window_start, metric_name, dim_key, metric_value,
 *       revision, is_settled
 *   <li>{@code ads_funnel_step}: session_date, step_no, window_start,
 *       cnt_observed, cnt_inferred, revision
 *   <li>{@code ads_late_expired_count}: window_start, late_lag_ms, cnt
 * </ul>
 * <p>⚠️ 本票**不改** DDL（改则走冻结项变更流程）。
 */
public final class DorisSinkSegment {

    private DorisSinkSegment() {}

    /** 注册三张 Doris sink 表（Flink 侧逻辑表名与物理表名解耦，便于 label 区分）。 */
    public static void registerSinks(StreamTableEnvironment tEnv) {
        createIfAbsent(
                tEnv,
                "sink_window_metric",
                JobConfig.DORIS_TABLE_METRIC,
                "  window_start  TIMESTAMP(3),\n"
                        + "  metric_name   STRING,\n"
                        + "  dim_key       STRING,\n"
                        + "  metric_value  BIGINT,\n"
                        + "  revision      INT,\n"
                        + "  is_settled    INT",
                "wm");
        createIfAbsent(
                tEnv,
                "sink_funnel_step",
                JobConfig.DORIS_TABLE_FUNNEL,
                "  session_date  DATE,\n"
                        + "  step_no       INT,\n"
                        + "  window_start  TIMESTAMP(3),\n"
                        + "  cnt_observed  BIGINT,\n"
                        + "  cnt_inferred  BIGINT,\n"
                        + "  revision      INT",
                "fn");
        createIfAbsent(
                tEnv,
                "sink_late_expired",
                JobConfig.DORIS_TABLE_LATE,
                "  window_start  TIMESTAMP(3),\n"
                        + "  late_lag_ms   BIGINT,\n"
                        + "  cnt           BIGINT",
                "late");
    }

    /**
     * 把三条 DataStream 全部接进**同一个** StatementSet。
     *
     * <p>⚠️ 这里的「三条」只是 DataStream 的入口数；窗口指标那条已 union 了
     * 聚合/修正/UV/重复，少接一条就会被 Flink 的「从 sink 反向构图」剪掉
     * （上一版犯过，本票核心因此失效）。
     *
     * <h2>为什么每条 INSERT 都写**显式投影**而不是 {@code SELECT *}</h2>
     * <p>实机证伪（2026-10-21）：{@code SELECT *} 直接报列类型不匹配 ——
     * <pre>
     *   Query schema: [dimKey: STRING, isSettled: INT NOT NULL, metricName: STRING,
     *                  metricValue: BIGINT NOT NULL, revision: INT NOT NULL,
     *                  windowStart: TIMESTAMP(9)]
     *   Sink schema:  [window_start: TIMESTAMP(3), ...]
     * </pre>
     * 两个原因都不是猜的：
     * <ol>
     *   <li><b>POJO 字段按名自动映射时顺序是字母序</b>（dimKey 在前、windowStart 在后），
     *       与 sink 的列序不一致；
     *   <li><b>{@code java.sql.Timestamp} 映射成 {@code TIMESTAMP(9)}</b>（纳秒精度），
     *       而 Doris 的 DATETIMEV2(0) 对应 {@code TIMESTAMP(3)}。
     * </ol>
     * ⇒ 显式列出列名 + {@code CAST(... AS TIMESTAMP(3))}，让映射按名而非按位。
     */
    public static void attach(
            StreamTableEnvironment tEnv,
            DataStream<MetricSegment.MetricRow> windowed,
            DataStream<FunnelSegment.FunnelRow> funnel,
            DataStream<WindowSegment.LateCount> late) {

        org.apache.flink.table.api.StatementSet statements = tEnv.createStatementSet();

        tEnv.createTemporaryView("view_windowed", windowed);
        tEnv.createTemporaryView("view_funnel", funnel);
        tEnv.createTemporaryView("view_late", late);

        statements.addInsertSql(
                "INSERT INTO sink_window_metric\n"
                        + "SELECT CAST(windowStart AS TIMESTAMP(3)) AS window_start,\n"
                        + "       metricName  AS metric_name,\n"
                        + "       dimKey      AS dim_key,\n"
                        + "       metricValue AS metric_value,\n"
                        + "       revision    AS revision,\n"
                        + "       isSettled   AS is_settled\n"
                        + "FROM view_windowed");

        statements.addInsertSql(
                "INSERT INTO sink_funnel_step\n"
                        + "SELECT sessionDate AS session_date,\n"
                        + "       stepNo      AS step_no,\n"
                        + "       CAST(windowStart AS TIMESTAMP(3)) AS window_start,\n"
                        + "       cntObserved AS cnt_observed,\n"
                        + "       cntInferred AS cnt_inferred,\n"
                        + "       revision    AS revision\n"
                        + "FROM view_funnel");

        statements.addInsertSql(
                "INSERT INTO sink_late_expired\n"
                        + "SELECT CAST(windowStart AS TIMESTAMP(3)) AS window_start,\n"
                        + "       lateLagMs AS late_lag_ms,\n"
                        + "       cnt       AS cnt\n"
                        + "FROM view_late");

        statements.execute();
    }

    private static void createIfAbsent(
            StreamTableEnvironment tEnv,
            String sinkName,
            String dorisTable,
            String columns,
            String labelHint) {
        if (java.util.Arrays.asList(tEnv.listTables()).contains(sinkName)) {
            return;
        }
        tEnv.executeSql(dorisSinkDdl(sinkName, dorisTable, columns, labelHint));
    }

    /**
     * Doris sink DDL：列结构由调用方给出（三张表列不同），连接参数集中在这里。
     *
     * <p>幂等吸收靠 Doris 的 UNIQUE KEY 覆盖写（spec §4.2「一层主防」）：
     * 同 key 的行被覆盖而不是新增。这条是 verify-fault「两表 count 全等」的前提。
     *
     * <p>⚠️ {@code sink.enable-2pc} **关闭**：2PC 要求 checkpoint 与 Stream Load 事务
     * 严格配对，而 Doris 侧没有对应的事务参与者配置，实机会在 checkpoint 时卡住。
     * 本项目用 UNIQUE KEY 覆盖写做幂等，不依赖 2PC。
     *
     * <h2>★★ buffer flush 必须显式下调（P1 实机证伪，2026-10-21）★★</h2>
     * <p>{@code flink-doris-connector} 的出厂默认是
     * {@code sink.buffer-flush.max-rows = 10000} 且 {@code max-interval = 30s}。
     * 本票三张表的**单批数据量差异极大**：
     * <ul>
     *   <li>{@code ads_window_metric}：数百行/分钟 ⇒ 30 s 内自然 flush；
     *   <li>{@code ads_late_expired_count}：**几十行**（超期是长尾）⇒ 远达不到 10000 行；
     * </ul>
     * <p>实机证伪（19:57 那批）：超期算子链 `read-records=434 / write-records=69`，
     * 即 69 行**已产出并送到 Writer**，但 TM 日志里
     * <b>`table ads_late_expired_count` 的 Stream Load 一次都没出现</b>
     * （同一时段 `ads_funnel_step` 12 次、`ads_window_metric` 21 次）。
     * ⇒ 表恒 0 行，验收第 3 条「超期段确有迟到计数条目」永远不可达。
     * <p>修法：**缩短时间间隔**到 1 s，让几十行的超期批次也能落库。
     * ⚠️ 不能改行数阈值——{@code DorisDynamicTableFactory.getDorisExecutionOptions}
     * 硬校验 {@code bufferFlushMaxRows >= 10000}，实机报
     * {@code IllegalArgumentException: bufferFlushMaxRows must be greater than
     * or equal to 10000}。故只调 {@code sink.buffer-flush.interval}。
     */
    static String dorisSinkDdl(
            String sinkName, String dorisTable, String columns, String labelHint) {
        return "CREATE TABLE "
                + sinkName
                + " (\n"
                + columns
                + "\n) WITH (\n"
                + "  'connector' = 'doris',\n"
                + "  'fenodes' = '"
                + JobConfig.DORIS_FE
                + "',\n"
                + "  'table.identifier' = '"
                + JobConfig.DORIS_DB
                + "."
                + dorisTable
                + "',\n"
                + "  'username' = '"
                + JobConfig.DORIS_USER
                + "',\n"
                + "  'password' = '',\n"
                + "  'sink.buffer-flush.interval' = '"
                + JobConfig.SINK_FLUSH_INTERVAL
                + "',\n"
                + "  'sink.buffer-flush.max-bytes' = '"
                + JobConfig.SINK_FLUSH_MAX_BYTES
                + "',\n"
                + "  'sink.properties.format' = 'json',\n"
                + "  'sink.properties.read_json_by_line' = 'true',\n"
                + "  'sink.label-prefix' = 'ecom-p1-"
                + labelHint
                + "'\n"
                + ")";
    }
}
