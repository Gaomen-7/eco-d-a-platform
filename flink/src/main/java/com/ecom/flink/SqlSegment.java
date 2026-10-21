package com.ecom.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * SQL 段：Kafka source → 1 分钟窗口聚合 → Doris sink。
 *
 * <p>本段的职责边界（spec §4.6）：只做<b>聚合</b>。迟到修正<b>不在这里</b>——
 * SQL 段物理上做不到（官方 drop late events / 无 allow-lateness），故由
 * {@link CorrectionSegment} 在 DataStream 段承担。
 */
public final class SqlSegment {

    private SqlSegment() {}

    /**
     * Doris sink DDL：列结构由调用方给出（三张表列不同），连接参数集中在这里。
     *
     * <p>幂等吸收靠 Doris 的 UNIQUE KEY 覆盖写（spec §4.2「一层主防」）：
     * 同 key 的行被覆盖而不是新增；{@code revision} 递增表达修正次数。
     * 这条是 verify-fault「两表 count 全等」的前提。
     *
     * <p>⚠️ 列结构必须与 {@code docker/doris/init.sql} 逐字对齐（本票**不改** DDL）；
     * 多写/少写列都会让 Stream Load 报错，而不是静默丢弃——这是好事，能当场发现。
     *
     * @param sinkName Flink 侧逻辑表名（与 Doris 物理表名可不同）
     * @param dorisTable Doris 物理表名
     * @param columns 列定义（不含括号）
     * @param labelHint Stream Load label 前缀，须全局唯一以免 2PC 冲突
     */
    static String dorisSinkDdl(String sinkName, String dorisTable, String columns, String labelHint) {
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
                + "  'sink.properties.format' = 'json',\n"
                + "  'sink.properties.read_json_by_line' = 'true',\n"
                + "  'sink.enable-2pc' = 'true',\n"
                + "  'sink.label-prefix' = 'ecom-p1-"
                + labelHint
                + "'\n"
                + ")";
    }

    /** 建 Kafka source 表与 Doris sink 表（DDL 冻结，与 docker/doris/init.sql 对齐）。 */
    public static void registerTables(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE kafka_events (\n"
                        + "  event_id        STRING,\n"
                        + "  event_time      BIGINT,\n"
                        + "  user_id         STRING,\n"
                        + "  item_id         STRING,\n"
                        + "  category_id     STRING,\n"
                        + "  event_type      STRING,\n"
                        + "  schema_version  STRING,\n"
                        + "  page            STRING,\n"
                        + "  source          STRING,\n"
                        + "  channel         STRING,\n"
                        + "  province        STRING,\n"
                        + "  amount          STRING,\n"
                        + "  currency        STRING,\n"
                        + "  quantity        INT,\n"
                        + "  order_id        STRING,\n"
                        + "  payload         STRING,\n"
                        + "  -- 处理时间列：水印推进用 event_time，这一列只用于 Kafka 记录的摄取时刻\n"
                        + "  proc_time AS PROCTIME(),\n"
                        + "  -- 事件时间：由 event_time（UTC epoch ms）派生的 TIMESTAMP(3)\n"
                        + "  row_time AS TO_TIMESTAMP_LTZ(event_time, 3),\n"
                        + "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '"
                        + JobConfig.TOPIC_MAIN
                        + "',\n"
                        + "  'properties.bootstrap.servers' = '"
                        + JobConfig.KAFKA_BOOTSTRAP
                        + "',\n"
                        + "  'properties.group.id' = '"
                        + JobConfig.CONSUMER_GROUP
                        + "',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'format' = 'json',\n"
                        + "  'json.ignore-parse-errors' = 'true'\n"
                        + ")"
        );
    }

    /**
     * 1 分钟窗口聚合（{@code gdd} 【已确认 Q13】：窗口语义仍是 1 分钟）。
     *
     * <p>只产出<b>已关闭</b>窗口的聚合（暂定值由提前发射承担，见 PI-3 标注）。
     * 落定语义（{@code is_settled}）与修订（{@code revision}）由修正段与幂等覆盖写完成。
     */
    public static void runWindowAggregation(StreamTableEnvironment tEnv, StreamExecutionEnvironment env) {
        // 窗口聚合：按「分钟窗 + metric_name + dim_key」聚合
        Table windowed =
                tEnv.sqlQuery(
                        "SELECT\n"
                                + "  window_start,\n"
                                + "  metric_name,\n"
                                + "  dim_key,\n"
                                + "  COUNT(*) AS metric_value,\n"
                                + "  0 AS revision,\n"
                                + "  1 AS is_settled\n"
                                + "FROM (\n"
                                + "  SELECT\n"
                                + "    -- 切窗：floor(event_time_ms / 60000) × 60000（UTC epoch 对齐，spec §4.2）\n"
                                + "    TO_TIMESTAMP_LTZ(\n"
                                + "      FLOOR(CAST(event_time AS DOUBLE) / 60000) * 60000, 3\n"
                                + "    ) AS window_start,\n"
                                + "    -- 指标名：事件类型即指标（page_view / view_item / ... 各自的计数）\n"
                                + "    CONCAT('event_', event_type) AS metric_name,\n"
                                + "    -- 维度键：无维度用空串（DDL 的 DEFAULT ''）\n"
                                + "    COALESCE(category_id, '') AS dim_key\n"
                                + "  FROM kafka_events\n"
                                + "  WHERE event_type IS NOT NULL\n"
                                + "    AND event_time IS NOT NULL\n"
                                + ") AS t\n"
                                + "GROUP BY window_start, metric_name, dim_key"
                );

        // 接 Doris sink：表必须已在 init.sql 建好（本票不改 DDL）。
        // 覆盖写靠 UNIQUE KEY (window_start, metric_name, dim_key)，revision 递增。
        String sinkTable = "doris_window_metric";
        if (!tableExists(tEnv, sinkTable)) {
            tEnv.executeSql(
                    dorisSinkDdl(
                            sinkTable,
                            JobConfig.DORIS_TABLE_METRIC,
                            "  window_start  TIMESTAMP(3),\n"
                                    + "  metric_name   STRING,\n"
                                    + "  dim_key       STRING,\n"
                                    + "  metric_value  BIGINT,\n"
                                    + "  revision      INT,\n"
                                    + "  is_settled    INT",
                            "wm-agg"));
        }
        windowed.executeInsert(sinkTable);
    }

    /** 供 {@link StreamTableEnvironment} 之外的调用方判断某表是否已注册（避免重复注册）。 */
    static boolean tableExists(StreamTableEnvironment tEnv, String name) {
        return java.util.Arrays.asList(tEnv.listTables()).contains(name);
    }
}
