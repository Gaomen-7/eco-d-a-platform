package com.ecom.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
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
                                + "  COUNT(*) AS metric_value\n"
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

        // TODO(P1-DORIS-SINK)：接 Doris sink（Stream Load 2PC，flink-doris-connector 26.3.0）。
        //   为什么留在这里而不写死：sink 的表名/列序必须与 init.sql 逐字对齐，
        //   实机联调时才有唯一判据（ADS 表有行）。见 verify 第 4 项。
        CorrectionSegment.attachWindowed(tEnv, env, windowed);
    }

    /** 供 {@link TableEnvironment} 之外的调用方判断某表是否已注册（避免重复注册）。 */
    static boolean tableExists(StreamTableEnvironment tEnv, String name) {
        return tEnv.listTables().length > 0 && java.util.Arrays.asList(tEnv.listTables()).contains(name);
    }
}
