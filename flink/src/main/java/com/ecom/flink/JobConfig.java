package com.ecom.flink;

/** 作业常量：集中一处，避免散落在各段里漂移。 */
public final class JobConfig {

    private JobConfig() {}

    /** 并行度 ≤4（{@code REQ-F-RT-08}）。 */
    public static final int PARALLELISM = 4;

    /** checkpoint 间隔 30 s（gdd §A.2，与 docker/flink/conf/config.yaml 一致）。 */
    public static final long CHECKPOINT_INTERVAL_MS = 30_000L;

    /** 允许延迟 = 2 分钟（spec §4.2）：窗口关闭后再等这么久才落定。 */
    public static final long ALLOWED_LATENESS_MS = 120_000L;

    /** 1 分钟滚动窗口（gdd 【已确认 Q13】）。 */
    public static final long WINDOW_SIZE_MS = 60_000L;

    /** UV 滚动去重窗口 1 小时（gdd §1.3-6）。 */
    public static final long UV_WINDOW_MS = 3_600_000L;

    // ── Kafka（冻结项，SRS §3.4）────────────────────────────────
    public static final String TOPIC_MAIN = "ecom.behavior.event.v1";
    public static final String TOPIC_DLQ = "ecom.behavior.event.v1.dlq";
    public static final String KAFKA_BOOTSTRAP = "kafka:9092";

    // ── Doris（冻结项，REQ-F-OLAP-01）──────────────────────────
    public static final String DORIS_FE = "doris-fe:8030";
    public static final String DORIS_DB = "ecom_ads";
    public static final String DORIS_TABLE_METRIC = "ads_window_metric";
    public static final String DORIS_TABLE_FUNNEL = "ads_funnel_step";
    public static final String DORIS_TABLE_LATE = "ads_late_expired_count";
    public static final String DORIS_USER = "root";

    /** DLQ 只摘真脏三原因码（票面 ②）。重复**不在**此列。 */
    public static final String REASON_SCHEMA_INVALID = "schema_invalid";
    public static final String REASON_VALUE_OUT_OF_RANGE = "value_out_of_range";
    public static final String REASON_TIME_OUT_OF_RANGE = "time_out_of_range";

    /** 消费组：verify-fault 靠 committed offset 判 lag==0。 */
    public static final String CONSUMER_GROUP = "ecom-p1-consumer";
}
