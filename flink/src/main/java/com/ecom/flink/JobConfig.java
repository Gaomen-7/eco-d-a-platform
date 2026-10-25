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

    /** event_id 去重状态保 1 小时（gdd §A.2「event_id 去重状态保 1 小时」）。 */
    public static final long DEDUP_TTL_MS = 3_600_000L;

    /**
     * 暂定值的提前发射周期 = 10 s（gdd §A.2）。
     *
     * <p><b>PI-3 标注级</b>（spec §4.6 第三行）：官方唯一支持 early-fire 的
     * Group Window Aggregation 已被标 deprecated（源码 "will be dropped"，
     * 移除跟踪 FLINK-29692），CUMULATE 走 TVF 会被 {@code checkEmitConfiguration}
     * 直接抛异常。故这里**不**用那个语法，改用 DataStream 段按处理时间定时器
     * 发射暂定行（详见 {@link ProvisionalSegment}），既满足
     * 「暂定/落定/修正同表同一行」的可观察性，又不押注弃用语法。
     */
    public static final long PROVISIONAL_INTERVAL_MS = 10_000L;

    // ── metric_name 取值（与 verify 判据的契约，改则同步 Python 侧）──────
    /** 重复被幂等吸收的条数（dim_key=''）。**不进脏率分子**（gdd §D D2）。 */
    public static final String METRIC_DUPLICATE = "duplicate_absorbed";
    /** 1 小时滚动 UV（dim_key=''，精确去重）。 */
    public static final String METRIC_UV = "uv";
    /** 暂定行发出的累计条数（dim_key=''，用于证明「暂定→落定」存在）。 */
    public static final String METRIC_PROVISIONAL = "provisional_emitted";
    /**
     * GMV（分）：窗口内 purchase 的 amount 之和，按品类分维度（dim_key=category_id）。
     * impl-04 新增——REQ-F-RT-02/REQ-F-BI-01 的「实时 GMV」此前实时侧无供数。
     * 单位＝分（与 ads_window_metric 的 BIGINT「以整数存（分）」DDL 注释一致；
     * 离线 Paimon 侧 gmv 是 DECIMAL 元——单位差异已由 impl-03c「仍待裁」第 4 条登记）。
     */
    public static final String METRIC_GMV = "gmv";
    /**
     * 地域 PV：窗口内事件数按省聚合（dim_key=province）。impl-04 新增——
     * REQ-F-RT-02/REQ-F-BI-01 的「地域分布」此前实时侧无供数。
     * 名字带 `_pv` 后缀以与「事件数」家族区分；词表新增不进 verify 的
     * event_* 对账面（reconcile 只认 event_ 前缀）。
     */
    public static final String METRIC_REGION_PV = "region_pv";

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

    // ── Doris sink 的缓冲刷写（设计值，理由见 DorisSinkSegment 类注释）──────
    /**
     * 刷写间隔：出厂默认 30 s，降到 1 s（设计值）。
     *
     * <p>★ 为什么只调「间隔」不调「行数」：{@code flink-doris-connector} 在
     * {@code DorisDynamicTableFactory.getDorisExecutionOptions} 里**硬校验**
     * {@code bufferFlushMaxRows >= 10000}，实机报：
     * <pre>
     *   java.lang.IllegalArgumentException:
     *     bufferFlushMaxRows must be greater than or equal to 10000
     * </pre>
     * ⇒ 行数阈值不可下调，「小流量表等不到 flush」只能靠**缩短时间间隔**解决。
     * 超期通道单批只有几十行，1 s 的间隔足以让它们落库。
     */
    public static final String SINK_FLUSH_INTERVAL = "1s";

    /** 单批最大字节：保持出厂默认（100 MB）——它不是小流量表的瓶颈。 */
    public static final String SINK_FLUSH_MAX_BYTES = "104857600";

    /** DLQ 只摘真脏三原因码（票面 ②）。重复**不在**此列。 */
    public static final String REASON_SCHEMA_INVALID = "schema_invalid";
    public static final String REASON_VALUE_OUT_OF_RANGE = "value_out_of_range";
    public static final String REASON_TIME_OUT_OF_RANGE = "time_out_of_range";

    /** 消费组：verify-fault 靠 committed offset 判 lag==0。 */
    public static final String CONSUMER_GROUP = "ecom-p1-consumer";
}
