package com.ecom.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * P1 实时作业：Kafka → 窗口聚合（SQL 段）→ 窗口修正（DataStream 段）→ Doris。
 *
 * <p>为什么必须**混合路线**（spec.md §4.6，官方能力强制，非选型偏好）：
 *
 * <ul>
 *   <li>Flink SQL <b>没有</b>迟到侧输出：FLINK-10031 / FLINK-20527 均 Open，官方原话
 *       "The Table API &amp; SQL drop late events right now."；{@code WindowAggOperator}
 *       javadoc 逐字 "late elements … will be simply dropped."
 *   <li>SQL 侧<b>没有</b> allow-lateness：FLINK-24000 逐字 "Window aggregate does not
 *       support allow-lateness"；{@code allowedLateness} 是 <b>DataStream-only</b>。
 * </ul>
 *
 * <p>⇒ 窗口<b>聚合</b>落 SQL 段，「允许延迟内的窗口<b>修正</b>」落 DataStream 段。
 * 这不是可以绕开的设计取舍：SQL 段物理上无法实现迟到修正。
 *
 * <p>PI-3 标注（spec §4.6 第三行）：窗口内提前发射（10 s 暂定值）只能走
 * 已被官方标 deprecated 的 Group Window Aggregation（{@code table.exec.emit.early-fire.*}），
 * 源码写 "will be dropped"，移除跟踪 FLINK-29692；CUMULATE 走 TVF 会被
 * {@code checkEmitConfiguration} 直接抛异常，<b>不是</b>替代路径。故该项只作文档标注，
 * 不进 PI-1 判据。
 */
public final class P1RealtimeJob {

    private P1RealtimeJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(JobConfig.PARALLELISM);
        env.enableCheckpointing(JobConfig.CHECKPOINT_INTERVAL_MS);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // SQL 段：建 source 表（Kafka）与 sink 表（Doris），跑窗口聚合
        SqlSegment.registerTables(tEnv);
        SqlSegment.runWindowAggregation(tEnv, env);

        // DataStream 段：迟到侧输出 → 允许延迟内修正 / 超期只计数
        CorrectionSegment.build(env, tEnv);

        env.execute("ecom-p1-realtime-closure");
    }
}
