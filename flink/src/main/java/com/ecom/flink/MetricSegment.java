package com.ecom.flink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 侧路指标段：UV 精确滚动去重 + 重复通道计数 + 暂定行计数。
 *
 * <h2>为什么这三样都不走 SQL 段</h2>
 * <ul>
 *   <li><b>UV 必须精确</b>（spec §4.2 逐字「Flink 侧<b>精确</b>滚动去重」）：
 *       近似（HyperLogLog）是可选实验不是默认。精确去重要维护「1 小时窗口内
 *       见过哪些 user_id」的状态并按事件时间驱逐 —— SQL 段没有这个能力。
 *   <li><b>重复通道</b>：重复根本不进 SQL 段（在上游被去重拦下）。
 *   <li><b>暂定计数</b>：是「提前发射真的发生了」的可证痕迹，
 *       与 {@link ProvisionalSegment} 同源。
 * </ul>
 *
 * <p>三者都产出 {@code ads_window_metric} 的行，<b>与聚合段同表同键</b>
 * ——这正是「暂定/落定/修正同表同一行」（spec §4.2）的落点。
 */
public final class MetricSegment {

    private MetricSegment() {}

    /** UV 去重窗口按 1 h epoch 对齐（spec §4.2「UV 按 1 h epoch 对齐同理」）。 */
    static long hourBucket(long eventTimeMs) {
        return (eventTimeMs / JobConfig.UV_WINDOW_MS) * JobConfig.UV_WINDOW_MS;
    }

    /**
     * UV：**精确**滚动去重。
     *
     * <p>状态是「本小时内见过的 user_id 集合」；本条事件若首次出现则 UV+1。
     * 窗口按 1 h **epoch 对齐**（不是「滑动 1 小时」）——对齐才使
     * 「同一分钟同一指标同一次运行得到同一个值」成立，否则结果依赖到达顺序。
     */
    public static DataStream<MetricRow> uv(DataStream<RawEvent> clean) {
        return clean.keyBy(e -> hourBucket(e.eventTime))
                .process(new UvProcess())
                .name("uv-exact")
                .uid("uv-exact");
    }

    /** 重复通道：**只累计被幂等吸收的重复条数**，绝不进脏率分子。 */
    public static DataStream<MetricRow> duplicates(DataStream<RawEvent> duplicates) {
        return duplicates.map(e -> {
                    long bucket = (e.eventTime / JobConfig.WINDOW_SIZE_MS)
                            * JobConfig.WINDOW_SIZE_MS;
                    return MetricRow.of(bucket, JobConfig.METRIC_DUPLICATE, "", 1L, 0, 1);
                })
                .returns(Types.POJO(MetricRow.class))
                .name("count-duplicate")
                .uid("count-duplicate");
    }

    /**
     * UV 精确去重器。
     *
     * <p>{@code seen} 是 MapState（user_id → 首次到达的 event_time），
     * 用事件时间计时器在 1 h 后驱逐，避免状态无限增长（gdd F13 的同类风险）。
     */
    public static class UvProcess
            extends KeyedProcessFunction<Long, RawEvent, MetricRow> {

        private transient MapState<String, Long> seen;
        private transient ValueState<Long> registeredCleanup;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            seen = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>("uv-seen", Types.STRING, Types.LONG));
            registeredCleanup = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("uv-cleanup", Types.LONG));
        }

        @Override
        public void processElement(RawEvent e, Context ctx, Collector<MetricRow> out)
                throws Exception {
            boolean firstInHour = !seen.contains(e.userId);
            if (firstInHour) {
                seen.put(e.userId, e.eventTime);
                long bucket = (e.eventTime / JobConfig.WINDOW_SIZE_MS)
                        * JobConfig.WINDOW_SIZE_MS;
                out.collect(MetricRow.of(bucket, JobConfig.METRIC_UV, "", 1L, 0, 1));
            }
            // 每小时只注册一次清理计时器（避免每元素一个计时器把状态撑爆）
            if (registeredCleanup.value() == null) {
                long hourStart = ctx.getCurrentKey();
                ctx.timerService().registerEventTimeTimer(hourStart + JobConfig.UV_WINDOW_MS);
                registeredCleanup.update(hourStart);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricRow> out)
                throws Exception {
            List<String> expired = new ArrayList<>();
            for (var entry : seen.entries()) {
                expired.add(entry.getKey());
            }
            for (String userId : expired) {
                seen.remove(userId);
            }
        }
    }

    /** `ads_window_metric` 的一行（三态同表同键的载体）。 */
    public static class MetricRow {
        public java.sql.Timestamp windowStart;
        public String metricName;
        public String dimKey;
        public long metricValue;
        public int revision;
        public int isSettled;

        static MetricRow of(
                long windowStartMs, String metricName, String dimKey,
                long value, int revision, int isSettled) {
            MetricRow r = new MetricRow();
            r.windowStart = new java.sql.Timestamp(windowStartMs);
            r.metricName = metricName;
            r.dimKey = dimKey;
            r.metricValue = value;
            r.revision = revision;
            r.isSettled = isSettled;
            return r;
        }
    }
}
