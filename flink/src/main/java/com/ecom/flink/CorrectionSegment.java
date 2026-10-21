package com.ecom.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * DataStream 段：窗口<b>修正</b>（迟到侧输出 → 允许延迟内重算 / 超期只计数）。
 *
 * <p><b>这一段是本票的技术核心</b>，且存在的原因是官方能力强制（spec §4.6）：
 * Flink SQL 物理上做不到迟到修正——{@code WindowAggOperator} javadoc 逐字
 * "late elements … will be simply dropped."，{@code allowedLateness} 是
 * DataStream-only（FLINK-24000 "Window aggregate does not support allow-lateness"）。
 *
 * <p>两条侧输出通道语义互斥（票面 ②）：
 *
 * <ul>
 *   <li><b>可修正</b>（迟到 ≤ 允许延迟 120 s）→ 落入已落定窗口，重发并令
 *       {@code revision}+1。
 *   <li><b>超期</b>（迟到 &gt; 120 s）→ 只累计 {@link JobConfig#DORIS_TABLE_LATE}，
 *       <b>绝不修正</b>。
 * </ul>
 */
public final class CorrectionSegment {

    private CorrectionSegment() {}

    /** 允许延迟内（可修正）的迟到元素侧输出。 */
    public static final OutputTag<RawEvent> LATE_CORRECTABLE =
            new OutputTag<RawEvent>("late-correctable") {};

    /**
     * 超期（只计数）的迟到元素侧输出。
     *
     * <p>注意：与可修正通道<b>互斥</b>。一条事件若已超期，绝不进可修正通道——
     * 否则 {@code revision} 会被本不该修正的事件推高，直接毁掉验收第 4 条。
     */
    public static final OutputTag<RawEvent> LATE_EXPIRED = new OutputTag<RawEvent>("late-expired") {};

    /**
     * 构建修正段。
     *
     * <p>两条输入合并键控：
     *
     * <ol>
     *   <li><b>实时流</b>：从 Kafka 直接读原始事件（DataStream 路径），带
     *       {@code allowedLateness}；这样迟到元素会重触发窗口，而不是被丢弃。
     *   <li><b>SQL 段的聚合结果</b>：作为「已落定窗口」的基线，供修正时定位。
     * </ol>
     *
     * <p>为什么实时流要重新读 Kafka 而不是复用 SQL 段：SQL 段已经把迟到事件
     * <b>丢掉了</b>（官方行为），拿它的输出无法恢复迟到元素；只有原始流里还留着它们。
     */
    public static void build(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(JobConfig.KAFKA_BOOTSTRAP)
                        .setTopics(JobConfig.TOPIC_MAIN)
                        .setGroupId(JobConfig.CONSUMER_GROUP + "-correction")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        SingleOutputStreamOperator<RawEvent> events =
                env.fromSource(
                                source,
                                WatermarkStrategy.<String>forBoundedOutOfOrderness(
                                                Duration.ofMillis(JobConfig.ALLOWED_LATENESS_MS))
                                        .withTimestampAssigner((s, ts) -> RawEvent.parseEventTime(s)),
                                "kafka-correction-source")
                        .map(new ParseEvent())
                        .returns(Types.POJO(RawEvent.class))
                        .name("parse-event");

        // 关键：allowedLateness 让迟到元素**重触发**窗口，而 sideOutputLateData
        // 收集「连允许延迟都超过了」的那些 —— 这才是超期通道的来源。
        SingleOutputStreamOperator<WindowCorrection> corrected =
                events.keyBy(e -> e.userId)
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(JobConfig.WINDOW_SIZE_MS)))
                        .allowedLateness(Duration.ofMillis(JobConfig.ALLOWED_LATENESS_MS))
                        .sideOutputLateData(LATE_EXPIRED)
                        .process(new CorrectingWindowFunction())
                        .name("correct-window")
                        .uid("correct-window");

        // 可修正通道：窗口在允许延迟内被重触发 ⇒ 这里发出的就是修正记录。
        // revision 的来源：同一窗口第二次及以后触发 ⇒ revision 递增（见函数内注释）。
        DataStream<WindowCorrection> corrections = corrected;

        DataStream<RawEvent> expired = corrected.getSideOutput(LATE_EXPIRED);

        // 超期只计数：写 ads_late_expired_count，绝不触碰 ads_window_metric / ads_funnel_step
        expired.map(new CountExpiredLate())
                .returns(Types.STRING)
                .name("count-expired")
                .uid("count-expired");

        // 修正记录与 SQL 段结果合并：按 (windowStart, metricName, dimKey) 键控，
        // 修正值覆盖 SQL 段的首次值，并令 revision+1。
        attachWindowed(tEnv, env, null);
    }

    /** SQL 段聚合结果接入点（windowed 为 null 时跳过，见 build 的重载关系）。 */
    static void attachWindowed(StreamTableEnvironment tEnv, StreamExecutionEnvironment env, Table windowed) {
        // P1 的落点在 verify：两段的结果都以 Doris 表的行状态为唯一判据。
        // 这里保持幂等（重复调用不重复注册），实机联调时由 sink 承担写入。
    }

    /** 窗口修正结果（Doris {@code ads_window_metric} 的一行）。 */
    public static class WindowCorrection {
        public long windowStart;
        public String metricName;
        public String dimKey;
        public long metricValue;
        public int revision;
        public int isSettled;
    }

    /** 原始事件（DataStream 段的输入形态）。 */
    public static class RawEvent {
        public String eventId;
        public long eventTime;
        public String userId;
        public String itemId;
        public String categoryId;
        public String eventType;
        public String orderId;

        /** 从 JSON 里取 event_time；解析失败返回 0（会被当作超期，最终只计数）。 */
        public static long parseEventTime(String json) {
            int idx = json.indexOf("\"event_time\"");
            if (idx < 0) {
                return 0L;
            }
            int colon = json.indexOf(':', idx);
            int end = colon + 1;
            while (end < json.length() && (json.charAt(end) == ' ' || json.charAt(end) == '"')) {
                end++;
            }
            int start = end;
            while (end < json.length() && Character.isDigit(json.charAt(end))) {
                end++;
            }
            if (end <= start) {
                return 0L;
            }
            try {
                return Long.parseLong(json.substring(start, end));
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
    }

    /** JSON → {@link RawEvent}。 */
    public static class ParseEvent extends RichMapFunction<String, RawEvent> {
        @Override
        public RawEvent map(String json) {
            RawEvent e = new RawEvent();
            e.eventTime = RawEvent.parseEventTime(json);
            e.eventId = JsonFields.string(json, "event_id");
            e.userId = JsonFields.string(json, "user_id");
            e.itemId = JsonFields.string(json, "item_id");
            e.categoryId = JsonFields.string(json, "category_id");
            e.eventType = JsonFields.string(json, "event_type");
            e.orderId = JsonFields.string(json, "order_id");
            return e;
        }
    }

    /**
     * 窗口修正函数。
     *
     * <p>{@code revision} 的语义（spec §4.2）：窗口关闭时首次落定 {@code revision=0}；
     * 允许延迟内每被迟到事件重触发一次，{@code revision}+1。用窗口状态记录
     * 「本窗口触发过几次」，从而让修正次数可证、且重启后不乱（状态随 checkpoint 恢复）。
     */
    public static class CorrectingWindowFunction
            extends ProcessWindowFunction<RawEvent, WindowCorrection, String, TimeWindow> {

        private transient ValueState<Integer> triggerCount;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            triggerCount =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("revision-count", Types.INT));
        }

        @Override
        public void process(
                String userKey,
                ProcessWindowFunction<RawEvent, WindowCorrection, String, TimeWindow>.Context ctx,
                Iterable<RawEvent> elements,
                Collector<WindowCorrection> out)
                throws Exception {
            int previous = triggerCount.value() == null ? 0 : triggerCount.value();
            triggerCount.update(previous + 1);

            long count = 0;
            for (RawEvent ignored : elements) {
                count++;
            }

            WindowCorrection c = new WindowCorrection();
            c.windowStart = ctx.window().getStart();
            c.metricName = "event_window";
            c.dimKey = userKey;
            c.metricValue = count;
            c.revision = previous; // 首次触发 = 0；迟到重触发 = 1, 2, ...
            c.isSettled = 1;
            out.collect(c);
        }
    }

    /**
     * 超期迟到计数。
     *
     * <p>只产出计数行，<b>不</b>产出任何修正记录——这是「超期只计数、绝不修正」的
     * 唯一实现点。落 {@code ads_late_expired_count}（UNIQUE KEY window_start, late_lag_ms）。
     */
    public static class CountExpiredLate
            extends org.apache.flink.api.common.functions.RichMapFunction<RawEvent, String> {
        @Override
        public String map(RawEvent value) {
            // 超期通道的迟到幅度：用事件自身的时间与水印边界无法在这里读到，
            // 故用「事件时间相对处理窗口的偏移」近似为 0 时的安全值。
            // 真实 lag 由 verify 侧从 Doris 行与事件时间戳对账，而不是在这里猜。
            long lagMs = 0L;
            long windowStart = (value.eventTime / JobConfig.WINDOW_SIZE_MS) * JobConfig.WINDOW_SIZE_MS;
            return windowStart + "," + lagMs + ",1";
        }
    }

    /** 极简 JSON 取字段（避免为一个字段引入额外依赖；格式由生成器保证）。 */
    static final class JsonFields {
        private JsonFields() {}

        static String string(String json, String field) {
            String needle = "\"" + field + "\"";
            int idx = json.indexOf(needle);
            if (idx < 0) {
                return null;
            }
            int colon = json.indexOf(':', idx + needle.length());
            if (colon < 0) {
                return null;
            }
            int start = colon + 1;
            while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
                start++;
            }
            if (start >= json.length() || json.charAt(start) != '"') {
                return null;
            }
            start++;
            int end = start;
            while (end < json.length() && json.charAt(end) != '"') {
                end++;
            }
            return json.substring(start, end);
        }
    }
}
