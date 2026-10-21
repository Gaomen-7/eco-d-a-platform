package com.ecom.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 入口段：解析 → 质量分流（DLQ）→ 去重（重复不计脏）。
 *
 * <p><b>这一段解决的是票面 ② 里的两个「必须分开」</b>（gdd §D D2）：
 *
 * <ul>
 *   <li><b>真脏</b> → 侧输出到 DLQ，只认三个原因码：
 *       {@code schema_invalid} / {@code value_out_of_range} / {@code time_out_of_range}。
 *   <li><b>重复</b> → <b>绝不进 DLQ</b>，走独立通道累计
 *       {@code duplicate_absorbed}。若把重放当脏，脏率会凭空飙升，
 *       verify-fault 的「条数一致但 DLQ 上升 ⇒ fail」反例正是防这个。
 * </ul>
 *
 * <p><b>为什么去重要在这里做</b>：verify-fault 的证明逻辑是「kill TM → 从 checkpoint
 * 回滚 Kafka offset → 同一批 event_id 二次到达 → 被幂等吸收」。没有这一段，
 * 重放只会让指标翻倍，没有任何幂等可言。
 *
 * <p>去重状态保 <b>1 小时</b>（gdd §A.2「event_id 去重状态保 1 小时」），
 * 用事件时间计时器清理，<b>不用处理时间</b>——处理时间会让「同 seed 同结果」不成立。
 */
public final class GateSegment {

    private GateSegment() {}

    /** 真脏事件的侧输出（三原因码）。 */
    public static final OutputTag<RawEvent> DLQ = new OutputTag<RawEvent>("dlq") {};

    /**
     * 重复事件的侧输出。**与 {@link #DLQ} 互斥**：重复绝不进脏率分子（gdd §D D2）。
     *
     * <p>两条通道必须物理分开：若把重放当脏，脏率会凭空飙升，
     * verify-fault 的「条数一致但 DLQ 上升 ⇒ fail」反例正是防这个。
     */
    public static final OutputTag<RawEvent> DUPLICATE = new OutputTag<RawEvent>("duplicate") {};

    /**
     * 构建入口段。
     *
     * @return 干净且**非重复**的事件流（后续各段共用）
     */
    public static SingleOutputStreamOperator<RawEvent> build(
            StreamExecutionEnvironment env, DataStream<String> rawJson) {

        SingleOutputStreamOperator<RawEvent> parsed =
                rawJson.map(new ParseEvent())
                        .returns(Types.POJO(RawEvent.class))
                        .name("parse-event")
                        .uid("parse-event");

        // ★ 校验+去重是 **KeyedProcessFunction**（去重状态按 event_id 键控），
        //   故必须先 keyBy 再 process；侧输出也只有 SingleOutputStreamOperator 才拿得到。
        //   ★ 键选 event_id：去重的语义就是「同一 event_id 只算一次」。
        SingleOutputStreamOperator<RawEvent> clean =
                parsed.keyBy(e -> e.eventId == null ? "" : e.eventId)
                        .process(new ValidateAndDedup())
                        .name("validate-dedup")
                        .uid("validate-dedup");

        // DLQ 必须接 sink，否则会被作业图剪掉（CorrectionSegment 的教训）
        DataStream<RawEvent> dlq = clean.getSideOutput(DLQ);
        dlq.map(new ToDlqJson())
                .returns(Types.STRING)
                .name("dlq-payload")
                .uid("dlq-payload")
                .sinkTo(KafkaSinks.dlq())
                .name("dlq-sink")
                .uid("dlq-sink");

        return clean;
    }

    /**
     * 重复通道（供 MetricSegment 累计 `duplicate_absorbed`）。
     *
     * <p>★ 入参必须是 {@link SingleOutputStreamOperator}（build 的返回类型）——
     * 侧输出只有它拿得到；写成 {@code DataStream} 会编译失败 `cannot find symbol getSideOutput`。
     */
    public static DataStream<RawEvent> duplicates(SingleOutputStreamOperator<RawEvent> clean) {
        return clean.getSideOutput(DUPLICATE);
    }

    /**
     * 校验 + 去重。
     *
     * <p>顺序是承重的：**先校验、后去重**。
     * 脏事件连 {@code event_id} 都可能是坏的，拿去 dedup 会把好心数据也标成重复。
     *
     * <p>重复判定用 {@code event_id} 作状态键、**事件时间** 1 小时作 TTL。
     */
    public static class ValidateAndDedup extends KeyedProcessFunction<String, RawEvent, RawEvent> {

        private transient ValueState<Long> lastSeenMs;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            lastSeenMs = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("event-id-seen", Types.LONG));
        }

        @Override
        public void processElement(RawEvent e, Context ctx, Collector<RawEvent> out)
                throws Exception {
            // ① 校验：真脏 → DLQ（**不进** dedup 状态）
            String reason = EventQuality.reason(e);
            if (reason != null) {
                e.reason = reason;
                ctx.output(DLQ, e);
                return;
            }

            // ② 去重：同 event_id 在 1 小时内第二次到达 ⇒ **重复通道**，不进主流
            //    ★ 必须走 DUPLICATE 侧输出而不是直接 return：重复要能被计数
            //      （verify-fault 靠「重复通道计数上升」证明确有重放）。
            Long previous = lastSeenMs.value();
            if (previous != null) {
                ctx.output(DUPLICATE, RawEvent.duplicateOf(e));
                return;
            }
            lastSeenMs.update(e.eventTime);
            // 用事件时间清理：处理时间会让「同 seed 同结果」不成立
            ctx.timerService().registerEventTimeTimer(e.eventTime + JobConfig.DEDUP_TTL_MS);
            out.collect(e);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<RawEvent> out) {
            // 状态过期即释放；1 小时窗口滚动去重的语义由 DEDUP_TTL_MS 承担
            try {
                lastSeenMs.clear();
            } catch (Exception ignored) {
                // clear 在状态已释放时是幂等的
            }
        }
    }

    /** JSON → {@link RawEvent}，并附上「原始 JSON」供 DLQ 落原文。 */
    public static class ParseEvent extends RichMapFunction<String, RawEvent> {
        @Override
        public RawEvent map(String json) {
            return RawEvent.parse(json);
        }
    }

    /** DLQ 记录：保留原始 JSON，另加机读原因码（票面 ② 要求「带机读原因码」）。 */
    public static class ToDlqJson extends RichMapFunction<RawEvent, String> {
        @Override
        public String map(RawEvent e) {
            return e.toDlqJson();
        }
    }

    /** Kafka source 构造（主 topic）：水印策略与允许延迟一致。 */
    public static DataStream<String> mainSource(
            StreamExecutionEnvironment env, String groupSuffix) {
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(JobConfig.KAFKA_BOOTSTRAP)
                        .setTopics(JobConfig.TOPIC_MAIN)
                        .setGroupId(JobConfig.CONSUMER_GROUP + groupSuffix)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        return env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(
                                Duration.ofMillis(JobConfig.ALLOWED_LATENESS_MS))
                        .withTimestampAssigner((s, ts) -> RawEvent.parseEventTime(s)),
                "gate-source" + groupSuffix);
    }

}
