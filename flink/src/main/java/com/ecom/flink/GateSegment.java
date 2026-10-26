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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


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

        // ★★ DLQ 必须进 StatementSet（impl-02d 实机证伪，2026-10-22）★★
        //   旧实现这里是 `dlq.map(...).sinkTo(KafkaSinks.dlq())`，看起来「接了 sink」，
        //   但作业是**用 StatementSet 提交的**（DorisSinkSegment.attach → statements.execute()），
        //   而 StatementSet 只执行 addInsertSql 的那几张表 —— 挂在 env 上的
        //   DataStream sink **根本不进作业图**。
        //
        //   实机证据（决定性）：`/jobs/<jid>` 的 plan 里只有 7 个顶点，
        //   `validate-dedup` 的输出只列出 `count-duplicate` 与 `funnel-only`，
        //   **没有 dlq**；投 5 条缺 `event_type` 的必脏事件后，
        //   源端 read-records 268→273（确实读到了），而 DLQ topic 六个分区
        //   offset 全为 0、`.log` 字节数全为 0。
        //
        //   后果比「DLQ 是空的」更严重：verify-fault 的「DLQ 增量==0」因此是
        //   **恒真的假断言** —— 它断言的是一条根本没接线的通道。票面验收第 7 条
        //   （改坏输入 ⇒ 退非 0）正是靠这一点发现的。
        //
        //   修法：把 DLQ 也变成 StatementSet 的一张 Kafka 表。返回该流，
        //   由 `P1RealtimeJob` 连同其他 INSERT 一起提交（单作业、单 checkpoint 边界）。
        return clean;
    }

    /**
     * 把入口段的 DLQ 侧输出注册成 Kafka 表（`sink_dlq`）。
     *
     * <p>为什么不再是 {@code sinkTo(KafkaSinks.dlq())}：见 {@link #build} 的注释 ——
     * StatementSet 提交会丢掉 env 上的 DataStream sink。改成 Table 后，
     * DLQ 与其他三张表在**同一次** {@code statements.execute()} 里提交。
     *
     * <p>配套用法：先调本方法建表，再调 {@link #createDlqView} 产出 `view_dlq`，
     * 最后由 {@code DorisSinkSegment.attach} 里的 {@code INSERT INTO sink_dlq}
     * 把它纳入 StatementSet。
     */
    public static void registerDlqSink(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS sink_dlq (\n"
                        + "  payload STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '"
                        + JobConfig.TOPIC_DLQ
                        + "',\n"
                        + "  'properties.bootstrap.servers' = '"
                        + JobConfig.KAFKA_BOOTSTRAP
                        + "',\n"
                        + "  'format' = 'raw'\n"
                        + ")");
    }

    /** DLQ 的待插视图：`INSERT INTO sink_dlq SELECT payload FROM view_dlq`。 */
    public static void createDlqView(
            StreamTableEnvironment tEnv, SingleOutputStreamOperator<RawEvent> clean) {
        DataStream<RawEvent> dlq = clean.getSideOutput(DLQ);
        DataStream<String> payload = dlq.map(new ToDlqJson())
                .returns(Types.STRING)
                .name("dlq-payload")
                .uid("dlq-payload");
        // ★ 必须给列起名（`tEnv.fromDataStream` 的默认列名是 `f0`）：
        //   直接用 `createTemporaryView(name, stream)` 会让列叫 `f0`，
        //   而 `INSERT INTO sink_dlq SELECT payload FROM view_dlq` 会报
        //   实机错误原文：`Column 'payload' not found in any table`。
        tEnv.createTemporaryView(
                "view_dlq",
                tEnv.fromDataStream(payload).as("payload"));
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

    /**
     * Kafka source 构造（主 topic）：起始偏移与水位策略都由 {@link SourceTimePolicy} 定。
     *
     * <h2>★ R7-i 两案在此收口（impl-06d）</h2>
     * <ol>
     *   <li><b>起始偏移</b>：旧实现 {@code OffsetsInitializer.earliest()} ⇒ 无视
     *       committed offsets、每次提交全量重放。改为
     *       {@link SourceTimePolicy#startingOffsets()}（committed 优先、无则 earliest）。
     *   <li><b>时间戳分配先于质量门</b>：旧实现用
     *       {@code forBoundedOutOfOrderness(...).withTimestampAssigner((s, ts) -> parseEventTime(s))}
     *       ⇒ 未来脏事件在**被判脏之前**就把水位抬到未来。改为
     *       {@link SourceTimePolicy#watermarkGenerator()}（先过质量门再抬水位）。
     * </ol>
     *
     * <p>⚠️ 这里**不再显式设时间戳分配器**：水位生成器自己读 {@code event_time}
     * 判合法性并推进 max，{@code withTimestampAssigner} 的返回值只用于下游算窗口
     * 归属（脏事件到不了下游）。两者必须读**同一个** et 口径，故时间戳仍由
     * {@link RawEvent#parseEventTime(String)} 给——见下。
     *
     * <p><b>判据</b>：{@code scripts/r7i-falsify.sh}（退出码即判据）。
     */
    public static DataStream<String> mainSource(
            StreamExecutionEnvironment env, String groupSuffix) {
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(JobConfig.KAFKA_BOOTSTRAP)
                        .setTopics(JobConfig.TOPIC_MAIN)
                        .setGroupId(JobConfig.CONSUMER_GROUP + groupSuffix)
                        .setStartingOffsets(SourceTimePolicy.startingOffsets())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        return env.fromSource(
                source,
                // ★ 生成器**不冻结时钟**：质量门必须用每条事件到达时的墙钟判
                //   「不晚于 now + 裕度」。首版在提交时取一次 nowMs，作业 RUNNING
                //   超过 5 min 后一切真实事件都被判 time_out_of_range（实测：
                //   04:02 提交的作业在 04:29/04:35 两轮探针批全部进 DLQ，Doris 只剩
                //   provisional_emitted 行 —— 与 R7-i 的静默误丢同形）。
                WatermarkStrategy.<String>forGenerator(
                                ignored -> SourceTimePolicy.watermarkGenerator())
                        // 时间戳仍由同一份 et 口径给：下游窗口归属用它。
                        // 脏事件的 et 不参与水位，故这里给它什么都不会劫持链路。
                        .withTimestampAssigner((s, ts) -> RawEvent.parseEventTime(s)),
                "gate-source" + groupSuffix);
    }

}
