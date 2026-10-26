package com.ecom.flink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.time.Duration;

/**
 * source 的**时间策略**：起始偏移（案 2）与水印生成器（案 3）。
 *
 * <h2>R7-i 三案里的两案都落在「source 怎么读、什么时候给时间戳」这一个接缝上</h2>
 * <p>两者抽到本类，是因为它们必须被<b>判据直接求值</b>：
 * {@code flink/src/test/java/com/ecom/flink/R7iFalsification.java} 把
 * {@link #startingOffsets()} 放到桩 retriever 上求起始决策、把
 * {@link #watermarkGenerator(long)} 喂「合法事件 + 未来脏事件」看水印。
 * 若这两处埋在 {@code mainSource()} 的链式调用里，判据只能靠 grep 源码字符串，
 * 那就能被无关改动满足（impl-02d 判据纪律：判据要能证伪）。
 *
 * <h2>案 2：起始偏移 = committedOffsets(EARLIEST)</h2>
 * <p>旧实现 {@code OffsetsInitializer.earliest()} 让 KafkaSource <b>无视消费组的
 * committed offsets</b>，每次作业提交都从 topic 头全量重放。实测（impl-05 §4）：
 * TM 日志逐字 {@code Seeking to AutoOffsetResetStrategy{type=earliest}}，两次独立
 * 提交的 {@code numRecordsOut} 均恰等于 topic 全量 53,789。
 *
 * <p>{@code committedOffsets(EARLIEST)} 的官方契约（Flink Kafka connector 源码
 * {@code OffsetsInitializer.committedOffsets(OffsetResetStrategy)}）：先查该消费组
 * 的 committed offsets，**查不到**时按给定的 reset 策略（此处 EARLIEST）回落。
 * ⇒ 作业重提只读消费组 committed 之后的增量，全新消费组仍从头读（earliest 语义
 * 不丢）。
 *
 * <p>⚠️ 修复后**首次重提仍会全量重放一次**：语义切换前的 committed offsets 是本
 * 项目旧口径留下的（票面 Notes 已登记为一次性的运维动作，不算回归失败）。
 *
 * <h2>案 3：被质量门拒绝的事件不得抬水印</h2>
 * <p>旧链路是 {@code KafkaSource.withTimestampAssigner((s, ts) -> RawEvent.parseEventTime(s))}
 * —— 时间戳分配发生在 <b>格式转换之前</b>，质量门在下游，于是：
 * <ol>
 *   <li>一条 et = now + 2 h 的脏事件先把 {@code BoundedOutOfOrdernessWatermarks}
 *       的 max 抬到 now + 2 h；
 *   <li>质量门随后才判它 {@code time_out_of_range} 并送去 DLQ —— <b>判得太晚</b>，
 *       水印已经被抬过了。
 * </ol>
 * 实测（impl-05 §4）：水印被钉在未来约 2 h，此后一切真实 et≈now 的事件命中
 * {@code WindowSumProcess} 的超期旁路被静默丢弃，直到墙钟追过未来尾。
 *
 * <p>修法：**水印生成器自己先过质量门**（{@link GatedWatermarks}）——
 * 被拒事件不参与时间戳/水印计算，LOWER_BOUND 照旧向下有界（下游窗口仍会关闭）。
 * 质量门本身仍在 {@code ValidateAndDedup} 里跑一次（DLQ 分流需要它）；两处各读
 * 一次墙钟（非同一 instant，毫秒级差远小于 5 min 裕度、不影响结论——见
 * {@link EventQuality#reason(RawEvent, long)} 注释）。
 */
public final class SourceTimePolicy {

    private SourceTimePolicy() {}

    /**
     * source 的起始偏移决策器：**committed 优先，无 committed 回落 earliest**。
     *
     * <p>见类注释「案 2」。这条也是判据 {@code R7iFalsification#checkCommittedOffsetsSemantics}
     * 的入口——判据把它放在桩 retriever 上求值，故它必须是可独立调用的纯工厂。
     */
    public static OffsetsInitializer startingOffsets() {
        return OffsetsInitializer.committedOffsets(
                org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
    }

    /**
     * source 的水印生成器（**先过质量门**）：被拒事件不抬水印。
     *
     * <p>★ 这里**不冻结时钟**：生成器的质量门必须用 <b>每条事件到达时的</b>墙钟判
     * 「不晚于 now + 裕度」。{@code GatedWatermarks} 拿到 {@link Clock} 而不是一个
     * 固定的 long —— 首版传 {@code System.currentTimeMillis()} 的快照，作业 RUNNING
     * 超过 5 min（未来的裕度）后**一切真实事件都被判 {@code time_out_of_range}**，
     * 与 R7-i 的「静默误丢」同形（实测：作业 04:02 提交、04:29/04:35 两轮探针批
     * 全部进了 DLQ，Doris 只剩 {@code provisional_emitted} 行）。
     *
     * <p>判据侧仍需可复现 ⇒ 判据走固定时钟重载
     * {@code watermarkGenerator(long)}（包私有，仅供判据）；本工厂固定用墙钟。
     */
    public static WatermarkGenerator<String> watermarkGenerator() {
        return new GatedWatermarks(System::currentTimeMillis, JobConfig.ALLOWED_LATENESS_MS);
    }

    /** 判据专用：固定时钟的生成器（可复现，不依赖墙钟）。 */
    static WatermarkGenerator<String> watermarkGenerator(long fixedNowMs) {
        return new GatedWatermarks(() -> fixedNowMs, JobConfig.ALLOWED_LATENESS_MS);
    }

    /**
     * 带质量门的水印生成器：**先判脏、再抬水印**（案 3 的落点）。
     *
     * <h2>为什么不用 {@code WatermarkStrategy.withTimestampAssigner} 里的过滤</h2>
     * <p>时间戳分配器只能返回一个 long，**无法表达「这条不要」**（返回 0 或
     * MIN_VALUE 仍会进 {@code onEvent} 抬高水印）。故这里不用内置的
     * {@code BoundedOutOfOrdernessWatermarks}，而是实现
     * {@link WatermarkGenerator} 的同一语义（max − outOfOrderness − 1，见其源码），
     * 唯一差别：脏事件的 et 不更新 max。
     *
     * <h2>下界必须与内置实现逐字相同（为什么不能拿 nowMs 当前水印初值）</h2>
     * <p>内置 {@code BoundedOutOfOrdernessWatermarks} 的初值是
     * {@code Long.MIN_VALUE + outOfOrderness + 1}（javap 实证 2.2.1），故未见事件时
     * 发出的水印恰是 {@code Long.MIN_VALUE}。这条初值**不是**随手取的：下游
     * {@code WindowSumProcess} 与窗口算子都以「水印是否越过 deadline」判超期，
     * 若初值取 {@code nowMs − lateness}，作业冷启后的第一条真实事件就会被误判超期
     * （deadline 恒在 nowMs 之前）—— 那正是 R7-i 案 3 要消灭的「静默误丢」形态，
     * 只是诱因从"未来脏事件"换成了"初值太高"。
     * <p>⇒ 初值**照抄**内置实现，只在 {@code onEvent} 的准入上做区分。
     */
    public static final class GatedWatermarks implements WatermarkGenerator<String> {

        /** 内置实现同款：水印 = max − outOfOrderness − 1（毫秒）。 */
        private final long maxOutOfOrdernessMs;

        /** 时钟：生产路径是墙钟（每条事件取一次），判据是固定值。 */
        private final Clock clock;

        /**
         * 只由**合法**事件推进。
         *
         * <p>初值逐字照抄内置实现的构造器（javap 实证 2.2.1）：
         * {@code maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1}
         * ⇒ 未见事件时 {@code onPeriodicEmit} 发的水印恰是 {@code Long.MIN_VALUE}。
         * <b>不能</b>自己用 {@code Long.MIN_VALUE + 1} 起（那样减完 outOfOrderness
         * 会**下溢**成极大的正数，水印一步跨到未来 —— 恰好是要消掉的病）。
         */
        private long maxTimestampMs;

        GatedWatermarks(Clock clock, long maxOutOfOrdernessMs) {
            this.clock = clock;
            this.maxOutOfOrdernessMs = maxOutOfOrdernessMs;
            this.maxTimestampMs = Long.MIN_VALUE + maxOutOfOrdernessMs + 1;
        }

        /** 可注入时钟：生产＝墙钟，判据＝固定值（同一条判据不依赖跑批时刻）。 */
        @FunctionalInterface
        interface Clock {
            long nowMs();
        }

        @Override
        public void onEvent(String json, long eventTimestamp, WatermarkOutput output) {
            // ★★ 案 3 的判据就在这三行上 ★★
            // (a) 不可能推进水印的事件（et ≤ 已见最大）直接跳过：Math.max 的结果不变，
            //     连质量门都不必跑 ⇒ 判脏开销只在「本会推进水印」的事件上付出，
            //     而不是每条事件都重复 parse 一遍。
            if (eventTimestamp <= maxTimestampMs) {
                return;
            }
            // (b) 会推进水印的候选：先过质量门。被拒 ⇒ **不**采信它的 et。
            String reason = rejectReason(json, clock.nowMs());
            if (reason != null) {
                return;
            }
            maxTimestampMs = eventTimestamp;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(
                    new Watermark(maxTimestampMs - maxOutOfOrdernessMs - 1));
        }

        /** 供判据使用：当前时钟读数（生产路径必须是**会走的**墙钟，不是快照）。 */
        long clockNowMs() {
            return clock.nowMs();
        }

        /** 供判据使用：这条事件会不会被质量门拒（{@code null} ⇒ 合法）。 */
        static String rejectReason(String json, long nowMs) {
            return EventQuality.reason(RawEvent.parse(json), nowMs);
        }
    }
}
