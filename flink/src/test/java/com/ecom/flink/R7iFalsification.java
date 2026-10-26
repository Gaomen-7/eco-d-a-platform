package com.ecom.flink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Collection;
import java.util.Map;

/**
 * R7-i 两案的**机读反例证伪**（impl-06d；案 1 在 {@code tests/test_cli.py}）。
 *
 * <h2>为什么不是 JUnit</h2>
 * <p>离线环境（WSL + Maven 本地缓存）里没有可直接跑的 JUnit：实测
 * {@code ~/.m2/repository/org/junit/jupiter} 不存在、{@code junit/junit} 只有
 * 3.8.1 的 pom（无 jar）、{@code surefire} 只有 plugin jar 没有 provider。
 * 引入新依赖必须联网，而本票验收面要求离线可复现 ⇒
 * <b>纯 javac + java 断言程序，退出码即判据</b>（0=全绿，1=有反例）。
 * 跑法：{@code bash scripts/r7i-falsify.sh}。
 *
 * <h2>判据打的是**行为**，不是源码字符串</h2>
 * <ul>
 *   <li><b>案 2</b>：把生产代码的起始偏移决策器（{@link SourceTimePolicy#startingOffsets()}）
 *       放到**桩 retriever** 上求值 —— 有 committed 时起始必须 == committed。
 *       旧实现 {@code OffsetsInitializer.earliest()} 在此必然返回 beginning ⇒ 红。
 *   <li><b>案 3</b>：把生产代码的水位生成器（{@link SourceTimePolicy#watermarkGenerator(long)}）
 *       喂「一条合法事件 + 一条未来 2 h 的脏事件」—— 水位必须不被脏事件抬起。
 *       旧实现（{@code BoundedOutOfOrdernessWatermarks} 裸用、时间戳在 parse 之前赋值）
 *       会抬到脏事件 et − 允许延迟 ⇒ 红。
 * </ul>
 * 两处都不是「grep 源码有没有新方法名」，故不会被无关改动满足。
 */
public final class R7iFalsification {

    private static int failures = 0;

    /** 显式传时钟，避免判据自身依赖墙钟（可复现）。 */
    static final long NOW_MS = 1_800_000_000_000L;

    public static void main(String[] args) throws Exception {
        checkCommittedOffsetsSemantics();
        checkRejectedEventDoesNotAdvanceWatermark();
        checkDirtyNonTimeReasonDoesNotAdvanceWatermark();
        checkInitialWatermarkStaysAtLowerBound();
        checkProductionClockIsLive();

        if (failures == 0) {
            System.out.println("R7iFalsification: PASS（案 2 / 案 3 判据全绿）");
        } else {
            System.out.println("R7iFalsification: FAIL（" + failures + " 条反例未转绿）");
            System.exit(1);
        }
    }

    // ── 案 2：冷启只消费未消费的增量（committed offsets 生效）──────────

    /**
     * 起始偏移决策器必须**表达 committed 优先**，且无 committed 时回落 earliest。
     *
     * <h2>★ 判据为什么不能去 retriever 上求值（本轮实证，2026-10-25）</h2>
     * <p>flink-connector-kafka 5.0.0-2.2 的 {@code committedOffsets()} 是
     * {@code ReaderHandledOffsetsInitializer(COMMITTED_OFFSET = -3, ...)}：它在
     * {@code getPartitionOffsets} 里对**每个分区原样返回 -3**（javap 实证），
     * 真正的「committed 优先」由 **reader 侧**
     * {@code KafkaPartitionSplitReader} 解析（{@code parseStartingOffsets} 对 -3
     * 单独分支、{@code KafkaPartitionSplit.COMMITTED_OFFSET} 常量实证）。
     * ⇒ 在桩 retriever 上求值只能得到 -2 / -3 这类 sentinel，**判不出**语义差异；
     * 硬写 sentinel 数字又会被等价写法（如自建 initializer）误判。
     *
     * <p>故判据改为打**两层可判别的事实**：
     * <ol>
     *   <li>决策器的 auto-offset-reset 策略是 EARLIEST ⇒ 「无 committed 时不丢
     *       earliest 语义」（这是 {@code committedOffsets(EARLIEST)} 与
     *       {@code committedOffsets()} 的**唯一**差别，可判别）；
     *   <li>决策器**不是**「对每个分区一律给 beginning」的那一类 ⇒ 排除旧语义。
     *       旧 {@code earliest()} 的 {@code getPartitionOffsets} 返回 -2（EARLIEST
     *       sentinel）而非 -3，与 committed 决策器**在 retriever 层可区分**。
     * </ol>
     * <p>加上真机侧证据（{@code numRecordsOut} 与消费组 committed 之差、TM 日志
     * {@code Seeking to committed offsets}），两层合起来才构成完整判据。
     */
    static void checkCommittedOffsetsSemantics() {
        OffsetsInitializer init = SourceTimePolicy.startingOffsets();

        // ① 无 committed 时不得丢 earliest 语义
        org.apache.kafka.clients.consumer.OffsetResetStrategy reset =
                init.getAutoOffsetResetStrategy();
        if (reset != org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST) {
            fail("案2 A：reset 策略应为 EARLIEST（无 committed 时回落 earliest 语义），实得 " + reset);
        } else {
            System.out.println("[案2 A] 无 committed 时回落 EARLIEST：PASS");
        }

        // ② 决策器不得是「一律 beginning」那一类（旧 earliest() 的形态）
        StubRetriever caughtUp = new StubRetriever(0L, 60000L, 53789L);
        Long startA = init.getPartitionOffsets(caughtUp.partitions(), caughtUp)
                .get(caughtUp.partition());
        if (startA == null) {
            fail("案2 B：决策器无返回值");
        } else if (startA == org.apache.flink.connector.kafka.source.split
                .KafkaPartitionSplit.EARLIEST_OFFSET) {
            fail("案2 B：决策器返回 EARLIEST sentinel(-2) ⇒ 无视 committed、每次冷启全量重放");
        } else if (startA != org.apache.flink.connector.kafka.source.split
                .KafkaPartitionSplit.COMMITTED_OFFSET) {
            fail("案2 B：决策器既不是 committed(-3) 也不是 earliest(-2)，实得 " + startA);
        } else {
            System.out.println("[案2 B] 决策器交给 reader 解析 committed(-3)：PASS");
        }

        // ③ 反例侧的对照组：旧实现 earliest() 在同一桩上必须给出 EARLIEST sentinel
        //    ⇒ 上一条 ② 的判别力是真实存在的，不是两条恒真断言对着同一个值。
        Long legacy = OffsetsInitializer.earliest()
                .getPartitionOffsets(caughtUp.partitions(), caughtUp)
                .get(caughtUp.partition());
        if (legacy == null
                || legacy != org.apache.flink.connector.kafka.source.split
                        .KafkaPartitionSplit.EARLIEST_OFFSET) {
            fail("案2 C：对照组前提不成立——earliest() 应返回 -2，实得 " + legacy);
        } else {
            System.out.println("[案2 C] 对照组 earliest() 返回 -2（与 committed -3 可区分）：PASS");
        }
    }

    /** 桩 retriever：固定 committed/beginning/end 输入，只测**决策**。 */
    static final class StubRetriever
            implements OffsetsInitializer.PartitionOffsetsRetriever {

        private final org.apache.kafka.common.TopicPartition tp =
                new org.apache.kafka.common.TopicPartition("ecom.behavior.event.v1", 0);
        private final long beginning;
        private final long end;
        private final Long committed;

        StubRetriever(long beginning, long end, Long committed) {
            this.beginning = beginning;
            this.end = end;
            this.committed = committed;
        }

        org.apache.kafka.common.TopicPartition partition() {
            return tp;
        }

        Collection<org.apache.kafka.common.TopicPartition> partitions() {
            return java.util.List.of(tp);
        }

        @Override
        public Map<org.apache.kafka.common.TopicPartition, Long> committedOffsets(
                Collection<org.apache.kafka.common.TopicPartition> partitions) {
            return committed == null ? Map.of() : Map.of(tp, committed);
        }

        @Override
        public Map<org.apache.kafka.common.TopicPartition, Long> endOffsets(
                Collection<org.apache.kafka.common.TopicPartition> partitions) {
            return Map.of(tp, end);
        }

        @Override
        public Map<org.apache.kafka.common.TopicPartition, Long> beginningOffsets(
                Collection<org.apache.kafka.common.TopicPartition> partitions) {
            return Map.of(tp, beginning);
        }

        @Override
        public Map<org.apache.kafka.common.TopicPartition,
                        org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes(
                Map<org.apache.kafka.common.TopicPartition, Long> timestampsToSearch) {
            return Map.of();
        }
    }

    // ── 案 3：被质量门拒绝的事件不得抬水位 ─────────────────────────────

    /**
     * 喂「合法事件 et=now−1s」并周期发射，再喂「未来 2 h 的脏事件」并周期发射：
     * 水位必须停留在 ≈ now−1s−允许延迟，**不得**跳到脏事件附近。
     *
     * <p>实测劫持幅度：gen-a 的尾部未来事件 et ≈ +2.1 h（impl-05 §4）。
     */
    static void checkRejectedEventDoesNotAdvanceWatermark() {
        WatermarkGenerator<String> gen = SourceTimePolicy.watermarkGenerator(NOW_MS);
        Sink sink = new Sink();

        long cleanEt = NOW_MS - 1_000L;
        long dirtyEt = NOW_MS + 2 * 3_600_000L; // +2 h

        gen.onEvent(eventJson("e-clean", cleanEt), cleanEt, sink);
        gen.onPeriodicEmit(sink);
        long afterClean = sink.last;

        // 前置：这条脏事件**必须**被判 time_out_of_range（否则反例不成立）
        String reason = SourceTimePolicy.GatedWatermarks.rejectReason(
                eventJson("e-dirty", dirtyEt), NOW_MS);
        if (!JobConfig.REASON_TIME_OUT_OF_RANGE.equals(reason)) {
            fail("案3 前置：未来 2 h 的事件未被判 time_out_of_range，实得 reason=" + reason);
            return;
        }

        gen.onEvent(eventJson("e-dirty", dirtyEt), dirtyEt, sink);
        gen.onPeriodicEmit(sink);
        long afterDirty = sink.last;

        if (afterDirty > afterClean) {
            fail("案3 A：被拒的未来事件把水位抬了（before=" + afterClean
                    + " after=" + afterDirty + "，脏事件 et=" + dirtyEt
                    + "，劫持幅度 " + (afterDirty - afterClean) + " ms）");
        } else if (afterDirty < cleanEt - JobConfig.ALLOWED_LATENESS_MS - 1) {
            fail("案3 A：合法事件的水位反而不对（" + afterDirty
                    + "，期望 ≈ " + (cleanEt - JobConfig.ALLOWED_LATENESS_MS) + "）");
        } else {
            System.out.println("[案3 A] 未来脏事件不抬水位（水位=" + afterDirty + "）：PASS");
        }
    }

    /** 非时间型脏事件（缺必填字段）带未来 et，同样不得抬水位。 */
    static void checkDirtyNonTimeReasonDoesNotAdvanceWatermark() {
        WatermarkGenerator<String> gen = SourceTimePolicy.watermarkGenerator(NOW_MS);
        Sink sink = new Sink();

        long cleanEt = NOW_MS - 1_000L;
        gen.onEvent(eventJson("e-clean", cleanEt), cleanEt, sink);
        gen.onPeriodicEmit(sink);
        long before = sink.last;

        // 缺 user_id ⇒ schema_invalid（规则④在枚举/时间之前），但 et 是未来的
        long dirtyEt = NOW_MS + 3_600_000L;
        String bad = "{\"event_id\":\"e-bad\",\"event_time\":" + dirtyEt
                + ",\"item_id\":\"i_1\",\"category_id\":\"c_1\","
                + "\"event_type\":\"page_view\"}";
        String reason = SourceTimePolicy.GatedWatermarks.rejectReason(bad, NOW_MS);
        if (!JobConfig.REASON_SCHEMA_INVALID.equals(reason)) {
            fail("案3 B 前置：缺 user_id 的事件未判 schema_invalid，实得 " + reason);
            return;
        }
        gen.onEvent(bad, dirtyEt, sink);
        gen.onPeriodicEmit(sink);
        if (sink.last > before) {
            fail("案3 B：schema_invalid 的未来事件抬了水位（before=" + before
                    + " after=" + sink.last + "）");
        } else {
            System.out.println("[案3 B] 非时间型脏事件不抬水位：PASS");
        }
    }

    /**
     * 未见任何事件时，水位必须是 {@code Long.MIN_VALUE}（与内置实现逐字相同）。
     *
     * <p>这条是**本轮实测补上的**：首版把初值写成 {@code Long.MIN_VALUE + 1}，
     * 减掉 outOfOrderness 后**下溢**成极大的正数 ⇒ 作业冷启第一步水位就跳到未来，
     * 恰好复现 R7-i 的「静默误丢」形态（只是诱因从脏事件换成了初值）。
     * 本条把那次回归钉死在判据里。
     */
    static void checkInitialWatermarkStaysAtLowerBound() {
        WatermarkGenerator<String> gen = SourceTimePolicy.watermarkGenerator(NOW_MS);
        Sink sink = new Sink();
        gen.onPeriodicEmit(sink);
        if (sink.last != Long.MIN_VALUE) {
            fail("案3 初值：未见事件时水位应为 Long.MIN_VALUE，实得 " + sink.last
                    + "（= 下溢到未来 ⇒ 冷启首条真实事件会被误判超期）");
        } else {
            System.out.println("[案3 初值] 未见事件时水位 = Long.MIN_VALUE：PASS");
        }
    }

    /**
     * 生产路径的质量门必须用**会走的墙钟**判「不晚于 now + 裕度」，不能是
     * 提交时的时钟快照。
     *
     * <p>这条是**真机实证补上的**：首版 {@code GateSegment.mainSource} 在提交时取
     * 一次 {@code nowMs} 并冻结，实测作业 04:02 提交、04:29/04:35 两轮探针批
     * <b>全部被判 time_out_of_range 进 DLQ</b>，Doris 只剩 {@code provisional_emitted}
     * 行 —— 与 R7-i 的「静默误丢」同形，只是诱因换成了「时钟停在提交时刻」。
     *
     * <p>判据形态：拿**生产工厂**造生成器，读它两次时钟，必须观察到推进。
     * 冻结快照的实现两次读数相同 ⇒ 红。
     */
    static void checkProductionClockIsLive() throws Exception {
        WatermarkGenerator<String> gen = SourceTimePolicy.watermarkGenerator();
        if (!(gen instanceof SourceTimePolicy.GatedWatermarks)) {
            fail("案3 时钟：生产工厂返回的不是 GatedWatermarks，判据无法求值");
            return;
        }
        SourceTimePolicy.GatedWatermarks gated = (SourceTimePolicy.GatedWatermarks) gen;
        long first = gated.clockNowMs();
        Thread.sleep(15L);
        long second = gated.clockNowMs();
        if (second <= first) {
            fail("案3 时钟：生成器的时钟不走（" + first + " -> " + second
                    + "）⇒ 作业 RUNNING 超过未来裕度后一切真实事件都会被判 time_out_of_range");
        } else {
            System.out.println("[案3 时钟] 生产生成器用活墙钟（+" + (second - first)
                    + " ms 推进）：PASS");
        }
    }

    /** 一条最小合法事件的 JSON（字段齐全、枚举合法）。 */
    static String eventJson(String id, long et) {
        return "{\"event_id\":\"" + id + "\",\"event_time\":" + et
                + ",\"user_id\":\"u_1\",\"item_id\":\"i_1\","
                + "\"category_id\":\"c_1\",\"event_type\":\"page_view\","
                + "\"schema_version\":\"1.0\",\"payload\":\"{}\"}";
    }

    /** 只记最后一次水位。 */
    static final class Sink implements WatermarkOutput {
        long last = Long.MIN_VALUE;

        @Override
        public void emitWatermark(Watermark watermark) {
            last = watermark.getTimestamp();
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }

    private static void fail(String message) {
        failures++;
        System.out.println("✗ " + message);
    }
}
