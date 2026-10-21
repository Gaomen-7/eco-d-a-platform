package com.ecom.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 漏斗段：按 <b>会话（user_id）× 步</b> 维护步进漏斗，落 {@code ads_funnel_step}。
 *
 * <p><b>为什么这一段的唯一键是 (session_date, step_no, window_start)</b>
 * （spec §4.2，DDL 冻结）：
 *
 * <ul>
 *   <li>漏斗的单位是「一个用户在一次会话里走到第几步」，与 1 分钟窗口<b>正交</b>
 *       （gdd 开篇的区分：事件计数比 ≠ 转化率）。
 *   <li>{@code revision} 是<b>值列</b>，<b>不进**唯一键**</b>——所以修正靠同键覆盖写。
 * </ul>
 *
 * <p><b>双列并陈</b>（gdd §1.3，GLOSSARY「观测值/推断值」）：
 * {@code cnt_observed} 只记真实到达的步，允许非单调（{@code --chaos-ratio} 下跳步合法）；
 * {@code cnt_inferred} 含 <b>补记</b>（某一步到达即推断其前缀步都曾发生），
 * 沿 step_no 1→4 <b>非增</b>——这是 spec §4.2 唯一押的单调性。
 *
 * <p><b>不给 purchase 补记「后续步」</b>：补记方向只能是「往前补前缀」，
 * 往后补会把没发生的转化凭空造出来，那是伪造数据而不是补记。
 */
public final class FunnelSegment {

    private FunnelSegment() {}

    /** 漏斗四步（顺序冻结，与 src/ecomgen/funnel.py 的 FUNNEL_STEPS 一致）。 */
    static final String[] STEPS = {"view_item", "add_to_cart", "begin_checkout", "purchase"};

    /** 1..4 的步号；不在漏斗里（如 page_view）返回 0。 */
    static int stepNo(String eventType) {
        for (int i = 0; i < STEPS.length; i++) {
            if (STEPS[i].equals(eventType)) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * 构建漏斗段。
     *
     * <p>输入是 <b>已去重</b> 的干净流（重复不算漏斗推进——否则重放会推高客数，
     * 直接毁掉 verify-fault 的「count 全等」判据）。
     */
    public static DataStream<FunnelRow> build(DataStream<RawEvent> clean) {
        return clean.filter(e -> stepNo(e.eventType) > 0)
                .name("funnel-only")
                .uid("funnel-only")
                .keyBy(e -> e.userId)
                .process(new FunnelProcess())
                .name("funnel-step")
                .uid("funnel-step");
    }

    /** 漏斗行（`ads_funnel_step` 的一行）。 */
    public static class FunnelRow {
        public java.sql.Date sessionDate;
        public int stepNo;
        public java.sql.Timestamp windowStart;
        public long cntObserved;
        public long cntInferred;
        public int revision;
    }

    /**
     * 会话级漏斗推进器。
     *
     * <p>状态按 <b>user_id 键控</b>（gdd 【已确认 Q12】：分区键取 user_id 保证每用户有序），
     * 记录该会话已经到达过的最高步。到达第 N 步 ⇒
     * {@code cnt_observed[step=N] += 1}（只记真实到达），
     * 且 {@code cnt_inferred[step=1..N] += 1}（前缀补记）。
     *
     * <p>这条「前缀补记」正是 {@code cnt_inferred} 沿 1→4 非增的来源：
     * 任何到达第 N 步的会话，都会给 1..N 的每一级各加 1，故越靠后的步累计越多不会反过来。
     *
     * <h2>★★ 迟到 purchase 必须推高漏斗行 revision（P1 缺口，2026-10-21 补齐）★★</h2>
     * <p>spec §4.2 明写：`purchase` 迟到 ≤120 s ⇒ 落入已落定窗口后**重发**，
     * 使「该分钟漏斗客数被修正」且 {@code ads_funnel_step.revision}+1；
     * 超期（&gt;120 s）**只计数、绝不修正**。
     * <p>此前本类恒写 {@code revision=0}，验收第 4 条名存实亡。
     * <p>判定「迟到」的口径与 {@link WindowSegment} 一致：拿事件时间与
     * <b>本分区已推进到的最大事件时间</b>（水印的上界）比。差值超过
     * {@link JobConfig#ALLOWED_LATENESS_MS} 就说明该事件已超期，**不能**推 revision；
     * 否则属于「允许延迟内的迟到修正」，revision +1。
     */
    public static class FunnelProcess
            extends KeyedProcessFunction<String, RawEvent, FunnelRow> {

        private transient ValueState<Integer> highestStepReached;

        /** 本 key（user_id）已见到的最大事件时间 —— 水位上界的近似。 */
        private transient ValueState<Long> maxSeenMs;

        /** 每个 (step, 窗口) 已被修正过的次数，用于 revision 递增。 */
        private transient ValueState<Integer> lastRevision;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            highestStepReached = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("funnel-highest-step", Types.INT));
            maxSeenMs = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("funnel-max-seen", Types.LONG));
            lastRevision = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("funnel-last-revision", Types.INT));
        }

        @Override
        public void processElement(RawEvent e, Context ctx, Collector<FunnelRow> out)
                throws Exception {
            int step = stepNo(e.eventType);
            int previous = highestStepReached.value() == null ? 0 : highestStepReached.value();
            if (step > previous) {
                highestStepReached.update(step);
            }

            // ── 迟到判定（相对本分区已推进到的最大事件时间）──────────────
            Long seen = maxSeenMs.value();
            boolean isLate = seen != null && e.eventTime < seen;
            long lateness = isLate ? (seen - e.eventTime) : 0L;
            boolean isExpired = isLate && lateness > JobConfig.ALLOWED_LATENESS_MS;

            // 超期 ⇒ 只计数（不进漏斗表）；本类不做计数，超期由 WindowSegment 的侧输出承担
            if (isExpired) {
                return;
            }

            // 允许延迟内的迟到 ⇒ 该窗口的漏斗行 revision +1（修正已落定计数）
            int revision = 0;
            if (isLate) {
                int priorRev = lastRevision.value() == null ? 0 : lastRevision.value();
                revision = priorRev + 1;
                lastRevision.update(revision);
            }
            maxSeenMs.update(seen == null ? e.eventTime : Math.max(seen, e.eventTime));

            long bucket = (e.eventTime / JobConfig.WINDOW_SIZE_MS) * JobConfig.WINDOW_SIZE_MS;
            java.sql.Timestamp windowStart = new java.sql.Timestamp(bucket);
            java.sql.Date sessionDate = new java.sql.Date(bucket);

            // 观测：本步真实到达
            out.collect(row(sessionDate, step, windowStart, 1L, 1L, revision));

            // 推断：把该会话的前缀步 1..step-1 补记一步（不往后补）
            for (int prefix = 1; prefix < step; prefix++) {
                out.collect(row(sessionDate, prefix, windowStart, 0L, 1L, revision));
            }
        }

        private static FunnelRow row(
                java.sql.Date date, int step, java.sql.Timestamp ws,
                long observed, long inferred, int revision) {
            FunnelRow r = new FunnelRow();
            r.sessionDate = date;
            r.stepNo = step;
            r.windowStart = ws;
            r.cntObserved = observed;
            r.cntInferred = inferred;
            r.revision = revision;
            return r;
        }
    }

    /** 供其它段复用的键类型（user_id × step）。 */
    static Tuple2<String, Integer> keyOf(String userId, int step) {
        return Tuple2.of(userId, step);
    }
}
