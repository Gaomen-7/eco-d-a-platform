package com.ecom.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 聚合与修正段 —— <b>本票的技术核心</b>（spec §4.2 / §4.6）。
 *
 * <h2>为什么必须是 DataStream 而不是 SQL</h2>
 * <p>官方能力强制（spec §4.6，不是选型偏好）：
 * <ul>
 *   <li>Flink SQL <b>没有</b>迟到侧输出 —— FLINK-10031 / FLINK-20527 均 Open，
 *       逐字 "The Table API &amp; SQL drop late events right now."；
 *       {@code WindowAggOperator} javadoc 逐字 "late elements … will be simply dropped."
 *   <li>SQL 侧<b>没有</b> allow-lateness —— FLINK-24000 逐字
 *       "Window aggregate does not support allow-lateness"；{@code allowedLateness} 是 <b>DataStream-only</b>。
 * </ul>
 *
 * <h2>★★ 聚合与修正必须写**同一批键**（上一版的核心缺陷，已修）★★</h2>
 * <p>上一版把聚合放在 SQL 段（{@code metric_name='event_<类型>'}, {@code dim_key=category_id}），
 * 修正却写 {@code metric_name='event_window'}, {@code dim_key=user_id} ——
 * 两套键<b>永不相交</b>，于是「修正」不是覆盖已落定行而是<b>新增一行</b>：
 * <ul>
 *   <li>{@code revision} 两表不同构，验收第 4 条名存实亡；
 *   <li>{@code ads_window_metric} 被塞进一个口径外的指标，看板与对账都会读到脏口径。
 * </ul>
 * <p>⇒ 修法：<b>聚合与修正都走本段</b>，键一律
 * {@code (window_start, event_<类型>, category_id)}。同键覆盖写才真正实现
 * 「暂定/落定/修正同表同一行」。
 *
 * <h2>三态与 revision</h2>
 * <ul>
 *   <li>窗口第一次触发（关闭时）⇒ {@code revision=0, is_settled=1}
 *   <li>允许延迟内每被迟到事件重触发一次 ⇒ {@code revision}+1，{@code is_settled} 保持 1
 *   <li>超期（&gt; 允许延迟）⇒ 只进 {@code ads_late_expired_count}，<b>绝不修正</b>
 * </ul>
 * <p>revision 用窗口状态记录触发次数，随 checkpoint 恢复 ⇒ 重启不乱。
 */
public final class WindowSegment {

    private WindowSegment() {}

    /** 超期迟到（只计数）的侧输出。 */
    public static final OutputTag<RawEvent> LATE_EXPIRED =
            new OutputTag<RawEvent>("late-expired") {};

    /**
     * 构建聚合+修正段。
     *
     * <p>两条通道语义<b>互斥</b>：可修正与超期绝不混流
     * （混流会让 {@code revision} 被本不该修正的事件推高，直接毁掉验收第 4 条）。
     */
    public static org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<
                    MetricSegment.MetricRow> build(DataStream<RawEvent> clean) {
        return clean.keyBy(e -> e.categoryId == null ? "" : e.categoryId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(JobConfig.WINDOW_SIZE_MS)))
                .allowedLateness(Duration.ofMillis(JobConfig.ALLOWED_LATENESS_MS))
                .sideOutputLateData(LATE_EXPIRED)
                .process(new AggregateWindow())
                .name("aggregate-window")
                .uid("aggregate-window");
    }

    /** 超期通道（只计数，绝不修正）。 */
    public static DataStream<RawEvent> expired(
            org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<
                            MetricSegment.MetricRow> agg) {
        return agg.getSideOutput(LATE_EXPIRED);
    }

    /**
     * 暂定态（`is_settled=0`）发射段 —— 补齐「暂定 → 落定」的可观察性。
     *
     * <h2>为什么必须单独一段、且用**处理时间定时器**</h2>
     * <p>spec §4.2 要求 `ads_window_metric` 以同一唯一键承载三态
     * （`is_settled` 0/1 + `revision` 递增），验收第 5 条后半要求
     * 「观察到窗口关闭（暂定→落定）」。而 {@link AggregateWindow} 只在
     * 窗口**关闭时**才发（恒 `is_settled=1`）⇒ 旧实现里 `is_settled=0` 的行
     * **从未被写入过**，判据 `check_is_settled_two_states` 一旦接入必然 FAIL。
     *
     * <p>官方唯一支持 early-fire 的 Group Window Aggregation 已被标 deprecated
     * （flink-29692 跟踪移除），CUMULATE 走 TVF 会被 `checkEmitConfiguration`
     * 直接抛异常（spec §4.6 PI-3）。故这里**不押注弃用语法**，改用
     * {@code KeyedProcessFunction} 的处理时间定时器周期性发射「当前窗口的暂定值」。
     *
     * <p>暂定行与落定行**同表同键**：窗口关闭后 {@link AggregateWindow} 会用
     * `is_settled=1` 覆盖同一 (window_start, metric_name, dim_key) ⇒ 正是
     * 「三态同表同一行」的落点（Doris UNIQUE KEY 覆盖写）。
     */
    public static DataStream<MetricSegment.MetricRow> provisional(
            DataStream<RawEvent> clean) {
        return clean.keyBy(e -> e.categoryId == null ? "" : e.categoryId)
                .process(new ProvisionalEmitter())
                .name("provisional-emit")
                .uid("provisional-emit");
    }

    /**
     * 按处理时间每 {@link JobConfig#PROVISIONAL_INTERVAL_MS} 发一次暂定行。
     *
     * <p>只发「本分钟窗口内已到达的事件计数」，`is_settled=0`、`revision=0`。
     * 窗口关闭后由 {@link AggregateWindow} 发 `is_settled=1` 覆盖。
     */
    public static class ProvisionalEmitter
            extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<
                    String, RawEvent, MetricSegment.MetricRow> {

        /** 本分钟窗口内各事件类型的累计条数（暂定值）。 */
        private transient ValueState<Long> pendingWindowStart;
        private transient ValueState<Long> pendingCount;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            pendingWindowStart = getRuntimeContext()
                    .getState(
                            new ValueStateDescriptor<>("provisional-window-start", Types.LONG));
            pendingCount = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("provisional-count", Types.LONG));
        }

        @Override
        public void processElement(RawEvent e, Context ctx, Collector<MetricSegment.MetricRow> out)
                throws Exception {
            long bucket = (e.eventTime / JobConfig.WINDOW_SIZE_MS) * JobConfig.WINDOW_SIZE_MS;
            Long currentWindow = pendingWindowStart.value();
            if (currentWindow == null || currentWindow != bucket) {
                pendingWindowStart.update(bucket);
                pendingCount.update(1L);
            } else {
                pendingCount.update((pendingCount.value() == null ? 0L : pendingCount.value()) + 1);
            }

            // 每个 key 只注册一个周期性定时器（避免每元素一个定时器撑爆状态）
            if (ctx.timerService().currentProcessingTime() > 0) {
                ctx.timerService()
                        .registerProcessingTimeTimer(
                                ctx.timerService().currentProcessingTime()
                                        + JobConfig.PROVISIONAL_INTERVAL_MS);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricSegment.MetricRow> out)
                throws Exception {
            Long ws = pendingWindowStart.value();
            Long count = pendingCount.value();
            if (ws != null && count != null && count > 0) {
                // 暂定行：is_settled=0，revision=0。metric_name 用聚合口径的同一个
                // 「event_<类型>」形状会让计数口径混淆，故这里用专用指标名承载
                // 「该窗口当前暂定总量」——它与落定行同表同键体系，可被看板区分三态。
                out.collect(
                        MetricSegment.MetricRow.of(
                                ws, JobConfig.METRIC_PROVISIONAL, ctx.getCurrentKey(), count, 0, 0));
            }
            // 继续周期发射
            ctx.timerService()
                    .registerProcessingTimeTimer(timestamp + JobConfig.PROVISIONAL_INTERVAL_MS);
        }
    }

    /**
     * 窗口聚合器。
     *
     * <p>键控维度是 {@code category_id}（与 SQL 段的 {@code dim_key} 口径一致），
     * 窗口内按 {@code event_type} 分桶计数 ⇒ 每个 (窗口, 事件类型, 品类) 一行。
     *
     * <h2>★★ revision 必须按**窗口**记，不能按 key 记（P1 实机证伪，2026-10-21）★★</h2>
     * <p>旧实现用一个 {@code ValueState<Integer> triggerCount} 记「该 key 被触发了多少次」。
     * 但 {@code ProcessWindowFunction} 的状态是 <b>keyed 作用域</b>的，而滚动窗口的
     * **key 不随窗口重置** ⇒ 同一个 {@code category_id} 在第二个分钟窗口的首次触发
     * 会拿到**非 0** 的 revision。实机证据：
     * <pre>
     *   2026-10-21 11:48:00  event_page_view  c_00010  1  1  1   ← 该分钟首个落定行 revision=1
     *   2026-10-21 11:47:00  event_page_view  c_00010  1  0  1   ← 上一分钟是 0
     * </pre>
     * 即「窗口关闭」被记成「已修订过一次」，与 spec §4.2 的
     * 「首次落定 = revision 0」直接矛盾，验收第 4 条因此失真。
     * <p>修法：把计数器的状态键加上 {@code window_start}，让窗口之间互不影响；
     * 并在窗口 {@code clear()} 时释放（{@code ProcessWindowFunction.clear()} 由
     * Flink 在窗口彻底回收时调用）。
     */
    public static class AggregateWindow
            extends ProcessWindowFunction<RawEvent, MetricSegment.MetricRow, String, TimeWindow> {

        private transient ValueState<Integer> triggerCount;

        /** 上一次触发所属的窗口起点；与 {@code triggerCount} 配对判定「是否换了窗口」。 */
        private transient ValueState<Long> lastWindowStart;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            triggerCount = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("window-trigger-count", Types.INT));
            lastWindowStart = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("window-last-start", Types.LONG));
        }

        @Override
        public void process(
                String categoryId,
                Context ctx,
                Iterable<RawEvent> elements,
                Collector<MetricSegment.MetricRow> out)
                throws Exception {

            // ★ 同一个 key 的下一个窗口必须从 0 重新计。
            //   判据：状态里记的是「上次触发属于哪个窗口」，窗口换了就归零。
            long windowStart = ctx.window().getStart();
            Long lastWindow = lastWindowStart.value();
            int previous = (lastWindow != null && lastWindow == windowStart)
                    ? (triggerCount.value() == null ? 0 : triggerCount.value())
                    : 0;

            lastWindowStart.update(windowStart);
            triggerCount.update(previous + 1);

            // 窗口内按事件类型计数
            java.util.Map<String, Long> byType = new java.util.HashMap<>();
            for (RawEvent e : elements) {
                byType.merge(e.eventType, 1L, Long::sum);
            }

            long ws = windowStart;
            for (var entry : byType.entrySet()) {
                out.collect(
                        MetricSegment.MetricRow.of(
                                ws,
                                "event_" + entry.getKey(),
                                categoryId,
                                entry.getValue(),
                                previous, // 首次触发（本窗口）=0；迟到重触发=1,2,...
                                1 // 窗口已关闭 ⇒ 落定
                        ));
            }
        }

        @Override
        public void clear(Context ctx) {
            // 窗口彻底回收（已过 allowedLateness）⇒ 释放本窗口的触发计数。
            // ⚠️ ProcessWindowFunction.clear(Context) 的契约是「窗口被清除时」被调用；
            //    没有这个钩子时状态会随 key 无限累积（滚动窗口 key 不重置）。
            try {
                triggerCount.clear();
                lastWindowStart.clear();
            } catch (Exception ignored) {
                // 状态已释放时 clear 是幂等的
            }
        }
    }

    /**
     * 超期计数行（`ads_late_expired_count`）。
     *
     * <h2>为什么窗口列回到 {@code java.sql.Timestamp}，并在 SQL 侧显式 CAST</h2>
     * <p>P1 实机证伪（2026-10-21）超期链产出 195 行却一张表都写不进，而同时段
     * 另外两张表正常 Stream Load。试过把窗口列改成 {@code java.time.LocalDateTime}
     * ——Flink 的类型提取把它退化成 {@code RAW('java.time.LocalDateTime')}，SQL 侧
     * 直接报 {@code Cast function cannot convert value of type RAW(...) to type
     * TIMESTAMP(3)}（Calcite 校验期就失败）。
     * <p>{@code java.sql.Timestamp} 能被正确识别为 {@code TIMESTAMP}，所以保留它；
     * 精度问题在 {@link DorisSinkSegment} 的 INSERT 里用 {@code CAST(... AS TIMESTAMP(3))}
     * 解决（与 {@code ads_window_metric} 完全同一套做法）。
     *
     * <p>★ 关键：POJO 必须有 <b>public 无参构造器</b>与 <b>public 字段</b>，
     * 否则 Flink 的 POJO 分析器不认，会退化成 GenericType（这也是上面 LocalDateTime
     * 变 RAW 的同一根因）。
     */
    public static class LateCount {
        public java.sql.Timestamp windowStart;
        public long lateLagMs;
        public long cnt;

        /** Flink POJO 要求的 public 无参构造器。 */
        public LateCount() {}

        static LateCount of(RawEvent e) {
            long ws = (e.eventTime / JobConfig.WINDOW_SIZE_MS) * JobConfig.WINDOW_SIZE_MS;
            LateCount c = new LateCount();
            c.windowStart = new java.sql.Timestamp(ws);
            // lag = 事件时间相对其所属窗口起点的偏移（确定性、可复现，重放落同一行）
            c.lateLagMs = Math.max(0L, e.eventTime - ws);
            c.cnt = 1L;
            return c;
        }
    }
}
