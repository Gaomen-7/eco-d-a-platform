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

    // ── impl-04 新增：GMV（分类目）与地域 PV（分省）────────────────────
    //
    // ★★ 为什么这两条**不并进 AggregateWindow**而另立 KeyedProcessFunction ★★
    // AggregateWindow 键控 category_id、按 event_type 分桶——它的 revision 语义
    // （窗口内重触发次数）与 verify/reconcile 的对账面（event_* 词表）是冻结契约：
    // verify_runner 的探针对账按「与 AggregateWindow 同源的公式」复算期望，
    // 词表过滤只认 event_ 前缀。把 gmv/region_pv 塞进去＝动 P1 已验收的判据面
    // （impl-02e/03f 都验过），impl-04 无权改。故另立两条侧路：
    //   - 键 = (窗口起点, 维度值)，两阶段计时器（窗口结束发暂定行、
    //     +ALLOWED_LATENESS 发落定行，迟到修正 revision+1 立即覆盖写）；
    //   - 词表（gmv / region_pv）不带 event_ 前缀 ⇒ reconcile 词表过滤天然不判。

    /** GMV：按 (分钟窗口, category) 键控累加 purchase 金额（分），落定发射。 */
    public static DataStream<MetricRow> gmv(DataStream<RawEvent> clean) {
        return clean
                .filter(e -> "purchase".equals(e.eventType))
                .name("gmv-only")
                .uid("gmv-only")
                .keyBy(e -> windowDimKey(e.eventTime, e.categoryId))
                .process(new WindowSumProcess(JobConfig.METRIC_GMV, true))
                .name("gmv-window-sum")
                .uid("gmv-window-sum");
    }

    /** 地域 PV：按 (分钟窗口, province) 键控计数全事件，落定发射。 */
    public static DataStream<MetricRow> regionPv(DataStream<RawEvent> clean) {
        return clean
                .keyBy(e -> windowDimKey(e.eventTime, e.province))
                .process(new WindowSumProcess(JobConfig.METRIC_REGION_PV, false))
                .name("region-pv-window-count")
                .uid("region-pv-window-count");
    }

    /** 复合键字符串：`窗口起点毫秒 + '|' + 维度值`（维度空 ⇒ ''，同 dim 缺省语义）。 */
    static String windowDimKey(long eventTimeMs, String dim) {
        long bucket = (eventTimeMs / JobConfig.WINDOW_SIZE_MS) * JobConfig.WINDOW_SIZE_MS;
        return bucket + "|" + (dim == null ? "" : dim);
    }

    /**
     * 窗口求和器：按「窗口|维度」键控累加，**两阶段发射**（与 event_* 家族三态同构）：
     * <ul>
     *   <li>计时器 A（窗口结束）⇒ 发 is_settled=0 暂定行（值=当前累计和）；</li>
     *   <li>计时器 B（窗口结束+ALLOWED_LATENESS）⇒ 发落定行 is_settled=1，
     *       revision=窗口关闭后到达的迟到修正次数（0=无修正）。</li>
     * </ul>
     * 同键覆盖写由 Doris UNIQUE KEY 吸收 ⇒ 面板看到 暂定→落定（值可能变）→
     * 每次迟到修正 revision+1 且值累加。
     *
     * <h2>★ 为什么必须两阶段（code-review 修复，2026-10-25）</h2>
     * 首版只有单计时器（窗口结束+ALLOWED_LATENESS）+「先到先发、迟到 revision+1
     * 重发」。死锁在阈值重合：settle 计时器与 processElement 的超期旁路用的是
     * **同一个** watermark 阈值（窗口结束+ALLOWED_LATENESS）⇒ 任何在窗口关闭后
     * 到达的迟到事件**先被旁路丢弃**，永远到不了「revision+1 重发」分支——
     * 「已修订」态对 gmv/region_pv 是**死代码**（两位 code-review 评审独立指出）。
     * 两阶段发射消除了阈值重合：窗口关闭（计时器 A）后、ALLOWED_LATENESS
     * 宽限内的迟到既不被旁路（deadline 未到）又能触发计时器 B 的 revision 计数。
     *
     * <h2>★★ 超期语义必须与 AggregateWindow 对齐（impl-04 实机证伪）★★</h2>
     * <p>首版直接挂 {@code clean}：历史重放里「到达序被打乱」的事件在
     * AggregateWindow 一侧被 {@code sideOutputLateData} 判超期旁路（只进
     * {@code ads_late_expired_count}，不进 event_*），而本段没有窗口算子、
     * 没有旁路 ⇒ **同一批事件 gmv/region_pv 计入而 event_* 不计入**。
     * 实机证据：重放历史后 58 个 (窗,品类) 键有 gmv 无 event_purchase，
     * 其中 2026-10-21 15:46:00 窗的超期表恰有 45 行。
     * <p>修法：不在本段重建窗口/旁路（那等于复制 AggregateWindow），而是
     * processElement 里按 {@code eventTime + WINDOW_SIZE + ALLOWED_LATENESS
     * < currentWatermark} 自判超期丢弃（与 sideOutputLateData 同参同语义；
     * 不进超期表——超期计数表只属于 AggregateWindow 那条主流，重复计数会毁
     * verify-fault）。
     *
     * <p>金额解析：amount 形如 "123.45"（生成器 "%.2f"）；分值 = round(×100)。
     * 解析失败按 0 计（质量闸门已拦 amount 非法的 purchase；此处是双保险）。
     */
    public static class WindowSumProcess
            extends KeyedProcessFunction<String, RawEvent, MetricRow> {

        private final String metricName;
        private final boolean byAmount;
        private transient ValueState<Long> sum;
        private transient ValueState<Integer> lateCorrections;
        private transient ValueState<Boolean> provisionalEmitted;

        public WindowSumProcess(String metricName, boolean byAmount) {
            this.metricName = metricName;
            this.byAmount = byAmount;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("wsum-sum", Types.LONG));
            lateCorrections =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("wsum-late", Types.INT));
            provisionalEmitted =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("wsum-prov", Types.BOOLEAN));
        }

        @Override
        public void processElement(RawEvent e, Context ctx, Collector<MetricRow> out)
                throws Exception {
            // ★ 超期旁路（与 sideOutputLateData 同参同语义）：到达时已晚于
            //   「窗口结束 + 允许延迟」⇒ 不计入。见类注释的实机证伪记录。
            long watermark = ctx.timerService().currentWatermark();
            long windowStart =
                    e.eventTime - (e.eventTime % JobConfig.WINDOW_SIZE_MS);
            long deadline =
                    windowStart + JobConfig.WINDOW_SIZE_MS + JobConfig.ALLOWED_LATENESS_MS;
            if (watermark >= deadline) {
                return;
            }
            String[] parts = splitKey(ctx.getCurrentKey());
            long keyedWindowStart = Long.parseLong(parts[0]);
            String dim = parts[1];
            long value = byAmount ? amountToCents(e.amount) : 1L;
            Long cur = sum.value();
            sum.update((cur == null ? 0L : cur) + value);
            boolean fired = provisionalEmitted.value() != null && provisionalEmitted.value();
            if (fired) {
                // 窗口已关（暂定行已发）：这是 ALLOWED_LATENESS 宽限内的迟到修正
                int rev = lateCorrections.value() == null ? 0 : lateCorrections.value();
                lateCorrections.update(rev + 1);
                // 立即重发覆盖行（is_settled=1 + revision+1）：Doris UNIQUE KEY
                // 同键覆盖 ⇒ 面板上「值变化 + revision 递增」同次可见（10 s 刷新内）。
                out.collect(
                        MetricRow.of(
                                keyedWindowStart,
                                metricName,
                                dim,
                                sum.value(),
                                rev + 1,
                                1));
            }
            // 计时器 A（窗口结束 ⇒ 暂定行）：只注册一次
            long settleA = keyedWindowStart + JobConfig.WINDOW_SIZE_MS;
            if (!fired) {
                ctx.timerService().registerEventTimeTimer(settleA);
            }
            // 计时器 B（窗口结束+ALLOWED_LATENESS ⇒ 落定行 + 清态）
            ctx.timerService()
                    .registerEventTimeTimer(
                            keyedWindowStart
                                    + JobConfig.WINDOW_SIZE_MS
                                    + JobConfig.ALLOWED_LATENESS_MS);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricRow> out)
                throws Exception {
            String[] parts = splitKey(ctx.getCurrentKey());
            long windowStart = Long.parseLong(parts[0]);
            String dim = parts[1];
            Long value = sum.value();
            if (value == null) {
                return; // 状态已被清空（不该发生）：不发
            }
            boolean isSettle =
                    timestamp
                            >= windowStart + JobConfig.WINDOW_SIZE_MS + JobConfig.ALLOWED_LATENESS_MS;
            if (isSettle) {
                // 计时器 B：落定行（revision=迟到修正次数）+ 清态（窗口生命周期完结）
                int rev = lateCorrections.value() == null ? 0 : lateCorrections.value();
                out.collect(MetricRow.of(windowStart, metricName, dim, value, rev, 1));
                sum.clear();
                lateCorrections.clear();
                provisionalEmitted.clear();
            } else if (provisionalEmitted.value() == null || !provisionalEmitted.value()) {
                // 计时器 A：暂定行（is_settled=0）
                out.collect(MetricRow.of(windowStart, metricName, dim, value, 0, 0));
                provisionalEmitted.update(true);
            }
        }

        private static String[] splitKey(String key) {
            int idx = key.indexOf('|');
            return new String[] {key.substring(0, idx), key.substring(idx + 1)};
        }

        /** "123.45" → 12345（分）。解析失败返回 0（双保险，闸门已拦非法 amount）。 */
        static long amountToCents(String amount) {
            if (amount == null || amount.isEmpty()) {
                return 0L;
            }
            try {
                return Math.round(Double.parseDouble(amount.trim()) * 100.0);
            } catch (NumberFormatException ex) {
                return 0L;
            }
        }
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
