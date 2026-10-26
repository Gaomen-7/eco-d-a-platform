package com.ecom.flink;

import java.util.ArrayList;
import java.util.List;

/**
 * 事件质量规则 —— {@code src/ecomgen/events.py} 的 Java 对偶。
 *
 * <p><b>为什么两侧必须逐条对应</b>：生成器的产出约束与作业的 DLQ 判据若是两份口径，
 * 就会出现「生成器认为合法、作业认为脏」的错位，脏率断言立刻失去意义。
 * 故这里的三条规则与 Python 侧
 * {@code validate_event()} 一一对应（SRS §3.3 规则 ①②④）。
 *
 * <p><b>DLQ 只认三个原因码</b>（票面 ②，gdd §D D2）：
 * {@code schema_invalid} / {@code value_out_of_range} / {@code time_out_of_range}。
 * <b>重复不在其中</b> —— 重复走独立的 {@code duplicate_absorbed} 通道。
 */
public final class EventQuality {

    private EventQuality() {}

    /** 五类事件枚举（SRS §3.1 冻结）。 */
    static final List<String> EVENT_TYPES =
            List.of("page_view", "view_item", "add_to_cart", "begin_checkout", "purchase");

    /**
     * 规则①上界裕度：不晚于「基准 + 5 分钟」（设计值，非官方保证）。
     *
     * <p>对偶名：Python 侧 {@code events.FUTURE_TOLERANCE_MS}（impl-06d 公开化）。
     * 两处各写一份 5×60_000 字面量，靠本注释互相指认——改任一侧必须同步另一侧
     * （tests/test_replay_sql.py 已把 Python↔SQL 两侧的一致性判据机器化，Java↔Python
     * 侧由 {@code R7iFalsification} 的「未来 2 h 必判脏」前置间接钉住）。
     */
    static final long FUTURE_TOLERANCE_MS = 5 * 60_000L;

    /**
     * 判定一个事件是否为真脏（用墙上时钟判规则①上界）。
     *
     * @return 原因码；{@code null} ⇒ 合法（可继续走去重）
     */
    public static String reason(RawEvent e) {
        return reason(e, System.currentTimeMillis());
    }

    /**
     * 判定一个事件是否为真脏，**显式给上界基准时钟**（R7-i 案 3 修复新增）。
     *
     * <p><b>为什么必须能显式传时钟</b>：判据（{@code R7iFalsification}）需要
     * 可复现，不能依赖墙钟——固定时钟让「未来 2 h 事件必被判脏」这条前置在
     * 任何时刻跑判据都成立。
     *
     * <p>⚠️ 与下游 {@code ValidateAndDedup} 的关系：两处各读一次墙钟，**不是**
     * 同一个 instant（读数必有毫秒级差）。这条差异可接受：上界裕度 5 min 远大于
     * 毫秒级差，同一事件不可能在两处得到相反结论；两处判定分工不同——水印
     * 生成器处判「准不准水印」，下游判「进主流还是 DLQ」。
     *
     * @param nowMs 规则①上界的基准时刻（UTC epoch 毫秒）
     */
    public static String reason(RawEvent e, long nowMs) {
        // ── 规则④ 非空 + 必填（SRS §3.3 公共字段表）──────────────────
        if (isBlank(e.eventId)
                || isBlank(e.userId)
                || isBlank(e.itemId)
                || isBlank(e.categoryId)
                || isBlank(e.eventType)) {
            return JobConfig.REASON_SCHEMA_INVALID;
        }

        // ── 枚举（取值越界，不是结构缺失）──────────────────────────
        if (!EVENT_TYPES.contains(e.eventType)) {
            return JobConfig.REASON_VALUE_OUT_OF_RANGE;
        }

        // ── 五类事件的必填子集（SRS §3.3）────────────────────────────
        if ("add_to_cart".equals(e.eventType) && e.quantity == null) {
            return JobConfig.REASON_SCHEMA_INVALID;
        }
        if ("begin_checkout".equals(e.eventType) && isBlank(e.orderId)) {
            return JobConfig.REASON_SCHEMA_INVALID;
        }
        if ("purchase".equals(e.eventType)) {
            if (isBlank(e.amount) || isBlank(e.currency) || isBlank(e.orderId)) {
                return JobConfig.REASON_SCHEMA_INVALID;
            }
        }

        // ── 规则② amount ≥ 0 ────────────────────────────────────────
        if (!isBlank(e.amount) && !isNonNegative(e.amount)) {
            return JobConfig.REASON_VALUE_OUT_OF_RANGE;
        }

        // ── 规则① 时间范围 ──────────────────────────────────────────
        // 下界是**相对**判据（相对本批最早事件 / 水印），不是绝对日期；
        // 绝对下界会把历史回放全部判脏，与「相对水印而非墙上时钟」的裁决冲突。
        if (e.eventTime <= 0L) {
            return JobConfig.REASON_TIME_OUT_OF_RANGE;
        }
        // 上界：不与未来对齐（生成器的 5 min 裕度与这条同源）
        if (e.eventTime > nowMs + FUTURE_TOLERANCE_MS) {
            return JobConfig.REASON_TIME_OUT_OF_RANGE;
        }

        return null;
    }

    /** 收集全部违规原因（与 Python 侧 `validate_event` 返回列表的形态一致，供日志用）。 */
    public static List<String> reasons(RawEvent e) {
        List<String> out = new ArrayList<>();
        String one = reason(e);
        if (one != null) {
            out.add(one);
        }
        return out;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static boolean isNonNegative(String token) {
        try {
            return Double.parseDouble(token.trim()) >= 0.0;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}
