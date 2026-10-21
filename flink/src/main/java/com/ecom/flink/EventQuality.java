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

    /** 规则①上界裕度：不晚于「基准 + 5 分钟」（设计值，非官方保证）。 */
    static final long FUTURE_TOLERANCE_MS = 5 * 60_000L;

    /**
     * 判定一个事件是否为真脏。
     *
     * @return 原因码；{@code null} ⇒ 合法（可继续走去重）
     */
    public static String reason(RawEvent e) {
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
        if (e.eventTime > System.currentTimeMillis() + FUTURE_TOLERANCE_MS) {
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
