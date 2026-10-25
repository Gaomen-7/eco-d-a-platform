package com.ecom.flink;

import java.util.HashMap;
import java.util.Map;

/**
 * 原始事件（各段共用的内部形态）与质量规则。
 *
 * <p><b>为什么用中性结构而不是 JSON 解析库</b>：本作业只读十几个扁平字段，
 * 而 Kafka 表的 {@code format='json'} 已在 SQL 段用过；DataStream 段要的是
 * **同一份字段口径**，不是另一套 schema。极简取值器让「生成器认为合法、
 * 作业认为脏」的错位在源码层可见（两侧规则逐条对应 {@code src/ecomgen/events.py}）。
 *
 * <p><b>事件时间 vs 处理时间</b>：一切判据（迟到、窗口、DLQ 的 time_out_of_range）
 * 都用 {@code eventTime}（UTC epoch 毫秒），**不读墙上时钟**——否则
 * 「同 seed 同结果」与「超期只计数」都无法复现（F7 裁决）。
 */
public final class RawEvent {

    public String eventId;
    public long eventTime;
    public String userId;
    public String itemId;
    public String categoryId;
    public String eventType;
    public String orderId;
    public String amount;
    public String currency;
    public Integer quantity;
    public String payload;
    /**
     * 省级行政区（SRS §3.3 `province` 字段）。impl-04 新增：地域分布面板
     * （REQ-F-RT-02/REQ-F-BI-01 的「地域分布」）需要按省聚合，此前未解析。
     * 质量闸门不校验它（缺省落 dim_key=''，与 SQL 段 dim 缺省同语义）。
     */
    public String province;

    /** 原始 JSON 原文（DLQ 落原文用；主流不需要）。 */
    public String rawJson;

    /** 非空 ⇒ 该事件是「重复」，不是脏（通道标记，供下游分开计数）。 */
    public String reason;

    public boolean isDuplicate() {
        return JobConfig.METRIC_DUPLICATE.equals(reason);
    }

    public boolean isDirty() {
        return reason != null && !isDuplicate();
    }

    // ── 构造与解析 ────────────────────────────────────────────────

    public static RawEvent parse(String json) {
        RawEvent e = new RawEvent();
        e.rawJson = json;
        e.eventId = field(json, "event_id");
        e.eventTime = parseEventTime(json);
        e.userId = field(json, "user_id");
        e.itemId = field(json, "item_id");
        e.categoryId = field(json, "category_id");
        e.eventType = field(json, "event_type");
        e.orderId = field(json, "order_id");
        e.amount = field(json, "amount");
        e.currency = field(json, "currency");
        String q = field(json, "quantity");
        e.quantity = q == null ? null : parseIntOrNull(q);
        e.payload = field(json, "payload");
        e.province = field(json, "province");
        return e;
    }

    /** 复制一份并标记为「重复」（不改原对象，避免下游看到被污染的 reason）。 */
    public static RawEvent duplicateOf(RawEvent source) {
        RawEvent e = copyOf(source);
        e.reason = JobConfig.METRIC_DUPLICATE;
        return e;
    }

    public static RawEvent copyOf(RawEvent s) {
        RawEvent e = new RawEvent();
        e.eventId = s.eventId;
        e.eventTime = s.eventTime;
        e.userId = s.userId;
        e.itemId = s.itemId;
        e.categoryId = s.categoryId;
        e.eventType = s.eventType;
        e.orderId = s.orderId;
        e.amount = s.amount;
        e.currency = s.currency;
        e.quantity = s.quantity;
        e.payload = s.payload;
        e.province = s.province;
        e.rawJson = s.rawJson;
        e.reason = s.reason;
        return e;
    }

    /** DLQ 行：原文 + 机读原因码（票面 ②：拒收事件带机读原因码）。 */
    public String toDlqJson() {
        String reasonJson = reason == null ? "" : reason;
        return "{\"reason\":\"" + reasonJson + "\","
                + "\"event_id\":\"" + nullToEmpty(eventId) + "\","
                + "\"event_time\":" + eventTime + ","
                + "\"raw\":" + (rawJson == null ? "null" : quoteRaw(rawJson)) + "}";
    }

    private static String quoteRaw(String raw) {
        // 原文嵌入为 JSON 字符串：只需转义反斜杠与引号，换行由生成器保证不存在
        StringBuilder sb = new StringBuilder(raw.length() + 2);
        sb.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\\' || c == '"') {
                sb.append('\\');
            }
            sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    // ── 极简 JSON 取值（格式由生成器保证；缺字段返回 null，交由校验判脏）──

    static String field(String json, String name) {
        String needle = "\"" + name + "\"";
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
        if (start >= json.length()) {
            return null;
        }
        char c = json.charAt(start);
        if (c == '"') {
            start++;
            int end = start;
            while (end < json.length() && json.charAt(end) != '"') {
                if (json.charAt(end) == '\\') {
                    end++;
                }
                end++;
            }
            return end <= json.length() ? json.substring(start, end) : null;
        }
        // 数字 / true / false / null
        int end = start;
        while (end < json.length() && ",}]".indexOf(json.charAt(end)) < 0) {
            end++;
        }
        String token = json.substring(start, end).trim();
        return token.isEmpty() || "null".equals(token) ? null : token;
    }

    /** 取 event_time（UTC epoch 毫秒）；解析失败返回 0（会被 time_out_of_range 拦下）。 */
    public static long parseEventTime(String json) {
        String token = field(json, "event_time");
        if (token == null) {
            return 0L;
        }
        try {
            return Long.parseLong(token.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static Integer parseIntOrNull(String token) {
        try {
            return Integer.valueOf(token.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 供上报 / 调试用的扁平视图。 */
    public Map<String, String> asMap() {
        Map<String, String> m = new HashMap<>();
        m.put("event_id", eventId);
        m.put("event_time", Long.toString(eventTime));
        m.put("user_id", userId);
        m.put("item_id", itemId);
        m.put("category_id", categoryId);
        m.put("event_type", eventType);
        return m;
    }
}
