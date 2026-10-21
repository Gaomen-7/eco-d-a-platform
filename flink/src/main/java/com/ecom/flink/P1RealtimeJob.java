package com.ecom.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * P1 实时作业：Kafka → 质量闸门 → 漏斗/UV/窗口聚合 → Doris。
 *
 * <h2>为什么是「DataStream 为主 + SQL 只做落库」而不是「SQL 段 + DataStream 段」</h2>
 * <p>上一版把聚合放 Flink SQL，修正放 DataStream，结果两者写的<b>键不相交</b>，
 * 「修正」变成了新增口径外的行（详见 {@link WindowSegment} 的类注释）。
 * 而窗口修正又<b>物理上只能</b>在 DataStream 做（官方能力强制，spec §4.6）：
 * <ul>
 *   <li>Flink SQL 无迟到侧输出：FLINK-10031/20527 均 Open，逐字
 *       "The Table API &amp; SQL drop late events right now."
 *   <li>SQL 侧无 allow-lateness：FLINK-24000 逐字
 *       "Window aggregate does not support allow-lateness"
 * </ul>
 * <p>⇒ 既然修正必须是 DataStream，就把<b>聚合也放在同一段</b>（同一个窗口、同一批键），
 * 这样 revision 的语义才同构。SQL 在本作业里只承担 Doris sink 的落库（DDL + INSERT）。
 * 这不是绕开「SQL 侧全 SQL 零 jar」的约束——那条约束针对 P2 的 {@code make replay}，
 * 而本票 P1 的实时链路本来就依赖 Doris connector jar。
 *
 * <h2>单作业保证</h2>
 * <p>全部 sink 收敛到一个 {@code StatementSet}（见 {@link DorisSinkSegment}），
 * 只提交<b>一个</b>作业 ⇒ 一个 checkpoint 边界 ⇒ verify-fault 的两表比对有意义。
 */
public final class P1RealtimeJob {

    private P1RealtimeJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(JobConfig.PARALLELISM);
        env.enableCheckpointing(JobConfig.CHECKPOINT_INTERVAL_MS);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ── ① 入口：解析 → 质量分流（DLQ）→ 去重（重复独立通道）──────
        var rawJson = GateSegment.mainSource(env, "");
        var clean = GateSegment.build(env, rawJson);

        // ── ② 漏斗（会话步进，双列并陈）────────────────────────────
        var funnel = FunnelSegment.build(clean);

        // ── ③ 窗口聚合 + 迟到修正（同一段、同一批键）────────────────
        var windowed = WindowSegment.build(clean);

        // ── ③b 暂定态（is_settled=0）：窗口关闭前周期性发射，证「暂定→落定」────
        var provisional = WindowSegment.provisional(clean);

        // ── ④ 超期只计数（绝不修正）────────────────────────────────
        // ★ impl-02a 取证结论（2026-10-21）：此前的「195 行已到 Writer、sink 异步
        //   吞错、Timestamp 精度」是**误读**——顶点级 numRecordsOut=195 是窗口
        //   主输出（落定行去 view_windowed），不是超期行；且旧 topic 在修正版
        //   水位语义下根本没有会被判超期的行（逐分区重放仅 2 条，keyBy 合并水印
        //   更保守 ⇒ 实机 0 条）。挂临时探针喂 seed2/seed3 各一轮后实测：
        //   侧输出→count-expired→view_late→Calc→Writer→flush→Doris 全链路通，
        //   探针 44 行 == Doris 44 行。断点不存在；超期表恒空是数据侧（topic 无
        //   运行时超期事件）+ 计数口径误读共同造成的。
        var late = WindowSegment.expired(windowed)
                .map(WindowSegment.LateCount::of)
                .returns(WindowSegment.LateCount.class)
                .name("count-expired")
                .uid("count-expired");
        // ── ⑤ 侧路指标：UV 精确去重 / 重复计数 ─────────────────────
        var uv = MetricSegment.uv(clean);
        var duplicates = MetricSegment.duplicates(GateSegment.duplicates(clean));

        // ── ⑥ 合并进窗口指标流（同表同键，三态同行）──────────────────
        var metrics = windowed.union(uv, duplicates, provisional);

        // ── ⑦ 落 Doris：全部 sink 一个 StatementSet，只提交一个作业 ──
        DorisSinkSegment.registerSinks(tEnv);
        DorisSinkSegment.attach(tEnv, metrics, funnel, late);
    }
}
