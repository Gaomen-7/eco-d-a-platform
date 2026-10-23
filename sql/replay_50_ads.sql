-- replay_50_ads.sql — DWS → ads_window_metric（impl-03c）
--
-- 规格落点：spec §5.2（ADS 行 + 「两套物化、一个口径」）、§5.3（`replay_50_ads.sql`）。
--
-- ★★ ADS 表主键**逐列同名同义**于 Doris `ecom_ads.ads_window_metric`：
--     (window_start, metric_name, dim_key)      ← Doris UNIQUE KEY
--     (dt, metric_name, dim_key, window_start)  ← Paimon PRIMARY KEY
--   后者的 `dt` 是**分区列**（窗口起点日），前者的 `window_start` 是键列。
--   `dt` 可由 `window_start` 唯一确定 ⇒ 两边是同一组键的两种写法，
--   「同构」成立；这是 impl-03f 逐值对账的前提。**改这里必先改 03f 的对账面**。
--
-- ★ metric_name 取值：
--     pv  / uv / gmv
--
-- ★★ 三条**推定口径**（票面 Notes 逐字「依据不如其它决策硬，不自洽就停下登记」）：
--   1. **uv 的口径分两层，不是同一个数**：实时侧 `uv` 是 **1 h 滚动去重**
--      （`JobConfig.UV_WINDOW_MS`），离线这里是 **1 分钟窗口去重**。
--      对账（03f）必须先就这一点表态，不能默认「同名即同值」。
--   2. **三行 DWS → 一行 ADS 的合并规则**（★ 实机纠正，见下）：ADS 键里没有
--      `subject_type`，三行 DWS 若各写一次 `dim_key='ALL'` 会同键互相覆盖 = 静默丢数据，
--      故必须**显式**合成一行。但**不能相加**：
--        实测 `SUM(pv)` over 三 subject = 1 409 772，而 DWD 的 page_view 只有 469 924
--        —— 恰好 3.0 倍。原因：item / category / user 三条 subject join 都是
--        **1:1 覆盖**（每条事件恰命中一个商品版本、一个品类版本、一个用户版本），
--        三条各自就已经是**全量**事件集，相加即三倍计数。
--      正确做法：取**单一 subject** 作为全量的 1:1 代表。这里取 `subject_type='user'`
--      ——每条事件恰有一个 user，故 user-subject 行**恰好一一覆盖**事件集，
--      pv / gmv 精确等于 DWD 真值。
--      ⚠️ **三个 subject 的 uv 并不同值**（此处曾误写「本就同值」，实测纠正）：
--        item 行的 uv 是「碰过该商品的用户数」，category 行是「碰过该品类的用户数」，
--        都可能**远小于**该窗口的全量去重用户数。故 uv 必须**单独算**：
--        user-subject 的每一行就是一个用户 ⇒ `COUNT(DISTINCT subject_id)`；
--        实测首版对 `uv` 取 `MAX(uv)` 得到恒 1，4 304 个窗口全部与 DWD 对不上。
--      ⇒ 这就是「DWS 落成 ADS」的正确读法：**取一条 1:1 subject 的正确读法**。
--   3. **dim_key 的全量值写 'ALL'**：Doris 侧「无维度」的约定值是空串 ''，
--      这一处差异只影响**对账怎么配键**，由 03f 处理，不改键语义。
--   ⇒ 本票只保证「键同构 + 同一公式取名」，**逐值相等由 03f 判**。
--
-- ★★ 为什么每个聚合面**只算一遍**（impl-03c 实机教训）：
--   首版把 pv/uv/gmv 写成三个各自 GROUP BY 的 UNION ALL ⇒ 每条支路一次全表
--   shuffle，4 并行下直接撞网络缓冲上限：
--     java.io.IOException: Insufficient number of network buffers: required 512,
--     but only 471 available. (total 5079 × 32768 B)
--   改成「一次聚合算出三列 → 再对**小结果**做 unpivot」后，shuffle 次数从 3 降到 1，
--   且 unpivot 的输入是窗口数（数千行）而不是事件数（百万行）。
--   ⚠️ 这不是「调参绕开报错」：三次全表扫描本身就该合并，报错只是把它逼出来了。
--
-- ★ 本文件不写断言（断言归 impl-03d）。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
-- ★ 并行度降到 2（**本层专用**，不是全局改）：ADS 的输入是 DWS 的窗口级小结果
--   （数千行），4 并行只会把每个算子的 network buffer 配额摊薄到不够用 ——
--   实测 4 并行时报：
--     java.io.IOException: Insufficient number of network buffers: required 512,
--     but only 471 available. (total 5079 × 32768 B)
--   ⇒ 该报错与数据量无关，是「并行度 × 算子链数」把小结果切得太碎。
--   REQ-F-RT-08 的「并行度 ≤4」是**实时作业**的约束（flink JobConfig），
--   batch 层用 2 不违反它，也不影响任何口径。
SET 'parallelism.default' = '2';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ecom_lake;
USE ecom_lake;

-- ── ⓪ 指标名：三条静态行（**广播**维，不是 shuffle）──────────────────────
-- ★ 为什么要有它：本层要把「一次聚合算出的 pv/uv/gmv 三列」摊成三行
--   （ADS 的键里有 metric_name）。首版用「三条各读一遍聚合视图的 `UNION ALL`」
--   ⇒ 三条支路各自 shuffle 一遍，4 并行直接撞网络缓冲上限：
--     java.io.IOException: Insufficient number of network buffers: required 512,
--     but only 471 available. (total 5079 × 32768 B)
--   改成「聚合只做一次 + CROSS JOIN 一张 3 行静态视图」后，指标名的展开是
--   **广播**（3 行 × N 行），不产生任何额外 shuffle 边。
--   ⚠️ 这不是「调参绕开报错」：三次全表扫描本身就该合并成一次，
--      报错只是把它逼出来了；并行度本身仍是 2（见文件头）。
CREATE TEMPORARY VIEW v_metric_name AS
SELECT metric_name FROM (
  SELECT 'pv' AS metric_name
  UNION ALL SELECT 'uv'
  UNION ALL SELECT 'gmv'
);

-- ── ① 全量口径（dim_key='ALL'）：取 user-subject 的 1:1 覆盖 ──────────────
-- ★★ 为什么是 `WHERE subject_type = 'user'` 而**不是**把三 subject 相加：
--   三条 subject 的 join 都是 **1:1 覆盖**（每条事件恰有一个 item 版本、一个
--   category 版本、一个 user 版本）⇒ 每条 subject 流都已是**全量事件集**，
--   相加 = 三倍计数。实机证据（impl-03c，2026-10-23）：
--     SUM(pv) over 三 subject = 1 409 772 ；DWD 的 page_view = 469 924（恰 3.0×）
--     SUM(gmv) over 三 subject = 184 326 357 ；DWD 的 purchase = 61 442 119（恰 3.0×）
--   这正是票面「口径不自洽就停下登记」要拦的形态 —— 它**不会报错**，只会静默三倍。
--   user-subject 与事件**一一对应**（user_id 是事件的必有字段），故它单独就是真值；
--   uv 也因此天然是这个窗口的全量去重用户数。
-- ★ 三个 subject 各自的**分面**语义仍完整保留在 DWS 里（item/category 分面看 DWS）——
--   ADS 的 `dim_key='ALL'` 只是「全站合计」这一张面，不是三张面之和。
-- ★★ uv **不能取** MAX(uv)：DWS 的 uv 是「该 subject 行内的去重用户数」，
--   而 subject_type='user' 的每一行**就是一个用户** ⇒ 该行的 uv 恒等于 1，
--   取 MAX 会得到 1（实机：4304 个窗口全部对不上）。
--   全量去重用户数 = 该窗口的 **user-subject 行数** = `COUNT(DISTINCT subject_id)`。
CREATE TEMPORARY VIEW v_all_metric AS
SELECT
  dt,
  window_start,
  SUM(pv)                       AS pv,
  COUNT(DISTINCT subject_id)    AS uv,
  SUM(gmv)                      AS gmv
FROM dws_subject_window
WHERE subject_type = 'user'
GROUP BY dt, window_start;

-- ── ② 渠道切分（dim_key='channel=<channel>'）────────────────────────────
-- ★ 渠道**不在** DWS 的键里（DWS 键只有 subject_type/subject_id），故渠道切分
--   必须回到 DWD 重新按「窗口 × 渠道」聚合 —— 这不是「绕过 DWS」，而是 ADS 的
--   一类**不同切面**：DWS 键里没有渠道列，硬塞会破坏 §5.2 的形状。
-- ★ 渠道是**事件自带字段**、不是时态维（`dim_channel` 无拉链），故这里没有区间 join。
-- ★ 与 ① 同构：一次聚合算出三列，指标名由 ⓪ 的静态行广播展开。
CREATE TEMPORARY VIEW v_channel_metric AS
SELECT
  (event_time / 60000) * 60000 AS window_start,
  channel,
  -- ★ pv 必须是 **page_view 事件条数**，不是 COUNT(*)（全部事件条数）。
  --   实机：COUNT(*) 让渠道合计 = 1 048 576（= 事件总数），而全量口径 pv = 469 924
  --   （= page_view 数）—— 两个面**不可比**，对账（03f）会当场破。
  --   与 DWS 的 pv 定义逐字同源（replay_40_dws.sql 的 SUM(CASE WHEN page_view)）。
  SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)            AS pv,
  COUNT(DISTINCT user_id)                                               AS uv,
  COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END), 0) AS gmv
FROM dwd_behavior_event
GROUP BY (event_time / 60000) * 60000, channel;

-- ── ③ 两个切面摊成行 ────────────────────────────────────────────────────
-- ★ 先把两个切面并成**一张同列中间结果**（2 条支路，列集相同），再与 ⓪ 的 3 行
--   静态指标名 **CROSS JOIN** 摊成三行 —— 指标名是**广播**的，不产生额外 shuffle。
--   反面写法（首版）：三条各读一遍聚合视图的 `UNION ALL` ⇒ 每条支路独立 shuffle，
--   4 并行直接撞 `Insufficient number of network buffers`。
-- ★ 摊平时用 `CASE metric_name` **挑列**（一次取三列），不是三条各扫一遍。
CREATE TEMPORARY VIEW v_metric_src AS
SELECT
  dt, window_start, 'ALL' AS dim_key, pv, uv, gmv
FROM v_all_metric
UNION ALL
SELECT
  DATE_FORMAT(TO_TIMESTAMP_LTZ(window_start, 3), 'yyyy-MM-dd') AS dt,
  window_start,
  CONCAT('channel=', channel)                                   AS dim_key,
  pv, uv, gmv
FROM v_channel_metric;

CREATE TEMPORARY VIEW v_ads_row AS
SELECT
  dt,
  window_start,
  dim_key,
  metric_name,
  CASE metric_name
    WHEN 'pv'  THEN CAST(pv  AS DECIMAL(18, 2))
    WHEN 'uv'  THEN CAST(uv  AS DECIMAL(18, 2))
    ELSE             CAST(gmv AS DECIMAL(18, 2))
  END AS metric_value
FROM v_metric_src CROSS JOIN v_metric_name;

-- ── ④ 一条 `INSERT OVERWRITE` 写全表 ────────────────────────────────────
-- 🚨 **这一条是实机事故换来的，别拆回两条语句**（impl-03c，2026-10-23）：
--   ADS 有两个切面（`dim_key='ALL'` + 6 个 `channel=…`）写**同一张主键表**。
--   ① 首版：全量面 `INSERT OVERWRITE`、渠道面 `INSERT INTO` ⇒ 换更小跨度重跑时，
--      渠道面残留上批次的行（全量面被清、渠道面没清）。
--   ② 把渠道面也改成 `INSERT OVERWRITE` ⇒ 后一条把前一条刚写的行**整表抹掉**：
--       修复前 rows=90 348 ；两条 OVERWRITE 时 rows=77 436
--       差额 12 912 == 当时 `dim_key='ALL'` 的全部行数
--   ⚠️ 两次都**五层 exit 0、marker 齐全**，只有数字错 —— 「marker 不是断言」的活例子。
--   `INSERT OVERWRITE` 是**整表**语义 ⇒ 一张表必须由**一条**语句写全。
--   守卫：tests/test_replay_temporal_sql.py::
--     test_no_sql_file_writes_a_table_more_than_once /
--     test_ads_writes_both_slices_in_one_insert_statement
INSERT OVERWRITE ads_window_metric
SELECT
  dt,
  metric_name,
  dim_key,
  window_start,
  window_start + 60000                AS window_end,
  metric_value,
  CAST(1 AS TINYINT)                  AS is_settled
FROM v_ads_row;

-- ── ③ 迟到计数（dwd_late_event_count：只存计数，不存明细）───────────────
-- 🚨 **本段有一处未裁的语义缺口（已登记在票面 + replay_01_ddl.sql，勿当已成立引用）**：
--   DDL 说这张表装的是**超期事件**（被 sideOutputLateData 丢掉、压根没进 DWD 的）；
--   下面这条语句装的却是**进了 DWD 的 `is_late=1` 行**。两者不是同一个群体 ——
--   超期事件在 DWD 里没有行，故对它们**零覆盖**。
--   ⇒ 要么把本表语义改称「DWD 内迟到行按档计数」（则 DDL 注释要改），
--     要么补超期通道的写入（需旁路输出，SQL-only 面做不到）。
--   本票按 Notes「不猜」只**写入 + 登记**，裁决归 03d（A3 断言前必须先裁）。
-- ★ 另外：`sum(cnt) == count(*) WHERE is_late=1` 是**同义反复**（cnt 由同一谓词
--   算出），它只能证「分桶没丢」，**不能**证迟到口径正确 —— 别拿它当证据。
-- ★ 60 s 档：`late_bucket = floor(event_time / 60000) × 60000`
--   （票面「dt × 60 s 档」；dt = 事件日，与 DWD 的 dt 同源）。
-- ★ 只存计数：`cnt` 是**该档迟到条数**，不是 0/1 标志；表里没有事件明细。
INSERT OVERWRITE dwd_late_event_count
SELECT
  DATE_FORMAT(TO_TIMESTAMP_LTZ((event_time / 60000) * 60000, 3), 'yyyy-MM-dd') AS dt,
  (event_time / 60000) * 60000                                                 AS late_bucket,
  CAST(COUNT(*) AS BIGINT)                                                     AS cnt
FROM dwd_behavior_event
WHERE is_late = 1
GROUP BY
  DATE_FORMAT(TO_TIMESTAMP_LTZ((event_time / 60000) * 60000, 3), 'yyyy-MM-dd'),
  (event_time / 60000) * 60000;

-- ── 层报告（spec §5.3 可观察性）：layer=<层> rows=… ─────────────────────
-- ★ 层③（两切面）是**一条** `INSERT OVERWRITE` ⇒ 表里只有本批次（ALL 行 + channel 行），
--   既不会残留上批次的渠道行（`INSERT INTO` 的错），也不会被第二切面抹掉
--   （两条 OVERWRITE 的错，见层③ 抬头的事故记录）。本报告按整表计数即可。
-- ⚠️ rows 只是**可观察性**，不是断言：行的**内容**对不对由 impl-03d 的断言判。
SELECT CONCAT('layer=ads rows=', CAST(COUNT(*) AS STRING)) AS layer_report
FROM ads_window_metric;