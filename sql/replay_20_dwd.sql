-- replay_20_dwd.sql — ods → dwd_behavior_event + dwd_behavior_event_reject（impl-03b）
--
-- 规格落点：spec §5.3（replay_20_dwd）、§5.2（两表形状）、§5.4 A1/A3。
--
-- ★★ 本文件相对首版（2026-10-22 上午「已写但从未执行」，§E3）四处**实测**修正：
--
-- ① 【字段位置错，最严重】amount/currency/quantity/order_id 在 dump 行的**顶层**，
--    不在 payload 里。实测（30 000 行抽样）：payload 的键只有 session_id（100%）；
--    四字段在顶层出现量 889/889/6 864/2 534。
--    首版从 payload 取 ⇒ 恒 NULL ⇒ 29 979 条 purchase 全被判 schema_invalid。
--    ⇒ 已改 replay_10_ods.sql 让 payload 存**整行原文**，本文件从整行原文取四字段。
--
-- ② 【函数签名错】UNIX_TIMESTAMP(TIMESTAMP_LTZ) **不存在**。实测逐字：
--      SqlValidatorException: Cannot apply 'UNIX_TIMESTAMP' to arguments of type
--      'UNIX_TIMESTAMP(<TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)>)'
--      Supported form(s): 'UNIX_TIMESTAMP()' / '(<CHARACTER>)' / '(<CHARACTER>,<CHARACTER>)'
--    ⇒ 摄入时刻用无参 UNIX_TIMESTAMP() * 1000。
--
-- ③ 【日期口径】dt 必须走 TO_TIMESTAMP_LTZ(ms, 3) + table.local-time-zone=UTC，
--    不能用 FROM_UNIXTIME（它收秒、且按会话时区渲染）。实测两者在本容器（TZ 空）恰好
--    同值，但「碰巧同值」不是判据，故钉死 UTC 路径。
--
-- ④ 【is_late 语义错，会让 A3 恒真】首版拿「批次最大 event_time」当水印：
--    那样 lag = max_ms − event_time 对**几乎每一行**都 > 0 ⇒ is_late 全表为 1
--    ⇒ A3「is_late=1 行数 == 迟到计数和」退化成恒真断言（本仓 impl-02e 记过的病）。
--    正确语义 = **到达序**（Flink 水印只由到达序推进）：
--      水印 = 到此行为止**已见过的最大 event_time**（running max，按回放序）
--      迟到 ⟺ event_time < 该水印
--    ⚠️ 需要真实行序，而 batch 下 ROW_NUMBER() OVER () **不可用**。实测逐字：
--      ValidationException: Over Agg: The window rank function requires order by
--      clause with non-constant fields
--    ⇒ 行序取 ODS 的 src_seq（= dump 的 _src_offset = 投递序 = 回放序）。
--    ⚠️ 口径边界（**设计值**，非官方保证）：dump 单文件顺序回放 ⟹ 单分区到达序；
--      Kafka 多分区时各分区各自推进水印，本文件的「全局回放序」是**单分区等价物**。
--
-- ⑤ 类型容错用 TRY_CAST：一处脏值不该让整批作业崩，脏值应进拒绝表。
--
-- ⑥ 【与 validate_event 的口径对齐（spec 轴审阅指出后修，2026-10-23）】
--    TRY_CAST 的「静默变 NULL」会**掩盖** Python 侧判 value_out_of_range 的那一支：
--      · amount = "abc"（字段存在但不可转数字）
--          Python: 必填子集通过（'abc' 不为 None/''）→ float('abc') 抛 ValueError
--                  → **value_out_of_range**
--          SQL 旧版: TRY_CAST → NULL → 被「purchase 缺 amount」抢走 → schema_invalid ❌
--          现版: 先判 `amount_raw IS NOT NULL AND amount IS NULL` → value_out_of_range ✅
--          该分支必须排在「purchase 缺 amount」**之前**，否则仍会被抢走。
--      · event_time 是「存在的非整数」（如字符串 "abc"）
--          Python: `isinstance(event_time, int)` 为 False ⇒ **跳过时间规则**，不报违规
--          SQL:   TO_TIMESTAMP_LTZ / BIGINT 转换得 NULL ⇒ 目前判 schema_invalid
--          ⚠️ **这一处仍不同源，是有意从严**：SQL 侧无法区分「字段缺失」与
--             「字段类型错」，而后者的时间值不可信、不能进入 dt 分区。
--             门槛批与脏批探针都**不含**该形态，故不影响本票任何判据；
--             已在票面 §A·13 的同类口径收窄处登记，交 03d 决定是否补。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
-- ★ UTC 是 dt 口径的前提（spec §5.2）。
SET 'table.local-time-zone' = 'UTC';
-- ★ 必须 true：否则 INSERT 异步提交，紧随的行数查询读到 0（§D5 实测）。
SET 'table.dml-sync' = 'true';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ecom_lake;
USE ecom_lake;

-- ── 批次基准量（单行）──────────────────────────────────────────────────
-- ★ batch_min_ms = 规则①**下界基准**（SRS §3.3 逐字：「不早于本批次最早事件时间 −
--   允许乱序幅度」）。首版错用「MAX − 120000」，会把绝大多数正常行判成超期。
-- ★ now_ms = 规则①**上界基准**（不晚于 now + 5 min）。两处都是**区间**判据。
-- ★★ 只读**本次落地分区**（impl-03b spec 轴审阅实测证伪，2026-10-23）：
--   ODS 的 dt 是**落地日**，append 表的 INSERT OVERWRITE 只改本次 dt 分区，
--   历史落地日的分区按设计保留。若这里读**整表**，隔天重跑就会把前一天的
--   1 048 576 行再喂一遍 ⇒ DWD 变 2 097 152 行、`layer=dwd rows=` 漂移，
--   而 spec §5.4 的 A1 是**全局比对**（ods_distinct = dwd_distinct + reject_distinct），
--   一旦 DWD 混进上一天的副本，这个等式当场破。
--   ⇒ 本视图与下面的 `o` 都加同一个 dt 过滤，两个读取点必须同源（改一处必改另处）。
CREATE TEMPORARY VIEW v_ods_scope AS
SELECT * FROM ods_behavior_event
WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');

CREATE TEMPORARY VIEW v_batch_ref AS
SELECT
  MIN(event_time)         AS batch_min_ms,
  UNIX_TIMESTAMP() * 1000 AS now_ms
FROM v_ods_scope;

-- ── 校验视图：每条 ODS 行 + 该行的迟到标记 + 它该被判成什么 ──────────────
-- ★ 校验口径与 src/ecomgen/events.py:validate_event **逐条同源**（该文件头注要求
--   两侧共用一份口径，避免「生成器认为合法、作业认为脏」）。逐条映射：
--     schema_invalid     ← 公共必填缺失/空白、event_time 缺失、五类事件必填子集缺失
--     value_out_of_range ← event_type ∉ 五枚举、amount < 0
--     time_out_of_range  ← event_time < 批次最早 − 120 s（相对下界），或 > now + 5 min
-- ★ 判定顺序同源：**脏优先于迟到**（gdd §1.9）；结构 → 取值 → 时间（SRS §3.3 ①②）。
CREATE TEMPORARY VIEW v_checked AS
SELECT
  c.event_id, c.event_time, c.user_id, c.item_id, c.category_id,
  c.event_type, c.schema_version, c.page, c.source, c.channel, c.province,
  c.payload, c.ods_dt, c.src_seq, c.event_dt, c.batch_min_ms,
  c.amount, c.currency, c.quantity, c.order_id,
  -- 迟到量：0 表示不迟到；> 0 表示相对到达序是迟到的
  CASE WHEN c.arrival_wm_ms > c.event_time
       THEN c.arrival_wm_ms - c.event_time ELSE 0 END AS late_lag_ms,
  CASE WHEN c.arrival_wm_ms > c.event_time THEN 1 ELSE 0 END AS is_late,
  CASE
    -- ── 结构：公共必填（含 event_time）─────────────────────────────
    WHEN c.event_id IS NULL OR TRIM(c.event_id) = ''
      OR c.user_id IS NULL OR TRIM(c.user_id) = ''
      OR c.item_id IS NULL OR TRIM(c.item_id) = ''
      OR c.category_id IS NULL OR TRIM(c.category_id) = ''
      OR c.event_type IS NULL OR TRIM(c.event_type) = ''
      OR c.schema_version IS NULL OR TRIM(c.schema_version) = ''
      OR c.payload IS NULL OR TRIM(c.payload) = ''
      OR c.event_time IS NULL
      THEN 'schema_invalid'
    -- ── 取值：枚举之外 ⇒ 取值越界（不是结构缺失，与 validate_event 同源）
    WHEN c.event_type NOT IN ('page_view','view_item','add_to_cart','begin_checkout','purchase')
      THEN 'value_out_of_range'
    -- ── 取值：amount 存在但**不可转成数字** ⇒ 取值越界（不是结构缺失）
    --   ★ 必须排在下面「purchase 缺 amount」之前，否则会被那条抢走判成 schema_invalid。
    --   与 events.validate_event 逐字同源：该函数先查必填子集（`event.get(f) in (None,'')`，
    --   字符串 'abc' **不算缺失** ⇒ 通过），再 `float(amount)` 失败 ⇒ value_out_of_range。
    --   SQL 侧 TRY_CAST 会把它静默变成 NULL，若不显式挡就会被上面的必填子集误判。
    WHEN c.amount_raw IS NOT NULL AND c.amount IS NULL THEN 'value_out_of_range'
    -- ── 结构：五类事件的必填子集（SRS §3.3）
    WHEN c.event_type = 'add_to_cart'    AND c.quantity IS NULL THEN 'schema_invalid'
    WHEN c.event_type = 'begin_checkout' AND c.order_id IS NULL THEN 'schema_invalid'
    WHEN c.event_type = 'purchase'
      AND (c.amount IS NULL OR c.currency IS NULL OR c.order_id IS NULL)
      THEN 'schema_invalid'
    -- ── 取值：amount ≥ 0
    WHEN c.amount IS NOT NULL AND c.amount < 0 THEN 'value_out_of_range'
    -- ── 时间区间：相对下界 + 上界（与 validate_event 同源）
    WHEN c.event_time < c.batch_min_ms - 120000 THEN 'time_out_of_range'
    WHEN c.event_time > c.now_ms + 300000       THEN 'time_out_of_range'
    ELSE CAST(NULL AS STRING)
  END AS reject_reason
FROM (
  SELECT
    o.event_id, o.event_time, o.user_id, o.item_id, o.category_id,
    o.event_type, o.schema_version, o.page, o.source, o.channel, o.province,
    o.payload, o.dt AS ods_dt, o.src_seq,
    r.batch_min_ms, r.now_ms,
    -- ★ DWD 的 dt = **事件日 UTC**（≠ ODS 的落地日）
    DATE_FORMAT(TO_TIMESTAMP_LTZ(o.event_time, 3), 'yyyy-MM-dd') AS event_dt,
    -- ★ 四个信封字段从**整行原文**取（见头注①），只用于校验与 DWD 列
    TRY_CAST(JSON_VALUE(o.payload, '$.amount')   AS DOUBLE) AS amount,
    -- ★ 原始串：用于把「amount 存在但不可转数字」判成 value_out_of_range
    --   （与 events.validate_event 的 float(amount) 失败分支同源，见头注⑥）
    JSON_VALUE(o.payload, '$.amount')                       AS amount_raw,
    JSON_VALUE(o.payload, '$.currency')                     AS currency,
    TRY_CAST(JSON_VALUE(o.payload, '$.quantity') AS INT)    AS quantity,
    JSON_VALUE(o.payload, '$.order_id')                     AS order_id,
    -- ★ 到达序水印：按 src_seq 定序，到此行（含）为止已见过的最大 event_time
    MAX(o.event_time) OVER (
      ORDER BY o.src_seq
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS arrival_wm_ms
  FROM v_ods_scope o
  CROSS JOIN v_batch_ref r
) c;

-- ── 写入 DWD：只取 reject_reason IS NULL 的行（与下面**互补**）───────────
-- ★ 用 INSERT OVERWRITE 而非 INSERT：
--   DWD 是主键表，重跑同一批本就会被主键吸收；但 OVERWRITE 让「重放两遍仍同结果」
--   成为**可判**事实（幂等判据，spec §5.4），而不是依赖主键覆盖的副作用。
INSERT OVERWRITE dwd_behavior_event
SELECT
  c.event_dt                                  AS dt,
  c.event_id, c.event_time, c.user_id, c.item_id, c.category_id,
  c.event_type, c.schema_version, c.page, c.source, c.channel, c.province,
  CAST(c.amount AS DECIMAL(18, 2))            AS amount,
  c.currency, c.quantity, c.order_id,
  CAST(c.is_late AS TINYINT)                  AS is_late,
  c.late_lag_ms
FROM v_checked c
WHERE c.reject_reason IS NULL;

-- ── 写入拒绝表：reject_reason IS NOT NULL 的行 ─────────────────────────
-- ★ 与 DWD 侧的过滤条件**互补** ⇒ 每条 ODS 行恰好进一侧（A1 分流完备的产出面）。
-- ★ dt = **摄入日（处理时间）**，不是事件日：拒绝行的事件时间本身不可信，
--   拿它做分区布局＝让坏数据决定布局（spec §5.2 逐字）。
INSERT OVERWRITE dwd_behavior_event_reject
SELECT
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
  c.event_id, c.event_time, c.user_id, c.item_id, c.category_id,
  c.event_type, c.schema_version,
  c.payload                                    AS raw_payload,
  c.reject_reason,
  UNIX_TIMESTAMP() * 1000                      AS ingest_time
FROM v_checked c
WHERE c.reject_reason IS NOT NULL;
-- ── 层报告（spec §5.3 可观察性）：逐层 layer=<层> rows=… ──────────────
-- ★ DWD 与拒绝表两行都要打：A1 分流完备要求两侧行数都可见。
SELECT CONCAT('layer=dwd rows=', CAST(COUNT(*) AS STRING)) AS layer_report
FROM dwd_behavior_event;

-- ★ marker 词汇表：spec §5.3 只定义 layer=ods|dwd|dim|dws|ads 五个标签。
--   拒绝表不是第六"层"，它是 dwd 层的**旁路产物**，故用 dwd_reject 前缀而**不**新造
--   layer=reject —— 否则编排脚本的层 marker 表会出现一个 LAYER= 无法选中的标签。
SELECT CONCAT('dwd_reject rows=', CAST(COUNT(*) AS STRING)) AS layer_report
FROM dwd_behavior_event_reject;
