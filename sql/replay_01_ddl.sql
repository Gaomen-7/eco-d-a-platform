-- replay_01_ddl.sql — 十一张表的 DDL 形状（impl-03b）
--
-- 规格落点：spec §5.2「五层＋两伴生表形状表」（逐字）。本文件的每一段都对应形状表
-- 的一行；验收要求「分区键、主键、bucket、表属性与形状表逐项一致，逐条列表核对」。
--
-- ★ 三条全表共用的定案（附录 A·N，2026-10-21 签）：
--   1. 分区键统一 dt STRING = yyyy-MM-dd，UTC。**全表无 hour 分区**。
--   2. 主键表默认 MOR 而非 MOW ⇒ **不开** 'deletion-vectors.enabled'
--      （MOW 才需要显式开它；开了就从 MOR 变 MOW，与裁决相反）。
--   3. 主键表固定 bucket=1 且**不设 sequence.field**
--      （同 event_id 重复行该值相同 ⇒ 回落内部序、白设；靠重放确定性），
--      并**必须**设 table.exec.sink.upsert-materialize=NONE。
--
-- ★ 关于 dt 的两种语义（形状表逐条不同，别统一）：
--   ODS  dt = 落地日（数据到达湖的日期）
--   DWD  dt = 事件日（event_time 的 UTC 日）
--   拒绝 dt = 摄入日（**处理时间**，不是事件日——拒绝行的事件时间本身不可信）
--   DWS/ADS dt = 窗口起点日
--   DIM 不分区
--
-- ★ 本文件只建表，不写任何断言（断言归 impl-03d，03d 之前 assert_lake.sql 可不存在）。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ecom_lake;
USE ecom_lake;

-- ════════════════════════════════════════════════════════════════════
-- ① ODS：ods_behavior_event —— append，分区 dt=落地日，payload 原文零解析
-- ════════════════════════════════════════════════════════════════════
-- 形状表：append / 分区 dt（落地日） / 无主键 / bucket -1 / payload 原文列；零解析
--
-- ★「零解析」的准确边界（**别把它读成「本表一个字段都不拆」**）：
--   形状表的「零解析」限定在 **payload 这一列** —— 它存 STRING 原文，
--   本层**不拆 payload 的子对象、不做类型归属、不做时间单位换算**。
--   实测依据：payload 里只有 session_id，amount/currency/quantity/order_id 在行的
--   **顶层**；首版只存 `$.payload` 子对象就丢了四个字段（见 replay_10 头注）。
--   ⚠️ 但本表**确实**另有 11 个信封列（event_id…province）由 replay_10 用
--      JSON_VALUE 从行里投影出来 —— 它们是**信封字段的结构化投影**，不是 payload 解析。
--      这不是本票新开的口子：形状表的 ODS 行没列全信封列，而分区/join/下游校验
--      都需要它们；「一切解析归 DWD」的原始出处（gdd Q25）针对的是 payload 内容。
--      ⇒ 已在票面 §A·13 登记为**已知口径收窄**，不是隐式偏离。
-- ★ bucket 显式写 -1：形状表列的取值就是 -1。append 表 -1 = 不分桶；
--   这与主键表的固定 bucket=1 是两套语义，故显式写出来更可核对（非「不写」）。
CREATE TABLE IF NOT EXISTS ods_behavior_event (
  event_id       STRING COMMENT '事件唯一标识（原样，不校验——校验归 DWD）',
  event_time     BIGINT COMMENT '事件时间 epoch ms（原样）',
  user_id        STRING,
  item_id        STRING,
  category_id    STRING,
  event_type     STRING,
  schema_version STRING,
  page           STRING,
  source         STRING,
  channel        STRING,
  province       STRING,
  payload        STRING COMMENT '整行 JSON 原文——零解析、逐字节保留（见文件头注）',
  src_seq        BIGINT COMMENT '投递序（dump 的 _src_offset）—— DWD 算迟到用的到达序',
  dt             STRING COMMENT '落地日 yyyy-MM-dd（UTC）'
) PARTITIONED BY (dt)
WITH (
  'bucket' = '-1'
);

-- ════════════════════════════════════════════════════════════════════
-- ② DWD：dwd_behavior_event —— 主键 deduplicate，dt=事件日 UTC，bucket=1
-- ════════════════════════════════════════════════════════════════════
-- 形状表：主键 deduplicate / 分区 dt（**事件日** UTC） / 主键 (dt, event_id) /
--         bucket 固定 1 / is_late、late_lag_ms / upsert-materialize=NONE
CREATE TABLE IF NOT EXISTS dwd_behavior_event (
  dt             STRING COMMENT '事件日 yyyy-MM-dd（UTC）—— event_time 的 UTC 日',
  event_id       STRING,
  event_time     BIGINT,
  user_id        STRING,
  item_id        STRING,
  category_id    STRING,
  event_type     STRING,
  schema_version STRING,
  page           STRING,
  source         STRING,
  channel        STRING,
  province       STRING,
  amount         DECIMAL(18, 2),
  currency       STRING,
  quantity       INT,
  order_id       STRING,
  is_late        TINYINT COMMENT '该行是否来自迟到通道（0/1）',
  late_lag_ms    BIGINT  COMMENT '迟到量 = 水印 - event_time（ms）；非迟到为 0',
  PRIMARY KEY (dt, event_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

-- ════════════════════════════════════════════════════════════════════
-- ③ 拒绝表：dwd_behavior_event_reject —— append，dt=摄入日（处理时间）
-- ════════════════════════════════════════════════════════════════════
-- 形状表：append / 分区 dt（**摄入日＝处理时间**） / 无主键 / bucket -1 /
--         reject_reason 封闭三值；**重复不进表**
--
-- ★ 为什么 dt 取处理时间而不是事件日（spec §5.2 逐字）：
--   拒绝行的 event_time 本身不可信（坏数据），拿它做分区布局＝让坏数据决定布局。
-- ★ reject_reason 取值域**恰为三值**，三值里**没有 duplicate**：
--     schema_invalid / value_out_of_range / time_out_of_range
--   重复走幂等吸收通道（DWD 主键覆盖），不进拒绝表、不入脏率。
--   ⚠️ 取值域由 replay_20_dwd.sql 的 CASE **唯一**保证。
--      为什么表侧不加 CHECK：**Flink SQL 解析器不支持 CHECK 约束**（本票实机证伪，
--      2026-10-22：ParseException Encountered "CHECK"）。Flink/Paimon 均无该语法，
--      ⇒ 取值域是**产出侧**的契约，不是存储侧的 schema 约束；判据只能查实际取值。
CREATE TABLE IF NOT EXISTS dwd_behavior_event_reject (
  dt             STRING COMMENT '摄入日 yyyy-MM-dd（处理时间，UTC）—— 不是事件日',
  event_id       STRING COMMENT '原样保留（可能为空——schema_invalid 时正是它坏了）',
  event_time     BIGINT COMMENT '原样保留，**不可信**，不用于分区',
  user_id        STRING,
  item_id        STRING,
  category_id    STRING,
  event_type     STRING,
  schema_version STRING,
  raw_payload    STRING COMMENT '被拒原文（整条 JSON，便于事后复核）',
  reject_reason  STRING COMMENT '封闭三值：schema_invalid / value_out_of_range / time_out_of_range',
  ingest_time    BIGINT COMMENT '摄入时刻 epoch ms（处理时间）'
) PARTITIONED BY (dt)
WITH (
  'bucket' = '-1'
);

-- ════════════════════════════════════════════════════════════════════
-- ④ DIM 拉链：dim_item / dim_category / dim_user —— **不分区**，bucket=1
-- ════════════════════════════════════════════════════════════════════
-- 形状表：主键 deduplicate / **不分区** / 主键 (id, valid_from_ms) / bucket 固定 1 /
--         拉链区间 ms；区间 join
--
-- ★ 字段改名（spec §5.2）：start_date/end_date → valid_from_ms/valid_to_ms（BIGINT ms）
--   valid_to_ms = 9999999999999 表示仍有效。
-- ★ 时间源 = 维度变更流 change_time（事件时间语义，满足 D-3）。
-- ★★ **区间两侧都是闭区间**：[valid_from_ms, valid_to_ms]，且
--   `valid_to_ms = 下一版 valid_from_ms − 1`。
--   于是「边界那一毫秒」恰归**新版本**（旧版在它之前一毫秒就已失效），
--   两个版本既不重叠、也不留空隙。若把 valid_to_ms 当开区间，同一毫秒会
--   既不命中旧版（因为 `− 1` 已让旧版提前结束）而新版从该毫秒开始 ⇒ 不重叠成立；
--   但若改成「valid_to_ms = 下一版起点」再配开区间，则边界毫秒两版都不命中。
--   ⇒ 写法（−1）与判据（BETWEEN，闭）**必须成对**；
--     已由 tests/test_replay_temporal_sql.py::test_zipper_valid_to_is_next_change_time_minus_one 钉住。
-- ★ 时态维 join **走 batch 区间 join**（impl-03c 落地），不是 lookup join
--   （lookup join 是 processing-time，回放必错）。
--   ⚠️ 分级：这句是**本仓上下游文档的一致口径**（spec §5.2 + [05] Q10），
--      **本票没有查到逐字官方出处（无版本号/URL）** ⇒ 按 HLD §0.3 属**设计值级**约束，
--      **不是** `官方给出`。首版写成「官方逐字否定」是越界（code-review 指出），已改。
-- ★ 不分区：维度表行数少且需要**全量**区间 join，分区只会让 join 端多一层裁剪。
--   ⚠️ 因此这里**没有** PARTITIONED BY —— 不是漏写。

CREATE TABLE IF NOT EXISTS dim_item (
  item_id       STRING,
  valid_from_ms BIGINT COMMENT '本版本生效起点（epoch ms，闭区间）',
  item_name     STRING,
  category_id   STRING,
  price_cents   INT,
  brand         STRING,
  valid_to_ms   BIGINT COMMENT '本版本失效**终点**（epoch ms，**闭区间含本毫秒**）；9999999999999=仍有效',
  change_time   BIGINT COMMENT '维度变更事件时间（epoch ms）——拉链时间源',
  PRIMARY KEY (item_id, valid_from_ms) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

CREATE TABLE IF NOT EXISTS dim_category (
  category_id   STRING,
  valid_from_ms BIGINT,
  category_name STRING,
  parent_id     STRING,
  level         INT,
  valid_to_ms   BIGINT,
  change_time   BIGINT,
  PRIMARY KEY (category_id, valid_from_ms) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

CREATE TABLE IF NOT EXISTS dim_user (
  user_id       STRING,
  valid_from_ms BIGINT,
  user_level    STRING,
  province      STRING,
  register_date STRING,
  valid_to_ms   BIGINT,
  change_time   BIGINT,
  PRIMARY KEY (user_id, valid_from_ms) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

-- ════════════════════════════════════════════════════════════════════
-- ⑤ DIM 静态：dim_region / dim_channel —— 无拉链，不分区
-- ════════════════════════════════════════════════════════════════════
-- 形状表：主键 deduplicate / 不分区 / 主键 (province) / (channel) / bucket 固定 1 / 无拉链
CREATE TABLE IF NOT EXISTS dim_region (
  province   STRING,
  region     STRING COMMENT '大区（如 华北/华东）',
  province_code STRING,
  PRIMARY KEY (province) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

CREATE TABLE IF NOT EXISTS dim_channel (
  channel      STRING,
  channel_group STRING COMMENT '渠道分组（如 付费/自然）',
  is_paid      TINYINT,
  PRIMARY KEY (channel) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

-- ════════════════════════════════════════════════════════════════════
-- ⑥ DWS：dws_subject_window —— 分区 dt=窗口起点日，bucket=1
-- ════════════════════════════════════════════════════════════════════
-- 形状表：主键 deduplicate / 分区 dt（窗口起点日） /
--         主键 (dt, subject_type, subject_id, window_start) / bucket 固定 1 /
--         subject_type∈user/item/category；指标列 pv/uv/gmv
CREATE TABLE IF NOT EXISTS dws_subject_window (
  dt           STRING COMMENT '窗口起点日 yyyy-MM-dd（UTC）',
  subject_type STRING COMMENT 'user / item / category',
  subject_id   STRING,
  window_start BIGINT COMMENT '窗口起点 epoch ms',
  window_end   BIGINT COMMENT '窗口终点 epoch ms（开区间）',
  pv           BIGINT COMMENT '页面浏览次数',
  uv           BIGINT COMMENT '去重用户数',
  gmv          DECIMAL(18, 2) COMMENT '成交金额',
  PRIMARY KEY (dt, subject_type, subject_id, window_start) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

-- ════════════════════════════════════════════════════════════════════
-- ⑦ ADS：ads_window_metric —— 与 Doris ecom_ads.ads_window_metric 键同构
-- ════════════════════════════════════════════════════════════════════
-- 形状表：主键 deduplicate / 分区 dt（窗口起点日） /
--         主键 (dt, metric_name, dim_key, window_start) / bucket 固定 1
--
-- ★ 两套物化、一个口径（spec §5.2）：
--   Paimon 侧 = 离线口径基准（对账基准面，**不直接供看板**）
--   Doris 侧  = 看板供数点（唯一供看板消费，REQ-F-OLAP-01 冻结）
-- 同名指标同一公式产出 ⇒ 键必须同构，这是 impl-03f 对账的前提。
CREATE TABLE IF NOT EXISTS ads_window_metric (
  dt           STRING COMMENT '窗口起点日 yyyy-MM-dd（UTC）',
  metric_name  STRING COMMENT '指标名（与 Doris ecom_ads 同名）',
  dim_key      STRING COMMENT '维度键（如 channel=organic / 全局 ALL）',
  window_start BIGINT COMMENT '窗口起点 epoch ms',
  window_end   BIGINT COMMENT '窗口终点 epoch ms（开区间）',
  metric_value DECIMAL(18, 2) COMMENT '指标值',
  is_settled   TINYINT COMMENT '是否落定（对账只比 is_settled=1）',
  PRIMARY KEY (dt, metric_name, dim_key, window_start) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE'
);

-- ════════════════════════════════════════════════════════════════════
-- ⑧ 迟到计数表：dwd_late_event_count —— append，只存计数
-- ════════════════════════════════════════════════════════════════════
-- 形状表：append / 分区 dt / 无主键 / bucket -1 / dt × late_bucket（**60 s 档**）只存计数
--
-- ★「只存计数」是承重的（canonical-truth §3.3 三原则）：本表**不存**事件明细，
--   只有 (dt, late_bucket, cnt)。它是 A3 迟到对账的唯一数据源：
--       count(*) FROM dwd WHERE is_late=1  ==  sum(cnt) FROM dwd_late_event_count
--   ⇒ 因此 cnt 必须是「该 60 s 档内的迟到条数」，不是 0/1 标志。
--   ★ **写入已由 impl-03c 落地**（`sql/replay_50_ads.sql` 的 ⑤ 段，
--      `INSERT OVERWRITE dwd_late_event_count`）：按 (事件日 × 60 s 档) 聚合 `is_late=1`。
--   ⚠️⚠️ **这条等价式是同义反复，不是证据**（spec 轴审阅指出，已更正）：
--      `cnt` 就是由 `WHERE is_late = 1` 这个**同一谓词**算出来的，
--      所以 `sum(cnt) == count(*) WHERE is_late=1` 必然成立（同义反复）。
--      它能证的只有「写入没写错分区/没丢桶」，**不能**证「迟到计数与上游一致」。
--   ⚠️⚠️ **更实质的语义缺口（本票未解决，挂 03d）**：本表按设计该装的是
--      **超期事件**（被 sideOutputLateData 丢掉、根本没进 DWD 的那些），
--      而当前实现装的是**进了 DWD 的 `is_late=1` 行**。两者**不是同一个群体**：
--      超期事件在 DWD 里根本没有行，故当前写法对它们**零覆盖**。
--      ⇒ 要么本表语义改为「DWD 内迟到行按档计数」（则 DDL 上这段注释要改），
--        要么补一条从超期通道来的写入（那要走旁路输出，SQL-only 面做不到）。
--      本票**不猜**：登记在此，03d 做 A3 断言时必须先裁这一条。
-- ★ 超期事件不进 DWD（被 sideOutputLateData 丢弃），它们只在此表计数——
--   这就是「为什么迟到标记差异不可加」的实例（A2 只入日志、不作断言）。
CREATE TABLE IF NOT EXISTS dwd_late_event_count (
  dt           STRING COMMENT '事件日 yyyy-MM-dd（UTC）',
  late_bucket  BIGINT COMMENT '该事件所属的 60 s 档（epoch ms 向下取整到 60 s）',
  cnt          BIGINT COMMENT '本档迟到事件条数（只存计数，不存明细）'
) PARTITIONED BY (dt)
WITH (
  'bucket' = '-1'
);
