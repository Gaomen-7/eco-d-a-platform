-- assert_lake.sql — 离线断言集：A1 / A3 / 幂等指纹 / 快照三态 / 拉链两条 / 属性钉住
--
-- 规格落点：spec §5.3（`assert_lake.sql`）、§5.4（离线断言、快照 [5,20]、拉链两条）、
--          §5.5（**断言执行面**：哨兵 + stdout 解析）。
-- 配套：`scripts/lake_assert.sh`（判定面）+ `tests/test_lake_assert.py`（判定逻辑单测）。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ 为什么断言要写成「哨兵」而不是靠退出码（spec §5.5 + 本票实机复核）
-- ════════════════════════════════════════════════════════════════════
-- 源码事实：`sql-client.sh -f` 语句报错后**仍以 0 退出**（`CliClient` 丢弃返回值、
-- 无 `System.exit`）。本机实测一份「先一条非法语句、再一条合法语句」的脚本：
--     EXIT=0 且输出里恰有 1 条错误标记
-- ⇒ **退出码不能当判据**，唯一可用的信号是 stdout。
-- ⟹ 每条断言必须自己把结论**打印成字符串**，由 `lake_assert.sh` 解析。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ 输出语法（`lake_assert.sh` 逐字按这个语法解析；改语法必改脚本 + 单测）
-- ════════════════════════════════════════════════════════════════════
-- TABLEAU 结果表渲染成 `| cell | cell |`。本文件**只**产出下面两种两列行：
--
--   A) **哨兵行**（判定面）：  | <NAME> | ok=true |   或   | <NAME> | ok=false |
--      · 名字是**稳定标识**，脚本按名字逐条核对；脚本硬编码一份期望清单，
--        「少一条」「多一条」「改了名」都算失败。
--      · `ok` 必须是**字符串字面量** `ok=true`/`ok=false`，不是 BOOLEAN ——
--        脚本据此区分的不是"值真假"，而是"这一行是不是哨兵"。
--
--   B) **度量行**（只日志）：  | metric.<name> | <value> |
--      · 第二列**绝不**以 `ok=` 开头 ⇒ 结构上不可能被误判成断言。
--      · A2（重复记账）落在这一类：spec §5.4 逐字「只入日志、不作断言」。
--
-- ★★ 两条实机限制（不满足就会静默漏判，都已实测）：
--   1. **单元格被截断**：TABLEAU 的列宽由会话参数
--      `sql-client.display.max-column-width` 控制，**官方默认 30 字符**，
--      超宽单元格渲染成 `xxxx...`（本机实测：42 字符的断言名被截断）。
--      ⚠️ 分级：**默认值 30 是官方给出**；但它是**可配项** —— 换会话参数就会变。
--        本仓不靠"正好卡在 30"，而是留余量把哨兵名钉在 **≤ 26**（**设计值**，
--        留 4 字符余量），指纹则拆成多条短度量行、永不塞进一个长单元格。
--   2. **sql-client 会回显 SQL 源码**（含注释），每条语句前有 `Flink SQL> ` 或 `> `
--      前缀。⇒ 本文件的注释里**不得**出现带方括号的错误标记（否则脚本会把它当
--      真错误）。静态守卫：`tests/test_lake_assert.py::test_no_error_token_in_sql`。
--      脚本侧则只解析以 `|` 开头的行 ⇒ 不会被回显文本里的 `ok=true` 字样带偏。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ 两处**实测证伪**的写法（别照 spec 的伪代码抄）
-- ════════════════════════════════════════════════════════════════════
-- 1) **`crc32` 在 Flink 2.2 SQL 里不存在**（spec §5.4 的
--    `SUM(crc32(concat_ws(...)))` 逐字不能执行）。本机实测逐字：
--      SqlValidatorException: No match found for function signature crc32(<CHARACTER>)
--    同一台机实测可用的替代：`HASH_CODE(x)`（INT）、`MD5(x)` / `SHA1(x)` / `SHA2(x,n)`。
--    ⇒ 幂等指纹改用 **`SUM(CAST(HASH_CODE(...) AS BIGINT))`**。
--      ⚠️ `HASH_CODE` 返回 **INT**，百万行求和会**溢出 INT** ⇒ 必须先 `CAST` 成
--        `BIGINT` 再 `SUM`。直接 `SUM(HASH_CODE(...))` 会得到负数/错值，
--        而它在两次跑之间**照样稳定** ⇒ 会是一条"恒绿但不校验"的假指纹。
-- 2) **A3 不得写成 `sum(cnt) == count(*) WHERE is_late=1`**（impl-03c 已裁）：
--    `cnt` 由**同一谓词** `is_late=1` 算出 ⇒ 同义反复。按裁定写成
--    **逐档比对 + 档集合比对**（见 ③ 段）。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ A1 的粒度：**不按事件日分区比**（但 ODS 侧限定本次落地批 —— 见 ① 段）
-- ════════════════════════════════════════════════════════════════════
-- spec §5.4 逐字：`ods_distinct = dwd_distinct + reject_distinct`，**全局比对**。
-- 「全局」的**目的**是「不要按事件日分区比」——因为 ODS 的 `dt` 是**落地日**
-- （append、跨天累积），DWD 的 `dt` 是**事件日** ⇒ 两边没有共同日期轴。
-- 硬按日比会把「今天落地、事件发生在昨天」这种正常情况判成失败。这一点本文件遵守。
-- ⚠️ 但「不加任何过滤 = 整表」会引入另一类病（实测）：ODS 整表会累积历史落地批，
--   一旦某天落了**另一批**，等式两侧的数据范围就不再同源，A1 会无缘无故红。
--   ⇒ ODS 侧限定**本次落地日**，与 `replay_20_dwd.sql` 的 `v_ods_scope` **同源**。
--   这是**粒度收窄**（整表 → 本次落地批），不是改回按事件日分区。
--   已登记票面 Progress（不静默改判据）；若上游判定必须整表，
--   正确修法是**清理历史落地批**，而不是放宽判据。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ 快照区间的**三态**（单看一条会在空表/新表上假红）
-- ════════════════════════════════════════════════════════════════════
--   · 有数据、提交过多次：判 **[1,20]**（上界是 `retain_max => 20` 收的；
--     下界不要求 5 —— `snapshot.num-retained.min` 是**保护下界、不制造快照**，
--     官方逐字 "When the number of snapshots is less than snapshot.num-retained.min,
--     no snapshots will be expired"）。
--   · **空表**（本批 0 行 ⇒ 0 快照，如 `dwd_behavior_event_reject`）：判 **= 0**。
--     这同时是「min 不制造快照」的**可观察反证**。
--   · 属性本身是否钉住 → 另立一条（`$options`），因为 `10` 也落在 [1,20] 里，
--     光看数量**看不出**有人漏写了 min=5。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ 拉链两条（spec §5.4 逐字：批读会丢 DELETE ⇒ 分两条走）
-- ════════════════════════════════════════════════════════════════════
--   ① **区间自洽** → 走**当前快照的普通 SELECT**（`LEAD` 窗口 + `BETWEEN` 的闭区间约定）；
--   ② **变更发生过** → 走 `t$audit_log`（审计面，能看见被批读抹掉的历史）。
--   ★ ② 的判据**不能**写成「存在 UPDATE rowkind」：本机实测三张拉链表与 DWD 的
--     `$audit_log` 里 **rowkind 全是 `+I`**，一条 `-U`/`+U`/`-D` 都没有 ——
--     原因不是"没变更"，而是**写路径**：`replay_30_dim.sql` 用整表 `INSERT OVERWRITE`，
--     每个版本是一条**新的物理行**（拉链表键含 `valid_from_ms`）⇒ Paimon 看去全是 INSERT。
--     ⇒ 判据改成「按 id 数出 **≥2 条**审计记录」：与写路径无关，且确实读的是审计面。
--   ⚠️ 审计日志同样受快照过期影响 ⇒ 本条只要求「现在能看到已发生的变更」，
--      不要求「历史上所有变更都在」。这也正是①②要分开的理由：可靠性不同。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
-- 紧随 replay 之后跑，与层脚本共用同一同步口径以免时序歧义（§D5 实测）。
SET 'table.dml-sync' = 'true';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f6_derived BEGIN' AS fx;

-- ════════════════════════════════════════════════════════════════════
-- ⓪ 规模（**度量行**，只日志）
-- ════════════════════════════════════════════════════════════════════
-- 五层 + 拒绝表 + 迟到计数表 的行数汇总。给报告提供规模上下文；
-- **不是**断言 —— 「某层 > 0」由各层自己的 `layer=<层> rows=` 正向证据覆盖。
SELECT 'metric.rows.ods' AS k, CAST(COUNT(*) AS STRING) AS v FROM ods_behavior_event;
-- ★★ `landing_day` = 本次判定**实际比对的那个落地日**。
--   为什么必须有这一行：下面 A1/A2 的 ODS 侧取的是 `MAX(dt)`（见 ① 段的 G1 说明），
--   而不是 `CURRENT_TIMESTAMP` 的日期。两者在**跨 UTC 日**时会不同 ——
--   若不给这一行，读者看到 `a2_dup_overshoot = 0` 无法分辨那是
--   「这批真没重复」还是「比到的那天压根是另一批 / 空分区」。
--   ⇒ 把"比的是哪天"显式打出来，让数值可归因。
SELECT 'metric.rows.landing_day' AS k,
       CAST(MAX(dt) AS STRING) AS v FROM ods_behavior_event;
-- ★ `ods_today` = **本次落地日**的 ODS 行数。它与 `rows.ods` 并列出现是刻意的：
--   A1 的 ODS 侧按落地日过滤（见 ① 段），而 DWD 是整表。若这两个数差得离谱、
--   同时 A1 红了，读者必须立刻能分辨是「分流真出了问题」还是
--   「刚落过 UTC 日边界 / 本批还没落地」——后者会表现为 `ods_today = 0`
--   而 `rows.dwd` 仍是上一批的行数。没有这一行，A1 的失败会指向完全错误的排查方向。
SELECT 'metric.rows.ods_today' AS k, CAST(COUNT(*) AS STRING) AS v FROM ods_behavior_event
WHERE dt = (SELECT MAX(dt) FROM ods_behavior_event);
SELECT 'metric.rows.dwd' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event;
SELECT 'metric.rows.reject' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event_reject;
SELECT 'metric.rows.dim_item' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_item;
SELECT 'metric.rows.dim_category' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_category;
SELECT 'metric.rows.dim_user' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_user;
SELECT 'metric.rows.dws' AS k, CAST(COUNT(*) AS STRING) AS v FROM dws_subject_window;
SELECT 'metric.rows.ads' AS k, CAST(COUNT(*) AS STRING) AS v FROM ads_window_metric;
SELECT 'metric.rows.late_count' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_late_event_count;

-- ════════════════════════════════════════════════════════════════════
-- ① A1 — 分流完备（**断言**）
-- ════════════════════════════════════════════════════════════════════
-- 判据：`ods_distinct = dwd_distinct + reject_distinct`
-- 用 DISTINCT 而非 count(*)：ODS 是 append、允许重复（重复的记账归 A2），
-- 而「分流完备」问的是**每个不同事件都恰好落进某一侧**。
-- 产出侧保证互补：`replay_20_dwd.sql` 的两条 INSERT 用
-- `reject_reason IS NULL` / `IS NOT NULL` 划界，故等式应恒等成立；
-- 它破的时候一定是**读取侧**出了问题（例如某侧被过滤掉了）。
--
-- ★★ 一处**口径收窄**（实测发现，按仓规登记、不猜）：
--   票面/spec §5.4 逐字写「**全局**比对（ODS 无事件日分区，降为全局粒度）」。
--   「全局」在那句话里的目的是「**不要按事件日分区比**」——因为 ODS 的 `dt` 是
--   **落地日**、不是事件日，两边没有共同的日期轴。这一点本文件遵守。
--   但把它读成「ODS 整表、不加任何过滤」会得到一个**依赖历史**的判据：
--   本机实测（数值留在票面 E1，此处只记形态）：ODS 整表 = 两个落地日各一套同一批，
--   而 `ods_distinct` 因 event_id 相同而**塌回单批的规模** ⇒ 这条**碰巧**是绿的。
--   一旦某天落了**另一批**（不同 seed / 不同规模），`ods_distinct` 会把两批并起来，
--   而 DWD 只装本次落地日的那一批 ⇒ A1 会**无缘无故红**，且红得无法归因。
--   ⇒ 本文件把 A1 的 ODS 侧限定为**本次落地日**，与 DWD 的读取范围
--     （`replay_20_dwd.sql` 的 `v_ods_scope`）**同源**。
--   这**不是**改成"按事件日分区比"，故不违背 spec 的原意；
--   粒度从「整个 ODS 表」收窄到「本次落地批」。
--   ⚠️ 已登记到票面 Progress（不静默改判据）：若上游判定必须严格"整表"，
--      正确修法是把历史落地日的旧批**清理掉**，而不是放宽本判据。
-- ★★ 这一条**还会挡一个真事故**：若在 UTC 日边界刚过、本批还没重放时跑断言，
--    新落地日无数据而 DWD 仍是上一批的规模 ⇒ 等式不成立 ⇒ **正确地红**。
-- ★★ 落地日取 `(SELECT MAX(dt) FROM ods_behavior_event)`，**不取**
--    `DATE_FORMAT(CURRENT_TIMESTAMP,…)`（code-review 的 G1）。为什么：
--    用 CURRENT_TIMESTAMP 会把判据绑死到「跑断言那天的 UTC 日」，
--    而 replay 可能在前一天跑完 —— 跨日后两处时间基不一致，
--    A2 会**静默变成 0**（0 在日志里读起来正是"这批没有重复"，语义被悄悄换掉），
--    A1 会红但失败信息读不出「是时间窗错位」。
--    `MAX(dt)` 表达的是「最近一次落地的批」，与 replay 的产出天然对齐；
--    它到底取到了哪天，由 `metric.rows.landing_day` 明确打出来，可核。
--    若不加这个过滤（整表），两边都是上一批的数，A1 会**假绿**地通过
--    —— 断言在"批次根本不对齐"时通过，比红更糟。

-- ════════════════════════════════════════════════════════════════════
-- ② A2 — 重复记账（**只入日志、不作断言**，spec §5.4 逐字）
-- ════════════════════════════════════════════════════════════════════
-- 数值 = `count(*) − count(DISTINCT event_id)`（ODS 侧）。
-- 为什么只报 ODS 侧：DWD 是主键表，重复已被**结构**吸收 ⇒ 那里的差恒为 0，
-- 写出来只是一条恒零。而 ODS 侧这个差是**非恒真**的实测量。
-- 为什么它是"只记账"而非"一道门"：只要 DWD 用主键吸收重复，「DWD 无重复」就是
-- 表结构保证的，拿它当断言只会得到一条永远绿的假门（impl-02e 记过同类教训）。
-- 它的真实用途：解释「为什么迟到标记差异**不可加**」——
-- 重复行在 DWD 被吸收，却会被 count(*) 口径重复计入。
-- ★ `lake_assert.sh` **不**判这一行（第二列不以 `ok=` 开头）。
-- ★★ **必须**限定到「最近一次落地批」（实测抓到的坑）：ODS 是 append + 落地日分区，
--    跨天累积。首版没加过滤时，得到的数与"批内重复"**完全无关** ——
--    它等于**前一天那一整个分区**的行数（本机跑过两遍、跨了两个 UTC 日）。
--    ⚠️ 落地日取 `MAX(dt)` 而非 `CURRENT_TIMESTAMP`：理由见 ① 段末尾（G1）。
--      这一条尤其重要 —— A2 **不作断言**，它出错时没有任何哨兵会红，
--      只能靠读者看数值；而"0"这个值在两种含义下都合法（真无重复 / 比错天）。
--    ⇒ 与 `replay_20_dwd.sql` 的 `v_ods_scope` **同一过滤表达式**（同源），
--      否则这个数字报的是"跑了几次"，不是"这批有多少重复"。
--    （A1 不受此影响：它用 DISTINCT，且 DWD 侧本就只读本次落地日。
--      但两个数并排看时，粒度不同会让读者以为 A2 在度量别的东西。）
SELECT 'metric.a2_dup_overshoot' AS k,
       CAST(COUNT(*) - COUNT(DISTINCT event_id) AS STRING) AS v
FROM ods_behavior_event
WHERE dt = (SELECT MAX(dt) FROM ods_behavior_event);

-- ════════════════════════════════════════════════════════════════════
-- ③ A3 — 迟到对账（**断言**，按裁定写成「逐档 + 档集合」）
-- ════════════════════════════════════════════════════════════════════
-- ★★ 为什么不写 `sum(cnt) == count(*) WHERE is_late=1`（impl-03c 已裁）：
--   `cnt` 由**同一谓词** `is_late=1` 算出 ⇒ 必然相等，同义反复，
--   只证「分桶没丢」，不证迟到口径正确。
-- ★★ 按 DDL ⑧ 段抬头给的**可写形态**拆两条：
--   ③a **逐档数值相等 + 档集合相等**：两侧按同一 `(dt, late_bucket)` 对齐后
--      **逐档**比 `cnt`；用 `FULL OUTER JOIN` 让「少一档」「多一档」也现形
--      （只写 INNER JOIN 时，计数表**整段缺失**会得到 0 个不匹配行 = 假绿）。
--   ③b **迟到标记确有量**：`count(*) WHERE is_late=1 > 0`。
--      这条是**必需**的兜底：若 `is_late` 全 0（impl-03b 修过的那类事故），
--      ③a 会以「两侧皆空集」的形式**通过** ⇒ 必须有它挡着。
-- ⚠️ ③a 不是"强证据"：两侧的分桶表达式都在本仓，它能拦**口径漂移**，
--   拦不了"两边一起错"。后者需外部真值，归 03e 的反例证伪（本票刻意不穷举）。
SELECT 'A3_late_bucket_split' AS assertion,
       CASE WHEN COUNT(*) = 0 THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM (
  SELECT COALESCE(l.dt, '∅') AS dt, COALESCE(l.late_bucket, -1) AS bucket
  FROM (SELECT dt, late_bucket, cnt FROM dwd_late_event_count) l
  FULL OUTER JOIN (
    SELECT dt, late_bucket, cnt FROM (
      SELECT
        DATE_FORMAT(TO_TIMESTAMP_LTZ((event_time / 60000) * 60000, 3), 'yyyy-MM-dd') AS dt,
        (event_time / 60000) * 60000 AS late_bucket,
        CAST(COUNT(*) AS BIGINT) AS cnt
      FROM dwd_behavior_event
      WHERE is_late = 1
      GROUP BY
        DATE_FORMAT(TO_TIMESTAMP_LTZ((event_time / 60000) * 60000, 3), 'yyyy-MM-dd'),
        (event_time / 60000) * 60000
    ) g
  ) r
  ON l.dt = r.dt AND l.late_bucket = r.late_bucket
  WHERE l.cnt IS NULL OR r.cnt IS NULL OR l.cnt <> r.cnt
) mismatch;

SELECT 'A3_late_flag_nonempty' AS assertion,
       CASE WHEN COUNT(*) > 0 THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM dwd_behavior_event
WHERE is_late = 1;

-- ════════════════════════════════════════════════════════════════════
-- ④ 幂等 — DWD 指纹（**指纹是度量行；「两次一致」由脚本比较**）
-- ════════════════════════════════════════════════════════════════════
-- `REQ-F-VERIFY-02` 的离线版：同批 dump 重放两遍，DWD 内容必须逐字节等价。
-- ★ 为什么指纹拆成多条短度量行：TABLEAU 会把长单元格截断（见文件头限制 1），
--   一个 `ok=true rows=… fp=… devent=…` 的长串会被截成 `ok=true rows=… fp=<前几位>...`
--   ⇒ **指纹被截掉的部分恰恰是判据本身**。
-- ★ 指纹四件套（每件都短、都单独可比）：
--   `metric.fp.rows`   = COUNT(*)                     ← 行数
--   `metric.fp.hash`   = SUM(CAST(HASH_CODE(三列) AS BIGINT))  ← 逐列校验和
--   `metric.fp.devent` = COUNT(DISTINCT event_id)      ← 拦「和相同但分布不同」
--   `metric.fp.dlag`   = COUNT(DISTINCT late_lag_ms)
--   ⚠️ 只比 hash 会漏「两行互换同值」；只比 rows 会漏内容变化。四件套缺一不可。
-- ★ 单次运行无法自证幂等（没有"上一遍"）⇒ 本段只**产出**指纹；
--   `lake_assert.sh --record` 落盘、`--compare` 与上一遍比对。
-- ★ 但本节仍要有**一条**非恒真哨兵：指纹本身可不可信？
--   若 DWD 为空，`SUM` 返回 NULL、rows=0 ⇒ 指纹是"空指纹"，
--   两次比对会**因为都是空而通过**。故判 `rows>0 且 hash 非空`。
SELECT 'IDEMPOTENCY_fp_ready' AS assertion,
       CASE WHEN COUNT(*) > 0
             AND SUM(CAST(HASH_CODE(CONCAT_WS('|',
                   event_id,
                   CAST(is_late AS STRING),
                   CAST(late_lag_ms AS STRING))) AS BIGINT)) IS NOT NULL
            THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM dwd_behavior_event;

SELECT 'metric.fp.rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event;
SELECT 'metric.fp.hash' AS k,
       CAST(SUM(CAST(HASH_CODE(CONCAT_WS('|',
             event_id,
             CAST(is_late AS STRING),
             CAST(late_lag_ms AS STRING))) AS BIGINT)) AS STRING) AS v
FROM dwd_behavior_event;
SELECT 'metric.fp.devent' AS k, CAST(COUNT(DISTINCT event_id) AS STRING) AS v
FROM dwd_behavior_event;
SELECT 'metric.fp.dlag' AS k, CAST(COUNT(DISTINCT late_lag_ms) AS STRING) AS v
FROM dwd_behavior_event;

-- ════════════════════════════════════════════════════════════════════
-- ⑤ 快照保留（**断言**，三态）
-- ════════════════════════════════════════════════════════════════════
-- ⑤a 非空表：快照数落 **[1,20]**（上界由 `retain_max => 20` 收；下界只要 ≥1）。
--     `dwd_behavior_event_reject` **不在此清单**：本批 0 行 ⇒ 0 快照，归 ⑤c。
SELECT 'SNAP_within_upper_bound' AS assertion,
       CASE WHEN COUNT(*) = 0 THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM (
  SELECT 'ods_behavior_event' AS t, COUNT(*) AS n FROM ods_behavior_event$snapshots
  UNION ALL SELECT 'dwd_behavior_event', COUNT(*) FROM dwd_behavior_event$snapshots
  UNION ALL SELECT 'dim_item', COUNT(*) FROM dim_item$snapshots
  UNION ALL SELECT 'dim_category', COUNT(*) FROM dim_category$snapshots
  UNION ALL SELECT 'dim_user', COUNT(*) FROM dim_user$snapshots
  UNION ALL SELECT 'dim_region', COUNT(*) FROM dim_region$snapshots
  UNION ALL SELECT 'dim_channel', COUNT(*) FROM dim_channel$snapshots
  UNION ALL SELECT 'dws_subject_window', COUNT(*) FROM dws_subject_window$snapshots
  UNION ALL SELECT 'ads_window_metric', COUNT(*) FROM ads_window_metric$snapshots
  UNION ALL SELECT 'dwd_late_event_count', COUNT(*) FROM dwd_late_event_count$snapshots
) s
WHERE s.n < 1 OR s.n > 20;

-- ⑤b 逐表快照数（**度量行**，票面「逐表查 $snapshots，留下数值」）
SELECT 'metric.snap.ods' AS k, CAST(COUNT(*) AS STRING) AS v FROM ods_behavior_event$snapshots;
SELECT 'metric.snap.dwd' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event$snapshots;
SELECT 'metric.snap.reject' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event_reject$snapshots;
SELECT 'metric.snap.dim_item' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_item$snapshots;
SELECT 'metric.snap.dim_category' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_category$snapshots;
SELECT 'metric.snap.dim_user' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_user$snapshots;
SELECT 'metric.snap.dim_region' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_region$snapshots;
SELECT 'metric.snap.dim_channel' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_channel$snapshots;
SELECT 'metric.snap.dws' AS k, CAST(COUNT(*) AS STRING) AS v FROM dws_subject_window$snapshots;
SELECT 'metric.snap.ads' AS k, CAST(COUNT(*) AS STRING) AS v FROM ads_window_metric$snapshots;
SELECT 'metric.snap.late_count' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_late_event_count$snapshots;

-- ⑤c 空表 / 非空表的快照数（**双向**判据）
--     空表（0 行）⇒ 快照必须 **0**（证明 `min=5` 不凭空制造快照）；
--     非空表(>0 行) ⇒ 快照必须 **≥1**（有数据就该有提交）。
--     ⚠️ 本仓当前唯一的空表是 reject（门槛批 0 条拒绝）。若未来门槛批出现拒绝行，
--        本哨兵会走第二条分支（≥1），**不会**因此变红 —— 这正是双向写法的收益：
--        单向写法（`行数 > 0 OR 快照 = 0`）在"有行但 0 快照"时恒绿，
--        那是本文件通篇在防的「判据在某状态下什么都不判」（code-review 抓到的 G5）。
SELECT 'SNAP_empty_table_zero' AS assertion,
       CASE WHEN (
         (SELECT COUNT(*) FROM dwd_behavior_event_reject) = 0
         AND (SELECT COUNT(*) FROM dwd_behavior_event_reject$snapshots) = 0
       ) OR (
         (SELECT COUNT(*) FROM dwd_behavior_event_reject) > 0
         AND (SELECT COUNT(*) FROM dwd_behavior_event_reject$snapshots) >= 1
       ) THEN 'ok=true' ELSE 'ok=false' END AS ok;

-- ════════════════════════════════════════════════════════════════════
-- ⑥ 拉链两条（**断言**）
-- ════════════════════════════════════════════════════════════════════
-- ⑥a **区间自洽**：走**当前快照的普通 SELECT**（不依赖批读），三件事任一不满足即失败：
--     ① 同一 id 的版本起点严格递增；
--     ② `valid_to_ms = 下一版 valid_from_ms − 1`（写侧 −1 与读侧闭区间是一对）；
--     ③ 最后一版的 `valid_to_ms` 是「仍有效」哨兵 9999999999999。
--     ★ 三张拉链表**都要判**（impl-03c 的 code-review 抓到过「只查 dim_item」）。
SELECT 'ZIP_interval_self_sync' AS assertion,
       CASE WHEN COUNT(*) = 0 THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM (
  SELECT z.item_id AS id, z.valid_from_ms AS vf, z.valid_to_ms AS vt,
         LEAD(z.valid_from_ms) OVER (PARTITION BY z.item_id ORDER BY z.valid_from_ms) AS next_vf
  FROM dim_item z
  UNION ALL
  SELECT z.category_id, z.valid_from_ms, z.valid_to_ms,
         LEAD(z.valid_from_ms) OVER (PARTITION BY z.category_id ORDER BY z.valid_from_ms)
  FROM dim_category z
  UNION ALL
  SELECT z.user_id, z.valid_from_ms, z.valid_to_ms,
         LEAD(z.valid_from_ms) OVER (PARTITION BY z.user_id ORDER BY z.valid_from_ms)
  FROM dim_user z
) chain
WHERE (chain.next_vf IS NOT NULL AND chain.next_vf <= chain.vf)
   OR (chain.next_vf IS NOT NULL AND chain.vt <> chain.next_vf - 1)
   OR (chain.next_vf IS NULL AND chain.vt <> 9999999999999);

-- ⑥b **变更发生过**：走 `t$audit_log`（审计面），判「三张表都有 id 出现 ≥2 次」。
--     ⚠️ 必须 `COUNT(DISTINCT t)`：内层是按 id 分组的**多行**结果，
--        直接 `COUNT(*)` 数的是**id 个数**（每张表几十到几千不等），不是表数
--        —— 实测得到过一个远大于 3 的三位数级结果，那正是被数错的东西。
--     ★ 为何不用 rowkind 判：见文件头「拉链两条」——本仓写路径全是 INSERT。
SELECT 'ZIP_audit_log_shows_change' AS assertion,
       CASE WHEN COUNT(DISTINCT t) = 3 THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM (
  SELECT 'dim_item' AS t FROM (
    SELECT item_id FROM dim_item$audit_log GROUP BY item_id HAVING COUNT(*) >= 2
  ) a
  UNION ALL
  SELECT 'dim_category' FROM (
    SELECT category_id FROM dim_category$audit_log GROUP BY category_id HAVING COUNT(*) >= 2
  ) b
  UNION ALL
  SELECT 'dim_user' FROM (
    SELECT user_id FROM dim_user$audit_log GROUP BY user_id HAVING COUNT(*) >= 2
  ) c
) changed;

-- ════════════════════════════════════════════════════════════════════
-- ⑦ 属性钉住（**断言**）：11 张表的 min=5 / max=20 真的生效
-- ════════════════════════════════════════════════════════════════════
-- ★ 为什么必须查 `$options`：`CREATE TABLE IF NOT EXISTS` 对**已存在**的表一句不改
--   ⇒ 老库会保持官方默认 **10**，而 10 **也落在 [1,20] 里** ⇒ ⑤ 的数量判据
--   **看不出**漏写。只有直接读属性才判得出。`replay_90_maintain.sql` 的 ⓪ 段
--   用 `ALTER TABLE ... SET` 兜这个底（幂等）。
-- ★ `$options` 的列是 `(key, value)`（实测），`key` 是保留字 ⇒ 必须反引号。
-- ★ 期望条数 = 11 张表 × 2 条属性 = **22**：少一条即失败，
--   「某表漏了 min」与「某表整个 $options 读不到」都会露出来。
-- ★ `snapshot.time-retained` **不判**：本仓刻意保持官方默认 1 h，
--   「表里没有这条 key」正是"用默认"的正常形态。
-- ★★ 为什么是**逐条枚举**而不是「GROUP BY 表名再比 count」这种派生写法：
--   派生写法只能判「有几张表两行都对」，**判不出是哪张表错**；
--   更关键的是它**判不出表清单本身缺了一张** —— 若某张表整个漏建，
--   `GROUP BY` 出来的行数会少 1，而 `HAVING`/`count` 条件可以恰好仍成立。
--   枚举 + 定值 22 让「表集合」与「属性值」**同时**被钉住。
-- ⚠️ 这是同一契约的**第三处**字面量（另两处：`replay_01_ddl.sql` 的 DDL、
--   `replay_90_maintain.sql` 的 ALTER）。三处**都必要、无法合并**：
--     · DDL 管**新建**的表；
--     · ALTER 管**已存在**的表（`CREATE TABLE IF NOT EXISTS` 对老表一句不改）；
--     · 本段是**判据**（判据不能去读 DDL 文本 —— 那测的是源码，不是集群状态）。
--   ⇒ 改 `5`/`20` 任一值必须**同时**改这三处；守卫：
--     `tests/test_lake_assert.py::test_snapshot_retention_literals_agree`。
SELECT 'OPT_snapshot_retention' AS assertion,
       CASE WHEN COUNT(*) = 22 THEN 'ok=true' ELSE 'ok=false' END AS ok
FROM (
  SELECT 'ods' AS t FROM ods_behavior_event$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'ods' FROM ods_behavior_event$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dwd' FROM dwd_behavior_event$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dwd' FROM dwd_behavior_event$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'reject' FROM dwd_behavior_event_reject$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'reject' FROM dwd_behavior_event_reject$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dim_item' FROM dim_item$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dim_item' FROM dim_item$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dim_category' FROM dim_category$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dim_category' FROM dim_category$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dim_user' FROM dim_user$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dim_user' FROM dim_user$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dim_region' FROM dim_region$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dim_region' FROM dim_region$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dim_channel' FROM dim_channel$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dim_channel' FROM dim_channel$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'dws' FROM dws_subject_window$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'dws' FROM dws_subject_window$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'ads' FROM ads_window_metric$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'ads' FROM ads_window_metric$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
  UNION ALL SELECT 'late_count' FROM dwd_late_event_count$options
    WHERE `key` = 'snapshot.num-retained.min' AND `value` = '5'
  UNION ALL SELECT 'late_count' FROM dwd_late_event_count$options
    WHERE `key` = 'snapshot.num-retained.max' AND `value` = '20'
) pinned;
