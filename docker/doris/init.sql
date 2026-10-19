-- Doris 初始化 —— P0/P1 前置
--
-- ★ 为什么必须显式开 FE 级总开关（gdd §D D8 的第三个前置条件，最容易漏）：
--   `dynamic_partition_enable` 的**官方默认是 false**。即使表上写了
--   `"dynamic_partition.enable" = "true"`，FE 侧总开关关闭时**分区创建/删除调度根本不跑**。
--   ⇒ gdd §1.5 的判据「SHOW PARTITIONS 显示历史分区被实际删除」在默认配置下
--     **必然不通过**。这条必须先执行，否则后面所有分区保留断言都是假的。
--
-- 执行方式（P0 实测：Doris Stream Load 的 FE→BE 第二跳宿主 follow 不到，
-- 故所有 Doris 客户端操作都应在 compose 网内发起，见 HLD §4.4）：
--   docker compose run --rm client mysql -h doris-fe -P 9030 -u root -e "$(cat docker/doris/init.sql)"
-- 或分步：
--   docker compose run --rm client mysql -h doris-fe -P 9030 -u root < docker/doris/init.sql

-- ── ① FE 级动态分区总开关（默认 false ⇒ 必须显式开）──────────────────
ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");

-- 顺便放宽单表可建分区数（默认 500；30 天保留 × 多表不需要更多，保持默认即可）
-- ADMIN SET FRONTEND CONFIG ("max_dynamic_partition_num" = "500");

-- ── ② 看板直连库（REQ-F-OLAP-01 冻结值：ecom_ads）──────────────────
CREATE DATABASE IF NOT EXISTS ecom_ads;

-- ── ③ ADS 指标表：UNIQUE KEY + Merge-on-Write（幂等覆盖写）──────────
-- 依据 gdd §1.5：
--   - UNIQUE KEY 模型保留（MoW 官方**默认即开**，无需额外 property）
--   - 覆盖写修正依赖 same-key upsert；如需部分列更新，INSERT INTO 路径的开关是
--     **session 变量** `enable_unique_key_partial_update`（Stream Load 侧才是 header
--     `partial_columns:true`）——两条路是两个不同开关，gdd §1.5 已记
--   - 「分区键必须是 Key 列的子集」（官方约束）
USE ecom_ads;

-- 分钟级指标（PV/UV/GMV/漏斗等）——窗口修正靠同 key 覆盖 ⇒ revision 递增
CREATE TABLE IF NOT EXISTS ads_window_metric (
    window_start    DATETIMEV2(0)  NOT NULL COMMENT '1 分钟窗口起点',
    metric_name     VARCHAR(64)    NOT NULL COMMENT '指标名：pv/uv/gmv/...',
    dim_key         VARCHAR(128)   NOT NULL DEFAULT '' COMMENT '维度值（品类/地域等），无维度用空串',
    metric_value    BIGINT         NOT NULL DEFAULT 0 COMMENT '指标值；UV/GMV 亦以整数存（分）',
    revision        INT            NOT NULL DEFAULT 0 COMMENT '窗口修正次数（迟到覆盖写 +1）',
    is_settled      TINYINT        NOT NULL DEFAULT 0 COMMENT '1=落定 0=暂定（看板须视觉区分）',
    updated_at      DATETIMEV2(0)  NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = OLAP
UNIQUE KEY (window_start, metric_name, dim_key)
PARTITION BY RANGE (window_start)()
DISTRIBUTED BY HASH (metric_name, dim_key) BUCKETS 4
PROPERTIES (
    -- ★ 30 天保留（REQ-F-OLAP-03）。三处必须齐备，缺一则不生效：
    --   dynamic_partition.start 出厂默认 -2147483648 ＝「历史分区不删除」（gdd D8）
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",                  -- ← 出厂默认永不删，必须显式设
    "dynamic_partition.end" = "3",                      -- ← 必填
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.create_history_partition" = "true",  -- ← 不设则历史分区不建
    "replication_allocation" = "tag.location.default: 1"    -- 单 BE
);

-- 步进漏斗（双列并陈：观测值 / 推断值）——gdd §1.3 / GLOSSARY「观测值 / 推断值」
CREATE TABLE IF NOT EXISTS ads_funnel_step (
    window_start     DATETIMEV2(0) NOT NULL,
    session_date     DATE          NOT NULL COMMENT '会话归属日',
    step_no          TINYINT       NOT NULL COMMENT '1=浏览 2=加购 3=下单 4=支付',
    cnt_observed     BIGINT        NOT NULL DEFAULT 0 COMMENT '只记真实到达的步',
    cnt_inferred     BIGINT        NOT NULL DEFAULT 0 COMMENT '含补记（推断值，不得冒充观测值）',
    revision         INT           NOT NULL DEFAULT 0,
    updated_at       DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = OLAP
UNIQUE KEY (session_date, step_no, window_start)
PARTITION BY RANGE (session_date)()
DISTRIBUTED BY HASH (step_no) BUCKETS 2
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.create_history_partition" = "true",
    "replication_allocation" = "tag.location.default: 1"
);

-- ── ④ 迟到超期计数表（gdd §D D10：超期只计数，绝不修正）────────────
CREATE TABLE IF NOT EXISTS ads_late_expired_count (
    window_start  DATETIMEV2(0) NOT NULL COMMENT '本应落入的窗口',
    late_lag_ms   BIGINT        NOT NULL DEFAULT 0 COMMENT '迟到幅度（毫秒）',
    cnt           BIGINT        NOT NULL DEFAULT 0,
    updated_at    DATETIMEV2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = OLAP
UNIQUE KEY (window_start, late_lag_ms)
PARTITION BY RANGE (window_start)()
DISTRIBUTED BY HASH (window_start) BUCKETS 2
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.create_history_partition" = "true",
    "replication_allocation" = "tag.location.default: 1"
);

-- ── ⑤ 自检：确认总开关与保留策略真的生效（判据可证，不是「配置可见」）──
-- 期望 dynamic_partition_enable = true
SHOW FRONTEND CONFIG LIKE 'dynamic_partition_enable';
-- 期望看到 p<日期> 分区存在，且 30 天前的分区会被调度删除
SHOW PARTITIONS FROM ads_window_metric;
SHOW DYNAMIC PARTITION TABLES;
