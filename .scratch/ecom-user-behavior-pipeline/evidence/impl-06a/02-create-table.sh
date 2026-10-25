#!/usr/bin/env bash
# impl-06a 演练 · 第 2 步：建独立演练库 + HOUR 演练表（验收①）
# 只建 ecom_rehearsal 库，不碰 ecom_ads（票面风险边界）。
# 产物：stdout 全量进 evidence/impl-06a/02-create-table.out.txt
set -euo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] 建独立演练库（ecom_ads 之外） ==="
dq -e "CREATE DATABASE IF NOT EXISTS ecom_rehearsal;"
echo "CREATE DATABASE IF NOT EXISTS ecom_rehearsal; -- OK"
echo

echo "=== [2] 建 HOUR 演练表：time_unit=HOUR + 小 start(-2) + create_history_partition=true ==="
cat <<'SQL'
CREATE TABLE ecom_rehearsal.ads_window_metric_rehearsal (
    window_start    DATETIMEV2(0)  NOT NULL COMMENT '1 分钟窗口起点（演练表）',
    metric_name     VARCHAR(64)    NOT NULL COMMENT '指标名',
    dim_key         VARCHAR(128)   NOT NULL DEFAULT '' COMMENT '维度值',
    metric_value    BIGINT         NOT NULL DEFAULT 0,
    revision        INT            NOT NULL DEFAULT 0,
    is_settled      TINYINT        NOT NULL DEFAULT 0,
    updated_at      DATETIMEV2(0)  NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = OLAP
UNIQUE KEY (window_start, metric_name, dim_key)
PARTITION BY RANGE (window_start)()
DISTRIBUTED BY HASH (metric_name, dim_key) BUCKETS 1
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "HOUR",
    "dynamic_partition.start" = "-2",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.create_history_partition" = "true",
    "replication_allocation" = "tag.location.default: 1"
);
SQL
dq -e "CREATE TABLE ecom_rehearsal.ads_window_metric_rehearsal (
    window_start    DATETIMEV2(0)  NOT NULL COMMENT '1 分钟窗口起点（演练表）',
    metric_name     VARCHAR(64)    NOT NULL COMMENT '指标名',
    dim_key         VARCHAR(128)   NOT NULL DEFAULT '' COMMENT '维度值',
    metric_value    BIGINT         NOT NULL DEFAULT 0,
    revision        INT            NOT NULL DEFAULT 0,
    is_settled      TINYINT        NOT NULL DEFAULT 0,
    updated_at      DATETIMEV2(0)  NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = OLAP
UNIQUE KEY (window_start, metric_name, dim_key)
PARTITION BY RANGE (window_start)()
DISTRIBUTED BY HASH (metric_name, dim_key) BUCKETS 1
PROPERTIES (
    \"dynamic_partition.enable\" = \"true\",
    \"dynamic_partition.time_unit\" = \"HOUR\",
    \"dynamic_partition.start\" = \"-2\",
    \"dynamic_partition.end\" = \"1\",
    \"dynamic_partition.prefix\" = \"p\",
    \"dynamic_partition.create_history_partition\" = \"true\",
    \"replication_allocation\" = \"tag.location.default: 1\"
);"
echo "-- CREATE TABLE OK"
echo

echo "=== [3] FE 当前时刻（UTC，分区边界基准） ==="
dq -e "SELECT NOW() AS fe_now_utc;"
echo

echo "=== [4] 建表当场分区清单（预期 4 个：now-2h/now-1h/now/now+1h，名形 p<yyyyMMddHH>） ==="
dq -e "SHOW PARTITIONS FROM ecom_rehearsal.ads_window_metric_rehearsal;"
echo

echo "=== [5] SHOW DYNAMIC PARTITION TABLES（注册态） ==="
dq -e "SHOW DYNAMIC PARTITION TABLES FROM ecom_rehearsal;"
