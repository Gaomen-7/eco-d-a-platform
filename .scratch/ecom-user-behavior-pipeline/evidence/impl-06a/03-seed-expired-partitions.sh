#!/usr/bin/env bash
# impl-06a 演练 · 第 3 步：预置 4 个「已过期」HOUR 分区 + 写入行
# 为什么手工预置：start=-2 的建表只造 [now-2h, now+1h] 的分区，没有「已过期」分区可删；
# 手工 ADD PARTITION 造出 3-6 小时前的分区（命名/边界严格按 HOUR 规范），
# 让调度器在 ≤2 个 tick（≤1200 s）内真实删除它们 —— 这正是被观测的行为本身。
# 产物：stdout 全量进 evidence/impl-06a/03-seed-expired-partitions.out.txt
set -euo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [0] 记录观测起点 T0（UTC + 本地） ==="
date -u '+T0(UTC): %Y-%m-%d %H:%M:%S'
date '+T0(local): %Y-%m-%d %H:%M:%S (%Z)'
echo

echo "=== [1] 由 FE 时钟核对：host 侧推算的 UTC 小时与 FE NOW() 一致 ==="
for off in 6 5 4 3 2 1 0; do
  printf 'now-%dh => p%s  [%s, %s)\n' "$off" "$(date -u -d "-${off} hours" +%Y%m%d%H)" \
    "$(date -u -d "-${off} hours" +'%Y-%m-%d %H:00:00')" \
    "$(date -u -d "$((1-off)) hours" +'%Y-%m-%d %H:00:00')"
done
dq -e "SELECT NOW() AS fe_now_utc;"
echo

echo "=== [2] ADD PARTITION ×4（now-6h .. now-3h，均落在保留窗口 [now-2h,∞) 之外） ==="
P6=$(date -u -d "-6 hours" +%Y%m%d%H); B6=$(date -u -d "-6 hours" +'%Y-%m-%d %H:00:00'); E6=$(date -u -d "-5 hours" +'%Y-%m-%d %H:00:00')
P5=$(date -u -d "-5 hours" +%Y%m%d%H); B5=$(date -u -d "-5 hours" +'%Y-%m-%d %H:00:00'); E5=$(date -u -d "-4 hours" +'%Y-%m-%d %H:00:00')
P4=$(date -u -d "-4 hours" +%Y%m%d%H); B4=$(date -u -d "-4 hours" +'%Y-%m-%d %H:00:00'); E4=$(date -u -d "-3 hours" +'%Y-%m-%d %H:00:00')
P3=$(date -u -d "-3 hours" +%Y%m%d%H); B3=$(date -u -d "-3 hours" +'%Y-%m-%d %H:00:00'); E3=$(date -u -d "-2 hours" +'%Y-%m-%d %H:00:00')
echo "★ 实机发现（4.1.5）：动态分区表上直接 ADD PARTITION 被拒——"
echo "  \"Cannot add/drop partition on a Dynamic Partition Table, Use command"
echo "   \`ALTER TABLE ... SET (\"dynamic_partition.enable\" = \"false\")\` firstly.\""
echo "  ⇒ 官方指定的绕法：临时关表级 enable → 手工 ADD → 再开回。删除仍由调度器执行。"
cat <<SQL
ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal SET ("dynamic_partition.enable" = "false");
ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P6} VALUES [("${B6}"),("${E6}"));
ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P5} VALUES [("${B5}"),("${E5}"));
ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P4} VALUES [("${B4}"),("${E4}"));
ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P3} VALUES [("${B3}"),("${E3}"));
ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal SET ("dynamic_partition.enable" = "true");
SQL
dq -e "ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal SET (\"dynamic_partition.enable\" = \"false\");"
dq -e "ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P6} VALUES [(\"${B6}\"),(\"${E6}\"));"
dq -e "ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P5} VALUES [(\"${B5}\"),(\"${E5}\"));"
dq -e "ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P4} VALUES [(\"${B4}\"),(\"${E4}\"));"
dq -e "ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal ADD PARTITION p${P3} VALUES [(\"${B3}\"),(\"${E3}\"));"
dq -e "ALTER TABLE ecom_rehearsal.ads_window_metric_rehearsal SET (\"dynamic_partition.enable\" = \"true\");"
echo "-- ALTER ADD PARTITION OK（${P6} ${P5} ${P4} ${P3}）"
echo

echo "=== [3] 写入行：4 行落过期分区 + 1 行落当前小时分区（p$(date -u +%Y%m%d%H)） ==="
H6_TS=$(date -u -d "-6 hours" +'%Y-%m-%d %H:30:00')
H5_TS=$(date -u -d "-5 hours" +'%Y-%m-%d %H:30:00')
H4_TS=$(date -u -d "-4 hours" +'%Y-%m-%d %H:30:00')
H3_TS=$(date -u -d "-3 hours" +'%Y-%m-%d %H:30:00')
CUR_TS=$(date -u +'%Y-%m-%d %H:30:00')
cat <<SQL
INSERT INTO ecom_rehearsal.ads_window_metric_rehearsal (window_start, metric_name, dim_key, metric_value) VALUES
  ('${H6_TS}', 'pv', 'rehearsal', 1),
  ('${H5_TS}', 'pv', 'rehearsal', 1),
  ('${H4_TS}', 'pv', 'rehearsal', 1),
  ('${H3_TS}', 'pv', 'rehearsal', 1),
  ('${CUR_TS}', 'pv', 'rehearsal', 1);
SQL
dq -e "INSERT INTO ecom_rehearsal.ads_window_metric_rehearsal (window_start, metric_name, dim_key, metric_value) VALUES
  ('${H6_TS}', 'pv', 'rehearsal', 1),
  ('${H5_TS}', 'pv', 'rehearsal', 1),
  ('${H4_TS}', 'pv', 'rehearsal', 1),
  ('${H3_TS}', 'pv', 'rehearsal', 1),
  ('${CUR_TS}', 'pv', 'rehearsal', 1);"
echo

echo "=== [4] 写后状态：8 个分区 + 全表 5 行 ==="
dq -e "SHOW PARTITIONS FROM ecom_rehearsal.ads_window_metric_rehearsal;"
dq -e "SELECT COUNT(*) AS rehearsal_rows_total FROM ecom_rehearsal.ads_window_metric_rehearsal;"
echo

echo "=== [5] 观测基准：SHOW DYNAMIC PARTITION TABLES（LastDropPartitionMsg 此时应为 NULL） ==="
dq -e "SHOW DYNAMIC PARTITION TABLES FROM ecom_rehearsal;"
echo

echo "=== [6] T0 重述（观测窗从此刻起算，≤1200 s） ==="
date -u '+T0(UTC): %Y-%m-%d %H:%M:%S'
date '+T0(local): %Y-%m-%d %H:%M:%S (%Z)'
