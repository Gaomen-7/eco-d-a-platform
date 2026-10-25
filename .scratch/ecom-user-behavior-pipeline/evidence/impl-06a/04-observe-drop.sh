#!/usr/bin/env bash
# impl-06a 演练 · 第 4 步：观测「过期 HOUR 分区被调度器真实删除」（验收②）
# 观测窗 T0 起 ≤1200 s（= 2 × dynamic_partition_check_interval_seconds 600 s，官方给出）。
# 每 30 s 轮询：过期分区是否还在 + LastDropPartitionMsg；全部消失即记录 T_end 收口。
# 退出码：0 = 观测到删除；1 = 1200 s 内未观测到（诚实失败）。
# 产物：stdout 全量进 evidence/impl-06a/04-observe-drop.out.txt
set -uo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

EXPIRED=(p2026102506 p2026102507 p2026102508 p2026102509)
T0=$(date -u +%s)
T0_STR=$(date -u '+%Y-%m-%d %H:%M:%S')
echo "T0(UTC): ${T0_STR}  —— 观测窗 ≤1200 s（2 个调度周期，interval=600 s 官方给出）"
echo "待观测删除的目标分区: ${EXPIRED[*]}"
echo "轮询间隔 30 s；每行格式：<UTC时刻> | 剩余过期分区数 n/4 | 剩余名单 | LastDropPartitionMsg"
echo "---------------------------------------------------------------"

i=0
while :; do
  i=$((i+1))
  now_epoch=$(date -u +%s)
  elapsed=$((now_epoch - T0))
  if [ "$elapsed" -gt 1200 ]; then
    echo "---------------------------------------------------------------"
    echo "T_OUT(UTC): $(date -u '+%Y-%m-%d %H:%M:%S')  elapsed=${elapsed}s —— ✗ 1200 s 内未观测到全部过期分区被删除"
    dq -e "SHOW PARTITIONS FROM ecom_rehearsal.ads_window_metric_rehearsal;"
    dq -e "SHOW DYNAMIC PARTITION TABLES FROM ecom_rehearsal;"
    exit 1
  fi

  parts=$(dq -e "SHOW PARTITIONS FROM ecom_rehearsal.ads_window_metric_rehearsal;" 2>/dev/null | tail -n +2 | awk -F'\t' '{print $2}')
  remaining=""
  for p in "${EXPIRED[@]}"; do
    if echo "$parts" | grep -qx "$p"; then remaining="$remaining $p"; fi
  done
  n=$(echo $remaining | wc -w)
  msg=$(dq -e "SHOW DYNAMIC PARTITION TABLES FROM ecom_rehearsal;" 2>/dev/null | tail -n 1 | awk -F'\t' '{print $14}')
  printf '%s | elapsed=%4ds | remaining %d/4 |%s | LastDropPartitionMsg=%s\n' \
    "$(date -u '+%H:%M:%S')" "$elapsed" "$n" "$remaining" "${msg:-(empty)}"

  if [ "$n" -eq 0 ]; then
    T_END=$(date -u +%s)
    echo "---------------------------------------------------------------"
    echo "✓ T_END(UTC): $(date -u '+%Y-%m-%d %H:%M:%S')  elapsed=$((T_END - T0))s —— 全部过期 HOUR 分区已消失"
    echo
    echo "=== 收口证据 [A] 终态分区清单（应只剩 4 个保留窗内分区 p10-p13，随后滚动+1） ==="
    dq -e "SHOW PARTITIONS FROM ecom_rehearsal.ads_window_metric_rehearsal;"
    echo
    echo "=== 收口证据 [B] SHOW DYNAMIC PARTITION TABLES（LastDropPartitionMsg 应有删除记录） ==="
    dq -e "SHOW DYNAMIC PARTITION TABLES FROM ecom_rehearsal;"
    echo
    echo "=== 收口证据 [C] 演练表行数（预期 1：只剩当前小时那行，过期分区内 4 行随分区删除） ==="
    dq -e "SELECT COUNT(*) AS rehearsal_rows_after FROM ecom_rehearsal.ads_window_metric_rehearsal;"
    dq -e "SELECT window_start, metric_name, dim_key, metric_value FROM ecom_rehearsal.ads_window_metric_rehearsal;"
    exit 0
  fi
  sleep 30
done
