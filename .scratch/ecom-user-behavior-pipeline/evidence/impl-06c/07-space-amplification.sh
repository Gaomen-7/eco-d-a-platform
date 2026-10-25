#!/usr/bin/env bash
# impl-06c · 第 7 步：沉降后测量——空间放大实测 + compaction 日志取证 + BE 配置快照
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@" 2>/dev/null; }

echo "=== [1] 行数 + SHOW DATA（统计可能异步滞后，以 du 为磁盘权威） ==="
dq -e "SELECT COUNT(*) AS rows_probe FROM ecom_collision.compaction_probe;"
dq -e "SHOW DATA FROM ecom_collision.compaction_probe;"

echo
echo "=== [2] tablet 清单（观测对象） ==="
dq -e "SHOW TABLETS FROM ecom_collision.compaction_probe;" | awk '{print $1}' | tail -n +2 > /tmp/impl06c-tablets.txt
cat /tmp/impl06c-tablets.txt

echo
echo "=== [3] du 精测（storage/data 逐子目录 top10） ==="
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage /opt/apache-doris/be/storage/data"
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/data/* 2>/dev/null | sort -rn | head -10"

echo
echo "=== [4] 本表 8 个 tablet 的目录占用（du 求和=表落盘净增） ==="
TOT=0
while read -r tid; do
  KB=$(docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/data/*/\"$tid\" 2>/dev/null | cut -f1" 2>/dev/null)
  [ -z "$KB" ] && KB=0
  echo "tablet $tid: ${KB} KB"
  TOT=$((TOT + KB))
done < /tmp/impl06c-tablets.txt
echo "TABLET_DU_TOTAL_KB: $TOT"

echo
echo "=== [5] compaction 日志逐字取证（本表 tablet 的 succeed 行 + 失败行） ==="
PATTERN=$(paste -sd'|' /tmp/impl06c-tablets.txt)
docker exec eco-doris-be sh -c "grep -E 'compaction.cpp:[0-9]+\] (succeed|failed)' /opt/apache-doris/be/log/be.INFO 2>/dev/null | grep -E \"tablet=($PATTERN)|tablet_id: ($PATTERN)\"" > /tmp/impl06c-compaction-words.log || true
wc -l /tmp/impl06c-compaction-words.log
echo "--- cumulative 成功行（succeed to do cumulative）条数与耗时合计:"
grep -c 'succeed to do cumulative' /tmp/impl06c-compaction-words.log || true
grep 'succeed to do cumulative' /tmp/impl06c-compaction-words.log | grep -o 'elapsed time=[0-9.]*s' | awk -F= '{s+=$2} END {printf "N=%d total_elapsed=%.2fs\n", NR, s}'
echo "--- base 成功行条数与耗时合计:"
grep -c 'succeed to do base' /tmp/impl06c-compaction-words.log || true
grep 'succeed to do base' /tmp/impl06c-compaction-words.log | grep -o 'elapsed time=[0-9.]*s' | awk -F= '{s+=$2} END {printf "N=%d total_elapsed=%.2fs\n", NR, s}'
echo "--- 失败行（如有）:"
grep -c 'failed' /tmp/impl06c-compaction-words.log || echo 0
echo "--- 输入/输出字节合计（cumulative+base）:"
grep -o 'input_rowsets_data_size=[0-9.]* [KMG]B' /tmp/impl06c-compaction-words.log | head -0
grep 'succeed to do' /tmp/impl06c-compaction-words.log | grep -oE 'input_rowsets_data_size=[0-9.]+ (KB|MB|GB)' | sort | uniq -c | sort -rn | head -8
grep 'succeed to do' /tmp/impl06c-compaction-words.log | grep -oE 'output_rowset_data_size=[0-9.]+ (KB|MB|GB)' | sort | uniq -c | sort -rn | head -8
echo "--- compaction 日志全文副本落盘: 07-compaction-words.out.txt（注意：本步 grep 漏 -a 产 0 行，权威副本由 07b 补齐）"
cp /tmp/impl06c-compaction-words.log ./07-compaction-words.out.txt

echo
echo "=== [6] BE compaction/内存相关配置快照（含 memtable flush 限流参数） ==="
docker exec eco-doris-be sh -c "curl -s localhost:8040/api/show_config | tr ',' '\n' | grep -Ei 'compaction|memtable|memory_limit|flush' | grep -vi 'comment'" | head -40

echo
echo "=== [7] 内存/flush 运行时形态快照（memtracker top） ==="
docker exec eco-doris-be sh -c "curl -s localhost:8040/metrics | grep -Ei '^doris_be_(memory_allocated|process_mem|memtable|jemalloc_allocated|jemalloc_active)' | grep -v '^#'"
echo "=== AMPLIFICATION DONE ==="
date -u '+end: %Y-%m-%d %H:%M:%S UTC'
