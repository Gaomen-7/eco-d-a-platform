#!/usr/bin/env bash
# impl-06c · 07b：修复 07[5] 的 grep -a 缺失（be.INFO 二进制混合文件），补 compaction 逐字取证 + 关键配置值
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

echo "=== [5-fix] 本表 tablet 的 compaction succeed/failed 行（-a 修复版） ==="
docker exec eco-doris-be sh -c "grep -aE 'compaction.cpp:[0-9]+\] (succeed|failed)' /opt/apache-doris/be/log/be.INFO 2>/dev/null" > /tmp/impl06c-comp-all.log
grep -F -f /tmp/impl06c-tablets.txt /tmp/impl06c-comp-all.log > /tmp/impl06c-compaction-words.log
cp /tmp/impl06c-compaction-words.log ./07-compaction-words.out.txt
wc -l /tmp/impl06c-compaction-words.log

echo "--- cumulative 成功行:"
grep -c 'succeed to do cumulative' /tmp/impl06c-compaction-words.log || true
grep 'succeed to do cumulative' /tmp/impl06c-compaction-words.log | grep -o 'elapsed time=[0-9.]*s' | awk -F= '{s+=$2} END {printf "N=%d total_elapsed=%.2fs\n", NR, s}'
echo "--- base 成功行（含 ordered data base）:"
grep -c 'succeed to do.*base compaction' /tmp/impl06c-compaction-words.log || true
grep 'succeed to do.*base compaction' /tmp/impl06c-compaction-words.log | grep -o 'elapsed time=[0-9.]*s' | awk -F= '{s+=$2} END {printf "N=%d total_elapsed=%.2fs\n", NR, s}'
echo "--- 失败行:"
grep -c 'failed' /tmp/impl06c-compaction-words.log || echo 0
echo "--- compaction 首末时刻（本表）:"
grep -oE 'I20261025 [0-9:.]+' /tmp/impl06c-compaction-words.log | head -1
grep -oE 'I20261025 [0-9:.]+' /tmp/impl06c-compaction-words.log | tail -1
echo "--- 输入行数/输出行数合计（merged 应=0，DUPLICATE 表）:"
grep -oE 'merged_row_num=[0-9]+' /tmp/impl06c-compaction-words.log | awk -F= '{s+=$2} END {print "merged_total=" s}'
grep -oE 'input_row_num=[0-9]+' /tmp/impl06c-compaction-words.log | awk -F= '{s+=$2} END {print "input_rows_total=" s}'
grep -oE 'output_row_num=[0-9]+' /tmp/impl06c-compaction-words.log | awk -F= '{s+=$2} END {print "output_rows_total=" s}'

echo
echo "=== [6-fix] BE 关键 compaction/内存配置取值 ==="
docker exec eco-doris-be sh -c "curl -s localhost:8040/api/show_config" 2>/dev/null > /tmp/impl06c-be-config.json
python3 - <<'PY'
import json
with open('/tmp/impl06c-be-config.json') as f:
    cfg = json.load(f)
rows = cfg.get('rows') or cfg.get('data',{}).get('rows') or []
keys = ['compaction_memory_bytes_limit','memtable_flush_running_count_limit','memtable_memory_limit',
        'compaction_max_rowset_count','compaction_task_num_per_disk','cumulative_compaction_max_deltas',
        'base_compaction_max_compaction_score','compaction_thread_pool_num','compaction_queue_size',
        'memory_limit_hard_bytes','mem_limit','load_process_max_memory_limit_percent',
        'load_process_max_memory_number_bytes','compaction_promotion_size_mbytes','write_buffer_size',
        'streaming_load_max_mb','compaction_min_size_mbytes','compaction_bandwidth_limit']
for r in rows:
    name, val = r[0], r[1]
    if name in keys:
        print(f"{name} = {val}")
PY
echo "=== 07B DONE ==="
date -u '+end: %Y-%m-%d %H:%M:%S UTC'
