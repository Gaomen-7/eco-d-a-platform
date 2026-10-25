#!/usr/bin/env bash
# impl-06c · 07c：BE 配置值解析（07b [6-fix] 的修复版，脱离 heredoc）
set -uo pipefail
cd "$(dirname "$0")"
python3 - /tmp/impl06c-be-config.json <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    cfg = json.load(f)
rows = cfg if isinstance(cfg, list) else cfg.get('rows', [])
keys = ['compaction_memory_bytes_limit','memtable_flush_running_count_limit','memtable_memory_limit',
        'compaction_max_rowset_count','compaction_task_num_per_disk','cumulative_compaction_max_deltas',
        'base_compaction_max_compaction_score','memory_limit_hard_bytes','mem_limit',
        'load_process_max_memory_limit_percent','load_process_max_memory_number_bytes',
        'compaction_min_size_mbytes','compaction_promotion_size_mbytes','write_buffer_size',
        'memtable_flush_thread_num','flush_thread_num_per_store','compaction_bandwidth_limit']
for r in rows:
    if r[0] in keys:
        print(f'{r[0]} = {r[2]} (mutable={r[3]})')
PY
echo "=== 07C DONE ==="
