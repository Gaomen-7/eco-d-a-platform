#!/usr/bin/env bash
# impl-06c · 10b：限流/OOM 痕迹按时间窗过滤重查（10 号 [3] 是全量计数，含历史会话噪声）
set -uo pipefail
cd "$(dirname "$0")"

echo "=== load+compaction 窗口 I20261025 [13:49–15:01] 内的痕迹 ==="
docker exec eco-doris-be sh -c "grep -aE 'I20261025 (1[34]|15):' /opt/apache-doris/be/log/be.INFO 2>/dev/null" > /tmp/impl06c-window.log
wc -l /tmp/impl06c-window.log

echo "--- 窗口内 'reached hard limit'（memtable_memory_limiter）:"
grep -c 'reached hard limit' /tmp/impl06c-window.log || echo 0
echo "--- 窗口内 'reached hard limit' 首末时刻:"
grep 'reached hard limit' /tmp/impl06c-window.log | head -1 | grep -oE 'I20261025 [0-9:.]+' || true
grep 'reached hard limit' /tmp/impl06c-window.log | tail -1 | grep -oE 'I20261025 [0-9:.]+' || true
echo "--- 窗口内 -235（需人工看上下文，可能含版本号等）:"
grep -aE '\-235' /tmp/impl06c-window.log | grep -vE '2\.235|0\.235' | head -5
echo "--- 窗口内 OOM/OutOfMemory:"
grep -aE 'oom|OOM|OutOfMemory' /tmp/impl06c-window.log | grep -viE 'vlog|debug' | head -5
echo "--- 窗口内 'memory limit' 逐条样本（前 5）:"
grep -aiE 'memory limit|MemLimit' /tmp/impl06c-window.log | head -5
echo
echo "--- 窗口内 memtable_memory_limiter 的非零 load mem 行（真限流形态）:"
grep 'reached hard limit' /tmp/impl06c-window.log | grep -v 'load mem: 0,' | head -8
echo "(以上为空=全程 load mem 0，未真实触限)"
echo "=== 10B DONE ==="
