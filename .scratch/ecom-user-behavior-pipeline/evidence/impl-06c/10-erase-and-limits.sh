#!/usr/bin/env bash
# impl-06c · 10：收尾取证——BE erase 逐字 + load 窗口内存限流/OOM 痕迹检索
set -uo pipefail
cd "$(dirname "$0")"
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"

echo "=== [1] 本表 8 tablet 的 BE erase/trash 逐字行 ==="
grep -F -f /tmp/impl06c-tablets.txt /tmp/impl06c-be-erase-all.log | tail -12
echo
echo "=== [2] BE 删除动作时刻（delete tablet dir 行，15:00 前后） ==="
docker exec eco-doris-be sh -c "grep -a 'delete tablet dir' /opt/apache-doris/be/log/be.INFO 2>/dev/null | tail -10"
echo
echo "=== [3] load+compaction 窗口（13:49–15:01）内存限流/OOM 痕迹检索 ==="
echo "--- 'memory limit' / 'MemLimit' / 'MemTracker' 告警:"
docker exec eco-doris-be sh -c "grep -aE 'memory limit|MemLimit|MemoryLimitExceed|memory exceed' /opt/apache-doris/be/log/be.INFO 2>/dev/null | wc -l"
echo "--- '-235'（load 内存超限错误码）:"
docker exec eco-doris-be sh -c "grep -ac '\-235' /opt/apache-doris/be/log/be.INFO 2>/dev/null"
echo "--- 'oom' / 'OOM' / 'killed' 相关:"
docker exec eco-doris-be sh -c "grep -acE 'oom|OOM|OutOfMemory' /opt/apache-doris/be/log/be.INFO 2>/dev/null"
echo "--- 'memtable flush' 限流/等待痕迹（memtable_flush_running_count_limit=2）:"
docker exec eco-doris-be sh -c "grep -aiE 'flush.*limit|limit.*flush|memtable.*wait|submit.*memtable' /opt/apache-doris/be/log/be.INFO 2>/dev/null | tail -5"
echo "--- BE 容器 OOMKilled 状态复核:"
docker inspect eco-doris-be --format 'OOMKilled={{.State.OOMKilled}} Status={{.State.Status}} RestartCount={{.RestartCount}}'
echo
echo "=== [4] memtable flush 计数终值 ==="
docker exec eco-doris-be sh -c "curl -s localhost:8040/metrics | grep -E '^doris_be_memtable'"
echo
echo "=== [5] 观测器终态采样 ==="
tail -2 04-observer.log
echo "=== 10 DONE ==="
date -u '+end: %Y-%m-%d %H:%M:%S UTC'
