#!/usr/bin/env bash
# impl-06a 演练 · 第 4.5 步：删除记录取证补强
# ① SHOW DYNAMIC PARTITION TABLES 全行（不截列，核对 LastDropPartitionMsg 原文）
# ② FE master 日志中 DynamicPartitionScheduler 在 12:27:50 前后的逐字记录
# 产物：stdout 全量进 evidence/impl-06a/04b-drop-msg-and-fe-log.out.txt
set -uo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] SHOW DYNAMIC PARTITION TABLES（全列原文） ==="
dq -e "SHOW DYNAMIC PARTITION TABLES FROM ecom_rehearsal;"
echo

echo "=== [2] FE 日志：DynamicPartitionScheduler 12:27 前后逐字 ==="
docker compose exec -T doris-fe bash -c "grep -h 'DynamicPartitionScheduler' /opt/apache-doris/fe/log/fe.log | grep '2026-10-25 12:2[6-9]' | tail -40"
echo

echo "=== [3] FE 日志：12:27:50 前后 drop 相关（含分区名） ==="
docker compose exec -T doris-fe bash -c "grep -h -E 'drop|Drop' /opt/apache-doris/fe/log/fe.log | grep '2026-10-25 12:2[6-9]' | grep -i -E 'rehearsal|partition' | tail -30"
