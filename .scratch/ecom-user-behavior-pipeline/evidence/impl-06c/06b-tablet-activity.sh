#!/usr/bin/env bash
# impl-06c · 沉降判定辅助：本表 tablet 的 compaction 日志活动检查
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch -e 'SHOW TABLETS FROM ecom_collision.compaction_probe;' 2>/dev/null | awk '{print $1}' | tail -n +2 > /tmp/impl06c-tablets.txt
echo "tablets:"; cat /tmp/impl06c-tablets.txt

docker exec eco-doris-be sh -c "grep -a 'succeed to do' /opt/apache-doris/be/log/be.INFO 2>/dev/null" > /tmp/impl06c-succeed-all.log
grep -F -f /tmp/impl06c-tablets.txt /tmp/impl06c-succeed-all.log > /tmp/impl06c-succeed-tablets.log
echo "---- total succeed lines (our tablets): $(wc -l < /tmp/impl06c-succeed-tablets.log)"
echo "---- last 3 lines (truncated 260c):"
tail -3 /tmp/impl06c-succeed-tablets.log | cut -c1-260
echo "---- last succeed timestamp:"
tail -1 /tmp/impl06c-succeed-tablets.log | grep -oE 'I[0-9]{8} [0-9:.]+' || true
