#!/usr/bin/env bash
# impl-06c · 08b：回收站可见性补取证（08 号误用 SHOW RECYCLE BIN，正确形态是 SHOW CATALOG RECYCLE BIN——06b 同款）
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"
date -u '+now: %Y-%m-%d %H:%M:%S UTC (T0_DROP was 14:48:30 UTC)'
docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch -e 'SHOW CATALOG RECYCLE BIN;' 2>/dev/null
echo "=== 08B DONE ==="
