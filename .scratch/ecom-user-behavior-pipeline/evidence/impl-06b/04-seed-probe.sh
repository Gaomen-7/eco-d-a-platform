#!/usr/bin/env bash
# impl-06b · 第 4 步：生成 ~1 GB 随机 hex 探针数据 + Stream Load + 灌数后测量
# 产物：stdout 全量进 evidence/impl-06b/04-seed-probe.out.txt
# 说明：payload 用 os.urandom 的 hex（不可压缩），保证磁盘占用≈原始字节数，
#       使「DROP 后磁盘回落」在 du 上清晰可测。执行面：stream load 直发 BE（容器内 curl）。
set -euo pipefail

ROWS=600000
HEX_BYTES=850   # 每行 payload = 1700 个 hex 字符
CSV=/tmp/reclaim_probe.csv
TS=$(date -u +%Y%m%dT%H%M%SZ)
LABEL="reclaim_probe_${TS}"

echo "=== [1] 生成 ${ROWS} 行 × 1700 hex 字符（随机不可压缩） CSV ==="
date -u '+gen start: %Y-%m-%d %H:%M:%S'
python3 - "$ROWS" "$HEX_BYTES" "$CSV" <<'PY'
import sys, os
rows, nbytes, path = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
with open(path, 'wb') as f:
    for i in range(rows):
        f.write(b'%d,%s\n' % (i + 1, os.urandom(nbytes).hex().encode()))
PY
date -u '+gen done : %Y-%m-%d %H:%M:%S'
ls -l "$CSV" | awk '{printf "raw bytes: %d\n", $5}'
echo

echo "=== [2] docker cp 进 doris-be（stream load 直发 BE，容器内 curl） ==="
docker cp "$CSV" eco-doris-be:/tmp/reclaim_probe.csv
docker exec eco-doris-be sh -c "ls -l /tmp/reclaim_probe.csv | awk '{printf \"in-be bytes: %s\n\", \$5}'"
echo

echo "=== [3] 灌数前 du 基线 ==="
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage"
echo

echo "=== [4] Stream Load（label=$LABEL） ==="
date -u '+load start: %Y-%m-%d %H:%M:%S'
docker exec eco-doris-be sh -c "curl -s -u root: \
  -H 'Expect: 100-continue' \
  -H 'label: ${LABEL}' \
  -H 'column_separator: ,' \
  -H 'columns: id, payload' \
  -H 'max_filter_ratio: 0' \
  -T /tmp/reclaim_probe.csv \
  http://localhost:8040/api/ecom_rehearsal/reclaim_probe/_stream_load"
echo
date -u '+load done : %Y-%m-%d %H:%M:%S'
echo

echo "=== [5] 行数核对 + SHOW DATA + 灌数后 du ==="
dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }
dq -e "SELECT COUNT(*) AS probe_rows FROM ecom_rehearsal.reclaim_probe;"
dq -e "SHOW DATA FROM ecom_rehearsal;"
dq -e "SHOW DATA;"
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage"
echo

echo "=== [6] 等 du 稳定（30 s × 3 采样，防 segment 写入/compaction 未落定） ==="
for i in 1 2 3; do
  sleep 30
  printf 'sample %d  %s  ' "$i" "$(date -u '+%H:%M:%S')"
  docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage | cut -f1"
done
echo

echo "=== [7] 清理 BE 容器内临时 CSV（volume 外，不影响测量） ==="
docker exec eco-doris-be sh -c "rm -f /tmp/reclaim_probe.csv && echo removed"
wsl_tmp_cleanup=$(rm -f "$CSV" && echo removed-host-copy)
echo "host tmp: $wsl_tmp_cleanup"
echo "PRE_DROP_DU_KB marker below (收口口径：以 [6] 末次采样为准)"
