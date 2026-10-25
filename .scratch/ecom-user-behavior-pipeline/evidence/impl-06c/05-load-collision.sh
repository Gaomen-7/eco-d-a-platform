#!/usr/bin/env bash
# impl-06c · 第 5 步：20 批 × 600,000 行（每批 ≈1.0247 GB，总 ≈20.49 GB 原始 CSV）对撞灌数
# 每批：生成(hex 850B) → docker cp → Stream Load（label=coll_chunk_NN）
# 输出：逐批 TSV 时间/字节/耗时/Status + 全响应落 05-chunk-NN-resp/
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"
EV="$(pwd)/.scratch/ecom-user-behavior-pipeline/evidence/impl-06c"
RESP="$EV/05-chunk-resp"
mkdir -p "$RESP"

ROWS=600000
HEX_BYTES=850
CHUNKS=20
CSV=/tmp/coll_chunk.csv
TS=$(date -u +%Y%m%dT%H%M%SZ)

echo "params: chunks=$CHUNKS rows/chunk=$ROWS hex_bytes=$HEX_BYTES label_prefix=coll_chunk_${TS}"
echo "ts_utc	chunk	raw_bytes	gen_s	cp_s	load_s	status	num_rows	msg"
date -u '+T_LOAD_START: %Y-%m-%d %H:%M:%S UTC'

for i in $(seq -w 1 $CHUNKS); do
  t0=$(date +%s)
  # 生成：块状 urandom 加速（50k 行一块）
  python3 - "$ROWS" "$HEX_BYTES" "$CSV" "$i" <<'PY'
import sys, os
rows, nbytes, path, ci = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4]
base = (int(ci) - 1) * rows
with open(path, 'wb') as f:
    for start in range(0, rows, 50000):
        n = min(50000, rows - start)
        hexs = os.urandom(n * nbytes).hex().encode()
        out = bytearray()
        for j in range(n):
            rid = base + start + j + 1
            out += b'%d,' % rid
            out += hexs[j*(nbytes*2):(j+1)*(nbytes*2)]
            out += b'\n'
        f.write(out)
PY
  t1=$(date +%s)
  RAW=$(stat -c %s "$CSV")

  docker cp "$CSV" eco-doris-be:/tmp/coll_chunk.csv
  t2=$(date +%s)

  RESP_JSON=$(docker exec eco-doris-be sh -c "curl -s -u root: \
    -H 'Expect: 100-continue' \
    -H 'label: coll_chunk_${TS}_${i}' \
    -H 'column_separator: ,' \
    -H 'columns: id, payload' \
    -H 'max_filter_ratio: 0' \
    -T /tmp/coll_chunk.csv \
    http://localhost:8040/api/ecom_collision/compaction_probe/_stream_load" 2>/dev/null)
  t3=$(date +%s)

  echo "$RESP_JSON" > "$RESP/chunk-${i}.json"
  STATUS=$(echo "$RESP_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('Status','PARSE_FAIL'))" 2>/dev/null || echo PARSE_FAIL)
  NROWS=$(echo "$RESP_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('NumberTotalRows','NA'))" 2>/dev/null || echo NA)
  MSG=$(echo "$RESP_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);print((d.get('Message','') or '')[:120])" 2>/dev/null || echo NA)

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -u '+%H:%M:%S')" "$i" "$RAW" "$((t1-t0))" "$((t2-t1))" "$((t3-t2))" "$STATUS" "$NROWS" "$MSG"

  if [ "$STATUS" != "Success" ]; then
    echo "!! chunk $i FAILED ($STATUS) — 中止后续批次（观测器继续记录现场）"
    break
  fi
done

docker exec eco-doris-be sh -c "rm -f /tmp/coll_chunk.csv"
rm -f "$CSV"
date -u '+T_LOAD_END: %Y-%m-%d %H:%M:%S UTC'
