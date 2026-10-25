#!/usr/bin/env bash
# impl-06b · 第 5b 步：续测——05 号脚本判据 bug（终止阈值误用灌数后基线，24 s 假 SUCCESS）后重新观测
# 真实终止：du 回落到【灌数前】基线 96,768 KB +20 MB 内（磁盘真实释放）或 elapsed(自 T0) > 5400 s
# T0（权威）＝2026-10-25 12:57:57 UTC（05 号凭证），bin 条目 DropTime 12:58:02（05 号凭证已录）
set -uo pipefail

DQ() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }
BE_DU() { docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage 2>/dev/null | cut -f1"; }
TRASH_DU() { docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/trash 2>/dev/null | cut -f1"; echo 0; }
BE_METRIC() { docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics | grep -E 'doris_be_disks_(avail_capacity|trash_used_capacity)\{path'"; }

# 权威 T0（epoch）：2026-10-25 12:57:57 UTC = $(date -u -d '2026-10-25 12:57:57' +%s)
T0_EPOCH=$(date -u -d '2026-10-25 12:57:57' +%s)
PRE_LOAD_KB=96768          # 04 号凭证 [3]：灌数前 du 基线
GONE_THRESHOLD=$((PRE_LOAD_KB + 20480))   # 回落到灌数前基线 +20MB 内＝磁盘真实释放

echo "=== 5b 续测起：当前状态快照 ==="
date -u '+UTC now: %Y-%m-%d %H:%M:%S (T0 was 12:57:57)'
echo "PRE_LOAD_KB=${PRE_LOAD_KB}  GONE_THRESHOLD(KB)=${GONE_THRESHOLD}"
echo "du now: $(BE_DU) KB"
BE_METRIC
echo
echo "=== 轮询观测（每 ≈35 s＝30 s sleep＋查询开销：du / trash / metrics / recycle bin 条目） ==="
BIN_GONE_AT=""
END_REASON=""
while :; do
  NOW_EPOCH=$(date -u +%s)
  ELAPSED=$((NOW_EPOCH - T0_EPOCH))
  KB=$(BE_DU)
  TR=$(TRASH_DU | tail -1)
  TRASH_METRIC=$(docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics" | grep 'doris_be_disks_trash_used_capacity{path' | awk '{print $2}')
  if DQ -e "SHOW CATALOG RECYCLE BIN;" 2>/dev/null | grep -q "^Table\s*reclaim_probe"; then
    BIN_STATE="in-bin"
  else
    BIN_STATE="gone"
  fi
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] elapsed=${ELAPSED}s du=${KB}KB trash_dir=${TR}KB trash_metric=${TRASH_METRIC:-?} bin=${BIN_STATE}"

  if [ "$BIN_STATE" = "gone" ] && [ -z "$BIN_GONE_AT" ]; then
    BIN_GONE_AT="$(date -u '+%Y-%m-%d %H:%M:%S') (elapsed ${ELAPSED}s)"
    echo ">>> BIN_ENTRY_GONE (FE purge): $BIN_GONE_AT"
  fi

  if [ "${KB:-999999999}" -le "$GONE_THRESHOLD" ]; then
    END_REASON="SUCCESS: du 回落到灌数前基线+20MB 内（磁盘真实释放）"
    break
  fi
  if [ "$ELAPSED" -gt 5400 ]; then
    END_REASON="TIMEOUT: T0 起 5400s 内自然路径未回落（触发缓解方案 §5 人工触发点另测）"
    break
  fi
  sleep 30
done

T_END=$(date -u '+%Y-%m-%d %H:%M:%S')
ELAPSED=$(($(date -u +%s) - T0_EPOCH))
echo
echo "=== 5b 收口 ==="
echo "T0(权威)     : 2026-10-25 12:57:57 UTC (epoch ${T0_EPOCH})"
echo "BIN_ENTRY_GONE: ${BIN_GONE_AT:-未观测到}"
echo "T_END        : $T_END (自 T0 elapsed ${ELAPSED}s)"
echo "END_REASON   : $END_REASON"
echo "du final     : $(BE_DU) KB (灌数前基线 ${PRE_LOAD_KB} KB)"
BE_METRIC
