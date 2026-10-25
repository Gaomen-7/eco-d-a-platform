#!/usr/bin/env bash
# impl-06b · 第 5 步：T0 DROP → recycle bin → FE purge → BE GC → 磁盘回落 全时间线观测
# 产物：stdout 全量进 evidence/impl-06b/05-drop-and-observe.out.txt
# 终止：du 回落到灌数前基线 +20 MB 内（SUCCESS）或 elapsed > 5400 s（TIMEOUT，人工触发点另测）
# 注意：本脚本自身先采灌数后 du 作为 PRE_DROP 基线（收口口径），再执行 DROP。
set -uo pipefail

DQ() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }
BE_DU() { docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage 2>/dev/null | cut -f1"; }
TRASH_DU() { docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/trash 2>/dev/null | cut -f1 || echo 0"; }
BE_METRIC() { docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics | grep -E 'doris_be_disks_(avail_capacity|trash_used_capacity)\{path' "; }
BIN_ROWS() { DQ -e "SHOW CATALOG RECYCLE BIN;" 2>/dev/null | grep -E "^Table\s+reclaim_probe|^Partition\s+p" | grep -v rollback; }

echo "=== [0] 灌数后（DROP 前）基线 ==="
T_PRE=$(date -u '+%Y-%m-%d %H:%M:%S')
PRE_DROP_KB=$(BE_DU)
echo "T_PRE: $T_PRE  PRE_DROP_DU_KB: ${PRE_DROP_KB}"
BE_METRIC
echo

echo "=== [T0] DROP TABLE ecom_rehearsal.reclaim_probe（非 force） ==="
T0_EPOCH=$(date -u +%s)
T0=$(date -u '+%Y-%m-%d %H:%M:%S')
echo "T0: $T0 (epoch ${T0_EPOCH})"
DQ -e "DROP TABLE ecom_rehearsal.reclaim_probe;"
echo

echo "=== [T0+x] DROP 后立即采样（预期：bin 立即可见，磁盘不动） ==="
sleep 5
BIN_ROWS || echo "(bin 中未见 reclaim_probe——异常，记录)"
echo "du: $(BE_DU) KB (pre-drop ${PRE_DROP_KB} KB)"
echo

echo "=== 轮询观测（每 ≈30 s：du / trash / metrics / recycle bin 条目） ==="
BIN_GONE_AT=""
END_REASON=""
LAST_KB="$PRE_DROP_KB"
while :; do
  NOW_EPOCH=$(date -u +%s)
  ELAPSED=$((NOW_EPOCH - T0_EPOCH))
  KB=$(BE_DU)
  TR=$(TRASH_DU)
  AVAIL=$(docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics" | grep 'doris_be_disks_avail_capacity{path' | awk '{print $2}')
  TRASH_METRIC=$(docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics" | grep 'doris_be_disks_trash_used_capacity{path' | awk '{print $2}')
  if BIN_ROWS > /tmp/06b_bin_rows.txt 2>/dev/null && [ -s /tmp/06b_bin_rows.txt ]; then
    BIN_STATE="in-bin"
  else
    BIN_STATE="gone"
  fi
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] elapsed=${ELAPSED}s du=${KB}KB trash_dir=${TR}KB trash_metric=${TRASH_METRIC:-?} avail=${AVAIL:-?} bin=${BIN_STATE}"

  if [ "$BIN_STATE" = "gone" ] && [ -z "$BIN_GONE_AT" ]; then
    BIN_GONE_AT="$(date -u '+%Y-%m-%d %H:%M:%S') (elapsed ${ELAPSED}s)"
    echo ">>> BIN_ENTRY_GONE (FE purge): $BIN_GONE_AT"
  fi

  if [ "${KB:-0}" -le $((PRE_DROP_KB + 20480)) ]; then
    END_REASON="SUCCESS: du 回落到基线+20MB 内（磁盘真实释放）"
    break
  fi
  if [ "$ELAPSED" -gt 5400 ]; then
    END_REASON="TIMEOUT: 5400s 内自然路径未回落"
    break
  fi
  LAST_KB="$KB"
  sleep 30
done

T_END_EPOCH=$(date -u +%s)
T_END=$(date -u '+%Y-%m-%d %H:%M:%S')
ELAPSED=$((T_END_EPOCH - T0_EPOCH))
echo
echo "=== 收口 ==="
echo "T0           : $T0"
echo "BIN_ENTRY_GONE: ${BIN_GONE_AT:-未观测到}"
echo "T_END        : $T_END (elapsed ${ELAPSED}s)"
echo "END_REASON   : $END_REASON"
echo "du final     : $(BE_DU) KB (pre-drop ${PRE_DROP_KB} KB, peak-recorded ${LAST_KB} KB)"
BE_METRIC
echo
echo "=== recycle bin 终态（含 DropTime 对照） ==="
DQ -e "SHOW CATALOG RECYCLE BIN;"
