#!/usr/bin/env bash
# impl-06c · 第 6 步：灌数结束后的 compaction 沉降观测
# 沉降判据（客观三连，全部满足连续 5 个 60s tick）：
#   ① /api/compaction/run_status 三个类目（Base/Cumulative/Full）均无 running 条目
#   ② compaction 计数器（base/cumu engine_requests_total）连续不变
#   ③ du(data) 连续不变（±1%）
# 输出：06-settle-watch.out.txt（逐 tick 一行）+ 沉降时刻 T_SETTLED
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

STREAK=0
NEED=5
PREV_BASE=0; PREV_CUMU=0; PREV_DU=0
echo "tick_ts	base_run	cumu_run	du_data_kb	score	size_score	running_base	running_cumu	running_full	streak"
for i in $(seq 1 120); do   # 上限 120 tick = 2h
  TS=$(date -u '+%H:%M:%S')
  M=$(docker exec eco-doris-be sh -c "curl -s -m 5 localhost:8040/metrics" 2>/dev/null)
  RUN=$(docker exec eco-doris-be sh -c "curl -s -m 5 'localhost:8040/api/compaction/run_status'" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);b=d.get('BaseCompaction',{});c=d.get('CumulativeCompaction',{});f=d.get('FullCompaction',{});print(len(list(b.values())[0]),len(list(c.values())[0]),sum(len(v) for v in f.values()))" 2>/dev/null || echo "NA NA NA")
  RB=$(echo $RUN | cut -d' ' -f1); RC=$(echo $RUN | cut -d' ' -f2); RF=$(echo $RUN | cut -d' ' -f3)
  BASE=$(echo "$M" | awk '/^doris_be_engine_requests_total\{status="total",type="base_compaction"/{print $2}')
  CUMU=$(echo "$M" | awk '/^doris_be_engine_requests_total\{status="total",type="cumulative_compaction"/{print $2}')
  SCORE=$(echo "$M" | awk '/^doris_be_disks_compaction_score/{print $NF}')
  SSCORE=$(echo "$M" | awk '/^doris_be_tablet_size_based_max_compaction_score /{print $2}')
  DU=$(docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/data 2>/dev/null" 2>/dev/null | cut -f1)

  IDLE=0
  [ "$RB" = "0" ] && [ "$RC" = "0" ] && [ "$RF" = "0" ] && IDLE=1
  STABLE=0
  if [ "$BASE" = "$PREV_BASE" ] && [ "$CUMU" = "$PREV_CUMU" ]; then STABLE=1; fi
  DUSTABLE=0
  if [ "$PREV_DU" -gt 0 ] 2>/dev/null && [ "$DU" -gt 0 ] 2>/dev/null; then
    DIFF=$(( DU - PREV_DU )); ADIFF=${DIFF#-}
    [ "$ADIFF" -le $(( PREV_DU / 100 )) ] && DUSTABLE=1
  fi

  if [ $IDLE -eq 1 ] && [ $STABLE -eq 1 ] && [ $DUSTABLE -eq 1 ]; then
    STREAK=$((STREAK+1))
  else
    STREAK=0
  fi
  echo "$TS	$BASE	$CUMU	$DU	$SCORE	$SSCORE	$RB	$RC	$RF	$STREAK"
  PREV_BASE=$BASE; PREV_CUMU=$CUMU; PREV_DU=$DU

  if [ "$STREAK" -ge "$NEED" ]; then
    date -u '+T_SETTLED: %Y-%m-%d %H:%M:%S UTC'
    echo "settled after $i ticks"
    exit 0
  fi
  sleep 60
done
echo "!! 120 ticks (2h) 上限到仍未满足沉降判据 —— 按未沉降登记"
date -u '+T_SETTLE_TIMEOUT: %Y-%m-%d %H:%M:%S UTC'
