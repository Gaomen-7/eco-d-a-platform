#!/usr/bin/env bash
# impl-06c · 后台观测器 v3（终版）：每 15 s 采样 BE 内存/compaction score/flush 队列/du，直到 STOP 文件出现
# 输出：04-observer.out.txt（TSV，一行一样本）；峰值形态以此为权威
# v1 用了 4.1.5 不存在的 tablet_max_compaction_score（NA 列）；v2 修正指标名后被热编辑打断；
# v3＝指标名修正 + flush 线程池 queue_size 精确匹配（排除 max_queue_size），为权威版本
set -uo pipefail
STOPFILE="/tmp/impl06c-observer-stop"
rm -f "$STOPFILE"

printf 'ts\tbe_mem\tbe_pct\tfe_mem\tfe_pct\talloc_mb\tscore\tsize_score\tflush_q\tflush_act\tdb_kb\tdata_kb\tbase_run\tcumu_run\tpswpin\tpswpout\n'

while [ ! -f "$STOPFILE" ]; do
  TS=$(date -u '+%H:%M:%S')
  STATS=$(docker stats --no-stream --format '{{.Name}}|{{.MemUsage}}|{{.MemPerc}}' 2>/dev/null | grep -E 'doris-be|doris-fe')
  BE=$(echo "$STATS" | grep doris-be | cut -d'|' -f2,3 | tr '|' '\t')
  FE=$(echo "$STATS" | grep doris-fe | cut -d'|' -f2,3 | tr '|' '\t')
  M=$(docker exec eco-doris-be sh -c "curl -s -m 5 localhost:8040/metrics" 2>/dev/null)
  ALLOC=$(echo "$M" | awk '/^doris_be_memory_allocated_bytes /{print $2}')
  SCORE=$(echo "$M" | awk '/^doris_be_disks_compaction_score/{print $NF}')
  SSCORE=$(echo "$M" | awk '/^doris_be_tablet_size_based_max_compaction_score /{print $2}')
  FQ=$(echo "$M" | awk '/^doris_be_thread_pool_queue_size\{.*MemTableFlushThreadPool/{print $NF}')
  FA=$(echo "$M" | awk '/^doris_be_thread_pool_active_threads\{.*MemTableFlushThreadPool/{print $NF}')
  DB_KB=$(docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage 2>/dev/null" 2>/dev/null | cut -f1)
  DATA_KB=$(docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/data 2>/dev/null" 2>/dev/null | cut -f1)
  BASE_RUN=$(echo "$M" | awk '/^doris_be_engine_requests_total\{status="total",type="base_compaction"/{print $2}')
  CUMU_RUN=$(echo "$M" | awk '/^doris_be_engine_requests_total\{status="total",type="cumulative_compaction"/{print $2}')
  PSI_IN=$(echo "$M" | awk '/^doris_be_memory_pswpin /{print $2}')
  PSI_OUT=$(echo "$M" | awk '/^doris_be_memory_pswpout /{print $2}')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$TS" "$BE" "$FE" "${ALLOC:-NA}" "${SCORE:-NA}" "${SSCORE:-NA}" "${FQ:-NA}" "${FA:-NA}" \
    "${DB_KB:-NA}" "${DATA_KB:-NA}" "${BASE_RUN:-NA}" "${CUMU_RUN:-NA}" "${PSI_IN:-NA}" "${PSI_OUT:-NA}"
  sleep 15
done
echo "observer stopped at $(date -u '+%H:%M:%S')"
