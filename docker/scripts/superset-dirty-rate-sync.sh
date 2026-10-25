#!/usr/bin/env bash
# 脏率基线同步 —— impl-04 票面 ⑤（脏率面板的「基线」从哪来）。
#
# 面板读 ecom_ads.ads_window_metric 的 metric_name='dlq_total_offset' 行；
# 这个 metric 没有任何 Flink 作业写它（数据面词表里不存在）——它是
# **面板专用的镜像行**，由本脚本从两个来源同步：
#   分子基线 = artifacts/verify/dirty-rate-baseline.json 的 dlq_offset
#              （= verify-fault 输入冻结时的 DLQ 快照，票面 ⑤ 逐字）；
#   分子当前 = Kafka DLQ topic 总 offset（kafka-get-offsets.sh 第三列之和，
#              与 verify_runner._topic_total_offset 同一口径）；
#   分母     = 输入 topic 总 offset（同口径）。
# 面板显示「当前值 vs 基线 Δ」＝ dlq_count_delta / total_events（票面 ⑤ 分式，
# 分子只含真脏；重复不进分子——重复走幂等吸收，不产生 DLQ 行）。
#
# 写入形态：单行 upsert 到 ads_window_metric（UNIQUE KEY 覆盖写，幂等）。
# 不动 verify_runner / fault —— 基线文件的写者只有 verify-fault（票面 ⑤）。
set -uo pipefail

export MSYS_NO_PATHCONV=1  # 同 superset-provision.sh：防 Git-Bash 路径改写

BASELINE_FILE="${BASELINE_FILE:-artifacts/verify/dirty-rate-baseline.json}"
DORIS_QUERY_HOST="${DORIS_QUERY_HOST:-127.0.0.1}"
DORIS_QUERY_PORT="${DORIS_QUERY_PORT:-9030}"

step() { echo "[dirty-rate-sync] ▶ $*"; }
fail() { echo "[dirty-rate-sync] ✗ $*" >&2; exit 1; }

# ── ① 基线文件在位且可解析 ─────────────────────────────────────────────
# ★ 解析器用 ${PYTHON:-python3}（impl-05 实测 2026-10-25）：WSL 宿主只有 python3；
#   旧写法 `python` command not found 时 stderr 被 2>/dev/null 吞掉 ⇒ 误报
#   「基线文件不可解析（缺 dlq_offset 字段？）」——文件其实完好，是解释器缺席。
step "读基线 ${BASELINE_FILE}"
[ -f "$BASELINE_FILE" ] || fail "基线文件不存在——先跑 make verify-fault（首跑落盘基线）"
baseline_dlq="$("${PYTHON:-python3}" -c "import json,sys;print(json.load(open(sys.argv[1]))['dlq_offset'])" "$BASELINE_FILE" 2>/dev/null)" \
  || fail "基线文件不可解析（缺 dlq_offset 字段？）"
baseline_input="$("${PYTHON:-python3}" -c "import json,sys;print(json.load(open(sys.argv[1]))['input_events'])" "$BASELINE_FILE" 2>/dev/null)" \
  || fail "基线文件不可解析（缺 input_events 字段？）"
echo "[dirty-rate-sync] 基线：dlq_offset=${baseline_dlq} input_events=${baseline_input}"

# ── ② Kafka 两侧总 offset（与 verify_runner 同口径）───────────────────
topic_total_offset() {
  local topic="$1"
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$topic" \
    | awk -F: 'NF==3 && $3 ~ /^[0-9]+$/ { sum += $3 } END { print sum+0 }'
}

step "读 Kafka DLQ 总 offset"
cur_dlq="$(topic_total_offset 'ecom.behavior.event.v1.dlq')"
[ -n "$cur_dlq" ] && [ "$cur_dlq" -ge 0 ] 2>/dev/null || fail "DLQ 总 offset 取不到：${cur_dlq}"
step "读 Kafka 输入 topic 总 offset"
cur_input="$(topic_total_offset 'ecom.behavior.event.v1')"
[ -n "$cur_input" ] && [ "$cur_input" -ge 0 ] 2>/dev/null || fail "输入总 offset 取不到：${cur_input}"
echo "[dirty-rate-sync] 当前：dlq=${cur_dlq} input=${cur_input}"

# ── ③ Δ 与保护 ────────────────────────────────────────────────────────
delta=$(( cur_dlq - baseline_dlq ))
[ "$delta" -lt 0 ] && fail "Δ 为负（dlq=${cur_dlq} < 基线 ${baseline_dlq}）——topic 被重建过，基线失效，重跑 make verify-fault 立新基线"
[ "$cur_input" -eq 0 ] && fail "输入事件数为 0，脏率无定义"

# ── ④ 单行 upsert（UNIQUE KEY 覆盖写，幂等）────────────────────────────
# 实机证伪：window_start 用固定 2020-01-01 ⇒ `no partition for this tuple`
# （表是 dynamic_partition DAY，历史分区只建到 -30，2020 在范围外）。改用当天。
ANCHOR_DATE="$(date +%Y-%m-%d)"
step "写入 ads_window_metric（dlq_total_offset / dlq_count_delta，anchor=${ANCHOR_DATE}）"
write_row() {
  docker compose exec -T doris-fe mysql -h"${DORIS_QUERY_HOST}" -P"${DORIS_QUERY_PORT}" -uroot ecom_ads -e "$1"
}
write_row "INSERT INTO ads_window_metric (window_start, metric_name, dim_key, metric_value, revision, is_settled) VALUES ('${ANCHOR_DATE} 00:00:00','dlq_total_offset','baseline_mirror',${baseline_dlq},0,1)" \
  || fail "基线镜像行写入失败"
write_row "INSERT INTO ads_window_metric (window_start, metric_name, dim_key, metric_value, revision, is_settled) VALUES ('${ANCHOR_DATE} 00:00:00','dlq_total_offset','dlq_count_delta',${delta},0,1)" \
  || fail "Δ 行写入失败"
write_row "INSERT INTO ads_window_metric (window_start, metric_name, dim_key, metric_value, revision, is_settled) VALUES ('${ANCHOR_DATE} 00:00:00','dlq_total_offset','total_events',${cur_input},0,1)" \
  || fail "分母行写入失败"

echo "[dirty-rate-sync] 完成：Δ=${delta}（dlq ${baseline_dlq}→${cur_dlq}，input=${cur_input}）"
echo "[dirty-rate-sync] 面板读法：dlq_count_delta / total_events = ${delta}/${cur_input}"
