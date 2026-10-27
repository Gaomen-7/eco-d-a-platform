#!/usr/bin/env bash
# 脏率基线同步 —— impl-04 票面 ⑤（脏率面板的「基线」从哪来）。
#
# 面板读 ecom_ads.ads_window_metric 的 metric_name='dlq_total_offset' 行；
# 这个 metric 没有任何 Flink 作业写它（数据面词表里不存在）——它是
# **面板专用的镜像行**，由本脚本从两个来源同步：
#   分子基线 = artifacts/verify/dirty-rate-baseline.json 的 dlq_offset
#              （make verify 维护的棘轮上界：首跑判据 SKIP 落盘、PASS 前推、
#              verify-fault --corrupt 收尾吸收；impl-06h 裁决 A 按实现改写）；
#   分子当前 = Kafka DLQ topic 总 offset（kafka-get-offsets.sh 第三列之和，
#              与 verify_runner._topic_total_offset 同一口径）；
#   分母     = 输入 topic 总 offset（同口径）。
# 面板显示「当前值 vs 基线 Δ」＝ dlq_count_delta / total_events（票面 ⑤ 分式，
# 分子只含真脏；重复不进分子——重复走幂等吸收，不产生 DLQ 行）。
#
# 写入形态：单行 upsert 到 ads_window_metric（UNIQUE KEY 覆盖写，幂等）。
# 不动 verify_runner / fault —— 基线文件的写者是 make verify 判据（首跑 SKIP 写、
# PASS 前推）与 --corrupt 收尾的吸收函数；普通 verify-fault 不写基线（impl-06h A）。
#
# ★ impl-07a：失败要报**真因**。读基线失败至少有四类，其中前两类**文件完全完好**：
#     ①解释器缺席（不在 PATH）  ②解释器在 PATH 上但**不可用**（空壳/商店跳转）
#     ③文件不是合法 JSON        ④文件缺必需字段
#   施工前这四类都被吞成同一句「基线文件不可解析（缺 X 字段？）」，把排查方向
#   引向基线文件。②尤其恶劣：只做**存在**探测会放行，只有**真的执行**一条最小
#   语句才能分辨 ⇒ 本段判据是「执行 + 核对输出」，不是存在性探测
#   （impl-05 §5 原文那条建议已被实测证伪，见 impl-07a 票面）。
#   同时：基线只解析**一次**（旧形态为取两个键起了两个解释器进程，两次解析之间
#   文件可能被 make verify 前推），且 input_events 降级为「仅日志参考」——它从不
#   参与判断或写入（面板 total_events 行写的是**当前**输入总数），故缺失不再致命。
set -uo pipefail

export MSYS_NO_PATHCONV=1  # 同 superset-provision.sh：防 Git-Bash 路径改写

BASELINE_FILE="${BASELINE_FILE:-artifacts/verify/dirty-rate-baseline.json}"
DORIS_QUERY_HOST="${DORIS_QUERY_HOST:-127.0.0.1}"
DORIS_QUERY_PORT="${DORIS_QUERY_PORT:-9030}"

step() { echo "[dirty-rate-sync] ▶ $*"; }
fail() { echo "[dirty-rate-sync] ✗ $*" >&2; exit 1; }

# ── ⓪ 解释器自检：真的执行一条最小语句，并**核对输出** ────────────────
# 静默成功：正常路径的输出必须与施工前逐字一致 ⇒ 本段成功时不打印任何东西。
# ★ 判据只看 **stdout 与退出码**：stderr 单独收集仅供诊断。把 stderr 折进哨兵比对会
#   让「启动时吐一句警告的**可用**解释器」（sitecustomize/pyenv/conda banner）被误报成
#   不可用——那正是本票要消灭的失败模式，不能自己再造一个。
PY_BIN="${PYTHON:-python3}"
PY_PROBE_SENTINEL="impl07a-probe-ok"
py_probe_err="$(mktemp)"
py_probe_out="$("${PY_BIN}" -c "print('impl07a-probe-ok')" 2>"${py_probe_err}")"
py_probe_rc=$?
py_probe_err_text="$(cat "${py_probe_err}")"
rm -f "${py_probe_err}"
if [ "${py_probe_rc}" -eq 127 ]; then
  fail "解释器缺席：'${PY_BIN}' 不在 PATH 上（bash rc=127）——**这不是基线文件的问题**。下一步：装一个 python3（apt-get install -y python3），或用 export PYTHON=/path/to/python3 指定绝对路径后重跑。"
fi
if [ "${py_probe_rc}" -ne 0 ]; then
  fail "解释器不可用：'${PY_BIN}' 存在但执行最小语句失败（rc=${py_probe_rc}；stderr：${py_probe_err_text:-<空>}）——**这不是基线文件的问题**，换一个能真正执行的解释器（例如 export PYTHON=/usr/bin/python3）后重跑；先跑 '${PY_BIN}' -c 'print(1)' 即可复现。"
fi
if [ "${py_probe_out}" != "${PY_PROBE_SENTINEL}" ]; then
  fail "解释器不可用：'${PY_BIN}' 能执行但不按预期输出（rc=0，stdout：${py_probe_out:-<空>}，期望 '${PY_PROBE_SENTINEL}'；stderr：${py_probe_err_text:-<空>}）——**这不是基线文件的问题**，仍是解释器/环境的问题，换一个干净的解释器（例如 export PYTHON=/usr/bin/python3）后重跑。"
fi

# ── ① 基线文件在位，且**一次**解析取出所需的键 ────────────────────────
step "读基线 ${BASELINE_FILE}"
[ -f "${BASELINE_FILE}" ] || fail "基线文件不存在——先跑 make verify（首跑判据 SKIP 时落盘基线）"

# 一次解析：机读标签 + 字段，失败时把解释器自己的 stderr 一并带出（不再丢弃）。
py_err_file="$(mktemp)"
parsed="$("${PY_BIN}" - "$BASELINE_FILE" 2>"${py_err_file}" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        doc = json.load(fh)
except json.JSONDecodeError as exc:
    print("PARSE_ERROR\t" + str(exc))
    raise SystemExit(3) from None
except OSError as exc:
    print("READ_ERROR\t" + str(exc))
    raise SystemExit(4) from None
if not isinstance(doc, dict):
    print("SHAPE_ERROR\t" + type(doc).__name__)
    raise SystemExit(5)
if "dlq_offset" not in doc:
    print("MISSING\tdlq_offset")
    raise SystemExit(6)
# 类型也要核：字符串/None/浮点/对象会让后面的 $(( )) 算术裸炸（unbound variable），
# 那种死法既不说原因也不给下一步——正是本票要消灭的形态。
if not isinstance(doc["dlq_offset"], int) or isinstance(doc["dlq_offset"], bool):
    print("TYPE_ERROR\t" + type(doc["dlq_offset"]).__name__)
    raise SystemExit(7)
if doc["dlq_offset"] < 0:
    print("NEGATIVE\t" + str(doc["dlq_offset"]))
    raise SystemExit(8)
# input_events 只供日志（面板分母写的是**当前**输入总数）⇒ 缺失不算错。
print(
    "OK\t"
    + str(doc["dlq_offset"])
    + "\t"
    + ("<缺失·仅日志参考>" if "input_events" not in doc else str(doc["input_events"]))
)
PY
)"
parse_rc=$?
parse_err="$(cat "${py_err_file}")"
rm -f "${py_err_file}"

# 「立新基线」是多数错误分支共用的下一步 ⇒ 只写一次（下面各臂引用它）。
REBASE_HINT="删掉 ${BASELINE_FILE}，重跑 make verify 立新基线（首跑判据 SKIP 时落盘）"

parse_tag="" detail="" extra=""
IFS=$'\t' read -r parse_tag detail extra <<< "${parsed}" || true

case "${parse_tag}" in
  OK)
    baseline_dlq="${detail}"
    baseline_input="${extra}"
    ;;
  MISSING)
    fail "基线文件缺必需字段：${detail}——文件不完整（多半是写入被打断；impl-07c 的原子替换就为除这条面）。**这不是解释器的问题**。下一步：${REBASE_HINT}。"
    ;;
  PARSE_ERROR)
    fail "基线文件不是合法 JSON（${detail}）——文件损坏或没写完。下一步：${REBASE_HINT}；若刚有人在跑 make verify-fault，等它结束再重试。"
    ;;
  READ_ERROR)
    fail "基线文件读不出内容（${detail}）——权限或编码问题，**不是字段缺失**。下一步：chmod 644 ${BASELINE_FILE} 后重跑；仍失败则${REBASE_HINT}。"
    ;;
  SHAPE_ERROR)
    fail "基线文件的顶层不是 JSON 对象（是 ${detail}）——文件被换成了别的形状。下一步：${REBASE_HINT}。"
    ;;
  TYPE_ERROR)
    fail "基线文件的 dlq_offset 不是整数（是 ${detail}）——字段类型变了，多半是有人手写改过这个文件。下一步：${REBASE_HINT}；这个值正常只由 make verify 写，不要手改成字符串。"
    ;;
  NEGATIVE)
    fail "基线文件的 dlq_offset 是负数（${detail}）——基线不可能为负，文件被改坏了。下一步：${REBASE_HINT}。"
    ;;
  *)
    # 解释器已通过自检，却没吐出机读结果 ⇒ 把解释器 stderr 原样带出（以前被丢弃）。
    fail "基线文件读不出目标值：解析没有任何机读输出（rc=${parse_rc}）——解释器自检已通过，**不是解释器不可用**。解释器 stderr：${parse_err:-<空>}。下一步：单独跑 '${PY_BIN}' -c 'import json,sys;print(json.load(open(sys.argv[1]))["dlq_offset"])' ${BASELINE_FILE} 复现，确认后${REBASE_HINT}。"
    ;;
esac
echo "[dirty-rate-sync] 基线：dlq_offset=${baseline_dlq} input_events=${baseline_input}"

# ── ② Kafka 两侧总 offset（与 verify_runner 同口径）───────────────────
topic_total_offset() {
  local topic="$1"
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "${topic}" \
    | awk -F: 'NF==3 && $3 ~ /^[0-9]+$/ { sum += $3 } END { print sum+0 }'
}

# 取回的 offset 必须是纯十进制（空串/报错文本都走这里，不再靠重定向掩盖噪声）。
require_offset() {
  case "$1" in '' | *[!0-9]*) fail "$2：$1" ;; esac
}

step "读 Kafka DLQ 总 offset"
cur_dlq="$(topic_total_offset 'ecom.behavior.event.v1.dlq')"
require_offset "${cur_dlq}" "DLQ 总 offset 取不到"
step "读 Kafka 输入 topic 总 offset"
cur_input="$(topic_total_offset 'ecom.behavior.event.v1')"
require_offset "${cur_input}" "输入总 offset 取不到"
echo "[dirty-rate-sync] 当前：dlq=${cur_dlq} input=${cur_input}"

# ── ③ Δ 与保护 ────────────────────────────────────────────────────────
delta=$(( cur_dlq - baseline_dlq ))
[ "${delta}" -lt 0 ] && fail "Δ 为负（dlq=${cur_dlq} < 基线 ${baseline_dlq}）——topic 被重建过，基线失效；删基线文件后重跑 make verify 立新基线（首跑 SKIP 重立）"
[ "${cur_input}" -eq 0 ] && fail "输入事件数为 0，脏率无定义"

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
