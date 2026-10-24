#!/usr/bin/env bash
# lake_assert.sh — 离线断言执行面：跑 `sql/assert_lake.sql`，解析 stdout，判成败。
#
# 规格落点：spec §5.3（`assert_lake.sql`）、§5.4（A1/A2/A3 + 幂等 + 快照 + 拉链）、
#          §5.5（**断言执行面：为什么是 lake_assert.sh**）、ticket impl-03d。
#
# ════════════════════════════════════════════════════════════════════
# ★★ 为什么需要这个脚本（而不是看 sql-client 的退出码）
# ════════════════════════════════════════════════════════════════════
# 源码事实：`sql-client.sh -f` 语句报错后**仍以 0 退出**（`CliClient` 丢弃返回值、
# 无 `System.exit`）。本机实测：一份「先一条非法语句、再一条合法语句」的脚本
# ⇒ EXIT=0 且输出里恰有 1 条错误标记。**退出码不可用。**
# ⇒ 唯一信号是 stdout。故判据 = 「无错误标记」**且**「每条期望哨兵 ok=true」。
#   ⚠️ 两个条件缺一不可：
#     · 只看"无错误标记" ⇒ 语句整段没跑也会绿（缺席不等于成功）；
#     · 只看"哨兵 ok=true" ⇒ 有错误但输出恰好含旧行/回显文本时会绿。
#
# ════════════════════════════════════════════════════════════════════
# ★★ 判据的三层（缺一层就有假绿通道）
# ════════════════════════════════════════════════════════════════════
#   1. **docker exec 退出码**（容器不存在/挂载失败/被 OOM-kill 时会非 0，见 03c 事故）；
#   2. **无错误标记**（SQL 语义错误）；
#   3. **逐条哨兵**：脚本硬编码期望清单，按名字核对，**少一条即失败**。
#      这一层刻意不用"没有 ok=false 就通过"——那对"语句根本没执行"零区分力。
#
# ════════════════════════════════════════════════════════════════════
# ★★ 解析规则（与 `sql/assert_lake.sql` 的输出语法严格配对）
# ════════════════════════════════════════════════════════════════════
# sql-client 在 TABLEAU 模式下把结果渲染成 `| cell | cell |`，**并且会回显 SQL
# 源码**（带 `Flink SQL> ` / `> ` 前缀，注释也在内）。故解析规则是：
#   · 只认**以 `|` 开头**的行（回显行以 `>` 或 `Flink SQL>` 开头 ⇒ 天然排除）；
#       —— 这一条是必需的：assert_lake.sql 的注释里本身就有 `ok=true` 字样，
#          回显后会出现"看起来像哨兵"的行。实测确认回显确实带 `> ` 前缀。
#   · 哨兵行：第 2 列以 `ok=` 开头 ⇒ 记入断言表；
#   · 度量行：第 2 列**不**以 `ok=` 开头 ⇒ 记入度量表（A2 落这一类）。
#   · 表头/分隔行（`| assertion | ok |`、`+---+---+`）自然被上面两条排除。
#
# ════════════════════════════════════════════════════════════════════
# ★★ 幂等：两次跑之间比对指纹（单次运行无法自证）
# ════════════════════════════════════════════════════════════════════
# `metric.fp.*` 四件套是 DWD 的指纹。同一次 SQL 会话里没有"上一遍"可比 ⇒
# 本脚本提供两条模式：
#   `--record`   跑完把指纹写到 `--fingerprint-file`（缺省 artifacts/lake_assert/fp.txt）
#   `--compare`  跑完与已存指纹比；不一致 ⇒ 失败（这是幂等判据本身）
#   `--record-then-compare`（或 `--idempotent`）先比、再把本次覆盖写入
# 缺省（都不给）= 只判断言、不碰指纹文件。
#
# ════════════════════════════════════════════════════════════════════
# 退出码（诚实，不谎报）
# ════════════════════════════════════════════════════════════════════
#   0 = 全部断言通过（且幂等比对通过，若要求）
#   1 = 断言失败 / 出现错误标记 / docker 层失败
#   2 = 用法错误（未知参数、SQL 文件不存在）
#   3 = 幂等比对失败（指纹与上一遍不同）—— 与 1 分开，便于归因
#
# ════════════════════════════════════════════════════════════════════
# 用法
# ════════════════════════════════════════════════════════════════════
#   bash scripts/lake_assert.sh                     # 只判断言
#   bash scripts/lake_assert.sh --record            # 判 + 存指纹
#   bash scripts/lake_assert.sh --compare           # 判 + 与已存指纹比
#   bash scripts/lake_assert.sh --record-then-compare   # 判 + 比 + 覆盖（幂等两遍用）
#   bash scripts/lake_assert.sh --judge <stdout>    # **只判定面**（不连集群，供单测）
#   bash scripts/lake_assert.sh --parse <stdout>    # 只解析并打印（排障用）
#
#   --judge 的存在理由：判定逻辑必须能**脱离集群**被单测（票面 AC 要求
#   「lake_assert.sh 的判定逻辑有单元测试」）。把判定抽成纯函数式的子命令，
#   单测就不需要 docker / 集群 / 90 秒的等待。**注入的是 stdout，不是 SQL。**

set -uo pipefail

cd "$(dirname "$0")/.." || exit 2

CONTAINER="${LAKE_FLINK_CONTAINER:-eco-flink-jm}"
SQL_FILE="sql/assert_lake.sql"
FP_FILE="${LAKE_FP_FILE:-artifacts/lake_assert/fp.txt}"

# ── 期望哨兵清单（**硬编码**，与 sql/assert_lake.sql 逐一对应）──────────────
# ★ 为什么硬编码而不是"从 SQL 里自动提取名字"：
#   自动提取的话，**整条哨兵被删掉**时清单也一起少一条 ⇒ 缺一条 = 通过，
#   正是要堵的假绿通道。硬编码让"删掉一条断言"必然变红。
# ★ 名字必须与 SQL 里的 `AS assertion` 字面量**逐字相同**（含大小写）。
#   静态守卫：tests/test_lake_assert.py::test_expected_names_match_sql。
#
# ★★ 同一个名字在**每一份**都要出现；`<NAME>=true` 表示该条必须 ok=true。
EXPECTED_ASSERTIONS=(
  "A1_split_complete"
  "A3_late_bucket_split"
  "A3_late_flag_nonempty"
  "IDEMPOTENCY_fp_ready"
  "SNAP_within_upper_bound"
  "SNAP_empty_table_zero"
  "ZIP_interval_self_sync"
  "ZIP_audit_log_shows_change"
  "OPT_snapshot_retention"
)

# ── 期望度量清单（只日志，**不判**；但要求存在 ⇒ 「A2 没被删掉」可判）──────
# ★ 票面 AC 说 A2「只入日志、不作断言」——它不参与成败，但**必须真的被打印**。
#   故这里单独核"存在性"，而不是核它的值。
EXPECTED_METRICS=(
  "metric.a2_dup_overshoot"
  "metric.fp.rows"
  "metric.fp.hash"
  "metric.fp.devent"
  "metric.fp.dlag"
)

# ── 指纹的四个键（顺序固定，落盘按此顺序）──────────────────────────────────
FP_KEYS=(rows hash devent dlag)

usage() {
  sed -n '/^# 用法$/,/^set -uo/p' "$0" | sed 's/^# \{0,1\}//' | sed '$d'
}

MODE="judge-only"       # judge-only | record | compare | record-then-compare | parse
STDIN_FILE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --judge)  MODE="judge-only"; shift; STDIN_FILE="${1:-}"; shift || true ;;
    --parse)  MODE="parse"; shift; STDIN_FILE="${1:-}"; shift || true ;;
    --record) MODE="record"; shift ;;
    --compare) MODE="compare"; shift ;;
    --record-then-compare|--idempotent) MODE="record-then-compare"; shift ;;
    --fingerprint-file) FP_FILE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[assert] ✗ 未知参数：$1" >&2; usage >&2; exit 2 ;;
  esac
done

# ════════════════════════════════════════════════════════════════════════
# 解析：从 sql-client 的 stdout 抽断言表与度量表
# ════════════════════════════════════════════════════════════════════════
# 输出（每行一条，供调用方 grep）：
#   ASSERT<TAB><name><TAB>ok=true|ok=false
#   METRIC<TAB><name><TAB><value>
#   ERR<TAB><count>
#
# ★ 全部用 awk：`sed`/`grep` 处理"多列 + 前后空白"的组合在这类判断上过脆。
# ★ 去 ANSI 色：sql-client 会给 `[ERROR]` 与 INFO 上色，`\033[...m` 会把
#   `[ERROR]` 切成 `[+ESC[31;1m+ERROR]` ⇒ 直接 grep 会漏判（03b 已踩过）。
parse_stdout() {
  awk '
    # 只认以 | 开头的表格行（回显行以 ">" / "Flink SQL>" 开头 ⇒ 天然排除）
    /^\|/ {
      # 去掉 ANSI 色
      line = $0
      gsub(/\033\[[0-9;]*m/, "", line)
      # 以 | 切列：去首尾的空字段
      n = split(line, parts, "|")
      # parts[1] 与 parts[n] 是首尾空串（行以 | 开头/结尾）
      if (n < 4) next
      k = parts[2]; v = parts[3]
      gsub(/^[ \t]+|[ \t]+$/, "", k)
      gsub(/^[ \t]+|[ \t]+$/, "", v)
      if (k == "" || v == "") next
      # 表头 / 分隔行（两种形状：assertion 表头、度量行的 k/v 表头）
      if (k == "assertion") next
      if (k == "k" && v == "v") next
      if (v == "ok") next
      if (v ~ /^ok=/) {
        print "ASSERT\t" k "\t" v
      } else {
        print "METRIC\t" k "\t" v
      }
      next
    }
    # 错误标记：在**非表格行**上找
    {
      line = $0
      gsub(/\033\[[0-9;]*m/, "", line)
      if (line ~ /\[ERROR\]/) err++
    }
    END { print "ERR\t" err + 0 }
  ' "$1"
}

# ════════════════════════════════════════════════════════════════════════
# 判定：三层判据（见文件头）
# ════════════════════════════════════════════════════════════════════════
# 入参：解析结果文件、docker 退出码
# 出参：0 全绿 / 1 失败；失败原因打印到 stderr
judge() {
  local parsed="$1" docker_rc="$2" rc=0
  local err_count
  err_count="$(awk -F'\t' '$1=="ERR"{print $2}' "$parsed")"
  err_count="${err_count:-0}"

  if [ "$docker_rc" -ne 0 ]; then
    echo "[assert] ✗ docker exec 退出 $docker_rc（容器/挂载/守护进程层失败，不是断言失败）" >&2
    rc=1
  fi

  if [ "$err_count" -ne 0 ]; then
    echo "[assert] ✗ SQL 层错误标记 $err_count 条（见原始输出）" >&2
    rc=1
  fi

  # ── 逐条核对期望哨兵（**少一条即失败**）────────────────────────────
  local name got
  for name in "${EXPECTED_ASSERTIONS[@]}"; do
    got="$(awk -F'\t' -v n="$name" '$1=="ASSERT" && $2==n {print $3; exit}' "$parsed")"
    if [ -z "$got" ]; then
      echo "[assert] ✗ 哨兵 [$name] **没有出现**（缺席不等于通过）" >&2
      rc=1
    elif [ "$got" != "ok=true" ]; then
      echo "[assert] ✗ 哨兵 [$name] = $got" >&2
      rc=1
    else
      echo "[assert] ✓ $name"
    fi
  done

  # ── 意外出现的哨兵（SQL 里加了断言但清单没跟上 ⇒ 也该红）──────────────
  local unexpected
  unexpected="$(
    awk -F'\t' '$1=="ASSERT"{print $2}' "$parsed" \
      | while IFS= read -r name; do
          local known=0 e
          for e in "${EXPECTED_ASSERTIONS[@]}"; do
            [ "$e" = "$name" ] && known=1 && break
          done
          [ "$known" -eq 0 ] && echo "$name"
        done
  )"
  if [ -n "$unexpected" ]; then
    echo "[assert] ✗ 出现未登记的哨兵：$(printf "%s " $unexpected)" >&2
    echo "[assert]   清单在 lake_assert.sh 的 EXPECTED_ASSERTIONS，必须同步" >&2
    rc=1
  fi

  # ── 度量行存在性（A2 必须真的被打印，但不判它的值）────────────────────
  for name in "${EXPECTED_METRICS[@]}"; do
    got="$(awk -F'\t' -v n="$name" '$1=="METRIC" && $2==n {print $3; exit}' "$parsed")"
    if [ -z "$got" ]; then
      echo "[assert] ✗ 度量 [$name] 缺失（A2/指纹必须入日志）" >&2
      rc=1
    else
      echo "[assert] · $name = $got"
    fi
  done

  # ── A2 **不参与判定**：这是一条**结构性**保证，不是"忘了判" ────────────
  # 票面 AC：「A2 只入日志、不作断言（日志里有重复率数值，但 lake_assert.sh
  # 不因它退非 0）」。上面只核"存在"，值多大都不影响 rc。
  echo "[assert] · A2 只记账、不判定（spec §5.4 逐字：恒真，不作断言）"

  return "$rc"
}

# ════════════════════════════════════════════════════════════════════════
# 指纹：读取 / 落盘 / 比对
# ════════════════════════════════════════════════════════════════════════
read_fp_from_parsed() {
  local parsed="$1" key
  for key in "${FP_KEYS[@]}"; do
    awk -F'\t' -v n="metric.fp.$key" '$1=="METRIC" && $2==n {print $3; exit}' "$parsed"
  done | paste -sd' ' -
}

write_fp() {
  local parsed="$1" out
  out="$(read_fp_from_parsed "$parsed")"
  mkdir -p "$(dirname "$FP_FILE")" || return 1
  printf '%s\n' "$out" > "$FP_FILE" || return 1
  echo "[assert] · 指纹已落盘：$FP_FILE  ($out)"
}

compare_fp() {
  local parsed="$1" now prev
  now="$(read_fp_from_parsed "$parsed")"
  if [ ! -f "$FP_FILE" ]; then
    echo "[assert] ✗ 幂等比对要求已存指纹，但 $FP_FILE 不存在" >&2
    echo "[assert]   先跑一次 --record（或 --record-then-compare）" >&2
    return 3
  fi
  prev="$(cat "$FP_FILE")"
  if [ "$now" != "$prev" ]; then
    echo "[assert] ✗ 幂等破：DWD 指纹与上一遍不同" >&2
    echo "[assert]   上一遍：$prev" >&2
    echo "[assert]   本  遍：$now" >&2
    echo "[assert]   顺序为 (rows hash devent dlag)；四件套见 sql/assert_lake.sql ④ 段" >&2
    return 3
  fi
  echo "[assert] ✓ 幂等：DWD 指纹与上一遍逐字相同（$now）"
  return 0
}

# ════════════════════════════════════════════════════════════════════════
# 主流程
# ════════════════════════════════════════════════════════════════════════
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
PARSED="$WORK/parsed.txt"
RAW="$WORK/raw.txt"

if [ "$MODE" = "parse" ] || [ "$MODE" = "judge-only" ] && [ -n "$STDIN_FILE" ]; then
  if [ ! -f "$STDIN_FILE" ]; then
    echo "[assert] ✗ 找不到输入文件：$STDIN_FILE" >&2
    exit 2
  fi
  parse_stdout "$STDIN_FILE" > "$PARSED"
  if [ "$MODE" = "parse" ]; then
    echo "--- ASSERT ---"
    awk -F'\t' '$1=="ASSERT"' "$PARSED"
    echo "--- METRIC ---"
    awk -F'\t' '$1=="METRIC"' "$PARSED"
    echo "--- ERR ---"
    awk -F'\t' '$1=="ERR"' "$PARSED"
    exit 0
  fi
  # --judge：**不连集群**，docker_rc 视为 0（供单测注入）
  if judge "$PARSED" 0; then
    echo "[assert] 通过（--judge 注入模式）"
    exit 0
  fi
  echo "[assert] 不通过（--judge 注入模式）" >&2
  exit 1
fi

if [ ! -f "$SQL_FILE" ]; then
  echo "[assert] ✗ 断言脚本缺失：$SQL_FILE" >&2
  exit 2
fi

echo "[assert] >> 运行 $SQL_FILE（容器 $CONTAINER）"
MSYS_NO_PATHCONV=1 docker exec -i "$CONTAINER" \
  bash -c "cat > /tmp/assert_lake.sql" < "$SQL_FILE" >/dev/null 2>&1
MSYS_NO_PATHCONV=1 docker exec -i "$CONTAINER" \
  /opt/flink/bin/sql-client.sh -f /tmp/assert_lake.sql > "$RAW" 2>&1
DOCKER_RC=$?

echo "[assert] -- docker exec 退出码 = $DOCKER_RC（**不作为成功判据**：sql-client 报错仍退 0）"

parse_stdout "$RAW" > "$PARSED"

# 原始输出留档，便于失败归因（artifacts/ 不入 git，见 .gitignore）
mkdir -p artifacts/lake_assert 2>/dev/null && cp "$RAW" artifacts/lake_assert/last_stdout.txt 2>/dev/null || true

if ! judge "$PARSED" "$DOCKER_RC"; then
  echo "[assert] ✗ 断言不通过（原始输出：artifacts/lake_assert/last_stdout.txt）" >&2
  exit 1
fi

# ── 幂等模式 ────────────────────────────────────────────────────────────
FP_RC=0
case "$MODE" in
  compare)
    compare_fp "$PARSED" || FP_RC=$?
    ;;
  record)
    write_fp "$PARSED" || FP_RC=1
    ;;
  record-then-compare)
    # ★ 顺序是「先比、后覆盖」：比要跟**上一遍**比；先覆盖就把参照物删了。
    if [ -f "$FP_FILE" ]; then
      compare_fp "$PARSED" || FP_RC=$?
    else
      echo "[assert] · 无已存指纹（首遍），本次作为基线"
    fi
    if [ "$FP_RC" -eq 0 ]; then
      write_fp "$PARSED" || FP_RC=1
    fi
    ;;
esac

if [ "$FP_RC" -ne 0 ]; then
  exit "$FP_RC"
fi

echo "[assert] ✓ 全部断言通过"
exit 0
