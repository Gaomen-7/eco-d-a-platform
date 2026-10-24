#!/usr/bin/env bash
# lake_replay.sh — `make replay` 的执行面：现产门槛批 → 七表 DDL → 按层串 SQL。
#
# 规格落点：spec §5.1（`make replay` **前置生成器现产**门槛批，seed 固定、落 data/、不入 git）、
#          §5.3（全 SQL 零 jar；`sql-client.sh -f` 串联；LAYER 截断；逐层 layer=<层> rows=…）。
#
# ★ 为什么生成器放在这里而不是 Makefile recipe：
#   ① 现产是 `make replay` 的**前置换作**（spec §5.1 逐字），不是可选前置；
#   ② 生成器必须在 WSL 里用 uv 跑（Makefile 的 $(UV) 是同一权威），
#      塞进 recipe 会与 `docker exec` 的引用/转义打架。
#   ⚠️ 现产有守卫：raw.jsonl 已存在且非空就**不重产**（否则每次 make replay 都要
#      几分钟重写 351 MB）。要强制重产：FORCE_GEN=1 make replay。
#
# ★ 为什么判据要「docker 退出码 + 层 marker」双证据：
#   spec §5.5 记录 sql-client 报错后**仍以 0 退出**（CliClient 丢弃返回值、无 System.exit），
#   故 [ERROR] 子串是必要的；但**只**看子串会漏掉「容器根本不存在」这一支 ——
#   那种情况 stdout 里一个 [ERROR] 都没有，旧版会打印「完成」并 exit 0（审阅实测复现）。
#   因此每条语句另查 `docker exec` 的 $?，且每层要求**正向证据**：该层的
#   `layer=<层> rows=` marker 必须真的出现（absence of error != success）。
#
# ★ 退出码语义（诚实的非绿）：
#   0 = 请求范围**全部**跑完；
#   2 = LAYER 取值非法；
#   3 = 请求范围里有**尚未实现**的层（脚本不存在）—— 绝不谎报成功；
#   1 = 执行失败（docker 层失败 / SQL [ERROR] / 层 marker 缺失）。
#   本票（03b）在范围内的绿路径是 `make replay LAYER=dwd`。

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

LAYER="${LAYER:-${1:-}}"
CONTAINER="${LAKE_FLINK_CONTAINER:-eco-flink-jm}"
SQL_DIR="sql"

# ── 门槛批现产参数（spec §5.1：seed **固定**；密度 ≈4 ev/s 与 200 ev/s 不同源）──
GEN_EVENTS="${GEN_EVENTS:-1048576}"
GEN_SPAN="${GEN_SPAN:-3d}"
GEN_SEED="${GEN_SEED:-20241022}"
GEN_OUT="${GEN_OUT:-data/threshold/raw.jsonl}"
GEN_MANIFEST="${GEN_MANIFEST:-data/threshold/manifest.json}"
# ★ 维度变更流**另一个文件**（impl-03c）：事件文件已被 03b 的门槛批字节判据锁定，
#   混进去会推翻那份判据。两个文件同属「门槛批现产」这一步，一起产一起守。
GEN_DIM_OUT="${GEN_DIM_OUT:-data/threshold/dim.jsonl}"
UV="${UV:-$HOME/.local/bin/uv}"

# ── 层表："层名:脚本:该层必须出现的 marker" ────────────────────────────
# 顺序不可交换（后者读前者的表）。marker 是该层的**正向证据**。
# 五层自 impl-03c 起全部落盘；缺失仍按 exit 3 处理（见退出码语义），不静默跳过。
LAYERS=(
  "ods:replay_10_ods.sql:layer=ods rows="
  "dwd:replay_20_dwd.sql:layer=dwd rows="
  "dim:replay_30_dim.sql:layer=dim rows="
  "dws:replay_40_dws.sql:layer=dws rows="
  "ads:replay_50_ads.sql:layer=ads rows="
)

CATALOG_SQL="replay_00_catalog.sql"
DDL_SQL="replay_01_ddl.sql"

# ── LAYER → 有序下标 ─────────────────────────────────────────────────
order_index() {
  local want="$1" i=0 entry name
  for entry in "${LAYERS[@]}"; do
    name="${entry%%:*}"
    if [ "$name" = "$want" ]; then echo "$i"; return 0; fi
    i=$((i + 1))
  done
  return 1
}

STOP_AT=-1
if [ -n "$LAYER" ]; then
  if ! STOP_AT="$(order_index "$LAYER")"; then
    echo "[replay] ✗ 未知 LAYER=$LAYER（可选：ods dwd dim dws ads）" >&2
    exit 2
  fi
  echo "[replay] LAYER=$LAYER ⇒ 跑到该层为止（含；不是只跑该层——层间有数据依赖）"
else
  echo "[replay] LAYER 未指定 ⇒ 五层全跑"
fi

# ── 0. 门槛批现产（spec §5.1 前置换作）─────────────────────────────────
if [ "${FORCE_GEN:-0}" = "1" ] || [ ! -s "$GEN_OUT" ] || [ ! -s "$GEN_DIM_OUT" ]; then
  echo "[replay] >> gen  (ecomgen dump --events $GEN_EVENTS --logical-span $GEN_SPAN --seed $GEN_SEED)"
  # ★ 必须走 **console script** `ecomgen`，不能 `python -m ecomgen`
  #   （impl-03b spec 轴审阅实测证伪）：本包没有 `src/ecomgen/__main__.py`，
  #   pyproject 只声明 `ecomgen = "ecomgen.cli:main"`。实测逐字：
  #     No module named ecomgen.__main__; 'ecomgen' is a package and cannot be directly executed
  #   干净检出（raw.jsonl 不存在）时 gen 是链首第一步 ⇒ 旧写法会让 `make replay`
  #   直接以 1 退出，整条链根本跑不起来。
  if ! "$UV" run ecomgen dump \
        --events "$GEN_EVENTS" --logical-span "$GEN_SPAN" --seed "$GEN_SEED" \
        --out "$GEN_OUT" --manifest "$GEN_MANIFEST" --dim-out "$GEN_DIM_OUT"; then
    echo "[replay] ✗ 门槛批现产失败" >&2
    exit 1
  fi
else
  echo "[replay] -- 门槛批已存在（$GEN_OUT / $GEN_DIM_OUT），跳过现产（强制：FORCE_GEN=1）"
fi

# ── 执行一个 SQL 文件：0=成功 / 1=失败 / 3=脚本缺失 ────────────────────
# ★ 双证据：docker 退出码 + [ERROR] 子串 + （若给了 marker）marker 真的出现。
# ★ SQL 以只读卷挂进两容器（compose: ./sql:/sql:ro），执行的是容器内 /sql/<file>；
#   下面的存在性检查查的是**宿主**镜像路径，二者由挂载互为约束（改一处必改另一处）。
run_sql() {
  local label="$1" file="$2" marker="${3:-}" out rc
  if [ ! -f "$SQL_DIR/$file" ]; then
    echo "[replay] -- $label：$SQL_DIR/$file 不存在"
    return 3
  fi
  echo "[replay] >> $label  ($file)"
  out="$(MSYS_NO_PATHCONV=1 docker exec -i "$CONTAINER" \
           /opt/flink/bin/sql-client.sh -f "/sql/$file" 2>&1)"
  rc=$?
  # 去 ANSI 颜色，避免 [ERROR] 被色彩码隔开而漏判
  out="$(printf "%s" "$out" | sed "s/$(printf "\033")\[[0-9;]*m//g")"
  if [ "$rc" -ne 0 ]; then
    printf "%s\n" "$out" | tail -20 >&2
    echo "[replay] ✗ $label：docker exec 退出 $rc（容器/挂载/守护进程层失败，不是 SQL 错误）" >&2
    return 1
  fi
  if printf "%s" "$out" | grep -q "\[ERROR\]"; then
    printf "%s\n" "$out" | grep -A6 "\[ERROR\]" | head -40 >&2
    echo "[replay] ✗ $label：SQL 报错（见上）" >&2
    return 1
  fi
  if [ -n "$marker" ] && ! printf "%s" "$out" | grep -q "$marker"; then
    printf "%s\n" "$out" | tail -20 >&2
    echo "[replay] ✗ $label：缺正向证据 marker [$marker]（无报错 != 成功）" >&2
    return 1
  fi
  # 逐个 marker 转成统一日志行（行数必须是 SQL 查出来的，不是脚本猜的）
  printf "%s" "$out" | grep -oE "(layer=[a-z]+|dwd_reject) rows=[0-9]+" | while IFS= read -r line; do
    echo "[layer] $line"
  done
  return 0
}

# ── 0. catalog ──────────────────────────────────────────────────────
run_sql catalog "$CATALOG_SQL"
rc=$?
if [ "$rc" -eq 3 ]; then echo "[replay] ✗ catalog 脚本缺失，无法继续" >&2; exit 3; fi
if [ "$rc" -ne 0 ]; then exit 1; fi

# ── 0.5. 七表 DDL ───────────────────────────────────────────────────
# ★ 不计入 LAYER 截断：LAYER=dwd 也要建表，否则层脚本无表可写。
#   CREATE TABLE IF NOT EXISTS ⇒ 幂等，可常驻链首。
run_sql ddl "$DDL_SQL"
rc=$?
if [ "$rc" -eq 3 ]; then echo "[replay] ✗ DDL 脚本缺失，无法继续" >&2; exit 3; fi
if [ "$rc" -ne 0 ]; then exit 1; fi

# ── 1. 逐层 ─────────────────────────────────────────────────────────
i=0
for entry in "${LAYERS[@]}"; do
  name="${entry%%:*}"
  rest="${entry#*:}"
  file="${rest%%:*}"
  marker="${rest#*:}"
  if [ "$STOP_AT" -ge 0 ] && [ "$i" -gt "$STOP_AT" ]; then
    echo "[replay] -- 停在 $name 之前（LAYER=$LAYER 的边界）"
    break
  fi
  run_sql "$name" "$file" "$marker"
  rc=$?
  if [ "$rc" -eq 3 ]; then
    echo "[replay] ✗ 层 $name 未实现（$SQL_DIR/$file 不存在）。" >&2
    echo "[replay]   五层缺任一脚本即 exit 3（绝不谎报成功）" >&2
    exit 3
  fi
  if [ "$rc" -ne 0 ]; then exit 1; fi
  i=$((i + 1))
done

# ── 2. 维护段（impl-03d，spec §5.3 末两项之二）──────────────────────────
# ★ 只在**全量 replay**（未给 LAYER）时跑：维护段的语义是「这批五层数据落稳之后
#   收快照/压文件」，LAYER 截断时数据还不完整，压了也只是白做。
# ★ 不计入 LAYERS 数组：它不是「层」（spec §5.3 的层词汇表是 ods|dwd|dim|dws|ads），
#   故 `LAYER=maintain` **不是**合法取值（与拒绝表用 `dwd_reject` 的取舍同源）。
# ★ 这一段的 marker 用 `maintain rows=` 而**不是** `layer=maintain`，理由同上；
#   run_sql 的 marker 参数是任意子串，故直接传即可。
if [ "$STOP_AT" -lt 0 ]; then
  run_sql maintain "replay_90_maintain.sql" "maintain rows="
  rc=$?
  if [ "$rc" -eq 3 ]; then
    echo "[replay] ✗ 维护脚本缺失（$SQL_DIR/replay_90_maintain.sql）。" >&2
    echo "[replay]   维护段是 replay 的一部分（快照 [5,20] 靠它收敛），缺失即 exit 3" >&2
    exit 3
  fi
  if [ "$rc" -ne 0 ]; then exit 1; fi
else
  echo "[replay] -- 跳过维护段（LAYER=$LAYER 截断时不收快照/不压文件）"
fi

echo "[replay] 完成（请求范围全部跑完）"
exit 0
