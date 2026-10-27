#!/usr/bin/env bash
# Superset 置备 —— `make init` 第④段的增补（impl-04）。
#
# 在 superset-init.sh（db upgrade / create-admin / superset init / health）之后跑：
#   1. PyMySQL 引导检查（compose 已改 docker-bootstrap.sh 路径装 requirements-local；
#      这里断言 pymysql 可 import——失败说明容器是旧定义，需要 recreate）；
#   2. superset-provision.py（容器内 ORM 置备：database(uuid 固定)/dataset/chart/
#      dashboard/Public 授权，幂等）；
#   3. 匿名可达断言（判据面：无 cookie GET 页面 200 + API 200 + chart_data 出数）。
#
# 判据纪律：第 3 步是「链路活着」的机读形态；票面判据（无痕浏览器五面板出数
# 并自动刷新、GIF）走浏览器，与本脚本的退出码互补。
set -uo pipefail

# Git-Bash/MSYS 会把 exec 参数里的 /docker/... 改写成宿主绝对路径（实机证伪：
# 'can't open file /app/C:/Users/.../docker/scripts/superset-provision.py'）。
# make init 本在 WSL 跑不受影响，这里自防一手。
export MSYS_NO_PATHCONV=1

step() { echo "[superset-provision] ▶ $*"; }
fail() { echo "[superset-provision] ✗ $*" >&2; exit 1; }

SUPERSET_HOST="${SUPERSET_HOST:-127.0.0.1}"
SUPERSET_PORT="${SUPERSET_PORT:-8088}"
DASH_SLUG="${DASH_SLUG:-ecom-live}"

# ── ① PyMySQL 在位（bootstrap 装的；旧容器会缺）──────────────────────
step "检查 PyMySQL 驱动（docker-bootstrap.sh requirements-local 装载）"
if ! docker compose exec -T superset python -c "import pymysql" >/dev/null 2>&1; then
  fail "pymysql 缺失——容器未走 bootstrap 引导；docker compose up -d --force-recreate superset 后重跑"
fi
echo "[superset-provision] ✓ pymysql 在位"

# ── ② ORM 置备（幂等）────────────────────────────────────────────────
step "运行 superset-provision.py（固定 database_uuid + dataset + 图表 + dashboard + Public 授权）"
if ! docker compose exec -T superset python /docker/scripts/superset-provision.py; then
  fail "置备脚本失败"
fi

# ── ③ 匿名可达断言（无任何 cookie/登录）──────────────────────────────
step "匿名 GET /superset/dashboard/${DASH_SLUG}/（期望 200，不重定向 login）"
code="$(curl -s -o /dev/null -w '%{http_code}' "http://${SUPERSET_HOST}:${SUPERSET_PORT}/superset/dashboard/${DASH_SLUG}/")"
[ "$code" = "200" ] || fail "匿名页面非 200：${code}"
echo "[superset-provision] ✓ 匿名页面 200"

step "匿名 GET 评审 URL（?standalone=1，票面 ①——演示干净形态，CSP 同源放行）"
scode="$(curl -s -o /dev/null -w '%{http_code}' "http://${SUPERSET_HOST}:${SUPERSET_PORT}/superset/dashboard/${DASH_SLUG}/?standalone=1")"
[ "$scode" = "200" ] || fail "匿名 standalone 页面非 200：${scode}"
echo "[superset-provision] ✓ ?standalone=1 评审 URL 200"

step "匿名 GET /api/v1/dashboard/${DASH_SLUG}（期望 200 + json）"
body="$(curl -s -H "Accept: application/json" "http://${SUPERSET_HOST}:${SUPERSET_PORT}/api/v1/dashboard/${DASH_SLUG}")"
echo "$body" | grep -q '"dashboard_title"' || fail "匿名 dashboard API 非 JSON：${body:0:120}"
echo "[superset-provision] ✓ 匿名 dashboard API 200"

# ★ dataset id 不硬编码（code-review 指出 id=1 是未断言的假设）：先查列表拿
#   kpi_current 的真实 id（列表 API 缺省只回 id 数组，要带 q 才回 result 行；
#   键序是 table_name 在 id **之后**，grep 邻接匹配不可靠 ⇒ python -c 解析），再探 chart_data。
#   ★ 解析器用 ${PYTHON:-python3}（impl-05 实测：WSL 宿主只有 python3，无 `python`；
#     2026-10-25 make init 在此步 `python: command not found` 退 1。与 Makefile
#     PYBIN=python3 的口径对齐，PYTHON 环境变量可覆盖。）
step "解析 kpi_current dataset id（不硬编码）"
DS_ID="$(curl -s -m 20 "http://${SUPERSET_HOST}:${SUPERSET_PORT}/api/v1/dataset/?q=(order_columns:table_name,order_direction:asc)" \
  -H "Accept: application/json" \
  | "${PYTHON:-python3}" -c 'import json,sys
try:
    d = json.load(sys.stdin)
    rows = d.get("result", [])
    print(next(str(r["id"]) for r in rows if r.get("table_name") == "kpi_current"))
except StopIteration:
    pass')"
[ -n "$DS_ID" ] || fail "找不到 kpi_current 的 dataset id"
echo "[superset-provision] ✓ kpi_current id=${DS_ID}"

step "匿名 chart_data 出数（kpi_current 的 UV，期望 status=success）"
resp="$(curl -s -X POST "http://${SUPERSET_HOST}:${SUPERSET_PORT}/api/v1/chart/data" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d "{\"datasource\": {\"id\": ${DS_ID}, \"type\": \"table\"},\"force\": false,\"queries\": [{\"granularity\": \"window_start\",\"filters\": [{\"col\": \"metric_name\", \"op\": \"==\", \"val\": \"uv\"}],\"metrics\": [{\"expressionType\": \"SIMPLE\", \"column\": {\"column_name\": \"metric_value\"}, \"aggregate\": \"SUM\", \"label\": \"UV\"}],\"row_limit\": 10,\"time_range\": \"No filter\",\"timeseries_limit\": 0}],\"result_format\": \"json\",\"result_type\": \"full\"}")"
echo "$resp" | grep -q '"status": *"success"' || fail "匿名 chart_data 不出数：${resp:0:200}"
echo "[superset-provision] ✓ 匿名 chart_data 出数（链路活着）"

# ── ④ zip 载体闭环（票面 ⑥：make init 的复现手段；判据归 SRS）────────
# 顺序承重（code-review 修复，2026-10-25）：importer 按 database_uuid 匹配
# database（逐字 config["database_id"] = database_ids[config["database_uuid"]]），
# 冻结 UUID 的 database 由本脚本第②步创建 ⇒ 导入必须在置备**之后**。
# 先导出（刷新 zip），再导入回读——「导得进、连得上」是 zip 复现手段的机读证明。
EXPORT_ZIP="${SUPERSET_EXPORT_ZIP:-docker/superset/export/ecom-live.zip}"
step "export-dashboards（刷新 ${EXPORT_ZIP}）"
if ! mkdir -p "$(dirname "$EXPORT_ZIP")" \
  || ! docker compose exec -T superset superset export-dashboards -f /tmp/ecom-live.zip \
  || ! docker compose cp superset:/tmp/ecom-live.zip "$EXPORT_ZIP"; then
  fail "zip 导出失败"
fi
echo "[superset-provision] ✓ zip 已导出（${EXPORT_ZIP}）"

step "import-dashboards（zip 回读校验；6.1.0 CLI 必带 -u）"
if ! docker compose cp "$EXPORT_ZIP" superset:/tmp/ecom-live-reimport.zip \
  || ! docker compose exec -T superset superset import-dashboards \
       -p /tmp/ecom-live-reimport.zip -u "${SUPERSET_ADMIN_USER:-admin}"; then
  fail "zip 导入失败（回读校验）"
fi
echo "[superset-provision] ✓ zip 导入回读成功"

# ── ⑤ 脏率基线同步（面板的基线行从 dirty-rate-baseline.json 来——make verify
#    维护的棘轮上界；原「verify-fault 快照」口径经 impl-06h 裁决 A 按实现改写）──
# 脚本幂等；基线文件缺席（从未跑过 verify-fault）则跳过并提示，不算失败。
DIRTY_SYNC="${SUPERSET_DIRTY_SYNC:-docker/scripts/superset-dirty-rate-sync.sh}"
step "脏率基线同步（${DIRTY_SYNC}）"
if [ -f "artifacts/verify/dirty-rate-baseline.json" ]; then
  if ! bash "$DIRTY_SYNC"; then
    fail "脏率同步失败"
  fi
  echo "[superset-provision] ✓ 脏率基线已同步"
else
  echo "[superset-provision]   （artifacts/verify/dirty-rate-baseline.json 不存在——先跑 make verify 立基线（首跑判据 SKIP 时落盘）；面板脏率行暂缺，不阻塞置备）"
fi

step "完成"
