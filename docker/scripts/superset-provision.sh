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
#
# ★ impl-07b：解析失败要报**真因**。施工前那段内联解析（`except StopIteration: pass`
#   之外的一切异常都退化成「空」）把**三类**互不相同的失败压成同一句
#   「找不到 kpi_current 的 dataset id」：
#     ①接口里**确实没有**这个 dataset（列表里没有目标表）← **唯一**该报「找不到」的一类
#     ②接口返回的**结构变了**（result 不是数组 / 行不是对象 / id 键没了 / id 不可用 /
#       接口干脆回了 message 报错）——取值路径对不上
#     ③**没能解析**：响应体不是 JSON（空响应、HTML 登录页）、解析器不可用、连响应都没取回来
#   ③ 最恶劣：接口正常、dataset 存在，脚本却报「找不到」⇒ 排查方向被引向「看板里是不是
#   没这个 dataset / 置备是不是没跑」，而真因在执行面。⇒ 判据改成「机读标签分流」，
#   且**先把响应体落进临时文件**再解析：这样「HTTP 取不到」与「取到了但解析不了」不再
#   混为一谈（管道形态下 curl 的失败与解释器的失败都是「一个空串」）。
#   ★ 与 impl-07a **刻意不合流**：那边从**文件**读、取两个数值键；这边从**接口响应**读、
#     取一个对象里的标识符。输入源与语义都不同，合流只会造出一个谁都不合适的抽象。
step "解析 kpi_current dataset id（不硬编码）"
PY_BIN="${PYTHON:-python3}"
# 临时目录取不到是**执行面**问题（磁盘满 / TMPDIR 不可写），必须在这里就报掉：
# 否则 DS_BODY 会落到 "//dataset-list.json"，curl 写不进去 ⇒ 后面被读成「接口连不上」。
DS_TMP="$(mktemp -d 2>/dev/null)"
[ -n "${DS_TMP}" ] && [ -d "${DS_TMP}" ] || fail "建不了临时目录（mktemp -d 失败；${TMPDIR:-/tmp} 不可写或磁盘满）——**这不是『接口里没有这个 dataset』**，也不是接口的问题。下一步：检查 df -h 与 ${TMPDIR:-/tmp} 的权限后重跑置备（bash docker/scripts/superset-provision.sh）。"
trap 'rm -rf "${DS_TMP}"' EXIT
DS_BODY="${DS_TMP}/dataset-list.json"
DS_ERR="${DS_TMP}/dataset-parse.err"

DS_HTTP_CODE="$(curl -s -m 20 -o "${DS_BODY}" -w '%{http_code}' \
  "http://${SUPERSET_HOST}:${SUPERSET_PORT}/api/v1/dataset/?q=(order_columns:table_name,order_direction:asc)" \
  -H "Accept: application/json")"
curl_rc=$?
# 连响应都没取回来是**第三类**（不是「接口里没有」）：网络/端口/连接失败都在这里，
# 与「取到了但不是 JSON」分开报，否则又会被读成「看板里没这个 dataset」。
[ "${curl_rc}" -eq 0 ] || fail "没能取回 dataset 列表响应：curl 退出码 ${curl_rc}（HTTP ${DS_HTTP_CODE}）——**这不是『接口里没有这个 dataset』**，是 Superset 接口根本连不上。下一步：curl -sv 'http://${SUPERSET_HOST}:${SUPERSET_PORT}/health' 看服务是否在跑（docker compose ps superset），不对就先 make init 把第三段拉起来。"

# 解析只做「JSON → 标识符」的映射，**绝不**把失败折叠成「没找到」：每类失败吐一条
#   机读标签（TAB 分隔），由下面的 case 分流成「确实没有」与「没能解析出目标」。
parse_out="$("${PY_BIN}" - "${DS_BODY}" "${DS_HTTP_CODE}" 2>"${DS_ERR}" <<'PY'
import json
import sys


def emit_and_stop(tag, detail=""):
    """打印一条机读标签并**立刻结束**（本段只做「JSON → 一个标签」的映射）。"""
    print(tag + ("\t" + detail if detail else ""))
    raise SystemExit(0)


try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        doc = json.load(fh)
except OSError as exc:
    emit_and_stop("READ_ERROR", str(exc))
except json.JSONDecodeError as exc:
    emit_and_stop("BAD_JSON", str(exc))

if not isinstance(doc, dict):
    emit_and_stop("STRUCT", "顶层不是 JSON 对象（是 " + type(doc).__name__ + "）")
if "result" not in doc:
    if "message" in doc:
        emit_and_stop("API_ERROR", str(doc["message"]))
    emit_and_stop("STRUCT", "响应里没有 result 键（顶层键：" + ",".join(sorted(map(str, doc))) + "）")
rows = doc["result"]
if not isinstance(rows, list):
    emit_and_stop("STRUCT", "result 不是数组（是 " + type(rows).__name__ + "）")
for row in rows:
    # 旧代码是 r.get("table_name") ⇒ **别的行**缺键就只是跳过，不是失败。
    # 正常路径必须与施工前行为一致：只有**目标行**结构不对才报 STRUCT。
    if not isinstance(row, dict) or row.get("table_name") != "kpi_current":
        continue
    if "id" not in row:
        emit_and_stop(
            "STRUCT",
            "kpi_current 那一行没有 id 键（行内键：" + ",".join(sorted(map(str, row))) + "）",
        )
    ident = row["id"]
    if (
        isinstance(ident, bool)
        or not isinstance(ident, (int, str))
        or str(ident).strip() == ""
    ):
        emit_and_stop(
            "STRUCT",
            "kpi_current 的 id 不可用（" + repr(ident) + "，类型 " + type(ident).__name__ + "）",
        )
    # 与施工前逐字一致：旧代码就是 str(r["id"])，不做 strip（带空白的 id 也原样用）。
    emit_and_stop("OK", str(ident))
emit_and_stop("NOT_FOUND", "接口返回了 " + str(len(rows)) + " 个 dataset")
PY
)"
parse_rc=$?
parse_err="$(cat "${DS_ERR}")"

ds_tag="" ds_detail=""
IFS=$'\t' read -r ds_tag ds_detail <<< "${parse_out}" || true

# 「重跑置备」是多数分支共用的下一步 ⇒ 只写一次（各臂引用它）。
PROVISION_HINT="重跑置备（bash docker/scripts/superset-provision.sh，或 make init 第三段增补）"
PROBE_HINT="先看解释器是否可用：'${PY_BIN}' -c 'print(1)' 应打印 1"
LIST_URL="http://${SUPERSET_HOST}:${SUPERSET_PORT}/api/v1/dataset/?q=(order_columns:table_name,order_direction:asc)"

case "${ds_tag}" in
  OK)
    DS_ID="${ds_detail}"
    ;;
  NOT_FOUND)
    fail "找不到 kpi_current 的 dataset id（${ds_detail}，其中没有 table_name=kpi_current 的那一行）——**接口是通的、响应是合法 JSON，是这个 dataset 确实不在接口返回的列表里**。下一步：①先确认置备跑过——${PROVISION_HINT}；kpi_current 的 dataset 由它创建；②确认看板连的是哪个 database（冻结的 ECOM_ADS_UUID 那一个）；③确认没有别的东西删掉了它（docker compose logs superset | grep -i dataset）。"
    ;;
  BAD_JSON)
    fail "没能从 dataset 列表响应里解析出目标：响应体不是合法 JSON（${ds_detail}；HTTP ${DS_HTTP_CODE}）——**这不是『接口里没有这个 dataset』**，是这一步**根本没拿到可解析的内容**（空响应、返回了 HTML 登录页、或中间有代理改写）。下一步：先看接口到底回了什么：curl -s '${LIST_URL}' | head -c 300；回 HTML/空 ⇒ 查匿名访问与 403 授权（docker/scripts/superset-provision.py 的 Public 角色），然后${PROVISION_HINT}。"
    ;;
  READ_ERROR)
    fail "没能读出 dataset 列表的响应体：${ds_detail}——**这不是『接口里没有这个 dataset』**，是临时文件读写出了问题（磁盘满 / TMPDIR 不可写）。下一步：检查 df -h 与 ${TMPDIR:-/tmp} 的权限，然后${PROVISION_HINT}。"
    ;;
  API_ERROR)
    fail "dataset 列表接口回了 error message：${ds_detail}（HTTP ${DS_HTTP_CODE}）——**这是接口在报错，不是 dataset 不存在**。下一步：按 message 处置（401/403 多半是匿名授权或会话失效 ⇒ 跑置备里的 Public 授权段；5xx 看 docker compose logs superset），然后${PROVISION_HINT}。"
    ;;
  STRUCT)
    fail "没能从 dataset 列表响应里解析出目标：响应的**结构变了**——${ds_detail}（HTTP ${DS_HTTP_CODE}）——接口通了、也是合法 JSON，但取值路径对不上，**这不是『接口里没有这个 dataset』**。下一步：先看真实形状：curl -s '${LIST_URL}' | head -c 300（Superset 6.1.0 应为 {\"result\": [{\"id\": …, \"table_name\": …}]}）；形状确有不同 ⇒ 按实际结构改本脚本「解析 kpi_current dataset id（不硬编码）」那一段的 heredoc；形状没变而这里报结构错 ⇒ 是脚本缺陷，把上面的行内键报错一并上报。"
    ;;
  *)
    case "${parse_rc}" in
      126|127)
        fail "解析器无法执行：'${PY_BIN}' 不在 PATH 上或不可执行（bash rc=${parse_rc}）——**这不是『接口里没有这个 dataset』**，是解析器根本没起来（impl-05 实测 WSL 宿主只有 python3，没有 python 这个命令）。下一步：装 python3（apt-get install -y python3），或用 export PYTHON=/path/to/python3 指定绝对路径后${PROVISION_HINT}。"
        ;;
      *)
        fail "没能从 dataset 列表响应里解析出目标：解析没有任何机读输出（rc=${parse_rc}；HTTP ${DS_HTTP_CODE}）——**这不是『接口里没有这个 dataset』**，也不是「列表里没有这张表」。解析器 stderr：${parse_err:-<空>}。下一步：${PROBE_HINT}（**存在但不可用的空壳解释器**正是这种「退非 0、零输出、零 stderr」形态）；解析器可用则 stderr 里的 traceback 是解析段的缺陷，按它改本脚本的 heredoc；然后${PROVISION_HINT}。"
        ;;
    esac
    ;;
esac
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
