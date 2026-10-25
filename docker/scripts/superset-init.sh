#!/usr/bin/env bash
# Superset 引导 —— `make init` 的第 ④ 段（票面 ③，四段冻结）。
#
# 为什么单独成脚本而不是写进 Makefile：Makefile 的 `make init` 已有 ①②③ 段，
# heredoc 与长命令在里面会被 make 当成语法解析（config-check 那处已踩过），
# 且这几步一旦失败要能单独重跑（Superset 元数据库在卷里，重跑是幂等的）。
#
# 四个步骤的顺序是**冻结**的（官方 CLI 依赖顺序，不可交换）：
#   1. `superset db upgrade`   —— 建/迁移元数据库表
#   2. `fab create-admin`      —— 建管理员（**已存在时必须吞掉错误**，否则重跑失败）
#   3. `superset init`         —— 建默认角色/权限（Public 角色在此生成）
#   4. 健康判据               —— /health 响应体是**纯文本 OK**，不是 JSON（P0 实测）
#   （zip 导入不在本脚本：依赖 provision 段先建冻结 UUID 的 database，
#     见文件尾注与 superset-provision.sh ④）
#
# 依据：REQ-F-BI-03（零账号只读，靠 superset_config.py 的 AUTH_ROLE_PUBLIC）、
#       spec §3.3（四段冻结）、HLD §4（Superset 元数据库走 SQLite，镜像不带驱动）。
set -uo pipefail

SUPERSET_HOST="${SUPERSET_HOST:-127.0.0.1}"
SUPERSET_PORT="${SUPERSET_PORT:-8088}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${SUPERSET_ADMIN_EMAIL:-admin@example.com}"

step() { echo "[superset-init] ▶ $*"; }
fail() { echo "[superset-init] ✗ $*" >&2; exit 1; }

# ── 前置：容器在跑（由 make init 的 up -d --wait 保证）────────────────
step "确认 eco-superset 容器在运行"
if ! docker compose ps --status running --format '{{.Name}}' 2>/dev/null | grep -qx 'eco-superset'; then
  fail "eco-superset 未运行 —— 先执行 make init 的前三段"
fi

# ── ① db upgrade ─────────────────────────────────────────────────────
step "superset db upgrade（建元数据库表）"
if ! docker compose exec -T superset superset db upgrade; then
  fail "db upgrade 失败"
fi

# ── ② create-admin（幂等：已存在则吞错）───────────────────────────────
step "superset fab create-admin（已存在则跳过）"
docker compose exec -T superset \
  superset fab create-admin \
    --username "$ADMIN_USER" \
    --firstname Superset \
    --lastname Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" 2>&1 || step "  （管理员已存在，按幂等继续）"

# ── ③ superset init ──────────────────────────────────────────────────
step "superset init（建默认角色与权限）"
if ! docker compose exec -T superset superset init; then
  fail "superset init 失败"
fi

# ── ④ 健康判据（纯文本 OK，不是 JSON —— P0 实测）────────────────────
step "健康断言：GET /health 应返回纯文本 OK"
ok=0
for i in $(seq 1 40); do
  body="$(curl -sf -m 3 "http://${SUPERSET_HOST}:${SUPERSET_PORT}/health" 2>/dev/null || true)"
  if [ "$body" = "OK" ]; then
    echo "[superset-init] ✓ /health 返回 OK（第 ${i} 次探测）"
    ok=1
    break
  fi
  echo "[superset-init]   等待 Superset 就绪 ... ($i/40)"
  sleep 3
done
[ "$ok" -eq 1 ] || fail "Superset /health 在 120 s 内未返回 OK（超时 120 s 为设计值）"

# ── ⑤ zip 导入说明（impl-04 票面 ⑥）─────────────────────────────────
# ★ code-review 修复（2026-10-25）：导入段已**挪出本脚本**——importer 按
#   database_uuid 匹配 database（逐字 config["database_id"] = database_ids[
#   config["database_uuid"]]），而冻结 UUID 的 database 由 provision 段创建；
#   本脚本跑在 provision **之前**，全新元数据卷上带 zip 导入必 KeyError。
#   zip 的导入/导出都归 superset-provision.sh（置备后），本脚本只负责引导。

step "完成"
