#!/usr/bin/env bash
# Doris 断言 —— 经 compose 网内的 `client` 服务发起（票面 ③ 的第二条）。
#
# 两条判据都**不是**「配置可见」（spec §4.3 明确反对那种假通过）：
#   1. `ads_window_metric` 真有行 —— 查 COUNT(*)，不是查 FE 健康
#   2. `ads_funnel_step` 真有行 —— 同上
#
# ★ 为什么必须经 client 发起（HLD §4.4，P0 实测）：
#   Doris 的客户端交互会从 FE **重定向到某个 BE**（第二跳是容器地址），
#   Docker Desktop 下宿主**不可路由**到容器地址 ⇒ 宿主侧 mysql 客户端 follow 不到。
#   官方原话：a stream load from the host cannot follow FE's redirect to a BE;
#            run it from the `client` service instead, which is inside the network.
#
# 用法：由 Makefile 的 verify 目标调用，或手工 `bash docker/scripts/doris-assert.sh`。
set -uo pipefail

FE_HOST="${FE_HOST:-doris-fe}"
FE_PORT="${FE_PORT:-9030}"
FE_USER="${FE_USER:-root}"
DB="${DB:-ecom_ads}"

# 判据表：表名 + 期望「至少有一行」。
TABLES=("ads_window_metric" "ads_funnel_step" "ads_late_expired_count")

step() { echo "[doris-assert] ▶ $*"; }
fail() { echo "[doris-assert] ✗ $*" >&2; exit 1; }

# 统一的查询执行器：全部经 client 服务，**不**在宿主直连。
# 用 `run --rm`（不常驻、不占常驻预算），并把 SQL 经 stdin 传入。
doris_query() {
  local sql="$1"
  docker compose run --rm -T client \
    mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -N --batch -e "$sql" 2>&1
}

step "前置：client 服务可拉起（profile client）"
if ! docker compose --profile client run --rm -T client \
      mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -N --batch \
      -e "SHOW FRONTENDS;" >/dev/null 2>&1; then
  fail "client 无法连通 FE ${FE_HOST}:${FE_PORT}（Doris 或 client 服务未就绪）"
fi

overall=0

# ── 判据 1：每张 ADS 表真有行 ────────────────────────────────────────
for table in "${TABLES[@]}"; do
  step "COUNT(*) FROM ${DB}.${table}"
  out="$(doris_query "SELECT COUNT(*) FROM ${DB}.${table};")"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "[doris-assert]   FAIL ${table} 查询失败：${out}" >&2
    overall=1
    continue
  fi
  # 去掉可能的表头/空白，取最后一个纯数字行
  count="$(printf '%s\n' "$out" | tr -d ' \r' | grep -E '^[0-9]+$' | tail -1)"
  if [ -z "$count" ]; then
    echo "[doris-assert]   FAIL ${table} 返回值不可解析：${out}" >&2
    overall=1
    continue
  fi
  if [ "$count" -gt 0 ]; then
    echo "[doris-assert]   PASS ${table} count=${count} vs 期望 >0"
  else
    echo "[doris-assert]   FAIL ${table} count=0 vs 期望 >0" >&2
    overall=1
  fi
done

# ── 判据 2：动态分区总开关真的开着（数据落在正确分区的前提）─────────
step "SHOW FRONTEND CONFIG LIKE 'dynamic_partition_enable'（期望 true）"
cfg="$(doris_query "SHOW FRONTEND CONFIG LIKE 'dynamic_partition_enable';")"
if printf '%s' "$cfg" | grep -qi 'true'; then
  echo "[doris-assert]   PASS dynamic_partition_enable=true"
else
  echo "[doris-assert]   FAIL dynamic_partition_enable 非 true：${cfg}" >&2
  overall=1
fi

if [ "$overall" -eq 0 ]; then
  echo "[doris-assert] 全部断言通过"
else
  echo "[doris-assert] 有断言失败" >&2
fi
exit "$overall"
