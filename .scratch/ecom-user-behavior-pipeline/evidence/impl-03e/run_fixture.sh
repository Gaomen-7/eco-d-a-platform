#!/usr/bin/env bash
# run_fixture.sh <name> [expect-error] — 把 artifacts/impl-03e/<name>.sql 灌进容器执行。
# 判据：docker exec rc=0 + stdout 出现 `FIXTURE <name> BEGIN` marker（证明灌的是
# 本次文件、不是上一轮遗留的 /tmp 残骸）+ 除非 expect-error，不得出现错误标记。
# 输出留档 artifacts/impl-03e/<name>.out。
#
# ★ 2026-10-24 修一处误报：原 marker 检查写成 `printf "%s" "$clean" | grep -q …`，
#   在 `set -o pipefail` 下，grep -q 命中即退出 ⇒ 大输出（>管道缓冲）时 printf 收
#   SIGPIPE(141) ⇒ 管道整体 141 ⇒ `if ! …` 判成「marker 缺失」。
#   实测症状：f5_derived.out 里 marker 行干净存在（`112:| FIXTURE f5_derived BEGIN |`），
#   同一检查事后复测却通过。⇒ 改为纯 bash `case` 匹配，不再走管道。
set -uo pipefail
# ★ 2026-10-24 修：同 run_assert.sh —— 相对 cd 只适配 artifacts/ 副本；
#   从 evidence/ 副本调用会读错 sql/out 路径。改为向上找仓库根。
d="$(cd "$(dirname "$0")" && pwd)"
while [ "$d" != "/" ] && { [ ! -f "$d/Makefile" ] || [ ! -d "$d/scripts" ]; }; do
  d="$(dirname "$d")"
done
[ "$d" != "/" ] || { echo "FX ✗ 找不到仓库根（Makefile/scripts 均缺席）" >&2; exit 2; }
cd "$d" || exit 2

name="$1"
# ★ 2026-10-24 修（code-review ③）：夹具先找**本脚本所在目录**（入库副本），
#   再退回 artifacts/impl-03e/（gitignore 的工作副本）。原版只读 artifacts/，
#   新克隆上 evidence/ 里的复跑命令找不到夹具。
here="$(cd "$(dirname "$0")" && pwd)"
sql="$here/$name.sql"
[ -f "$sql" ] || sql="artifacts/impl-03e/$name.sql"
out="artifacts/impl-03e/$name.out"
[ -f "$sql" ] || { echo "FX[$name] ✗ fixture 不存在：$sql" >&2; exit 2; }

# JM 作业历史积压防 OOM（impl-03d 登记的环境性现象：数百作业后 heap OOM）
n=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | grep -o '"jid"' | wc -l)
if [ "$n" -gt 140 ]; then
  echo "FX[$name] -- JM 作业历史 ${n} 条 > 140，重启 eco-flink-jm（环境性操作，非断言行为）"
  docker restart eco-flink-jm >/dev/null 2>&1
  for _ in $(seq 1 40); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' eco-flink-jm 2>/dev/null)" = "healthy" ] && break
    sleep 5
  done
fi

docker exec -i eco-flink-jm bash -c 'cat > /tmp/fx.sql && /opt/flink/bin/sql-client.sh -f /tmp/fx.sql' \
  < "$sql" > "$out" 2>&1
rc=$?
clean=$(sed "s/$(printf "\033")\[[0-9;]*m//g" "$out")
if [ "$rc" -ne 0 ]; then
  echo "FX[$name] ✗ docker exec rc=$rc（容器/挂载层失败）" >&2
  exit 1
fi
found=0
case "$clean" in *"FIXTURE $name BEGIN"*) found=1 ;; esac
if [ "$found" -ne 1 ]; then
  echo "FX[$name] ✗ BEGIN marker 缺失（可能执行的是旧 /tmp 残留文件）" >&2
  exit 1
fi
has_err=0
case "$clean" in *"[ERROR]"*) has_err=1 ;; esac
if [ "${2:-}" != "expect-error" ] && [ "$has_err" -ne 0 ]; then
  echo "FX[$name] ✗ 出现 SQL 错误标记（fixture 语句报错 ⇒ 破坏可能没生效）" >&2
  exit 1
fi
echo "FX[$name] ✓ rc=0 marker=ok error=${has_err}（详见 $out）"
exit 0
