#!/usr/bin/env bash
# run_assert.sh <label> [lake_assert.sh 参数...] — 跑一次 lake_assert.sh，
# RC 打印到 stdout（不透传，便于编排脚本读行）；完整输出留档
# artifacts/impl-03e/<label>.log。
set -uo pipefail
# ★ 2026-10-24 修：原 `cd "$(dirname "$0")/../.."` 只在 artifacts/impl-03e/ 副本下
#   落到仓库根；从 evidence/impl-03e/（深一级）调用时落在 .scratch/<feature>/，
#   重定向 artifacts/impl-03e/<label>.log 直接 ENOENT。改为向上找仓库根
#   （含 Makefile 与 scripts/ 的目录），两处副本行为一致。
d="$(cd "$(dirname "$0")" && pwd)"
while [ "$d" != "/" ] && { [ ! -f "$d/Makefile" ] || [ ! -d "$d/scripts" ]; }; do
  d="$(dirname "$d")"
done
[ "$d" != "/" ] || { echo "ASSERT ✗ 找不到仓库根（Makefile/scripts 均缺席）" >&2; exit 2; }
cd "$d" || exit 2

label="$1"; shift
log="artifacts/impl-03e/$label.log"

# JM 作业历史积压防 OOM（impl-03d 登记的环境性现象）
n=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | grep -o '"jid"' | wc -l)
if [ "$n" -gt 140 ]; then
  echo "ASSERT[$label] -- JM 作业历史 ${n} 条 > 140，重启 eco-flink-jm（环境性操作，非断言行为）"
  docker restart eco-flink-jm >/dev/null 2>&1
  for _ in $(seq 1 40); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' eco-flink-jm 2>/dev/null)" = "healthy" ] && break
    sleep 5
  done
fi

bash scripts/lake_assert.sh "$@" > "$log" 2>&1
rc=$?
echo "ASSERT[$label] RC=$rc"
tail -n 30 "$log"
exit 0
