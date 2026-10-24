#!/usr/bin/env bash
# make_derived.sh — 从 sql/assert_lake.sql 派生 F5/F6 两个判定面反例文件。
#   F5 = 原文件 + 1 条必然报错的语句（在 USE ecom_lake; 后）⇒ 哨兵应全 ok=true
#        但 stdout 有一条 SQL 错误标记 —— 证明判定面「无错误标记」这一层不是摆设。
#   F6 = 原文件 − A1_split_complete 哨兵语句块（整条删掉，不是改成 ok=false）
#        ⇒ 无报错、其余 8 条哨兵 ok=true，但 A1 行整条缺席 —— 证明判定是
#        「必须出现且为 true」，不是「没有 ok=false 就过」。
# 生成方式全部走 diff 可复核：生成后立即 diff 给人看。
# ★ 实测修正（2026-10-24）：报错语句放在中间会让 sql-client **中止整个脚本**
#   （stdout 逐字：[ERROR] … not found 后直接 Shutting down the session）⇒ 哨兵全缺席。
#   票面 F5 要求「哨兵仍全 ok=true」，故报错语句必须放在**末尾**。
#   （报错在中间的形态已单独留档 f5_derived_mid.out，作为「中止」事实的证据。）
set -euo pipefail
# ★ 2026-10-24 修：同 run_assert.sh —— 原 `cd "$(dirname "$0")/../.."` 只在
#   artifacts/impl-03e/ 副本下落到仓库根；从 evidence/impl-03e/ 调用时落在
#   .scratch/<feature>/，读不到 sql/assert_lake.sql。改为向上找仓库根。
d="$(cd "$(dirname "$0")" && pwd)"
while [ "$d" != "/" ] && { [ ! -f "$d/Makefile" ] || [ ! -d "$d/scripts" ]; }; do
  d="$(dirname "$d")"
done
[ "$d" != "/" ] || { echo "✗ 找不到仓库根（Makefile/scripts 均缺席）" >&2; exit 2; }
cd "$d"

awk '
  /^USE ecom_lake;$/ && !done {
    print
    print "SELECT '\''FIXTURE f5_derived BEGIN'\'' AS fx;"
    done = 1
    next
  }
  { print }
' sql/assert_lake.sql > artifacts/impl-03e/f5_derived.sql
# 报错语句放末尾：哨兵全部执行完之后才报错
printf "SELECT * FROM no_such_table_f5;\n" >> artifacts/impl-03e/f5_derived.sql

awk '
  /^USE ecom_lake;$/ && !m {
    print
    print "SELECT '\''FIXTURE f6_derived BEGIN'\'' AS fx;"
    m = 1
    next
  }
  /^SELECT '\''A1_split_complete'\'' AS assertion,/ { skip = 1 }
  skip && /END AS ok;$/ { skip = 0; next }
  !skip { print }
' sql/assert_lake.sql > artifacts/impl-03e/f6_derived.sql

echo "=== F5 派生 diff（应恰为 marker + 报错语句两行新增）==="
diff sql/assert_lake.sql artifacts/impl-03e/f5_derived.sql || true
echo
echo "=== F6 派生 diff（应恰为 marker 一行新增 + A1 哨兵语句块删除）==="
diff sql/assert_lake.sql artifacts/impl-03e/f6_derived.sql || true
echo
echo "=== F6 复核：A1 哨兵不得再以语句形式出现 ==="
grep -n "SELECT 'A1_split_complete'" artifacts/impl-03e/f6_derived.sql && { echo "✗ A1 语句仍在"; exit 1; } || echo "✓ A1 语句已删除（只剩注释提及）"
