#!/usr/bin/env bash
# impl-06i 改后函数级冒烟（一次性，gitignore 覆盖区 .scratch/tmp/）：
# 从改后的 scripts/deploy_surface_check.sh **逐字抽取** ⑤ 的真实函数体与输出助手，
# 桩掉全局计数器后直接调用 —— 干净树必须 PASS；sql/ 注入真 ADD JAR 必须 FAIL；
# scripts/*.sh 注入宿主路径 jar（无 ADD JAR）必须 FAIL（seg2 腿）；还原后 PASS。
# 这是全门实跑前的快速预检，不替代全门证据（全门证据见 02/03/04）。
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

EXTRACT=/tmp/impl06i-extracted-5.sh
{
  sed -n '/^hr()/,/^info()/p' scripts/deploy_surface_check.sh
  sed -n '/^check_no_add_jar_host_path()/,/^}/p' scripts/deploy_surface_check.sh
} > "$EXTRACT"
echo "抽取行数: $(wc -l < "$EXTRACT")"
# 语法自检：抽取结果必须可解析
bash -n "$EXTRACT" || { echo 'FATAL: 抽取片段语法错误'; exit 2; }
# 全脚本语法自检
bash -n scripts/deploy_surface_check.sh || { echo 'FATAL: deploy_surface_check.sh 语法错误'; exit 2; }
echo '语法自检：bash -n 全脚本与抽取片段均通过'

PASS_COUNT=0; FAIL_COUNT=0; FAILED_ITEMS=()
source "$EXTRACT"

reset_counters() { PASS_COUNT=0; FAIL_COUNT=0; FAILED_ITEMS=(); }

echo
echo '=== T0 干净树（期望 PASS，rc=0）==='
reset_counters
check_no_add_jar_host_path; rc=$?
echo "T0 rc=$rc PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
[[ $rc -eq 0 ]] || { echo 'T0 未过，中止'; exit 1; }

echo
echo '=== T1 注入 sql/zz_impl06i_smoke.sql：真 ADD JAR 宿主路径（期望 FAIL，rc=1）==='
printf "ADD JAR '/mnt/f/zz/fake.jar';\n" > sql/zz_impl06i_smoke.sql
reset_counters
check_no_add_jar_host_path; rc=$?
echo "T1 rc=$rc PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
rm -f sql/zz_impl06i_smoke.sql
[[ $rc -eq 1 ]] || { echo 'T1 未抓到真 ADD JAR —— 收窄把真缺陷放走了！'; exit 1; }

echo
echo '=== T2 注入 scripts/zz_impl06i_smoke.sh：宿主路径 jar 引用、无 ADD JAR（期望 FAIL，rc=1，走 seg2 腿）==='
printf 'HOST_JAR=/mnt/f/zz/fake.jar\n' > scripts/zz_impl06i_smoke.sh
reset_counters
check_no_add_jar_host_path; rc=$?
echo "T2 rc=$rc PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
rm -f scripts/zz_impl06i_smoke.sh
[[ $rc -eq 1 ]] || { echo 'T2 未抓到宿主路径 jar 引用 —— seg2 腿脱管！'; exit 1; }

echo
echo '=== T3 还原后干净树（期望 PASS，rc=0）==='
reset_counters
check_no_add_jar_host_path; rc=$?
echo "T3 rc=$rc PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
[[ $rc -eq 0 ]] || { echo 'T3 还原后未回绿'; exit 1; }

echo
echo '=== T4 探针残留核查（期望全部不存在）==='
ls sql/zz_impl06i_smoke.sql scripts/zz_impl06i_smoke.sh 2>&1 || true
echo 'SMOKE_ALL_PASS'
