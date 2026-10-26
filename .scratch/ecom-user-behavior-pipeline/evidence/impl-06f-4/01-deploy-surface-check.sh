#!/usr/bin/env bash
# impl-06f-4 判据链第一环：**不设 COMPOSE_EXTRA_FILES** 跑部署面全判据（11 条）。
# 执行面：WSL（脚本头注要求）。副作用：条目 ③ 会 down（不带 -v）+ up -d，
# 全部容器重启、RUNNING 的 Flink 作业被杀（跑前作业状态见 00-preconditions）。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/01-deploy-surface-check.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

# 票面要求：不设任何端口 override。显式 unset（防外层 shell 残留）。
unset COMPOSE_EXTRA_FILES
echo "COMPOSE_EXTRA_FILES=[${COMPOSE_EXTRA_FILES:-}]（期望为空：本票不设 override）"
echo "START_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
start=$(date +%s)

bash scripts/deploy_surface_check.sh
rc=$?

end=$(date +%s)
echo "EXIT_CODE=$rc"
echo "ELAPSED_SECONDS=$((end - start))"
echo "END_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
exit $rc
