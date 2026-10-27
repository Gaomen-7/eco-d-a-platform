#!/usr/bin/env bash
# deploy-job.sh —— 构建并提交 Flink 作业（P1 实时作业）。
#
# ★ 为什么需要这个脚本（2026-10-27 补）：
#   `make init` **不**构建也不提交 Flink 作业——它只做「拉起容器 + 建库建表 +
#   置备看板」四段。作业 jar 的编译与 `flink run` 提交此前**只存在于被 gitignore
#   的 `.uv-cache/deploy-job.sh`**（不在版本库里）⇒ 换机部署时这一步是**隐形缺口**：
#   `make verify` 的第 2 项（flink-job-running）必然 FAIL，而原因不出现在任何
#   README 或 Makefile 里。
#   ⇒ 本脚本把那一步**固化成入库的、可复现的**执行面。
#
# ★ 三条承重纪律（每一条都对应一次实机证伪，改动前先读 deploy_surface_check.sh ④）：
#
#   ① 作业 jar **只能**放 /opt/flink/usrlib/，**不得**放 /opt/flink/lib/。
#      根因（impl-06d 实测）：作业 jar 是 shade jar，**内含 org/apache/kafka/**，
#      而 flink-dist-2.2.1.jar 不含 ⇒ 父子类加载器各加载一份 Kafka 的
#      OffsetResetStrategy ⇒ 同名类来自两个加载器 ⇒
#        LinkageError: loader constraint violation … committedOffsets
#      usrlib 走**用户类加载器**，与父子关系不冲突。
#
#   ② **两端都要拷**，且必须**同一字节**。只给 JM 拷 = 半部署：JM 提交作业，
#      TM 执行作业，TM 侧没有 jar 就加载不到作业类。
#
#   ③ lib/ 里若**曾经**有过作业 jar 副本，必须让新 JVM 起来（docker restart），
#      否则 JVM 会命中已缓存的类 ⇒ 报的还是同一个 LinkageError。
#      本脚本默认**先清干净再拷**，并用 --restart 参数显式重启（幂等、代价可控）。
#
# 用法：
#   bash scripts/deploy-job.sh              # 构建（若无 jar）+ 提交 + 等 RUNNING
#   bash scripts/deploy-job.sh --no-build   # 跳过 mvn package（jar 已在）
#   bash scripts/deploy-job.sh --restart    # 额外重启 JM/TM（新 JVM，清类缓存）
#
# 退出码：0 = 作业已 RUNNING；1 = 失败（每种失败都打印真因，不折叠成同一句话）。

set -uo pipefail

# ── 定位仓库根（脚本可能从任意 cwd 调用）──────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT" || { echo "FATAL: 无法进入仓库根 $REPO_ROOT" >&2; exit 1; }

JAR="flink/target/ecom-flink-0.1.0-SNAPSHOT.jar"
JAR_NAME="ecom-flink.jar"
JM_CONTAINER="eco-flink-jm"
TM_CONTAINER="eco-flink-tm"
JOB_CLASS="com.ecom.flink.P1RealtimeJob"
FLINK_UI="http://127.0.0.1:8081"

DO_BUILD=1
DO_RESTART=0
for arg in "$@"; do
  case "$arg" in
    --no-build)  DO_BUILD=0 ;;
    --restart)   DO_RESTART=1 ;;
    -h|--help)   sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "FATAL: 未知参数 $arg（可用：--no-build / --restart）" >&2; exit 1 ;;
  esac
done

step() { echo "[deploy-job] ▶ $*"; }
fail() { echo "[deploy-job] ✗ $*" >&2; exit 1; }
info() { echo "[deploy-job]   · $*"; }

# ── 前置：容器在跑 ────────────────────────────────────────────────────────
step "前置检查：$JM_CONTAINER / $TM_CONTAINER 是否在运行"
for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
  state="$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null)"
  [ "$state" = "running" ] || fail "容器 $c 未运行（state=${state:-<不存在>}）——先跑 make init"
done
info "两个容器均在运行"

# ── 前置：jar 存在 ───────────────────────────────────────────────────────
if [ "$DO_BUILD" -eq 1 ]; then
  step "构建作业 jar（mvn -DskipTests package）"
  # ⚠️ -Djava.net.preferIPv4Stack=true：WSL 的 resolv.conf 指向 172.17.32.1，
  #   repo.maven.apache.org **只解析出 IPv6** 而本机 IPv6 不通 ⇒
  #   "Temporary failure in name resolution"（不是 404）。详见 scripts/build-flink.sh 头注。
  if ! command -v mvn >/dev/null 2>&1; then
    fail "找不到 mvn —— 构建 Flink 作业需要 JDK 17 + Maven（无 sudo 装法见 scripts/p0-wsl-install-nodeb.sh + scripts/p0-wsl-fix-symlinks.sh）"
  fi
  mvnp="$(command -v mvn)"
  ( cd flink && MAVEN_OPTS="${MAVEN_OPTS:--Djava.net.preferIPv4Stack=true}" \
      "$mvnp" -DskipTests package 2>&1 | tail -30 )
  rc=${PIPESTATUS[0]}
  [ "$rc" -eq 0 ] || fail "mvn package 失败（rc=$rc）——见上方最后 30 行输出"
fi

[ -s "$JAR" ] || fail "作业 jar 不存在：$JAR（构建失败或 --no-build 时它还没被构建过）"
info "作业 jar：$JAR（$(wc -c < "$JAR" | tr -d ' ') 字节）"

# ── ① 清干净 lib/ 与 usrlib/ 里的作业 jar ─────────────────────────────────
step "① 清理两个容器里的作业 jar（lib/ 与 usrlib/ 都清）"
for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
  docker exec "$c" bash -c "rm -f /opt/flink/lib/$JAR_NAME /opt/flink/usrlib/$JAR_NAME"
done
for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
  n="$(docker exec "$c" bash -c "ls /opt/flink/lib/*ecom* /opt/flink/usrlib/*ecom* 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \r')"
  info "$c 侧剩余作业 jar = ${n:-?}"
done

# ── ② （可选）重启 JM/TM：新 JVM 不带任何缓存类 ─────────────────────────
if [ "$DO_RESTART" -eq 1 ]; then
  step "② 重启 JM/TM（--restart：新 JVM 不会命中已缓存的类）"
  docker restart "$JM_CONTAINER" "$TM_CONTAINER" >/dev/null
  for i in $(seq 1 90); do
    if curl -sf -m 2 "$FLINK_UI/overview" >/dev/null 2>&1; then
      info "JM 就绪（${i} 次探测后）"
      break
    fi
    sleep 2
  done
  curl -sf -m 2 "$FLINK_UI/overview" >/dev/null 2>&1 \
    || fail "JM 重启后 180 s 内未就绪——先看 docker compose logs $JM_CONTAINER"
fi

# ── ③ 拷到两端 usrlib（同一字节）──────────────────────────────────────────
step "③ 拷贝作业 jar 到两端 /opt/flink/usrlib/"
for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
  docker exec "$c" mkdir -p /opt/flink/usrlib
  docker cp "$JAR" "$c":"/opt/flink/usrlib/$JAR_NAME" || fail "docker cp 到 $c 失败"
done
# 逐字比对两端 size + sha256：只拷一端 = 半部署（判据④的阳性面）
jm_sig="$(docker exec "$JM_CONTAINER" bash -c "stat -c '%s' /opt/flink/usrlib/$JAR_NAME && sha256sum /opt/flink/usrlib/$JAR_NAME | cut -d' ' -f1" 2>/dev/null | tr '\n' ':')"
tm_sig="$(docker exec "$TM_CONTAINER" bash -c "stat -c '%s' /opt/flink/usrlib/$JAR_NAME && sha256sum /opt/flink/usrlib/$JAR_NAME | cut -d' ' -f1" 2>/dev/null | tr '\n' ':')"
info "JM 侧 size+sha256 = ${jm_sig}"
info "TM 侧 size+sha256 = ${tm_sig}"
[ -n "$jm_sig" ] && [ "$jm_sig" = "$tm_sig" ] \
  || fail "两端 usrlib 的作业 jar 不是同一字节（JM=[${jm_sig}] TM=[${tm_sig}]）——只拷一端 = 半部署"

# ── ④ 提交作业 ────────────────────────────────────────────────────────────
step "④ 提交作业（flink run -d -c $JOB_CLASS）"
submit_out="$(docker exec "$JM_CONTAINER" /opt/flink/bin/flink run -d \
  -c "$JOB_CLASS" "/opt/flink/usrlib/$JAR_NAME" 2>&1)"
submit_rc=$?
printf '%s\n' "$submit_out" | grep -vE "^WARNING|Unknown module" | tail -12
[ "$submit_rc" -eq 0 ] || fail "flink run 退出 $submit_rc —— 作业提交失败（见上方输出；常见原因：作业 jar 放错了目录）"

# ── ⑤ 等 RUNNING ──────────────────────────────────────────────────────────
step "⑤ 等待作业 RUNNING（最多 200 s）"
ok=0
for i in $(seq 1 40); do
  out="$(curl -sf -m 3 "$FLINK_UI/jobs/overview" 2>/dev/null)"
  case "$out" in
    *'"state":"RUNNING"'*) echo "[deploy-job] ✓ RUNNING（$((i * 5)) s）"; ok=1; break ;;
  esac
  sleep 5
done
if [ "$ok" -ne 1 ]; then
  fail "作业未在 200 s 内达到 RUNNING。/jobs/overview 最后读到：${out:-<REST 不可达>}。"
fi

# ── ⑥ 等消费组追平（lag=0）────────────────────────────────────────────────
step "⑥ 等待消费组追平（六分区 lag=0，最多 300 s）"
for i in $(seq 1 60); do
  lag_total="$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group ecom-p1-consumer 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
  info "lag_total=${lag_total:-?}（$((i * 5)) s）"
  if [ "${lag_total:-999}" = "0" ]; then
    echo "[deploy-job] ✓ 消费组已追平（lag=0）"
    break
  fi
  sleep 5
done

echo "[deploy-job] 完成：作业已 RUNNING"
