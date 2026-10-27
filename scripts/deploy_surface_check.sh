#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
# deploy_surface_check.sh —— 部署面验收实证（impl-03a 票面 8 条 + 演进新增）
#
# ⚠️ 判据条数**不**以本条注释为准：先按「票面 8 条」建立，impl-03a 自己加了
#    4b/4c，2026-10-26 又按 impl-06d 实测把 ④ 从「jar 在两容器 + sha256」改成
#    「作业 jar 在 /opt/flink/usrlib/」并把原断言下移为 4d ⇒ 现在是 **11 条**
#    （①②③④ 4b 4c 4d ⑤⑥⑦⑧）。条数以 main() 里的调用清单为准。
#    票面 impl-03a §A 里写的「10 项判据」是当日快照，此后语义已变更。

#
# 定位：本脚本是本票的**证伪面**。票面判据本身可能**恒真**，恒真判据等于没判，
#       所以每条都设计成「构造反例也能被它抓到」的形式。
#
# ★★ 恒真风险与对策（为什么每条要这样证）★★
#
#  ① 具名卷同路径挂 JM/TM —— **恒真风险最高的一条**
#     反例：compose 里确实有两行 paimon-warehouse:/warehouse，但若两处 source
#     写的是**两个不同卷名**（或其中一个写成匿名卷/绑定挂载），docker compose
#     config 照样打印两行 volumes ⇒ 「compose 里有这一行」恒真、抓不到。
#     对策：断言拆两层 —— (a) config JSON 里两服务都必须有 source=paimon-warehouse
#     且 target=/warehouse；(b) **实机** docker inspect 各取 Mounts 中
#     Type=volume && Destination=/warehouse 的那条，比对两边的 **Name 逐字相等**
#     且 **Destination 逐字相等**。卷名不同 ⇒ Name 不等 ⇒ FAIL。
#
#  ② 跨容器互见（本票核心）—— 恒真风险次高
#     反例：两容器各挂一个**同名但独立**的卷（或各自独立的本地目录），
#     「文件存在」这条判据仍然成立 —— 因为**写探针的容器自己也能读回来**。
#     对策：探针必须**可区分**（文件名带 UTC 时间戳 + 随机数，内容带时间戳 +
#     随机数 + 主机名），且**写入方是 JM、读取方是 TM**，逐字比对。TM 读不到或
#     内容不符（哪怕只差一个字符）即 FAIL。绝不能只判「文件存在」。
#
#  ③ down（不带 -v）/ up 后数据仍在
#     反例：down 前在**容器可写层**（而非卷内）留了文件，up 后重建容器时它消失，
#     但若判据只比「探针路径存在」而不比对**内容**，仍可能误判。
#     对策：down 前把探针内容**备份到宿主**（down 之后卷内文件就取不到了，必须
#     先备份），up 后再读同一路径，与备份**逐字**比对。
#     ⚠️ 安全自检：本脚本**任何路径都不允许** -v / --volumes。down 的调用
#        被 compose_down_preserving_volumes() 包住，该函数在真正执行前会逐字
#        扫描参数，发现 -v/--volumes 即**拒绝执行并 FAIL**（防止后人误改）。
#
#  ④ 作业 jar 在 **/opt/flink/usrlib/**（impl-06d 实测口径，取代原「jar 在两容器
#     + sha256」判据）—— 恒真风险中等偏高
#     原判据**结构上抓不到真正的缺陷**：它只查 Paimon connector 在不在 lib/，
#     **完全不看作业 jar 放在哪个目录**。而 impl-06d 实测：作业 jar 放 lib/ 会
#     LinkageError: loader constraint violation … committedOffsets
#     （作业 shade jar 含 org/apache/kafka/**，flink-dist-2.2.1.jar 不含
#     ⇒ 父子类加载器各加载一份 Kafka 的 OffsetResetStrategy）。
#     反例（本条必须能抓到它）：usrlib 放一份、lib 里也留一份历史 docker cp 副本
#     ⇒ 原判据照样 PASS，缺陷原样放过。
#     对策：拆成阳性 + **阴性**两条一起判 ——
#       (a) 两端 /opt/flink/usrlib/ 各有一份 ecom 作业 jar，且两端**逐字节相同**
#           （size + sha256 逐字相等；只给一端拷过 = 半部署）；
#       (b) 两端 /opt/flink/lib/ **不许**有作业 jar 副本 —— 承重点，抓 null 形态。
#     Paimon connector 在两端 lib/ 的断言保留为 4d（部署面事实，语义不变）。
#
#  ⑤ 无 ADD JAR 指向宿主路径 —— 恒真风险：路径写错就扫不到
#     对策（2026-10-27 impl-06i 改口径，用户裁决方向 C）：**可执行面白名单**
#     grep（*.sql/*.sh/*.java/Makefile/Dockerfile/*.yml/*.yaml），零命中才 PASS；
#     并额外扫白名单内对宿主路径 jar 的引用。
#     旧「全仓 grep」口径被 32 行非 SQL 噪音打成恒假（impl-06f-4 实测）：
#     .mimosa/ 会话基线副本 27 行 + 守卫测试自身 docstring 4 行 + README 判据名
#     1 行 —— 判据对象是「可能出现 ADD JAR 语句的文件」，不是「工作树里的一切文件」。
#
#  ⑥ data/ 被 .gitignore 覆盖（C5）—— 门槛批 raw 数据落 data/，必须不入库
#     对策：git check-ignore -v 对 data/ 与 data/x **都要**命中
#     （只查目录不查其下文件是不够的：目录被忽略不等于其下文件被忽略）。
#
#  ⑦ ps 七服务全 healthy —— **判据坑**（impl-02e 验收时踩过）
#     反例：把「全 healthy」当成一个笼统判据 ⇒ 一次性服务 kafka-init 永远满足不了。
#     kafka-init 是 restart: "no" 的**一次性服务**，跑完即 Exit 0，且
#     **没有 healthcheck** ⇒ 它在 ps 里的 HEALTH 列是 <no value>，要求它
#     healthy 是**结构性不可能**（不是机器慢）。同理 client 是 profile 隔离的
#     按需容器，不计入常驻。
#     对策：拆成两条独立判据 ——
#       (i)  **常驻且有 healthcheck** 的服务（kafka / flink-jm / flink-tm /
#            superset / doris-fe / doris-be）必须 healthy；
#       (ii) kafka-init 必须**已成功退出**（State=exited, ExitCode=0）。
#     两者分开判、分别打印。
#
#  ⑧ 数字标注审查 —— 本仓写作风格：凡时间/容量/阈值数字，逐处标
#     官方给出 或 设计值，**不得把设计值当官方保证**。
#     对策：grep compose / Dockerfile 里新增的数字行，打印命中行供人复核。
#
# ──────────────────────────────────────────────────────────────────────────────
# 运行方式（★ 必须在 WSL 下跑 —— 宿主无 make，且 docker CLI 在 WSL 内）：
#   bash scripts/deploy_surface_check.sh
# 也可在 Windows 侧跑（脚本自带 MSYS_NO_PATHCONV=1 封装，见 dc()），
# 但**推荐 WSL**：down/up 会重建容器，Windows 侧路径转换更易出岔子。
#
# 退出码：0 = 全部判据 PASS；1 = 有 FAIL（供 make / CI 判）。
#
# ⚠️ 副作用声明：条目 ③ 会 docker compose down 再 up -d（**不带 -v**），
#    期间全部容器会重启。这是**票面要求的实证**，不是可选项。
#    down 不带 -v ⇒ 具名卷全部保留 ⇒ 其它票的数据不受损。
# ══════════════════════════════════════════════════════════════════════════════

# ⚠️ 只开 -uo pipefail，**不开 -e**：本脚本要收集全部条目的结果再统一退出，
#    任何一条命令失败都不能提前终止（否则后面的条目根本不会被执行）。
set -uo pipefail

# ── 定位仓库根（脚本可能从任意 cwd 调用）──────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT" || { echo "FATAL: 无法进入仓库根 $REPO_ROOT"; exit 1; }

# ── 常量（★ 逐处标来源）────────────────────────────────────────────────────
COMPOSE_FILE="docker-compose.yml"
# 卷名 / 容器内挂载点 / jar 名 / 容器名：全部来自 impl-03a 票面与 T1/T2 的约定。
VOLUME_NAME="paimon-warehouse"
WAREHOUSE_PATH="/warehouse"
PAIMON_JAR="paimon-flink-2.2-2.1.0.jar"
# jar 钉死值（设计值口径：本仓实测；Paimon 官方未公布该 artifact 校验值）。
# 与 scripts/fetch-paimon-jar.sh 及 docker/flink/Dockerfile 的 LABEL/RUN 三处一致。
EXPECTED_JAR_SIZE="57697871"
EXPECTED_JAR_SHA256="43a6416a241ab736fb6ed2b6061a37fc5cca5ba8737d77eac1b977f859107dd1"
JM_CONTAINER="eco-flink-jm"
TM_CONTAINER="eco-flink-tm"
KAFKA_INIT_CONTAINER="eco-kafka-init"
# 7 服务（票面判据）：默认 profile 5 + --profile doris 的 2 = 7。
# client 是 profile 隔离的**按需**容器，不计入常驻（票面逐字）。
PROFILE_ARGS=(--profile doris)

# 宿主侧探针备份目录（down 之后卷内取不到，必须先备份到宿主）
PROBE_BACKUP_DIR="${TMPDIR:-/tmp}/eco-deploy-surface-check"
mkdir -p "$PROBE_BACKUP_DIR" 2>/dev/null || true

# 本轮运行的三元组（时间戳 + 随机数）：**可区分**是本条判据的命门。
RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_NONCE="${RANDOM}${RANDOM}"
PROBE_NAME=".probe-${RUN_STAMP}-${RUN_NONCE}"
PROBE_PATH="${WAREHOUSE_PATH}/${PROBE_NAME}"

PASS_COUNT=0
FAIL_COUNT=0
FAILED_ITEMS=()

# ── docker 调用封装 ────────────────────────────────────────────────────────
# ★ MSYS_NO_PATHCONV=1 是 **Windows(Git Bash/MSYS) 侧必须**的：
#   否则 MSYS 会把以 / 开头的参数（如 /warehouse、/opt/flink/lib）当成路径
#   转换成 C:/Program Files/Git/warehouse 之类，docker 侧全部错位。
#   在 WSL/Linux 下该变量无害（无 MSYS 路径转换层）。
# COMPOSE_EXTRA_FILES（2026-10-22 新增，环境适配钩子，不改任何判据语义）：
#   用途：当宿主防火墙/WinNAT 抢占了冻结端口时（本机实测：WinNAT 保留
#   8024-8123 段 => 8030/8040/8081/8088 全部 bind 失败，且非管理员无法释放），
#   可以额外叠加一个只改宿主侧发布端口的 compose override 来跑完功能性实证。
#   该 override 不得改动：容器内端口、卷挂载、mem_limit、healthcheck、
#      镜像名 —— 否则验的就不是本票的东西了。它只能改 ports 的宿主侧数字。
#   端口恢复后必须不设该变量重跑一次，否则「冻结端口」这条不成立。
#   用法： COMPOSE_EXTRA_FILES="-f .scratch/tmp/compose.portfree.yaml" bash scripts/deploy_surface_check.sh
EXTRA_COMPOSE_ARGS=()
if [[ -n "${COMPOSE_EXTRA_FILES:-}" ]]; then
  read -r -a EXTRA_COMPOSE_ARGS <<< "$COMPOSE_EXTRA_FILES"
fi
dc() { MSYS_NO_PATHCONV=1 docker compose -f "$COMPOSE_FILE" "${EXTRA_COMPOSE_ARGS[@]}" "${PROFILE_ARGS[@]}" "$@"; }
dcx() { MSYS_NO_PATHCONV=1 docker "$@"; }

# ── Python 解释器解析（★ 实测踩到的坑，2026-10-22）─────────────────────────
# ★ 为什么不能直接写 python3：**Windows 侧（Git Bash/MSYS）的 python3 是
#   Microsoft Store 的占位 stub**（C:/Users/.../WindowsApps/python3），它
#   **不打印任何东西、直接 exit 49**。于是所有基于 python3 的判据会**静默拿到
#   空字符串**，然后被误判成「服务缺失 / State 为空」—— 判据恒假，比恒真更难发现。
#   实测（本机，Windows 侧）：
#     $ python3 -V   → rc=49, 无输出
#     $ python  -V   → Python 3.13.0
#   WSL 侧则正常（/usr/bin/python3 = 3.14.4）。
#   ⇒ 这里按序探测，**必须**用 -c 'print(1)' 真正验证它能执行并输出，
#     而不是只看 command -v（stub 也在 PATH 里，command -v 会命中）。
#   ⚠️ 解析失败即 FATAL 退出：绝不允许带着空解释器继续跑（那会让判据全部恒假）。
resolve_python() {
  local cand out
  for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
      out="$("$cand" -c 'print(1)' 2>/dev/null)"
      if [[ "$out" == "1" ]]; then
        printf '%s' "$cand"
        return 0
      fi
    fi
  done
  return 1
}
PY="$(resolve_python)" || {
  echo "FATAL: 找不到可用的 Python 解释器（python3 / python 均不可执行）。"
  echo "       注意：Windows 侧的 python3 可能是 Microsoft Store stub（exit 49、无输出）。"
  echo "       请在 WSL 内运行本脚本。"
  exit 1
}

# ── 输出小工具 ─────────────────────────────────────────────────────────────
hr()  { printf '%s\n' "────────────────────────────────────────────────────────────────"; }
sec() { hr; printf '▶ %s\n' "$1"; hr; }
pass() { PASS_COUNT=$((PASS_COUNT + 1)); printf '[PASS] %s\n' "$1"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); FAILED_ITEMS+=("$1"); printf '[FAIL] %s\n' "$1"; }
# 原始输出统一缩进，便于与结论行区分（票面要求「逐条打印原始输出」）
raw() { sed 's/^/    │ /'; }
info() { printf '  · %s\n' "$1"; }

# ==============================================================================
# ensure_stack_up：保证被测环境处于「已拉起」状态
# ==============================================================================
# 为什么需要（2026-10-22 实机踩到）：
#   ③ 会 docker compose down 再 up。若上一轮在 ③ 之后异常中断（或 ③ 自身 FAIL 提前
#   return），环境就停在「已 down」状态。下一次跑本脚本时，①/4b/②/④ 会全部以
#   「容器不存在」的形式 FAIL —— 那些是**环境未起**的噪音，不是部署面的缺陷，
#   会把真正的病因淹没。故开跑前先确保栈是 up 的。
#   只在必要时 up：已在跑就不动（避免打扰正在观察的人）。
ensure_stack_up() {
  local running
  running="$(dc ps --format '{{.Service}}' 2>/dev/null | grep -c . || true)"
  if [[ "${running:-0}" -ge 5 ]]; then
    info "被测栈已在运行（$running 个服务），跳过 up"
    return 0
  fi
  info "被测栈未起（仅 ${running:-0} 个服务），执行 up -d --wait"
  local out rc
  out="$(dc up -d 2>&1)"; rc=$?
  printf '%s
' "$out" | raw
  if [[ $rc -ne 0 ]]; then
    fail "前置 up：docker compose up -d 失败 rc=$rc（环境未起 ⇒ 下列各条判据均不成立）"
    return 1
  fi
  wait_for_healthy 600 || info "（健康等待未在超时内完成，仍继续 —— 由 7 独立判健康）"
  return 0
}

# ══════════════════════════════════════════════════════════════════════════════
# ① 具名卷同路径挂 JM/TM
# ══════════════════════════════════════════════════════════════════════════════
check_volume_same_path() {
  sec "① 具名卷同路径挂载 JM/TM（compose 断言 + 实机 inspect 比对卷名）"
  local item="① 具名卷同路径挂 JM/TM"
  local ok=1

  # (a) compose config 层：两服务的 volumes 都必须有 source=paimon-warehouse / target=/warehouse
  info "compose config --format json 的 flink-jm / flink-tm volumes："
  local cfg
  if ! cfg="$(dc config --format json 2>&1)"; then
    printf '%s\n' "$cfg" | raw
    fail "$item（compose config 执行失败）"
    return 1
  fi
  local cfg_out cfg_rc
  cfg_out="$(printf '%s' "$cfg" | "$PY" -c '
import json, sys
cfg = json.load(sys.stdin)
rc = 0
for svc in ("flink-jm", "flink-tm"):
    vols = cfg.get("services", {}).get(svc, {}).get("volumes", [])
    hit = [v for v in vols if v.get("source") == "paimon-warehouse" and v.get("target") == "/warehouse"]
    print("%s: 卷条目 %d 个, 匹配 source=paimon-warehouse&target=/warehouse 的 %d 个" % (svc, len(vols), len(hit)))
    for v in vols:
        print("   - source=%s target=%s type=%s read_only=%s" % (v.get("source"), v.get("target"), v.get("type"), v.get("read_only")))
    if len(hit) != 1:
        rc = 1
sys.exit(rc)
' 2>&1)"; cfg_rc=$?
  printf '%s\n' "$cfg_out" | raw
  if [[ $cfg_rc -ne 0 ]]; then
    fail "$item（compose config 层：两服务未各恰有一条 source=paimon-warehouse target=/warehouse）"
    ok=0
  else
    info "compose config 层：PASS"
  fi

  # (b) 实机层：**这才是能抓反例的那一半**
  #     反例「挂了两个不同名的卷」在 config 层看不出来（卷名写在 source 里，
  #     只要写的是 paimon-warehouse 就都一样）；但容器**实际挂的卷名**由
  #     inspect 的 Mounts[].Name 给出 —— 两边 Name 不等即说明各挂各的。
  info "docker inspect 各取 Mounts 中 Type=volume && Destination=/warehouse 的那条："
  local jm_name tm_name jm_dst tm_dst jm_rc tm_rc
  jm_name="$(dcx inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/warehouse")}}{{.Name}}{{end}}{{end}}' "$JM_CONTAINER" 2>&1)"; jm_rc=$?
  tm_name="$(dcx inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/warehouse")}}{{.Name}}{{end}}{{end}}' "$TM_CONTAINER" 2>&1)"; tm_rc=$?
  jm_dst="$(dcx inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/warehouse")}}{{.Destination}}{{end}}{{end}}' "$JM_CONTAINER" 2>&1)"
  tm_dst="$(dcx inspect -f '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/warehouse")}}{{.Destination}}{{end}}{{end}}' "$TM_CONTAINER" 2>&1)"
  printf '    %s Mounts -> Name=[%s] Destination=[%s] (rc=%s)\n' "$JM_CONTAINER" "$jm_name" "$jm_dst" "$jm_rc" | raw
  printf '    %s Mounts -> Name=[%s] Destination=[%s] (rc=%s)\n' "$TM_CONTAINER" "$tm_name" "$tm_dst" "$tm_rc" | raw
  # 原始 inspect 全量 Mounts 也打印一份，供人复核（票面要求原始输出）
  dcx inspect -f '    {{range .Mounts}}{{.Type}} {{.Name}} -> {{.Destination}}{{println}}{{end}}' "$JM_CONTAINER" 2>&1 | raw
  dcx inspect -f '    {{range .Mounts}}{{.Type}} {{.Name}} -> {{.Destination}}{{println}}{{end}}' "$TM_CONTAINER" 2>&1 | raw

  if [[ $jm_rc -ne 0 || $tm_rc -ne 0 ]]; then
    fail "$item（inspect 失败：容器可能未起，请先 docker compose up -d）"
    return 1
  fi
  if [[ -z "$jm_name" || -z "$tm_name" ]]; then
    fail "$item（至少一侧没有 Type=volume 且 Destination=/warehouse 的挂载）"
    return 1
  fi
  if [[ "$jm_name" != "$tm_name" ]]; then
    info "反例命中：两侧卷名不同 ⇒ 各挂各的卷（同路径 != 同卷）"
    fail "$item（卷名不等：JM=[$jm_name] TM=[$tm_name]）"
    return 1
  fi
  if [[ "$jm_dst" != "$tm_dst" || "$jm_dst" != "$WAREHOUSE_PATH" ]]; then
    fail "$item（容器内路径不逐字相同：JM=[$jm_dst] TM=[$tm_dst]）"
    return 1
  fi
  info "实机层：两侧 Name 逐字相等（$jm_name），Destination 逐字相等（$jm_dst）"
  # 不重复计数（2026-10-22 code-review）：失败已在上面各处逐次 fail；
  # 末尾再 fail 一次会把总数推高到 > 判据条数。
  if [[ $ok -eq 1 ]]; then pass "$item"; fi
  return $(( 1 - ok ))
}

# ══════════════════════════════════════════════════════════════════════════════
# ② 跨容器互见（本票核心）
# ══════════════════════════════════════════════════════════════════════════════
check_cross_container_visibility() {
  sec "② 跨容器互见：JM 写探针 -> TM 读回同一路径逐字比对"
  local item="② 跨容器互见（JM 写 / TM 读）"

  # 探针内容：时间戳 + 随机数 + 主机名。三项都带上，任何一个不同都能被抓到。
  local payload="impl-03a-probe stamp=${RUN_STAMP} nonce=${RUN_NONCE}"
  info "探针路径：$PROBE_PATH"
  info "探针内容（JM 侧生成，含 stamp+nonce+JM 主机名）："

  local jm_out jm_rc
  jm_out="$(dcx exec "$JM_CONTAINER" sh -c 'printf "%s host=%s\n" "$1" "$(hostname -s 2>/dev/null || hostname)" > "$2" && echo WROTE && cat "$2"' _ "$payload" "$PROBE_PATH" 2>&1)"; jm_rc=$?
  printf '    JM 侧命令: docker exec %s sh -c [printf ... > %s && cat %s]\n' "$JM_CONTAINER" "$PROBE_PATH" "$PROBE_PATH" | raw
  printf '    JM 侧 stdout/rc=%s:\n' "$jm_rc" | raw
  printf '%s\n' "$jm_out" | raw
  if [[ $jm_rc -ne 0 ]]; then
    fail "$item（JM 侧写入失败）"
    return 1
  fi

  # TM 侧读回**同一路径** —— 这是判据的承重点
  local tm_out tm_rc
  tm_out="$(dcx exec "$TM_CONTAINER" sh -c 'cat "$1"' _ "$PROBE_PATH" 2>&1)"; tm_rc=$?
  printf '    TM 侧命令: docker exec %s sh -c [cat %s]\n' "$TM_CONTAINER" "$PROBE_PATH" | raw
  printf '    TM 侧 stdout/rc=%s:\n' "$tm_rc" | raw
  printf '%s\n' "$tm_out" | raw
  if [[ $tm_rc -ne 0 ]]; then
    info "反例命中：TM 读不到该路径（写者能读回自己 => 「文件存在」是恒真判据）"
    fail "$item（TM 侧读取失败）"
    return 1
  fi

  # 逐字比对：JM 侧 cat 出来的那一行 vs TM 侧 cat 出来的那一行
  local jm_line tm_line
  jm_line="$(printf '%s\n' "$jm_out" | sed -n '2p')"   # 第 1 行是 WROTE
  tm_line="$(printf '%s\n' "$tm_out" | sed -n '1p')"
  printf '    逐字比对：\n' | raw
  printf '      JM 行 = [%s]\n' "$jm_line" | raw
  printf '      TM 行 = [%s]\n' "$tm_line" | raw
  if [[ "$jm_line" != "$tm_line" ]]; then
    fail "$item（内容不逐字相同）"
    return 1
  fi
  # 再确认内容里确实带本轮的时间戳与随机数（防「读到的其实是上一轮的旧文件」）
  if [[ "$tm_line" != *"$RUN_STAMP"* || "$tm_line" != *"$RUN_NONCE"* ]]; then
    fail "$item（内容里没有本轮 stamp/nonce => 可能读到的是旧文件）"
    return 1
  fi
  pass "$item"
  return 0
}

# ══════════════════════════════════════════════════════════════════════════════
# ③ down（不带 -v）/ up 后数据仍在
# ══════════════════════════════════════════════════════════════════════════════

# ★★ 安全自检：**任何路径都不允许 down -v / --volumes** ★★
#    本函数是 down 的唯一出口。它在执行前逐字扫描参数，发现 -v / --volumes
#    即**拒绝执行**并返回 99（由调用方判 FAIL），防止后人误改脚本把卷删掉。
#    同时它自己**永远不追加**任何 -v/--volumes。
compose_down_preserving_volumes() {
  local arg
  for arg in "$@"; do
    case "$arg" in
      -v|--volumes|--volumes=*)
        printf '    X 安全自检拒绝：down 参数里出现 %s（会删除具名卷）\n' "$arg" | raw
        return 99
        ;;
    esac
  done
  printf '    > 执行: docker compose down（参数: [%s]，已通过 -v 自检）\n' "$*" | raw
  dc down "$@"
}

check_persistence_across_down_up() {
  sec "③ docker compose down（不带 -v）-> up -d -> 探针内容仍在且逐字相同"
  local item="③ down/up 后数据仍在"

  # 3.1 先备份探针内容到宿主 —— down 之后卷内文件就取不到了，必须**先**备份
  local backup_file="${PROBE_BACKUP_DIR}/${PROBE_NAME}.bak"
  local before b_rc
  before="$(dcx exec "$TM_CONTAINER" cat "$PROBE_PATH" 2>&1)"; b_rc=$?
  printf '    down 前：从 TM 读探针（rc=%s）：\n' "$b_rc" | raw
  printf '%s\n' "$before" | raw
  if [[ $b_rc -ne 0 || -z "$before" ]]; then
    fail "$item（down 前读探针失败，无法建立比对基准）"
    return 1
  fi
  if ! printf '%s\n' "$before" > "$backup_file"; then
    fail "$item（无法备份探针到宿主 $backup_file）"
    return 1
  fi
  info "已备份到宿主：$backup_file（内容 $(printf '%s' "$before" | wc -c) 字节）"

  # 3.2 down（**不带 -v**，由自检函数保证）
  printf '    down 原始输出：\n' | raw
  local down_out down_rc
  down_out="$(compose_down_preserving_volumes 2>&1)"; down_rc=$?
  printf '%s\n' "$down_out" | raw
  if [[ $down_rc -eq 99 ]]; then
    fail "$item（down 参数含 -v，安全自检拦截）"
    return 1
  fi
  if [[ $down_rc -ne 0 ]]; then
    fail "$item（docker compose down 执行失败 rc=$down_rc）"
    return 1
  fi

  # 3.3 卷必须在 down 之后依然存在（具名卷不随容器删除）
  # 卷的**实际名字带 compose 项目前缀**（2026-10-22 实机踩到）：
  #   compose 文件里声明的是 paimon-warehouse，但 Docker 里落地成
  #     eco-a-d-platform_paimon-warehouse
  #   故 -f name=^paimon-warehouse$ 这个**锚定正则永远匹配不到** => 判据恒假，
  #   会把「卷还在」误报成「卷被删了，误加了 -v」。改用**子串**匹配。
  local vol_after_down
  vol_after_down="$(dcx volume ls -q -f "name=${VOLUME_NAME}" 2>&1)"
  printf '    down 后 docker volume ls -q -f name=%s -> [%s]\n' "$VOLUME_NAME" "$vol_after_down" | raw
  if [[ "$vol_after_down" != *"$VOLUME_NAME"* ]]; then
    fail "$item（down 后卷 $VOLUME_NAME 不存在 => 卷被删了，请检查是否误加了 -v）"
    return 1
  fi

  # 3.4 up -d 并等 healthy（含 doris profile，见 PROFILE_ARGS）
  printf '    up -d 原始输出：\n' | raw
  local up_out up_rc
  up_out="$(dc up -d 2>&1)"; up_rc=$?
  printf '%s\n' "$up_out" | raw
  if [[ $up_rc -ne 0 ]]; then
    fail "$item（docker compose up -d 执行失败 rc=$up_rc）"
    return 1
  fi
  wait_for_healthy 600 || info "（健康等待未在超时内完成，仍继续读探针 —— 由条目⑦独立判健康）"

  # 3.5 读回同一路径，与备份逐字比对
  local after a_rc
  after="$(dcx exec "$TM_CONTAINER" cat "$PROBE_PATH" 2>&1)"; a_rc=$?
  printf '    up 后：从 TM 读同一路径（rc=%s）：\n' "$a_rc" | raw
  printf '%s\n' "$after" | raw
  printf '    down 前备份内容：\n' | raw
  cat "$backup_file" 2>&1 | raw
  if [[ $a_rc -ne 0 ]]; then
    fail "$item（up 后读探针失败 => 卷内数据丢了）"
    return 1
  fi
  if [[ "$after" != "$before" ]]; then
    fail "$item（内容不逐字相同：down 前 [$before] vs up 后 [$after]）"
    return 1
  fi
  info "逐字比对通过（down 前后同一路径、同一内容）"
  pass "$item"
  return 0
}

# ══════════════════════════════════════════════════════════════════════════════
# ④ 作业 jar 与 Paimon connector：**目录必须分开**（impl-06d 登记的环境事实）
# ══════════════════════════════════════════════════════════════════════════════
# ★ 2026-10-26 口径变更（impl-06d 实测，用户裁决）：本判据原为「Paimon
#   bundled connector 在 JM/TM 两容器 /opt/flink/lib + size/sha256」。
#   原口径判的是**部署面 jar**（Paimon 的两个 jar，Dockerfile 构建期 COPY 进
#   /opt/flink/lib），它仍然成立 —— 但**作业 jar** 走的是另一条路：
#   flink/target/ecom-flink-0.1.0-SNAPSHOT.jar 在运行时 docker cp 进容器，
#   且**必须放 /opt/flink/usrlib/，放 lib/ 会 LinkageError**：
#
#     LinkageError: loader constraint violation … committedOffsets
#
#   根因（impl-06d 实测，非推断）：作业 jar 是 shade jar，**内含
#   org/apache/kafka/** 的类；而 flink-dist-2.2.1.jar 不含 kafka 类。作业 jar
#   一旦进 /opt/flink/lib，父子类加载器各加载一份 Kafka 的 OffsetResetStrategy
#   ⇒ 同名类来自两个加载器 ⇒ loader constraint violation。usrlib 走**用户类
#   加载器**，与 flink-dist 的父子关系不冲突。
#   ⇒ 原判据**结构上抓不到**这个缺陷：它只看 lib/ 有没有 Paimon connector，
#     **完全不查作业 jar 在哪个目录**（放错目录时它照样 PASS）。
#
# ⇒ 本条判据要**两边都查**，只查一边都是恒真：
#   (a) **阳性**：两端 /opt/flink/usrlib/ 下必须有作业 jar，且两端**同一字节**
#       （size + sha256 逐字相等 ⇒ 排除「只给一端拷了」这种半部署）。
#   (b) **阴性（承重点）**：清点 /opt/flink/lib/ **不许有任何作业 jar 副本**
#       —— 这正是原口径抓不到的缺陷形态。只查 (a) 会让「usrlib 有、lib 也有」
#       照样 PASS，而那正是 LinkageError 的形态。
#   旧的「/opt/flink/lib 里有 Paimon connector」断言**保留但下移为 4d**（它是
#   部署面事实，不是本条要判的作业面事实；两类对象混在一条判据里会让 FAIL
#   文案指不准病因——见 impl-03a §D 未采纳项的同一条理由）。
check_job_jar_usrlib() {
  sec "④ 作业 jar 在 /opt/flink/usrlib/（两端同一字节；lib/ 不许有副本）"
  local item="④ 作业 jar 在 usrlib 且 lib 无副本"
  local ok=1
  local c out rc size sha n path
  # 任一侧在 (a) 段已经失败时，不再跑「两端比对」——否则同一个缺陷
  # 会计两次 FAIL（impl-03a §D #2 的计数重复缺陷，勿重踩）。
  local side_bad=0

  # ── (a) 两端 usrlib 里的 ecom 作业 jar：必须存在、字节一致 ──────────────
  # ★ 不用固定文件名锚死：实测部署名是 ecom-flink.jar（.uv-cache/deploy-job.sh），
  #   而构建产物名是 ecom-flink-0.1.0-SNAPSHOT.jar —— 两者都可接受，
  #   故按**模式**匹配 ecom.*.jar（与 deploy-job.sh 的清点口径一致）。
  local jm_read="" tm_read=""
  for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
    printf '    docker exec %s ls -l /opt/flink/usrlib/ 原始输出：\n' "$c" | raw
    out="$(dcx exec "$c" ls -l /opt/flink/usrlib/ 2>&1)"; rc=$?
    printf '%s\n' "$out" | raw
    if [[ $rc -ne 0 ]]; then
      fail "$item（$c 侧 ls /opt/flink/usrlib 失败 rc=$rc）"
      ok=0; side_bad=1
      continue
    fi
    # 只认**常规文件**且名字匹配 ecom：目录、改名残留、非 jar 都不算命中
    if ! printf '%s\n' "$out" | grep -qE '^-.*ecom.*\.jar$'; then
      fail "$item（$c 侧 /opt/flink/usrlib/ 里没有 ecom 作业 jar）"
      ok=0; side_bad=1
      continue
    fi
    printf '    命中行（%s）：\n' "$c" | raw
    printf '%s\n' "$out" | grep -E 'ecom.*\.jar$' | raw

    # 「恰 1 个 ecom jar」不是洁癖：多版本共存时类加载器可能命中旧版本，
    # 而 (a) 的字节校验只校第一个 ⇒ 不判条数就会给「usrlib 里新旧两份」放行。
    n="$(printf '%s\n' "$out" | grep -cE '^-.*ecom.*\.jar$')"
    path="$(printf '%s\n' "$out" | awk '/^-.*ecom.*\.jar$/ {print $NF; exit}')"
    printf '    %s: ecom jar 条目数=%s，校验对象=%s\n' "$c" "$n" "$path" | raw
    if [[ "$n" != "1" ]]; then
      fail "$item（$c 侧 usrlib 里有 $n 个 ecom jar，期望恰 1 —— 多版本共存会让类加载器命中旧版本）"
      ok=0; side_bad=1
      continue
    fi
    # ls -l 的最后一列是文件名（usrlib 是平铺目录，无空格名，故 $NF 安全）
    size="$(dcx exec "$c" stat -c '%s' "/opt/flink/usrlib/$path" 2>&1)"
    sha="$(dcx exec "$c" sha256sum "/opt/flink/usrlib/$path" 2>&1 | cut -d' ' -f1)"
    printf '    %s: size=%s sha256=%s\n' "$c" "$size" "$sha" | raw
    if [[ -z "$size" || ! "$size" =~ ^[0-9]+$ ]]; then
      fail "$item（$c 侧 usrlib jar 体积为 0/不可读：[/$size]）"
      ok=0; side_bad=1
      continue
    fi
    # 两端读数各落一个标量：末尾要**逐字**比「两端是不是同一字节」。
    if [[ "$c" == "$JM_CONTAINER" ]]; then
      jm_read="$size:$sha"
    else
      tm_read="$size:$sha"
    fi
    info "$c 侧 usrlib 命中作业 jar（$path, size=$size）"
  done

  # 两端必须**同一字节**：只给一端拷过 = 半部署 ⇒ TM 侧加载不到作业类。
  # side_bad=1 时上面已经为「该侧缺失/多版本/不可读」记过一次 FAIL，
  # 这里再比一次会把同一个缺陷计两遍 ⇒ 只在两侧都读到读数时才比。
  printf '    两端读数比对（size:sha256）：\n' | raw
  printf '      %s\n' "${jm_read:-<缺>}" | raw
  printf '      %s\n' "${tm_read:-<缺>}" | raw
  if [[ $side_bad -eq 1 ]]; then
    info "（(a) 段已有失败 ⇒ 跳过两端比对，避免同一缺陷重复计数）"
  elif [[ -z "$jm_read" || -z "$tm_read" ]]; then
    fail "$item（两端读数不齐：JM=[$jm_read] TM=[$tm_read]）"
    ok=0
  elif [[ "$jm_read" != "$tm_read" ]]; then
    fail "$item（两端 usrlib 作业 jar 不是同一字节：J=[$jm_read] T=[$tm_read]）"
    ok=0
  else
    info "两端 usrlib 作业 jar 逐字节相同（$jm_read）"
  fi

  # ── (b) 承重点：两端 /opt/flink/lib 里**不许**有作业 jar 副本 ──────────────
  # 反例形态：usrlib 放一份，lib 里还留着历史 docker cp 的副本。
  #   ⇒ 父子类加载器各加载一份 ⇒ LinkageError: loader constraint violation。
  #   只判 (a) 这条**永远抓不到**，故 (b) 是独立判据，不是 (a) 的补充说明。
  for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
    printf '    docker exec %s ls -l /opt/flink/lib/ 检查作业 jar 副本：\n' "$c" | raw
    out="$(dcx exec "$c" ls -l /opt/flink/lib/ 2>&1)"; rc=$?
    printf '%s\n' "$out" | raw
    if [[ $rc -ne 0 ]]; then
      fail "$item（$c 侧 ls /opt/flink/lib 失败 rc=$rc）"
      ok=0
      continue
    fi
    if printf '%s\n' "$out" | grep -qE 'ecom.*\.jar$'; then
      printf '    反例命中行（%s）：\n' "$c" | raw
      printf '%s\n' "$out" | grep -E 'ecom.*\.jar$' | raw
      fail "$item（$c 侧 /opt/flink/lib/ 里有作业 jar 副本 ⇒ 父子类加载器双份 ⇒ LinkageError）"
      ok=0
      continue
    fi
    info "$c 侧 /opt/flink/lib 无作业 jar 副本（正确）"
  done

  # 同 ①：失败已在各段内逐次计入，末尾不重复计。
  if [[ $ok -eq 1 ]]; then pass "$item"; fi
  return $(( 1 - ok ))
}

# ══════════════════════════════════════════════════════════════════════════════
# 4d Paimon bundled connector 在 JM 与 TM 两端 /opt/flink/lib（部署面事实）
# ══════════════════════════════════════════════════════════════════════════════
# 原 ④ 的内容整体下移到这里，**判据语义不变**（官方逐字要求 connector 装进
# 「每一个」Flink 进程；两容器各自 /opt/flink/lib 必须命中同一个 jar，且
# size/sha256 与钉死值逐字节一致 —— 只认名字抓不住截断/替换/错版本）。
# 拆出来的理由：④ 要判的「作业 jar 不得与 connector 混在同一目录」与
# 「connector 在不在 lib」是**两件事**，混在一条判据里时 FAIL 文案指不准病因。
check_paimon_connector_in_each_process() {
  sec "4d Paimon bundled connector 在 JM 与 TM 两端 /opt/flink/lib 各查一次"
  local item="4d Paimon connector 在两端 lib"
  local ok=1
  local c out rc

  # ★ 官方逐字："Install one matching Paimon bundled connector in **each** Flink
  #   process." => 两端都要查，只查一端不算通过。
  for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
    printf '    docker exec %s ls -l /opt/flink/lib/ 原始输出：\n' "$c" | raw
    out="$(dcx exec "$c" ls -l /opt/flink/lib/ 2>&1)"; rc=$?
    printf '%s\n' "$out" | raw
    if [[ $rc -ne 0 ]]; then
      fail "$item（$c 侧 ls 失败 rc=$rc）"
      ok=0
      continue
    fi
    if ! printf '%s\n' "$out" | grep -q "$PAIMON_JAR"; then
      fail "$item（$c 侧 /opt/flink/lib 里没有 $PAIMON_JAR）"
      ok=0
      continue
    fi
    printf '    命中行（%s）：\n' "$c" | raw
    printf '%s\n' "$out" | grep "$PAIMON_JAR" | raw
    info "$c 侧命中 $PAIMON_JAR"

    # ---- 体积 + sha256 逐字节校验（2026-10-22 code-review 指出）：
    #      此前只 grep 文件名，而票面要求的是「57,697,871 B」这个**具体**事实；
    #      只认名字抓不住截断、替换或错误版本的 jar。
    local in_size in_sha
    in_size="$(dcx exec "$c" stat -c '%s' "/opt/flink/lib/$PAIMON_JAR" 2>&1)"
    in_sha="$(dcx exec "$c" sha256sum "/opt/flink/lib/$PAIMON_JAR" 2>&1 | cut -d' ' -f1)"
    printf '    %s: size=%s sha256=%s\n' "$c" "$in_size" "$in_sha" | raw
    if [[ "$in_size" != "$EXPECTED_JAR_SIZE" ]]; then
      fail "$item（$c 侧 jar 体积 [$in_size] != 期望 $EXPECTED_JAR_SIZE）"
      ok=0; continue
    fi
    if [[ "$in_sha" != "$EXPECTED_JAR_SHA256" ]]; then
      fail "$item（$c 侧 jar sha256 不符：[$in_sha]）"
      ok=0; continue
    fi
    info "$c 侧 jar 字节级校验通过（size + sha256 均与钉死值一致）"
  done

  # 同 ①：失败已在循环内逐侧计入，末尾不重复计。
  if [[ $ok -eq 1 ]]; then pass "$item"; fi
  return $(( 1 - ok ))
}

# ==============================================================================
# 4c ADR-0002 补丁锚点：镜像实际运行的 Flink 补丁必须 == 2.2.1
# ==============================================================================
# 为什么加这条（2026-10-22 code-review 指出）：
#   ADR-0002 把 Flink 补丁锚点定为 **2.2.1**，并在 Consequences 里明确要求
#   「flink/ 工程落地时须核对镜像实际运行补丁与 2.2.1 一致（flink --version）」。
#   本票把 compose 的镜像从官方 apache/flink:2.2-java17 换成本地 tag
#   eco/flink:2.2-paimon；本地 tag 里的 2.2 是**浮动**的（不写补丁号），
#   若哪天基础镜像换成 2.2.0/2.2.2，tag 名字照样成立而锚点已经破了。
#   故把 ADR 要求的那次核对**机器化**：不靠人记得跑 flink --version。
# 反例：把 Dockerfile 的 FROM 换成 apache/flink:2.2.0-java17，本条立刻 FAIL。
# 实测（2026-10-22）：镜像内 `flink --version` -> `Version: 2.2.1, Commit ID: 450c63e`；
#   /opt/flink/lib 下 flink-dist-2.2.1.jar 等亦印证同一补丁号。
check_flink_patch_anchor() {
  sec "4c ADR-0002 补丁锚点：两端 flink --version 须为 2.2.1"
  local item="4c ADR-0002 补丁锚点 = 2.2.1"
  local ok=1 c out rc ver
  for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
    out="$(dcx exec "$c" sh -c 'flink --version 2>/dev/null || $FLINK_HOME/bin/flink --version' 2>&1)"; rc=$?
    printf '    [%s] flink --version (rc=%s)：\n' "$c" "$rc" | raw
    printf '%s\n' "$out" | raw
    if [[ $rc -ne 0 ]]; then
      fail "$item（$c 侧 flink --version 失败 rc=$rc）"
      ok=0; continue
    fi
    ver="$(printf '%s\n' "$out" | sed -n 's/^Version: \([^,]*\).*/\1/p' | head -1)"
    if [[ "$ver" != "2.2.1" ]]; then
      fail "$item（$c 侧实际补丁 = [$ver]，ADR-0002 锚点是 2.2.1）"
      ok=0; continue
    fi
    info "$c 侧补丁 = $ver（与 ADR-0002 一致）"
  done
  if [[ $ok -eq 1 ]]; then pass "$item"; fi
  return $(( 1 - ok ))
}
# ==============================================================================
# 4b /warehouse 对 flink 用户可写（2026-10-22 实机新增，票面未写但承重）
# ==============================================================================
# 为什么这是独立判据而不是并入 2：
#   2 证明「两端读到同一份数据」，但那时探针是 root 之外的用户能否写入 被忽略了。
#   实机首次跑 2 就撞到：
#     docker exec eco-flink-jm sh -c 'echo x > /warehouse/probe'
#       -> sh: 1: cannot create /warehouse/.probe-...: Permission denied
#   根因：官方 flink 镜像以 uid=9999(flink) 运行，而 Docker 具名卷首次挂载时
#     以 root:root 建目录 => 该用户对 /warehouse 只有 r-x，没有 w。
#   Paimon 要在 /warehouse 下建 database/表目录并写 bucket 文件 => 必然失败。
#   => 「挂对了卷」不等于「能写」。2 + 4b 才是完整的可写性证据。
#
# 对策（两层，缺一不可）：
#   (a) 两侧 ls -ldn /warehouse 的属主 uid 必须等于容器内 id -u（即 flink 用户）；
#   (b) JM 与 TM 各实际写一次探针（独立路径），cat 回来逐字比对，
#       写失败即 FAIL —— 这条能抓住「属主对了但权限位不对（如 555）」。
#
# 反例已被本判据抓住过一次（修复前 uid=0 且写入 Permission denied），
#   故本条不是恒真判据：把 Dockerfile 里的 chown 删掉，它会立刻 FAIL。
check_warehouse_writable() {
  sec "4b /warehouse 对 flink 用户可写（挂对了卷 != 能写；Paimon 写 bucket 的前提）"
  local item="4b /warehouse 对 flink 用户可写"
  local ok=1 c uid ls_out my_uid probe payload back wrc
  for c in "$JM_CONTAINER" "$TM_CONTAINER"; do
    printf '    [%s] id / ls -ldn /warehouse 原始输出：\n' "$c" | raw
    ls_out="$(dcx exec "$c" sh -c 'id && ls -ldn /warehouse' 2>&1)"; wrc=$?
    printf '%s\n' "$ls_out" | raw
    if [[ $wrc -ne 0 ]]; then
      fail "$item（$c 侧 id/ls 失败 rc=$wrc）"
      ok=0; continue
    fi
    # (a) 属主 uid 必须 == 当前运行用户 uid
    my_uid="$(printf '%s\n' "$ls_out" | sed -n 's/^uid=\([0-9]\+\)(.*/\1/p' | head -1)"
    uid="$(printf '%s\n' "$ls_out" | awk '/\/warehouse$/ {print $3}' | head -1)"
    printf '    %s: 运行 uid=%s, /warehouse 属主 uid=%s\n' "$c" "${my_uid:-?}" "${uid:-?}" | raw
    if [[ -z "$my_uid" || -z "$uid" ]]; then
      fail "$item（$c 侧无法解析 uid，见上方原始输出）"
      ok=0; continue
    fi
    if [[ "$uid" != "$my_uid" ]]; then
      fail "$item（$c 侧 /warehouse 属主 uid=$uid != 运行 uid=$my_uid => flink 用户写不进去）"
      ok=0; continue
    fi
    # (b) 真正写一次再读回（抓「属主对但权限位错」）
    probe="/warehouse/.rwcheck-$(date -u +%Y%m%dT%H%M%SZ)-$$-$c"
    payload="rw-$c-$(date -u +%s)-$RANDOM"
    printf '    [%s] 实写探针 %s 内容 [%s] 并读回：\n' "$c" "$probe" "$payload" | raw
    back="$(dcx exec "$c" sh -c "printf '%s' '$payload' > '$probe' && cat '$probe' && rm -f '$probe'" 2>&1)"; wrc=$?
    printf '%s\n' "$back" | raw
    if [[ $wrc -ne 0 ]]; then
      fail "$item（$c 侧写入被拒 rc=$wrc => flink 用户对 /warehouse 不可写）"
      ok=0; continue
    fi
    if [[ "$back" != "$payload" ]]; then
      fail "$item（$c 侧写后读回不一致：写入 [$payload] vs 读回 [$back]）"
      ok=0; continue
    fi
    info "$c 侧可写（uid 一致 + 写入读回逐字相同）"
  done
  # 同 ①：见上。
  if [[ $ok -eq 1 ]]; then pass "$item"; fi
  return $(( 1 - ok ))
}
# ══════════════════════════════════════════════════════════════════════════════
# ⑤ 全仓无 ADD JAR 指向宿主路径
# ══════════════════════════════════════════════════════════════════════════════
check_no_add_jar_host_path() {
  sec "⑤ 全仓无 ADD JAR 指向宿主路径"
  local item="⑤ 无 ADD JAR 指向宿主路径"
  local hits n

  # ★ 官方路径不可行：ADD JAR 指向宿主路径在**分布式集群**下不可行（文件只在
  #   client 的机器上），必须**构建期 COPY** 进 /opt/flink/lib。
  #
  # ★★ 扫描面 = 可执行面白名单（2026-10-27 impl-06i，用户裁决方向 C）★★
  #   判据对象是「仓库源码里**可能出现 ADD JAR 语句**的文件」，不是「工作树里的
  #   一切文件」。旧口径「全仓 grep -r」在 2026-10-26 收尾门（impl-06f-4）被
  #   32 行非 SQL 噪音打成恒假：
  #     · .mimosa/hook-state/**/*.source 27 行 —— DSH harness 会话基线副本
  #       （含本脚本自身快照），判据写就（2026-10-22）时该目录不存在；
  #     · tests/test_replay_sql.py 4 行 —— 守卫测试自身的 docstring/断言消息；
  #     · README.md 1 行 —— 判据名列举（叙述面）。
  #   白名单 = 能把 SQL 文本送进执行的面：
  #     *.sql（sql-client -f 入口）、*.sh（docker exec sql-client 编排面）、
  #     *.java（Table API 内嵌 SQL）、Makefile / Dockerfile / *.yml / *.yaml
  #     （compose command 等内嵌调用面）。
  #   叙述面（*.md / *.py / *.json / *.source …）不在扫描面内 ⇒ README 与守卫
  #   测试的叙述行**结构性免疫**；对未来出现的未跟踪垃圾目录（工具缓存 / IDE
  #   状态 / 新的 harness 目录）同样免疫——白名单之外的扩展名永远进不来。
  #   ⚠️ 代价（票面 Notes 已记）：新增可执行源码类型时须把扩展名加进白名单，
  #      否则该类型脱管（判不到）。
  #   排除目录仍保留：.git（版本库元数据）、.scratch（设计链/票面/证据）、
  #   artifacts（构建产物）、依赖与构建树（.venv/venv/node_modules/.uv-cache/
  #   target/build/lib —— 这些树里也有 *.sh，但不是本仓源码）。
  local -a SCAN_INCLUDE SCAN_EXCLUDE
  SCAN_INCLUDE=(--include='*.sql' --include='*.sh' --include='*.java'
                --include='Makefile' --include='Dockerfile'
                --include='*.yml' --include='*.yaml')
  SCAN_EXCLUDE=(--exclude-dir=.git --exclude-dir=.scratch --exclude-dir=artifacts
                --exclude-dir=.venv --exclude-dir=venv --exclude-dir=node_modules
                --exclude-dir=.uv-cache --exclude-dir=target --exclude-dir=build
                --exclude-dir=lib)
  info "grep 范围：可执行面白名单（sql/sh/java/Makefile/Dockerfile/yml/yaml），排除 .git / .scratch / artifacts / 依赖与构建树"
  # 必须排除**注释行**与本**检测脚本自身**（2026-10-22 实机踩到）：
  #   直接 grep 'ADD JAR' 会把自己这个判据的说明文字、以及 Dockerfile/compose 里
  #   「为什么不用 ADD JAR」的解释性注释全部算成命中 => 15 行「命中」，判据恒假。
  #   真正的判据是「有没有一条**可执行语句**在用 ADD JAR」，注释不是语句。
  #   注释形态随白名单扩展（impl-06i）：#（sh/yaml/Make）、--（sql）、//（java）。
  hits="$(grep -rniI "${SCAN_EXCLUDE[@]}" "${SCAN_INCLUDE[@]}" -e 'ADD JAR' . 2>/dev/null \
          | grep -v -E '^[^:]+:[0-9]+:[[:space:]]*(--|#|//)' \
          | grep -v -F 'scripts/deploy_surface_check.sh')"
  if [[ -n "$hits" ]]; then
    printf '%s\n' "$hits" | raw
    n="$(printf '%s\n' "$hits" | wc -l)"
    fail "$item（命中 $n 行 ADD JAR）"
    return 1
  fi
  info "ADD JAR 零命中"

  # 额外一层：白名单内引用宿主路径 jar 的行（形如 F:\\... 或 /mnt/f/... 或 C:/...）
  # 恒真风险：只 grep 'ADD JAR' 会漏掉别的引用形式。
  # 2026-10-27 impl-06i：本段与 seg1 **同一份白名单**（用户裁决方向 C）——旧全仓
  #   口径下本段同样被 .mimosa/ 基线副本打成恒假（18 行 docker cp 宿主路径 jar
  #   引用），只是 seg1 先 FAIL 提前 return，把本段的恒假挡住了。
  info "附加扫描：白名单内引用宿主路径 jar 的行（排除第三方依赖树）"
  local host_hits
  # ★ 排除第三方依赖树（2026-10-22 实机踩到，判据恒假）：
  #   .venv / node_modules 下有数万个文件（stubs 的 METADATA.toml、npm 的 LICENSE、
  #   basedpyright 的 dist 甚至内嵌整段 sourcemap），里面出现 "jar" 字样纯属噪音，
  #   实测把本该零命中的判据打成数万行命中。
  #   本判据要判的是「**本仓源码/配置**有没有引用宿主路径的 jar」，第三方依赖不在其列。
  #   另排除 https?:// 行：那是**下载 URL**（Maven 坐标），不是宿主文件路径。
  host_hits="$(grep -rniI \
    "${SCAN_EXCLUDE[@]}" "${SCAN_INCLUDE[@]}" \
    -E '(/mnt/[a-z]/|[A-Za-z]:[\\\\/])' . 2>/dev/null \
    | grep -i 'jar' \
    | grep -v -E '^[^:]+:[0-9]+:[[:space:]]*(--|#|//)' \
    | grep -v -F 'scripts/deploy_surface_check.sh' \
    | grep -v -E 'https?://' )"
  if [[ -n "$host_hits" ]]; then
    printf '%s\n' "$host_hits" | raw
    fail "$item（有文件引用宿主路径的 jar，见上）"
    return 1
  fi
  info "宿主路径 jar 引用零命中"
  pass "$item"
  return 0
}

# ══════════════════════════════════════════════════════════════════════════════
# ⑥ data/ 被 .gitignore 覆盖（C5）
# ══════════════════════════════════════════════════════════════════════════════
check_data_ignored() {
  sec "⑥ data/ 被 .gitignore 覆盖（门槛批 raw 数据不入库，C5）"
  local item="⑥ data/ 被 .gitignore 覆盖"
  local ok=1 p out rc

  # 只查 data/ 不够：目录被忽略 != 其下文件被忽略。两条都查才算这条判据真的成立。
  for p in "data/" "data/x"; do
    out="$(git check-ignore -v "$p" 2>&1)"; rc=$?
    printf '    git check-ignore -v %s （rc=%s）：\n' "$p" "$rc" | raw
    printf '%s\n' "$out" | raw
    if [[ $rc -ne 0 || -z "$out" ]]; then
      fail "$item（$p 未被 .gitignore 覆盖）"
      ok=0
    fi
  done
  info ".gitignore 中相关规则："
  grep -n -E '^[[:space:]]*(\*\*/)?data/' .gitignore 2>&1 | raw
  if [[ $ok -eq 1 ]]; then pass "$item"; else fail "$item"; fi
  return $(( 1 - ok ))
}

# ══════════════════════════════════════════════════════════════════════════════
# ⑦ docker compose ps 七服务
# ══════════════════════════════════════════════════════════════════════════════
wait_for_healthy() {
  local deadline=$(( SECONDS + ${1:-300} ))
  local bad
  while (( SECONDS < deadline )); do
    bad="$(dc ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null | awk '$2=="running" && ($3=="" || $3=="starting") {print $1}')"
    [[ -z "$bad" ]] && return 0
    sleep 5
  done
  return 1
}

check_compose_ps_all_healthy() {
  sec "⑦ docker compose ps 七服务（常驻须 healthy / kafka-init 须已成功退出 Exit 0）"
  local item="⑦ 七服务全 healthy"
  local ok=1 ps_out state_json

  printf '    docker compose ps 原始输出（含 --profile doris）：\n' | raw
  ps_out="$(dc ps 2>&1)"
  printf '%s\n' "$ps_out" | raw
  if [[ -z "$ps_out" ]]; then
    fail "$item（compose ps 无输出 => 服务根本没起）"
    return 1
  fi

  info "ps -a --format json 关键字段（供复核判据来源）："
  # ★★ 必须用 `ps -a`（实测踩到，2026-10-22）★★
  #   不带 -a 时 `docker compose ps` **只列 running 的容器**，而 kafka-init 是
  #   跑完即退出的**一次性服务** => 它**根本不在输出里**。实测（WSL）：
  #     docker compose --profile doris ps --format json        → 6 行（无 kafka-init）
  #     docker compose --profile doris ps -a --format json     → 7 行（含 kafka-init,
  #                                                              State=exited ExitCode=0）
  #   ⇒ 判「kafka-init 已成功退出」就必须用 -a，否则判据**恒假**（永远 MISSING）。
  state_json="$(dc ps -a --format json 2>/dev/null)"
  if [[ -z "$state_json" ]]; then
    fail "$item（compose ps -a --format json 无输出 => 服务根本没起，或 JSON 格式不被支持）"
    return 1
  fi
  printf '%s\n' "$state_json" | "$PY" -c '
import json, sys, re

def svc_of(r):
    # ★ 实测（WSL, Docker Compose v5.5.1）：JSON 行里 **有** Service 字段，
    #   但为稳妥起见再给一条从 Labels 解析 com.docker.compose.service 的退路
    #   （旧版本 compose 的 JSON 模式字段集不同）。
    s = r.get("Service")
    if s:
        return s
    m = re.search(r"com\.docker\.compose\.service=([^,]+)", r.get("Labels", "") or "")
    return m.group(1) if m else "?"

rows = []
for l in sys.stdin:
    if not l.strip():
        continue
    try:
        rows.append(json.loads(l))
    except Exception:
        pass
for r in sorted(rows, key=svc_of):
    print("    Service=%-10s State=%-8s Health=%-10s ExitCode=%s" % (
        svc_of(r), r.get("State"), r.get("Health") or "<no value>", r.get("ExitCode")))
' 2>&1 | raw

  # ★ 判据坑（impl-02e 踩过）：kafka-init 是**一次性服务**（restart: "no"），
  #   跑完即 Exit 0，且**没有 healthcheck** => ps 的 HEALTH 列是 <no value>。
  #   把「全 healthy」当一条笼统判据 => 这条**结构性永远不成立**。
  #   故拆成两条独立判据：(i) 常驻且有 healthcheck 的服务必须 healthy；
  #                        (ii) kafka-init 必须已成功退出（State=exited, ExitCode=0）。
  local resident_services="kafka flink-jm flink-tm superset doris-fe doris-be"
  local svc state health
  for svc in $resident_services; do
    read -r state health < <(printf '%s\n' "$state_json" | "$PY" -c '
import json, sys
want = sys.argv[1]
for l in sys.stdin:
    if not l.strip():
        continue
    try:
        r = json.loads(l)
    except Exception:
        continue
    if r.get("Service") == want:
        print("%s %s" % (r.get("State"), r.get("Health") or "<no value>"))
        break
else:
    print("MISSING <no value>")
' "$svc" 2>/dev/null)
    printf '    %-10s State=%-10s Health=%s\n' "$svc" "$state" "$health" | raw
    if [[ "$state" != "running" ]]; then
      fail "$item（$svc State=$state，非常驻运行中）"
      ok=0
      continue
    fi
    if [[ "$health" != "healthy" ]]; then
      fail "$item（$svc Health=$health，须 healthy）"
      ok=0
    fi
  done

  # (ii) 一次性服务 kafka-init：必须已成功退出（Exit 0），**不要求** healthy
  local ki_line ki_name
  ki_line="$(printf '%s\n' "$state_json" | "$PY" -c '
import json, sys
want = sys.argv[1]
for l in sys.stdin:
    if not l.strip():
        continue
    try:
        r = json.loads(l)
    except Exception:
        continue
    if r.get("Service") == want:
        print("State=%s ExitCode=%s Health=%s" % (r.get("State"), r.get("ExitCode"), r.get("Health") or "<no value>"))
        break
else:
    print("MISSING")
' "kafka-init" 2>/dev/null)"
  printf '    %-10s %s（一次性服务：判「已成功退出」而非「healthy」）\n' "kafka-init" "$ki_line" | raw
  if [[ "$ki_line" == *"State=exited"* && "$ki_line" == *"ExitCode=0"* ]]; then
    info "kafka-init：已成功退出（Exit 0）—— 判据成立"
  else
    ki_name="$(dcx inspect -f 'State={{.State.Status}} ExitCode={{.State.ExitCode}}' "$KAFKA_INIT_CONTAINER" 2>&1)"
    printf '    fallback docker inspect %s -> %s\n' "$KAFKA_INIT_CONTAINER" "$ki_name" | raw
    if [[ "$ki_name" == *"State=exited"* && "$ki_name" == *"ExitCode=0"* ]]; then
      info "kafka-init：已成功退出（Exit 0，经 docker inspect 复核）—— 判据成立"
    else
      fail "$item（kafka-init 未成功退出：$ki_line / $ki_name）"
      ok=0
    fi
  fi

  # 服务数量核对：默认 5 + doris 2 = 7（client 是 profile 隔离的按需容器，不计）
  local n
  # ★ 必须**排除 client**（2026-10-22 实机踩到，判据恒假）：
  #   `docker compose ps -a` 会把**所有 profile** 的容器都列出来，包括 profile="client"
  #   的按需工具容器 eco-client（它此刻 State=exited ExitCode=255 —— 上次 run --rm
  #   的残留）。票面的「七服务」= 默认 5 + doris 2，**client 明确不计入常驻**。
  #   不排除就会得到 8 != 7 => 判据恒假。
  n="$(printf '%s\n' "$state_json" | "$PY" -c '
import json,sys
EXCLUDE = {"client"}
cnt = 0
for l in sys.stdin:
    l = l.strip()
    if not l: continue
    try: r = json.loads(l)
    except Exception: continue
    s = r.get("Service") or ""
    if s in EXCLUDE: continue
    cnt += 1
print(cnt)
' 2>/dev/null)"
  printf '    服务条目数（含 --profile doris，排除 client）= %s（票面判据：5 + 2 = 7）\n' "$n" | raw
  if [[ "$n" != "7" ]]; then
    fail "$item（服务条目数 $n != 7）"
    ok=0
  fi

  if [[ $ok -eq 1 ]]; then pass "$item"; else fail "$item"; fi
  return $(( 1 - ok ))
}

# ══════════════════════════════════════════════════════════════════════════════
# ⑧ 数字标注审查（官方给出 / 设计值）
# ══════════════════════════════════════════════════════════════════════════════
check_number_annotations() {
  sec "⑧ 数字标注审查：compose / Dockerfile 里的时间·容量数字须逐处标注"
  local item="⑧ 数字标注审查"

  # 断言化（2026-10-22 code-review 修正）：原实现只 grep|wc -l > 0，一条无关注释就能
  # 通过 —— 属本脚本头部自己点名的恒真判据失败模式；票面要求是「逐处标」不是「至少标一处」。
  # 改为四部分，缺一即 FAIL：
  #   (a) compose 与 docker/flink/ 两边都要有标注词；
  #   (b) Dockerfile（本票新增）带标注行数不得为 0；
  #   (c) compose 的 flink 段（本票改动处）必须有标注；
  #   (d) 全文照旧打印，供人复核语义是否正确（机器只保证不会一处不标）。
  local ok=1 c_annot d_annot f_annot seg_annot

  c_annot="$(grep -c -E '官方给出|设计值|官方未给出' "$COMPOSE_FILE" 2>/dev/null || true)"
  d_annot="$(grep -rn -E '官方给出|设计值|官方未给出' docker/flink/ 2>/dev/null | wc -l | tr -d ' ')"
  printf '    标注词出现次数：compose=%s  docker/flink/=%s\n' "${c_annot:-0}" "${d_annot:-0}" | raw

  # (a) 两边都要有标注
  if [[ "${c_annot:-0}" -eq 0 ]]; then
    fail "$item（compose 里零标注词 —— 票面要求「compose 里新增的时间/容量数字逐处标」）"
    ok=0
  fi
  if [[ "${d_annot:-0}" -eq 0 ]]; then
    fail "$item（docker/flink/ 下零标注词 —— 新增数字面未按本仓风格标注）"
    ok=0
  fi

  # (b) Dockerfile 是本票新增文件，⑧ 的主要对象
  if [[ -f docker/flink/Dockerfile ]]; then
    f_annot="$(grep -c -E '官方给出|设计值|官方未给出' docker/flink/Dockerfile 2>/dev/null || true)"
    printf '    Dockerfile：带标注 %s 行\n' "${f_annot:-0}" | raw
    if [[ "${f_annot:-0}" -eq 0 ]]; then
      fail "$item（Dockerfile 零标注 —— 本票新增的数字面未按本仓风格标注）"
      ok=0
    fi
  fi

  # (c) compose 的 flink 段是本票改动处
  seg_annot="$(awk '/^  flink-jm:/,/^  doris-fe:/' "$COMPOSE_FILE" 2>/dev/null | grep -c -E '官方给出|设计值|官方未给出' || true)"
  printf '    compose flink 段：带标注 %s 行\n' "${seg_annot:-0}" | raw
  if [[ "${seg_annot:-0}" -eq 0 ]]; then
    fail "$item（compose 的 flink 段零标注 —— 本票新增挂载说明缺 设计值/官方给出）"
    ok=0
  fi

  if [[ $ok -eq 1 ]]; then pass "$item（compose 与 docker/flink/ 均有标注、新增面无遗漏；全文已打印供人工复核）"; fi
  return $(( 1 - ok ))
}

# ══════════════════════════════════════════════════════════════════════════════
# 前置检查：docker / compose 可用性
# ══════════════════════════════════════════════════════════════════════════════
preflight() {
  sec "前置检查：docker CLI / compose 可用性"
  if ! command -v docker >/dev/null 2>&1; then
    echo "FATAL: 找不到 docker CLI。本脚本必须在 **WSL** 内运行（宿主无 make，docker 在 WSL 内）。"
    exit 1
  fi
  MSYS_NO_PATHCONV=1 docker version --format '{{.Server.Version}}' 2>&1 | raw
  if ! dc version >/dev/null 2>&1; then
    echo "FATAL: docker compose 不可用。"
    exit 1
  fi
  dc version 2>&1 | raw
  echo "  仓库根 = $REPO_ROOT"
  echo "  本轮 stamp=$RUN_STAMP nonce=$RUN_NONCE"
}

# ══════════════════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════════════════
main() {
  echo "════════════════════════════════════════════════════════════════════════════"
  echo " deploy_surface_check.sh —— 部署面验收实证（逐条；impl-03a 票面 + 4b/4c/4d 演进）"
  echo " 时间: $(date -u +%Y-%m-%dT%H:%M:%SZ)  (UTC)"
  echo "════════════════════════════════════════════════════════════════════════════"
  preflight
  ensure_stack_up

  # 顺序刻意安排为：先静态（⑤⑥⑧），再实机读（①②④），最后 ③（会重启）-> ⑦。
  # ④b（可写性）**必须排在 ②（跨容器互见）之前**：② 的探针由 JM 写入，
  #   若 /warehouse 不可写，② 会以「写入失败」的形式失败，把真正的病因
  #   （属主/权限）掩盖成「挂载不对」。先判可写，② 的失败就只剩「挂载/卷」一个解释。
  check_no_add_jar_host_path
  check_data_ignored
  check_number_annotations
  check_volume_same_path
  check_warehouse_writable
  check_cross_container_visibility
  check_job_jar_usrlib
  check_paimon_connector_in_each_process
  check_flink_patch_anchor
  check_persistence_across_down_up
  check_compose_ps_all_healthy

  hr
  printf '汇总：%d PASS / %d FAIL\n' "$PASS_COUNT" "$FAIL_COUNT"
  if (( FAIL_COUNT > 0 )); then
    printf '失败条目：\n'
    local f
    for f in "${FAILED_ITEMS[@]}"; do printf '  - %s\n' "$f"; done
  fi
  hr
  printf '探针备份留存于：%s\n' "$PROBE_BACKUP_DIR"
  if (( FAIL_COUNT == 0 )); then exit 0; else exit 1; fi
}

main "$@"
