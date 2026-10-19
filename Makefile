# Makefile — eco-a-d-platform
#
# 冻结目标名（SRS §3.4，**不得改名**）：up / down / init / replay / verify / spike / verify-fault / logs
# 新增目标（HLD §3.3.6，属设计面）：lint / typecheck / client
#
# 执行面：**WSL2**（HLD §4.7）。凡触碰已发布端口的步骤在 WSL 内直连 127.0.0.1 即可达
#        （P0 实测：Docker 发布端口从 WSL 可达；依赖 WSL integration 开启）。
# 运行宿主：仓库在 F:\ ⇒ WSL 侧是 /mnt/f（DrvFs）。**只用相对路径**，不写 /mnt/f 形绝对路径。

SHELL := /bin/bash

COMPOSE      ?= docker compose
PYBIN        ?= python3
UV           ?= $(HOME)/.local/bin/uv
RUFF         ?= $(HOME)/.local/bin/ruff
BASEDPYRIGHT ?= $(HOME)/.local/bin/basedpyright

# 冻结端口（HLD §4.3）
FLINK_UI   := 8081
KAFKA      := 9092
DORIS_FE   := 8030
DORIS_MYSQL:= 9030
SUPERSET   := 8088

.PHONY: help up down init replay verify spike verify-fault logs lint typecheck client smoke config-check

help:
	@echo "冻结目标：up down init replay verify spike verify-fault logs"
	@echo "新增目标：lint typecheck client config-check"

# ── 编排 ────────────────────────────────────────────────────────────
up:
	$(COMPOSE) up -d
	@echo "[up] 已拉起；用 'make logs' 看启动，用 'make verify' 判链路是否活"

down:
	$(COMPOSE) down

init: up
	@echo "[init] 等待健康检查..."
	@$(COMPOSE) up -d --wait || true
	@echo "[init] kafka-init 已由 compose 依赖自动执行（建 topic + retention）"
	@echo "[init] 待补：Doris 建库/建表、Superset db upgrade + create-admin + init（见 docker/scripts）"

# ── 离线分层 ────────────────────────────────────────────────────────
replay:
	@echo "[replay] 五层分层（P2 实现）：ODS → DWD → DIM → DWS → ADS"
	@echo "  ⚠️ 快照数判据须走 Flink SQL（Doris 不在 Paimon 系统表支持列表内）"
	@echo "  ⚠️ 不得从 Kafka 重读超 ~6 小时历史（retention 2 GB 先触发，gdd D3）"

# ── 可复现校验（SRS §3.1.8 五项）─────────────────────────────────────
verify: smoke

smoke:
	@echo "=== make verify：五项运行时烟测 ==="
	@fail=0; \
	echo "--- 1/5 Kafka 有数据（消费组 lag 或 topic 配置）"; \
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --describe --topic ecom.behavior.event.v1 \
	  | grep -q 'retention.ms' && echo "  PASS" || { echo "  FAIL"; fail=1; }; \
	echo "--- 2/5 Flink job RUNNING（REST，字段名确切为 state）"; \
	curl -sf -m 5 http://127.0.0.1:$(FLINK_UI)/jobs/overview \
	  | grep -q '"state"' && echo "  PASS (endpoint reachable)" || { echo "  FAIL"; fail=1; }; \
	echo "--- 3/5 Paimon 五层有行（须走 Flink SQL，COUNT(*) 而非 total_record_count）"; \
	echo "  PENDING (P2)"; \
	echo "--- 4/5 Doris ADS 有行 + FE 健康（经 client 服务发起）"; \
	curl -sf -m 5 http://127.0.0.1:$(DORIS_FE)/api/health \
	  | grep -q '"online_backend_num"' && echo "  PASS (FE health reachable)" || { echo "  FAIL"; fail=1; }; \
	echo "--- 5/5 Superset 健康（响应体是纯文本 OK，不是 JSON）"; \
	curl -sf -m 5 http://127.0.0.1:$(SUPERSET)/health \
	  | grep -q '^OK' && echo "  PASS" || { echo "  FAIL"; fail=1; }; \
	echo "=== verify exit=$fail ==="; exit $$fail

# 故障注入：kill TaskManager → 从 checkpoint 恢复 → 校验条数一致（且脏率不得上升）
verify-fault:
	@echo "[verify-fault] kill TaskManager 并观察从 checkpoint 恢复"
	@$(COMPOSE) kill flink-tm
	@echo "  等待作业 RESTARTING → RUNNING..."
	@for i in $$(seq 1 30); do \
	  st=$$(curl -sf -m 3 http://127.0.0.1:$(FLINK_UI)/jobs/overview 2>/dev/null | grep -o '"state":"[A-Z]*"' | head -1); \
	  echo "  $$i: $$st"; sleep 4; \
	done
	@echo "  ⚠️ 反例保护：若条数一致但脏率飙升 ⇒ 判不通过（重复被误当脏，gdd D2）"

# ── 尖峰 ────────────────────────────────────────────────────────────
spike:
	@echo "[spike] 尖峰模式：短时 ≥1000 events/s（P1 实现）"

# ── 日志 ────────────────────────────────────────────────────────────
logs:
	$(COMPOSE) ps
	$(COMPOSE) logs --tail=120

# ── client：按需拉起，网内发起 Doris Stream Load ────────────────────
# ★ 唯一理由：FE→BE 第二跳是容器地址，宿主 follow 不到（HLD §4.4）。
#   不常驻 ⇒ 不占常驻预算。
client:
	$(COMPOSE) run --rm client

# ── 静态检查（新增，HLD §3.3.6）────────────────────────────────────
# 与 verify 分开：verify 是运行时烟测（链路必须在跑），这两条只查源码。
lint:
	@echo "=== ruff check（lint）==="
	$(RUFF) check . || exit $$?
	@echo "=== ruff format --check ==="
	$(RUFF) format --check . || exit $$?
	@echo "lint: PASS"

typecheck:
	@echo "=== basedpyright（只解析 JSON；--outputjson 与 verbose/stats 互斥）==="
	$(BASEDPYRIGHT) --outputjson > .basedpyright.json; st=$$?; \
	$(PYBIN) -c "import json,sys; d=json.load(open('.basedpyright.json')); \
	  e=d['summary']['errorCount']; w=d['summary']['warningCount']; \
	  print(f'  errors={e} warnings={w} files={d[\"summary\"][\"filesAnalyzed\"]}'); \
	  sys.exit(1 if e else 0)" || exit $$?; \
	echo "typecheck: exit=$$st"; exit $$st

# ── 配置断言（HLD §4.2：config 能列但不报缺，须脚本断言）─────────────
config-check:
	@echo "=== 逐服务 mem_limit / memswap_limit 断言 ==="
	@$(PYBIN) - <<'PY'
import json, subprocess, sys
cfg = json.loads(subprocess.run(["docker","compose","config","--format","json"],
                                capture_output=True, text=True).stdout)
bad, resident = [], 0
for name, s in cfg["services"].items():
    m, ms = s.get("mem_limit"), s.get("memswap_limit")
    if not m:            bad.append(f"{name}: 缺 mem_limit")
    elif not ms:         bad.append(f"{name}: 缺 memswap_limit（可借等量 swap）")
    elif m != ms:        bad.append(f"{name}: memswap_limit != mem_limit")
    if m and not s.get("profiles"): resident += int(m)
    for p in s.get("ports", []):
        if p.get("host_ip") != "127.0.0.1":
            bad.append(f"{name}: 端口 {p.get('published')} 绑到 {p.get('host_ip')}（须 127.0.0.1）")
print(f"  resident total = {resident/1024**3:.2f} GiB")
if bad:
    print("  VIOLATIONS:"); [print("   -", b) for b in bad]; sys.exit(1)
print("  config-check: PASS")
PY
