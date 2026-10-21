# Makefile — eco-a-d-platform
#
# 冻结目标名（SRS §3.4，**不得改名**）：up / down / init / replay / verify / spike / verify-fault / logs
# 新增目标（HLD §3.3.6，属设计面）：lint / typecheck / client
#
# 执行面：**WSL2**（HLD §4.7）。凡触碰已发布端口的步骤在 WSL 内直连 127.0.0.1 即可达
#        （P0 实测：Docker 发布端口从 WSL 可达；依赖 WSL integration 开启）。
# 运行宿主：仓库在 F:\ ⇒ WSL 侧是 /mnt/f（DrvFs）。**只用相对路径**，不写 /mnt/f 形绝对路径。

SHELL := /bin/bash

# ★ 工具链自举：WSL 的非交互 shell **不会** source ~/.bashrc（Ubuntu 默认在
#   ~/.bashrc:21-23 用 `case $- in *i*) ;; *) return;;` 早退），因此把 JDK17 /
#   Maven / make 的用户级解包环境显式引入，使 make 在任何 shell 下都能用。
#   依据：P0 实测（无需 sudo，.deb 解包到 ~/p0-toolchain）。
-include $(HOME)/.p0-toolchain-env.mk

export PATH := $(HOME)/.local/bin:$(PATH)

COMPOSE      ?= docker compose
PYBIN        ?= python3
# uv 只是**引导**（找到工程与 .venv 的那个二进制）；ruff / basedpyright 的
# 版本唯一权威是 uv.lock（ADR-0001）。**不要**把 lint/typecheck 改回直调
# ~/.local/bin 下的独立安装——那会重新引入跨机漂移，违背锚点政策（SRS C3）。
UV           ?= $(HOME)/.local/bin/uv

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
	echo "--- 1/5 Kafka 有数据（topic 配置与 gdd §A.2 一致）"; \
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --describe --topic ecom.behavior.event.v1 \
	  | grep -q 'retention.ms=604800000' && echo "  PASS" || { echo "  FAIL"; fail=1; }; \
	echo "--- 2/5 Flink 集群存活（P0 无作业 ⇒ 断言 JM+TM 可见，不断言 job state）"; \
	curl -sf -m 5 http://127.0.0.1:$(FLINK_UI)/overview \
	  | grep -q '"taskmanagers"' && echo "  PASS (JM+TM 可见)" || { echo "  FAIL"; fail=1; }; \
	echo "  ⚠️ P1 起改断言 /jobs/overview 的 jobs[].state==RUNNING —— 该字段**有作业后才存在**（实测 /jobs/overview 现为 {\"jobs\":[]}）"; \
	echo "--- 3/5 Paimon 五层有行（须走 Flink SQL，COUNT(*) 而非 total_record_count）"; \
	echo "  PENDING (P2)"; \
	echo "--- 4/5 Doris ADS 有行 + FE 健康（经 client 服务发起）"; \
	curl -sf -m 5 http://127.0.0.1:$(DORIS_FE)/api/health \
	  | grep -q '"online_backend_num"' && echo "  PASS" \
	  || echo "  PENDING (Doris 镜像须本地构建，见 gdd F15)"; \
	echo "--- 5/5 Superset 健康（响应体是纯文本 OK，不是 JSON）"; \
	curl -sf -m 5 http://127.0.0.1:$(SUPERSET)/health \
	  | grep -q '^OK' && echo "  PASS" || { echo "  FAIL"; fail=1; }; \
	echo "=== verify 五项完成（fail=$$fail，0=全通过）==="; exit $$fail

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
# 一律经 `uv run`：跑的就是 uv.lock 里 pin 住的工具版本（ADR-0001）。
lint:
	@echo "=== ruff check（lint）==="
	$(UV) run ruff check . || exit $$?
	@echo "=== ruff format --check ==="
	$(UV) run ruff format --check . || exit $$?
	@echo "lint: PASS"

typecheck:
	@echo "=== basedpyright（只解析 JSON；--outputjson 与 verbose/stats 互斥）==="
	@$(UV) run basedpyright --outputjson > .basedpyright.json || true; \
	$(UV) run python -c "import json,sys; d=json.load(open('.basedpyright.json')); \
	  s=d['summary']; e=s['errorCount']; w=s['warningCount']; \
	  print(f\"  errors={e} warnings={w} files={s['filesAnalyzed']}\"); \
	  print('  typecheck: PASS' if e==0 else '  typecheck: FAIL'); \
	  sys.exit(1 if e else 0)"

# ── 配置断言（HLD §4.2：config 能列但不报缺，须脚本断言）─────────────
# ⚠️ 逻辑放在 scripts/config_check.py，**不**内嵌 heredoc：
#    make 会把未缩进的 heredoc 正文当成 makefile 语法 ⇒ "missing separator"（已踩到）。
config-check:
	@echo "=== 逐服务 mem_limit / memswap_limit / 端口绑定 断言 ==="
	@$(PYBIN) scripts/config_check.py
