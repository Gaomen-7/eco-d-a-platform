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

.PHONY: help up down init replay verify spike verify-fault verify-fault-corrupt logs lint typecheck client smoke config-check

help:
	@echo "冻结目标：up down init replay verify spike verify-fault logs"
	@echo "新增目标（设计面）：verify-fault-corrupt lint typecheck client config-check"

# ── 编排 ────────────────────────────────────────────────────────────
up:
	$(COMPOSE) up -d
	@echo "[up] 已拉起；用 'make logs' 看启动，用 'make verify' 判链路是否活"

down:
	$(COMPOSE) down

# `make init` = 一条命令把环境弄到「能跑 verify」的状态。
# 四段**冻结**（spec §3.3，票面 ③）：顺序不可交换，缺一段则后续判据无意义。
init: up
	@echo "[init] 第一段：默认 profile up -d"
	@$(COMPOSE) up -d --wait || true
	@echo "[init] 第一段：kafka-init 已由 compose 依赖自动执行（建 topic + retention）"
	@echo "[init] 第二段：Doris profile + 初始化脚本"
	@$(COMPOSE) --profile doris up -d --wait doris-fe doris-be
	@$(COMPOSE) run --rm client /scripts/doris-init.sh
	@echo "[init] 第三段：Superset 引导（db upgrade → create-admin → init）"
	@bash docker/scripts/superset-init.sh
	@echo "[init] 第四段：断言经 client 查 ADS 真有行（不是查 FE 健康）"
	@bash docker/scripts/doris-assert.sh

# ── 离线分层 ────────────────────────────────────────────────────────
replay:
	@echo "[replay] 五层分层（P2 实现）：ODS → DWD → DIM → DWS → ADS"
	@echo "  ⚠️ 快照数判据须走 Flink SQL（Doris 不在 Paimon 系统表支持列表内）"
	@echo "  ⚠️ 不得从 Kafka 重读超 ~6 小时历史（retention 2 GB 先触发，gdd D3）"

# ── 可复现校验（SRS §3.1.8 五项；spec §4.3 分层）─────────────────────
# 判据逻辑在 src/ecomgen/verify_runner.py，**不**内嵌 heredoc：
#   make 会把未缩进的 heredoc 正文当成 makefile 语法（config-check 已踩过）。
# P1 形态：4 真 + 1 显式 SKIP（第三项「Paimon 五层有行」跳过，不计通过不计 fail）。
verify:
	@$(UV) run python -m ecomgen.verify_runner

# 故障注入：kill TaskManager → 从 checkpoint 恢复 → 证明重放被幂等吸收
# 判据（spec §4.4）在 src/ecomgen/fault.py；超时 120 s / 300 s 均为**设计值**。
verify-fault:
	@$(UV) run python -m ecomgen.fault

# 反例证伪（票面验收第 7 条）：同一流程 + 同一判据，只是往主流里注入脏数据。
# **期望退出非 0** —— 若这条返回 0，说明 verify-fault 的判据是恒真的假断言。
# 判读：看 DLQ 增量必须 != 0、结论必须「不通过」。
verify-fault-corrupt:
	@$(UV) run python -m ecomgen.fault artifacts/verify --corrupt; \
	  rc=$$?; \
	  if [ $$rc -eq 0 ]; then \
	    echo "[verify-fault-corrupt] ✗ 反例竟然通过 —— 判据是恒真的，不成立"; \
	    exit 1; \
	  fi; \
	  echo "[verify-fault-corrupt] ✓ 反例按预期失败（退出码 $$rc）—— 判据确实在断言"; \
	  exit 0

# ── 尖峰 ────────────────────────────────────────────────────────────
spike:
	@echo "[spike] 尖峰模式：短时 ≥1000 events/s"
	@$(UV) run ecomgen live --seed 1 --sessions 5000 --rate 1000 \
	  --bootstrap $${KAFKA_BOOTSTRAP:-kafka:9092} --dry-run
	@echo "  ⚠️ 真发需 compose 网内可达 kafka:9092（宿主跑要改用 127.0.0.1:9092）"

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
