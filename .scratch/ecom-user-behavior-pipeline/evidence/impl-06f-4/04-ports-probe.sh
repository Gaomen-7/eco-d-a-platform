#!/usr/bin/env bash
# impl-06f-4 第四环：冻结端口逐个实测（票面「端口核实」）。
# 八端口两层探测：HTTP 面用 curl（8030/8040/8081/8088）；协议面（9010/9030/9060/9092）
# 用 TCP 握手——README「已知环境限制」明示 curl 对 MySQL/brpc/Kafka 端口报
# 「连不上」但 TCP 层可达，协议层失败 ≠ 端口不通。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/04-ports-probe.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

echo "PROBE_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "（COMPOSE_EXTRA_FILES=[${COMPOSE_EXTRA_FILES:-}]，全程未设）"
echo

echo "== 1. HTTP 面逐端口 curl（-m 5，记 http_code 与错误）=="
declare -A probe=(
  [8030]="/api/health"
  [8040]="/metrics"
  [8081]="/overview"
  [8088]="/health"
)
for port in 8030 8040 8081 8088; do
  url="http://127.0.0.1:${port}${probe[$port]}"
  code=$(curl -s -o /dev/null -w "%{http_code}" -m 5 "$url" 2>&1) || code="curl_rc=$?"
  body=$(curl -s -m 5 "$url" 2>/dev/null | head -c 60 | tr -d "\n")
  printf "port %-5s %-8s curl=%-22s body=[%s]\n" "$port" "HTTP" "$code" "$body"
done

echo
echo "== 2. 协议面 TCP 握手（bash /dev/tcp；9010 edit_log / 9030 MySQL / 9060 brpc / 9092 Kafka）=="
for port in 9010 9030 9060 9092; do
  if timeout 3 bash -c "echo > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
    printf "port %-5s %-8s %s\n" "$port" "TCP" "可达（OPEN）"
  else
    printf "port %-5s %-8s %s\n" "$port" "TCP" "不可达"
  fi
done

echo
echo "== 3. 容器侧发布面核对（docker port —— 端口映射面事实）=="
for c in eco-doris-fe eco-doris-be eco-flink-jm eco-superset eco-kafka; do
  echo "-- $c --"
  docker port "$c" 2>&1
done

echo
echo "== 4. （对照组）协议面端口也各跑一次 curl，印证「协议层失败 ≠ TCP 不通」=="
for port in 9010 9030 9060 9092; do
  code=$(curl -s -o /dev/null -w "%{http_code}" -m 5 "http://127.0.0.1:$port/" 2>&1) || code="curl_rc=$?"
  printf "port %-5s curl-http=%s（预期非 200：协议不是 HTTP；TCP 层可达见上表）\n" "$port" "$code"
done
