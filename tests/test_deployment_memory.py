"""部署面内存契约的**静态守卫**（impl-03c 实机事故的直接产物）。

## 这条守卫为什么存在

`make replay` 不是「在宿主跑 SQL」，而是 `docker exec eco-flink-jm sql-client.sh -f`
—— **sql-client 的 JVM 与 JobManager 的 JVM 住同一个 cgroup**（同一个容器的同一个
`mem_limit`）。于是 `flink-jm` 的限额必须同时装下：

  1. Flink 进程自己（`jobmanager.memory.process.size`，本仓配 1024m）；
  2. sql-client 客户端（Flink 官方脚本，堆按客户端默认另算）。

2026-10-23 impl-03c 实测：`mem_limit: 1g` 恰好卡在 (1) 这一个数上，(2) 只能靠
内核挤出余量 ⇒ 跑到 ADS 层（最重的一层）时被 **cgroup OOM-kill**：

    oom-kill:constraint=CONSTRAINT_MEMCG, task_memcg=/wsl-user/.../35ada115b62f...,
      task=java,pid=23998
    Memory cgroup out of memory: Killed process 23998 (java)
      total-vm:1876884kB, anon-rss:563428kB

⚠️ 这个症状**不会**让 `.State.OOMKilled` 变 true，而是让 `docker exec` 返回 **137**、
JM 被 `restart: unless-stopped` 拉起（`RestartCount` 每次 +1）⇒ 现场看起来像
「SQL 报错」或「TaskManager 掉线」，与真实根因隔着两层。故必须**静态**钉住，
而不是等下一次跑到 ADS 层才发现。

## 判据形态

`mem_limit(flink-jm) >= jobmanager.memory.process.size + 512MiB`

512MiB 是给 sql-client 客户端 JVM 留的余量（**设计值**，不是官方给出的最小堆）。
本文件只读文本，不连 docker ⇒ 可在无守护进程的环境跑。
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_COMPOSE = _ROOT / "docker-compose.yml"
_FLINK_CONFIG = _ROOT / "docker" / "flink" / "conf" / "config.yaml"

_MIB = 1024**2
# sql-client 是第二个 JVM，与 JM 同 cgroup。512 MiB 是**设计值**（本仓取的经验余量，
# 不是官方最小堆）。实测取值依据：1g 上限下 anon-rss 已到 563 428 kB 就被 OOM-kill，
# 故余量必须显著超过「客户端类加载 + 表环境」的量级。
_CLIENT_HEADROOM = 512 * _MIB


def _parse_size(text: str) -> int:
    """把 `1024m` / `2g` / `1024mb` / 纯字节数解析成字节。"""
    m = re.fullmatch(r"\s*(\d+)\s*([kmg]?b?)\s*", text, re.IGNORECASE)
    assert m, f"无法解析的内存尺寸：{text!r}"
    value, unit = int(m.group(1)), m.group(2).lower()
    factor = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
    }[unit]
    return value * factor


def _compose_service_block(service: str) -> str:
    """粗切出 compose 里某个服务的块（到下一个顶格服务名或文件尾）。"""
    text = _COMPOSE.read_text(encoding="utf-8")
    start = re.search(rf"^  {re.escape(service)}:\s*$", text, re.MULTILINE)
    assert start, f"docker-compose.yml 里找不到服务 {service}"
    rest = text[start.end() :]
    nxt = re.search(r"^  [a-z][\w-]*:\s*$", rest, re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def _compose_mem_limit(service: str) -> int:
    block = _compose_service_block(service)
    m = re.search(r"^\s+mem_limit:\s*(\S+)\s*$", block, re.MULTILINE)
    assert m, f"{service} 缺 mem_limit"
    return _parse_size(m.group(1))


def _yaml_scalar(path_keys: list[str]) -> str | None:
    """按点路径从缩进式 YAML 取标量值（**不引入 pyyaml**）。

    只支持本仓用到的极小子集：嵌套映射 + 标量。够读 config.yaml 的内存段即可；
    为了一个部署面守卫引第三方解析器，会把守卫变成额外依赖。
    """
    stack: list[tuple[int, str]] = []
    for raw in _FLINK_CONFIG.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip() or ":" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        key, _, value = line.strip().partition(":")
        key, value = key.strip(), value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        keys = [k for _, k in stack] + [key]
        if value:  # 叶子（标量）
            if keys == path_keys:
                return value
        else:  # 映射开头
            stack.append((indent, key))
    return None


def _jobmanager_process_size() -> int:
    """读 `jobmanager.memory.process.size`（只认这个点路径）。"""
    value = _yaml_scalar(["jobmanager", "memory", "process", "size"])
    assert value, (
        "docker/flink/conf/config.yaml 里找不到 jobmanager.memory.process.size"
        "（Flink 官方：该键无默认值，不配则启动失败）"
    )
    return _parse_size(value)


def test_flink_jm_mem_limit_covers_its_own_process_plus_sql_client() -> None:
    """`flink-jm` 的限额必须装得下 Flink 进程 **加上** sql-client 的 JVM。

    这是 impl-03c 实机事故的回归守卫（见模块 docstring）。判据：
        mem_limit >= jobmanager.memory.process.size + 512MiB

    ⚠️ 不要改成「只断言 mem_limit == 2g」：那样一旦有人把 process.size 上调到 1536m，
    2g 这个硬编码值就再次变成「装不下」而测试仍绿。
    """
    limit = _compose_mem_limit("flink-jm")
    process = _jobmanager_process_size()
    required = process + _CLIENT_HEADROOM
    assert limit >= required, (
        f"flink-jm 的 mem_limit ({limit / _MIB:.0f} MiB) 装不下 "
        f"jobmanager.memory.process.size ({process / _MIB:.0f} MiB) + "
        f"sql-client 余量 ({_CLIENT_HEADROOM / _MIB:.0f} MiB)；"
        "make replay 会以 docker exec 退出 137 / JM RestartCount 增长的形式失败"
        "（cgroup OOM-kill，不是 SQL 错误）"
    )


# ── 与 config_check.py 的边界（避免一条契约两个真源）────────────────────
# ⚠️ @memswap_limit == mem_limit@ 这条**不在这里重复实现**：
#   HLD §4.2 指定它的落点是 @scripts/config_check.py@（走 @docker compose config@ 的
#   规范化 JSON，不限 flink-jm，覆盖全部 8 个服务）。本轮 code-review 指出首版在本文件
#   重写了同一条断言 ⇒ 一条契约两个真源，改一处漏一处时两边会给出相反结论。
#   ⇒ 删掉重复项；本文件只负责 config_check **不覆盖**的那条：
#     「mem_limit 必须装得下该容器**必然**要住的进程」（需要读 config.yaml，config_check 不做）。
#   config_check 的调用点是 @make config-check@，不进 pytest（要 docker 守护进程）。
