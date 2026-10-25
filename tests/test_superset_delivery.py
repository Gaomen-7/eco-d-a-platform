"""Superset 交付面的**静态守卫**（impl-04 实机事故的直接产物）。

## 这些守卫为什么存在

impl-04 在实机上踩了三处「配置看起来对、跑起来必炸」的缺口，每一处都
靠静态钉住防止回退（判据形态见各 docstring；事故取证见票面
`impl-04-dashboard-delivery.md` §Progress）：

1. **bootstrap command 缺失 ⇒ PyMySQL 从未装上**：镜像 Entrypoint=null、
   CMD 直跑 run-server.sh，`docker/docker-bootstrap.sh app-gunicorn` 才是
   官方 compose 装 requirements-local 的路径。少了它，任何真实查询必炸
   `No module named 'MySQLdb'`。
2. **`database_uuid` 不固定 ⇒ zip 导入后图表全断链**：dashboard importer
   逐字 `config["database_id"] = database_ids[config["database_uuid"]]`，
   置备脚本必须把 UUID 冻结成常量并回填。
3. **import-dashboards 缺 `--username` ⇒ 导入直接报错退出**：6.1.0 CLI
   硬要求该选项（2026-10-25 实机复现 `Missing option '--username' / '-u'`）。

本文件只读文本，不连 docker / 不进 Superset ⇒ 可在无守护进程的环境跑。
"""

from __future__ import annotations

import re
import uuid as uuidlib
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_COMPOSE = _ROOT / "docker-compose.yml"
_REQUIREMENTS = _ROOT / "docker" / "superset" / "requirements-local.txt"
_CONFIG = _ROOT / "docker" / "superset" / "superset_config.py"
_PROVISION_PY = _ROOT / "docker" / "scripts" / "superset-provision.py"
_PROVISION_SH = _ROOT / "docker" / "scripts" / "superset-provision.sh"


def _compose_service_block(service: str) -> str:
    """粗切出 compose 里某个服务的块（到下一个顶格服务名或文件尾）。"""
    text = _COMPOSE.read_text(encoding="utf-8")
    start = re.search(rf"^  {re.escape(service)}:\s*$", text, re.MULTILINE)
    assert start, f"docker-compose.yml 里找不到服务 {service}"
    rest = text[start.end() :]
    nxt = re.search(r"^  [a-z][\w-]*:\s*$", rest, re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def test_superset_runs_bootstrap_command_as_official_compose() -> None:
    """superset 服务必须走官方 bootstrap command + root 用户（PyMySQL 装载路径）。

    官方 6.1.0 `docker-compose-image-tag.yml` 逐字是
    `command: ["/app/docker/docker-bootstrap.sh", "app-gunicorn"]` + `user: "root"`。
    镜像自己的 CMD 直跑 run-server.sh（不装 requirements-local）⇒ 首跑
    `import pymysql` 必失败。守卫两条：command 含 docker-bootstrap.sh、
    user 是 root（bootstrap 要往 root 所有的 .venv 写）。
    """
    block = _compose_service_block("superset")
    assert "docker-bootstrap.sh" in block, (
        "superset 缺官方 bootstrap command：PyMySQL（requirements-local）"
        "不会随容器启动装载，任何真实查询必炸 No module named 'MySQLdb'"
    )
    m = re.search(r"^\s+user:\s*\"?root\"?\s*$", block, re.MULTILINE)
    assert m, "superset 必须以 root 跑（官方同款；bootstrap 写 /app/.venv 需要）"


def test_requirements_local_pins_pymysql() -> None:
    """requirements-local.txt 必须含 PyMySQL（MySQLdb 硬编码的兼容层载体）。

    Superset `db_engine_specs/mysql.py:405` 逐字 `import MySQLdb`，而
    mysqlclient 2.2.7 在 6.1.0 镜像编译失败（无 pkg-config）⇒ 唯一路径是
    PyMySQL + `pymysql.install_as_MySQLdb()`（后者由 superset_config.py 做，
    这里只钉依赖在位）。
    """
    text = _REQUIREMENTS.read_text(encoding="utf-8")
    assert re.search(r"^\s*PyMySQL\b", text, re.MULTILINE), (
        "requirements-local.txt 缺 PyMySQL：Doris 走 MySQL 协议查询的驱动没了"
    )


def test_superset_config_installs_pymysql_as_mysqldb() -> None:
    """superset_config.py 必须在 engine spec 加载前装 MySQLdb 兼容层。

    `pymysql.install_as_MySQLdb()` 必须出现在文件顶部 import 区（先于
    Superset 首次 import mysql engine spec）。
    """
    text = _CONFIG.read_text(encoding="utf-8")
    assert "install_as_MySQLdb" in text, (
        "superset_config.py 缺 pymysql.install_as_MySQLdb()："
        "db_engine_specs/mysql.py 硬编码 import MySQLdb 必炸"
    )


def test_provision_freezes_database_uuid() -> None:
    """置备脚本必须冻结 database_uuid（zip 导入按 UUID 匹配 database）。

    dashboard importer 逐字
    `config["database_id"] = database_ids[config["database_uuid"]]` ⇒
    不冻结则 zip 导入到新实例时全部图表断链。判据三件事：
      1. 脚本里有冻结常量（合法 uuid4 形态）；
      2. 该常量真的被用在 Database 的 uuid 赋值上；
      3. UUID 不符时会回填（存在回填分支）。
    """
    text = _PROVISION_PY.read_text(encoding="utf-8")
    m = re.search(r'ECOM_ADS_UUID\s*=\s*"([0-9a-f-]{36})"', text)
    assert m, "superset-provision.py 缺冻结的 ECOM_ADS_UUID 常量"
    # 必须是合法 uuid（首版曾写成非法占位串，导入时静默错位）
    uuidlib.UUID(m.group(1))
    assert "uuid=uuidlib.UUID(ECOM_ADS_UUID)" in text, (
        "冻结的 UUID 常量必须用在 Database 的 uuid 赋值上"
    )
    assert "dbobj.uuid = uuidlib.UUID(ECOM_ADS_UUID)" in text, (
        "必须有「已存在但 UUID 不符 ⇒ 回填」分支"
    )


def test_provision_sh_guards_msys_path_conversion() -> None:
    """provision 包装脚本必须自防 MSYS 路径转换（Git-Bash 宿主）。

    实机证伪：Git-Bash 把 `exec superset python /docker/scripts/...` 的参数
    改写成宿主绝对路径（`can't open file '/app/C:/Users/...'`）。
    `make init` 在 WSL 跑不受影响，但脚本要自防一手。
    """
    text = _PROVISION_SH.read_text(encoding="utf-8")
    assert "MSYS_NO_PATHCONV=1" in text, (
        "superset-provision.sh 缺 export MSYS_NO_PATHCONV=1"
        "（Git-Bash 宿主上 docker compose exec 的容器内路径会被改写）"
    )


def test_init_sh_imports_zip_with_username() -> None:
    """zip 导入命令必须带 --username（6.1.0 CLI 硬要求）且在 provision.sh 里置备后跑。

    2026-10-25 实机复现：缺省报 `Missing option '--username' / '-u'`，导入直接退出。
    ★ code-review 修复后导入段在 superset-provision.sh（置备后），不在 init.sh——
    importer 按 database_uuid 匹配 database，置备前导入必 KeyError。
    """
    text = _PROVISION_SH.read_text(encoding="utf-8")
    # 命令形态：`superset import-dashboards \` 换行后同一命令里出现 `-p ... -u`
    m = re.search(r"superset import-dashboards\s*\\\s*\n\s*-p\s+\S+\s+-u", text)
    assert m, (
        "superset-provision.sh 的 import-dashboards 必须带 -u/--username"
        "（6.1.0 CLI 硬要求，缺省报 Missing option '--username'）"
    )
    # 顺序承重：导入必须在置备（provision.py）之后
    prov_pos = text.find("superset-provision.py")
    import_pos = text.find("superset import-dashboards")
    assert prov_pos != -1 and import_pos > prov_pos, (
        "zip 导入必须跑在 superset-provision.py（冻结 UUID 建 database）之后："
        "importer 按 database_uuid 匹配，顺序颠倒在全新卷上必 KeyError"
    )
