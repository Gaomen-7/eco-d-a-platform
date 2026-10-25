# Superset 6.1.0 配置 —— 只写 HLD/gdd 明确的必需项，不搬演示配置。
#
# 依据：
#  - REQ-F-BI-03「零账号只读」：默认 init 只建管理员 ⇒ 默认**非**零账号，必须显式开 Public 角色。
#  - R3 降配 A：CELERY_CONFIG=None 关闭全部 celery 配置（官方原话
#    "Set celery config to None to disable all the above configuration"）
#    ⇒ worker/beat 两个容器可整段不写，省 ≈0.3–0.5 GB。
#  - 元数据库走 compose 内的 postgres（降配 B 的另一半在此体现：不再依赖 Redis）。
import os

# ── MySQLdb 兼容层（impl-04 实机证伪补丁，先于一切 engine spec）────────
# Superset 6.1.0 的 MySQL engine spec 在 db_engine_specs/mysql.py:405 逐字
# `import MySQLdb`（取 FIELD_TYPE 类型码表）；镜像无 mysqlclient（编译失败：
# 无 pkg-config），只有 PyMySQL。本文件经 PYTHONPATH 先于 engine spec 加载，
# 在这里执行官方兼容层（PyMySQL 文档 "MySQLdb Compatibility"）：
#   pymysql.install_as_MySQLdb()  ⇒  sys.modules["MySQLdb"] = pymysql
# 之后 engine spec 的 import MySQLdb 命中的是 PyMySQL（协议兼容 MySQL 5.x+）。
# Doris FE 走 MySQL 协议，实测查询正常。
try:
    import pymysql

    pymysql.install_as_MySQLdb()
except ImportError:  # 引导期（requirements-local 尚未装上）不致命：
    # docker-bootstrap.sh 装完 PyMySQL 后重启的进程会走到上面的分支。
    pass

# ── 元数据库 ─────────────────────────────────────────────────────────
# ★ P0 实测：官方 superset:6.1.0 镜像**不含任何 DB 驱动**（psycopg2/psycopg/
#   mysqlclient/pymysql 全 MISSING）⇒ 不能直接用 Postgres 元数据库。
#   处置：元数据库用 SQLite（镜像自带 sqlite3 3.40.1，零驱动依赖）。
#   URI 由 compose 的 SUPERSET_DB_URI 提供，这里只做回落。
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "sqlite:////app/superset_home/superset.db",
)

# ── 关闭 Celery（R3 降配 A）──────────────────────────────────────────
# 本项目无异步查询/定时报告/缩略图需求（SRS §6.1 范围），故整段关闭。
CELERY_CONFIG = None

# 无 Redis：默认 NullCache + 元数据库即够（官方 config.py 默认值）
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}

# ── 零账号只读（REQ-F-BI-03）────────────────────────────────────────
# Public 角色继承 Gamma（只读）；评审者无需任何账号即可打开看板。
#
# ★★ impl-04 实机取证（2026-10-25）：`AUTH_TYPE = AUTH_ANONYMOUS`（即 0）在
#   Superset 6.1.0 / FAB 5.0.2 **不可用**——`AUTH_ANONYMOUS` 常量已从
#   `flask_appbuilder.const` 删除（只剩 AUTH_DB=1/AUTH_LDAP=2/AUTH_REMOTE_USER=3/
#   AUTH_OAUTH=6），且 SecurityManager.register_views 的 auth_view 分支不含 0：
#   AUTH_TYPE=0 时 user_view 保持 None → `add_view(None)` 崩
#   （AttributeError: 'NoneType' object has no attribute 'appbuilder'）。
#   票面（impl-04 ①）的 `AUTH_TYPE = AUTH_ANONYMOUS` 是「匿名前提」的意图；
#   落地面＝保留 AUTH_DB（默认）+ 下面的 PUBLIC_ROLE_LIKE + **dataset 显式授
#   Public**（置备脚本 superset-provision.py 做）——FAB 对匿名请求的判定链是
#   `has_access → is_item_public → Public 角色的权限`（security/manager.py:1587
#   逐字 "Check if current user or public has access to view"），实测匿名
#   GET /superset/dashboard/<slug>/ 与 /api/v1/dashboard/<id> 均 200。
AUTH_ROLE_PUBLIC = "Public"
PUBLIC_ROLE_LIKE = "Gamma"
# 关掉匿名用户自助注册（互联网侧暴露面收窄；本机 127.0.0.1 仍可匿名查看）
AUTH_USER_REGISTRATION = False

# ── 刷新间隔（REQ-F-BI-02 ≤10 s）────────────────────────────────────
# 官方 SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT=0 只是**警告阈值**，
# 不是硬下限（gdd §1.6 已核实）⇒ 10 s 可实现。
SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT = 0

# ── 其它 ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
    "EMBEDDED_SUPERSET": False,
}
# 避免 dev 默认的示例数据/WebSocket
SUPERSET_LOAD_EXAMPLES = False
WTF_CSRF_ENABLED = True
