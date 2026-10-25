# Superset 置备 —— impl-04 看板交付的落地面（`make init` 第④段的增补步骤）。
#
# 职责（全部幂等，重跑覆盖）：
#   1. 建 `ecom_ads` Database 连接，**固定 UUID**（票面 ⑥ 源码级硬约束：
#      dashboard importer 逐字 `config["database_id"] = database_ids[config["database_uuid"]]`
#      ⇒ 不固定 UUID 则 zip 导入后全部图表断链）；
#   2. 建虚拟 dataset（SQL 写在 dataset 上，不依赖物理表同步）；
#   3. 建五面板图表（全部内置 viz）＋三态 B 载体 Table ＋ A 式双 Big Number ＋脏率面板；
#   4. 建单一 dashboard，`json_metadata.refresh_frequency = 10`（票面 ④ 判据）；
#   5. **dataset 显式授给 Public**（票面 ① 第③步——Public 只有 UI/API 权限，
#      不授具体 dataset 则面板无数据；置备脚本若不授权，验收第 2 条的
#      「反差证明」就做不了）。
#
# 为什么是 Python 而不是 import-dashboards 一步到位：zip 是 `make init` 的
# **复现手段**（票面 ⑥），而本脚本是**活实例的置备与修复面**——两者并存：
#   - `make init` 路径：superset-init.sh（引导）→ export-dashboards 产出的 zip
#     （首次由本脚本生成后 export）→ import-dashboards；
#   - 本脚本路径：直接 ORM 置备（不依赖 zip 的 YAML 漂移），并负责 Public
#     授权（zip 导入**不含**角色授权——FAB 的 Role/Permission 不在 dashboard
#     export 的载荷里，官方未给出，实测 zip 内只有 dashboard/chart/dataset）。
#
# 判据纪律（票面 Notes）：「UI 里能看到」「配置可见」不是判据。本脚本只负责
# 把东西置备对；判据走 superset-verify.sh（匿名 HTTP 实测）与浏览器 GIF。
#
# 运行环境：**在 superset 容器内**执行（要 ORM 与 PyMySQL 驱动）：
#   docker compose exec -T superset python /docker/scripts/superset-provision.py
# （/docker/scripts 由 compose 只读挂载提供；见 docker-compose.yml superset.volumes）
"""Superset 置备脚本（impl-04）。幂等；容器内运行。"""

from __future__ import annotations

import json
import sys
import uuid as uuidlib

# ── 冻结常量（与 docker/doris/init.sql、票面对齐）──────────────────────
# ★ 冻结的 database UUID（票面 ⑥：zip 导入按 UUID 匹配 database）。
#   首版写成过含 'ecom04dashboard' 的占位串（非法 uuid4 形态）——删。
ECOM_ADS_UUID = "0f4d8a52-9b3e-4c71-a6d8-3f2b1c9e7a04"
SQLALCHEMY_URI = "mysql+pymysql://root@doris-fe:9030/ecom_ads"
DASHBOARD_SLUG = "ecom-live"
DASHBOARD_TITLE = "电商用户行为实时看板（三态·零账号·10s 刷新）"

# 图表 viz 类型全部取自 6.1.0 MainPreset 默认注册清单（票面 ② 取证）。
VIZ_BIG_NUMBER = "big_number"
VIZ_ECHART_TIMESERIES_BAR = "echarts_timeseries_bar"
VIZ_ECHART_FUNNEL = "echarts_funnel"  # legacy Funnel 已不在 6.1.0 注册清单
VIZ_ECHART_TREEMAP = "echarts_treemap"  # legacy Treemap 已移除，ECharts 版默认内置
# 地域横向排行也用 timeseries_bar（6.1.0 无独立横向 bar viz；横向靠
# x_axis/y_axis 交换——评审指出 orientation 键属 legacy dist_bar，是 no-op）
VIZ_TABLE = "table"

# 时间粒度：PT1M = 1 分钟（ISO-8601 期间）。★ 首版误写 P1M = 1 个月
#   （code-review 指出），分钟级窗口的正确写法是 PT1M。
GRAIN_PT1M = "PT1M"


def _adhoc_metric(column: str, label: str) -> dict:
    """6.1.0 探索器同款的 adhoc SIMPLE metric 形态（评审指出旧
    `{"metricName","aggregateType"}` 形态会被 6.1.0 丢弃）。"""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": "SUM",
        "label": label,
    }


def _eq_filter(subject: str, comparator: str) -> dict:
    """6.1.0 adhoc filter 形态（旧顶层 `where` 字符串会被丢弃）。"""
    return {
        "expressionType": "SIMPLE",
        "subject": subject,
        "operator": "==",
        "operatorId": "EQUALS",
        "comparator": comparator,
        "clause": "WHERE",
    }


VIZ_TABLE = "table"

# 三态 CASE 表达式（票面 ③ B 主载体，逐字）：
STATE_CASE = (
    "CASE WHEN is_settled=0 THEN '暂定' WHEN revision>0 THEN '已修订' ELSE '落定' END"
)


def die(msg: str) -> None:
    print(f"[superset-provision] ✗ {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        # ── ① Database（固定 UUID：zip 重导入的硬约束）──────────────────
        dbobj = (
            db.session.query(Database).filter_by(database_name="ecom_ads").one_or_none()
        )
        if dbobj is None:
            dbobj = Database(
                database_name="ecom_ads",
                sqlalchemy_uri=SQLALCHEMY_URI,
                uuid=uuidlib.UUID(ECOM_ADS_UUID),
            )
            db.session.add(dbobj)
            print("[superset-provision] + database ecom_ads（新建，固定 UUID）")
        elif str(dbobj.uuid) != ECOM_ADS_UUID:
            # 已存在但 UUID 不对 ⇒ 回填（票面「固定/回填」的回填支）
            dbobj.uuid = uuidlib.UUID(ECOM_ADS_UUID)
            print("[superset-provision] ~ database ecom_ads UUID 已回填为冻结值")
        db.session.commit()

        # ── ② 虚拟 dataset（每个面板一个，SQL 层承载口径）───────────────
        datasets: dict[str, SqlaTable] = {}

        def upsert_dataset(
            name: str,
            sql: str,
            columns: list[tuple[str, str]],
            metrics: list[tuple[str, str]] | None = None,
        ) -> SqlaTable:
            """建/刷新虚拟 dataset，并**物化列与度量**（ORM 行，非可选）。

            实机证伪（impl-04）：只写 `sql` 不建 TableColumn/SqlMetric 时，
            查询处理器报 `Columns missing in dataset`——SqlaTable 的列不在
            SQL 里推演，必须逐列落 ORM。metrics 缺省给每数值列一个 SUM。
            """
            from superset.connectors.sqla.models import SqlMetric, TableColumn

            tbl = (
                db.session.query(SqlaTable)
                .filter_by(table_name=name, database_id=dbobj.id)
                .one_or_none()
            )
            if tbl is None:
                tbl = SqlaTable(table_name=name, database_id=dbobj.id, sql=sql)
                db.session.add(tbl)
            else:
                tbl.sql = sql
            db.session.flush()
            # 列：整组重建（幂等；旧列残留会让 WHERE/SELECT 引用悬空列）
            for old in list(tbl.columns):
                db.session.delete(old)
            db.session.flush()
            for col_name, col_type in columns:
                tbl.columns.append(
                    TableColumn(
                        column_name=col_name,
                        type=col_type,
                        groupby=True,
                        filterable=True,
                    )
                )
            for old in list(tbl.metrics):
                db.session.delete(old)
            db.session.flush()
            for metric_name, expr in metrics or [
                (f"sum__{c}", f"SUM({c})")
                for c, t in columns
                if t in ("BIGINT", "LONG", "INTEGER")
            ]:
                tbl.metrics.append(
                    SqlMetric(
                        metric_name=metric_name,
                        expression=expr,
                        verbose_name=metric_name,
                    )
                )
            db.session.flush()
            datasets[name] = tbl
            return tbl

        # 1. 实时 PV/UV/GMV（Big Number 家族 + ECharts Timeseries Bar）
        #    回看窗设计值：KPI 120 分钟（两小时运营视图）、三态表 30 分钟
        #    （跳变取证窗）；均非官方给出。
        upsert_dataset(
            "kpi_current",
            "SELECT window_start, metric_name, metric_value, revision, is_settled, "
            f"{STATE_CASE} AS state "
            "FROM ecom_ads.ads_window_metric "
            "WHERE metric_name IN ('event_page_view','uv','gmv') "
            "AND window_start >= DATE_SUB(NOW(), INTERVAL 120 MINUTE) "
            "ORDER BY window_start DESC",
            columns=[
                ("window_start", "DATETIME"),
                ("metric_name", "STRING"),
                ("metric_value", "BIGINT"),
                ("revision", "INTEGER"),
                ("is_settled", "INTEGER"),
                ("state", "STRING"),
            ],
        )
        # 2. 4 步转化漏斗（ECharts Funnel；观测/推断同屏双列见 dataset 5）
        upsert_dataset(
            "funnel_steps",
            "SELECT step_no, "
            "SUM(cnt_observed) AS cnt_observed, "
            "SUM(cnt_inferred) AS cnt_inferred "
            "FROM ecom_ads.ads_funnel_step "
            # 当日+昨日=设计值：漏斗跨日会话未关窗前的双日视图
            "WHERE session_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) "
            "GROUP BY step_no ORDER BY step_no",
            columns=[
                ("step_no", "INTEGER"),
                ("cnt_observed", "BIGINT"),
                ("cnt_inferred", "BIGINT"),
            ],
        )
        # 3. 品类分布（ECharts Treemap；60 分钟回看=设计值：运营视图取近一小时头部品类）
        upsert_dataset(
            "category_dist",
            "SELECT dim_key AS category_id, SUM(metric_value) AS pv "
            "FROM ecom_ads.ads_window_metric "
            "WHERE metric_name='event_page_view' AND dim_key != '' "
            "AND window_start >= DATE_SUB(NOW(), INTERVAL 60 MINUTE) "
            "GROUP BY dim_key ORDER BY pv DESC",
            columns=[
                ("category_id", "STRING"),
                ("pv", "BIGINT"),
            ],
        )
        # 4. 地域分布（横向条形排行——票面裁决：不做方格地图；60 分钟=设计值，同 3）
        upsert_dataset(
            "region_rank",
            "SELECT dim_key AS province, SUM(metric_value) AS pv "
            "FROM ecom_ads.ads_window_metric "
            "WHERE metric_name='region_pv' AND dim_key != '' "
            "AND window_start >= DATE_SUB(NOW(), INTERVAL 60 MINUTE) "
            "GROUP BY dim_key ORDER BY pv DESC",
            columns=[
                ("province", "STRING"),
                ("pv", "BIGINT"),
            ],
        )
        # 5. 三态 B 载体（Table + CASE 列 + 条件格式）＋漏斗观测/推断同屏双列
        #    30 分钟回看=设计值：正好覆盖「暂定→落定→修订」全生命周期的观察窗
        upsert_dataset(
            "window_states",
            "SELECT window_start, metric_name, dim_key, metric_value, revision, "
            f"{STATE_CASE} AS state, updated_at "
            "FROM ecom_ads.ads_window_metric "
            "WHERE metric_name IN ('event_page_view','uv','gmv') "
            "AND window_start >= DATE_SUB(NOW(), INTERVAL 30 MINUTE) "
            "ORDER BY window_start DESC, metric_name",
            columns=[
                ("window_start", "DATETIME"),
                ("metric_name", "STRING"),
                ("dim_key", "STRING"),
                ("metric_value", "BIGINT"),
                ("revision", "INTEGER"),
                ("state", "STRING"),
                ("updated_at", "DATETIME"),
            ],
        )
        upsert_dataset(
            "funnel_dual",
            "SELECT step_no, SUM(cnt_observed) AS cnt_observed, "
            "SUM(cnt_inferred) AS cnt_inferred, MAX(revision) AS revision "
            "FROM ecom_ads.ads_funnel_step "
            # 双日视图=设计值，与 funnel_steps 同口径（当日+昨日）
            "WHERE session_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) "
            "GROUP BY step_no ORDER BY step_no",
            columns=[
                ("step_no", "INTEGER"),
                ("cnt_observed", "BIGINT"),
                ("cnt_inferred", "BIGINT"),
                ("revision", "INTEGER"),
            ],
        )
        # 7. A 式双块（票面 ③ 补充：「暂定 Big Number 与落定值并排」）——
        #    观察中的窗口（is_settled=0）暂定值 vs 最近已落定窗口的值同屏。
        upsert_dataset(
            "kpi_dual_state",
            "SELECT window_start, metric_name, metric_value, is_settled, revision, "
            f"{STATE_CASE} AS state "
            "FROM ecom_ads.ads_window_metric "
            "WHERE metric_name IN ('event_page_view','uv','gmv') "
            "AND window_start >= DATE_SUB(NOW(), INTERVAL 15 MINUTE) "
            "ORDER BY window_start DESC",
            columns=[
                ("window_start", "DATETIME"),
                ("metric_name", "STRING"),
                ("metric_value", "BIGINT"),
                ("is_settled", "INTEGER"),
                ("revision", "INTEGER"),
                ("state", "STRING"),
            ],
        )
        # 6. 脏率面板（当前值 vs 基线 Δ；分子只含真脏——票面 ⑤）
        # 行由 docker/scripts/superset-dirty-rate-sync.sh 同步（dim_key 三行：
        # baseline_mirror / dlq_count_delta / total_events；本票暂停点清单第 3 条）
        upsert_dataset(
            "dirty_rate",
            "SELECT dim_key, metric_value, updated_at "
            "FROM ecom_ads.ads_window_metric "
            "WHERE metric_name='dlq_total_offset' AND window_start >= CURDATE() "
            "AND dim_key IN ('baseline_mirror','dlq_count_delta','total_events') "
            "ORDER BY window_start DESC LIMIT 3",
            columns=[
                ("dim_key", "STRING"),
                ("metric_value", "BIGINT"),
                ("updated_at", "DATETIME"),
            ],
        )
        db.session.commit()

        # ── ③ 图表（7 张：五面板 + 三态 B 载体 + 脏率）──────────────────
        def upsert_chart(
            name: str, dataset: SqlaTable, viz: str, params: dict
        ) -> Slice:
            slc = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
            payload = {
                "datasource": f"{dataset.id}__table",
                "viz_type": viz,
                **params,
            }
            if slc is None:
                slc = Slice(
                    slice_name=name,
                    datasource_id=dataset.id,
                    datasource_type="table",
                    viz_type=viz,
                    params=json.dumps(payload, ensure_ascii=False),
                )
                db.session.add(slc)
            else:
                slc.datasource_id = dataset.id
                slc.viz_type = viz
                slc.params = json.dumps(payload, ensure_ascii=False)
            db.session.flush()
            return slc

        pv_now = upsert_chart(
            "实时 PV（当前窗口）",
            datasets["kpi_current"],
            VIZ_BIG_NUMBER,
            {
                "metric": _adhoc_metric("metric_value", "PV"),
                "granularity_sqla": "window_start",
                "time_grain_sqla": GRAIN_PT1M,
                "adhoc_filters": [_eq_filter("metric_name", "event_page_view")],
                "y_axis_format": ",d",
                "subheader": "页面浏览量（暂定值随窗口跳动）",
            },
        )
        uv_now = upsert_chart(
            "实时 UV（1h 滚动去重）",
            datasets["kpi_current"],
            VIZ_BIG_NUMBER,
            {
                "metric": _adhoc_metric("metric_value", "UV"),
                "granularity_sqla": "window_start",
                "adhoc_filters": [_eq_filter("metric_name", "uv")],
                "y_axis_format": ",d",
                "subheader": "独立访客（精确去重）",
            },
        )
        gmv_now = upsert_chart(
            "实时 GMV（分）",
            datasets["kpi_current"],
            VIZ_BIG_NUMBER,
            {
                "metric": _adhoc_metric("metric_value", "GMV"),
                "granularity_sqla": "window_start",
                "adhoc_filters": [_eq_filter("metric_name", "gmv")],
                "y_axis_format": ",d",
                "subheader": "成交额·整数分（暂定值随窗口跳动）",
            },
        )
        pv_series = upsert_chart(
            "PV 分钟趋势",
            datasets["kpi_current"],
            VIZ_ECHART_TIMESERIES_BAR,
            {
                "metrics": [_adhoc_metric("metric_value", "PV")],
                "granularity_sqla": "window_start",
                "time_grain_sqla": GRAIN_PT1M,
                "adhoc_filters": [_eq_filter("metric_name", "event_page_view")],
                "groupby": [],
                "row_limit": 1000,  # 设计值：120 分钟 × 5 指标上限内的宽松界
            },
        )
        funnel = upsert_chart(
            "4 步转化漏斗（推断值）",
            datasets["funnel_steps"],
            VIZ_ECHART_FUNNEL,
            {
                "metrics": [_adhoc_metric("cnt_inferred", "推断值（含补记）")],
                "groupby": ["step_no"],
                "row_limit": 10,  # 设计值：步数固定 4，上限防炸
            },
        )
        treemap = upsert_chart(
            "品类分布 Treemap",
            datasets["category_dist"],
            VIZ_ECHART_TREEMAP,
            {
                "metrics": [_adhoc_metric("pv", "PV")],
                "groupby": ["category_id"],
                "row_limit": 50,  # 设计值：品类池 2000，看板取头部 50
            },
        )
        region = upsert_chart(
            "地域分布排行（横向条形）",
            datasets["region_rank"],
            VIZ_ECHART_TIMESERIES_BAR,
            {
                "metrics": [_adhoc_metric("pv", "PV")],
                "groupby": ["province"],
                "row_limit": 15,  # 设计值：省级单位上限 34，看板取头部
                # ★ 横向 = 类目轴放 Y（评审指出 orientation 键属 legacy
                #   dist_bar，对 echarts_timeseries_bar 是 no-op）
                "y_axis": "province",
                "x_axis": "pv",
            },
        )
        states = upsert_chart(
            "窗口三态（暂定/落定/已修订 · 同表同行）",
            datasets["window_states"],
            VIZ_TABLE,
            {
                "all_columns": [
                    "window_start",
                    "metric_name",
                    "dim_key",
                    "metric_value",
                    "revision",
                    "state",
                    "updated_at",
                ],
                "granularity_sqla": "window_start",
                "row_limit": 50,  # 设计值：三态表取近 30 分钟头部行
                "page_length": 15,  # 设计值：单页 15 行（Superset Table 默认 10）
                "conditional_formatting": [
                    {
                        "column": "state",
                        "operator": "==",
                        "value": "暂定",
                        "color": "#FFF7ED",
                        "textColor": "#B45309",
                    },
                    {
                        "column": "state",
                        "operator": "==",
                        "value": "已修订",
                        "color": "#EDE9FE",
                        "textColor": "#6D28D9",
                    },
                    {
                        "column": "state",
                        "operator": "==",
                        "value": "落定",
                        "color": "#D1FAE5",
                        "textColor": "#047857",
                    },
                ],
            },
        )
        funnel_dual = upsert_chart(
            "漏斗双列（观测/推断同屏）",
            datasets["funnel_dual"],
            VIZ_TABLE,
            {
                "all_columns": ["step_no", "cnt_observed", "cnt_inferred", "revision"],
                "granularity_sqla": "step_no",
                "row_limit": 10,
            },
        )
        dirty = upsert_chart(
            "脏率面板（当前值 vs 基线 Δ）",
            datasets["dirty_rate"],
            VIZ_TABLE,
            {
                "all_columns": ["dim_key", "metric_value", "updated_at"],
                "metrics": [],
                "groupby": ["dim_key"],
                "row_limit": 3,
                "conditional_formatting": [
                    {
                        "column": "dim_key",
                        "operator": "==",
                        "value": "dlq_count_delta",
                        "color": "#FEF3C7",
                        "textColor": "#B45309",
                    },
                ],
            },
        )
        # ★ A 式双块（票面 ③ 补充载体）：暂定值与落定值并排——「跳动的那个数
        #   是暂定值」的直觉载体。两张 Big Number 各筛一个状态（adhoc_filters
        #   形态，6.1.0 探索器同款；评审指出 metric/where 旧形态会被丢弃）。
        prov_number = upsert_chart(
            "当前窗口·暂定值（A 式）",
            datasets["kpi_dual_state"],
            VIZ_BIG_NUMBER,
            {
                "metric": {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "metric_value"},
                    "aggregate": "SUM",
                    "label": "暂定值",
                },
                "granularity_sqla": "window_start",
                "adhoc_filters": [
                    {
                        "expressionType": "SIMPLE",
                        "subject": "metric_name",
                        "operator": "==",
                        "operatorId": "EQUALS",
                        "comparator": "event_page_view",
                        "clause": "WHERE",
                    },
                    {
                        "expressionType": "SIMPLE",
                        "subject": "state",
                        "operator": "==",
                        "operatorId": "EQUALS",
                        "comparator": "暂定",
                        "clause": "WHERE",
                    },
                ],
                "y_axis_format": ",d",
                "subheader": "未关闭窗口的暂定 PV（会跳）",
                "time_grain_sqla": "PT1M",
            },
        )
        settled_number = upsert_chart(
            "最近窗口·落定值（A 式）",
            datasets["kpi_dual_state"],
            VIZ_BIG_NUMBER,
            {
                "metric": {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "metric_value"},
                    "aggregate": "SUM",
                    "label": "落定值",
                },
                "granularity_sqla": "window_start",
                "adhoc_filters": [
                    {
                        "expressionType": "SIMPLE",
                        "subject": "metric_name",
                        "operator": "==",
                        "operatorId": "EQUALS",
                        "comparator": "event_page_view",
                        "clause": "WHERE",
                    },
                    {
                        "expressionType": "SIMPLE",
                        "subject": "state",
                        "operator": "==",
                        "operatorId": "EQUALS",
                        "comparator": "落定",
                        "clause": "WHERE",
                    },
                ],
                "y_axis_format": ",d",
                "subheader": "已关闭窗口的落定 PV（不再变）",
                "time_grain_sqla": "PT1M",
            },
        )
        db.session.commit()

        # ── ④ Dashboard（单一载体；refresh=10s 进 json_metadata）────────
        charts = [
            pv_now,
            uv_now,
            gmv_now,
            pv_series,
            funnel,
            treemap,
            region,
            states,
            funnel_dual,
            dirty,
            prov_number,
            settled_number,
        ]
        dash = db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
        json_metadata = {
            # 票面 ④：10 s 是 DASHBOARD_AUTO_REFRESH_INTERVALS 最小自动档；
            # refresh_frequency 作用于打开页面的自动刷新。
            "refresh_frequency": 10,
            "color_scheme": "",
            "expanded_slices": {},
            "positions": {},
        }
        if dash is None:
            dash = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug=DASHBOARD_SLUG,
                published=True,
            )
            dash.uuid = uuidlib.uuid4()
            db.session.add(dash)
            print("[superset-provision] + dashboard（新建）")
        dash.slices = charts
        dash.published = True
        dash.json_metadata = json.dumps(json_metadata, ensure_ascii=False)
        db.session.commit()

        # ── ⑤ dataset 显式授 Public（票面 ① 第③步）────────────────────
        sm = app.appbuilder.sm
        pub = sm.find_role("Public")
        if pub is None:
            die("Public 角色不存在——先跑 superset init（superset-init.sh ③）")
        granted = 0
        for tbl in datasets.values():
            pvm = sm.find_permission_view_menu("datasource_access", tbl.perm)
            if pvm is None:
                die(f"找不到 datasource_access 权限对象：{tbl.perm}")
            if pvm not in pub.permissions:
                pub.permissions.append(pvm)
                granted += 1
        db.session.commit()
        print(
            f"[superset-provision] dataset 授权 Public：本次新授 {granted} 个（共 {len(datasets)} 个）"
        )

        print(
            f"[superset-provision] 完成：dashboard={DASHBOARD_SLUG} "
            f"charts={len(charts)} datasets={len(datasets)} "
            f"database_uuid={ECOM_ADS_UUID}"
        )


if __name__ == "__main__":
    main()
