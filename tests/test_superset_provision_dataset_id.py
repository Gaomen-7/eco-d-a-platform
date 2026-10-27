"""`superset-provision.sh` 的 **dataset id 解析真因** 守卫（impl-07b）。

## 这些守卫为什么存在

施工前，置备脚本用一段内联解析从接口响应里取 kpi_current 的 dataset id：

    DS_ID="«D»(curl -s -m 20 "…/api/v1/dataset/?q=…" -H "Accept: application/json" \
      | "${PYTHON:-python3}" -c 'import json,sys
    try:
        d = json.load(sys.stdin)
        rows = d.get("result", [])
        print(next(str(r["id"]) for r in rows if r.get("table_name") == "kpi_current"))
    except StopIteration:
        pass')"
    [ -n "$DS_ID" ] || fail "找不到 kpi_current 的 dataset id"

那段代码把**三类**互不相同的失败折叠成同一个「空」：

| 真因 | 施工前的报错 | 该报的 |
| --- | --- | --- |
| 接口确实没有这个 dataset | 找不到 dataset id | 找不到（唯一该这么报的一类） |
| 接口结构变了 / 行里没 id 键 | 找不到 dataset id | 结构变了，按实际形状改解析段 |
| 响应不是 JSON / 解释器不可用 / 连接失败 | 找不到 dataset id | 没能解析出目标（真因在执行面） |

第三类最恶劣：**接口正常、dataset 存在**，脚本却报「找不到」⇒ 排查方向被引向
「看板里是不是没这个 dataset / 置备是不是没跑」。本文件钉住：

1. 「确实没有」与「没能解析出目标」必须是**两种不同的失败**，各自带下一步；
2. 结构变了（result 非数组 / 行缺 table_name / 目标行缺 id / id 不可用）**不得**
   被报成「没有这个 dataset」；
3. 正常路径的输出与施工前**逐字一致**（正向路径的行为变化一律视为回归）。

## 测试策略：假 PATH 上的 `curl` + `docker`，不连栈

脚本在到达解析段之前要跑 pymysql 检查、置备、匿名可达断言；解析段之后还有 zip 往返与
脏率同步。这里在临时目录造 `curl` / `docker` 两个桩并前置到 `PATH`，再把导出 zip 与脏率
同步脚本都指到临时目录 ⇒ 整条脚本在**无守护进程、无 Superset**的环境里跑到尾，
判据覆盖的是脚本自己的分流行为，不是集群。

★ 与 `tests/test_dirty_rate_sync.py`（impl-07a）**刻意不合并**：那边是文件输入的数值键，
这里是接口响应的对象标识符——输入源与语义不同，合成一套桩只会两边都不像。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _ROOT / "docker" / "scripts" / "superset-provision.sh"
_BASH = shutil.which("bash")

# 假 docker：脚本里所有 docker 调用（pymysql 检查 / 置备 / zip 往返）都直接成功。
_DOCKER_STUB = "#!/bin/sh\nexit 0\n"

# 假 curl：按 URL 分流 —— dataset 列表回 IMPL07B_BODY 等环境变量指向的响应原文，
# chart_data 回 status=success，其余回一段带 dashboard_title 的 JSON（匿名断言要它）。
# 带 -o 的调用把响应体写进该文件并打印状态码；不带 -o 的调用把响应体打到 stdout
# （施工前的管道形态就是这么取数据的 ⇒ 同一套桩能同时跑旧脚本做逐字对照）。
_CURL_STUB = r"""#!/bin/sh
out=""
prev=""
url=""
for a in "$@"; do
  if [ "$prev" = "-o" ]; then out="$a"; fi
  case "$a" in http://*|https://*) url="$a" ;; esac
  prev="$a"
done
code="${IMPL07B_CODE:-200}"
case "$url" in
  *"/api/v1/dataset/"*)
    if [ -n "${IMPL07B_BODY_FILE:-}" ]; then body="$(cat "${IMPL07B_BODY_FILE}")"; else body=""; fi
    ;;
  *"/api/v1/chart/data"*) body='{"status": "success"}' ;;
  *) body='{"dashboard_title": "ecom-live"}' ;;
esac
if [ -n "$out" ]; then
  [ "$out" = "/dev/null" ] || printf '%s' "$body" > "$out"
  # 状态码只对 dataset 列表接口可配：其余调用（匿名可达断言自检）恒 200，
  # 否则故障注入会先在匿名断言上炸，测不到解析段的分流。
  case "$url" in
    *"/api/v1/dataset/"*) printf '%s' "$code" ;;
    *) printf '%s' 200 ;;
  esac
else
  printf '%s' "$body"
fi
exit "${IMPL07B_CURL_RC:-0}"
"""

_GOOD_BODY = json.dumps(
    {
        "result": [
            {"id": 1, "table_name": "ads_window_metric"},
            {"id": 42, "table_name": "kpi_current"},
        ]
    }
)


def _script_text() -> str:
    return _SCRIPT.read_text(encoding="utf-8")


def _stub(bin_dir: Path, name: str, body: str) -> None:
    p = bin_dir / name
    p.write_text(body, encoding="utf-8")
    p.chmod(0o755)


def _prepare(tmp_path: Path) -> dict[str, str]:
    """造假 PATH（docker/curl 桩）+ 导出 zip / 脏率同步的临时落点，返回环境变量片段。"""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    _stub(bin_dir, "docker", _DOCKER_STUB)
    _stub(bin_dir, "curl", _CURL_STUB)
    sync = tmp_path / "dirty-sync-stub.sh"
    sync.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    return {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "SUPERSET_EXPORT_ZIP": str(tmp_path / "export" / "ecom-live.zip"),
        "SUPERSET_DIRTY_SYNC": str(sync),
    }


def _run(
    tmp_path: Path,
    body: str = _GOOD_BODY,
    *,
    code: int = 200,
    curl_rc: int = 0,
    python: str | None = None,
    script: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """在假 PATH 上跑整条置备脚本（$body$就是 dataset 列表接口的响应原文）。"""
    assert _BASH, "本仓库的脚本执行面是 bash（Makefile 亦如此）"
    env = dict(os.environ)
    env.update(_prepare(tmp_path))
    body_file = tmp_path / "dataset-list.json"
    body_file.write_text(body, encoding="utf-8")
    env["IMPL07B_BODY_FILE"] = str(body_file)
    env["IMPL07B_CODE"] = str(code)
    env["IMPL07B_CURL_RC"] = str(curl_rc)
    if python is not None:
        env["PYTHON"] = python
    return subprocess.run(
        [_BASH, str(script or _SCRIPT)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=tmp_path,  # 临时目录里没有 artifacts/ ⇒ 脏率段走「基线不存在」分支（确定）
        check=False,
    )


def _expected_stdout(tmp_path: Path, *, ds_id: str) -> list[str]:
    """正常路径的逐行期望（手写，不从脚本里推——否则断言只是在重算实现）。"""
    zip_path = tmp_path / "export" / "ecom-live.zip"
    return [
        "[superset-provision] ▶ 检查 PyMySQL 驱动（docker-bootstrap.sh requirements-local 装载）",
        "[superset-provision] ✓ pymysql 在位",
        (
            "[superset-provision] ▶ 运行 superset-provision.py"
            "（固定 database_uuid + dataset + 图表 + dashboard + Public 授权）"
        ),
        "[superset-provision] ▶ 匿名 GET /superset/dashboard/ecom-live/（期望 200，不重定向 login）",
        "[superset-provision] ✓ 匿名页面 200",
        "[superset-provision] ▶ 匿名 GET 评审 URL（?standalone=1，票面 ①——演示干净形态，CSP 同源放行）",
        "[superset-provision] ✓ ?standalone=1 评审 URL 200",
        "[superset-provision] ▶ 匿名 GET /api/v1/dashboard/ecom-live（期望 200 + json）",
        "[superset-provision] ✓ 匿名 dashboard API 200",
        "[superset-provision] ▶ 解析 kpi_current dataset id（不硬编码）",
        f"[superset-provision] ✓ kpi_current id={ds_id}",
        "[superset-provision] ▶ 匿名 chart_data 出数（kpi_current 的 UV，期望 status=success）",
        "[superset-provision] ✓ 匿名 chart_data 出数（链路活着）",
        f"[superset-provision] ▶ export-dashboards（刷新 {zip_path}）",
        f"[superset-provision] ✓ zip 已导出（{zip_path}）",
        "[superset-provision] ▶ import-dashboards（zip 回读校验；6.1.0 CLI 必带 -u）",
        "[superset-provision] ✓ zip 导入回读成功",
        f"[superset-provision] ▶ 脏率基线同步（{tmp_path / 'dirty-sync-stub.sh'}）",
        (
            "[superset-provision]   （artifacts/verify/dirty-rate-baseline.json 不存在"
            "——先跑 make verify 立基线（首跑判据 SKIP 时落盘）；面板脏率行暂缺，不阻塞置备）"
        ),
        "[superset-provision] ▶ 完成",
    ]


# ════════════════════════════════════════════════════════════════════════
# 正常路径：取到真 id，且输出与施工前逐字一致
# ════════════════════════════════════════════════════════════════════════
def test_normal_path_reports_the_real_dataset_id(tmp_path: Path) -> None:
    """dataset 存在且响应正常 ⇒ 取到那个 id、退出 0、不写 stderr。"""
    r = _run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert r.stderr == "", r.stderr
    assert r.stdout.splitlines() == _expected_stdout(tmp_path, ds_id="42")


def test_normal_path_matches_pre_change_script(tmp_path: Path) -> None:
    """★ 票面 AC：正常路径的输出与施工前**逐字一致**。

    把 git HEAD 上的旧脚本取到临时文件，与新脚本在**同一套桩、同一份响应**下各跑
    一遍：stdout 逐字、stderr 逐字、退出码一致。旧脚本的管道形态在同一个 curl 桩上能
    正常工作（桩把响应体打到 stdout），所以这是真对照，不是自证。
    """
    show = subprocess.run(
        ["git", "show", "HEAD:docker/scripts/superset-provision.sh"],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    old = tmp_path / "superset-provision.head.sh"
    old.write_text(show.stdout, encoding="utf-8")

    # 两遍用**同一个** tmp_path：脚本会打印 zip 与脏率脚本的路径，路径不同会伪造出
    # 「输出不一致」的假红。
    new_r = _run(tmp_path)
    old_r = _run(tmp_path, script=old)
    assert new_r.returncode == old_r.returncode == 0, (old_r.stderr, new_r.stderr)
    assert new_r.stdout.splitlines() == old_r.stdout.splitlines(), (
        "正常路径 stdout 与施工前不一致（正向路径的变化一律视为回归）"
    )
    assert new_r.stderr == old_r.stderr == ""


# ════════════════════════════════════════════════════════════════════════
# 反例：三类失败互不冒充（票面「解析失败与未找到必须是两种不同的失败」）
# ════════════════════════════════════════════════════════════════════════
_NOT_FOUND_MARK = "确实不在接口返回的列表里"
_PARSE_FAIL_MARK = "没能从 dataset 列表响应里解析出目标"


def test_absent_dataset_is_the_only_case_reported_as_not_found(tmp_path: Path) -> None:
    """★ 真·找不到的凭证：合法列表里没有 kpi_current ⇒ 必须报「找不到」。

    这一条是**反向保护**：新逻辑不得把这个唯一的合法失败吞掉或改写。
    """
    body = json.dumps({"result": [{"id": 1, "table_name": "ads_window_metric"}]})
    r = _run(tmp_path, body)
    assert r.returncode != 0
    assert _NOT_FOUND_MARK in r.stderr, f"应报「确实没有」，实得：{r.stderr}"
    assert "找不到 kpi_current 的 dataset id" in r.stderr
    assert _PARSE_FAIL_MARK not in r.stderr, "真·找不到不得被改写成一类解析失败"


def test_non_json_body_is_reported_as_parse_failure_not_absent(tmp_path: Path) -> None:
    """★ 反例凭证：响应体不是合法 JSON ⇒ 报「没能解析出目标」，**不得**报「找不到」。"""
    r = _run(tmp_path, "<!DOCTYPE html>\n<html><body>login</body></html>\n")
    assert r.returncode != 0
    assert _PARSE_FAIL_MARK in r.stderr, f"应报解析失败，实得：{r.stderr}"
    assert "不是合法 JSON" in r.stderr
    assert "找不到 kpi_current 的 dataset id" not in r.stderr, (
        "响应不是 JSON 时不得指控「看板里没有这个 dataset」——真因在执行面"
    )


def test_truncated_json_is_reported_as_parse_failure(tmp_path: Path) -> None:
    """半截 JSON（响应被截断）同样属于「没能解析出目标」，不得报「找不到」。"""
    r = _run(tmp_path, '{"result": [{"id": 42, "table_name": "kpi_cur')
    assert r.returncode != 0
    assert _PARSE_FAIL_MARK in r.stderr
    assert "找不到 kpi_current 的 dataset id" not in r.stderr


def test_api_error_message_is_not_reported_as_absent(tmp_path: Path) -> None:
    """接口回 error message（403 等）⇒ 报「接口在报错」，不是「没有这个 dataset」。"""
    r = _run(tmp_path, json.dumps({"message": "Forbidden"}), code=403)
    assert r.returncode != 0
    assert "接口在报错" in r.stderr, f"应报接口报错，实得：{r.stderr}"
    assert "Forbidden" in r.stderr
    assert "找不到 kpi_current 的 dataset id" not in r.stderr


def test_changed_structure_is_named_as_structure_change(tmp_path: Path) -> None:
    """★ 接口返回的结构变了（取值路径对不上）⇒ 报「结构变了」，不得报「找不到」。"""
    r = _run(tmp_path, json.dumps({"result": {"items": []}}))
    assert r.returncode != 0
    assert "结构变了" in r.stderr, f"应报结构变化，实得：{r.stderr}"
    assert "result 不是数组" in r.stderr
    # 票面 L35-36：取值路径不对属于「没能解析出目标」大类 ⇒ 必须同时命中该大类的措辞。
    assert _PARSE_FAIL_MARK in r.stderr, "结构变化必须归入「没能解析出目标」大类"
    assert "找不到 kpi_current 的 dataset id" not in r.stderr


def test_row_without_id_key_is_named_as_structure_change(tmp_path: Path) -> None:
    """目标行存在但**没有 id 键** ⇒ 报结构变化（点名缺的键），不是「找不到」。"""
    body = json.dumps({"result": [{"table_name": "kpi_current", "uid": "42"}]})
    r = _run(tmp_path, body)
    assert r.returncode != 0
    assert "结构变了" in r.stderr
    assert "id" in r.stderr and "table_name" in r.stderr
    assert _PARSE_FAIL_MARK in r.stderr, "缺 id 键属「没能解析出目标」大类"
    assert "找不到 kpi_current 的 dataset id" not in r.stderr


def test_unusable_id_value_is_named_not_used_as_identifier(tmp_path: Path) -> None:
    """id 存在但不可用（None / 空串）⇒ 不得把它当标识符送进 chart_data。"""
    r = _run(
        tmp_path, json.dumps({"result": [{"id": None, "table_name": "kpi_current"}]})
    )
    assert r.returncode != 0
    assert "id 不可用" in r.stderr, f"应报 id 不可用，实得：{r.stderr}"
    assert "chart_data" not in r.stdout, "不可用的 id 不得继续往下用"


def test_unreachable_superset_is_not_reported_as_absent(tmp_path: Path) -> None:
    """连响应都没取回来（curl 退非 0）⇒ 报「没能取回响应」，不是「没有这个 dataset」。"""
    r = _run(tmp_path, "", curl_rc=7)
    assert r.returncode != 0
    assert "没能取回 dataset 列表响应" in r.stderr, f"应报连接失败，实得：{r.stderr}"
    assert "找不到 kpi_current 的 dataset id" not in r.stderr


def test_missing_interpreter_is_not_reported_as_absent(tmp_path: Path) -> None:
    """★ 反例凭证：解析器不可用（不在 PATH）⇒ 报解析器问题，**不得**报「找不到」。

    这是本票点名要消灭的失败模式：接口正常、dataset 存在，却报「找不到」。
    """
    r = _run(tmp_path, _GOOD_BODY, python="/nonexistent/pyimpl07b/phantom-python3")
    assert r.returncode != 0
    assert "解析器" in r.stderr and "无法执行" in r.stderr, (
        f"应报解析器问题，实得：{r.stderr}"
    )
    assert "找不到 kpi_current 的 dataset id" not in r.stderr


def test_present_but_unusable_interpreter_is_pinned(tmp_path: Path) -> None:
    """★ 票面 L22 的第三类：解释器**存在但不可用**（空壳：退 49、零输出、零 stderr）。

    这一类不是「解释器缺席」（rc 不是 127），走的是「解析没有任何机读输出」那一臂。
    报错必须点明「先查解释器是否可用」，因为接口与 dataset 此时都完全正常。
    """
    stub = tmp_path / "python3-shell-stub"
    stub.write_text("#!/bin/sh\nexit 49\n", encoding="utf-8")
    stub.chmod(0o755)
    r = _run(tmp_path, _GOOD_BODY, python=str(stub))
    assert r.returncode != 0
    assert "解析没有任何机读输出" in r.stderr, f"实得：{r.stderr}"
    assert "rc=49" in r.stderr
    assert "空壳解释器" in r.stderr, "必须指出空壳解释器这一真因"
    assert "找不到 kpi_current 的 dataset id" not in r.stderr
    assert _PARSE_FAIL_MARK in r.stderr


def test_other_rows_missing_table_name_key_are_skipped_like_before(
    tmp_path: Path,
) -> None:
    """★ 回归钉（code-review 抓到）：**别的行**缺 table_name 键 ⇒ 跳过，不是失败。

    旧代码是 `r.get("table_name")`，对缺键行只是跳过；新解析器一度对任意行缺键就报
    STRUCT 致命 ⇒ 目标行存在、另有一行缺键时，旧脚本成功而新脚本退 1，违反「正常时零变化」。
    """
    body = json.dumps(
        {
            "result": [
                {"id": 7},  # 无关行，没有 table_name 键
                {"id": 42, "table_name": "kpi_current"},
            ]
        }
    )
    r = _run(tmp_path, body)
    assert r.returncode == 0, f"别的行缺键不得致命，实得：{r.stderr}"
    assert "✓ kpi_current id=42" in r.stdout


def test_dataset_id_is_used_verbatim_without_stripping(tmp_path: Path) -> None:
    """★ 回归钉（code-review 抓到）：id 必须与施工前**逐字一致**（旧代码 str(r["id"])）。

    带空白/前导零的 id 不得被 strip 掉——那会与票面「同 id、逐字一致」有出入。
    """
    body = json.dumps({"result": [{"id": " 0042 ", "table_name": "kpi_current"}]})
    r = _run(tmp_path, body)
    assert r.returncode == 0, r.stderr
    assert "✓ kpi_current id= 0042 " in r.stdout, f"id 被改写了：{r.stdout}"


def test_every_error_message_carries_a_next_step(tmp_path: Path) -> None:
    """票面 AC：报错带**可操作的下一步**（该查接口返回什么 / 该重跑置备 / 该看解释器）。"""
    cases = {
        "absent": _run(tmp_path / "a", json.dumps({"result": []})),
        "non-json": _run(tmp_path / "b", "<html>login</html>"),
        "struct": _run(tmp_path / "c", json.dumps({"result": 3})),
        "api-error": _run(
            tmp_path / "d", json.dumps({"message": "Forbidden"}), code=403
        ),
        "no-conn": _run(tmp_path / "e", "", curl_rc=7),
        "no-python": _run(
            tmp_path / "f", _GOOD_BODY, python="/nonexistent/pyimpl07b/phantom-python3"
        ),
    }
    for name, r in cases.items():
        assert r.returncode != 0, f"{name} 应当失败"
        assert "下一步" in r.stderr, f"{name} 的报错缺下一步动作：{r.stderr}"
        assert any(
            hint in r.stderr
            for hint in (
                "superset-provision.sh",
                "make init",
                "curl",
                "apt-get install",
                "PYTHON",
            )
        ), f"{name} 的下一步不够具体：{r.stderr}"


# ════════════════════════════════════════════════════════════════════════
# 静态形态：解析不得再「把失败折叠成空」
# ════════════════════════════════════════════════════════════════════════
def test_parse_no_longer_collapses_failure_into_empty_output() -> None:
    """解析段不得回到「管道 + except StopIteration: pass」形态。

    那种形态下 curl 的失败与解释器的失败**都表现为一个空串**，两者再被同一句
    「找不到」收口——正是本票要消灭的误诊。钉三条：
      1. 不存在 except StopIteration（把「没找到」与「解析炸了」折叠成同一个空）；
      2. 响应体先落盘（-o 到临时文件）再解析，不走「curl 直接管进解释器」的管道；
      3. 三类失败各有自己的机读标签（否则分流就退化成 if 空串）。
    """
    # 只看非注释行：注释里正当地复述旧形态（说明它在哪儿、为什么坏），不算回退。
    code_lines = [
        line
        for line in _script_text().splitlines()
        if not line.lstrip().startswith("#")
    ]
    code = "\n".join(code_lines)
    assert "except StopIteration" not in code, (
        "解析段又回到 except StopIteration: pass：它把「没找到」与「解析失败」折叠成同一个空"
    )
    text = "\n".join(code_lines)
    assert '"\u0024{PY_BIN}" -c' not in text, (
        "解析段不得再用 python -c 直接吃管道：管道形态下 curl 失败与解析失败都是空串"
    )
    assert '"\u0024{PY_BIN}" - "\u0024{DS_BODY}"' in text, (
        "解析必须先把响应体落进 DS_BODY 临时文件，再把文件路径交给解释器"
    )
    assert "NOT_FOUND" in text and "BAD_JSON" in text and "STRUCT" in text, (
        "分流标签缺失：三类失败必须各有自己的机读标签"
    )
    # MSYS_NO_PATHCONV 自防已有唯一属主：tests/test_superset_delivery.py 的
    # test_provision_sh_guards_msys_path_conversion（同一个文件，不重复钉）。
