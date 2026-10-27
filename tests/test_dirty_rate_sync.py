"""`superset-dirty-rate-sync.sh` 的**真因报错**守卫（impl-07a）。

## 这些守卫为什么存在

施工前该脚本这样读基线：

    baseline_dlq="$("${PYTHON:-python3}" -c "..." "$BASELINE_FILE" 2>/dev/null)" \\
      || fail "基线文件不可解析（缺 dlq_offset 字段？）"

`2>/dev/null` 把**一切**失败证据丢掉 ⇒ 四种互不相同的故障（解释器缺席 / 解释器在
PATH 上但是空壳 / 文件是坏 JSON / 文件缺字段）全被报成同一句「基线文件不可解析」。
其中**文件完好却被指控**的那两类最恶劣：排查方向被引向基线文件，而真因在执行面。

本文件钉住三件事（票面 Acceptance criteria 逐条）：

1. **解释器可用性的判据是「真的执行一条最小语句」**，不是 `command -v` 存在性探测
   ——空壳解释器（存在、可 invoke、执行静默失败）必须被抓住。这是 impl-05 §5
   原文建议的**反面**：那条「先 `command -v` 探测」已被实测证伪。
2. **四类故障互不冒充**：每类报自己的真因，且都带下一步动作。
3. **基线只解析一次**；「输入事件数」这个只被打印、从不参与判断的键，缺失**不再致命**。

## 测试策略：假 PATH 上的 `docker` + 假解释器，不连栈

脚本第②步要 `docker compose exec` 去读 Kafka offset。这里在临时目录造一个
`docker` 桩（kafka 报固定 offset、doris 直接成功）并前置到 `PATH`，于是整条脚本
可以在**无守护进程**的环境里跑到尾——判据覆盖的是脚本自己的行为，不是集群。
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _ROOT / "docker" / "scripts" / "superset-dirty-rate-sync.sh"
_BASH = shutil.which("bash")

# 假 docker 的固定 offset：DLQ 与输入同值 ⇒ Δ = 15000 - 基线 dlq_offset。
_STUB_OFFSET = 15_000

# 解释器自检的哨兵：脚本必须**核对输出**，只核 rc 挡不住「退 0 却什么都不打印」的壳。
_PROBE_SENTINEL = "impl07a-probe-ok"

_DOCKER_STUB = (
    "#!/bin/sh\n"
    "# 假 docker：只为让脚本跑到尾。kafka 读 offset 报固定值，doris 写入直接成功。\n"
    'case "$*" in\n'
    "  *kafka-get-offsets.sh*)\n"
    '    echo "ecom.behavior.event.v1.dlq:0:15000"\n'
    "    ;;\n"
    "esac\n"
    "exit 0\n"
)


def _script_text() -> str:
    return _SCRIPT.read_text(encoding="utf-8")


def _today() -> str:
    assert _BASH
    out = subprocess.run(
        [_BASH, "-c", "date +%Y-%m-%d"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return out.stdout.strip()


# 基线文件内容的三种来源，**互斥**（一个参数一个含义，不用 None 当哨兵）。
_NO_FILE = object()


def _run(
    tmp_path: Path,
    baseline: object = _NO_FILE,
    *,
    python: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """在假 PATH 上跑整条同步脚本。

    `baseline` 就是**要写进基线文件的原文**（造坏 JSON / 缺字段都靠它）；
    传 `_NO_FILE` 表示不建文件，验「基线文件不存在」分支。
    """
    assert _BASH, "本仓库的脚本执行面是 bash（Makefile 亦如此）"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    docker = bin_dir / "docker"
    docker.write_text(_DOCKER_STUB, encoding="utf-8")
    docker.chmod(0o755)

    baseline_file = tmp_path / "dirty-rate-baseline.json"
    if baseline is not _NO_FILE:
        assert isinstance(baseline, str)
        baseline_file.write_text(baseline, encoding="utf-8")

    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["BASELINE_FILE"] = str(baseline_file)
    if python is not None:
        env["PYTHON"] = python
    return subprocess.run(
        [_BASH, str(_SCRIPT)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        check=False,
    )


def _shell(path: Path, body: str) -> Path:
    """写一个可执行的 sh 假解释器/包装器（body 为脚本正文，行间用 \\n）。"""
    path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    path.chmod(0o755)
    return path


# ════════════════════════════════════════════════════════════════════════
# 静态形态：不再有裸 `2>/dev/null`
# ════════════════════════════════════════════════════════════════════════
def test_script_does_not_discard_stderr() -> None:
    """脚本里不得再出现 `2>/dev/null`（吞掉证据的那个重定向）。

    票面 AC：「新增守卫测试钉住「不再有裸 `2>/dev/null` 吞掉解析错误」这一形态」。
    取**全脚本零出现**而不是「解析段零出现」：只钉解析段挡不住下一个人把同一个
    反模式搬回别的错误分支；本脚本每一处需要 stderr 的地方都该**读它、说出去**。
    """
    offenders = [
        f"{i}: {line.strip()}"
        for i, line in enumerate(_script_text().splitlines(), 1)
        if "2>/dev/null" in line
    ]
    assert not offenders, (
        "superset-dirty-rate-sync.sh 又出现了吞 stderr 的 2>/dev/null："
        f"{offenders} —— 那会把真因压成一句假的「不可解析」，见 impl-07a"
    )


def test_script_probes_interpreter_by_execution_and_checks_output() -> None:
    """解释器自检必须是「执行一条最小语句 + 核对输出」。

    两条都要：只做存在性探测会放行空壳（实测：存在性通过、执行静默失败）；
    只核退出码会放行「退 0 但什么都不干」的壳。故脚本里必须有哨兵字符串、
    有对输出的捕获，且哨兵被用于**比对**。
    """
    text = _script_text()
    assert _PROBE_SENTINEL in text, (
        "缺解释器自检哨兵：脚本必须真的执行一条最小语句并核对输出"
        "（impl-05 §5 的「command -v 探测」已被实测证伪，不要照它做）"
    )
    assert "PY_PROBE_SENTINEL=" in text, "哨兵必须是脚本里的常量（供自检比对）"
    assert "py_probe_out" in text, (
        "自检必须**捕获**最小语句的 stdout（只跑不看等于没查）"
    )
    assert "2>&1)" not in text, (
        "自检不得把 stderr 折进哨兵比对：可用但启动时吐警告的解释器会被误报成不可用"
    )
    assert "command -v" not in text, (
        "不得用 command -v 做解释器判据——存在性探测会放行空壳（impl-05 §5 已被证伪）"
    )


# ════════════════════════════════════════════════════════════════════════
# 行为：四类真因互不冒充
# ════════════════════════════════════════════════════════════════════════
_GOOD = '{"dlq_offset": 14911, "input_events": 77700}'


def test_interpreter_missing_is_named_as_missing(tmp_path: Path) -> None:
    """解释器不在 PATH 上 ⇒ 报「缺席」，不得指控基线文件。"""
    r = _run(tmp_path, _GOOD, python="/nonexistent/pyimpl07a/python3")
    assert r.returncode != 0
    assert "解释器缺席" in r.stderr, f"应报解释器缺席，实得：{r.stderr}"
    assert "不可解析" not in r.stderr
    assert "JSON" not in r.stderr


def test_shell_interpreter_is_caught_by_execution_not_existence(tmp_path: Path) -> None:
    """★ 反例：**存在但不可用**的假解释器（退 49 且不吐 stderr）必须被抓到。

    这是票面指名的那一类：存在性探测会通过，只有真的执行才知道它不能用。
    报错必须是「解释器不可用」，**不得**是「基线文件不可解析」。
    """
    fake = _shell(tmp_path / "python3-shell-stub", "exit 49\n")
    r = _run(tmp_path, _GOOD, python=str(fake))
    assert r.returncode != 0
    assert "解释器不可用" in r.stderr, f"空壳解释器未被抓到，实得：{r.stderr}"
    assert "不可解析" not in r.stderr
    assert "不是基线文件的问题" in r.stderr, "报错必须点明真因不在基线文件上"


def test_broken_json_is_named_as_json(tmp_path: Path) -> None:
    """坏 JSON ⇒ 报「不是合法 JSON」，而不是「缺字段」。"""
    r = _run(tmp_path, '{"dlq_offset": 14911,')
    assert r.returncode != 0
    assert "不是合法 JSON" in r.stderr, f"坏 JSON 应被如此命名，实得：{r.stderr}"
    assert "解释器不可用" not in r.stderr, "坏 JSON 不得被报成解释器问题"
    assert "缺必需字段" not in r.stderr, "坏 JSON 不得被报成缺字段"


def test_missing_field_is_named_with_the_field(tmp_path: Path) -> None:
    """缺字段 ⇒ 报出**哪个**字段缺，而不是笼统的「不可解析」。"""
    r = _run(tmp_path, '{"input_events": 77700}')
    assert r.returncode != 0
    assert "缺必需字段" in r.stderr
    assert "dlq_offset" in r.stderr, f"报错必须点名缺失字段，实得：{r.stderr}"
    assert "解释器不可用" not in r.stderr, "字段缺失不得被报成解释器问题"
    assert "不是合法 JSON" not in r.stderr, "字段缺失不得被报成 JSON 损坏"


def test_every_error_message_carries_a_next_step(tmp_path: Path) -> None:
    """票面 AC：报错带**可操作的下一步**，不是只说哪里错了。"""
    cases = [
        _run(tmp_path / "a"),
        _run(tmp_path / "b", "{}"),
        _run(tmp_path / "c", _GOOD, python="/nonexistent/pyimpl07a/python3"),
    ]
    for r in cases:
        assert r.returncode != 0
        assert any(
            hint in r.stderr
            for hint in ("make verify", "export PYTHON", "apt-get install")
        ), f"报错缺下一步动作：{r.stderr}"


# ════════════════════════════════════════════════════════════════════════
# 行为：正常路径逐字不变 + 只解析一次
# ════════════════════════════════════════════════════════════════════════
def test_normal_path_output_is_unchanged(tmp_path: Path) -> None:
    """基线完好 ⇒ 输出与施工前**逐字一致**（票面 AC：正向路径零变化）。

    期望值逐行写出，而不是从脚本里推——否则断言就是在重算实现。
    """
    r = _run(tmp_path, _GOOD, python=shutil.which("python3") or "python3")
    assert r.returncode == 0, r.stderr
    assert r.stderr == "", r.stderr
    assert r.stdout.splitlines() == [
        f"[dirty-rate-sync] ▶ 读基线 {tmp_path / 'dirty-rate-baseline.json'}",
        "[dirty-rate-sync] 基线：dlq_offset=14911 input_events=77700",
        "[dirty-rate-sync] ▶ 读 Kafka DLQ 总 offset",
        "[dirty-rate-sync] ▶ 读 Kafka 输入 topic 总 offset",
        f"[dirty-rate-sync] 当前：dlq={_STUB_OFFSET} input={_STUB_OFFSET}",
        (
            "[dirty-rate-sync] ▶ 写入 ads_window_metric"
            f"（dlq_total_offset / dlq_count_delta，anchor={_today()}）"
        ),
        (
            f"[dirty-rate-sync] 完成：Δ={_STUB_OFFSET - 14911}"
            f"（dlq 14911→{_STUB_OFFSET}，input={_STUB_OFFSET}）"
        ),
        (
            "[dirty-rate-sync] 面板读法：dlq_count_delta / total_events = "
            f"{_STUB_OFFSET - 14911}/{_STUB_OFFSET}"
        ),
    ]


def test_baseline_is_parsed_exactly_once(tmp_path: Path) -> None:
    """基线**只解析一次**：恰好一个解释器进程收到过基线文件路径。

    旧形态为取两个键起了两个解释器进程 ⇒ 两次解析之间基线若正被 make verify 前推
    （impl-07c 的原子替换也在这一面），两次读到的可能是**不同版本**的两份文件。
    计数用包装解释器记 argv：`-c` 自检那次不带文件路径，解析那次才带。
    """
    log = tmp_path / "py.log"
    real = shutil.which("python3")
    assert real, "需要真 python3 做包装"
    wrapper = tmp_path / "python3-counting"
    body = 'printf "%s\\n" "$*" >> "' + str(log) + '"; exec "' + real + '" "$@"\n'
    wrapper.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    wrapper.chmod(0o755)
    r = _run(tmp_path, _GOOD, python=str(wrapper))
    assert r.returncode == 0, r.stderr
    calls = log.read_text(encoding="utf-8").splitlines()
    baseline_calls = [c for c in calls if "dirty-rate-baseline.json" in c]
    assert len(baseline_calls) == 1, (
        f"基线文件被解析了 {len(baseline_calls)} 次（应 1 次）：{baseline_calls}"
    )
    assert len(calls) == 2, (
        f"解释器应恰好被调 2 次（自检 + 解析），实得 {len(calls)}：{calls}"
    )


def test_input_events_is_log_only_and_not_fatal(tmp_path: Path) -> None:
    """「输入事件数」缺失**不再致命**（票面「（乙）」的处置：仅日志参考）。

    它从不参与判断或写入（面板 total_events 行写的是**当前**输入总数），
    所以它不该再成为一次致命失败的来源；但正常路径的打印要保留（逐字一致）。
    """
    r = _run(tmp_path, '{"dlq_offset": 14911}')
    assert r.returncode == 0, r.stderr
    assert "dlq_offset=14911" in r.stdout
    assert "input_events=<缺失·仅日志参考>" in r.stdout


def test_working_interpreter_with_stderr_noise_is_not_called_unavailable(
    tmp_path: Path,
) -> None:
    """★ 反例：**可用**但启动时往 stderr 吐一句警告的解释器，不得被报成不可用。

    票面纪律：「正向路径的行为变化一律视为回归」。若自检把 stderr 折进哨兵比对，
    任何带 sitecustomize/pyenv/conda banner 的**正常**解释器都会在正常路径上失败，
    ——那等于给本票再添一个假真因（code-review 抓到过这一版）。判据只看 stdout+rc。
    """
    real = shutil.which("python3")
    assert real, "需要真 python3 做包装"
    noisy = _shell(
        tmp_path / "python3-noisy",
        'echo "warning: sitecustomize noise" >&2\nexec "' + real + '" "$@"\n',
    )
    r = _run(tmp_path, _GOOD, python=str(noisy))
    assert r.returncode == 0, r.stderr
    assert "解释器不可用" not in r.stderr


def test_non_integer_dlq_offset_is_named_with_a_next_step(tmp_path: Path) -> None:
    """dlq_offset 类型不对 ⇒ 报「不是整数」并给下一步，不得裸炸在算术上。

    旧形态下 `{"dlq_offset": "oops"}` 会被当成 OK 一路走到 `$(( ))`，
    报 `oops: unbound variable`——既不说原因也不给动作（票面 AC 反例）。
    """
    r = _run(tmp_path, '{"dlq_offset": "oops"}')
    assert r.returncode != 0
    assert "不是整数" in r.stderr, f"应报类型错误，实得：{r.stderr}"
    assert "unbound variable" not in r.stderr
    assert "make verify" in r.stderr, "报错必须带下一步动作"


def test_negative_dlq_offset_is_rejected_with_a_next_step(tmp_path: Path) -> None:
    """基线为负 ⇒ 报「被改坏了」，而不是带着负数往 Δ 里算。"""
    r = _run(tmp_path, '{"dlq_offset": -5}')
    assert r.returncode != 0
    assert "负数" in r.stderr, f"应报负数，实得：{r.stderr}"
