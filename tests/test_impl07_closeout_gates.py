"""impl-07 母票（收口门）的守卫 ——「对照不得自证」＋「收口门本身必须真跑」。

## 这个文件为什么存在（以及它**不**重复什么）

三张副票各自已有行为守卫：`test_dirty_rate_sync.py`（07a，四类真因）、
`test_superset_provision_dataset_id.py`（07b，找不到 vs 没解析出来）、
`test_verify_baseline_atomic.py`（07c，原子落盘）。**母票不重跑那些判据**——那是副票的活。

母票这一层要钉的是**收口门自身的可信度**，即三个**很可能假绿**的位置：

1. **对照自证**：07a/07b 的「与施工前逐字一致」用 `git show HEAD:` 取旧版本。施工提交
   入库后 `HEAD` 就是新脚本 ⇒ 对照退化成「新脚本 vs 新脚本」，永远 PASS、零信息量。
   这是「用实现证明实现」的隐蔽形态：**看起来有一条对照，实际什么都没比**。
   ⇒ 本文件钉住：仓库里任何以 `HEAD:` 为锚做「施工前对照」的地方都必须显式声明锚，
   且锚必须**不是** HEAD 的当前值（即锚上内容与工作树不同）。已实测：把 07b 测试里的
   锚改回 `HEAD` 时，那条对照的守卫**必红**（`_PRE_CHANGE_COMMIT` 断言）。

2. **收口门没有可执行落点**：母票的收口门（故障注入四类、正常路径零变化、置备侧对照）
   若只存在于票面散文里，下一个人无法复跑，也无法知道它还成不成立。
   ⇒ 本文件钉住三份门脚本存在、可执行、且**退出码语义是硬的**（红了退非 0，不是只打印）。

3. **门脚本自己不做事**：门脚本最容易写成「只 echo 一堆 PASS」的剧场。
   ⇒ 本文件对每份门脚本做**结构性断言**：必须有 `FAIL=` 计数、必须 `exit 1`、必须真的
   调用被测脚本，且断言数（ok/no 调用点）不得退化到只剩个位数。

这些断言都是**静态**的（读文本），因此不连 docker、不进 Superset，秒级完成。
行为判据由三份门脚本自己跑（`PASS=n FAIL=0 退 0`），见 `evidence/impl-07/`。
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_EVIDENCE = _ROOT / ".scratch" / "ecom-user-behavior-pipeline" / "evidence" / "impl-07"

# 门 B/C 的施工前锚：必须是一个**具体提交号**（见下面 test_gate_scripts_anchor_on_a_real_commit_not_head）。
_ANCHOR_RE = re.compile(r'PRE_CONSTRUCTION="([0-9a-f]{7,40})"')

# 本仓的脚本执行面是 bash（Makefile 亦如此）。缺 bash 时给出**明确的失败原因**，
# 而不是让 subprocess 抛一个裸 FileNotFoundError（与 test_dirty_rate_sync.py:38 /
# test_superset_provision_dataset_id.py:54 同一口径）。
_BASH = shutil.which("bash")

# 三份收口门脚本（母票 Acceptance criteria 第 2/3/4 条各一份）。
_GATES = {
    "01-baseline-chain-fault-injection.sh": "四类故障注入（解释器缺席/空壳/坏JSON/缺字段）",
    "02-normal-path-vs-preconstruction.sh": "正常路径与施工前逐字零变化",
    "03-provision-notfound-vs-other.sh": "置备侧：找不到 vs 真因不是找不到",
}


def _gate_text(name: str) -> str:
    return (_EVIDENCE / name).read_text(encoding="utf-8")


# ════════════════════════════════════════════════════════════════════════
# 1. 对照不得自证
# ════════════════════════════════════════════════════════════════════════
def test_provision_guard_anchors_on_a_fixed_commit_not_head() -> None:
    """07b 的「施工前对照」必须锚在固定提交上，不得用 `HEAD:`。

    用 `HEAD:` 的版本在施工**当时**是对的，提交入库后就会自证（见文件头 §1）。
    本仓现有唯一一处这类对照就在 `test_superset_provision_dataset_id.py`。
    """
    text = (_ROOT / "tests" / "test_superset_provision_dataset_id.py").read_text(
        encoding="utf-8"
    )
    assert '"git", "show", "HEAD:' not in text, (
        "07b 的施工前对照又用回了 HEAD: —— 提交入库后它会退化成「新脚本 vs 新脚本」，"
        "永远 PASS 且无信息量。改用固定提交（_PRE_CHANGE_COMMIT）"
    )
    assert "_PRE_CHANGE_COMMIT" in text, (
        "对照的锚必须是显式常量（供人复核它是否真的是施工前）"
    )


def test_provision_guard_asserts_the_anchor_is_not_the_worktree() -> None:
    """光把锚写成常量不够：锚若被写成当前提交，照样自证 ⇒ 必须有「锚≠工作树」的断言。

    ★ code-review（Standards 轴）指出第一版这里是**为错的理由通过**的：
    `assert "_script_text()" in text` 在文件里有 3 处命中，其中与锚无关的那处也能让它绿。
    ⇒ 改成钉**紧邻锚的那条断言**：`old.read_text(...) != _script_text()` 这个比较必须在，
    且必须出现在 `_PRE_CHANGE_COMMIT` 之后（即真的用来校验锚）。
    """
    text = (_ROOT / "tests" / "test_superset_provision_dataset_id.py").read_text(
        encoding="utf-8"
    )
    anchor_at = text.find("_PRE_CHANGE_COMMIT")
    assert anchor_at >= 0, "缺 _PRE_CHANGE_COMMIT 常量"
    # 锚被**引用**在 `git show f"{_PRE_CHANGE_COMMIT}:..."` 里。
    assert 'f"{_PRE_CHANGE_COMMIT}:' in text, (
        "锚必须真的用来取旧版本（git show 的 ref 部分）"
    )
    # 「锚≠工作树」的比较必须存在，且在锚之后（真的在用它校验）。
    cmp_at = text.find("!= _script_text()")
    assert cmp_at >= 0, (
        "缺「锚上脚本 != 工作树脚本」的断言：锚一旦漂到当前提交，对照会静默自证"
    )
    assert cmp_at > anchor_at, (
        "「锚≠工作树」的比较必须出现在锚之后（真的用于校验那个锚）"
    )


def _code_lines(text: str) -> list[str]:
    """只保留**非注释**行（`#` 起头的行剥掉），供形态断言用。

    注释里出现 `git show HEAD:` 是在**解释**为什么不许用它，不是在用它；
    若把注释也算进去，守卫会误红——于是守卫自己变成噪音，下一个人就会删掉它。
    """
    return [line for line in text.splitlines() if not line.lstrip().startswith("#")]


def test_gate_scripts_anchor_on_a_real_commit_not_head() -> None:
    """门 B/C 的锚必须是**具体提交**，且不得在求值时依赖 HEAD。

    ★ code-review（Standards 轴）打穿过第一版：`assert "HEAD:" not in code` 只挡 `HEAD:`
    这个 token，而把 `PRE_CONSTRUCTION="fc1445f"` 改成 `PRE_CONSTRUCTION="HEAD"` 时
    **仍然全绿** —— 那正是最该挡的回归（锚变成「当前提交」，提交入库后立即自证）。
    ⇒ 改成两条**形态**判据：①锚的值必须是 7-40 位十六进制提交号；②代码里不得出现
    `git rev-parse HEAD` / `HEAD:` 这类**在求值时指向当前提交**的形态。
    """
    import re

    commit_re = re.compile(r'PRE_CONSTRUCTION="([0-9a-f]{7,40})"')
    for name in (
        "02-normal-path-vs-preconstruction.sh",
        "03-provision-notfound-vs-other.sh",
    ):
        code = "\n".join(_code_lines(_gate_text(name)))
        m = commit_re.search(code)
        assert m, (
            f"{name} 的 PRE_CONSTRUCTION 必须直接写成一个**具体提交号**"
            '（形如 PRE_CONSTRUCTION="fc1445f"）；写成变量/HEAD 都会在提交后自证'
        )
        anchor = m.group(1)
        assert anchor != "HEAD", f"{name} 的锚被改成了 HEAD ⇒ 提交后自证"
        assert not re.search(r"git\s+rev-parse\s+HEAD", code), (
            f"{name} 不得用 `git rev-parse HEAD` 求锚（那是当前提交）"
        )
        assert "HEAD:" not in code, f"{name} 不得用 `HEAD:` 取旧版本"


def test_gate_scripts_verify_the_anchor_commit_exists() -> None:
    """锚必须是仓库里**真实存在**的提交（写一个不存在的哈希不能算锚）。"""
    import subprocess

    for name in (
        "02-normal-path-vs-preconstruction.sh",
        "03-provision-notfound-vs-other.sh",
    ):
        code = "\n".join(_code_lines(_gate_text(name)))
        m = _ANCHOR_RE.search(code)
        assert m, f"{name} 缺 PRE_CONSTRUCTION 常量"
        r = subprocess.run(
            ["git", "cat-file", "-e", f"{m.group(1)}^{{commit}}"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert r.returncode == 0, (
            f"{name} 的锚 {m.group(1)} 在仓库里不存在：{r.stderr.strip()}"
        )


# ════════════════════════════════════════════════════════════════════════
# 2. 收口门有可执行落点
# ════════════════════════════════════════════════════════════════════════
def test_all_three_gate_scripts_exist() -> None:
    """母票 AC 第 2/3/4 条各有**一份**可复跑的门脚本（且各自有输出存档）。"""
    missing = [n for n in _GATES if not (_EVIDENCE / n).is_file()]
    assert not missing, f"缺收口门脚本：{missing}（票面散文不是可执行落点）"


def test_all_three_gate_outputs_are_archived() -> None:
    """每份门脚本都要有**实跑输出**存档（「不接受『看起来对』」）。"""
    missing = [
        f"{n[:-3]}.out.txt"
        for n in _GATES
        if not (_EVIDENCE / f"{n[:-3]}.out.txt").is_file()
    ]
    assert not missing, f"缺收口门实跑输出：{missing}"


def test_gate_scripts_are_syntactically_valid() -> None:
    """门脚本自己得先是合法 bash（语法坏了的门等于没有门）。"""
    assert _BASH, "本仓库的脚本执行面是 bash（Makefile 亦如此）"
    for name in _GATES:
        r = subprocess.run(
            [_BASH, "-n", str(_EVIDENCE / name)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert r.returncode == 0, f"{name} 语法错：{r.stderr}"


# ════════════════════════════════════════════════════════════════════════
# 3. 门脚本必须真的在判，不是只打印
# ════════════════════════════════════════════════════════════════════════
def test_gate_scripts_fail_hard() -> None:
    """每份门脚本必须①累计 FAIL ②**末行**按 FAIL 判 exit —— 红了退非 0。

    只 echo「PASS」的剧场脚本是收口门最典型的假绿形态：它永不失败，也就从不判任何东西。

    ★ code-review（Standards 轴）用变异打穿过这条的第一版：`assert "exit 1" in text`
    命中的是脚本**开头**几条中止路径（锚不存在 / 取不到响应 / 语法错），把真正的
    判定行 `[ "$FAIL" -eq 0 ] || exit 1` 改成 `exit 0` 后仍全绿。⇒ 改成断言**判定行
    本身**：从文件尾往前找第一条 `[ "$FAIL" ... ] || exit ...`，它必须 `exit 1`。
    """
    import re

    for name, purpose in _GATES.items():
        text = _gate_text(name)
        assert "FAIL=" in text, f"{name}（{purpose}）没有 FAIL 计数 ⇒ 它不会失败"
        assert "PASS=" in text, f"{name}（{purpose}）没有 PASS 计数"
        # 从尾往前找判定行；它必须真的按 FAIL 退非 0。
        verdict = None
        for line in reversed(text.splitlines()):
            if re.search(r'FAIL"?\s*"?-eq\s*"?0', line):
                verdict = line
                break
        assert verdict is not None, (
            f'{name}（{purpose}）没有 `[ "$FAIL" -eq 0 ]` 形态的判定行 ⇒ 它没有在判'
        )
        assert "exit 1" in verdict, (
            f"{name}（{purpose}）的判定行不是 `|| exit 1`（实得：{verdict.strip()}）"
            " ⇒ 它红了也不退非 0"
        )


def test_gate_scripts_actually_invoke_the_scripts_under_test() -> None:
    """门脚本必须**真的跑**被测脚本，而不是只读它的源码做静态检查。

    判据是「脚本里出现 `bash "<变量>/被测脚本名"` 形态的**执行**」——写成
    `bash "$SYNC"` / `bash "${SH}"` 这类形态即命中。只 grep 名字出现（比如注释里提到）
    不算执行，故要求同时出现 `bash` 与一个 `$` 变量、且该变量指向被测脚本。
    """
    expectations = {
        "01-baseline-chain-fault-injection.sh": "superset-dirty-rate-sync.sh",
        "02-normal-path-vs-preconstruction.sh": "superset-dirty-rate-sync.sh",
        "03-provision-notfound-vs-other.sh": "superset-provision.sh",
    }
    for name, target in expectations.items():
        text = _gate_text(name)
        assert target in text, f"{name} 没有引用被测脚本 {target}"
        # 真的执行：出现 `bash $"` 或 `bash ${` 的调用形态（变量里装的是被测脚本路径）。
        executed = 'bash "$' in text or "bash ${" in text
        assert executed, (
            f'{name} 没有以 `bash "$VAR"` 形态**执行**脚本——名字出现可能只是在注释里提到'
        )


def test_gate_scripts_have_a_non_trivial_number_of_assertions() -> None:
    """断言点不得退化成个位数——门的意义就是覆盖面。

    计数口径是「**任何位置**出现 `ok`/`no`/`assert_*` 调用」（含 `if …; then ok …` 单行形态）：
    先按「行首」数过一版，结果漏掉了单行 if 里的调用点，把门 B 的 11 条判据数成 5 条
    ——**守卫自己量错了**。数量阈值的意义是挡「被删到只剩一两条」的静默劣化，故取 ≥8
    （三份现有分别为 29/11/35 条，留了余量）。
    """
    import re

    call_re = re.compile(r"\b(ok|no|assert_[a-z_]+)\s+\"")
    for name in _GATES:
        code = "\n".join(_code_lines(_gate_text(name)))
        # 先去掉函数定义行（`ok() {` / `assert_class() {`），它们不是调用点。
        body = "\n".join(
            line
            for line in code.splitlines()
            if not re.match(r"^\s*\w+\(\)\s*\{", line)
        )
        calls = len(call_re.findall(body))
        assert calls >= 8, f"{name} 只有 {calls} 个断言调用点，覆盖面已退化"


def test_gate_a_asserts_all_four_fault_classes_do_not_impersonate_each_other() -> None:
    """母票 AC 第 2 条的要害是「四类**互不冒充**」，不只是「四类都报了点东西」。"""
    text = _gate_text("01-baseline-chain-fault-injection.sh")
    for cls in ("解释器缺席", "解释器不可用", "不是合法 JSON", "缺必需字段"):
        assert cls in text, f"门 A 没有断言「{cls}」这一类"
    assert "assert_not_classes" in text, (
        "门 A 必须断言「不得冒充其他类」——否则四类都报同一句话也能通过"
    )
    assert "矩阵" in text, "门 A 必须做四类矩阵自检（每类只命中自己）"


def test_gate_a_keeps_the_command_v_refutation_on_record() -> None:
    """母票 AC 最后一节要求留档「impl-05 的 `command -v` 建议已被证伪」。

    光在票面写一句话不够：证伪的**反证**要可复跑，否则下一个人还会再提一次这条建议。

    ★ code-review（Standards 轴）打穿过第一版：`assert "command -v" in text` 命中的是
    门 A 第 ⑦ 段那条**静态守卫**（`grep -q "command -v" "$SYNC"`），把 `[2b]` 反证整段
    （761 B）删掉后仍全绿。⇒ 改成钉 `[2b]` 段的**实质**：要有造出来的假解释器、
    要有「存在性探测」与「真跑一次」两个相反结论，且都要被 `ok/no` 断言（不是只 echo）。
    """
    code = "\n".join(_code_lines(_gate_text("01-baseline-chain-fault-injection.sh")))
    assert "留档反证" in code, "门 A 必须保留 `[2b]` 留档反证段的标题（供人一眼找到）"
    assert "存在性探测" in code, "门 A 必须打出「存在性探测」的结论"
    assert "真跑一次" in code, "门 A 必须打出「真跑一次」的结论"
    probe_asserts = [
        line
        for line in code.splitlines()
        if ("存在性探测" in line or "真跑一次" in line)
        and ("ok " in line or "no " in line)
    ]
    assert len(probe_asserts) >= 2, (
        "门 A 的 [2b] 反证段必须有 ≥2 条 **断言**（存在性探测 / 真跑一次各一条）；"
        f"实得 {len(probe_asserts)} 条 —— 只 echo 不判，等于没留证"
    )
    assert "python3-shell-stub" in code and "exit 49" in code, (
        "门 A 必须真的造一个「存在但不可执行」的假解释器（exit 49）"
    )


# ════════════════════════════════════════════════════════════════════════
# 4. 母票与副票的状态口径一致（防「副票已结、母票还写着未结」）
# ════════════════════════════════════════════════════════════════════════
def test_subtickets_are_resolved_when_mother_is_resolved() -> None:
    """母票 AC 第 1 条：三张副票全部收口，且**票面状态与本票一致**。

    这里只钉「副票都 resolved」这一半（母票自己的状态由票面维护）；若哪天有人把某张
    副票回退成 open 而母票还写 resolved，本条会红——那正是母票 AC 说不许出现的状态。

    ★ code-review（Spec 轴）打穿过第一版：`assert "Status: resolved" in text` 是**子串**
    测试，而 07a 正文里恰好有一句散文「…`Status: resolved`」（§6 的 A1 行）⇒ 把 07a 的
    表头改成 `Status: open` 后**仍然全绿**，正好违背票面承诺的「必红」。
    改成解析**表头**：`Status:` 行必须存在，且第一行的值严格等于 `resolved`
    （正文里出现同样的字样一律不算数）。
    """
    issues = _ROOT / ".scratch" / "ecom-user-behavior-pipeline" / "issues"
    for name in (
        "impl-07a-dirty-rate-baseline-parse-true-cause.md",
        "impl-07b-provision-dataset-id-true-cause.md",
        "impl-07c-dirty-rate-baseline-atomic-write.md",
    ):
        lines = (issues / name).read_text(encoding="utf-8").splitlines()
        headers = [line.strip() for line in lines if line.strip().startswith("Status:")]
        assert headers, f"{name} 没有 `Status:` 表头行"
        # 票面状态写在文件顶部 ⇒ 取第一条，值必须严格是 resolved（不是「包含」）。
        value = headers[0].split(":", 1)[1].strip()
        assert value == "resolved", (
            f"{name} 的表头状态是 {value!r}，不是 `resolved` —— "
            "母票不能先于副票收口（正文里出现 `Status: resolved` 字样不算数）"
        )
