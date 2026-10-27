"""脏率基线**原子落盘**的守卫（impl-07c）。

## 为什么这张票存在

基线文件是**闸门文件**：面板的基线行、以及脏率判据的读侧都依赖它，而且它是**自我维护**的
（首跑落盘、之后每次 PASS 前推、`--corrupt` 收尾吸收）——写入频率是常规路径级别，不是
一年一次。施工前的写法是**先截断再写**：

    path.write_text(json.dumps(...) + "\\n", encoding="utf-8")

`write_text` 先以 `"w"` 打开（**此时目标文件已被清零**）再往里写。若写的过程被中断
（Ctrl-C、进程被杀、掉电），目标文件就停在半截——读者随后拿到一段不完整的 JSON，
而读侧把解析错误一律报成「基线文件不可解析」⇒ **所有**后续面板同步与脏率判据一起
指向一个误导性的方向。

⇒ 修法：同目录写临时文件 → `os.replace` 原子替换。读者只会看到完整旧版或完整新版。

## 本文件钉住什么（票面 Acceptance criteria 逐条）

1. **格式零变化**：正常写入的字节与施工前的非原子写法**逐字一致**（字段、缩进、换行）。
   本票只改「怎么落盘」，不改「落成什么样」。
2. **两条落盘路径**：首次落盘（目标不存在，父目录可能也不存在）与覆盖落盘。
3. **读者侧零改动可用**：`load_dirty_rate_baseline` 拿到的永远是完整 JSON。
4. **反例**：写入被中断 ⇒ 目标文件**仍是完整的旧版**（不是半截），且下一次读它**能解析**；
   临时文件不残留。
5. **临时文件与目标同目录**（跨文件系统的 replace 不原子），且成功替换后不残留。
6. **失败不被吞**：落盘失败仍抛 `OSError`，由调用方兜底（票面 Notes 第 4 条）。
7. **静态守卫**：源码里**不得**再出现「先截断再写」的落盘形态。

## 测试策略

不连栈、不碰 `artifacts/`（那是运行产物）：每条用例都在 `tmp_path` 里造一个 report 目录。
「进程被打断」用两种手段模拟：①在 `os.replace` / 临时文件写入处注入异常（进程内，
可控且确定）；②真把子进程杀掉（`evidence/impl-07c/` 里的凭证脚本，见票面 Answer）。
"""

from __future__ import annotations

import builtins
import json
import os
import tempfile
import time
from pathlib import Path

import pytest

from ecomgen import verify_runner
from ecomgen.verify_runner import DirtyRateSample

# 施工前非原子写法的**逐字**输出（独立于实现手写的期望值，不是拿 json.dumps 复算——
# 那样断言就成了「用实现证明实现」，永远绿）。
_OLD_FORMAT_TEXT = '{\n  "dlq_offset": 14911,\n  "input_events": 77700\n}\n'


def _sample(dlq: int = 14911, total: int = 77700) -> DirtyRateSample:
    return DirtyRateSample(dlq_offset=dlq, input_events=total)


def _old_non_atomic_writer(sample: DirtyRateSample, path: Path) -> None:
    """施工前的落盘形态（impl-07c 施工前 `save_dirty_rate_baseline` 的写盘四行）。

    在这里**原样复刻**是为了让「新旧逐字一致」是一次真对照，而不是自证。
    """
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "dlq_offset": sample.dlq_offset,
        "input_events": sample.input_events,
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _dir_entries(directory: Path) -> set[str]:
    return {p.name for p in directory.iterdir()}


# ── 1. 格式零变化（票面 AC：本票不改基线的字段/格式/换行）────────────────


def test_saved_baseline_text_matches_old_non_atomic_format_exactly(
    tmp_path: Path,
) -> None:
    """★ 正例凭证：写入内容与施工前的非原子写法**逐字一致**。

    若这条红了，说明改落盘方式时顺手改了格式——票面明写「不改基线的字段、格式、换行」。
    """
    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    written = (tmp_path / verify_runner.DIRTY_RATE_BASELINE).read_text(encoding="utf-8")

    assert written == _OLD_FORMAT_TEXT


def test_saved_baseline_bytes_match_a_replica_of_the_old_writer(tmp_path: Path) -> None:
    """同一平台上**字节级**对照：新写法 vs 施工前写法的落盘结果。

    `read_text` 会吞掉换行差异（Windows 的 CRLF → LF），所以再比一次原始字节，
    把「同样的换行」这条 AC 也钉住。
    """
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    _old_non_atomic_writer(_sample(), old_dir / verify_runner.DIRTY_RATE_BASELINE)
    verify_runner.save_dirty_rate_baseline(_sample(), str(new_dir))

    old_bytes = (old_dir / verify_runner.DIRTY_RATE_BASELINE).read_bytes()
    new_bytes = (new_dir / verify_runner.DIRTY_RATE_BASELINE).read_bytes()

    assert new_bytes == old_bytes


# ── 2. 两条落盘路径：首次落盘 / 覆盖落盘 ────────────────────────────────


def test_first_write_creates_baseline_even_when_parent_dir_is_missing(
    tmp_path: Path,
) -> None:
    """首次落盘：目标与父目录都不存在 ⇒ 建目录、写完整文件（接口契约不变）。"""
    report_dir = tmp_path / "artifacts" / "verify"
    assert not report_dir.exists()

    returned = verify_runner.save_dirty_rate_baseline(_sample(), str(report_dir))

    target = report_dir / verify_runner.DIRTY_RATE_BASELINE
    assert returned == target
    assert target.read_text(encoding="utf-8") == _OLD_FORMAT_TEXT


def test_overwrite_replaces_existing_baseline_with_new_values(tmp_path: Path) -> None:
    """覆盖落盘：旧版被新版整体替换（不是拼在后面，也不是留下一段旧尾巴）。"""
    verify_runner.save_dirty_rate_baseline(_sample(14911, 77700), str(tmp_path))
    verify_runner.save_dirty_rate_baseline(_sample(15200, 80000), str(tmp_path))

    sample = verify_runner.load_dirty_rate_baseline(str(tmp_path))

    assert sample == _sample(15200, 80000)
    text = (tmp_path / verify_runner.DIRTY_RATE_BASELINE).read_text(encoding="utf-8")
    assert text == '{\n  "dlq_offset": 15200,\n  "input_events": 80000\n}\n'


# ── 3. 读者侧零改动可用 ───────────────────────────────────────────────


def test_reader_side_needs_no_change_and_sees_complete_json(tmp_path: Path) -> None:
    """读者拿到的永远是完整 JSON：落盘后立刻读，两条路径都解析得出原值。"""
    # 首次落盘后读
    verify_runner.save_dirty_rate_baseline(_sample(14911, 77700), str(tmp_path))
    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) == _sample(
        14911, 77700
    )

    # 覆盖落盘后读
    verify_runner.save_dirty_rate_baseline(_sample(15555, 80000), str(tmp_path))
    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) == _sample(
        15555, 80000
    )


# ── 4. 反例：写入被中断 ⇒ 目标仍是完整的旧版 ────────────────────────────


def test_interrupted_before_replace_keeps_old_version_intact_and_parseable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """★ 反例凭证（进程内）：替换前被打断 ⇒ 目标文件仍是**完整的旧版**。

    模拟「临时文件已写好、还没替换时进程被杀/Ctrl-C」。施工前的写法在这里会把目标
    截断成空文件（第一步就已清零），故这条正是本票要证伪的缺陷面。
    """
    verify_runner.save_dirty_rate_baseline(_sample(14911, 77700), str(tmp_path))

    def _die_before_replace(src: object, dst: object) -> None:
        raise KeyboardInterrupt("模拟：替换前进程被打断")

    monkeypatch.setattr(os, "replace", _die_before_replace)

    with pytest.raises(KeyboardInterrupt):
        verify_runner.save_dirty_rate_baseline(_sample(15555, 80000), str(tmp_path))

    target = tmp_path / verify_runner.DIRTY_RATE_BASELINE
    # 目标仍是完整的旧版，内容逐字未变
    assert target.read_text(encoding="utf-8") == _OLD_FORMAT_TEXT
    # 且下一次读它**能正常解析**（不是「文件不可解析」）
    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) == _sample(
        14911, 77700
    )
    # 临时文件不残留
    assert _dir_entries(tmp_path) == {verify_runner.DIRTY_RATE_BASELINE}


def test_any_failure_while_producing_the_new_content_never_touches_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """★ 反例：新内容还没产出来就失败 ⇒ 目标文件逐字未动，也没有残骸。

    这是更恶劣的时刻：施工前写法此时目标已被清零（`write_text` 一打开就截断），
    随后的失败把它留成「半截 JSON」。新写法下中断只发生在目标之外。
    """
    verify_runner.save_dirty_rate_baseline(_sample(14911, 77700), str(tmp_path))

    def _exploding_dumps(*args: object, **kwargs: object) -> str:
        raise ValueError("模拟：序列化中途被打断")

    monkeypatch.setattr(json, "dumps", _exploding_dumps)

    with pytest.raises(ValueError):
        verify_runner.save_dirty_rate_baseline(_sample(15555, 80000), str(tmp_path))

    target = tmp_path / verify_runner.DIRTY_RATE_BASELINE
    assert target.read_text(encoding="utf-8") == _OLD_FORMAT_TEXT
    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) == _sample(
        14911, 77700
    )
    assert _dir_entries(tmp_path) == {verify_runner.DIRTY_RATE_BASELINE}


def test_interrupted_first_write_leaves_no_half_written_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """首次落盘被打断 ⇒ 目标文件**不存在**（而不是「存在但是半截」）。

    这条钉住「首次落盘」这条路径的反例：读者此时看到的是「没有基线」（⇒ 判据 SKIP
    并重立），而不是一份不可解析的垃圾。
    """
    monkeypatch.setattr(
        os,
        "replace",
        lambda src, dst: (_ for _ in ()).throw(SystemExit("模拟：首次落盘时进程退出")),
    )

    with pytest.raises(SystemExit):
        verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    target = tmp_path / verify_runner.DIRTY_RATE_BASELINE
    assert not target.exists(), "被打断的首次落盘不得留下半截文件"
    assert _dir_entries(tmp_path) == set(), "临时文件不得残留"
    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) is None


# ── 5. 临时文件：同目录 + 不残留 ───────────────────────────────────────


def test_temp_file_is_created_in_the_same_dir_as_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """临时文件与目标**同目录**——跨文件系统的 `os.replace` 不保证原子。"""
    seen: list[Path] = []
    real_replace = os.replace

    def _spy(src: object, dst: object) -> None:
        seen.append(Path(str(src)))
        real_replace(src, dst)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "replace", _spy)

    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    assert len(seen) == 1
    assert seen[0].parent == tmp_path, "临时文件必须与目标同目录"
    assert not seen[0].exists(), "替换成功后临时文件不残留"


def test_temp_file_name_is_unique_per_process_not_a_fixed_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """临时名带进程号且不是固定名（票面 Notes：不要用会撞名的固定名）。"""
    names: list[str] = []
    real_replace = os.replace

    def _spy(src: object, dst: object) -> None:
        names.append(Path(str(src)).name)
        real_replace(src, dst)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "replace", _spy)

    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))
    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    assert len(names) == 2
    assert names[0] != names[1], "两次落盘的临时名不得相同"
    assert all(str(os.getpid()) in name for name in names), (
        f"临时名应带进程号（当前 {os.getpid()}）：{names}"
    )


def test_no_temp_residue_after_successful_overwrite(tmp_path: Path) -> None:
    """反复覆盖落盘后目录里**只有**基线文件本体。"""
    for dlq in (14911, 15000, 15100):
        verify_runner.save_dirty_rate_baseline(_sample(dlq, 77700), str(tmp_path))

    assert _dir_entries(tmp_path) == {verify_runner.DIRTY_RATE_BASELINE}


# ── 6. 落盘失败仍按今天的语义抛出（不得静默吞掉）────────────────────────


def test_temp_file_creation_failure_propagates_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """目录不可写 ⇒ `OSError` 照旧抛给调用方（票面 Notes 第 4 条：不得静默吞掉）。"""

    def _cannot_create(*args: object, **kwargs: object) -> object:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", _cannot_create)

    with pytest.raises(OSError):
        verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))


def test_replace_failure_without_old_file_leaves_nothing_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """替换失败同样抛 `OSError`，且不把半截临时文件留在目录里。"""
    real_replace = os.replace

    def _replace_fails(src: object, dst: object) -> None:
        # 真替换同样失败：证明 `OSError` **穿过**了写盘函数，而不是被 except 吞掉。
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(os, "replace", _replace_fails)

    with pytest.raises(OSError):
        verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    monkeypatch.setattr(os, "replace", real_replace)
    assert _dir_entries(tmp_path) == set()


# ── 7. 「不得先截断再写」的行为守卫（形态守卫见 §8）─────────────────────
#
# ★ 这两条为什么是**行为**判据而不是读源码文本
# 第一版守卫读函数源码做字符串黑名单，被 code-review 的 Spec 轴用三处变异打穿：
#   ① `with path.open("w")`（黑名单只查 `open(path`，查不到这种写法）；
#   ② 把截断挪进一个被调用的辅助函数（函数体内不再出现该模式）；
#   ③ 把 `os.replace` 包进 `except OSError: pass`（黑名单根本不管异常语义）。
# 故守卫下沉到**可观察行为**：目标文件在整个落盘过程中**从未**被以写模式打开，
# 且覆盖落盘**换了 inode**（原地重写不会换）。这两条对上述三种变异都敏感。


def test_target_file_is_never_opened_for_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """★ 行为守卫：落盘全程**没有任何**写模式打开指向目标文件的调用。

    钩住所有能截断文件的入口（`builtins.open` / `Path.open` / `Path.write_text` /
    `Path.write_bytes` / `os.open`），所以「截断挪进辅助函数」这种变异也逃不掉。
    """
    target = tmp_path / verify_runner.DIRTY_RATE_BASELINE
    verify_runner.save_dirty_rate_baseline(_sample(14911, 77700), str(tmp_path))

    touched: list[str] = []
    real_open = builtins.open
    real_path_open = Path.open
    real_write_text = Path.write_text
    real_write_bytes = Path.write_bytes
    real_os_open = os.open

    def _same(path: object) -> bool:
        try:
            return Path(str(path)).resolve() == target.resolve()
        except (OSError, ValueError):
            return False

    def _spy_open(file: object, mode: str = "r", *args: object, **kwargs: object):
        if _same(file) and any(flag in mode for flag in ("w", "a", "x", "+")):
            touched.append(f"builtins.open({file!r}, {mode!r})")
        return real_open(file, mode, *args, **kwargs)

    def _spy_path_open(self: Path, mode: str = "r", *args: object, **kwargs: object):
        if _same(self) and any(flag in mode for flag in ("w", "a", "x", "+")):
            touched.append(f"Path.open({self}, {mode!r})")
        return real_path_open(self, mode, *args, **kwargs)

    def _spy_write_text(self: Path, *args: object, **kwargs: object) -> int:
        if _same(self):
            touched.append(f"Path.write_text({self})")
        return real_write_text(self, *args, **kwargs)

    def _spy_write_bytes(self: Path, *args: object, **kwargs: object) -> int:
        if _same(self):
            touched.append(f"Path.write_bytes({self})")
        return real_write_bytes(self, *args, **kwargs)

    def _spy_os_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        if _same(path) and flags & (os.O_WRONLY | os.O_RDWR | os.O_TRUNC | os.O_APPEND):
            touched.append(f"os.open({path!r}, flags={flags})")
        return real_os_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _spy_open)
    monkeypatch.setattr(Path, "open", _spy_path_open)
    monkeypatch.setattr(Path, "write_text", _spy_write_text)
    monkeypatch.setattr(Path, "write_bytes", _spy_write_bytes)
    monkeypatch.setattr(os, "open", _spy_os_open)

    verify_runner.save_dirty_rate_baseline(_sample(15555, 80000), str(tmp_path))

    assert touched == [], f"目标文件被以写模式打开过（= 先截断再写）：{touched}"
    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) == _sample(
        15555, 80000
    )


@pytest.mark.skipif(os.name != "posix", reason="inode 语义只在 POSIX 上有意义")
def test_overwrite_replaces_the_inode_proving_no_in_place_truncation(
    tmp_path: Path,
) -> None:
    """★ 行为守卫：覆盖落盘**换了 inode** ⇒ 旧文件不是被原地重写的。

    这是「原子替换」最干净的机器可见证据：原地 `write_text` 复用同一个 inode，
    只有「新文件 + rename」才会换。变异①「截断挪进辅助函数」同样逃不掉。
    """
    verify_runner.save_dirty_rate_baseline(_sample(14911, 77700), str(tmp_path))
    target = tmp_path / verify_runner.DIRTY_RATE_BASELINE
    inode_before = target.stat().st_ino

    verify_runner.save_dirty_rate_baseline(_sample(15555, 80000), str(tmp_path))

    assert target.stat().st_ino != inode_before, (
        "覆盖落盘沿用了旧 inode ⇒ 是原地重写（先截断再写），不是原子替换"
    )


# ── 8. 静态形态守卫（黑名单；行为判据在上）─────────────────────────────


def _writer_code() -> str:
    """`save_dirty_rate_baseline` 的**代码**（剥掉 docstring），静态守卫用。

    剥 docstring 是必要的：第一版守卫在整段源码里找 `"OSError"`，命中的其实是
    docstring 里的散文，等于拿注释证明行为（code-review 两轴都点了这条）。
    """
    import ast

    src = Path(verify_runner.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name == "save_dirty_rate_baseline"
        ):
            body = list(node.body)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body = body[1:]  # 剥 docstring
            assert body
            start = min(child.lineno for child in body)
            end = max(
                getattr(child, "end_lineno", child.lineno) or child.lineno
                for child in body
            )
            return "\n".join(src.splitlines()[start - 1 : end])
    raise AssertionError("找不到 save_dirty_rate_baseline（守卫需要更新）")


def test_writer_code_contains_no_truncating_write_of_the_target() -> None:
    """★ 形态守卫：写盘函数里**不得**出现「先截断再写」的典型形态。

    这条是行为判据（§7）之外的一层廉价回归网，挡的是把实现整段改写回旧形态。
    「目标文件在整个过程中不得被写模式打开」这条**命题**由 §7 的钩子兜住，
    所以将来换用别的原子手段（例如 `os.rename` + `O_EXCL`）时，删掉本条不损失覆盖。
    """
    code = _writer_code()

    assert "os.replace(" in code, "原子替换调用点被删：半截文件的风险回来了"
    for banned in (
        "write_text(",
        "write_bytes(",
        'open("w"',
        "open('w'",
        '.open("w"',
        ".open('w'",
    ):
        assert banned not in code, f"写盘函数里出现了先截断再写的形态：{banned}"


# ── 9. 权限位：读者不改也得读得到（impl-07c code-review 实测发现）────────
#
# 实测缺陷：`tempfile.NamedTemporaryFile` 固定建 0600，`os.replace` 把 0600 一起搬
# 到目标上；而施工前的 `write_text` 走 `open(path,"w")` ⇒ `0o666 & ~umask`（本仓 0644）。
# 不管这条，首次落盘就把基线从 0644 悄悄降成 0600，本来读得到它的读者会丢掉读权限。


@pytest.mark.skipif(os.name != "posix", reason="POSIX 权限位")
def test_first_write_mode_matches_what_open_write_would_have_produced(
    tmp_path: Path,
) -> None:
    """首次落盘的权限位 = 施工前 `open(path,"w")` 的产物（`0o666 & ~umask`）。"""
    probe = tmp_path / "probe.txt"
    probe.write_text("x", encoding="utf-8")  # 与施工前同一形态
    expected = probe.stat().st_mode & 0o777

    report_dir = tmp_path / "report"
    verify_runner.save_dirty_rate_baseline(_sample(), str(report_dir))

    actual = (report_dir / verify_runner.DIRTY_RATE_BASELINE).stat().st_mode & 0o777
    assert actual == expected, (
        f"首次落盘权限位 {oct(actual)} ≠ 施工前的 {oct(expected)}：读者会丢读权限"
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX 权限位")
def test_overwrite_preserves_the_existing_mode(tmp_path: Path) -> None:
    """覆盖落盘**沿用**旧文件的权限位——不因为走了临时文件就把它改成 0600。"""
    target = tmp_path / verify_runner.DIRTY_RATE_BASELINE
    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))
    os.chmod(target, 0o644)

    verify_runner.save_dirty_rate_baseline(_sample(15555, 80000), str(tmp_path))

    assert target.stat().st_mode & 0o777 == 0o644, "覆盖落盘把旧权限位改掉了"


# ── 10. 硬杀残骸：finally 抓不住 SIGKILL，故下一次落盘前清扫 ────────────


def test_stale_temp_from_a_hard_kill_is_swept_on_next_write(tmp_path: Path) -> None:
    """★ SIGKILL 留下的临时文件在下一次落盘时被清掉（`finally` 对 SIGKILL 无效）。"""
    stale = tmp_path / f".{verify_runner.DIRTY_RATE_BASELINE}.999999.deadbeef.tmp"
    stale.write_text('{\n  "dlq_offset": 1', encoding="utf-8")  # 半截残骸
    old = time.time() - verify_runner.STALE_BASELINE_TEMP_GRACE_S - 10
    os.utime(stale, (old, old))

    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    assert not stale.exists(), "硬杀留下的残骸没有被清扫"
    assert _dir_entries(tmp_path) == {verify_runner.DIRTY_RATE_BASELINE}


def test_fresh_temp_of_a_concurrent_writer_is_not_swept(tmp_path: Path) -> None:
    """★ 反例保护：**新鲜**临时文件不得被清扫——否则会踩掉并发写者。

    清扫按「超龄」判定，所以同一时刻另一个进程正在写的临时文件是安全的。
    """
    fresh = tmp_path / f".{verify_runner.DIRTY_RATE_BASELINE}.123456.cafebabe.tmp"
    fresh.write_text("still being written", encoding="utf-8")  # mtime = 现在

    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    assert fresh.exists(), "新鲜临时文件被误删：并发写者会被踩掉"


def test_reader_side_needs_no_change_after_a_stale_temp_is_present(
    tmp_path: Path,
) -> None:
    """残骸在位也不影响读侧：读者按**确切文件名**读，不会被目录里的杂物误导。"""
    (tmp_path / f".{verify_runner.DIRTY_RATE_BASELINE}.999.dead.tmp").write_text(
        "garbage", encoding="utf-8"
    )
    verify_runner.save_dirty_rate_baseline(_sample(), str(tmp_path))

    assert verify_runner.load_dirty_rate_baseline(str(tmp_path)) == _sample()
