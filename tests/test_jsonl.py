"""`ecomgen.jsonl.write_jsonl` 的字节约定（impl-03c）。

**为什么单独测这一片**：事件流与维度变更流都靠它落盘，而「同 seed 同字节」的
可复现判据是按**文件**比对哈希的。实现只有一份，所以判据也只需要一份：
键序固定 / 无空格 / LF 换行 / 末尾有换行 / 父目录自动创建。
"""

from __future__ import annotations

import json
from pathlib import Path

from ecomgen.jsonl import write_jsonl


def test_writes_sorted_compact_lines_with_trailing_newline(tmp_path: Path) -> None:
    """一行一个对象：键序固定、无空格、LF、末尾换行。"""
    rows = [{"b": 2, "a": 1}, {"a": 3, "b": None}]
    out = tmp_path / "x.jsonl"
    write_jsonl(out, rows)

    raw = out.read_bytes()
    assert b"\r" not in raw, "出现 CRLF（约定是纯 LF）"
    assert raw.endswith(b"\n"), "文件末尾缺换行"
    text = raw.decode("utf-8")
    assert text == '{"a":1,"b":2}\n{"a":3,"b":null}\n', f"字节不符：{text!r}"


def test_creates_parent_directories(tmp_path: Path) -> None:
    """父目录不存在也要能写（调用方不必各自 mkdir）。"""
    out = tmp_path / "deep" / "nest" / "x.jsonl"
    write_jsonl(out, [{"a": 1}])
    assert json.loads(out.read_text(encoding="utf-8")) == {"a": 1}


def test_accepts_an_iterable_not_just_a_list(tmp_path: Path) -> None:
    """接受生成器（事件流按 enumerate 惰性产出，不必先物化）。"""
    out = tmp_path / "g.jsonl"
    write_jsonl(out, ({"i": i} for i in range(3)))
    assert [
        json.loads(x)["i"] for x in out.read_text(encoding="utf-8").splitlines()
    ] == [
        0,
        1,
        2,
    ]


def test_empty_stream_yields_empty_file(tmp_path: Path) -> None:
    """空输入产出空文件（不产出「只有一个换行」的怪文件）。"""
    out = tmp_path / "e.jsonl"
    write_jsonl(out, [])
    assert out.read_bytes() == b""
