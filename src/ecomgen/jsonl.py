"""JSONL 落盘的**唯一**实现（impl-03c）。

**为什么单独一个模块**：事件流（`cli.py` 的 `dump`）与维度变更流（`dims.py` 的
`write_dimension_stream`）都必须满足同一条可复现判据 ——
**键序固定、无空格、`\n` 换行、末尾换行**。两边各写一遍 `json.dumps(...)`
时，任一侧少一个 `sort_keys=True` 就会让「同 seed 同字节」的判据按文件类型分叉，
而两份文件各自的单测都还是绿的（各自和自己比当然相等）。

★ 只处理「一行一个 JSON 对象」这一种形状；带缩进的 manifest / plan 不走这里。
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """写 JSONL：键序固定、无空格、`\n` 换行、末尾有换行。

    父目录不存在则创建（调用方不必各自 mkdir）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            )
            handle.write("\n")
