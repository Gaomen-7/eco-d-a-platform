#!/usr/bin/env python3
"""反例/正例的可注入落盘器：在新旧两种写法各自「中断窗口内」停下来，供父进程 kill。

父进程（02）用它演示两件事：

- **施工前写法**：`write_text` 一 `open(...,"w")` 就把目标清零 ⇒ 在 kill 时刻，
  目标文件是**半截 JSON**（不可解析）。这就是 impl-07c 要消灭的面。
- **施工后写法**：目标文件在 `os.replace` 之前**一个字节都没被碰过** ⇒ 在同一个 kill
  时刻，目标仍是**完整的旧版**、可解析，被写坏的只有临时文件。

「落到中断窗口」用 `SIGSTOP` 自停（父进程 `waitpid` 收到 WUNTRACED 即确认已到达该点），
再由父进程 `SIGKILL` —— 这是**真进程、真信号、真文件系统**，不是进程内造假。

用法：evidence_killer.py <impl: old|new> <report_dir>
"""

from __future__ import annotations

import os
import signal
import sys
from pathlib import Path


def _stop_here() -> None:
    """在中断窗口内自停，等父进程 kill。"""
    os.kill(os.getpid(), signal.SIGSTOP)


def main() -> int:
    impl, report_dir = sys.argv[1], sys.argv[2]
    repo = Path(__file__).resolve().parents[5]
    sys.path.insert(0, str(repo / "src"))

    if impl == "old":
        # 施工前形态：先截断再写。把「open 之后、写完之前」这一段停下来。
        path = Path(report_dir) / "dirty-rate-baseline.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = '{\n  "dlq_offset": 15555,\n  "input_events": 80000\n}\n'
        with open(path, "w", encoding="utf-8") as handle:  # ← 此刻目标已被清零
            handle.write(payload[: len(payload) // 2])
            handle.flush()
            _stop_here()  # 父进程在这杀
            handle.write(payload[len(payload) // 2 :])
        return 0

    # 施工后形态：真调的 `save_dirty_rate_baseline`，只在 os.replace 前停下来。
    from ecomgen import verify_runner as vr

    real_replace = vr.os.replace

    def _stopping_replace(src: object, dst: object) -> None:
        _stop_here()  # ⚠️ 停在这里：临时文件已写好并 fsync，目标尚未被替换
        real_replace(src, dst)  # type: ignore[arg-type]

    vr.os.replace = _stopping_replace  # type: ignore[assignment]
    vr.save_dirty_rate_baseline(
        vr.DirtyRateSample(dlq_offset=15555, input_events=80000), report_dir
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
