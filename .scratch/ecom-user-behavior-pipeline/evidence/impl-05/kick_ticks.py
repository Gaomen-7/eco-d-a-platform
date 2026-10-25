"""impl-05 取证辅助：逐分区踢水位 tick，然后按给定间隔轮询 Doris 的探针键行。

用法（WSL）：
  uv run python .scratch/ecom-user-behavior-pipeline/evidence/impl-05/kick_ticks.py
"""

import sys
import time

sys.path.insert(0, "src")

from ecomgen.reconcile_probe import send_watermark_ticks


def main() -> int:
    print("ticks sent:", send_watermark_ticks())
    time.sleep(10)
    print("second round:", send_watermark_ticks())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
