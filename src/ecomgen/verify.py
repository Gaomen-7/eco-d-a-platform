"""verify 的分层判据与结果模型（spec.md §4.3 / §4.5）。

**为什么判据要独立成模型而不是散在 bash 里**：本票的交付物是「骗不过去的判据」，
判据本身的语义（什么算通过、什么算跳过、退出码怎么算）必须可单测。把它做成
纯数据模型 + 可注入的检查函数，就能在不碰 Docker 的前提下验证：

1. P1 形态「4 真 + 1 SKIP」⇒ 退出码 0；
2. SKIP **既不算通过也不算失败**——这是结构性堵死「打 PENDING 却假通过」的关键；
3. fail 三层痕迹（stdout 行 / JSON 报告 / 原始响应快照）都真的落盘。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any


class Status(StrEnum):
    """断言状态。`SKIP` 是一等公民，不是「未实现」的代名词。"""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class CheckResult:
    """单条断言结果。

    `actual` / `expected` 是给 stdout 人读面对照用的；`detail` 只在 FAIL 时
    作为原始响应快照落盘（spec §4.5 第 3 层）。
    """

    status: Status
    name: str
    actual: str
    expected: str
    detail: str | None = None

    @property
    def is_pass(self) -> bool:
        return self.status is Status.PASS

    @property
    def is_failure(self) -> bool:
        """只有 FAIL 算失败。SKIP **不**算（否则第三项会恒真失败）。"""
        return self.status is Status.FAIL

    def format_line(self) -> str:
        """stdout 人读行：`PASS/FAIL/SKIP <名> <实测> vs <期望>`。"""
        return f"{self.status.value} {self.name} {self.actual} vs {self.expected}"


@dataclass
class VerifyReport:
    """一次 verify / verify-fault 的全部断言结果。

    `degradation` 是 **F6 降级阶梯**的实测值（spec 附录 A·Q）：容器被 OOM 杀
    的次数、阈值、逐容器原始状态。它**不折成一条断言就完事**——阈值 2 次是
    **设计值**（非官方保证），报告里必须留下实测值供人复核。
    """

    checks: list[CheckResult] = field(default_factory=list)
    degradation: dict[str, Any] | None = None

    def exit_code(self) -> int:
        """有任一 FAIL ⇒ 1；否则 0（SKIP 不影响退出码）。"""
        return 1 if any(c.is_failure for c in self.checks) else 0

    def counts(self) -> dict[str, int]:
        counts = {"PASS": 0, "FAIL": 0, "SKIP": 0}
        for check in self.checks:
            counts[check.status.value] += 1
        return counts

    def format_lines(self) -> list[str]:
        return [c.format_line() for c in self.checks]

    def write_json(self, directory: Path) -> Path:
        """写 JSON 报告 + （有 FAIL 时）原始响应快照。

        返回报告路径。文件名用 **UTC** 时间戳——多次运行不覆盖，便于对比。
        """
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = directory / f"verify-report-{stamp}.json"

        payload: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "counts": self.counts(),
            "exit_code": self.exit_code(),
            "degradation": self.degradation,
            "checks": [
                {
                    "status": c.status.value,
                    "name": c.name,
                    "actual": c.actual,
                    "expected": c.expected,
                }
                for c in self.checks
            ],
        }
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

        # 第 3 层：FAIL 时快照原始响应全文（人看报告不够，要能看原始输出）
        failures = [c for c in self.checks if c.is_failure and c.detail]
        if failures:
            snapshot = path.with_suffix(".snapshot.txt")
            blocks = []
            for c in failures:
                blocks.append(f"===== {c.name} =====\n{c.detail}\n")
            snapshot.write_text("\n".join(blocks), encoding="utf-8")

        return path
