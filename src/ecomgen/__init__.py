"""ecomgen — 电商用户行为数据生成器（模块①）。

P1 实现：五类事件、漏斗递减、三个注入器、`dump`/`live` 双模式。
调用面：`uv run ecomgen dump --seed N --out events.jsonl`
"""

__all__ = ["cli", "events", "funnel", "injectors", "producer", "world"]
