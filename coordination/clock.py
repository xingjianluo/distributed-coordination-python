"""时间源抽象。

生产环境使用单调系统时钟；测试使用可手动推进的 :class:`FakeClock`，
从而确定性地验证租约过期、过期续租等时序场景。
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """返回当前时间（单调秒）。"""
        ...


class SystemClock:
    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """测试用时钟：从固定起点开始，只能前进不能回退。"""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("时钟只能前进，不能回退")
        self._now += seconds
