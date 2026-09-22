"""时钟抽象：生产用系统时钟，测试用手动时钟以确定性模拟过期。"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """返回当前单调时间（秒）。"""
        ...


class SystemClock:
    def now(self) -> float:
        return time.monotonic()


class ManualClock:
    """测试用：时间只在 advance() 时前进。"""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("clock cannot go backwards")
        self._t += seconds
