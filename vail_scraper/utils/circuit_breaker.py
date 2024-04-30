import asyncio
from types import TracebackType
from typing import Final, Type


class CircuitBreaker:
    def __init__(self, max_errors: int, per: float) -> None:
        self.max_errors: Final[int] = max_errors
        self.per: Final[float] = per
        self.tripped: bool = False
        self._current_errors: int = 0

    def __enter__(self) -> None:
        if self.tripped:
            raise CicuitTrippedError()

    def __exit__(self, exc_type: Type[BaseException], exc: BaseException, tb: TracebackType) -> None:
        if exc is not None:
            if self._current_errors == 0:
                event_loop = asyncio.get_running_loop()
                event_loop.call_later(self.per, self._reset)
            self._current_errors += 1
            if self._current_errors == self.max_errors:
                self.tripped = True
        raise exc

    def _reset(self) -> None:
        self._current_errors = 0


class CicuitTrippedError(Exception):
    def __init__(self) -> None:
        super().__init__("the circuit popped")
